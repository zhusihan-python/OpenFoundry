// Command connector-management-service hosts the foundation slice of
// the Foundry Data Connection app (connections CRUD plus sync/media-set verticals).
//
// Foundation slice scope: connections CRUD on Postgres. sync_jobs,
// enterprise_connectivity and remaining connector runtime adapters land in follow-up
// slices.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/libs/capabilities/probes"
	"github.com/openfoundry/openfoundry-go/libs/observability"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/adapters"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/adapters/kafka"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/config"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/server"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/workers"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("service exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

// run owns the whole service lifecycle so EVERY exit path — listen errors,
// HTTP Shutdown timeouts, incomplete coordinator joins — flows through the
// same cleanup stack before main decides the exit code. No os.Exit below.
func run() error {
	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	cfg, err := config.FromEnv()
	if err != nil {
		return fmt.Errorf("config load: %w", err)
	}
	if cfg.Service.Version == "dev" {
		cfg.Service.Version = version
	}

	log := observability.InitLogging(cfg.Service.Name, cfg.Service.Version)
	shutdownTracing, err := observability.InitTracing(sigCtx, cfg.Service.Name, cfg.Service.Version)
	if err != nil {
		return fmt.Errorf("tracing init: %w", err)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	pool, err := pgxpool.New(sigCtx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("pgx pool: %w", err)
	}
	if err := repo.Migrate(sigCtx, pool); err != nil {
		pool.Close()
		return fmt.Errorf("migrations: %w", err)
	}

	store := &repo.Repo{Pool: pool}
	// P2.4 schedule boot identity: a dedicated per-process random UUID —
	// deliberately NOT the private lease-holder value, hostname or any lease
	// material (root round-1 rule).
	scheduleBootID := uuid.New()
	authTTL := secondsOrDefault(cfg.ScheduleAuthTTLSecs, 300*time.Second)
	scheduleAuthz := handlers.NewScheduleAuthorizer(handlers.ScheduleAuthorizerConfig{
		MemoryTTL: authTTL,
	}, store, scheduleBootID)
	scheduleAuthz.Start(context.Background())
	// Background schedulers run on their OWN root (not the signal context):
	// the lifecycle cancels it explicitly after serve exits — including on
	// listen errors, where no signal ever arrives — and joins them before the
	// pool closes (no naked goroutines).
	bgCtx, cancelBG := context.WithCancel(context.Background())
	var bgWG sync.WaitGroup
	if cfg.AutoRegistrationIntervalSecs > 0 {
		worker := &workers.AutoRegistrationWorker{Store: store, Clock: workers.RealClock{}, Recorder: workers.DefaultAutoRegistrationRecorder}
		interval := time.Duration(cfg.AutoRegistrationIntervalSecs) * time.Second
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			if err := worker.RunLoop(bgCtx, interval); err != nil && !errors.Is(err, context.Canceled) {
				log.Warn("auto-registration scheduler exited", slog.String("error", err.Error()))
			}
		}()
		log.Info("auto-registration scheduler enabled", slog.Duration("interval", interval))
	}

	jwt := authmw.NewJWTConfig(cfg.JWTSecret)
	adapterRegistry := adapters.NewRegistry()
	adapterRegistry.MustRegister("kafka", kafka.Factory())

	// P2.2 coordinator bounds: unset env keeps the defaults; explicit values
	// were already range-validated by config.FromEnv (out-of-range is a
	// startup error there).
	coordCfg := handlers.CoordinatorConfig{
		QueueDepth:    int64OrDefault(cfg.SyncAsyncQueueDepth, handlers.DefaultCoordinatorQueueDepth),
		Workers:       int64OrDefault(cfg.SyncAsyncWorkers, handlers.DefaultCoordinatorWorkers),
		AuthMemoryTTL: secondsOrDefault(cfg.SyncAsyncAuthTTLSecs, handlers.DefaultCoordinatorAuthTTL),
		TaskDeadline:  secondsOrDefault(cfg.SyncAsyncTaskTTLSecs, handlers.DefaultCoordinatorTaskDeadline),
	}
	h := &handlers.Handlers{
		Repo:            store,
		AdapterRegistry: adapterRegistry,
		MediaSetRuntime: &handlers.HTTPMediaSetRuntime{MediaSetsBaseURL: cfg.MediaSetsServiceURL},
		Config: handlers.RuntimeConfig{
			DatasetServiceURL:            cfg.DatasetServiceURL,
			PipelineServiceURL:           cfg.PipelineServiceURL,
			OntologyServiceURL:           cfg.OntologyServiceURL,
			IngestionReplicationGRPCURL:  cfg.IngestionReplicationGRPCURL,
			NetworkBoundaryServiceURL:    cfg.NetworkBoundaryServiceURL,
			SyncPollIntervalSecs:         cfg.SyncPollIntervalSecs,
			AllowPrivateNetworkEgress:    cfg.AllowPrivateNetworkEgress,
			AllowedEgressHosts:           cfg.AllowedEgressHosts,
			AgentStaleAfterSecs:          cfg.AgentStaleAfterSecs,
			CredentialEncryptionKey:      cfg.CredentialEncryptionKey,
			CredentialKey:                cfg.CredentialKey,
			SecretManagerURL:             cfg.SecretManagerURL,
			OutboxEnabled:                cfg.OutboxEnabled,
			AutoRegistrationIntervalSecs: cfg.AutoRegistrationIntervalSecs,
			VendedCredentialsTTLSeconds:  cfg.VendedCredentialsTTLSeconds,
		},
	}
	// The coordinator root is deliberately NOT the signal context: a signal
	// must not interrupt live executions while HTTP drain is still finishing.
	// The signal only closes ADMISSION (see runServiceLifecycle); the root is
	// cancelled exclusively inside Stop, after the gate is shut and the queue
	// drained.
	h.Coordinator = handlers.NewRunCoordinator(context.Background(), coordCfg, h)
	// P2.4 wiring: the same repo serves the grant store; the authorizer was
	// started above (its maintenance loop runs even with the scheduler off,
	// so armed grants expire observably); the runner ticks only when the
	// interval env explicitly enables it (default 0 = off).
	h.ScheduleGrants = store
	h.ScheduleAuthorizer = scheduleAuthz
	if cfg.SyncSchedulerIntervalSecs > 0 {
		interval := time.Duration(cfg.SyncSchedulerIntervalSecs) * time.Second
		runner := handlers.NewScheduleRunner(handlers.ScheduleRunnerConfig{Interval: interval}, store, h.Coordinator, scheduleAuthz, log)
		h.ScheduleRunner = runner
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			if err := runner.RunLoop(bgCtx); err != nil && !errors.Is(err, context.Canceled) {
				log.Warn("sync scheduler exited", slog.String("error", err.Error()))
			}
		}()
		log.Info("sync scheduler enabled", slog.Duration("interval", interval))
	} else {
		log.Info("sync scheduler loop disabled (OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS unset/0); due slots stay unclaimed and no ticks are written until the loop is enabled")
	}
	// P2.6 automatic recovery (default OFF): a disabled boot starts NO loop
	// and performs ZERO candidate scans; a persisted grant without a live
	// process-local arm is always skipped. Adoption is only ever through
	// ClaimAutomaticRecovery (full P24 grant re-adjudication in the claim
	// transaction); manual fresh-auth recovery is unaffected.
	if cfg.SyncAutoRecoveryEnabled {
		autoRunner := handlers.NewAutoRecoveryRunner(h, h.Coordinator, scheduleAuthz, handlers.AutoRecoveryConfig{})
		autoRunner.Start(bgCtx)
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			<-bgCtx.Done()
			autoRunner.Stop()
		}()
		log.Info("automatic recovery enabled (OPENFOUNDRY_SYNC_AUTO_RECOVERY_ENABLED=true); loop is bounded, ticket-capped and adopts only via ClaimAutomaticRecovery")
	} else {
		log.Info("automatic recovery disabled (default); only manual fresh-auth recovery is active")
	}
	log.Info("sync run coordinator started",
		slog.Int("capacity", coordCfg.QueueDepth),
		slog.Int("workers", coordCfg.Workers),
		slog.Duration("auth_ttl", coordCfg.AuthMemoryTTL),
		slog.Duration("task_deadline", coordCfg.TaskDeadline))

	metrics := observability.NewMetrics()
	server.Probes = append(server.Probes, probes.Postgres("primary", pool))
	srv := server.New(cfg, jwt, h, metrics, pool.Ping)

	return runServiceLifecycle(sigCtx,
		func(ctx context.Context) error { return server.Run(ctx, srv, log) },
		h.Coordinator.CloseAdmission,
		cancelBG,
		func() error {
			// P2.4: the authorizer stops FIRST — close the arm/activate gate,
			// clear every credential, join the bounded maintenance — then the
			// full coordinator stop (queue drain, root cancel, bounded
			// joins). BOTH stops are always attempted; a failure of either is
			// aggregated, never short-circuits the other, and runServiceLifecycle
			// still runs the background join (an unjoined state preserves the
			// pool rather than closing it under live writers).
			return joinStops(scheduleAuthz.Stop, h.Coordinator.Stop)()
		},
		func() bool {
			done := make(chan struct{})
			go func() {
				bgWG.Wait()
				close(done)
			}()
			select {
			case <-done:
				return true
			case <-time.After(10 * time.Second):
				log.Warn("background schedulers did not join within budget; pool close proceeds without them")
				return false
			}
		},
		pool.Close,
	)
}

func int64OrDefault(v *int64, def int) int {
	if v == nil {
		return def
	}
	return int(*v)
}

// joinStops composes shutdown stops so EVERY stop is attempted even when an
// earlier one fails; the individual errors are aggregated with errors.Join
// (nil contributors are dropped automatically). The background join and pool
// preservation decisions in runServiceLifecycle are unchanged: any non-nil
// result keeps the pool open under potentially live writers.
func joinStops(stops ...func() error) func() error {
	return func() error {
		errs := make([]error, 0, len(stops))
		for _, stop := range stops {
			if stop == nil {
				continue
			}
			errs = append(errs, stop())
		}
		return errors.Join(errs...)
	}
}

func secondsOrDefault(v *int64, def time.Duration) time.Duration {
	if v == nil {
		return def
	}
	return time.Duration(*v) * time.Second
}

// runServiceLifecycle is the REAL shutdown control flow (covered by
// TestLifecycle* in main_test.go):
//
//   - the moment the signal lands, ADMISSION closes FIRST (no new durable
//     sync work is accepted) and only then is the dedicated serve/drain
//     context cancelled — the two steps are sequenced, not racing on the
//     same Done channel — while live executions keep running so the HTTP
//     graceful drain can finish them;
//   - after serve returns — graceful drain, Shutdown timeout, or any listen
//     error — the background roots are cancelled EXPLICITLY (a listen error
//     never delivers a signal), the coordinator's full Stop runs (queue
//     drain with answers to every synchronously waiting handler, root
//     cancel, bounded joins) and the schedulers are joined (bounded);
//   - the pool closes ONLY when everything joined. An incomplete Stop means
//     live workers may still be writing: the pool is deliberately left to
//     process exit and the error says exactly what stayed unconfirmed —
//     never a fake graceful exit.
//
// The deferred guard also covers early serve errors: Stop still runs before
// any pool close.
func runServiceLifecycle(
	sigCtx context.Context,
	serve func(context.Context) error,
	closeAdmission func(),
	cancelBackground func(),
	stop func() error,
	joinBackground func() bool,
	poolClose func(),
) error {
	poolClosed := false
	defer func() {
		if !poolClosed {
			poolClose()
		}
	}()
	// Signal sequence: close admission to completion, THEN start the HTTP
	// drain (its own context — never the signal context, so the two Done
	// channels cannot race).
	drainCtx, cancelDrain := context.WithCancel(context.Background())
	defer cancelDrain()
	go func() {
		<-sigCtx.Done()
		closeAdmission()
		cancelDrain()
	}()
	serveErr := serve(drainCtx)
	// Serve is gone for ANY reason (drain finished, Shutdown timeout, listen
	// error): stop the background roots explicitly — no signal is guaranteed
	// to have arrived — then run the coordinator's full stop and join.
	cancelBackground()
	stopErr := stop()
	joined := joinBackground()
	if stopErr != nil {
		poolClosed = true // deliberately NOT closing the pool under live workers
		return fmt.Errorf("coordinator stop incomplete; runs stay honestly unconfirmed and the pool is left to process exit: %w", stopErr)
	}
	if !joined {
		poolClosed = true // schedulers may still touch the pool
		return errors.New("background schedulers did not join; the pool is left to process exit")
	}
	poolClose()
	poolClosed = true
	if serveErr != nil && !errors.Is(serveErr, context.Canceled) {
		return fmt.Errorf("server exited with error: %w", serveErr)
	}
	return nil
}
