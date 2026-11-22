// Command pipeline-build-service serves the build / execution side of
// Pipeline Builder. The Go HTTP surface is intentionally audited
// route-by-route rather than described as 1:1; see
// docs/migration/route-parity-audit.md for generated route state and
// remaining config-gated production adapters.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/libs/capabilities/probes"
	"github.com/openfoundry/openfoundry-go/libs/observability"
	pythonsidecar "github.com/openfoundry/openfoundry-go/libs/python-sidecar"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/config"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/resolver"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
	livellogs "github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/logs"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/postgres"
	runtimepkg "github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/runtime"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/scheduling"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/server"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/spark"
)

var version = "dev"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.FromEnv()
	if err != nil {
		slog.Error("config load failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	if cfg.Service.Version == "dev" {
		cfg.Service.Version = version
	}

	log := observability.InitLogging(cfg.Service.Name, cfg.Service.Version)
	shutdownTracing, err := observability.InitTracing(ctx, cfg.Service.Name, cfg.Service.Version)
	if err != nil {
		log.Error("tracing init failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	if cfg.FoundryIcebergCatalogURL == "" {
		log.Warn("FoundryIcebergTxn disabled, multi-table atomicity not enforced " +
			"(set FOUNDRY_ICEBERG_CATALOG_URL to enable; ADR-0041)")
	}

	var pythonRuntime runtimepkg.TransformExecutor
	var pyMgr *pythonsidecar.Manager
	if cfg.PythonSidecarBinary != "" {
		pyMgr, err = pythonsidecar.New(pythonsidecar.Config{
			BinaryPath:      cfg.PythonSidecarBinary,
			HardCallTimeout: time.Duration(cfg.PythonSidecarTimeoutSeconds+5) * time.Second,
		}, log)
		if err != nil {
			log.Error("python sidecar config failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
		startCtx, cancelStart := context.WithTimeout(ctx, 15*time.Second)
		if err := pyMgr.Start(startCtx); err != nil {
			cancelStart()
			log.Error("python sidecar start failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
		cancelStart()
		defer func() { _ = pyMgr.Stop(context.Background()) }()
		pythonRuntime = runtimepkg.NewSidecarTransformExecutor(runtimepkg.SidecarTransform{Mgr: pyMgr})
		log.Info("python sidecar runtime wired")
	} else {
		log.Warn("PYTHON_SIDECAR_BINARY unset; Python pipeline transforms require an injected test fake or will fail explicitly")
	}

	var pool *pgxpool.Pool
	var repo *postgres.Repository
	var versioningPort resolver.DatasetVersioningRepository
	var execPorts handler.ExecutionPorts
	if cfg.DatabaseURL != "" {
		pool, err = pgxpool.New(ctx, cfg.DatabaseURL)
		if err != nil {
			log.Error("postgres pool init failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
		defer pool.Close()
		if err := postgres.RunMigrations(ctx, pool); err != nil {
			log.Error("postgres migrations failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
		repo = postgres.NewRepositoryFromPool(pool)
		metadataCommitter := handler.DatasetServiceOutputCommitter{
			BaseURL:     cfg.DatasetServiceURL,
			BearerToken: cfg.DatasetServiceBearer,
			Client:      &http.Client{Timeout: 30 * time.Second},
		}
		outputCommitter := handler.OntologyObjectOutputCommitter{
			Dataset:                  metadataCommitter,
			Metadata:                 repo,
			OntologyDefinitionURL:    cfg.OntologyDefinitionServiceURL,
			OntologyDefinitionBearer: cfg.OntologyDefinitionServiceBearer,
			ObjectDatabaseURL:        cfg.ObjectDatabaseServiceURL,
			ObjectDatabaseBearer:     cfg.ObjectDatabaseServiceBearer,
			Client:                   &http.Client{Timeout: 30 * time.Second},
		}
		// Dataset versioning access strategy:
		//   - DATASET_SERVICE_URL set → HTTP adapter: every dataset
		//     request carries the CALLER's JWT (the dataset service
		//     enforces tenants); works across split databases.
		//   - unset → co-located SQL repository; scoped callers are
		//     gated on the dataset org columns and fail closed with a
		//     stable 503 otherwise (no silent unscoped fallback).
		versioningPort = repo
		if cfg.DatasetServiceURL != "" {
			versioningPort = handler.DatasetHTTPVersioning{BaseURL: cfg.DatasetServiceURL, Client: &http.Client{Timeout: 30 * time.Second}}
			log.Info("dataset HTTP versioning adapter wired", slog.String("base_url", cfg.DatasetServiceURL))
		}
		handler.SetBuildLifecyclePorts(handler.BuildLifecyclePorts{JobSpecs: repo, Versioning: versioningPort, Locks: repo, Builds: repo})
		// WP3.3 3B batched dataset client: direct dataset-versioning access,
		// every request carries the CALLER's bearer (no service token,
		// redirects refused); knobs default to the dataset service budgets.
		handler.SetBatchedDatasetClient(handler.BatchedClientConfig{
			BaseURL:              cfg.DatasetServiceURL,
			Client:               &http.Client{Timeout: 60 * time.Second},
			ScanPageRows:         cfg.BatchedScanPageRows,
			ScanMaxResponseBytes: cfg.BatchedScanMaxResponseBytes,
			ScanMaxPages:         cfg.BatchedScanMaxPages,
			StageTimeout:         time.Duration(cfg.BatchedStageTimeoutSecs) * time.Second,
			LeaseTTL:             time.Duration(cfg.BatchedExecuteLeaseTTLSecs) * time.Second,
		})
		distributedRunner := handler.NewSparkFlinkDistributedRunner(handler.DistributedRuntimeConfig{
			Namespace:    cfg.SparkNamespace,
			RunnerImage:  cfg.PipelineRunnerImage,
			PollInterval: time.Duration(cfg.DistributedComputePollIntervalMS) * time.Millisecond,
			Timeout:      time.Duration(cfg.DistributedComputeTimeoutSecs) * time.Second,
		})
		aiClient := &http.Client{Timeout: 45 * time.Second}
		aiConfig := handler.AIServiceLLMConfig{AgentRuntime: cfg.PipelineLLMProtocol == "agent-runtime", BaseURL: cfg.AIServiceURL, BearerToken: cfg.AIServiceBearer, Client: aiClient}
		llmRunner := handler.NewAIServiceLLMRunner(aiConfig)
		aipGenerator := handler.NewAIServicePipelineAIPGenerator(aiConfig)
		// Uses its own redirect-refusing client: the rows endpoint must be
		// answered directly by dataset-versioning-service, never via a
		// presigned or foreign redirect.
		datasetRowReader := handler.DatasetServiceRowReader{BaseURL: cfg.DatasetServiceURL, BearerToken: cfg.DatasetServiceBearer}
		execPorts = handler.ExecutionPorts{Plans: repo, Runs: repo, Python: pythonRuntime, LLM: llmRunner, AIP: aipGenerator, Distributed: distributedRunner, Transactions: handler.ConfigGatedTransactionManager{Metadata: repo, CatalogConfigured: cfg.FoundryIcebergCatalogURL != ""}, Committer: handler.ConfigGatedOutputCommitter{Metadata: outputCommitter, CatalogConfigured: cfg.FoundryIcebergCatalogURL != ""}, Audit: repo, DatasetRows: datasetRowReader, Parallelism: cfg.DistributedPipelineWorkers}
		if cfg.DatasetServiceURL != "" {
			execPorts.ManualDataset = &handler.DatasetHTTPVersioning{BaseURL: metadataCommitter.BaseURL, Client: metadataCommitter.Client}
		}
		handler.SetExecutionPorts(execPorts)
		handler.SetBuildQueryRepository(repo)
		handler.SetPipelineAuthoringRepository(repo)
		handler.SetSparkSubmissionRepository(repo)
		handler.SetJobLogService(&livellogs.Service{Store: repo, Subscriber: livellogs.NewMemoryService()})
		log.Info("postgres repositories wired", slog.String("database_url", "set"))
	} else {
		log.Warn("DATABASE_URL unset — production repositories disabled; supported handlers return explicit 503 instead of fake success")
	}

	if kubeClient, err := spark.NewKubernetesClientFromEnv(); err == nil {
		handler.SetSparkClient(kubeClient)
		log.Info("kubernetes SparkApplication client wired")
	} else {
		log.Warn("kubernetes client unavailable — SparkApplication endpoints return explicit 503", slog.String("error", err.Error()))
	}

	// T8 scheduling consumer (default OFF). With SCHEDULER_CONSUMER_ENABLED
	// the whole chain is REQUIRED: canonical runner over the production
	// ports + Kafka source. Any init failure exits the process — a
	// "healthy" HTTP server with a dead scheduler is fail-open and not
	// acceptable (T8 decision §4/§6).
	var schedulerStop func()
	if cfg.Scheduler.Enabled {
		if pool == nil {
			log.Error("scheduler enabled but DATABASE_URL unset — cannot start scheduling consumer")
			os.Exit(1)
		}
		cipher, cerr := scheduling.NewTokenCipher(cfg.Scheduler.CredentialEncryptionKey)
		if cerr != nil {
			log.Error("scheduler credential cipher init failed", slog.String("error", cerr.Error()))
			os.Exit(1)
		}
		owner := scheduling.NewOwnerID()
		runner := scheduling.NewCanonicalRunner(scheduling.CanonicalDeps{
			Store:                repo,
			JobSpecs:             repo,
			Versioning:           versioningPort,
			Locks:                repo,
			Builds:               repo,
			Plans:                repo,
			NodeRunner:           handler.NewSchedulerNodeRunner(execPorts),
			MetadataCommitter:    execPorts.Committer,
			MetadataTransactions: execPorts.Transactions,
			DVSBaseURL:           cfg.DatasetServiceURL,
			HTTPClient:           &http.Client{Timeout: 60 * time.Second},
			JWT:                  authmw.NewJWTConfig(cfg.JWTSecret),
			Cipher:               cipher,
			Owner:                owner,
			Log:                  log,
		})
		runtime, rerr := scheduling.BuildRuntime(cfg.Scheduler, repo, scheduling.OutcomeEnqueueHook(cipher), runner, owner, log)
		if rerr != nil {
			log.Error("scheduler consumer init failed", slog.String("error", rerr.Error()))
			os.Exit(1)
		}
		runtime.Start(ctx)
		schedulerStop = runtime.Stop
		log.Info("scheduler consumer started", slog.String("owner", owner), slog.String("topic", "pipeline.builds.v1"))
	}

	metrics := observability.NewMetrics()
	srv := server.New(cfg, metrics, probes.Postgres("primary", pool))
	serveErr := run(ctx, srv, log)
	if schedulerStop != nil {
		schedulerStop()
	}
	if serveErr != nil && !errors.Is(serveErr, context.Canceled) {
		log.Error("server exited with error", slog.String("error", serveErr.Error()))
		os.Exit(1)
	}
}

func run(ctx context.Context, srv *http.Server, log *slog.Logger) error {
	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", slog.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		log.Info("shutting down")
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}
