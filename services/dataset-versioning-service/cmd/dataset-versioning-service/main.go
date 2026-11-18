// Command dataset-versioning-service hosts the foundation slice of
// the Rust crate `dataset-versioning-service` (25k LOC; this slice
// covers datasets, versions, branches, and the Files API backing-fs vertical).
//
// Follow-up slices: dataset_quality, lint, views, health, retention_worker,
// foundry-model surface, and Iceberg writer integration.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/libs/capabilities/probes"
	"github.com/openfoundry/openfoundry-go/libs/observability"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/config"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
	retentionworker "github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/runtime/retention"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/server"
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

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("pgx pool failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer pool.Close()
	if err := repo.Migrate(ctx, pool); err != nil {
		log.Error("migrations failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	jwt := authmw.NewJWTConfig(cfg.JWTSecret)
	baseURL := os.Getenv("DATASET_FILES_BASE_URL")
	if baseURL == "" {
		baseURL = "http://" + srvAddr(cfg.Server.Host, cfg.Server.Port)
	}
	// DATASET_FILES_ROOT_DIR is an opt-in absolute local root for the
	// backing filesystem (empty keeps the historical cwd default). The
	// WP3.3 staged-output feature requires a private staging directory on
	// the same filesystem but outside this root; deployments that enable
	// DATASET_STAGED_OUTPUTS_DIR should pin both to sibling directories.
	backingFS := storageabstraction.NewLocalBackingFS(baseURL, os.Getenv("DATASET_FILES_BASE_DIR"), []byte(cfg.JWTSecret))
	backingFS.RootDir = os.Getenv("DATASET_FILES_ROOT_DIR")
	store := &repo.Repo{Pool: pool}
	h := &handlers.Handlers{
		Repo:             store,
		BackingFS:        backingFS,
		PresignTTL:       15 * time.Minute,
		StagedOutputRoot: os.Getenv("DATASET_STAGED_OUTPUTS_DIR"),
	}
	if raw := os.Getenv("DATASET_STAGED_OUTPUTS_MAX_BYTES"); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > 0 {
			h.StagedOutputMaxBytes = v
		} else {
			log.Error("invalid DATASET_STAGED_OUTPUTS_MAX_BYTES", slog.String("value", raw))
			os.Exit(1)
		}
	}
	if raw := os.Getenv("DATASET_STAGED_OUTPUTS_MAX_RECORDS"); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > 0 {
			h.StagedOutputMaxRecords = v
		} else {
			log.Error("invalid DATASET_STAGED_OUTPUTS_MAX_RECORDS", slog.String("value", raw))
			os.Exit(1)
		}
	}
	if raw := os.Getenv("DATASET_STAGED_OUTPUTS_LEASE_TTL"); raw != "" {
		if v, err := time.ParseDuration(raw); err == nil && v > 0 {
			h.StagedOutputLeaseTTL = v
		} else {
			log.Error("invalid DATASET_STAGED_OUTPUTS_LEASE_TTL", slog.String("value", raw))
			os.Exit(1)
		}
	}
	if raw := os.Getenv("DATASET_STAGED_OUTPUTS_TTL"); raw != "" {
		if v, err := time.ParseDuration(raw); err == nil && v > 0 {
			h.StagedOutputTTL = v
		} else {
			log.Error("invalid DATASET_STAGED_OUTPUTS_TTL", slog.String("value", raw))
			os.Exit(1)
		}
	}
	metrics := observability.NewMetrics()

	if cfg.RetentionWorkerEnabled {
		w := retentionworker.New(retentionworker.NewRepoStore(store))
		w.TickInterval = cfg.RetentionWorkerInterval
		w.Logger = log
		go w.RunLoop(ctx)
		log.Info("branch retention worker started", slog.Duration("interval", cfg.RetentionWorkerInterval))
	}

	srv := server.New(cfg, jwt, h, metrics, probes.Postgres("datasets", pool))
	if err := server.Run(ctx, srv, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("server exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func srvAddr(host string, port uint16) string {
	if host == "" || host == "0.0.0.0" {
		host = "localhost"
	}
	return host + ":" + strconv.FormatUint(uint64(port), 10)
}
