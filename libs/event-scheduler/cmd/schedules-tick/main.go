// `schedules-tick` is a single-shot CLI that runs one tick and exits.
//
// Intended deployment is a Kubernetes CronJob running every minute.
//
// Dispatch modes (SCHEDULES_DISPATCH_MODE):
//
//   - unset or "legacy_direct" — legacy direct publish: one
//     [eventscheduler.Scheduler.Tick] that publishes payload_template
//     straight to Kafka. Requires DATABASE_URL and
//     KAFKA_BOOTSTRAP_SERVERS (+ optional KAFKA_SASL_*; without
//     KAFKA_SASL_PASSWORD we fall back to a PLAINTEXT dev principal).
//   - "pipeline_outbox_v1" — outbox dispatch (T8 decision §1
//     transaction ①): one [eventscheduler.OutboxScheduler.TickOutbox]
//     that fires scheduling-mode definitions through the transactional
//     outbox; Kafka is reached via the CDC relay, not directly.
//     Requires DATABASE_URL and SCHEDULES_REGISTRY_URL (the Apicurio
//     v2 registry the trigger/outcome schemas are registered in) —
//     missing or unknown configuration fails closed with exit 1.
//     Optional: SCHEDULES_REGISTRY_GROUP (default "default").
//
// Any other value fails closed. The outbox mode is opt-in and defaults
// OFF: with SCHEDULES_DISPATCH_MODE unset this binary behaves exactly
// like the pre-T8 direct-mode tick.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	databus "github.com/openfoundry/openfoundry-go/libs/event-bus-data"
	es "github.com/openfoundry/openfoundry-go/libs/event-scheduler"
)

const serviceName = "schedules-tick"

const (
	dispatchModeLegacy = "legacy_direct"
	dispatchModeOutbox = "pipeline_outbox_v1"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(); err != nil {
		slog.Error("schedules-tick failed", "err", err.Error())
		os.Exit(1)
	}
}

func run() error {
	// Optional -dispatch-mode flag overrides the env; keep the flag
	// surface minimal (everything else stays env-driven like the rest
	// of the platform).
	mode := os.Getenv("SCHEDULES_DISPATCH_MODE")
	for _, arg := range os.Args[1:] {
		const flag = "-dispatch-mode="
		if strings.HasPrefix(arg, flag) {
			mode = strings.TrimPrefix(arg, flag)
		}
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL must be set (Postgres connection URL)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pgCfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	pgCfg.MaxConns = 4
	pgCfg.MaxConnLifetime = 30 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, pgCfg)
	if err != nil {
		return fmt.Errorf("connect to Postgres: %w", err)
	}
	defer pool.Close()

	now := time.Now().UTC()

	switch mode {
	case "", dispatchModeLegacy:
		return runLegacyDirect(ctx, pool, now)
	case dispatchModeOutbox:
		return runOutbox(ctx, pool, now)
	default:
		return fmt.Errorf("SCHEDULES_DISPATCH_MODE %q is not a valid dispatch mode (want %q or %q)",
			mode, dispatchModeLegacy, dispatchModeOutbox)
	}
}

// runLegacyDirect is the pre-T8 behavior, unchanged: claim due
// direct-mode rows and publish payload_template to Kafka.
func runLegacyDirect(ctx context.Context, pool *pgxpool.Pool, now time.Time) error {
	bootstrap := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if bootstrap == "" {
		return fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS must be set (comma-separated brokers)")
	}

	publisher, err := databus.NewKafkaPublisher(databus.NewConfig(splitCSV(bootstrap), buildPrincipal()))
	if err != nil {
		return fmt.Errorf("build Kafka publisher: %w", err)
	}
	defer func() { _ = publisher.Close() }()

	scheduler := es.NewScheduler(pool, publisher)
	fired, tickErr := scheduler.Tick(ctx, now)
	if tickErr != nil {
		return fmt.Errorf("scheduler tick failed: %w", tickErr)
	}

	slog.Info("schedules-tick completed", "mode", dispatchModeLegacy, "fired", fired, "now", now.Format(time.RFC3339))
	fmt.Println(fired)
	return nil
}

// runOutbox fires scheduling-mode definitions through the transactional
// outbox. Fail-closed on any missing configuration: a half-configured
// tick must never run (T8 decision §4 "默认全关" posture).
func runOutbox(ctx context.Context, pool *pgxpool.Pool, now time.Time) error {
	registryURL := os.Getenv("SCHEDULES_REGISTRY_URL")
	if registryURL == "" {
		return fmt.Errorf("SCHEDULES_REGISTRY_URL must be set for dispatch mode %s (Apicurio v2 base URL, e.g. http://apicurio:8080)", dispatchModeOutbox)
	}
	group := os.Getenv("SCHEDULES_REGISTRY_GROUP")

	verifier := es.NewApicurioRegistry(registryURL, nil)
	scheduler := es.NewOutboxScheduler(pool, verifier, group)
	// Optional fail-closed CDC gate: refuse to fire until the Debezium
	// slot exists and is actively streaming (outbox events are
	// WAL-only; firing before the connector runs loses them forever).
	if slot := os.Getenv("SCHEDULES_CDC_SLOT"); slot != "" {
		scheduler.WithCDCSlot(slot)
	}

	result, tickErr := scheduler.TickOutbox(ctx, now)
	if tickErr != nil {
		return fmt.Errorf("outbox tick failed: %w", tickErr)
	}

	slog.Info("schedules-tick completed",
		"mode", dispatchModeOutbox,
		"fired", result.Fired,
		"quarantined", result.Quarantined,
		"now", now.Format(time.RFC3339))
	for _, id := range result.FiredEventIDs {
		fmt.Printf("fired %s\n", id)
	}
	for _, id := range result.QuarantinedIDs {
		fmt.Printf("quarantined %s\n", id)
	}
	return nil
}

// splitCSV parses a comma-separated list, trimming whitespace and
// dropping empty entries. Mirrors the helper used by other Go services
// in this repo.
func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// buildPrincipal mirrors the env contract used by other Go services in
// this repo. With no KAFKA_SASL_PASSWORD we fall back to a PLAINTEXT
// dev principal so testcontainer / docker-compose runs work without
// extra env knobs.
func buildPrincipal() databus.ServicePrincipal {
	password := os.Getenv("KAFKA_SASL_PASSWORD")
	username := defaultStr(os.Getenv("KAFKA_SASL_USERNAME"),
		defaultStr(os.Getenv("KAFKA_CLIENT_ID"), serviceName))
	if password == "" {
		return databus.InsecureDev(username)
	}
	mechanism := defaultStr(os.Getenv("KAFKA_SASL_MECHANISM"), databus.SASLMechanismScramSHA512)
	protocol := defaultStr(os.Getenv("KAFKA_SECURITY_PROTOCOL"), databus.SecurityProtocolSASLSSL)
	return databus.ServicePrincipal{
		Service:          username,
		Password:         password,
		Mechanism:        mechanism,
		SecurityProtocol: protocol,
	}
}

func defaultStr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
