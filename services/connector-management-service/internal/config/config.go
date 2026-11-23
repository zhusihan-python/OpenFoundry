// Package config resolves connector-management-service env config.
package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/handlers"
	cmruntime "github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/runtime"
)

const (
	DefaultHost                        = "0.0.0.0"
	DefaultPort                 uint16 = 50088
	DefaultDatasetServiceURL           = "http://localhost:50079"
	DefaultPipelineServiceURL          = "http://localhost:50080"
	DefaultOntologyServiceURL          = "http://localhost:50103"
	DefaultNetworkBoundaryURL          = "http://localhost:50119"
	DefaultSyncPollIntervalSecs        = 2
	DefaultAllowPrivateEgress          = true
	DefaultAgentStaleAfterSecs         = 120
	DefaultMediaSetsServiceURL         = "http://localhost:50156"
	DefaultVendedCredentialsTTL        = 900
)

type Config struct {
	Service struct{ Name, Version string }
	Server  struct {
		Host string
		Port uint16
	}
	DatabaseURL string
	JWTSecret   string
	MetricsAddr string

	DatasetServiceURL           string
	PipelineServiceURL          string
	OntologyServiceURL          string
	IngestionReplicationGRPCURL string
	NetworkBoundaryServiceURL   string
	SyncPollIntervalSecs        uint64
	AllowPrivateNetworkEgress   bool
	MySQLSyncEndpoint           string
	PostgresSyncEndpoint        string
	S3SyncEndpoint              string
	S3SyncRegion                string
	AllowedEgressHosts          []string
	AgentStaleAfterSecs         uint64
	MediaSetsServiceURL         string

	CredentialEncryptionKey      string
	CredentialKey                [32]byte
	SecretManagerURL             string
	OutboxEnabled                bool
	AutoRegistrationIntervalSecs uint64
	SyncSchedulerIntervalSecs    uint64
	OpenFoundryDevAuth           bool
	// P2.6 automatic recovery (default OFF): when false the service performs
	// ZERO candidate scans and no auto recovery loop exists at all; manual
	// fresh-auth recovery is always available.
	SyncAutoRecoveryEnabled     bool
	VendedCredentialsTTLSeconds int64

	// P2.2 interruptible-run coordinator overrides. nil = not configured
	// (main substitutes the handlers defaults); a PRESENT value has already
	// been range-validated here — explicit 0/negative/over-max is a startup
	// error, never a silent default.
	SyncAsyncQueueDepth  *int64
	SyncAsyncWorkers     *int64
	SyncAsyncAuthTTLSecs *int64
	SyncAsyncTaskTTLSecs *int64

	// P2.4 schedule grant memory TTL (nil = default 300s; a present value is
	// range-validated here — explicit out-of-range is a startup error).
	ScheduleAuthTTLSecs *int64
}

func FromEnv() (*Config, error) {
	cfg := &Config{}
	cfg.Service.Name = "connector-management-service"
	cfg.Service.Version = defaultStr(os.Getenv("SERVICE_VERSION"), "dev")
	cfg.Server.Host = defaultStr(os.Getenv("HOST"), DefaultHost)
	port, err := parseUint16("PORT", os.Getenv("PORT"), DefaultPort)
	if err != nil {
		return nil, err
	}
	cfg.Server.Port = port
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	if cfg.DatabaseURL == "" {
		return nil, &MissingEnvError{Key: "DATABASE_URL"}
	}
	cfg.JWTSecret = defaultStr(os.Getenv("OPENFOUNDRY_JWT_SECRET"), os.Getenv("JWT_SECRET"))
	if cfg.JWTSecret == "" {
		return nil, &MissingEnvError{Key: "JWT_SECRET"}
	}
	cfg.MetricsAddr = defaultStr(os.Getenv("METRICS_ADDR"), "0.0.0.0:9090")

	cfg.DatasetServiceURL = defaultStr(os.Getenv("DATASET_SERVICE_URL"), DefaultDatasetServiceURL)
	cfg.PipelineServiceURL = defaultStr(os.Getenv("PIPELINE_SERVICE_URL"), DefaultPipelineServiceURL)
	cfg.OntologyServiceURL = defaultStr(os.Getenv("ONTOLOGY_SERVICE_URL"), DefaultOntologyServiceURL)
	cfg.IngestionReplicationGRPCURL = os.Getenv("INGESTION_REPLICATION_GRPC_URL")
	cfg.NetworkBoundaryServiceURL = defaultStr(os.Getenv("NETWORK_BOUNDARY_SERVICE_URL"), DefaultNetworkBoundaryURL)
	cfg.SyncPollIntervalSecs, err = parseUint64("SYNC_POLL_INTERVAL_SECS", os.Getenv("SYNC_POLL_INTERVAL_SECS"), DefaultSyncPollIntervalSecs)
	if err != nil {
		return nil, err
	}
	cfg.AllowPrivateNetworkEgress, err = parseBool("ALLOW_PRIVATE_NETWORK_EGRESS", os.Getenv("ALLOW_PRIVATE_NETWORK_EGRESS"), DefaultAllowPrivateEgress)
	if err != nil {
		return nil, err
	}
	cfg.S3SyncEndpoint = strings.TrimSpace(os.Getenv("S3_SYNC_ENDPOINT"))
	cfg.S3SyncRegion = defaultStr(os.Getenv("S3_SYNC_REGION"), "us-east-1")
	cfg.AllowedEgressHosts = parseList(os.Getenv("ALLOWED_EGRESS_HOSTS"))
	cfg.MySQLSyncEndpoint = strings.TrimSpace(os.Getenv("MYSQL_SYNC_ENDPOINT"))
	if cfg.MySQLSyncEndpoint != "" {
		if _, err := cmruntime.MySQLEndpoint(cfg.MySQLSyncEndpoint, cfg.AllowedEgressHosts, cfg.AllowPrivateNetworkEgress); err != nil {
			return nil, fmt.Errorf("MYSQL_SYNC_ENDPOINT: %w", err)
		}
	}
	cfg.PostgresSyncEndpoint = strings.TrimSpace(os.Getenv("POSTGRES_SYNC_ENDPOINT"))
	if cfg.PostgresSyncEndpoint != "" {
		if _, err := cmruntime.PostgresEndpoint(cfg.PostgresSyncEndpoint, cfg.AllowedEgressHosts, cfg.AllowPrivateNetworkEgress); err != nil {
			return nil, fmt.Errorf("POSTGRES_SYNC_ENDPOINT: %w", err)
		}
	}
	if cfg.S3SyncEndpoint != "" {
		if _, err := cmruntime.S3Endpoint(cfg.S3SyncEndpoint, cfg.AllowedEgressHosts, cfg.AllowPrivateNetworkEgress); err != nil {
			return nil, fmt.Errorf("S3_SYNC_ENDPOINT: %w", err)
		}
	}

	cfg.AgentStaleAfterSecs, err = parseUint64("AGENT_STALE_AFTER_SECS", os.Getenv("AGENT_STALE_AFTER_SECS"), DefaultAgentStaleAfterSecs)
	if err != nil {
		return nil, err
	}
	cfg.MediaSetsServiceURL = defaultStr(os.Getenv("MEDIA_SETS_SERVICE_URL"), DefaultMediaSetsServiceURL)

	cfg.CredentialEncryptionKey = os.Getenv("CREDENTIAL_ENCRYPTION_KEY")
	cfg.CredentialKey, err = deriveCredentialKey(cfg.CredentialEncryptionKey, cfg.JWTSecret, os.Getenv(DeploymentEnvironmentEnv))
	if err != nil {
		return nil, err
	}
	cfg.SecretManagerURL = os.Getenv("SECRET_MANAGER_URL")
	cfg.OutboxEnabled, err = parseBool("OUTBOX_ENABLED", os.Getenv("OUTBOX_ENABLED"), true)
	if err != nil {
		return nil, err
	}
	cfg.AutoRegistrationIntervalSecs, err = parseUint64("OPENFOUNDRY_AUTO_REGISTRATION_INTERVAL_SECS", os.Getenv("OPENFOUNDRY_AUTO_REGISTRATION_INTERVAL_SECS"), 0)
	if err != nil {
		return nil, err
	}
	cfg.SyncSchedulerIntervalSecs, err = parseUint64("OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS", os.Getenv("OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS"), 0)
	if err != nil {
		return nil, err
	}
	cfg.SyncAutoRecoveryEnabled, err = parseBool("OPENFOUNDRY_SYNC_AUTO_RECOVERY_ENABLED", os.Getenv("OPENFOUNDRY_SYNC_AUTO_RECOVERY_ENABLED"), false)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	// P2.4: the scheduler stays off by default (0); an explicitly enabled
	// interval is bounded to 1..30 seconds so the tick loop can never park on
	// sub-minute cron granularity.
	if cfg.SyncSchedulerIntervalSecs > 30 {
		return nil, fmt.Errorf("OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS %d out of bounds [0,30]", cfg.SyncSchedulerIntervalSecs)
	}
	// P2.2 coordinator overrides: nil when unset; a present value is parsed
	// AND range-validated right here against the handlers bounds (explicit 0,
	// negatives and over-max values are startup errors — an unset variable
	// keeps the defaults, an explicit one may not lie outside the bounds).
	optionalInt64 := func(key string) (*int64, error) {
		raw := strings.TrimSpace(os.Getenv(key))
		if raw == "" {
			return nil, nil
		}
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		return &v, nil
	}
	if cfg.SyncAsyncQueueDepth, err = optionalInt64("CONNECTOR_SYNC_ASYNC_QUEUE"); err != nil {
		return nil, err
	}
	if cfg.SyncAsyncQueueDepth != nil {
		if *cfg.SyncAsyncQueueDepth < int64(handlers.MinCoordinatorQueueDepth) || *cfg.SyncAsyncQueueDepth > int64(handlers.MaxCoordinatorQueueDepth) {
			return nil, fmt.Errorf("CONNECTOR_SYNC_ASYNC_QUEUE %d out of bounds [%d,%d]", *cfg.SyncAsyncQueueDepth, handlers.MinCoordinatorQueueDepth, handlers.MaxCoordinatorQueueDepth)
		}
	}
	if cfg.SyncAsyncWorkers, err = optionalInt64("CONNECTOR_SYNC_ASYNC_WORKERS"); err != nil {
		return nil, err
	}
	if cfg.SyncAsyncWorkers != nil {
		if *cfg.SyncAsyncWorkers < int64(handlers.MinCoordinatorWorkers) || *cfg.SyncAsyncWorkers > int64(handlers.MaxCoordinatorWorkers) {
			return nil, fmt.Errorf("CONNECTOR_SYNC_ASYNC_WORKERS %d out of bounds [%d,%d]", *cfg.SyncAsyncWorkers, handlers.MinCoordinatorWorkers, handlers.MaxCoordinatorWorkers)
		}
	}
	if cfg.SyncAsyncAuthTTLSecs, err = optionalInt64("CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS"); err != nil {
		return nil, err
	}
	if cfg.SyncAsyncAuthTTLSecs != nil {
		if *cfg.SyncAsyncAuthTTLSecs < int64(handlers.MinCoordinatorAuthTTL/time.Second) || *cfg.SyncAsyncAuthTTLSecs > int64(handlers.MaxCoordinatorAuthTTL/time.Second) {
			return nil, fmt.Errorf("CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS %d out of bounds [%d,%d]", *cfg.SyncAsyncAuthTTLSecs, int64(handlers.MinCoordinatorAuthTTL/time.Second), int64(handlers.MaxCoordinatorAuthTTL/time.Second))
		}
	}
	if cfg.SyncAsyncTaskTTLSecs, err = optionalInt64("CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS"); err != nil {
		return nil, err
	}
	if cfg.SyncAsyncTaskTTLSecs != nil {
		if *cfg.SyncAsyncTaskTTLSecs < int64(handlers.MinCoordinatorTaskDeadline/time.Second) || *cfg.SyncAsyncTaskTTLSecs > int64(handlers.MaxCoordinatorTaskDeadline/time.Second) {
			return nil, fmt.Errorf("CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS %d out of bounds [%d,%d]", *cfg.SyncAsyncTaskTTLSecs, int64(handlers.MinCoordinatorTaskDeadline/time.Second), int64(handlers.MaxCoordinatorTaskDeadline/time.Second))
		}
	}
	// P2.4 schedule grant memory TTL: nil = default 300s; explicit values are
	// bounded to [1,86400]. The effective grant deadline is further clamped
	// by the verified caller JWT exp at arm time.
	if cfg.ScheduleAuthTTLSecs, err = optionalInt64("OPENFOUNDRY_SYNC_SCHEDULER_AUTH_TTL_SECS"); err != nil {
		return nil, err
	}
	if cfg.ScheduleAuthTTLSecs != nil {
		if *cfg.ScheduleAuthTTLSecs < 1 || *cfg.ScheduleAuthTTLSecs > 86400 {
			return nil, fmt.Errorf("OPENFOUNDRY_SYNC_SCHEDULER_AUTH_TTL_SECS %d out of bounds [1,86400]", *cfg.ScheduleAuthTTLSecs)
		}
	}
	cfg.OpenFoundryDevAuth, err = parseBool("OPENFOUNDRY_DEV_AUTH", os.Getenv("OPENFOUNDRY_DEV_AUTH"), false)
	if err != nil {
		return nil, err
	}
	cfg.VendedCredentialsTTLSeconds, err = parseInt64("OPENFOUNDRY_VENDED_CREDENTIALS_TTL_SECS", os.Getenv("OPENFOUNDRY_VENDED_CREDENTIALS_TTL_SECS"), DefaultVendedCredentialsTTL)
	if err != nil {
		return nil, err
	}
	if cfg.VendedCredentialsTTLSeconds <= 0 {
		cfg.VendedCredentialsTTLSeconds = DefaultVendedCredentialsTTL
	}
	return cfg, nil
}

type MissingEnvError struct{ Key string }

func (e *MissingEnvError) Error() string {
	return fmt.Sprintf("required environment variable %s is not set", e.Key)
}

func IsMissingEnv(err error) bool { var me *MissingEnvError; return errors.As(err, &me) }

type InvalidEnvError struct {
	Key   string
	Value string
	Err   error
}

func (e *InvalidEnvError) Error() string {
	return fmt.Sprintf("invalid value for %s=%q: %v", e.Key, e.Value, e.Err)
}

func (e *InvalidEnvError) Unwrap() error { return e.Err }

func defaultStr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func parseUint16(key, v string, fallback uint16) (uint16, error) {
	if strings.TrimSpace(v) == "" {
		return fallback, nil
	}
	n, err := strconv.ParseUint(v, 10, 16)
	if err != nil {
		return 0, &InvalidEnvError{Key: key, Value: v, Err: err}
	}
	return uint16(n), nil
}

func parseUint64(key, v string, fallback uint64) (uint64, error) {
	if strings.TrimSpace(v) == "" {
		return fallback, nil
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, &InvalidEnvError{Key: key, Value: v, Err: err}
	}
	return n, nil
}

func parseInt64(key, v string, fallback int64) (int64, error) {
	if strings.TrimSpace(v) == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, &InvalidEnvError{Key: key, Value: v, Err: err}
	}
	return n, nil
}

func parseBool(key, v string, fallback bool) (bool, error) {
	if strings.TrimSpace(v) == "" {
		return fallback, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, &InvalidEnvError{Key: key, Value: v, Err: err}
	}
	return b, nil
}

func parseList(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

// DeploymentEnvironmentEnv is the deployment environment signal injected into
// every workload by the shared Helm chart (_helpers.tpl: environment from
// global.deploymentFabric, values-prod.yaml sets "production",
// values-dev.yaml "development", Helm default "shared"). It is the ONLY
// environment signal consulted by the credential-key gate: SERVICE_VERSION
// (which defaults to "dev") and the OPENFOUNDRY_DEV_AUTH shim deliberately
// must never widen credential-key policy.
const DeploymentEnvironmentEnv = "OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT"

// legacyCredentialKeyDomain is the pre-gate compat derivation prefix that
// bootstrapped the credential key from the JWT secret. It survives only for
// explicit local dev/test environments (see credentialKeyDevFallbackAllowed).
const legacyCredentialKeyDomain = "openfoundry/credential-encryption/v1\x00"

// credentialKeyDevFallbackAllowed reports whether the deployment environment
// is an EXPLICIT local development/test environment — the only place the
// legacy JWT-derived credential-key fallback still applies. Production
// (production/prod) and every other or unset value — including the Helm
// default "shared" and the staging/sovereign profiles — require an
// independent CREDENTIAL_ENCRYPTION_KEY at startup (fail closed).
func credentialKeyDevFallbackAllowed(environment string) bool {
	switch strings.ToLower(strings.TrimSpace(environment)) {
	case "development", "dev", "test", "testing", "local":
		return true
	}
	return false
}

// CredentialKeyError reports a CREDENTIAL_ENCRYPTION_KEY startup-gate
// failure. It carries only the variable name, the policy reason and
// non-secret context — never the key material or the JWT secret — so the
// fatal log in main cannot echo them.
type CredentialKeyError struct {
	Reason string
}

func (e *CredentialKeyError) Error() string {
	return fmt.Sprintf("CREDENTIAL_ENCRYPTION_KEY: %s", e.Reason)
}

// deriveCredentialKey resolves the domain credential-encryption key. An
// independent base64 32-byte CREDENTIAL_ENCRYPTION_KEY is required everywhere
// except explicit local dev/test environments, where a missing/blank value
// still falls back to the legacy JWT-derived key for compatibility. A
// configured key must be valid AND independent of JWT_SECRET: values equal
// to the raw JWT secret or to the legacy JWT-derived digest are rejected.
func deriveCredentialKey(envKeyB64, jwtSecret, environment string) ([32]byte, error) {
	var out [32]byte
	trimmed := strings.TrimSpace(envKeyB64)
	if trimmed == "" {
		if !credentialKeyDevFallbackAllowed(environment) {
			return out, &CredentialKeyError{Reason: fmt.Sprintf(
				"independent credential key is required (OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT=%q or unset); set it to the base64 of 32 random bytes, e.g. `openssl rand -base64 32`; the JWT-derived fallback only applies when OPENFOUNDRY_DEPLOYMENT_ENVIRONMENT is one of development|dev|test|testing|local",
				strings.TrimSpace(environment),
			)}
		}
		digest := sha256.Sum256([]byte(legacyCredentialKeyDomain + jwtSecret))
		copy(out[:], digest[:])
		return out, nil
	}
	raw, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		return out, &CredentialKeyError{Reason: "invalid base64 value"}
	}
	if len(raw) != 32 {
		return out, &CredentialKeyError{Reason: fmt.Sprintf("must decode to 32 bytes (got %d)", len(raw))}
	}
	if bytes.Equal(raw, []byte(jwtSecret)) {
		return out, &CredentialKeyError{Reason: "must be independent of JWT_SECRET (value equals the raw JWT secret)"}
	}
	legacy := sha256.Sum256([]byte(legacyCredentialKeyDomain + jwtSecret))
	if bytes.Equal(raw, legacy[:]) {
		return out, &CredentialKeyError{Reason: "must be independent of JWT_SECRET (value equals the legacy JWT-derived key)"}
	}
	copy(out[:], raw)
	return out, nil
}
