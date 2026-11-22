// Package config mirrors `services/pipeline-build-service/src/config.rs`.
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
)

type Config struct {
	Service struct {
		Name    string
		Version string
	}

	Host string
	Port uint16

	DatabaseURL string
	JWTSecret   string
	DataDir     string

	DatasetServiceURL               string
	DatasetServiceBearer            string
	OntologyDefinitionServiceURL    string
	OntologyDefinitionServiceBearer string
	ObjectDatabaseServiceURL        string
	ObjectDatabaseServiceBearer     string
	WorkflowServiceURL              string
	AIServiceURL                    string
	AIServiceBearer                 string
	PipelineLLMProtocol             string

	StorageBackend   string
	StorageBucket    string
	S3Endpoint       string
	S3Region         string
	S3AccessKey      string
	S3SecretKey      string
	LocalStorageRoot string

	DistributedPipelineWorkers       int
	DistributedComputePollIntervalMS uint64
	DistributedComputeTimeoutSecs    uint64

	// FASE 3 / Tarea 3.4 — kube + Spark wiring.
	SparkNamespace      string
	PipelineRunnerImage string

	// ADR-0041 — Foundry Iceberg catalog client.
	FoundryIcebergCatalogURL    string
	FoundryIcebergCatalogBearer string

	// Python sidecar replaces the former Rust PyO3 embedded interpreter
	// for pipeline transform execution. Empty disables Python runtime
	// execution and leaves callers with an explicit runner_not_wired error.
	PythonSidecarBinary         string
	PythonSidecarTimeoutSeconds uint32
	// WP3.3 3B batched execution (scan paging, bounded decode, stage
	// attempt timeout, execute-lease fence).
	BatchedScanPageRows         int
	BatchedScanMaxResponseBytes int64
	BatchedScanMaxPages         int
	BatchedStageTimeoutSecs     uint32
	BatchedExecuteLeaseTTLSecs  uint32

	// T8 scheduling consumer (WP3.2). DEFAULT OFF: the consumer starts
	// only when SCHEDULER_CONSUMER_ENABLED=true, and then requires the
	// dedicated SCHEDULER_CREDENTIAL_ENCRYPTION_KEY — base64 that decodes
	// to exactly 32 bytes (same convention as the Connector service,
	// e.g. `openssl rand -base64 32`) — and Kafka bootstrap servers.
	// NO JWT-derived and NO dev fallback exists for this key.
	Scheduler SchedulerConfig
}

// SchedulerConfig carries the T8 scheduling consumer knobs (T8 decision
// §4: default-off, bounded grants, fenced execution). All durations are
// SECONDS.
type SchedulerConfig struct {
	Enabled        bool
	KafkaBootstrap string
	GroupID        string
	// CredentialEncryptionKeyB64 is the raw env value (never logged);
	// CredentialEncryptionKey is the decoded 32-byte key.
	CredentialEncryptionKeyB64 string
	CredentialEncryptionKey    []byte
	LeaseTTLSeconds            uint32
	HeartbeatSeconds           uint32
	MaxEventAttempts           uint32
	// Kafka client wiring. The PRODUCTION listener (9093) is mTLS
	// (C provisions a scoped KafkaUser): protocol SSL + the three TLS
	// file paths. SASL_SSL remains available for optional SASL
	// deployments. Unknown protocols NEVER downgrade to plaintext;
	// PLAINTEXT is an explicit dev-only choice.
	KafkaSecurityProto string // SSL | SASL_SSL | SASL_PLAINTEXT | PLAINTEXT
	KafkaSASLUsername  string
	KafkaSASLPassword  string
	KafkaSASLMechanism string // SCRAM-SHA-512 (default) | SCRAM-SHA-256 | PLAIN
	KafkaTLSCAFile     string // SCHEDULER_KAFKA_TLS_CA_FILE
	KafkaTLSCertFile   string // SCHEDULER_KAFKA_TLS_CERT_FILE (user.crt)
	KafkaTLSKeyFile    string // SCHEDULER_KAFKA_TLS_KEY_FILE (user.key)
}

// Validate enforces the enabled-path invariants. Error strings carry no
// secret material. Mirrors the Connector credential-key discipline
// (base64→32 bytes; reject the raw JWT secret and the legacy
// JWT-derived digest) WITHOUT its dev fallback: the scheduler has no
// JWT-derived key under any environment.
func (s *SchedulerConfig) Validate(jwtSecret string) error {
	if !s.Enabled {
		return nil
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s.CredentialEncryptionKeyB64))
	if err != nil {
		return errors.New("SCHEDULER_CREDENTIAL_ENCRYPTION_KEY must be valid base64 (e.g. openssl rand -base64 32)")
	}
	if len(raw) != 32 {
		return fmt.Errorf("SCHEDULER_CREDENTIAL_ENCRYPTION_KEY must decode to 32 bytes (got %d)", len(raw))
	}
	if jwtSecret != "" {
		if bytes.Equal(raw, []byte(jwtSecret)) {
			return errors.New("SCHEDULER_CREDENTIAL_ENCRYPTION_KEY must be independent of JWT_SECRET (equals the raw secret)")
		}
		legacy := sha256.Sum256([]byte(legacySchedulingKeyDomain + jwtSecret))
		if bytes.Equal(raw, legacy[:]) {
			return errors.New("SCHEDULER_CREDENTIAL_ENCRYPTION_KEY must be independent of JWT_SECRET (equals the legacy JWT-derived digest)")
		}
	}
	s.CredentialEncryptionKey = raw
	if s.KafkaBootstrap == "" {
		return errors.New("SCHEDULER_CONSUMER_ENABLED requires SCHEDULER_KAFKA_BOOTSTRAP_SERVERS")
	}
	if s.HeartbeatSeconds == 0 || s.HeartbeatSeconds >= s.LeaseTTLSeconds {
		return errors.New("SCHEDULER_HEARTBEAT_INTERVAL (seconds) must be positive and below SCHEDULER_LEASE_TTL (seconds)")
	}
	if s.MaxEventAttempts < 1 || s.MaxEventAttempts > 5 {
		return errors.New("SCHEDULER_MAX_EVENT_ATTEMPTS must be between 1 and 5 (ADR-0038 retry budget)")
	}
	switch s.KafkaSecurityProto {
	case "SSL":
		// mTLS: all three files required together.
		if s.KafkaTLSCertFile == "" || s.KafkaTLSKeyFile == "" || s.KafkaTLSCAFile == "" {
			return errors.New("SCHEDULER_KAFKA_SECURITY_PROTOCOL=SSL requires TLS CA, cert and key files")
		}
	case "SASL_SSL":
		if s.KafkaSASLUsername == "" || s.KafkaSASLPassword == "" {
			return errors.New("SASL_SSL requires both SASL username and password")
		}
	case "SASL_PLAINTEXT":
		if s.KafkaSASLUsername == "" || s.KafkaSASLPassword == "" {
			return errors.New("SASL_PLAINTEXT requires both SASL username and password")
		}
	case "PLAINTEXT":
		// explicit dev choice only
	default:
		return fmt.Errorf("unknown SCHEDULER_KAFKA_SECURITY_PROTOCOL %q (SSL|SASL_SSL|SASL_PLAINTEXT|PLAINTEXT)", s.KafkaSecurityProto)
	}
	switch s.KafkaSASLMechanism {
	case "SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN":
	default:
		return fmt.Errorf("unknown SCHEDULER_KAFKA_SASL_MECHANISM %q", s.KafkaSASLMechanism)
	}
	return nil
}

// legacySchedulingKeyDomain reproduces the Connector legacy digest
// domain so the known JWT-derived digest is rejected here too (same
// separator convention; not imported across internal packages).
const legacySchedulingKeyDomain = "openfoundry/credential-encryption/v1\x00"

func FromEnv() (*Config, error) {
	c := &Config{}
	c.Service.Name = "pipeline-build-service"
	c.Service.Version = defaultStr(os.Getenv("SERVICE_VERSION"), "dev")
	c.Host = defaultStr(os.Getenv("HOST"), "0.0.0.0")
	c.Port = parseUint16(os.Getenv("PORT"), 50081)
	c.DatabaseURL = os.Getenv("DATABASE_URL")
	c.JWTSecret = os.Getenv("JWT_SECRET")
	c.DataDir = defaultStr(os.Getenv("DATA_DIR"), "/var/lib/openfoundry/pipeline-build")
	c.DatasetServiceURL = defaultStr(os.Getenv("DATASET_SERVICE_URL"), "http://localhost:50079")
	c.DatasetServiceBearer = os.Getenv("DATASET_SERVICE_BEARER")
	c.OntologyDefinitionServiceURL = defaultStr(os.Getenv("ONTOLOGY_DEFINITION_SERVICE_URL"), "http://localhost:50103")
	c.OntologyDefinitionServiceBearer = os.Getenv("ONTOLOGY_DEFINITION_SERVICE_BEARER")
	c.ObjectDatabaseServiceURL = defaultStr(os.Getenv("OBJECT_DATABASE_SERVICE_URL"), "http://localhost:50104")
	c.ObjectDatabaseServiceBearer = os.Getenv("OBJECT_DATABASE_SERVICE_BEARER")
	c.WorkflowServiceURL = defaultStr(os.Getenv("WORKFLOW_SERVICE_URL"), "http://localhost:50080")
	c.AIServiceURL = defaultStr(os.Getenv("AI_SERVICE_URL"), "http://localhost:50127")
	c.AIServiceBearer = os.Getenv("AI_SERVICE_BEARER")
	c.PipelineLLMProtocol = defaultStr(os.Getenv("PIPELINE_LLM_PROTOCOL"), "ai-service")
	if c.PipelineLLMProtocol != "ai-service" && c.PipelineLLMProtocol != "agent-runtime" {
		return nil, errors.New("PIPELINE_LLM_PROTOCOL must be ai-service or agent-runtime")
	}
	c.StorageBackend = defaultStr(os.Getenv("STORAGE_BACKEND"), "local")
	c.StorageBucket = os.Getenv("STORAGE_BUCKET")
	c.S3Endpoint = os.Getenv("S3_ENDPOINT")
	c.S3Region = os.Getenv("S3_REGION")
	c.S3AccessKey = os.Getenv("S3_ACCESS_KEY")
	c.S3SecretKey = os.Getenv("S3_SECRET_KEY")
	c.LocalStorageRoot = os.Getenv("LOCAL_STORAGE_ROOT")
	c.DistributedPipelineWorkers = int(parseUint16(os.Getenv("DISTRIBUTED_PIPELINE_WORKERS"), 4))
	c.DistributedComputePollIntervalMS = parseUint64(os.Getenv("DISTRIBUTED_COMPUTE_POLL_INTERVAL_MS"), 1000)
	c.DistributedComputeTimeoutSecs = parseUint64(os.Getenv("DISTRIBUTED_COMPUTE_TIMEOUT_SECS"), 1800)
	c.SparkNamespace = defaultStr(os.Getenv("SPARK_NAMESPACE"), "openfoundry-spark")
	c.PipelineRunnerImage = defaultStr(os.Getenv("PIPELINE_RUNNER_IMAGE"), "openfoundry/pipeline-runner:dev")
	c.FoundryIcebergCatalogURL = os.Getenv("FOUNDRY_ICEBERG_CATALOG_URL")
	c.FoundryIcebergCatalogBearer = os.Getenv("FOUNDRY_ICEBERG_CATALOG_BEARER")
	c.PythonSidecarBinary = os.Getenv("PYTHON_SIDECAR_BINARY")
	c.PythonSidecarTimeoutSeconds = parseUint32(os.Getenv("PYTHON_SIDECAR_TIMEOUT_SECONDS"), 60)
	// WP3.3 3B batched execution knobs (defaults mirror the dataset
	// service's own page/line budgets; see handler/batched_dataset_client.go).
	c.BatchedScanPageRows = int(parseUint32(os.Getenv("BATCHED_SCAN_PAGE_ROWS"), 500))
	c.BatchedScanMaxResponseBytes = int64(parseUint64(os.Getenv("BATCHED_SCAN_MAX_RESPONSE_BYTES"), 64<<20))
	c.BatchedScanMaxPages = int(parseUint32(os.Getenv("BATCHED_SCAN_MAX_PAGES"), 10000))
	c.BatchedStageTimeoutSecs = parseUint32(os.Getenv("BATCHED_STAGE_TIMEOUT_SECONDS"), 30)
	c.BatchedExecuteLeaseTTLSecs = parseUint32(os.Getenv("BATCHED_EXECUTE_LEASE_TTL_SECONDS"), 1800)
	c.Scheduler.Enabled = os.Getenv("SCHEDULER_CONSUMER_ENABLED") == "true"
	c.Scheduler.KafkaBootstrap = strings.TrimSpace(os.Getenv("SCHEDULER_KAFKA_BOOTSTRAP_SERVERS"))
	c.Scheduler.GroupID = defaultStr(os.Getenv("SCHEDULER_GROUP_ID"), "pipeline-build-service-scheduler")
	c.Scheduler.CredentialEncryptionKeyB64 = os.Getenv("SCHEDULER_CREDENTIAL_ENCRYPTION_KEY")
	// Numeric knobs fail CLOSED when the consumer is enabled: a typo'd
	// value must abort startup, not silently fall back (units: seconds).
	if err := parseSchedulerUint32(os.Getenv("SCHEDULER_LEASE_TTL"), "SCHEDULER_LEASE_TTL", 120, c.Scheduler.Enabled, &c.Scheduler.LeaseTTLSeconds); err != nil {
		return nil, err
	}
	if err := parseSchedulerUint32(os.Getenv("SCHEDULER_HEARTBEAT_INTERVAL"), "SCHEDULER_HEARTBEAT_INTERVAL", 30, c.Scheduler.Enabled, &c.Scheduler.HeartbeatSeconds); err != nil {
		return nil, err
	}
	if err := parseSchedulerUint32(os.Getenv("SCHEDULER_MAX_EVENT_ATTEMPTS"), "SCHEDULER_MAX_EVENT_ATTEMPTS", 5, c.Scheduler.Enabled, &c.Scheduler.MaxEventAttempts); err != nil {
		return nil, err
	}
	c.Scheduler.KafkaSASLUsername = os.Getenv("SCHEDULER_KAFKA_SASL_USERNAME")
	c.Scheduler.KafkaSASLPassword = os.Getenv("SCHEDULER_KAFKA_SASL_PASSWORD")
	c.Scheduler.KafkaSASLMechanism = defaultStr(os.Getenv("SCHEDULER_KAFKA_SASL_MECHANISM"), "SCRAM-SHA-512")
	c.Scheduler.KafkaSecurityProto = defaultStr(os.Getenv("SCHEDULER_KAFKA_SECURITY_PROTOCOL"), "SSL")
	c.Scheduler.KafkaTLSCAFile = os.Getenv("SCHEDULER_KAFKA_TLS_CA_FILE")
	c.Scheduler.KafkaTLSCertFile = os.Getenv("SCHEDULER_KAFKA_TLS_CERT_FILE")
	c.Scheduler.KafkaTLSKeyFile = os.Getenv("SCHEDULER_KAFKA_TLS_KEY_FILE")
	if err := c.Scheduler.Validate(c.JWTSecret); err != nil {
		return nil, err
	}
	return c, nil
}

// parseSchedulerUint32 fills dst from env: empty -> fallback; invalid
// digits -> error when enabled (fail-closed), fallback otherwise.
func parseSchedulerUint32(v, name string, fallback uint32, enabled bool, dst *uint32) error {
	trimmed := strings.TrimSpace(v)
	if trimmed == "" {
		*dst = fallback
		return nil
	}
	n, err := strconv.ParseUint(trimmed, 10, 32)
	if err != nil {
		if enabled {
			return fmt.Errorf("%s must be a non-negative integer (seconds), got %q", name, trimmed)
		}
		*dst = fallback
		return nil
	}
	*dst = uint32(n)
	return nil
}

func defaultStr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func parseUint16(v string, fallback uint16) uint16 {
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseUint(v, 10, 16)
	if err != nil {
		return fallback
	}
	return uint16(n)
}

func parseUint32(v string, fallback uint32) uint32 {
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return fallback
	}
	return uint32(n)
}

func parseUint64(v string, fallback uint64) uint64 {
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}
