package config_test

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/config"
)

func setBaseEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"SERVICE_VERSION", "HOST", "PORT", "DATABASE_URL", "JWT_SECRET", "OPENFOUNDRY_JWT_SECRET", "METRICS_ADDR",
		"DATASET_SERVICE_URL", "PIPELINE_SERVICE_URL", "ONTOLOGY_SERVICE_URL", "INGESTION_REPLICATION_GRPC_URL",
		"NETWORK_BOUNDARY_SERVICE_URL", "SYNC_POLL_INTERVAL_SECS", "ALLOW_PRIVATE_NETWORK_EGRESS", "ALLOWED_EGRESS_HOSTS",
		"AGENT_STALE_AFTER_SECS", "MEDIA_SETS_SERVICE_URL", "CREDENTIAL_ENCRYPTION_KEY", "SECRET_MANAGER_URL",
		"OUTBOX_ENABLED", "OPENFOUNDRY_AUTO_REGISTRATION_INTERVAL_SECS", "OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS", "OPENFOUNDRY_DEV_AUTH", "OPENFOUNDRY_VENDED_CREDENTIALS_TTL_SECS",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost/db")
	t.Setenv("JWT_SECRET", "secret")
}

func TestFromEnvRustCompatibleDefaults(t *testing.T) {
	setBaseEnv(t)

	cfg, err := config.FromEnv()
	require.NoError(t, err)
	require.Equal(t, "connector-management-service", cfg.Service.Name)
	require.Equal(t, "dev", cfg.Service.Version)
	require.Equal(t, config.DefaultHost, cfg.Server.Host)
	require.Equal(t, config.DefaultPort, cfg.Server.Port)
	require.Equal(t, "postgres://user:pass@localhost/db", cfg.DatabaseURL)
	require.Equal(t, "secret", cfg.JWTSecret)
	require.Equal(t, config.DefaultDatasetServiceURL, cfg.DatasetServiceURL)
	require.Equal(t, config.DefaultPipelineServiceURL, cfg.PipelineServiceURL)
	require.Equal(t, config.DefaultOntologyServiceURL, cfg.OntologyServiceURL)
	require.Empty(t, cfg.IngestionReplicationGRPCURL)
	require.Equal(t, config.DefaultNetworkBoundaryURL, cfg.NetworkBoundaryServiceURL)
	require.Equal(t, uint64(config.DefaultSyncPollIntervalSecs), cfg.SyncPollIntervalSecs)
	require.True(t, cfg.AllowPrivateNetworkEgress)
	require.Nil(t, cfg.AllowedEgressHosts)
	require.Equal(t, uint64(config.DefaultAgentStaleAfterSecs), cfg.AgentStaleAfterSecs)
	require.Equal(t, config.DefaultMediaSetsServiceURL, cfg.MediaSetsServiceURL)
	require.Empty(t, cfg.CredentialEncryptionKey)
	require.NotEqual(t, [32]byte{}, cfg.CredentialKey)
	require.Empty(t, cfg.SecretManagerURL)
	require.True(t, cfg.OutboxEnabled)
	require.Zero(t, cfg.AutoRegistrationIntervalSecs)
	require.Zero(t, cfg.SyncSchedulerIntervalSecs)
	require.False(t, cfg.OpenFoundryDevAuth)
	require.Equal(t, int64(config.DefaultVendedCredentialsTTL), cfg.VendedCredentialsTTLSeconds)
}

func TestFromEnvOverridesAndParsesListsAndBools(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("SERVICE_VERSION", "v1")
	t.Setenv("HOST", "127.0.0.1")
	t.Setenv("PORT", "6000")
	t.Setenv("OPENFOUNDRY_JWT_SECRET", "preferred")
	t.Setenv("DATASET_SERVICE_URL", "http://dataset")
	t.Setenv("PIPELINE_SERVICE_URL", "http://pipeline")
	t.Setenv("ONTOLOGY_SERVICE_URL", "http://ontology")
	t.Setenv("INGESTION_REPLICATION_GRPC_URL", "http://ingestion:50091")
	t.Setenv("NETWORK_BOUNDARY_SERVICE_URL", "http://boundary")
	t.Setenv("SYNC_POLL_INTERVAL_SECS", "7")
	t.Setenv("ALLOW_PRIVATE_NETWORK_EGRESS", "false")
	t.Setenv("ALLOWED_EGRESS_HOSTS", "api.example.com, *.trusted.test ,,data.local")
	t.Setenv("AGENT_STALE_AFTER_SECS", "321")
	t.Setenv("MEDIA_SETS_SERVICE_URL", "http://media")
	keyBytes := []byte("12345678901234567890123456789012")
	t.Setenv("CREDENTIAL_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString(keyBytes))
	t.Setenv("SECRET_MANAGER_URL", "http://secrets")
	t.Setenv("OUTBOX_ENABLED", "false")
	t.Setenv("OPENFOUNDRY_AUTO_REGISTRATION_INTERVAL_SECS", "60")
	t.Setenv("OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS", "15")
	t.Setenv("OPENFOUNDRY_DEV_AUTH", "true")
	t.Setenv("OPENFOUNDRY_VENDED_CREDENTIALS_TTL_SECS", "1200")

	cfg, err := config.FromEnv()
	require.NoError(t, err)
	require.Equal(t, "v1", cfg.Service.Version)
	require.Equal(t, "127.0.0.1", cfg.Server.Host)
	require.Equal(t, uint16(6000), cfg.Server.Port)
	require.Equal(t, "preferred", cfg.JWTSecret)
	require.Equal(t, "http://dataset", cfg.DatasetServiceURL)
	require.Equal(t, "http://pipeline", cfg.PipelineServiceURL)
	require.Equal(t, "http://ontology", cfg.OntologyServiceURL)
	require.Equal(t, "http://ingestion:50091", cfg.IngestionReplicationGRPCURL)
	require.Equal(t, "http://boundary", cfg.NetworkBoundaryServiceURL)
	require.Equal(t, uint64(7), cfg.SyncPollIntervalSecs)
	require.False(t, cfg.AllowPrivateNetworkEgress)
	require.Equal(t, []string{"api.example.com", "*.trusted.test", "data.local"}, cfg.AllowedEgressHosts)
	require.Equal(t, uint64(321), cfg.AgentStaleAfterSecs)
	require.Equal(t, "http://media", cfg.MediaSetsServiceURL)
	require.Equal(t, base64.StdEncoding.EncodeToString(keyBytes), cfg.CredentialEncryptionKey)
	require.Equal(t, [32]byte(keyBytes), cfg.CredentialKey)
	require.Equal(t, "http://secrets", cfg.SecretManagerURL)
	require.False(t, cfg.OutboxEnabled)
	require.Equal(t, uint64(60), cfg.AutoRegistrationIntervalSecs)
	require.Equal(t, uint64(15), cfg.SyncSchedulerIntervalSecs)
	require.True(t, cfg.OpenFoundryDevAuth)
	require.Equal(t, int64(1200), cfg.VendedCredentialsTTLSeconds)
}

func TestFromEnvRequiresDatabaseAndJWT(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("DATABASE_URL", "")
	_, err := config.FromEnv()
	require.Error(t, err)
	require.True(t, config.IsMissingEnv(err))

	setBaseEnv(t)
	t.Setenv("JWT_SECRET", "")
	_, err = config.FromEnv()
	require.Error(t, err)
	require.True(t, config.IsMissingEnv(err))
}

func TestFromEnvRejectsInvalidTypedValues(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("ALLOW_PRIVATE_NETWORK_EGRESS", "not-bool")
	_, err := config.FromEnv()
	require.Error(t, err)

	setBaseEnv(t)
	t.Setenv("PORT", "999999")
	_, err = config.FromEnv()
	require.Error(t, err)

	setBaseEnv(t)
	t.Setenv("CREDENTIAL_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString([]byte("too-short")))
	_, err = config.FromEnv()
	require.Error(t, err)
}

// P2.2 coordinator overrides: unset stays nil (main substitutes defaults);
// explicit values are parsed AND range-validated HERE — explicit 0, negatives
// and over-max are startup errors, never silently turned into defaults.
func TestFromEnvSyncAsyncOverrides(t *testing.T) {
	t.Run("unset keeps nil", func(t *testing.T) {
		setBaseEnv(t)
		cfg, err := config.FromEnv()
		require.NoError(t, err)
		require.Nil(t, cfg.SyncAsyncQueueDepth)
		require.Nil(t, cfg.SyncAsyncWorkers)
		require.Nil(t, cfg.SyncAsyncAuthTTLSecs)
		require.Nil(t, cfg.SyncAsyncTaskTTLSecs)
	})

	t.Run("valid explicit values pass through", func(t *testing.T) {
		setBaseEnv(t)
		t.Setenv("CONNECTOR_SYNC_ASYNC_QUEUE", "128")
		t.Setenv("CONNECTOR_SYNC_ASYNC_WORKERS", "4")
		t.Setenv("CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS", "600")
		t.Setenv("CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS", "900")
		cfg, err := config.FromEnv()
		require.NoError(t, err)
		require.NotNil(t, cfg.SyncAsyncQueueDepth)
		require.Equal(t, int64(128), *cfg.SyncAsyncQueueDepth)
		require.Equal(t, int64(4), *cfg.SyncAsyncWorkers)
		require.Equal(t, int64(600), *cfg.SyncAsyncAuthTTLSecs)
		require.Equal(t, int64(900), *cfg.SyncAsyncTaskTTLSecs)
	})

	for name, kv := range map[string]string{
		"explicit zero":  "0",
		"negative":       "-5",
		"over max":       "99999",
		"not an integer": "twelve",
	} {
		t.Run("queue rejects "+name, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("CONNECTOR_SYNC_ASYNC_QUEUE", kv)
			_, err := config.FromEnv()
			require.Error(t, err)
		})
		t.Run("workers rejects "+name, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("CONNECTOR_SYNC_ASYNC_WORKERS", kv)
			_, err := config.FromEnv()
			require.Error(t, err)
		})
	}
	for _, v := range []string{"0", "-1", "90000000", "soon"} {
		t.Run("auth ttl rejects "+v, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS", v)
			_, err := config.FromEnv()
			require.Error(t, err)
		})
		t.Run("task ttl rejects "+v, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS", v)
			_, err := config.FromEnv()
			require.Error(t, err)
		})
	}
}
