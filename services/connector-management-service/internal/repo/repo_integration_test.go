//go:build integration

// Real PostgreSQL 16 tenant-isolation regressions for the Source/Sync/SyncRun
// repository surface. These complement the handler-level fakes: they prove the
// generated tenant predicates (alias `c` / `d` / `r`) actually execute against
// PostgreSQL and that migrations work on empty databases, legacy (pre-org)
// databases, and on re-runs.
package repo

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	testingx "github.com/openfoundry/openfoundry-go/libs/testing"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// bootRepo provisions a FRESH database per test so migration and isolation
// tests never observe each other's applied state. It honours
// CONNECTOR_DB_DSN when set (acceptance runner inside a compose container,
// no Docker socket available) and otherwise falls back to the standard
// testcontainers harness.
func bootRepo(t *testing.T) *Repo {
	t.Helper()
	r, _ := bootRepoWithDSN(t)
	return r
}

// bootRepoWithDSN is bootRepo plus the DSN of the fresh database, so
// multi-connection lease tests can open independent sessions.
func bootRepoWithDSN(t *testing.T) (*Repo, string) {
	t.Helper()
	ctx := context.Background()
	if dsn := os.Getenv("CONNECTOR_DB_DSN"); dsn != "" {
		admin, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("connect CONNECTOR_DB_DSN: %v", err)
		}
		if err := admin.Ping(ctx); err != nil {
			t.Fatalf("ping CONNECTOR_DB_DSN: %v", err)
		}
		dbName := "connector_test_" + strings.ReplaceAll(uuid.New().String()[:8], "-", "")
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
			t.Fatalf("create fresh database: %v", err)
		}
		t.Cleanup(func() {
			_, _ = admin.Exec(ctx, "DROP DATABASE "+dbName)
			admin.Close()
		})
		fresh := freshDSN(dsn, dbName)
		pool, err := pgxpool.New(ctx, fresh)
		if err != nil {
			t.Fatalf("connect fresh database: %v", err)
		}
		t.Cleanup(pool.Close)
		return &Repo{Pool: pool}, fresh
	}
	harness := testingx.BootPostgres(ctx, t)
	t.Cleanup(func() { harness.Stop(ctx) })
	return &Repo{Pool: harness.Pool}, harness.URL
}

// freshDSN rewrites the path component of a DSN to the given database.
func freshDSN(dsn, dbName string) string {
	if idx := strings.LastIndex(dsn, "/"); idx >= 0 && !strings.Contains(dsn[idx:], "?") {
		return dsn[:idx+1] + dbName
	}
	if idx := strings.LastIndex(dsn, "/"); idx >= 0 {
		q := strings.Index(dsn[idx:], "?")
		return dsn[:idx+1] + dbName + dsn[idx+q:]
	}
	return dsn
}

// applyMigrationsExcept runs every embedded migration except the named one, in
// order — used to build a "legacy" database as it existed before org columns.
func applyMigrationsExcept(ctx context.Context, t *testing.T, pool *pgxpool.Pool, skipPrefix string) {
	t.Helper()
	entries, err := migrationsFS.ReadDir("migrations")
	require.NoError(t, err)
	names := []string{}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if strings.HasPrefix(name, skipPrefix) {
			continue
		}
		body, err := migrationsFS.ReadFile("migrations/" + name)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, string(body))
		require.NoError(t, err, "apply %s", name)
	}
}

func orgPtr(v uuid.UUID) *uuid.UUID { return &v }

func TestPGMigrateEmptyDatabaseAndRerun(t *testing.T) {
	r := bootRepo(t)
	ctx := context.Background()
	require.NoError(t, Migrate(ctx, r.Pool), "empty database migration")
	require.NoError(t, Migrate(ctx, r.Pool), "migration re-run must be idempotent")

	var count int
	require.NoError(t, r.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM connections`).Scan(&count))
	assert.Equal(t, 0, count)
}

func TestPGMigrateLegacyDatabaseGainsOrgColumns(t *testing.T) {
	r := bootRepo(t)
	ctx := context.Background()
	applyMigrationsExcept(ctx, t, r.Pool, "20261113000000")

	// Legacy rows: no org columns existed, owner-scoped only.
	legacyOwner := uuid.New()
	legacySource := uuid.New()
	legacyDataset := uuid.New()
	_, err := r.Pool.Exec(ctx,
		`INSERT INTO connections (id, name, connector_type, config, owner_id) VALUES ($1,'legacy','postgresql','{}',$2)`,
		legacySource, legacyOwner)
	require.NoError(t, err)
	_, err = r.Pool.Exec(ctx,
		`INSERT INTO batch_sync_defs (id, source_id, output_dataset_id) VALUES ($1,$2,$3)`,
		legacyDataset, legacySource, uuid.New())
	require.NoError(t, err)
	_, err = r.Pool.Exec(ctx,
		`INSERT INTO sync_runs (id, sync_def_id, status) VALUES ($1,$2,'succeeded')`,
		uuid.New(), legacyDataset)
	require.NoError(t, err)

	// The new migration adds the org columns and must succeed on the old shape.
	require.NoError(t, Migrate(ctx, r.Pool))
	require.NoError(t, Migrate(ctx, r.Pool), "re-run after legacy upgrade stays idempotent")

	// Legacy rows keep NULL org: personal-domain owner still sees them.
	personal := Tenant{OwnerID: legacyOwner}
	got, err := r.GetConnectionForTenant(ctx, legacySource, personal)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Nil(t, got.OrgID)

	// Org callers never see them, even with the same subject.
	org := uuid.New()
	cross, err := r.GetConnectionForTenant(ctx, legacySource, Tenant{OwnerID: legacyOwner, OrgID: orgPtr(org)})
	require.NoError(t, err)
	assert.Nil(t, cross)

	jobs, err := r.ListSyncJobs(ctx, legacySource, personal)
	require.NoError(t, err)
	assert.Len(t, jobs, 1, "legacy sync defs stay reachable from the personal domain")
	orgJobs, err := r.ListSyncJobs(ctx, legacySource, Tenant{OwnerID: legacyOwner, OrgID: orgPtr(org)})
	require.NoError(t, err)
	assert.Empty(t, orgJobs)
}

func TestPGSourceTenantCRUDMatrix(t *testing.T) {
	r := bootRepo(t)
	ctx := context.Background()
	require.NoError(t, Migrate(ctx, r.Pool))

	user := uuid.New()
	orgA, orgB := uuid.New(), uuid.New()
	tenantA := Tenant{OwnerID: user, OrgID: orgPtr(orgA)}
	tenantB := Tenant{OwnerID: user, OrgID: orgPtr(orgB)}
	personal := Tenant{OwnerID: user}

	srcA, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{Name: "a-src", ConnectorType: "postgresql"}, tenantA)
	require.NoError(t, err)
	require.NotNil(t, srcA.OrgID)
	assert.Equal(t, orgA, *srcA.OrgID)
	srcB, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{Name: "a-src", ConnectorType: "postgresql"}, tenantB)
	require.NoError(t, err)
	srcP, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{Name: "a-src", ConnectorType: "postgresql"}, personal)
	require.NoError(t, err)
	assert.Nil(t, srcP.OrgID)

	t.Run("list is scoped per domain", func(t *testing.T) {
		for _, tc := range []struct {
			tenant Tenant
			want   uuid.UUID
		}{{tenantA, srcA.ID}, {tenantB, srcB.ID}, {personal, srcP.ID}} {
			items, err := r.ListConnections(ctx, tc.tenant, nil)
			require.NoError(t, err)
			require.Len(t, items, 1)
			assert.Equal(t, tc.want, items[0].ID)
		}
	})

	t.Run("owner filter narrows within tenant", func(t *testing.T) {
		items, err := r.ListConnections(ctx, tenantA, &user)
		require.NoError(t, err)
		assert.Len(t, items, 1)
		other := uuid.New()
		items, err = r.ListConnections(ctx, tenantA, &other)
		require.NoError(t, err)
		assert.Empty(t, items)
	})

	t.Run("cross-domain get is nil", func(t *testing.T) {
		got, err := r.GetConnectionForTenant(ctx, srcA.ID, tenantB)
		require.NoError(t, err)
		assert.Nil(t, got)
		got, err = r.GetConnectionForTenant(ctx, srcA.ID, personal)
		require.NoError(t, err)
		assert.Nil(t, got, "org rows must not leak into the personal domain")
	})

	t.Run("cross-domain update is a no-op", func(t *testing.T) {
		renamed := "hijacked"
		updated, err := r.UpdateConnection(ctx, srcA.ID, tenantB, &models.UpdateConnectionRequest{Name: &renamed})
		require.NoError(t, err)
		assert.Nil(t, updated)
		got, err := r.GetConnectionForTenant(ctx, srcA.ID, tenantA)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "a-src", got.Name, "cross-domain update must not mutate")
	})

	t.Run("same-domain update works", func(t *testing.T) {
		renamed := "renamed-a"
		updated, err := r.UpdateConnection(ctx, srcA.ID, tenantA, &models.UpdateConnectionRequest{Name: &renamed})
		require.NoError(t, err)
		require.NotNil(t, updated)
		assert.Equal(t, renamed, updated.Name)
	})

	t.Run("cross-domain delete is refused", func(t *testing.T) {
		deleted, err := r.DeleteConnection(ctx, srcA.ID, tenantB)
		require.NoError(t, err)
		assert.False(t, deleted)
		got, err := r.GetConnectionForTenant(ctx, srcA.ID, tenantA)
		require.NoError(t, err)
		assert.NotNil(t, got)
	})

	t.Run("credentials are tenant-scoped", func(t *testing.T) {
		set, err := r.SetCredential(ctx, srcA.ID, tenantA, "api_key", []byte("ct"), "fp")
		require.NoError(t, err)
		require.NotNil(t, set)
		set, err = r.SetCredential(ctx, srcA.ID, tenantB, "api_key", []byte("ct-b"), "fp-b")
		require.NoError(t, err)
		assert.Nil(t, set, "cross-domain credential writes must not attach")

		ct, err := r.GetSourceCredentialCiphertext(ctx, srcA.ID, tenantA, "api_key")
		require.NoError(t, err)
		assert.Equal(t, []byte("ct"), ct)
		ct, err = r.GetSourceCredentialCiphertext(ctx, srcA.ID, tenantB, "api_key")
		require.NoError(t, err)
		assert.Nil(t, ct)

		creds, err := r.ListCredentials(ctx, srcA.ID, tenantA)
		require.NoError(t, err)
		assert.Len(t, creds, 1)
		creds, err = r.ListCredentials(ctx, srcA.ID, personal)
		require.NoError(t, err)
		assert.Empty(t, creds)
	})

	t.Run("sync defs inherit org and stay isolated", func(t *testing.T) {
		target := uuid.New()
		job, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{SourceID: srcA.ID, OutputDatasetID: target}, tenantA)
		require.NoError(t, err)
		require.NotNil(t, job)
		require.NotNil(t, job.OrgID)
		assert.Equal(t, orgA, *job.OrgID)

		denied, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{SourceID: srcA.ID, OutputDatasetID: target}, tenantB)
		require.NoError(t, err)
		assert.Nil(t, denied)

		got, err := r.GetSyncJob(ctx, job.ID, tenantB)
		require.NoError(t, err)
		assert.Nil(t, got)
		jobs, err := r.ListSyncJobs(ctx, srcA.ID, tenantB)
		require.NoError(t, err)
		assert.Empty(t, jobs)

		repointed := uuid.New()
		updated, err := r.UpdateSyncJob(ctx, job.ID, &models.UpdateSyncJobRequest{OutputDatasetID: &repointed}, tenantA)
		require.NoError(t, err)
		require.NotNil(t, updated)
		assert.Equal(t, repointed, updated.OutputDatasetID)
		crossUpdate, err := r.UpdateSyncJob(ctx, job.ID, &models.UpdateSyncJobRequest{OutputDatasetID: &target}, tenantB)
		require.NoError(t, err)
		assert.Nil(t, crossUpdate, "cross-domain sync update must not mutate")
	})

	t.Run("runs inherit org and stay isolated", func(t *testing.T) {
		job, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{SourceID: srcA.ID, OutputDatasetID: uuid.New()}, tenantA)
		require.NoError(t, err)
		run, err := r.RunSyncJob(ctx, job.ID, tenantA)
		require.NoError(t, err)
		require.NotNil(t, run)
		require.NotNil(t, run.OrgID)
		assert.Equal(t, orgA, *run.OrgID)

		denied, err := r.RunSyncJob(ctx, job.ID, tenantB)
		require.NoError(t, err)
		assert.Nil(t, denied, "cross-domain trigger must not create a run")
		runs, err := r.ListSyncRuns(ctx, job.ID, tenantA)
		require.NoError(t, err)
		assert.Len(t, runs, 1)

		hash := "deadbeef"
		version := uuid.New()
		rows := int64(42)
		completed, err := r.CompleteSyncRun(ctx, run.ID, tenantA, "succeeded", 100, 1, rows, nil, nil, &version, &hash)
		require.NoError(t, err)
		require.NotNil(t, completed)
		assert.Equal(t, int64(100), completed.BytesWritten)
		assert.Equal(t, rows, completed.RowsWritten)
		assert.Equal(t, version, *completed.DatasetVersionID)

		crossCompleted, err := r.CompleteSyncRun(ctx, run.ID, tenantB, "failed", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, crossCompleted, "cross-domain completion must not mutate the run")

		empty, err := r.ListSyncRuns(ctx, job.ID, tenantB)
		require.NoError(t, err)
		assert.Empty(t, empty)
	})

	t.Run("update connection config returns org column", func(t *testing.T) {
		updated, err := r.UpdateConnectionConfig(ctx, srcA.ID, json.RawMessage(`{"k":1}`))
		require.NoError(t, err)
		require.NotNil(t, updated, "10-column scan must succeed after UPDATE")
		require.NotNil(t, updated.OrgID)
		assert.Equal(t, orgA, *updated.OrgID)
	})

	t.Run("same-domain delete works", func(t *testing.T) {
		deleted, err := r.DeleteConnection(ctx, srcB.ID, tenantB)
		require.NoError(t, err)
		assert.True(t, deleted)
	})
}
