//go:build integration

package postgres

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	testingx "github.com/openfoundry/openfoundry-go/libs/testing"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/resolver"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
	livellogs "github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/logs"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/migrations"
)

// bootPool provisions a FRESH database per test so migration tests never
// observe each other's applied state. It honours PIPELINE_DB_DSN when set
// (standalone container) and otherwise falls back to the standard
// testcontainers harness.
func bootPool(ctx context.Context, t *testing.T) *pgxpool.Pool {
	t.Helper()
	if dsn := os.Getenv("PIPELINE_DB_DSN"); dsn != "" {
		admin, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("connect PIPELINE_DB_DSN: %v", err)
		}
		if err := admin.Ping(ctx); err != nil {
			t.Fatalf("ping PIPELINE_DB_DSN: %v", err)
		}
		dbName := "tenant_test_" + strings.ReplaceAll(uuid.New().String()[:8], "-", "")
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
			t.Fatalf("create fresh database: %v", err)
		}
		t.Cleanup(func() {
			_, _ = admin.Exec(ctx, "DROP DATABASE "+dbName)
			admin.Close()
		})
		pool, err := pgxpool.New(ctx, freshDSN(dsn, dbName))
		if err != nil {
			t.Fatalf("connect fresh database: %v", err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	return testingx.BootPostgres(ctx, t).Pool
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

// TestTenantIsolationMigrationsAndBoundary is the P1.1 acceptance spine
// against a real Postgres 16:
//
//  1. full migration set applies on a fresh database and re-runs
//     unchanged (including the constraint swap in
//     20261113100000_tenant_isolation.sql);
//  2. a database populated with pre-tenant rows migrates forward: legacy
//     rows keep tenant NULL (personal domain of their owner) and the
//     legacy global unique constraint is replaced by the tenant-aware
//     index;
//  3. org A / org B with the SAME subject create pipelines with the SAME
//     name and stay mutually invisible across list, get, update, delete;
//  4. runs, job specs, builds, jobs and job logs inherit the boundary;
//  5. admin claims do not cross tenants;
//  6. a no-OrgID subject keeps exclusive access to its personal rows.
func TestTenantIsolationMigrationsAndBoundary(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)

	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("first RunMigrations on empty DB: %v", err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("second RunMigrations (re-run): %v", err)
	}

	repo := NewRepositoryFromPool(pool)
	orgA := uuid.MustParse("aaaaaaaa-0000-0000-0000-000000000001")
	orgB := uuid.MustParse("bbbbbbbb-0000-0000-0000-000000000002")
	sub := uuid.MustParse("cccccccc-0000-0000-0000-000000000003")
	otherUser := uuid.MustParse("dddddddd-0000-0000-0000-000000000004")
	scopeA := &handler.TenantScope{TenantID: &orgA, Subject: sub}
	scopeB := &handler.TenantScope{TenantID: &orgB, Subject: sub}
	scopePersonal := &handler.TenantScope{TenantID: nil, Subject: sub}
	scopeOtherPersonal := &handler.TenantScope{TenantID: nil, Subject: otherUser}
	ctxA := handler.ContextWithTenantScope(ctx, scopeA)
	ctxB := handler.ContextWithTenantScope(ctx, scopeB)
	ctxPersonal := handler.ContextWithTenantScope(ctx, scopePersonal)
	ctxOtherPersonal := handler.ContextWithTenantScope(ctx, scopeOtherPersonal)

	// --- org A / org B, same subject, same name -----------------------
	pA, err := repo.CreatePipeline(ctxA, models.CreatePipelineRequest{Name: "orders-etl"}, sub)
	require.NoError(t, err, "org A creates orders-etl")
	require.NotNil(t, pA.TenantID)
	require.Equal(t, orgA, *pA.TenantID)

	pB, err := repo.CreatePipeline(ctxB, models.CreatePipelineRequest{Name: "orders-etl"}, sub)
	require.NoError(t, err, "org B creates same-named pipeline")
	require.Equal(t, orgB, *pB.TenantID)

	listA, err := repo.ListPipelines(ctxA, models.ListPipelinesQuery{})
	require.NoError(t, err)
	require.Equal(t, int64(1), listA.Total, "org A list sees only its pipeline")
	listB, err := repo.ListPipelines(ctxB, models.ListPipelinesQuery{})
	require.NoError(t, err)
	require.Equal(t, int64(1), listB.Total, "org B list sees only its pipeline")
	require.Equal(t, pB.ID, listB.Data[0].ID)

	foreign, err := repo.GetPipeline(ctxB, pA.ID)
	require.NoError(t, err)
	require.Nil(t, foreign, "org B cannot fetch org A pipeline by id")

	updated, err := repo.UpdatePipeline(ctxB, pA.ID, models.UpdatePipelineRequest{})
	require.NoError(t, err)
	require.Nil(t, updated, "org B cannot update org A pipeline")

	deleted, err := repo.DeletePipeline(ctxB, pA.ID)
	require.NoError(t, err)
	require.False(t, deleted, "org B cannot delete org A pipeline")

	// --- admin does not cross tenants ---------------------------------
	adminA := &handler.TenantScope{TenantID: &orgA, Subject: sub}
	// (roles are irrelevant to the repository predicates; the scope is
	// derived from claims.OrgID only — see handler.TenantScopeFromClaims)
	adminSeen, err := repo.ListPipelines(handler.ContextWithTenantScope(ctx, adminA), models.ListPipelinesQuery{})
	require.NoError(t, err)
	require.Equal(t, int64(1), adminSeen.Total)

	// --- runs inherit the pipeline boundary ---------------------------
	run, err := repo.OpenPipelineRunWithOptions(ctxA, pA, models.TriggerPipelineRequest{}, &sub, "manual", nil, nil, 1, []byte(`{}`))
	require.NoError(t, err)
	require.NotNil(t, run)

	runB, err := repo.GetPipelineRun(ctxB, pA.ID, run.ID)
	require.NoError(t, err)
	require.Nil(t, runB, "org B cannot read org A run")

	cancelled, exists, err := repo.AbortPipelineRun(ctxB, run.ID)
	require.NoError(t, err)
	require.Nil(t, cancelled)
	require.False(t, exists, "org B cancel of org A run reports not-found")

	queueB, err := repo.ListBuildQueue(ctxB, handler.BuildQueueQuery{})
	require.NoError(t, err)
	require.Empty(t, queueB, "org B queue excludes org A run")

	// --- job specs: same key string in two tenants --------------------
	const sharedPipelineRID = "ri.foundry.main.pipeline.shared-key"
	const sharedOutput = "ri.foundry.main.dataset.shared-out"
	specA, err := repo.PublishJobSpec(ctxA, "TRANSFORM", handler.CreateJobSpecRequest{PipelineRID: sharedPipelineRID, BranchName: "main", OutputDatasetRIDs: []string{sharedOutput}, LogicPayload: []byte(`{"a":1}`)}, sub.String())
	require.NoError(t, err, "org A publishes spec")
	specB, err := repo.PublishJobSpec(ctxB, "TRANSFORM", handler.CreateJobSpecRequest{PipelineRID: sharedPipelineRID, BranchName: "main", OutputDatasetRIDs: []string{sharedOutput}, LogicPayload: []byte(`{"a":1}`)}, sub.String())
	require.NoError(t, err, "org B publishes the same key — new unique index allows it")
	require.NotEqual(t, specA.RID, specB.RID)

	lookupA, err := repo.Lookup(ctxA, sharedPipelineRID, sharedOutput, "main", nil)
	require.NoError(t, err)
	require.NotNil(t, lookupA)
	require.NotEmpty(t, lookupA.RID)
	lookupB, err := repo.Lookup(ctxB, sharedPipelineRID, sharedOutput, "main", nil)
	require.NoError(t, err)
	require.NotNil(t, lookupB)
	require.NotEqual(t, lookupA.RID, lookupB.RID, "each tenant resolves its own spec row")

	// Personal-domain (tenant NULL) upsert on the NULLS NOT DISTINCT
	// index: the same key published twice by the same subject must
	// upsert, not error and not duplicate.
	const personalKey = "ri.foundry.main.pipeline.personal-key"
	personalOnce, err := repo.PublishJobSpec(ctxPersonal, "TRANSFORM", handler.CreateJobSpecRequest{PipelineRID: personalKey, BranchName: "main", OutputDatasetRIDs: []string{sharedOutput}, LogicPayload: []byte(`{}`)}, sub.String())
	require.NoError(t, err)
	personalTwice, err := repo.PublishJobSpec(ctxPersonal, "TRANSFORM", handler.CreateJobSpecRequest{PipelineRID: personalKey, BranchName: "main", OutputDatasetRIDs: []string{sharedOutput}, LogicPayload: []byte(`{}`)}, sub.String())
	require.NoError(t, err, "NULL-tenant republish upserts via the NULLS NOT DISTINCT arbiter")
	require.Equal(t, personalOnce.RID, personalTwice.RID, "identical content dedups to the existing row")
	var personalRows int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM pipeline_job_specs WHERE pipeline_rid=$1`, personalKey).Scan(&personalRows)
	require.NoError(t, err)
	require.Equal(t, 1, personalRows)

	// --- builds / jobs / logs chain to the build tenant ---------------
	err = repo.OpenBuild(ctxA, resolveArgsFor(sharedPipelineRID, sub), uuid.New())
	require.NoError(t, err)
	buildsA, err := repo.ListBuilds(ctxA, models.ListBuildsQuery{})
	require.NoError(t, err)
	require.Len(t, buildsA, 1)
	buildsB, err := repo.ListBuilds(ctxB, models.ListBuildsQuery{})
	require.NoError(t, err)
	require.Empty(t, buildsB, "org B build list excludes org A builds")

	buildA := buildsA[0]
	gotB, err := repo.GetBuild(ctxB, buildA.RID)
	require.NoError(t, err)
	require.Nil(t, gotB, "org B cannot fetch org A build by rid")

	jobID := uuid.New()
	var jobRID string
	err = pool.QueryRow(ctx, `INSERT INTO jobs (id, build_id, job_spec_rid, state) VALUES ($1,$2,$3,'WAITING') RETURNING rid`, jobID, buildA.ID, specA.RID).Scan(&jobRID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO job_logs (id, job_id, level, message) VALUES ($1,$2,'INFO','tenant-owned line')`, uuid.New(), jobID)
	require.NoError(t, err)

	jobForeign, err := repo.GetJob(ctxB, jobRID)
	require.NoError(t, err)
	require.Nil(t, jobForeign, "org B cannot fetch org A job by rid")

	// The UUID branch of the id lookup must be tenant-guarded too —
	// AND binds tighter than OR, so an unparenthesized filter would let
	// a bare build/job UUID bypass the boundary.
	buildByUUIDForeign, err := repo.GetBuild(ctxB, buildA.ID.String())
	require.NoError(t, err)
	require.Nil(t, buildByUUIDForeign, "org B cannot fetch org A build by UUID")

	jobsByUUIDForeign, err := repo.ListJobsForBuildID(ctxB, buildA.ID.String())
	require.NoError(t, err)
	require.Empty(t, jobsByUUIDForeign, "org B cannot list org A build jobs by UUID")

	jobByUUIDForeign, err := repo.GetJob(ctxB, jobID.String())
	require.NoError(t, err)
	require.Nil(t, jobByUUIDForeign, "org B cannot fetch org A job by UUID")

	// Personal domain mirrors the same dual-path check against org data.
	buildByUUIDPersonal, err := repo.GetBuild(ctxOtherPersonal, buildA.ID.String())
	require.NoError(t, err)
	require.Nil(t, buildByUUIDPersonal, "personal user cannot fetch org build by UUID")
	jobByUUIDPersonal, err := repo.GetJob(ctxOtherPersonal, jobID.String())
	require.NoError(t, err)
	require.Nil(t, jobByUUIDPersonal, "personal user cannot fetch org job by UUID")

	// Same-tenant lookups still resolve through both branches.
	sameByUUID, err := repo.GetBuild(ctxA, buildA.ID.String())
	require.NoError(t, err)
	require.NotNil(t, sameByUUID, "own tenant resolves by UUID")
	sameByRID, err := repo.GetBuild(ctxA, buildA.RID)
	require.NoError(t, err)
	require.NotNil(t, sameByRID, "own tenant resolves by RID")

	logsA, err := repo.History(ctxA, jobRID, livellogs.Query{Limit: 10})
	require.NoError(t, err)
	require.Len(t, logsA, 1)
	logsB, err := repo.History(ctxB, jobRID, livellogs.Query{Limit: 10})
	require.NoError(t, err)
	require.Empty(t, logsB, "org B job-log history is empty")

	inScopeA, err := repo.JobRIDInScope(ctxA, jobRID)
	require.NoError(t, err)
	require.True(t, inScopeA)
	inScopeB, err := repo.JobRIDInScope(ctxB, jobRID)
	require.NoError(t, err)
	require.False(t, inScopeB)

	// --- spark submissions: cross-tenant run-id squatting rejected ----
	sparkRunID := uuid.New()
	err = repo.SaveSparkSubmission(ctxA, handler.SparkSubmission{PipelineRunID: sparkRunID, Namespace: "ns", SparkAppName: "app-a", Status: "SUBMITTED"})
	require.NoError(t, err, "org A saves its submission")
	err = repo.SaveSparkSubmission(ctxB, handler.SparkSubmission{PipelineRunID: sparkRunID, Namespace: "ns", SparkAppName: "app-b", Status: "SUBMITTED"})
	require.ErrorIs(t, err, handler.ErrSparkSubmissionDomainConflict, "org B cannot hijack org A's run id")
	held, err := repo.GetSparkSubmission(ctxA, sparkRunID)
	require.NoError(t, err)
	require.NotNil(t, held)
	require.Equal(t, "app-a", held.SparkAppName, "foreign upsert left the row untouched")

	// --- legacy personal rows: owner keeps access, nobody else --------
	legacyOwner := uuid.MustParse("eeeeeeee-0000-0000-0000-000000000005")
	legacyID := uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO pipelines (id, name, owner_id, dag, status, tenant_id) VALUES ($1,'legacy-csv-flow',$2,'[]','draft',NULL)`, legacyID, legacyOwner)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO pipeline_versions (id, pipeline_id, version_number, version_kind, dag, name, created_by) VALUES ($1,$2,1,'draft','[]','legacy-csv-flow',$3)`, uuid.New(), legacyID, legacyOwner)
	require.NoError(t, err)

	legacyOwnerScope := &handler.TenantScope{TenantID: nil, Subject: legacyOwner}
	own, err := repo.GetPipeline(handler.ContextWithTenantScope(ctx, legacyOwnerScope), legacyID)
	require.NoError(t, err)
	require.NotNil(t, own, "legacy owner keeps access to tenant-NULL row")

	otherPersonal, err := repo.GetPipeline(ctxOtherPersonal, legacyID)
	require.NoError(t, err)
	require.Nil(t, otherPersonal, "another personal user cannot see the legacy row")

	orgView, err := repo.GetPipeline(ctxA, legacyID)
	require.NoError(t, err)
	require.Nil(t, orgView, "org token cannot claim legacy personal rows")

	personalListOwner, err := repo.ListPipelines(handler.ContextWithTenantScope(ctx, legacyOwnerScope), models.ListPipelinesQuery{})
	require.NoError(t, err)
	require.Equal(t, int64(1), personalListOwner.Total, "personal list for legacy owner sees only its row")

	personalListSub, err := repo.ListPipelines(ctxPersonal, models.ListPipelinesQuery{})
	require.NoError(t, err)
	require.Equal(t, int64(0), personalListSub.Total, "personal list for another subject sees nothing")

	versionsForeign, err := repo.ListPipelineVersions(ctxA, legacyID)
	require.NoError(t, err)
	require.Empty(t, versionsForeign, "org A cannot list versions of a personal pipeline")
	versionsOwner, err := repo.ListPipelineVersions(handler.ContextWithTenantScope(ctx, legacyOwnerScope), legacyID)
	require.NoError(t, err)
	require.Len(t, versionsOwner, 1)
}

// TestTenantMigrationOnPopulatedLegacyDB proves the constraint swap works
// on a database that already carries pipeline_job_specs rows under the old
// global unique constraint: apply everything before the tenant migration,
// insert legacy rows, then run the full set and verify the tenant-aware
// index admits the same key in a second tenant.
func TestTenantMigrationOnPopulatedLegacyDB(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)

	// Apply the pre-tenant migration set exactly once, recording each
	// file the way RunMigrations would so the forward pass below only
	// needs to apply the tenant migration.
	_, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS pipeline_build_schema_migrations (filename TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`)
	require.NoError(t, err)
	entries, err := migrations.FS.ReadDir(".")
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() || nameBatchesAtOrAfterTenant(entry.Name()) {
			continue
		}
		raw, err := migrations.FS.ReadFile(entry.Name())
		require.NoError(t, err)
		_, err = pool.Exec(ctx, string(raw))
		require.NoError(t, err, "pre-tenant migration %s", entry.Name())
		_, err = pool.Exec(ctx, `INSERT INTO pipeline_build_schema_migrations (filename) VALUES ($1) ON CONFLICT (filename) DO NOTHING`, entry.Name())
		require.NoError(t, err, "record pre-tenant migration %s", entry.Name())
	}

	// Legacy spec row under the old global unique constraint.
	legacyRID := "ri.foundry.main.pipeline.legacy"
	legacyOutput := "ri.foundry.main.dataset.legacy-out"
	legacyPub := uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO pipeline_job_specs (id, pipeline_rid, branch_name, output_dataset_rid, output_branch, job_spec_json, inputs, content_hash, published_by)
VALUES ($1,$2,'main',$3,'main','{}'::jsonb,'[]'::jsonb,'legacy-hash',$4)`, uuid.New(), legacyRID, legacyOutput, legacyPub)
	require.NoError(t, err)

	// Forward migration over the populated database.
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("RunMigrations over populated legacy DB: %v", err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("RunMigrations re-run over populated legacy DB: %v", err)
	}

	// Legacy row survived, tenant stays NULL.
	var tenant any
	err = pool.QueryRow(ctx, `SELECT tenant_id FROM pipeline_job_specs WHERE pipeline_rid=$1`, legacyRID).Scan(&tenant)
	require.NoError(t, err)
	require.Nil(t, tenant)

	// The tenant-aware unique index now admits the same key under an org.
	org := uuid.MustParse("aaaaaaaa-0000-0000-0000-000000000001")
	sub := uuid.MustParse("cccccccc-0000-0000-0000-000000000003")
	repo := NewRepositoryFromPool(pool)
	scopeOrg := &handler.TenantScope{TenantID: &org, Subject: sub}
	_, err = repo.PublishJobSpec(handler.ContextWithTenantScope(ctx, scopeOrg), "TRANSFORM", handler.CreateJobSpecRequest{PipelineRID: legacyRID, BranchName: "main", OutputDatasetRIDs: []string{legacyOutput}, LogicPayload: []byte(`{}`)}, sub.String())
	require.NoError(t, err, "same key in an org no longer conflicts with the legacy personal row")

	// ...and the personal-domain lookup still resolves the legacy row
	// for its publisher.
	scopeLegacy := &handler.TenantScope{TenantID: nil, Subject: legacyPub}
	spec, err := repo.Lookup(handler.ContextWithTenantScope(ctx, scopeLegacy), legacyRID, legacyOutput, "main", nil)
	require.NoError(t, err)
	require.NotNil(t, spec)
	require.Equal(t, "legacy-hash", spec.ContentHash)
}

func nameBatchesAtOrAfterTenant(name string) bool {
	target := "20261113100000_tenant_isolation.sql"
	return name >= target
}

func resolveArgsFor(pipelineRID string, sub uuid.UUID) resolver.ResolveBuildArgs {
	return resolver.ResolveBuildArgs{PipelineRID: pipelineRID, BuildBranch: "main", JobSpecFallback: []string{}, RequestedBy: sub.String()}
}

// TestTenantRepairRegressionBundle covers the review repairs on a real
// Postgres 16: restore-version tenant WHERE numbering (draft vs publish),
// personal-domain spec keys across owners, restore cross-tenant denial,
// and the dataset-schema gate (fail closed without org columns, filtered
// with them).
func TestTenantRepairRegressionBundle(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	repo := NewRepositoryFromPool(pool)
	orgA := uuid.MustParse("aaaaaaaa-0000-0000-0000-000000000001")
	sub1 := uuid.MustParse("cccccccc-0000-0000-0000-000000000003")
	sub2 := uuid.MustParse("dddddddd-0000-0000-0000-000000000004")
	scopeA := &handler.TenantScope{TenantID: &orgA, Subject: sub1}
	scopeP1 := &handler.TenantScope{TenantID: nil, Subject: sub1}
	scopeP2 := &handler.TenantScope{TenantID: nil, Subject: sub2}
	ctxA := handler.ContextWithTenantScope(ctx, scopeA)
	ctxP1 := handler.ContextWithTenantScope(ctx, scopeP1)
	ctxP2 := handler.ContextWithTenantScope(ctx, scopeP2)

	// --- restore version: draft path ---------------------------------
	p, err := repo.CreatePipeline(ctxA, models.CreatePipelineRequest{Name: "restore-flow"}, sub1)
	require.NoError(t, err)
	published, err := repo.PublishPipeline(ctxA, p.ID, models.PublishPipelineRequest{}, &sub1)
	require.NoError(t, err)
	require.NotEmpty(t, published.Version.ID)

	restored, err := repo.RestorePipelineVersion(ctxA, p.ID, published.Version.ID, models.RestorePipelineVersionRequest{AsDraft: true}, &sub1)
	require.NoError(t, err, "draft restore under org scope succeeds (tenant placeholder $8)")
	require.Equal(t, p.ID, restored.Pipeline.ID)

	restoredPub, err := repo.RestorePipelineVersion(ctxA, p.ID, published.Version.ID, models.RestorePipelineVersionRequest{AsDraft: false}, &sub1)
	require.NoError(t, err, "publish restore under org scope succeeds (tenant placeholder $9 after active_version_id)")
	require.NotNil(t, restoredPub.Pipeline.ActiveVersionID)
	require.NotEqual(t, published.Version.ID, *restoredPub.Pipeline.ActiveVersionID, "publish restore pinned the restored version")

	// Cross-tenant restore is denied before any write.
	scopeB := &handler.TenantScope{TenantID: uuidPtr(uuid.MustParse("bbbbbbbb-0000-0000-0000-000000000002")), Subject: sub1}
	foreign, err := repo.RestorePipelineVersion(handler.ContextWithTenantScope(ctx, scopeB), p.ID, published.Version.ID, models.RestorePipelineVersionRequest{AsDraft: true}, &sub1)
	require.NoError(t, err)
	require.Nil(t, foreign, "org B restore of org A pipeline is invisible")

	// --- personal-domain spec key across owners ----------------------
	const personalKey = "ri.foundry.main.pipeline.personal-collision"
	const personalOut = "ri.foundry.main.dataset.personal-out"
	specP1, err := repo.PublishJobSpec(ctxP1, "TRANSFORM", handler.CreateJobSpecRequest{PipelineRID: personalKey, BranchName: "main", OutputDatasetRIDs: []string{personalOut}, LogicPayload: []byte(`{"owner":1}`)}, sub1.String())
	require.NoError(t, err)
	specP2, err := repo.PublishJobSpec(ctxP2, "TRANSFORM", handler.CreateJobSpecRequest{PipelineRID: personalKey, BranchName: "main", OutputDatasetRIDs: []string{personalOut}, LogicPayload: []byte(`{"owner":2}`)}, sub2.String())
	require.NoError(t, err, "second personal owner does not conflict")
	var rows int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM pipeline_job_specs WHERE pipeline_rid=$1`, personalKey).Scan(&rows)
	require.NoError(t, err)
	require.Equal(t, 2, rows, "two personal domains keep separate spec rows")
	gotP1, err := repo.Lookup(ctxP1, personalKey, personalOut, "main", nil)
	require.NoError(t, err)
	require.NotNil(t, gotP1)
	require.Equal(t, specP1.ContentHash, gotP1.ContentHash, "owner 1 still resolves its own row (no overwrite)")
	gotP2, err := repo.Lookup(ctxP2, personalKey, personalOut, "main", nil)
	require.NoError(t, err)
	require.NotNil(t, gotP2)
	require.Equal(t, specP2.ContentHash, gotP2.ContentHash)

	// --- OpenBuild with omitted job_spec_fallback ---------------------
	// HTTP requests that omit the optional field hand a nil slice to
	// OpenBuild; it must bind an empty array, not SQL NULL (23502).
	nilFallbackBuild := uuid.New()
	err = repo.OpenBuild(ctxA, resolver.ResolveBuildArgs{PipelineRID: "ri.foundry.main.pipeline.nil-fallback", BuildBranch: "main", RequestedBy: sub1.String()}, nilFallbackBuild)
	require.NoError(t, err, "nil job_spec_fallback binds '{}' instead of NULL")
	var fallback []string
	err = pool.QueryRow(ctx, `SELECT job_spec_fallback FROM builds WHERE id=$1`, nilFallbackBuild).Scan(&fallback)
	require.NoError(t, err)
	require.NotNil(t, fallback)
	require.Empty(t, fallback)

	// --- dataset schema gate ------------------------------------------
	// Pipeline-only database: the dataset tables do not exist, so the
	// capability probe reports false and scoped callers fail closed.
	_, err = repo.ListBranches(ctxA, "ri.foundry.main.dataset.any")
	require.ErrorIs(t, err, handler.ErrDatasetTenantBoundaryUnsupported, "scoped branch listing fails closed without dataset org columns")

	// Add the dataset org columns to a minimal datasets/branches schema
	// and verify the same call now filters by organization.
	_, err = pool.Exec(ctx, `CREATE TABLE datasets (id UUID PRIMARY KEY, rid TEXT UNIQUE, owner_id UUID NOT NULL, org_id UUID)`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `CREATE TABLE dataset_branches (id UUID PRIMARY KEY, dataset_id UUID NOT NULL REFERENCES datasets(id), dataset_rid TEXT NOT NULL, name TEXT NOT NULL, deleted_at TIMESTAMPTZ, head_transaction_id UUID)`)
	require.NoError(t, err)
	fresh := NewRepositoryFromPool(pool) // new instance → re-probes capability
	dsA := uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO datasets (id, rid, owner_id, org_id) VALUES ($1,'ri.foundry.main.dataset.a',$2,$3)`, dsA, sub1, orgA)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO dataset_branches (id, dataset_id, dataset_rid, name) VALUES ($1,$2,'ri.foundry.main.dataset.a','main')`, uuid.New(), dsA)
	require.NoError(t, err)

	branchesA, err := fresh.ListBranches(ctxA, "ri.foundry.main.dataset.a")
	require.NoError(t, err)
	require.Len(t, branchesA, 1, "own org resolves the branch through the filtered join")

	scopeOtherOrg := &handler.TenantScope{TenantID: uuidPtr(uuid.New()), Subject: sub1}
	branchesForeign, err := fresh.ListBranches(handler.ContextWithTenantScope(ctx, scopeOtherOrg), "ri.foundry.main.dataset.a")
	var visibilityErr *handler.DatasetHTTPError
	require.ErrorAs(t, err, &visibilityErr, "foreign org gets a precise 404, not an empty success")
	require.Equal(t, http.StatusNotFound, visibilityErr.Status)
	require.Equal(t, "dataset_not_found", visibilityErr.Code)
	require.Empty(t, branchesForeign)
}

func uuidPtr(v uuid.UUID) *uuid.UUID { return &v }
