package postgres

import (
	"context"
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"

	sparkpkg "github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/spark"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
	livellogs "github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/logs"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

func orgScope(t *testing.T) (*handler.TenantScope, *handler.TenantScope, *handler.TenantScope) {
	t.Helper()
	orgA := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	orgB := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	sub := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	return &handler.TenantScope{TenantID: &orgA, Subject: sub},
		&handler.TenantScope{TenantID: &orgB, Subject: sub},
		&handler.TenantScope{TenantID: nil, Subject: sub}
}

var pipelineColumns = []string{
	"id", "name", "description", "owner_id", "tenant_id", "dag", "status",
	"pipeline_type", "lifecycle",
	"schedule_config", "retry_policy", "next_run_at",
	"external_config", "incremental_config", "streaming_config", "distributed_config", "compute_profile_id", "project_id",
	"draft_dag", "published_dag", "branch_name",
	"draft_updated_at", "published_at", "active_version_id",
	"proposal_state", "proposal_title", "proposal_description",
	"created_at", "updated_at",
}

func pipelineRow(id, orgTenant, owner uuid.UUID, name string) *pgxmock.Rows {
	now := time.Now().UTC()
	var tenant any
	if orgTenant != uuid.Nil {
		tenant = &orgTenant
	}
	return pgxmock.NewRows(pipelineColumns).AddRow(
		id, name, "", owner, tenant, []byte(`[]`), "draft",
		"BATCH", "DRAFT",
		[]byte(`{}`), []byte(`{"max_attempts":1}`), nil,
		nil, nil, nil, nil, nil, nil,
		[]byte(`[]`), []byte(`null`), "main",
		&now, nil, nil,
		"none", nil, nil,
		now, now,
	)
}

// TestListPipelinesOrgScopingIsTenantFirst proves the list SQL carries the
// caller's tenant as a SQL predicate (same subject, two orgs → two
// disjoint queries) instead of filtering in Go.
func TestListPipelinesOrgScopingIsTenantFirst(t *testing.T) {
	orgA, orgB, _ := orgScope(t)
	idA := uuid.New()
	owner := uuid.New()
	orgAID := *orgA.TenantID
	orgBID := *orgB.TenantID

	mock, repo := newMockRepo(t)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM pipelines")).
		WithArgs("", "", orgAID).
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery("FROM pipelines").
		WithArgs("", "", orgAID, int64(50), int64(0)).
		WillReturnRows(pipelineRow(idA, orgAID, owner, "shared-name"))

	outA, err := repo.ListPipelines(handler.ContextWithTenantScope(context.Background(), orgA), models.ListPipelinesQuery{})
	require.NoError(t, err)
	require.Len(t, outA.Data, 1)
	require.Equal(t, idA, outA.Data[0].ID)
	require.NotNil(t, outA.Data[0].TenantID)
	require.Equal(t, orgAID, *outA.Data[0].TenantID)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM pipelines")).
		WithArgs("", "", orgBID).
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(0)))
	mock.ExpectQuery("FROM pipelines").
		WithArgs("", "", orgBID, int64(50), int64(0)).
		WillReturnRows(pgxmock.NewRows(pipelineColumns))

	outB, err := repo.ListPipelines(handler.ContextWithTenantScope(context.Background(), orgB), models.ListPipelinesQuery{})
	require.NoError(t, err)
	require.Empty(t, outB.Data)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestListPipelinesPersonalScopeOwnerIsolation proves a no-OrgID caller
// only matches tenant-NULL rows owned by the same subject.
func TestListPipelinesPersonalScopeOwnerIsolation(t *testing.T) {
	_, _, personal := orgScope(t)
	mock, repo := newMockRepo(t)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM pipelines")).
		WithArgs("", "", personal.Subject).
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(0)))
	mock.ExpectQuery("tenant_id IS NULL").
		WithArgs("", "", personal.Subject, int64(50), int64(0)).
		WillReturnRows(pgxmock.NewRows(pipelineColumns))
	out, err := repo.ListPipelines(handler.ContextWithTenantScope(context.Background(), personal), models.ListPipelinesQuery{})
	require.NoError(t, err)
	require.Empty(t, out.Data)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestGetPipelineCrossTenantReturnsNil covers the by-id path: a scoped
// lookup that the DB filters out resolves to nil (→ HTTP 404).
func TestGetPipelineCrossTenantReturnsNil(t *testing.T) {
	orgA, _, _ := orgScope(t)
	mock, repo := newMockRepo(t)
	mock.ExpectQuery("FROM pipelines").
		WithArgs(uuid.MustParse("44444444-4444-4444-4444-444444444444"), *orgA.TenantID).
		WillReturnRows(pgxmock.NewRows(pipelineColumns))
	p, err := repo.GetPipeline(handler.ContextWithTenantScope(context.Background(), orgA), uuid.MustParse("44444444-4444-4444-4444-444444444444"))
	require.NoError(t, err)
	require.Nil(t, p)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestCreatePipelinePersistsCallerTenant proves the INSERT stamps the
// verified JWT org, not anything from the request body.
func TestCreatePipelinePersistsCallerTenant(t *testing.T) {
	orgA, _, _ := orgScope(t)
	mock, repo := newMockRepo(t)
	orgAID := *orgA.TenantID
	owner := orgA.Subject
	ctx := handler.ContextWithTenantScope(context.Background(), orgA)
	mock.ExpectQuery("INSERT INTO pipelines").
		WithArgs(pgxmock.AnyArg(), "dual-org", "", owner, orgA.TenantID, pgxmock.AnyArg(), "draft", "BATCH", "DRAFT", "main", pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pipelineRow(uuid.New(), orgAID, owner, "dual-org"))
	mock.ExpectQuery("COALESCE\\(MAX\\(version_number\\)").WithArgs(pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"next"}).AddRow(int64(1)))
	mock.ExpectQuery("INSERT INTO pipeline_versions").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), int64(1), "main", "draft", pgxmock.AnyArg(), "dual-org", "", pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), "Initial draft", pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"id", "pipeline_id", "version_number", "branch_name", "version_kind", "dag", "name", "description", "schedule_config", "retry_policy", "created_by", "created_at", "message", "restored_from_version_id"}).
			AddRow(uuid.New(), uuid.New(), int64(1), "main", "draft", []byte(`[]`), "dual-org", "", []byte(`{}`), []byte(`{}`), &owner, time.Now(), "Initial draft", nil))
	p, err := repo.CreatePipeline(ctx, models.CreatePipelineRequest{Name: "dual-org"}, owner)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NotNil(t, p.TenantID)
	require.Equal(t, orgAID, *p.TenantID)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestGlobalEndpointQueriesCarryTenantBoundary walks the global
// (non-/pipelines/{id}) surfaces: every one of them must embed the tenant
// predicate in SQL when a scope is present.
func TestGlobalEndpointQueriesCarryTenantBoundary(t *testing.T) {
	orgA, _, _ := orgScope(t)
	orgAID := *orgA.TenantID
	ctx := handler.ContextWithTenantScope(context.Background(), orgA)

	t.Run("ListBuilds", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		mock.ExpectQuery("FROM builds").
			WithArgs("", "", "", pgxmock.AnyArg(), pgxmock.AnyArg(), int64(50), orgAID).
			WillReturnRows(pgxmock.NewRows([]string{"id", "rid", "pipeline_rid", "build_branch", "job_spec_fallback", "state", "trigger_kind", "force_build", "abort_policy", "queued_at", "started_at", "finished_at", "error_message", "requested_by", "created_at"}))
		_, err := repo.ListBuilds(ctx, models.ListBuildsQuery{})
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("GetBuild", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		mock.ExpectQuery("FROM builds").
			WithArgs("ri.build.x", orgAID).
			WillReturnRows(pgxmock.NewRows([]string{"id", "rid", "pipeline_rid", "build_branch", "job_spec_fallback", "state", "trigger_kind", "force_build", "abort_policy", "queued_at", "started_at", "finished_at", "error_message", "requested_by", "created_at"}))
		env, err := repo.GetBuild(ctx, "ri.build.x")
		require.NoError(t, err)
		require.Nil(t, env)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("GetJobByBuildParent", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		mock.ExpectQuery(regexp.QuoteMeta("EXISTS (SELECT 1 FROM builds b")).
			WithArgs("ri.job.x", orgAID).
			WillReturnRows(pgxmock.NewRows([]string{"id", "rid", "build_id", "job_spec_rid", "state", "output_transaction_rids", "state_changed_at", "attempt", "stale_skipped", "failure_reason", "output_content_hash", "created_at"}))
		job, err := repo.GetJob(ctx, "ri.job.x")
		require.NoError(t, err)
		require.Nil(t, job)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("HistoryResolvesThroughBuildBoundary", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		mock.ExpectQuery(regexp.QuoteMeta("EXISTS (SELECT 1 FROM builds b")).
			WithArgs("ri.job.x", orgAID).
			WillReturnRows(pgxmock.NewRows([]string{"id"}))
		logs, err := repo.History(ctx, "ri.job.x", newHistoryQuery())
		require.NoError(t, err)
		require.Empty(t, logs)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ListBuildQueueJoinsPipelines", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		mock.ExpectQuery(regexp.QuoteMeta("EXISTS (SELECT 1 FROM pipelines p")).
			WithArgs("", "", pgxmock.AnyArg(), int64(50), int64(0), orgAID).
			WillReturnRows(pgxmock.NewRows([]string{"id", "pipeline_id", "status", "trigger_type", "started_by", "attempt_number", "started_from_node_id", "retry_of_run_id", "execution_context", "node_results", "error_message", "started_at", "finished_at"}))
		_, err := repo.ListBuildQueue(ctx, handler.BuildQueueQuery{})
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("QueueSummaryJoinsPipelines", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		mock.ExpectQuery(regexp.QuoteMeta("EXISTS (SELECT 1 FROM pipelines p")).
			WithArgs(orgAID).
			WillReturnRows(pgxmock.NewRows([]string{"status", "count"}))
		_, err := repo.QueueSummary(ctx)
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("AbortPipelineRunScoped", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		runID := uuid.New()
		mock.ExpectQuery("UPDATE pipeline_runs").
			WithArgs(runID, orgAID).
			WillReturnRows(pgxmock.NewRows([]string{"id", "pipeline_id", "status", "trigger_type", "started_by", "attempt_number", "started_from_node_id", "retry_of_run_id", "execution_context", "node_results", "error_message", "started_at", "finished_at"}))
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs(runID, orgAID).
			WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))
		run, exists, err := repo.AbortPipelineRun(ctx, runID)
		require.NoError(t, err)
		require.Nil(t, run)
		require.False(t, exists)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ListSparkSubmissionsScoped", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		mock.ExpectQuery("FROM pipeline_run_submissions").
			WithArgs(int64(50), orgAID).
			WillReturnRows(pgxmock.NewRows([]string{"pipeline_run_id", "namespace", "spark_app_name", "status", "error_message", "submitted_at", "last_observed_at"}))
		_, err := repo.ListSparkSubmissions(ctx, 50)
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("LookupJobSpecScoped", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		mock.ExpectQuery("FROM pipeline_job_specs").
			WithArgs("ri.pipeline.dup", "ri.dataset.out", "main", orgAID).
			WillReturnRows(pgxmock.NewRows([]string{"rid", "pipeline_rid", "branch_name", "inputs", "output_dataset_rid", "job_spec_json", "content_hash", "logic_payload"}))
		spec, err := repo.Lookup(ctx, "ri.pipeline.dup", "ri.dataset.out", "main", nil)
		require.NoError(t, err)
		require.Nil(t, spec)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestNoScopeKeepsLegacySQL pins the compatibility path: internal callers
// without a scope keep the unfiltered legacy SQL (byte-shape unchanged),
// which is what the no-JWT CSV→Pipeline regression relies on for direct
// handler tests.
func TestNoScopeKeepsLegacySQL(t *testing.T) {
	mock, repo := newMockRepo(t)
	mock.ExpectQuery("FROM pipelines WHERE id=\\$1").
		WithArgs(uuid.MustParse("55555555-5555-5555-5555-555555555555")).
		WillReturnRows(pgxmock.NewRows(pipelineColumns))
	_, err := repo.GetPipeline(context.Background(), uuid.MustParse("55555555-5555-5555-5555-555555555555"))
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func newHistoryQuery() livellogs.Query {
	return livellogs.Query{Limit: 10}
}

// TestIdLookupParenthesizesORForTenantGuard proves the generated SQL
// wraps the id/rid OR in parentheses INSIDE the WHERE clause (AND binds
// tighter than OR; an unparenthesized OR would let the UUID branch bypass
// the tenant comparison).
func TestIdLookupParenthesizesORForTenantGuard(t *testing.T) {
	orgA, _, _ := orgScope(t)
	ctx := handler.ContextWithTenantScope(context.Background(), orgA)
	buildCols := []string{"id", "rid", "pipeline_rid", "build_branch", "job_spec_fallback", "state", "trigger_kind", "force_build", "abort_policy", "queued_at", "started_at", "finished_at", "error_message", "requested_by", "created_at"}

	t.Run("GetBuild", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		mock.ExpectQuery(regexp.QuoteMeta("FROM builds WHERE (id::text=$1 OR rid=$1) AND builds.tenant_id = $2")).
			WithArgs("ri.build.x", *orgA.TenantID).
			WillReturnRows(pgxmock.NewRows(buildCols))
		env, err := repo.GetBuild(ctx, "ri.build.x")
		require.NoError(t, err)
		require.Nil(t, env)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ListJobsForBuildID", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		mock.ExpectQuery(regexp.QuoteMeta("SELECT id FROM builds WHERE (id::text=$1 OR rid=$1) AND builds.tenant_id = $2")).
			WithArgs("ri.build.x", *orgA.TenantID).
			WillReturnRows(pgxmock.NewRows([]string{"id"}))
		jobs, err := repo.ListJobsForBuildID(ctx, "ri.build.x")
		require.NoError(t, err)
		require.Empty(t, jobs)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("GetJob", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		mock.ExpectQuery(regexp.QuoteMeta("FROM jobs WHERE (id::text=$1 OR rid=$1) AND EXISTS (SELECT 1 FROM builds b WHERE b.id = jobs.build_id AND b.tenant_id = $2)")).
			WithArgs("ri.job.x", *orgA.TenantID).
			WillReturnRows(pgxmock.NewRows([]string{"id", "rid", "build_id", "job_spec_rid", "state", "output_transaction_rids", "state_changed_at", "attempt", "stale_skipped", "failure_reason", "output_content_hash", "created_at"}))
		job, err := repo.GetJob(ctx, "ri.job.x")
		require.NoError(t, err)
		require.Nil(t, job)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestSaveSparkSubmissionRejectsForeignRunID pins the domain guard on the
// upsert: a conflict with another tenant's row updates nothing and
// surfaces ErrSparkSubmissionDomainConflict instead of hijacking the row.
func TestSaveSparkSubmissionRejectsForeignRunID(t *testing.T) {
	orgA, orgB, _ := orgScope(t)
	runID := uuid.New()
	sub := handler.SparkSubmission{PipelineRunID: runID, Namespace: "ns", SparkAppName: "app", Status: sparkpkg.SparkRunSubmitted}

	t.Run("own domain upserts", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		mock.ExpectExec("INSERT INTO pipeline_run_submissions").
			WithArgs(runID, "app", "ns", "SUBMITTED", pgxmock.AnyArg(), orgA.TenantID, &orgA.Subject).
			WillReturnResult(pgconn.NewCommandTag("INSERT 0 1"))
		require.NoError(t, repo.SaveSparkSubmission(handler.ContextWithTenantScope(context.Background(), orgA), sub))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("foreign domain conflicts", func(t *testing.T) {
		mock, repo := newMockRepo(t)
		mock.ExpectExec("INSERT INTO pipeline_run_submissions").
			WithArgs(runID, "app", "ns", "SUBMITTED", pgxmock.AnyArg(), orgB.TenantID, &orgB.Subject).
			WillReturnResult(pgconn.NewCommandTag("UPDATE 0"))
		err := repo.SaveSparkSubmission(handler.ContextWithTenantScope(context.Background(), orgB), sub)
		require.ErrorIs(t, err, handler.ErrSparkSubmissionDomainConflict)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestDatasetHelpersFailClosedWithoutOrgColumns proves scoped callers get
// an explicit error when the co-located dataset schema lacks the org
// columns (older schema / probe failure) instead of unfiltered SQL.
func TestDatasetHelpersFailClosedWithoutOrgColumns(t *testing.T) {
	orgA, _, _ := orgScope(t)
	ctx := handler.ContextWithTenantScope(context.Background(), orgA)
	mock, repo := newMockRepo(t)
	mock.ExpectQuery("information_schema.columns").WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))

	_, err := repo.ListBranches(ctx, "ri.foundry.main.dataset.x")
	require.ErrorIs(t, err, handler.ErrDatasetTenantBoundaryUnsupported)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestDatasetHelpersFilterWhenOrgColumnsPresent covers the capable path:
// the branch listing joins datasets and applies the org boundary.
func TestDatasetHelpersFilterWhenOrgColumnsPresent(t *testing.T) {
	orgA, _, _ := orgScope(t)
	ctx := handler.ContextWithTenantScope(context.Background(), orgA)
	mock, repo := newMockRepo(t)
	mock.ExpectQuery("information_schema.columns").WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
	// Explicit dataset-ownership probe (foreign/absent datasets answer
	// a precise 404 instead of an empty-but-legitimate branch list).
	mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS (SELECT 1 FROM datasets d WHERE d.rid=$1 AND d.org_id = $2)")).
		WithArgs("ri.foundry.main.dataset.x", *orgA.TenantID).
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(regexp.QuoteMeta("JOIN datasets d ON d.id = b.dataset_id")).
		WithArgs("ri.foundry.main.dataset.x", *orgA.TenantID).
		WillReturnRows(pgxmock.NewRows([]string{"name", "head"}))
	out, err := repo.ListBranches(ctx, "ri.foundry.main.dataset.x")
	require.NoError(t, err)
	require.Empty(t, out)
	require.NoError(t, mock.ExpectationsWereMet())

	// Invisible dataset: ownership probe reports false → precise 404.
	mock2, repo2 := newMockRepo(t)
	mock2.ExpectQuery("information_schema.columns").WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
	mock2.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS (SELECT 1 FROM datasets d WHERE d.rid=$1 AND d.org_id = $2)")).
		WithArgs("ri.foundry.main.dataset.foreign", *orgA.TenantID).
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))
	_, err = repo2.ListBranches(ctx, "ri.foundry.main.dataset.foreign")
	var httpErr *handler.DatasetHTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusNotFound, httpErr.Status)
	require.Equal(t, "dataset_not_found", httpErr.Code)
	require.NoError(t, mock2.ExpectationsWereMet())
}

// TestRestorePipelineVersionScopesBothPaths pins the placeholder numbering
// of the draft ($8) and publish ($9 after active_version_id $8) tenant
// WHERE clauses — the bug bound the publish tenant comparison to the
// restored version id instead.
func TestRestorePipelineVersionScopesBothPaths(t *testing.T) {
	orgA, _, _ := orgScope(t)
	pipelineID := uuid.New()
	versionID := uuid.New()
	now := time.Now().UTC()

	for _, asDraft := range []bool{true, false} {
		t.Run(map[bool]string{true: "draft", false: "publish"}[asDraft], func(t *testing.T) {
			mock, repo := newMockRepo(t)
			ctx := handler.ContextWithTenantScope(context.Background(), orgA)
			versionCols := []string{"id", "pipeline_id", "version_number", "branch_name", "version_kind", "dag", "name", "description", "schedule_config", "retry_policy", "created_by", "created_at", "message", "restored_from_version_id"}
			mock.ExpectQuery("FROM pipeline_versions").WithArgs(pipelineID, versionID, *orgA.TenantID).WillReturnRows(
				pgxmock.NewRows(versionCols).AddRow(versionID, pipelineID, int64(2), "main", "published", []byte(`[]`), "v", "", []byte(`{}`), []byte(`{}`), nil, now, "", nil))
			mock.ExpectQuery("FROM pipelines WHERE id").WithArgs(pipelineID, *orgA.TenantID).WillReturnRows(pipelineRow(pipelineID, *orgA.TenantID, orgA.Subject, "restore-me"))
			mock.ExpectQuery("COALESCE\\(MAX\\(version_number\\)").WithArgs(pipelineID).WillReturnRows(pgxmock.NewRows([]string{"next"}).AddRow(int64(3)))
			restoredID := uuid.New()
			mock.ExpectQuery("INSERT INTO pipeline_versions").WithArgs(pgxmock.AnyArg(), pipelineID, int64(3), "main", "restored", pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnRows(
				pgxmock.NewRows(versionCols).AddRow(restoredID, pipelineID, int64(3), "main", "restored", []byte(`[]`), "v", "", []byte(`{}`), []byte(`{}`), nil, now, "", &versionID))
			if asDraft {
				mock.ExpectQuery(regexp.QuoteMeta("WHERE id=$1 AND pipelines.tenant_id = $8")).WithArgs(pipelineID, pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), *orgA.TenantID).WillReturnRows(pipelineRow(pipelineID, *orgA.TenantID, orgA.Subject, "restore-me"))
			} else {
				mock.ExpectQuery(regexp.QuoteMeta("WHERE id=$1 AND pipelines.tenant_id = $9")).WithArgs(pipelineID, pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), restoredID, *orgA.TenantID).WillReturnRows(pipelineRow(pipelineID, *orgA.TenantID, orgA.Subject, "restore-me"))
			}
			out, err := repo.RestorePipelineVersion(ctx, pipelineID, versionID, models.RestorePipelineVersionRequest{AsDraft: asDraft}, nil)
			require.NoError(t, err)
			require.NotNil(t, out)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
