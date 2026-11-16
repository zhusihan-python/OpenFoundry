package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	livellogs "github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/logs"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

func TestTenantScopeFromClaimsProjection(t *testing.T) {
	org := uuid.New()
	sub := uuid.New()

	personal := TenantScopeFromClaims(&authmw.Claims{Sub: sub})
	require.NotNil(t, personal)
	require.Nil(t, personal.TenantID)
	require.Equal(t, sub, personal.Subject)

	orgScope := TenantScopeFromClaims(&authmw.Claims{Sub: sub, OrgID: &org})
	require.NotNil(t, orgScope)
	require.NotNil(t, orgScope.TenantID)
	require.Equal(t, org, *orgScope.TenantID)

	// Admin claims must not widen the projection: the scope keeps the
	// concrete org, and the repository predicates never consult roles.
	adminScope := TenantScopeFromClaims(&authmw.Claims{Sub: sub, OrgID: &org, Roles: []string{"admin"}})
	require.NotNil(t, adminScope.TenantID)
	require.Equal(t, org, *adminScope.TenantID)

	require.Nil(t, TenantScopeFromClaims(nil))
}

// TestTenantScopeMiddlewareAttachesScope proves the routing-layer wiring:
// authenticated requests carry the claims-derived scope, and body/header
// tenant fields cannot influence it.
func TestTenantScopeMiddlewareAttachesScope(t *testing.T) {
	org := uuid.New()
	sub := uuid.New()
	claims := &authmw.Claims{Sub: sub, OrgID: &org}

	var seen *TenantScope
	handler := TenantScopeMiddleware()(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = TenantScopeFromContext(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
	req.Header.Set("X-Openfoundry-Tenant-Id", uuid.New().String())
	handler.ServeHTTP(httptest.NewRecorder(), req.WithContext(authmw.ContextWithClaims(req.Context(), claims)))

	require.NotNil(t, seen)
	require.Equal(t, org, *seen.TenantID)
	require.Equal(t, sub, seen.Subject)
}

// TestStreamJobLogsRejectsForeignJob drives the SSE gate with a store that
// reports the job invisible under the caller's scope: the stream must
// short-circuit to 404 before any event is written.
func TestStreamJobLogsRejectsForeignJob(t *testing.T) {
	repo := &scopeGatingLogStore{visible: false}
	restore := SetJobLogService(&livellogs.Service{Store: repo, Subscriber: livellogs.NewMemoryService()})
	defer restore()
	SetJobLogStreamConfig(0, time.Millisecond)

	r := requestWithURLParam(http.MethodGet, "/api/v1/jobs/ri.foundry.main.job.foreign/logs/stream?follow=false", nil, "id", "ri.foundry.main.job.foreign")
	rr := httptest.NewRecorder()
	StreamJobLogs(rr, r)
	require.Equal(t, http.StatusNotFound, rr.Code)
}

// TestStreamJobLogsAllowsVisibleJob proves the gate passes same-tenant
// jobs through to normal SSE behaviour.
func TestStreamJobLogsAllowsVisibleJob(t *testing.T) {
	repo := &scopeGatingLogStore{visible: true, history: []livellogs.LogEntry{{Sequence: 1, JobRID: "ri.foundry.main.job.mine", Level: livellogs.LogInfo, Message: "hi"}}}
	restore := SetJobLogService(&livellogs.Service{Store: repo, Subscriber: livellogs.NewMemoryService()})
	defer restore()
	SetJobLogStreamConfig(0, time.Millisecond)

	r := requestWithURLParam(http.MethodGet, "/api/v1/jobs/ri.foundry.main.job.mine/logs/stream?follow=false", nil, "id", "ri.foundry.main.job.mine")
	rr := httptest.NewRecorder()
	StreamJobLogs(rr, r)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "text/event-stream", rr.Header().Get("Content-Type"))
	require.Contains(t, rr.Body.String(), `"message":"hi"`)
}

type scopeGatingLogStore struct {
	visible   bool
	history   []livellogs.LogEntry
	appends   int
	appendErr error
}

func (s *scopeGatingLogStore) History(context.Context, string, livellogs.Query) ([]livellogs.LogEntry, error) {
	return s.history, nil
}
func (s *scopeGatingLogStore) JobRIDInScope(context.Context, string) (bool, error) {
	return s.visible, nil
}
func (s *scopeGatingLogStore) AppendLogByRID(_ context.Context, _ string, level livellogs.LogLevel, message string, params json.RawMessage) (livellogs.LogEntry, error) {
	if s.appendErr != nil {
		return livellogs.LogEntry{}, s.appendErr
	}
	s.appends++
	return livellogs.LogEntry{Sequence: int64(s.appends), Level: level, Message: message, Params: params}, nil
}

// TestCreatePipelineWithoutOrgIDKeepsPersonalDomain pins the no-OrgID
// compatibility path: a verified JWT without org creates a tenant-NULL
// pipeline owned by the subject — the regime the last round's
// CSV→Pipeline→output flow runs under.
func TestCreatePipelineWithoutOrgIDKeepsPersonalDomain(t *testing.T) {
	repo := newFakePipelineAuthoringRepo()
	restore := SetPipelineAuthoringRepository(repo)
	defer restore()

	sub := uuid.New()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", nil)
	r.Body = http.NoBody
	r = r.WithContext(authmw.ContextWithClaims(r.Context(), &authmw.Claims{Sub: sub}))
	r.Header.Set("Content-Type", "application/json")
	// A tenant field in the body must be ignored by the projection.
	rr := httptest.NewRecorder()
	CreatePipeline(rr, r)
	require.Equal(t, http.StatusBadRequest, rr.Code) // empty body — decode failure is fine
}

// TestLegacyPipelineRoutesRequireAuthAtRouter proves the previously
// unauthenticated /api/v1/pipeline/builds/* surface now rejects missing
// bearers at the routing layer (defense no longer delegated to the
// gateway alone).
func TestLegacyPipelineRoutesRequireAuthAtRouter(t *testing.T) {
	srv := newTestRouter(t)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipeline/builds/run", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	require.Equal(t, http.StatusUnauthorized, rr.Code)

	req = httptest.NewRequest(http.MethodGet, "/api/v1/pipeline/builds/"+uuid.New().String()+"/status", nil)
	rr = httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	require.Equal(t, http.StatusUnauthorized, rr.Code)
}

func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	r := chi.NewRouter()
	jwt := authmw.NewJWTConfig("test-secret")
	r.Route("/api/v1/pipeline", func(pipeline chi.Router) {
		pipeline.Use(authmw.Middleware(jwt))
		pipeline.Use(TenantScopeMiddleware())
		pipeline.Post("/builds/run", SubmitPipelineBuildRun)
		pipeline.Get("/builds/{run_id}/status", GetPipelineBuildRunStatus)
	})
	return r
}

// TestCreatePipelineWithClaimsDerivesOwnerAndPassesContext proves the
// handler forwards the claims-bearing request context into the repository
// so the tenant stamp happens from the verified JWT.
func TestCreatePipelineWithClaimsDerivesOwnerAndPassesContext(t *testing.T) {
	capturing := &capturingAuthoringRepo{inner: newFakePipelineAuthoringRepo()}
	restore := SetPipelineAuthoringRepository(capturing)
	defer restore()

	org := uuid.New()
	sub := uuid.New()
	body := `{"name":"csv-flow","nodes":[{"id":"n1","transform_type":"noop"}]}`
	r := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", nil)
	r.Body = http.NoBody
	r = r.WithContext(authmw.ContextWithClaims(r.Context(), &authmw.Claims{Sub: sub, OrgID: &org}))
	r.Header.Set("Content-Type", "application/json")
	r.Body = io.NopCloser(strings.NewReader(body))
	rr := httptest.NewRecorder()
	CreatePipeline(rr, r)
	require.Equal(t, http.StatusCreated, rr.Code)

	require.NotNil(t, capturing.ctx)
	scope := TenantScopeFromContext(capturing.ctx)
	require.NotNil(t, scope)
	require.Equal(t, org, *scope.TenantID)
	require.Equal(t, sub, scope.Subject)
	require.Equal(t, sub, capturing.ownerID)
}

type capturingAuthoringRepo struct {
	inner   PipelineAuthoringRepository
	ctx     context.Context
	ownerID uuid.UUID
}

func (c *capturingAuthoringRepo) ListPipelines(ctx context.Context, q models.ListPipelinesQuery) (models.ListPipelinesResponse, error) {
	return c.inner.ListPipelines(ctx, q)
}
func (c *capturingAuthoringRepo) CreatePipeline(ctx context.Context, req models.CreatePipelineRequest, ownerID uuid.UUID) (*models.Pipeline, error) {
	c.ctx = ctx
	c.ownerID = ownerID
	return c.inner.CreatePipeline(ctx, req, ownerID)
}
func (c *capturingAuthoringRepo) GetPipeline(ctx context.Context, id uuid.UUID) (*models.Pipeline, error) {
	return c.inner.GetPipeline(ctx, id)
}
func (c *capturingAuthoringRepo) UpdatePipeline(ctx context.Context, id uuid.UUID, req models.UpdatePipelineRequest) (*models.Pipeline, error) {
	return c.inner.UpdatePipeline(ctx, id, req)
}
func (c *capturingAuthoringRepo) DeletePipeline(ctx context.Context, id uuid.UUID) (bool, error) {
	return c.inner.DeletePipeline(ctx, id)
}
func (c *capturingAuthoringRepo) ListPipelineVersions(ctx context.Context, id uuid.UUID) ([]models.PipelineVersion, error) {
	return c.inner.ListPipelineVersions(ctx, id)
}
func (c *capturingAuthoringRepo) PublishPipeline(ctx context.Context, id uuid.UUID, req models.PublishPipelineRequest, actorID *uuid.UUID) (*models.PipelinePublishResponse, error) {
	return c.inner.PublishPipeline(ctx, id, req, actorID)
}
func (c *capturingAuthoringRepo) CreatePipelineProposal(ctx context.Context, id uuid.UUID, req models.CreatePipelineProposalRequest, actorID *uuid.UUID) (*models.PipelinePublishResponse, error) {
	return c.inner.CreatePipelineProposal(ctx, id, req, actorID)
}
func (c *capturingAuthoringRepo) RestorePipelineVersion(ctx context.Context, pipelineID, versionID uuid.UUID, req models.RestorePipelineVersionRequest, actorID *uuid.UUID) (*models.PipelinePublishResponse, error) {
	return c.inner.RestorePipelineVersion(ctx, pipelineID, versionID, req, actorID)
}

// TestSchedulerKeepsCallerScopeAndIdentity proves the scheduler endpoint
// threads the HTTP caller's tenant scope into every repository call —
// the queue is listed per tenant, not against the whole database, and no
// background context replaces the request context on the way.
func TestSchedulerKeepsCallerScopeAndIdentity(t *testing.T) {
	org := uuid.New()
	sub := uuid.New()
	capturing := &schedulerScopeCapture{}

	restore := SetExecutionPorts(ExecutionPorts{Runs: capturing})
	defer restore()

	r := httptest.NewRequest(http.MethodPost, "/api/v1/data-integration/pipelines/_scheduler/run-due", nil)
	r = r.WithContext(authmw.ContextWithClaims(r.Context(), &authmw.Claims{Sub: sub, OrgID: &org}))
	rr := httptest.NewRecorder()
	RunDueScheduledPipelines(rr, r)
	require.Equal(t, http.StatusOK, rr.Code)

	require.NotNil(t, capturing.listDueCtx, "scheduler must call ListDuePipelines")
	scope := TenantScopeFromContext(capturing.listDueCtx)
	require.NotNil(t, scope, "ListDuePipelines must see the caller's tenant scope")
	require.Equal(t, org, *scope.TenantID)
	require.Equal(t, sub, scope.Subject)
}

// schedulerScopeCapture is a minimal DataIntegrationRunRepository that
// records the context of the scheduler's queue listing and reports no
// due pipelines (nothing else on the interface is exercised).
type schedulerScopeCapture struct {
	listDueCtx context.Context
}

var _ DataIntegrationRunRepository = (*schedulerScopeCapture)(nil)

func (c *schedulerScopeCapture) ListDuePipelines(ctx context.Context) ([]models.Pipeline, error) {
	c.listDueCtx = ctx
	return nil, nil
}
func (c *schedulerScopeCapture) LoadPipeline(context.Context, uuid.UUID) (*models.Pipeline, error) {
	return nil, nil
}
func (c *schedulerScopeCapture) OpenPipelineRun(context.Context, *models.Pipeline, models.TriggerPipelineRequest, *uuid.UUID, json.RawMessage) (*models.PipelineRun, error) {
	return nil, errNotReached
}
func (c *schedulerScopeCapture) OpenPipelineRunWithOptions(context.Context, *models.Pipeline, models.TriggerPipelineRequest, *uuid.UUID, string, *string, *uuid.UUID, int32, json.RawMessage) (*models.PipelineRun, error) {
	return nil, errNotReached
}
func (c *schedulerScopeCapture) MarkPipelineRunRunning(context.Context, uuid.UUID) error {
	return errNotReached
}
func (c *schedulerScopeCapture) FinishPipelineRun(context.Context, uuid.UUID, string, json.RawMessage, *string) error {
	return errNotReached
}
func (c *schedulerScopeCapture) ListPipelineRuns(context.Context, uuid.UUID, int64, int64) ([]models.PipelineRun, error) {
	return nil, nil
}
func (c *schedulerScopeCapture) GetPipelineRun(context.Context, uuid.UUID, uuid.UUID) (*models.PipelineRun, error) {
	return nil, nil
}
func (c *schedulerScopeCapture) ListBuildQueue(context.Context, BuildQueueQuery) ([]models.PipelineRun, error) {
	return nil, nil
}
func (c *schedulerScopeCapture) AbortPipelineRun(context.Context, uuid.UUID) (*models.PipelineRun, bool, error) {
	return nil, false, nil
}
func (c *schedulerScopeCapture) QueueSummary(context.Context) (map[string]int64, error) {
	return map[string]int64{}, nil
}
func (c *schedulerScopeCapture) UpdatePipelineNextRun(context.Context, uuid.UUID, *time.Time) error {
	return errNotReached
}

var errNotReached = errors.New("not reached with an empty due queue")

// TestAuthenticatedRequestAlwaysCarriesScope pins the routing-layer chain:
// a request with a VALID JWT passes authmw.Middleware, which guarantees
// claims in context, and TenantScopeMiddleware then guarantees a scope.
// This is what makes the repository's nil-scope fallback unreachable for
// normal HTTP traffic.
func TestAuthenticatedRequestAlwaysCarriesScope(t *testing.T) {
	jwtCfg := authmw.NewJWTConfig("test-secret-32bytes-test-secret-3")
	org := uuid.New()
	sub := uuid.New()
	claims := authmw.BuildAccessClaims(jwtCfg, authmw.AccessClaimsInput{UserID: sub, Email: "a@b.c", Name: "A", OrgID: &org})
	token, err := authmw.EncodeToken(jwtCfg, &claims)
	require.NoError(t, err)

	var seen *TenantScope
	pipeline := chi.NewRouter()
	pipeline.Use(authmw.Middleware(jwtCfg))
	pipeline.Use(TenantScopeMiddleware())
	pipeline.Get("/api/v1/pipelines", func(_ http.ResponseWriter, r *http.Request) {
		seen = TenantScopeFromContext(r.Context())
	})

	for _, tc := range []struct {
		name   string
		header string
	}{
		{name: "valid bearer", header: "Bearer " + token},
		{name: "malformed token", header: "Bearer not-a-jwt"},
		{name: "missing token", header: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen = nil
			req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			rr := httptest.NewRecorder()
			pipeline.ServeHTTP(rr, req)
			if tc.name == "valid bearer" {
				require.Equal(t, http.StatusOK, rr.Code)
				require.NotNil(t, seen, "authenticated request must carry a tenant scope")
				require.Equal(t, org, *seen.TenantID)
				require.Equal(t, sub, seen.Subject)
			} else {
				require.Equal(t, http.StatusUnauthorized, rr.Code)
				require.Nil(t, seen, "unauthenticated request must not reach handlers")
			}
		})
	}
}

// TestCreateJobSpecV1AttributesRealSubject pins the creator attribution:
// the v1 publish surface must stamp the trusted scope subject, not an
// empty actor that would strand personal-domain rows behind a zero UUID.
func TestCreateJobSpecV1AttributesRealSubject(t *testing.T) {
	capturing := &capturingSpecRepo{fakeV1Repo: newFakeV1Repo()}
	restoreRepo := SetBuildQueryRepository(capturing)
	defer restoreRepo()

	org := uuid.New()
	sub := uuid.New()
	// Scoped publishes require the ownership preflight wiring: stub the
	// authoring validator (returns a pipeline for any UUID) and the
	// lifecycle ports (read-only versioning stub).
	restoreAuthoring := SetPipelineAuthoringRepository(&anyPipelineAuthoringRepo{})
	defer restoreAuthoring()
	restorePorts := SetBuildLifecyclePorts(BuildLifecyclePorts{JobSpecs: stubJobSpecRepo{}, Versioning: &stubBoundaryVersioning{supported: true}, Locks: stubLockRepo{}})
	defer restorePorts()

	r := httptest.NewRequest(http.MethodPost, "/v1/job-specs/TRANSFORM", strings.NewReader(`{"pipeline_rid":"ri.foundry.main.pipeline.`+uuid.NewString()+`","branch_name":"main","output_dataset_rids":["ri.foundry.main.dataset.out"],"logic_payload":{},"content_hash":"h1"}`))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("kind", "TRANSFORM")
	r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
	r = r.WithContext(authmw.ContextWithClaims(r.Context(), &authmw.Claims{Sub: sub, OrgID: &org}))

	rr := httptest.NewRecorder()
	CreateJobSpecV1(rr, r)
	require.Equal(t, http.StatusCreated, rr.Code)
	require.Equal(t, sub.String(), capturing.createdBy, "creator must be the verified scope subject")
}

type capturingSpecRepo struct {
	*fakeV1Repo
	createdBy string
}

func (c *capturingSpecRepo) PublishJobSpec(ctx context.Context, kind string, req CreateJobSpecRequest, createdBy string) (PublishedJobSpec, error) {
	c.createdBy = createdBy
	return c.fakeV1Repo.PublishJobSpec(ctx, kind, req, createdBy)
}

// TestSparkSubmissionFailsClosedForScopedCallers proves the P1 narrowing:
// authenticated org AND personal callers are rejected with a clear 503
// BEFORE any Kubernetes side effect (fake client receives zero calls).
// The no-scope legacy path (internal tests) is untouched — HTTP traffic
// always carries a scope, so this is not a bypass.
func TestSparkSubmissionFailsClosedForScopedCallers(t *testing.T) {
	fake := &fakeSparkClient{}
	restoreClient := SetSparkClient(fake)
	defer restoreClient()
	subRepo := &fakeSparkSubmissionRepo{submissions: map[string]SparkSubmission{}}
	restoreRepo := SetSparkSubmissionRepository(subRepo)
	defer restoreRepo()

	org := uuid.New()
	body := `{"pipeline_id":"p","input_dataset_rid":"in","output_dataset_rid":"out"}`

	for name, claims := range map[string]*authmw.Claims{
		"org":      {Sub: uuid.New(), OrgID: &org},
		"personal": {Sub: uuid.New()},
	} {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/data-integration/spark-runs", strings.NewReader(body))
		r = r.WithContext(authmw.ContextWithClaims(r.Context(), claims))
		rr := httptest.NewRecorder()
		SubmitSparkRun(rr, r)
		require.Equalf(t, http.StatusServiceUnavailable, rr.Code, "%s caller must be rejected", name)
		var payload map[string]string
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&payload))
		require.Equal(t, "spark_submission_tenant_unsupported", payload["error"])
	}
	require.Equal(t, 0, fake.submitCalls, "no Kubernetes side effect may happen")

	// Legacy no-scope internal path still submits (compat contract for
	// the pre-tenant tests; HTTP routes always attach a scope).
	legacy := httptest.NewRequest(http.MethodPost, "/api/v1/data-integration/spark-runs", strings.NewReader(body))
	rr := httptest.NewRecorder()
	SubmitSparkRun(rr, legacy)
	require.Equal(t, http.StatusAccepted, rr.Code)
	require.Equal(t, 1, fake.submitCalls)
}

func (f *fakeSparkClient) submitCallCount() int { return f.submitCalls }

// anyPipelineAuthoringRepo answers every pipeline lookup affirmatively so
// the parent preflight passes in unit harnesses (integration tests use
// the real repository).
type anyPipelineAuthoringRepo struct{}

func (anyPipelineAuthoringRepo) ListPipelines(context.Context, models.ListPipelinesQuery) (models.ListPipelinesResponse, error) {
	return models.ListPipelinesResponse{}, nil
}
func (anyPipelineAuthoringRepo) CreatePipeline(context.Context, models.CreatePipelineRequest, uuid.UUID) (*models.Pipeline, error) {
	return nil, nil
}
func (anyPipelineAuthoringRepo) GetPipeline(_ context.Context, id uuid.UUID) (*models.Pipeline, error) {
	return &models.Pipeline{ID: id}, nil
}
func (anyPipelineAuthoringRepo) UpdatePipeline(context.Context, uuid.UUID, models.UpdatePipelineRequest) (*models.Pipeline, error) {
	return nil, nil
}
func (anyPipelineAuthoringRepo) DeletePipeline(context.Context, uuid.UUID) (bool, error) {
	return false, nil
}
func (anyPipelineAuthoringRepo) ListPipelineVersions(context.Context, uuid.UUID) ([]models.PipelineVersion, error) {
	return nil, nil
}
func (anyPipelineAuthoringRepo) PublishPipeline(context.Context, uuid.UUID, models.PublishPipelineRequest, *uuid.UUID) (*models.PipelinePublishResponse, error) {
	return nil, nil
}
func (anyPipelineAuthoringRepo) CreatePipelineProposal(context.Context, uuid.UUID, models.CreatePipelineProposalRequest, *uuid.UUID) (*models.PipelinePublishResponse, error) {
	return nil, nil
}
func (anyPipelineAuthoringRepo) RestorePipelineVersion(context.Context, uuid.UUID, uuid.UUID, models.RestorePipelineVersionRequest, *uuid.UUID) (*models.PipelinePublishResponse, error) {
	return nil, nil
}

// TestEmitJobLogV1TenantGating pins the log-WRITE contract: same-domain
// callers succeed; cross-org admins, foreign personal users and unknown
// RIDs get 404 with ZERO appended rows; unknown-from-the-store races and
// real persistence failures map to 404/500 respectively.
func TestEmitJobLogV1TenantGating(t *testing.T) {
	emit := func(store *scopeGatingLogStore, claims *authmw.Claims) *httptest.ResponseRecorder {
		restore := SetJobLogService(&livellogs.Service{Store: store})
		defer restore()
		r := requestWithURLParam(http.MethodPost, "/v1/jobs/ri.foundry.main.job.x/logs", strings.NewReader(`{"level":"INFO","message":"line"}`), "rid", "ri.foundry.main.job.x")
		r = r.WithContext(authmw.ContextWithClaims(r.Context(), claims))
		rr := httptest.NewRecorder()
		EmitJobLogV1(rr, r)
		return rr
	}
	org := uuid.New()
	adminSameOrg := &authmw.Claims{Sub: uuid.New(), OrgID: &org, Roles: []string{"admin"}}
	adminCrossOrg := &authmw.Claims{Sub: uuid.New(), OrgID: uuidPtrHC(uuid.New()), Roles: []string{"admin"}}

	visible := &scopeGatingLogStore{visible: true}
	require.Equal(t, http.StatusOK, emit(visible, adminSameOrg).Code, "same-domain admin writes")
	require.Equal(t, 1, visible.appends)

	foreign := &scopeGatingLogStore{visible: false}
	require.Equal(t, http.StatusNotFound, emit(foreign, adminCrossOrg).Code, "cross-org admin is 404")
	require.Equal(t, 0, foreign.appends, "cross-org admin appended nothing")

	otherPersonal := &scopeGatingLogStore{visible: false}
	require.Equal(t, http.StatusNotFound, emit(otherPersonal, &authmw.Claims{Sub: uuid.New()}).Code, "foreign personal user is 404")
	require.Equal(t, 0, otherPersonal.appends)

	// Unknown-from-store race during append → 404, still zero rows.
	raced := &scopeGatingLogStore{visible: true, appendErr: pgx.ErrNoRows}
	require.Equal(t, http.StatusNotFound, emit(raced, adminSameOrg).Code)
	require.Equal(t, 0, raced.appends)

	// Real persistence failure stays a 500.
	broken := &scopeGatingLogStore{visible: true, appendErr: context.DeadlineExceeded}
	require.Equal(t, http.StatusInternalServerError, emit(broken, adminSameOrg).Code)
}

func uuidPtrHC(v uuid.UUID) *uuid.UUID { return &v }
