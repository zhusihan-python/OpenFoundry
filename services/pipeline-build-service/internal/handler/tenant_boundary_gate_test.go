package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/resolver"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// stubBoundaryVersioning is a versioning port whose tenant-boundary
// capability is toggled explicitly, standing in for the production
// repository (which this package cannot import — the postgres package
// imports this one).
type stubBoundaryVersioning struct {
	supported bool
}

var _ resolver.DatasetVersioningRepository = (*stubBoundaryVersioning)(nil)
var _ datasetBoundaryProber = (*stubBoundaryVersioning)(nil)

func (s *stubBoundaryVersioning) ListBranches(context.Context, string) ([]models.BranchSnapshot, error) {
	return []models.BranchSnapshot{{Name: "main"}}, nil
}
func (s *stubBoundaryVersioning) OpenTransaction(_ context.Context, _, _ string) (string, error) {
	return "ri.foundry.main.transaction." + uuid.NewString(), nil
}
func (s *stubBoundaryVersioning) ViewSchema(context.Context, string, string) (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}
func (s *stubBoundaryVersioning) DatasetTenantBoundarySupported(context.Context) bool {
	return s.supported
}

type stubLockRepo struct{}

func (stubLockRepo) TryAcquire(context.Context, string, uuid.UUID, string) (uuid.UUID, bool, error) {
	return uuid.Nil, true, nil
}
func (stubLockRepo) HasUpstreamInProgress(context.Context, []string, uuid.UUID) (bool, error) {
	return false, nil
}

type stubJobSpecRepo struct{}

func (stubJobSpecRepo) Lookup(_ context.Context, _, output, branch string, _ []string) (*models.JobSpec, error) {
	spec := &models.JobSpec{RID: "ri.foundry.main.job_spec.stub", PipelineRID: "ri.foundry.main.pipeline.x", BranchName: branch, LogicKind: "TRANSFORM", OutputDatasetRIDs: []string{output}}
	return spec, nil
}

type capturingBuildsPort struct {
	openBuilds int
	failWith   error
}

func (c *capturingBuildsPort) OpenBuild(context.Context, resolver.ResolveBuildArgs, uuid.UUID) error {
	c.openBuilds++
	return c.failWith
}
func (c *capturingBuildsPort) PersistResolvedBuild(context.Context, *models.ResolvedBuild) error {
	return nil
}
func (c *capturingBuildsPort) MarkBuildFailed(context.Context, uuid.UUID, string) error { return nil }

var _ BuildRepository = (*capturingBuildsPort)(nil)

// TestCreateBuildRejectsBeforeWriteWhenDatasetBoundaryUnavailable pins the
// split-database contract: a scoped request against a versioning
// repository without tenant columns gets the stable 503
// dataset_tenant_boundary_unsupported with ZERO OpenBuild writes; with the
// columns present the request proceeds to persistence; unrelated
// persistence failures keep surfacing as 5xx.
func TestCreateBuildRejectsBeforeWriteWhenDatasetBoundaryUnavailable(t *testing.T) {
	org := uuid.New()
	restoreAuthoring := SetPipelineAuthoringRepository(&anyPipelineAuthoringRepo{})
	defer restoreAuthoring()
	body := `{"pipeline_rid":"ri.foundry.main.pipeline.` + uuid.NewString() + `","build_branch":"main","output_dataset_rids":["ri.foundry.main.dataset.out"]}`

	scopedReq := func() *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/builds", bytes.NewReader([]byte(body)))
		return r.WithContext(authmw.ContextWithClaims(r.Context(), &authmw.Claims{Sub: uuid.New(), OrgID: &org}))
	}

	t.Run("boundary unavailable rejects before any write", func(t *testing.T) {
		builds := &capturingBuildsPort{}
		restore := SetBuildLifecyclePorts(BuildLifecyclePorts{JobSpecs: stubJobSpecRepo{}, Versioning: &stubBoundaryVersioning{supported: false}, Locks: stubLockRepo{}, Builds: builds})
		defer restore()

		rr := httptest.NewRecorder()
		CreateBuild(rr, scopedReq())
		require.Equal(t, http.StatusServiceUnavailable, rr.Code)
		var payload map[string]string
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&payload))
		require.Equal(t, "dataset_tenant_boundary_unsupported", payload["error"])
		require.Equal(t, 0, builds.openBuilds, "no OpenBuild write may happen")
	})

	t.Run("boundary available proceeds to persistence", func(t *testing.T) {
		builds := &capturingBuildsPort{failWith: context.DeadlineExceeded}
		restore := SetBuildLifecyclePorts(BuildLifecyclePorts{JobSpecs: stubJobSpecRepo{}, Versioning: &stubBoundaryVersioning{supported: true}, Locks: stubLockRepo{}, Builds: builds})
		defer restore()

		rr := httptest.NewRecorder()
		CreateBuild(rr, scopedReq())
		require.Equal(t, http.StatusInternalServerError, rr.Code, "capability present: unrelated failures stay 5xx")
		require.Equal(t, 1, builds.openBuilds)
	})

	t.Run("unscoped legacy request bypasses the capability gate only", func(t *testing.T) {
		builds := &capturingBuildsPort{}
		restore := SetBuildLifecyclePorts(BuildLifecyclePorts{JobSpecs: stubJobSpecRepo{}, Versioning: &stubBoundaryVersioning{supported: false}, Locks: stubLockRepo{}, Builds: builds})
		defer restore()

		rr := httptest.NewRecorder()
		CreateBuild(rr, httptest.NewRequest(http.MethodPost, "/api/v1/builds", bytes.NewReader([]byte(body))))
		require.Equal(t, http.StatusAccepted, rr.Code, "unscoped legacy callers skip the capability probe and complete the stubbed resolution")
		require.Equal(t, 1, builds.openBuilds)
	})
}

// TestDryRunResolveSameBoundary pins the read-side twin: scoped dry-runs
// against a boundary-incapable versioning repository get the same stable
// 503 before any spec or branch work.
func TestDryRunResolveSameBoundary(t *testing.T) {
	org := uuid.New()
	restoreAuthoring := SetPipelineAuthoringRepository(&anyPipelineAuthoringRepo{})
	defer restoreAuthoring()
	restore := SetBuildLifecyclePorts(BuildLifecyclePorts{JobSpecs: stubJobSpecRepo{}, Versioning: &stubBoundaryVersioning{supported: false}, Locks: stubLockRepo{}})
	defer restore()

	r := httptest.NewRequest(http.MethodPost, "/api/v1/dry-run/resolve", bytes.NewReader([]byte(`{"pipeline_rid":"ri.foundry.main.pipeline.`+uuid.NewString()+`","build_branch":"main","output_dataset_rids":["ri.foundry.main.dataset.out"]}`)))
	r = r.WithContext(authmw.ContextWithClaims(r.Context(), &authmw.Claims{Sub: uuid.New(), OrgID: &org}))
	rr := httptest.NewRecorder()
	DryRunResolve(rr, r)
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	var payload map[string]string
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&payload))
	require.Equal(t, "dataset_tenant_boundary_unsupported", payload["error"])
}
