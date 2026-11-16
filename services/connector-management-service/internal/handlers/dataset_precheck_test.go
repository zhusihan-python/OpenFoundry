package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
	cmruntime "github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/runtime"
)

func TestCreateSyncJobPrechecksTargetDataset(t *testing.T) {
	user := uuid.New()
	dataset := uuid.New()
	deps := []struct {
		name     string
		getErr   error
		wantCode int
		wantBody string
	}{
		{"not found", cmruntime.ErrDatasetNotFound, http.StatusNotFound, "output dataset not found"},
		{"forbidden", cmruntime.ErrDatasetForbidden, http.StatusForbidden, "not accessible"},
		{"unauthorized", cmruntime.ErrDatasetUnauthorized, http.StatusUnauthorized, "rejected"},
		{"upstream 5xx", &cmruntime.DependencyError{Op: "get_dataset", Status: 500}, http.StatusBadGateway, "status 500"},
		{"upstream timeout", &cmruntime.DependencyError{Op: "get_dataset", Timeout: true}, http.StatusBadGateway, "timed out"},
		{"unconfigured", &cmruntime.DependencyError{Op: "get_dataset", Detail: "url is not configured"}, http.StatusServiceUnavailable, "not configured"},
	}
	for _, tc := range deps {
		t.Run(tc.name, func(t *testing.T) {
			store, source := newSyncTestStore(t, user, json.RawMessage(`{}`))
			store.connections[0].ConnectorType = "postgresql"
			h := newSyncHandlers(store, &fakeCommitPort{getErr: tc.getErr})
			body := `{"source_id":"` + source.ID.String() + `","output_dataset_id":"` + dataset.String() + `"}`
			req := authedReq(http.MethodPost, "/syncs", body, user)
			req.Header.Set("Authorization", "Bearer caller-jwt")
			rec := httptest.NewRecorder()
			h.CreateSyncJob(rec, req)
			require.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tc.wantBody)
			assert.Empty(t, store.syncJobs[source.ID], "rejected bindings must not create sync definitions")
			port := h.DatasetVersioning.(*fakeCommitPort)
			gotIDs, gotAuths := port.gets()
			require.Len(t, gotIDs, 1)
			assert.Equal(t, dataset, gotIDs[0], "the precheck must target the requested dataset")
			require.Len(t, gotAuths, 1)
			assert.Equal(t, "Bearer caller-jwt", gotAuths[0], "precheck must run under the caller JWT")
		})
	}
}

func TestCreateSyncJobFailsClosedWithoutDatasetDependency(t *testing.T) {
	user := uuid.New()
	store, source := newSyncTestStore(t, user, json.RawMessage(`{}`))
	store.connections[0].ConnectorType = "postgresql"
	h := newSyncHandlers(store, nil) // no injected port, no DatasetServiceURL
	body := `{"source_id":"` + source.ID.String() + `","output_dataset_id":"` + uuid.NewString() + `"}`
	req := authedReq(http.MethodPost, "/syncs", body, user)
	rec := httptest.NewRecorder()
	h.CreateSyncJob(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Empty(t, store.syncJobs[source.ID])
}

func TestUpdateSyncJobRetargetPrecheck(t *testing.T) {
	user := uuid.New()
	store, source := newSyncTestStore(t, user, json.RawMessage(`{}`))
	store.connections[0].ConnectorType = "postgresql"
	original := uuid.New()
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: original}, repo.Tenant{OwnerID: user})
	require.NoError(t, err)

	// Retarget to an invisible dataset is rejected and leaves the binding alone.
	h := newSyncHandlers(store, &fakeCommitPort{getErr: cmruntime.ErrDatasetNotFound})
	patch := `{"output_dataset_id":"` + uuid.NewString() + `"}`
	req := withRouteParam(authedReq(http.MethodPatch, "/syncs/"+job.ID.String(), patch, user), "sync_id", job.ID.String())
	rec := httptest.NewRecorder()
	h.UpdateSyncJob(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	got, err := store.GetSyncJob(context.Background(), job.ID, repo.Tenant{OwnerID: user})
	require.NoError(t, err)
	assert.Equal(t, original, got.OutputDatasetID, "rejected retarget must not mutate the binding")

	// Other-field updates precheck the effective (unchanged) target.
	port := &fakeCommitPort{}
	h = newSyncHandlers(store, port)
	patch = `{"schedule_cron":"0 * * * *"}`
	req = withRouteParam(authedReq(http.MethodPatch, "/syncs/"+job.ID.String(), patch, user), "sync_id", job.ID.String())
	rec = httptest.NewRecorder()
	h.UpdateSyncJob(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	ids, _ := port.gets()
	require.Len(t, ids, 1, "the effective target is validated even without a retarget")
	assert.Equal(t, original, ids[0])

	// A visible retarget goes through.
	newTarget := uuid.New()
	patch = `{"output_dataset_id":"` + newTarget.String() + `"}`
	req = withRouteParam(authedReq(http.MethodPatch, "/syncs/"+job.ID.String(), patch, user), "sync_id", job.ID.String())
	rec = httptest.NewRecorder()
	h.UpdateSyncJob(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	got, err = store.GetSyncJob(context.Background(), job.ID, repo.Tenant{OwnerID: user})
	require.NoError(t, err)
	assert.Equal(t, newTarget, got.OutputDatasetID)
}

func TestUpdateSyncJobCrossTenantIsStable404EvenWhenDependencyFails(t *testing.T) {
	user := uuid.New()
	other := uuid.New()
	store, source := newSyncTestStore(t, user, json.RawMessage(`{}`))
	store.connections[0].ConnectorType = "postgresql"
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: user})
	require.NoError(t, err)

	// The dataset dependency is hard-down; the cross-tenant caller must still
	// see a plain 404, never a 502 that implies the job exists.
	h := newSyncHandlers(store, &fakeCommitPort{getErr: &cmruntime.DependencyError{Op: "get_dataset", Status: 500}})
	patch := `{"schedule_cron":"0 * * * *"}`
	req := withRouteParam(authedReq(http.MethodPatch, "/syncs/"+job.ID.String(), patch, other), "sync_id", job.ID.String())
	rec := httptest.NewRecorder()
	h.UpdateSyncJob(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	got, err := store.GetSyncJob(context.Background(), job.ID, repo.Tenant{OwnerID: user})
	require.NoError(t, err)
	assert.Nil(t, got.ScheduleCron, "cross-tenant patch must not mutate")
}

func TestUpdateSyncJobSameOrgNonOwnerWithoutWriteGrantForbidden(t *testing.T) {
	owner := uuid.New()
	member := uuid.New() // same org, no roles, no connections permissions
	org := uuid.New()
	store := newFakeStore(owner)
	source := models.Connection{ID: uuid.New(), Name: "org-src", ConnectorType: "postgresql", Config: json.RawMessage(`{}`), Status: "connected", OwnerID: owner, OrgID: &org}
	store.connections = append(store.connections, source)
	job := models.SyncJob{ID: uuid.New(), SourceID: source.ID, OutputDatasetID: uuid.New(), OrgID: &org}
	store.syncJobs[source.ID] = []models.SyncJob{job}
	h := newSyncHandlers(store, &fakeCommitPort{})

	patch := `{"schedule_cron":"*/5 * * * *"}`
	req := withRouteParam(orgReq(http.MethodPatch, "/syncs/"+job.ID.String(), patch, member, org), "sync_id", job.ID.String())
	rec := httptest.NewRecorder()
	h.UpdateSyncJob(rec, req)
	assert.Equal(t, http.StatusForbidden, rec.Code, "same-org non-owner without connections:write cannot mutate someone else's sync")
	port := h.DatasetVersioning.(*fakeCommitPort)
	ids, _ := port.gets()
	assert.Empty(t, ids, "the grant check runs before any dependency call")

	// The owner in the same org stays able to update.
	req = withRouteParam(orgReq(http.MethodPatch, "/syncs/"+job.ID.String(), patch, owner, org), "sync_id", job.ID.String())
	rec = httptest.NewRecorder()
	h.UpdateSyncJob(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestRunSyncJobPrecheckFailureFailsBeforeFetch(t *testing.T) {
	user := uuid.New()
	upstream := &syncUpstream{bodies: []string{`{"data":[{"i":1}]}`}}
	sourceServer := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer sourceServer.Close()

	store, source := newSyncTestStore(t, user, restSyncSourceConfig(sourceServer.URL))
	storeSourceCredential(t, store, source.ID, "oauth_token", "src-tok")
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: user})
	require.NoError(t, err)
	committer := &fakeCommitPort{getErr: cmruntime.ErrDatasetForbidden}
	h := newSyncHandlers(store, committer)

	run := runSync(t, h, job.ID, user, "Bearer caller-jwt")
	assert.Equal(t, "failed", run.Status)
	require.NotNil(t, run.Error)
	assert.Contains(t, *run.Error, "not accessible")
	assert.Nil(t, run.DatasetVersionID)
	assert.Empty(t, upstream.requests(), "no upstream page may be pulled for an unvalidated target")
	requests, _ := committer.snapshot()
	assert.Empty(t, requests, "no publish may happen for an unvalidated target")
}

// TestRunSyncJobSanitizesQueryCredentialFromPersistedError drives an api_key
// credential placed behind an arbitrary query parameter name against an
// unreachable upstream: neither the persisted run error nor the HTTP response
// may contain the raw or URL-encoded credential.
func TestRunSyncJobSanitizesQueryCredentialFromPersistedError(t *testing.T) {
	user := uuid.New()
	secret := `s3cr3t+API/key==`
	deadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadServer.Close() // unreachable from here on

	cfg := fmt.Sprintf(`{
		"base_url": %q,
		"auth": {"type": "api_key", "query_param": "client_key", "secret_ref": "api_key"},
		"sync": {"resource_path": "/items", "records_path": "data", "pagination": {"type": "page"}, "limits": {"max_pages": 3}}
	}`, deadServer.URL)
	store, source := newSyncTestStore(t, user, json.RawMessage(cfg))
	storeSourceCredential(t, store, source.ID, "api_key", secret)
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: user})
	require.NoError(t, err)
	h := newSyncHandlers(store, &fakeCommitPort{})

	run := runSync(t, h, job.ID, user, "")
	assert.Equal(t, "failed", run.Status)
	require.NotNil(t, run.Error)

	escaped := url.QueryEscape(secret)
	assert.NotContains(t, *run.Error, secret, "run errors must not leak the raw credential")
	assert.NotContains(t, *run.Error, escaped, "run errors must not leak the URL-encoded credential")
	assert.NotContains(t, *run.Error, strings.ReplaceAll(escaped, "+", "%20"))

	// The persisted run record carries the same sanitized text.
	runs := store.runs[job.ID]
	require.Len(t, runs, 1)
	require.NotNil(t, runs[0].Error)
	assert.NotContains(t, *runs[0].Error, secret)
	assert.NotContains(t, *runs[0].Error, escaped)
}
