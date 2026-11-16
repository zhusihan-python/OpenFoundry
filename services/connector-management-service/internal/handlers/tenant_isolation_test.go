package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

func orgClaims(sub uuid.UUID, org uuid.UUID, roles ...string) *authmw.Claims {
	return &authmw.Claims{Sub: sub, OrgID: &org, Roles: roles}
}

func orgReq(method, target, body string, sub, org uuid.UUID, roles ...string) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	ctx := authmw.ContextWithClaims(req.Context(), orgClaims(sub, org, roles...))
	return req.WithContext(ctx)
}

// createSourceInOrg drives the CreateConnection handler under the given claims.
func createSourceInOrg(t *testing.T, h *handlers.Handlers, sub, org uuid.UUID, name string) models.Connection {
	t.Helper()
	body := `{"name":` + strconv.Quote(name) + `,"connector_type":"postgresql","config":{}}`
	req := orgReq(http.MethodPost, "/sources", body, sub, org)
	rec := httptest.NewRecorder()
	h.CreateConnection(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var created models.Connection
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	require.NotNil(t, created.OrgID)
	assert.Equal(t, org, *created.OrgID)
	return created
}

func TestSameSubjectTwoOrgsIsolation(t *testing.T) {
	user := uuid.New()
	orgA, orgB := uuid.New(), uuid.New()
	store := newFakeStore(user)
	// The fixture row is a legacy personal row of `user`.
	personal := store.connections[0]
	h := &handlers.Handlers{Repo: store}

	sourceA := createSourceInOrg(t, h, user, orgA, "orders-db")
	sourceB := createSourceInOrg(t, h, user, orgB, "orders-db")

	// Org A list sees only its own source — not Org B's, not the personal row.
	req := orgReq(http.MethodGet, "/sources", "", user, orgA)
	rec := httptest.NewRecorder()
	h.ListConnections(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var list struct {
		Items []models.Connection `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
	require.Len(t, list.Items, 1)
	assert.Equal(t, sourceA.ID, list.Items[0].ID)

	// Same subject in Org B cannot read, update or delete Org A's source.
	for _, tc := range []struct {
		name   string
		run    func(req *http.Request) *httptest.ResponseRecorder
		method string
	}{
		{"get", func(req *http.Request) *httptest.ResponseRecorder {
			rec := httptest.NewRecorder()
			h.GetConnection(rec, req)
			return rec
		}, http.MethodGet},
		{"patch", func(req *http.Request) *httptest.ResponseRecorder {
			rec := httptest.NewRecorder()
			h.UpdateConnection(rec, req)
			return rec
		}, http.MethodPatch},
		{"delete", func(req *http.Request) *httptest.ResponseRecorder {
			rec := httptest.NewRecorder()
			h.DeleteConnection(rec, req)
			return rec
		}, http.MethodDelete},
	} {
		req := withRouteParam(orgReq(tc.method, "/sources/"+sourceA.ID.String(), `{"name":"x"}`, user, orgB), "id", sourceA.ID.String())
		rec := tc.run(req)
		assert.Equal(t, http.StatusNotFound, rec.Code, tc.name+" must not leak cross-org sources")
	}

	// Sync definitions: create under Org A, then cross-org access is denied.
	h.DatasetVersioning = &fakeCommitPort{}
	syncBody := `{"source_id":"` + sourceA.ID.String() + `","output_dataset_id":"` + uuid.NewString() + `"}`
	req = orgReq(http.MethodPost, "/syncs", syncBody, user, orgA)
	rec = httptest.NewRecorder()
	h.CreateSyncJob(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var job models.SyncJob
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &job))
	require.NotNil(t, job.OrgID)
	assert.Equal(t, orgA, *job.OrgID)

	req = withRouteParam(orgReq(http.MethodGet, "/syncs/"+job.ID.String(), "", user, orgB), "sync_id", job.ID.String())
	rec = httptest.NewRecorder()
	h.GetSyncJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code, "same subject in another org cannot read the sync def")

	req = withRouteParam(orgReq(http.MethodPost, "/syncs/"+job.ID.String()+"/run", "", user, orgB), "sync_id", job.ID.String())
	rec = httptest.NewRecorder()
	h.RunSyncJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code, "cross-org trigger is denied before any run is created")
	assert.Empty(t, store.runs[job.ID], "no run record may be created for a cross-org trigger")

	// Sync runs are readable only inside their org.
	store.runs[job.ID] = append(store.runs[job.ID], models.SyncRun{ID: uuid.New(), SyncDefID: job.ID, Status: "succeeded", OrgID: &orgA})
	req = withRouteParam(orgReq(http.MethodGet, "/syncs/"+job.ID.String()+"/runs", "", user, orgB), "sync_id", job.ID.String())
	rec = httptest.NewRecorder()
	h.ListRuns(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Credentials follow the source's tenant.
	req = withRouteParam(orgReq(http.MethodGet, "/sources/"+sourceA.ID.String()+"/credentials", "", user, orgB), "id", sourceA.ID.String())
	rec = httptest.NewRecorder()
	h.ListCredentials(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// The legacy personal row stays invisible to org-context callers.
	req = withRouteParam(orgReq(http.MethodGet, "/sources/"+personal.ID.String(), "", user, orgA), "id", personal.ID.String())
	rec = httptest.NewRecorder()
	h.GetConnection(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code, "org requests must not claim legacy NULL-org rows")
	_ = sourceB
}

func TestOrgAdminCannotCrossOrgs(t *testing.T) {
	owner := uuid.New()
	admin := uuid.New()
	orgA, orgB := uuid.New(), uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}

	orgBSource := models.Connection{ID: uuid.New(), Name: "b-only", ConnectorType: "postgresql", Config: json.RawMessage(`{}`), Status: "connected", OwnerID: owner, OrgID: &orgB, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	store.connections = append(store.connections, orgBSource)
	job := models.SyncJob{ID: uuid.New(), SourceID: orgBSource.ID, OutputDatasetID: uuid.New(), OrgID: &orgB, CreatedAt: time.Now().UTC()}
	store.syncJobs[orgBSource.ID] = []models.SyncJob{job}
	store.runs[job.ID] = []models.SyncRun{{ID: uuid.New(), SyncDefID: job.ID, Status: "succeeded", OrgID: &orgB, StartedAt: time.Now().UTC()}}

	// Org A admin: tenant predicate excludes Org B rows before any grant applies.
	req := withRouteParam(orgReq(http.MethodGet, "/sources/"+orgBSource.ID.String(), "", admin, orgA, "admin"), "id", orgBSource.ID.String())
	rec := httptest.NewRecorder()
	h.GetConnection(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code, "org admin must not read another org's source")

	req = orgReq(http.MethodGet, "/sources", "", admin, orgA, "admin")
	rec = httptest.NewRecorder()
	h.ListConnections(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var list struct {
		Items []models.Connection `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
	assert.Empty(t, list.Items, "org A admin list must not include org B sources")

	req = withRouteParam(orgReq(http.MethodGet, "/syncs/"+job.ID.String()+"/runs", "", admin, orgA, "admin"), "sync_id", job.ID.String())
	rec = httptest.NewRecorder()
	h.ListRuns(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code, "org A admin cannot read org B sync runs")

	req = withRouteParam(orgReq(http.MethodPost, "/syncs/"+job.ID.String()+"/run", "", admin, orgA, "admin"), "sync_id", job.ID.String())
	rec = httptest.NewRecorder()
	h.RunSyncJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code, "org A admin cannot trigger org B syncs")
}

func TestPersonalDomainLegacyRowsOnlyVisibleToOwner(t *testing.T) {
	owner := uuid.New()
	other := uuid.New()
	store := newFakeStore(owner) // fixture row: NULL org, owned by owner
	legacy := store.connections[0]
	h := &handlers.Handlers{Repo: store}

	// Another personal-domain user cannot see the legacy row.
	req := withRouteParam(authedReq(http.MethodGet, "/sources/"+legacy.ID.String(), "", other), "id", legacy.ID.String())
	rec := httptest.NewRecorder()
	h.GetConnection(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// The owner (personal context) still can — P1 compatibility.
	req = withRouteParam(authedReq(http.MethodGet, "/sources/"+legacy.ID.String(), "", owner), "id", legacy.ID.String())
	rec = httptest.NewRecorder()
	h.GetConnection(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// An org-scoped admin sees nothing personal.
	org := uuid.New()
	req = withRouteParam(orgReq(http.MethodGet, "/sources/"+legacy.ID.String(), "", uuid.New(), org, "admin"), "id", legacy.ID.String())
	rec = httptest.NewRecorder()
	h.GetConnection(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestEgressConfigSurvivesSourceNormalization(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}

	full := `{
		"base_url": "http://fixture.local",
		"auth": {"type": "none"},
		"egress": {
			"allow_insecure_http": true,
			"allow_private_networks": false,
			"allowed_hosts": ["fixture.local", "fixture.local", "api.example.com"],
			"blocked_hosts": ["blocked.example.com"]
		}
	}`
	req := authedReq(http.MethodPost, "/sources", `{"name":"egress-src","connector_type":"rest_api","config":`+full+`}`, owner)
	rec := httptest.NewRecorder()
	h.CreateConnection(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var created models.Connection
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	assert.Contains(t, string(created.Config), `"allow_private_networks":false`, "explicit false must round-trip, not collapse to absent")

	parsed, err := models.ParseRESTAPISourceConfig(created.Config)
	require.NoError(t, err)
	require.NotNil(t, parsed.Egress)
	require.NotNil(t, parsed.Egress.AllowInsecureHTTP)
	assert.True(t, *parsed.Egress.AllowInsecureHTTP)
	require.NotNil(t, parsed.Egress.AllowPrivateNetworks)
	assert.False(t, *parsed.Egress.AllowPrivateNetworks)
	assert.Equal(t, []string{"api.example.com", "fixture.local"}, parsed.Egress.AllowedHosts, "host lists trim+dedupe+sort")
	assert.Equal(t, []string{"blocked.example.com"}, parsed.Egress.BlockedHosts)

	// Absent block stays absent so service defaults apply.
	req = authedReq(http.MethodPost, "/sources", `{"name":"plain-src","connector_type":"rest_api","config":{"base_url":"http://plain.local","auth":{"type":"none"}}}`, owner)
	rec = httptest.NewRecorder()
	h.CreateConnection(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	json.Unmarshal(rec.Body.Bytes(), &created)
	parsed, err = models.ParseRESTAPISourceConfig(created.Config)
	require.NoError(t, err)
	assert.Nil(t, parsed.Egress, "no egress block must remain no override")
}

func TestSyncConfigSurvivesSourceNormalization(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}

	config := `{
		"base_url": "https://api.example.com",
		"auth": {"type": "none"},
		"sync": {
			"resource_path": "/v2/items",
			"records_path": "payload.rows",
			"pagination": {"type": "cursor", "cursor_param": "after", "next_cursor_path": "links.next"},
			"limits": {"max_pages": 25, "max_records": 5000, "request_timeout_ms": 5000}
		}
	}`
	req := authedReq(http.MethodPost, "/sources", `{"name":"api","connector_type":"rest_api","config":`+config+`}`, owner)
	req = withRouteParam(req, "id", "")
	rec := httptest.NewRecorder()
	h.CreateConnection(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var created models.Connection
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))

	parsed, err := models.ParseRESTAPISourceConfig(created.Config)
	require.NoError(t, err)
	require.NotNil(t, parsed.Sync, "the sync block must survive NormalizeRESTAPISourceConfig")
	assert.Equal(t, "/v2/items", parsed.Sync.ResourcePath)
	assert.Equal(t, "payload.rows", parsed.Sync.RecordsPath)
	assert.Equal(t, models.RESTAPISyncPaginationCursor, parsed.Sync.Pagination.Type)
	assert.Equal(t, "after", parsed.Sync.Pagination.CursorParam)
	assert.Equal(t, "links.next", parsed.Sync.Pagination.NextCursorPath)
	assert.Equal(t, 25, parsed.Sync.Limits.MaxPages)
	assert.Equal(t, 5000, parsed.Sync.Limits.MaxRecords)
	assert.Equal(t, 5000, parsed.Sync.Limits.RequestTimeoutMS)
}
