package handlers_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/adapters"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
)

func TestConnectionJSONShape(t *testing.T) {
	t.Parallel()
	c := models.Connection{
		ID: uuid.New(), Name: "snowflake-prod",
		ConnectorType: "snowflake",
		Config:        json.RawMessage(`{"account":"x"}`),
		Status:        "disconnected",
		OwnerID:       uuid.New(),
		CreatedAt:     time.Date(2026, 5, 6, 0, 0, 0, 0, time.UTC),
		UpdatedAt:     time.Date(2026, 5, 6, 0, 0, 0, 0, time.UTC),
	}
	out, err := json.Marshal(c)
	require.NoError(t, err)
	var view map[string]any
	require.NoError(t, json.Unmarshal(out, &view))
	for _, k := range []string{
		"id", "name", "connector_type", "config", "status",
		"owner_id", "last_sync_at", "created_at", "updated_at",
	} {
		assert.Contains(t, view, k)
	}
}

func TestCreateConnectionRequiresAuth(t *testing.T) {
	t.Parallel()
	h := &handlers.Handlers{}
	req := httptest.NewRequest("POST", "/connections",
		strings.NewReader(`{"name":"x","connector_type":"y"}`))
	rec := httptest.NewRecorder()
	h.CreateConnection(rec, req)
	assert.Equal(t, 401, rec.Code)
}

func TestCreateConnectionRejectsEmptyFields(t *testing.T) {
	t.Parallel()
	h := &handlers.Handlers{}
	c := &authmw.Claims{Sub: uuid.New()}
	req := httptest.NewRequest("POST", "/connections",
		strings.NewReader(`{"name":"","connector_type":""}`))
	req = req.WithContext(authmw.ContextWithClaims(context.Background(), c))
	rec := httptest.NewRecorder()
	h.CreateConnection(rec, req)
	assert.Equal(t, 400, rec.Code)
}

func TestCreateRestAPISourceNormalizesOutboundWebhookModel(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}
	req := httptest.NewRequest(http.MethodPost, "/sources", strings.NewReader(`{
		"name":"Open-Meteo",
		"connector_type":"rest_api",
		"config":{
			"domain":"api.open-meteo.com",
			"auth":{"type":"none"},
			"runtime":{"worker":"foundry","timeout_ms":15000,"allowed_methods":["get","post"]},
			"permissions":{"invokable":true},
			"webhook":{"path":"/v1/forecast"}
		}
	}`))
	req = req.WithContext(authmw.ContextWithClaims(context.Background(), &authmw.Claims{Sub: owner}))
	rec := httptest.NewRecorder()

	h.CreateConnection(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	var body models.Connection
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	var cfg models.RESTAPISourceConfig
	require.NoError(t, json.Unmarshal(body.Config, &cfg))
	require.Equal(t, "Open-Meteo", body.Name)
	require.Equal(t, "rest_api", body.ConnectorType)
	require.Equal(t, "api.open-meteo.com", cfg.Domain)
	require.Equal(t, "https://api.open-meteo.com", cfg.BaseURL)
	require.Equal(t, "none", cfg.Auth.Type)
	require.Equal(t, "foundry", cfg.Runtime.Worker)
	require.Equal(t, 15000, cfg.Runtime.TimeoutMS)
	require.Equal(t, []string{"GET", "POST"}, cfg.Runtime.AllowedMethods)
	require.True(t, cfg.Permissions.Invokable)
	require.Equal(t, []string{"api.open-meteo.com"}, cfg.Permissions.AllowedEgressHosts)
	require.NotNil(t, cfg.Webhook)
	require.Equal(t, "GET", cfg.Webhook.Method)
	require.Equal(t, "/v1/forecast", cfg.Webhook.Path)
}

func TestCreateRestAPISourceRejectsInvalidDomain(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}
	req := httptest.NewRequest(http.MethodPost, "/sources", strings.NewReader(`{
		"name":"bad",
		"connector_type":"rest_api",
		"config":{"base_url":"ftp://example.com"}
	}`))
	req = req.WithContext(authmw.ContextWithClaims(context.Background(), &authmw.Claims{Sub: owner}))
	rec := httptest.NewRecorder()

	h.CreateConnection(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "http or https")
}

func TestUpdateRestAPISourceNormalizesConfig(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	store := newFakeStore(owner)
	store.connections[0].ConnectorType = "rest_api"
	h := &handlers.Handlers{Repo: store}
	req := httptest.NewRequest(http.MethodPatch, "/sources/"+store.connections[0].ID.String(), strings.NewReader(`{
		"config":{
			"base_url":"https://api.example.com/",
			"auth":{"type":"bearer","secret_ref":"secret://weather-token"},
			"runtime":{"worker":"agent","timeout_ms":30000,"allowed_methods":["GET"]},
			"permissions":{"discoverable":true,"invokable":true,"allowed_egress_hosts":["api.example.com"]}
		}
	}`))
	req = req.WithContext(authmw.ContextWithClaims(context.Background(), &authmw.Claims{Sub: owner}))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", store.connections[0].ID.String())
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	rec := httptest.NewRecorder()

	h.UpdateConnection(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body models.Connection
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	var cfg models.RESTAPISourceConfig
	require.NoError(t, json.Unmarshal(body.Config, &cfg))
	require.Equal(t, "api.example.com", cfg.Domain)
	require.Equal(t, "https://api.example.com", cfg.BaseURL)
	require.Equal(t, "bearer", cfg.Auth.Type)
	require.Equal(t, "secret://weather-token", cfg.Auth.SecretRef)
	require.Equal(t, "agent", cfg.Runtime.Worker)
	require.Equal(t, []string{"GET"}, cfg.Runtime.AllowedMethods)
}

func TestWebhookHistoryRepositoryContract(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	store := newFakeStore(owner)
	sourceID := store.connections[0].ID
	now := time.Now().UTC()

	entry, err := store.AppendWebhookHistory(context.Background(), &models.CreateWebhookHistoryEntry{
		SourceID: sourceID,
		UserID:   owner,
		Status:   "succeeded",
		InputPolicy: models.WebhookHistoryInputPolicy{
			StoreOutputs: true,
			Visibility:   "hidden",
		},
		OutputParameters:   json.RawMessage(`{"temperature":84}`),
		StartedAt:          now.Add(-25 * time.Millisecond),
		FinishedAt:         now,
		RetentionExpiresAt: now.Add(time.Hour),
	})
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, entry.ID)
	require.Positive(t, entry.DurationMS)

	expiredErr := "expired"
	_, err = store.AppendWebhookHistory(context.Background(), &models.CreateWebhookHistoryEntry{
		SourceID:           sourceID,
		UserID:             owner,
		Status:             "failed",
		Error:              &expiredErr,
		StartedAt:          now.Add(-2 * time.Hour),
		FinishedAt:         now.Add(-2 * time.Hour),
		RetentionExpiresAt: now.Add(-time.Hour),
	})
	require.NoError(t, err)

	items, err := store.ListWebhookHistory(context.Background(), sourceID, 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "succeeded", items[0].Status)
	assert.JSONEq(t, `{"temperature":84}`, string(items[0].OutputParameters))

	items, err = store.ListWebhookHistory(context.Background(), sourceID, 1)
	require.NoError(t, err)
	require.Len(t, items, 1)
}

func TestInboundListenerRepositoryContract(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	store := newFakeStore(owner)
	sourceID := store.connections[0].ID
	objectTypeID := uuid.New()

	entry, err := store.AppendInboundListenerEvent(context.Background(), &models.CreateInboundListenerEvent{
		SourceID:          sourceID,
		ListenerID:        "trail-events",
		EventID:           "evt-1",
		Status:            "accepted",
		SignatureVerified: true,
		Payload:           json.RawMessage(`{"trail_id":"mule-deer"}`),
		Destination: models.InboundListenerDestinationConfig{
			Mode:         "object",
			ObjectTypeID: &objectTypeID,
		},
	})
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, entry.ID)

	items, err := store.ListInboundListenerEvents(context.Background(), sourceID, 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "trail-events", items[0].ListenerID)
	assert.Equal(t, "evt-1", items[0].EventID)
	assert.True(t, items[0].SignatureVerified)
	assert.Equal(t, objectTypeID, *items[0].Destination.ObjectTypeID)
	assert.JSONEq(t, `{"trail_id":"mule-deer"}`, string(items[0].Payload))
}

func TestListConnectionsRequiresAuth(t *testing.T) {
	t.Parallel()
	h := &handlers.Handlers{}
	req := httptest.NewRequest("GET", "/connections", nil)
	rec := httptest.NewRecorder()
	h.ListConnections(rec, req)
	assert.Equal(t, 401, rec.Code)
}

type testConnectionAdapter struct {
	result adapters.ConnectionTestResult
	err    error
}

func (a testConnectionAdapter) TestConnection(_ context.Context, _ json.RawMessage) (adapters.ConnectionTestResult, error) {
	return a.result, a.err
}
func (a testConnectionAdapter) DiscoverSources(context.Context, *models.Connection, string) ([]adapters.Source, error) {
	return nil, adapters.ErrNotImplemented
}
func (a testConnectionAdapter) QueryVirtualTable(context.Context, *models.Connection, *adapters.Query, string) (*adapters.Result, error) {
	return nil, adapters.ErrNotImplemented
}
func (a testConnectionAdapter) StreamArrow(context.Context, *models.Connection, *adapters.Query, string) (adapters.ArrowStream, error) {
	return adapters.EmptyArrowStream{}, adapters.ErrNotImplemented
}
func (a testConnectionAdapter) BuildIngestSpec(context.Context, *models.Connection, *adapters.Source) (*adapters.IngestSpec, error) {
	return nil, adapters.ErrNotImplemented
}

func TestTestConnectionUsesAdapterResultAndUpdatesStatus(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	store := newFakeStore(owner)
	store.connections[0].ConnectorType = "kafka"
	store.connections[0].Status = "disconnected"
	registry := adapters.NewRegistry()
	registry.MustRegister("kafka", adapters.SingletonFactory(testConnectionAdapter{result: adapters.ConnectionTestResult{
		Success:   true,
		Message:   "validated kafka catalog with 1 topic(s)",
		LatencyMS: 7,
		Details:   json.RawMessage(`{"mode":"catalog_backed","topic_count":1}`),
	}}))
	h := &handlers.Handlers{Repo: store, AdapterRegistry: registry}
	req := httptest.NewRequest(http.MethodPost, "/connections/"+store.connections[0].ID.String()+"/test", nil)
	req = req.WithContext(authmw.ContextWithClaims(context.Background(), &authmw.Claims{Sub: owner}))
	rec := httptest.NewRecorder()
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", store.connections[0].ID.String())
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	h.TestConnection(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "connected", store.connections[0].Status)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, true, body["success"])
	assert.Equal(t, "validated kafka catalog with 1 topic(s)", body["message"])
	assert.Equal(t, float64(7), body["latency_ms"])
	assert.Equal(t, map[string]any{"mode": "catalog_backed", "topic_count": float64(1)}, body["details"])
}

func TestTestConnectionAdapterErrorMarksConnectionError(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	store := newFakeStore(owner)
	store.connections[0].ConnectorType = "kafka"
	registry := adapters.NewRegistry()
	registry.MustRegister("kafka", adapters.SingletonFactory(testConnectionAdapter{err: assert.AnError}))
	h := &handlers.Handlers{Repo: store, AdapterRegistry: registry}
	req := httptest.NewRequest(http.MethodPost, "/connections/"+store.connections[0].ID.String()+"/test", nil)
	req = req.WithContext(authmw.ContextWithClaims(context.Background(), &authmw.Claims{Sub: owner}))
	rec := httptest.NewRecorder()
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", store.connections[0].ID.String())
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	h.TestConnection(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "error", store.connections[0].Status)
	assert.Contains(t, rec.Body.String(), assert.AnError.Error())
}

type fakeStore struct {
	connections    []models.Connection
	syncJobs       map[uuid.UUID][]models.SyncJob
	mediaSyncs     map[uuid.UUID][]models.MediaSetSync
	runs           map[uuid.UUID][]models.SyncRun
	links          map[string]models.VirtualTableSourceLink
	vtables        map[string]models.VirtualTable
	registrations  map[uuid.UUID][]models.ConnectionRegistration
	policies       map[uuid.UUID][]models.SourcePolicyBindingResponse
	agents         []models.ConnectorAgent
	webhookHistory map[uuid.UUID][]models.WebhookHistoryEntry
	listenerEvents map[uuid.UUID][]models.InboundListenerEvent
	credentials    map[uuid.UUID]map[string][]byte

	// P2.1 lease emulation. mu guards runs + lease; fault flags inject the
	// outcomes the handler must not paper over.
	mu           sync.Mutex
	lease        map[uuid.UUID]*fakeLease
	claimFails   bool                                     // claim CAS returns nil (another execution won)
	denyRenew    bool                                     // renew CAS returns nil (lease lost)
	renewErr     error                                    // renew returns a storage error
	renewHook    func(call int) (*repo.LeaseClaim, error) // per-call renew outcome override (call counts every renew)
	renewCalls   int
	checkResult  *bool // page-gate probe override (nil = derive from lease)
	checkHook    func(call int) (bool, error)
	checkCalls   int
	denyGate     bool                                    // P2.2 publish gate refuses (fenced)
	gateHook     func() (*repo.PublishGateResult, error) // per-call gate override (heartbeat ticks excluded)
	gateCalls    int
	denyComplete bool  // terminal CAS returns nil (fenced)
	completeErr  error // terminal write returns a storage error
	enqueueErr   error
	// P2.2 test hooks. enqueueBlock gates EnqueueSyncRun (admission barrier,
	// enqueueEntered signals arrival); cancelBlock gates RequestCancelSyncRun
	// (stop-budget barrier); claimCalls counts claims for no-replay
	// assertions.
	enqueueBlock   chan struct{}
	enqueueEntered chan struct{}
	cancelBlock    chan struct{}
	claimCalls     int
	claimErr       error // claim COMMITS (row running + lease installed) but returns this error — ack loss
}

// connectionInTenant mirrors the repository tenant predicate: org callers see
// only their organization's rows; personal callers see only NULL-org rows they
// own.
func (f *fakeStore) connectionInTenant(c *models.Connection, t repo.Tenant) bool {
	if t.OrgID != nil {
		return c.OrgID != nil && *c.OrgID == *t.OrgID
	}
	return c.OrgID == nil && c.OwnerID == t.OwnerID
}

// connectionForOwnerLegacy keeps the owner-only lookup the media-set sync
// surfaces still use (out of the P1.2 first slice).
func (f *fakeStore) connectionForOwnerLegacy(id, ownerID uuid.UUID) *models.Connection {
	for i := range f.connections {
		if f.connections[i].ID == id && f.connections[i].OwnerID == ownerID {
			return &f.connections[i]
		}
	}
	return nil
}

func newFakeStore(owner uuid.UUID) *fakeStore {
	conn := models.Connection{ID: uuid.New(), Name: "pg", ConnectorType: "postgresql", Config: json.RawMessage(`{}`), Status: "connected", OwnerID: owner, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	return &fakeStore{connections: []models.Connection{conn}, syncJobs: map[uuid.UUID][]models.SyncJob{}, mediaSyncs: map[uuid.UUID][]models.MediaSetSync{}, runs: map[uuid.UUID][]models.SyncRun{}, links: map[string]models.VirtualTableSourceLink{}, vtables: map[string]models.VirtualTable{}, registrations: map[uuid.UUID][]models.ConnectionRegistration{}, policies: map[uuid.UUID][]models.SourcePolicyBindingResponse{}, agents: []models.ConnectorAgent{}, webhookHistory: map[uuid.UUID][]models.WebhookHistoryEntry{}, listenerEvents: map[uuid.UUID][]models.InboundListenerEvent{}, credentials: map[uuid.UUID]map[string][]byte{}, lease: map[uuid.UUID]*fakeLease{}}
}

// fakeLease mirrors the persisted lease columns for the fake store.
type fakeLease struct {
	holder     string
	token      string
	generation int
}

// renewCallCount reports how many renew calls the hook has observed.
func (f *fakeStore) renewCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.renewCalls
}

// findRunLocked locates a run; callers must hold f.mu.
func (f *fakeStore) findRunLocked(runID uuid.UUID) (uuid.UUID, int, bool) {
	for syncID, runs := range f.runs {
		for i := range runs {
			if runs[i].ID == runID {
				return syncID, i, true
			}
		}
	}
	return uuid.Nil, -1, false
}

func (f *fakeStore) ListConnections(_ context.Context, t repo.Tenant, ownerFilter *uuid.UUID) ([]models.Connection, error) {
	out := []models.Connection{}
	for i := range f.connections {
		if !f.connectionInTenant(&f.connections[i], t) {
			continue
		}
		if ownerFilter != nil && f.connections[i].OwnerID != *ownerFilter {
			continue
		}
		out = append(out, f.connections[i])
	}
	return out, nil
}
func (f *fakeStore) GetConnection(_ context.Context, id uuid.UUID) (*models.Connection, error) {
	for i := range f.connections {
		if f.connections[i].ID == id {
			return &f.connections[i], nil
		}
	}
	return nil, nil
}
func (f *fakeStore) GetConnectionForTenant(_ context.Context, id uuid.UUID, t repo.Tenant) (*models.Connection, error) {
	for i := range f.connections {
		if f.connections[i].ID == id && f.connectionInTenant(&f.connections[i], t) {
			return &f.connections[i], nil
		}
	}
	return nil, nil
}
func (f *fakeStore) CreateConnection(_ context.Context, body *models.CreateConnectionRequest, t repo.Tenant) (*models.Connection, error) {
	c := models.Connection{ID: uuid.New(), Name: body.Name, ConnectorType: body.ConnectorType, Config: body.Config, OwnerID: t.OwnerID, OrgID: t.OrgID, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	f.connections = append(f.connections, c)
	return &f.connections[len(f.connections)-1], nil
}
func (f *fakeStore) UpdateConnection(_ context.Context, id uuid.UUID, t repo.Tenant, body *models.UpdateConnectionRequest) (*models.Connection, error) {
	for i := range f.connections {
		if f.connections[i].ID == id && f.connectionInTenant(&f.connections[i], t) {
			if len(body.Config) > 0 {
				if active, _ := f.HasActiveSyncRunsForSource(context.Background(), id, t); active {
					return nil, repo.ErrActiveRuns
				}
			}
			if body.Status != nil {
				f.connections[i].Status = *body.Status
			}
			if body.Name != nil {
				f.connections[i].Name = *body.Name
			}
			if body.Config != nil {
				f.connections[i].Config = body.Config
			}
			return &f.connections[i], nil
		}
	}
	return nil, nil
}
func (f *fakeStore) DeleteConnection(_ context.Context, id uuid.UUID, t repo.Tenant) (bool, error) {
	c, _ := f.GetConnectionForTenant(context.Background(), id, t)
	if c == nil {
		return false, nil
	}
	if active, _ := f.HasActiveSyncRunsForSource(context.Background(), id, t); active {
		return false, repo.ErrActiveRuns
	}
	return true, nil
}
func (f *fakeStore) ListSyncJobs(_ context.Context, sourceID uuid.UUID, t repo.Tenant) ([]models.SyncJob, error) {
	if c, _ := f.GetConnectionForTenant(context.Background(), sourceID, t); c == nil {
		return []models.SyncJob{}, nil
	}
	return f.syncJobs[sourceID], nil
}
func (f *fakeStore) GetSyncJob(_ context.Context, id uuid.UUID, t repo.Tenant) (*models.SyncJob, error) {
	for source, jobs := range f.syncJobs {
		if c, _ := f.GetConnectionForTenant(context.Background(), source, t); c == nil {
			continue
		}
		for i := range jobs {
			if jobs[i].ID == id {
				return &jobs[i], nil
			}
		}
	}
	return nil, nil
}
func (f *fakeStore) CreateSyncJob(_ context.Context, body *models.CreateSyncJobRequest, t repo.Tenant) (*models.SyncJob, error) {
	c, _ := f.GetConnectionForTenant(context.Background(), body.SourceID, t)
	if c == nil {
		return nil, nil
	}
	j := models.SyncJob{ID: uuid.New(), SourceID: body.SourceID, OutputDatasetID: body.OutputDatasetID, FileGlob: body.FileGlob, ScheduleCron: body.ScheduleCron, OrgID: c.OrgID, CreatedAt: time.Now().UTC()}
	f.syncJobs[body.SourceID] = append([]models.SyncJob{j}, f.syncJobs[body.SourceID]...)
	return &f.syncJobs[body.SourceID][0], nil
}
func (f *fakeStore) UpdateSyncJob(_ context.Context, id uuid.UUID, body *models.UpdateSyncJobRequest, t repo.Tenant) (*models.SyncJob, error) {
	for source, jobs := range f.syncJobs {
		if c, _ := f.GetConnectionForTenant(context.Background(), source, t); c == nil {
			continue
		}
		for i := range jobs {
			if jobs[i].ID == id {
				if body.OutputDatasetID != nil && *body.OutputDatasetID != jobs[i].OutputDatasetID {
					if active, _ := f.HasActiveSyncRunsForDef(context.Background(), id, t); active {
						return nil, repo.ErrActiveRuns
					}
				}
				if body.OutputDatasetID != nil {
					jobs[i].OutputDatasetID = *body.OutputDatasetID
				}
				if body.FileGlob != nil {
					jobs[i].FileGlob = body.FileGlob
				}
				if body.ScheduleCron != nil {
					jobs[i].ScheduleCron = body.ScheduleCron
				}
				f.syncJobs[source] = jobs
				return &jobs[i], nil
			}
		}
	}
	return nil, nil
}
func (f *fakeStore) RunSyncJob(_ context.Context, id uuid.UUID, t repo.Tenant) (*models.SyncRun, error) {
	job, _ := f.GetSyncJob(context.Background(), id, t)
	if job == nil {
		return nil, nil
	}
	run := models.SyncRun{ID: uuid.New(), SyncDefID: id, Status: "running", OrgID: job.OrgID, StartedAt: time.Now().UTC(), ControlMode: models.RunControlLegacy}
	f.runs[id] = append(f.runs[id], run)
	return &f.runs[id][len(f.runs[id])-1], nil
}
func (f *fakeStore) ListSyncRuns(_ context.Context, syncID uuid.UUID, t repo.Tenant) ([]models.SyncRun, error) {
	if job, _ := f.GetSyncJob(context.Background(), syncID, t); job == nil {
		return []models.SyncRun{}, nil
	}
	out := []models.SyncRun{}
	for _, run := range f.runs[syncID] {
		if t.OrgID != nil {
			if run.OrgID != nil && *run.OrgID == *t.OrgID {
				out = append(out, run)
			}
			continue
		}
		if run.OrgID == nil {
			out = append(out, run)
		}
	}
	return out, nil
}

// CompleteSyncRun mirrors the guarded legacy path: it can only close
// lease-less running rows exactly once. Leased runs, pending tasks and
// terminal rows are never mutated through it.
func (f *fakeStore) CompleteSyncRun(_ context.Context, runID uuid.UUID, _ repo.Tenant, status string, bytesWritten int64, filesWritten int64, rowsWritten int64, errMsg *string, ingestJobID *string, datasetVersionID *uuid.UUID, contentHash *string) (*models.SyncRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for syncID, runs := range f.runs {
		for i := range runs {
			if runs[i].ID == runID {
				if runs[i].Status != "running" || f.lease[runID] != nil {
					return nil, nil
				}
				now := time.Now().UTC()
				runs[i].Status = status
				runs[i].FinishedAt = &now
				runs[i].BytesWritten = bytesWritten
				runs[i].FilesWritten = filesWritten
				runs[i].RowsWritten = rowsWritten
				runs[i].Error = errMsg
				runs[i].IngestJobID = ingestJobID
				runs[i].DatasetVersionID = datasetVersionID
				runs[i].ContentHash = contentHash
				f.runs[syncID] = runs
				return &runs[i], nil
			}
		}
	}
	return nil, nil
}

func (f *fakeStore) EnqueueSyncRun(ctx context.Context, syncID, sourceID uuid.UUID, t repo.Tenant, requestedBy uuid.UUID) (*models.SyncRun, error) {
	if f.enqueueErr != nil {
		return nil, f.enqueueErr
	}
	if f.enqueueBlock != nil {
		if f.enqueueEntered != nil {
			select {
			case f.enqueueEntered <- struct{}{}:
			default:
			}
		}
		select {
		case <-f.enqueueBlock:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	job, _ := f.GetSyncJob(context.Background(), syncID, t)
	if job == nil || job.SourceID != sourceID {
		return nil, nil
	}
	owner := t.OwnerID
	for i := range f.connections {
		if f.connections[i].ID == sourceID {
			owner = f.connections[i].OwnerID
		}
	}
	requester := requestedBy
	run := models.SyncRun{ID: uuid.New(), SyncDefID: syncID, Status: "pending", OrgID: job.OrgID, StartedAt: time.Now().UTC(), RequestedBy: &requester, OwnerID: &owner, ControlMode: models.RunControlControlled}
	f.mu.Lock()
	f.runs[syncID] = append(f.runs[syncID], run)
	f.mu.Unlock()
	return &run, nil
}

func (f *fakeStore) ClaimSyncRun(_ context.Context, runID uuid.UUID, _ repo.Tenant, holder, token string, _ time.Duration) (*repo.LeaseClaim, error) {
	if f.claimFails {
		return nil, nil
	}
	f.mu.Lock()
	f.claimCalls++
	defer f.mu.Unlock()
	defID, idx, ok := f.findRunLocked(runID)
	if !ok || f.runs[defID][idx].Status != "pending" {
		return nil, nil
	}
	run := &f.runs[defID][idx]
	run.Status = "running"
	run.Attempt++
	run.Generation++
	run.StartedAt = time.Now().UTC()
	f.lease[runID] = &fakeLease{holder: holder, token: token, generation: run.Generation}
	if f.claimErr != nil {
		// The claim committed; only the acknowledgement was lost.
		return nil, f.claimErr
	}
	return &repo.LeaseClaim{Run: *run, Token: token, Holder: holder, ExpiresAt: time.Now().UTC().Add(time.Minute)}, nil
}

func (f *fakeStore) RenewSyncRunLease(_ context.Context, runID uuid.UUID, _ repo.Tenant, holder, token string, generation int, _ time.Duration) (*repo.LeaseClaim, error) {
	// renewErr/denyRenew/renewHook are read under the lock: tests may flip
	// them from other goroutines mid-run (page handlers, heartbeat races).
	f.mu.Lock()
	renewErr := f.renewErr
	hook := f.renewHook
	var call int
	if hook != nil {
		f.renewCalls++
		call = f.renewCalls
	}
	f.mu.Unlock()
	if hook != nil {
		return hook(call)
	}
	if renewErr != nil {
		return nil, renewErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.denyRenew {
		return nil, nil
	}
	defID, idx, ok := f.findRunLocked(runID)
	if !ok {
		return nil, nil
	}
	run := &f.runs[defID][idx]
	ls := f.lease[runID]
	if run.Status != "running" || ls == nil || ls.holder != holder || ls.token != token || ls.generation != generation {
		return nil, nil
	}
	return &repo.LeaseClaim{Run: *run, Token: token, Holder: holder, ExpiresAt: time.Now().UTC().Add(time.Minute)}, nil
}

func (f *fakeStore) CheckSyncRunLease(_ context.Context, runID uuid.UUID, _ repo.Tenant, holder, token string, generation int) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.checkHook != nil {
		f.checkCalls++
		return f.checkHook(f.checkCalls)
	}
	if f.checkResult != nil {
		return *f.checkResult, nil
	}
	defID, idx, ok := f.findRunLocked(runID)
	if !ok {
		return false, nil
	}
	run := &f.runs[defID][idx]
	ls := f.lease[runID]
	return run.Status == "running" && ls != nil && ls.holder == holder && ls.token == token && ls.generation == generation, nil
}

func (f *fakeStore) CompleteLeasedSyncRun(_ context.Context, runID uuid.UUID, _ repo.Tenant, holder, token string, generation int, status string, bytesWritten int64, filesWritten int64, rowsWritten int64, errMsg *string, ingestJobID *string, datasetVersionID *uuid.UUID, contentHash *string) (*models.SyncRun, error) {
	if f.completeErr != nil {
		return nil, f.completeErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.denyComplete {
		return nil, nil
	}
	defID, idx, ok := f.findRunLocked(runID)
	if !ok {
		return nil, nil
	}
	run := &f.runs[defID][idx]
	ls := f.lease[runID]
	if run.Status != "running" || ls == nil || ls.holder != holder || ls.token != token || ls.generation != generation {
		return nil, nil
	}
	// P2.2 stage machine for controlled runs (mirrors the SQL guard):
	// succeeded requires the gate entered and no cancel won; aborted requires
	// the gate NOT entered; failed is unconstrained; legacy keeps P2.1.
	if run.ControlMode != models.RunControlLegacy {
		switch status {
		case "succeeded":
			if run.PublishEnteredAt == nil || run.CancelRequestedAt != nil {
				return nil, nil
			}
		case "aborted":
			if run.PublishEnteredAt != nil {
				return nil, nil
			}
		}
	}
	now := time.Now().UTC()
	run.Status = status
	run.FinishedAt = &now
	run.BytesWritten = bytesWritten
	run.FilesWritten = filesWritten
	run.RowsWritten = rowsWritten
	run.Error = errMsg
	run.IngestJobID = ingestJobID
	run.DatasetVersionID = datasetVersionID
	run.ContentHash = contentHash
	return run, nil
}

// runTenantVisible mirrors syncRunPredicate for the fake rows: org tenants
// see rows of their org; personal tenants see NULL-org rows THROUGH the
// def→source owner chain (a NULL-org row alone proves nothing — the same
// rule the SQL predicate enforces).
func (f *fakeStore) runTenantVisible(run *models.SyncRun, t repo.Tenant) bool {
	if t.OrgID != nil {
		return run.OrgID != nil && *run.OrgID == *t.OrgID
	}
	if run.OrgID != nil {
		return false
	}
	for source, jobs := range f.syncJobs {
		for i := range jobs {
			if jobs[i].ID != run.SyncDefID {
				continue
			}
			for c := range f.connections {
				if f.connections[c].ID == source && f.connections[c].OwnerID == t.OwnerID {
					return true
				}
			}
		}
	}
	return false
}

// leaseMaterialOK reports whether the fake row currently holds exactly this
// lease.
func (f *fakeStore) leaseMaterialOK(runID uuid.UUID, holder, token string, generation int) bool {
	run := f.runByAnyID(runID)
	if run == nil || run.Status != "running" {
		return false
	}
	ls := f.lease[runID]
	return ls != nil && ls.holder == holder && ls.token == token && ls.generation == generation
}

func (f *fakeStore) runByAnyID(runID uuid.UUID) *models.SyncRun {
	for syncID := range f.runs {
		for i := range f.runs[syncID] {
			if f.runs[syncID][i].ID == runID {
				return &f.runs[syncID][i]
			}
		}
	}
	return nil
}

// GetSyncRunForTenant mirrors the double-binding single-row read.
func (f *fakeStore) GetSyncRunForTenant(_ context.Context, syncID, runID uuid.UUID, t repo.Tenant) (*models.SyncRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.runs[syncID] {
		run := &f.runs[syncID][i]
		if run.ID == runID && f.runTenantVisible(run, t) {
			cp := *run
			return &cp, nil
		}
	}
	return nil, nil
}

// RequestCancelSyncRun mirrors the durable cancel state machine, including
// first-request preservation (repeats return the row unchanged).
func (f *fakeStore) RequestCancelSyncRun(ctx context.Context, runID, syncID uuid.UUID, t repo.Tenant, cancelledBy *uuid.UUID, cause string) (*repo.CancelResult, error) {
	if f.cancelBlock != nil {
		select {
		case <-f.cancelBlock:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := -1
	for i := range f.runs[syncID] {
		if f.runs[syncID][i].ID == runID {
			idx = i
			break
		}
	}
	if idx < 0 || !f.runTenantVisible(&f.runs[syncID][idx], t) {
		return &repo.CancelResult{Outcome: repo.CancelOutcomeNotVisible}, nil
	}
	run := &f.runs[syncID][idx]
	cp := func() *models.SyncRun { c := *run; return &c }
	if run.ControlMode != models.RunControlControlled {
		return &repo.CancelResult{Outcome: repo.CancelOutcomeUnsupported, Run: cp()}, nil
	}
	switch run.Status {
	case "succeeded", "failed", "aborted":
		return &repo.CancelResult{Outcome: repo.CancelOutcomeAlreadyTerminal, Run: cp()}, nil
	}
	if run.PublishEnteredAt != nil {
		return &repo.CancelResult{Outcome: repo.CancelOutcomeTooLate, Run: cp()}, nil
	}
	if run.Status == "pending" {
		now := time.Now().UTC()
		run.Status = "aborted"
		run.FinishedAt = &now
		msg := cause
		run.Error = &msg
		if run.CancelRequestedAt == nil {
			run.CancelRequestedAt = &now
			run.CancelledBy = cancelledBy
		}
		return &repo.CancelResult{Outcome: repo.CancelOutcomeAborted, Run: cp()}, nil
	}
	// running: first write only, verbatim repeats.
	if run.CancelRequestedAt != nil {
		return &repo.CancelResult{Outcome: repo.CancelOutcomeRequested, Run: cp()}, nil
	}
	now := time.Now().UTC()
	run.CancelRequestedAt = &now
	run.CancelledBy = cancelledBy
	return &repo.CancelResult{Outcome: repo.CancelOutcomeRequested, Run: cp()}, nil
}

// P2.5 checkpoint surface: legacy tests carry no captured incremental runs,
// so the incremental views answer nil and the incremental ops delegate to
// their non-incremental twins (the handler only reaches them for
// run.cp_mode != "" rows, which these fixtures never build).
func (f *fakeStore) SyncRunIncrementalInfo(_ context.Context, _ uuid.UUID, _ repo.Tenant) (*models.RunIncrementalView, error) {
	return nil, nil
}

func (f *fakeStore) GetCheckpointView(_ context.Context, _ uuid.UUID, _ repo.Tenant) (*repo.CheckpointView, error) {
	return nil, nil
}

func (f *fakeStore) BeginIncrementalPublishGate(ctx context.Context, runID uuid.UUID, t repo.Tenant, holder, token string, generation int, ttl time.Duration) (*repo.IncrementalPublishGateResult, error) {
	res, err := f.BeginPublishGate(ctx, runID, t, holder, token, generation, ttl)
	if err != nil || res == nil {
		return nil, err
	}
	out := &repo.IncrementalPublishGateResult{Claim: res.Claim, Run: res.Run}
	switch res.Outcome {
	case repo.PublishGateEntered:
		out.Outcome = repo.IncrementalPublishGateEntered
	case repo.PublishGateCancelled:
		out.Outcome = repo.IncrementalPublishGateCancelled
	default:
		out.Outcome = repo.IncrementalPublishGateFenced
	}
	return out, nil
}

func (f *fakeStore) CompleteIncrementalSyncRun(ctx context.Context, runID uuid.UUID, t repo.Tenant, holder, token string, generation int, adv repo.IncrementalAdvance) (*models.SyncRun, error) {
	legacy, err := f.CompleteLeasedSyncRun(ctx, runID, t, holder, token, generation, "succeeded", adv.Bytes, 1, adv.Rows, nil, nil, &adv.TxnID, &adv.ContentHash)
	if err != nil || legacy == nil {
		return nil, err
	}
	return legacy, nil
}

// BeginPublishGate mirrors the atomic gate decision: entered (with renewed
// claim), cancelled (only when the caller's material is the valid holder and
// a request is persisted), or fenced. denyGate/gateHook inject outcomes the
// handler must not paper over (lease lost at the gate, cancellation races).
func (f *fakeStore) BeginPublishGate(_ context.Context, runID uuid.UUID, _ repo.Tenant, holder, token string, generation int, ttl time.Duration) (*repo.PublishGateResult, error) {
	f.mu.Lock()
	denyGate := f.denyGate
	hook := f.gateHook
	if hook != nil {
		f.gateCalls++
	}
	f.mu.Unlock()
	if hook != nil {
		return hook()
	}
	if denyGate {
		return &repo.PublishGateResult{Outcome: repo.PublishGateFenced}, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	defID, idx, ok := f.findRunLocked(runID)
	if !ok {
		return &repo.PublishGateResult{Outcome: repo.PublishGateFenced}, nil
	}
	run := &f.runs[defID][idx]
	ls := f.lease[runID]
	materialOK := run.Status == "running" && run.ControlMode == models.RunControlControlled &&
		ls != nil && ls.holder == holder && ls.token == token && ls.generation == generation &&
		run.PublishEnteredAt == nil
	if materialOK && run.CancelRequestedAt == nil {
		now := time.Now().UTC()
		run.PublishEnteredAt = &now
		claim := &repo.LeaseClaim{Run: *run, Token: token, Holder: holder, ExpiresAt: now.Add(ttl)}
		return &repo.PublishGateResult{Outcome: repo.PublishGateEntered, Claim: claim}, nil
	}
	if materialOK && run.CancelRequestedAt != nil {
		cp := *run
		return &repo.PublishGateResult{Outcome: repo.PublishGateCancelled, Run: &cp}, nil
	}
	return &repo.PublishGateResult{Outcome: repo.PublishGateFenced}, nil
}

// CheckSyncRunControl mirrors the combined lease + cancel probe. The P2.1
// checkResult/checkHook overrides apply here too so the existing page-gate
// fault injections keep their meaning (cancel always derives from the row).
func (f *fakeStore) CheckSyncRunControl(_ context.Context, runID uuid.UUID, _ repo.Tenant, holder, token string, generation int) (bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	run := f.runByAnyID(runID)
	if run == nil {
		return false, false, nil
	}
	cancelRequested := run.CancelRequestedAt != nil
	if f.checkHook != nil {
		f.checkCalls++
		ok, err := f.checkHook(f.checkCalls)
		return ok, cancelRequested, err
	}
	if f.checkResult != nil {
		return *f.checkResult, cancelRequested, nil
	}
	ls := f.lease[runID]
	leaseOK := run.Status == "running" && ls != nil && ls.holder == holder && ls.token == token && ls.generation == generation
	return leaseOK, cancelRequested, nil
}

func (f *fakeStore) HasActiveSyncRunsForSource(_ context.Context, sourceID uuid.UUID, t repo.Tenant) (bool, error) {
	defs, _ := f.ListSyncJobs(context.Background(), sourceID, t)
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, job := range defs {
		for _, run := range f.runs[job.ID] {
			if run.Status == "pending" || run.Status == "running" {
				return true, nil
			}
		}
	}
	return false, nil
}

func (f *fakeStore) HasActiveSyncRunsForDef(_ context.Context, defID uuid.UUID, _ repo.Tenant) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, run := range f.runs[defID] {
		if run.Status == "pending" || run.Status == "running" {
			return true, nil
		}
	}
	return false, nil
}
func (f *fakeStore) PreviousDatasetVersionForHash(_ context.Context, syncDefID uuid.UUID, contentHash string) (*uuid.UUID, error) {
	for _, run := range f.runs[syncDefID] {
		if run.ContentHash != nil && *run.ContentHash == contentHash && run.DatasetVersionID != nil {
			return run.DatasetVersionID, nil
		}
	}
	return nil, nil
}
func (f *fakeStore) RecordDatasetVersionOnRun(_ context.Context, runID uuid.UUID, datasetVersionID uuid.UUID, contentHash string) error {
	for syncID, runs := range f.runs {
		for i := range runs {
			if runs[i].ID == runID {
				runs[i].DatasetVersionID = &datasetVersionID
				runs[i].ContentHash = &contentHash
				f.runs[syncID] = runs
				return nil
			}
		}
	}
	return nil
}
func (f *fakeStore) ListCredentials(_ context.Context, sourceID uuid.UUID, t repo.Tenant) ([]models.CredentialResponse, error) {
	if c, _ := f.GetConnectionForTenant(context.Background(), sourceID, t); c == nil {
		return []models.CredentialResponse{}, nil
	}
	out := []models.CredentialResponse{}
	for kind := range f.credentials[sourceID] {
		out = append(out, models.CredentialResponse{ID: uuid.New(), SourceID: sourceID, Kind: kind, CreatedAt: time.Now().UTC()})
	}
	return out, nil
}

func (f *fakeStore) GetSourceCredentialCiphertext(_ context.Context, sourceID uuid.UUID, t repo.Tenant, kind string) ([]byte, error) {
	if c, _ := f.GetConnectionForTenant(context.Background(), sourceID, t); c == nil {
		return nil, nil
	}
	return f.credentials[sourceID][kind], nil
}
func (f *fakeStore) SetCredential(_ context.Context, sourceID uuid.UUID, t repo.Tenant, kind string, ciphertext []byte, fingerprint string) (*models.CredentialResponse, error) {
	if c, _ := f.GetConnectionForTenant(context.Background(), sourceID, t); c == nil {
		return nil, nil
	}
	if f.credentials[sourceID] == nil {
		f.credentials[sourceID] = map[string][]byte{}
	}
	f.credentials[sourceID][kind] = ciphertext
	return &models.CredentialResponse{ID: uuid.New(), SourceID: sourceID, Kind: kind, Fingerprint: fingerprint, CreatedAt: time.Now().UTC()}, nil
}
func (f *fakeStore) ListConnectorAgents(_ context.Context, ownerID uuid.UUID) ([]models.ConnectorAgent, error) {
	out := []models.ConnectorAgent{}
	for _, agent := range f.agents {
		if agent.OwnerID == ownerID {
			out = append(out, agent)
		}
	}
	return out, nil
}
func (f *fakeStore) RegisterConnectorAgent(_ context.Context, body *models.RegisterAgentRequest, ownerID uuid.UUID) (*models.ConnectorAgent, error) {
	now := time.Now().UTC()
	for i := range f.agents {
		if f.agents[i].AgentURL == body.AgentURL {
			f.agents[i].Name = body.Name
			f.agents[i].OwnerID = ownerID
			f.agents[i].Status = "online"
			f.agents[i].Capabilities = body.Capabilities
			f.agents[i].Metadata = body.Metadata
			f.agents[i].LastHeartbeatAt = &now
			f.agents[i].UpdatedAt = now
			return &f.agents[i], nil
		}
	}
	agent := models.ConnectorAgent{ID: uuid.New(), Name: body.Name, AgentURL: body.AgentURL, OwnerID: ownerID, Status: "online", Capabilities: body.Capabilities, Metadata: body.Metadata, LastHeartbeatAt: &now, CreatedAt: now, UpdatedAt: now}
	f.agents = append([]models.ConnectorAgent{agent}, f.agents...)
	return &f.agents[0], nil
}
func (f *fakeStore) HeartbeatConnectorAgent(_ context.Context, id uuid.UUID, body *models.AgentHeartbeatRequest, ownerID uuid.UUID) (*models.ConnectorAgent, error) {
	now := time.Now().UTC()
	for i := range f.agents {
		if f.agents[i].ID == id && f.agents[i].OwnerID == ownerID {
			f.agents[i].Status = "online"
			f.agents[i].Capabilities = body.Capabilities
			f.agents[i].Metadata = body.Metadata
			f.agents[i].LastHeartbeatAt = &now
			f.agents[i].UpdatedAt = now
			return &f.agents[i], nil
		}
	}
	return nil, nil
}
func (f *fakeStore) DeleteConnectorAgent(_ context.Context, id uuid.UUID, ownerID uuid.UUID) (bool, error) {
	for i := range f.agents {
		if f.agents[i].ID == id && f.agents[i].OwnerID == ownerID {
			f.agents = append(f.agents[:i], f.agents[i+1:]...)
			return true, nil
		}
	}
	return false, nil
}
func (f *fakeStore) ListSourcePolicies(_ context.Context, sourceID uuid.UUID, t repo.Tenant) ([]models.SourcePolicyBindingResponse, error) {
	if c, _ := f.GetConnectionForTenant(context.Background(), sourceID, t); c == nil {
		return []models.SourcePolicyBindingResponse{}, nil
	}
	return append([]models.SourcePolicyBindingResponse(nil), f.policies[sourceID]...), nil
}
func (f *fakeStore) AttachPolicy(_ context.Context, sourceID uuid.UUID, t repo.Tenant, policyID uuid.UUID, kind string) (*models.SourcePolicyBindingResponse, error) {
	if c, _ := f.GetConnectionForTenant(context.Background(), sourceID, t); c == nil {
		return nil, nil
	}
	binding := models.SourcePolicyBindingResponse{SourceID: sourceID, PolicyID: policyID, Kind: kind}
	items := f.policies[sourceID]
	for i := range items {
		if items[i].PolicyID == policyID {
			items[i] = binding
			f.policies[sourceID] = items
			return &items[i], nil
		}
	}
	f.policies[sourceID] = append(items, binding)
	return &binding, nil
}
func (f *fakeStore) DetachPolicy(_ context.Context, sourceID uuid.UUID, t repo.Tenant, policyID uuid.UUID) (bool, error) {
	if c, _ := f.GetConnectionForTenant(context.Background(), sourceID, t); c == nil {
		return false, nil
	}
	items := f.policies[sourceID]
	for i := range items {
		if items[i].PolicyID == policyID {
			f.policies[sourceID] = append(items[:i], items[i+1:]...)
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeStore) EnableVirtualTableSource(_ context.Context, sourceRID string, body *models.EnableVirtualTableSourceRequest) (*models.VirtualTableSourceLink, error) {
	if body.Provider == "" {
		return nil, assert.AnError
	}
	l := models.VirtualTableSourceLink{SourceRID: sourceRID, Provider: body.Provider, VirtualTablesEnabled: true, ExportControls: []byte(`{}`), AutoRegisterTagFilters: []byte(`[]`), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	f.links[sourceRID] = l
	return &l, nil
}
func (f *fakeStore) CreateVirtualTable(_ context.Context, sourceRID string, actorID string, body *models.CreateVirtualTableRequest) (*models.VirtualTable, error) {
	if _, ok := f.links[sourceRID]; !ok {
		return nil, nil
	}
	loc, err := body.Locator.CanonicalJSON()
	if err != nil {
		return nil, err
	}
	name := body.Locator.DefaultDisplayName()
	if body.Name != nil {
		name = *body.Name
	}
	rid := "ri.foundry.main.virtual-table." + uuid.NewString()
	creator := actorID
	v := models.VirtualTable{ID: uuid.New(), RID: rid, SourceRID: sourceRID, ProjectRID: body.ProjectRID, Name: name, Locator: loc, TableType: body.TableType, SchemaInferred: []byte(`[]`), Capabilities: []byte(`{}`), Markings: body.Markings, Properties: []byte(`{}`), CreatedBy: &creator, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	f.vtables[rid] = v
	return &v, nil
}
func (f *fakeStore) ListVirtualTables(_ context.Context, ownerID string, project, source string, _ int) ([]models.VirtualTable, error) {
	out := []models.VirtualTable{}
	for _, v := range f.vtables {
		if v.CreatedBy != nil && *v.CreatedBy == ownerID && (project == "" || v.ProjectRID == project) && (source == "" || v.SourceRID == source) {
			out = append(out, v)
		}
	}
	return out, nil
}
func (f *fakeStore) GetVirtualTable(_ context.Context, rid string, ownerID string) (*models.VirtualTable, error) {
	v, ok := f.vtables[rid]
	if !ok || v.CreatedBy == nil || *v.CreatedBy != ownerID {
		return nil, nil
	}
	return &v, nil
}

func (f *fakeStore) ListRegistrations(_ context.Context, sourceID uuid.UUID) ([]models.ConnectionRegistration, error) {
	return f.registrations[sourceID], nil
}
func (f *fakeStore) UpsertRegistration(_ context.Context, sourceID uuid.UUID, source models.DiscoveredSource, mode string, autoSync bool, updateDetection bool, targetDatasetID *uuid.UUID, metadata json.RawMessage) (*models.ConnectionRegistration, error) {
	if len(metadata) == 0 || string(metadata) == "null" {
		metadata = []byte(`{}`)
	}
	for i := range f.registrations[sourceID] {
		if f.registrations[sourceID][i].Selector == source.Selector {
			f.registrations[sourceID][i].DisplayName = source.DisplayName
			f.registrations[sourceID][i].SourceKind = source.SourceKind
			f.registrations[sourceID][i].RegistrationMode = mode
			f.registrations[sourceID][i].AutoSync = autoSync
			f.registrations[sourceID][i].UpdateDetection = updateDetection
			f.registrations[sourceID][i].TargetDatasetID = targetDatasetID
			f.registrations[sourceID][i].Metadata = metadata
			f.registrations[sourceID][i].UpdatedAt = time.Now().UTC()
			return &f.registrations[sourceID][i], nil
		}
	}
	reg := models.ConnectionRegistration{ID: uuid.New(), ConnectionID: sourceID, Selector: source.Selector, DisplayName: source.DisplayName, SourceKind: source.SourceKind, RegistrationMode: mode, AutoSync: autoSync, UpdateDetection: updateDetection, TargetDatasetID: targetDatasetID, LastSourceSignature: source.SourceSignature, Metadata: metadata, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	f.registrations[sourceID] = append([]models.ConnectionRegistration{reg}, f.registrations[sourceID]...)
	return &reg, nil
}
func (f *fakeStore) GetRegistration(_ context.Context, sourceID uuid.UUID, registrationID uuid.UUID) (*models.ConnectionRegistration, error) {
	for i := range f.registrations[sourceID] {
		if f.registrations[sourceID][i].ID == registrationID {
			return &f.registrations[sourceID][i], nil
		}
	}
	return nil, nil
}
func (f *fakeStore) DeleteRegistration(_ context.Context, sourceID uuid.UUID, registrationID uuid.UUID) (bool, error) {
	regs := f.registrations[sourceID]
	for i := range regs {
		if regs[i].ID == registrationID {
			f.registrations[sourceID] = append(regs[:i], regs[i+1:]...)
			return true, nil
		}
	}
	return false, nil
}
func (f *fakeStore) UpdateConnectionConfig(_ context.Context, id uuid.UUID, config json.RawMessage) (*models.Connection, error) {
	for i := range f.connections {
		if f.connections[i].ID == id {
			f.connections[i].Config = config
			f.connections[i].UpdatedAt = time.Now().UTC()
			return &f.connections[i], nil
		}
	}
	return nil, nil
}
func (f *fakeStore) AppendWebhookHistory(_ context.Context, body *models.CreateWebhookHistoryEntry) (*models.WebhookHistoryEntry, error) {
	now := time.Now().UTC()
	if body.StartedAt.IsZero() {
		body.StartedAt = now
	}
	if body.FinishedAt.IsZero() {
		body.FinishedAt = now
	}
	if body.RetentionExpiresAt.IsZero() {
		body.RetentionExpiresAt = body.FinishedAt.Add(30 * 24 * time.Hour)
	}
	durationMS := body.FinishedAt.Sub(body.StartedAt).Milliseconds()
	if durationMS < 0 {
		durationMS = 0
	}
	entry := models.WebhookHistoryEntry{
		ID:                 uuid.New(),
		SourceID:           body.SourceID,
		UserID:             body.UserID,
		Status:             body.Status,
		HTTPStatus:         body.HTTPStatus,
		InputPolicy:        body.InputPolicy,
		Inputs:             cloneTestRawMessage(body.Inputs),
		OutputParameters:   cloneTestRawMessage(body.OutputParameters),
		Error:              body.Error,
		CallCount:          body.CallCount,
		StartedAt:          body.StartedAt,
		FinishedAt:         body.FinishedAt,
		DurationMS:         durationMS,
		RetentionExpiresAt: body.RetentionExpiresAt,
		CreatedAt:          now,
	}
	f.webhookHistory[body.SourceID] = append([]models.WebhookHistoryEntry{entry}, f.webhookHistory[body.SourceID]...)
	return &entry, nil
}
func (f *fakeStore) ListWebhookHistory(_ context.Context, sourceID uuid.UUID, limit int) ([]models.WebhookHistoryEntry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	now := time.Now().UTC()
	out := []models.WebhookHistoryEntry{}
	for _, entry := range f.webhookHistory[sourceID] {
		if !entry.RetentionExpiresAt.IsZero() && entry.RetentionExpiresAt.Before(now) {
			continue
		}
		out = append(out, entry)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}
func (f *fakeStore) AppendInboundListenerEvent(_ context.Context, body *models.CreateInboundListenerEvent) (*models.InboundListenerEvent, error) {
	now := time.Now().UTC()
	entry := models.InboundListenerEvent{
		ID:                uuid.New(),
		SourceID:          body.SourceID,
		ListenerID:        body.ListenerID,
		EventID:           body.EventID,
		Status:            body.Status,
		SignatureVerified: body.SignatureVerified,
		Payload:           cloneTestRawMessage(body.Payload),
		Headers:           cloneTestRawMessage(body.Headers),
		Destination:       body.Destination,
		CreatedAt:         now,
	}
	if entry.Status == "" {
		entry.Status = "accepted"
	}
	f.listenerEvents[body.SourceID] = append([]models.InboundListenerEvent{entry}, f.listenerEvents[body.SourceID]...)
	return &entry, nil
}
func (f *fakeStore) ListInboundListenerEvents(_ context.Context, sourceID uuid.UUID, limit int) ([]models.InboundListenerEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	out := []models.InboundListenerEvent{}
	for _, entry := range f.listenerEvents[sourceID] {
		out = append(out, entry)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}
func cloneTestRawMessage(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}
func (f *fakeStore) ListIcebergNamespaces(_ context.Context) ([]models.Connection, error) {
	out := []models.Connection{}
	for _, c := range f.connections {
		for _, r := range f.registrations[c.ID] {
			var meta map[string]any
			_ = json.Unmarshal(r.Metadata, &meta)
			if meta["supports_zero_copy"] == true {
				out = append(out, c)
				break
			}
		}
	}
	return out, nil
}
func (f *fakeStore) GetIcebergConnection(_ context.Context, namespace string) (*models.Connection, error) {
	for i := range f.connections {
		if f.connections[i].Name == namespace || strings.NewReplacer("-", "-", " ", "_", ".", "_").Replace(f.connections[i].Name) == namespace {
			return &f.connections[i], nil
		}
	}
	return nil, nil
}
func (f *fakeStore) ListIcebergTables(_ context.Context, connectionID uuid.UUID) ([]models.ConnectionRegistration, error) {
	out := []models.ConnectionRegistration{}
	for _, r := range f.registrations[connectionID] {
		var meta map[string]any
		_ = json.Unmarshal(r.Metadata, &meta)
		if meta["supports_zero_copy"] == true {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeStore) ListMediaSetSyncs(_ context.Context, sourceID uuid.UUID, ownerID uuid.UUID) ([]models.MediaSetSync, error) {
	if c := f.connectionForOwnerLegacy(sourceID, ownerID); c == nil {
		return []models.MediaSetSync{}, nil
	}
	return f.mediaSyncs[sourceID], nil
}
func (f *fakeStore) GetMediaSetSync(_ context.Context, id uuid.UUID, ownerID uuid.UUID) (*models.MediaSetSync, error) {
	for source, syncs := range f.mediaSyncs {
		if c := f.connectionForOwnerLegacy(source, ownerID); c == nil {
			continue
		}
		for i := range syncs {
			if syncs[i].ID == id {
				return &syncs[i], nil
			}
		}
	}
	return nil, nil
}
func (f *fakeStore) CreateMediaSetSync(_ context.Context, sourceID uuid.UUID, body *models.CreateMediaSetSyncRequest, ownerID uuid.UUID) (*models.MediaSetSync, error) {
	if c := f.connectionForOwnerLegacy(sourceID, ownerID); c == nil {
		return nil, nil
	}
	m := models.MediaSetSync{ID: uuid.New(), SourceID: sourceID, Kind: body.Kind, TargetMediaSetRID: body.TargetMediaSetRID, Subfolder: strings.Trim(body.Subfolder, "/"), Filters: body.Filters, ScheduleCron: body.ScheduleCron, CreatedAt: time.Now().UTC()}
	f.mediaSyncs[sourceID] = append([]models.MediaSetSync{m}, f.mediaSyncs[sourceID]...)
	return &m, nil
}
func (f *fakeStore) UpdateMediaSetSync(_ context.Context, id uuid.UUID, body *models.UpdateMediaSetSyncRequest, ownerID uuid.UUID) (*models.MediaSetSync, error) {
	for source, syncs := range f.mediaSyncs {
		if c := f.connectionForOwnerLegacy(source, ownerID); c == nil {
			continue
		}
		for i := range syncs {
			if syncs[i].ID == id {
				if body.Kind != nil {
					syncs[i].Kind = *body.Kind
				}
				if body.TargetMediaSetRID != nil {
					syncs[i].TargetMediaSetRID = *body.TargetMediaSetRID
				}
				if body.Subfolder != nil {
					syncs[i].Subfolder = strings.Trim(*body.Subfolder, "/")
				}
				if body.Filters != nil {
					syncs[i].Filters = *body.Filters
				}
				if body.ScheduleCron != nil {
					syncs[i].ScheduleCron = body.ScheduleCron
				}
				f.mediaSyncs[source] = syncs
				return &syncs[i], nil
			}
		}
	}
	return nil, nil
}

type fakeRuntime struct {
	report *models.MediaSetSyncExecutionReport
	err    error
	called bool
}

func (f *fakeRuntime) ExecuteMediaSetSync(_ context.Context, _ *models.MediaSetSync, _ *models.RunMediaSetSyncRequest, _ string) (*models.MediaSetSyncExecutionReport, error) {
	f.called = true
	return f.report, f.err
}

func authedReq(method, target, body string, sub uuid.UUID) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	return req.WithContext(authmw.ContextWithClaims(context.Background(), &authmw.Claims{Sub: sub}))
}

func withRouteParam(req *http.Request, key, val string) *http.Request {
	rctx := chi.RouteContext(req.Context())
	if rctx == nil {
		rctx = chi.NewRouteContext()
	}
	rctx.URLParams.Add(key, val)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func listenerHMAC(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(payload))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestConnectorAgentHandlersRegisterHeartbeatAndDelete(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}

	req := authedReq(http.MethodPost, "/agents", `{"name":"Edge bridge","agent_url":"https://agent.local:8443","capabilities":{"connectors":["postgres"]},"metadata":{"region":"eu"}}`, owner)
	rec := httptest.NewRecorder()
	h.RegisterConnectorAgent(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var created models.ConnectorAgent
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	require.Equal(t, "online", created.Status)
	require.NotNil(t, created.LastHeartbeatAt)

	req = authedReq(http.MethodGet, "/agents", ``, owner)
	rec = httptest.NewRecorder()
	h.ListConnectorAgents(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var listed models.ListResponse[models.ConnectorAgent]
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listed))
	require.Len(t, listed.Items, 1)

	req = withRouteParam(authedReq(http.MethodPost, "/agents/"+created.ID.String()+"/heartbeat", `{"capabilities":{"connectors":["postgres","mysql"]},"metadata":{"region":"eu"}}`, owner), "id", created.ID.String())
	rec = httptest.NewRecorder()
	h.HeartbeatConnectorAgent(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "mysql")

	req = withRouteParam(authedReq(http.MethodDelete, "/agents/"+created.ID.String(), ``, owner), "id", created.ID.String())
	rec = httptest.NewRecorder()
	h.DeleteConnectorAgent(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	require.Empty(t, store.agents)
}

func TestRegisterConnectorAgentRejectsInvalidPayloads(t *testing.T) {
	owner := uuid.New()
	h := &handlers.Handlers{Repo: newFakeStore(owner)}

	for name, body := range map[string]string{
		"missing name":     `{"agent_url":"https://agent.local"}`,
		"invalid url":      `{"name":"agent","agent_url":"agent.local"}`,
		"array metadata":   `{"name":"agent","agent_url":"https://agent.local","metadata":[]}`,
		"bad capabilities": `{"name":"agent","agent_url":"https://agent.local","capabilities":{`,
	} {
		t.Run(name, func(t *testing.T) {
			req := authedReq(http.MethodPost, "/agents", body, owner)
			rec := httptest.NewRecorder()
			h.RegisterConnectorAgent(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}

func TestSourcePolicyBindingHandlersMatchRustContract(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	sourceID := store.connections[0].ID
	policyID := uuid.New()
	h := &handlers.Handlers{Repo: store}

	req := withRouteParam(authedReq(http.MethodPost, "/sources/"+sourceID.String()+"/egress-policies", `{"policy_id":"`+policyID.String()+`"}`, owner), "id", sourceID.String())
	rec := httptest.NewRecorder()
	h.AttachPolicy(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var attached models.SourcePolicyBindingResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &attached))
	require.Equal(t, sourceID, attached.SourceID)
	require.Equal(t, policyID, attached.PolicyID)
	require.Equal(t, "direct", attached.Kind)

	req = withRouteParam(authedReq(http.MethodPost, "/sources/"+sourceID.String()+"/egress-policies", `{"policy_id":"`+policyID.String()+`","kind":"agent_proxy"}`, owner), "id", sourceID.String())
	rec = httptest.NewRecorder()
	h.AttachPolicy(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, store.policies[sourceID], 1)
	require.Equal(t, "agent_proxy", store.policies[sourceID][0].Kind)

	req = withRouteParam(authedReq(http.MethodGet, "/sources/"+sourceID.String()+"/egress-policies", ``, owner), "id", sourceID.String())
	rec = httptest.NewRecorder()
	h.ListSourcePolicies(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var listed []models.SourcePolicyBindingResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listed))
	require.Len(t, listed, 1)
	require.Equal(t, "agent_proxy", listed[0].Kind)

	req = withRouteParam(withRouteParam(authedReq(http.MethodDelete, "/sources/"+sourceID.String()+"/egress-policies/"+policyID.String(), ``, owner), "source_id", sourceID.String()), "policy_id", policyID.String())
	rec = httptest.NewRecorder()
	h.DetachPolicy(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	require.Empty(t, store.policies[sourceID])
}

func TestAttachPolicyRejectsEdgeCases(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	sourceID := store.connections[0].ID
	h := &handlers.Handlers{Repo: store}

	for name, body := range map[string]string{
		"nil policy id":     `{"policy_id":"00000000-0000-0000-0000-000000000000"}`,
		"unsupported kind":  `{"policy_id":"` + uuid.NewString() + `","kind":"bucket_endpoint"}`,
		"malformed payload": `{`,
	} {
		t.Run(name, func(t *testing.T) {
			req := withRouteParam(authedReq(http.MethodPost, "/sources/"+sourceID.String()+"/egress-policies", body, owner), "id", sourceID.String())
			rec := httptest.NewRecorder()
			h.AttachPolicy(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}

func TestCreateListGetUpdateSyncJobAndRun(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store, DatasetVersioning: &fakeCommitPort{}}
	source := store.connections[0].ID
	out := uuid.New()
	req := authedReq("POST", "/syncs", `{"source_id":"`+source.String()+`","output_dataset_id":"`+out.String()+`","file_glob":"*.csv"}`, owner)
	rec := httptest.NewRecorder()
	h.CreateSyncJob(rec, req)
	require.Equal(t, 201, rec.Code)
	var created models.SyncJob
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	assert.Equal(t, source, created.SourceID)

	req = withRouteParam(authedReq("GET", "/sources/"+source.String()+"/syncs", "", owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.ListSyncJobs(rec, req)
	require.Equal(t, 200, rec.Code)
	var list []models.SyncJob
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
	require.Len(t, list, 1)

	req = withRouteParam(authedReq("GET", "/syncs/"+created.ID.String(), "", owner), "sync_id", created.ID.String())
	rec = httptest.NewRecorder()
	h.GetSyncJob(rec, req)
	assert.Equal(t, 200, rec.Code)
	cron := "0 * * * *"
	req = withRouteParam(authedReq("PATCH", "/syncs/"+created.ID.String(), `{"schedule_cron":"`+cron+`"}`, owner), "sync_id", created.ID.String())
	rec = httptest.NewRecorder()
	h.UpdateSyncJob(rec, req)
	assert.Equal(t, 200, rec.Code)
	req = withRouteParam(authedReq("POST", "/syncs/"+created.ID.String()+"/run", "", owner), "sync_id", created.ID.String())
	rec = httptest.NewRecorder()
	h.RunSyncJob(rec, req)
	require.Equal(t, 202, rec.Code)
	var run models.SyncRun
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &run))
	assert.Equal(t, "failed", run.Status)
	require.NotNil(t, run.Error)
	assert.Contains(t, *run.Error, "postgresql")
	assert.Nil(t, run.DatasetVersionID, "unsupported connector types must not publish anything")
}

func TestCreateListGetVirtualTable(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}
	sourceRID := "ri.foundry.main.source." + uuid.NewString()
	req := withRouteParam(authedReq("POST", "/sources/enable", `{"provider":"BIGQUERY"}`, owner), "source_rid", sourceRID)
	rec := httptest.NewRecorder()
	h.EnableVirtualTableSource(rec, req)
	require.Equal(t, 200, rec.Code)
	body := `{"project_rid":"ri.project.main","locator":{"kind":"tabular","database":"db","schema":"public","table":"orders"},"table_type":"TABLE"}`
	req = withRouteParam(authedReq("POST", "/sources/virtual-tables", body, owner), "source_rid", sourceRID)
	rec = httptest.NewRecorder()
	h.CreateVirtualTable(rec, req)
	require.Equal(t, 201, rec.Code)
	var created models.VirtualTable
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	assert.Equal(t, "orders", created.Name)
	req = authedReq("GET", "/virtual-tables", "", owner)
	rec = httptest.NewRecorder()
	h.ListVirtualTables(rec, req)
	require.Equal(t, 200, rec.Code)
	var list models.ListVirtualTablesResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
	require.Len(t, list.Items, 1)
	req = withRouteParam(authedReq("GET", "/virtual-tables/"+created.RID, "", owner), "rid", created.RID)
	rec = httptest.NewRecorder()
	h.GetVirtualTable(rec, req)
	assert.Equal(t, 200, rec.Code)
}

func TestSyncAndVirtualValidationErrors(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}
	req := authedReq("POST", "/syncs", `{}`, owner)
	rec := httptest.NewRecorder()
	h.CreateSyncJob(rec, req)
	assert.Equal(t, 400, rec.Code)
	req = withRouteParam(authedReq("POST", "/sources/virtual-tables", `{"project_rid":"p","locator":{"kind":"bad"},"table_type":"TABLE"}`, owner), "source_rid", "missing")
	rec = httptest.NewRecorder()
	h.CreateVirtualTable(rec, req)
	assert.Equal(t, 404, rec.Code)
}

func TestSyncAndVirtualAuthTenantIsolation(t *testing.T) {
	owner := uuid.New()
	intruder := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}
	source := store.connections[0].ID
	out := uuid.New()
	created, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source, OutputDatasetID: out}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	req := withRouteParam(authedReq("GET", "/syncs/"+created.ID.String(), "", intruder), "sync_id", created.ID.String())
	rec := httptest.NewRecorder()
	h.GetSyncJob(rec, req)
	assert.Equal(t, 404, rec.Code)
	req = httptest.NewRequest("GET", "/virtual-tables", nil)
	rec = httptest.NewRecorder()
	h.ListVirtualTables(rec, req)
	assert.Equal(t, 401, rec.Code)
}

func TestCreateListGetUpdateMediaSetSync(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}
	source := store.connections[0].ID
	body := `{"kind":"MEDIA_SET_SYNC","target_media_set_rid":"ri.foundry.main.media_set.` + uuid.NewString() + `","subfolder":"images","filters":{"path_glob":"*.png","file_size_limit":1024},"schedule_cron":"0 * * * *"}`
	req := withRouteParam(authedReq("POST", "/sources/"+source.String()+"/media-set-syncs", body, owner), "id", source.String())
	rec := httptest.NewRecorder()
	h.CreateMediaSetSync(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)
	var created models.MediaSetSync
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	assert.Equal(t, source, created.SourceID)
	assert.Equal(t, models.MediaSetSyncKindCopy, created.Kind)

	req = withRouteParam(authedReq("GET", "/sources/"+source.String()+"/media-set-syncs", "", owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.ListMediaSetSyncs(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var list []models.MediaSetSync
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
	require.Len(t, list, 1)

	req = withRouteParam(authedReq("GET", "/media-set-syncs/"+created.ID.String(), "", owner), "sync_id", created.ID.String())
	rec = httptest.NewRecorder()
	h.GetMediaSetSync(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	patch := `{"kind":"VIRTUAL_MEDIA_SET_SYNC","subfolder":"archive"}`
	req = withRouteParam(authedReq("PATCH", "/media-set-syncs/"+created.ID.String(), patch, owner), "sync_id", created.ID.String())
	rec = httptest.NewRecorder()
	h.UpdateMediaSetSync(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var updated models.MediaSetSync
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, models.MediaSetSyncKindVirtual, updated.Kind)
	assert.Equal(t, "archive", updated.Subfolder)
}

func TestMediaSetSyncValidationErrors(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}
	source := store.connections[0].ID
	req := withRouteParam(authedReq("POST", "/sources/"+source.String()+"/media-set-syncs", `{"kind":"MEDIA_SET_SYNC","target_media_set_rid":"bad","filters":{"file_size_limit":0},"schedule_cron":"bad cron"}`, owner), "id", source.String())
	rec := httptest.NewRecorder()
	h.CreateMediaSetSync(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "target_media_set_rid")
	assert.Contains(t, rec.Body.String(), "file_size_limit")

	created, err := store.CreateMediaSetSync(context.Background(), source, &models.CreateMediaSetSyncRequest{Kind: models.MediaSetSyncKindCopy, TargetMediaSetRID: "ri.foundry.main.media_set." + uuid.NewString()}, owner)
	require.NoError(t, err)
	badKind := `{"kind":"BAD"}`
	req = withRouteParam(authedReq("PATCH", "/media-set-syncs/"+created.ID.String(), badKind, owner), "sync_id", created.ID.String())
	rec = httptest.NewRecorder()
	h.UpdateMediaSetSync(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestMediaSetSyncAuthTenantIsolation(t *testing.T) {
	owner := uuid.New()
	intruder := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}
	source := store.connections[0].ID
	created, err := store.CreateMediaSetSync(context.Background(), source, &models.CreateMediaSetSyncRequest{Kind: models.MediaSetSyncKindCopy, TargetMediaSetRID: "ri.foundry.main.media_set." + uuid.NewString()}, owner)
	require.NoError(t, err)

	req := httptest.NewRequest("GET", "/sources/"+source.String()+"/media-set-syncs", nil)
	req = withRouteParam(req, "id", source.String())
	rec := httptest.NewRecorder()
	h.ListMediaSetSyncs(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	req = withRouteParam(authedReq("GET", "/media-set-syncs/"+created.ID.String(), "", intruder), "sync_id", created.ID.String())
	rec = httptest.NewRecorder()
	h.GetMediaSetSync(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	req = withRouteParam(authedReq("POST", "/sources/"+source.String()+"/media-set-syncs", `{"kind":"MEDIA_SET_SYNC","target_media_set_rid":"ri.foundry.main.media_set.x"}`, intruder), "id", source.String())
	rec = httptest.NewRecorder()
	h.CreateMediaSetSync(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRunMediaSetSyncRuntimeErrorMapping(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	source := store.connections[0].ID
	created, err := store.CreateMediaSetSync(context.Background(), source, &models.CreateMediaSetSyncRequest{Kind: models.MediaSetSyncKindCopy, TargetMediaSetRID: "ri.foundry.main.media_set." + uuid.NewString()}, owner)
	require.NoError(t, err)
	rt := &fakeRuntime{err: &handlers.RuntimeError{Kind: handlers.RuntimeDispatch, Msg: "media-sets-service returned HTTP 500"}}
	h := &handlers.Handlers{Repo: store, MediaSetRuntime: rt}

	req := withRouteParam(authedReq("POST", "/media-set-syncs/"+created.ID.String()+"/run", `{"source_files":[{"path":"a.png","size_bytes":1,"mime_type":"image/png"}]}`, owner), "sync_id", created.ID.String())
	rec := httptest.NewRecorder()
	h.RunMediaSetSync(rec, req)
	assert.Equal(t, http.StatusBadGateway, rec.Code)
	assert.True(t, rt.called)
}

func TestRunMediaSetSyncRuntimeSuccess(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	source := store.connections[0].ID
	created, err := store.CreateMediaSetSync(context.Background(), source, &models.CreateMediaSetSyncRequest{Kind: models.MediaSetSyncKindCopy, TargetMediaSetRID: "ri.foundry.main.media_set." + uuid.NewString()}, owner)
	require.NoError(t, err)
	h := &handlers.Handlers{Repo: store, MediaSetRuntime: &fakeRuntime{report: &models.MediaSetSyncExecutionReport{Stats: models.SyncStats{Accepted: 1}, Dispatched: 1}}}

	req := withRouteParam(authedReq("POST", "/media-set-syncs/"+created.ID.String()+"/run", `{"source_files":[{"path":"a.png","size_bytes":1,"mime_type":"image/png"}]}`, owner), "sync_id", created.ID.String())
	rec := httptest.NewRecorder()
	h.RunMediaSetSync(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code)
	assert.Contains(t, rec.Body.String(), "dispatched")
}

func TestHTTPMediaSetRuntimeDispatchesAcceptedFiles(t *testing.T) {
	seen := []string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.Path)
		assert.Equal(t, "Bearer token", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	limit := uint64(10)
	glob := "*.png"
	sync := &models.MediaSetSync{
		Kind:              models.MediaSetSyncKindCopy,
		TargetMediaSetRID: "ri.foundry.main.media_set.x",
		Filters: models.MediaSetSyncFilters{
			PathGlob:      &glob,
			FileSizeLimit: &limit,
		},
	}
	req := &models.RunMediaSetSyncRequest{SourceFiles: []models.SourceFile{
		{Path: "ok.png", SizeBytes: 1, MimeType: "image/png"},
		{Path: "too-large.png", SizeBytes: 11, MimeType: "image/png"},
		{Path: "notes.txt", SizeBytes: 1, MimeType: "text/plain"},
	}, AllowedMIMETypes: []string{"image/png"}}
	rt := &handlers.HTTPMediaSetRuntime{MediaSetsBaseURL: srv.URL, Client: srv.Client()}
	report, err := rt.ExecuteMediaSetSync(context.Background(), sync, req, "Bearer token")
	require.NoError(t, err)
	require.Equal(t, uint32(1), report.Dispatched)
	require.Equal(t, uint32(1), report.Stats.Accepted)
	require.Equal(t, uint32(2), report.Stats.Skipped)
	require.Equal(t, []string{"/media-sets/ri.foundry.main.media_set.x/items/upload-url"}, seen)
}

func TestCatalogSurfaceMatchesGoldenFixtures(t *testing.T) {
	t.Parallel()
	h := &handlers.Handlers{}
	cases := []struct {
		name   string
		handle http.HandlerFunc
		golden string
	}{
		{name: "catalog", handle: h.GetConnectorCatalog, golden: "testdata/catalog.golden.json"},
		{name: "contracts", handle: h.GetConnectorContracts, golden: "testdata/contracts.golden.json"},
		{name: "streaming_sources", handle: h.ListStreamingSources, golden: "testdata/streaming_sources.golden.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.handle(rec, httptest.NewRequest(http.MethodGet, "/", nil))
			require.Equal(t, http.StatusOK, rec.Code)
			assertJSONGolden(t, tc.golden, rec.Body.Bytes())
		})
	}
}

func TestCatalogIncludesAllRustConnectorModules(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	(&handlers.Handlers{}).GetConnectorContracts(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var catalog models.ConnectorContractCatalog
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &catalog))
	byType := map[string]models.ConnectorContractProfile{}
	for _, connector := range catalog.Connectors {
		byType[connector.ConnectorType] = connector
	}
	for _, connectorType := range []string{"azure_blob", "bigquery", "csv", "databricks", "excel", "gcs", "generic", "graphql", "iot", "jdbc", "json", "kafka", "kinesis", "ldap", "mssql", "mysql", "odbc", "onelake", "open_table_catalog", "oracle", "parquet", "postgresql", "power_bi", "rest_api", "s3", "salesforce", "sap", "sftp", "snowflake", "tableau"} {
		require.Contains(t, byType, connectorType)
	}
}

func TestConnectionCapabilitiesCombineContractConfigAndPolicy(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	store := newFakeStore(owner)
	store.connections[0].ConnectorType = "snowflake"
	store.connections[0].Config = json.RawMessage(`{"account":"acct","private_key":"pk","cursor_field":"updated_at","zero_copy":true}`)
	h := &handlers.Handlers{Repo: store, Config: handlers.RuntimeConfig{AllowedEgressHosts: []string{"snowflake.example.com"}}}

	r := chi.NewRouter()
	r.Get("/api/v1/data-connection/sources/{id}/capabilities", h.GetConnectionCapabilities)
	rec := httptest.NewRecorder()
	req := authedReq(http.MethodGet, "/api/v1/data-connection/sources/"+store.connections[0].ID.String()+"/capabilities", "", owner)
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var got models.ConnectionCapabilityResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, "snowflake", got.ConnectorType)
	require.Equal(t, "warehouse_zero_copy", got.Contract.TemplateFamily)
	require.True(t, got.Capabilities.SupportsZeroCopy)
	require.True(t, got.Capabilities.SupportsIncremental)
	require.True(t, got.Capabilities.ConfigInferred.HasPrivateKey)
	require.True(t, got.Capabilities.ConfigInferred.HasIncrementalCursor)
	require.True(t, got.Capabilities.PrivateNetworkEgressAllowed)
	require.False(t, got.Capabilities.RequiresPrivateNetworkAgent)
	require.Contains(t, got.Capabilities.Workers, "agent")
	require.Contains(t, got.Capabilities.ConfigKeys, "private_key")
}

func assertJSONGolden(t *testing.T, golden string, got []byte) {
	t.Helper()
	want, err := os.ReadFile(golden)
	require.NoError(t, err)
	var wantJSON any
	var gotJSON any
	require.NoError(t, json.Unmarshal(want, &wantJSON))
	require.NoError(t, json.Unmarshal(got, &gotJSON))
	assert.Equal(t, wantJSON, gotJSON)
}

func TestRegistrationHandlerFlow(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	source := store.connections[0].ID
	store.connections[0].Config = json.RawMessage(`{"tables":[{"selector":"public.orders","display_name":"Orders","source_kind":"table","supports_zero_copy":true}]}`)
	h := &handlers.Handlers{Repo: store}

	req := withRouteParam(authedReq(http.MethodPost, "/discover", ``, owner), "id", source.String())
	rec := httptest.NewRecorder()
	h.DiscoverRegistrations(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "public.orders")

	body := `{"registrations":[{"selector":"public.orders","registration_mode":"zero_copy","auto_sync":true}]}`
	req = withRouteParam(authedReq(http.MethodPost, "/bulk/preview", body, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.BulkRegisterPreview(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "matched")

	req = withRouteParam(authedReq(http.MethodPost, "/bulk", body, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.BulkRegister(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	reg := store.registrations[source][0]

	req = withRouteParam(authedReq(http.MethodGet, "/registrations", ``, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.ListRegistrations(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	req = withRouteParam(withRouteParam(authedReq(http.MethodPost, "/query", `{"limit":1}`, owner), "source_id", source.String()), "registration_id", reg.ID.String())
	rec = httptest.NewRecorder()
	h.QueryRegistration(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	req = withRouteParam(withRouteParam(authedReq(http.MethodPost, "/query/arrow", `{"limit":1}`, owner), "source_id", source.String()), "registration_id", reg.ID.String())
	rec = httptest.NewRecorder()
	h.QueryRegistrationArrow(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "application/vnd.apache.arrow.stream", rec.Header().Get("Content-Type"))

	req = withRouteParam(withRouteParam(authedReq(http.MethodDelete, "/registrations/"+reg.ID.String(), ``, owner), "source_id", source.String()), "registration_id", reg.ID.String())
	rec = httptest.NewRecorder()
	h.DeleteRegistration(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
}

func TestAutoRegistrationHandlers(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	source := store.connections[0].ID
	h := &handlers.Handlers{Repo: store}

	req := withRouteParam(authedReq(http.MethodPut, "/auto", `{"enabled":true,"registration_mode":"sync","selectors":["pg"]}`, owner), "id", source.String())
	rec := httptest.NewRecorder()
	h.UpdateAutoRegistration(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	req = withRouteParam(authedReq(http.MethodGet, "/auto/status", ``, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.AutoRegisterStatus(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "settings")

	req = withRouteParam(authedReq(http.MethodPost, "/auto", `{}`, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.AutoRegister(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.NotEmpty(t, store.registrations[source])
}

func TestConnectionWebhookAndIcebergHandlers(t *testing.T) {
	owner := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}
	source := store.connections[0].ID

	req := withRouteParam(authedReq(http.MethodPost, "/test", ``, owner), "id", source.String())
	rec := httptest.NewRecorder()
	h.TestConnection(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"output_parameters":{"ok":true}}`))
	}))
	defer srv.Close()
	store.connections[0].ConnectorType = "webhook"
	store.connections[0].Config = json.RawMessage(`{"url":"` + srv.URL + `","method":"POST"}`)
	req = withRouteParam(authedReq(http.MethodPost, "/webhooks/"+source.String()+"/invoke", `{"inputs":{"x":1}}`, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.InvokeWebhook(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "output_parameters")

	srvWeather := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/forecast", r.URL.Path)
		if r.URL.Query().Has("latitude") {
			assert.Equal(t, "40.016353", r.URL.Query().Get("latitude"))
			assert.Equal(t, "-105.34458", r.URL.Query().Get("longitude"))
			assert.Equal(t, "temperature_2m,wind_speed_10m,relative_humidity_2m", r.URL.Query().Get("current"))
		}
		_, _ = w.Write([]byte(`{"current":{"temperature_2m":84,"wind_speed_10m":4.8,"relative_humidity_2m":62}}`))
	}))
	defer srvWeather.Close()
	store.connections[0].ConnectorType = "rest_api"
	store.connections[0].Config = json.RawMessage(`{
		"base_url":"` + srvWeather.URL + `",
		"auth":{"type":"none"},
		"runtime":{"worker":"foundry","timeout_ms":5000},
		"permissions":{"invokable":true},
		"webhook":{
			"method":"GET",
			"path":"/v1/forecast",
			"inputs":[
				{"id":"latitude","type":"number","required":true},
				{"id":"longitude","type":"number","required":true}
			],
			"calls":[{
				"id":"weather",
				"method":"GET",
				"path":"/v1/forecast",
				"query_params":{
					"latitude":"{{latitude}}",
					"longitude":"{{longitude}}",
					"current":"temperature_2m,wind_speed_10m,relative_humidity_2m"
				}
			}],
			"outputs":[
				{"id":"temperature","type":"number","extractor":{"from_call":"weather","path":"/current/temperature_2m"}},
				{"id":"wind_speed","type":"number","extractor":{"from_call":"weather","path":"/current/wind_speed_10m"}},
				{"id":"humidity","type":"number","extractor":{"from_call":"weather","path":"/current/relative_humidity_2m"}}
			],
			"history":{"enabled":true,"retention_days":14,"store_outputs":true}
		}
	}`)
	req = withRouteParam(authedReq(http.MethodPost, "/webhooks/"+source.String()+"/invoke", `{"inputs":{"latitude":40.016353,"longitude":-105.34458}}`, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.InvokeWebhook(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"temperature":84`)
	assert.Contains(t, rec.Body.String(), `"wind_speed":4.8`)
	assert.Contains(t, rec.Body.String(), `"humidity":62`)
	var weatherResp models.InvokeWebhookResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &weatherResp))
	var weatherHistory map[string]any
	require.NoError(t, json.Unmarshal(weatherResp.History, &weatherHistory))
	assert.Equal(t, "succeeded", weatherHistory["status"])
	assert.Equal(t, true, weatherHistory["stored"])
	assert.NotEmpty(t, weatherHistory["entry_id"])

	req = withRouteParam(authedReq(http.MethodGet, "/webhooks/"+source.String()+"/history?limit=10", ``, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.ListWebhookHistory(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var historyList models.ListResponse[models.WebhookHistoryEntry]
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &historyList))
	require.NotEmpty(t, historyList.Items)
	weatherEntry := historyList.Items[0]
	assert.Equal(t, source, weatherEntry.SourceID)
	assert.Equal(t, owner, weatherEntry.UserID)
	assert.Equal(t, "succeeded", weatherEntry.Status)
	assert.NotNil(t, weatherEntry.HTTPStatus)
	assert.Equal(t, uint16(http.StatusOK), *weatherEntry.HTTPStatus)
	assert.True(t, weatherEntry.InputPolicy.StoreOutputs)
	assert.False(t, weatherEntry.InputPolicy.StoreInputs)
	assert.Nil(t, weatherEntry.Inputs)
	assert.JSONEq(t, `{"temperature":84,"wind_speed":4.8,"humidity":62}`, string(weatherEntry.OutputParameters))
	assert.WithinDuration(t, time.Now().UTC().Add(14*24*time.Hour), weatherEntry.RetentionExpiresAt, time.Minute)

	store.connections[0].Config = json.RawMessage(`{
		"base_url":"` + srvWeather.URL + `",
		"auth":{"type":"none"},
		"runtime":{"worker":"foundry","allowed_methods":["GET"]},
		"permissions":{"invokable":false},
		"webhook":{"method":"GET","path":"/v1/forecast"}
	}`)
	req = withRouteParam(authedReq(http.MethodPost, "/webhooks/"+source.String()+"/invoke", `{"inputs":{}}`, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.InvokeWebhook(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())

	store.connections[0].Config = json.RawMessage(`{
		"base_url":"` + srvWeather.URL + `",
		"auth":{"type":"none"},
		"runtime":{"worker":"foundry","allowed_methods":["POST"]},
		"permissions":{"invokable":true},
		"webhook":{"method":"GET","path":"/v1/forecast"}
	}`)
	req = withRouteParam(authedReq(http.MethodPost, "/webhooks/"+source.String()+"/invoke", `{"inputs":{}}`, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.InvokeWebhook(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "method GET")

	req = withRouteParam(authedReq(http.MethodGet, "/webhooks/"+source.String()+"/history?limit=1", ``, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.ListWebhookHistory(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &historyList))
	require.Len(t, historyList.Items, 1)
	assert.Equal(t, "failed", historyList.Items[0].Status)
	require.NotNil(t, historyList.Items[0].Error)
	assert.Contains(t, *historyList.Items[0].Error, "method GET")
	assert.Nil(t, historyList.Items[0].Inputs)

	srvLarge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "super-secret", r.URL.Query().Get("api_key"))
		_, _ = w.Write([]byte(`{"large":"this payload is intentionally larger than the configured limit"}`))
	}))
	defer srvLarge.Close()
	store.connections[0].Config = json.RawMessage(`{
		"base_url":"` + srvLarge.URL + `",
		"auth":{"type":"api_key","query_param":"api_key","value":"super-secret"},
		"runtime":{"worker":"foundry","allowed_methods":["GET"]},
		"permissions":{"invokable":true},
		"webhook":{
			"method":"GET",
			"path":"/v1/forecast",
			"limits":{"max_response_bytes":16}
		}
	}`)
	req = withRouteParam(authedReq(http.MethodPost, "/webhooks/"+source.String()+"/invoke", `{"inputs":{}}`, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.InvokeWebhook(rec, req)
	require.Equal(t, http.StatusBadGateway, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "max_response_bytes")
	assert.NotContains(t, rec.Body.String(), "super-secret")

	store.connections[0].Config = json.RawMessage(`{
		"base_url":"` + srvWeather.URL + `",
		"auth":{"type":"none"},
		"runtime":{"worker":"foundry","allowed_methods":["GET"]},
		"permissions":{"invokable":true},
		"webhook":{
			"method":"GET",
			"path":"/v1/forecast",
			"rate_limit":{"max_requests":1,"per_seconds":60}
		}
	}`)
	req = withRouteParam(authedReq(http.MethodPost, "/webhooks/"+source.String()+"/invoke", `{"inputs":{}}`, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.InvokeWebhook(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	req = withRouteParam(authedReq(http.MethodPost, "/webhooks/"+source.String()+"/invoke", `{"inputs":{}}`, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.InvokeWebhook(rec, req)
	require.Equal(t, http.StatusTooManyRequests, rec.Code, rec.Body.String())

	// WP2.1 tenant isolation: a different personal-domain caller is OUTSIDE
	// the owner's tenant, so the invoke is a 404 before any side effect —
	// not a 403 that would confirm the webhook exists.
	req = withRouteParam(authedReq(http.MethodPost, "/webhooks/"+source.String()+"/invoke", `{"inputs":{}}`, uuid.New()), "id", source.String())
	rec = httptest.NewRecorder()
	h.InvokeWebhook(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())

	trailObjectTypeID := uuid.New()
	store.connections[0].ConnectorType = "rest_api"
	store.connections[0].Config = json.RawMessage(`{
		"base_url":"https://ingest.example.test",
		"auth":{"type":"none"},
		"runtime":{"worker":"foundry"},
		"permissions":{"invokable":true},
		"listener":{
			"id":"trail-events",
			"type":"https",
			"enabled":true,
			"auth":{"type":"hmac_sha256","header":"X-OpenFoundry-Signature","secret":"listener-secret"},
			"destination":{"mode":"object","object_type_id":"` + trailObjectTypeID.String() + `"},
			"limits":{"max_payload_bytes":4096}
		}
	}`)
	listenerPayload := `{"event_id":"evt-trail-1","trail_id":"mule-deer","distance_miles":8.8}`
	req = withRouteParam(withRouteParam(httptest.NewRequest(http.MethodPost, "/api/v1/data-connection/sources/"+source.String()+"/listeners/trail-events/events", strings.NewReader(listenerPayload)), "source_id", source.String()), "listener_id", "trail-events")
	req.Header.Set("X-OpenFoundry-Signature", listenerHMAC("listener-secret", listenerPayload))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	h.ReceiveInboundListener(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	var listenerResp models.ReceiveInboundListenerResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listenerResp))
	assert.Equal(t, source, listenerResp.SourceID)
	assert.Equal(t, "trail-events", listenerResp.ListenerID)
	assert.True(t, listenerResp.SignatureVerified)
	assert.Equal(t, "object", listenerResp.Destination.Mode)
	assert.Equal(t, trailObjectTypeID, *listenerResp.Destination.ObjectTypeID)

	req = withRouteParam(authedReq(http.MethodGet, "/api/v1/data-connection/sources/"+source.String()+"/listener-events?limit=10", ``, owner), "id", source.String())
	rec = httptest.NewRecorder()
	h.ListInboundListenerEvents(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var listenerHistory models.ListResponse[models.InboundListenerEvent]
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listenerHistory))
	require.Len(t, listenerHistory.Items, 1)
	assert.Equal(t, "evt-trail-1", listenerHistory.Items[0].EventID)
	assert.JSONEq(t, listenerPayload, string(listenerHistory.Items[0].Payload))
	assert.Contains(t, string(listenerHistory.Items[0].Headers), "[redacted]")

	req = withRouteParam(withRouteParam(httptest.NewRequest(http.MethodPost, "/api/v1/data-connection/sources/"+source.String()+"/listeners/trail-events/events", strings.NewReader(listenerPayload)), "source_id", source.String()), "listener_id", "trail-events")
	req.Header.Set("X-OpenFoundry-Signature", "sha256=bad")
	rec = httptest.NewRecorder()
	h.ReceiveInboundListener(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())

	store.connections[0].ConnectorType = "postgresql"
	store.registrations[source] = []models.ConnectionRegistration{{ID: uuid.New(), ConnectionID: source, Selector: "public.orders", DisplayName: "Orders", SourceKind: "table", RegistrationMode: "zero_copy", Metadata: json.RawMessage(`{"supports_zero_copy":true}`)}}
	req = authedReq(http.MethodGet, "/iceberg/v1/config", ``, owner)
	rec = httptest.NewRecorder()
	h.IcebergGetConfig(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	req = authedReq(http.MethodGet, "/iceberg/v1/namespaces", ``, owner)
	rec = httptest.NewRecorder()
	h.IcebergListNamespaces(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	req = withRouteParam(authedReq(http.MethodGet, "/iceberg/v1/namespaces/pg", ``, owner), "namespace", "pg")
	rec = httptest.NewRecorder()
	h.IcebergGetNamespace(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	req = withRouteParam(authedReq(http.MethodGet, "/iceberg/v1/namespaces/pg/tables", ``, owner), "namespace", "pg")
	rec = httptest.NewRecorder()
	h.IcebergListTables(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	req = withRouteParam(withRouteParam(authedReq(http.MethodGet, "/iceberg/v1/namespaces/pg/tables/public.orders", ``, owner), "namespace", "pg"), "table", "public.orders")
	rec = httptest.NewRecorder()
	h.IcebergLoadTable(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}
