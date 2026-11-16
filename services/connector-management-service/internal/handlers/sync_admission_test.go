// P2.3 per-source sync admission: handler-level unit coverage for the typed
// 429 mapping and the write-surface validation of the sync.admission config
// subtree. The durable protocol (locking, counting, interval stamping) lives
// in the real-PG admission integration suite.
package handlers_test

import (
	"context"
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

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
)

// newAdmissionHandlers wires a handler set with an EXPLICIT coordinator (the
// old async-worker test pattern) so every case's worker+sweeper are joined by
// a bounded t.Cleanup Stop instead of leaking on the lazy background default.
func newAdmissionHandlers(t *testing.T, store *fakeStore) *handlers.Handlers {
	t.Helper()
	h := &handlers.Handlers{Repo: store}
	h.Coordinator = handlers.NewRunCoordinator(context.Background(),
		handlers.CoordinatorConfig{StopBudget: 5 * time.Second}, h)
	t.Cleanup(func() { _ = h.Coordinator.Stop() })
	return h
}

// TestRunSyncJobSyncAdmissionThrottleResponses pins the whole 429 contract in
// BOTH trigger modes (default synchronous and ?async=true): stable code,
// discriminating reason, retry_after_seconds consistent with the Retry-After
// header, the repo's static sentence as message, zero rows (the fake store's
// enqueue error path precedes any run creation) and the local reservation
// returned.
func TestRunSyncJobSyncAdmissionThrottleResponses(t *testing.T) {
	cases := []struct {
		name         string
		rejection    *repo.AdmissionRejection
		wantReason   string
		wantSeconds  int
		wantSentence string
	}{
		{
			name:         "capacity",
			rejection:    &repo.AdmissionRejection{Reason: repo.AdmissionReasonCapacity, RetryAfter: time.Second},
			wantReason:   "capacity",
			wantSeconds:  1,
			wantSentence: "source sync admission refused: the source already holds its maximum number of pending or running sync runs",
		},
		{
			name:         "min_interval",
			rejection:    &repo.AdmissionRejection{Reason: repo.AdmissionReasonMinInterval, RetryAfter: 3 * time.Second},
			wantReason:   "min_interval",
			wantSeconds:  3,
			wantSentence: "source sync admission refused: the minimum interval since the last admitted run has not elapsed",
		},
		{
			name:         "contention",
			rejection:    &repo.AdmissionRejection{Reason: repo.AdmissionReasonContention, RetryAfter: time.Second},
			wantReason:   "contention",
			wantSeconds:  1,
			wantSentence: "source sync admission refused: the source is busy with another writer holding its lock; retry shortly",
		},
		{
			name:        "sub-second retry hint clamps to one second",
			rejection:   &repo.AdmissionRejection{Reason: repo.AdmissionReasonContention, RetryAfter: 400 * time.Millisecond},
			wantReason:  "contention",
			wantSeconds: 1,
		},
	}
	for _, mode := range []string{"sync", "async"} {
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				owner := uuid.New()
				store := newFakeStore(owner)
				job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: store.connections[0].ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
				require.NoError(t, err)
				store.enqueueErr = tc.rejection
				h := newAdmissionHandlers(t, store)

				target := "/syncs/" + job.ID.String() + "/run"
				if mode == "async" {
					target += "?async=true"
				}
				req := withRouteParam(authedReq(http.MethodPost, target, "", owner), "sync_id", job.ID.String())
				rec := httptest.NewRecorder()
				h.RunSyncJob(rec, req)

				require.Equal(t, http.StatusTooManyRequests, rec.Code, rec.Body.String())
				assert.Equal(t, strconv.Itoa(tc.wantSeconds), rec.Header().Get("Retry-After"))
				var body struct {
					Error             string `json:"error"`
					Code              string `json:"code"`
					Reason            string `json:"reason"`
					Message           string `json:"message"`
					RetryAfterSeconds int    `json:"retry_after_seconds"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "source_sync_throttled", body.Error)
				assert.Equal(t, "source_sync_throttled", body.Code)
				assert.Equal(t, tc.wantReason, body.Reason)
				assert.Equal(t, tc.wantSeconds, body.RetryAfterSeconds)
				if tc.wantSentence != "" {
					assert.Equal(t, tc.wantSentence, body.Message)
				}
				// The refusal is a static sentence: no credential, JWT or lease
				// material can appear in it.
				lower := strings.ToLower(rec.Body.String())
				assert.NotContains(t, lower, "bearer ")
				assert.NotContains(t, lower, "jwt")
				assert.NotContains(t, lower, "lease_token")
				// Zero durable rows: the store's error path precedes run creation.
				assert.Empty(t, store.runs[job.ID])
				// The local reservation was returned: nothing stays admitted.
				assert.Equal(t, 0, h.Coordinator.OutstandingAdmissions())
			})
		}
	}
}

// TestConnectionWriteSurfacesValidateSyncAdmission pins that BOTH write
// surfaces (create and update) reject an illegal sync.admission subtree with
// 400 and no mutation, for the passthrough (non-rest_api) types as well as
// the typed rest_api config.
func TestConnectionWriteSurfacesValidateSyncAdmission(t *testing.T) {
	owner := uuid.New()

	// doCreate returns the store so refused cases can prove ZERO creations
	// and unchanged originals, not just the 400 status.
	doCreate := func(t *testing.T, connectorType, config string) (*httptest.ResponseRecorder, *fakeStore) {
		t.Helper()
		store := newFakeStore(owner)
		h := &handlers.Handlers{Repo: store}
		body := `{"name":"src","connector_type":` + strconv.Quote(connectorType) + `,"config":` + config + `}`
		rec := httptest.NewRecorder()
		h.CreateConnection(rec, authedReq(http.MethodPost, "/connections", body, owner))
		return rec, store
	}

	t.Run("postgresql create rejects illegal admission with zero mutation", func(t *testing.T) {
		rec, store := doCreate(t, "postgresql", `{"sync":{"admission":{"max_inflight_runs":0}}}`)
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		assert.Len(t, store.connections, 1, "a refused create must not append a connection")
		assert.Equal(t, json.RawMessage(`{}`), store.connections[0].Config,
			"the pre-existing connection's config must be untouched")
	})
	t.Run("postgresql create accepts and passes through a valid admission", func(t *testing.T) {
		rec, store := doCreate(t, "postgresql", `{"sync":{"admission":{"max_inflight_runs":2,"min_interval_ms":1000}}}`)
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
		assert.Len(t, store.connections, 2)
		var created models.Connection
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
		p, err := models.ParseSyncAdmissionPolicy(created.Config)
		require.NoError(t, err)
		require.NotNil(t, p)
		require.NotNil(t, p.MaxInflightRuns)
		assert.Equal(t, 2, *p.MaxInflightRuns)
	})

	t.Run("rest_api create rejects illegal admission with zero mutation", func(t *testing.T) {
		rec, store := doCreate(t, "rest_api", `{"base_url":"https://api.example.test","sync":{"resource_path":"/items","admission":{"min_interval_ms":-1}}}`)
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		assert.Len(t, store.connections, 1, "a refused create must not append a connection")
	})
	t.Run("rest_api create roundtrips a valid admission", func(t *testing.T) {
		rec, store := doCreate(t, "rest_api", `{"base_url":"https://api.example.test","sync":{"resource_path":"/items","admission":{"max_inflight_runs":4,"min_interval_ms":1500}}}`)
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
		assert.Len(t, store.connections, 2)
		var created models.Connection
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
		p, err := models.ParseSyncAdmissionPolicy(created.Config)
		require.NoError(t, err)
		assert.True(t, p.Enabled())
	})

	t.Run("update rejects illegal admission without mutating the source", func(t *testing.T) {
		store := newFakeStore(owner)
		before := store.connections[0].Config
		h := &handlers.Handlers{Repo: store}
		body := `{"config":{"sync":{"admission":{"max_inflight_runs":4097}}}}`
		req := withRouteParam(authedReq(http.MethodPatch, "/connections/"+store.connections[0].ID.String(), body, owner), "id", store.connections[0].ID.String())
		rec := httptest.NewRecorder()
		h.UpdateConnection(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		assert.Equal(t, before, store.connections[0].Config, "a refused update must not change the stored config")
	})
}
