package handlers_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/domain"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
	cmruntime "github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/runtime"
)

// fakeCommitPort captures the publish requests issued by the sync run path.
type fakeCommitPort struct {
	mu          sync.Mutex
	requests    []cmruntime.DatasetCommitRequest
	auths       []string
	err         error
	txn         uuid.UUID
	getErr      error
	seenGets    []uuid.UUID
	seenGetAuth []string
	block       chan struct{} // P2.2: when non-nil, CommitOutput waits here
}

func (f *fakeCommitPort) CommitOutput(ctx context.Context, req cmruntime.DatasetCommitRequest, authorization string) (*cmruntime.DatasetCommitResult, error) {
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.auths = append(f.auths, authorization)
	err := f.err
	block := f.block
	txn := f.txn
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	if txn == uuid.Nil {
		txn = uuid.New()
	}
	return &cmruntime.DatasetCommitResult{DatasetID: req.DatasetID, TransactionID: txn}, nil
}

func (f *fakeCommitPort) snapshot() ([]cmruntime.DatasetCommitRequest, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]cmruntime.DatasetCommitRequest(nil), f.requests...), append([]string(nil), f.auths...)
}

func (f *fakeCommitPort) GetDataset(_ context.Context, id uuid.UUID, authorization string) (*cmruntime.DatasetInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seenGets = append(f.seenGets, id)
	f.seenGetAuth = append(f.seenGetAuth, authorization)
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &cmruntime.DatasetInfo{ID: id, Name: "target"}, nil
}

func (f *fakeCommitPort) gets() ([]uuid.UUID, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uuid.UUID(nil), f.seenGets...), append([]string(nil), f.seenGetAuth...)
}

// syncUpstream is a page/cursor REST source that records the requests it saw.
type syncUpstream struct {
	mu         sync.Mutex
	bodies     []string
	statuses   map[int]int
	authSeen   []string
	requestLog []string
	mutate     func(page int) string
}

func (u *syncUpstream) handler(w http.ResponseWriter, r *http.Request) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.requestLog = append(u.requestLog, r.URL.String())
	if auth := r.Header.Get("Authorization"); auth != "" {
		u.authSeen = append(u.authSeen, auth)
	}
	idx := len(u.requestLog)
	if status, ok := u.statuses[idx]; ok {
		w.WriteHeader(status)
		return
	}
	body := ""
	if u.mutate != nil {
		body = u.mutate(idx)
	} else if idx-1 < len(u.bodies) {
		body = u.bodies[idx-1]
	} else {
		body = `{"data":[]}`
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(body))
}

func (u *syncUpstream) requests() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.requestLog...)
}

func (u *syncUpstream) authHeaders() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.authSeen...)
}

func restSyncSourceConfig(baseURL string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{
		"base_url": %q,
		"auth": {"type": "bearer", "secret_ref": "oauth_token"},
		"sync": {
			"resource_path": "/items",
			"records_path": "data",
			"pagination": {"type": "page", "page_size": 2},
			"limits": {"max_pages": 10, "max_records": 100}
		}
	}`, baseURL))
}

func newSyncTestStore(t *testing.T, owner uuid.UUID, config json.RawMessage) (*fakeStore, *models.Connection) {
	t.Helper()
	store := newFakeStore(owner)
	store.connections[0].ConnectorType = "rest_api"
	store.connections[0].Config = config
	return store, &store.connections[0]
}

func newSyncHandlers(store *fakeStore, committer cmruntime.DatasetVersioningPort) *handlers.Handlers {
	var key [32]byte
	copy(key[:], "sync-run-test-key-32-bytes-xxxxxxxxx")
	return &handlers.Handlers{
		Repo:              store,
		DatasetVersioning: committer,
		Config: handlers.RuntimeConfig{
			AllowPrivateNetworkEgress: true,
			CredentialKey:             key,
		},
	}
}

// storeSourceCredential encrypts a secret into the fake store the same way
// SetCredential does.
func storeSourceCredential(t *testing.T, store *fakeStore, sourceID uuid.UUID, kind, value string) {
	t.Helper()
	key := newSyncHandlers(store, nil).Config.CredentialKey
	ciphertext, err := domain.EncryptCredential(key, []byte(value))
	require.NoError(t, err)
	if store.credentials == nil {
		store.credentials = map[uuid.UUID]map[string][]byte{}
	}
	if store.credentials[sourceID] == nil {
		store.credentials[sourceID] = map[string][]byte{}
	}
	store.credentials[sourceID][kind] = ciphertext
}

func runSync(t *testing.T, h *handlers.Handlers, jobID, owner uuid.UUID, authorization string) models.SyncRun {
	t.Helper()
	req := withRouteParam(authedReq(http.MethodPost, "/syncs/"+jobID.String()+"/run", "", owner), "sync_id", jobID.String())
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	rec := httptest.NewRecorder()
	h.RunSyncJob(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	var run models.SyncRun
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &run))
	return run
}

func TestRunSyncJobRealRestSnapshotPublishesRealContent(t *testing.T) {
	owner := uuid.New()
	upstream := &syncUpstream{bodies: []string{
		`{"data":[{"sku":"00100","qty":1},{"sku":"00200","qty":2}]}`,
		`{"data":[{"sku":"00300","qty":3},{"sku":"00400","qty":4}]}`,
		`{"data":[{"sku":"00500","qty":5},{"sku":"00600","qty":6}]}`,
	}}
	sourceServer := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer sourceServer.Close()

	store, source := newSyncTestStore(t, owner, restSyncSourceConfig(sourceServer.URL))
	storeSourceCredential(t, store, source.ID, "oauth_token", "source-secret-token")
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	h := newSyncHandlers(store, committer)

	run := runSync(t, h, job.ID, owner, "Bearer caller-platform-jwt")

	require.Equal(t, "succeeded", run.Status)
	expectedNDJSON := strings.Join([]string{
		`{"sku":"00100","qty":1}`, `{"sku":"00200","qty":2}`,
		`{"sku":"00300","qty":3}`, `{"sku":"00400","qty":4}`,
		`{"sku":"00500","qty":5}`, `{"sku":"00600","qty":6}`,
	}, "\n") + "\n"
	assert.Equal(t, int64(6), run.RowsWritten)
	assert.Equal(t, int64(len(expectedNDJSON)), run.BytesWritten)
	assert.Equal(t, int64(1), run.FilesWritten)
	digest := sha256.Sum256([]byte(expectedNDJSON))
	assert.Equal(t, fmt.Sprintf("%x", digest[:]), *run.ContentHash)
	require.NotNil(t, run.DatasetVersionID, "run must reference the published transaction")

	requests, auths := committer.snapshot()
	require.Len(t, requests, 1)
	commit := requests[0]
	assert.Equal(t, job.OutputDatasetID, commit.DatasetID)
	assert.Equal(t, expectedNDJSON, string(commit.Content), "published content must be the exact NDJSON")
	assert.Equal(t, int64(len(expectedNDJSON)), int64(len(commit.Content)))
	assert.Equal(t, "TEXT", commit.Format)
	assert.True(t, strings.HasSuffix(commit.LogicalPath, ".ndjson"), commit.LogicalPath)
	assert.Equal(t, int64(6), commit.RowCount)
	assert.Equal(t, *run.ContentHash, commit.ContentHash)
	require.Len(t, auths, 1)
	assert.Equal(t, "Bearer caller-platform-jwt", auths[0], "caller JWT must be forwarded to dataset-versioning")

	// The upstream REST source sees the decrypted source credential — never
	// the platform JWT.
	upstreamAuth := upstream.authHeaders()
	require.NotEmpty(t, upstreamAuth)
	for _, auth := range upstreamAuth {
		assert.Equal(t, "Bearer source-secret-token", auth)
	}
	require.Len(t, upstream.requests(), 4, "three data pages plus the empty terminator")
}

func TestRunSyncJobContentChangeChangesHash(t *testing.T) {
	owner := uuid.New()
	current := map[int]string{
		1: `{"data":[{"sku":"00100","qty":1},{"sku":"00200","qty":2}]}`,
		2: `{"data":[{"sku":"00300","qty":3},{"sku":"00400","qty":4}]}`,
		3: `{"data":[]}`,
	}
	upstream := &syncUpstream{mutate: func(page int) string { return current[page] }}
	sourceServer := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer sourceServer.Close()

	store, source := newSyncTestStore(t, owner, restSyncSourceConfig(sourceServer.URL))
	storeSourceCredential(t, store, source.ID, "oauth_token", "tok")
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	h := newSyncHandlers(store, committer)

	first := runSync(t, h, job.ID, owner, "Bearer jwt")
	require.Equal(t, "succeeded", first.Status)

	// Same configuration, changed upstream content (fresh request counter).
	upstream.mu.Lock()
	upstream.requestLog = nil
	current[2] = `{"data":[{"sku":"00300","qty":99},{"sku":"00400","qty":4}]}`
	upstream.mu.Unlock()
	second := runSync(t, h, job.ID, owner, "Bearer jwt")
	require.Equal(t, "succeeded", second.Status)

	assert.NotEqual(t, *first.ContentHash, *second.ContentHash, "hash must follow the real source content")
	assert.Equal(t, first.RowsWritten, second.RowsWritten)
	requests, _ := committer.snapshot()
	require.Len(t, requests, 2, "each snapshot run publishes its own version — no cross-run no-op reuse")
}

func TestRunSyncJobSecondPageFailureFailsWithoutPublish(t *testing.T) {
	owner := uuid.New()
	upstream := &syncUpstream{bodies: []string{`{"data":[{"i":1}]}`}, statuses: map[int]int{2: 503}}
	sourceServer := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer sourceServer.Close()

	store, source := newSyncTestStore(t, owner, restSyncSourceConfig(sourceServer.URL))
	storeSourceCredential(t, store, source.ID, "oauth_token", "tok")
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	h := newSyncHandlers(store, committer)

	run := runSync(t, h, job.ID, owner, "Bearer jwt")
	assert.Equal(t, "failed", run.Status)
	require.NotNil(t, run.Error)
	assert.Contains(t, *run.Error, "503")
	assert.Nil(t, run.DatasetVersionID)
	requests, _ := committer.snapshot()
	assert.Empty(t, requests, "no half-finished snapshot may be published")
}

func TestRunSyncJobInvalidJSONFailsWithoutPublish(t *testing.T) {
	owner := uuid.New()
	upstream := &syncUpstream{bodies: []string{`{"data":[{"i":broken`}}
	sourceServer := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer sourceServer.Close()

	store, source := newSyncTestStore(t, owner, restSyncSourceConfig(sourceServer.URL))
	storeSourceCredential(t, store, source.ID, "oauth_token", "tok")
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	h := newSyncHandlers(store, committer)

	run := runSync(t, h, job.ID, owner, "")
	assert.Equal(t, "failed", run.Status)
	require.NotNil(t, run.Error)
	assert.Contains(t, *run.Error, "records_path")
	requests, _ := committer.snapshot()
	assert.Empty(t, requests)
}

func TestRunSyncJobPageLimitExceededFailsWithoutPublish(t *testing.T) {
	owner := uuid.New()
	upstream := &syncUpstream{bodies: []string{
		`{"data":[{"i":1},{"i":2}]}`, `{"data":[{"i":3},{"i":4}]}`, `{"data":[{"i":5},{"i":6}]}`,
	}}
	sourceServer := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer sourceServer.Close()

	config := restSyncSourceConfig(sourceServer.URL)
	// Lower max_pages below the number of data pages.
	config = json.RawMessage(strings.Replace(string(config), `"max_pages": 10`, `"max_pages": 2`, 1))
	store, source := newSyncTestStore(t, owner, config)
	storeSourceCredential(t, store, source.ID, "oauth_token", "tok")
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	h := newSyncHandlers(store, committer)

	run := runSync(t, h, job.ID, owner, "")
	assert.Equal(t, "failed", run.Status)
	require.NotNil(t, run.Error)
	assert.Contains(t, *run.Error, "max_pages")
	assert.Nil(t, run.DatasetVersionID)
	requests, _ := committer.snapshot()
	assert.Empty(t, requests, "limit breaches must fail, never publish a truncated snapshot")
}

func TestRunSyncJobPublishFailurePropagates(t *testing.T) {
	owner := uuid.New()
	upstream := &syncUpstream{bodies: []string{`{"data":[{"i":1}]}`, `{"data":[]}`}}
	sourceServer := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer sourceServer.Close()

	store, source := newSyncTestStore(t, owner, restSyncSourceConfig(sourceServer.URL))
	storeSourceCredential(t, store, source.ID, "oauth_token", "tok")
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{err: fmt.Errorf("dataset-versioning outputs:commit responded with status 403")}
	h := newSyncHandlers(store, committer)

	run := runSync(t, h, job.ID, owner, "Bearer jwt")
	assert.Equal(t, "failed", run.Status)
	require.NotNil(t, run.Error)
	assert.Contains(t, *run.Error, "403")
	assert.Nil(t, run.DatasetVersionID)
	assert.NotNil(t, run.ContentHash, "the fetched content hash is still recorded for diagnosis")
}

func TestRunSyncJobEmptySourcePublishesEmptyNDJSON(t *testing.T) {
	owner := uuid.New()
	upstream := &syncUpstream{bodies: []string{`{"data":[]}`}}
	sourceServer := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer sourceServer.Close()

	store, source := newSyncTestStore(t, owner, restSyncSourceConfig(sourceServer.URL))
	storeSourceCredential(t, store, source.ID, "oauth_token", "tok")
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	h := newSyncHandlers(store, committer)

	run := runSync(t, h, job.ID, owner, "")
	require.Equal(t, "succeeded", run.Status)
	assert.Equal(t, int64(0), run.RowsWritten)
	assert.Equal(t, int64(0), run.BytesWritten)
	requests, _ := committer.snapshot()
	require.Len(t, requests, 1)
	assert.NotNil(t, requests[0].Content)
	assert.Empty(t, requests[0].Content, "zero-row snapshots publish a downloadable empty .ndjson file")
	digest := sha256.Sum256([]byte{})
	assert.Equal(t, fmt.Sprintf("%x", digest[:]), *run.ContentHash)
}

func TestRunSyncJobMissingCredentialFailsExplicitly(t *testing.T) {
	owner := uuid.New()
	upstream := &syncUpstream{bodies: []string{`{"data":[]}`}}
	sourceServer := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer sourceServer.Close()

	store, source := newSyncTestStore(t, owner, restSyncSourceConfig(sourceServer.URL))
	// No credential stored for secret_ref "oauth_token".
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	h := newSyncHandlers(store, committer)

	run := runSync(t, h, job.ID, owner, "")
	assert.Equal(t, "failed", run.Status)
	require.NotNil(t, run.Error)
	assert.Contains(t, *run.Error, "oauth_token")
	assert.Empty(t, upstream.requests(), "no upstream call may leave the service without credentials")
	requests, _ := committer.snapshot()
	assert.Empty(t, requests)
}

func TestRunSyncJobMissingSyncConfigFailsExplicitly(t *testing.T) {
	owner := uuid.New()
	upstream := &syncUpstream{bodies: []string{`{"data":[]}`}}
	sourceServer := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer sourceServer.Close()

	store, source := newSyncTestStore(t, owner, json.RawMessage(fmt.Sprintf(`{"base_url":%q,"auth":{"type":"none"}}`, sourceServer.URL)))
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	h := newSyncHandlers(store, committer)

	run := runSync(t, h, job.ID, owner, "")
	assert.Equal(t, "failed", run.Status)
	require.NotNil(t, run.Error)
	assert.Contains(t, *run.Error, "sync configuration")
	assert.Empty(t, upstream.requests())
}

// TestRunSyncJobHTTPClientForwardsCallerJWT exercises the production HTTP
// adapter against a fake dataset-versioning service: the caller's platform JWT
// authenticates the publish, and only a transaction id in the response makes
// the run succeed.
func TestRunSyncJobHTTPClientForwardsCallerJWT(t *testing.T) {
	owner := uuid.New()
	upstream := &syncUpstream{bodies: []string{`{"data":[{"i":1}]}`, `{"data":[]}`}}
	sourceServer := httptest.NewServer(http.HandlerFunc(upstream.handler))
	defer sourceServer.Close()

	var mu sync.Mutex
	var gotAuth string
	var gotGetAuth string
	var gotBody struct {
		Files []struct {
			LogicalPath   string `json:"logical_path"`
			SizeBytes     int64  `json:"size_bytes"`
			ContentBase64 string `json:"content_base64"`
		} `json:"files"`
		Format          string `json:"format"`
		TransactionType string `json:"transaction_type"`
	}
	txnID := uuid.New()
	datasetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			// Dataset precheck: echo the requested id under the caller JWT.
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			require.Len(t, parts, 4)
			require.Equal(t, "datasets", parts[2])
			mu.Lock()
			gotGetAuth = r.Header.Get("Authorization")
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(fmt.Sprintf(`{"id":%q,"name":"orders"}`, parts[3])))
			return
		}
		require.Equal(t, http.MethodPost, r.Method)
		require.True(t, strings.HasSuffix(r.URL.Path, "/outputs:commit"), r.URL.Path)
		// P2.5 usable-ACK fixture: echo the REQUESTED dataset id exactly — a
		// well-formed dataset-versioning success response does; the adapter
		// now rejects mismatched echoes (root implementation-release §5:
		// correct malformed mocks, never weaken the check).
		postParts := strings.Split(strings.Trim(strings.TrimSuffix(r.URL.Path, "/outputs:commit"), "/"), "/")
		require.Len(t, postParts, 4)
		require.Equal(t, "datasets", postParts[2])
		mu.Lock()
		defer mu.Unlock()
		gotAuth = r.Header.Get("Authorization")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{"dataset_id":%q,"transaction":{"id":%q,"status":"COMMITTED"}}`, postParts[3], txnID.String())))
	}))
	defer datasetServer.Close()

	store, source := newSyncTestStore(t, owner, restSyncSourceConfig(sourceServer.URL))
	storeSourceCredential(t, store, source.ID, "oauth_token", "src-tok")
	job, err := store.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	// No injected committer: the handler must fall back to the HTTP adapter.
	h := newSyncHandlers(store, nil)
	h.Config.DatasetServiceURL = datasetServer.URL

	run := runSync(t, h, job.ID, owner, "Bearer real-caller-jwt")
	require.Equal(t, "succeeded", run.Status)
	require.NotNil(t, run.DatasetVersionID)
	assert.Equal(t, txnID, *run.DatasetVersionID)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "Bearer real-caller-jwt", gotAuth, "commit must carry the caller JWT")
	assert.Equal(t, "Bearer real-caller-jwt", gotGetAuth, "dataset precheck must carry the caller JWT")
	assert.Equal(t, "TEXT", gotBody.Format)
	assert.Equal(t, "SNAPSHOT", gotBody.TransactionType)
	require.Len(t, gotBody.Files, 1)
	assert.True(t, strings.HasSuffix(gotBody.Files[0].LogicalPath, ".ndjson"))
	decoded, err := base64Decode(gotBody.Files[0].ContentBase64)
	require.NoError(t, err)
	assert.Equal(t, `{"i":1}`+"\n", string(decoded))
	assert.Equal(t, int64(len(decoded)), gotBody.Files[0].SizeBytes)
}

func base64Decode(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}
