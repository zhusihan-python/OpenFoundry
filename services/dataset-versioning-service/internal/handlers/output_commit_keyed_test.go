// P2.6 W1 regressions: idempotent keyed outputs:commit.
//
// These cases pin the keyed contract on the handler + in-memory store:
// byte-stable first/replay/GET responses, fingerprint conflict 409 with
// zero side effects, cross-tenant invisibility, explicit-branch-only
// requests, whole-transaction rollback on failure (no receipt, no OPEN
// transaction, key still unbound), replays that never touch the write path
// or head/index, and the unchanged shape of no-key commits.
package handlers_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

// outputReceiptKey scopes a keyed-commit receipt to one dataset: the key is
// never unique across datasets and never queried globally.
type outputReceiptKey struct {
	datasetID uuid.UUID
	key       string
}

// keyedStoreSnapshot captures the mutable state a keyed build touches so a
// failed build can be rolled back wholesale — the in-memory mirror of the
// single outer transaction rollback.
type keyedStoreSnapshot struct {
	datasets            []models.Dataset
	runtimeTransactions map[uuid.UUID]models.RuntimeTransaction
	transactions        map[uuid.UUID]string
	stagedFiles         map[uuid.UUID][]models.StageTransactionFile
	fileIndex           map[uuid.UUID][]models.DatasetFileIndexEntry
	lineageLinks        map[uuid.UUID][]models.DatasetLineageLink
	files               map[uuid.UUID][]models.DatasetFile
	branches            map[uuid.UUID][]models.DatasetBranch
}

func (f *fakeStore) snapshotKeyedCommit() keyedStoreSnapshot {
	snap := keyedStoreSnapshot{
		datasets:            append([]models.Dataset(nil), f.datasets...),
		runtimeTransactions: make(map[uuid.UUID]models.RuntimeTransaction, len(f.runtimeTransactions)),
		transactions:        make(map[uuid.UUID]string, len(f.transactions)),
		stagedFiles:         make(map[uuid.UUID][]models.StageTransactionFile, len(f.stagedFiles)),
		fileIndex:           make(map[uuid.UUID][]models.DatasetFileIndexEntry, len(f.fileIndex)),
		lineageLinks:        make(map[uuid.UUID][]models.DatasetLineageLink, len(f.lineageLinks)),
		files:               make(map[uuid.UUID][]models.DatasetFile, len(f.files)),
		branches:            make(map[uuid.UUID][]models.DatasetBranch, len(f.branches)),
	}
	for k, v := range f.runtimeTransactions {
		snap.runtimeTransactions[k] = v
	}
	for k, v := range f.transactions {
		snap.transactions[k] = v
	}
	for k, v := range f.stagedFiles {
		snap.stagedFiles[k] = append([]models.StageTransactionFile(nil), v...)
	}
	for k, v := range f.fileIndex {
		snap.fileIndex[k] = append([]models.DatasetFileIndexEntry(nil), v...)
	}
	for k, v := range f.lineageLinks {
		snap.lineageLinks[k] = append([]models.DatasetLineageLink(nil), v...)
	}
	for k, v := range f.files {
		snap.files[k] = append([]models.DatasetFile(nil), v...)
	}
	for k, v := range f.branches {
		snap.branches[k] = append([]models.DatasetBranch(nil), v...)
	}
	return snap
}

func (f *fakeStore) restoreKeyedCommit(snap keyedStoreSnapshot) {
	f.datasets = append([]models.Dataset(nil), snap.datasets...)
	f.runtimeTransactions = snap.runtimeTransactions
	f.transactions = snap.transactions
	f.stagedFiles = snap.stagedFiles
	f.fileIndex = snap.fileIndex
	f.lineageLinks = snap.lineageLinks
	f.files = snap.files
	f.branches = snap.branches
}

// WithIdempotentOutputCommit mirrors the real repo's protocol on the
// in-memory store: tenant visibility first, first-writer-wins per
// (dataset, key), fingerprint mismatch → typed conflict, build failures
// rolled back completely, response serialized exactly once.
func (f *fakeStore) WithIdempotentOutputCommit(ctx context.Context, scope repo.TenantScope, datasetID uuid.UUID, key string, fingerprint string, build func(context.Context, repo.OutputCommitTxStore) (models.CommitDatasetOutputResponse, error)) (*models.OutputCommitReceipt, bool, error) {
	var dataset *models.Dataset
	for i := range f.datasets {
		if f.datasets[i].ID == datasetID && f.tenantCovers(scope, &f.datasets[i]) {
			dataset = &f.datasets[i]
			break
		}
	}
	if dataset == nil {
		return nil, false, repo.ErrNotFound
	}
	if f.outputCommitReceipts == nil {
		f.outputCommitReceipts = map[outputReceiptKey]models.OutputCommitReceipt{}
	}
	k := outputReceiptKey{datasetID: datasetID, key: key}
	if existing, ok := f.outputCommitReceipts[k]; ok {
		if existing.RequestFingerprint != fingerprint {
			return nil, false, repo.ErrIdempotencyKeyConflict
		}
		replay := existing
		return &replay, true, nil
	}
	snap := f.snapshotKeyedCommit()
	response, err := build(ctx, f)
	if err != nil {
		f.restoreKeyedCommit(snap)
		return nil, false, err
	}
	response.IdempotencyKey = key
	response.RequestFingerprint = fingerprint
	body, err := json.Marshal(response)
	if err != nil {
		f.restoreKeyedCommit(snap)
		return nil, false, err
	}
	receipt := models.OutputCommitReceipt{
		DatasetID:          datasetID,
		IdempotencyKey:     key,
		RequestFingerprint: fingerprint,
		Branch:             response.Branch,
		Transaction:        response.Transaction,
		ResponseBody:       body,
		CreatedAt:          time.Now().UTC(),
	}
	f.outputCommitReceipts[k] = receipt
	return &receipt, false, nil
}

// GetOutputCommitReceipt answers only when the tenant scope covers the
// dataset AND a committed receipt exists for the key — a foreign tenant
// never learns whether a key exists.
func (f *fakeStore) GetOutputCommitReceipt(_ context.Context, scope repo.TenantScope, datasetID uuid.UUID, key string) (*models.OutputCommitReceipt, error) {
	for i := range f.datasets {
		if f.datasets[i].ID != datasetID || !f.tenantCovers(scope, &f.datasets[i]) {
			continue
		}
		if receipt, ok := f.outputCommitReceipts[outputReceiptKey{datasetID, key}]; ok {
			out := receipt
			return &out, nil
		}
		return nil, nil
	}
	return nil, nil
}

// keyedCommitBody builds a keyed commit carrying a zero-byte inline file —
// the minimal P26 Connector shape (including empty content) on a local
// backing store.
func keyedCommitBody(key, branch string) string {
	return fmt.Sprintf(`{"idempotency_key":%q,"branch":%q,"files":[{"logical_path":"part-00000.ndjson","content_base64":"","size_bytes":0}]}`, key, branch)
}

// TestKeyedOutputCommitFirstReplayGetByteStable: the first commit answers
// 201, a byte-identical replay answers 200, and GET serves the same stored
// bytes — with the version advanced exactly once, one committed
// transaction, one receipt, and a replay that survives even a deleted
// branch because it never re-enters the build path.
func TestKeyedOutputCommitFirstReplayGetByteStable(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	org := uuid.New()
	store := newFakeStore(owner)
	fs := storageabstraction.NewLocalBackingFS("http://files.local", "store", []byte("keyed-test"))
	fs.RootDir = t.TempDir()
	h := &handlers.Handlers{Repo: store, BackingFS: fs}
	target := store.datasets[0]
	target.OrgID = orgPtr(org)
	target.ActiveBranch = "main"
	store.datasets[0] = target
	rid := "ri.foundry.main.dataset." + target.ID.String()
	claims := claimsFor(owner, orgPtr(org))
	params := map[string]string{"rid": rid}
	key := "of-p26-pub-v1:" + uuid.NewString()
	body := keyedCommitBody(key, "main")

	first := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", body, params, claims)
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	versionAfterFirst := store.datasets[0].CurrentVersion
	require.Len(t, store.outputCommitReceipts, 1)

	replay := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", body, params, claims)
	require.Equal(t, http.StatusOK, replay.Code, replay.Body.String())
	require.Equal(t, first.Body.String(), replay.Body.String(), "replay must return the stored bytes verbatim")

	// The replay took the short path: no new transaction, no version bump.
	assert.Equal(t, versionAfterFirst, store.datasets[0].CurrentVersion, "replay must not advance the version")
	committedTxns := 0
	for _, tx := range store.runtimeTransactions {
		if tx.DatasetID == target.ID && tx.Status == models.TransactionStatusCommitted {
			committedTxns++
		}
	}
	assert.Equal(t, 1, committedTxns, "replay must not create a second transaction")

	// The stored bytes contain the keyed echo fields.
	var decoded models.CommitDatasetOutputResponse
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &decoded))
	assert.Equal(t, key, decoded.IdempotencyKey)
	assert.Equal(t, 64, len(decoded.RequestFingerprint))

	// Deleting the branch (and even every branch) cannot affect the replay:
	// a replay never queries branch/backing/lineage existence.
	store.branches[target.ID] = nil
	replayAfterBranchGone := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", body, params, claims)
	require.Equal(t, http.StatusOK, replayAfterBranchGone.Code)
	assert.Equal(t, first.Body.String(), replayAfterBranchGone.Body.String())

	fetched := do(h.GetDatasetOutputCommit, http.MethodGet, "/v1/datasets/"+rid+"/outputs/commits/"+key, "", map[string]string{"rid": rid, "key": key}, claims)
	require.Equal(t, http.StatusOK, fetched.Code, fetched.Body.String())
	assert.Equal(t, first.Body.String(), fetched.Body.String(), "GET must serve the stored bytes verbatim")
}

// TestKeyedOutputCommitFingerprintConflict409: the same key with different
// bytes is a typed 409 with zero side effects, and the original receipt
// stays replayable afterwards.
func TestKeyedOutputCommitFingerprintConflict409(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	org := uuid.New()
	store := newFakeStore(owner)
	fs := storageabstraction.NewLocalBackingFS("http://files.local", "store", []byte("keyed-test"))
	fs.RootDir = t.TempDir()
	h := &handlers.Handlers{Repo: store, BackingFS: fs}
	target := store.datasets[0]
	target.OrgID = orgPtr(org)
	target.ActiveBranch = "main"
	store.datasets[0] = target
	rid := "ri.foundry.main.dataset." + target.ID.String()
	claims := claimsFor(owner, orgPtr(org))
	params := map[string]string{"rid": rid}
	key := "of-p26-pub-v1:" + uuid.NewString()

	first := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", keyedCommitBody(key, "main"), params, claims)
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	versionAfterFirst := store.datasets[0].CurrentVersion

	// Semantically similar but byte-different body (reordered field) — the
	// byte contract makes this a conflict, not a replay.
	conflictBody := fmt.Sprintf(`{"branch":"main","idempotency_key":%q,"files":[{"logical_path":"part-00000.ndjson","content_base64":"","size_bytes":0}]}`, key)
	conflict := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", conflictBody, params, claims)
	require.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())
	assert.Contains(t, conflict.Body.String(), "IDEMPOTENCY_KEY_CONFLICT")

	// Zero side effects: version unchanged, no extra transactions, one receipt.
	assert.Equal(t, versionAfterFirst, store.datasets[0].CurrentVersion)
	assert.Equal(t, 1, countTransactions(store, target.ID))
	require.Len(t, store.outputCommitReceipts, 1)

	// The original body still replays.
	replay := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", keyedCommitBody(key, "main"), params, claims)
	require.Equal(t, http.StatusOK, replay.Code)
	assert.Equal(t, first.Body.String(), replay.Body.String())
}

// TestKeyedOutputCommitCrossTenantIsolation: another organization can
// neither replay-and-overwrite (404 dataset, not 409 — no key-existence
// leak) nor read the receipt; the same key is usable independently on the
// other tenant's own dataset.
func TestKeyedOutputCommitCrossTenantIsolation(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	orgA, orgB := uuid.New(), uuid.New()
	store := newFakeStore(owner)
	fs := storageabstraction.NewLocalBackingFS("http://files.local", "store", []byte("keyed-test"))
	fs.RootDir = t.TempDir()
	h := &handlers.Handlers{Repo: store, BackingFS: fs}
	target := store.datasets[0]
	target.OrgID = orgPtr(orgA)
	target.ActiveBranch = "main"
	store.datasets[0] = target
	other := models.Dataset{ID: uuid.New(), Name: "b-side", Format: "parquet", StoragePath: "bronze/b", OwnerID: uuid.New(), OrgID: orgPtr(orgB), ActiveBranch: "main", Tags: []string{}, Metadata: []byte(`{}`), CurrentVersion: 1}
	store.datasets = append(store.datasets, other)
	rid := "ri.foundry.main.dataset." + target.ID.String()
	key := "of-p26-pub-v1:" + uuid.NewString()
	bodyA := keyedCommitBody(key, "main")

	claimsA := claimsFor(owner, orgPtr(orgA))
	first := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", bodyA, map[string]string{"rid": rid}, claimsA)
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())

	claimsB := claimsFor(uuid.New(), orgPtr(orgB))
	foreignReplay := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", bodyA, map[string]string{"rid": rid}, claimsB)
	require.Equal(t, http.StatusNotFound, foreignReplay.Code, foreignReplay.Body.String())
	assert.NotContains(t, foreignReplay.Body.String(), "IDEMPOTENCY", "cross-tenant must not leak key state")

	foreignGet := do(h.GetDatasetOutputCommit, http.MethodGet, "/v1/datasets/"+rid+"/outputs/commits/"+key, "", map[string]string{"rid": rid, "key": key}, claimsB)
	require.Equal(t, http.StatusNotFound, foreignGet.Code)

	// Same key on tenant B's own dataset binds independently — keys are
	// scoped per dataset, never global.
	ridB := "ri.foundry.main.dataset." + other.ID.String()
	firstB := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+ridB+"/outputs:commit", bodyA, map[string]string{"rid": ridB}, claimsB)
	require.Equal(t, http.StatusCreated, firstB.Code, firstB.Body.String())
	require.Len(t, store.outputCommitReceipts, 2)
}

// TestKeyedOutputCommitRequestValidation: intrinsic keyed constraints are
// rejected with 400 before any transaction work.
func TestKeyedOutputCommitRequestValidation(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	org := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}
	target := store.datasets[0]
	target.OrgID = orgPtr(org)
	target.ActiveBranch = "main"
	store.datasets[0] = target
	rid := "ri.foundry.main.dataset." + target.ID.String()
	claims := claimsFor(owner, orgPtr(org))
	params := map[string]string{"rid": rid}

	cases := []struct {
		name string
		body string
	}{
		{"invalid key characters", `{"idempotency_key":"bad key!","branch":"main"}`},
		{"missing branch", `{"idempotency_key":"of-p26-pub-v1:` + uuid.NewString() + `"}`},
		{"create_if_missing", `{"idempotency_key":"of-p26-pub-v1:` + uuid.NewString() + `","branch":"main","create_if_missing":true}`},
	}
	for _, tc := range cases {
		rec := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", tc.body, params, claims)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "%s: %s", tc.name, rec.Body.String())
	}
	assert.Empty(t, store.runtimeTransactions, "rejected keyed commits must not start transactions")
	assert.Empty(t, store.outputCommitReceipts)
}

// TestKeyedOutputCommitMissingBranchNoFallback: an explicit branch that
// does not exist is a 404 — never a silent fallback to main — while the
// dataset's active branch changing between commit and replay never
// redirects the replay.
func TestKeyedOutputCommitMissingBranchNoFallback(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	org := uuid.New()
	store := newFakeStore(owner)
	fs := storageabstraction.NewLocalBackingFS("http://files.local", "store", []byte("keyed-test"))
	fs.RootDir = t.TempDir()
	h := &handlers.Handlers{Repo: store, BackingFS: fs}
	target := store.datasets[0]
	target.OrgID = orgPtr(org)
	target.ActiveBranch = "main"
	store.datasets[0] = target
	rid := "ri.foundry.main.dataset." + target.ID.String()
	claims := claimsFor(owner, orgPtr(org))
	params := map[string]string{"rid": rid}

	missing := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", keyedCommitBody("of-p26-pub-v1:"+uuid.NewString(), "feature-x"), params, claims)
	require.Equal(t, http.StatusNotFound, missing.Code, missing.Body.String())
	assert.Contains(t, missing.Body.String(), "branch not found")
	assert.Empty(t, store.runtimeTransactions, "missing branch must not start a transaction")

	// A committed keyed receipt ignores a later active-branch change.
	key := "of-p26-pub-v1:" + uuid.NewString()
	body := keyedCommitBody(key, "main")
	first := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", body, params, claims)
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	store.datasets[0].ActiveBranch = "somewhere-else"
	replay := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", body, params, claims)
	require.Equal(t, http.StatusOK, replay.Code)
	assert.Equal(t, first.Body.String(), replay.Body.String())
	var decoded models.CommitDatasetOutputResponse
	require.NoError(t, json.Unmarshal(replay.Body.Bytes(), &decoded))
	assert.Equal(t, "main", decoded.Branch, "replay keeps the committed explicit branch")
}

// TestKeyedOutputCommitFailureRollsBackEverything: a publish-step failure
// inside the keyed build leaves no receipt, no OPEN/ABORTED transaction, no
// staged rows and no version advance; the key stays unbound so the retry
// commits for real and then replays.
func TestKeyedOutputCommitFailureRollsBackEverything(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	org := uuid.New()
	store := newFakeStore(owner)
	fs := storageabstraction.NewLocalBackingFS("http://files.local", "store", []byte("keyed-test"))
	fs.RootDir = t.TempDir()
	h := &handlers.Handlers{Repo: store, BackingFS: fs}
	target := store.datasets[0]
	target.OrgID = orgPtr(org)
	target.ActiveBranch = "main"
	store.datasets[0] = target
	rid := "ri.foundry.main.dataset." + target.ID.String()
	claims := claimsFor(owner, orgPtr(org))
	params := map[string]string{"rid": rid}
	key := "of-p26-pub-v1:" + uuid.NewString()
	body := keyedCommitBody(key, "main")

	store.publishErr = errPublishInjected{}
	failed := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", body, params, claims)
	require.Equal(t, http.StatusInternalServerError, failed.Code, failed.Body.String())

	assert.Empty(t, store.outputCommitReceipts, "failed attempt must not bind the key")
	assert.Empty(t, store.runtimeTransactions, "rollback must remove the whole transaction — no OPEN leftovers")
	assert.Empty(t, store.stagedFiles)
	assert.EqualValues(t, 1, store.datasets[0].CurrentVersion, "version must not advance on failure")
	assert.Empty(t, store.fileIndex[target.ID], "nothing may be published")
	fetched := do(h.GetDatasetOutputCommit, http.MethodGet, "/v1/datasets/"+rid+"/outputs/commits/"+key, "", map[string]string{"rid": rid, "key": key}, claims)
	require.Equal(t, http.StatusNotFound, fetched.Code)

	store.publishErr = nil
	retry := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", body, params, claims)
	require.Equal(t, http.StatusCreated, retry.Code, retry.Body.String())
	require.Len(t, store.outputCommitReceipts, 1)
	assert.EqualValues(t, 2, store.datasets[0].CurrentVersion)

	replay := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", body, params, claims)
	require.Equal(t, http.StatusOK, replay.Code)
	assert.Equal(t, retry.Body.String(), replay.Body.String())
}

// TestKeyedOutputCommitReplayAfterOtherCommitsKeepsHead: after commit A
// (keyed) a later no-key commit B moves head/index; replaying A returns the
// ORIGINAL stored bytes and leaves B's head/index and the version untouched.
func TestKeyedOutputCommitReplayAfterOtherCommitsKeepsHead(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	org := uuid.New()
	store := newFakeStore(owner)
	fs := storageabstraction.NewLocalBackingFS("http://files.local", "store", []byte("keyed-test"))
	fs.RootDir = t.TempDir()
	h := &handlers.Handlers{Repo: store, BackingFS: fs}
	target := store.datasets[0]
	target.OrgID = orgPtr(org)
	target.ActiveBranch = "main"
	store.datasets[0] = target
	rid := "ri.foundry.main.dataset." + target.ID.String()
	claims := claimsFor(owner, orgPtr(org))
	params := map[string]string{"rid": rid}
	key := "of-p26-pub-v1:" + uuid.NewString()

	first := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", keyedCommitBody(key, "main"), params, claims)
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())

	// Commit B without a key: the head moves to B's file.
	bodyB := `{"branch":"main","files":[{"logical_path":"later.ndjson","size_bytes":3,"storage_path":"datasets/` + target.ID.String() + `/out/later.ndjson"}]}`
	second := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", bodyB, params, claims)
	require.Equal(t, http.StatusCreated, second.Code, second.Body.String())
	versionAfterB := store.datasets[0].CurrentVersion
	require.Len(t, store.fileIndex[target.ID], 1)
	require.Equal(t, "later.ndjson", store.fileIndex[target.ID][0].Path)

	replayA := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", keyedCommitBody(key, "main"), params, claims)
	require.Equal(t, http.StatusOK, replayA.Code)
	assert.Equal(t, first.Body.String(), replayA.Body.String(), "replay serves the original bytes, not a rebuild from today's head")
	assert.Equal(t, versionAfterB, store.datasets[0].CurrentVersion, "replay must not move the version")
	require.Len(t, store.fileIndex[target.ID], 1)
	assert.Equal(t, "later.ndjson", store.fileIndex[target.ID][0].Path, "replay must not rewrite the file index")
}

// TestNoKeyOutputCommitShapeUnchanged: a commit without idempotency_key
// behaves exactly as before — 201 and a response free of the new fields.
func TestNoKeyOutputCommitShapeUnchanged(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	org := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}
	target := store.datasets[0]
	target.OrgID = orgPtr(org)
	target.ActiveBranch = "main"
	store.datasets[0] = target
	rid := "ri.foundry.main.dataset." + target.ID.String()
	claims := claimsFor(owner, orgPtr(org))

	rec := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", `{"branch":"main"}`, map[string]string{"rid": rid}, claims)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "idempotency_key", "no-key responses must not carry the new fields")
	assert.NotContains(t, rec.Body.String(), "request_fingerprint")
	assert.Empty(t, store.outputCommitReceipts, "no-key commits never write receipts")
}

// TestGetDatasetOutputCommitUnknownAndInvalidKey: unknown key or invisible
// dataset → 404 (no in-flight conclusion), malformed key → 400.
func TestGetDatasetOutputCommitUnknownAndInvalidKey(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	org := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}
	target := store.datasets[0]
	target.OrgID = orgPtr(org)
	target.ActiveBranch = "main"
	store.datasets[0] = target
	rid := "ri.foundry.main.dataset." + target.ID.String()
	claims := claimsFor(owner, orgPtr(org))

	unknown := do(h.GetDatasetOutputCommit, http.MethodGet, "/v1/datasets/"+rid+"/outputs/commits/of-p26-pub-v1:"+uuid.NewString(), "", map[string]string{"rid": rid, "key": "of-p26-pub-v1:" + uuid.NewString()}, claims)
	require.Equal(t, http.StatusNotFound, unknown.Code)

	invalid := do(h.GetDatasetOutputCommit, http.MethodGet, "/v1/datasets/"+rid+"/outputs/commits/bad%20key", "", map[string]string{"rid": rid, "key": "bad key"}, claims)
	require.Equal(t, http.StatusBadRequest, invalid.Code)
}

// TestNoKeyTrailingDataKeepsDecoderSemantics: a no-key commit whose body
// carries garbage after the first JSON value still succeeds — the pre-P2.6
// handler decoded the first value and never required the trailing data, and
// the buffered-read interim candidate would have wrongly 413'd it.
func TestNoKeyTrailingDataKeepsDecoderSemantics(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	org := uuid.New()
	store := newFakeStore(owner)
	h := &handlers.Handlers{Repo: store}
	target := store.datasets[0]
	target.OrgID = orgPtr(org)
	target.ActiveBranch = "main"
	store.datasets[0] = target
	rid := "ri.foundry.main.dataset." + target.ID.String()
	claims := claimsFor(owner, orgPtr(org))

	rec := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", `{"branch":"main"} trailing-garbage-after-first-value`, map[string]string{"rid": rid}, claims)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
}

// TestKeyedBodyTrailingTokensRejected: More() could not distinguish a
// top-level trailing ']' or '}' from end-of-input; the second-Decode EOF
// gate must reject every trailing token and accept only one JSON value
// optionally followed by whitespace — and those whitespace bytes are part
// of the fingerprint (byte-different replay conflicts).
func TestKeyedBodyTrailingTokensRejected(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	org := uuid.New()
	store := newFakeStore(owner)
	fs := storageabstraction.NewLocalBackingFS("http://files.local", "store", []byte("keyed-test"))
	fs.RootDir = t.TempDir()
	h := &handlers.Handlers{Repo: store, BackingFS: fs}
	target := store.datasets[0]
	target.OrgID = orgPtr(org)
	target.ActiveBranch = "main"
	store.datasets[0] = target
	rid := "ri.foundry.main.dataset." + target.ID.String()
	claims := claimsFor(owner, orgPtr(org))
	params := map[string]string{"rid": rid}
	key := "of-p26-pub-v1:" + uuid.NewString()
	base := keyedCommitBody(key, "main")

	for name, tail := range map[string]string{
		"trailing ]":            "]",
		"trailing }":            "}",
		"invalid garbage":       "garbage",
		"second value":          ` {"second":"value"}`,
		"partial second value":  ` {"a":`,
	} {
		rec := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", base+tail, params, claims)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "%s: %s", name, rec.Body.String())
	}
	assert.Empty(t, store.runtimeTransactions, "rejected keyed bodies must not start transactions")
	assert.Empty(t, store.outputCommitReceipts)

	// Trailing whitespace is legal — and every byte of it is fingerprinted.
	padded := base + "  \n\t "
	first := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", padded, params, claims)
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	var decoded models.CommitDatasetOutputResponse
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &decoded))
	sum := sha256.Sum256([]byte(padded))
	require.Equal(t, hex.EncodeToString(sum[:]), decoded.RequestFingerprint, "fingerprint must cover the padded raw body exactly")

	replay := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", padded, params, claims)
	require.Equal(t, http.StatusOK, replay.Code)
	assert.Equal(t, first.Body.String(), replay.Body.String())

	// Same semantics, different trailing bytes → fingerprint differs → 409.
	conflict := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", base+"\n", params, claims)
	require.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())
	assert.Contains(t, conflict.Body.String(), "IDEMPOTENCY_KEY_CONFLICT")
}

func countTransactions(f *fakeStore, datasetID uuid.UUID) int {
	n := 0
	for _, tx := range f.runtimeTransactions {
		if tx.DatasetID == datasetID {
			n++
		}
	}
	return n
}
