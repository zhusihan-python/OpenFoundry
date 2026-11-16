package handlers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

type fileRowsHarness struct {
	h       *handlers.Handlers
	store   *fakeStore
	backing *storageabstraction.LocalBackingFS
	dataset models.Dataset
	ownerID uuid.UUID
}

func newFileRowsHarness(t *testing.T) *fileRowsHarness {
	t.Helper()
	owner := uuid.New()
	store := &fakeStore{
		datasets:     []models.Dataset{{ID: uuid.New(), Name: "orders", ActiveBranch: "main", OwnerID: owner}},
		fileIndex:    map[uuid.UUID][]models.DatasetFileIndexEntry{},
		files:        map[uuid.UUID][]models.DatasetFile{},
		branches:     map[uuid.UUID][]models.DatasetBranch{},
		transactions: map[uuid.UUID]string{},
		views:        map[uuid.UUID][]models.DatasetView{},
		viewFiles:    map[uuid.UUID][]models.RuntimeViewFile{},
	}
	backing := &storageabstraction.LocalBackingFS{BaseURL: "http://dataset-versioning.local", BaseDir: "data", RootDir: t.TempDir()}
	return &fileRowsHarness{
		h:       &handlers.Handlers{Repo: store, BackingFS: backing},
		store:   store,
		backing: backing,
		dataset: store.datasets[0],
		ownerID: owner,
	}
}

// registerUploadedCSV mimics UploadData: bytes under the dataset's own
// object directory plus a file-index entry pointing at it.
func (hs *fileRowsHarness) registerUploadedCSV(t *testing.T, logical string, content []byte, lastModified time.Time) {
	t.Helper()
	objectKey := path.Join(hs.backing.BaseDirectory(), "datasets", hs.dataset.ID.String(), logical)
	require.NoError(t, hs.backing.WriteLocalObject(objectKey, content))
	entryType := "file"
	hs.store.fileIndex[hs.dataset.ID] = append(hs.store.fileIndex[hs.dataset.ID], models.DatasetFileIndexEntry{
		DatasetID: hs.dataset.ID, Path: logical, StoragePath: "local:///" + objectKey, EntryType: entryType,
		SizeBytes: int64(len(content)), LastModified: &lastModified,
	})
}

func (hs *fileRowsHarness) rowsRequest(query string, claims *authmw.Claims) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/v1/datasets/"+hs.dataset.ID.String()+"/files/rows"+query, nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("rid", hs.dataset.ID.String())
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	if claims == nil {
		claims = &authmw.Claims{Sub: hs.ownerID}
	}
	req = req.WithContext(authmw.ContextWithClaims(req.Context(), claims))
	rec := httptest.NewRecorder()
	hs.h.ReadDatasetFileRows(rec, req)
	return rec
}

type fileRowsResponse struct {
	DatasetID string                       `json:"dataset_id"`
	Path      string                       `json:"path"`
	Format    string                       `json:"format"`
	Source    string                       `json:"source"`
	Columns   []string                     `json:"columns"`
	Rows      []map[string]json.RawMessage `json:"rows"`
	Truncated bool                         `json:"truncated"`
}

func TestReadDatasetFileRowsRequiresAuth(t *testing.T) {
	t.Parallel()
	hs := newFileRowsHarness(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/datasets/"+hs.dataset.ID.String()+"/files/rows", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("rid", hs.dataset.ID.String())
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	rec := httptest.NewRecorder()
	hs.h.ReadDatasetFileRows(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestReadDatasetFileRowsRequiresOwnerOrAdmin(t *testing.T) {
	t.Parallel()
	hs := newFileRowsHarness(t)
	hs.registerUploadedCSV(t, "orders.csv", []byte("a\n1\n"), time.Now().UTC())

	// Owner reads.
	rec := hs.rowsRequest("", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Non-owner without admin is denied with the same not-found shape the
	// rest of the service uses for foreign datasets.
	rec = hs.rowsRequest("", &authmw.Claims{Sub: uuid.New()})
	require.Equal(t, http.StatusNotFound, rec.Code)

	// Admin role no longer widens the tenant boundary: this harness's
	// dataset lives in the owner's personal domain (org NULL), so an
	// admin without an OrgID is a foreign tenant and gets 404 too.
	rec = hs.rowsRequest("", &authmw.Claims{Sub: uuid.New(), Roles: []string{"admin"}})
	require.Equal(t, http.StatusNotFound, rec.Code)

	// An admin carrying an OrgID still cannot reach personal-domain data.
	orgID := uuid.New()
	rec = hs.rowsRequest("", &authmw.Claims{Sub: uuid.New(), Roles: []string{"admin"}, OrgID: &orgID})
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestReadDatasetFileRowsParsesUploadedCSV(t *testing.T) {
	t.Parallel()
	hs := newFileRowsHarness(t)
	hs.registerUploadedCSV(t, "orders.csv", []byte("sku,qty,price\n007,2,1.5\n0,0,0\nAB-9,10,2.25\n"), time.Now().UTC())

	rec := hs.rowsRequest("", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var out fileRowsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, hs.dataset.ID.String(), out.DatasetID)
	assert.Equal(t, "orders.csv", out.Path)
	assert.Equal(t, "csv", out.Format)
	assert.Equal(t, "file_index", out.Source)
	assert.Equal(t, []string{"sku", "qty", "price"}, out.Columns)
	require.Len(t, out.Rows, 3)
	assert.Equal(t, json.RawMessage(`"007"`), out.Rows[0]["sku"]) // SKU leading zeros preserved
	assert.Equal(t, json.RawMessage(`2`), out.Rows[0]["qty"])     // numeric column stays numeric
	assert.Equal(t, json.RawMessage(`1.5`), out.Rows[0]["price"])
	assert.Equal(t, json.RawMessage(`0`), out.Rows[1]["qty"])
	assert.Equal(t, json.RawMessage(`"AB-9"`), out.Rows[2]["sku"])
	assert.False(t, out.Truncated)
}

func TestReadDatasetFileRowsExecutionAmbiguityFails(t *testing.T) {
	t.Parallel()
	hs := newFileRowsHarness(t)
	hs.registerUploadedCSV(t, "old.csv", []byte("a,b\n1,2\n"), time.Now().UTC().Add(-time.Hour))
	hs.registerUploadedCSV(t, "new.ndjson", []byte("{\"x\": 1}\n"), time.Now().UTC())

	// Execution refuses to guess between multiple data files.
	rec := hs.rowsRequest("", nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "dataset_file_selection_ambiguous")
	assert.Contains(t, rec.Body.String(), "old.csv")
	assert.Contains(t, rec.Body.String(), "new.ndjson")

	// Preview may take the newest.
	rec = hs.rowsRequest("?mode=preview", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var out fileRowsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "new.ndjson", out.Path)

	// An explicit path resolves the ambiguity for execution.
	rec = hs.rowsRequest("?path=old.csv", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "old.csv", out.Path)
}

func TestReadDatasetFileRowsExecutionRefusesTruncatedInput(t *testing.T) {
	t.Parallel()
	hs := newFileRowsHarness(t)
	hs.registerUploadedCSV(t, "orders.csv", []byte("a\n1\n2\n3\n"), time.Now().UTC())

	rec := hs.rowsRequest("?limit=2", nil)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Contains(t, rec.Body.String(), "dataset_file_rows_exceeds_limit")

	rec = hs.rowsRequest("?limit=2&mode=preview", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var out fileRowsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Rows, 2)
	assert.True(t, out.Truncated)
}

func TestReadDatasetFileRowsUnknownPathAndNoDataFiles(t *testing.T) {
	t.Parallel()
	hs := newFileRowsHarness(t)
	hs.registerUploadedCSV(t, "orders.csv", []byte("a\n1\n"), time.Now().UTC())

	rec := hs.rowsRequest("?path=missing.csv", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "file not registered")

	rec = hs.rowsRequest("?path=../../etc/passwd", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "file not registered")

	empty := newFileRowsHarness(t)
	empty.store.fileIndex[empty.dataset.ID] = []models.DatasetFileIndexEntry{{DatasetID: empty.dataset.ID, Path: "reports.pdf", StoragePath: "local:///data/reports.pdf", EntryType: "file", SizeBytes: 8}}
	rec = empty.rowsRequest("", nil)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Contains(t, rec.Body.String(), "no registered data files")
}

func TestReadDatasetFileRowsRejectsOversizedRegistration(t *testing.T) {
	t.Parallel()
	hs := newFileRowsHarness(t)
	hs.store.fileIndex[hs.dataset.ID] = []models.DatasetFileIndexEntry{{DatasetID: hs.dataset.ID, Path: "big.csv", StoragePath: "local:///data/datasets/" + hs.dataset.ID.String() + "/big.csv", EntryType: "file", SizeBytes: 64 << 20}}
	rec := hs.rowsRequest("", nil)
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	assert.Contains(t, rec.Body.String(), "dataset_file_too_large")
}

func TestReadDatasetFileRowsRequiresRealBytes(t *testing.T) {
	t.Parallel()
	hs := newFileRowsHarness(t)
	// A registered data-format entry in the dataset's own namespace whose
	// backing bytes were never written (e.g. a pipeline output commit that
	// registered preview metadata only) must fail, not serve the metadata.
	meta, err := json.Marshal(map[string]any{
		"preview_columns": []string{"sku", "qty"},
		"preview_rows":    [][]json.RawMessage{{json.RawMessage(`"009"`), json.RawMessage(`7`)}},
	})
	require.NoError(t, err)
	hs.store.fileIndex[hs.dataset.ID] = []models.DatasetFileIndexEntry{{
		DatasetID: hs.dataset.ID, Path: "part-00000.ndjson",
		StoragePath: "local:///" + path.Join(hs.backing.BaseDirectory(), "datasets", hs.dataset.ID.String(), "part-00000.ndjson"),
		EntryType:   "file", SizeBytes: int64(len(meta)), Metadata: meta,
	}}
	rec := hs.rowsRequest("", nil)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Contains(t, rec.Body.String(), "dataset_file_bytes_missing")
}

func TestReadDatasetFileRowsRejectsForeignStoragePaths(t *testing.T) {
	t.Parallel()
	hs := newFileRowsHarness(t)
	other := uuid.New()
	objectKey := path.Join(hs.backing.BaseDirectory(), "datasets", other.String(), "stolen.csv")
	require.NoError(t, hs.backing.WriteLocalObject(objectKey, []byte("a\n1\n")))
	hs.store.fileIndex[hs.dataset.ID] = []models.DatasetFileIndexEntry{{DatasetID: hs.dataset.ID, Path: "stolen.csv", StoragePath: "local:///" + objectKey, EntryType: "file", SizeBytes: 6}}
	rec := hs.rowsRequest("", nil)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Contains(t, rec.Body.String(), "dataset_file_storage_outside_dataset")

	// outputs/ object of a transaction that does not belong to this dataset.
	foreignTxn := uuid.New()
	foreignKey := path.Join(hs.backing.BaseDirectory(), "outputs", foreignTxn.String(), "part.ndjson")
	require.NoError(t, hs.backing.WriteLocalObject(foreignKey, []byte("{}\n")))
	hs.store.fileIndex[hs.dataset.ID] = []models.DatasetFileIndexEntry{{DatasetID: hs.dataset.ID, Path: "part.ndjson", StoragePath: "local:///" + foreignKey, EntryType: "file", SizeBytes: 4}}
	rec = hs.rowsRequest("", nil)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Contains(t, rec.Body.String(), "dataset_file_storage_outside_dataset")
}

func TestReadDatasetFileRowsAllowsOwnTransactionOutputs(t *testing.T) {
	t.Parallel()
	hs := newFileRowsHarness(t)
	txn := uuid.New()
	hs.store.transactions[txn] = "COMMITTED"
	objectKey := path.Join(hs.backing.BaseDirectory(), "outputs", txn.String(), "part.ndjson")
	require.NoError(t, hs.backing.WriteLocalObject(objectKey, []byte("{\"sku\":\"0451\",\"qty\":3}\n")))
	hs.store.fileIndex[hs.dataset.ID] = []models.DatasetFileIndexEntry{{DatasetID: hs.dataset.ID, Path: "part.ndjson", StoragePath: "local:///" + objectKey, EntryType: "file", SizeBytes: 20}}
	rec := hs.rowsRequest("", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var out fileRowsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Rows, 1)
	assert.Equal(t, json.RawMessage(`"0451"`), out.Rows[0]["sku"])
	assert.Equal(t, json.RawMessage(`3`), out.Rows[0]["qty"])
}

func TestReadDatasetFileRowsBranchTransactionFiles(t *testing.T) {
	t.Parallel()
	hs := newFileRowsHarness(t)
	txn := uuid.New()
	hs.store.transactions[txn] = "COMMITTED"
	hs.store.branches[hs.dataset.ID] = []models.DatasetBranch{{DatasetID: hs.dataset.ID, Name: "release"}}
	objectKey := path.Join(hs.backing.BaseDirectory(), "transactions", txn.String(), "branch.csv")
	require.NoError(t, hs.backing.WriteLocalObject(objectKey, []byte("sku,qty\nBR-1,5\n")))
	// The branch view is the source of truth for branch files; entries come
	// from the branch's committed transactions.
	hs.store.viewFiles[hs.dataset.ID] = []models.RuntimeViewFile{{
		LogicalPath: "branch.csv", PhysicalPath: "local:///" + objectKey, SizeBytes: 14, IntroducedBy: &txn,
	}}

	rec := hs.rowsRequest("?branch=release", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var out fileRowsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "branch_transaction", out.Source)
	assert.Equal(t, "branch.csv", out.Path)
	require.Len(t, out.Rows, 1)
	assert.Equal(t, json.RawMessage(`5`), out.Rows[0]["qty"])

	// Unknown branch is explicit.
	rec = hs.rowsRequest("?branch=nope", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "branch not found")
}

func TestReadDatasetFileRowsNonLocalStorageFails(t *testing.T) {
	t.Parallel()
	hs := newFileRowsHarness(t)
	hs.store.fileIndex[hs.dataset.ID] = []models.DatasetFileIndexEntry{{DatasetID: hs.dataset.ID, Path: "remote.csv", StoragePath: "s3://bucket/remote.csv", EntryType: "file", SizeBytes: 4}}
	rec := hs.rowsRequest("", nil)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Contains(t, rec.Body.String(), "dataset_rows_backing_driver_unsupported")
}

func TestReadDatasetFileRowsRejectsBadModeAndLimit(t *testing.T) {
	t.Parallel()
	hs := newFileRowsHarness(t)
	rec := hs.rowsRequest("?mode=sloppy", nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	rec = hs.rowsRequest("?limit=0", nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}
