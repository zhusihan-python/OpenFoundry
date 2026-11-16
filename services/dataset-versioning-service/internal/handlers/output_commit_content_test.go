package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

// triggerMirrorStore mirrors the trg_dataset_files_from_committed_txn SQL
// trigger: on OPEN → COMMITTED it projects staged transaction files into
// dataset_files with physical_uri = local:/// + staged physical_path.
type triggerMirrorStore struct {
	*fakeStore
}

func (f *triggerMirrorStore) projectStagedFiles(datasetID uuid.UUID, txnID uuid.UUID) {
	for _, file := range f.stagedFiles[txnID] {
		entry := models.DatasetFile{
			ID:            uuid.New(),
			DatasetID:     datasetID,
			TransactionID: txnID,
			LogicalPath:   file.LogicalPath,
			PhysicalURI:   "local:///" + strings.Trim(file.PhysicalPath, "/"),
			SizeBytes:     file.SizeBytes,
		}
		if file.Operation == models.FileOperationRemove {
			now := time.Now().UTC()
			entry.DeletedAt = &now
		}
		f.files[datasetID] = append(f.files[datasetID], entry)
	}
}

func (f *triggerMirrorStore) CommitTransaction(ctx context.Context, datasetID uuid.UUID, txnID uuid.UUID) error {
	if err := f.fakeStore.CommitTransaction(ctx, datasetID, txnID); err != nil {
		return err
	}
	f.projectStagedFiles(datasetID, txnID)
	return nil
}

// CommitTransactionWithPublish keeps the trigger projection attached to
// the atomic publish: the projection only runs when the whole publish
// (commit + index + lineage + snapshot) succeeded, matching the
// in-transaction SQL trigger.
func (f *triggerMirrorStore) CommitTransactionWithPublish(ctx context.Context, datasetID uuid.UUID, txnID uuid.UUID, files []models.PutDatasetFileIndexEntry, links []models.PutDatasetLineageLink) (*repo.PublishSnapshot, error) {
	snapshot, err := f.fakeStore.CommitTransactionWithPublish(ctx, datasetID, txnID, files, links)
	if err != nil {
		return nil, err
	}
	f.projectStagedFiles(datasetID, txnID)
	return snapshot, nil
}

func newOutputCommitContentHarness(t *testing.T) (*triggerMirrorStore, *handlers.Handlers, string) {
	t.Helper()
	owner := uuid.New()
	store := &triggerMirrorStore{newFakeStore(owner)}
	root := t.TempDir()
	fs := storageabstraction.NewLocalBackingFS("http://files.local", "store", []byte("output-commit-test"))
	fs.RootDir = root
	h := &handlers.Handlers{Repo: store, BackingFS: fs}
	return store, h, root
}

func commitOutputViaJSON(t *testing.T, h *handlers.Handlers, owner uuid.UUID, rid string, body models.CommitDatasetOutputRequest) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", strings.NewReader(string(raw)))
	req = req.WithContext(authmw.ContextWithClaims(context.Background(), &authmw.Claims{Sub: owner, Permissions: []string{"dataset.write"}}))
	req = withRouteParam(req, "rid", rid)
	rec := httptest.NewRecorder()
	h.CommitDatasetOutput(rec, req)
	return rec
}

// downloadOutputCommitFile drives the real download route and the presign
// proxy, returning the bytes a client would receive.
func downloadOutputCommitFile(t *testing.T, h *handlers.Handlers, owner uuid.UUID, datasetID uuid.UUID, fileID uuid.UUID) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/datasets/"+datasetID.String()+"/files/"+fileID.String()+"/download", nil)
	req = req.WithContext(authmw.ContextWithClaims(context.Background(), &authmw.Claims{Sub: owner}))
	req = withRouteParam(req, "rid", datasetID.String())
	req = withRouteParam(req, "file_id", fileID.String())
	rec := httptest.NewRecorder()
	h.DownloadFile(rec, req)
	require.Equal(t, http.StatusFound, rec.Code, rec.Body.String())
	location := rec.Header().Get("Location")
	parsed, err := url.Parse(location)
	require.NoError(t, err)
	key := strings.TrimPrefix(parsed.Path, "/v1/_internal/local-fs/")
	proxyReq := httptest.NewRequest(http.MethodGet, location, nil)
	proxyReq = withRouteParam(proxyReq, "*", key)
	proxyRec := httptest.NewRecorder()
	h.LocalPresignProxy(proxyRec, proxyReq)
	return proxyRec.Code, proxyRec.Body.String()
}

func TestCommitDatasetOutputWritesFullContentAndServesDownload(t *testing.T) {
	store, h, root := newOutputCommitContentHarness(t)
	owner := uuid.New()
	datasetID := uuid.New()
	rid := "ri.foundry.main.dataset." + datasetID.String()

	rows := 150 // exceeds the 100-row preview cap
	var ndjson strings.Builder
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&ndjson, `{"i":%d}`+"\n", i)
	}
	content := []byte(ndjson.String())
	contentType := "application/x-ndjson"
	body := models.CommitDatasetOutputRequest{
		CreateIfMissing: true,
		DatasetName:     "Full output",
		Branch:          "main",
		TransactionType: models.TransactionTypeSnapshot,
		Files: []models.CommitOutputFile{{
			LogicalPath: "part-00000.ndjson",
			// Caller storage_path must be ignored for content-bearing files.
			StoragePath:   "pipeline-build/run/output/part-00000.ndjson",
			SizeBytes:     int64(len(content)),
			ContentType:   &contentType,
			ContentBase64: content,
		}},
		PreviewColumns: []string{"i"},
		PreviewRows:    [][]models.JSONValue{{[]byte(`0`)}, {[]byte(`1`)}},
	}

	rec := commitOutputViaJSON(t, h, owner, rid, body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var out models.CommitDatasetOutputResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	staged := store.stagedFiles[out.Transaction.ID]
	require.Len(t, staged, 1)
	wantKey := "store/datasets/" + datasetID.String() + "/transactions/" + out.Transaction.ID.String() + "/part-00000.ndjson"
	require.Equal(t, wantKey, staged[0].PhysicalPath)
	require.Len(t, out.Files, 1)
	require.Equal(t, "local:///"+wantKey, out.Files[0].StoragePath)
	require.Equal(t, int64(len(content)), out.Files[0].SizeBytes)

	// Bytes are on disk at the server-generated path before the commit.
	onDisk, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(wantKey)))
	require.NoError(t, err)
	require.Equal(t, content, onDisk)

	files, err := store.ListFiles(context.Background(), datasetID, "main", "")
	require.NoError(t, err)
	require.Len(t, files, 1)
	code, downloaded := downloadOutputCommitFile(t, h, owner, datasetID, files[0].ID)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, content, []byte(downloaded))
	require.Equal(t, rows, strings.Count(downloaded, "\n"))

	// Preview keeps serving its capped view, unchanged by the content upload.
	require.Equal(t, []string{"i"}, out.Preview.Columns)
	require.Len(t, out.Preview.Rows, 2)
}

func TestCommitDatasetOutputZeroRowsWritesEmptyObject(t *testing.T) {
	store, h, root := newOutputCommitContentHarness(t)
	owner := uuid.New()
	datasetID := uuid.New()
	rid := "ri.foundry.main.dataset." + datasetID.String()

	contentType := "application/x-ndjson"
	body := models.CommitDatasetOutputRequest{
		CreateIfMissing: true,
		Branch:          "main",
		Files: []models.CommitOutputFile{{
			LogicalPath:   "part-00000.ndjson",
			SizeBytes:     0,
			ContentType:   &contentType,
			ContentBase64: []byte{},
		}},
		PreviewColumns: []string{"i"},
	}

	rec := commitOutputViaJSON(t, h, owner, rid, body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var out models.CommitDatasetOutputResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	staged := store.stagedFiles[out.Transaction.ID]
	require.Len(t, staged, 1)
	wantKey := "store/datasets/" + datasetID.String() + "/transactions/" + out.Transaction.ID.String() + "/part-00000.ndjson"
	require.Equal(t, wantKey, staged[0].PhysicalPath)

	files, err := store.ListFiles(context.Background(), datasetID, "main", "")
	require.NoError(t, err)
	require.Len(t, files, 1)
	code, downloaded := downloadOutputCommitFile(t, h, owner, datasetID, files[0].ID)
	require.Equal(t, http.StatusOK, code)
	require.Empty(t, downloaded)

	onDisk, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(wantKey)))
	require.NoError(t, err)
	require.Empty(t, onDisk)
}

func TestCommitDatasetOutputRejectsSizeMismatch(t *testing.T) {
	store, h, _ := newOutputCommitContentHarness(t)
	owner := uuid.New()
	datasetID := uuid.New()
	rid := "ri.foundry.main.dataset." + datasetID.String()

	body := models.CommitDatasetOutputRequest{
		CreateIfMissing: true,
		Files: []models.CommitOutputFile{{
			LogicalPath:   "part-00000.ndjson",
			SizeBytes:     5,
			ContentBase64: []byte("abc"),
		}},
	}
	rec := commitOutputViaJSON(t, h, owner, rid, body)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "size_bytes must match content_base64 length")
	requireTxnAbortedAndNoIndex(t, store, datasetID)
}

func TestCommitDatasetOutputRejectsOversizedContent(t *testing.T) {
	store, h, _ := newOutputCommitContentHarness(t)
	h.MaxOutputCommitContentBytes = 8
	owner := uuid.New()
	datasetID := uuid.New()
	rid := "ri.foundry.main.dataset." + datasetID.String()

	content := []byte("123456789")
	body := models.CommitDatasetOutputRequest{
		CreateIfMissing: true,
		Files: []models.CommitOutputFile{{
			LogicalPath:   "part-00000.ndjson",
			SizeBytes:     int64(len(content)),
			ContentBase64: content,
		}},
	}
	rec := commitOutputViaJSON(t, h, owner, rid, body)
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "exceeds the output commit limit")
	requireTxnAbortedAndNoIndex(t, store, datasetID)
}

func TestCommitDatasetOutputContentWithoutBackingFails(t *testing.T) {
	owner := uuid.New()
	store := &triggerMirrorStore{newFakeStore(owner)}
	h := &handlers.Handlers{Repo: store}
	datasetID := uuid.New()
	rid := "ri.foundry.main.dataset." + datasetID.String()

	content := []byte("{}\n")
	body := models.CommitDatasetOutputRequest{
		CreateIfMissing: true,
		Files: []models.CommitOutputFile{{
			LogicalPath:   "part-00000.ndjson",
			SizeBytes:     int64(len(content)),
			ContentBase64: content,
		}},
	}
	rec := commitOutputViaJSON(t, h, owner, rid, body)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assertJSONErrorCode(t, rec, "local_backing_filesystem_unavailable")
	requireTxnAbortedAndNoIndex(t, store, datasetID)
}

func TestCommitDatasetOutputMetadataOnlyStaysUnchangedWithBackingFS(t *testing.T) {
	store, h, root := newOutputCommitContentHarness(t)
	owner := uuid.New()
	datasetID := uuid.New()
	rid := "ri.foundry.main.dataset." + datasetID.String()

	body := models.CommitDatasetOutputRequest{
		CreateIfMissing: true,
		Branch:          "main",
		Files: []models.CommitOutputFile{{
			LogicalPath: "part-00000.ndjson",
			StoragePath: "pipeline-build/run/output/part-00000.ndjson",
			SizeBytes:   128,
		}},
		PreviewColumns: []string{"trail_id"},
		PreviewRows:    [][]models.JSONValue{{[]byte(`"mesa"`)}},
	}
	rec := commitOutputViaJSON(t, h, owner, rid, body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var out models.CommitDatasetOutputResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	// Metadata-only commits keep registering the caller's storage path and
	// write nothing to the backing filesystem.
	staged := store.stagedFiles[out.Transaction.ID]
	require.Len(t, staged, 1)
	require.Equal(t, "pipeline-build/run/output/part-00000.ndjson", staged[0].PhysicalPath)
	require.Len(t, out.Files, 1)
	require.Equal(t, "pipeline-build/run/output/part-00000.ndjson", out.Files[0].StoragePath)
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestCommitDatasetOutputRejectsRemoveWithContent(t *testing.T) {
	store, h, _ := newOutputCommitContentHarness(t)
	owner := uuid.New()
	datasetID := uuid.New()
	rid := "ri.foundry.main.dataset." + datasetID.String()

	content := []byte("{}\n")
	body := models.CommitDatasetOutputRequest{
		CreateIfMissing: true,
		Files: []models.CommitOutputFile{{
			LogicalPath:   "part-00000.ndjson",
			SizeBytes:     int64(len(content)),
			ContentBase64: content,
			Operation:     models.FileOperationRemove,
		}},
	}
	rec := commitOutputViaJSON(t, h, owner, rid, body)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "content_base64 requires operation ADD")
	requireTxnAbortedAndNoIndex(t, store, datasetID)
}

func TestCommitDatasetOutputRejectsDuplicateLogicalPathWithContent(t *testing.T) {
	store, h, _ := newOutputCommitContentHarness(t)
	owner := uuid.New()
	datasetID := uuid.New()
	rid := "ri.foundry.main.dataset." + datasetID.String()

	content := []byte("{}\n")
	body := models.CommitDatasetOutputRequest{
		CreateIfMissing: true,
		Files: []models.CommitOutputFile{
			{LogicalPath: "part-00000.ndjson", SizeBytes: int64(len(content)), ContentBase64: content},
			{LogicalPath: "part-00000.ndjson", SizeBytes: 10},
		},
	}
	rec := commitOutputViaJSON(t, h, owner, rid, body)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "duplicate logical_path")
	requireTxnAbortedAndNoIndex(t, store, datasetID)
}

func TestCommitDatasetOutputRejectsOversizedBodyBeforeDecode(t *testing.T) {
	store, h, _ := newOutputCommitContentHarness(t)
	h.MaxOutputCommitContentBytes = 8 // body cap becomes 8 + 2 + 1MiB
	owner := uuid.New()
	datasetID := uuid.New()
	rid := "ri.foundry.main.dataset." + datasetID.String()

	oversized := `{"create_if_missing":true,"metadata":"` + strings.Repeat("A", 2<<20) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", strings.NewReader(oversized))
	req = req.WithContext(authmw.ContextWithClaims(context.Background(), &authmw.Claims{Sub: owner, Permissions: []string{"dataset.write"}}))
	req = withRouteParam(req, "rid", rid)
	rec := httptest.NewRecorder()
	h.CommitDatasetOutput(rec, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "request body exceeds the output commit size limit")
	requireTxnAbortedAndNoIndex(t, store, datasetID)
}

func requireTxnAbortedAndNoIndex(t *testing.T, store *triggerMirrorStore, datasetID uuid.UUID) {
	t.Helper()
	for _, txn := range store.runtimeTransactions {
		if txn.DatasetID == datasetID {
			require.Equal(t, models.TransactionStatusAborted, txn.Status)
		}
	}
	index, err := store.ListDatasetFileIndex(context.Background(), datasetID)
	require.NoError(t, err)
	require.Empty(t, index)
}
