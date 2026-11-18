package handlers_test

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// uploadViaHandler drives the real UploadData handler with a multipart body
// carrying logical_path and file bytes.
func uploadViaHandler(t *testing.T, h *handlers.Handlers, datasetID uuid.UUID, logical string, content []byte, claims *authmw.Claims) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	require.NoError(t, writer.WriteField("logical_path", logical))
	part, err := writer.CreateFormFile("file", "upload.bin")
	require.NoError(t, err)
	_, err = part.Write(content)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	req := httptest.NewRequest(http.MethodPost, "/v1/datasets/"+datasetID.String()+"/upload", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("rid", datasetID.String())
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	req = req.WithContext(authmw.ContextWithClaims(req.Context(), claims))
	rec := httptest.NewRecorder()
	h.UploadData(rec, req)
	return rec
}

// TestUploadDataCannotWriteReservedTransactionSubtree: committed output
// objects live under …/datasets/<id>/transactions/<txn UUID>/…, exactly
// where a client-chosen logical path "transactions/<uuid>/…" would land.
// The upload entrypoint must refuse precisely that UUID-shaped subtree —
// case-insensitively for the directory name, so case-insensitive local
// volumes cannot slip a collision through — while ordinary upload paths,
// including non-UUID spellings under transactions/, keep their legacy
// semantics.
func TestUploadDataCannotWriteReservedTransactionSubtree(t *testing.T) {
	owner := uuid.New()
	dataset := models.Dataset{ID: uuid.New(), Name: "orders", ActiveBranch: "main", OwnerID: owner}
	store := &fakeStore{
		datasets: []models.Dataset{dataset},
		fileIndex: map[uuid.UUID][]models.DatasetFileIndexEntry{},
	}
	backing := &storageabstraction.LocalBackingFS{BaseURL: "http://dataset-versioning.local", BaseDir: "data", RootDir: t.TempDir()}
	h := &handlers.Handlers{Repo: store, BackingFS: backing}
	claims := &authmw.Claims{Sub: owner, Permissions: []string{"dataset.write"}}
	txnID := uuid.New()

	// Reserved: the exact committed-object shape, its trimmed alias, the
	// directory itself, and case-insensitive spellings of the reserved
	// directory (case-insensitive local volumes collide on these).
	reserved := []string{
		"transactions/" + txnID.String() + "/part-00000.ndjson",
		"/transactions/" + txnID.String() + "/part-00000.ndjson",
		"transactions/" + txnID.String(),
		"TRANSACTIONS/" + txnID.String() + "/part-00000.ndjson",
		"Transactions/" + strings.ToUpper(txnID.String()) + "/part-00000.ndjson",
	}
	for _, logical := range reserved {
		rec := uploadViaHandler(t, h, dataset.ID, logical, []byte("{\"evil\":true}\n"), claims)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "logical %q must be refused", logical)
		assert.Contains(t, rec.Body.String(), "reserved", logical)
	}

	// Non-UUID second segments under transactions/ are ordinary user
	// paths — no committed object can ever live there — and stay
	// uploadable, including case variants of the directory name.
	for _, logical := range []string{
		"transactions/notes.csv",
		"Transactions/notes.txt",
		"transactions/not-a-uuid/part.ndjson",
		"uploads/data.ndjson",
	} {
		rec := uploadViaHandler(t, h, dataset.ID, logical, []byte("x"), claims)
		assert.Equal(t, http.StatusCreated, rec.Code, "logical %q must stay uploadable: %s", logical, rec.Body.String())
	}

	// Normalizable aliases that survive the trim fall to safeObjectKey and
	// are rejected as invalid keys before the reservation check.
	for _, logical := range []string{
		"transactions//" + txnID.String() + "/part-00000.ndjson",
		"../transactions/" + txnID.String() + "/part-00000.ndjson",
		"a/../transactions/" + txnID.String() + "/part-00000.ndjson",
	} {
		rec := uploadViaHandler(t, h, dataset.ID, logical, []byte("{}\n"), claims)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "alias %q must be refused", logical)
	}

	// No object may exist under the reserved UUID subtree after all attempts.
	committed := "data/datasets/" + dataset.ID.String() + "/transactions/" + txnID.String() + "/part-00000.ndjson"
	if _, err := backing.ReadLocalObject(committed); err == nil {
		t.Fatal("upload must not have created the reserved object")
	}
}
