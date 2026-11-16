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
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// registerExcelImport mimics the excel import: an ndjson object under the
// dataset's own directory plus a file-index entry carrying the excel_import
// marker metadata (with or without real backing bytes on purpose).
func (hs *fileRowsHarness) registerExcelImport(t *testing.T, logical string, content []byte, writeBytes bool) {
	t.Helper()
	objectKey := path.Join(hs.backing.BaseDirectory(), "datasets", hs.dataset.ID.String(), "excel", "00000000-0000-0000-0000-000000000001", "data.ndjson")
	if writeBytes {
		require.NoError(t, hs.backing.WriteLocalObject(objectKey, content))
	}
	meta, err := models.MarshalJSONValue(map[string]any{
		"excel_import": 1, "normalized": true, "source_filename": "orders.xlsx",
		"columns": []string{"a"}, "row_count": len(content) / 11,
	})
	require.NoError(t, err)
	entryType := "file"
	ct := "application/x-ndjson"
	now := time.Now().UTC()
	hs.store.fileIndex[hs.dataset.ID] = append(hs.store.fileIndex[hs.dataset.ID], models.DatasetFileIndexEntry{
		DatasetID: hs.dataset.ID, Path: logical, StoragePath: "local:///" + objectKey, EntryType: entryType,
		SizeBytes: int64(len(content)), ContentType: &ct, Metadata: meta, LastModified: &now,
	})
}

func (hs *fileRowsHarness) excelGet(t *testing.T, handler func(http.ResponseWriter, *http.Request), suffix string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/datasets/"+hs.dataset.ID.String()+suffix, nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("rid", hs.dataset.ID.String())
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	req = req.WithContext(authmw.ContextWithClaims(req.Context(), &authmw.Claims{Sub: hs.ownerID}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

// The dependency errors surfaced by the excel preview/schema branches are
// VALUE-typed datasetRowsError; through the real writer they must stay 422
// (a pointer/value mismatch would silently degrade them to 500).
func TestExcelImportedPreviewDependencyErrorsStay422(t *testing.T) {
	t.Run("missing backing bytes", func(t *testing.T) {
		hs := newFileRowsHarness(t)
		hs.registerExcelImport(t, "orders.ndjson", []byte(`{"a":"007"}`+"\n"), false)
		rec := hs.excelGet(t, hs.h.PreviewDataset, "/preview")
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		require.Contains(t, rec.Body.String(), "dataset_file_bytes_missing")
	})
	t.Run("malformed ndjson", func(t *testing.T) {
		hs := newFileRowsHarness(t)
		hs.registerExcelImport(t, "orders.ndjson", []byte("this is { not ndjson\n"), true)
		rec := hs.excelGet(t, hs.h.PreviewDataset, "/preview")
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		require.Contains(t, rec.Body.String(), "dataset_file_parse_failed")
	})
}

func TestExcelImportedSchemaDependencyErrorsStay422(t *testing.T) {
	t.Run("missing backing bytes", func(t *testing.T) {
		hs := newFileRowsHarness(t)
		hs.registerExcelImport(t, "orders.ndjson", []byte(`{"a":"007"}`+"\n"), false)
		rec := hs.excelGet(t, hs.h.GetCurrentSchema, "/schema")
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		require.Contains(t, rec.Body.String(), "dataset_file_bytes_missing")
	})
	t.Run("malformed ndjson", func(t *testing.T) {
		hs := newFileRowsHarness(t)
		hs.registerExcelImport(t, "orders.ndjson", []byte("junk line\n"), true)
		rec := hs.excelGet(t, hs.h.GetCurrentSchema, "/schema")
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		require.Contains(t, rec.Body.String(), "dataset_file_parse_failed")
	})
}

// An index lookup failure under an explicit branch must fail the request
// immediately (5xx) — never fall back to the legacy path, whose metadata
// samples could otherwise leak global preview_rows for a dataset whose
// scope could not be determined.
func TestExplicitBranchPreviewIndexLookupFailureFailsClosed(t *testing.T) {
	hs := newFileRowsHarness(t)
	hs.store.failListFileIndex = true

	rec := hs.excelGet(t, hs.h.PreviewDataset, "/preview?branch=release")
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Contains(t, rec.Body.String(), "failed to load dataset file index")
	require.Zero(t, hs.store.previewDataCalls, "legacy PreviewData must not be consulted on lookup failure")
}

// The healthy excel-import path through the same writers stays 200 and keeps
// the unversioned schema shape (branch null, TEXT format).
func TestExcelImportedPreviewAndSchemaHealthyThroughWriter(t *testing.T) {
	hs := newFileRowsHarness(t)
	hs.registerExcelImport(t, "orders.ndjson", []byte(`{"a":"007"}`+"\n"+`{"a":"008"}`+"\n"), true)

	rec := hs.excelGet(t, hs.h.PreviewDataset, "/preview")
	require.Equal(t, http.StatusOK, rec.Code)
	var preview struct {
		Columns []string     `json:"columns"`
		Rows    [][]string   `json:"rows"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &preview))
	require.Equal(t, []string{"a"}, preview.Columns)
	require.Equal(t, []string{"007"}, preview.Rows[0])

	rec = hs.excelGet(t, hs.h.GetCurrentSchema, "/schema")
	require.Equal(t, http.StatusOK, rec.Code)
	var schema struct {
		Branch *string `json:"branch"`
		Schema struct {
			Fields     []models.Field `json:"fields"`
			FileFormat string         `json:"file_format"`
		} `json:"schema"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &schema))
	require.Nil(t, schema.Branch)
	require.Equal(t, "TEXT", schema.Schema.FileFormat)
	require.Equal(t, "a", schema.Schema.Fields[0].Name)
}
