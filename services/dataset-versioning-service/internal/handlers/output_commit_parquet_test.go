package handlers_test

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

func typedInlineParquetBody(key string, content []byte) models.CommitDatasetOutputRequest {
	version, mime := json.RawMessage(`"orders.v1"`), testParquetMIME
	return models.CommitDatasetOutputRequest{
		IdempotencyKey: key, Branch: "main", TransactionType: models.TransactionTypeSnapshot, OutputSchemaVersion: &version,
		Files: []models.CommitOutputFile{{
			LogicalPath: "new.parquet", Operation: models.FileOperationAdd,
			SizeBytes: int64(len(content)), ContentBase64: content, ContentType: &mime,
		}},
	}
}

func TestTypedInlineParquetPublishesCanonicalMetadata(t *testing.T) {
	for _, tt := range []struct {
		name, key string
		groups    int
	}{
		{name: "keyed", key: "orders-key-1", groups: 2},
		{name: "unkeyed-empty"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store, h, root := newOutputCommitContentHarness(t)
			store.datasets[0].ActiveBranch = "main"
			dataset := store.datasets[0]
			data := makeStagedOrdersParquet(t, tt.groups, "orders.v1")
			body := typedInlineParquetBody(tt.key, data)
			body.Schema = &models.DatasetSchema{FileFormat: models.FileFormatText}
			body.Format = &body.Schema.FileFormat
			body.Metadata = json.RawMessage(`{"row_count":-1,"format":"fake","schema_version":"wrong"}`)
			body.Files[0].Metadata = json.RawMessage(`{"record_count":-1,"sha256":"fake","custom":"kept"}`)
			if tt.groups == 0 {
				// The typed path also accepts null metadata via the
				// normal empty-map treatment.
				body.Metadata = json.RawMessage("null")
			}

			first := commitOutputViaJSON(t, h, dataset.OwnerID, dataset.ID.String(), body)
			require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
			var result models.CommitDatasetOutputResponse
			require.NoError(t, json.Unmarshal(first.Body.Bytes(), &result))
			require.Len(t, result.Files, 1)
			file := result.Files[0]
			assert.Equal(t, "new.parquet", file.Path)
			require.NotNil(t, file.ContentType)
			assert.Equal(t, testParquetMIME, *file.ContentType)
			require.NotNil(t, result.Schema)
			assert.Equal(t, models.FileFormatParquet, result.Schema.FileFormat)
			require.Len(t, result.Schema.Fields, 6)
			assert.Equal(t, models.FieldTypeLong, result.Schema.Fields[0].Type)
			assert.Equal(t, models.FieldTypeDecimal, result.Schema.Fields[1].Type)
			require.NotNil(t, result.Schema.Fields[1].Scale)
			assert.Equal(t, uint8(4), *result.Schema.Fields[1].Scale)
			assert.True(t, result.Schema.Fields[5].Nullable)

			var fileMeta, txnMeta map[string]any
			require.NoError(t, json.Unmarshal(file.Metadata, &fileMeta))
			require.NoError(t, json.Unmarshal(result.Transaction.Metadata, &txnMeta))
			for _, meta := range []map[string]any{fileMeta, txnMeta} {
				assert.Equal(t, "parquet", meta["format"])
				assert.Equal(t, "orders.v1", meta["schema_version"])
				assert.Equal(t, float64(tt.groups), meta["record_count"])
				assert.Equal(t, float64(tt.groups), meta["row_count"])
				assert.Equal(t, sha256Hex(data), meta["sha256"])
			}
			assert.Equal(t, "kept", fileMeta["custom"])
			physicalPath := filepath.Join(root, strings.TrimPrefix(file.StoragePath, "local:///"))
			stored, err := os.ReadFile(physicalPath)
			require.NoError(t, err)
			assert.Equal(t, data, stored)
			assert.Contains(t, physicalPath, result.Transaction.ID.String())
			assert.Equal(t, dataset.CurrentVersion+1, store.datasets[0].CurrentVersion)

			if tt.key != "" {
				// A receipt skips current validation/config. A fresh
				// request with two rows under this cap returns 413.
				h.StagedOutputMaxRecords = 1
				replay := commitOutputViaJSON(t, h, dataset.OwnerID, dataset.ID.String(), body)
				require.Equal(t, http.StatusOK, replay.Code, replay.Body.String())
				assert.Equal(t, first.Body.String(), replay.Body.String())
				assert.Equal(t, dataset.CurrentVersion+1, store.datasets[0].CurrentVersion)
				body.Summary = "different bytes"
				conflict := commitOutputViaJSON(t, h, dataset.OwnerID, dataset.ID.String(), body)
				require.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())
				assert.Contains(t, conflict.Body.String(), "IDEMPOTENCY_KEY_CONFLICT")
			}
		})
	}
}

func TestTypedInlineParquetRejectsBeforeWriting(t *testing.T) {
	store, h, root := newOutputCommitContentHarness(t)
	store.datasets[0].ActiveBranch = "main"
	dataset := store.datasets[0]
	data := makeStagedOrdersParquet(t, 2, "orders.v1")
	old := typedInlineParquetBody("orders-old", data)
	first := commitOutputViaJSON(t, h, dataset.OwnerID, dataset.ID.String(), old)
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	var published models.CommitDatasetOutputResponse
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &published))
	require.Len(t, published.Files, 1)

	corrupt := corruptParquetSecondGroupStringLength(t, data)
	cases := []struct {
		name string
		edit func(*models.CommitDatasetOutputRequest)
	}{
		{"corrupt-plain-value", func(w *models.CommitDatasetOutputRequest) { w.Files[0].ContentBase64 = corrupt }},
		{"wrong-footer-profile", func(w *models.CommitDatasetOutputRequest) {
			w.Files[0].ContentBase64 = makeStagedOrdersParquet(t, 2, "orders.v2")
			w.Files[0].SizeBytes = int64(len(w.Files[0].ContentBase64))
		}},
		{"typed-marker", func(w *models.CommitDatasetOutputRequest) { *w.OutputSchemaVersion = json.RawMessage(`""`) }},
		{"non-string-marker", func(w *models.CommitDatasetOutputRequest) { *w.OutputSchemaVersion = json.RawMessage(`5`) }},
		{"mime", func(w *models.CommitDatasetOutputRequest) { *w.Files[0].ContentType = "application/octet-stream" }},
		{"short-size", func(w *models.CommitDatasetOutputRequest) { w.Files[0].SizeBytes-- }},
		{"staged-sha", func(w *models.CommitDatasetOutputRequest) { w.Files[0].SHA256 = sha256Hex(data) }},
		{"staged-reservation", func(w *models.CommitDatasetOutputRequest) { w.ReservationID = uuid.NewString() }},
		{"two-files", func(w *models.CommitDatasetOutputRequest) { w.Files = append(w.Files, w.Files[0]) }},
		{"metadata-only", func(w *models.CommitDatasetOutputRequest) { w.Files[0].ContentBase64 = nil }},
		{"incorrect-operation", func(w *models.CommitDatasetOutputRequest) { w.Files[0].Operation = models.FileOperationReplace }},
		{"append", func(w *models.CommitDatasetOutputRequest) { w.TransactionType = models.TransactionTypeAppend }},
		{"noncanonical-path", func(w *models.CommitDatasetOutputRequest) { w.Files[0].LogicalPath = "nested/../new.parquet" }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			next := typedInlineParquetBody("orders-next", data)
			tt.edit(&next)
			rec := commitOutputViaJSON(t, h, dataset.OwnerID, dataset.ID.String(), next)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Equal(t, dataset.CurrentVersion+1, store.datasets[0].CurrentVersion)
			assert.Len(t, store.outputCommitReceipts, 1)
			index := store.fileIndex[dataset.ID]
			require.Len(t, index, 1)
			assert.Equal(t, published.Files[0].StoragePath, index[0].StoragePath)
			files, err := filepath.Glob(filepath.Join(root, "store", "datasets", dataset.ID.String(), "transactions", "*", "new.parquet"))
			require.NoError(t, err)
			require.Len(t, files, 1)
			retained, err := os.ReadFile(files[0])
			require.NoError(t, err)
			assert.Equal(t, data, retained)
		})
	}

	corrected := typedInlineParquetBody("orders-next", data)
	rec := commitOutputViaJSON(t, h, dataset.OwnerID, dataset.ID.String(), corrected)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	assert.Equal(t, dataset.CurrentVersion+2, store.datasets[0].CurrentVersion)
}

func TestTypedInlineParquetRespectsWorkBudgetAndLegacyPath(t *testing.T) {
	store, h, _ := newOutputCommitContentHarness(t)
	store.datasets[0].ActiveBranch = "main"
	dataset := store.datasets[0]
	data := makeStagedOrdersParquet(t, 22, "orders.v1")
	lenFooter := binary.LittleEndian.Uint32(data[len(data)-8:])
	overBudget := rewriteParquetFooter(t, data, len(data)-8-int(lenFooter), func(meta *metadata.FileMetaData) {
		for _, group := range meta.RowGroups {
			for _, chunk := range group.Columns {
				chunk.MetaData.TotalUncompressedSize = 1 << 20
			}
			group.TotalByteSize = 6 << 20
		}
	})
	body := typedInlineParquetBody("orders-budget", overBudget)
	rec := commitOutputViaJSON(t, h, dataset.OwnerID, dataset.ID.String(), body)
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "validation work")
	assert.Equal(t, dataset.CurrentVersion, store.datasets[0].CurrentVersion)

	tooSmall := typedInlineParquetBody("orders-limit", data)
	h.MaxOutputCommitContentBytes = int64(len(data) - 1)
	rec = commitOutputViaJSON(t, h, dataset.OwnerID, dataset.ID.String(), tooSmall)
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
	assert.Equal(t, dataset.CurrentVersion, store.datasets[0].CurrentVersion)
	h.MaxOutputCommitContentBytes = 0

	arbitrary := []byte("opaque existing upload")
	legacy := typedInlineParquetBody("orders-legacy", arbitrary)
	legacy.OutputSchemaVersion = nil
	rec = commitOutputViaJSON(t, h, dataset.OwnerID, dataset.ID.String(), legacy)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	assert.Equal(t, dataset.CurrentVersion+1, store.datasets[0].CurrentVersion)
	var published models.CommitDatasetOutputResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &published))
	require.Len(t, published.Files, 1)
	assert.NotContains(t, string(published.Files[0].Metadata), "schema_version")
	assert.Nil(t, published.Schema)
	replay := commitOutputViaJSON(t, h, dataset.OwnerID, dataset.ID.String(), legacy)
	require.Equal(t, http.StatusOK, replay.Code, replay.Body.String())
	assert.Equal(t, rec.Body.String(), replay.Body.String())
}

// Older outputs:commit ignored an unknown top-level marker. An existing
// receipt still serves its original bytes, even for a marker that would
// be invalid on a receipt miss in the new handler.
func TestTypedInlineParquetReplaysReceiptWrittenBeforeMarker(t *testing.T) {
	store, h, _ := newOutputCommitContentHarness(t)
	dataset := store.datasets[0]
	body := typedInlineParquetBody("before-marker", []byte("generic inline"))
	*body.OutputSchemaVersion = json.RawMessage(`{"old_client_payload":true}`)
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	const receiptJSON = `{"historical_receipt":true}`
	store.outputCommitReceipts = map[outputReceiptKey]models.OutputCommitReceipt{
		{datasetID: dataset.ID, key: body.IdempotencyKey}: {
			DatasetID: dataset.ID, IdempotencyKey: body.IdempotencyKey,
			RequestFingerprint: sha256Hex(raw), ResponseBody: json.RawMessage(receiptJSON),
		},
	}
	rec := commitOutputViaJSON(t, h, dataset.OwnerID, dataset.ID.String(), body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, receiptJSON, rec.Body.String())
	assert.Empty(t, store.runtimeTransactions)
}
