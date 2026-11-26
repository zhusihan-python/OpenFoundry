package handlers_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// The publish contract of the noon order lines: seven string columns in the
// key order of the published NDJSON. The second constant is the public field
// list the versioned schema answers for it.
const noonOrderLinesSchema = `{"file_format":"TEXT","fields":[` +
	`{"name":"currency","type":"STRING","nullable":false},{"name":"item_nr","type":"STRING","nullable":false},` +
	`{"name":"market","type":"STRING","nullable":false},{"name":"order_nr","type":"STRING","nullable":false},` +
	`{"name":"sku","type":"STRING","nullable":false},{"name":"store","type":"STRING","nullable":false},` +
	`{"name":"unit_price","type":"STRING","nullable":false}]}`

const noonOrderLinesFields = `[{"name":"currency","field_type":"STRING","nullable":false},` +
	`{"name":"item_nr","field_type":"STRING","nullable":false},{"name":"market","field_type":"STRING","nullable":false},` +
	`{"name":"order_nr","field_type":"STRING","nullable":false},{"name":"sku","field_type":"STRING","nullable":false},` +
	`{"name":"store","field_type":"STRING","nullable":false},{"name":"unit_price","field_type":"STRING","nullable":false}]`

// The publish contract of one monthly report file: three nullable columns of
// a CSV output, in header order. The second constant is the public field
// list the versioned schema answers for it.
const reportCSVSchema = `{"file_format":"TEXT","fields":[` +
	`{"name":"order postal","type":"STRING","nullable":true},{"name":"quantity","type":"LONG","nullable":true},` +
	`{"name":"total","type":"DOUBLE","nullable":true}]}`

const reportCSVFields = `[{"name":"order postal","field_type":"STRING","nullable":true},` +
	`{"name":"quantity","field_type":"LONG","nullable":true},{"name":"total","field_type":"DOUBLE","nullable":true}]`

type receiptEdit func(*models.CommitDatasetOutputResponse)

type historicalSchemaAnswer struct {
	TransactionID uuid.UUID       `json:"transaction_id"`
	ContentHash   string          `json:"content_hash"`
	Fields        json.RawMessage `json:"fields"`
}

func noonSchema(t *testing.T) *models.DatasetSchema {
	t.Helper()
	var schema models.DatasetSchema
	require.NoError(t, json.Unmarshal([]byte(noonOrderLinesSchema), &schema))
	return &schema
}

func reportCSVSchemaValue(t *testing.T) *models.DatasetSchema {
	t.Helper()
	var schema models.DatasetSchema
	require.NoError(t, json.Unmarshal([]byte(reportCSVSchema), &schema))
	return &schema
}

// sealReceipt records the immutable receipt of the keyed publication that cut
// one version, holding the schema the publisher declared for its one file.
func (hs *scanHarness) sealReceipt(t *testing.T, version int32, schema *models.DatasetSchema, edits ...receiptEdit) uuid.UUID {
	t.Helper()
	txnID := *hs.store.versions[hs.dataset.ID][version-1].TransactionID
	staged := hs.store.stagedFiles[txnID][0]
	response := models.CommitDatasetOutputResponse{
		DatasetID: hs.dataset.ID, Branch: "main", Transaction: hs.store.runtimeTransactions[txnID], Schema: schema,
		Files: []models.DatasetFileIndexEntry{{Path: staged.LogicalPath, SizeBytes: staged.SizeBytes}},
	}
	for _, edit := range edits {
		edit(&response)
	}
	body, err := json.Marshal(response)
	require.NoError(t, err)
	if hs.store.outputCommitReceipts == nil {
		hs.store.outputCommitReceipts = map[outputReceiptKey]models.OutputCommitReceipt{}
	}
	hs.store.outputCommitReceipts[outputReceiptKey{hs.dataset.ID, fmt.Sprintf("publication-%d", version)}] = models.OutputCommitReceipt{
		Branch: response.Branch, Transaction: response.Transaction, ResponseBody: body,
	}
	return txnID
}

func TestHistoricalSchemaReadsNDJSONSchemaFromTheVersionReceipt(t *testing.T) {
	hs := newScanHarness(t)
	schema := noonSchema(t)
	query := func(version int32) string { return fmt.Sprintf("branch=main&version=%d", version) }
	firstLines := []byte("{\"currency\":\"AED\",\"item_nr\":\"NRPL0001-1\",\"unit_price\":\"49\"}\n")
	version1 := hs.commitSnapshotVersion(t, "order-lines.ndjson", firstLines)
	txn1 := *hs.store.versions[hs.dataset.ID][version1-1].TransactionID

	// A schema key in the transaction's free-form metadata is not a receipt.
	txn := hs.store.runtimeTransactions[txn1]
	txn.Metadata = models.JSONValue(fmt.Sprintf(`{"schema":%s,"staged_output":{"staged_object_id":%q}}`, noonOrderLinesSchema, uuid.New()))
	hs.store.runtimeTransactions[txn1] = txn
	rec := historicalSchemaRequest(hs, query(version1), hs.ownerID)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "dataset_schema_format_unsupported")
	require.NotContains(t, rec.Body.String(), `"fields"`)

	hs.sealReceipt(t, version1, schema)
	first := historicalSchemaRequest(hs, query(version1), hs.ownerID)
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(noonOrderLinesFields)))
	require.JSONEq(t, fmt.Sprintf(`{"dataset_id":%q,"branch":"main","version":%d,"transaction_id":%q,"content_hash":%q,"fields":%s}`,
		hs.dataset.ID, version1, txn1, digest, noonOrderLinesFields), first.Body.String())
	var original historicalSchemaAnswer
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &original))

	// The next publication changes row values and declares the same columns.
	version2 := hs.commitSnapshotVersion(t, "order-lines.ndjson", []byte("{\"currency\":\"AED\",\"item_nr\":\"NRPL0001-1\",\"unit_price\":\"49.25\"}\n"))
	txn2 := hs.sealReceipt(t, version2, schema)
	latest := historicalSchemaRequest(hs, query(version2), hs.ownerID)
	require.Equal(t, http.StatusOK, latest.Code, latest.Body.String())
	var changed historicalSchemaAnswer
	require.NoError(t, json.Unmarshal(latest.Body.Bytes(), &changed))
	require.Equal(t, txn2, changed.TransactionID)
	require.Equal(t, original.ContentHash, changed.ContentHash)
	require.JSONEq(t, string(original.Fields), string(changed.Fields))
	again := historicalSchemaRequest(hs, query(version1), hs.ownerID)
	require.Equal(t, http.StatusOK, again.Code, again.Body.String())
	require.Equal(t, first.Body.String(), again.Body.String())

	for _, tc := range []struct {
		version int32
		txn     uuid.UUID
	}{{version1, txn2}, {version2, txn1}} {
		rec := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d&transaction_id=%s", tc.version, tc.txn), hs.ownerID)
		require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "dataset_schema_transaction_mismatch")
		require.NotContains(t, rec.Body.String(), `"fields"`)
	}
	for _, query := range []string{
		fmt.Sprintf("branch=main&version=%d&transaction_id=%s", version2, txn2),
		fmt.Sprintf("branch=main&version=%d&expected_content_hash=%s", version2, original.ContentHash),
		fmt.Sprintf("branch=main&version=%d&expected_content_hash=%s&transaction_id=%s", version2, original.ContentHash, txn2),
	} {
		rec := historicalSchemaRequest(hs, query, hs.ownerID)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.JSONEq(t, latest.Body.String(), rec.Body.String())
	}
	other := fmt.Sprintf("%x", sha256.Sum256([]byte(`[]`)))
	mismatch := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d&expected_content_hash=%s", version2, other), hs.ownerID)
	require.Equal(t, http.StatusPreconditionFailed, mismatch.Code, mismatch.Body.String())
	require.Contains(t, mismatch.Body.String(), "dataset_schema_content_hash_mismatch")
	require.NotContains(t, mismatch.Body.String(), `"fields"`)
}

func TestHistoricalSchemaRefusesNDJSONWithoutAUsableReceiptSchema(t *testing.T) {
	schema := noonSchema(t)
	precision, scale := uint8(10), uint8(2)
	text := func(fields ...models.Field) *models.DatasetSchema {
		return &models.DatasetSchema{FileFormat: models.FileFormatText, Fields: fields}
	}
	otherFile := func(response *models.CommitDatasetOutputResponse) { response.Files[0].Path = "other.ndjson" }
	otherSize := func(response *models.CommitDatasetOutputResponse) { response.Files[0].SizeBytes++ }
	for _, tc := range []struct {
		name   string
		schema *models.DatasetSchema
		edits  []receiptEdit
		code   string
	}{
		{"absent schema", nil, nil, "dataset_schema_format_unsupported"},
		{"null fields", &models.DatasetSchema{FileFormat: models.FileFormatText}, nil, "dataset_schema_format_unsupported"},
		{"other file format", &models.DatasetSchema{FileFormat: models.FileFormatParquet, Fields: schema.Fields}, nil, "dataset_schema_format_unsupported"},
		{"another file", schema, []receiptEdit{otherFile}, "dataset_schema_format_unsupported"},
		{"other file size", schema, []receiptEdit{otherSize}, "dataset_schema_format_unsupported"},
		{"another transaction", schema, []receiptEdit{func(response *models.CommitDatasetOutputResponse) { response.Transaction.ID = uuid.New() }}, "dataset_schema_format_unsupported"},
		{"other branch", schema, []receiptEdit{func(response *models.CommitDatasetOutputResponse) { response.Branch = "other" }}, "dataset_schema_format_unsupported"},
		{"zero columns", text([]models.Field{}...), nil, "dataset_schema_unsupported"},
		{"decimal", text(models.Field{Name: "unit_price", Type: models.FieldTypeDecimal, Precision: &precision, Scale: &scale}), nil, "dataset_schema_unsupported"},
		{"array", text(models.Field{Name: "items", Type: models.FieldTypeArray, ArraySubType: &models.Field{Name: "items", Type: models.FieldTypeString}}), nil, "dataset_schema_unsupported"},
		{"map", text(models.Field{Name: "tags", Type: models.FieldTypeMap}), nil, "dataset_schema_unsupported"},
		{"unnamed", text(models.Field{Name: "", Type: models.FieldTypeString}), nil, "dataset_schema_unsupported"},
		{"duplicate names", text(models.Field{Name: "sku", Type: models.FieldTypeString}, models.Field{Name: "sku", Type: models.FieldTypeString}), nil, "dataset_schema_unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hs := newScanHarness(t)
			version := hs.commitSnapshotVersion(t, "order-lines.ndjson", []byte("{}\n"))
			hs.sealReceipt(t, version, tc.schema, tc.edits...)
			rec := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version), hs.ownerID)
			require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), tc.code)
			require.NotContains(t, rec.Body.String(), `"fields"`)
		})
	}
}

func TestHistoricalSchemaReadsCSVSchemaFromTheVersionReceipt(t *testing.T) {
	hs := newScanHarness(t)
	// The bytes hold text where the receipt declares a DOUBLE, and a BOM
	// and CRLF line ends: none of it is read.
	version := hs.commitSnapshotVersion(t, "transaction_view.csv",
		[]byte("\xef\xbb\xbforder postal,quantity,total\r\n00123,2,N/A\r\n"))

	// A CSV version without a keyed receipt is unsupported, as before.
	rec := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version), hs.ownerID)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "dataset_schema_format_unsupported")
	require.NotContains(t, rec.Body.String(), `"fields"`)

	txn := hs.sealReceipt(t, version, reportCSVSchemaValue(t))
	answer := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version), hs.ownerID)
	require.Equal(t, http.StatusOK, answer.Code, answer.Body.String())
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(reportCSVFields)))
	require.JSONEq(t, fmt.Sprintf(`{"dataset_id":%q,"branch":"main","version":%d,"transaction_id":%q,"content_hash":%q,"fields":%s}`,
		hs.dataset.ID, version, txn, digest, reportCSVFields), answer.Body.String())

	// The binding is proved through the route: the version's own hash is
	// accepted, alone and with its transaction, and another hash is refused.
	for _, query := range []string{
		fmt.Sprintf("branch=main&version=%d&expected_content_hash=%s", version, digest),
		fmt.Sprintf("branch=main&version=%d&expected_content_hash=%s&transaction_id=%s", version, digest, txn),
	} {
		rec := historicalSchemaRequest(hs, query, hs.ownerID)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.JSONEq(t, answer.Body.String(), rec.Body.String())
	}
	other := fmt.Sprintf("%x", sha256.Sum256([]byte(`[]`)))
	mismatch := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d&expected_content_hash=%s", version, other), hs.ownerID)
	require.Equal(t, http.StatusPreconditionFailed, mismatch.Code, mismatch.Body.String())
	require.JSONEq(t, `{"error": "dataset_schema_content_hash_mismatch: the committed schema does not match expected_content_hash",
		"code": "dataset_schema_content_hash_mismatch"}`, mismatch.Body.String())
}

func TestHistoricalSchemaRefusesCSVWithoutAUsableReceiptSchema(t *testing.T) {
	noFields := &models.DatasetSchema{FileFormat: models.FileFormatText}
	for _, tc := range []struct {
		name    string
		path    string
		schema  *models.DatasetSchema
		refusal string
	}{
		{"absent schema", "transaction_view.csv", nil, "the NDJSON or CSV output of this version has no schema in its commit receipt"},
		{"null fields", "transaction_view.csv", noFields, "the NDJSON or CSV output of this version has no schema in its commit receipt"},
		{"tsv output", "transaction_view.tsv", reportCSVSchemaValue(t), "versioned schemas require Parquet ADD files or one NDJSON or CSV output with a schema in its commit receipt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hs := newScanHarness(t)
			version := hs.commitSnapshotVersion(t, tc.path, []byte("order postal,quantity,total\n00123,2,1.5\n"))
			hs.sealReceipt(t, version, tc.schema)
			rec := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version), hs.ownerID)
			require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
			require.JSONEq(t, fmt.Sprintf(`{"error":"dataset_schema_format_unsupported: %s"}`, tc.refusal), rec.Body.String())
		})
	}
}

func TestHistoricalSchemaNDJSONKeepsTheVersionAvailabilityRules(t *testing.T) {
	schema := noonSchema(t)
	lines := []byte("{}\n")
	for _, tc := range []struct {
		name  string
		files []stagedSpec
		code  string
	}{
		{"missing bytes", []stagedSpec{{path: "order-lines.ndjson", sizeOverride: int64Ptr(int64(len(lines)))}}, "dataset_file_bytes_missing"},
		{"size differs from the object", []stagedSpec{{path: "order-lines.ndjson", content: lines, sizeOverride: int64Ptr(int64(len(lines)) + 1)}}, "dataset_file_size_mismatch"},
		{"not the transaction object", []stagedSpec{{path: "order-lines.ndjson", content: lines, physical: "datasets/elsewhere/order-lines.ndjson"}}, "dataset_scan_object_not_transaction_scoped"},
		{"replaced file", []stagedSpec{{path: "order-lines.ndjson", content: lines, op: models.FileOperationReplace}}, "dataset_schema_format_unsupported"},
		{"second NDJSON file", []stagedSpec{{path: "a.ndjson", content: lines}, {path: "b.ndjson", content: lines}}, "dataset_schema_format_unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hs := newScanHarness(t)
			version := hs.commitSnapshotVersionFiles(t, tc.files...)
			hs.sealReceipt(t, version, schema)
			rec := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version), hs.ownerID)
			require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), tc.code)
			require.NotContains(t, rec.Body.String(), `"fields"`)
		})
	}

	// An empty snapshot has no row to read columns from. It answers the
	// columns its publication declared.
	hs := newScanHarness(t)
	version := hs.commitSnapshotVersionFiles(t, stagedSpec{path: "order-lines.ndjson", content: []byte{}})
	hs.sealReceipt(t, version, schema)
	rec := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version), hs.ownerID)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var answer historicalSchemaAnswer
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &answer))
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte(noonOrderLinesFields))), answer.ContentHash)
	require.JSONEq(t, noonOrderLinesFields, string(answer.Fields))
}
