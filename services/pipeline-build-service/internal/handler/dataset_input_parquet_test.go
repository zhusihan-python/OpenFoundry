package handler

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

const ordersPageMetadata = `,"format":"parquet","schema_version":"orders.v1","schema":{"file_format":"PARQUET","fields":[{"name":"order_id","type":"LONG","nullable":false},{"name":"amount","type":"DECIMAL","precision":18,"scale":4,"nullable":false},{"name":"order_date","type":"DATE","nullable":false},{"name":"ordered_at","type":"TIMESTAMP","nullable":false},{"name":"source_time_zone","type":"STRING","nullable":false},{"name":"note","type":"STRING","nullable":true}]}`

const parquetScanOrder = `{"order_id":9223372036854775807,"amount":"-12345678901234.5000","order_date":"2024-02-29","ordered_at":"2024-02-28T18:20:00.123456Z","source_time_zone":"Asia/Kathmandu","note":null}`

func TestDatasetSnapshotReadParquetPreservesExactRows(t *testing.T) {
	second := strings.Replace(parquetScanOrder, `"note":null`, `"note":""`, 1)
	fake := newSnapshotTestServer(t, [][]byte{[]byte(parquetScanOrder), []byte(second)})
	fake.pageRows = 1
	fake.metadataByPage = func(int) string { return ordersPageMetadata }
	reader := DatasetServiceRowReader{BaseURL: fake.server.URL}
	datasetID := uuid.NewString()
	req := snapshotRequest(datasetID)
	req.Path = "part-00000.parquet"
	result, err := reader.ReadDatasetRows(callerCtx(), req)
	require.NoError(t, err)
	require.Equal(t, "parquet", result.Format)
	require.Equal(t, "orders.v1", result.SchemaVersion)
	require.Equal(t, []string{"order_id", "amount", "order_date", "ordered_at", "source_time_zone", "note"}, result.Columns)
	require.Len(t, result.Rows, 2)
	require.Equal(t, map[string]json.RawMessage{
		"order_id":         json.RawMessage(`9223372036854775807`),
		"amount":           json.RawMessage(`"-12345678901234.5000"`),
		"order_date":       json.RawMessage(`"2024-02-29"`),
		"ordered_at":       json.RawMessage(`"2024-02-28T18:20:00.123456Z"`),
		"source_time_zone": json.RawMessage(`"Asia/Kathmandu"`),
		"note":             json.RawMessage(`null`),
	}, result.Rows[0])
	require.Equal(t, json.RawMessage(`""`), result.Rows[1]["note"])
	require.Len(t, fake.requests, 2)
	require.Contains(t, fake.requests[0].Query, "path=part-00000.parquet")
	require.Contains(t, fake.requests[1].Query, "cursor=off1")
}

func TestDatasetSnapshotRejectsChangedParquetScanMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		second string
		txn    string
		want   string
	}{
		{name: "missing format", want: "batched_scan_schema_mismatch"},
		{name: "mixed formats", second: `,"format":"ndjson"`, want: "batched_scan_schema_mismatch"},
		{name: "changed version", second: strings.Replace(ordersPageMetadata, "orders.v1", "orders.v2", 1), want: "batched_scan_schema_mismatch"},
		{name: "missing typed schema", second: `,"format":"parquet","schema_version":"orders.v1"`, want: "batched_scan_schema_invalid"},
		{name: "changed decimal scale", second: strings.Replace(ordersPageMetadata, `"scale":4`, `"scale":2`, 1), want: "batched_scan_schema_invalid"},
		{name: "stale transaction", second: ordersPageMetadata, txn: uuid.NewString(), want: "batched_scan_binding_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newSnapshotTestServer(t, [][]byte{[]byte(parquetScanOrder), []byte(parquetScanOrder)})
			fake.pageRows = 1
			fake.metadataByPage = func(page int) string {
				if page == 1 {
					return ordersPageMetadata
				}
				return tc.second
			}
			fake.txnByPage = func(page int) string {
				if page > 1 && tc.txn != "" {
					return tc.txn
				}
				return fake.txn
			}
			reader := DatasetServiceRowReader{BaseURL: fake.server.URL}
			result, err := reader.ReadDatasetRows(callerCtx(), snapshotRequest(uuid.NewString()))
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, DatasetRowsResult{}, result, "no partial snapshot escapes a failed page")
			require.Len(t, fake.requests, 2)
		})
	}
}

func TestDatasetSnapshotRejectsInvalidParquetFirstPage(t *testing.T) {
	for _, tc := range []struct {
		name, metadata, row, path, want string
		rowsFirst                       bool
	}{
		{name: "unknown format", metadata: `,"format":"orc"`, want: "batched_scan_format_invalid"},
		{name: "duplicate format", metadata: ordersPageMetadata + `,"format":"ndjson"`, want: "duplicate format field"},
		{name: "missing version", metadata: `,"format":"parquet"`, want: "batched_scan_schema_invalid"},
		{name: "stale DVS", path: "orders.parquet", want: "batched_scan_format_invalid"},
		{name: "stale DVS for CSV", path: "orders.csv", want: "batched_scan_format_invalid"},
		{name: "CSV page for NDJSON path", path: "orders.ndjson", metadata: `,"format":"csv"`, want: "batched_scan_format_invalid"},
		{name: "numeric ID is string", metadata: ordersPageMetadata, row: strings.Replace(parquetScanOrder, "9223372036854775807", `"9223372036854775807"`, 1), want: "batched_scan_record_invalid"},
		{name: "late metadata and bad amount", metadata: ordersPageMetadata, row: strings.Replace(parquetScanOrder, `"-12345678901234.5000"`, "-12345678901234.5000", 1), rowsFirst: true, want: "batched_scan_record_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := tc.row
			if row == "" {
				row = parquetScanOrder
			}
			fake := newSnapshotTestServer(t, [][]byte{[]byte(row)})
			fake.metadataByPage = func(int) string { return tc.metadata }
			fake.rowsFirst = tc.rowsFirst
			reader := DatasetServiceRowReader{BaseURL: fake.server.URL}
			req := snapshotRequest(uuid.NewString())
			req.Path = tc.path
			result, err := reader.ReadDatasetRows(callerCtx(), req)
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, DatasetRowsResult{}, result)
		})
	}
}

func TestPreviewPinnedEmptyParquetReturnsSchema(t *testing.T) {
	fake := newSnapshotTestServer(t, nil)
	fake.metadataByPage = func(int) string { return ordersPageMetadata }
	restore := SetExecutionPorts(ExecutionPorts{DatasetRows: DatasetServiceRowReader{BaseURL: fake.server.URL}})
	t.Cleanup(restore)
	pipelineID, datasetID := uuid.New(), uuid.New()
	body, err := json.Marshal(previewPipelineNodeRequest{
		SampleSize: 10,
		Nodes: []models.PipelineNode{{
			ID:              "source",
			TransformType:   "dataset_input",
			InputDatasetIDs: []uuid.UUID{datasetID},
			Config:          json.RawMessage(`{"dataset_snapshot":` + validSnapshot + `}`),
		}},
	})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+pipelineID.String()+"/nodes/source/preview", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer caller-jwt")
	w := httptest.NewRecorder()
	PreviewPipelineNode(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var result pipelineNodePreviewResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.Equal(t, "parquet", result.DatasetFileFormat)
	require.Equal(t, "orders.v1", result.DatasetSchemaVersion)
	require.Len(t, result.DatasetSchema.Fields, 6)
	require.Equal(t, []string{"order_id", "amount", "order_date", "ordered_at", "source_time_zone", "note"}, result.Columns)
	require.NotNil(t, result.Rows)
	require.Empty(t, result.Rows)
	require.Len(t, fake.requests, 1)
	require.Equal(t, "Bearer caller-jwt", fake.requests[0].Auth)
}

func TestBatchedParquetInputPreservesDefaultNDJSONPublish(t *testing.T) {
	record := []byte(parquetScanOrder)
	h := newBatchedHarness(t, nil)
	h.specRepo.specs[0] = batchedGrammarSpec(t, func(p *batchedPayload) {
		p.Input.AllFiles = false
		p.Input.Path = "part-00000.parquet"
	}, h.inputRID, h.outputRID)
	fake := snapshotTestServer{
		records:        [][]byte{record},
		txn:            "11111111-1111-1111-1111-111111111111",
		pageRows:       1,
		metadataByPage: func(int) string { return ordersPageMetadata },
	}
	h.dvs.scanBehavior = func(r *http.Request) (int, []byte) { return http.StatusOK, fake.scanPage(r) }
	buildID, _ := h.createBuild()
	result := h.execute(buildID, h.actor)
	require.Equal(t, http.StatusOK, result.Code, result.Body.String())
	var got executeBatchedResponse
	require.NoError(t, json.Unmarshal(result.Body.Bytes(), &got))
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(append(record, '\n'))), got.Batched.SHA256)
	require.EqualValues(t, len(record)+1, got.Batched.SizeBytes)
	require.EqualValues(t, 1, got.Batched.Records)
	require.Len(t, h.dvs.commits, 1)
	var commit batchedCommitRequest
	require.NoError(t, json.Unmarshal([]byte(h.dvs.commits[0]), &commit))
	require.Len(t, commit.Files, 1)
	require.Equal(t, "part-00000.ndjson", commit.Files[0].LogicalPath)
	require.Empty(t, commit.Files[0].ContentType)
	require.Empty(t, commit.Files[0].Metadata)
}
