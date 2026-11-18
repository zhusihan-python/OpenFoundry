package handler

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/libs/schedule-events"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

func inlineOrdersResult(t *testing.T, records ...string) executor.NodeResult {
	t.Helper()
	rows := make([]map[string]json.RawMessage, 0, len(records))
	for _, raw := range records {
		var row map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(raw), &row))
		rows = append(rows, row)
	}
	return executor.NodeResult{Metadata: map[string]any{"data_rows": rows}}
}

func TestDatasetOutputOrdersV1DefaultsToTypedParquet(t *testing.T) {
	tx := executor.OutputTransaction{DatasetRID: uuid.NewString(), OutputSchemaVersion: "orders.v1"}
	result := inlineOrdersResult(t, parquetScanOrder)
	body, err := datasetOutputCommitBody(tx, result)
	require.NoError(t, err)
	require.Equal(t, "orders.v1", body.OutputSchemaVersion)
	require.Equal(t, "PARQUET", *body.Format)
	require.Equal(t, "SNAPSHOT", body.TransactionType)
	require.Equal(t, []string{"order_id", "amount", "order_date", "ordered_at", "source_time_zone", "note"}, body.PreviewColumns)
	require.Len(t, body.PreviewRows, 1)
	require.Equal(t, json.RawMessage(`"9223372036854775807"`), body.PreviewRows[0][0])
	require.Equal(t, json.RawMessage(`"-12345678901234.5000"`), body.PreviewRows[0][1])
	require.Equal(t, json.RawMessage(`null`), body.PreviewRows[0][5])
	require.Equal(t, json.RawMessage(`9223372036854775807`), result.Metadata["data_rows"].([]map[string]json.RawMessage)[0]["order_id"])
	require.Len(t, body.Files, 1)
	f := body.Files[0]
	require.Equal(t, "ADD", f.Operation)
	require.Equal(t, "part-00000.parquet", f.LogicalPath)
	require.Equal(t, "application/vnd.apache.parquet", *f.ContentType)
	require.EqualValues(t, len(f.ContentBase64), f.SizeBytes)
	var metadata map[string]string
	require.NoError(t, json.Unmarshal(f.Metadata, &metadata))
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(f.ContentBase64)), metadata["sha256"])
	require.Equal(t, "orders.v1", metadata["schema_version"])
	require.EqualValues(t, 1, openOrdersParquet(t, f.ContentBase64).NumRows())
	require.JSONEq(t, `{"file_format":"PARQUET","fields":[{"name":"order_id","type":"LONG","nullable":false},{"name":"amount","type":"DECIMAL","nullable":false,"precision":18,"scale":4},{"name":"order_date","type":"DATE","nullable":false},{"name":"ordered_at","type":"TIMESTAMP","nullable":false},{"name":"source_time_zone","type":"STRING","nullable":false},{"name":"note","type":"STRING","nullable":true}]}`, string(mustRuntimeJSON(body.Schema)))
	rawFile := mustRuntimeJSON(f)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rawFile, &fields))
	require.NotContains(t, fields, "sha256", "inline ADD retains the digest in file metadata")
}

func TestDatasetOutputOrdersV1WritesTypedEmptyAndBoundsPreview(t *testing.T) {
	tx := executor.OutputTransaction{DatasetRID: uuid.NewString(), OutputSchemaVersion: "orders.v1"}
	empty, err := datasetOutputCommitBody(tx, inlineOrdersResult(t))
	require.NoError(t, err)
	require.Len(t, empty.Schema.Fields, 6)
	reader := openOrdersParquet(t, empty.Files[0].ContentBase64)
	require.Zero(t, reader.NumRows())
	require.Zero(t, reader.NumRowGroups())
	require.Empty(t, empty.PreviewRows)

	long := strings.Replace(parquetScanOrder, `"note":null`, `"note":"`+strings.Repeat("a", inlineParquetPreviewMaxBytes)+`"`, 1)
	result, err := datasetOutputCommitBody(tx, inlineOrdersResult(t, parquetScanOrder, long, parquetScanOrder))
	require.NoError(t, err)
	require.Len(t, result.PreviewRows, 1, "preview is bounded independently from the result")
	require.EqualValues(t, 3, openOrdersParquet(t, result.Files[0].ContentBase64).NumRows())
}

func TestInlineParquetWorkBudgetIncludesUncompressedRows(t *testing.T) {
	compressible := strings.Replace(parquetScanOrder, `"note":null`, `"note":"`+strings.Repeat("a", 300_000)+`"`, 1)
	content, _, _, err := encodeInlineOrdersParquet(t.Context(), inlineOrdersResult(t, compressible))
	require.NoError(t, err)
	require.Less(t, len(content), 64<<10, "the compressed artifact alone fits")
	require.ErrorContains(t, validateInlineParquetWork(content, 64<<10), "validation work limit")
	require.NoError(t, validateInlineParquetWork(content, 1<<20))
}

func TestDatasetOutputOrdersV1PreservesExplicitNDJSON(t *testing.T) {
	result := inlineOrdersResult(t, parquetScanOrder)
	for _, tc := range []struct{ format, path string }{
		{}, {format: "ndjson"}, {format: "TEXT", path: "legacy.ndjson"}, {path: "existing/ordered.ndjson"},
	} {
		legacy := executor.OutputTransaction{DatasetRID: uuid.NewString(), FileFormat: tc.format, LogicalPath: tc.path}
		withProfile := legacy
		if tc.format != "" || tc.path != "" {
			withProfile.OutputSchemaVersion = "orders.v1"
		}
		a, err := datasetOutputCommitBody(legacy, result)
		require.NoError(t, err)
		b, err := datasetOutputCommitBody(withProfile, result)
		require.NoError(t, err)
		require.Equal(t, mustRuntimeJSON(a), mustRuntimeJSON(b))
		require.Equal(t, "application/x-ndjson", *b.Files[0].ContentType)
		require.Empty(t, b.OutputSchemaVersion)
	}
}

func TestDatasetOutputOrdersV1InvalidConfigAndRows(t *testing.T) {
	for _, invalid := range []string{
		`{"output_schema_version":null}`,
		`{"output_schema_version":"orders.v2"}`,
		`{"output_schema_version":"orders.v1","file_format":3}`,
		`{"output_schema_version":"orders.v1","file_format":"CSV"}`,
		`{"output_schema_version":"orders.v1","file_format":"PARQUET","logical_path":"out.ndjson"}`,
		`{"output_schema_version":"orders.v1","file_format":"TEXT","logical_path":"out.parquet"}`,
		`{"output_schema_version":"orders.v1","logical_path":"folder/../out.parquet"}`,
		`{"output_schema_version":"orders.v1","logical_path":"folder/.parquet"}`,
		`{"output_schema_version":"orders.v1","logical_path":"transactions/out.parquet"}`,
		`{"output_schema_version":"orders.v1","write_mode":"APPEND"}`,
		`{"output_schema_version":"orders.v1","_output":{"output_schema_version":"orders.v1"}}`,
		`{"file_format":"NDJSON","logical_path":"existing.ndjson","_output":{"output_schema_version":"orders.v1"}}`,
	} {
		err := BindDatasetOutputSchema(json.RawMessage(invalid), []executor.OutputTransaction{{DatasetRID: uuid.NewString()}})
		require.ErrorContains(t, err, "dataset_output_config_invalid", invalid)
	}
	tx := executor.OutputTransaction{DatasetRID: uuid.NewString(), OutputSchemaVersion: "orders.v1"}
	for _, result := range []executor.NodeResult{
		{Metadata: map[string]any{"sample_rows": inlineOrdersResult(t, parquetScanOrder).Metadata["data_rows"]}},
		{Metadata: map[string]any{"data_rows": []any{"not an object"}}},
		{Metadata: map[string]any{"data_rows": []map[string]json.RawMessage(nil)}},
		inlineOrdersResult(t, strings.Replace(parquetScanOrder, `"-12345678901234.5000"`, `"1.12345"`, 1)),
	} {
		_, err := datasetOutputCommitBody(tx, result)
		require.ErrorContains(t, err, "dataset_output_parquet_invalid")
	}
}

func TestInlineOrdersRejectsTimestampOffsetOutsideUTCYearRange(t *testing.T) {
	for _, value := range []string{"0000-01-01T00:00:00+01:00", "9999-12-31T23:59:59.999999-01:00"} {
		t.Run(value, func(t *testing.T) {
			row := strings.Replace(parquetScanOrder, "2024-02-28T18:20:00.123456Z", value, 1)
			_, _, _, err := encodeInlineOrdersParquet(t.Context(), inlineOrdersResult(t, row))
			require.ErrorContains(t, err, "ordered_at UTC year")
		})
	}
}

func TestGraphOutputUsesFrozenTypedPublicationConfig(t *testing.T) {
	target := uuid.New()
	p := &models.Pipeline{
		ID: uuid.New(),
		DAG: mustRuntimeJSON([]models.PipelineNode{{
			ID: "output", TransformType: "output_dataset", OutputDatasetID: &target,
			Config: json.RawMessage(`{"_output":{"output_schema_version":"orders.v1","logical_path":"existing.ndjson"}}`),
		}}),
	}
	plan, err := planFromPipeline(p, uuid.New(), models.TriggerPipelineRequest{}, ExecutionPorts{})
	require.NoError(t, err)
	require.Len(t, plan.Nodes, 1)
	require.Len(t, plan.Nodes[0].Outputs, 1)
	tx := plan.Nodes[0].Outputs[0]
	require.Equal(t, "orders.v1", tx.OutputSchemaVersion)
	require.Equal(t, "existing.ndjson", tx.LogicalPath)
	require.Empty(t, tx.FileFormat, "an inferred graph default is not an explicit Parquet selection")
	body, err := datasetOutputCommitBody(tx, inlineOrdersResult(t, parquetScanOrder))
	require.NoError(t, err)
	require.Empty(t, body.OutputSchemaVersion)
	require.Equal(t, "existing.ndjson", body.Files[0].LogicalPath)
}

func TestScheduledTypedOutputValidatesBeforeHandoffAndReplaysExactly(t *testing.T) {
	dataset := uuid.New()
	reservation := "ri.foundry.main.transaction." + uuid.NewString()
	event := uuid.New()
	store, probe := &receiptTestStore{}, &abortProbe{}
	var received [][]byte
	var receivedMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		receivedMu.Lock()
		received = append(received, raw)
		first := len(received) == 1
		receivedMu.Unlock()
		if first {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"dataset_id": dataset, "transaction": map[string]any{"id": uuid.New(), "status": "COMMITTED"}})
	}))
	t.Cleanup(server.Close)
	committer := ScheduledOutputCommitter{
		Store: store, Aborter: probe, BaseURL: server.URL, EventID: event, BuildBranch: "main",
		Reservations: map[string]string{dataset.String(): reservation},
	}
	tx := executor.OutputTransaction{
		DatasetRID: dataset.String(), Branch: "main", OutputNodeID: "orders-output",
		TransactionRID: reservation, OutputSchemaVersion: "orders.v1",
	}
	ctx := ContextWithSchedulerBearer(t.Context(), "caller")
	require.ErrorContains(t, committer.Commit(ctx, tx, executor.NodeResult{}), "dataset_output_parquet_invalid")
	require.Zero(t, probe.calls)
	require.Empty(t, store.body)

	err := committer.Commit(ctx, tx, inlineOrdersResult(t, parquetScanOrder))
	require.ErrorIs(t, err, ErrScheduledPublishUnknown)
	require.Equal(t, 1, probe.calls)
	var body scheduledCommitRequest
	require.NoError(t, json.Unmarshal(store.body, &body))
	require.Equal(t, "orders.v1", body.OutputSchemaVersion)
	require.Equal(t, scheduleevents.DeriveOutputPublishKey(event, tx.OutputNodeID, tx.DatasetRID), body.IdempotencyKey)
	require.NoError(t, committer.ReplayReceipt(ctx, body.IdempotencyKey, tx.OutputNodeID, tx.DatasetRID, store.body, reservation))
	receivedMu.Lock()
	defer receivedMu.Unlock()
	require.Equal(t, [][]byte{store.body, store.body}, received)
	require.Equal(t, 1, store.completed)
	require.Equal(t, 1, probe.calls, "replay does not exchange the reservation again")
}
