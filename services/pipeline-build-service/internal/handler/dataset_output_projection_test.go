package handler

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

func projectionResultMeta() map[string]any {
	return map[string]any{
		"runtime":       "lightweight_table",
		"columns":       []string{"customer", "order_id", "total_amount"},
		"rows_affected": 2,
		"data_rows": []map[string]json.RawMessage{
			{"order_id": json.RawMessage(`"o-1"`), "customer": json.RawMessage(`"acme"`), "total_amount": json.RawMessage(`12.5`)},
			{"order_id": json.RawMessage(`"o-2"`), "customer": json.RawMessage(`"globex"`), "total_amount": json.RawMessage(`30`)},
		},
	}
}

func projectedNDJSONLines(t *testing.T, body datasetOutputCommitRequest) []map[string]json.RawMessage {
	t.Helper()
	file := body.Files[0]
	require.Equal(t, "part-00000.ndjson", file.LogicalPath)
	lines := strings.Split(strings.TrimRight(string(file.ContentBase64), "\n"), "\n")
	out := make([]map[string]json.RawMessage, 0, len(lines))
	for _, line := range lines {
		var row map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(line), &row))
		out = append(out, row)
	}
	return out
}

func rowKeys(row map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func TestDatasetOutputCommitBodyAppliesDeclaredColumnProjection(t *testing.T) {
	body, err := datasetOutputCommitBody(executor.OutputTransaction{
		OutputColumns: []string{"total_amount", "order_id"},
		FileFormat:    "NDJSON",
		LogicalPath:   "part-00000.ndjson",
	}, executor.NodeResult{Metadata: projectionResultMeta()})
	require.NoError(t, err)
	require.Equal(t, []string{"total_amount", "order_id"}, body.PreviewColumns)
	fieldNames := make([]string, 0, len(body.Schema.Fields))
	for _, field := range body.Schema.Fields {
		fieldNames = append(fieldNames, field.Name)
	}
	require.Equal(t, []string{"total_amount", "order_id"}, fieldNames)
	rows := projectedNDJSONLines(t, body)
	require.Len(t, rows, 2)
	require.Equal(t, []string{"order_id", "total_amount"}, rowKeys(rows[0]))
	require.Equal(t, json.RawMessage(`"o-1"`), rows[0]["order_id"])
	require.Equal(t, json.RawMessage(`12.5`), rows[0]["total_amount"])
	require.NotContains(t, rows[0], "customer")
}

func TestDatasetOutputCommitBodyKeepsAllColumnsWithoutDeclaration(t *testing.T) {
	body, err := datasetOutputCommitBody(executor.OutputTransaction{
		FileFormat:  "NDJSON",
		LogicalPath: "part-00000.ndjson",
	}, executor.NodeResult{Metadata: projectionResultMeta()})
	require.NoError(t, err)
	require.Equal(t, []string{"customer", "order_id", "total_amount"}, body.PreviewColumns)
	require.Len(t, body.PreviewRows[0], 3)
	rows := projectedNDJSONLines(t, body)
	require.Len(t, rows, 2)
	require.Equal(t, []string{"customer", "order_id", "total_amount"}, rowKeys(rows[0]))
}

func TestDatasetOutputCommitBodyFillsMissingProjectedColumnWithNull(t *testing.T) {
	meta := projectionResultMeta()
	meta["data_rows"] = []map[string]json.RawMessage{
		{"order_id": json.RawMessage(`"o-1"`), "customer": json.RawMessage(`"acme"`)},
	}
	body, err := datasetOutputCommitBody(executor.OutputTransaction{
		OutputColumns: []string{"order_id", "total_amount"},
		FileFormat:    "NDJSON",
		LogicalPath:   "part-00000.ndjson",
	}, executor.NodeResult{Metadata: meta})
	require.NoError(t, err)
	rows := projectedNDJSONLines(t, body)
	require.Equal(t, json.RawMessage(`null`), rows[0]["total_amount"])
}

func TestDatasetOutputCommitBodyRejectsInvalidColumnDeclarations(t *testing.T) {
	cases := []struct {
		name     string
		columns  []string
		expected string
	}{
		{"unknown column", []string{"order_id", "shipped_at"}, "does not carry"},
		{"duplicate column", []string{"order_id", "order_id"}, "repeats column"},
		// Whitespace-only names are trimmed to "" when the config is parsed
		// (outputColumnsFromConfig); a transaction built elsewhere still
		// fails loudly on an empty entry.
		{"empty column name", []string{"order_id", ""}, "empty column name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := datasetOutputCommitBody(executor.OutputTransaction{
				OutputColumns: tc.columns,
				FileFormat:    "NDJSON",
				LogicalPath:   "part-00000.ndjson",
			}, executor.NodeResult{Metadata: projectionResultMeta()})
			require.ErrorContains(t, err, "dataset_output_projection_invalid")
			require.ErrorContains(t, err, tc.expected)
		})
	}
}

func TestBindDatasetOutputSchemaRejectsColumnsWithTypedDeclaration(t *testing.T) {
	err := BindDatasetOutputSchema(json.RawMessage(`{"_output":{"output_schema_version":"orders.v1"}}`),
		[]executor.OutputTransaction{{OutputColumns: []string{"order_id"}}})
	require.ErrorContains(t, err, "dataset_output_projection_invalid")
}

func TestProjectionGraphPreviewAndPublicationShareColumnOrder(t *testing.T) {
	for _, test := range []struct {
		name, predicate string
		wantRows        int
	}{
		{"ordered", "true", 1}, {"empty filter", "false", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := uuid.New()
			nodes := []models.PipelineNode{
				{ID: "source", TransformType: "dataset_input", Config: json.RawMessage(`{"rows":[{"id":"A","extra":false,"amount":7}]}`)},
				{ID: "filter", TransformType: "filter", Config: mustRuntimeJSON(map[string]string{"predicate": test.predicate}), DependsOn: []string{"source"}},
				{ID: "out", TransformType: "output_dataset", Config: json.RawMessage(`{"_output":{"columns":["id","amount"]}}`), DependsOn: []string{"filter"}, OutputDatasetID: &target},
			}
			schemas := map[string]*models.PipelineIRSchema{"source": {Fields: []models.PipelineIRField{
				{Name: "id", FieldType: "STRING"}, {Name: "amount", FieldType: "LONG"}, {Name: "extra", FieldType: "BOOLEAN"},
			}}}
			pid := uuid.New()
			preview, err := executeLocalPreviewWithSchemas(t.Context(), pid, "out", nodes, 5, ExecutionPorts{}, schemas)
			require.NoError(t, err)
			require.Equal(t, []string{"id", "amount"}, preview.Columns)
			require.Len(t, preview.Rows, test.wantRows)
			if test.wantRows > 0 {
				require.JSONEq(t, `{"id":"A","amount":7}`, string(mustRuntimeJSON(preview.Rows[0])))
			}

			rt := newLightweightTableRuntime()
			for _, n := range nodes {
				outputs, _, err := plannedNodeOutputs(pid, uuid.New(), n, nodes)
				require.NoError(t, err)
				node := executor.Node{ID: n.ID, DependsOn: n.DependsOn, Outputs: outputs, Metadata: map[string]any{runtimeDeclaredSchema: schemas[n.ID]}}
				result, err := rt.Run(t.Context(), executor.NodeContext{Node: node}, n.Config, n.TransformType)
				require.NoError(t, err)
				if n.ID != "out" {
					continue
				}
				body, err := datasetOutputCommitBody(outputs[0], result)
				require.NoError(t, err)
				require.Equal(t, preview.Columns, body.PreviewColumns)
				require.Equal(t, []datasetOutputField{{Name: "id", Type: "STRING"}, {Name: "amount", Type: "LONG"}}, body.Schema.Fields)
				if test.wantRows == 0 {
					require.NotNil(t, body.Files[0].ContentBase64)
					require.Empty(t, body.Files[0].ContentBase64)
				} else {
					require.Equal(t, preview.Rows, projectedNDJSONLines(t, body))
				}
			}
		})
	}
}

func TestProjectionValidatesGraphAndStoredJobs(t *testing.T) {
	target := uuid.New()
	for _, config := range []string{
		`{"_output":{"columns":"id","dataset_rid":"chosen-target"}}`,
		`{"_output":{"columns":null}}`, `{"_output":{"columns":[]}}`, `{"_output":{"columns":[null]}}`,
		`{"_output":{"columns":["a",7]}}`, `{"_output":{"columns":["a"],"kind":"object_type"}}`,
		`{"_output":{"columns":["a"],"kind":"link_type"}}`, `{"_output":{"columns":["a"],"file_format":"PARQUET"}}`,
		`{"_output":{"columns":["a"],"logical_path":"output.parquet"}}`,
		`{"_output":{"columns":["a"],"output_schema_version":"orders.v1","file_format":"NDJSON"}}`,
	} {
		node := models.PipelineNode{ID: "out", TransformType: "output_dataset", Config: json.RawMessage(config)}
		outputs, rid, err := plannedNodeOutputs(uuid.New(), uuid.New(), node, []models.PipelineNode{node})
		require.ErrorContains(t, err, "dataset_output_projection_invalid", config)
		require.Empty(t, rid)
		require.Empty(t, outputs)
		_, err = executeLocalPipelinePreviewWithPorts(t.Context(), uuid.New(), node.ID, []models.PipelineNode{node}, 5, ExecutionPorts{})
		require.ErrorContains(t, err, "dataset_output_projection_invalid", config)
		require.ErrorContains(t, BindDatasetOutputSchema(node.Config, []executor.OutputTransaction{{DatasetRID: target.String()}}), "dataset_output_projection_invalid")
	}
	for _, columns := range [][]string{{"ghost"}, {"a", "a"}} {
		nodes := []models.PipelineNode{
			{ID: "source", TransformType: "dataset_input", Config: json.RawMessage(`{"rows":[{"a":42}]}`)},
			{ID: "out", TransformType: "output_dataset", Config: mustRuntimeJSON(map[string]any{"_output": map[string]any{"columns": columns}}), DependsOn: []string{"source"}},
		}
		_, err := executeLocalPipelinePreviewWithPorts(t.Context(), uuid.New(), "out", nodes, 5, ExecutionPorts{})
		require.ErrorContains(t, err, "dataset_output_projection_invalid")
	}
	stored := []executor.OutputTransaction{{DatasetRID: target.String()}}
	payload := json.RawMessage(`{"_output":{"columns":["b","a"]}}`)
	require.NoError(t, BindDatasetOutputSchema(payload, stored))
	require.Equal(t, []string{"b", "a"}, stored[0].OutputColumns)
	require.True(t, PayloadDeclaresDatasetOutputSchema(payload))
	require.False(t, PayloadDeclaresDatasetOutputSchema([]byte(`{"columns":["b","a"]}`)))

	drop := models.PipelineNode{ID: "drop", TransformType: "drop", Config: json.RawMessage(`{"columns":["extra"]}`), OutputDatasetID: &target}
	tx, _, err := plannedNodeOutputs(uuid.New(), uuid.New(), drop, []models.PipelineNode{drop})
	require.NoError(t, err)
	require.Empty(t, tx[0].OutputColumns)
}

func TestProjectionOfExcelRowsPreservesOnlySelectedSchemaFields(t *testing.T) {
	meta := projectionResultMeta()
	schema := &datasetOutputSchema{FileFormat: "TEXT", Fields: []datasetOutputField{{Name: "customer", Type: "STRING"}, {Name: "total_amount", Type: "DECIMAL", Precision: 15, Scale: 2}, {Name: "order_id", Type: "STRING"}}}
	meta["excel_files_output_schema"] = schema
	body, err := datasetOutputCommitBody(executor.OutputTransaction{OutputColumns: []string{"order_id", "total_amount"}}, executor.NodeResult{Metadata: meta})
	require.NoError(t, err)
	require.Equal(t, []datasetOutputField{schema.Fields[2], schema.Fields[1]}, body.Schema.Fields)
	require.Len(t, schema.Fields, 3)
}
