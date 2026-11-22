package handler

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
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

func TestParseOutputDatasetConfigReadsNestedColumns(t *testing.T) {
	cfg := parseOutputDatasetConfig(json.RawMessage(`{"_output":{"columns":["order_id","total_amount"]}}`))
	require.Equal(t, []string{"order_id", "total_amount"}, cfg.Columns)
	require.Equal(t, []string{"order_id", "total_amount"}, outputColumnsFromConfig(cfg))
}

func TestBindDatasetOutputSchemaRejectsColumnsWithTypedDeclaration(t *testing.T) {
	err := BindDatasetOutputSchema(json.RawMessage(`{"_output":{"output_schema_version":"orders.v1"}}`),
		[]executor.OutputTransaction{{OutputColumns: []string{"order_id"}}})
	require.ErrorContains(t, err, "dataset_output_projection_invalid")
}
