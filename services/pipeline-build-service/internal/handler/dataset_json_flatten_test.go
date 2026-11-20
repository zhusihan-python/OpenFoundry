package handler

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlattenJSONRecordEscapesPathsAndInfersContainers(t *testing.T) {
	row, err := flattenJSONRecord(json.RawMessage(`{"a":{"b":2,"c":null},"a.b":"literal","a\\b":1,"items":[{"id":7}],"empty":{}}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"a.b":2,"a.c":null,"a\\.b":"literal","a\\\\b":1,"items":[{"id":7}],"empty":{}}`, string(row))

	inference := datasetSchemaInference{format: "json"}
	require.NoError(t, inference.addRow(row))
	require.NoError(t, inference.addRow(json.RawMessage(`{"a.b":2.5,"empty":{},"items":[]}`)))
	require.Equal(t, []datasetRowsField{
		{Name: "a.b", Type: "DOUBLE"},
		{Name: "a.c", Type: "STRING", Nullable: true},
		{Name: "a\\.b", Type: "STRING", Nullable: true},
		{Name: "a\\\\b", Type: "LONG", Nullable: true},
		{Name: "empty", Type: "STRUCT"},
		{Name: "items", Type: "ARRAY"},
	}, inference.schema().Fields)
}

func TestFlattenJSONRecordRejectsAmbiguityAndBounds(t *testing.T) {
	tooDeep := strings.Repeat(`{"nested":`, jsonFlattenMaxDepth+1) + "1" + strings.Repeat("}", jsonFlattenMaxDepth+1)
	wide := make([]string, schemaInferenceMaxColumns+1)
	for i := range wide {
		wide[i] = fmt.Sprintf(`"field%d":0`, i)
	}
	for name, input := range map[string]string{
		"dataset_json_flatten_duplicate_key":   `{"obj":{"id":1,"\u0069d":2}}`,
		"dataset_json_flatten_invalid":         `{"":1}`,
		"dataset_json_flatten_depth":           tooDeep,
		"dataset_json_schema_columns_exceeded": "{" + strings.Join(wide, ",") + "}",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := flattenJSONRecord(json.RawMessage(input))
			require.ErrorContains(t, err, name)
		})
	}
}

func TestJSONInferencePreservesOutOfRangeNumbers(t *testing.T) {
	raw := json.RawMessage(`{"limits":{"huge":9223372036854775808,"exponent":1e400}}`)
	row, err := flattenJSONRecord(raw)
	require.NoError(t, err)
	require.Equal(t, `{"limits.exponent":1e400,"limits.huge":9223372036854775808}`, string(row))
	inference := datasetSchemaInference{format: "json"}
	require.NoError(t, inference.addRow(row))
	require.Equal(t, []datasetRowsField{
		{Name: "limits.exponent", Type: "STRING"},
		{Name: "limits.huge", Type: "STRING"},
	}, inference.schema().Fields)
}

func TestJSONInferencePreservesNonzeroUnderflow(t *testing.T) {
	raw := json.RawMessage(`{"limits":{"negative":-1e-400,"positive":1e-400,"subnormal":5e-324,"zero":-0.0E-400}}`)
	row, err := flattenJSONRecord(raw)
	require.NoError(t, err)
	require.Equal(t, `{"limits.negative":-1e-400,"limits.positive":1e-400,"limits.subnormal":5e-324,"limits.zero":-0.0E-400}`, string(row))

	inference := datasetSchemaInference{format: "json"}
	require.NoError(t, inference.addRow(row))
	require.Equal(t, []datasetRowsField{
		{Name: "limits.negative", Type: "STRING"},
		{Name: "limits.positive", Type: "STRING"},
		{Name: "limits.subnormal", Type: "DOUBLE"},
		{Name: "limits.zero", Type: "DOUBLE"},
	}, inference.schema().Fields)
}

func TestDatasetSnapshotFlattenJSONRequiresBoolean(t *testing.T) {
	base := `{"branch":"main","version":1,"transaction_id":"11111111-1111-1111-1111-111111111111","flatten_json":%s}`
	pin, err := parseDatasetInputSnapshot(json.RawMessage(fmt.Sprintf(base, "true")))
	require.NoError(t, err)
	require.True(t, pin.FlattenJSON)
	for _, invalid := range []string{`"true"`, "null", "1"} {
		_, err := parseDatasetInputSnapshot(json.RawMessage(fmt.Sprintf(base, invalid)))
		require.ErrorContains(t, err, "flatten_json must be a boolean")
	}
}

func TestDatasetSnapshotExcelCannotShadowFlattening(t *testing.T) {
	base := `{"branch":"main","version":1,"transaction_id":"11111111-1111-1111-1111-111111111111",%s}`
	pin, err := parseDatasetInputSnapshot(json.RawMessage(fmt.Sprintf(base, `"excel":{"sheet":"订单"}`)))
	require.NoError(t, err)
	require.JSONEq(t, `{"sheet":"订单"}`, string(pin.Excel))
	for _, invalid := range []string{`"excel":null`, `"excel":[]`, `"excel":{},"flatten_json":true`} {
		_, err := parseDatasetInputSnapshot(json.RawMessage(fmt.Sprintf(base, invalid)))
		require.ErrorContains(t, err, "excel must be an object")
	}
}
