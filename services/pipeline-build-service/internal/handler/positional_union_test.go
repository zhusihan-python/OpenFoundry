package handler

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

func TestPositionalUnionPreviewPreservesProjectionOrder(t *testing.T) {
	nodes := []models.PipelineNode{
		{ID: "left", TransformType: "external", Config: json.RawMessage(`{"rows":[{"a":1,"z":"left"}]}`)},
		{ID: "right", TransformType: "external", Config: json.RawMessage(`{"rows":[{"y":"right"},{"b":2,"y":null}]}`)},
		{ID: "left_projection", TransformType: "select", DependsOn: []string{"left"}, Config: json.RawMessage(`{"columns":["z","a"]}`)},
		{ID: "right_projection", TransformType: "sql", DependsOn: []string{"right"}, Config: json.RawMessage(`{"_stack":{"blocks":[{"kind":"select","applied":true,"columns":["y","b"]}]}}`)},
		{ID: "union", TransformType: "sql", DependsOn: []string{"left_projection", "right_projection"}, Config: json.RawMessage(`{"_union":{"union_type":"by_position"}}`)},
		{ID: "output", TransformType: "output_dataset", DependsOn: []string{"union"}},
	}
	full, err := executeLocalPipelinePreview(t.Context(), uuid.New(), "output", nodes, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"z", "a"}, full.Columns)
	require.JSONEq(t, `[{"z":"left","a":1},{"z":"right","a":null},{"z":null,"a":2}]`, string(mustRuntimeJSON(full.Rows)))

	short, err := executeLocalPipelinePreview(t.Context(), uuid.New(), "output", nodes, 2)
	require.NoError(t, err)
	require.Equal(t, full.Columns, short.Columns)
	require.Equal(t, full.Rows[:2], short.Rows)
}

func TestPositionalUnionEmptyDeclaredInputAndWidthMismatch(t *testing.T) {
	nodes := []models.PipelineNode{
		{ID: "schema_source", TransformType: "external", Config: json.RawMessage(`{"rows":[{"a":1,"z":"filtered"}]}`)},
		{ID: "empty", TransformType: "filter", DependsOn: []string{"schema_source"}, Config: json.RawMessage(`{"predicate":"false"}`)},
		{ID: "values", TransformType: "external", Config: json.RawMessage(`{"rows":[{"b":"value","y":null}]}`)},
		{ID: "union", TransformType: "sql", DependsOn: []string{"empty", "values"}, Config: json.RawMessage(`{"_union":{"union_type":"by_position"}}`)},
	}
	schemas := map[string]*models.PipelineIRSchema{
		"empty": {Fields: []models.PipelineIRField{{Name: "z", FieldType: "STRING"}, {Name: "a", FieldType: "LONG"}}},
	}
	out, err := executeLocalPreviewWithSchemas(t.Context(), uuid.New(), "union", nodes, 1, ExecutionPorts{}, schemas)
	require.NoError(t, err)
	require.Equal(t, []string{"z", "a"}, out.Columns)
	require.JSONEq(t, `[{"z":"value","a":null}]`, string(mustRuntimeJSON(out.Rows)))

	nodes[2].Config = json.RawMessage(`{"rows":[{"b":"value"}]}`)
	_, err = executeLocalPreviewWithSchemas(t.Context(), uuid.New(), "union", nodes, 1, ExecutionPorts{}, schemas)
	require.ErrorContains(t, err, "union_position_arity_mismatch")
	nodes[3].Config = json.RawMessage(`{"_union":{"union_type":"by_name"}}`)
	out, err = executeLocalPreviewWithSchemas(t.Context(), uuid.New(), "union", nodes, 1, ExecutionPorts{}, schemas)
	require.NoError(t, err)
	require.Equal(t, []string{"b"}, out.Columns)
	require.JSONEq(t, `[{"b":"value"}]`, string(mustRuntimeJSON(out.Rows)))
}
