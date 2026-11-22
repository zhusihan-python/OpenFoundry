package handler

import (
	"encoding/json"
	"testing"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
	"github.com/stretchr/testify/require"
)

func TestPublishedCastTypesSurviveProjection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		filter bool
		drop   bool
	}{{"rows", false, false}, {"empty", true, false}, {"drop casts before empty filter", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			rt := newLightweightTableRuntime()
			run := func(id, parent, kind, payload string) executor.NodeResult {
				node := executor.Node{ID: id}
				if parent != "" {
					node.DependsOn = []string{parent}
				}
				result, err := rt.Run(t.Context(), executor.NodeContext{Node: node}, json.RawMessage(payload), kind)
				require.NoError(t, err)
				return result
			}
			run("in", "", "dataset_input", `{"rows":[{"id":"order","amount":"2","day":"2026-11-23","n":"1"}]}`)
			run("clean", "in", "sql", `{"_stack":{"blocks":[{"kind":"cast","source_column":"amount","target_type":"Double","applied":true},{"kind":"cast","source_column":"day","target_type":"Date","applied":true},{"kind":"cast","source_column":"n","target_type":"Integer","applied":true},{"kind":"rename","renames":[{"from":"amount","to":"net"}],"applied":true}]}}`)
			prev := "clean"
			if tc.drop {
				run("dropped", prev, "drop", `{"columns":["day","net","n"]}`)
				prev = "dropped"
			}
			if tc.filter {
				run("filtered", prev, "filter", `{"predicate":"id = 'missing'"}`)
				prev = "filtered"
			}
			result := run("out", prev, "output_dataset", `{}`)
			body, err := datasetOutputCommitBody(executor.OutputTransaction{FileFormat: "NDJSON"}, result)
			require.NoError(t, err)
			if tc.drop {
				require.Equal(t, []string{"id"}, body.PreviewColumns)
				require.Len(t, body.Schema.Fields, 1)
				return
			}
			require.Equal(t, []string{"day", "id", "n", "net"}, body.PreviewColumns)
			got := map[string]string{}
			for _, f := range body.Schema.Fields {
				got[f.Name] = f.Type
			}
			require.Equal(t, map[string]string{"id": "STRING", "net": "DOUBLE", "day": "DATE", "n": "INTEGER"}, got)
		})
	}
}

func TestUnionCastSchemaRequiresSameLogicalType(t *testing.T) {
	for _, positional := range []bool{false, true} {
		rightName := "gross"
		if positional {
			rightName = "other"
		}
		base := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{{Name: "gross", FieldType: "Double"}}}
		peer := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{{Name: rightName, FieldType: "DOUBLE", castTarget: true}}}
		merged := unionCastSchema([]pipelineStrictSchema{base, peer}, positional)
		require.True(t, merged.Fields[0].castTarget)
		require.Equal(t, "DOUBLE", strictTypeName(fieldType(merged.Fields[0])))
		peer.Fields[0].FieldType = "LONG"
		merged = unionCastSchema([]pipelineStrictSchema{base, peer}, positional)
		require.False(t, merged.Fields[0].castTarget)
	}
}

func TestCastPublicationHonorsCompatibleDeclaredOutput(t *testing.T) {
	rt := newLightweightTableRuntime()
	input := executor.NodeContext{Node: executor.Node{ID: "source"}}
	_, err := rt.Run(t.Context(), input, json.RawMessage(`{"rows":[{"value":"3"}]}`), "dataset_input")
	require.NoError(t, err)
	declared := &models.PipelineIRSchema{Fields: []models.PipelineIRField{{Name: "value", FieldType: "Double", Nullable: true}}}
	node := executor.NodeContext{Node: executor.Node{ID: "cast", DependsOn: []string{"source"}, Metadata: map[string]any{runtimeDeclaredSchema: declared}}}
	_, err = rt.Run(t.Context(), node, json.RawMessage(`{"_stack":{"blocks":[{"kind":"cast","applied":true,"source_column":"value","target_type":"Integer"}]}}`), "sql")
	require.NoError(t, err)
	out := executor.NodeContext{Node: executor.Node{ID: "out", DependsOn: []string{"cast"}}}
	result, err := rt.Run(t.Context(), out, json.RawMessage(`{}`), "output_dataset")
	require.NoError(t, err)
	body, err := datasetOutputCommitBody(executor.OutputTransaction{FileFormat: "NDJSON"}, result)
	require.NoError(t, err)
	require.Equal(t, "DOUBLE", body.Schema.Fields[0].Type)
	require.True(t, body.Schema.Fields[0].Nullable)
	field, _ := rt.tableSchema("cast").field("value")
	require.True(t, field.Nullable)
}
