package handler

import (
	"encoding/json"
	"testing"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/stretchr/testify/require"
)

func TestPublishedCastTypesSurviveProjection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		filter bool
	}{{"rows", false}, {"empty", true}} {
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
			if tc.filter {
				run("filtered", "clean", "filter", `{"predicate":"id = 'missing'"}`)
				prev = "filtered"
			}
			result := run("out", prev, "output_dataset", `{}`)
			body, err := datasetOutputCommitBody(executor.OutputTransaction{FileFormat: "NDJSON"}, result)
			require.NoError(t, err)
			got := map[string]string{}
			for _, f := range body.Schema.Fields {
				got[f.Name] = f.Type
			}
			require.Equal(t, map[string]string{"id": "STRING", "net": "DOUBLE", "day": "DATE", "n": "INTEGER"}, got)
		})
	}
}
