package handler

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

func TestWindowAggregatePartitionsTiesAndFrameEdges(t *testing.T) {
	var rows []pe.Row
	require.NoError(t, json.Unmarshal([]byte(`[
		{"id":"b1","p":"B","k":1,"v":4},
		{"id":"a2","p":"A","k":2,"v":8},
		{"id":"a1","p":"A","k":1,"v":2},
		{"id":"a3","p":"A","k":2,"v":null},
		{"id":"bn","p":"B","k":null,"v":9},
		{"id":"an","p":"A","k":null,"v":4},
		{"id":"c","p":null,"k":0,"v":null},
		{"id":"d","k":1,"v":null}
	]`), &rows))
	block := runtimeTransformBlock{
		Kind: "window_aggregate", GroupBy: []string{"p"}, SortKeys: []runtimeSortKey{{Column: "k"}},
		Frame: &runtimeWindowFrame{Preceding: 1, Following: 1},
		Aggregations: []runtimeAggregation{
			{Function: "count", TargetColumn: "rows"},
			{Function: "count", SourceColumn: "v", TargetColumn: "present"},
			{Function: "sum", SourceColumn: "v", TargetColumn: "total"},
		},
	}
	out, err := applyRuntimeWindowBlock(rows, block)
	require.NoError(t, err)
	total := []string{"13", "10", "10", "12", "13", "4", "null", "null"}
	present := []string{"2", "2", "2", "2", "2", "1", "0", "0"}
	frameRows := []string{"2", "3", "2", "3", "2", "2", "2", "2"}
	require.Len(t, out, len(rows))
	for i, row := range out {
		require.Equal(t, rows[i]["id"], row["id"])
		require.Equal(t, total[i], string(row["total"]))
		require.Equal(t, present[i], string(row["present"]))
		require.Equal(t, frameRows[i], string(row["rows"]))
		require.NotContains(t, rows[i], "total")
	}
	block.Frame = &runtimeWindowFrame{Preceding: math.MaxInt, Following: math.MaxInt}
	out, err = applyRuntimeWindowBlock(rows, block)
	require.NoError(t, err)
	require.Equal(t, "14", string(out[1]["total"]))
	require.Equal(t, "13", string(out[0]["total"]))
	out, err = applyRuntimeWindowBlock(nil, block)
	require.NoError(t, err)
	require.Empty(t, out)
}

func TestWindowRangeMatchesDirectAccumulation(t *testing.T) {
	var rows []pe.Row
	require.NoError(t, json.Unmarshal([]byte(`[{"v":null},{"v":1.0},{"v":1},{"v":9007199254740993},{"v":-5},{"v":12},{"v":3}]`), &rows))
	indices := []int{0, 1, 2, 3, 4, 5, 6}
	for _, fn := range []string{"count", "sum", "avg", "min", "max"} {
		measure := runtimeAggregation{Function: fn, SourceColumn: "v", TargetColumn: "result"}
		for preceding := 0; preceding < len(rows); preceding++ {
			for following := 0; following < len(rows); following++ {
				frame := runtimeWindowFrame{Preceding: preceding, Following: following}
				out := cloneRows(rows)
				require.NoError(t, applyWindowMeasure(rows, out, indices, frame, measure))
				for i := range rows {
					var direct runtimeAggregateState
					for j := max(0, i-preceding); j <= min(len(rows)-1, i+following); j++ {
						require.NoError(t, direct.add(rows[j]["v"], measure))
					}
					want, err := direct.result(fn)
					require.NoError(t, err)
					require.Equal(t, want, out[i]["result"], "%s row %d frame %v", fn, i, frame)
				}
			}
		}
	}
}

func TestWindowSchemaAndRuntimeRejectInvalidConfiguration(t *testing.T) {
	schema := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{{Name: "k", FieldType: "LONG"}}}
	block := runtimeTransformBlock{
		Kind: "window_aggregate", SortKeys: []runtimeSortKey{{Column: "k", Direction: "desc"}},
		Frame:        &runtimeWindowFrame{Preceding: 0, Following: 2},
		Aggregations: []runtimeAggregation{{Function: "count", TargetColumn: "rows"}},
	}
	var valid pipelineStrictValidationReport
	out := applyWindowSchema("win", schema, block, &valid)
	require.Empty(t, valid.Errors)
	require.Equal(t, []pipelineStrictValidationField{
		schema.Fields[0], {Name: "rows", FieldType: "LONG"},
	}, out.Fields)

	var missing pipelineStrictValidationReport
	block.SortKeys[0].Column = "absent"
	applyWindowSchema("win", schema, block, &missing)
	require.Len(t, missing.Errors, 1)
	require.Equal(t, "missing_column", missing.Errors[0].Code)
	block.SortKeys[0].Column = "k"

	rows := []pe.Row{
		{"k": json.RawMessage("1")},
		{"k": json.RawMessage("2"), "rows": json.RawMessage("null")},
	}
	_, err := applyRuntimeWindowBlock(rows, block)
	require.ErrorContains(t, err, "already exists in row 2")

	block.Frame.Following = -1
	_, err = applyRuntimeWindowBlock(nil, block)
	require.ErrorContains(t, err, "nonnegative")
}

func TestWindowPreviewLimitsDisplayAfterCompletingFrame(t *testing.T) {
	nodes := []models.PipelineNode{
		{ID: "source", TransformType: "external", Config: json.RawMessage(`{"rows":[{"k":2,"v":8},{"k":1,"v":2},{"k":3,"v":4}]}`)},
		{ID: "window", TransformType: "sql", DependsOn: []string{"source"}, Config: json.RawMessage(`{"_stack":{"blocks":[{"kind":"window_aggregate","applied":true,"sort_keys":[{"column":"k"}],"frame":{"preceding":1,"following":1},"aggregations":[{"function":"sum","source_column":"v","target_column":"total"}]}]}}`)},
	}
	out, err := executeLocalPipelinePreview(t.Context(), uuid.New(), "window", nodes, 1)
	require.NoError(t, err)
	require.Len(t, out.Rows, 1)
	require.Equal(t, "14", string(out.Rows[0]["total"]))
}
