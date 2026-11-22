package handler

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

func TestRuntimeAggregateGroupingAndEmptyInputs(t *testing.T) {
	var rows, want []pe.Row
	require.NoError(t, json.Unmarshal([]byte(`[
		{"region":"B","slot":1,"value":3},
		{"region":"A","slot":1,"value":null},
		{"region":"B","slot":1.0,"value":7.5},
		{"region":"A","slot":1},
		{"region":"A","value":9007199254740993},
		{"region":"A","slot":null,"value":1},
		{"region":"B","slot":"1","value":4}
	]`), &rows))
	measures := []runtimeAggregation{
		{Function: "count", TargetColumn: "rows"}, {Function: "count", SourceColumn: "value", TargetColumn: "present"},
		{Function: "sum", SourceColumn: "value", TargetColumn: "total"}, {Function: "avg", SourceColumn: "value", TargetColumn: "mean"},
		{Function: "min", SourceColumn: "value", TargetColumn: "lowest"}, {Function: "max", SourceColumn: "value", TargetColumn: "highest"},
	}
	block := runtimeTransformBlock{GroupBy: []string{"region", "slot"}, Aggregations: measures}
	out, err := applyRuntimeAggregateBlock(rows, block)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(`[
		{"region":"B","slot":1,"rows":2,"present":2,"total":10.5,"mean":5.25,"lowest":3,"highest":7.5},
		{"region":"A","slot":1,"rows":2,"present":0,"total":null,"mean":null,"lowest":null,"highest":null},
		{"region":"A","slot":null,"rows":2,"present":2,"total":9007199254740994,"mean":4.503599627370497e+15,"lowest":1,"highest":9007199254740993},
		{"region":"B","slot":"1","rows":1,"present":1,"total":4,"mean":4,"lowest":4,"highest":4}
	]`), &want))
	require.Equal(t, want, out)
	out, err = applyRuntimeAggregateBlock(nil, block)
	require.NoError(t, err)
	require.Empty(t, out)
	block.GroupBy = nil
	out, err = applyRuntimeAggregateBlock(nil, block)
	require.NoError(t, err)
	require.Len(t, out, 1)
	encoded, err := json.Marshal(out[0])
	require.NoError(t, err)
	require.JSONEq(t, `{"rows":0,"present":0,"total":null,"mean":null,"lowest":null,"highest":null}`, string(encoded))
}

func TestRuntimeAggregateRejectsOverflowAndInvalidValues(t *testing.T) {
	block := runtimeTransformBlock{Aggregations: []runtimeAggregation{{Function: "sum", SourceColumn: "v", TargetColumn: "total"}}}
	for _, test := range []struct{ left, right, err string }{
		{"9223372036854775807", "1", "integer sum exceeds int64"},
		{"1e308", "1e308", "aggregate result is not finite"},
		{`"1"`, "1", "requires a numeric value"},
	} {
		_, err := applyRuntimeAggregateBlock([]pe.Row{{"v": json.RawMessage(test.left)}, {"v": json.RawMessage(test.right)}}, block)
		require.ErrorContains(t, err, test.err)
	}
	block.GroupBy = []string{"total"}
	_, err := applyRuntimeAggregateBlock(nil, block)
	require.ErrorContains(t, err, "empty or repeated")
}

func TestRuntimeAggregateSchemaTypes(t *testing.T) {
	in := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{{Name: "region", FieldType: "STRING"}, {Name: "v", FieldType: "LONG"}}}
	block := runtimeTransformBlock{GroupBy: []string{"region"}, Aggregations: []runtimeAggregation{
		{Function: "count", TargetColumn: "rows"}, {Function: "sum", SourceColumn: "v", TargetColumn: "total"}, {Function: "avg", SourceColumn: "v", TargetColumn: "mean"},
	}}
	report := pipelineStrictValidationReport{}
	out := applyAggregateSchema("aggr", in, block, &report)
	require.Empty(t, report.Errors)
	require.Equal(t, []pipelineStrictValidationField{
		in.Fields[0], {Name: "rows", FieldType: "LONG"}, {Name: "total", FieldType: "LONG", Nullable: true}, {Name: "mean", FieldType: "DOUBLE", Nullable: true},
	}, out.Fields)
}

func TestConditionalAggregateKeepsGroupsAndFiltersEachMeasure(t *testing.T) {
	var input, expected []pe.Row
	require.NoError(t, json.Unmarshal([]byte(`[
		{"g":"a","flag":true,"priority":false,"v":9007199254740993},
		{"g":"a","flag":false,"priority":true,"v":1},
		{"g":"a","flag":null,"priority":false,"v":8},
		{"g":"b","flag":false,"priority":null,"v":9},
		{"g":"a","flag":true,"priority":false,"v":null}
	]`), &input))
	block := runtimeTransformBlock{
		Kind: "conditional_aggregate", GroupBy: []string{"g"},
		Aggregations: []runtimeAggregation{
			{Function: "count", TargetColumn: "selected", ConditionColumn: "flag"},
			{Function: "count", SourceColumn: "v", TargetColumn: "present", ConditionColumn: "flag"},
			{Function: "sum", SourceColumn: "v", TargetColumn: "total", ConditionColumn: "flag"},
			{Function: "max", SourceColumn: "v", TargetColumn: "highest_priority", ConditionColumn: "priority"},
		},
	}
	require.NoError(t, json.Unmarshal([]byte(`[
		{"g":"a","selected":2,"present":1,"total":9007199254740993,"highest_priority":1},
		{"g":"b","selected":0,"present":0,"total":null,"highest_priority":null}
	]`), &expected))
	out, err := applyRuntimeAggregateBlock(input, block)
	require.NoError(t, err)
	require.Equal(t, expected, out)
	out, err = applyRuntimeAggregateBlock(nil, block)
	require.NoError(t, err)
	require.Empty(t, out)
	block.GroupBy = nil
	out, err = applyRuntimeAggregateBlock(nil, block)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, json.RawMessage("0"), out[0]["selected"])
	require.Equal(t, json.RawMessage("null"), out[0]["total"])

	block.Kind = "aggregate"
	out, err = applyRuntimeAggregateBlock(input, block)
	require.NoError(t, err)
	require.Equal(t, json.RawMessage("5"), out[0]["selected"])
}

func TestConditionalAggregateRejectsInvalidCondition(t *testing.T) {
	block := runtimeTransformBlock{Kind: "conditional_aggregate", Aggregations: []runtimeAggregation{
		{Function: "count", TargetColumn: "matched"},
	}}
	_, err := applyRuntimeAggregateBlock(nil, block)
	require.ErrorContains(t, err, "requires a Boolean condition column")

	block.Aggregations[0].ConditionColumn = "flag"
	for _, value := range []json.RawMessage{nil, json.RawMessage(`"true"`), json.RawMessage("1")} {
		out, err := applyRuntimeAggregateBlock([]pe.Row{{"flag": json.RawMessage("true")}, {"flag": value}}, block)
		require.ErrorContains(t, err, "must be Boolean or NULL in row 2")
		require.Nil(t, out)
	}
	for _, tc := range []struct {
		fields []pipelineStrictValidationField
		code   string
	}{
		{nil, "missing_column"},
		{[]pipelineStrictValidationField{{Name: "flag", FieldType: "STRING"}}, "invalid_aggregate_condition"},
	} {
		var report pipelineStrictValidationReport
		applyAggregateSchema("conditional", pipelineStrictSchema{Known: true, Fields: tc.fields}, block, &report)
		require.Len(t, report.Errors, 1)
		require.Equal(t, tc.code, report.Errors[0].Code)
	}
	var report pipelineStrictValidationReport
	out := applyAggregateSchema("conditional", pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{
		{Name: "flag", FieldType: "BOOLEAN", Nullable: true},
	}}, block, &report)
	require.Empty(t, report.Errors)
	require.Equal(t, []pipelineStrictValidationField{{Name: "matched", FieldType: "LONG"}}, out.Fields)
}
