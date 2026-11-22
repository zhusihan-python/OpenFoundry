package handler

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

func TestNormalizeUnitsValues(t *testing.T) {
	for _, test := range []struct {
		from, to, value string
		want            float64
	}{
		{" miles ", "Meters", "1", 1609.344},
		{"in", "ft", "12", 1},
		{"oz", "lb", "16", 1},
		{"lb", "kg", "-2", -0.90718474},
		{"°C", "fahrenheit", "-40", -40},
		{"c", "f", "0", 32},
		{"f", "c", "212", 100},
		{"k", "c", "273.15", 0},
		{"°F", "f", "32.5", 32.5},
	} {
		t.Run(test.from+" to "+test.to+" "+test.value, func(t *testing.T) {
			block := runtimeTransformBlock{SourceColumn: "measurement", SourceUnit: test.from, TargetUnit: test.to, TargetColumn: "converted"}
			rows := []pe.Row{{"measurement": json.RawMessage(test.value)}, {"measurement": json.RawMessage("null")}, {}}
			out, err := applyRuntimeNormalizeUnitsBlock(rows, block)
			require.NoError(t, err)
			value := pe.EvalValueFromJSON(out[0]["converted"])
			var got float64
			require.NoError(t, json.Unmarshal(value.ToJSON(), &got))
			require.InDelta(t, test.want, got, 1e-12)
			require.JSONEq(t, "null", string(out[1]["converted"]))
			require.JSONEq(t, "null", string(out[2]["converted"]))
			require.NotContains(t, rows[0], "converted")
		})
	}
}

func TestNormalizeUnitsRejectsInvalidConfigAndValues(t *testing.T) {
	config := runtimeTransformBlock{SourceColumn: "measurement", SourceUnit: "km", TargetUnit: "m"}
	for _, raw := range []json.RawMessage{json.RawMessage(`"2"`), json.RawMessage("true"), json.RawMessage("[2]")} {
		_, err := applyRuntimeNormalizeUnitsBlock([]pe.Row{{"measurement": raw}}, config)
		require.ErrorContains(t, err, "requires a numeric value")
	}
	_, err := applyRuntimeNormalizeUnitsBlock([]pe.Row{{"measurement": json.RawMessage("1e308")}}, config)
	require.ErrorContains(t, err, "is not finite")

	out, err := applyRuntimeNormalizeUnitsBlock([]pe.Row{{"measurement": json.RawMessage("2")}}, config)
	require.NoError(t, err)
	require.Equal(t, pe.Row{"measurement": json.RawMessage("2000")}, out[0])

	config.TargetUnit = "kg"
	_, err = applyRuntimeNormalizeUnitsBlock(nil, config)
	require.ErrorContains(t, err, "must have the same dimension")
	config.TargetUnit = "unknown"
	_, err = applyRuntimeNormalizeUnitsBlock(nil, config)
	require.ErrorContains(t, err, "unsupported measurement unit")
}

func TestNormalizeUnitsSchemaAddsAndReplacesDouble(t *testing.T) {
	block := runtimeTransformBlock{SourceColumn: "amount", SourceUnit: "km", TargetUnit: "m", TargetColumn: "converted"}
	input := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{{Name: "amount", FieldType: "LONG", Nullable: true}, {Name: "label", FieldType: "STRING"}}}
	report := pipelineStrictValidationReport{}
	schema := applyNormalizeUnitsSchema("convert", input, block, &report)
	require.Empty(t, report.Errors)
	require.Equal(t, []pipelineStrictValidationField{input.Fields[0], input.Fields[1], {Name: "converted", FieldType: "DOUBLE", Nullable: true}}, schema.Fields)
	block.TargetColumn = "label"
	schema = applyNormalizeUnitsSchema("convert", input, block, &report)
	require.Equal(t, []pipelineStrictValidationField{input.Fields[0], {Name: "label", FieldType: "DOUBLE", Nullable: true}}, schema.Fields)
	block.SourceColumn = "label"
	require.Equal(t, input, applyNormalizeUnitsSchema("convert", input, block, &report))
	require.Equal(t, "invalid_unit_conversion_column", report.Errors[0].Code)
}
