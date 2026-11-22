package handler

import (
	"encoding/json"
	"testing"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
	"github.com/stretchr/testify/require"
)

func TestRuntimeFormulaAddsAndReplacesScalarColumns(t *testing.T) {
	rows := []pe.Row{
		{"id": json.RawMessage(`"first"`), "name": json.RawMessage(`"Ada"`), "n": json.RawMessage("9007199254740993")},
		{"id": json.RawMessage(`"second"`), "name": json.RawMessage("null"), "n": json.RawMessage("-4")},
	}
	out, err := applyRuntimeFormulaBlock(rows, runtimeTransformBlock{TargetColumn: " n ", Expression: "n + 1"})
	require.NoError(t, err)
	out, err = applyRuntimeFormulaBlock(out, runtimeTransformBlock{TargetColumn: "uppercase", Expression: "upper(name)"})
	require.NoError(t, err)
	require.Equal(t, []pe.Row{
		{"id": json.RawMessage(`"first"`), "name": json.RawMessage(`"Ada"`), "n": json.RawMessage("9007199254740994"), "uppercase": json.RawMessage(`"ADA"`)},
		{"id": json.RawMessage(`"second"`), "name": json.RawMessage("null"), "n": json.RawMessage("-3"), "uppercase": json.RawMessage("null")},
	}, out)
	require.Equal(t, "9007199254740993", string(rows[0]["n"]))
	require.NotContains(t, rows[0], "uppercase")
}

func TestRuntimeFormulaRejectsInvalidExpressionsAndRows(t *testing.T) {
	testCases := []struct {
		expression string
		errorText  string
	}{
		{"n +", "formula expression parse"},
		{"sum(n)", "supported scalar functions"},
		{"abs(n, n)", "correct number of arguments"},
		{"missing + 1", `formula target "result" row 1`},
		{"n + 1", `formula target "result" row 2`},
		{"objects", "requires a scalar result"},
	}
	rows := []pe.Row{
		{"n": json.RawMessage("5"), "objects": json.RawMessage("[1,2]")},
		{"n": json.RawMessage(`"bad"`)},
	}

	for _, tc := range testCases {
		t.Run(tc.expression, func(t *testing.T) {
			out, err := applyRuntimeFormulaBlock(rows, runtimeTransformBlock{TargetColumn: "result", Expression: tc.expression})
			require.ErrorContains(t, err, tc.errorText)
			require.Nil(t, out)
			require.NotContains(t, rows[0], "result")
		})
	}
	empty, err := applyRuntimeFormulaBlock(nil, runtimeTransformBlock{TargetColumn: "result", Expression: "5"})
	require.NoError(t, err)
	require.Empty(t, empty)
	_, err = applyRuntimeFormulaBlock(nil, runtimeTransformBlock{Expression: "5"})
	require.ErrorContains(t, err, "requires a target column")
}

func TestRuntimeFormulaInfersOutputAndRejectsInvalidTypes(t *testing.T) {
	input := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{
		{Name: "n", FieldType: "LONG"},
		{Name: "name", FieldType: "STRING"},
	}}
	var report pipelineStrictValidationReport
	out := applyFormulaSchema("formula", input, runtimeTransformBlock{TargetColumn: "name", Expression: "n + 0.5"}, &report)
	require.Empty(t, report.Errors)
	require.Equal(t, []pipelineStrictValidationField{
		{Name: "n", FieldType: "LONG"},
		{Name: "name", FieldType: "DOUBLE", Nullable: true},
	}, out.Fields)
	require.Equal(t, "STRING", input.Fields[1].FieldType)

	out = applyFormulaSchema("formula", input, runtimeTransformBlock{TargetColumn: "new", Expression: "missing + 1"}, &report)
	require.Equal(t, input, out)
	require.Len(t, report.Errors, 1)
	require.Equal(t, "missing_column", report.Errors[0].Code)
}

func TestRuntimeDeriveColumnAppendsAndRejectsSparseCollision(t *testing.T) {
	first := pe.Row{"n": json.RawMessage("6")}
	second := pe.Row{"n": json.RawMessage("7"), "derived": json.RawMessage("null")}
	block := runtimeTransformBlock{Kind: "derive_column", TargetColumn: " derived ", Expression: "n + 1"}
	out, err := applyRuntimeFormulaBlock([]pe.Row{first}, block)
	require.NoError(t, err)
	require.Equal(t, []pe.Row{{"n": json.RawMessage("6"), "derived": json.RawMessage("7")}}, out)
	require.NotContains(t, first, "derived")

	out, err = applyRuntimeFormulaBlock([]pe.Row{first, second}, block)
	require.ErrorContains(t, err, `derived column target "derived" already exists in row 2`)
	require.Nil(t, out)
	require.NotContains(t, first, "derived")
	require.Equal(t, "null", string(second["derived"]))

	block.Kind = "formula"
	out, err = applyRuntimeFormulaBlock([]pe.Row{first, second}, block)
	require.NoError(t, err)
	require.Equal(t, "8", string(out[1]["derived"]))
}

func TestRuntimeDeriveColumnSchemaRejectsKnownCollisionAndAllowsAppend(t *testing.T) {
	input := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{{Name: "n", FieldType: "LONG"}}}
	block := runtimeTransformBlock{Kind: "derive_column", TargetColumn: "n", Expression: "n + 1"}

	var rejected pipelineStrictValidationReport
	require.Equal(t, input, applyFormulaSchema("derived", input, block, &rejected))
	require.Len(t, rejected.Errors, 1)
	require.Equal(t, "duplicate_column_name", rejected.Errors[0].Code)

	block.TargetColumn = "next_n"
	var accepted pipelineStrictValidationReport
	out := applyFormulaSchema("derived", input, block, &accepted)
	require.Empty(t, accepted.Errors)
	require.Equal(t, []pipelineStrictValidationField{
		{Name: "n", FieldType: "LONG"},
		{Name: "next_n", FieldType: "LONG", Nullable: true},
	}, out.Fields)
	require.Len(t, input.Fields, 1)
}
