package handler

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

func TestExplodePreservesArrayValuesAndOrder(t *testing.T) {
	var rows []pe.Row
	require.NoError(t, json.Unmarshal([]byte(`[
		{"id":"a","items":[9007199254740993,null,{"k":[1]}]},
		{"id":"b","items":" [true, false] "},
		{"id":"c","items":[]},{"id":"d","items":null},{"id":"e"}
	]`), &rows))
	config := runtimeTransformBlock{SourceColumn: "items", TargetColumn: "element"}
	out, err := applyRuntimeExplodeBlock(rows, config)
	require.NoError(t, err)
	require.Len(t, out, 5)
	require.Equal(t, []json.RawMessage{
		json.RawMessage("9007199254740993"), json.RawMessage("null"), json.RawMessage(`{"k":[1]}`), json.RawMessage("true"), json.RawMessage("false"),
	}, []json.RawMessage{out[0]["element"], out[1]["element"], out[2]["element"], out[3]["element"], out[4]["element"]})
	require.Equal(t, rows[0]["items"], out[0]["items"])
	require.Equal(t, rows[1]["items"], out[4]["items"])
	require.Equal(t, rows[0]["id"], out[2]["id"])
	require.Equal(t, rows[1]["id"], out[3]["id"])
	require.NotContains(t, rows[0], "element")

	config.TargetColumn = ""
	out, err = applyRuntimeExplodeBlock(rows[:1], config)
	require.NoError(t, err)
	require.Equal(t, pe.Row{"id": json.RawMessage(`"a"`), "items": json.RawMessage("null")}, out[1])

	out, err = applyRuntimeExplodeBlock(rows[2:], config)
	require.NoError(t, err)
	require.Empty(t, out)
}

func TestExplodeRejectsInvalidAndExcessiveInput(t *testing.T) {
	block := runtimeTransformBlock{SourceColumn: "items"}
	for _, value := range []string{`"hello"`, `{} `, `"[1] [2]"`, `"[1,"`, `"null"`, "2"} {
		out, err := applyRuntimeExplodeBlock([]pe.Row{{"items": json.RawMessage(value)}}, block)
		require.ErrorContains(t, err, "expected a JSON array")
		require.Nil(t, out)
	}
	tooMany := "[" + strings.Repeat("0,", maxExplodedRows) + "0]"
	_, err := applyRuntimeExplodeBlock([]pe.Row{{"items": json.RawMessage(tooMany)}}, block)
	require.ErrorContains(t, err, "100000-row lightweight limit")

	manyCopies := "[" + strings.Repeat("0,", 99) + "0]"
	largeField := `"` + strings.Repeat("x", 1<<20) + `"`
	_, err = applyRuntimeExplodeBlock([]pe.Row{{"items": json.RawMessage(manyCopies), "note": json.RawMessage(largeField)}}, block)
	require.ErrorContains(t, err, "lightweight materialization budget")
}

func TestExplodeSchemaUsesDeclaredArrayElementType(t *testing.T) {
	array := pipelineStrictValidationField{Name: "items", FieldType: "ARRAY<ARRAY<LONG>>"}
	input := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{array, {Name: "value", FieldType: "STRING"}}}
	block := runtimeTransformBlock{SourceColumn: "items", TargetColumn: "element"}
	report := pipelineStrictValidationReport{}
	schema := applyExplodeSchema("explode", input, block, &report)
	require.Empty(t, report.Errors)
	require.Equal(t, []pipelineStrictValidationField{input.Fields[0], input.Fields[1], {Name: "element", FieldType: "ARRAY<LONG>", Nullable: true}}, schema.Fields)
	block.TargetColumn = ""
	schema = applyExplodeSchema("explode", input, block, &report)
	require.Equal(t, pipelineStrictValidationField{Name: "items", FieldType: "ARRAY<LONG>", Nullable: true}, schema.Fields[0])
	block.SourceColumn = "value"
	require.False(t, applyExplodeSchema("explode", input, block, &report).Known)
	require.Empty(t, report.Errors)
}
