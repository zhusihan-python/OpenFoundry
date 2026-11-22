package handler

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

func csvTestRow(text string) pe.Row {
	return pe.Row{"document": pe.EvalString(text).ToJSON(), "original": json.RawMessage("9")}
}

func TestCSVParseHeadersQuotesAndReorderedColumns(t *testing.T) {
	base := []pe.Row{
		csvTestRow("\uFEFFcode,name,note\r\n001,\"a,b\",\"line 1\r\nline 2\"\r\n"),
		csvTestRow("note,code,name\n,002,\"with \"\"quotes\"\"\"\n"),
		csvTestRow("code,name,note\n"),
		csvTestRow(""),
		{"document": json.RawMessage("null")},
		{},
	}
	out, err := applyRuntimeCSVParseBlock(base, runtimeTransformBlock{SourceColumn: "document"})
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, json.RawMessage(`"001"`), out[0]["code"])
	require.Equal(t, json.RawMessage(`"a,b"`), out[0]["name"])
	require.Equal(t, json.RawMessage(`"line 1\nline 2"`), out[0]["note"])
	require.Equal(t, json.RawMessage(`"002"`), out[1]["code"])
	require.Equal(t, json.RawMessage(`"with \"quotes\""`), out[1]["name"])
	require.Equal(t, json.RawMessage(`""`), out[1]["note"])
	require.Equal(t, base[0]["document"], out[0]["document"])
	require.Equal(t, json.RawMessage("9"), out[1]["original"])
	require.NotContains(t, base[0], "name")
}

func TestCSVParseHeaderlessFieldsAreText(t *testing.T) {
	hasHeader := false
	out, err := applyRuntimeCSVParseBlock([]pe.Row{csvTestRow("001;true\n002;")}, runtimeTransformBlock{
		SourceColumn: "document", CSVDelimiter: ";", CSVHasHeader: &hasHeader,
	})
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, json.RawMessage(`"001"`), out[0]["column_1"])
	require.Equal(t, json.RawMessage(`"true"`), out[0]["column_2"])
	require.Equal(t, json.RawMessage(`""`), out[1]["column_2"])
}

func TestCSVParseRejectsInvalidDocuments(t *testing.T) {
	config := runtimeTransformBlock{SourceColumn: "document"}
	for _, test := range []struct {
		rows    []pe.Row
		message string
	}{
		{[]pe.Row{csvTestRow("id,id\n1,2")}, "nonempty and unique"},
		{[]pe.Row{csvTestRow("id, \n1,2")}, "nonempty and unique"},
		{[]pe.Row{csvTestRow("id,original\n1,2")}, "already exists"},
		{[]pe.Row{csvTestRow("id,label\n1")}, "wrong number of fields"},
		{[]pe.Row{csvTestRow("id\n\"not closed")}, "quoted-field"},
		{[]pe.Row{csvTestRow("id\n1"), csvTestRow("other\n2")}, "different output columns"},
		{[]pe.Row{{"document": json.RawMessage("4")}}, "requires text"},
	} {
		out, err := applyRuntimeCSVParseBlock(test.rows, config)
		require.ErrorContains(t, err, test.message)
		require.Nil(t, out)
	}
	for _, delimiter := range []string{"::", "\"", "\n", "\x00"} {
		config.CSVDelimiter = delimiter
		_, err := applyRuntimeCSVParseBlock(nil, config)
		require.ErrorContains(t, err, "CSV delimiter must be one character")
	}
}

func TestCSVParseSchemaDefersOutputNames(t *testing.T) {
	input := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{
		{Name: "document", FieldType: "STRING"},
		{Name: "original", FieldType: "LONG"},
	}}
	report := pipelineStrictValidationReport{}
	config := runtimeTransformBlock{SourceColumn: "document"}
	require.False(t, applyCSVParseSchema("parse", input, config, &report).Known)
	require.Empty(t, report.Errors)
	config.SourceColumn = "original"
	require.Equal(t, input, applyCSVParseSchema("parse", input, config, &report))
	require.Equal(t, "invalid_csv_parse_column", report.Errors[0].Code)
	config.CSVDelimiter = ";;"
	applyCSVParseSchema("parse", input, config, &report)
	require.Equal(t, "invalid_csv_parse", report.Errors[1].Code)
}

func TestCSVParseExpansionBudget(t *testing.T) {
	row := csvTestRow("value\n" + strings.Repeat("x\n", 70))
	row["retained"] = pe.EvalString(strings.Repeat("x", 1<<20)).ToJSON()
	out, err := applyRuntimeCSVParseBlock([]pe.Row{row}, runtimeTransformBlock{SourceColumn: "document"})
	require.ErrorContains(t, err, "lightweight materialization budget")
	require.Nil(t, out)
}
