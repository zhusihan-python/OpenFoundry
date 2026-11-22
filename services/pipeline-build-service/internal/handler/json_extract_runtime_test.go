package handler

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

func TestJSONExtractText(t *testing.T) {
	for _, test := range []struct{ name, document, path, expected string }{
		{"exact number", `{"items":[{"id":9007199254740993}]}`, "$.items[0].id", `"9007199254740993"`},
		{"encoded object", `"{\"key\":\"含空格 \"}"`, "$.key", `"含空格 "`},
		{"quoted member", `{"a.b":true}`, `$["a.b"]`, `"true"`},
		{"escaped member", `{"a\"b":false}`, `$["a\"b"]`, `"false"`},
		{"compact root", `{ "a" : [1, null] }`, "$", `"{\"a\":[1,null]}"`},
		{"nested array result", `{"a":[1, 2]}`, "$.a", `"[1,2]"`},
		{"absent", `{}`, "$.name", "null"},
		{"null", "null", "$.name", "null"},
		{"missing column", "", "$", "null"},
		{"index out of array", `{"items":[]}`, "$.items[0]", "null"},
		{"incompatible intermediate", `{"items":9}`, "$.items[0]", "null"},
	} {
		t.Run(test.name, func(t *testing.T) {
			steps, err := jsonExtractPath(test.path)
			require.NoError(t, err)
			result, err := extractJSONText(json.RawMessage(test.document), steps)
			require.NoError(t, err)
			require.JSONEq(t, test.expected, string(result))
		})
	}
}

func TestJSONExtractRejectsInvalidConfigAndData(t *testing.T) {
	for _, path := range []string{"", "items[0]", "$.", "$..name", "$[*]", "$[-1]", "$[999999999999999999999999]", `$["\q"]`} {
		_, err := jsonExtractPath(path)
		require.Error(t, err, path)
	}
	block := runtimeTransformBlock{SourceColumn: "payload", TargetColumn: "name", JSONPath: "$.name"}
	good := pe.Row{"payload": json.RawMessage(`{"name":"ok"}`)}
	rows, err := applyRuntimeJSONExtractBlock([]pe.Row{good}, block)
	require.NoError(t, err)
	require.JSONEq(t, `"ok"`, string(rows[0]["name"]))
	require.NotContains(t, good, "name")
	rows, err = applyRuntimeJSONExtractBlock([]pe.Row{good, {"payload": json.RawMessage(`"{bad}"`)}}, block)
	require.ErrorContains(t, err, "at row 2: malformed JSON document")
	require.Nil(t, rows)
	_, err = applyRuntimeJSONExtractBlock([]pe.Row{{"name": json.RawMessage("null")}}, block)
	require.ErrorContains(t, err, "already exists")
}

func TestJSONExtractSchemaIsNullableText(t *testing.T) {
	block := runtimeTransformBlock{SourceColumn: "payload", TargetColumn: "name", JSONPath: "$.name"}
	input := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{{Name: "payload", FieldType: "STRING"}}}
	report := pipelineStrictValidationReport{}
	result := applyJSONExtractSchema("extract", input, block, &report)
	require.Empty(t, report.Errors)
	require.Equal(t, []pipelineStrictValidationField{input.Fields[0], {Name: "name", FieldType: "STRING", Nullable: true}}, result.Fields)
	require.Equal(t, result, applyJSONExtractSchema("extract", result, block, &report))
	require.Equal(t, "json_extract_target_exists", report.Errors[0].Code)
}
