package handler

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

func TestRuntimeSortBlockStableNullsLastAndExactIntegers(t *testing.T) {
	var rows []pe.Row
	require.NoError(t, json.Unmarshal([]byte(`[
		{"id":"small","group":"a","value":9007199254740992},
		{"id":"null","group":"a","value":null},
		{"id":"large","group":"a","value":9007199254740993},
		{"id":"tie","group":"a","value":9007199254740993},
		{"id":"b","group":"b","value":-1},
		{"id":"missing","group":"a"},
		{"id":"fraction","group":"a","value":1.5}
	]`), &rows))
	keys := []runtimeSortKey{{Column: "group", Direction: "asc"}, {Column: "value", Direction: "desc"}}
	result, err := applyRuntimeSortBlock(rows, keys)
	require.NoError(t, err)
	require.Equal(t, []pe.Row{rows[2], rows[3], rows[0], rows[6], rows[1], rows[5], rows[4]}, result)
	require.Equal(t, json.RawMessage(`"small"`), rows[0]["id"])

	keys[1].Direction = "asc"
	result, err = applyRuntimeSortBlock(rows, keys)
	require.NoError(t, err)
	require.Equal(t, []pe.Row{rows[6], rows[0], rows[2], rows[3], rows[1], rows[5], rows[4]}, result)
}

func TestRuntimeSortBlockRejectsInvalidSort(t *testing.T) {
	var rows []pe.Row
	require.NoError(t, json.Unmarshal([]byte(`[{"mixed":1,"object":{"a":1}},{"mixed":"2"}]`), &rows))
	for _, test := range []struct {
		keys    []runtimeSortKey
		message string
	}{
		{nil, "requires at least one key"},
		{[]runtimeSortKey{{Column: "mixed", Direction: "sideways"}}, "requires asc or desc"},
		{[]runtimeSortKey{{Column: "mixed"}, {Column: "mixed"}}, "empty or repeated"},
		{[]runtimeSortKey{{Column: "mixed"}}, "mixed scalar types"},
		{[]runtimeSortKey{{Column: "object"}}, "unsupported value"},
	} {
		_, err := applyRuntimeSortBlock(rows, test.keys)
		require.ErrorContains(t, err, test.message)
	}
}

func TestRuntimeSortBlockBooleanAndEmpty(t *testing.T) {
	var rows []pe.Row
	require.NoError(t, json.Unmarshal([]byte(`[{"enabled":true},{"enabled":false},{}]`), &rows))
	keys := []runtimeSortKey{{Column: "enabled"}}
	result, err := applyRuntimeSortBlock(rows, keys)
	require.NoError(t, err)
	require.Equal(t, []pe.Row{rows[1], rows[0], rows[2]}, result)
	result, err = applyRuntimeSortBlock(nil, keys)
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestRuntimeSortBlockSchema(t *testing.T) {
	schema := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{{Name: "value", FieldType: "LONG"}}}
	report := pipelineStrictValidationReport{}
	stack := runtimeTransformStack{Blocks: []runtimeTransformBlock{
		{Kind: "sort", Applied: true, SortKeys: []runtimeSortKey{{Column: "value"}}},
		{Kind: "sort", Applied: true, SortKeys: []runtimeSortKey{{Column: "absent"}}},
		{Kind: "sort", Applied: true},
	}}
	require.Equal(t, schema, validateTransformStack("sort_node", stack, schema, &report))
	require.Len(t, report.Errors, 2)
	require.Equal(t, "missing_column", report.Errors[0].Code)
	require.Equal(t, "invalid_sort_keys", report.Errors[1].Code)
}
