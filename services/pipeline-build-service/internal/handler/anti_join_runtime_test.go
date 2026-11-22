package handler

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

func TestLeftAntiJoinCompositeKeys(t *testing.T) {
	var left, right []pe.Row
	require.NoError(t, json.Unmarshal([]byte(`[
		{"id":"match","tenant":"a","key":9007199254740992},
		{"id":"precise","tenant":"a","key":9007199254740993},
		{"id":"precise","tenant":"a","key":9007199254740993},
		{"id":"tenant","tenant":"b","key":9007199254740992},
		{"id":"null","tenant":"a","key":null},
		{"id":"missing","tenant":"a"},
		{"id":"empty","tenant":"a","key":""},
		{"id":"as_text","tenant":"a","key":"9007199254740992"}
	]`), &left))
	require.NoError(t, json.Unmarshal([]byte(`[
		{"scope":"a","match":9007199254740992.0},
		{"scope":"a","match":9007199254740992},
		{"scope":"a","match":null},
		{"scope":"a","match":""}
	]`), &right))
	matches := []runtimeJoinMatch{{LeftColumn: "tenant", RightColumn: "scope"}, {LeftColumn: "key", RightColumn: "match"}}
	got, err := joinLeftAnti(left, right, matches, []string{"id"})
	require.NoError(t, err)
	want := []pe.Row{}
	require.NoError(t, json.Unmarshal([]byte(`[
		{"id":"precise"},{"id":"precise"},{"id":"tenant"},{"id":"null"},{"id":"missing"},{"id":"as_text"}
	]`), &want))
	require.Equal(t, want, got)

	got, err = joinLeftAnti(left, nil, matches, []string{"id"})
	require.NoError(t, err)
	require.Len(t, got, len(left))
	got, err = joinLeftAnti(left[:1], right, matches, []string{"id"})
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestLeftAntiJoinRejectsStructuredKeys(t *testing.T) {
	matches := []runtimeJoinMatch{{LeftColumn: "key", RightColumn: "key"}}
	got, err := joinLeftAnti([]pe.Row{{"key": json.RawMessage(`{"a":1}`)}}, nil, matches, []string{"key"})
	require.ErrorContains(t, err, "requires a scalar value")
	require.Nil(t, got)
}

func TestLeftAntiJoinSchemaProjectsOnlyLeft(t *testing.T) {
	left := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{
		{Name: "key", FieldType: "LONG"}, {Name: "value", FieldType: "STRING", Nullable: true},
	}}
	right := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{{Name: "key", FieldType: "LONG"}}}
	draft := runtimeJoinDraft{
		JoinType: runtimeJoinLeftAnti, Matches: []runtimeJoinMatch{{LeftColumn: "key", RightColumn: "key"}},
		AutoSelectRight: true, RightPrefix: "ignored_", LeftColumns: []string{"value"},
	}
	report := pipelineStrictValidationReport{}
	result := validateJoinNode("join", draft, []pipelineStrictSchema{left, right}, &report)
	require.Empty(t, report.Errors)
	require.Equal(t, pipelineStrictSchema{Known: true, Fields: left.Fields[1:]}, result)
	draft.Matches = append(draft.Matches, runtimeJoinMatch{LeftColumn: "key"})
	validateJoinNode("join", draft, []pipelineStrictSchema{left, right}, &report)
	require.Equal(t, "join_incomplete_match_conditions", report.Errors[0].Code)
}
