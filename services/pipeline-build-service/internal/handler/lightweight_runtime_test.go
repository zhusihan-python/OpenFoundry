package handler

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pipelineexpression "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// WP6.5 order-association regressions for the lightweight relational join.

func joinPreviewNodes(leftRows, rightRows, joinConfig string) []models.PipelineNode {
	return []models.PipelineNode{
		{ID: "orders", TransformType: "external", Config: json.RawMessage(leftRows)},
		{ID: "refunds", TransformType: "external", Config: json.RawMessage(rightRows)},
		{ID: "link", TransformType: "sql", DependsOn: []string{"orders", "refunds"}, Config: json.RawMessage(joinConfig)},
		{ID: "output", TransformType: "output_dataset", DependsOn: []string{"link"}},
	}
}

func TestRowsJoinMatchNullishKeysNeverMatch(t *testing.T) {
	match := []runtimeJoinMatch{{LeftColumn: "k", RightColumn: "k"}}
	tests := []struct {
		name  string
		left  pipelineexpression.Row
		right pipelineexpression.Row
		match bool
	}{
		{"null never matches null", pipelineexpression.Row{"k": json.RawMessage(`null`)}, pipelineexpression.Row{"k": json.RawMessage(`null`)}, false},
		{"missing never matches null", pipelineexpression.Row{"other": json.RawMessage(`1`)}, pipelineexpression.Row{"k": json.RawMessage(`null`)}, false},
		{"null never matches missing", pipelineexpression.Row{"k": json.RawMessage(`null`)}, pipelineexpression.Row{}, false},
		{"missing never matches missing", pipelineexpression.Row{}, pipelineexpression.Row{}, false},
		{"null never matches a value", pipelineexpression.Row{"k": json.RawMessage(`null`)}, pipelineexpression.Row{"k": json.RawMessage(`"O-1"`)}, false},
		{"empty string still matches", pipelineexpression.Row{"k": json.RawMessage(`""`)}, pipelineexpression.Row{"k": json.RawMessage(`""`)}, true},
		{"equal values still match", pipelineexpression.Row{"k": json.RawMessage(`"O-1"`)}, pipelineexpression.Row{"k": json.RawMessage(`"O-1"`)}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.match, rowsJoinMatch(tc.left, tc.right, match, false))
		})
	}
}

func TestRunJoinNullAndMissingKeysNeverMatch(t *testing.T) {
	nodes := joinPreviewNodes(
		`{"rows":[
			{"order_id":"O-1","customer":"ann"},
			{"order_id":null,"customer":"bob"},
			{"customer":"cid"},
			{"order_id":"","customer":"dee"}
		]}`,
		`{"rows":[
			{"order_id":"O-1","reason":"damaged"},
			{"order_id":null,"reason":"no reference"},
			{"reason":"keyless"},
			{"order_id":"","reason":"empty key"}
		]}`,
		`{"_join":{"join_type":"left","auto_select_right":true,"matches":[{"left_column":"order_id","right_column":"order_id"}],"right_prefix":"refund_"}}`,
	)
	out, err := executeLocalPipelinePreview(t.Context(), uuid.New(), "output", nodes, 50)
	require.NoError(t, err)
	require.Len(t, out.Rows, 4)

	// A real key keeps matching.
	require.JSONEq(t, `"O-1"`, string(out.Rows[0]["order_id"]))
	require.JSONEq(t, `"damaged"`, string(out.Rows[0]["refund_reason"]))

	// NULL and missing keys never match — the left rows survive unmatched
	// with NULL right columns, never paired with the NULL/keyless refunds.
	require.JSONEq(t, `"bob"`, string(out.Rows[1]["customer"]))
	require.JSONEq(t, `null`, string(out.Rows[1]["refund_order_id"]))
	require.JSONEq(t, `null`, string(out.Rows[1]["refund_reason"]))
	require.JSONEq(t, `"cid"`, string(out.Rows[2]["customer"]))
	require.JSONEq(t, `null`, string(out.Rows[2]["refund_order_id"]))
	require.JSONEq(t, `null`, string(out.Rows[2]["refund_reason"]))

	// A present empty string is a real value and still matches.
	require.JSONEq(t, `"dee"`, string(out.Rows[3]["customer"]))
	require.JSONEq(t, `""`, string(out.Rows[3]["order_id"]))
	require.JSONEq(t, `"empty key"`, string(out.Rows[3]["refund_reason"]))

	for _, row := range out.Rows {
		assert.NotContains(t, []string{`"no reference"`, `"keyless"`}, string(row["refund_reason"]), "NULL/keyless refunds must never be linked")
	}
}

func TestRunJoinRejectsDuplicateOutputColumns(t *testing.T) {
	const left = `{"rows":[{"order_id":"O-1","customer":"ann"}]}`
	const right = `{"rows":[{"order_id":"O-1","reason":"late"}]}`
	const matches = `{"left_column":"order_id","right_column":"order_id"}`

	t.Run("unprefixed right selection collides with the left key", func(t *testing.T) {
		nodes := joinPreviewNodes(left, right, `{"_join":{"join_type":"left","auto_select_right":true,"matches":[`+matches+`]}}`)
		_, err := executeLocalPipelinePreview(t.Context(), uuid.New(), "output", nodes, 20)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "lightweight_join_duplicate_column_name")
		assert.Contains(t, err.Error(), `duplicate column name "order_id"`)
	})

	t.Run("right prefix keeps the left key", func(t *testing.T) {
		nodes := joinPreviewNodes(left, right, `{"_join":{"join_type":"left","auto_select_right":true,"matches":[`+matches+`],"right_prefix":"refund_"}}`)
		out, err := executeLocalPipelinePreview(t.Context(), uuid.New(), "output", nodes, 20)
		require.NoError(t, err)
		require.Len(t, out.Rows, 1)
		assert.JSONEq(t, `"O-1"`, string(out.Rows[0]["order_id"]))
		assert.JSONEq(t, `"O-1"`, string(out.Rows[0]["refund_order_id"]))
		assert.JSONEq(t, `"late"`, string(out.Rows[0]["refund_reason"]))
	})

	t.Run("explicit right projection without prefix keeps the left key", func(t *testing.T) {
		nodes := joinPreviewNodes(left, right, `{"_join":{"join_type":"left","right_columns":["reason"],"matches":[`+matches+`]}}`)
		out, err := executeLocalPipelinePreview(t.Context(), uuid.New(), "output", nodes, 20)
		require.NoError(t, err)
		require.Len(t, out.Rows, 1)
		// Only "reason" is projected from the right; the left columns are
		// auto-derived, so nothing collides and nothing is overwritten.
		assert.Len(t, out.Rows[0], 3)
		assert.JSONEq(t, `"O-1"`, string(out.Rows[0]["order_id"]))
		assert.JSONEq(t, `"ann"`, string(out.Rows[0]["customer"]))
		assert.JSONEq(t, `"late"`, string(out.Rows[0]["reason"]))
	})

	t.Run("cross join collision is rejected too", func(t *testing.T) {
		nodes := joinPreviewNodes(left, right, `{"_join":{"join_type":"cross","auto_select_right":true}}`)
		_, err := executeLocalPipelinePreview(t.Context(), uuid.New(), "output", nodes, 20)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `duplicate column name "order_id"`)
	})

	t.Run("cross join with prefix keeps the cartesian product", func(t *testing.T) {
		nodes := joinPreviewNodes(
			`{"rows":[{"order_id":"O-1"},{"order_id":"O-2"}]}`,
			`{"rows":[{"reason":"late"}]}`,
			`{"_join":{"join_type":"cross","auto_select_right":true,"right_prefix":"refund_"}}`,
		)
		out, err := executeLocalPipelinePreview(t.Context(), uuid.New(), "output", nodes, 20)
		require.NoError(t, err)
		require.Len(t, out.Rows, 2)
		assert.JSONEq(t, `"O-1"`, string(out.Rows[0]["order_id"]))
		assert.JSONEq(t, `"O-2"`, string(out.Rows[1]["order_id"]))
		assert.JSONEq(t, `"late"`, string(out.Rows[1]["refund_reason"]))
	})
}
