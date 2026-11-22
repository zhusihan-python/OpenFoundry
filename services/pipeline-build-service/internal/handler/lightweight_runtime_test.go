package handler

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pipelineexpression "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
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

func TestRowsJoinMatchTypedScalarKeys(t *testing.T) {
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
		{"equal strings still match", pipelineexpression.Row{"k": json.RawMessage(`"O-1"`)}, pipelineexpression.Row{"k": json.RawMessage(`"O-1"`)}, true},
		{"adjacent int64 keys never collapse", pipelineexpression.Row{"k": json.RawMessage(`9007199254740993`)}, pipelineexpression.Row{"k": json.RawMessage(`9007199254740992`)}, false},
		{"equal int64 keys match", pipelineexpression.Row{"k": json.RawMessage(`9007199254740993`)}, pipelineexpression.Row{"k": json.RawMessage(`9007199254740993`)}, true},
		{"integer equals its double spelling", pipelineexpression.Row{"k": json.RawMessage(`1`)}, pipelineexpression.Row{"k": json.RawMessage(`1.0`)}, true},
		{"distinct integers never match", pipelineexpression.Row{"k": json.RawMessage(`1`)}, pipelineexpression.Row{"k": json.RawMessage(`2`)}, false},
		{"string one is a different key than number one", pipelineexpression.Row{"k": json.RawMessage(`"1"`)}, pipelineexpression.Row{"k": json.RawMessage(`1`)}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rt := newLightweightTableRuntime()
			rt.storeTable("orders", lightweightTable{rows: []pipelineexpression.Row{tc.left}})
			rt.storeTable("refunds", lightweightTable{rows: []pipelineexpression.Row{tc.right}})
			node := executor.NodeContext{Node: executor.Node{ID: "link", DependsOn: []string{"orders", "refunds"}}}
			rows, err := rt.runJoin(node, runtimeJoinDraft{JoinType: "inner", Matches: match, LeftColumns: []string{"k"}})
			require.NoError(t, err)
			assert.Equal(t, tc.match, len(rows) == 1)
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

	t.Run("duplicate left projection is rejected", func(t *testing.T) {
		nodes := joinPreviewNodes(left, right, `{"_join":{"join_type":"left","left_columns":["order_id","order_id"],"auto_select_right":true,"right_prefix":"refund_","matches":[`+matches+`]}}`)
		_, err := executeLocalPipelinePreview(t.Context(), uuid.New(), "output", nodes, 20)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "lightweight_join_duplicate_column_name")
		assert.Contains(t, err.Error(), `duplicate column name "order_id"`)
	})

	t.Run("left anti projection duplicates are rejected too", func(t *testing.T) {
		nodes := joinPreviewNodes(left, right, `{"_join":{"join_type":"left_anti","left_columns":["order_id","order_id"],"matches":[`+matches+`]}}`)
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

func TestRejectRuntimeJoinColumnCollisionMatchesStrictRule(t *testing.T) {
	err := rejectRuntimeJoinColumnCollision("join", []string{"order_id", "customer"}, []string{"order_id", "reason"}, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, errJoinDuplicateColumn)

	// Same rule, same message the strict report itself would carry.
	report := pipelineStrictValidationReport{}
	checkDuplicateNames("join", []string{"order_id", "customer", "order_id", "reason"}, "duplicate_column_name", &report)
	require.Len(t, report.Errors, 1)
	assert.Equal(t, "duplicate_column_name", report.Errors[0].Code)
	assert.Contains(t, err.Error(), report.Errors[0].Message)
	assert.Equal(t, `lightweight_join_duplicate_column_name: duplicate column name "order_id"`, err.Error())

	assert.NoError(t, rejectRuntimeJoinColumnCollision("join", []string{"a"}, []string{"b"}, ""))
	assert.NoError(t, rejectRuntimeJoinColumnCollision("join", []string{"a"}, []string{"a"}, "refund_"))
	// The prefixing rule mirrors composeRuntimeJoinRow: a blank prefix is no
	// prefix, so the collision must still fire.
	assert.Error(t, rejectRuntimeJoinColumnCollision("join", []string{"a"}, []string{"a"}, "  "))
}

func TestRunJoinInt64KeysPairExactly(t *testing.T) {
	// Adjacent int64 order numbers around the 2^53 float64 boundary: the
	// refund keyed 9007199254740992 must link only to the same integer and
	// the raw key must survive the join unchanged (published-data parity).
	nodes := joinPreviewNodes(
		`{"rows":[
			{"order_id":9007199254740992,"customer":"ann"},
			{"order_id":9007199254740993,"customer":"bob"}
		]}`,
		`{"rows":[
			{"order_id":9007199254740992,"refund_id":"R1","reason":"damaged"}
		]}`,
		`{"_join":{"join_type":"left","auto_select_right":true,"matches":[{"left_column":"order_id","right_column":"order_id"}],"right_prefix":"refund_"}}`,
	)
	out, err := executeLocalPipelinePreview(t.Context(), uuid.New(), "output", nodes, 50)
	require.NoError(t, err)
	require.Len(t, out.Rows, 2)
	assert.Equal(t, "9007199254740992", string(out.Rows[0]["order_id"]))
	assert.Equal(t, "9007199254740992", string(out.Rows[0]["refund_order_id"]))
	assert.JSONEq(t, `"R1"`, string(out.Rows[0]["refund_refund_id"]))
	// The adjacent order stays unmatched — never paired with R1 — and the
	// exact integer is preserved, not rounded through float64.
	assert.Equal(t, "9007199254740993", string(out.Rows[1]["order_id"]))
	assert.JSONEq(t, `null`, string(out.Rows[1]["refund_order_id"]))
	assert.JSONEq(t, `null`, string(out.Rows[1]["refund_refund_id"]))
	assert.JSONEq(t, `null`, string(out.Rows[1]["refund_reason"]))
}

// declaredJoinNodes builds the collide-shaped graph with a declared output
// schema on the refunds input, so the runtime table carries the declared
// columns even when the rows are empty or sparse. The schemas map mirrors
// what declaredTableSchemas yields for a persisted IR carrying that
// output_schema (legacy nodes have no schema field, so build it directly).
func declaredJoinNodes(refundRows string, declaredOrderID bool, join string) ([]models.PipelineNode, map[string]*models.PipelineIRSchema) {
	fields := []models.PipelineIRField{{Name: "reason", FieldType: "STRING", Nullable: true}}
	if declaredOrderID {
		fields = append([]models.PipelineIRField{{Name: "order_id", FieldType: "STRING", Nullable: true}}, fields...)
	}
	refundSchema := &models.PipelineIRSchema{Fields: fields}
	nodes := []models.PipelineNode{
		{ID: "orders", TransformType: "external", Config: json.RawMessage(`{"rows":[{"order_id":"O-1","customer":"ann"}]}`)},
		{ID: "refunds", TransformType: "external", Config: json.RawMessage(refundRows)},
		{ID: "link", TransformType: "sql", DependsOn: []string{"orders", "refunds"}, Config: json.RawMessage(join)},
		{ID: "output", TransformType: "output_dataset", DependsOn: []string{"link"}},
	}
	return nodes, map[string]*models.PipelineIRSchema{"refunds": refundSchema}
}

func previewJoinWithSchemas(t *testing.T, nodes []models.PipelineNode, schemas map[string]*models.PipelineIRSchema) (pipelineNodePreviewResponse, error) {
	t.Helper()
	return executeLocalPreviewWithSchemas(t.Context(), uuid.New(), "output", nodes, 20, ExecutionPorts{}, schemas)
}

func TestRunJoinAutoColumnsUseTableSchema(t *testing.T) {
	const match = `{"left_column":"order_id","right_column":"order_id"}`

	t.Run("prefixed empty right keeps declared refund columns as NULLs", func(t *testing.T) {
		nodes, schemas := declaredJoinNodes(`{"rows":[]}`, true, `{"_join":{"join_type":"left","auto_select_right":true,"matches":[`+match+`],"right_prefix":"refund_"}}`)
		out, err := previewJoinWithSchemas(t, nodes, schemas)
		require.NoError(t, err)
		require.Len(t, out.Rows, 1)
		assert.JSONEq(t, `"O-1"`, string(out.Rows[0]["order_id"]))
		assert.JSONEq(t, `null`, string(out.Rows[0]["refund_order_id"]))
		assert.JSONEq(t, `null`, string(out.Rows[0]["refund_reason"]))
		assert.Contains(t, out.Columns, "refund_order_id")
		assert.Contains(t, out.Columns, "refund_reason")
	})

	t.Run("unprefixed empty right is still rejected", func(t *testing.T) {
		nodes, schemas := declaredJoinNodes(`{"rows":[]}`, true, `{"_join":{"join_type":"left","auto_select_right":true,"matches":[`+match+`]}}`)
		_, err := previewJoinWithSchemas(t, nodes, schemas)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "lightweight_join_duplicate_column_name")
		assert.Contains(t, err.Error(), `duplicate column name "order_id"`)
	})

	t.Run("unprefixed sparse right is still rejected", func(t *testing.T) {
		nodes, schemas := declaredJoinNodes(`{"rows":[{"reason":"late"}]}`, true, `{"_join":{"join_type":"left","auto_select_right":true,"matches":[`+match+`]}}`)
		_, err := previewJoinWithSchemas(t, nodes, schemas)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "lightweight_join_duplicate_column_name")
		assert.Contains(t, err.Error(), `duplicate column name "order_id"`)
	})

	t.Run("prefixed sparse right fills missing values with NULLs", func(t *testing.T) {
		nodes, schemas := declaredJoinNodes(`{"rows":[{"order_id":"O-1"}]}`, true, `{"_join":{"join_type":"left","auto_select_right":true,"matches":[`+match+`],"right_prefix":"refund_"}}`)
		out, err := previewJoinWithSchemas(t, nodes, schemas)
		require.NoError(t, err)
		require.Len(t, out.Rows, 1)
		assert.JSONEq(t, `"O-1"`, string(out.Rows[0]["order_id"]))
		assert.JSONEq(t, `"O-1"`, string(out.Rows[0]["refund_order_id"]))
		assert.JSONEq(t, `null`, string(out.Rows[0]["refund_reason"]))
	})
}

func TestJoinPreviewExpansionBudgetIsHardCap(t *testing.T) {
	matches := []runtimeJoinMatch{{LeftColumn: "order_id", RightColumn: "order_id"}}
	orders := []pipelineexpression.Row{
		{"order_id": json.RawMessage(`"O-1"`)},
		{"order_id": json.RawMessage(`"O-2"`)},
	}
	joins := []struct {
		name     string
		joinType string
		left     []pipelineexpression.Row
		right    []pipelineexpression.Row
		wantRows int
	}{
		{"matching rows", "inner", orders, orders, 2},
		{"unmatched left rows", "left", orders, nil, 2},
		{"unmatched right rows", "right", nil, orders, 2},
		{"cross product", "cross", orders, orders, 4},
	}
	for _, tc := range joins {
		t.Run(tc.name, func(t *testing.T) {
			rt := newLightweightTableRuntime()
			rt.storeRows("orders", tc.left)
			rt.storeRows("refunds", tc.right)
			node := executor.NodeContext{Node: executor.Node{ID: "link", DependsOn: []string{"orders", "refunds"}}}
			draft := runtimeJoinDraft{JoinType: tc.joinType, Matches: matches, AutoSelectRight: true, RightPrefix: "refund_"}

			rt.joinExpansionLimit = tc.wantRows
			rows, err := rt.runJoin(node, draft)
			require.NoError(t, err)
			require.Len(t, rows, tc.wantRows)

			rt.joinExpansionLimit = tc.wantRows - 1
			_, err = rt.runJoin(node, draft)
			require.ErrorContains(t, err, "lightweight_join_rows_exceeded")
		})
	}
	t.Run("build runners stay uncapped", func(t *testing.T) {
		runner := newRuntimeNodeRunner(ExecutionPorts{})
		assert.Zero(t, runner.Table.joinExpansionLimit)
	})
}
