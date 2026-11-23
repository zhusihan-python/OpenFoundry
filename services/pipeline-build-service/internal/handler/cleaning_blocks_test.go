package handler

import (
	"context"
	"encoding/json"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pipelineexpression "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// WP6.5 batch-2 regressions: drop columns, whole-column cast and column-name
// normalization in the lightweight transform stack. Every case mirrors a
// behavior verified against a real PBS + DVS + PostgreSQL stack; see
// tools/smoke evidence for the live run.

func rowsFromMaps(t testing.TB, maps ...map[string]any) []pipelineexpression.Row {
	t.Helper()
	out := make([]pipelineexpression.Row, 0, len(maps))
	for _, m := range maps {
		raw, err := json.Marshal(m)
		require.NoError(t, err)
		row := pipelineexpression.Row{}
		require.NoError(t, json.Unmarshal(raw, &row))
		out = append(out, row)
	}
	return out
}

func rowsToMapsForTest(t testing.TB, rows []pipelineexpression.Row) []map[string]any {
	t.Helper()
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		raw, err := json.Marshal(row)
		require.NoError(t, err)
		m := map[string]any{}
		require.NoError(t, json.Unmarshal(raw, &m))
		out = append(out, m)
	}
	return out
}

func TestNormalizeRuntimeColumnNameMatchesEditorPreview(t *testing.T) {
	tests := []struct {
		name          string
		in            string
		removeSpecial bool
		want          string
	}{
		{"acronym camel case", "OrderID", false, "order_id"},
		{"lower camel case", "CustomerName", false, "customer_name"},
		{"digit before upper", "userID2X", false, "user_id2_x"},
		{"leading acronym is squashed", "XMLHttpRequest", false, "xmlhttp_request"},
		{"spaces collapse", "Order ID", false, "order_id"},
		{"hyphen collapses", "order-id", false, "order_id"},
		{"symbols survive by default", "paid amount (usd)", false, "paid_amount_(usd)"},
		{"symbols drop in strict mode", "paid amount (usd)", true, "paid_amount_usd"},
		{"unicode letters survive strict mode", "订单号", true, "订单号"},
		{"surrounding separators trim", "  order id  ", false, "order_id"},
		{"already normalized", "order_id", false, "order_id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, normalizeRuntimeColumnName(tc.in, tc.removeSpecial))
		})
	}
}

func TestApplyRuntimeDropBlockRemovesConfiguredColumns(t *testing.T) {
	rows := rowsFromMaps(t,
		map[string]any{"Order ID": "O-1", "internal_note": "vc", "Paid Amount": "10.5"},
		map[string]any{"Order ID": "O-2", "internal_note": "vc", "Paid Amount": nil},
	)
	out, err := applyRuntimeDropBlock(rows, []string{"internal_note"}, nil)
	require.NoError(t, err)
	assert.Equal(t, []map[string]any{
		{"Order ID": "O-1", "Paid Amount": "10.5"},
		{"Order ID": "O-2", "Paid Amount": nil},
	}, rowsToMapsForTest(t, out))
}

func TestApplyRuntimeDropBlockRejectsUnknownColumn(t *testing.T) {
	rows := rowsFromMaps(t, map[string]any{"Order ID": "O-1", "internal_note": "vc"})
	_, err := applyRuntimeDropBlock(rows, []string{"internal_note", "ghost_col"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `column "ghost_col" does not exist in upstream schema`)
	assert.NotContains(t, err.Error(), "internal_note")
}

func TestApplyRuntimeDropBlockAllowsDeclaredSparseColumn(t *testing.T) {
	// A column declared upstream but missing from every row is still a valid
	// drop target — the strict schema validation addresses it by name.
	rows := rowsFromMaps(t, map[string]any{"Order ID": "O-1"})
	out, err := applyRuntimeDropBlock(rows, []string{"declared_note"}, []string{"Order ID", "declared_note"})
	require.NoError(t, err)
	assert.Equal(t, []map[string]any{{"Order ID": "O-1"}}, rowsToMapsForTest(t, out))
}

func TestApplyRuntimeCastBlockConversions(t *testing.T) {
	tests := []struct {
		name       string
		source     string
		rows       []map[string]any
		targetType string
		target     string
		want       []map[string]any
	}{
		{"string amount to double", "a", []map[string]any{{"a": "120.50"}}, "Double", "", []map[string]any{{"a": 120.5}}},
		{"string amount to integer truncates", "a", []map[string]any{{"a": "80.9"}}, "Integer", "", []map[string]any{{"a": float64(80)}}},
		{"negative integer truncates toward zero", "a", []map[string]any{{"a": -3.9}}, "Integer", "", []map[string]any{{"a": float64(-3)}}},
		{"number to string", "a", []map[string]any{{"a": 120.5}}, "String", "", []map[string]any{{"a": "120.5"}}},
		{"boolean words", "a", []map[string]any{{"a": "true"}, {"a": "false"}}, "Boolean", "", []map[string]any{{"a": true}, {"a": false}}},
		{"boolean literals", "a", []map[string]any{{"a": true}, {"a": 0}}, "Boolean", "", []map[string]any{{"a": true}, {"a": false}}},
		{"target column renames the source away", "price", []map[string]any{{"price": "10.5"}}, "Double", "price_usd", []map[string]any{{"price_usd": 10.5}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			block := runtimeTransformBlock{SourceColumn: tc.source, TargetType: tc.targetType, TargetColumn: tc.target}
			out, err := applyRuntimeCastBlock(rowsFromMaps(t, tc.rows...), block, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.want, rowsToMapsForTest(t, out))
		})
	}
}

func TestApplyRuntimeCastBlockNullStaysNull(t *testing.T) {
	for _, targetType := range []string{"Double", "Integer", "String", "Boolean", "Timestamp"} {
		t.Run(targetType, func(t *testing.T) {
			rows := rowsFromMaps(t, map[string]any{"Paid Amount": nil})
			block := runtimeTransformBlock{SourceColumn: "Paid Amount", TargetType: targetType}
			out, err := applyRuntimeCastBlock(rows, block, nil)
			require.NoError(t, err)
			assert.Equal(t, []map[string]any{{"Paid Amount": nil}}, rowsToMapsForTest(t, out))
		})
	}
}

func TestApplyRuntimeCastBlockRejectsInvalidValues(t *testing.T) {
	rows := rowsFromMaps(t,
		map[string]any{"Paid Amount": "120.50"},
		map[string]any{"Paid Amount": "N/A"},
	)
	block := runtimeTransformBlock{SourceColumn: "Paid Amount", TargetType: "Double"}
	_, err := applyRuntimeCastBlock(rows, block, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `cast column "Paid Amount" at row 2`)
	assert.Contains(t, err.Error(), "requires a numeric value")
}

func TestApplyRuntimeCastBlockRejectsOutOfRangeInteger(t *testing.T) {
	rows := rowsFromMaps(t, map[string]any{"n": "1e30"})
	block := runtimeTransformBlock{SourceColumn: "n", TargetType: "Integer"}
	_, err := applyRuntimeCastBlock(rows, block, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "32-bit integer range")
}

func TestApplyRuntimeCastBlockRejectsInvalidConfiguration(t *testing.T) {
	rows := rowsFromMaps(t, map[string]any{"a": "1", "b": "2"})

	_, err := applyRuntimeCastBlock(rows, runtimeTransformBlock{SourceColumn: "", TargetType: "Double"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cast block requires source_column")

	_, err = applyRuntimeCastBlock(rows, runtimeTransformBlock{SourceColumn: "a", TargetType: "Wibble"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `cast target "Wibble" is not a supported type`)

	_, err = applyRuntimeCastBlock(rows, runtimeTransformBlock{SourceColumn: "ghost", TargetType: "Double"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `column "ghost" does not exist in upstream schema`)

	_, err = applyRuntimeCastBlock(rows, runtimeTransformBlock{SourceColumn: "a", TargetType: "Double", TargetColumn: "b"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `duplicate column name "b"`)
}

func TestApplyRuntimeNormalizeBlockRenamesPredictably(t *testing.T) {
	rows := rowsFromMaps(t, map[string]any{"Order ID": "O-1", "CustomerName": "ann", "paid amount (usd)": "9.5"})
	out, err := applyRuntimeNormalizeBlock(rows, false, nil)
	require.NoError(t, err)
	assert.Equal(t, []map[string]any{{"order_id": "O-1", "customer_name": "ann", "paid_amount_(usd)": "9.5"}},
		rowsToMapsForTest(t, out))

	out, err = applyRuntimeNormalizeBlock(rows, true, nil)
	require.NoError(t, err)
	assert.Equal(t, []map[string]any{{"order_id": "O-1", "customer_name": "ann", "paid_amount_usd": "9.5"}},
		rowsToMapsForTest(t, out))
}

func TestApplyRuntimeNormalizeBlockRejectsCollidingNames(t *testing.T) {
	rows := rowsFromMaps(t,
		map[string]any{"Order ID": "O-1", "order-id": "O-1"},
		map[string]any{"Order ID": "O-2", "order-id": "O-2"},
	)
	_, err := applyRuntimeNormalizeBlock(rows, false, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `duplicate column name "order_id" (from "Order ID", "order-id")`)
}

func TestApplyRuntimeNormalizeBlockRejectsEmptyNormalizedName(t *testing.T) {
	rows := rowsFromMaps(t, map[string]any{"###": "x"})
	_, err := applyRuntimeNormalizeBlock(rows, true, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `column "###" normalizes to an empty name`)
}

func TestApplyNormalizeSchemaAgreesWithRuntime(t *testing.T) {
	schema := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{
		{Name: "Order ID", FieldType: "STRING"},
		{Name: "CustomerName", FieldType: "STRING"},
	}}

	report := pipelineStrictValidationReport{}
	out := applyNormalizeSchema("clean", schema, false, &report)
	require.Empty(t, report.Errors)
	assert.Equal(t, []string{"order_id", "customer_name"}, out.columnNames())

	clash := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{
		{Name: "Order ID", FieldType: "STRING"},
		{Name: "order-id", FieldType: "STRING"},
	}}
	report = pipelineStrictValidationReport{}
	applyNormalizeSchema("clean", clash, false, &report)
	require.False(t, report.AllValid)
	require.Len(t, report.Errors, 1)
	assert.Equal(t, "duplicate_column_name", report.Errors[0].Code)
	assert.Equal(t, "order_id", *report.Errors[0].Column)

	empty := pipelineStrictSchema{Known: true, Fields: []pipelineStrictValidationField{
		{Name: "###", FieldType: "STRING"},
	}}
	report = pipelineStrictValidationReport{}
	applyNormalizeSchema("clean", empty, true, &report)
	require.False(t, report.AllValid)
	require.Len(t, report.Errors, 1)
	assert.Equal(t, "normalize_column_name_empty", report.Errors[0].Code)
}

func TestPreviewChainWithPinnedSnapshotReadsBuildInputs(t *testing.T) {
	pinned := json.RawMessage(`{"dataset_id":"ri.foundry.main.dataset.00000000-0000-0000-0000-000000000000","dataset_snapshot":{"branch":"main","version":1,"transaction_id":"ri.foundry.main.transaction.00000000-0000-0000-0000-000000000000"}}`)
	cleaningStack := `{"sql":"SELECT * FROM __source__","_stack":{"blocks":[{"kind":"normalize","applied":true,"remove_special_characters":false}]}}`

	chain := []models.PipelineNode{
		{ID: "orders", TransformType: "dataset_input", Config: pinned},
		{ID: "clean", TransformType: "sql", DependsOn: []string{"orders"}, Config: json.RawMessage(cleaningStack)},
	}
	assert.True(t, previewChainRequiresBuildInputs(chain))

	unpinned := []models.PipelineNode{
		{ID: "orders", TransformType: "dataset_input", Config: json.RawMessage(`{"dataset_id":"ri.foundry.main.dataset.00000000-0000-0000-0000-000000000000"}`)},
		{ID: "clean", TransformType: "sql", DependsOn: []string{"orders"}, Config: json.RawMessage(cleaningStack)},
	}
	assert.False(t, previewChainRequiresBuildInputs(unpinned))
}

// Geo joins pair rows across the complete relations — a nearest neighbor can
// sit far beyond the right side's first sample row — so their preview chains
// must read the bounded build inputs like a real run, pin or no pin.
func TestPreviewChainGeoJoinReadsBuildInputs(t *testing.T) {
	geoJoin := json.RawMessage(`{"_geo_join":{"mode":"nearest","left_lat_column":"lat","left_lon_column":"lon","right_lat_column":"lat","right_lon_column":"lon","k":1}}`)
	for _, pinned := range []bool{true, false} {
		config := json.RawMessage(`{"dataset_id":"ri.foundry.main.dataset.00000000-0000-0000-0000-000000000000"}`)
		if pinned {
			config = json.RawMessage(`{"dataset_id":"ri.foundry.main.dataset.00000000-0000-0000-0000-000000000000","dataset_snapshot":{"branch":"main","version":1,"transaction_id":"ri.foundry.main.transaction.00000000-0000-0000-0000-000000000000"}}`)
		}
		chain := []models.PipelineNode{
			{ID: "left", TransformType: "dataset_input", Config: config},
			{ID: "right", TransformType: "dataset_input", Config: config},
			{ID: "join", TransformType: "geo_nearest_neighbor_join", DependsOn: []string{"left", "right"}, Config: geoJoin},
		}
		assert.True(t, previewChainRequiresBuildInputs(chain), "pinned=%v", pinned)
	}
}

// Haversine and GPX chains map each input row to exactly one output row, so a
// pinned chain's display sample must come from the final rows, never from the
// snapshot read's bound (pinned reads never truncate).
func TestPreviewChainPinnedHaversineAndGPXReadBuildInputs(t *testing.T) {
	pinned := json.RawMessage(`{"dataset_id":"ri.foundry.main.dataset.00000000-0000-0000-0000-000000000000","dataset_snapshot":{"branch":"main","version":1,"transaction_id":"ri.foundry.main.transaction.00000000-0000-0000-0000-000000000000"}}`)
	unpinned := json.RawMessage(`{"dataset_id":"ri.foundry.main.dataset.00000000-0000-0000-0000-000000000000"}`)
	haversineStack := `{"sql":"SELECT * FROM __source__","_stack":{"blocks":[{"kind":"haversine_distance","applied":true,"start_lat_column":"lat1","start_lon_column":"lon1","end_lat_column":"lat2","end_lon_column":"lon2","unit":"meters","target_column":""}]}}`

	pinnedHaversine := []models.PipelineNode{
		{ID: "pts", TransformType: "dataset_input", Config: pinned},
		{ID: "dist", TransformType: "sql", DependsOn: []string{"pts"}, Config: json.RawMessage(haversineStack)},
	}
	assert.True(t, previewChainRequiresBuildInputs(pinnedHaversine))

	pinnedGPX := []models.PipelineNode{
		{ID: "docs", TransformType: "dataset_input", Config: pinned},
		{ID: "parse", TransformType: "gpx_parse", DependsOn: []string{"docs"}, Config: json.RawMessage(`{"gpx_column":"gpx"}`)},
	}
	assert.True(t, previewChainRequiresBuildInputs(pinnedGPX))

	unpinnedHaversine := []models.PipelineNode{
		{ID: "pts", TransformType: "dataset_input", Config: unpinned},
		{ID: "dist", TransformType: "sql", DependsOn: []string{"pts"}, Config: json.RawMessage(haversineStack)},
	}
	assert.False(t, previewChainRequiresBuildInputs(unpinnedHaversine))

	pinnedPassthrough := []models.PipelineNode{
		{ID: "pts", TransformType: "dataset_input", Config: pinned},
		{ID: "pass", TransformType: "sql", DependsOn: []string{"pts"}, Config: json.RawMessage(`{"sql":"SELECT * FROM __source__"}`)},
	}
	assert.False(t, previewChainRequiresBuildInputs(pinnedPassthrough))
}

func TestCleaningStackReusesDroppedName(t *testing.T) {
	rt := newLightweightTableRuntime(nil)
	rt.storeTable("in", lightweightTable{rows: rowsFromMaps(t, map[string]any{"a": "3", "b": 2})})
	node := executor.NodeContext{Node: executor.Node{ID: "clean", DependsOn: []string{"in"}}}
	drop := runtimeTransformBlock{Kind: "drop", Applied: true, Columns: []string{"b"}}
	cast := runtimeTransformBlock{Kind: "cast", Applied: true, SourceColumn: "a", TargetColumn: "b", TargetType: "Integer"}
	out, err := rt.runTransformStack(context.Background(), node, runtimeTransformStack{Blocks: []runtimeTransformBlock{drop, cast}})
	require.NoError(t, err)
	assert.Equal(t, []map[string]any{{"b": float64(3)}}, rowsToMapsForTest(t, out))
	_, err = rt.runTransformStack(context.Background(), node, runtimeTransformStack{Blocks: []runtimeTransformBlock{drop, drop}})
	require.ErrorContains(t, err, `column "b" does not exist`)
}

func TestNormalizeRejectsDeclaredCollisionInEmptyTable(t *testing.T) {
	_, err := applyRuntimeNormalizeBlock(nil, false, []string{"OrderID", "Order ID"})
	require.ErrorContains(t, err, `duplicate column name "order_id"`)
}

func TestNamedUnionUsesCompleteInputs(t *testing.T) {
	config := json.RawMessage(`{"_union":{"union_type":"by_name"}}`)
	require.True(t, previewChainRequiresBuildInputs([]models.PipelineNode{{ID: "merge", TransformType: "sql", Config: config}}))
}
