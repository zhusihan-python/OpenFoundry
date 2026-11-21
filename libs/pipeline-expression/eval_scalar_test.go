package pipelineexpression_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

func TestScalarRoundingBoundaries(t *testing.T) {
	row := pe.Row{
		"min_value": json.RawMessage("-9223372036854775808"),
		"max_value": json.RawMessage("9223372036854775807"),
		"precise":   json.RawMessage("9007199254740993"),
	}
	for expression, expected := range map[string]string{
		"ceil(2.01)": "3", "ceil(-2.01)": "-2",
		"floor(2.99)": "2", "floor(-2.01)": "-3",
		"round(2.5)": "3", "round(-2.5)": "-3", "round(2.49)": "2",
		"ceil(max_value)":  "9223372036854775807",
		"floor(min_value)": "-9223372036854775808",
		"round(precise)":   "9007199254740993",
		"ceil(null)":       "null", "floor(null)": "null", "round(null)": "null",
	} {
		assertScalarJSON(t, row, expression, expected)
	}
	for _, expression := range []string{"round('2.5')", "ceil(cast('NaN','DOUBLE'))", "floor(cast('Inf','DOUBLE'))"} {
		assertScalarError(t, row, expression, pe.EvalErrTypeMismatch)
	}
	assertScalarError(t, row, "ceil(1,2)", pe.EvalErrArity)

	env := pe.NewColumnEnv().With("precise", pe.LongType())
	assertOk(t, "round(precise)", env, pe.LongType())
	assertOk(t, "ceil(null)", env, pe.DoubleType())
}

func TestScalarCallsRejectUnsupportedColumnValues(t *testing.T) {
	row := pe.Row{
		"array_value":  json.RawMessage(`[1,2]`),
		"object_value": json.RawMessage(`{"text":"2024-02-29"}`),
		"overflow":     json.RawMessage("1e309"),
	}
	for _, expression := range []string{"ceil(array_value)", "round(overflow)", "to_date(object_value)"} {
		assertScalarError(t, row, expression, pe.EvalErrTypeMismatch)
	}
}

func TestScalarTemporalValues(t *testing.T) {
	for expression, expected := range map[string]string{
		"to_date('2024-02-29')":                               `"2024-02-29"`,
		"to_timestamp('2024-02-29T08:00:00.123456789+08:00')": `"2024-02-29T00:00:00.123456789Z"`,
		"to_date(null)":                                       "null",
		"to_timestamp(null)":                                  "null",
		"to_timestamp('2024-02-29T00:00:00.1Z') > to_timestamp('2024-02-29T00:00:00Z')": "true",
		"to_date('2024-02-29') = to_timestamp('2024-02-29T08:00:00+08:00')":             "true",
	} {
		assertScalarJSON(t, nil, expression, expected)
	}
	for _, expression := range []string{
		"to_date('2023-02-29')", "to_date('2024-2-9')", "to_date(12)",
		"to_timestamp('2024-02-29T08:00:00')", "to_timestamp('2024-02-29T08:00:00+24:00')",
		"to_timestamp('2024-02-29T08:00:00,1Z')", "to_timestamp('2024-02-29T08:00:00.1234567891Z')",
		"to_timestamp('9999-12-31T23:30:00-02:00')",
	} {
		assertScalarError(t, nil, expression, pe.EvalErrTypeMismatch)
	}
	assertScalarError(t, nil, "to_timestamp()", pe.EvalErrArity)
}

func TestScalarGeomWithin(t *testing.T) {
	row := pe.Row{
		"parcel":        json.RawMessage(`{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,10],[0,0]],[[4,4],[4,6],[6,6],[6,4],[4,4]]]}`),
		"inside":        json.RawMessage(`{"type":"Point","coordinates":[2,2]}`),
		"outside":       json.RawMessage(`{"type":"Point","coordinates":[11,2]}`),
		"edge":          json.RawMessage(`{"type":"Point","coordinates":[0,5]}`),
		"hole":          json.RawMessage(`{"type":"Point","coordinates":[5,5]}`),
		"hole_edge":     json.RawMessage(`{"type":"Point","coordinates":[4,5]}`),
		"line":          json.RawMessage(`{"type":"LineString","coordinates":[[0,2],[3,2]]}`),
		"crossing_hole": json.RawMessage(`{"type":"LineString","coordinates":[[3,5],[7,5]]}`),
		"inner":         json.RawMessage(`{"type":"Polygon","coordinates":[[[1,1],[3,1],[3,3],[1,3],[1,1]]]}`),
		"empty":         json.RawMessage("null"),
		"broken":        json.RawMessage(`{"type":"Point","coordinates":[null,2]}`),
		"encoded":       json.RawMessage(`"{\"type\":\"Point\",\"coordinates\":[2,2]}"`),
		"self_crossing": json.RawMessage(`{"type":"Polygon","coordinates":[[[1,1],[3,3],[3,1],[1,3],[1,1]]]}`),
		"other_crs":     json.RawMessage(`{"type":"Point","coordinates":[2,2],"crs":{"authority":"EPSG","code":3857}}`),
	}
	for expression, expected := range map[string]string{
		"geom_within(encoded,parcel)":       "true",
		"geom_within(inside,parcel)":        "true",
		"geom_within(outside,parcel)":       "false",
		"geom_within(edge,parcel)":          "false",
		"geom_within(hole,parcel)":          "false",
		"geom_within(hole_edge,parcel)":     "false",
		"geom_within(line,parcel)":          "true",
		"geom_within(crossing_hole,parcel)": "false",
		"geom_within(inner,parcel)":         "true",
		"geom_within(parcel,parcel)":        "true",
		"geom_within(empty,parcel)":         "null",
		"geom_within(inside,null)":          "null",
	} {
		assertScalarJSON(t, row, expression, expected)
	}
	for _, expression := range []string{"geom_within(broken,parcel)", "geom_within(self_crossing,parcel)", "geom_within(other_crs,parcel)", "geom_within('bad',parcel)"} {
		assertScalarError(t, row, expression, pe.EvalErrTypeMismatch)
	}
	assertScalarError(t, row, "geom_within(parcel)", pe.EvalErrArity)
	assertOk(t, "geom_within(null,parcel)", pe.NewColumnEnv().With("parcel", pe.GeometryType()), pe.BooleanType())
}

func assertScalarJSON(t *testing.T, row pe.Row, expression, expected string) {
	t.Helper()
	node, err := pe.ParseExpr(expression)
	require.NoError(t, err, expression)
	value, err := pe.Eval(node, row)
	require.NoError(t, err, expression)
	require.Equal(t, expected, string(value.ToJSON()), expression)
}

func assertScalarError(t *testing.T, row pe.Row, expression string, expected pe.EvalErrorKind) {
	t.Helper()
	node, err := pe.ParseExpr(expression)
	require.NoError(t, err, expression)
	_, err = pe.Eval(node, row)
	var failure *pe.EvalError
	require.ErrorAs(t, err, &failure, expression)
	require.Equal(t, expected, failure.Kind, expression)
}
