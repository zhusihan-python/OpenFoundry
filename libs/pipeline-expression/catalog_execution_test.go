package pipelineexpression_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

func TestDeclaredScalarsAreExecutable(t *testing.T) {
	for _, expression := range []string{
		"title_case('hello')", "lower('A')", "upper('a')", "trim(' a ')", "clean_string('a  b')",
		"concat('a','b')", "ABS(-2)", "floor(1.2)", "ceil(1.2)", "round(1.2)",
		"haversine_meters(0,0,0,1)", "haversine_km(0,0,0,1)", "haversine_miles(0,0,0,1)",
		"haversine_distance(0,0,0,1,'km')", "to_date('2026-11-22')", "to_timestamp('2026-11-22T00:00:00Z')",
		"cast('2','LONG')", "is_null(null)", "is_not_null(1)",
		`geom_within('{"type":"Point","coordinates":[1,1]}','{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,2],[0,0]]]}')`,
	} {
		t.Run(expression, func(t *testing.T) {
			expr, err := pe.ParseExpr(expression)
			require.NoError(t, err)
			_, declared := pe.ScalarSignatureFor(expr.Name)
			require.True(t, declared)
			require.True(t, pe.HasExecutableScalarCalls(expr))
			_, err = pe.Eval(expr, nil)
			require.NoError(t, err)
		})
	}
}

func TestExecutableScalarCallsRejectUnknownNestedCalls(t *testing.T) {
	expr, err := pe.ParseExpr("not (abs(unknown_function(1)) > 2)")
	require.NoError(t, err)
	require.False(t, pe.HasExecutableScalarCalls(expr))
	_, err = pe.Eval(expr, nil)
	var failure *pe.EvalError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, pe.EvalErrUnknownFunction, failure.Kind)
	require.Equal(t, "unknown_function", failure.Name)
}
