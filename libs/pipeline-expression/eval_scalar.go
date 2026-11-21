package pipelineexpression

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"time"

	geospatialcore "github.com/openfoundry/openfoundry-go/libs/geospatial-core"
)

func evalRounding(name string, args []Expr, row Row, apply func(float64) float64) (EvalValue, error) {
	if err := needArity(name, args, 1); err != nil {
		return EvalValue{}, err
	}
	value, err := evalScalarArgument(name, args[0], row)
	if err != nil {
		return EvalValue{}, err
	}
	switch value.Kind {
	case EvalKindNull, EvalKindInteger:
		return value, nil
	case EvalKindDouble:
		if !math.IsNaN(value.Double) && !math.IsInf(value.Double, 0) {
			return EvalDouble(apply(value.Double)), nil
		}
	}
	return EvalValue{}, &EvalError{Kind: EvalErrTypeMismatch, Message: name + " expects a finite number"}
}

// time.Parse accepts fractional commas and timezone offsets outside RFC 3339.
var timestampText = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$`)

func evalTemporal(name string, args []Expr, row Row, kind EvalValueKind, layout string) (EvalValue, error) {
	if err := needArity(name, args, 1); err != nil {
		return EvalValue{}, err
	}
	value, err := evalScalarArgument(name, args[0], row)
	if err != nil || value.Kind == EvalKindNull {
		return value, err
	}
	format := "YYYY-MM-DD text"
	if kind == EvalKindTimestamp {
		format = "RFC 3339 text with an explicit offset"
	}
	if value.Kind == EvalKindString && (kind != EvalKindTimestamp || timestampText.MatchString(value.Str)) {
		parsed, err := time.Parse(layout, value.Str)
		if err == nil {
			utc := parsed.UTC()
			if utc.Year() >= 0 && utc.Year() <= 9999 {
				return EvalValue{Kind: kind, Time: utc}, nil
			}
		}
	}
	return EvalValue{}, &EvalError{Kind: EvalErrTypeMismatch, Message: name + " expects " + format}
}

// Generic evaluation collapses unsupported JSON values to null. These scalar
// calls distinguish a literal null column from a value they cannot decode.
func evalScalarArgument(name string, expr Expr, row Row) (EvalValue, error) {
	value, err := Eval(expr, row)
	if err == nil && value.Kind == EvalKindNull && expr.Kind == ExprColumn {
		raw := bytes.TrimSpace(row[expr.Name])
		if len(raw) != 0 && !bytes.Equal(raw, []byte("null")) {
			return EvalValue{}, &EvalError{Kind: EvalErrTypeMismatch, Message: name + " expects a supported scalar value"}
		}
	}
	return value, err
}

func (v EvalValue) isTemporal() bool {
	return v.Kind == EvalKindDate || v.Kind == EvalKindTimestamp
}

func evalGeomWithin(name string, args []Expr, row Row) (EvalValue, error) {
	if err := needArity(name, args, 2); err != nil {
		return EvalValue{}, err
	}
	left, err := geometryArgument(args[0], row)
	if err != nil {
		return EvalValue{}, err
	}
	right, err := geometryArgument(args[1], row)
	if err != nil {
		return EvalValue{}, err
	}
	if bytes.Equal(bytes.TrimSpace(left), []byte("null")) || bytes.Equal(bytes.TrimSpace(right), []byte("null")) {
		return EvalNull(), nil
	}
	inside, err := geospatialcore.GeoJSONWithin(left, right)
	if err != nil {
		return EvalValue{}, &EvalError{Kind: EvalErrTypeMismatch, Message: "geom_within: " + err.Error()}
	}
	return EvalBool(inside), nil
}

func geometryArgument(expr Expr, row Row) (json.RawMessage, error) {
	if expr.Kind == ExprColumn {
		raw, ok := row[expr.Name]
		if !ok {
			return nil, &EvalError{Kind: EvalErrUnknownColumn, Name: expr.Name}
		}
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) > 0 && trimmed[0] != '"' {
			return raw, nil
		}
	}
	value, err := Eval(expr, row)
	if err != nil {
		return nil, err
	}
	switch value.Kind {
	case EvalKindNull:
		return value.ToJSON(), nil
	case EvalKindString:
		raw := bytes.TrimSpace([]byte(value.Str))
		if len(raw) != 0 && raw[0] == '{' {
			return raw, nil
		}
	}
	return nil, &EvalError{Kind: EvalErrTypeMismatch,
		Message: fmt.Sprintf("geom_within expects GeoJSON geometry, got %s", typeHintDebug(value.TypeHint()))}
}
