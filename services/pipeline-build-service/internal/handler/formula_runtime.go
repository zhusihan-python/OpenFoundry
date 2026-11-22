package handler

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

func compileFormula(block runtimeTransformBlock) (string, pe.Expr, error) {
	target := strings.TrimSpace(block.TargetColumn)
	if target == "" {
		return "", pe.Expr{}, errors.New("formula requires a target column")
	}
	expression := strings.TrimSpace(block.Expression)
	if expression == "" {
		return "", pe.Expr{}, errors.New("formula requires an expression")
	}
	parsed, err := pe.ParseExpr(expression)
	if err != nil {
		return "", pe.Expr{}, fmt.Errorf("formula expression parse: %w", err)
	}
	if !pe.HasExecutableScalarCalls(parsed) {
		return "", pe.Expr{}, errors.New("formula expression requires supported scalar functions with the correct number of arguments")
	}
	return target, parsed, nil
}

func applyRuntimeFormulaBlock(rows []pe.Row, block runtimeTransformBlock) ([]pe.Row, error) {
	target, expression, err := compileFormula(block)
	if err != nil {
		return nil, err
	}
	out := make([]pe.Row, 0, len(rows))
	for index, row := range rows {
		if expression.Kind == pe.ExprColumn {
			raw := bytes.TrimSpace(row[expression.Name])
			if len(raw) > 0 && (raw[0] == '[' || raw[0] == '{') {
				return nil, fmt.Errorf("formula target %q row %d requires a scalar result", target, index+1)
			}
		}
		value, err := pe.Eval(expression, row)
		if err != nil {
			return nil, fmt.Errorf("formula target %q row %d: %w", target, index+1, err)
		}
		next := cloneRow(row)
		next[target] = value.ToJSON()
		out = append(out, next)
	}
	return out, nil
}

func applyFormulaSchema(nodeID string, schema pipelineStrictSchema, block runtimeTransformBlock, report *pipelineStrictValidationReport) pipelineStrictSchema {
	target, expression, err := compileFormula(block)
	if err != nil {
		report.addError(nodeID, nil, "invalid_formula", err.Error())
		return schema
	}
	if !schema.Known {
		return schema
	}
	result, typeErrors := pe.InferExpr(expression, schemaToColumnEnv(schema))
	for _, typeErr := range typeErrors {
		report.addError(nodeID, strictColumnFromTypeError(typeErr), validationCodeFromTypeError(typeErr), typeErr.Error())
	}
	if len(typeErrors) != 0 {
		return schema
	}
	switch result.Kind {
	case pe.KindArray, pe.KindStruct, pe.KindGeometry:
		report.addError(nodeID, strPtr(target), "invalid_formula_result", "formula requires a scalar result")
		return schema
	}

	field := pipelineStrictValidationField{Name: target, FieldType: strictTypeName(result), Nullable: true}
	fields := append([]pipelineStrictValidationField(nil), schema.Fields...)
	for index, previous := range fields {
		if previous.Name == target {
			fields[index] = field
			return pipelineStrictSchema{Known: true, Fields: fields}
		}
	}
	return pipelineStrictSchema{Known: true, Fields: append(fields, field)}
}
