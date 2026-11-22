package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

type runtimeAggregation struct {
	Function        string `json:"function"`
	SourceColumn    string `json:"source_column,omitempty"`
	TargetColumn    string `json:"target_column"`
	ConditionColumn string `json:"condition_column,omitempty"`
}

func validateRuntimeAggregate(block runtimeTransformBlock) ([]runtimeAggregation, error) {
	if len(block.Aggregations) == 0 {
		return nil, errors.New("aggregate requires at least one measure")
	}
	seen := make(map[string]bool)
	for _, group := range block.GroupBy {
		if strings.TrimSpace(group) == "" || seen[group] {
			return nil, fmt.Errorf("aggregate group column %q is empty or repeated", group)
		}
		seen[group] = true
	}
	conditional := strings.EqualFold(strings.TrimSpace(block.Kind), "conditional_aggregate")
	measures := make([]runtimeAggregation, 0, len(block.Aggregations))
	for _, measure := range block.Aggregations {
		measure.Function = strings.ToLower(strings.TrimSpace(measure.Function))
		switch measure.Function {
		case "count", "sum", "avg", "min", "max":
		default:
			return nil, fmt.Errorf("unsupported aggregation %q", measure.Function)
		}
		if strings.TrimSpace(measure.SourceColumn) == "" {
			if measure.Function != "count" {
				return nil, fmt.Errorf("%s requires a source column", measure.Function)
			}
			measure.SourceColumn = ""
		}
		if strings.TrimSpace(measure.TargetColumn) == "" || seen[measure.TargetColumn] {
			return nil, fmt.Errorf("aggregate output column %q is empty or repeated", measure.TargetColumn)
		}
		if conditional && strings.TrimSpace(measure.ConditionColumn) == "" {
			return nil, fmt.Errorf("aggregate %q requires a Boolean condition column", measure.TargetColumn)
		}
		if !conditional {
			measure.ConditionColumn = ""
		}
		seen[measure.TargetColumn] = true
		measures = append(measures, measure)
	}
	return measures, nil
}

func applyAggregateSchema(nodeID string, schema pipelineStrictSchema, block runtimeTransformBlock, report *pipelineStrictValidationReport) pipelineStrictSchema {
	measures, err := validateRuntimeAggregate(block)
	if err != nil {
		report.addError(nodeID, nil, "invalid_aggregate", err.Error())
		return schema
	}
	if !schema.Known {
		return schema
	}
	fields := make([]pipelineStrictValidationField, 0, len(block.GroupBy)+len(measures))
	for _, group := range block.GroupBy {
		field, ok := schema.field(group)
		if !ok {
			report.addError(nodeID, strPtr(group), "missing_column", fmt.Sprintf("group column %q does not exist", group))
			continue
		}
		if fieldType(field).Kind == pe.KindArray || fieldType(field).Kind == pe.KindGeometry {
			report.addError(nodeID, strPtr(group), "invalid_aggregate_group", "aggregate group keys must be scalar")
		}
		fields = append(fields, field)
	}
	for _, measure := range measures {
		if measure.ConditionColumn != "" {
			condition, exists := schema.field(measure.ConditionColumn)
			if !exists {
				report.addError(nodeID, strPtr(measure.ConditionColumn), "missing_column", fmt.Sprintf("condition column %q does not exist", measure.ConditionColumn))
				continue
			}
			if fieldType(condition).Kind != pe.KindBoolean {
				report.addError(nodeID, strPtr(measure.ConditionColumn), "invalid_aggregate_condition", "aggregate condition requires a Boolean column")
				continue
			}
		}
		source, exists := schema.field(measure.SourceColumn)
		if measure.SourceColumn != "" && !exists {
			report.addError(nodeID, strPtr(measure.SourceColumn), "missing_column", fmt.Sprintf("aggregate source column %q does not exist", measure.SourceColumn))
			continue
		}
		field := pipelineStrictValidationField{Name: measure.TargetColumn, FieldType: "LONG", Nullable: measure.Function != "count"}
		if measure.Function != "count" {
			typ := fieldType(source)
			if !typ.IsNumeric() {
				report.addError(nodeID, strPtr(measure.SourceColumn), "invalid_aggregate_column", "aggregate measure requires a numeric column")
				continue
			}
			switch measure.Function {
			case "avg":
				field.FieldType = "DOUBLE"
			case "sum":
				if typ.Kind != pe.KindInteger && typ.Kind != pe.KindLong {
					field.FieldType = "DOUBLE"
				}
			default:
				field.FieldType = source.FieldType
			}
		}
		fields = append(fields, field)
	}
	return pipelineStrictSchema{Known: true, Fields: fields}
}

type runtimeAggregateState struct {
	count   int64
	number  big.Rat
	raw     json.RawMessage
	hasReal bool
}

type runtimeAggregateGroup struct {
	row      pe.Row
	measures []runtimeAggregateState
}

func applyRuntimeAggregateBlock(rows []pe.Row, block runtimeTransformBlock) ([]pe.Row, error) {
	measures, err := validateRuntimeAggregate(block)
	if err != nil {
		return nil, err
	}
	groups := make(map[string]int)
	ordered := make([]runtimeAggregateGroup, 0)
	if len(block.GroupBy) == 0 {
		groups[""] = 0
		ordered = append(ordered, runtimeAggregateGroup{row: make(pe.Row, len(measures)), measures: make([]runtimeAggregateState, len(measures))})
	}
	for rowIndex, row := range rows {
		key, _, err := runtimeCompositeKey(row, block.GroupBy, true)
		if err != nil {
			return nil, fmt.Errorf("aggregate %w", err)
		}
		index, exists := groups[key]
		if !exists {
			index = len(ordered)
			groups[key] = index
			groupRow := make(pe.Row, len(block.GroupBy)+len(measures))
			for _, column := range block.GroupBy {
				raw := row[column]
				if len(raw) == 0 {
					raw = json.RawMessage("null")
				}
				groupRow[column] = raw
			}
			ordered = append(ordered, runtimeAggregateGroup{row: groupRow, measures: make([]runtimeAggregateState, len(measures))})
		}
		for i, measure := range measures {
			if measure.ConditionColumn != "" {
				switch string(bytes.TrimSpace(row[measure.ConditionColumn])) {
				case "true":
				case "false", "null":
					continue
				default:
					return nil, fmt.Errorf("aggregate %q: condition column %q must be Boolean or NULL in row %d", measure.TargetColumn, measure.ConditionColumn, rowIndex+1)
				}
			}
			if err := ordered[index].measures[i].add(row[measure.SourceColumn], measure); err != nil {
				return nil, fmt.Errorf("aggregate %q: %w", measure.TargetColumn, err)
			}
		}
	}
	out := make([]pe.Row, 0, len(ordered))
	for _, group := range ordered {
		for i, measure := range measures {
			result, err := group.measures[i].result(measure.Function)
			if err != nil {
				return nil, fmt.Errorf("aggregate %q: %w", measure.TargetColumn, err)
			}
			group.row[measure.TargetColumn] = result
		}
		out = append(out, group.row)
	}
	return out, nil
}

func (s *runtimeAggregateState) add(raw json.RawMessage, measure runtimeAggregation) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		if measure.Function == "count" && measure.SourceColumn == "" {
			s.count++
		}
		return nil
	}
	if measure.Function == "count" {
		s.count++
		return nil
	}

	value := pe.EvalValueFromJSON(raw)
	var number big.Rat
	switch value.Kind {
	case pe.EvalKindInteger:
		number.SetInt64(value.Int)
	case pe.EvalKindDouble:
		number.SetFloat64(value.Double)
		s.hasReal = true
	default:
		return fmt.Errorf("column %q requires a numeric value", measure.SourceColumn)
	}
	switch measure.Function {
	case "sum", "avg":
		s.number.Add(&s.number, &number)
	case "min":
		if s.count == 0 || number.Cmp(&s.number) < 0 {
			s.number.Set(&number)
			s.raw = raw
		}
	case "max":
		if s.count == 0 || number.Cmp(&s.number) > 0 {
			s.number.Set(&number)
			s.raw = raw
		}
	}
	s.count++
	return nil
}

func (s *runtimeAggregateState) result(function string) (json.RawMessage, error) {
	if function == "count" {
		return pe.EvalInt(s.count).ToJSON(), nil
	}
	if s.count == 0 {
		return pe.EvalNull().ToJSON(), nil
	}
	if function == "min" || function == "max" {
		return s.raw, nil
	}
	if function == "sum" && !s.hasReal {
		if !s.number.Num().IsInt64() {
			return nil, errors.New("integer sum exceeds int64")
		}
		return pe.EvalInt(s.number.Num().Int64()).ToJSON(), nil
	}
	number := &s.number
	if function == "avg" {
		number = new(big.Rat).Quo(number, big.NewRat(s.count, 1))
	}
	result, _ := number.Float64()
	if math.IsInf(result, 0) {
		return nil, errors.New("aggregate result is not finite")
	}
	return pe.EvalDouble(result).ToJSON(), nil
}
