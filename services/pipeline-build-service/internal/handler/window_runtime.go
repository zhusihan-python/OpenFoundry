package handler

import (
	"errors"
	"fmt"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

type runtimeWindowFrame struct {
	Preceding int `json:"preceding"`
	Following int `json:"following"`
}

func windowSortKeys(block runtimeTransformBlock) ([]runtimeSortKey, error) {
	if block.Frame == nil || block.Frame.Preceding < 0 || block.Frame.Following < 0 {
		return nil, errors.New("window requires a ROWS frame with nonnegative preceding and following offsets")
	}
	return normalizeRuntimeSortKeys(block.SortKeys)
}

func applyWindowSchema(nodeID string, schema pipelineStrictSchema, block runtimeTransformBlock, report *pipelineStrictValidationReport) pipelineStrictSchema {
	keys, err := windowSortKeys(block)
	if err != nil {
		report.addError(nodeID, nil, "invalid_window", err.Error())
		return schema
	}
	before := len(report.Errors)
	aggregate := applyAggregateSchema(nodeID, schema, block, report)
	if !schema.Known || len(report.Errors) != before {
		return schema
	}
	for _, key := range keys {
		field, exists := schema.field(key.Column)
		if !exists {
			report.addError(nodeID, strPtr(key.Column), "missing_column", fmt.Sprintf("window order column %q does not exist", key.Column))
			continue
		}
		switch fieldType(field).Kind {
		case pe.KindArray, pe.KindStruct, pe.KindGeometry:
			report.addError(nodeID, strPtr(key.Column), "invalid_window_order", "window order keys must be scalar")
		}
	}
	measures := aggregate.Fields[len(block.GroupBy):]
	for _, measure := range measures {
		if _, exists := schema.field(measure.Name); exists {
			report.addError(nodeID, strPtr(measure.Name), "duplicate_column_name", fmt.Sprintf("window output column %q already exists", measure.Name))
		}
	}
	if len(report.Errors) != before {
		return schema
	}
	fields := make([]pipelineStrictValidationField, 0, len(schema.Fields)+len(measures))
	fields = append(fields, schema.Fields...)
	fields = append(fields, measures...)
	return pipelineStrictSchema{Known: true, Fields: fields}
}

func applyRuntimeWindowBlock(rows []pe.Row, block runtimeTransformBlock) ([]pe.Row, error) {
	keys, err := windowSortKeys(block)
	if err != nil {
		return nil, err
	}
	measures, err := validateRuntimeAggregate(block)
	if err != nil {
		return nil, err
	}
	for rowIndex, row := range rows {
		for _, measure := range measures {
			if _, exists := row[measure.TargetColumn]; exists {
				return nil, fmt.Errorf("window output column %q already exists in row %d", measure.TargetColumn, rowIndex+1)
			}
		}
	}
	ordered, err := runtimeSortedRowIndices(rows, keys)
	if err != nil {
		return nil, fmt.Errorf("window %w", err)
	}
	partitions := make([][]int, 0)
	partitionByKey := make(map[string]int)
	for _, rowIndex := range ordered {
		key, _, err := runtimeCompositeKey(rows[rowIndex], block.GroupBy, true)
		if err != nil {
			return nil, fmt.Errorf("window %w", err)
		}
		index, ok := partitionByKey[key]
		if !ok {
			index = len(partitions)
			partitionByKey[key] = index
			partitions = append(partitions, nil)
		}
		partitions[index] = append(partitions[index], rowIndex)
	}
	out := cloneRows(rows)
	for _, partition := range partitions {
		for _, measure := range measures {
			if err := applyWindowMeasure(rows, out, partition, *block.Frame, measure); err != nil {
				return nil, fmt.Errorf("window %q: %w", measure.TargetColumn, err)
			}
		}
	}
	return out, nil
}

// Build a range tree once per measure and partition. It keeps wide frames
// from rescanning the same rows and retains the first equal extremum.
func applyWindowMeasure(rows, out []pe.Row, partition []int, frame runtimeWindowFrame, measure runtimeAggregation) error {
	size := 1
	for size < len(partition) {
		size *= 2
	}
	tree := make([]runtimeAggregateState, size*2)
	for position, index := range partition {
		if err := tree[size+position].add(rows[index][measure.SourceColumn], measure); err != nil {
			return fmt.Errorf("row %d: %w", index+1, err)
		}
	}
	for i := size - 1; i > 0; i-- {
		tree[i].mergeWindow(&tree[2*i], measure.Function)
		tree[i].mergeWindow(&tree[2*i+1], measure.Function)
	}
	for position, index := range partition {
		start := position - min(position, frame.Preceding)
		end := position + min(len(partition)-1-position, frame.Following) + 1
		state := windowRange(tree, size+start, size+end, measure.Function)
		value, err := state.result(measure.Function)
		if err != nil {
			return fmt.Errorf("row %d: %w", index+1, err)
		}
		out[index][measure.TargetColumn] = value
	}
	return nil
}

func windowRange(tree []runtimeAggregateState, start, end int, function string) runtimeAggregateState {
	var left, right runtimeAggregateState
	for start < end {
		if start%2 == 1 {
			left.mergeWindow(&tree[start], function)
			start++
		}
		if end%2 == 1 {
			end--
			var prefix runtimeAggregateState
			prefix.mergeWindow(&tree[end], function)
			prefix.mergeWindow(&right, function)
			right = prefix
		}
		start /= 2
		end /= 2
	}
	left.mergeWindow(&right, function)
	return left
}

func (s *runtimeAggregateState) mergeWindow(other *runtimeAggregateState, function string) {
	if other.count == 0 {
		return
	}
	switch function {
	case "sum", "avg":
		s.number.Add(&s.number, &other.number)
		s.hasReal = s.hasReal || other.hasReal
	case "min", "max":
		order := other.number.Cmp(&s.number)
		if s.count == 0 || (function == "min" && order < 0) || (function == "max" && order > 0) {
			s.number.Set(&other.number)
			s.raw = other.raw
		}
	}
	s.count += other.count
}
