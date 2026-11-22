package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

var jsonExtractStepPattern = regexp.MustCompile(`^(?:\.([A-Za-z_][A-Za-z0-9_]*)|\[([0-9]+)\]|\[("(?:[^"\\]|\\.)*")\])`)

type jsonExtractStep struct {
	member string
	index  int
	array  bool
}

func jsonExtractPath(path string) ([]jsonExtractStep, error) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(path), "$")
	if !ok {
		return nil, errors.New("JSON extraction path must start with $")
	}
	var steps []jsonExtractStep
	for rest != "" {
		match := jsonExtractStepPattern.FindStringSubmatch(rest)
		if match == nil {
			return nil, errors.New("JSON extraction path requires a member name or an array index")
		}
		var step jsonExtractStep
		switch {
		case match[2] != "":
			index, err := strconv.Atoi(match[2])
			if err != nil {
				return nil, errors.New("JSON extraction array index is out of range")
			}
			step.index, step.array = index, true
		case match[3] != "":
			if err := json.Unmarshal([]byte(match[3]), &step.member); err != nil {
				return nil, errors.New("JSON extraction member is not a valid quoted string")
			}
		default:
			step.member = match[1]
		}
		steps = append(steps, step)
		rest = rest[len(match[0]):]
	}
	return steps, nil
}

func validateJSONExtractBlock(block runtimeTransformBlock) ([]jsonExtractStep, error) {
	if strings.TrimSpace(block.SourceColumn) == "" || strings.TrimSpace(block.TargetColumn) == "" {
		return nil, errors.New("JSON extraction requires source and target columns")
	}
	if block.SourceColumn == block.TargetColumn {
		return nil, errors.New("JSON extraction target must be a new column")
	}
	return jsonExtractPath(block.JSONPath)
}

func applyJSONExtractSchema(nodeID string, schema pipelineStrictSchema, block runtimeTransformBlock, report *pipelineStrictValidationReport) pipelineStrictSchema {
	if _, err := validateJSONExtractBlock(block); err != nil {
		report.addError(nodeID, nil, "invalid_json_extract", err.Error())
		return schema
	}
	if !schema.Known {
		return schema
	}
	if _, ok := schema.field(block.SourceColumn); !ok {
		report.addError(nodeID, strPtr(block.SourceColumn), "missing_column", fmt.Sprintf("column %q does not exist in upstream schema", block.SourceColumn))
		return schema
	}
	if _, ok := schema.field(block.TargetColumn); ok {
		report.addError(nodeID, strPtr(block.TargetColumn), "json_extract_target_exists", "JSON extraction target must be a new column")
		return schema
	}
	fields := append(cloneStrictFields(schema.Fields), pipelineStrictValidationField{Name: block.TargetColumn, FieldType: "STRING", Nullable: true})
	return pipelineStrictSchema{Known: true, Fields: fields}
}

func applyRuntimeJSONExtractBlock(rows []pe.Row, block runtimeTransformBlock) ([]pe.Row, error) {
	steps, err := validateJSONExtractBlock(block)
	if err != nil {
		return nil, err
	}
	out := make([]pe.Row, 0, len(rows))
	for index, row := range rows {
		if _, ok := row[block.TargetColumn]; ok {
			return nil, fmt.Errorf("JSON extraction target column %q already exists", block.TargetColumn)
		}
		value, err := extractJSONText(row[block.SourceColumn], steps)
		if err != nil {
			return nil, fmt.Errorf("JSON extraction for column %q at row %d: %w", block.SourceColumn, index+1, err)
		}
		next := cloneRow(row)
		next[block.TargetColumn] = value
		out = append(out, next)
	}
	return out, nil
}

func extractJSONText(raw json.RawMessage, steps []jsonExtractStep) (json.RawMessage, error) {
	value := bytes.TrimSpace(raw)
	if len(value) == 0 {
		return pe.EvalNull().ToJSON(), nil
	}
	// A STRING input contains a JSON document. Native JSON uses its original bytes.
	if value[0] == '"' {
		var document string
		if err := json.Unmarshal(value, &document); err != nil {
			return nil, errors.New("malformed JSON document")
		}
		value = []byte(document)
	}
	if !json.Valid(value) {
		return nil, errors.New("malformed JSON document")
	}
	for _, step := range steps {
		value = bytes.TrimSpace(value)
		if len(value) == 0 {
			break
		}
		switch {
		case step.array && value[0] == '[':
			var array []json.RawMessage
			if err := json.Unmarshal(value, &array); err != nil {
				return nil, err
			}
			if step.index >= len(array) {
				return pe.EvalNull().ToJSON(), nil
			}
			value = array[step.index]
		case !step.array && value[0] == '{':
			var object map[string]json.RawMessage
			if err := json.Unmarshal(value, &object); err != nil {
				return nil, err
			}
			value = object[step.member]
		default:
			return pe.EvalNull().ToJSON(), nil
		}
	}
	value = bytes.TrimSpace(value)
	switch {
	case len(value) == 0 || bytes.Equal(value, []byte("null")):
		return pe.EvalNull().ToJSON(), nil
	case value[0] == '"':
		return value, nil
	default:
		var compact bytes.Buffer
		if err := json.Compact(&compact, value); err != nil {
			return nil, err
		}
		return pe.EvalString(compact.String()).ToJSON(), nil
	}
}
