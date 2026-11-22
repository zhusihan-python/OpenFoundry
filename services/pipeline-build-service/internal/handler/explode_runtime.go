package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

// The lightweight stack stores expanded rows in memory. Limit both large
// values and the number of small rows whose object overhead also adds up.
const maxExplodedRows = 100_000

func explodeColumns(block runtimeTransformBlock) (string, string, error) {
	source := block.SourceColumn
	if strings.TrimSpace(source) == "" {
		return "", "", errors.New("explode requires a source column")
	}
	target := block.TargetColumn
	if strings.TrimSpace(target) == "" {
		target = source
	}
	return source, target, nil
}

func applyExplodeSchema(nodeID string, schema pipelineStrictSchema, block runtimeTransformBlock, report *pipelineStrictValidationReport) pipelineStrictSchema {
	source, target, err := explodeColumns(block)
	if err != nil {
		report.addError(nodeID, nil, "invalid_explode", err.Error())
		return schema
	}
	if !schema.Known {
		return schema
	}
	input, ok := schema.field(source)
	if !ok {
		report.addError(nodeID, strPtr(source), "missing_column", fmt.Sprintf("column %q does not exist in upstream schema", source))
		return schema
	}
	switch fieldType(input).Kind {
	case pe.KindString:
		// A serialized array does not declare its element type.
		return pipelineStrictSchema{}
	case pe.KindArray:
		arrayType := strings.ToUpper(strings.TrimSpace(input.FieldType))
		elementType := strings.TrimSpace(arrayType[len("ARRAY<") : len(arrayType)-1])
		fields := cloneStrictFields(schema.Fields)
		element := pipelineStrictValidationField{Name: target, FieldType: elementType, Nullable: true}
		for index, field := range fields {
			if field.Name == target {
				fields[index] = element
				return pipelineStrictSchema{Known: true, Fields: fields}
			}
		}
		return pipelineStrictSchema{Known: true, Fields: append(fields, element)}
	default:
		report.addError(nodeID, strPtr(source), "invalid_explode_column", "explode requires an array or a JSON array string")
		return schema
	}
}

func applyRuntimeExplodeBlock(rows []pe.Row, block runtimeTransformBlock) ([]pe.Row, error) {
	source, target, err := explodeColumns(block)
	if err != nil {
		return nil, err
	}
	remainingBytes := int64(maxDatasetInputTotalBytes)
	out := make([]pe.Row, 0, len(rows))
	for index, row := range rows {
		elements, err := decodeExplodeArray(row[source], maxExplodedRows-len(out))
		if err != nil {
			return nil, fmt.Errorf("explode column %q at row %d: %w", source, index+1, err)
		}
		if len(elements) == 0 {
			continue
		}
		// Charge the row envelope, field names and the payloads copied below.
		rowBytes := int64(8 + len(target))
		for name, value := range row {
			if name != target {
				rowBytes += int64(len(name) + len(value) + 4)
			}
		}
		remainingBytes -= rowBytes * int64(len(elements))
		for _, value := range elements {
			remainingBytes -= int64(len(value))
		}
		if remainingBytes < 0 {
			return nil, fmt.Errorf("explode output exceeds the %d-byte lightweight materialization budget", maxDatasetInputTotalBytes)
		}
		for _, element := range elements {
			next := make(pe.Row, len(row)+1)
			for name, value := range row {
				if name != target {
					next[name] = append(json.RawMessage(nil), value...)
				}
			}
			next[target] = element
			out = append(out, next)
		}
	}
	return out, nil
}

func decodeExplodeArray(raw json.RawMessage, remainingRows int) ([]json.RawMessage, error) {
	value := bytes.TrimSpace(raw)
	if len(value) == 0 || bytes.Equal(value, []byte("null")) {
		return nil, nil
	}
	if value[0] == '"' {
		var encoded string
		if err := json.Unmarshal(value, &encoded); err != nil {
			return nil, errors.New("expected a JSON array or a JSON array string")
		}
		value = []byte(strings.TrimSpace(encoded))
	}
	if len(value) == 0 || value[0] != '[' || !json.Valid(value) {
		return nil, errors.New("expected a JSON array or a JSON array string")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	var elements []json.RawMessage
	for decoder.More() {
		if len(elements) == remainingRows {
			return nil, fmt.Errorf("output exceeds the %d-row lightweight limit", maxExplodedRows)
		}
		var element json.RawMessage
		if err := decoder.Decode(&element); err != nil {
			return nil, err
		}
		elements = append(elements, element)
	}
	return elements, nil
}
