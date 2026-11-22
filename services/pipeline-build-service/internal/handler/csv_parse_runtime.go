package handler

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	pe "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
)

type runtimeCSVParseConfig struct {
	source    string
	delimiter rune
	hasHeader bool
}

func validateRuntimeCSVParse(block runtimeTransformBlock) (runtimeCSVParseConfig, error) {
	if strings.TrimSpace(block.SourceColumn) == "" {
		return runtimeCSVParseConfig{}, errors.New("CSV parsing requires a source column")
	}
	delimiter := ','
	if block.CSVDelimiter != "" {
		char, size := utf8.DecodeRuneInString(block.CSVDelimiter)
		if size != len(block.CSVDelimiter) || char == utf8.RuneError || char == 0 || char == '"' || char == '\r' || char == '\n' {
			return runtimeCSVParseConfig{}, errors.New("CSV delimiter must be one character other than a quote, NUL, or newline")
		}
		delimiter = char
	}
	return runtimeCSVParseConfig{
		source: block.SourceColumn, delimiter: delimiter, hasHeader: block.CSVHasHeader == nil || *block.CSVHasHeader,
	}, nil
}

func applyCSVParseSchema(nodeID string, schema pipelineStrictSchema, block runtimeTransformBlock, report *pipelineStrictValidationReport) pipelineStrictSchema {
	config, err := validateRuntimeCSVParse(block)
	if err != nil {
		report.addError(nodeID, nil, "invalid_csv_parse", err.Error())
		return schema
	}
	if !schema.Known {
		return schema
	}
	field, ok := schema.field(config.source)
	if !ok {
		report.addError(nodeID, strPtr(config.source), "missing_column", fmt.Sprintf("column %q does not exist in upstream schema", config.source))
		return schema
	}
	if fieldType(field).Kind != pe.KindString {
		report.addError(nodeID, strPtr(config.source), "invalid_csv_parse_column", "CSV parsing requires a text column")
		return schema
	}
	// CSV headers or field count are only available after parsing data.
	return pipelineStrictSchema{}
}

func applyRuntimeCSVParseBlock(rows []pe.Row, block runtimeTransformBlock) ([]pe.Row, error) {
	config, err := validateRuntimeCSVParse(block)
	if err != nil {
		return nil, err
	}
	var expected []string
	out := make([]pe.Row, 0, len(rows))
	remainingBytes := int64(maxDatasetInputTotalBytes)
	for index, row := range rows {
		raw := bytes.TrimSpace(row[config.source])
		if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
			continue
		}
		var document string
		if err := json.Unmarshal(raw, &document); err != nil {
			return nil, fmt.Errorf("CSV column %q at row %d requires text", config.source, index+1)
		}
		reader := csv.NewReader(strings.NewReader(strings.TrimPrefix(document, "\uFEFF")))
		reader.Comma = config.delimiter
		first, err := reader.Read()
		if errors.Is(err, io.EOF) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("CSV column %q at row %d: %w", config.source, index+1, err)
		}
		names, err := csvParseColumnNames(first, row, config.hasHeader)
		if err != nil {
			return nil, fmt.Errorf("CSV column %q at row %d: %w", config.source, index+1, err)
		}
		sorted := slices.Clone(names)
		slices.Sort(sorted)
		if expected == nil {
			expected = sorted
		} else if !slices.Equal(expected, sorted) {
			return nil, fmt.Errorf("CSV column %q at row %d has different output columns", config.source, index+1)
		}
		if config.hasHeader {
			first, err = reader.Read()
		}
		rowBytes := int64(4)
		for name, value := range row {
			rowBytes += int64(len(name) + len(value) + 4)
		}
		for record, readErr := first, err; !errors.Is(readErr, io.EOF); record, readErr = reader.Read() {
			if readErr != nil {
				return nil, fmt.Errorf("CSV column %q at row %d: %w", config.source, index+1, readErr)
			}
			if len(out) == maxLightweightExpandedRows {
				return nil, fmt.Errorf("CSV output exceeds the %d-row lightweight limit", maxLightweightExpandedRows)
			}
			recordBytes := rowBytes
			values := make([]json.RawMessage, len(record))
			for i, value := range record {
				values[i] = pe.EvalString(value).ToJSON()
				recordBytes += int64(len(names[i]) + len(values[i]) + 4)
			}
			remainingBytes -= recordBytes
			if remainingBytes < 0 {
				return nil, fmt.Errorf("CSV output exceeds the %d-byte lightweight materialization budget", maxDatasetInputTotalBytes)
			}
			next := cloneRow(row)
			for i, name := range names {
				next[name] = values[i]
			}
			out = append(out, next)
		}
	}
	return out, nil
}

func csvParseColumnNames(first []string, row pe.Row, hasHeader bool) ([]string, error) {
	names := make([]string, len(first))
	seen := make(map[string]struct{}, len(first))
	for i, raw := range first {
		name := "column_" + strconv.Itoa(i+1)
		if hasHeader {
			name = strings.TrimSpace(raw)
		}
		if _, duplicate := seen[name]; name == "" || duplicate {
			return nil, errors.New("CSV headers must be nonempty and unique")
		}
		if _, exists := row[name]; exists {
			return nil, fmt.Errorf("CSV output column %q already exists", name)
		}
		seen[name] = struct{}{}
		names[i] = name
	}
	return names, nil
}
