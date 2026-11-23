package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var errDatasetOutputProjectionInvalid = errors.New("dataset_output_projection_invalid")

// Only the nested declaration describes a publication. Transforms such as
// drop and select already use a top-level columns parameter of their own.
func declaredOutputColumns(fields, nested map[string]json.RawMessage) ([]string, error) {
	raw, declared := nested["columns"]
	if !declared {
		return nil, nil
	}
	var cfg outputDatasetConfig
	if err := json.Unmarshal(fields["_output"], &cfg); err != nil {
		return nil, fmt.Errorf("%w: invalid _output settings: %w", errDatasetOutputProjectionInvalid, err)
	}
	if cfg.Kind != "" && !strings.EqualFold(strings.TrimSpace(cfg.Kind), "dataset") {
		return nil, fmt.Errorf("%w: _output.columns requires a dataset output", errDatasetOutputProjectionInvalid)
	}
	var columns []string
	if err := json.Unmarshal(raw, &columns); err != nil || len(columns) == 0 {
		return nil, fmt.Errorf("%w: _output.columns must be a nonempty array of column names", errDatasetOutputProjectionInvalid)
	}
	seen := make(map[string]bool, len(columns))
	for i, name := range columns {
		columns[i] = strings.TrimSpace(name)
		if columns[i] == "" {
			return nil, fmt.Errorf("%w: _output.columns contains an empty column name", errDatasetOutputProjectionInvalid)
		}
		if seen[columns[i]] {
			return nil, fmt.Errorf("%w: _output.columns repeats column %q", errDatasetOutputProjectionInvalid, columns[i])
		}
		seen[columns[i]] = true
	}
	if fields["output_schema_version"] != nil || nested["output_schema_version"] != nil {
		return nil, fmt.Errorf("%w: _output.columns cannot be combined with output_schema_version", errDatasetOutputProjectionInvalid)
	}
	for _, config := range []map[string]json.RawMessage{fields, nested} {
		var format string
		if value := config["file_format"]; value != nil {
			if err := json.Unmarshal(value, &format); err != nil || !projectionSupportsFormat(format) {
				return nil, fmt.Errorf("%w: _output.columns requires NDJSON output", errDatasetOutputProjectionInvalid)
			}
		}
		var path string
		if value := config["logical_path"]; value != nil {
			if err := json.Unmarshal(value, &path); err != nil || (path != "" && !strings.HasSuffix(path, ".ndjson")) {
				return nil, fmt.Errorf("%w: _output.columns requires an NDJSON logical_path", errDatasetOutputProjectionInvalid)
			}
		}
	}
	return columns, nil
}

func projectionSupportsFormat(format string) bool {
	switch strings.ToUpper(strings.TrimSpace(format)) {
	case "", "NDJSON", "TEXT":
		return true
	}
	return false
}

func projectedOutputFields(fields []datasetOutputField, columns []string) ([]datasetOutputField, error) {
	byName := make(map[string]datasetOutputField, len(fields))
	for _, field := range fields {
		byName[field.Name] = field
	}
	projected := make([]datasetOutputField, 0, len(columns))
	for _, name := range columns {
		field, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("dataset_output_projection_invalid: schema has no field %q", name)
		}
		projected = append(projected, field)
	}
	return projected, nil
}
