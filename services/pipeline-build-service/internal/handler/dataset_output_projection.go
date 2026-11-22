package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Only the nested declaration describes a publication. Transforms such as
// drop and select already use a top-level columns parameter of their own.
func declaredOutputColumns(fields, nested map[string]json.RawMessage) ([]string, error) {
	raw, declared := nested["columns"]
	if !declared {
		return nil, nil
	}
	var cfg outputDatasetConfig
	if err := json.Unmarshal(fields["_output"], &cfg); err != nil {
		return nil, fmt.Errorf("dataset_output_projection_invalid: invalid _output settings: %w", err)
	}
	if cfg.Kind != "" && !strings.EqualFold(strings.TrimSpace(cfg.Kind), "dataset") {
		return nil, errors.New("dataset_output_projection_invalid: _output.columns requires a dataset output")
	}
	var columns []string
	if err := json.Unmarshal(raw, &columns); err != nil || len(columns) == 0 {
		return nil, errors.New("dataset_output_projection_invalid: _output.columns must be a nonempty array of column names")
	}
	for i, name := range columns {
		columns[i] = strings.TrimSpace(name)
		if columns[i] == "" {
			return nil, errors.New("dataset_output_projection_invalid: _output.columns contains an empty column name")
		}
	}
	if fields["output_schema_version"] != nil || nested["output_schema_version"] != nil {
		return nil, errors.New("dataset_output_projection_invalid: _output.columns cannot be combined with output_schema_version")
	}
	for _, config := range []map[string]json.RawMessage{fields, nested} {
		var format string
		if value := config["file_format"]; value != nil {
			if err := json.Unmarshal(value, &format); err != nil || !projectionSupportsFormat(format) {
				return nil, errors.New("dataset_output_projection_invalid: _output.columns requires NDJSON output")
			}
		}
		var path string
		if value := config["logical_path"]; value != nil {
			if err := json.Unmarshal(value, &path); err != nil || (path != "" && !strings.HasSuffix(path, ".ndjson")) {
				return nil, errors.New("dataset_output_projection_invalid: _output.columns requires an NDJSON logical_path")
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
