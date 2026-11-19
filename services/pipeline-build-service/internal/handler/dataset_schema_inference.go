package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// Match the DVS CSV column limit. Streaming inference keeps one entry per
// column across the snapshot, even when input records have different keys.
const (
	schemaInferenceMaxColumns   = 256
	schemaInferenceMaxNameBytes = 64 << 10
)

type datasetSchemaInference struct {
	format    string
	rows      int64
	nameBytes int
	fields    map[string]inferredDatasetField
}

type inferredDatasetField struct {
	kind    string
	present int64
}

func (c *datasetSchemaInference) column(name string) (inferredDatasetField, error) {
	if field, ok := c.fields[name]; ok {
		return field, nil
	}
	if len(c.fields) >= schemaInferenceMaxColumns {
		return inferredDatasetField{}, fmt.Errorf("dataset_%s_schema_columns_exceeded: inference supports at most 256 columns per input", c.format)
	}
	if name == "" {
		return inferredDatasetField{}, fmt.Errorf("dataset_%s_schema_invalid: empty column name", c.format)
	}
	if len(name) > schemaInferenceMaxNameBytes-c.nameBytes {
		return inferredDatasetField{}, fmt.Errorf("dataset_%s_schema_names_exceeded: column names exceed 64 KiB", c.format)
	}
	if c.fields == nil {
		c.fields = make(map[string]inferredDatasetField)
	}
	c.nameBytes += len(name)
	c.fields[name] = inferredDatasetField{}
	return inferredDatasetField{}, nil
}

func (c *datasetSchemaInference) addRow(raw json.RawMessage) error {
	var row map[string]json.RawMessage
	if err := json.Unmarshal(raw, &row); err != nil || row == nil {
		return fmt.Errorf("dataset_%s_schema_invalid: scan returned a non-object row", c.format)
	}
	c.rows++
	for name, rawValue := range row {
		field, err := c.column(name)
		if err != nil {
			return err
		}
		value := bytes.TrimSpace(rawValue)
		if bytes.Equal(value, []byte("null")) {
			continue
		}
		kind := inferSchemaValueType(value)
		if c.format == "json" {
			if value[0] == '[' {
				kind = "ARRAY"
			} else if value[0] == '{' {
				kind = "STRUCT"
			}
		}
		switch {
		case field.kind == "", field.kind == kind:
			field.kind = kind
		case (field.kind == "LONG" && kind == "DOUBLE") || (field.kind == "DOUBLE" && kind == "LONG"):
			field.kind = "DOUBLE"
		default:
			field.kind = "STRING"
		}
		field.present++
		c.fields[name] = field
	}
	return nil
}

func (c *datasetSchemaInference) addHeaders(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	var headers []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &headers); err != nil {
		return fmt.Errorf("dataset_%s_schema_invalid: scan returned invalid column metadata", c.format)
	}
	for _, header := range headers {
		if _, err := c.column(header.Name); err != nil {
			return err
		}
	}
	return nil
}

func (c *datasetSchemaInference) schema() *datasetRowsSchema {
	fields := make([]datasetRowsField, 0, len(c.fields))
	for name, column := range c.fields {
		kind := column.kind
		if kind == "" {
			kind = "STRING"
		}
		fields = append(fields, datasetRowsField{
			Name: name, Type: kind, Nullable: c.rows == 0 || column.present < c.rows,
		})
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
	return &datasetRowsSchema{FileFormat: "TEXT", Fields: fields}
}
