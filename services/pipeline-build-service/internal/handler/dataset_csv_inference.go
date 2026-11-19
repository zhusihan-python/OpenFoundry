package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
)

// Match the DVS CSV column limit. Streaming inference keeps one entry per
// column across the snapshot, even when files have different headers.
const (
	csvInferenceMaxColumns   = 256
	csvInferenceMaxNameBytes = 64 << 10
)

type csvSchemaInference struct {
	rows      int64
	nameBytes int
	fields    map[string]csvInferredField
}

type csvInferredField struct {
	kind    string
	present int64
}

func (c *csvSchemaInference) column(name string) (csvInferredField, error) {
	if field, ok := c.fields[name]; ok {
		return field, nil
	}
	if len(c.fields) >= csvInferenceMaxColumns {
		return csvInferredField{}, errors.New("dataset_csv_schema_columns_exceeded: CSV inference supports at most 256 columns per input")
	}
	if name == "" {
		return csvInferredField{}, errors.New("dataset_csv_schema_invalid: empty column name")
	}
	if len(name) > csvInferenceMaxNameBytes-c.nameBytes {
		return csvInferredField{}, errors.New("dataset_csv_schema_names_exceeded: CSV column names exceed 64 KiB")
	}
	if c.fields == nil {
		c.fields = make(map[string]csvInferredField)
	}
	c.nameBytes += len(name)
	c.fields[name] = csvInferredField{}
	return csvInferredField{}, nil
}

func (c *csvSchemaInference) addRow(raw json.RawMessage) error {
	var row map[string]json.RawMessage
	if err := json.Unmarshal(raw, &row); err != nil || row == nil {
		return errors.New("dataset_csv_schema_invalid: CSV scan returned a non-object row")
	}
	c.rows++
	for name, rawValue := range row {
		field, err := c.column(name)
		if err != nil {
			return err
		}
		if bytes.Equal(bytes.TrimSpace(rawValue), []byte("null")) {
			continue
		}
		kind := inferSchemaValueType(rawValue)
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

func (c *csvSchemaInference) addHeaders(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	var headers []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &headers); err != nil {
		return errors.New("dataset_csv_schema_invalid: CSV scan returned invalid column metadata")
	}
	for _, header := range headers {
		if _, err := c.column(header.Name); err != nil {
			return err
		}
	}
	return nil
}

func (c *csvSchemaInference) schema() *datasetRowsSchema {
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
