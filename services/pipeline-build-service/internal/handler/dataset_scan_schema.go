package handler

import (
	"fmt"
	"slices"
)

type datasetRowsSchema struct {
	FileFormat string             `json:"file_format"`
	Fields     []datasetRowsField `json:"fields"`
}

type datasetRowsField struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Nullable  bool   `json:"nullable"`
	Precision int    `json:"precision,omitempty"`
	Scale     int    `json:"scale,omitempty"`
}

func (s *datasetRowsSchema) columns() []string {
	if s == nil {
		return nil
	}
	columns := make([]string, len(s.Fields))
	for i, field := range s.Fields {
		columns[i] = field.Name
	}
	return columns
}

type batchedScanMetadata struct {
	Format        string
	SchemaVersion string
	Schema        *datasetRowsSchema
}

// bindScanMetadata uses the first successful page to bind the format and
// typed schema. Later pages must match, including empty continuation pages.
func bindScanMetadata(path string, pinned *batchedScanMetadata, page batchedScanMetadata) error {
	if page.Format == "" {
		// A legacy scan response may omit the NDJSON format field.
		page.Format = "ndjson"
	}
	if pinned.Format != "" && (page.Format != pinned.Format || page.SchemaVersion != pinned.SchemaVersion) {
		return fmt.Errorf("batched_scan_schema_mismatch: page format/schema (%q/%q) != pinned format/schema (%q/%q)",
			page.Format, page.SchemaVersion, pinned.Format, pinned.SchemaVersion)
	}
	if path != "" && (page.Format == batchedParquetFormat) != batchedParquetPath(path) {
		return fmt.Errorf("batched_scan_format_invalid: page format %q does not match requested path %q", page.Format, path)
	}
	switch page.Format {
	case "ndjson":
		if page.SchemaVersion != "" || page.Schema != nil {
			return fmt.Errorf("batched_scan_schema_invalid: unexpected typed schema for NDJSON")
		}
	case batchedParquetFormat:
		if page.SchemaVersion != ordersSchemaVersion || !validOrdersScanSchema(page.Schema) {
			return fmt.Errorf("batched_scan_schema_invalid: Parquet pages must carry the %s schema", ordersSchemaVersion)
		}
	default:
		return fmt.Errorf("batched_scan_format_invalid: unsupported page format %q", page.Format)
	}
	if pinned.Format == "" {
		*pinned = page
	}
	return nil
}

func validOrdersScanSchema(schema *datasetRowsSchema) bool {
	want := ordersDatasetSchema()
	return schema != nil && schema.FileFormat == want.FileFormat && slices.Equal(schema.Fields, want.Fields)
}

func ordersDatasetSchema() datasetRowsSchema {
	return datasetRowsSchema{FileFormat: "PARQUET", Fields: []datasetRowsField{
		{Name: "order_id", Type: "LONG"},
		{Name: "amount", Type: "DECIMAL", Precision: ordersAmountPrecision, Scale: ordersAmountScale},
		{Name: "order_date", Type: "DATE"},
		{Name: "ordered_at", Type: "TIMESTAMP"},
		{Name: "source_time_zone", Type: "STRING"},
		{Name: "note", Type: "STRING", Nullable: true},
	}}
}
