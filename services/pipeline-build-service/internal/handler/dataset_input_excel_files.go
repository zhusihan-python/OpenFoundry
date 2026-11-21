package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/google/uuid"
)

type excelSnapshotFile struct {
	Path       string                    `json:"path"`
	Excel      manualExcelSheetSelection `json:"excel"`
	LongFields []string                  `json:"long_fields,omitempty"`
}

func parseExcelSnapshotFiles(raw json.RawMessage) ([]excelSnapshotFile, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var files []excelSnapshotFile
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&files) != nil || decoder.Decode(new(json.RawMessage)) != io.EOF ||
		len(files) < 2 || len(files) > 16 {
		return nil, fmt.Errorf("excel_files must contain 2 to 16 workbook selections")
	}
	seen := make(map[string]bool, len(files))
	for i := range files {
		file := &files[i]
		if !validBatchedObjectPath(file.Path) || excelSnapshotFormat(file.Path) == "" || seen[file.Path] ||
			strings.TrimSpace(file.Excel.Sheet) == "" || len(file.Excel.Mapping) == 0 || len(file.Excel.Mapping) > 256 {
			return nil, fmt.Errorf("excel_files requires a distinct workbook path, worksheet and mapping for each file")
		}
		seen[file.Path] = true
		names := make(map[string]bool, len(file.Excel.Mapping))
		for k := range file.Excel.Mapping {
			column := &file.Excel.Mapping[k]
			column.Target = strings.TrimSpace(column.Target)
			if column.Target == "" || names[column.Target] {
				return nil, fmt.Errorf("excel_files %q has empty or duplicate target columns", file.Path)
			}
			names[column.Target] = true
		}
		longs := make(map[string]bool, len(file.LongFields))
		for k, name := range file.LongFields {
			name = strings.TrimSpace(name)
			if !names[name] || longs[name] {
				return nil, fmt.Errorf("excel_files %q long_fields must be distinct mapped columns", file.Path)
			}
			longs[name] = true
			file.LongFields[k] = name
		}
	}
	slices.SortFunc(files, func(a, b excelSnapshotFile) int { return strings.Compare(a.Path, b.Path) })
	return files, nil
}

func workbookSchema(files []excelSnapshotFile) (*datasetRowsSchema, error) {
	types := map[string]string{}
	for _, file := range files {
		for _, column := range file.Excel.Mapping {
			kind := "STRING"
			if slices.Contains(file.LongFields, column.Target) {
				kind = "LONG"
			}
			if previous, ok := types[column.Target]; ok && previous != kind {
				return nil, fmt.Errorf("excel_files_type_conflict: target %q has both %s and %s declarations", column.Target, previous, kind)
			}
			types[column.Target] = kind
		}
	}
	const maxCombinedWorkbookColumns = 256
	if len(types) > maxCombinedWorkbookColumns {
		return nil, fmt.Errorf("excel_files_column_limit: at most %d combined columns", maxCombinedWorkbookColumns)
	}
	schema := &datasetRowsSchema{FileFormat: normaliseOutputFileFormat("NDJSON")}
	for name, kind := range types {
		schema.Fields = append(schema.Fields, datasetRowsField{Name: name, Type: kind})
	}
	slices.SortFunc(schema.Fields, func(a, b datasetRowsField) int { return strings.Compare(a.Name, b.Name) })
	return schema, nil
}

func (c batchedDatasetClient) readExcelSnapshotFiles(ctx context.Context, rid string, datasetID uuid.UUID, req DatasetRowsRequest, limit int, maxBytes int64) (DatasetRowsResult, error) {
	if req.Path != "" {
		return DatasetRowsResult{}, fmt.Errorf("dataset_snapshot_conflict: excel_files provides file paths; omit dataset_path")
	}
	files := req.Snapshot.ExcelFiles
	schema, err := workbookSchema(files)
	if err != nil {
		return DatasetRowsResult{}, err
	}
	out := DatasetRowsResult{DatasetID: datasetID.String(), Format: "ndjson", Source: "dataset-versioning-excel-files", Schema: schema, Columns: schema.columns()}
	remainingBytes := maxBytes
	var outputBytes int64
	for _, file := range files {
		remainingRows := limit - len(out.Rows)
		if remainingRows <= 0 || remainingBytes <= 0 {
			return DatasetRowsResult{}, fmt.Errorf("excel_files_input_limit: combined workbook input exceeds the row or byte budget")
		}
		pin := *req.Snapshot
		pin.ExcelFiles = nil
		pin.Excel, err = json.Marshal(file.Excel)
		if err != nil {
			return DatasetRowsResult{}, err
		}
		read := req
		read.Path = file.Path
		read.Snapshot = &pin
		page, err := c.readExcelSnapshotRows(ctx, rid, datasetID, read, remainingRows, remainingBytes)
		if err != nil {
			return DatasetRowsResult{}, fmt.Errorf("workbook %q: %w", file.Path, err)
		}
		columns := make([]string, 0, len(file.Excel.Mapping))
		for _, column := range file.Excel.Mapping {
			columns = append(columns, column.Target)
		}
		slices.Sort(columns)
		if !slices.Equal(columns, page.Columns) || len(page.Rows) != len(page.PhysicalRows) {
			return DatasetRowsResult{}, fmt.Errorf("excel_files_binding: unexpected columns or physical rows in workbook %q", file.Path)
		}
		remainingBytes -= page.responseBytes
		for i, row := range page.Rows {
			issues := validateExcelGroupRow(row, manualExcelGroupSheet{LongFields: file.LongFields})
			if len(issues) != 0 {
				return DatasetRowsResult{}, fmt.Errorf("excel_files_invalid_value: workbook %q worksheet %q row %d field %q: %s",
					file.Path, file.Excel.Sheet, page.PhysicalRows[i], issues[0].field, issues[0].code)
			}
			for k := range schema.Fields {
				field := &schema.Fields[k]
				if excelGroupFieldMissing(row[field.Name]) {
					row[field.Name] = json.RawMessage("null")
					field.Nullable = true
				}
			}
			encoded, err := json.Marshal(row)
			if err != nil {
				return DatasetRowsResult{}, err
			}
			outputBytes += int64(len(encoded)) + 1
			if outputBytes > maxBytes || len(encoded) >= 1<<20 {
				return DatasetRowsResult{}, fmt.Errorf("excel_files_output_limit: aligned output exceeds the row or byte budget")
			}
			out.Rows = append(out.Rows, row)
		}
	}
	return out, nil
}
