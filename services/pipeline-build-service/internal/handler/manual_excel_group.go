package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
)

type manualExcelSheetSelection struct {
	Sheet     string              `json:"sheet"`
	HeaderRow *int                `json:"header_row,omitempty"`
	Mapping   []manualExcelColumn `json:"mapping"`
}

type manualExcelColumn struct {
	SourceIndex int    `json:"source_index"`
	Target      string `json:"target"`
}

type manualExcelGroupSheet struct {
	manualExcelSheetSelection
	DatasetRID     string   `json:"dataset_rid"`
	RequiredFields []string `json:"required_fields,omitempty"`
}

type manualExcelGroupConfig struct {
	Worksheets      []manualExcelGroupSheet `json:"worksheets"`
	ErrorDatasetRID string                  `json:"error_dataset_rid"`
}

func parseManualExcelGroup(raw json.RawMessage, targets map[string]bool) (*manualExcelGroupConfig, error) {
	var cfg manualExcelGroupConfig
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cfg) != nil || decoder.Decode(new(json.RawMessage)) != io.EOF {
		return nil, errors.New("invalid excel_outputs configuration")
	}
	if len(cfg.Worksheets) < 2 || len(cfg.Worksheets)+1 != len(targets) {
		return nil, errors.New("excel_outputs must map two or more worksheets and one error dataset to all JobSpec outputs")
	}
	errorID, err := canonicalDatasetRIDForComparison(cfg.ErrorDatasetRID)
	if err != nil || !targets[errorID] {
		return nil, errors.New("excel_outputs error dataset must be a JobSpec output")
	}
	cfg.ErrorDatasetRID = datasetRIDPrefix + errorID
	usedTargets := map[string]bool{errorID: true}
	usedSheets := make(map[string]bool, len(cfg.Worksheets))
	for i := range cfg.Worksheets {
		sheet := &cfg.Worksheets[i]
		id, err := canonicalDatasetRIDForComparison(sheet.DatasetRID)
		if err != nil || !targets[id] || usedTargets[id] || usedSheets[sheet.Sheet] ||
			strings.TrimSpace(sheet.Sheet) == "" || len(sheet.Mapping) == 0 {
			return nil, errors.New("excel_outputs requires distinct worksheets and distinct declared output datasets")
		}
		usedSheets[sheet.Sheet], usedTargets[id] = true, true
		sheet.DatasetRID = datasetRIDPrefix + id
		columns := make(map[string]bool, len(sheet.Mapping))
		for _, col := range sheet.Mapping {
			columns[col.Target] = true
		}
		required := make(map[string]bool, len(sheet.RequiredFields))
		for _, field := range sheet.RequiredFields {
			if required[field] || !columns[field] {
				return nil, fmt.Errorf("worksheet %q required fields must be distinct mapped columns", sheet.Sheet)
			}
			required[field] = true
		}
	}
	return &cfg, nil
}

type excelGroupArtifact struct {
	result   executor.NodeResult
	schema   *datasetOutputSchema
	manifest batchedArtifactManifest
}

type manualExcelGroupRunner struct {
	dataset DatasetHTTPVersioning
	config  *manualStagedGroupConfig
}

var excelErrorColumns = []datasetOutputField{
	{Name: "source_dataset_rid", Type: "STRING"},
	{Name: "source_branch", Type: "STRING"},
	{Name: "source_version", Type: "LONG"},
	{Name: "source_transaction_id", Type: "STRING"},
	{Name: "source_path", Type: "STRING"},
	{Name: "sheet", Type: "STRING"},
	{Name: "physical_row", Type: "LONG"},
	{Name: "field", Type: "STRING"},
	{Name: "error_code", Type: "STRING"},
}

func (r manualExcelGroupRunner) Run(ctx context.Context, node executor.NodeContext) (executor.NodeResult, error) {
	var result executor.NodeResult
	if node.Node.ID != r.config.Manifest.JobSpecRID {
		return result, errors.New("unexpected Excel output JobSpec")
	}
	source := r.config.Manifest.Input
	config := r.config.Excel
	artifacts := make(map[string]excelGroupArtifact, len(config.Worksheets)+1)
	failures := make([]map[string]json.RawMessage, 0)
	remainingRows, remainingBytes := maxDatasetInputRows, int64(maxDatasetInputTotalBytes)
	for _, sheet := range config.Worksheets {
		if remainingRows == 0 || remainingBytes <= 0 {
			return result, errors.New("excel_group_input_limit: workbook exceeds the combined output group budget")
		}
		reader := DatasetServiceRowReader{BaseURL: r.dataset.BaseURL, Client: r.dataset.Client, SnapshotMaxTotalBytes: remainingBytes}
		selection, err := json.Marshal(sheet.manualExcelSheetSelection)
		if err != nil {
			return result, err
		}
		rows, err := reader.ReadDatasetRows(ctx, DatasetRowsRequest{
			DatasetRID: source.DatasetRID, Path: source.Path, Limit: remainingRows,
			Snapshot: &datasetInputSnapshot{Branch: source.Branch, Version: source.Version,
				TransactionID: source.TransactionID, Excel: selection},
		})
		if err != nil {
			return result, fmt.Errorf("worksheet %q: %w", sheet.Sheet, err)
		}
		encoded, err := json.Marshal(rows.Rows)
		if err != nil {
			return result, err
		}
		remainingRows -= len(rows.Rows)
		remainingBytes -= int64(len(encoded))
		if remainingBytes < 0 || len(rows.PhysicalRows) != len(rows.Rows) {
			return result, errors.New("excel_group_input_binding: invalid worksheet row positions or group payload exceeds the budget")
		}
		valid := make([]map[string]json.RawMessage, 0, len(rows.Rows))
		previous := 0
		for i, row := range rows.Rows {
			if rows.PhysicalRows[i] <= previous {
				return result, errors.New("excel_group_input_binding: worksheet row positions must increase")
			}
			previous = rows.PhysicalRows[i]
			admitted := true
			for _, field := range sheet.RequiredFields {
				value := bytes.TrimSpace(row[field])
				if len(value) != 0 && !bytes.Equal(value, []byte("null")) {
					continue
				}
				admitted = false
				if len(failures) == maxDatasetInputRows {
					return result, errors.New("excel_group_error_limit: too many rejected fields")
				}
				failures = append(failures, anyRowToRaw(map[string]any{
					"source_dataset_rid": source.DatasetRID, "source_branch": source.Branch,
					"source_version": source.Version, "source_transaction_id": source.TransactionID.String(),
					"source_path": source.Path, "sheet": sheet.Sheet, "physical_row": previous,
					"field": field, "error_code": "REQUIRED_VALUE_MISSING",
				}))
			}
			if admitted {
				valid = append(valid, row)
			}
		}
		if len(valid) == 0 {
			return result, fmt.Errorf("excel_group_all_rows_rejected: worksheet %q has no valid rows", sheet.Sheet)
		}
		fields := make([]datasetOutputField, 0, len(rows.Columns))
		for _, col := range rows.Columns {
			fields = append(fields, datasetOutputField{Name: col, Type: "STRING", Nullable: true})
		}
		manifest := r.config.Manifest
		manifest.Role, manifest.Worksheet = "worksheet", sheet.Sheet
		artifacts[sheet.DatasetRID] = newExcelGroupArtifact(valid, fields, manifest)
	}
	manifest := r.config.Manifest
	manifest.Role = "errors"
	artifacts[config.ErrorDatasetRID] = newExcelGroupArtifact(failures, excelErrorColumns, manifest)
	result.Metadata = map[string]any{"excel_artifacts": artifacts}
	return result, nil
}

func newExcelGroupArtifact(rows []map[string]json.RawMessage, fields []datasetOutputField, manifest batchedArtifactManifest) excelGroupArtifact {
	columns := make([]string, 0, len(fields))
	for _, field := range fields {
		columns = append(columns, field.Name)
	}
	return excelGroupArtifact{
		result: executor.NodeResult{
			OutputContentHash: manualGroupPayloadHash(encodeNDJSON(rows)),
			Metadata:          map[string]any{"data_rows": rows, "columns": columns, "rows_affected": len(rows)},
		},
		schema:   &datasetOutputSchema{Fields: fields, FileFormat: normaliseOutputFileFormat("NDJSON")},
		manifest: manifest,
	}
}
