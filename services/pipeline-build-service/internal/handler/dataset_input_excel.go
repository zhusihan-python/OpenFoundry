package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/google/uuid"
)

func excelSnapshotFormat(logicalPath string) string {
	format := strings.ToLower(strings.TrimPrefix(path.Ext(logicalPath), "."))
	switch format {
	case "xlsx", "xltx", "xls", "xlt", "xlsm", "xltm":
		return format
	}
	return ""
}

func (c batchedDatasetClient) readExcelSnapshotRows(ctx context.Context, rid string, datasetID uuid.UUID, req DatasetRowsRequest, limit int, maxBytes int64) (DatasetRowsResult, error) {
	format := excelSnapshotFormat(req.Path)
	if !validBatchedObjectPath(req.Path) || format == "" {
		return DatasetRowsResult{}, fmt.Errorf("dataset_excel_snapshot_invalid: dataset_path must identify one registered Excel workbook in a supported format")
	}
	pin := req.Snapshot
	body, err := json.Marshal(struct {
		Branch        string          `json:"branch"`
		Version       int32           `json:"version"`
		TransactionID uuid.UUID       `json:"transaction_id"`
		Path          string          `json:"path"`
		Limit         int             `json:"limit"`
		Excel         json.RawMessage `json:"excel"`
	}{pin.Branch, pin.Version, pin.TransactionID, req.Path, limit, pin.Excel})
	if err != nil {
		return DatasetRowsResult{}, err
	}
	endpoint := c.baseURL() + "/internal/datasets/" + url.PathEscape(rid) + "/files/rows:excel"
	request, err := c.newRequest(ctx, http.MethodPost, endpoint, bytes.NewReader(body), "application/json")
	if err != nil {
		return DatasetRowsResult{}, err
	}
	resp, err := c.httpClient().Do(request)
	if err != nil {
		return DatasetRowsResult{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, batchedErrorBodyLimit))
		return DatasetRowsResult{}, batchedErrorFromResponse(resp.StatusCode, detail)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return DatasetRowsResult{}, err
	}
	if int64(len(raw)) > maxBytes {
		return DatasetRowsResult{}, fmt.Errorf("dataset_input_bytes_exceeded: normalized workbook response exceeds %d bytes", maxBytes)
	}
	var reply struct {
		DatasetRowsResult
		Branch        string `json:"branch"`
		Version       int64  `json:"version"`
		TransactionID string `json:"transaction_id"`
	}
	if err := json.Unmarshal(raw, &reply); err != nil {
		return DatasetRowsResult{}, err
	}
	txn := pin.TransactionID
	if err := validateScanBinding(datasetID, batchedScanInput{Branch: pin.Branch, Version: pin.Version}, &txn, reply.DatasetID, reply.Branch, reply.TransactionID, reply.Version); err != nil {
		return DatasetRowsResult{}, err
	}
	if reply.Format != format || reply.Path != req.Path || reply.Truncated || len(reply.Rows) == 0 || len(reply.Rows) > limit {
		return DatasetRowsResult{}, fmt.Errorf("dataset_excel_snapshot_invalid: unexpected workbook response")
	}
	reply.Source = "dataset-versioning-excel-snapshot"
	reply.responseBytes = int64(len(raw))
	return reply.DatasetRowsResult, nil
}
