package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/xlsximp"
)

// ReadExcelSnapshotRows normalizes one explicitly pinned, registered
// workbook. Unlike import, it never writes dataset state.
func (h *Handlers) ReadExcelSnapshotRows(w http.ResponseWriter, r *http.Request) {
	datasetID, ok := h.resolveDatasetForCatalog(w, r)
	if !ok || !h.requireDatasetRead(w, r, datasetID) {
		return
	}
	var body struct {
		Branch        string         `json:"branch"`
		Version       int32          `json:"version"`
		TransactionID uuid.UUID      `json:"transaction_id"`
		Path          string         `json:"path"`
		Limit         int            `json:"limit"`
		Excel         xlsximp.Config `json:"excel"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || decoder.Decode(new(json.RawMessage)) != io.EOF {
		writeJSONErr(w, http.StatusBadRequest, "dataset_excel_snapshot_invalid: invalid request body")
		return
	}
	format := strings.ToLower(strings.TrimPrefix(path.Ext(body.Path), "."))
	if strings.TrimSpace(body.Branch) == "" || body.Version < 1 || body.TransactionID == uuid.Nil ||
		(format != "xlsx" && format != "xltx" && format != "xls" && format != "xlt") || body.Limit < 1 || body.Limit > xlsximp.MaxDataRows ||
		strings.TrimSpace(body.Excel.Sheet) == "" || len(body.Excel.Mapping) == 0 {
		writeJSONErr(w, http.StatusBadRequest, "dataset_excel_snapshot_invalid: branch, version, transaction_id, Excel workbook path, row limit, sheet and mapping are required")
		return
	}
	if !validScanRequestPath(w, body.Path) {
		return
	}
	target, ok := h.resolveScanTarget(w, r, datasetID, body.Branch, body.Version)
	if !ok {
		return
	}
	if target.txn.ID != body.TransactionID {
		writeJSONErr(w, http.StatusConflict, "dataset_excel_snapshot_mismatch: version does not match transaction_id")
		return
	}
	file, err := h.Repo.GetTransactionStagedFile(r.Context(), target.txn.ID, body.Path)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to load committed workbook")
		return
	}
	if file == nil || file.Operation != models.FileOperationAdd {
		writeJSONErr(w, http.StatusNotFound, "dataset_excel_file_not_found: workbook is not registered in this snapshot")
		return
	}
	opener, ok := h.scanBackingOpener()
	if !ok {
		writeJSONErr(w, http.StatusUnprocessableEntity, "dataset_excel_backing_unsupported: workbook reads require local backing")
		return
	}
	if !h.validateScanStagedObject(w, r.Context(), datasetID, target.txn, body.Path, file) {
		return
	}
	if file.SizeBytes > xlsximp.MaxCompressedBytes {
		writeJSONErr(w, http.StatusRequestEntityTooLarge, "excel_oversized: committed workbook exceeds 32 MiB")
		return
	}
	reader, err := opener.OpenLocalObject(file.PhysicalPath)
	if err != nil {
		writeJSONErr(w, http.StatusUnprocessableEntity, "dataset_excel_file_unreadable: committed workbook has no readable bytes")
		return
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(io.LimitReader(reader, xlsximp.MaxCompressedBytes+1))
	if err != nil || int64(len(data)) != file.SizeBytes {
		writeJSONErr(w, http.StatusUnprocessableEntity, "dataset_excel_file_size_mismatch: workbook bytes do not match the committed size")
		return
	}
	result, err := xlsximp.NormalizeSnapshot(data, body.Excel, format)
	if err != nil {
		writeExcelErr(w, err)
		return
	}
	if result.RowCount > body.Limit {
		writeJSONErr(w, http.StatusRequestEntityTooLarge, "dataset_excel_row_limit_exceeded: worksheet exceeds the requested row limit")
		return
	}
	records := bytes.Split(bytes.TrimSuffix(result.NDJSON(), []byte("\n")), []byte("\n"))
	rows := make([]json.RawMessage, 0, len(records))
	for _, record := range records {
		rows = append(rows, record)
	}
	writeScanJSON(w, http.StatusOK, struct {
		DatasetID     uuid.UUID         `json:"dataset_id"`
		Branch        string            `json:"branch"`
		Version       int32             `json:"version"`
		TransactionID uuid.UUID         `json:"transaction_id"`
		Path          string            `json:"path"`
		Format        string            `json:"format"`
		Columns       []string          `json:"columns"`
		Rows          []json.RawMessage `json:"rows"`
		PhysicalRows  []int             `json:"physical_rows"`
	}{datasetID, body.Branch, body.Version, target.txn.ID, body.Path, format, result.Columns, rows, result.PhysicalRows})
}
