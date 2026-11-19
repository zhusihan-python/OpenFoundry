package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

type pinnedPreviewColumn struct {
	Name      string                 `json:"name"`
	FieldType models.SchemaFieldType `json:"field_type,omitempty"`
	Nullable  *bool                  `json:"nullable,omitempty"`
}

type pinnedPreviewResponse struct {
	DatasetID     string                `json:"dataset_id"`
	Branch        string                `json:"branch"`
	Version       int32                 `json:"version"`
	TransactionID string                `json:"transaction_id"`
	Format        string                `json:"format"`
	SchemaVersion string                `json:"schema_version,omitempty"`
	Schema        *models.DatasetSchema `json:"schema,omitempty"`
	Columns       []pinnedPreviewColumn `json:"columns"`
	Rows          []json.RawMessage     `json:"rows"`
	RowCount      int                   `json:"row_count"`
	Limit         int                   `json:"limit"`
	NextCursor    *string               `json:"next_cursor"`
}

func (h *Handlers) previewPinnedDataset(w http.ResponseWriter, r *http.Request, query url.Values) {
	datasetID, ok := h.resolveDatasetForCatalog(w, r)
	if !ok || !h.requireDatasetRead(w, r, datasetID) {
		return
	}
	for key, values := range query {
		switch key {
		case "branch", "version", "limit", "cursor":
			if len(values) == 1 && strings.TrimSpace(values[0]) != "" {
				continue
			}
			writeJSONErr(w, http.StatusBadRequest, key+" must appear once with a value for versioned preview")
		default:
			writeJSONErr(w, http.StatusBadRequest, "unsupported option for versioned preview: "+key)
		}
		return
	}
	version, err := strconv.ParseInt(strings.TrimSpace(query.Get("version")), 10, 32)
	if err != nil || version < 1 {
		writeJSONErr(w, http.StatusBadRequest, "version must be a positive integer for versioned preview")
		return
	}
	branch := strings.TrimSpace(query.Get("branch"))
	if branch == "" {
		writeJSONErr(w, http.StatusBadRequest, "branch is required for versioned preview")
		return
	}
	limit, ok := parseScanLimit(w, query)
	if !ok {
		return
	}
	target, ok := h.resolveScanTarget(w, r, datasetID, branch, int32(version))
	if !ok {
		return
	}
	if h.previewPinnedCSV(w, r, datasetID, branch, target, int32(version), limit, query.Get("cursor")) {
		return
	}
	page, ok := h.scanAllFilesPage(w, r, datasetID, branch, target.branch, target.txn, int32(version), limit, nil, query.Get("cursor"))
	if !ok {
		return
	}
	columns, rows, err := pinnedPreviewRows(page)
	if err != nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_preview_invalid_rows: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, pinnedPreviewResponse{
		DatasetID: page.DatasetID, Branch: page.Branch, Version: page.Version,
		TransactionID: page.TransactionID, Format: page.Format, SchemaVersion: page.SchemaVersion,
		Schema: page.Schema, Columns: columns, Rows: rows, RowCount: page.RowCount, Limit: limit,
		NextCursor: page.NextCursor,
	})
}

func pinnedPreviewRows(page datasetScanAllFilesResponse) ([]pinnedPreviewColumn, []json.RawMessage, error) {
	if page.Format == "parquet" {
		if page.Schema == nil || page.SchemaVersion != stagedParquetSchemaVersion {
			return nil, nil, errors.New("unsupported Parquet preview schema")
		}
		columns := make([]pinnedPreviewColumn, 0, len(page.Schema.Fields))
		for _, field := range page.Schema.Fields {
			columns = append(columns, pinnedPreviewColumn{Name: field.Name, FieldType: field.Type, Nullable: &field.Nullable})
		}

		rows := make([]json.RawMessage, 0, len(page.Rows))
		for _, raw := range page.Rows {
			var row map[string]json.RawMessage
			if err := json.Unmarshal(raw, &row); err != nil {
				return nil, nil, errors.New("invalid Parquet preview row")
			}
			// Browser JSON numbers lose precision beyond 2^53−1. The preview
			// quotes this one int64 field without changing the raw scan rows.
			orderID, err := strconv.ParseInt(string(row["order_id"]), 10, 64)
			if err != nil {
				return nil, nil, errors.New("invalid orders.v1 order_id")
			}
			row["order_id"] = json.RawMessage(strconv.Quote(strconv.FormatInt(orderID, 10)))
			encoded, err := json.Marshal(row)
			if err != nil {
				return nil, nil, errors.New("invalid Parquet preview row")
			}
			rows = append(rows, encoded)
		}
		return columns, rows, nil
	}

	columnsByName := make(map[string]struct{})
	for _, raw := range page.Rows {
		var row map[string]json.RawMessage
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, nil, errors.New("invalid NDJSON preview row")
		}
		for name := range row {
			columnsByName[name] = struct{}{}
		}
	}
	names := make([]string, 0, len(columnsByName))
	for name := range columnsByName {
		names = append(names, name)
	}
	slices.Sort(names)
	columns := make([]pinnedPreviewColumn, 0, len(names))
	for _, name := range names {
		columns = append(columns, pinnedPreviewColumn{Name: name})
	}
	return columns, page.Rows, nil
}
