package handlers

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
)

const (
	rowComparePageRows       = 128
	rowCompareMaxRows        = 1024
	rowCompareMaxRowBytes    = 2 << 20
	rowCompareMaxPages       = 16
	rowCompareMaxChanges     = 100
	rowCompareMaxChangeBytes = 1 << 20

	rowComparePagedScanRows = 512
	rowComparePagedMaxRows  = 8192
	rowComparePagedMaxBytes = 8 << 20
	rowComparePagedMaxPages = 32
)

type rowCompareScanLimits struct {
	pageRows int
	maxRows  int
	maxBytes int
	maxPages int
}

type rowComparePin struct {
	Branch        string    `json:"branch"`
	Version       int32     `json:"version"`
	TransactionID uuid.UUID `json:"transaction_id"`
}

type rowCompareRequest struct {
	key    string
	base   rowComparePin
	target rowComparePin
	page   rowComparePage
}

type rowCompareCounts struct {
	Added     int `json:"added"`
	Removed   int `json:"removed"`
	Modified  int `json:"modified"`
	Unchanged int `json:"unchanged"`
}

type rowCompareChange struct {
	Key    string          `json:"key"`
	Kind   string          `json:"kind"`
	Before json.RawMessage `json:"before,omitempty"`
	After  json.RawMessage `json:"after,omitempty"`
}

type rowCompareResponse struct {
	DatasetID        uuid.UUID          `json:"dataset_id"`
	Key              string             `json:"key"`
	Base             rowComparePin      `json:"base"`
	Target           rowComparePin      `json:"target"`
	Counts           rowCompareCounts   `json:"counts"`
	Changes          []rowCompareChange `json:"changes"`
	ChangesTruncated bool               `json:"changes_truncated"`
	NextCursor       string             `json:"next_cursor,omitempty"`
}

// CompareDatasetRows compares two complete, bounded orders.v1 snapshots by a
// single explicitly chosen unique, non-null column.
func (h *Handlers) CompareDatasetRows(w http.ResponseWriter, r *http.Request) {
	datasetID, ok := h.resolveDatasetForCatalog(w, r)
	if !ok || !h.requireDatasetRead(w, r, datasetID) {
		return
	}
	request, err := parseRowCompareRequest(r.URL.RawQuery)
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	limits := rowCompareScanLimits{rowComparePageRows, rowCompareMaxRows, rowCompareMaxRowBytes, rowCompareMaxPages}
	if request.page.limit > 0 {
		request.page.scope = request.pageScope(datasetID)
		if request.page.cursor != nil && request.page.cursor.scope != request.page.scope {
			writeJSONErr(w, http.StatusBadRequest, "dataset_row_compare_cursor_mismatch: cursor belongs to another comparison")
			return
		}
		limits = rowCompareScanLimits{rowComparePagedScanRows, rowComparePagedMaxRows, rowComparePagedMaxBytes, rowComparePagedMaxPages}
	}
	base, ok := h.resolveRowComparePin(w, r, datasetID, request.base, "base")
	if !ok {
		return
	}
	target, ok := h.resolveRowComparePin(w, r, datasetID, request.target, "target")
	if !ok {
		return
	}
	baseRows, ok := h.indexRowCompareSnapshot(w, r, datasetID, request.base, base, request.key, "base", limits)
	if !ok {
		return
	}
	targetRows, ok := h.indexRowCompareSnapshot(w, r, datasetID, request.target, target, request.key, "target", limits)
	if !ok {
		return
	}
	out, err := compareRowIndexes(baseRows, targetRows, request.page)
	if err != nil {
		writeDatasetRowsError(w, err)
		return
	}
	if r.Context().Err() != nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusGatewayTimeout, "dataset_row_compare_interrupted: comparison request ended"))
		return
	}
	out.DatasetID, out.Key, out.Base, out.Target = datasetID, request.key, request.base, request.target
	if claims, present := authmw.FromContext(r.Context()); present {
		h.emitAudit(r.Context(), AuditEvent{Actor: claims.Sub.String(), Action: "files.rows_compare", DatasetRID: datasetID.String(),
			Details: map[string]any{"base": request.base, "target": request.target, "key": request.key, "counts": out.Counts},
		})
	}
	writeScanJSON(w, http.StatusOK, out)
}

func parseRowCompareRequest(rawQuery string) (rowCompareRequest, error) {
	var request rowCompareRequest
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return request, fmt.Errorf("dataset_row_compare_query_invalid: provide a key and both branch, version and transaction selections")
	}
	allowedFields := 7
	if query.Has("limit") {
		allowedFields++
	}
	if query.Has("cursor") {
		allowedFields++
	}
	if len(query) != allowedFields {
		return request, fmt.Errorf("dataset_row_compare_query_invalid: provide a key and both branch, version and transaction selections")
	}
	for _, name := range []string{"key", "base_branch", "base_version", "base_transaction", "target_branch", "target_version", "target_transaction"} {
		if values := query[name]; len(values) != 1 || strings.TrimSpace(values[0]) == "" {
			return request, fmt.Errorf("dataset_row_compare_query_invalid: %s requires exactly one value", name)
		}
	}
	if query.Has("limit") {
		limit, err := strconv.Atoi(query.Get("limit"))
		if len(query["limit"]) != 1 || err != nil || limit < 1 || limit > rowCompareMaxChanges {
			return request, fmt.Errorf("dataset_row_compare_query_invalid: limit must be a single integer from 1 to %d", rowCompareMaxChanges)
		}
		request.page.limit = limit
	}
	if query.Has("cursor") {
		values := query["cursor"]
		if request.page.limit == 0 || len(values) != 1 {
			return request, fmt.Errorf("dataset_row_compare_cursor_invalid: provide exactly one cursor with a limit")
		}
		request.page.cursor, err = parseRowCompareCursor(values[0])
		if err != nil {
			return request, err
		}
	}
	request.key = query.Get("key")
	matched := false
	for _, field := range parquetOrdersSchema().Fields {
		if field.Name == request.key {
			matched = true
			break
		}
	}
	if !matched {
		return request, fmt.Errorf("dataset_row_compare_key_invalid: select an orders.v1 column as the key")
	}
	request.base, err = parseRowComparePin(query, "base")
	if err != nil {
		return request, err
	}
	request.target, err = parseRowComparePin(query, "target")
	return request, err
}

func parseRowComparePin(query url.Values, name string) (rowComparePin, error) {
	pin := rowComparePin{Branch: strings.TrimSpace(query.Get(name + "_branch"))}
	version, err := strconv.ParseInt(query.Get(name+"_version"), 10, 32)
	if err != nil || version <= 0 {
		return pin, fmt.Errorf("dataset_row_compare_query_invalid: %s_version must be a positive integer", name)
	}
	pin.Version = int32(version)
	pin.TransactionID, err = uuid.Parse(query.Get(name + "_transaction"))
	if err != nil || pin.TransactionID == uuid.Nil {
		return pin, fmt.Errorf("dataset_row_compare_query_invalid: %s_transaction must be a transaction UUID", name)
	}
	return pin, nil
}

func (h *Handlers) resolveRowComparePin(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, pin rowComparePin, name string) (*scanTarget, bool) {
	target, ok := h.resolveScanTarget(w, r, datasetID, pin.Branch, pin.Version)
	if !ok {
		return nil, false
	}
	if target.txn.ID != pin.TransactionID {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity,
			"dataset_row_compare_transaction_mismatch: %s transaction does not match its selected version", name))
		return nil, false
	}
	return target, true
}

func (h *Handlers) indexRowCompareSnapshot(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, pin rowComparePin, target *scanTarget, key, name string, limits rowCompareScanLimits) (map[string]json.RawMessage, bool) {
	parquet, other, _, err := h.Repo.TransactionStagedFileFormats(r.Context(), target.txn.ID)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to load staged file formats")
		return nil, false
	}
	if !parquet || other {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity,
			"dataset_row_compare_format_unsupported: %s snapshot must contain orders.v1 Parquet files", name))
		return nil, false
	}
	rows := make(map[string]json.RawMessage)
	bytesRetained := 0
	cursor := ""
	for range limits.maxPages {
		if r.Context().Err() != nil {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusGatewayTimeout, "dataset_row_compare_interrupted: comparison request ended"))
			return nil, false
		}
		page, ok := h.scanParquetAllFilesPage(w, r, datasetID, pin.Branch, target.branch, target.txn, pin.Version, limits.pageRows, nil, cursor)
		if !ok {
			return nil, false
		}
		if len(page.Rows) > limits.maxRows-len(rows) {
			break
		}
		// The strict Parquet scanner emits compact records. Normalization quotes
		// the LONG column and may escape text, so the raw row lengths are a
		// lower bound on the retained bytes.
		if page.PageBytes-int64(len(page.Rows)) > int64(limits.maxBytes-bytesRetained) {
			break
		}
		_, normalized, err := pinnedPreviewRows(page)
		if err != nil {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity,
				"dataset_row_compare_invalid_rows: %s snapshot has invalid orders.v1 rows", name))
			return nil, false
		}
		if err := indexCompareRows(rows, normalized, key, name, &bytesRetained, limits); err != nil {
			writeDatasetRowsError(w, err)
			return nil, false
		}
		if page.NextCursor == nil {
			return rows, true
		}
		if *page.NextCursor == "" || *page.NextCursor == cursor {
			writeJSONErr(w, http.StatusInternalServerError, "dataset_row_compare_scan_stalled: snapshot scan did not advance")
			return nil, false
		}
		cursor = *page.NextCursor
	}
	writeDatasetRowsError(w, limits.exceeded(name))
	return nil, false
}

func indexCompareRows(index map[string]json.RawMessage, rows []json.RawMessage, key, name string, bytesRetained *int, limits rowCompareScanLimits) error {
	for _, row := range rows {
		if len(row) > limits.maxBytes-*bytesRetained {
			return limits.exceeded(name)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(row, &fields); err != nil {
			return datasetRowsErr(http.StatusUnprocessableEntity, "dataset_row_compare_invalid_rows: %s snapshot has invalid orders.v1 rows", name)
		}
		raw := fields[key]
		if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
			return datasetRowsErr(http.StatusUnprocessableEntity, "dataset_row_compare_null_key: %s snapshot contains a missing or null key", name)
		}
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return datasetRowsErr(http.StatusUnprocessableEntity, "dataset_row_compare_invalid_rows: %s snapshot has an invalid key value", name)
		}
		if _, duplicate := index[value]; duplicate {
			return datasetRowsErr(http.StatusUnprocessableEntity, "dataset_row_compare_duplicate_key: %s snapshot contains a duplicate key", name)
		}
		index[value] = row
		*bytesRetained += len(row)
	}
	return nil
}

func (limits rowCompareScanLimits) exceeded(name string) error {
	return datasetRowsErr(http.StatusUnprocessableEntity,
		"dataset_row_compare_limit_exceeded: %s snapshot exceeds the comparison budget (%d rows, %d bytes or %d scan pages)",
		name, limits.maxRows, limits.maxBytes, limits.maxPages)
}

func compareRowIndexes(base, target map[string]json.RawMessage, page rowComparePage) (rowCompareResponse, error) {
	keys := make([]string, 0, len(base)+len(target))
	for key := range base {
		keys = append(keys, key)
	}
	for key := range target {
		if _, exists := base[key]; !exists {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)

	out := rowCompareResponse{Changes: []rowCompareChange{}}
	paged := page.limit > 0
	limit := rowCompareMaxChanges
	if paged {
		limit = page.limit
	}
	afterCursor := page.cursor == nil
	hasMore := false
	changeBytes := 2 // Array brackets.
	for _, key := range keys {
		before, after := base[key], target[key]
		change := rowCompareChange{Key: key, Before: before, After: after}
		switch {
		case before == nil:
			change.Kind = "added"
			out.Counts.Added++
		case after == nil:
			change.Kind = "removed"
			out.Counts.Removed++
		case !bytes.Equal(before, after):
			change.Kind = "modified"
			out.Counts.Modified++
		default:
			out.Counts.Unchanged++
			continue
		}
		if !paged && (hasMore || len(out.Changes) == limit) {
			hasMore = true
			continue
		}
		detail, err := json.Marshal(change)
		if err != nil {
			return rowCompareResponse{}, datasetRowsErr(http.StatusInternalServerError, "failed to encode compared rows")
		}
		if paged && len(detail) > rowCompareMaxChangeBytes-2 {
			return rowCompareResponse{}, datasetRowsErr(http.StatusUnprocessableEntity,
				"dataset_row_compare_change_too_large: a single change exceeds the comparison detail budget")
		}
		if !afterCursor {
			afterCursor = sha256.Sum256([]byte(key)) == page.cursor.after
			continue
		}
		needed := len(detail)
		if len(out.Changes) > 0 {
			needed++ // Array comma.
		}
		if hasMore || len(out.Changes) == limit || needed > rowCompareMaxChangeBytes-changeBytes {
			hasMore = true
			continue
		}
		out.Changes = append(out.Changes, change)
		changeBytes += needed
	}
	if !afterCursor {
		return rowCompareResponse{}, datasetRowsErr(http.StatusBadRequest,
			"dataset_row_compare_cursor_invalid: cursor does not resolve to a changed key")
	}
	if paged && hasMore {
		last := out.Changes[len(out.Changes)-1]
		out.NextCursor = (rowCompareCursor{scope: page.scope, after: sha256.Sum256([]byte(last.Key))}).encode()
	}
	out.ChangesTruncated = !paged && hasMore
	return out, nil
}
