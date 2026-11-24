// WP3.3 increment 2 (predicate + fixed-snapshot multi-file scan): the s2
// cursor and the two scan modes that use it.
//
// Mode 1 (path + predicate) filters one version-pinned NDJSON file through
// a strictly parsed top-level scalar equality (see file_rows_scan_predicate.go).
// Mode 2 (all_files=true[, predicate]) walks EVERY NDJSON ADD file the
// version's committed SNAPSHOT transaction registered, in the database's
// stable logical-path order, streaming each file record by record.
//
// Both modes share the increment-1 resolution chain (re-run on every page,
// no current-head fallback) and the same per-file validation (server-named
// transaction object, ADD, ndjson format, size equality on open) — an
// unsupported file in the transaction fails the scan explicitly; nothing
// is silently skipped.
//
// Pages bind three budgets: matched rows (limit), cumulative raw bytes
// consumed across files (datasetScanMaxPageBytes) and backing opens
// (scanMaxFilesPerPage). When a page ends on a file boundary it issues the
// NEXT UNCONSUMED file at offset 0 (taken from the keyset batch's lookahead
// row) — never an end-of-file position, which would misrepresent the file
// end as an in-file continuation and could collide with the record-boundary
// check on files without a trailing newline.

package handlers

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// scanCursorV2Magic versions the s2 cursor wire format.
const scanCursorV2Magic = "s2"

// scanCursorV2 modes.
const (
	scanCursorV2ModePath     byte = 1 // predicate over one explicit path
	scanCursorV2ModeAllFiles byte = 2 // every file of the transaction
)

// scanMaxFilesPerPage caps how many backing objects one all_files page may
// open. The keyset batch is fetched with this + 1 rows: a page opens at
// most scanMaxFilesPerPage files (batch rows 0..scanMaxFilesPerPage-1), so
// whenever more files exist the batch still holds the lookahead row the
// boundary stop needs, and a batch shorter than the limit proves there are
// no further files.
const scanMaxFilesPerPage = 16

// scanCursorV2 is the page position plus the binding it was issued under,
// for predicate and all_files scans. Like the s1 cursor it is opaque,
// bounded and deliberately NOT signed: a forged cursor can only move the
// read position within the freshly re-resolved, re-authorized file set
// (offset bounds, record-boundary and per-file validation all re-run), and
// the logical-path/offset checks keep that position record-aligned. It
// carries no storage keys — physical keys are re-derived and validated on
// every page.
//
// The predicate is bound as a presence flag plus the SHA-256 of its
// canonical encoding, so continuations compare business semantics: equal
// values spelled differently ("1" vs "1.0" vs "1e0") keep paging, any
// semantic change is a mismatch. The digest (not the predicate itself)
// rides in the cursor to keep it small.
type scanCursorV2 struct {
	datasetID uuid.UUID
	branchID  uuid.UUID
	version   uint32
	txnID     uuid.UUID
	mode      byte
	predFlag  byte
	predSum   [32]byte
	path      string // mode 1: the bound logical path
	file      string // mode 2: the file the offset belongs to ("" = start)
	offset    int64
}

// Binary layout of the s2 cursor:
//
//	 0..1   magic "s2"
//	 2..17  datasetID
//	18..33  branchID
//	34..37  version (u32 LE)
//	38..53  txnID
//	54      mode
//	55      predicate presence flag
//	56..87  predicate digest (SHA-256, zeros when absent)
//	88..89  location length (u16 LE)
//	90..    location bytes (mode 1: path; mode 2: file)
//	last 8  offset (u64 LE)
//
// The fixed prefix is 90 bytes; the total is 98+len(location), at most
// 98+512 bytes ≈ 820 base64 characters — inside the 1024-character bound.
const scanCursorV2PrefixLen = 90

func encodeScanCursorV2(c scanCursorV2) string {
	return encodeScanCursorToken(c, scanCursorV2Magic)
}

func encodeScanCursorToken(c scanCursorV2, magic string) string {
	location := c.path
	if c.mode == scanCursorV2ModeAllFiles {
		location = c.file
	}
	buf := make([]byte, 0, scanCursorV2PrefixLen+len(location)+8)
	buf = append(buf, magic...)
	buf = append(buf, c.datasetID[:]...)
	buf = append(buf, c.branchID[:]...)
	buf = binary.LittleEndian.AppendUint32(buf, c.version)
	buf = append(buf, c.txnID[:]...)
	buf = append(buf, c.mode, c.predFlag)
	buf = append(buf, c.predSum[:]...)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(location)))
	buf = append(buf, location...)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(c.offset))
	return base64.RawURLEncoding.EncodeToString(buf)
}

// decodeScanCursorV2 strictly decodes an s2 cursor: exact length, no
// trailing bytes, bounded fields, consistent mode/flag invariants.
func decodeScanCursorV2(raw string) (scanCursorV2, error) {
	return decodeScanCursorToken(raw, scanCursorV2Magic)
}

func decodeScanCursorToken(raw, magic string) (scanCursorV2, error) {
	var zero scanCursorV2
	if len(raw) > 1024 {
		return zero, fmt.Errorf("cursor too long")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return zero, fmt.Errorf("cursor encoding invalid")
	}
	if len(decoded) < scanCursorV2PrefixLen+8 || string(decoded[:2]) != magic {
		return zero, fmt.Errorf("unrecognized cursor")
	}
	locationLen := int(binary.LittleEndian.Uint16(decoded[88:90]))
	if len(decoded) != scanCursorV2PrefixLen+locationLen+8 {
		return zero, fmt.Errorf("cursor length mismatch")
	}
	c := scanCursorV2{
		version:  binary.LittleEndian.Uint32(decoded[34:38]),
		mode:     decoded[54],
		predFlag: decoded[55],
		offset:   int64(binary.LittleEndian.Uint64(decoded[90+locationLen:])),
	}
	copy(c.datasetID[:], decoded[2:18])
	copy(c.branchID[:], decoded[18:34])
	copy(c.txnID[:], decoded[38:54])
	copy(c.predSum[:], decoded[56:88])
	location := string(decoded[90 : 90+locationLen])
	switch c.mode {
	case scanCursorV2ModePath:
		if location == "" || len(location) > scanCursorMaxPath {
			return zero, fmt.Errorf("cursor path out of bounds")
		}
		c.path = location
	case scanCursorV2ModeAllFiles:
		if len(location) > scanCursorMaxPath {
			return zero, fmt.Errorf("cursor file out of bounds")
		}
		if location == "" && c.offset != 0 {
			return zero, fmt.Errorf("cursor values out of range")
		}
		c.file = location
	default:
		return zero, fmt.Errorf("unrecognized cursor mode")
	}
	if c.predFlag > 1 || (c.predFlag == 0) != (c.predSum == [32]byte{}) {
		return zero, fmt.Errorf("cursor predicate binding invalid")
	}
	if c.version == 0 || c.offset < 0 {
		return zero, fmt.Errorf("cursor values out of range")
	}
	return c, nil
}

// matchScanCursorBinding verifies the identity binding of a decoded cursor
// against the freshly re-resolved request. The mode/predicate checks are
// the caller's to extend (mode 1 also binds the path).
func scanCursorBindingMismatch(cursor scanCursorV2, datasetID uuid.UUID, branchID uuid.UUID, version int32, txnID uuid.UUID, predFlag byte, predSum [32]byte) string {
	switch {
	case cursor.datasetID != datasetID:
		return "dataset_scan_cursor_mismatch: cursor belongs to another dataset"
	case cursor.branchID != branchID || cursor.version != uint32(version):
		return "dataset_scan_cursor_mismatch: cursor belongs to another branch or version"
	case cursor.txnID != txnID:
		return "dataset_scan_cursor_mismatch: cursor belongs to another transaction"
	case cursor.predFlag != predFlag || cursor.predSum != predSum:
		return "dataset_scan_cursor_mismatch: cursor belongs to another predicate"
	}
	return ""
}

// datasetScanAllFilesResponse is the mode-2 page. It reports only what this
// page produced — no whole-version sums, no duplicated statistics.
type datasetScanAllFilesResponse struct {
	DatasetID     string                `json:"dataset_id"`
	Branch        string                `json:"branch"`
	Version       int32                 `json:"version"`
	TransactionID string                `json:"transaction_id"`
	Format        string                `json:"format"`
	SchemaVersion string                `json:"schema_version,omitempty"`
	Schema        *models.DatasetSchema `json:"schema,omitempty"`
	Columns       []pinnedPreviewColumn `json:"columns,omitempty"`
	Path          string                `json:"path,omitempty"`
	Mode          string                `json:"mode"`
	Rows          []json.RawMessage     `json:"rows"`
	NextCursor    *string               `json:"next_cursor"`
	RowCount      int                   `json:"row_count"`
	PageBytes     int64                 `json:"page_bytes"`
	FilesAccessed int                   `json:"files_accessed"`
}

// scanPathWithPredicate serves mode 1: one explicit logical path, streamed
// through the eq predicate. The response shape matches the s1 scan; rows
// carry only the matching records, while page_bytes still counts every raw
// byte consumed (skipped blank and non-matching records included).
func (h *Handlers) scanPathWithPredicate(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, branchName string, branchRow *models.RuntimeBranch, txn *models.RuntimeTransaction, version int32, logical string, limit int, pred *scanPredicate, rawCursor string) {
	if datasetRowsFormat(logical) != "ndjson" {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_format_unsupported: rows:scan supports ndjson/jsonl files, got %s", logical))
		return
	}
	staged, err := h.Repo.GetTransactionStagedFile(r.Context(), txn.ID, logical)
	if err != nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusInternalServerError, "failed to load staged file"))
		return
	}
	if staged == nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusNotFound, "file not registered in dataset version"))
		return
	}
	if staged.Operation != "" && staged.Operation != models.FileOperationAdd {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_scan_file_operation_unsupported: %s is staged as %s, only ADD is readable", logical, staged.Operation))
		return
	}
	opener, backingOK := h.scanBackingOpener()
	if !backingOK {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_rows_backing_driver_unsupported: rows:scan requires the local backing filesystem"))
		return
	}
	if !h.validateScanStagedObject(w, r.Context(), datasetID, txn, logical, staged) {
		return
	}

	predFlag, predSum := scanPredicateDigest(pred)
	offset := int64(0)
	if rawCursor != "" {
		cursor, cursorErr := decodeScanCursorV2(rawCursor)
		if cursorErr != nil {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: %v", cursorErr))
			return
		}
		if mismatch := scanCursorBindingMismatch(cursor, datasetID, branchRow.ID, version, txn.ID, predFlag, predSum); mismatch != "" {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "%s", mismatch))
			return
		}
		if cursor.mode != scanCursorV2ModePath || cursor.path != logical {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_mismatch: cursor belongs to another scan mode or path"))
			return
		}
		if cursor.offset > staged.SizeBytes || (cursor.offset == staged.SizeBytes && cursor.offset > 0) {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: offset %d is not a readable position in a %d byte file", cursor.offset, staged.SizeBytes))
			return
		}
		offset = cursor.offset
	}

	seg, scanErr := scanNDJSONSegment(opener, staged.PhysicalPath, staged.SizeBytes, offset, limit, datasetScanMaxPageBytes, pred)
	if scanErr != nil {
		writeDatasetRowsError(w, scanErr)
		return
	}
	resp := datasetFileScanResponse{
		DatasetID: datasetID.String(), Branch: branchName, Version: version,
		TransactionID: txn.ID.String(), Path: logical, Format: "ndjson",
		Rows: seg.rows, RowCount: len(seg.rows),
		PageBytes: seg.consumed, SizeBytes: staged.SizeBytes,
	}
	if staged.SizeBytes == 0 {
		resp.Schema = emptyStagedNDJSONSchema(txn.Metadata)
	}
	if !seg.fileDone {
		next := encodeScanCursorV2(scanCursorV2{
			datasetID: datasetID, branchID: branchRow.ID, version: uint32(version),
			txnID: txn.ID, mode: scanCursorV2ModePath, predFlag: predFlag, predSum: predSum,
			path: logical, offset: seg.endOffset,
		})
		resp.NextCursor = &next
	}
	h.emitScanModeAudit(r, datasetID, branchName, version, logical, "path", limit, resp.RowCount, resp.NextCursor != nil)
	writeScanJSON(w, http.StatusOK, resp)
}

// scanAllFiles serves mode 2: every NDJSON ADD file of the transaction in
// stable logical-path order, streamed record by record under the shared
// page budgets. File order is exactly what the keyset query returns — the
// handler never re-sorts.
//
// Progress invariant: every issued cursor differs from the received one in
// (path order, offset). A page starts at its cursor file with FULL budgets,
// and the record cap (1MiB) is below the byte budget (8MiB), so the page's
// first segment always consumes at least one record or finishes its file;
// later files in the same page are strictly greater in path order. A file
// entered mid-page whose first record exceeds the remaining byte budget is
// deferred whole (cursor at that file, offset 0) — still an advance.
func (h *Handlers) scanAllFiles(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, branchName string, branchRow *models.RuntimeBranch, txn *models.RuntimeTransaction, version int32, limit int, pred *scanPredicate, rawCursor string) {
	resp, ok := h.scanAllFilesPage(w, r, datasetID, branchName, branchRow, txn, version, limit, pred, rawCursor)
	if !ok {
		return
	}
	h.emitScanModeAudit(r, datasetID, branchName, version, "*", "all_files", limit, resp.RowCount, resp.NextCursor != nil)
	writeScanJSON(w, http.StatusOK, resp)
}

// scanAllFilesPage reads an already resolved, authorized SNAPSHOT. It writes
// only errors so versioned preview can reuse the exact same read boundaries.
func (h *Handlers) scanAllFilesPage(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, branchName string, branchRow *models.RuntimeBranch, txn *models.RuntimeTransaction, version int32, limit int, pred *scanPredicate, rawCursor string) (datasetScanAllFilesResponse, bool) {
	parquet, other, allDelimited, _, err := h.Repo.TransactionStagedFileFormats(r.Context(), txn.ID)
	if err != nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusInternalServerError, "failed to load staged file formats"))
		return datasetScanAllFilesResponse{}, false
	}
	if allDelimited {
		return h.scanCSVPage(w, r, csvScanRequest{
			datasetID: datasetID, branch: branchName, target: scanTarget{branch: branchRow, txn: txn},
			version: version, limit: limit, pred: pred, rawCursor: rawCursor,
		})
	}
	if parquet && other {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_scan_mixed_formats: rows:scan all_files requires one file format"))
		return datasetScanAllFilesResponse{}, false
	}
	if parquet {
		return h.scanParquetAllFilesPage(w, r, datasetID, branchName, branchRow, txn, version, limit, pred, rawCursor)
	}
	return h.scanNDJSONAllFilesPage(w, r, datasetID, branchName, branchRow, txn, version, limit, pred, rawCursor)
}

func (h *Handlers) scanNDJSONAllFilesPage(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, branchName string, branchRow *models.RuntimeBranch, txn *models.RuntimeTransaction, version int32, limit int, pred *scanPredicate, rawCursor string) (datasetScanAllFilesResponse, bool) {
	predFlag, predSum := scanPredicateDigest(pred)
	fromPath := ""
	startOffset := int64(0)
	if rawCursor != "" {
		cursor, cursorErr := decodeScanCursorV2(rawCursor)
		if cursorErr != nil {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: %v", cursorErr))
			return datasetScanAllFilesResponse{}, false
		}
		if mismatch := scanCursorBindingMismatch(cursor, datasetID, branchRow.ID, version, txn.ID, predFlag, predSum); mismatch != "" {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "%s", mismatch))
			return datasetScanAllFilesResponse{}, false
		}
		if cursor.mode != scanCursorV2ModeAllFiles {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_mismatch: cursor belongs to another scan mode"))
			return datasetScanAllFilesResponse{}, false
		}
		fromPath = cursor.file
		startOffset = cursor.offset
	}

	opener, backingOK := h.scanBackingOpener()
	if !backingOK {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_rows_backing_driver_unsupported: rows:scan requires the local backing filesystem"))
		return datasetScanAllFilesResponse{}, false
	}

	// One keyset batch per page. The +1 window holds every file the page
	// could open (at most scanMaxFilesPerPage) plus the lookahead row a
	// boundary stop needs; a batch shorter than the limit proves there are
	// no further files.
	batch, err := h.Repo.ListTransactionStagedFilesFrom(r.Context(), txn.ID, fromPath, scanMaxFilesPerPage+1)
	if err != nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusInternalServerError, "failed to load staged files"))
		return datasetScanAllFilesResponse{}, false
	}
	// Validate EVERY batch row's logical path — including the lookahead row
	// this page may hand a cursor to — BEFORE any key is derived or any
	// object opened. Legacy metadata-only commits could register paths that
	// are not canonical object keys; such a path must fail the scan here
	// rather than become a cursor the next page would reject, or reach
	// path.Join, which would fold dot segments and blur the transaction
	// boundary the expectedKey check relies on.
	for _, file := range batch {
		if !validScanStagedPath(file.LogicalPath) {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity,
				"dataset_scan_file_path_invalid: %q is not a canonical logical path readable by rows:scan", file.LogicalPath))
			return datasetScanAllFilesResponse{}, false
		}
	}
	if fromPath != "" {
		if len(batch) == 0 || batch[0].LogicalPath != fromPath {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: cursor file is not registered in the dataset version"))
			return datasetScanAllFilesResponse{}, false
		}
		size := batch[0].SizeBytes
		if startOffset > size || (startOffset == size && startOffset > 0) {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: offset %d is not a readable position in a %d byte file", startOffset, size))
			return datasetScanAllFilesResponse{}, false
		}
	}

	resp := datasetScanAllFilesResponse{
		DatasetID: datasetID.String(), Branch: branchName, Version: version,
		TransactionID: txn.ID.String(), Format: "ndjson", Mode: "all_files",
		Rows: []json.RawMessage{},
	}
	var nextCursor scanCursorV2
	hasNext := false
	for i := 0; i < len(batch); i++ {
		file := batch[i]
		if resp.FilesAccessed >= scanMaxFilesPerPage || len(resp.Rows) >= limit || resp.PageBytes >= datasetScanMaxPageBytes {
			// A budget ran out on a file boundary: this file is entirely
			// unconsumed, so the continuation starts at its beginning.
			// (Unreachable at i==0 — a page begins with full budgets.)
			nextCursor = scanCursorV2{mode: scanCursorV2ModeAllFiles, file: file.LogicalPath}
			hasNext = true
			break
		}
		offset := int64(0)
		if i == 0 && file.LogicalPath == fromPath {
			offset = startOffset
		}
		if !h.validateScanStagedFileAll(w, r.Context(), datasetID, txn, file.LogicalPath, &file) {
			return datasetScanAllFilesResponse{}, false
		}
		seg, scanErr := scanNDJSONSegment(opener, file.PhysicalPath, file.SizeBytes, offset,
			limit-len(resp.Rows), datasetScanMaxPageBytes-resp.PageBytes, pred)
		if scanErr != nil {
			writeDatasetRowsError(w, scanErr)
			return datasetScanAllFilesResponse{}, false
		}
		resp.FilesAccessed++
		resp.Rows = append(resp.Rows, seg.rows...)
		resp.PageBytes += seg.consumed
		if !seg.fileDone {
			// Stopped inside the file on a row/byte budget, or deferred its
			// first record: the position strictly advanced either way.
			nextCursor = scanCursorV2{mode: scanCursorV2ModeAllFiles, file: file.LogicalPath, offset: seg.endOffset}
			hasNext = true
			break
		}
	}
	// Reaching here means every batch row was consumed without a budget
	// stop. The loop opens at most scanMaxFilesPerPage of the
	// scanMaxFilesPerPage+1 rows (row scanMaxFilesPerPage is only ever a
	// lookahead), so this can only happen when the batch was shorter than
	// its limit — there are no further files. Final page.

	if hasNext {
		nextCursor.datasetID = datasetID
		nextCursor.branchID = branchRow.ID
		nextCursor.version = uint32(version)
		nextCursor.txnID = txn.ID
		nextCursor.predFlag = predFlag
		nextCursor.predSum = predSum
		next := encodeScanCursorV2(nextCursor)
		resp.NextCursor = &next
	}
	resp.RowCount = len(resp.Rows)
	if !hasNext && resp.FilesAccessed == 1 && resp.PageBytes == 0 {
		resp.Schema = emptyStagedNDJSONSchema(txn.Metadata)
	}
	return resp, true
}

// validScanStagedPath accepts logical paths that are canonical object keys
// and always cursor-encodable: non-empty, within the cursor path bound,
// valid UTF-8 without NUL, and safeObjectKey-canonical (no "..", "//", or
// edge slashes). PostgreSQL TEXT already rules out NUL and invalid UTF-8;
// the checks stay as defense in depth for the fake store and future
// sources.
func validScanStagedPath(logical string) bool {
	if logical == "" || len(logical) > scanCursorMaxPath {
		return false
	}
	if strings.ContainsRune(logical, 0) || !utf8.ValidString(logical) {
		return false
	}
	return safeObjectKey(logical)
}

// validateScanStagedObject checks that one staged row is the server-named
// transaction object (the version binding includes the object bytes).
func (h *Handlers) validateScanStagedObject(w http.ResponseWriter, ctx context.Context, datasetID uuid.UUID, txn *models.RuntimeTransaction, logical string, staged *models.StageTransactionFile) bool {
	expectedKey := outputCommitObjectKey(h.BackingFS.BaseDirectory(), datasetID, txn.ID, logical)
	if staged.PhysicalPath != expectedKey {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_scan_object_not_transaction_scoped: %s is not the server-named output object of transaction %s", logical, txn.ID))
		return false
	}
	if err := h.requireDatasetOwnedObjectKey(ctx, datasetID, staged.PhysicalPath); err != nil {
		writeDatasetRowsError(w, err)
		return false
	}
	return true
}

// validateScanStagedFileAll applies the full per-file chain for all_files:
// format, operation and server-owned object key. Any unsupported file fails
// the whole scan — nothing is skipped.
func (h *Handlers) validateScanStagedFileAll(w http.ResponseWriter, ctx context.Context, datasetID uuid.UUID, txn *models.RuntimeTransaction, logical string, staged *models.StageTransactionFile) bool {
	if isDelimitedFile(logical) {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_scan_mixed_formats: CSV scan requires a CSV or TSV snapshot"))
		return false
	}
	if datasetRowsFormat(logical) != "ndjson" {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_format_unsupported: rows:scan supports ndjson/jsonl files, got %s", logical))
		return false
	}
	if staged.Operation != "" && staged.Operation != models.FileOperationAdd {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_scan_file_operation_unsupported: %s is staged as %s, only ADD is readable", logical, staged.Operation))
		return false
	}
	return h.validateScanStagedObject(w, ctx, datasetID, txn, logical, staged)
}

// emitScanModeAudit is emitScanAudit with the scan mode recorded; the
// all_files mode reports path "*" since no single path is bound.
func (h *Handlers) emitScanModeAudit(r *http.Request, datasetID uuid.UUID, branch string, version int32, path string, mode string, limit int, rowCount int, hasCursor bool) {
	actor := "anonymous"
	if claims, ok := authmw.FromContext(r.Context()); ok {
		actor = claims.Sub.String()
	}
	h.emitAudit(r.Context(), AuditEvent{Actor: actor, Action: "files.rows_scan", DatasetRID: datasetID.String(), Details: map[string]any{
		"path": path, "branch": branch, "version": version, "limit": limit,
		"row_count": rowCount, "has_next_cursor": hasCursor, "mode": mode,
	}})
}

// scanSegment is the result of scanning one file under row/byte budgets.
type scanSegment struct {
	rows      []json.RawMessage
	consumed  int64
	endOffset int64
	fileDone  bool
}

// scanNDJSONSegment streams one object from offset under a row budget, a
// raw-byte budget and an optional predicate. Semantics match the increment-1
// single-file scanner: records are returned verbatim, blank lines are
// skipped but counted, oversized or non-object records fail explicitly, and
// a record that would overflow the byte budget is deferred to the next
// page. When a segment starts with the full page byte budget (as every
// page's first segment does) it always consumes at least one record or
// finishes the file, because the record cap is below the page cap — that is
// the page-level progress guarantee. fileDone reports that every byte of
// the object has been consumed. See scanNDJSONPage for the memory bounds.
func scanNDJSONSegment(opener localObjectOpener, key string, expectedSize int64, offset int64, rowLimit int, byteLimit int64, pred *scanPredicate) (scanSegment, error) {
	reader, err := opener.OpenLocalObject(key)
	if err != nil {
		if os.IsNotExist(err) {
			return scanSegment{}, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_bytes_missing: the versioned file has no readable backing bytes at its registered storage path")
		}
		return scanSegment{}, datasetRowsErr(http.StatusInternalServerError, "failed to open versioned file bytes")
	}
	defer func() { _ = reader.Close() }()

	actualSize, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return scanSegment{}, datasetRowsErr(http.StatusInternalServerError, "failed to measure versioned file")
	}
	if actualSize != expectedSize {
		return scanSegment{}, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_size_mismatch: versioned file is %d bytes on disk but %d bytes in its committed file record", actualSize, expectedSize)
	}

	if offset > 0 {
		// A continuation offset must sit on a record boundary: the byte
		// before it has to be the newline that ended the previous record.
		if _, err := reader.Seek(offset-1, io.SeekStart); err != nil {
			return scanSegment{}, datasetRowsErr(http.StatusInternalServerError, "failed to seek versioned file")
		}
		var prev [1]byte
		if _, err := io.ReadFull(reader, prev[:]); err != nil {
			return scanSegment{}, datasetRowsErr(http.StatusInternalServerError, "failed to read versioned file boundary")
		}
		if prev[0] != '\n' {
			return scanSegment{}, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: offset %d does not start a record", offset)
		}
	}
	if _, err := reader.Seek(offset, io.SeekStart); err != nil {
		return scanSegment{}, datasetRowsErr(http.StatusInternalServerError, "failed to seek versioned file")
	}

	// Reads stop at the committed size even if the object grew after the
	// stat above, so a record can never run past expectedSize and no cursor
	// is ever issued beyond the committed bytes. An object that SHRANK
	// mid-read surfaces as an EOF short of expectedSize — an explicit page
	// failure below, not a short-but-successful page.
	limited := io.LimitReader(reader, expectedSize-offset)
	seg := scanSegment{rows: []json.RawMessage{}}
	bufReader := bufio.NewReaderSize(limited, 64*1024)
	deferred := false
	hitEOF := false
	for len(seg.rows) < rowLimit {
		record, readErr := readRecord(bufReader, datasetFileRowsMaxLineBytes)
		if errors.Is(readErr, io.EOF) {
			hitEOF = true
			break
		}
		if readErr != nil {
			return scanSegment{}, readErr
		}
		if seg.consumed+int64(len(record)) > byteLimit {
			// The record would overflow the remaining byte budget — defer
			// it to the next page, which starts exactly where it begins.
			deferred = true
			break
		}
		seg.consumed += int64(len(record))
		trimmed := bytesTrim(record)
		if len(trimmed) == 0 {
			continue
		}
		if !validNDJSONRecord(trimmed) {
			return scanSegment{}, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_parse_failed: record at offset %d is not a JSON object", offset+seg.consumed-int64(len(record)))
		}
		if pred != nil && !pred.matchesRecord(trimmed) {
			continue
		}
		seg.rows = append(seg.rows, json.RawMessage(append([]byte(nil), trimmed...)))
	}
	seg.endOffset = offset + seg.consumed
	if seg.endOffset > expectedSize {
		// Unreachable through the LimitReader; kept as an explicit failure
		// so no page can ever claim bytes beyond the committed size.
		return scanSegment{}, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_size_mismatch: versioned file read %d bytes past its committed size %d", seg.endOffset, expectedSize)
	}
	if hitEOF && seg.endOffset != expectedSize {
		// The window ended before the committed size: the object delivered
		// fewer bytes than were committed (it shrank between the size
		// check and the read). Fail this page explicitly.
		return scanSegment{}, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_size_mismatch: versioned file ended at %d bytes but %d were committed", seg.endOffset, expectedSize)
	}
	// Done means every committed byte was consumed; a deferred record
	// always leaves bytes behind.
	seg.fileDone = !deferred && seg.endOffset >= expectedSize
	return seg, nil
}
