// WP3.3 increment 1 (S3 batched read): version-pinned streaming scan of a
// single NDJSON output file committed through outputs:commit.
//
// GET /v1/datasets/{rid}/files/rows:scan is an explicit opt-in alternative
// to /files/rows. It requires branch + version + logical path, resolves the
// version to its exact committed SNAPSHOT transaction and the transaction's
// staged file, and streams the backing object record by record — the object
// is never loaded whole into memory. Pages are addressed by an opaque,
// bounded cursor that binds dataset, branch, version/transaction, logical
// path and a record-aligned byte offset; every page re-runs the full
// resolution chain, so a cursor never acts as authorization.
//
// WP3.3 increment 2 extends the same route with two opt-in modes: an eq
// predicate over one path (cursor version s2, see file_rows_scan_v2.go)
// and all_files=true, which scans every NDJSON ADD file of the version's
// transaction in stable logical-path order. Requests without the new
// parameters take the increment-1 path exactly as before, including the
// s1 cursor format.

package handlers

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

// datasetScanMaxPageBytes caps the record bytes one scan page may
// accumulate across all files it visits (newlines included). A record is at
// most datasetFileRowsMaxLineBytes, so a first record always fits and the
// cap only slices later records onto the next page.
const datasetScanMaxPageBytes = 8 << 20

// localObjectOpener is the streaming read capability the scan requires from
// the local backing store. *storageabstraction.LocalBackingFS satisfies it;
// the generic BackingFS interface is deliberately untouched.
type localObjectOpener interface {
	OpenLocalObject(key string) (io.ReadSeekCloser, error)
}

// scanCursorMagic versions the cursor wire format. A decoded cursor that
// does not start with the current magic is rejected.
const scanCursorMagic = "s1"

// scanCursorMaxPath bounds the logical-path segment of a cursor; anything
// longer cannot be a registered logical path anyway.
const scanCursorMaxPath = 512

// scanCursor is the page position plus the binding it was issued under.
// It is opaque to callers (base64 of a versioned binary layout) and is only
// ever compared for equality against the freshly re-resolved request — it
// never authorizes anything by itself. It is deliberately NOT signed: a
// forged cursor can only move the read position within the already
// authorized, freshly re-resolved file, and the offset checks (bounds,
// size equality, newline boundary) keep that position record-aligned. This
// matches the service's existing opaque offset-cursor convention
// (encodeCursor) rather than introducing an HMAC surface.
type scanCursor struct {
	datasetID uuid.UUID
	branchID  uuid.UUID
	version   uint32
	txnID     uuid.UUID
	offset    int64
	path      string
}

func encodeScanCursor(c scanCursor) string {
	buf := make([]byte, 0, 64+len(c.path))
	buf = append(buf, scanCursorMagic...)
	buf = append(buf, c.datasetID[:]...)
	buf = append(buf, c.branchID[:]...)
	buf = binary.LittleEndian.AppendUint32(buf, c.version)
	buf = append(buf, c.txnID[:]...)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(c.offset))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(c.path)))
	buf = append(buf, c.path...)
	return base64.RawURLEncoding.EncodeToString(buf)
}

func decodeScanCursor(raw string) (scanCursor, error) {
	var zero scanCursor
	if len(raw) > 1024 {
		return zero, fmt.Errorf("cursor too long")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return zero, fmt.Errorf("cursor encoding invalid")
	}
	if len(decoded) < 64 || string(decoded[:2]) != scanCursorMagic {
		return zero, fmt.Errorf("unrecognized cursor")
	}
	c := scanCursor{
		version: binary.LittleEndian.Uint32(decoded[34:38]),
		offset:  int64(binary.LittleEndian.Uint64(decoded[54:62])),
	}
	copy(c.datasetID[:], decoded[2:18])
	copy(c.branchID[:], decoded[18:34])
	copy(c.txnID[:], decoded[38:54])
	pathLen := int(binary.LittleEndian.Uint16(decoded[62:64]))
	if len(decoded) != 64+pathLen {
		return zero, fmt.Errorf("cursor length mismatch")
	}
	c.path = string(decoded[64:])
	if c.path == "" || len(c.path) > scanCursorMaxPath {
		return zero, fmt.Errorf("cursor path out of bounds")
	}
	if c.version == 0 || c.offset < 0 {
		return zero, fmt.Errorf("cursor values out of range")
	}
	return c, nil
}

type datasetFileScanResponse struct {
	DatasetID     string                `json:"dataset_id"`
	Branch        string                `json:"branch"`
	Version       int32                 `json:"version"`
	TransactionID string                `json:"transaction_id"`
	Path          string                `json:"path"`
	Format        string                `json:"format"`
	SchemaVersion string                `json:"schema_version,omitempty"`
	Schema        *models.DatasetSchema `json:"schema,omitempty"`
	Rows          []json.RawMessage     `json:"rows"`
	NextCursor    *string               `json:"next_cursor"`
	RowCount      int                   `json:"row_count"`
	PageBytes     int64                 `json:"page_bytes"`
	SizeBytes     int64                 `json:"size_bytes"`
}

// ScanDatasetFileRows serves GET /v1/datasets/{rid}/files/rows:scan.

// writeScanJSON writes one rows:scan SUCCESS response. It is the
// scan-local twin of writeJSON with HTML escaping disabled: the default
// encoder rewrites & < > into multi-byte entities, which can inflate a
// source record that is within the 1MiB per-record scan bound past the
// bound once a caller re-spools the received row bytes (WP3.3 outputs:stage
// applies the same newline-included cap). JSON types, limits and all other
// semantics are unchanged; nosniff is pinned because this body no longer
// goes through the shared writer.
func writeScanJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

func (h *Handlers) ScanDatasetFileRows(w http.ResponseWriter, r *http.Request) {
	datasetID, ok := h.resolveDatasetForCatalog(w, r)
	if !ok {
		return
	}
	if !h.requireDatasetRead(w, r, datasetID) {
		return
	}
	// ParseQuery, not r.URL.Query(): the convenience method silently drops
	// key/value pairs with invalid percent-escapes, which would turn a
	// corrupted cursor into a first-page request. Here a malformed encoding
	// is an explicit 400.
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid query encoding")
		return
	}
	branch := strings.TrimSpace(query.Get("branch"))
	if branch == "" {
		writeJSONErr(w, http.StatusBadRequest, "branch is required for rows:scan")
		return
	}
	rawVersion := strings.TrimSpace(query.Get("version"))
	versionInt, err := strconv.ParseInt(rawVersion, 10, 64)
	if rawVersion == "" || err != nil || versionInt < 1 || versionInt > 1<<31-1 {
		writeJSONErr(w, http.StatusBadRequest, "version must be a positive integer")
		return
	}
	version := int32(versionInt)

	// Increment-2 parameters. Both must appear at most once and all_files
	// accepts only the literal "true"; anything else is an explicit 400.
	// Predicate PRESENCE is tracked separately from its value — an explicit
	// empty `predicate=` is a parse error, not "no predicate". Without
	// these parameters the request takes the increment-1 path.
	allFiles := false
	if raws := query["all_files"]; len(raws) > 1 {
		writeJSONErr(w, http.StatusBadRequest, "all_files must appear at most once for rows:scan")
		return
	} else if len(raws) == 1 {
		if raws[0] != "true" {
			writeJSONErr(w, http.StatusBadRequest, "all_files must be true for rows:scan")
			return
		}
		allFiles = true
	}
	predPresent := false
	predRaw := ""
	if raws := query["predicate"]; len(raws) > 1 {
		writeJSONErr(w, http.StatusBadRequest, "predicate must appear at most once for rows:scan")
		return
	} else if len(raws) == 1 {
		predPresent = true
		predRaw = raws[0]
	}
	rawCursor := strings.TrimSpace(query.Get("cursor"))

	// Per-mode parameter validation, then the shared limit and target
	// resolution once. The order keeps the increment-1 error precedence for
	// requests without the new parameters: path checks come before limit
	// and before any branch/version resolution.
	logical := ""
	if allFiles {
		// Mutual exclusion is on the PRESENCE of the path key — `path=`,
		// whitespace-only and repeated spellings all count.
		if _, hasPath := query["path"]; hasPath {
			writeJSONErr(w, http.StatusBadRequest, "all_files and path are mutually exclusive for rows:scan")
			return
		}
	} else {
		logical = strings.TrimSpace(query.Get("path"))
		if logical == "" {
			writeJSONErr(w, http.StatusBadRequest, "path is required for rows:scan")
			return
		}
		if !validScanRequestPath(w, logical) {
			return
		}
	}
	var pred *scanPredicate
	if predPresent {
		parsed, predOK := parseScanPredicateParam(w, predRaw)
		if !predOK {
			return
		}
		pred = parsed
	}
	limit, limitOK := parseScanLimit(w, query)
	if !limitOK {
		return
	}
	target, ok := h.resolveScanTarget(w, r, datasetID, branch, version)
	if !ok {
		return
	}

	if allFiles {
		h.scanAllFiles(w, r, datasetID, branch, target.branch, target.txn, version, limit, pred, rawCursor)
		return
	}
	if strings.HasSuffix(logical, stagedParquetExtension) {
		h.scanParquetPath(w, r, datasetID, branch, target.branch, target.txn, version, logical, limit, pred, rawCursor)
		return
	}
	if predPresent {
		// Mode 1: one explicit path under an eq predicate (s2 cursor).
		h.scanPathWithPredicate(w, r, datasetID, branch, target.branch, target.txn, version, logical, limit, pred, rawCursor)
		return
	}

	// Increment-1 path: branch + version + path, no predicate, s1 cursor.
	branchRow, txn := target.branch, target.txn

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

	offset := int64(0)
	if rawCursor != "" {
		cursor, cursorErr := decodeScanCursor(rawCursor)
		if cursorErr != nil {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: %v", cursorErr))
			return
		}
		switch {
		case cursor.datasetID != datasetID:
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_mismatch: cursor belongs to another dataset"))
			return
		case cursor.branchID != branchRow.ID || cursor.version != uint32(version):
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_mismatch: cursor belongs to another branch or version"))
			return
		case cursor.txnID != txn.ID:
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_mismatch: cursor belongs to another transaction"))
			return
		case cursor.path != logical:
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_mismatch: cursor belongs to another path"))
			return
		case cursor.offset > staged.SizeBytes:
			writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: offset %d is past the versioned file size %d", cursor.offset, staged.SizeBytes))
			return
		}
		offset = cursor.offset
	}

	page, scanErr := scanNDJSONPage(opener, staged.PhysicalPath, staged.SizeBytes, offset, limit)
	if scanErr != nil {
		writeDatasetRowsError(w, scanErr)
		return
	}
	resp := datasetFileScanResponse{
		DatasetID: datasetID.String(), Branch: branch, Version: version,
		TransactionID: txn.ID.String(), Path: logical, Format: "ndjson",
		Rows: page.rows, RowCount: len(page.rows),
		PageBytes: page.pageBytes, SizeBytes: staged.SizeBytes,
	}
	if page.hasMore {
		next := encodeScanCursor(scanCursor{
			datasetID: datasetID, branchID: branchRow.ID, version: uint32(version),
			txnID: txn.ID, offset: page.endOffset, path: logical,
		})
		resp.NextCursor = &next
	}
	h.emitScanAudit(r, datasetID, branch, version, logical, limit, resp.RowCount, page.hasMore)
	writeScanJSON(w, http.StatusOK, resp)
}

// parseScanPredicateParam parses an explicitly present predicate parameter,
// writing the 400 itself on any validation failure — including an empty
// value, which is a parse error rather than "no predicate".
func parseScanPredicateParam(w http.ResponseWriter, predRaw string) (*scanPredicate, bool) {
	pred, err := parseScanPredicate(predRaw)
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, fmt.Sprintf("dataset_scan_predicate_invalid: %v", err))
		return nil, false
	}
	return pred, true
}

// validScanRequestPath applies the request-side path bound and canonicality
// check shared by every scan mode: the path becomes part of a cursor, so it
// is bounded before anything is issued, and non-canonical spellings are
// rejected outright (safeObjectKey refuses normalizable aliases, leaving
// one exact string per registered path).
func validScanRequestPath(w http.ResponseWriter, logical string) bool {
	if len(logical) > scanCursorMaxPath {
		writeJSONErr(w, http.StatusBadRequest, "path is too long for rows:scan")
		return false
	}
	if !safeObjectKey(logical) {
		writeJSONErr(w, http.StatusBadRequest, "invalid path for rows:scan")
		return false
	}
	return true
}

// parseScanLimit parses the optional row limit (default 100, cap 1000).
func parseScanLimit(w http.ResponseWriter, query url.Values) (int, bool) {
	limit := datasetFileRowsDefaultLimit
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed < 1 {
			writeJSONErr(w, http.StatusBadRequest, "limit must be a positive integer")
			return 0, false
		}
		limit = parsed
	}
	if limit > datasetFileRowsMaxLimit {
		limit = datasetFileRowsMaxLimit
	}
	return limit, true
}

// scanTarget is the common resolution result shared by every scan mode.
type scanTarget struct {
	branch *models.RuntimeBranch
	txn    *models.RuntimeTransaction
}

// resolveScanTarget runs the exact version→transaction resolution chain
// re-run on every page: GetRuntimeBranch (not GetBranch — deleted/archived
// branches must not resolve), GetVersion, and the transaction checks.
func (h *Handlers) resolveScanTarget(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, branch string, version int32) (*scanTarget, bool) {
	branchRow, err := h.Repo.GetRuntimeBranch(r.Context(), datasetID, branch)
	if err != nil && !errors.Is(err, repo.ErrNotFound) {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusInternalServerError, "failed to load branch"))
		return nil, false
	}
	if err != nil || branchRow == nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusNotFound, "branch not found"))
		return nil, false
	}
	versionRow, err := h.Repo.GetVersion(r.Context(), datasetID, version)
	if err != nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusInternalServerError, "failed to load version"))
		return nil, false
	}
	if versionRow == nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusNotFound, "dataset version not found"))
		return nil, false
	}
	if versionRow.TransactionID == nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_version_has_no_transaction: version %d was not cut by a publish transaction", version))
		return nil, false
	}
	txn, err := h.Repo.GetRuntimeTransaction(r.Context(), datasetID, *versionRow.TransactionID)
	if err != nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusInternalServerError, "failed to load transaction"))
		return nil, false
	}
	if txn == nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_version_transaction_missing: version %d references transaction %s which does not exist", version, versionRow.TransactionID))
		return nil, false
	}
	if txn.Status != models.TransactionStatusCommitted {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_version_transaction_not_committed: transaction %s is %s", txn.ID, txn.Status))
		return nil, false
	}
	if txn.TxType != models.TransactionTypeSnapshot {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_scan_transaction_type_unsupported: rows:scan supports SNAPSHOT transactions, version %d was cut by %s", version, txn.TxType))
		return nil, false
	}
	if txn.BranchID != branchRow.ID {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_version_branch_mismatch: version %d belongs to another branch of this dataset", version))
		return nil, false
	}
	return &scanTarget{branch: branchRow, txn: txn}, true
}

func (h *Handlers) emitScanAudit(r *http.Request, datasetID uuid.UUID, branch string, version int32, path string, limit int, rowCount int, hasCursor bool) {
	actor := "anonymous"
	if claims, ok := authmw.FromContext(r.Context()); ok {
		actor = claims.Sub.String()
	}
	h.emitAudit(r.Context(), AuditEvent{Actor: actor, Action: "files.rows_scan", DatasetRID: datasetID.String(), Details: map[string]any{
		"path": path, "branch": branch, "version": version, "limit": limit,
		"row_count": rowCount, "has_next_cursor": hasCursor,
	}})
}

// scanBackingOpener resolves the streaming opener from the configured
// backing store. It mirrors readRegisteredFileRows's driver gate: local
// backing only, and the concrete type must expose OpenLocalObject.
func (h *Handlers) scanBackingOpener() (localObjectOpener, bool) {
	if h.BackingFS == nil || h.BackingFS.FSID() != "local" {
		return nil, false
	}
	opener, ok := h.BackingFS.(localObjectOpener)
	if !ok {
		return nil, false
	}
	return opener, true
}

type scanPage struct {
	rows      []json.RawMessage
	pageBytes int64
	endOffset int64
	hasMore   bool
}

// scanNDJSONPage is the increment-1 single-file page: one budget-free
// (page-capped) segment of the file with no predicate. It is a thin
// wrapper over scanNDJSONSegment — the single streaming scan loop — and
// its hasMore answer ("there are unconsumed bytes after this page") is
// exactly !fileDone: a deferred record always leaves bytes behind, and a
// page that ended on the row limit has more iff its end offset has not
// reached the committed size.
func scanNDJSONPage(opener localObjectOpener, key string, expectedSize int64, offset int64, limit int) (scanPage, error) {
	seg, err := scanNDJSONSegment(opener, key, expectedSize, offset, limit, datasetScanMaxPageBytes, nil)
	if err != nil {
		return scanPage{}, err
	}
	return scanPage{rows: seg.rows, pageBytes: seg.consumed, endOffset: seg.endOffset, hasMore: !seg.fileDone}, nil
}

// readRecord reads one newline-terminated record including the newline; a
// final record without a trailing newline is returned as-is. A record
// longer than maxBytes before its newline fails explicitly.
func readRecord(reader *bufio.Reader, maxBytes int) ([]byte, error) {
	var buf []byte
	for {
		chunk, err := reader.ReadSlice('\n')
		buf = append(buf, chunk...)
		if len(buf) > maxBytes {
			return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_record_too_large: a single record exceeds %d bytes", maxBytes)
		}
		if err == nil {
			return buf, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			if len(buf) == 0 {
				return nil, io.EOF
			}
			return buf, nil
		}
		return nil, datasetRowsErr(http.StatusInternalServerError, "failed to read versioned file bytes")
	}
}

// validNDJSONRecord accepts non-empty records that parse as JSON objects.
func validNDJSONRecord(record []byte) bool {
	if len(record) == 0 || record[0] != '{' {
		return false
	}
	return json.Valid(record)
}

// bytesTrim strips the record's surrounding whitespace bytes.
func bytesTrim(record []byte) []byte {
	return bytes.Trim(record, " \t\r\n")
}
