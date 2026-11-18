package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

const (
	scanParquetMaxPageWorkBytes = 128 << 20
	scanParquetMaxGroupsPerPage = 4
	scanParquetMaxMetadataBytes = int64(stagedParquetMaxFooterBytes + len(stagedParquetMagic) + stagedParquetTrailerBytes)
)

type parquetScanPosition struct {
	group  uint32
	record uint32
}

func (p parquetScanPosition) cursorOffset() int64 {
	return int64(uint64(p.group)<<32 | uint64(p.record))
}

func unpackParquetScanPosition(offset int64) (parquetScanPosition, bool) {
	pos := parquetScanPosition{group: uint32(uint64(offset) >> 32), record: uint32(offset)}
	return pos, pos.group < stagedParquetMaxRowGroups && pos.record < stagedParquetMaxGroupRows
}

type parquetScanBudget struct {
	workBytes int64
	groups    int
}

func (b parquetScanBudget) canOpenFile() bool {
	return b.workBytes <= scanParquetMaxPageWorkBytes-scanParquetMaxMetadataBytes
}

type parquetScanSegment struct {
	rows     []json.RawMessage
	consumed int64
	next     parquetScanPosition
	fileDone bool
}

// The backing interface only promises Seek. The local production file also
// implements ReaderAt. The fallback is used sequentially.
type seekParquetReader struct{ io.ReadSeeker }

func (r seekParquetReader) ReadAt(p []byte, off int64) (int, error) {
	if _, err := r.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	return io.ReadFull(r.ReadSeeker, p)
}

func scanParquetSegment(ctx context.Context, opener localObjectOpener, key string, expectedSize int64, pos parquetScanPosition, rowLimit int, byteLimit int64, pred *scanPredicate, budget *parquetScanBudget) (parquetScanSegment, error) {
	out := parquetScanSegment{rows: []json.RawMessage{}, next: pos}
	r, err := opener.OpenLocalObject(key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_bytes_missing: the versioned file has no readable backing bytes at its registered storage path")
		}
		return out, datasetRowsErr(http.StatusInternalServerError, "failed to open versioned file bytes")
	}
	defer func() { _ = r.Close() }()
	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return out, datasetRowsErr(http.StatusInternalServerError, "failed to measure versioned file")
	}
	if size != expectedSize {
		return out, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_size_mismatch: versioned file is %d bytes on disk but %d bytes in its committed file record", size, expectedSize)
	}
	at, ok := r.(io.ReaderAt)
	if !ok {
		at = seekParquetReader{r}
	}
	meta, footerBytes, err := readStagedParquetMetadata(ctx, at, size, stagedParquetMaxRowGroups*stagedParquetMaxGroupRows)
	if err != nil {
		return out, parquetScanReadError(err)
	}
	budget.workBytes += footerBytes
	if len(meta.RowGroups) == 0 && pos == (parquetScanPosition{}) {
		out.fileDone = true
		return out, nil
	}
	if int(pos.group) >= len(meta.RowGroups) || int64(pos.record) >= meta.RowGroups[pos.group].NumRows {
		return out, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: Parquet row group or record is outside the versioned file")
	}

	var pages stagedParquetPageValidator
	var scratch bytes.Buffer
	encoder := json.NewEncoder(&scratch)
	encoder.SetEscapeHTML(false)
	for out.next.group < uint32(len(meta.RowGroups)) {
		if err := ctx.Err(); err != nil {
			return out, parquetScanReadError(err)
		}
		group := meta.RowGroups[out.next.group]
		// Every readable group must fit on a fresh page including maximum
		// metadata. Otherwise deferring the same cursor could loop forever.
		work := group.TotalByteSize
		for _, chunk := range group.Columns {
			compressed := chunk.MetaData.TotalCompressedSize
			if compressed > scanParquetMaxPageWorkBytes-scanParquetMaxMetadataBytes-work {
				return out, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_parquet_read_budget_exceeded: Parquet row group exceeds the readable work budget")
			}
			work += compressed
		}
		if budget.groups >= scanParquetMaxGroupsPerPage || work > scanParquetMaxPageWorkBytes-budget.workBytes {
			break
		}
		budget.groups++
		budget.workBytes += work

		rows, err := readParquetOrdersGroup(ctx, &pages, at, meta, out.next.group)
		if err != nil {
			return out, parquetScanReadError(err)
		}
		for out.next.record < uint32(len(rows)) {
			if err := ctx.Err(); err != nil {
				return out, parquetScanReadError(err)
			}
			wire, err := rows[out.next.record].wire()
			if err != nil {
				return out, parquetScanReadError(err)
			}
			scratch.Reset()
			if err := encoder.Encode(wire); err != nil {
				return out, parquetScanReadError(err)
			}
			n := int64(scratch.Len())
			if n > datasetScanMaxPageBytes {
				return out, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_record_too_large: Parquet record exceeds the rows:scan page byte limit")
			}
			if n > byteLimit-out.consumed {
				return out, nil
			}
			out.consumed += n
			out.next.record++
			record := bytes.TrimSuffix(scratch.Bytes(), []byte{'\n'})
			if pred == nil || pred.matchesRecord(record) {
				out.rows = append(out.rows, append(json.RawMessage(nil), record...))
			}
			if len(out.rows) == rowLimit {
				break
			}
		}
		if out.next.record == uint32(len(rows)) {
			out.next = parquetScanPosition{group: out.next.group + 1}
		}
		if len(out.rows) == rowLimit || out.consumed == byteLimit {
			break
		}
	}
	out.fileDone = int(out.next.group) == len(meta.RowGroups)
	return out, nil
}

func readParquetOrdersGroup(ctx context.Context, pages *stagedParquetPageValidator, at io.ReaderAt, meta *metadata.FileMetaData, index uint32) ([]parquetOrderRow, error) {
	group := meta.RowGroups[index]
	rows := make([]parquetOrderRow, group.NumRows)
	for i, chunk := range group.Columns {
		col := chunk.MetaData
		base := 0
		err := pages.validateColumn(ctx, at, col.DataPageOffset, col.DataPageOffset+col.TotalCompressedSize,
			col.TotalUncompressedSize, group.NumRows, func(data []byte, count int) error {
				end := base + count
				if err := decodeParquetOrdersColumn(i, data, rows[base:end]); err != nil {
					return err
				}
				base = end
				return nil
			})
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func parquetScanReadError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return datasetRowsErr(http.StatusGatewayTimeout, "dataset_parquet_read_interrupted: the request ended while reading a Parquet file")
	}
	return datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_parse_failed: invalid versioned Parquet data")
}

// p1 has the s2 binding fields. Its position stores a u32 row group followed
// by a u32 row within that group in the high and low halves of the offset.
const scanParquetCursorMagic = "p1"

func newParquetScanCursor(datasetID, branchID, txnID uuid.UUID, version int32, mode byte, pred *scanPredicate) scanCursorV2 {
	flag, sum := scanPredicateDigest(pred)
	return scanCursorV2{
		datasetID: datasetID, branchID: branchID, txnID: txnID, version: uint32(version),
		mode: mode, predFlag: flag, predSum: sum,
	}
}

func parseParquetScanCursor(w http.ResponseWriter, raw string, binding scanCursorV2) (string, parquetScanPosition, bool) {
	if raw == "" {
		return "", parquetScanPosition{}, true
	}
	c, err := decodeScanCursorToken(raw, scanParquetCursorMagic)
	if err != nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: %v", err))
		return "", parquetScanPosition{}, false
	}
	if mismatch := scanCursorBindingMismatch(c, binding.datasetID, binding.branchID, int32(binding.version),
		binding.txnID, binding.predFlag, binding.predSum); mismatch != "" {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "%s", mismatch))
		return "", parquetScanPosition{}, false
	}
	if c.mode != binding.mode {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_mismatch: cursor belongs to another scan mode"))
		return "", parquetScanPosition{}, false
	}
	if c.mode == scanCursorV2ModePath && c.path != binding.path {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_mismatch: cursor belongs to another path"))
		return "", parquetScanPosition{}, false
	}
	pos, valid := unpackParquetScanPosition(c.offset)
	if !valid || (c.mode == scanCursorV2ModeAllFiles && c.file == "") {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: Parquet position or file is out of bounds"))
		return "", parquetScanPosition{}, false
	}
	return c.file, pos, true
}

func (h *Handlers) scanParquetPath(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, branchName string, branchRow *models.RuntimeBranch, txn *models.RuntimeTransaction, version int32, logical string, limit int, pred *scanPredicate, rawCursor string) {
	staged, err := h.Repo.GetTransactionStagedFile(r.Context(), txn.ID, logical)
	if err != nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusInternalServerError, "failed to load staged file"))
		return
	}
	if staged == nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusNotFound, "file not registered in dataset version"))
		return
	}
	opener, ok := h.scanBackingOpener()
	if !ok {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_rows_backing_driver_unsupported: rows:scan requires the local backing filesystem"))
		return
	}
	if !h.validateScanParquetFile(w, r.Context(), datasetID, txn, logical, staged) {
		return
	}

	binding := newParquetScanCursor(datasetID, branchRow.ID, txn.ID, version, scanCursorV2ModePath, pred)
	binding.path = logical
	_, pos, ok := parseParquetScanCursor(w, rawCursor, binding)
	if !ok {
		return
	}
	var budget parquetScanBudget
	seg, err := scanParquetSegment(r.Context(), opener, staged.PhysicalPath, staged.SizeBytes, pos, limit, datasetScanMaxPageBytes, pred, &budget)
	if err != nil {
		writeDatasetRowsError(w, err)
		return
	}
	resp := datasetFileScanResponse{
		DatasetID: datasetID.String(), Branch: branchName, Version: version, TransactionID: txn.ID.String(),
		Path: logical, Format: stagedParquetFormat, SchemaVersion: stagedParquetSchemaVersion, Schema: parquetOrdersSchema(),
		Rows: seg.rows, RowCount: len(seg.rows), PageBytes: seg.consumed, SizeBytes: staged.SizeBytes,
	}
	if !seg.fileDone {
		binding.offset = seg.next.cursorOffset()
		next := encodeScanCursorToken(binding, scanParquetCursorMagic)
		resp.NextCursor = &next
	}
	h.emitScanModeAudit(r, datasetID, branchName, version, logical, "path", limit, resp.RowCount, resp.NextCursor != nil)
	writeScanJSON(w, http.StatusOK, resp)
}

func (h *Handlers) scanParquetAllFilesPage(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, branchName string, branchRow *models.RuntimeBranch, txn *models.RuntimeTransaction, version int32, limit int, pred *scanPredicate, rawCursor string) (datasetScanAllFilesResponse, bool) {
	var zero datasetScanAllFilesResponse
	binding := newParquetScanCursor(datasetID, branchRow.ID, txn.ID, version, scanCursorV2ModeAllFiles, pred)
	fromPath, start, ok := parseParquetScanCursor(w, rawCursor, binding)
	if !ok {
		return zero, false
	}
	opener, ok := h.scanBackingOpener()
	if !ok {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_rows_backing_driver_unsupported: rows:scan requires the local backing filesystem"))
		return zero, false
	}
	batch, err := h.Repo.ListTransactionStagedFilesFrom(r.Context(), txn.ID, fromPath, scanMaxFilesPerPage+1)
	if err != nil {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusInternalServerError, "failed to load staged files"))
		return zero, false
	}
	if fromPath != "" && (len(batch) == 0 || batch[0].LogicalPath != fromPath) {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: cursor file is not registered in the dataset version"))
		return zero, false
	}
	for _, file := range batch {
		if !validStagedParquetPath(file.LogicalPath) {
			writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_scan_file_path_invalid: %q is not a canonical Parquet logical path readable by rows:scan", file.LogicalPath))
			return zero, false
		}
	}
	resp := datasetScanAllFilesResponse{
		DatasetID: datasetID.String(), Branch: branchName, Version: version, TransactionID: txn.ID.String(),
		Format: stagedParquetFormat, SchemaVersion: stagedParquetSchemaVersion, Schema: parquetOrdersSchema(),
		Mode: "all_files", Rows: []json.RawMessage{},
	}
	var budget parquetScanBudget
	for i, file := range batch {
		if resp.FilesAccessed >= scanMaxFilesPerPage || len(resp.Rows) >= limit || resp.PageBytes >= datasetScanMaxPageBytes ||
			budget.groups >= scanParquetMaxGroupsPerPage || !budget.canOpenFile() {
			binding.file = file.LogicalPath
			break
		}
		pos := parquetScanPosition{}
		if i == 0 && fromPath != "" {
			pos = start
		}
		if !h.validateScanParquetFile(w, r.Context(), datasetID, txn, file.LogicalPath, &file) {
			return zero, false
		}
		seg, err := scanParquetSegment(r.Context(), opener, file.PhysicalPath, file.SizeBytes, pos,
			limit-len(resp.Rows), datasetScanMaxPageBytes-resp.PageBytes, pred, &budget)
		if err != nil {
			writeDatasetRowsError(w, err)
			return zero, false
		}
		resp.FilesAccessed++
		resp.Rows = append(resp.Rows, seg.rows...)
		resp.PageBytes += seg.consumed
		if !seg.fileDone {
			binding.file, binding.offset = file.LogicalPath, seg.next.cursorOffset()
			break
		}
	}
	if binding.file != "" {
		next := encodeScanCursorToken(binding, scanParquetCursorMagic)
		resp.NextCursor = &next
	}
	resp.RowCount = len(resp.Rows)
	return resp, true
}

func (h *Handlers) validateScanParquetFile(w http.ResponseWriter, ctx context.Context, datasetID uuid.UUID, txn *models.RuntimeTransaction, logical string, staged *models.StageTransactionFile) bool {
	if !validStagedParquetPath(logical) {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_scan_file_path_invalid: %q is not a canonical Parquet logical path readable by rows:scan", logical))
		return false
	}
	if staged.Operation != models.FileOperationAdd {
		writeDatasetRowsError(w, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_scan_file_operation_unsupported: %s is staged as %s, only ADD is readable", logical, staged.Operation))
		return false
	}
	return h.validateScanStagedObject(w, ctx, datasetID, txn, logical, staged)
}
