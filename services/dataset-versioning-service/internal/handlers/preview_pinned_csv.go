package handlers

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

const csvPreviewCursorMagic = "c1"

type csvScanRequest struct {
	datasetID uuid.UUID
	branch    string
	target    scanTarget
	version   int32
	limit     int
	path      string
	pred      *scanPredicate
	rawCursor string
}

func isDelimitedFile(path string) bool {
	format := datasetRowsFormat(path)
	return format == "csv" || format == "tsv"
}

func (h *Handlers) scanCSVPage(w http.ResponseWriter, r *http.Request, req csvScanRequest) (datasetScanAllFilesResponse, bool) {
	page, err := h.readCSVSnapshotPage(w, r, req)
	if err != nil {
		writeDatasetRowsError(w, err)
	}
	if page == nil {
		return datasetScanAllFilesResponse{}, false
	}
	return *page, true
}

func (h *Handlers) readCSVSnapshotPage(w http.ResponseWriter, r *http.Request, req csvScanRequest) (*datasetScanAllFilesResponse, error) {
	txn := req.target.txn
	flag, digest := scanPredicateDigest(req.pred)
	cursor := scanCursorV2{
		datasetID: req.datasetID, branchID: req.target.branch.ID, txnID: txn.ID, version: uint32(req.version),
		mode: scanCursorV2ModeAllFiles, predFlag: flag, predSum: digest,
	}
	mode := "all_files"
	if req.path != "" {
		cursor.mode, cursor.path, mode = scanCursorV2ModePath, req.path, "path"
	}
	if req.rawCursor != "" {
		next, err := decodeScanCursorToken(req.rawCursor, csvPreviewCursorMagic)
		if err != nil {
			return nil, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: %v", err)
		}
		if mismatch := scanCursorBindingMismatch(next, req.datasetID, req.target.branch.ID, req.version, txn.ID, flag, digest); mismatch != "" {
			return nil, datasetRowsErr(http.StatusBadRequest, "%s", mismatch)
		}
		if next.mode != cursor.mode || next.path != req.path || (next.mode == scanCursorV2ModeAllFiles && next.file == "") {
			return nil, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_mismatch: cursor belongs to another CSV file or scan mode")
		}
		cursor = next
	}

	from, count := cursor.file, 2
	if req.path != "" {
		from, count = req.path, 1
	}
	files, err := h.Repo.ListTransactionStagedFilesFrom(r.Context(), txn.ID, from, count)
	if err != nil {
		return nil, datasetRowsErr(http.StatusInternalServerError, "failed to load staged files")
	}
	if len(files) == 0 || (from != "" && files[0].LogicalPath != from) {
		if req.rawCursor != "" {
			return nil, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: CSV file is not registered in this version")
		}
		return nil, datasetRowsErr(http.StatusNotFound, "dataset_file_not_found: CSV file is not registered in this version")
	}
	file := files[0]
	if !validScanStagedPath(file.LogicalPath) || !isDelimitedFile(file.LogicalPath) || file.Operation != models.FileOperationAdd {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_csv_file_unsupported: scan requires registered CSV or TSV ADD files")
	}
	opener, ok := h.scanBackingOpener()
	if !ok {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_rows_backing_driver_unsupported: CSV scan requires local backing")
	}
	if !h.validateScanStagedObject(w, r.Context(), req.datasetID, txn, file.LogicalPath, &file) {
		return nil, nil // the shared validator wrote the HTTP error
	}
	options, err := csvPreviewOptions(txn.Metadata)
	if err != nil {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_csv_options_invalid: %v", err)
	}
	data, err := readCSVPreviewFile(opener, file)
	if err != nil {
		return nil, err
	}
	decoded, err := parseCSVPage(data, datasetRowsFormat(file.LogicalPath), options, cursor.offset, req.limit, req.pred)
	if err != nil {
		return nil, err
	}
	page := &datasetScanAllFilesResponse{
		DatasetID: req.datasetID.String(), Branch: req.branch, Version: req.version, TransactionID: txn.ID.String(),
		Format: "csv", Mode: mode, Path: req.path, Columns: decoded.columns, Rows: decoded.rows, RowCount: len(decoded.rows),
		PageBytes: int64(decoded.bytes), FilesAccessed: 1,
	}
	switch {
	case decoded.next >= 0:
		cursor.file = file.LogicalPath
		cursor.offset = decoded.next
	case len(files) == 2:
		cursor.file = files[1].LogicalPath
		cursor.offset = 0
	default:
		return page, nil
	}
	if !validScanStagedPath(cursor.file) || !isDelimitedFile(cursor.file) {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_csv_file_unsupported: invalid continuation file")
	}
	encoded := encodeScanCursorToken(cursor, csvPreviewCursorMagic)
	page.NextCursor = &encoded
	return page, nil
}

func csvPreviewOptions(metadata models.JSONValue) (models.CsvOptions, error) {
	var commit struct {
		Schema *models.DatasetSchema `json:"schema"`
	}
	if len(metadata) != 0 {
		if err := json.Unmarshal(metadata, &commit); err != nil {
			return models.CsvOptions{}, err
		}
	}
	if commit.Schema == nil || commit.Schema.CustomMetadata == nil || commit.Schema.CustomMetadata.CSV == nil {
		first := 1
		return models.CsvOptions{HeaderRow: &first, Charset: "UTF-8", BOM: "strip"}, nil
	}
	if err := normalizeOutputCSVOptions(commit.Schema); err != nil {
		return models.CsvOptions{}, err
	}
	return *commit.Schema.CustomMetadata.CSV, nil
}

func readCSVPreviewFile(opener localObjectOpener, file models.StageTransactionFile) ([]byte, error) {
	if file.SizeBytes > datasetFileRowsMaxBytes {
		return nil, datasetRowsErr(http.StatusRequestEntityTooLarge, "dataset_file_too_large: CSV preview limit is %d bytes", datasetFileRowsMaxBytes)
	}
	if file.SizeBytes < 0 {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_size_mismatch: invalid committed file size")
	}
	reader, err := opener.OpenLocalObject(file.PhysicalPath)
	if os.IsNotExist(err) {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_bytes_missing: the versioned file has no readable backing bytes at its registered storage path")
	}
	if err != nil {
		return nil, datasetRowsErr(http.StatusInternalServerError, "failed to open versioned file bytes")
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(io.LimitReader(reader, file.SizeBytes+1))
	if err != nil {
		return nil, datasetRowsErr(http.StatusInternalServerError, "failed to read versioned file bytes")
	}
	if int64(len(data)) != file.SizeBytes {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_file_size_mismatch: CSV bytes do not match the committed size")
	}
	return data, nil
}

func decodeCSVPreviewBytes(data []byte, options models.CsvOptions) ([]byte, error) {
	// Detect a BOM independently of the declared charset so mismatches
	// cannot be stripped and accidentally decoded using the wrong bytes.
	var bom string
	var width int
	switch {
	case bytes.HasPrefix(data, []byte{0xef, 0xbb, 0xbf}):
		bom, width = "UTF-8", 3
	case bytes.HasPrefix(data, []byte{0xff, 0xfe}):
		bom, width = "UTF-16LE", 2
	case bytes.HasPrefix(data, []byte{0xfe, 0xff}):
		bom, width = "UTF-16BE", 2
	}
	if bom != "" && (bom != options.Charset || options.BOM == "forbid") || bom == "" && options.BOM == "require" {
		return nil, errors.New("byte order mark does not match the configured encoding or BOM policy")
	}
	data = data[width:]
	if options.Charset == "UTF-8" {
		if !utf8.Valid(data) {
			return nil, errors.New("invalid UTF-8")
		}
		return data, nil
	}
	if len(data)%2 != 0 {
		return nil, errors.New("incomplete UTF-16 code unit")
	}
	var order binary.ByteOrder = binary.LittleEndian
	if options.Charset == "UTF-16BE" {
		order = binary.BigEndian
	}
	var decoded strings.Builder
	decoded.Grow(len(data))
	for i := 0; i < len(data); i += 2 {
		r := rune(order.Uint16(data[i:]))
		if utf16.IsSurrogate(r) {
			if i+2 == len(data) {
				return nil, errors.New("incomplete UTF-16 surrogate pair")
			}
			r = utf16.DecodeRune(r, rune(order.Uint16(data[i+2:])))
			if r == utf8.RuneError {
				return nil, errors.New("invalid UTF-16 surrogate pair")
			}
			i += 2
		}
		decoded.WriteRune(r)
	}
	return []byte(decoded.String()), nil
}

type csvDecodedPage struct {
	columns []pinnedPreviewColumn
	rows    []json.RawMessage
	bytes   int
	next    int64 // -1 means the file was exhausted
}

// Each page opens one bounded file. The logical record index is stable
// across UTF encodings, quoted newlines and optional predicates.
func parseCSVPage(raw []byte, format string, options models.CsvOptions, offset int64, limit int, pred *scanPredicate) (csvDecodedPage, error) {
	data, err := decodeCSVPreviewBytes(raw, options)
	if err != nil {
		return csvDecodedPage{}, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_csv_decode_failed: %v", err)
	}
	reader := csv.NewReader(bytes.NewReader(data))
	reader.FieldsPerRecord = -1 // preamble records can have a different width
	if options.Delimiter != "" {
		reader.Comma, _ = utf8.DecodeRuneInString(options.Delimiter)
	} else if format == "tsv" {
		reader.Comma = '\t'
	}
	columns, first, err := csvPreviewHeader(reader, *options.HeaderRow)
	if err != nil {
		return csvDecodedPage{}, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_csv_parse_failed: %v", err)
	}
	reader.FieldsPerRecord = len(columns)
	headerBytes, err := json.Marshal(columns)
	if err != nil || len(headerBytes) > datasetScanMaxPageBytes {
		return csvDecodedPage{}, datasetRowsErr(http.StatusRequestEntityTooLarge, "dataset_csv_header_too_large: columns exceed the scan page byte budget")
	}
	page := csvDecodedPage{columns: columns, rows: make([]json.RawMessage, 0, limit), bytes: len(headerBytes), next: -1}
	for index := int64(0); ; index++ {
		record := first
		first = nil
		if record == nil {
			record, err = reader.Read()
			if errors.Is(err, io.EOF) {
				if offset > 0 && index <= offset {
					return csvDecodedPage{}, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: CSV position is beyond its data records")
				}
				return page, nil
			}
			if err != nil {
				return csvDecodedPage{}, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_csv_parse_failed: %v", err)
			}
		}
		if index < offset {
			continue
		}
		values := make(map[string]models.JSONValue, len(columns))
		for i, cell := range record {
			values[columns[i].Name] = csvCellValue(cell)
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return csvDecodedPage{}, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_csv_parse_failed: %v", err)
		}
		if pred != nil && !pred.matchesRecord(encoded) {
			continue
		}
		if len(page.rows) == limit || page.bytes+len(encoded)+1 > datasetScanMaxPageBytes {
			if len(page.rows) == 0 {
				return csvDecodedPage{}, datasetRowsErr(http.StatusRequestEntityTooLarge, "dataset_csv_row_too_large: columns and row exceed the scan page byte budget")
			}
			page.next = index
			return page, nil
		}
		page.bytes += len(encoded) + 1
		page.rows = append(page.rows, encoded)
	}
}

func csvPreviewHeader(reader *csv.Reader, headerRow int) ([]pinnedPreviewColumn, []string, error) {
	record, err := reader.Read()
	if errors.Is(err, io.EOF) {
		return []pinnedPreviewColumn{}, nil, nil
	}
	for row := 1; err == nil && row < headerRow; row++ {
		record, err = reader.Read()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read header record: %w", err)
	}
	header := record
	if headerRow == 0 {
		header = make([]string, len(record)) // empty names become column_1, column_2, …
	}
	names, err := csvColumnNames(header)
	if err != nil {
		return nil, nil, err
	}
	columns := make([]pinnedPreviewColumn, len(names))
	for i, name := range names {
		columns[i].Name = name
	}
	if headerRow != 0 {
		record = nil
	}
	return columns, record, nil
}
