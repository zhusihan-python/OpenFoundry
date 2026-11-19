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

// previewPinnedCSV handles delimited snapshots. Each page opens one
// committed file and replays at most that bounded file to a logical
// record index. The index has the same meaning before and after decoding.
func (h *Handlers) previewPinnedCSV(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, branch string, target *scanTarget, version int32, limit int, rawCursor string) bool {
	files, err := h.Repo.ListTransactionStagedFilesFrom(r.Context(), target.txn.ID, "", 2)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to load staged files")
		return true
	}
	if len(files) == 0 || !isDelimitedFile(files[0].LogicalPath) {
		return false
	}
	page, err := h.readCSVPreviewPage(w, r, datasetID, branch, target, version, limit, rawCursor, files)
	if err != nil {
		writeDatasetRowsError(w, err)
	} else if page != nil {
		writeJSON(w, http.StatusOK, page)
	}
	return true
}

func isDelimitedFile(path string) bool {
	format := datasetRowsFormat(path)
	return format == "csv" || format == "tsv"
}

func (h *Handlers) readCSVPreviewPage(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, branch string, target *scanTarget, version int32, limit int, rawCursor string, files []models.StageTransactionFile) (*pinnedPreviewResponse, error) {
	cursor := scanCursorV2{datasetID: datasetID, branchID: target.branch.ID, txnID: target.txn.ID, version: uint32(version), mode: scanCursorV2ModeAllFiles}
	if rawCursor != "" {
		next, err := decodeScanCursorToken(rawCursor, csvPreviewCursorMagic)
		if err != nil {
			return nil, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: %v", err)
		}
		if mismatch := scanCursorBindingMismatch(next, datasetID, target.branch.ID, version, target.txn.ID, 0, [32]byte{}); mismatch != "" {
			return nil, datasetRowsErr(http.StatusBadRequest, "%s", mismatch)
		}
		if next.mode != cursor.mode || next.file == "" {
			return nil, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: invalid CSV position")
		}
		cursor = next
		files, err = h.Repo.ListTransactionStagedFilesFrom(r.Context(), target.txn.ID, cursor.file, 2)
		if err != nil {
			return nil, datasetRowsErr(http.StatusInternalServerError, "failed to load staged files")
		}
		if len(files) == 0 || files[0].LogicalPath != cursor.file {
			return nil, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: CSV file is not registered in this version")
		}
	}
	file := files[0]
	if !validScanStagedPath(file.LogicalPath) || !isDelimitedFile(file.LogicalPath) || file.Operation != models.FileOperationAdd {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_csv_file_unsupported: preview requires registered CSV or TSV ADD files")
	}
	opener, ok := h.scanBackingOpener()
	if !ok {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_rows_backing_driver_unsupported: CSV preview requires local backing")
	}
	if !h.validateScanStagedObject(w, r.Context(), datasetID, target.txn, file.LogicalPath, &file) {
		return nil, nil // the shared validator wrote the HTTP error
	}
	options, err := csvPreviewOptions(target.txn.Metadata)
	if err != nil {
		return nil, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_csv_options_invalid: %v", err)
	}
	data, err := readCSVPreviewFile(opener, file)
	if err != nil {
		return nil, err
	}
	columns, rows, more, err := parseCSVPreviewPage(data, datasetRowsFormat(file.LogicalPath), options, cursor.offset, limit)
	if err != nil {
		return nil, err
	}
	page := &pinnedPreviewResponse{
		DatasetID: datasetID.String(), Branch: branch, Version: version, TransactionID: target.txn.ID.String(),
		Format: "csv", Columns: columns, Rows: rows, RowCount: len(rows), Limit: limit,
	}
	switch {
	case more:
		cursor.file = file.LogicalPath
		cursor.offset += int64(len(rows))
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

func parseCSVPreviewPage(raw []byte, format string, options models.CsvOptions, offset int64, limit int) ([]pinnedPreviewColumn, []json.RawMessage, bool, error) {
	data, err := decodeCSVPreviewBytes(raw, options)
	if err != nil {
		return nil, nil, false, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_csv_decode_failed: %v", err)
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
		return nil, nil, false, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_csv_parse_failed: %v", err)
	}
	reader.FieldsPerRecord = len(columns)
	rows := make([]json.RawMessage, 0, limit)
	headerBytes, err := json.Marshal(columns)
	if err != nil || len(headerBytes) > datasetScanMaxPageBytes {
		return nil, nil, false, datasetRowsErr(http.StatusRequestEntityTooLarge, "dataset_csv_header_too_large: columns exceed the preview page byte budget")
	}
	pageBytes := len(headerBytes)
	for index := int64(0); ; index++ {
		record := first
		first = nil
		if record == nil {
			record, err = reader.Read()
			if errors.Is(err, io.EOF) {
				if offset > 0 && index <= offset {
					return nil, nil, false, datasetRowsErr(http.StatusBadRequest, "dataset_scan_cursor_invalid: CSV position is beyond its data records")
				}
				return columns, rows, false, nil
			}
			if err != nil {
				return nil, nil, false, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_csv_parse_failed: %v", err)
			}
		}
		if index < offset {
			continue
		}
		if len(rows) == limit {
			return columns, rows, true, nil
		}
		values := make(map[string]models.JSONValue, len(columns))
		for i, cell := range record {
			values[columns[i].Name] = csvCellValue(cell)
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return nil, nil, false, datasetRowsErr(http.StatusUnprocessableEntity, "dataset_csv_parse_failed: %v", err)
		}
		if pageBytes+len(encoded)+1 > datasetScanMaxPageBytes {
			if len(rows) == 0 {
				return nil, nil, false, datasetRowsErr(http.StatusRequestEntityTooLarge, "dataset_csv_row_too_large: columns and row exceed the preview page byte budget")
			}
			return columns, rows, true, nil
		}
		pageBytes += len(encoded) + 1
		rows = append(rows, encoded)
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
