package xlsximp

import (
	"bytes"
	"encoding/json"
	"sort"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/biff8"
)

// normalizeBIFF8 reads one worksheet through the existing pinned contract.
// It retains only normalized bytes and physical row numbers after each row.
func normalizeBIFF8(data []byte, cfg Config, format string, selection *cellRectangle) (*Result, error) {
	mapping, err := cfg.normalizedMapping()
	if err != nil {
		return nil, err
	}
	const biff8MaxRows, biff8MaxColumns = 65536, 256
	if selection != nil && (selection.lastRow > biff8MaxRows || selection.lastCol > biff8MaxColumns) {
		return nil, errKind(ErrConfig, "cell_range exceeds BIFF8 worksheet bounds")
	}
	if err := selection.validateMapping(mapping); err != nil {
		return nil, err
	}

	headerAt := 0
	if cfg.HeaderRow != nil {
		headerAt = *cfg.HeaderRow
		if headerAt < 1 || headerAt > MaxHeaderRow {
			return nil, errKind(ErrHeader, "header_row must be in 1..%d", MaxHeaderRow)
		}
	}
	book, err := biff8.OpenPinned(bytes.NewReader(data), int64(len(data)), format)
	if err != nil {
		return nil, errKind(ErrInvalid, "%v", err)
	}
	if book.SheetCount() > MaxSheets {
		return nil, errKind(ErrSheet, "workbook has more than %d worksheets", MaxSheets)
	}
	found := false
	for book.NextSheet() {
		if book.SheetName() == cfg.Sheet {
			found = true
			break
		}
	}
	if err := book.Err(); err != nil {
		return nil, errKind(ErrInvalid, "%v", err)
	}
	if !found {
		return nil, errKind(ErrSheet, "sheet %q is not a selectable worksheet", cfg.Sheet)
	}

	result := &Result{Config: Config{Sheet: cfg.Sheet, HeaderRow: cfg.HeaderRow, Mapping: mapping}}
	for _, m := range mapping {
		result.Columns = append(result.Columns, m.Target)
	}
	sort.Strings(result.Columns)
	var output bytes.Buffer
	width := 0
	for book.Read() {
		physical := book.RowIndex() + 1
		if physical < headerAt || (selection != nil && (physical < selection.firstRow || physical > selection.lastRow)) {
			continue
		}
		cells := book.Row()
		populated := false
		for i, cell := range cells {
			if cell.Raw != "" && (selection == nil || selection.includes(physical, i+1)) {
				populated = true
				width = max(width, i+1)
			}
		}
		if !populated || physical == headerAt {
			continue
		}
		if len(result.PhysicalRows) == MaxDataRows {
			return nil, errKind(ErrRowLimit, "sheet %q has more than %d data rows", cfg.Sheet, MaxDataRows)
		}
		row := make(map[string]any, len(mapping))
		budget := 2 + len(mapping) - 1
		for _, m := range mapping {
			budget += jsonEscapedLenUpperBound(m.Target) + 3
			var value any
			if m.SourceIndex < len(cells) && cells[m.SourceIndex].Raw != "" {
				text := cells[m.SourceIndex].Raw
				budget += jsonEscapedLenUpperBound(text) + 2
				value = text
			} else {
				budget += 4
			}
			if budget > MaxNDJSONLineBytes {
				return nil, errKind(ErrOutputLimit, "normalized row %d exceeds %d bytes", physical, MaxNDJSONLineBytes)
			}
			row[m.Target] = value
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return nil, errKind(ErrParse, "row %d cannot be encoded: %v", physical, err)
		}
		if output.Len()+len(encoded)+1 > MaxNDJSONTotalBytes {
			return nil, errKind(ErrOutputLimit, "normalized output exceeds %d bytes total", MaxNDJSONTotalBytes)
		}
		output.Write(encoded)
		output.WriteByte('\n')
		result.PhysicalRows = append(result.PhysicalRows, physical)
	}
	if err := book.Err(); err != nil {
		return nil, errKind(ErrInvalid, "%v", err)
	}
	if len(result.PhysicalRows) == 0 {
		return nil, errKind(ErrSheet, "sheet %q has no data rows", cfg.Sheet)
	}
	if selection != nil {
		width = selection.lastCol
	}
	for _, m := range mapping {
		if m.SourceIndex >= width {
			return nil, errKind(ErrMapping, "source_index %d is beyond the sheet width %d", m.SourceIndex, width)
		}
	}
	result.RowCount = len(result.PhysicalRows)
	result.ndjson = output.Bytes()
	return result, nil
}
