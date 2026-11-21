package biff8

import (
	"encoding/binary"
	"fmt"
	"io"
)

const maxPinnedWorkbookBytes = 32 << 20

// OpenPinned checks the workbook type and encryption state before
// letting the ordinary decoder materialize workbook globals.
func OpenPinned(ra io.ReaderAt, size int64, format string) (*Reader, error) {
	if size < 512 || size > maxPinnedWorkbookBytes {
		return nil, fmt.Errorf("workbook_file_size: %d", size)
	}
	if format != "xls" && format != "xlt" {
		return nil, fmt.Errorf("workbook_format: %q", format)
	}
	return openWorkbook(ra, size, format)
}

func checkWorkbookGlobals(wb *stream, format string) error {
	size := wb.Size()
	if size < 8 || size > maxPinnedWorkbookBytes {
		return fmt.Errorf("workbook_stream_size: %d", size)
	}
	rr := newRecordReader(wb)
	var bof [4]byte
	var template bool
	for offset := int64(0); offset < size; {
		kind, length, err := rr.readHeader(offset)
		if err != nil || length > 8224 || offset+4+int64(length) > size {
			return fmt.Errorf("workbook_record_invalid: offset %d", offset)
		}
		if offset == 0 {
			if kind != recBOF || length < 4 {
				return fmt.Errorf("workbook_bof_invalid")
			}
			if _, err := wb.ReadAt(bof[:], 4); err != nil {
				return fmt.Errorf("workbook_bof_invalid: %w", err)
			}
			if binary.LittleEndian.Uint16(bof[:2]) != biff8 ||
				binary.LittleEndian.Uint16(bof[2:]) != substreamWorkbook {
				return fmt.Errorf("workbook_requires_biff8")
			}
		}
		switch kind {
		case 0x002f: // FilePass
			return fmt.Errorf("workbook_encrypted")
		case 0x0060: // Template
			if template || length != 0 {
				return fmt.Errorf("workbook_template_marker_invalid")
			}
			template = true
		case recBoundSheet:
			var fields [6]byte
			if length < uint16(len(fields)) {
				return fmt.Errorf("workbook_sheet_invalid")
			}
			if _, err := wb.ReadAt(fields[:], offset+4); err != nil {
				return err
			}
			if fields[5] == 0x01 {
				return fmt.Errorf("workbook_macro_unsupported")
			}
		case recEOF:
			if length != 0 {
				return fmt.Errorf("workbook_eof_invalid")
			}
			if template != (format == "xlt") {
				return fmt.Errorf("workbook_type_mismatch")
			}
			return nil
		}
		offset += 4 + int64(length)
	}
	return fmt.Errorf("workbook_globals_missing_eof")
}
