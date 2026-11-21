package xlsximp

import (
	"strconv"
	"strings"
)

type cellRectangle struct {
	firstRow, lastRow int
	firstCol, lastCol int
}

// parseCellRectangle parses inclusive A1 corners in the selected worksheet.
func parseCellRectangle(value string, header *int) (*cellRectangle, error) {
	first, last, ok := strings.Cut(strings.ToUpper(strings.TrimSpace(value)), ":")
	if !ok || len(first) > 10 || len(last) > 10 {
		return nil, errKind(ErrConfig, "cell_range must have two A1 corners, such as C5:E12")
	}
	c1, r1, e1 := parseCellRef(first)
	c2, r2, e2 := parseCellRef(last)
	if e1 != nil || e2 != nil || c2 < c1 || r2 < r1 || c2 > MaxSourceColumns ||
		r2 > excelMaxRows || first != columnName(c1)+strconv.Itoa(r1) || last != columnName(c2)+strconv.Itoa(r2) {
		return nil, errKind(ErrConfig, "cell_range corners are invalid, reversed or out of bounds")
	}
	dataRows := r2 - r1 + 1
	if header != nil {
		if *header < r1 || *header > r2 {
			return nil, errKind(ErrHeader, "header_row must lie inside cell_range")
		}
		dataRows = r2 - *header
	}
	if dataRows > MaxDataRows {
		return nil, errKind(ErrRowLimit, "cell_range spans more than %d data rows", MaxDataRows)
	}
	return &cellRectangle{firstRow: r1, lastRow: r2, firstCol: c1, lastCol: c2}, nil
}

func (r *cellRectangle) includes(row, col int) bool {
	return row >= r.firstRow && row <= r.lastRow && col >= r.firstCol && col <= r.lastCol
}
