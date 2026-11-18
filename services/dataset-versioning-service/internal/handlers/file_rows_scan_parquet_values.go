package handlers

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

type parquetOrderRow struct {
	id        int64
	amount    int64
	date      int32
	orderedAt int64
	zone      string
	note      *string
}

func parquetOrdersSchema() *models.DatasetSchema {
	precision, scale := uint8(18), uint8(4)
	return &models.DatasetSchema{
		FileFormat: models.FileFormatParquet,
		Fields: []models.Field{
			{Name: "order_id", Type: models.FieldTypeLong},
			{Name: "amount", Type: models.FieldTypeDecimal, Precision: &precision, Scale: &scale},
			{Name: "order_date", Type: models.FieldTypeDate},
			{Name: "ordered_at", Type: models.FieldTypeTimestamp},
			{Name: "source_time_zone", Type: models.FieldTypeString},
			{Name: "note", Type: models.FieldTypeString, Nullable: true},
		},
	}
}

// PLAIN cells in the sealed orders.v1 profile have fixed widths or checked
// byte-array lengths. The only levels are the nullable note's V1 definition
// levels. The caller provides a row window capped before decompression.
func decodeParquetOrdersColumn(column int, data []byte, rows []parquetOrderRow) error {
	switch column {
	case 0, 1, 2, 3:
		return decodeParquetOrdersFixedColumn(column, data, rows)
	case 4:
		return decodeParquetOrdersTextColumn(data, rows, false)
	case 5:
		return decodeParquetOrdersTextColumn(data, rows, true)
	default:
		return errors.New("unknown orders.v1 Parquet column")
	}
}

func decodeParquetOrdersFixedColumn(column int, data []byte, rows []parquetOrderRow) error {
	width := 8
	if column == 2 {
		width = 4
	}
	if len(data) != width*len(rows) {
		return errors.New("invalid PLAIN fixed-width Parquet values")
	}
	for i := range rows {
		cell := data[i*width : (i+1)*width]
		switch column {
		case 0:
			rows[i].id = int64(binary.LittleEndian.Uint64(cell))
		case 1:
			value := int64(binary.BigEndian.Uint64(cell))
			if value <= -1_000_000_000_000_000_000 || value >= 1_000_000_000_000_000_000 {
				return errors.New("Parquet decimal exceeds precision 18")
			}
			rows[i].amount = value
		case 2:
			rows[i].date = int32(binary.LittleEndian.Uint32(cell))
		case 3:
			rows[i].orderedAt = int64(binary.LittleEndian.Uint64(cell))
		}
	}
	return nil
}

func decodeParquetOrdersTextColumn(data []byte, rows []parquetOrderRow, nullable bool) error {
	var present [stagedParquetMaxGroupRows]bool
	if nullable {
		if len(data) < 4 {
			return errors.New("missing Parquet definition levels")
		}
		size := uint64(binary.LittleEndian.Uint32(data))
		if size == 0 || size > uint64(len(data)-4) {
			return errors.New("invalid Parquet definition levels length")
		}
		levels := data[4 : 4+size]
		if err := decodeParquetNoteLevels(levels, present[:len(rows)]); err != nil {
			return err
		}
		data = data[4+size:]
	}
	for i := range rows {
		if nullable && !present[i] {
			continue
		}
		if len(data) < 4 {
			return errors.New("missing PLAIN Parquet string length")
		}
		size := uint64(binary.LittleEndian.Uint32(data))
		// PBS limits each source JSON record to 1 MiB. A source string
		// cannot be bigger than that record, or outgrow its page.
		if size > datasetFileRowsMaxLineBytes || size > uint64(len(data)-4) {
			return errors.New("invalid PLAIN Parquet string length")
		}
		value := data[4 : 4+size]
		if !utf8.Valid(value) {
			return errors.New("invalid UTF-8 in Parquet string")
		}
		text := string(value)
		data = data[4+size:]
		if nullable {
			rows[i].note = &text
		} else {
			rows[i].zone = text
		}
	}
	if len(data) != 0 {
		return errors.New("trailing PLAIN Parquet string values")
	}
	return nil
}

// Definition-level bit width is one for flat nullable note. Both hybrid
// run forms are bounded by the remaining page rows before multiplication
// or slicing. Only the final bit-packed group may contain padding.
func decodeParquetNoteLevels(data []byte, present []bool) error {
	for at := 0; at < len(present); {
		header, consumed := binary.Uvarint(data)
		run := header >> 1
		if consumed <= 0 || run == 0 {
			return errors.New("invalid Parquet definition level run")
		}
		data = data[consumed:]
		left := len(present) - at
		if header&1 == 0 {
			if run > uint64(left) || len(data) == 0 || data[0] > 1 {
				return errors.New("invalid RLE Parquet definition levels")
			}
			for i := range int(run) {
				present[at+i] = data[0] == 1
			}
			at += int(run)
			data = data[1:]
			continue
		}
		if run > uint64((left+7)/8) || run > uint64(len(data)) {
			return errors.New("invalid bit-packed Parquet definition levels")
		}
		values := min(int(run)*8, left)
		for i := range values {
			present[at+i] = data[i/8]&(1<<uint(i%8)) != 0
		}
		at += values
		data = data[run:]
	}
	if len(data) != 0 {
		return errors.New("trailing Parquet definition levels")
	}
	return nil
}

type parquetOrderJSON struct {
	OrderID        int64   `json:"order_id"`
	Amount         string  `json:"amount"`
	OrderDate      string  `json:"order_date"`
	OrderedAt      string  `json:"ordered_at"`
	SourceTimeZone string  `json:"source_time_zone"`
	Note           *string `json:"note"`
}

func (row parquetOrderRow) wire() (parquetOrderJSON, error) {
	date := time.Unix(int64(row.date)*86_400, 0).UTC()
	orderedAt := time.UnixMicro(row.orderedAt).UTC()
	if date.Year() < 0 || date.Year() > 9999 || orderedAt.Year() < 0 || orderedAt.Year() > 9999 {
		return parquetOrderJSON{}, errors.New("Parquet date or timestamp is outside the JSON year range")
	}
	amount, sign := row.amount, ""
	if amount < 0 {
		amount, sign = -amount, "-"
	}
	return parquetOrderJSON{
		OrderID: row.id, Amount: fmt.Sprintf("%s%d.%04d", sign, amount/10_000, amount%10_000),
		OrderDate: date.Format(time.DateOnly), OrderedAt: orderedAt.Format("2006-01-02T15:04:05.000000Z"),
		SourceTimeZone: row.zone, Note: row.note,
	}, nil
}
