package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	batchedParquetFormat      = "parquet"
	batchedParquetContentType = "application/vnd.apache.parquet"
	ordersSchemaVersion       = "orders.v1"
	ordersAmountPrecision     = 18
	ordersAmountScale         = 4
	ordersBatchRows           = 1024
	ordersBatchBytes          = 4 << 20
	ordersMaxRowGroups        = 4096
)

// Before Parquet opt-in, the JobSpec grammar only accepted .ndjson.
// The frozen logical path therefore identifies the selected format even
// after a restart. orders.v1 is the only accepted Parquet schema.
func batchedParquetPath(logicalPath string) bool {
	return strings.HasSuffix(logicalPath, ".parquet") && path.Base(logicalPath) != ".parquet"
}

// Keep the column order, types, and encoding properties from the WP3.12
// interoperability fixture (tools/smoke/data-integration-wp3-12).
func ordersParquetSchema() *arrow.Schema {
	metadata := arrow.NewMetadata([]string{"openfoundry.schema_version"}, []string{ordersSchemaVersion})
	return arrow.NewSchema([]arrow.Field{
		{Name: "order_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: ordersAmountPrecision, Scale: ordersAmountScale}},
		{Name: "order_date", Type: arrow.FixedWidthTypes.Date32},
		{Name: "ordered_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "source_time_zone", Type: arrow.BinaryTypes.String},
		{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
	}, &metadata)
}

func streamOrdersParquet(ctx context.Context, w io.Writer, digest hash.Hash, maxRecord int64, scan func(func([]byte) error) error) (int64, int64, error) {
	sink := &parquetStageSink{ctx: ctx, w: io.MultiWriter(w, digest)}
	schema := ordersParquetSchema()
	writer, err := pqarrow.NewFileWriter(schema, sink, parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_6),
		parquet.WithDataPageVersion(parquet.DataPageV1),
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(false),
		parquet.WithStoreDecimalAsInteger(false),
		parquet.WithMaxRowGroupLength(ordersBatchRows),
	), pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
		pqarrow.WithDeprecatedInt96Timestamps(false),
	))
	if err != nil {
		return 0, 0, fmt.Errorf("batched_parquet_writer: %w", err)
	}
	defer func() {
		// Arrow has no abort API. Detach the stage sink before disposing
		// of the writer on any path that did not finish successfully.
		sink.w = nil
		_ = writer.Close()
	}()

	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	var batchBytes, groups int
	var rows int64
	flushBatch := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if batchBytes == 0 {
			return nil
		}
		if groups == ordersMaxRowGroups {
			return fmt.Errorf("batched_parquet_group_limit: exceeded %d row groups", ordersMaxRowGroups)
		}
		record := builder.NewRecordBatch()
		defer record.Release()
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("batched_parquet_batch: %w", err)
		}
		groups++
		batchBytes = 0
		return sink.err
	}
	err = scan(func(record []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Raw bytes bound strings and all other data decoded into Arrow.
		// The row bound covers the per-row column buffers and overhead.
		if int64(len(record))+1 > maxRecord || len(record) > ordersBatchBytes {
			return fmt.Errorf("batched_record_too_large: orders.v1 row %d exceeds the record or batch byte limit", rows+1)
		}
		row, err := decodeParquetOrder(record)
		if err != nil {
			return fmt.Errorf("batched_record_invalid: orders.v1 row %d: %w", rows+1, err)
		}
		if builder.Field(0).Len() == ordersBatchRows || batchBytes+len(record) > ordersBatchBytes {
			if err := flushBatch(); err != nil {
				return err
			}
		}
		row.appendTo(builder)
		batchBytes += len(record)
		rows++
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if err := flushBatch(); err != nil {
		return 0, 0, err
	}
	// With no rows, Close emits a typed file with zero row groups.
	if err := writer.Close(); err != nil {
		return 0, 0, fmt.Errorf("batched_parquet_footer: %w", err)
	}
	if err := sink.finish(); err != nil {
		return 0, 0, err
	}
	return sink.size, rows, nil
}

// parquetStageSink holds the trailing magic until the entire scan and writer
// finish. Arrow itself may call Close after a column write error. IO errors
// are latched separately so Arrow can release the active group's buffers
// even if the pipe has closed.
type parquetStageSink struct {
	ctx     context.Context
	w       io.Writer
	size    int64
	tail    [4]byte
	tailLen int
	err     error
}

func (s *parquetStageSink) Write(p []byte) (int, error) {
	length := len(p)
	if s.err != nil || s.w == nil {
		return length, nil
	}
	if s.err = s.ctx.Err(); s.err != nil {
		return length, nil
	}
	if ready := s.tailLen + length - len(s.tail); ready > 0 {
		fromTail := min(ready, s.tailLen)
		s.emit(s.tail[:fromTail])
		fromInput := ready - fromTail
		s.emit(p[:fromInput])
		s.tailLen = copy(s.tail[:], s.tail[fromTail:s.tailLen])
		p = p[fromInput:]
	}
	s.tailLen += copy(s.tail[s.tailLen:], p)
	return length, nil
}

func (s *parquetStageSink) emit(p []byte) {
	if len(p) == 0 || s.err != nil {
		return
	}
	n, err := s.w.Write(p)
	s.size += int64(n)
	s.err = err
}

func (s *parquetStageSink) finish() error {
	if s.err != nil {
		return s.err
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if s.tailLen != len(s.tail) || string(s.tail[:]) != "PAR1" {
		return errors.New("batched_parquet_footer: missing trailing magic")
	}
	s.emit(s.tail[:])
	return s.err
}

type parquetOrder struct {
	id        int64
	amount    decimal128.Num
	date      arrow.Date32
	orderedAt arrow.Timestamp
	zone      string
	note      *string
}

func (row parquetOrder) appendTo(builder *array.RecordBuilder) {
	builder.Field(0).(*array.Int64Builder).Append(row.id)
	builder.Field(1).(*array.Decimal128Builder).Append(row.amount)
	builder.Field(2).(*array.Date32Builder).Append(row.date)
	builder.Field(3).(*array.TimestampBuilder).Append(row.orderedAt)
	builder.Field(4).(*array.StringBuilder).Append(row.zone)
	note := builder.Field(5).(*array.StringBuilder)
	if row.note == nil {
		note.AppendNull()
	} else {
		note.Append(*row.note)
	}
}

var (
	ordersAmountPattern    = regexp.MustCompile(`^[+-]?[0-9]+\.[0-9]{4}$`)
	ordersTimestampPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$`)
)

func decodeParquetOrder(record []byte) (parquetOrder, error) {
	if !validOrderUnicode(record) {
		return parquetOrder{}, errors.New("row contains invalid UTF-8 or an unpaired Unicode surrogate")
	}
	dec := json.NewDecoder(bytes.NewReader(record))
	dec.UseNumber()
	if token, err := dec.Token(); err != nil || token != json.Delim('{') {
		return parquetOrder{}, errors.New("row must be a JSON object")
	}
	fields := make(map[string]any, 6)
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return parquetOrder{}, errors.New("invalid row field name")
		}
		name := key.(string)
		switch name {
		case "order_id", "amount", "order_date", "ordered_at", "source_time_zone", "note":
		default:
			return parquetOrder{}, errors.New("unexpected field in orders.v1 row")
		}
		if _, exists := fields[name]; exists {
			return parquetOrder{}, fmt.Errorf("duplicate %s", name)
		}
		value, err := dec.Token()
		if err != nil {
			return parquetOrder{}, fmt.Errorf("invalid %s value", name)
		}
		if _, nested := value.(json.Delim); nested {
			return parquetOrder{}, fmt.Errorf("%s must be a scalar value", name)
		}
		fields[name] = value
	}
	if token, err := dec.Token(); err != nil || token != json.Delim('}') || len(fields) != 6 {
		return parquetOrder{}, errors.New("orders.v1 row must provide all six fields")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return parquetOrder{}, errors.New("unexpected content after orders.v1 row")
	}

	idToken, ok := fields["order_id"].(json.Number)
	id, err := idToken.Int64()
	if !ok || err != nil {
		return parquetOrder{}, errors.New("order_id must be a signed 64-bit JSON integer")
	}
	rawAmount, _ := fields["amount"].(string)
	whole, fraction, _ := strings.Cut(rawAmount, ".")
	scaled, err := strconv.ParseInt(whole+fraction, 10, 64)
	amount := decimal128.FromI64(scaled)
	if !ordersAmountPattern.MatchString(rawAmount) || err != nil || !amount.FitsInPrecision(ordersAmountPrecision) {
		return parquetOrder{}, errors.New("invalid DECIMAL(18,4) amount")
	}
	rawDate, _ := fields["order_date"].(string)
	date, err := time.Parse(time.DateOnly, rawDate)
	if err != nil || date.Format(time.DateOnly) != rawDate {
		return parquetOrder{}, errors.New("order_date must be a valid YYYY-MM-DD string")
	}
	rawTime, _ := fields["ordered_at"].(string)
	orderedAt, err := time.Parse(time.RFC3339Nano, rawTime)
	if !ordersTimestampPattern.MatchString(rawTime) || err != nil || orderedAt.Nanosecond()%1000 != 0 {
		return parquetOrder{}, errors.New("ordered_at must be a valid microsecond-aligned RFC3339 timestamp")
	}
	zone, ok := fields["source_time_zone"].(string)
	if !ok {
		return parquetOrder{}, errors.New("source_time_zone must be a string")
	}
	var note *string
	if fields["note"] != nil {
		value, ok := fields["note"].(string)
		if !ok {
			return parquetOrder{}, errors.New("note must be a string or null")
		}
		note = &value
	}
	return parquetOrder{
		id: id, amount: amount, date: arrow.Date32FromTime(date),
		orderedAt: arrow.Timestamp(orderedAt.UnixMicro()), zone: zone, note: note,
	}, nil
}

// encoding/json replaces malformed UTF-8 and unpaired surrogate escapes
// with U+FFFD. Reject them before decoding to avoid silently changing data.
func validOrderUnicode(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	for i := 0; i+1 < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		code, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil || (code >= 0xDC00 && code <= 0xDFFF) {
			return false
		}
		i += 4
		if code >= 0xD800 && code <= 0xDBFF {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			pair, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if err != nil || pair < 0xDC00 || pair > 0xDFFF {
				return false
			}
			i += 6
		}
	}
	return true
}
