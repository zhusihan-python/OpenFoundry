// Command data-integration-wp3-6 writes the WP3.6 incompatible Parquet fixture.
//
// The fixture holds the WP3.12 orders rows, written with the orders.v1
// reference writer properties, but its amount column is a string. Every
// other column keeps the orders.v1 type and nullability, so the committed
// schema differs from orders.v1 in exactly that field.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type order struct {
	ID             int64   `json:"order_id"`
	Amount         string  `json:"amount"`
	Date           string  `json:"order_date"`
	OrderedAt      string  `json:"ordered_at"`
	SourceTimeZone string  `json:"source_time_zone"`
	Note           *string `json:"note"`
}

func main() {
	log.SetFlags(0)
	if len(os.Args) != 3 {
		log.Fatal("usage: go run ./tools/smoke/data-integration-wp3-6 INPUT_JSON OUTPUT_PARQUET")
	}
	if err := generate(os.Args[1], os.Args[2]); err != nil {
		log.Fatal(err)
	}
}

func generate(inputPath, outputPath string) error {
	raw, err := os.ReadFile(inputPath)
	if err != nil {
		return err
	}
	var input struct {
		Orders []order `json:"orders"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return err
	}
	// Mirrors the orders.v1 reference schema; only amount is a string.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "order_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "amount", Type: arrow.BinaryTypes.String},
		{Name: "order_date", Type: arrow.FixedWidthTypes.Date32},
		{Name: "ordered_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "source_time_zone", Type: arrow.BinaryTypes.String},
		{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	record, err := buildOrders(schema, input.Orders)
	if err != nil {
		return err
	}
	defer record.Release()

	var data bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &data, parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_6),
		parquet.WithDataPageVersion(parquet.DataPageV1),
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(false),
		parquet.WithMaxRowGroupLength(1024),
	), pqarrow.NewArrowWriterProperties(
		pqarrow.WithDeprecatedInt96Timestamps(false),
	))
	if err != nil {
		return err
	}
	if err := errors.Join(writer.Write(record), writer.Close()); err != nil {
		return err
	}
	return os.WriteFile(outputPath, data.Bytes(), 0o644)
}

func buildOrders(schema *arrow.Schema, orders []order) (arrow.RecordBatch, error) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	ids := builder.Field(0).(*array.Int64Builder)
	amounts := builder.Field(1).(*array.StringBuilder)
	dates := builder.Field(2).(*array.Date32Builder)
	timestamps := builder.Field(3).(*array.TimestampBuilder)
	zones := builder.Field(4).(*array.StringBuilder)
	notes := builder.Field(5).(*array.StringBuilder)

	for _, row := range orders {
		if row.Amount == "" {
			return nil, fmt.Errorf("order %d: empty amount", row.ID)
		}
		date, err := time.Parse(time.DateOnly, row.Date)
		if err != nil {
			return nil, fmt.Errorf("order %d: %w", row.ID, err)
		}
		orderedAt, err := time.Parse(time.RFC3339Nano, row.OrderedAt)
		if err != nil {
			return nil, fmt.Errorf("order %d: %w", row.ID, err)
		}
		ids.Append(row.ID)
		amounts.Append(row.Amount)
		dates.Append(arrow.Date32FromTime(date))
		timestamps.Append(arrow.Timestamp(orderedAt.UnixMicro()))
		zones.Append(row.SourceTimeZone)
		if row.Note == nil {
			notes.AppendNull()
		} else {
			notes.Append(*row.Note)
		}
	}
	return builder.NewRecordBatch(), nil
}
