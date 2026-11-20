// Command data-integration-wp3-12 writes the standalone WP3.12 Parquet fixtures.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	schemaVersion   = "orders.v1"
	amountPrecision = 18
	amountScale     = 4
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
		log.Fatal("usage: go run ./tools/smoke/data-integration-wp3-12 INPUT_JSON OUTPUT_DIRECTORY")
	}
	if err := generate(os.Args[1], os.Args[2]); err != nil {
		log.Fatal(err)
	}
}

func generate(inputPath, outputDir string) error {
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
	metadata := arrow.NewMetadata(
		[]string{"openfoundry.fixture.schema_version", "openfoundry.fixture.input_sha256"},
		[]string{schemaVersion, fmt.Sprintf("%x", sha256.Sum256(raw))},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "order_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: amountPrecision, Scale: amountScale}},
		{Name: "order_date", Type: arrow.FixedWidthTypes.Date32},
		{Name: "ordered_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "source_time_zone", Type: arrow.BinaryTypes.String},
		{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
	}, &metadata)

	record, err := buildOrders(schema, input.Orders)
	if err != nil {
		return err
	}
	defer record.Release()

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return err
	}
	if err := writeParquet(filepath.Join(outputDir, "orders.parquet"), record); err != nil {
		return err
	}
	empty := record.NewSlice(0, 0)
	defer empty.Release()
	return writeParquet(filepath.Join(outputDir, "orders-empty.parquet"), empty)
}

func buildOrders(schema *arrow.Schema, orders []order) (arrow.RecordBatch, error) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	ids := builder.Field(0).(*array.Int64Builder)
	amounts := builder.Field(1).(*array.Decimal128Builder)
	dates := builder.Field(2).(*array.Date32Builder)
	timestamps := builder.Field(3).(*array.TimestampBuilder)
	zones := builder.Field(4).(*array.StringBuilder)
	notes := builder.Field(5).(*array.StringBuilder)

	for _, row := range orders {
		// Read the scaled integer directly so the fixture never rounds through a float.
		whole, fraction, ok := strings.Cut(row.Amount, ".")
		scaled, err := strconv.ParseInt(whole+fraction, 10, 64)
		amount := decimal128.FromI64(scaled)
		if !ok || len(fraction) != amountScale || err != nil || !amount.FitsInPrecision(amountPrecision) {
			return nil, fmt.Errorf("order %d: invalid DECIMAL(%d,%d) amount %q", row.ID, amountPrecision, amountScale, row.Amount)
		}
		date, err := time.Parse(time.DateOnly, row.Date)
		if err != nil {
			return nil, fmt.Errorf("order %d: %w", row.ID, err)
		}
		orderedAt, err := time.Parse(time.RFC3339Nano, row.OrderedAt)
		if err != nil {
			return nil, fmt.Errorf("order %d: %w", row.ID, err)
		}
		if orderedAt.Nanosecond()%1000 != 0 {
			return nil, fmt.Errorf("order %d: timestamp %q is not microsecond-aligned", row.ID, row.OrderedAt)
		}

		ids.Append(row.ID)
		amounts.Append(amount)
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

func writeParquet(path string, record arrow.RecordBatch) error {
	var data bytes.Buffer
	writer, err := pqarrow.NewFileWriter(record.Schema(), &data, parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_6),
		parquet.WithDataPageVersion(parquet.DataPageV1),
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(false),
		parquet.WithStoreDecimalAsInteger(false),
		parquet.WithMaxRowGroupLength(1024),
	), pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
		pqarrow.WithDeprecatedInt96Timestamps(false),
	))
	if err != nil {
		return err
	}
	// Close the writer without emitting a row group for a typed empty file.
	if record.NumRows() > 0 {
		err = writer.Write(record)
	}
	if err := errors.Join(err, writer.Close()); err != nil {
		return err
	}
	return os.WriteFile(path, data.Bytes(), 0o644)
}
