package handlers

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"io"
	"slices"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/klauspost/compress/snappy"
)

const stagedParquetMaxPageHeaderBytes = 64 << 10

// The staged profile uses V1 data pages with PLAIN values and RLE levels.
// Page headers and Snappy blocks are checked in reusable bounded buffers.
// Publishing does not interpret column values.
type stagedParquetPageValidator struct {
	header  [stagedParquetMaxPageHeaderBytes]byte
	encoded []byte
	decoded []byte
}

func (v *stagedParquetPageValidator) validateColumn(ctx context.Context, src io.ReaderAt, offset, end, uncompressedBytes, rows int64) error {
	var actualRows, actualBytes int64
	for offset < end {
		if err := ctx.Err(); err != nil {
			return err
		}
		raw := v.header[:min(end-offset, int64(len(v.header)))]
		if _, err := src.ReadAt(raw, offset); err != nil {
			return err
		}
		page, headerLen, err := readStagedParquetPageHeader(ctx, raw)
		if err != nil {
			return err
		}
		encodedLen, decodedLen := int64(page.encoded), int64(page.decoded)
		if encodedLen <= 0 || encodedLen > stagedParquetMaxChunkBytes || encodedLen > end-offset-headerLen ||
			decodedLen <= 0 || decodedLen > uncompressedBytes-actualBytes-headerLen ||
			page.numValues <= 0 || int64(page.numValues) > rows-actualRows {
			return errors.New("invalid Parquet page sizes or record count")
		}
		v.encoded = slices.Grow(v.encoded[:0], int(encodedLen))[:encodedLen]
		if _, err := src.ReadAt(v.encoded, offset+headerLen); err != nil {
			return err
		}
		if page.crcSet && crc32.ChecksumIEEE(v.encoded) != uint32(page.crc) {
			return errors.New("incorrect Parquet page checksum")
		}
		n, err := snappy.DecodedLen(v.encoded)
		if err != nil || int64(n) != decodedLen {
			return errors.New("incorrect Parquet Snappy block length")
		}
		v.decoded = slices.Grow(v.decoded[:0], n)[:n]
		if _, err := snappy.Decode(v.decoded, v.encoded); err != nil {
			return errors.New("invalid Parquet Snappy block")
		}
		offset += headerLen + encodedLen
		actualBytes += headerLen + decodedLen
		actualRows += int64(page.numValues)
	}
	if actualRows != rows || actualBytes != uncompressedBytes {
		return errors.New("inconsistent Parquet column and pages")
	}
	return nil
}

type stagedParquetPageHeader struct {
	encoded, decoded, numValues, crc int32
	crcSet                           bool
}

// PageHeader and DataPageHeader use the public parquet.thrift field
// numbers. Arrow's generated PageHeader type is internal to Arrow.
func readStagedParquetPageHeader(ctx context.Context, raw []byte) (stagedParquetPageHeader, int64, error) {
	transport := &thrift.TMemoryBuffer{Buffer: bytes.NewBuffer(raw)}
	p := &stagedParquetProtocol{
		TProtocol:         thrift.NewTCompactProtocolConf(transport, &thrift.TConfiguration{MaxMessageSize: stagedParquetMaxPageHeaderBytes}),
		remainingElements: 64,
	}
	page := stagedParquetPageHeader{encoded: -1, decoded: -1, numValues: -1}
	pageType := int32(-1)
	err := readStagedParquetHeaderFields(ctx, p, func(id int16, kind thrift.TType) error {
		switch id {
		case 1:
			return readStagedParquetHeaderInt(ctx, p, kind, &pageType)
		case 2:
			return readStagedParquetHeaderInt(ctx, p, kind, &page.decoded)
		case 3:
			return readStagedParquetHeaderInt(ctx, p, kind, &page.encoded)
		case 4:
			page.crcSet = true
			return readStagedParquetHeaderInt(ctx, p, kind, &page.crc)
		case 5:
			if kind != thrift.STRUCT {
				return errors.New("invalid Parquet V1 data page header")
			}
			return readStagedParquetV1Page(ctx, p, &page.numValues)
		case 6, 7, 8:
			return errors.New("unsupported Parquet page header")
		default:
			return p.Skip(ctx, kind)
		}
	})
	if err != nil {
		return page, 0, err
	}
	const parquetDataPageV1Type = 0 // PageType.DATA_PAGE in parquet.thrift
	if pageType != parquetDataPageV1Type || page.numValues < 1 {
		return page, 0, errors.New("expected a nonempty V1 Parquet data page")
	}
	return page, int64(len(raw) - transport.Len()), nil
}

func readStagedParquetV1Page(ctx context.Context, p thrift.TProtocol, numValues *int32) error {
	encoding, definition, repetition := int32(-1), int32(-1), int32(-1)
	err := readStagedParquetHeaderFields(ctx, p, func(id int16, kind thrift.TType) error {
		switch id {
		case 1:
			return readStagedParquetHeaderInt(ctx, p, kind, numValues)
		case 2:
			return readStagedParquetHeaderInt(ctx, p, kind, &encoding)
		case 3:
			return readStagedParquetHeaderInt(ctx, p, kind, &definition)
		case 4:
			return readStagedParquetHeaderInt(ctx, p, kind, &repetition)
		default:
			return p.Skip(ctx, kind)
		}
	})
	if err != nil {
		return err
	}
	if parquet.Encoding(encoding) != parquet.Encodings.Plain || parquet.Encoding(definition) != parquet.Encodings.RLE ||
		parquet.Encoding(repetition) != parquet.Encodings.RLE {
		return errors.New("unsupported Parquet data page encodings")
	}
	return nil
}

func readStagedParquetHeaderFields(ctx context.Context, p thrift.TProtocol, read func(int16, thrift.TType) error) error {
	if _, err := p.ReadStructBegin(ctx); err != nil {
		return err
	}
	var lastID int16
	for {
		_, kind, id, err := p.ReadFieldBegin(ctx)
		if err != nil {
			return err
		}
		if kind == thrift.STOP {
			return p.ReadStructEnd(ctx)
		}
		if id <= lastID {
			return errors.New("out-of-order or repeated Parquet page header field")
		}
		lastID = id
		if err := read(id, kind); err != nil {
			return err
		}
		if err := p.ReadFieldEnd(ctx); err != nil {
			return err
		}
	}
}

func readStagedParquetHeaderInt(ctx context.Context, p thrift.TProtocol, kind thrift.TType, value *int32) error {
	if kind != thrift.I32 {
		return errors.New("invalid Parquet page integer field")
	}
	n, err := p.ReadI32(ctx)
	*value = n
	return err
}
