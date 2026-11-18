package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/thrift/lib/go/thrift"
)

const (
	stagedParquetFormat        = "parquet"
	stagedParquetExtension     = ".parquet"
	stagedParquetContentType   = "application/vnd.apache.parquet"
	stagedParquetSchemaKey     = "openfoundry.schema_version"
	stagedParquetSchemaVersion = "orders.v1"

	stagedParquetMagic          = "PAR1"
	stagedParquetTrailerBytes   = 8
	stagedParquetMaxFooterBytes = 8 << 20
	stagedParquetMaxRowGroups   = 4096
	stagedParquetMaxGroupRows   = 1024
	stagedParquetMaxChunkBytes  = 8 << 20
)

var errStagedParquetInvalid = errors.New("invalid staged Parquet file")

func validStagedParquetPath(logical string) bool {
	return logical == strings.TrimSpace(logical) && strings.HasSuffix(logical, stagedParquetExtension) &&
		path.Base(logical) != stagedParquetExtension && validScanStagedPath(logical)
}

func (h *Handlers) streamStageParquetBody(w http.ResponseWriter, r *http.Request, sink io.Writer) (string, int64, int64, error) {
	body := http.MaxBytesReader(w, r.Body, h.stagedOutputMaxBytes())
	hasher := sha256.New()
	out := io.MultiWriter(sink, hasher)
	var size int64
	buf := make([]byte, 64<<10)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, writeErr := out.Write(buf[:n]); writeErr != nil {
				writeJSONErr(w, http.StatusInternalServerError, "failed to write staging spool")
				return "", 0, 0, writeErr
			}
			size += int64(n)
		}
		if errors.Is(err, io.EOF) {
			return hex.EncodeToString(hasher.Sum(nil)), size, 0, nil
		}
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				err = fmt.Errorf("%w: %d bytes", errStagedBodyTooLarge, h.stagedOutputMaxBytes())
				writeStageError(w, err, http.StatusRequestEntityTooLarge, "STAGED_BODY_TOO_LARGE")
			} else {
				writeJSONErr(w, http.StatusBadRequest, "invalid staged body: "+err.Error())
			}
			return "", 0, 0, err
		}
	}
}

// Arrow's file reader allocates the declared footer size, and its generated
// metadata decoder allocates each compact-Thrift list before reading the
// elements. Bound both at the source, then validate bounded pages within
// each column's verified ranges.
func inspectStagedParquet(ctx context.Context, src io.ReaderAt, size, maxRecords int64) (int64, error) {
	const headerBytes = int64(len(stagedParquetMagic))
	if size <= headerBytes+stagedParquetTrailerBytes {
		return 0, errors.New("missing Parquet header or footer")
	}
	var magic [headerBytes]byte
	if _, err := src.ReadAt(magic[:], 0); err != nil {
		return 0, err
	}
	var trailer [stagedParquetTrailerBytes]byte
	if _, err := src.ReadAt(trailer[:], size-stagedParquetTrailerBytes); err != nil {
		return 0, err
	}
	if string(magic[:]) != stagedParquetMagic || string(trailer[4:]) != stagedParquetMagic {
		return 0, errors.New("expected unencrypted Parquet magic")
	}
	footerLen := int64(binary.LittleEndian.Uint32(trailer[:4]))
	footerOffset := size - stagedParquetTrailerBytes - footerLen
	if footerLen == 0 || footerLen > stagedParquetMaxFooterBytes || footerOffset < headerBytes {
		return 0, errors.New("invalid Parquet footer length")
	}
	data := make([]byte, footerLen)
	if _, err := src.ReadAt(data, footerOffset); err != nil {
		return 0, err
	}

	transport := &thrift.TMemoryBuffer{Buffer: bytes.NewBuffer(data)}
	decoder := &stagedParquetProtocol{
		TProtocol:         thrift.NewTCompactProtocolConf(transport, &thrift.TConfiguration{MaxMessageSize: stagedParquetMaxFooterBytes}),
		remainingElements: 256 << 10,
	}
	var arrowMeta metadata.FileMetaData
	arrowMeta.FileMetaData = newStagedParquetThriftValue(arrowMeta.FileMetaData)
	footer := arrowMeta.FileMetaData
	if err := footer.Read(ctx, decoder); err != nil {
		return 0, err
	}
	if transport.Len() != 0 || footer.Version != 2 || footer.IsSetEncryptionAlgorithm() || footer.IsSetFooterSigningKeyMetadata() {
		return 0, errors.New("invalid or encrypted Parquet metadata")
	}
	if footer.NumRows < 0 {
		return 0, errors.New("negative Parquet record count")
	}
	if footer.NumRows > maxRecords {
		return 0, fmt.Errorf("%w: %d", errStagedRecordCountLimit, maxRecords)
	}
	if !validStagedOrdersSchema(&arrowMeta) {
		return 0, errors.New("expected Parquet schema " + stagedParquetSchemaVersion)
	}

	var records int64
	var pages stagedParquetPageValidator
	nextOffset := headerBytes
	for i, group := range footer.RowGroups {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if group.NumRows <= 0 || group.NumRows > stagedParquetMaxGroupRows ||
			group.NumRows > footer.NumRows-records || len(group.Columns) != len(footer.Schema)-1 {
			return 0, errors.New("invalid Parquet row group")
		}
		if (group.IsSetFileOffset() && group.GetFileOffset() != nextOffset) ||
			(group.IsSetOrdinal() && int(group.GetOrdinal()) != i) {
			return 0, errors.New("invalid Parquet row group offset or ordinal")
		}
		groupStart := nextOffset
		var uncompressed int64
		for j, chunk := range group.Columns {
			col := chunk.MetaData
			field := footer.Schema[j+1]
			if chunk.GetFilePath() != "" || chunk.IsSetCryptoMetadata() || chunk.IsSetEncryptedColumnMetadata() ||
				chunk.IsSetOffsetIndexOffset() || chunk.IsSetOffsetIndexLength() ||
				chunk.IsSetColumnIndexOffset() || chunk.IsSetColumnIndexLength() ||
				col == nil || col.IsSetDictionaryPageOffset() || col.IsSetIndexPageOffset() ||
				col.IsSetBloomFilterOffset() || col.IsSetBloomFilterLength() ||
				compress.Compression(col.Codec) != compress.Codecs.Snappy ||
				col.Type != field.GetType() || len(col.PathInSchema) != 1 || col.PathInSchema[0] != field.Name ||
				col.NumValues != group.NumRows || col.TotalUncompressedSize <= 0 ||
				len(col.Encodings) != 2 || parquet.Encoding(col.Encodings[0]) != parquet.Encodings.Plain ||
				parquet.Encoding(col.Encodings[1]) != parquet.Encodings.RLE ||
				col.TotalUncompressedSize > stagedParquetMaxChunkBytes {
				return 0, errors.New("invalid Parquet column metadata")
			}
			if col.DataPageOffset != nextOffset || col.TotalCompressedSize <= 0 ||
				col.TotalCompressedSize > footerOffset-nextOffset {
				return 0, errors.New("invalid Parquet column ranges")
			}
			end := nextOffset + col.TotalCompressedSize
			if err := pages.validateColumn(ctx, src, nextOffset, end, col.TotalUncompressedSize, group.NumRows); err != nil {
				return 0, err
			}
			nextOffset = end
			uncompressed += col.TotalUncompressedSize
		}
		if group.TotalByteSize != uncompressed || (group.IsSetTotalCompressedSize() && group.GetTotalCompressedSize() != nextOffset-groupStart) {
			return 0, errors.New("inconsistent Parquet column and row group sizes")
		}
		records += group.NumRows
	}
	if records != footer.NumRows || nextOffset != footerOffset {
		return 0, errors.New("inconsistent Parquet row groups and file size")
	}
	return records, nil
}

// The generated Thrift type is exposed as a field of Arrow FileMetaData.
// Construct it without invoking Arrow's unbounded footer or schema parser.
func newStagedParquetThriftValue[T any](_ *T) *T { return new(T) }

type stagedParquetProtocol struct {
	thrift.TProtocol
	remainingElements int
}

func (p *stagedParquetProtocol) ReadListBegin(ctx context.Context) (thrift.TType, int, error) {
	kind, n, err := p.TProtocol.ReadListBegin(ctx)
	if err != nil {
		return kind, n, err
	}
	// Each real list element occupies at least one byte even when it is
	// an empty struct. The aggregate budget also bounds nested lists.
	if n > stagedParquetMaxRowGroups || n > p.remainingElements || uint64(n) > p.Transport().RemainingBytes() {
		return kind, 0, errors.New("invalid Parquet metadata list length")
	}
	p.remainingElements -= n
	return kind, n, nil
}

func (p *stagedParquetProtocol) Skip(ctx context.Context, kind thrift.TType) error {
	return thrift.Skip(ctx, p, kind, 16)
}

func (p *stagedParquetProtocol) ReadMapBegin(context.Context) (thrift.TType, thrift.TType, int, error) {
	return thrift.STOP, thrift.STOP, 0, errors.New("unexpected map in Parquet metadata")
}

func (p *stagedParquetProtocol) ReadSetBegin(context.Context) (thrift.TType, int, error) {
	return thrift.STOP, 0, errors.New("unexpected set in Parquet metadata")
}

func validStagedOrdersSchema(meta *metadata.FileMetaData) bool {
	expected := [...]struct {
		name      string
		physical  parquet.Type
		converted schema.ConvertedType
	}{
		{"order_id", parquet.Types.Int64, schema.ConvertedTypes.Int64},
		{"amount", parquet.Types.FixedLenByteArray, schema.ConvertedTypes.Decimal},
		{"order_date", parquet.Types.Int32, schema.ConvertedTypes.Date},
		{"ordered_at", parquet.Types.Int64, schema.ConvertedTypes.TimestampMicros},
		{"source_time_zone", parquet.Types.ByteArray, schema.ConvertedTypes.UTF8},
		{"note", parquet.Types.ByteArray, schema.ConvertedTypes.UTF8},
	}
	raw := meta.FileMetaData.Schema
	if len(raw) != 1+len(expected) || raw[0].IsSetType() || raw[0].GetNumChildren() != int32(len(expected)) ||
		raw[0].IsSetLogicalType() || raw[0].IsSetConvertedType() ||
		(meta.IsSetColumnOrders() && len(meta.ColumnOrders) != len(expected)) {
		return false
	}
	for i, want := range expected {
		field := raw[i+1]
		repetition := parquet.Repetitions.Required
		if want.name == "note" {
			repetition = parquet.Repetitions.Optional
		}
		if field.Name != want.name || field.GetNumChildren() != 0 || !field.IsSetType() ||
			parquet.Type(field.GetType()) != want.physical || !field.IsSetRepetitionType() ||
			parquet.Repetition(field.GetRepetitionType()) != repetition || !field.IsSetConvertedType() ||
			schema.ConvertedType(field.GetConvertedType()) != want.converted ||
			field.LogicalType == nil || field.LogicalType.CountSetFieldsLogicalType() != 1 {
			return false
		}
		// Check the exact Thrift annotations. Arrow's generic schema
		// converter can panic on other logical types or malformed units.
		logical := field.LogicalType
		switch want.converted {
		case schema.ConvertedTypes.Int64:
			if logical.INTEGER == nil || logical.INTEGER.GetBitWidth() != 64 || !logical.INTEGER.GetIsSigned() {
				return false
			}
		case schema.ConvertedTypes.Decimal:
			if logical.DECIMAL == nil || logical.DECIMAL.GetPrecision() != 18 || logical.DECIMAL.GetScale() != 4 ||
				field.GetTypeLength() != 8 || field.GetPrecision() != 18 || field.GetScale() != 4 {
				return false
			}
		case schema.ConvertedTypes.Date:
			if logical.DATE == nil {
				return false
			}
		case schema.ConvertedTypes.TimestampMicros:
			ts := logical.TIMESTAMP
			if ts == nil || !ts.GetIsAdjustedToUTC() || ts.Unit == nil ||
				ts.Unit.GetMICROS() == nil || ts.Unit.CountSetFieldsTimeUnit() != 1 {
				return false
			}
		case schema.ConvertedTypes.UTF8:
			if logical.STRING == nil {
				return false
			}
		}
	}
	hasSchemaVersion := false
	for _, kv := range meta.FileMetaData.KeyValueMetadata {
		if kv.Key == stagedParquetSchemaKey {
			if hasSchemaVersion || kv.GetValue() != stagedParquetSchemaVersion {
				return false
			}
			hasSchemaVersion = true
		}
	}
	return hasSchemaVersion
}
