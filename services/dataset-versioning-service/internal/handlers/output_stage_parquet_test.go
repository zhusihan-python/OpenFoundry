package handlers_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

const testParquetMIME = "application/vnd.apache.parquet"

func makeStagedOrdersParquet(t *testing.T, groupCount int, schemaVersion string) []byte {
	t.Helper()
	return makeOrdersParquetTestFile(t, groupCount, schemaVersion, func(b *array.RecordBuilder, _ int) {
		appendOrdersParquetTestRow(b, 9_007_199_254_740_993, 32100, 19782, 1709144400123456, "Asia/Kathmandu", nil)
	})
}

func appendOrdersParquetTestRow(b *array.RecordBuilder, id, amount int64, day arrow.Date32, at arrow.Timestamp, zone string, note *string) {
	b.Field(0).(*array.Int64Builder).Append(id)
	b.Field(1).(*array.Decimal128Builder).Append(decimal128.FromI64(amount))
	b.Field(2).(*array.Date32Builder).Append(day)
	b.Field(3).(*array.TimestampBuilder).Append(at)
	b.Field(4).(*array.StringBuilder).Append(zone)
	if note == nil {
		b.Field(5).(*array.StringBuilder).AppendNull()
	} else {
		b.Field(5).(*array.StringBuilder).Append(*note)
	}
}

func makeOrdersParquetTestFile(t *testing.T, groupCount int, schemaVersion string, appendGroup func(*array.RecordBuilder, int)) []byte {
	t.Helper()
	kv := arrow.NewMetadata([]string{"openfoundry.schema_version"}, []string{schemaVersion})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "order_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 18, Scale: 4}},
		{Name: "order_date", Type: arrow.FixedWidthTypes.Date32},
		{Name: "ordered_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "source_time_zone", Type: arrow.BinaryTypes.String},
		{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
	}, &kv)
	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(schema, &buf, parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_6),
		parquet.WithDataPageVersion(parquet.DataPageV1),
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(false),
		parquet.WithStoreDecimalAsInteger(false),
		parquet.WithMaxRowGroupLength(1024),
	), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	require.NoError(t, err)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i := range groupCount {
		appendGroup(b, i)
		record := b.NewRecordBatch()
		err := w.Write(record)
		record.Release()
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func (fx *stageFixture) stageParquet(body []byte, mime string, overrides map[string]string) *httptest.ResponseRecorder {
	q := map[string]string{"format": "parquet", "logical_path": "part-00000.parquet"}
	for key, value := range overrides {
		q[key] = value
	}
	req := httptest.NewRequest(http.MethodPost, fx.stageURL(q), bytes.NewReader(body))
	req.Header.Set("Content-Type", mime)
	req = withRouteParam(req, "rid", fx.rid)
	req = req.WithContext(authmw.ContextWithClaims(req.Context(), fx.claims))
	rec := httptest.NewRecorder()
	fx.h.StageDatasetOutput(rec, req)
	return rec
}

func (fx *stageFixture) parquetCommit(staged models.StageDatasetOutputResponse, contentType string) *httptest.ResponseRecorder {
	body := stagedCommitBody(staged.IdempotencyKey, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, fx.reservation, fx.build, func(m map[string]any) {
		file := m["files"].([]map[string]any)[0]
		file["logical_path"] = staged.LogicalPath
		if contentType != "" {
			file["content_type"] = contentType
		}
		m["metadata"] = map[string]any{"format": "ndjson", "schema_version": "forged", "record_count": -1}
	})
	return do(fx.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+fx.rid+"/outputs:commit", body, map[string]string{"rid": fx.rid}, fx.claims)
}

func TestStagedParquetSealAndPublish(t *testing.T) {
	for _, groups := range []int{0, 2} {
		t.Run(map[int]string{0: "empty", 2: "two-row-groups"}[groups], func(t *testing.T) {
			fx := newStageFixture(t)
			body := makeStagedOrdersParquet(t, groups, "orders.v1")
			first := fx.stageParquet(body, testParquetMIME, nil)
			require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
			var sealed models.StageDatasetOutputResponse
			require.NoError(t, json.Unmarshal(first.Body.Bytes(), &sealed))
			assert.Equal(t, "parquet", sealed.Format)
			assert.Equal(t, int64(groups), sealed.RecordCount)
			assert.Equal(t, int64(len(body)), sealed.SizeBytes)
			assert.Equal(t, sha256Hex(body), sealed.SHA256)

			replay := fx.stageParquet(body, testParquetMIME, nil)
			require.Equal(t, http.StatusOK, replay.Code, replay.Body.String())
			assert.Equal(t, first.Body.String(), replay.Body.String())
			different := makeStagedOrdersParquet(t, groups+1, "orders.v1")
			require.Equal(t, http.StatusConflict, fx.stageParquet(different, testParquetMIME, nil).Code)

			before := fx.store.datasets[0].CurrentVersion
			for _, mime := range []string{"", "application/x-ndjson"} {
				bad := fx.parquetCommit(sealed, mime)
				require.Equal(t, http.StatusBadRequest, bad.Code, bad.Body.String())
				assert.Equal(t, before, fx.store.datasets[0].CurrentVersion)
			}
			result := fx.parquetCommit(sealed, testParquetMIME)
			require.Equal(t, http.StatusCreated, result.Code, result.Body.String())
			var published models.CommitDatasetOutputResponse
			require.NoError(t, json.Unmarshal(result.Body.Bytes(), &published))
			require.Len(t, published.Files, 1)
			file := published.Files[0]
			require.NotNil(t, file.ContentType)
			assert.Equal(t, testParquetMIME, *file.ContentType)
			var stamped map[string]any
			require.NoError(t, json.Unmarshal(file.Metadata, &stamped))
			assert.Equal(t, "parquet", stamped["format"])
			assert.Equal(t, "orders.v1", stamped["schema_version"])
			assert.Equal(t, float64(groups), stamped["record_count"])
			installed, err := os.ReadFile(filepath.Join(fx.backingRoot, strings.TrimPrefix(file.StoragePath, "local:///")))
			require.NoError(t, err)
			assert.Equal(t, body, installed)
			assert.Equal(t, before+1, fx.store.datasets[0].CurrentVersion)

			commitReplay := fx.parquetCommit(sealed, testParquetMIME)
			require.Equal(t, http.StatusOK, commitReplay.Code, commitReplay.Body.String())
			assert.Equal(t, result.Body.String(), commitReplay.Body.String())
			assert.Equal(t, before+1, fx.store.datasets[0].CurrentVersion)
		})
	}
}

func TestStagedParquetRejectsBadFormatAndBody(t *testing.T) {
	valid := makeStagedOrdersParquet(t, 1, "orders.v1")
	footerLen := binary.LittleEndian.Uint32(valid[len(valid)-8:])
	footerOffset := len(valid) - 8 - int(footerLen)
	oversize := bytes.Clone(valid)
	binary.LittleEndian.PutUint32(oversize[len(oversize)-8:], 9<<20)
	badHeader := bytes.Clone(valid)
	badHeader[0] = '!'
	badPage := bytes.Clone(valid)
	badPage[4] = 0 // Premature Thrift STOP at the first page.
	badBlock := bytes.Clone(valid)
	firstPage := thrift.NewTMemoryBuffer()
	_, err := firstPage.Write(valid[4:footerOffset])
	require.NoError(t, err)
	p := thrift.NewTCompactProtocolConf(firstPage, &thrift.TConfiguration{})
	require.NoError(t, p.Skip(context.Background(), thrift.STRUCT))
	payloadOffset := footerOffset - firstPage.Len()
	badBlock[payloadOffset] = 0xff // Invalid decoded length in the first Snappy block.
	badRange := rewriteParquetFooter(t, valid, footerOffset, func(m *metadata.FileMetaData) {
		m.RowGroups[0].Columns[0].MetaData.DataPageOffset++
	})
	badRows := rewriteParquetFooter(t, valid, footerOffset, func(m *metadata.FileMetaData) {
		m.NumRows++
	})
	badSchema := rewriteParquetFooter(t, valid, footerOffset, func(m *metadata.FileMetaData) {
		m.FileMetaData.Schema[1].Name = "forged"
	})
	badAnnotation := rewriteParquetFooter(t, valid, footerOffset, func(m *metadata.FileMetaData) {
		field := m.FileMetaData.Schema[1].LogicalType
		field.INTEGER = nil
		initParquetTestAnnotation(&field.GEOMETRY)
	})
	// A tiny footer uses one declared large list with no available elements.
	// This must be rejected before Arrow's generated reader allocates.
	forgedCount := makeOversizedParquetMetadataList(t)
	for _, tc := range []struct {
		name string
		body []byte
		mime string
		q    map[string]string
		code string
	}{
		{"mime", valid, "application/octet-stream", nil, ""},
		{"format", valid, testParquetMIME, map[string]string{"format": "PARQUET"}, ""},
		{"absent-format", valid, testParquetMIME, map[string]string{"format": ""}, ""},
		{"wrong-extension", valid, testParquetMIME, map[string]string{"logical_path": "part.ndjson"}, ""},
		{"empty-basename", valid, testParquetMIME, map[string]string{"logical_path": ".parquet"}, ""},
		{"unclean-path", valid, testParquetMIME, map[string]string{"logical_path": "nested/../part.parquet"}, ""},
		{"wrong-marker", makeStagedOrdersParquet(t, 1, "orders.v2"), testParquetMIME, nil, "STAGED_PARQUET_INVALID"},
		{"truncated", valid[:len(valid)-1], testParquetMIME, nil, "STAGED_PARQUET_INVALID"},
		{"oversize-footer", oversize, testParquetMIME, nil, "STAGED_PARQUET_INVALID"},
		{"forged-list-count", forgedCount, testParquetMIME, nil, "STAGED_PARQUET_INVALID"},
		{"header", badHeader, testParquetMIME, nil, "STAGED_PARQUET_INVALID"},
		{"page-header", badPage, testParquetMIME, nil, "STAGED_PARQUET_INVALID"},
		{"snappy-block", badBlock, testParquetMIME, nil, "STAGED_PARQUET_INVALID"},
		{"column-ranges", badRange, testParquetMIME, nil, "STAGED_PARQUET_INVALID"},
		{"record-count", badRows, testParquetMIME, nil, "STAGED_PARQUET_INVALID"},
		{"schema", badSchema, testParquetMIME, nil, "STAGED_PARQUET_INVALID"},
		{"schema-annotation", badAnnotation, testParquetMIME, nil, "STAGED_PARQUET_INVALID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newStageFixture(t)
			rec := fx.stageParquet(tc.body, tc.mime, tc.q)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			if tc.code != "" {
				assert.Contains(t, rec.Body.String(), tc.code)
			}
			assert.Empty(t, fx.store.stagedObjects)
			assert.Equal(t, models.TransactionStatusOpen, fx.store.runtimeTransactions[fx.reservation].Status)
			assert.Empty(t, stageReservationFiles(t, fx))
		})
	}
}

func initParquetTestAnnotation[T any](field **T) { *field = new(T) }

func rewriteParquetFooter(t *testing.T, good []byte, offset int, mutate func(*metadata.FileMetaData)) []byte {
	t.Helper()
	meta, err := metadata.NewFileMetaData(good[offset:len(good)-8], nil)
	require.NoError(t, err)
	mutate(meta)
	footer, err := meta.Serialize(context.Background())
	require.NoError(t, err)
	return appendParquetFooter(good[:offset], footer)
}

func makeOversizedParquetMetadataList(t *testing.T) []byte {
	t.Helper()
	ctx := context.Background()
	mem := thrift.NewTMemoryBuffer()
	proto := thrift.NewTCompactProtocolConf(mem, &thrift.TConfiguration{})
	require.NoError(t, proto.WriteStructBegin(ctx, "FileMetaData"))
	require.NoError(t, proto.WriteFieldBegin(ctx, "schema", thrift.LIST, 2))
	require.NoError(t, proto.WriteListBegin(ctx, thrift.STRUCT, 65536))
	return appendParquetFooter([]byte("PAR1"), mem.Bytes())
}

func appendParquetFooter(data, footer []byte) []byte {
	result := append(bytes.Clone(data), footer...)
	result = binary.LittleEndian.AppendUint32(result, uint32(len(footer)))
	return append(result, "PAR1"...)
}

func stageReservationFiles(t *testing.T, fx *stageFixture) []os.DirEntry {
	t.Helper()
	files, err := os.ReadDir(filepath.Join(fx.stageRoot, "datasets", fx.datasetID.String(), "reservations", fx.reservation.String()))
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	return files
}

func TestStagedParquetTakeoverLimitsAndSealedCorruption(t *testing.T) {
	fx := newStageFixture(t)
	body := makeStagedOrdersParquet(t, 2, "orders.v1")
	fx.h.StagedOutputMaxBytes = int64(len(body)) - 1
	result := fx.stageParquet(body, testParquetMIME, nil)
	require.Equal(t, http.StatusRequestEntityTooLarge, result.Code, result.Body.String())
	assert.Contains(t, result.Body.String(), "STAGED_BODY_TOO_LARGE")
	assert.Empty(t, stageReservationFiles(t, fx))

	fx.h.StagedOutputMaxBytes = int64(len(body))
	fx.h.StagedOutputMaxRecords = 1
	result = fx.stageParquet(body, testParquetMIME, nil)
	require.Equal(t, http.StatusRequestEntityTooLarge, result.Code, result.Body.String())
	assert.Contains(t, result.Body.String(), "STAGED_RECORD_COUNT_EXCEEDED")
	assert.Empty(t, stageReservationFiles(t, fx))

	fx.h.StagedOutputMaxRecords = 2
	expired, other := uuid.New(), uuid.New()
	deadline := time.Now().Add(-time.Minute)
	fx.store.stagedObjects[expired] = &repo.StagedOutputObject{
		ID: expired, DatasetID: fx.datasetID, ReservationID: fx.reservation, LogicalPath: "part-00000.parquet",
		Status: repo.StagedObjectStatusStaging, LeaseExpiresAt: &deadline,
	}
	dir := filepath.Join(fx.stageRoot, "datasets", fx.datasetID.String(), "reservations", fx.reservation.String())
	for _, name := range []string{expired.String() + ".tmp", expired.String() + ".parquet", other.String() + ".parquet"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), body, 0o600))
	}
	result = fx.stageParquet(body, testParquetMIME, nil)
	require.Equal(t, http.StatusCreated, result.Code, result.Body.String())
	var sealed models.StageDatasetOutputResponse
	require.NoError(t, json.Unmarshal(result.Body.Bytes(), &sealed))
	_, err := os.Stat(filepath.Join(dir, expired.String()+".tmp"))
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(dir, expired.String()+".parquet"))
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(dir, other.String()+".parquet"))
	assert.NoError(t, err)

	sealedPath := filepath.Join(dir, sealed.StagedObjectID+".parquet")
	corrupt := bytes.Clone(body)
	corrupt[4] ^= 0xff
	require.NoError(t, os.WriteFile(sealedPath, corrupt, 0o600))
	before := fx.store.datasets[0].CurrentVersion
	rec := fx.parquetCommit(sealed, testParquetMIME)
	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	assert.Equal(t, before, fx.store.datasets[0].CurrentVersion)
	assert.Equal(t, models.TransactionStatusOpen, fx.store.runtimeTransactions[fx.reservation].Status)
	assert.Equal(t, repo.StagedObjectStatusSealed, fx.store.stagedObjects[uuid.MustParse(sealed.StagedObjectID)].Status)
	installed, err := filepath.Glob(filepath.Join(fx.backingRoot, "store", "datasets", fx.datasetID.String(), "transactions", "*", "part-00000.parquet"))
	require.NoError(t, err)
	assert.Empty(t, installed)
}
