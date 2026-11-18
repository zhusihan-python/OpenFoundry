package handlers_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/google/uuid"
	"github.com/klauspost/compress/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

type parquetScanResponse struct {
	handlersScanResponse
	Format        string               `json:"format"`
	Mode          string               `json:"mode"`
	SchemaVersion string               `json:"schema_version"`
	Schema        models.DatasetSchema `json:"schema"`
}

func decodeParquetScan(t *testing.T, rec *httptest.ResponseRecorder) parquetScanResponse {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var out parquetScanResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "parquet", out.Format)
	assert.Equal(t, "orders.v1", out.SchemaVersion)
	assert.Equal(t, models.FileFormatParquet, out.Schema.FileFormat)
	require.Len(t, out.Schema.Fields, 6)
	for i, name := range []string{"order_id", "amount", "order_date", "ordered_at", "source_time_zone", "note"} {
		assert.Equal(t, name, out.Schema.Fields[i].Name)
		assert.Equal(t, name == "note", out.Schema.Fields[i].Nullable)
	}
	require.NotNil(t, out.Schema.Fields[1].Scale)
	require.NotNil(t, out.Schema.Fields[1].Precision)
	assert.Equal(t, uint8(4), *out.Schema.Fields[1].Scale)
	assert.Equal(t, uint8(18), *out.Schema.Fields[1].Precision)
	return out
}

func TestScanParquetExactValuesAcrossFilesGroupsAndHeadChange(t *testing.T) {
	hs := newScanHarness(t)
	emptyValue := ""
	multi := makeOrdersParquetTestFile(t, 2, "orders.v1", func(b *array.RecordBuilder, group int) {
		if group == 0 {
			appendOrdersParquetTestRow(b, 9_007_199_254_740_993, 123_456_789_012_345_000, 19782, 1709144400123456, "Asia/Kathmandu", nil)
			appendOrdersParquetTestRow(b, 9_007_199_254_740_995, -71000, 19782, 1709144400000000, "Etc/UTC", &emptyValue)
			return
		}
		note := "hold < & \"quote\""
		appendOrdersParquetTestRow(b, 9_223_372_036_854_775_807, 0, 0, -1, "America/New_York", &note)
	})
	empty := makeStagedOrdersParquet(t, 0, "orders.v1")
	tail := makeStagedOrdersParquet(t, 1, "orders.v1")
	version := hs.commitSnapshotVersionFiles(t,
		stagedSpec{path: "z.parquet", content: tail},
		stagedSpec{path: "a.parquet", content: multi},
		stagedSpec{path: "b-empty.parquet", content: empty})
	first := decodeParquetScan(t, hs.scanRequest(scanAllFilesQuery(version, "", 1), nil))
	assert.Equal(t, "all_files", first.Mode)
	require.Len(t, first.Rows, 1)
	require.NotNil(t, first.NextCursor)
	assert.Contains(t, string(first.Rows[0]), `"order_id":9007199254740993`)
	assert.Contains(t, string(first.Rows[0]), `"amount":"12345678901234.5000"`)
	assert.Contains(t, string(first.Rows[0]), `"order_date":"2024-02-29"`)
	assert.Contains(t, string(first.Rows[0]), `"note":null`)

	newHead := hs.commitSnapshotVersion(t, "part.ndjson", []byte("{\"new_head\":true}\n"))
	assert.NotEqual(t, version, newHead)
	cursor := *first.NextCursor
	var ids []string
	for pageNumber := 0; cursor != ""; pageNumber++ {
		require.Less(t, pageNumber, 4, "cursor must advance")
		page := decodeParquetScan(t, hs.scanRequest(scanAllFilesQuery(version, cursor, 1), nil))
		require.Equal(t, first.TransactionID, page.TransactionID)
		require.GreaterOrEqual(t, page.PageBytes, int64(0))
		require.LessOrEqual(t, page.PageBytes, int64(8<<20))
		for _, row := range page.Rows {
			ids = append(ids, string(row))
		}
		cursor = ""
		if page.NextCursor != nil {
			cursor = *page.NextCursor
		}
	}
	require.Len(t, ids, 3)
	assert.Contains(t, ids[0], `"order_id":9007199254740995`)
	assert.Contains(t, ids[0], `"amount":"-7.1000"`)
	assert.Contains(t, ids[0], `"ordered_at":"2024-02-28T18:20:00.000000Z"`)
	assert.Contains(t, ids[0], `"note":""`)
	assert.Contains(t, ids[1], `"order_id":9223372036854775807`)
	assert.Contains(t, ids[1], `"ordered_at":"1969-12-31T23:59:59.999999Z"`)
	assert.Contains(t, ids[1], `"note":"hold < & \"quote\""`)
	assert.Contains(t, ids[2], `"order_id":9007199254740993`)

	emptyPage := decodeParquetScan(t, hs.scanRequest(scanQuery(version, "b-empty.parquet", "", 1), nil))
	assert.NotNil(t, emptyPage.Rows)
	assert.Empty(t, emptyPage.Rows)
	assert.Nil(t, emptyPage.NextCursor)
}

func TestScanParquetPredicateWorkBudgetAndCursorBinding(t *testing.T) {
	hs := newScanHarness(t)
	data := makeStagedOrdersParquet(t, 6, "orders.v1")
	version := hs.commitSnapshotVersionFiles(t, stagedSpec{path: "a.parquet", content: data}, stagedSpec{path: "b.parquet", content: data})
	predicate := func(value string) string {
		return "&predicate=" + url.QueryEscape(`{"op":"eq","column":"order_id","value":`+value+`}`)
	}
	match := predicate("9007199254740993")
	first := decodeParquetScan(t, hs.scanRequest(scanQuery(version, "a.parquet", "", 1)+match, nil))
	require.Len(t, first.Rows, 1)
	require.NotNil(t, first.NextCursor)
	raw, err := base64.RawURLEncoding.DecodeString(*first.NextCursor)
	require.NoError(t, err)
	require.Equal(t, "p1", string(raw[:2]))

	same := decodeParquetScan(t, hs.scanRequest(scanQuery(version, "a.parquet", *first.NextCursor, 1)+predicate("9007199254740993.0"), nil))
	require.Len(t, same.Rows, 1)
	for _, query := range []string{
		scanQuery(version, "b.parquet", *first.NextCursor, 1) + match,
		scanQuery(version, "a.parquet", *first.NextCursor, 1) + predicate("9007199254740992"),
		scanAllFilesPredicateQuery(version, `{"op":"eq","column":"order_id","value":9007199254740993}`, *first.NextCursor, 1),
	} {
		rec := hs.scanRequest(query, nil)
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "dataset_scan_cursor_mismatch")
	}
	binary.LittleEndian.PutUint64(raw[len(raw)-8:], 15<<32)
	forged := base64.RawURLEncoding.EncodeToString(raw)
	rec := hs.scanRequest(scanQuery(version, "a.parquet", forged, 1)+match, nil)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "dataset_scan_cursor_invalid")
	cont := ""
	for i := range 2 {
		exhausted := decodeParquetScan(t, hs.scanRequest(scanQuery(version, "a.parquet", cont, 1000)+predicate("9007199254740992"), nil))
		require.Empty(t, exhausted.Rows)
		if i == 0 {
			require.NotNil(t, exhausted.NextCursor, "group-work limit must slice even an all-rejected page")
			cont = *exhausted.NextCursor
		} else {
			require.Nil(t, exhausted.NextCursor)
		}
	}
}

func TestScanParquetMixedUnauthorizedAndUnreadableContent(t *testing.T) {
	hs := newScanHarness(t)
	ownerOrg := uuid.New()
	hs.store.datasets[0].OrgID = &ownerOrg
	owner := &authmw.Claims{Sub: hs.ownerID, OrgID: &ownerOrg}
	data := makeStagedOrdersParquet(t, 2, "orders.v1")

	mixed := hs.commitSnapshotVersionFiles(t, stagedSpec{path: "a.parquet", content: data}, stagedSpec{path: "z.ndjson", content: []byte("{}\n")})
	rec := hs.scanRequest(scanAllFilesQuery(mixed, "", 1), owner)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "dataset_scan_mixed_formats")
	foreignOrg := uuid.New()
	rec = hs.scanRequest(scanAllFilesQuery(mixed, "", 1), &authmw.Claims{Sub: uuid.New(), OrgID: &foreignOrg})
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())

	bad := corruptParquetSecondGroupStringLength(t, data)
	version := hs.commitSnapshotVersion(t, "bad.parquet", bad)
	first := decodeParquetScan(t, hs.scanRequest(scanQuery(version, "bad.parquet", "", 1), owner))
	require.Len(t, first.Rows, 1, "first page reads only the first row group")
	require.NotNil(t, first.NextCursor)
	rec = hs.scanRequest(scanQuery(version, "bad.parquet", *first.NextCursor, 1), owner)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "dataset_file_parse_failed")
	assert.NotContains(t, rec.Body.String(), `"rows"`)

	unbound := hs.commitSnapshotVersionFiles(t, stagedSpec{path: "own.parquet", content: data, physical: "data/another-object.parquet"})
	rec = hs.scanRequest(scanQuery(unbound, "own.parquet", "", 1), owner)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "dataset_scan_object_not_transaction_scoped")
}

// Preserve the page header, compression framing and byte ranges. Only
// the PLAIN value length in the second group's source_time_zone is bad.
func corruptParquetSecondGroupStringLength(t *testing.T, good []byte) []byte {
	t.Helper()
	footerLen := binary.LittleEndian.Uint32(good[len(good)-8:])
	footer := good[len(good)-8-int(footerLen) : len(good)-8]
	meta, err := metadata.NewFileMetaData(footer, nil)
	require.NoError(t, err)
	chunk := meta.RowGroups[1].Columns[4].MetaData
	page := good[chunk.DataPageOffset : chunk.DataPageOffset+chunk.TotalCompressedSize]
	mem := &thrift.TMemoryBuffer{Buffer: bytes.NewBuffer(page)}
	proto := thrift.NewTCompactProtocolConf(mem, &thrift.TConfiguration{})
	require.NoError(t, proto.Skip(context.Background(), thrift.STRUCT))
	n := len(page) - mem.Len()
	raw, err := snappy.Decode(nil, page[n:])
	require.NoError(t, err)
	raw[0] = 0xff
	compressed := snappy.Encode(nil, raw)
	require.Len(t, compressed, len(page)-n)
	bad := bytes.Clone(good)
	copy(bad[int(chunk.DataPageOffset)+n:], compressed)
	return bad
}
