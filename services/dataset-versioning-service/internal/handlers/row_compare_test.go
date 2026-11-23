package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
)

func rowCompareQuery(hs *scanHarness, base, target int32, key string) url.Values {
	versions := hs.store.versions[hs.dataset.ID]
	return url.Values{
		"key":                {key},
		"base_branch":        {hs.branch.Name},
		"base_version":       {strconv.FormatInt(int64(base), 10)},
		"base_transaction":   {versions[base-1].TransactionID.String()},
		"target_branch":      {hs.branch.Name},
		"target_version":     {strconv.FormatInt(int64(target), 10)},
		"target_transaction": {versions[target-1].TransactionID.String()},
	}
}

func requestRowCompare(hs *scanHarness, query url.Values, subject uuid.UUID) *httptest.ResponseRecorder {
	id := hs.dataset.ID.String()
	return do(hs.h.CompareDatasetRows, http.MethodGet, "/v1/datasets/"+id+"/compare/rows?"+query.Encode(), "",
		map[string]string{"rid": id}, &authmw.Claims{Sub: subject})
}

func compareConsecutiveOrderIDs(t *testing.T, first, count int) []byte {
	t.Helper()
	groups := (count + 511) / 512
	return makeOrdersParquetTestFile(t, groups, "orders.v1", func(b *array.RecordBuilder, group int) {
		for i := group * 512; i < min(count, (group+1)*512); i++ {
			appendOrdersParquetTestRow(b, int64(first+i), 0, 19782, 1709144400123456, "Etc/UTC", nil)
		}
	})
}

func TestRowCompareMatchesWholeTypedSnapshots(t *testing.T) {
	hs := newScanHarness(t)
	common := compareConsecutiveOrderIDs(t, 1, 130)
	const modifiedID = 9_007_199_254_740_993
	emptyNote := ""
	beforeChange := makeOrdersParquetTestFile(t, 1, "orders.v1", func(b *array.RecordBuilder, _ int) {
		appendOrdersParquetTestRow(b, modifiedID, 123_456_789_012_345_000, 19782, 1709144400123456, "Etc/UTC", nil)
		appendOrdersParquetTestRow(b, 9_007_199_254_740_992, 0, 19782, 1709144400123456, "Etc/UTC", nil)
	})
	afterChange := makeOrdersParquetTestFile(t, 1, "orders.v1", func(b *array.RecordBuilder, _ int) {
		appendOrdersParquetTestRow(b, modifiedID, 123_456_789_012_345_001, 19782, 1709144400123456, "Etc/UTC", &emptyNote)
		appendOrdersParquetTestRow(b, 9_223_372_036_854_775_807, -1, 19782, 1709144400123456, "Etc/UTC", nil)
	})
	base := hs.commitSnapshotVersionFiles(t, stagedSpec{path: "01.parquet", content: common}, stagedSpec{path: "02.parquet", content: beforeChange})
	target := hs.commitSnapshotVersionFiles(t, stagedSpec{path: "01.parquet", content: afterChange}, stagedSpec{path: "02.parquet", content: common})

	query := rowCompareQuery(hs, base, target, "order_id")
	rec := requestRowCompare(hs, query, hs.ownerID)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var got struct {
		DatasetID string                     `json:"dataset_id"`
		Key       string                     `json:"key"`
		Base      map[string]json.RawMessage `json:"base"`
		Target    map[string]json.RawMessage `json:"target"`
		Counts    map[string]int             `json:"counts"`
		Changes   []struct {
			Key    string          `json:"key"`
			Kind   string          `json:"kind"`
			Before json.RawMessage `json:"before"`
			After  json.RawMessage `json:"after"`
		} `json:"changes"`
		Truncated bool `json:"changes_truncated"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, hs.dataset.ID.String(), got.DatasetID)
	require.Equal(t, "order_id", got.Key)
	require.Equal(t, strconv.Quote(query.Get("base_transaction")), string(got.Base["transaction_id"]))
	require.Equal(t, strconv.Quote(query.Get("target_transaction")), string(got.Target["transaction_id"]))
	require.Equal(t, map[string]int{"added": 1, "removed": 1, "modified": 1, "unchanged": 130}, got.Counts)
	require.False(t, got.Truncated)
	require.Len(t, got.Changes, 3)
	require.Equal(t, "removed", got.Changes[0].Kind)
	require.Equal(t, "9007199254740992", got.Changes[0].Key)
	require.Nil(t, got.Changes[0].After)
	require.Equal(t, "modified", got.Changes[1].Kind)
	require.Equal(t, "9007199254740993", got.Changes[1].Key)
	require.Contains(t, string(got.Changes[1].Before), `"order_id":"9007199254740993"`)
	require.Contains(t, string(got.Changes[1].Before), `"amount":"12345678901234.5000"`)
	require.Contains(t, string(got.Changes[1].Before), `"note":null`)
	require.Contains(t, string(got.Changes[1].After), `"amount":"12345678901234.5001"`)
	require.Contains(t, string(got.Changes[1].After), `"note":""`)
	require.Equal(t, "added", got.Changes[2].Kind)
	require.Equal(t, "9223372036854775807", got.Changes[2].Key)
	require.Nil(t, got.Changes[2].Before)

	_ = hs.commitSnapshotVersion(t, "new-head.parquet", compareConsecutiveOrderIDs(t, 999, 5))
	afterHead := requestRowCompare(hs, query, hs.ownerID)
	require.Equal(t, http.StatusOK, afterHead.Code, afterHead.Body.String())
	require.Equal(t, rec.Body.String(), afterHead.Body.String())
}

func TestRowCompareRejectsInvalidOrIncompleteSnapshots(t *testing.T) {
	baseData := makeStagedOrdersParquet(t, 0, "orders.v1")
	common := compareConsecutiveOrderIDs(t, 1, 130)
	duplicate := compareConsecutiveOrderIDs(t, 1, 1)
	large := compareConsecutiveOrderIDs(t, 1, 1025)
	for _, test := range []struct {
		name  string
		key   string
		files []stagedSpec
		code  string
	}{
		{"duplicate after a page boundary", "order_id", []stagedSpec{{path: "a.parquet", content: common}, {path: "z.parquet", content: duplicate}}, "dataset_row_compare_duplicate_key"},
		{"null key", "note", []stagedSpec{{path: "a.parquet", content: duplicate}}, "dataset_row_compare_null_key"},
		{"unavailable after a page boundary", "order_id", []stagedSpec{{path: "a.parquet", content: common}, {path: "z.parquet"}}, "dataset_file_bytes_missing"},
		{"mixed formats", "order_id", []stagedSpec{{path: "a.parquet", content: duplicate}, {path: "b.ndjson", content: []byte("{}\n")}}, "dataset_row_compare_format_unsupported"},
		{"unsupported NDJSON", "order_id", []stagedSpec{{path: "a.ndjson", content: []byte("{}\n")}}, "dataset_row_compare_format_unsupported"},
		{"row budget", "order_id", []stagedSpec{{path: "a.parquet", content: large}}, "dataset_row_compare_limit_exceeded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			hs := newScanHarness(t)
			base := hs.commitSnapshotVersion(t, "base.parquet", baseData)
			target := hs.commitSnapshotVersionFiles(t, test.files...)
			rec := requestRowCompare(hs, rowCompareQuery(hs, base, target, test.key), hs.ownerID)
			require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), test.code)
			require.NotContains(t, rec.Body.String(), `"counts"`)
			require.NotContains(t, rec.Body.String(), `"changes"`)
		})
	}
}

func TestRowCompareValidatesBothPinsBeforeReading(t *testing.T) {
	hs := newScanHarness(t)
	base := hs.commitSnapshotVersionFiles(t, stagedSpec{path: "missing.parquet"})
	target := hs.commitSnapshotVersion(t, "target.parquet", makeStagedOrdersParquet(t, 0, "orders.v1"))
	for _, test := range []struct {
		name   string
		status int
		code   string
		edit   func(url.Values)
	}{
		{"unknown key", http.StatusBadRequest, "dataset_row_compare_key_invalid", func(q url.Values) { q.Set("key", "other") }},
		{"ambiguous pin", http.StatusBadRequest, "dataset_row_compare_query_invalid", func(q url.Values) { q.Add("target_version", "3") }},
		{"missing pin", http.StatusBadRequest, "dataset_row_compare_query_invalid", func(q url.Values) { q.Del("base_transaction") }},
		{"mismatched pin is detected before the missing base file", http.StatusUnprocessableEntity, "dataset_row_compare_transaction_mismatch",
			func(q url.Values) { q.Set("target_transaction", q.Get("base_transaction")) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			q := rowCompareQuery(hs, base, target, "order_id")
			test.edit(q)
			rec := requestRowCompare(hs, q, hs.ownerID)
			require.Equal(t, test.status, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), test.code)
			require.NotContains(t, rec.Body.String(), `"counts"`)
		})
	}
	q := rowCompareQuery(hs, base, target, "order_id")
	rec := requestRowCompare(hs, q, uuid.New())
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), "missing.parquet")
}

func TestRowCompareKeepsCompleteCountsWhenDetailsAreBounded(t *testing.T) {
	t.Run("change count", func(t *testing.T) {
		hs := newScanHarness(t)
		base := hs.commitSnapshotVersion(t, "base.parquet", makeStagedOrdersParquet(t, 0, "orders.v1"))
		target := hs.commitSnapshotVersion(t, "target.parquet", compareConsecutiveOrderIDs(t, 1, 101))
		rec := requestRowCompare(hs, rowCompareQuery(hs, base, target, "order_id"), hs.ownerID)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var got struct {
			Counts    map[string]int    `json:"counts"`
			Changes   []json.RawMessage `json:"changes"`
			Truncated bool              `json:"changes_truncated"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		require.Equal(t, 101, got.Counts["added"])
		require.Len(t, got.Changes, 100)
		require.True(t, got.Truncated)
	})
	t.Run("change bytes", func(t *testing.T) {
		hs := newScanHarness(t)
		withNote := func(ch string) []byte {
			return makeOrdersParquetTestFile(t, 1, "orders.v1", func(b *array.RecordBuilder, _ int) {
				note := strings.Repeat(ch, 600<<10)
				appendOrdersParquetTestRow(b, 1, 0, 19782, 1709144400123456, "Etc/UTC", &note)
			})
		}
		base := hs.commitSnapshotVersion(t, "base.parquet", withNote("a"))
		target := hs.commitSnapshotVersion(t, "target.parquet", withNote("b"))
		rec := requestRowCompare(hs, rowCompareQuery(hs, base, target, "order_id"), hs.ownerID)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), `"modified":1`)
		require.Contains(t, rec.Body.String(), `"changes":[]`)
		require.Contains(t, rec.Body.String(), `"changes_truncated":true`)
	})
}
