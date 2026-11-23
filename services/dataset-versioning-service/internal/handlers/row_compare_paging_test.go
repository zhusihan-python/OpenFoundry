package handlers_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

type rowComparePageResult struct {
	DatasetID string         `json:"dataset_id"`
	Counts    map[string]int `json:"counts"`
	Changes   []struct {
		Key    string          `json:"key"`
		Kind   string          `json:"kind"`
		Before json.RawMessage `json:"before"`
		After  json.RawMessage `json:"after"`
	} `json:"changes"`
	Truncated bool   `json:"changes_truncated"`
	Next      string `json:"next_cursor"`
}

func readRowComparePage(t *testing.T, hs *scanHarness, q url.Values) rowComparePageResult {
	t.Helper()
	rec := requestRowCompare(hs, q, hs.ownerID)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var got rowComparePageResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, hs.dataset.ID.String(), got.DatasetID)
	require.False(t, got.Truncated)
	return got
}

func TestRowComparePagesAllChangesAcrossUnsortedLargeSnapshots(t *testing.T) {
	hs := newScanHarness(t)
	const first = 9_007_199_254_740_992
	base := hs.commitSnapshotVersionFiles(t,
		stagedSpec{path: "a.parquet", content: compareConsecutiveOrderIDs(t, first+600, 600)},
		stagedSpec{path: "z.parquet", content: compareConsecutiveOrderIDs(t, first, 600)})
	modified := makeOrdersParquetTestFile(t, 1, "orders.v1", func(b *array.RecordBuilder, _ int) {
		appendOrdersParquetTestRow(b, first, 1, 19782, 1709144400123456, "Etc/UTC", nil)
	})
	target := hs.commitSnapshotVersionFiles(t,
		stagedSpec{path: "a.parquet", content: compareConsecutiveOrderIDs(t, first+1300, 150)},
		stagedSpec{path: "b.parquet", content: compareConsecutiveOrderIDs(t, first+1150, 50)},
		stagedSpec{path: "modified.parquet", content: modified},
		stagedSpec{path: "z.parquet", content: compareConsecutiveOrderIDs(t, first+1, 999)})

	q := rowCompareQuery(hs, base, target, "order_id")
	legacy := requestRowCompare(hs, q, hs.ownerID)
	require.Equal(t, http.StatusUnprocessableEntity, legacy.Code, legacy.Body.String())
	require.Contains(t, legacy.Body.String(), "dataset_row_compare_limit_exceeded")

	q.Set("limit", "37")
	var want []string
	for i := range 150 {
		want = append(want, strconv.Itoa(first+1000+i)+":removed", strconv.Itoa(first+1300+i)+":added")
	}
	want = append(want, strconv.Itoa(first)+":modified")
	slices.Sort(want)
	var seen []string
	for page := 0; ; page++ {
		require.Less(t, page, 10, "pagination must finish")
		got := readRowComparePage(t, hs, q)
		require.Equal(t, map[string]int{"added": 150, "removed": 150, "modified": 1, "unchanged": 1049}, got.Counts)
		require.LessOrEqual(t, len(got.Changes), 37)
		for _, change := range got.Changes {
			seen = append(seen, change.Key+":"+change.Kind)
			if change.Key == strconv.Itoa(first) {
				require.Contains(t, string(change.Before), `"order_id":"9007199254740992"`)
				require.Contains(t, string(change.Before), `"amount":"0.0000"`)
				require.Contains(t, string(change.After), `"amount":"0.0001"`)
			}
		}
		if got.Next == "" {
			break
		}
		require.NotEqual(t, q.Get("cursor"), got.Next)
		q.Set("cursor", got.Next)
		if page == 0 {
			_ = hs.commitSnapshotVersion(t, "new-head.parquet", compareConsecutiveOrderIDs(t, 1, 5))
		}
	}
	require.Equal(t, want, seen)
}

func TestRowCompareRejectsWrongOrUnresolvedPageCursor(t *testing.T) {
	hs := newScanHarness(t)
	base := hs.commitSnapshotVersion(t, "base.parquet", compareConsecutiveOrderIDs(t, 1, 1))
	target := hs.commitSnapshotVersion(t, "target.parquet", compareConsecutiveOrderIDs(t, 1, 5))
	q := rowCompareQuery(hs, base, target, "order_id")
	q.Set("limit", "1")
	first := readRowComparePage(t, hs, q)
	require.NotEmpty(t, first.Next)
	other := newScanHarness(t)

	for _, test := range []struct {
		name string
		h    *scanHarness
		code string
		edit func(url.Values)
	}{
		{"invalid token", hs, "dataset_row_compare_cursor_invalid", func(q url.Values) { q.Set("cursor", "!"+first.Next[1:]) }},
		{"cursor without limit", hs, "dataset_row_compare_cursor_invalid", func(q url.Values) { q.Del("limit") }},
		{"duplicate cursor", hs, "dataset_row_compare_cursor_invalid", func(q url.Values) { q.Add("cursor", first.Next) }},
		{"duplicate limit", hs, "dataset_row_compare_query_invalid", func(q url.Values) { q.Add("limit", "2") }},
		{"limit out of range", hs, "dataset_row_compare_query_invalid", func(q url.Values) { q.Set("limit", "101") }},
		{"different limit", hs, "dataset_row_compare_cursor_mismatch", func(q url.Values) { q.Set("limit", "2") }},
		{"different key", hs, "dataset_row_compare_cursor_mismatch", func(q url.Values) { q.Set("key", "amount") }},
		{"different branch", hs, "dataset_row_compare_cursor_mismatch", func(q url.Values) { q.Set("base_branch", "other") }},
		{"different version", hs, "dataset_row_compare_cursor_mismatch", func(q url.Values) { q.Set("target_version", "3") }},
		{"different transaction", hs, "dataset_row_compare_cursor_mismatch",
			func(q url.Values) { q.Set("target_transaction", q.Get("base_transaction")) }},
		{"reordered sides", hs, "dataset_row_compare_cursor_mismatch", func(q url.Values) {
			for _, field := range []string{"branch", "version", "transaction"} {
				a, b := q.Get("base_"+field), q.Get("target_"+field)
				q.Set("base_"+field, b)
				q.Set("target_"+field, a)
			}
		}},
		{"different dataset", other, "dataset_row_compare_cursor_mismatch", func(url.Values) {}},
		{"unresolved anchor", hs, "dataset_row_compare_cursor_invalid", func(q url.Values) {
			decoded, err := base64.RawURLEncoding.DecodeString(first.Next)
			require.NoError(t, err)
			decoded[len(decoded)-1] ^= 1
			q.Set("cursor", base64.RawURLEncoding.EncodeToString(decoded))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			q := rowCompareQuery(hs, base, target, "order_id")
			q.Set("limit", "1")
			q.Set("cursor", first.Next)
			test.edit(q)
			rec := requestRowCompare(test.h, q, test.h.ownerID)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), test.code)
			require.NotContains(t, rec.Body.String(), `"counts"`)
		})
	}
}

func TestRowComparePageBudgets(t *testing.T) {
	t.Run("page byte limit continues at next changed key", func(t *testing.T) {
		hs := newScanHarness(t)
		note := strings.Repeat("a", 600<<10)
		targetData := makeOrdersParquetTestFile(t, 1, "orders.v1", func(b *array.RecordBuilder, _ int) {
			appendOrdersParquetTestRow(b, 2, 0, 19782, 1709144400123456, "Etc/UTC", &note)
			appendOrdersParquetTestRow(b, 1, 0, 19782, 1709144400123456, "Etc/UTC", &note)
		})
		base := hs.commitSnapshotVersion(t, "base.parquet", makeStagedOrdersParquet(t, 0, "orders.v1"))
		target := hs.commitSnapshotVersion(t, "target.parquet", targetData)
		q := rowCompareQuery(hs, base, target, "order_id")
		legacy := requestRowCompare(hs, q, hs.ownerID)
		require.Equal(t, http.StatusOK, legacy.Code, legacy.Body.String())
		require.NotContains(t, legacy.Body.String(), `"next_cursor"`)
		require.Contains(t, legacy.Body.String(), `"changes_truncated":true`)

		q.Set("limit", "2")
		a := readRowComparePage(t, hs, q)
		require.Len(t, a.Changes, 1)
		require.Equal(t, "1", a.Changes[0].Key)
		require.NotEmpty(t, a.Next)
		q.Set("cursor", a.Next)
		b := readRowComparePage(t, hs, q)
		require.Equal(t, a.Counts, b.Counts)
		require.Len(t, b.Changes, 1)
		require.Equal(t, "2", b.Changes[0].Key)
		require.Empty(t, b.Next)
	})
	t.Run("oversized single detail prevents partial success", func(t *testing.T) {
		hs := newScanHarness(t)
		withNote := func(ch string) []byte {
			return makeOrdersParquetTestFile(t, 1, "orders.v1", func(b *array.RecordBuilder, _ int) {
				note := strings.Repeat(ch, 600<<10)
				appendOrdersParquetTestRow(b, 2, 0, 19782, 1709144400123456, "Etc/UTC", &note)
			})
		}
		base := hs.commitSnapshotVersion(t, "base.parquet", withNote("a"))
		target := hs.commitSnapshotVersionFiles(t,
			stagedSpec{path: "small-first.parquet", content: compareConsecutiveOrderIDs(t, 1, 1)},
			stagedSpec{path: "large-second.parquet", content: withNote("b")})
		q := rowCompareQuery(hs, base, target, "order_id")
		q.Set("limit", "1")
		rec := requestRowCompare(hs, q, hs.ownerID)
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "dataset_row_compare_change_too_large")
		require.NotContains(t, rec.Body.String(), `"counts"`)
	})
	t.Run("paged row budget", func(t *testing.T) {
		hs := newScanHarness(t)
		base := hs.commitSnapshotVersion(t, "base.parquet", makeStagedOrdersParquet(t, 0, "orders.v1"))
		target := hs.commitSnapshotVersion(t, "target.parquet", compareConsecutiveOrderIDs(t, 1, 8193))
		q := rowCompareQuery(hs, base, target, "order_id")
		q.Set("limit", "10")
		rec := requestRowCompare(hs, q, hs.ownerID)
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "dataset_row_compare_limit_exceeded")
		require.NotContains(t, rec.Body.String(), `"counts"`)
	})
}
