package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scanPredicateQuery builds a mode-1 (path + predicate) scan query.
func scanPredicateQuery(version int32, logical string, predicate string, cursor string, limit int) string {
	query := fmt.Sprintf("?branch=main&version=%d&path=%s", version, logical)
	if predicate != "" {
		query += "&predicate=" + url.QueryEscape(predicate)
	}
	if limit > 0 {
		query += fmt.Sprintf("&limit=%d", limit)
	}
	if cursor != "" {
		query += "&cursor=" + cursor
	}
	return query
}

func decodeScanPage(t *testing.T, rec *httptest.ResponseRecorder) handlersScanResponse {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var out handlersScanResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	return out
}

// walkPredicateScan pages a mode-1 scan to the end and returns all rows,
// the number of pages and the summed page bytes. A limit of 0 means the
// request omits the parameter (server default 100).
func walkPredicateScan(t *testing.T, hs *scanHarness, version int32, logical string, predicate string, limit int) ([]string, int, int64) {
	t.Helper()
	effectiveLimit := limit
	if effectiveLimit <= 0 {
		effectiveLimit = 100
	}
	var got []string
	pages := 0
	var bytesSum int64
	cursor := ""
	for {
		page := decodeScanPage(t, hs.scanRequest(scanPredicateQuery(version, logical, predicate, cursor, limit), nil))
		pages++
		require.LessOrEqual(t, page.RowCount, effectiveLimit)
		require.LessOrEqual(t, page.PageBytes, int64(8<<20))
		for _, raw := range page.Rows {
			got = append(got, string(raw))
		}
		bytesSum += page.PageBytes
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
		require.Less(t, pages, 1000, "scan must terminate")
	}
	return got, pages, bytesSum
}

// TestScanPredicatePagesValueByValue: an eq predicate over one file keeps
// returning the matching records verbatim and in order across pages, with
// page bytes accounting for the whole file (skipped rows included).
func TestScanPredicatePagesValueByValue(t *testing.T) {
	hs := newScanHarness(t)
	lines := make([]string, 0, 300)
	for i := 0; i < 300; i++ {
		lines = append(lines, fmt.Sprintf(`{"n":%d,"bucket":%d,"s":"row-%d"}`, i, i%3, i))
	}
	content := []byte(joinLines(lines))
	version := hs.commitSnapshotVersion(t, "part-00000.ndjson", content)

	got, pages, bytesSum := walkPredicateScan(t, hs, version, "part-00000.ndjson", `{"column":"bucket","op":"eq","value":1}`, 10)
	assert.Greater(t, pages, 1, "sample must span pages")
	require.Len(t, got, 100)
	for i, row := range got {
		assert.Equal(t, lines[3*i+1], row, "matching row %d must be verbatim and in order", i)
	}
	assert.Equal(t, int64(len(content)), bytesSum, "page bytes must account for skipped rows too")
}

// TestScanPredicateTypedSemantics exercises the typed comparisons over a
// single committed file: exact case, string/bool/number/null typing,
// missing-vs-null, big-integer exactness and equal-value respellings.
func TestScanPredicateTypedSemantics(t *testing.T) {
	hs := newScanHarness(t)
	lines := []string{
		`{"id":9007199254740992,"kind":"big","active":true}`,
		`{"id":9007199254740993,"kind":"big","active":false}`,
		`{"id":1,"kind":"small","active":null}`,
		`{"kind":"missing-id","active":true}`,
		`{"id":"1","kind":"string-one"}`,
		`{"id":1.0,"kind":"one-float"}`,
	}
	version := hs.commitSnapshotVersion(t, "part-00000.ndjson", []byte(joinLines(lines)))

	rowsFor := func(predicate string) []string {
		got, _, _ := walkPredicateScan(t, hs, version, "part-00000.ndjson", predicate, 0)
		return got
	}

	assert.Equal(t, []string{lines[0]}, rowsFor(`{"column":"id","op":"eq","value":9007199254740992}`), "big ints stay distinguishable")
	assert.Equal(t, []string{lines[1]}, rowsFor(`{"column":"id","op":"eq","value":9007199254740993}`))
	assert.Equal(t, []string{lines[2], lines[5]}, rowsFor(`{"column":"id","op":"eq","value":1}`), "1 and 1.0 are the same value")
	assert.Equal(t, []string{lines[4]}, rowsFor(`{"column":"id","op":"eq","value":"1"}`), "string 1 matches only the string")
	assert.Empty(t, rowsFor(`{"column":"id","op":"eq","value":null}`), "missing field is not null")
	assert.Equal(t, []string{lines[2]}, rowsFor(`{"column":"active","op":"eq","value":null}`), "explicit null matches null")
	assert.Equal(t, []string{lines[0], lines[3]}, rowsFor(`{"column":"active","op":"eq","value":true}`))
	assert.Equal(t, []string{lines[1]}, rowsFor(`{"column":"active","op":"eq","value":false}`))
	assert.Empty(t, rowsFor(`{"column":"Kind","op":"eq","value":"big"}`), "column names are case-sensitive")
}

// TestScanPredicateNoMatchSingleEmptyPage: on a file that fits one page
// budget, a no-match predicate consumes every byte and answers one empty
// page with an explicit cursor-less end — empty results are a 200, not an
// error, and no bytes are skipped.
func TestScanPredicateNoMatchSingleEmptyPage(t *testing.T) {
	hs := newScanHarness(t)
	var content []byte
	for i := 0; i < 50; i++ {
		content = append(content, []byte(fmt.Sprintf(`{"seq":%d,"ghost":"no"}`+"\n", i))...)
	}
	version := hs.commitSnapshotVersion(t, "part-00000.ndjson", content)

	got, pages, bytesSum := walkPredicateScan(t, hs, version, "part-00000.ndjson", `{"column":"ghost","op":"eq","value":"yes"}`, 10)
	assert.Empty(t, got)
	assert.Equal(t, 1, pages, "small no-match file fits a single page")
	assert.Equal(t, int64(len(content)), bytesSum, "every byte is still consumed exactly once")
}

// TestScanPredicateCursorBinding: continuation rejects a changed predicate
// (semantically), a changed scan mode and a changed path; equal-value
// respellings of the SAME predicate keep paging.
func TestScanPredicateCursorBinding(t *testing.T) {
	hs := newScanHarness(t)
	lines := scanNDJSONLines(30, "p")
	version := hs.commitSnapshotVersion(t, "part-00000.ndjson", []byte(joinLines(lines)))

	// limit 1: page one stops right after the single z:5 match, mid-file.
	page := decodeScanPage(t, hs.scanRequest(scanPredicateQuery(version, "part-00000.ndjson", `{"column":"z","op":"eq","value":5}`, "", 1), nil))
	require.Len(t, page.Rows, 1)
	require.NotNil(t, page.NextCursor)
	cursor := *page.NextCursor
	base := scanPredicateQuery(version, "part-00000.ndjson", `{"column":"z","op":"eq","value":5}`, cursor, 5)
	require.Equal(t, http.StatusOK, hs.scanRequest(base, nil).Code, "same predicate replays fine")

	for name, query := range map[string]string{
		"changed value":     scanPredicateQuery(version, "part-00000.ndjson", `{"column":"z","op":"eq","value":6}`, cursor, 5),
		"changed column":    scanPredicateQuery(version, "part-00000.ndjson", `{"column":"a","op":"eq","value":5}`, cursor, 5),
		"dropped predicate": scanPredicateQuery(version, "part-00000.ndjson", "", cursor, 5),
		"changed type":      scanPredicateQuery(version, "part-00000.ndjson", `{"column":"z","op":"eq","value":"5"}`, cursor, 5),
	} {
		rec := hs.scanRequest(query, nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "case %q", name)
		assert.Contains(t, rec.Body.String(), "dataset_scan_cursor", "case %q", name)
	}

	// Equal-value respelling keeps the same business binding.
	respelled := scanPredicateQuery(version, "part-00000.ndjson", `{"column":"z","op":"eq","value":5.0}`, cursor, 5)
	assert.Equal(t, http.StatusOK, hs.scanRequest(respelled, nil).Code, "1 vs 1.0 is the same predicate")

	// Mode change: an all_files cursor cannot serve a path request and vice
	// versa — build an all_files cursor by scanning without predicate.
	allPage := decodeAllFilesPage(t, hs.scanRequest(scanAllFilesQuery(version, "", 5), nil))
	require.NotNil(t, allPage.NextCursor)
	pathWithAllCursor := scanPredicateQuery(version, "part-00000.ndjson", "", *allPage.NextCursor, 5)
	rec := hs.scanRequest(pathWithAllCursor, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code, "s1 request cannot consume an all_files cursor")
	assert.Contains(t, rec.Body.String(), "dataset_scan_cursor")
}

// TestScanPredicateRejectsBadRequests: strict 400 matrix for the new query
// parameters.
func TestScanPredicateRejectsBadRequests(t *testing.T) {
	hs := newScanHarness(t)
	hs.commitSnapshotVersion(t, "part-00000.ndjson", []byte(`{"a":1}`+"\n"))

	for name, query := range map[string]string{
		"empty predicate value":    "?branch=main&version=1&path=part-00000.ndjson&predicate=",
		"predicate not json":       "?branch=main&version=1&path=part-00000.ndjson&predicate=" + url.QueryEscape(`not-json`),
		"predicate unknown field":  "?branch=main&version=1&path=part-00000.ndjson&predicate=" + url.QueryEscape(`{"column":"a","op":"eq","value":1,"x":2}`),
		"predicate duplicate key":  "?branch=main&version=1&path=part-00000.ndjson&predicate=" + url.QueryEscape(`{"column":"a","column":"b","op":"eq","value":1}`),
		"predicate non-scalar":     "?branch=main&version=1&path=part-00000.ndjson&predicate=" + url.QueryEscape(`{"column":"a","op":"eq","value":[1]}`),
		"predicate bad op":         "?branch=main&version=1&path=part-00000.ndjson&predicate=" + url.QueryEscape(`{"column":"a","op":"gt","value":1}`),
		"predicate missing value":  "?branch=main&version=1&path=part-00000.ndjson&predicate=" + url.QueryEscape(`{"column":"a","op":"eq"}`),
		"predicate trailing junk":  "?branch=main&version=1&path=part-00000.ndjson&predicate=" + url.QueryEscape(`{"column":"a","op":"eq","value":1}x`),
		"predicate invalid utf8":   "?branch=main&version=1&path=part-00000.ndjson&predicate=%FF%FE",
		"duplicate predicate":      "?branch=main&version=1&path=part-00000.ndjson&predicate=%7B%22column%22%3A%22a%22%2C%22op%22%3A%22eq%22%2C%22value%22%3A1%7D&predicate=x",
		"all_files false":          "?branch=main&version=1&all_files=false",
		"all_files 1":              "?branch=main&version=1&all_files=1",
		"all_files TRUE":           "?branch=main&version=1&all_files=TRUE",
		"duplicate all_files":      "?branch=main&version=1&all_files=true&all_files=true",
		"all_files plus path":      "?branch=main&version=1&all_files=true&path=part-00000.ndjson",
		"all_files plus empty path": "?branch=main&version=1&all_files=true&path=",
		"all_files dup path keys":  "?branch=main&version=1&all_files=true&path=a&path=b",
		"predicate without path":   "?branch=main&version=1&predicate=" + url.QueryEscape(`{"column":"a","op":"eq","value":1}`),
	} {
		rec := hs.scanRequest(query, nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "case %q: %s", name, rec.Body.String())
	}
}
