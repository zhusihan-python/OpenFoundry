package handlers_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The order lines #76's pinned Pipeline publishes for the noon AE store: one
// compact object per record with sorted keys, every value a string. Its v1 and
// v3 hold the first three lines after the price of the first returns to 49;
// its v2 holds all five.
var noonLinesV2 = []string{
	`{"currency":"AED","item_nr":"NRPL0001-1","market":"AE","order_nr":"NRPL0001","store":"RPL01-NAE","unit_price":"49.25"}` + "\n",
	`{"currency":"AED","item_nr":"NRPL0002-1","market":"AE","order_nr":"NRPL0002","store":"RPL01-NAE","unit_price":"120"}` + "\n",
	`{"currency":"AED","item_nr":"NRPL0003-1","market":"AE","order_nr":"NRPL0003","store":"RPL01-NAE","unit_price":"30"}` + "\n",
	`{"currency":"AED","item_nr":"NRPL0004-1","market":"AE","order_nr":"NRPL0004","store":"RPL01-NAE","unit_price":"25"}` + "\n",
	`{"currency":"AED","item_nr":"NRPL0005-1","market":"AE","order_nr":"NRPL0005","store":"RPL01-NAE","unit_price":"60"}` + "\n",
}

var noonLinesV1 = append([]string{strings.Replace(noonLinesV2[0], `"49.25"`, `"49"`, 1)}, noonLinesV2[1:3]...)

func noonArtifact(lines []string) []byte {
	return []byte(strings.Join(lines, ""))
}

func manyNoonLines(count int) []byte {
	var out strings.Builder
	for i := range count {
		fmt.Fprintf(&out, `{"currency":"AED","item_nr":"NRPL%04d-1","market":"AE","order_nr":"NRPL%04d","store":"RPL01-NAE","unit_price":"1"}`+"\n", i+1, i+1)
	}
	return []byte(out.String())
}

func changeKeys(page rowComparePageResult) []string {
	keys := make([]string, len(page.Changes))
	for i, change := range page.Changes {
		keys[i] = change.Key
	}
	return keys
}

func TestRowCompareMatchesPublishedNoonOrderLines(t *testing.T) {
	hs := newScanHarness(t)
	v1 := hs.commitSnapshotVersion(t, "part-00000.ndjson", noonArtifact(noonLinesV1))
	v2 := hs.commitSnapshotVersion(t, "part-00000.ndjson", noonArtifact(noonLinesV2))
	v3 := hs.commitSnapshotVersion(t, "part-00000.ndjson", noonArtifact(noonLinesV1))

	got := readRowComparePage(t, hs, rowCompareQuery(hs, v1, v2, "item_nr"))
	require.Equal(t, map[string]int{"added": 2, "removed": 0, "modified": 1, "unchanged": 2}, got.Counts)
	require.Equal(t, []string{"NRPL0001-1", "NRPL0004-1", "NRPL0005-1"}, changeKeys(got))
	require.Equal(t, "modified", got.Changes[0].Kind)
	require.JSONEq(t, noonLinesV1[0], string(got.Changes[0].Before))
	require.JSONEq(t, noonLinesV2[0], string(got.Changes[0].After))
	for i, line := range noonLinesV2[3:] {
		require.Equal(t, "added", got.Changes[i+1].Kind)
		require.Nil(t, got.Changes[i+1].Before)
		require.JSONEq(t, line, string(got.Changes[i+1].After))
	}

	// v3 is the replay of v1 under another transaction: the same three lines.
	replay := readRowComparePage(t, hs, rowCompareQuery(hs, v1, v3, "item_nr"))
	require.Equal(t, map[string]int{"added": 0, "removed": 0, "modified": 0, "unchanged": 3}, replay.Counts)
	require.Empty(t, replay.Changes)

	// The page the browser asks for: two changes, then the last one.
	query := rowCompareQuery(hs, v1, v2, "item_nr")
	query.Set("limit", "2")
	first := readRowComparePage(t, hs, query)
	require.Equal(t, []string{"NRPL0001-1", "NRPL0004-1"}, changeKeys(first))
	require.NotEmpty(t, first.Next)
	query.Set("cursor", first.Next)
	last := readRowComparePage(t, hs, query)
	require.Equal(t, []string{"NRPL0005-1"}, changeKeys(last))
	require.Empty(t, last.Next)
	require.Equal(t, first.Counts, last.Counts)
}

func TestRowCompareReadsNoonValuesNotBytes(t *testing.T) {
	hs := newScanHarness(t)
	reshuffled := make([]string, len(noonLinesV1))
	for i, line := range noonLinesV1 {
		var fields map[string]string
		require.NoError(t, json.Unmarshal([]byte(line), &fields))
		reshuffled[i] = fmt.Sprintf(`{ "unit_price": %q, "store": %q, "item_nr": %q, "order_nr": %q, "market": %q, "currency": %q }`+"\n",
			fields["unit_price"], fields["store"], fields["item_nr"], fields["order_nr"], fields["market"], fields["currency"])
	}
	v1 := hs.commitSnapshotVersion(t, "part-00000.ndjson", noonArtifact(noonLinesV1))
	v2 := hs.commitSnapshotVersion(t, "part-00000.ndjson", noonArtifact(reshuffled))

	// Another key order or spacing is not a modification.
	got := readRowComparePage(t, hs, rowCompareQuery(hs, v1, v2, "item_nr"))
	require.Equal(t, map[string]int{"added": 0, "removed": 0, "modified": 0, "unchanged": 3}, got.Counts)
	require.Empty(t, got.Changes)
}

// A snapshot of several record files, either suffix rows:scan reads, is one
// complete snapshot.
func TestRowCompareReadsEveryNoonRecordFile(t *testing.T) {
	hs := newScanHarness(t)
	v1 := hs.commitSnapshotVersion(t, "part-00000.ndjson", noonArtifact(noonLinesV1))
	v2 := hs.commitSnapshotVersionFiles(t,
		stagedSpec{path: "part-00000.jsonl", content: noonArtifact(noonLinesV2[:3])},
		stagedSpec{path: "part-00001.ndjson", content: noonArtifact(noonLinesV2[3:])})

	got := readRowComparePage(t, hs, rowCompareQuery(hs, v1, v2, "item_nr"))
	require.Equal(t, map[string]int{"added": 2, "removed": 0, "modified": 1, "unchanged": 2}, got.Counts)
	require.Equal(t, []string{"NRPL0001-1", "NRPL0004-1", "NRPL0005-1"}, changeKeys(got))
}

// Real noon artifacts compare. Inputs whose support is not promised are
// rejected with a decidable status and code and without a partial result.
func TestRowCompareRejectsInputsAroundNoonOrderLines(t *testing.T) {
	real := noonArtifact(noonLinesV1)
	for _, test := range []struct {
		name   string
		key    string
		limit  string
		files  []stagedSpec
		status int
		code   string
	}{
		{"handoff order line that keeps its sku", "item_nr", "", []stagedSpec{{path: "part-00000.ndjson", content: []byte(`{"currency":"AED","item_nr":"NRPL0001-1","market":"AE","order_nr":"NRPL0001","sku":"N-RPL-SKU-001","store":"RPL01-NAE","unit_price":"49"}` + "\n")}},
			http.StatusUnprocessableEntity, "dataset_row_compare_invalid_rows"},
		{"order line without its currency", "item_nr", "", []stagedSpec{{path: "part-00000.ndjson", content: []byte(`{"item_nr":"NRPL0001-1","market":"AE","order_nr":"NRPL0001","store":"RPL01-NAE","unit_price":"49"}` + "\n")}},
			http.StatusUnprocessableEntity, "dataset_row_compare_invalid_rows"},
		{"numeric unit price", "item_nr", "", []stagedSpec{{path: "part-00000.ndjson", content: []byte(`{"currency":"AED","item_nr":"NRPL0001-1","market":"AE","order_nr":"NRPL0001","store":"RPL01-NAE","unit_price":49}` + "\n")}},
			http.StatusUnprocessableEntity, "dataset_row_compare_invalid_rows"},
		{"null unit price", "item_nr", "", []stagedSpec{{path: "part-00000.ndjson", content: []byte(`{"currency":"AED","item_nr":"NRPL0001-1","market":"AE","order_nr":"NRPL0001","store":"RPL01-NAE","unit_price":null}` + "\n")}},
			http.StatusUnprocessableEntity, "dataset_row_compare_invalid_rows"},
		{"orders.v1 key of an NDJSON snapshot", "order_id", "", []stagedSpec{{path: "part-00000.ndjson", content: real}},
			http.StatusUnprocessableEntity, "dataset_row_compare_format_unsupported"},
		{"noon lines under an unsupported .txt suffix", "item_nr", "", []stagedSpec{{path: "part-00000.txt", content: real}},
			http.StatusUnprocessableEntity, "dataset_row_compare_format_unsupported"},
		{"noon lines under an unsupported .json suffix", "item_nr", "", []stagedSpec{{path: "part-00000.json", content: real}},
			http.StatusUnprocessableEntity, "dataset_row_compare_format_unsupported"},
		{"NDJSON mixed with a delimited file", "item_nr", "", []stagedSpec{
			{path: "part-00000.ndjson", content: real}, {path: "z.csv", content: []byte("item_nr,unit_price\nNRPL0004-1,25\n")}},
			http.StatusUnprocessableEntity, "dataset_row_compare_format_unsupported"},
		{"empty transaction", "item_nr", "", nil,
			http.StatusUnprocessableEntity, "dataset_row_compare_format_unsupported"},
		{"delimited snapshot", "item_nr", "", []stagedSpec{{path: "part-00000.csv", content: []byte("item_nr,unit_price\nNRPL0001-1,49\n")}},
			http.StatusUnprocessableEntity, "dataset_row_compare_format_unsupported"},
		{"duplicate item number", "item_nr", "", []stagedSpec{{path: "part-00000.ndjson", content: noonArtifact([]string{noonLinesV1[0], noonLinesV1[0]})}},
			http.StatusUnprocessableEntity, "dataset_row_compare_duplicate_key"},
		{"row budget", "item_nr", "", []stagedSpec{{path: "part-00000.ndjson", content: manyNoonLines(1025)}},
			http.StatusUnprocessableEntity, "dataset_row_compare_limit_exceeded"},
		{"paged row budget", "item_nr", "50", []stagedSpec{{path: "part-00000.ndjson", content: manyNoonLines(8193)}},
			http.StatusUnprocessableEntity, "dataset_row_compare_limit_exceeded"},
		{"unavailable after a page boundary", "item_nr", "", []stagedSpec{{path: "part-00000.ndjson", content: real}, {path: "z.ndjson"}},
			http.StatusUnprocessableEntity, "dataset_file_bytes_missing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			hs := newScanHarness(t)
			base := hs.commitSnapshotVersion(t, "part-00000.ndjson", real)
			target := hs.commitSnapshotVersionFiles(t, test.files...)
			query := rowCompareQuery(hs, base, target, test.key)
			if test.limit != "" {
				query.Set("limit", test.limit)
			}
			rec := requestRowCompare(hs, query, hs.ownerID)
			require.Equal(t, test.status, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), test.code)
			require.NotContains(t, rec.Body.String(), `"counts"`)
			require.NotContains(t, rec.Body.String(), `"changes"`)

			// The same lines stay unreadable to another tenant.
			other := requestRowCompare(hs, rowCompareQuery(hs, base, target, "item_nr"), uuid.New())
			require.Equal(t, http.StatusNotFound, other.Code, other.Body.String())
			require.NotContains(t, other.Body.String(), "part-00000.ndjson")
		})
	}

	t.Run("noon key of a Parquet snapshot", func(t *testing.T) {
		hs := newScanHarness(t)
		base := hs.commitSnapshotVersion(t, "base.parquet", makeStagedOrdersParquet(t, 0, "orders.v1"))
		target := hs.commitSnapshotVersion(t, "target.parquet", compareConsecutiveOrderIDs(t, 1, 5))
		rec := requestRowCompare(hs, rowCompareQuery(hs, base, target, "item_nr"), hs.ownerID)
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "dataset_row_compare_key_invalid")
		require.NotContains(t, rec.Body.String(), `"counts"`)
	})
}
