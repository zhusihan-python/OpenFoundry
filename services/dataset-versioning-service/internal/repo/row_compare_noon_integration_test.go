//go:build integration

package repo_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

// The staged-file registry of a real commit decides which profile compares a
// snapshot. These publish the noon order lines #76's pinned Pipeline emits and
// read them back through the real router, repository and PostgreSQL.
const noonFirstBatch = `{"currency":"AED","item_nr":"NRPL0001-1","market":"AE","order_nr":"NRPL0001","store":"RPL01-NAE","unit_price":"49"}
{"currency":"AED","item_nr":"NRPL0002-1","market":"AE","order_nr":"NRPL0002","store":"RPL01-NAE","unit_price":"120"}
{"currency":"AED","item_nr":"NRPL0003-1","market":"AE","order_nr":"NRPL0003","store":"RPL01-NAE","unit_price":"30"}
`

const noonSecondBatch = `{"currency":"AED","item_nr":"NRPL0001-1","market":"AE","order_nr":"NRPL0001","store":"RPL01-NAE","unit_price":"49.25"}
{"currency":"AED","item_nr":"NRPL0002-1","market":"AE","order_nr":"NRPL0002","store":"RPL01-NAE","unit_price":"120"}
{"currency":"AED","item_nr":"NRPL0003-1","market":"AE","order_nr":"NRPL0003","store":"RPL01-NAE","unit_price":"30"}
{"currency":"AED","item_nr":"NRPL0004-1","market":"AE","order_nr":"NRPL0004","store":"RPL01-NAE","unit_price":"25"}
{"currency":"AED","item_nr":"NRPL0005-1","market":"AE","order_nr":"NRPL0005","store":"RPL01-NAE","unit_price":"60"}
`

type noonVersion struct {
	version     int32
	transaction string
}

type noonComparisonPage struct {
	Key     string         `json:"key"`
	Counts  map[string]int `json:"counts"`
	Changes []struct {
		Key    string            `json:"key"`
		Kind   string            `json:"kind"`
		Before map[string]string `json:"before"`
		After  map[string]string `json:"after"`
	} `json:"changes"`
	Truncated bool `json:"changes_truncated"`
}

func (s *scanStack) publishNoonLines(t *testing.T, files ...scanFileSpec) noonVersion {
	t.Helper()
	published := noonVersion{version: s.commitSnapshotFiles(t, files)}
	require.NoError(t, s.pool.QueryRow(context.Background(),
		`SELECT transaction_id::text FROM dataset_versions WHERE dataset_id = $1 AND version = $2`,
		s.dataset.ID, published.version).Scan(&published.transaction))
	return published
}

func noonComparison(rid string, base, target noonVersion, key string) string {
	query := url.Values{
		"key":                {key},
		"base_branch":        {"main"},
		"base_version":       {strconv.FormatInt(int64(base.version), 10)},
		"base_transaction":   {base.transaction},
		"target_branch":      {"main"},
		"target_version":     {strconv.FormatInt(int64(target.version), 10)},
		"target_transaction": {target.transaction},
	}
	return "/v1/datasets/" + rid + "/compare/rows?" + query.Encode()
}

func (s *scanStack) registryFormats(t *testing.T, transaction string) [4]bool {
	t.Helper()
	txn, err := uuid.Parse(transaction)
	require.NoError(t, err)
	hasParquet, hasOther, allDelimited, allNDJSON, err := (&repo.Repo{Pool: s.pool}).TransactionStagedFileFormats(context.Background(), txn)
	require.NoError(t, err)
	return [4]bool{hasParquet, hasOther, allDelimited, allNDJSON}
}

func TestRowCompareNoonOrderLinesRealRouter(t *testing.T) {
	s := newScanStack(t)
	one := s.publishNoonLines(t, scanFileSpec{logical: "part-00000.ndjson", content: []byte(noonFirstBatch)})
	two := s.publishNoonLines(t, scanFileSpec{logical: "part-00000.ndjson", content: []byte(noonSecondBatch)})
	three := s.publishNoonLines(t, scanFileSpec{logical: "part-00000.ndjson", content: []byte(noonFirstBatch)})
	// hasParquet, hasOther, allDelimited, allNDJSON
	require.Equal(t, [4]bool{false, true, false, true}, s.registryFormats(t, one.transaction))

	for _, test := range []struct {
		name   string
		base   noonVersion
		target noonVersion
		counts map[string]int
		kinds  []string
		keys   []string
	}{
		{"second batch", one, two, map[string]int{"added": 2, "removed": 0, "modified": 1, "unchanged": 2},
			[]string{"modified", "added", "added"}, []string{"NRPL0001-1", "NRPL0004-1", "NRPL0005-1"}},
		{"replayed first batch", one, three, map[string]int{"added": 0, "removed": 0, "modified": 0, "unchanged": 3}, []string{}, []string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rec := s.do(t, http.MethodGet, noonComparison(s.rid, test.base, test.target, "item_nr"), "", nil, s.tokenA)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var got noonComparisonPage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			require.Equal(t, "item_nr", got.Key)
			require.Equal(t, test.counts, got.Counts)
			require.False(t, got.Truncated)
			kinds, keys := make([]string, len(got.Changes)), make([]string, len(got.Changes))
			for i, change := range got.Changes {
				kinds[i], keys[i] = change.Kind, change.Key
			}
			require.Equal(t, test.kinds, kinds)
			require.Equal(t, test.keys, keys)
			if len(got.Changes) == 3 {
				require.Equal(t, "49", got.Changes[0].Before["unit_price"])
				require.Equal(t, "49.25", got.Changes[0].After["unit_price"])
				require.Empty(t, got.Changes[1].Before)
				require.Equal(t, "AED", got.Changes[2].After["currency"])
			}
		})
	}

	// Another tenant cannot read the same comparison.
	rec := s.do(t, http.MethodGet, noonComparison(s.rid, one, two, "item_nr"), "", nil, s.tokenB)
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), "part-00000.ndjson")
}

func TestRowCompareRejectsUnpromisedNoonSnapshotsRealRouter(t *testing.T) {
	s := newScanStack(t)
	base := s.publishNoonLines(t, scanFileSpec{logical: "part-00000.ndjson", content: []byte(noonFirstBatch)})
	for _, test := range []struct {
		name  string
		files []scanFileSpec
	}{
		{"noon lines under a .txt suffix", []scanFileSpec{{logical: "part-00000.txt", content: []byte(noonSecondBatch)}}},
		{"noon lines under a .json suffix", []scanFileSpec{{logical: "part-00000.json", content: []byte(noonSecondBatch)}}},
		{"NDJSON mixed with another record file", []scanFileSpec{
			{logical: "part-00000.ndjson", content: []byte(noonFirstBatch)}, {logical: "z.txt", content: []byte("notes\n")}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := s.publishNoonLines(t, test.files...)
			rec := s.do(t, http.MethodGet, noonComparison(s.rid, base, target, "item_nr"), "", nil, s.tokenA)
			require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), "dataset_row_compare_format_unsupported")
			require.NotContains(t, rec.Body.String(), `"counts"`)
			require.NotContains(t, rec.Body.String(), `"changes"`)
		})
	}

	// A transaction without registered files is no snapshot of any profile.
	txn, err := uuid.Parse(base.transaction)
	require.NoError(t, err)
	_, err = s.pool.Exec(context.Background(), `DELETE FROM dataset_transaction_files WHERE transaction_id = $1`, txn)
	require.NoError(t, err)
	hasParquet, hasOther, allDelimited, allNDJSON, err := (&repo.Repo{Pool: s.pool}).TransactionStagedFileFormats(context.Background(), txn)
	require.NoError(t, err)
	require.Equal(t, [4]bool{false, false, false, false}, [4]bool{hasParquet, hasOther, allDelimited, allNDJSON})
	rec := s.do(t, http.MethodGet, noonComparison(s.rid, base, base, "item_nr"), "", nil, s.tokenA)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "dataset_row_compare_format_unsupported")
	require.NotContains(t, rec.Body.String(), `"counts"`)
}
