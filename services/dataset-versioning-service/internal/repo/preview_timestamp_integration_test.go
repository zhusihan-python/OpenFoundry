//go:build integration

package repo_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDatasetPreviewTimestampUsesCommittedSnapshot(t *testing.T) {
	s := newScanStack(t)
	datasetURL := "/v1/datasets/" + s.rid
	preview := func(query url.Values, token string) *httptest.ResponseRecorder {
		return s.do(t, http.MethodGet, datasetURL+"/preview?"+query.Encode(), "", nil, token)
	}
	at := func(ts time.Time) url.Values {
		return url.Values{"branch": {"main"}, "ts": {ts.Format(time.RFC3339Nano)}}
	}
	read := func(query url.Values) (*httptest.ResponseRecorder, scanHTTPResponse) {
		rec := preview(query, s.tokenA)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var out scanHTTPResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		return rec, out
	}

	const logical = "rows.ndjson"
	first := s.commitSnapshot(t, logical, []byte("{\"name\":\"first\"}\n{\"name\":\"first tail\"}\n"))
	second := s.commitSnapshot(t, logical, []byte("{\"name\":\"second\"}\n"))
	committed := func(version int32) (time.Time, string) {
		var stamp time.Time
		var txn string
		require.NoError(t, s.pool.QueryRow(context.Background(), `SELECT t.committed_at, t.id::text
			FROM dataset_versions v JOIN dataset_transactions t ON t.id = v.transaction_id
			WHERE v.dataset_id = $1 AND v.version = $2`, s.dataset.ID, version).Scan(&stamp, &txn))
		return stamp, txn
	}
	firstAt, firstTxn := committed(first)
	secondAt, secondTxn := committed(second)
	require.True(t, firstAt.Before(secondAt))
	require.Equal(t, http.StatusNotFound, preview(at(firstAt.Add(-time.Microsecond)), s.tokenA).Code)

	firstResponse, _ := read(url.Values{"branch": {"main"}, "version": {strconv.Itoa(int(first))}})
	for _, stamp := range []time.Time{firstAt.In(time.FixedZone("offset", 5*3600+45*60)), secondAt.Add(-time.Microsecond)} {
		rec, out := read(at(stamp))
		require.Equal(t, firstTxn, out.TransactionID)
		require.JSONEq(t, firstResponse.Body.String(), rec.Body.String())
		require.Len(t, out.Rows, 2)
		require.JSONEq(t, `{"name":"first"}`, string(out.Rows[0]))
	}
	_, latest := read(at(secondAt))
	require.Equal(t, secondTxn, latest.TransactionID)
	require.Len(t, latest.Rows, 1)
	require.JSONEq(t, `{"name":"second"}`, string(latest.Rows[0]))

	paged := at(firstAt)
	paged.Set("limit", "1")
	_, head := read(paged)
	require.NotNil(t, head.NextCursor)
	paged.Set("cursor", *head.NextCursor)
	_, tail := read(paged)
	require.Equal(t, firstTxn, tail.TransactionID)
	require.Len(t, tail.Rows, 1)
	require.JSONEq(t, `{"name":"first tail"}`, string(tail.Rows[0]))
	paged.Set("ts", secondAt.Format(time.RFC3339Nano))
	require.Equal(t, http.StatusBadRequest, preview(paged, s.tokenA).Code)

	transactions := datasetURL + "/branches/main/transactions"
	open := s.do(t, http.MethodPost, transactions, "application/json", strings.NewReader(`{"type":"SNAPSHOT"}`), s.tokenA)
	require.Equal(t, http.StatusCreated, open.Code, open.Body.String())
	var draft struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(open.Body.Bytes(), &draft))
	future := at(secondAt.Add(time.Hour))
	_, whileOpen := read(future)
	require.Equal(t, secondTxn, whileOpen.TransactionID)
	aborted := s.do(t, http.MethodPost, transactions+"/"+draft.ID+":abort", "", nil, s.tokenA)
	require.Equal(t, http.StatusOK, aborted.Code, aborted.Body.String())
	_, afterAbort := read(future)
	require.Equal(t, secondTxn, afterAbort.TransactionID)

	for _, query := range []url.Values{
		{"branch": {"main"}, "ts": {"yesterday"}},
		{"branch": {"main"}, "ts": {""}},
		{"branch": {"main"}, "ts": {firstAt.Format(time.RFC3339Nano)}, "version": {"1"}},
		{"branch": {"main"}, "ts": {firstAt.Format(time.RFC3339Nano), secondAt.Format(time.RFC3339Nano)}},
		{"ts": {firstAt.Format(time.RFC3339Nano)}},
	} {
		require.Equal(t, http.StatusBadRequest, preview(query, s.tokenA).Code, query.Encode())
	}
	missingBranch := at(firstAt)
	missingBranch.Set("branch", "missing")
	require.Equal(t, http.StatusNotFound, preview(missingBranch, s.tokenA).Code)
	require.Equal(t, http.StatusNotFound, preview(at(firstAt), s.tokenB).Code)

	var key string
	require.NoError(t, s.pool.QueryRow(context.Background(),
		`SELECT physical_path FROM dataset_transaction_files WHERE transaction_id = $1 AND logical_path = $2`,
		firstTxn, logical).Scan(&key))
	require.NoError(t, os.Remove(filepath.Join(s.backing.RootDir, filepath.FromSlash(key))))
	unreadable := preview(at(firstAt), s.tokenA)
	require.Equal(t, http.StatusUnprocessableEntity, unreadable.Code, unreadable.Body.String())
	require.Contains(t, unreadable.Body.String(), "dataset_file_bytes_missing")
	_, stillCurrent := read(at(secondAt))
	require.Equal(t, secondTxn, stillCurrent.TransactionID)
}
