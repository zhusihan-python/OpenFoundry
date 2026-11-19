//go:build integration

package repo_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

func TestHistoricalCSVPreviewUsesPublishedOptions(t *testing.T) {
	s := newScanStack(t)
	dataset := "/v1/datasets/" + s.rid
	file := func(path string, data []byte) map[string]any {
		return map[string]any{"logical_path": path, "operation": "ADD",
			"content_base64": base64.StdEncoding.EncodeToString(data), "size_bytes": len(data)}
	}
	publish := func(options map[string]any, files ...map[string]any) (string, models.RuntimeTransaction) {
		body := map[string]any{"branch": "main", "files": files}
		if options != nil {
			body["schema"] = map[string]any{"file_format": "TEXT", "fields": []any{},
				"custom_metadata": map[string]any{"csv": options}}
		}
		request, err := json.Marshal(body)
		require.NoError(t, err)
		rec := s.do(t, http.MethodPost, dataset+"/outputs:commit", "application/json", bytes.NewReader(request), s.tokenA)
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
		var response models.CommitDatasetOutputResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		var version int
		require.NoError(t, s.pool.QueryRow(context.Background(),
			`SELECT version FROM dataset_versions WHERE dataset_id = $1 AND transaction_id = $2`,
			s.dataset.ID, response.Transaction.ID).Scan(&version))
		return strconv.Itoa(version), response.Transaction
	}
	preview := func(version, cursor string) *httptest.ResponseRecorder {
		query := url.Values{"branch": {"main"}, "version": {version}, "limit": {"1"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		return s.do(t, http.MethodGet, dataset+"/preview?"+query.Encode(), "", nil, s.tokenA)
	}
	read := func(rec *httptest.ResponseRecorder, txn models.RuntimeTransaction, names []string, expected string) scanHTTPResponse {
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var page struct {
			scanHTTPResponse
			Columns []struct{ Name string }
			Format  string
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
		require.Equal(t, txn.ID.String(), page.TransactionID)
		require.Equal(t, "csv", page.Format)
		actualNames := make([]string, 0, len(page.Columns))
		for _, col := range page.Columns {
			actualNames = append(actualNames, col.Name)
		}
		require.Equal(t, names, actualNames)
		actual, err := json.Marshal(page.Rows)
		require.NoError(t, err)
		require.JSONEq(t, expected, string(actual))
		return page.scanHTTPResponse
	}
	encode := func(text string, order binary.ByteOrder, signature []byte) []byte {
		data := bytes.Clone(signature)
		for _, unit := range utf16.Encode([]rune(text)) {
			var encoded [2]byte
			order.PutUint16(encoded[:], unit)
			data = append(data, encoded[:]...)
		}
		return data
	}
	little := func(text string) []byte { return encode(text, binary.LittleEndian, []byte{0xff, 0xfe}) }
	old, oldTxn := publish(map[string]any{"delimiter": ";", "headerRow": 2, "charset": "UTF-16LE", "bom": "require"},
		file("part-a.csv", little("notes\nname;quantity\n\"café\n🌱\";3\npear;4\n")),
		file("part-b.csv", little("notes\nname;quantity\nplum;5\n")))
	newer, newTxn := publish(map[string]any{"headerRow": 0, "charset": "UTF-16BE", "bom": "forbid"},
		file("part.tsv", encode("tea\t8\n", binary.BigEndian, nil)))

	rec := preview(old, "")
	names := []string{"name", "quantity"}
	first := read(rec, oldTxn, names, `[{"name":"café\n🌱","quantity":3}]`)
	require.NotNil(t, first.NextCursor)
	second := read(preview(old, *first.NextCursor), oldTxn, names, `[{"name":"pear","quantity":4}]`)
	require.NotNil(t, second.NextCursor)
	last := read(preview(old, *second.NextCursor), oldTxn, names, `[{"name":"plum","quantity":5}]`)
	require.Nil(t, last.NextCursor)
	require.Equal(t, http.StatusBadRequest, preview(newer, *first.NextCursor).Code)
	read(preview(newer, ""), newTxn, []string{"column_1", "column_2"}, `[{"column_1":"tea","column_2":8}]`)
	require.NotNil(t, oldTxn.CommittedAt)
	query := url.Values{"branch": {"main"}, "ts": {oldTxn.CommittedAt.Format(time.RFC3339Nano)}, "limit": {"1"}}
	fromTime := s.do(t, http.MethodGet, dataset+"/preview?"+query.Encode(), "", nil, s.tokenA)
	require.Equal(t, http.StatusOK, fromTime.Code, fromTime.Body.String())
	require.JSONEq(t, rec.Body.String(), fromTime.Body.String())

	legacy, legacyTxn := publish(nil, file("legacy.csv", []byte("\xef\xbb\xbfname,quantity\nfig,2\n")))
	read(preview(legacy, ""), legacyTxn, names, `[{"name":"fig","quantity":2}]`)
	empty, emptyTxn := publish(nil, file("empty.csv", nil))
	read(preview(empty, ""), emptyTxn, []string{}, `[]`)
	for _, data := range []struct {
		options map[string]any
		bytes   []byte
	}{
		{map[string]any{"header": true, "bom": "require"}, []byte("name,quantity\nfig,2\n")},
		{map[string]any{"header": true, "bom": "forbid"}, []byte("\xef\xbb\xbfname,quantity\nfig,2\n")},
		{map[string]any{"header": true, "charset": "UTF-16BE"}, little("name;quantity\nfig;2\n")},
		{nil, []byte("name,quantity\n\xff,2\n")},
		{map[string]any{"header": true, "charset": "UTF-16LE"}, []byte{0, 0xd8, 0, 0}},
	} {
		invalid, _ := publish(data.options, file("invalid.csv", data.bytes))
		errorPage := preview(invalid, "")
		require.Equal(t, http.StatusUnprocessableEntity, errorPage.Code, errorPage.Body.String())
		require.Contains(t, errorPage.Body.String(), "dataset_csv_decode_failed")
	}
	// Advancing the head through invalid data did not change the old snapshot.
	read(preview(old, ""), oldTxn, names, `[{"name":"café\n🌱","quantity":3}]`)
}
