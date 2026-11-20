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
	"testing"
	"unicode/utf16"

	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

func TestPublishedCSVOptionsStayWithVersion(t *testing.T) {
	s := newScanStack(t)
	ctx := context.Background()
	dataset := "/v1/datasets/" + s.rid
	post := func(body []byte) *httptest.ResponseRecorder {
		return s.do(t, http.MethodPost, dataset+"/outputs:commit", "application/json", bytes.NewReader(body), s.tokenA)
	}
	file := func(content []byte) []map[string]any {
		return []map[string]any{{
			"logical_path": "rows.csv", "operation": "ADD",
			"content_base64": base64.StdEncoding.EncodeToString(content), "size_bytes": len(content),
		}}
	}
	request := func(key string, options map[string]any, content []byte) []byte {
		body := map[string]any{
			"branch": "main", "files": file(content),
			"schema": map[string]any{
				"file_format": "TEXT", "fields": []any{},
				"custom_metadata": map[string]any{"csv": options},
			},
		}
		if key != "" {
			body["idempotency_key"] = key
		}
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		return raw
	}
	actualVersion := func(rec *httptest.ResponseRecorder) (int32, models.RuntimeTransaction) {
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
		var published models.CommitDatasetOutputResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &published))
		var version int32
		require.NoError(t, s.pool.QueryRow(ctx,
			`SELECT version FROM dataset_versions WHERE dataset_id = $1 AND transaction_id = $2`,
			s.dataset.ID, published.Transaction.ID).Scan(&version))
		return version, published.Transaction
	}
	readOptions := func(txn models.RuntimeTransaction) models.CsvOptions {
		rec := s.do(t, http.MethodGet, dataset+"/branches/main/transactions/"+txn.ID.String(), "", nil, s.tokenA)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var result struct {
			Metadata struct {
				Schema models.DatasetSchema `json:"schema"`
			} `json:"metadata"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
		require.NotNil(t, result.Metadata.Schema.CustomMetadata)
		require.NotNil(t, result.Metadata.Schema.CustomMetadata.CSV)
		return *result.Metadata.Schema.CustomMetadata.CSV
	}

	raw := []byte{0xff, 0xfe}
	for _, unit := range utf16.Encode([]rune("metadata\nname;price\ncafé;3\n")) {
		raw = binary.LittleEndian.AppendUint16(raw, unit)
	}
	firstBody := request("wp82-first-csv", map[string]any{
		"delimiter": ";", "headerRow": 2, "charset": " utf-16le ", "bom": "REQUIRE", "quote": `"`,
	}, raw)
	firstResponse := post(firstBody)
	firstVersion, first := actualVersion(firstResponse)
	rowTwo := 2
	firstOptions := models.CsvOptions{
		Delimiter: ";", Header: true, HeaderRow: &rowTwo, BOM: "require", Charset: "UTF-16LE", Quote: `"`,
	}
	require.Equal(t, firstOptions, readOptions(first))
	replay := post(firstBody)
	require.Equal(t, http.StatusOK, replay.Code, replay.Body.String())
	require.Equal(t, firstResponse.Body.String(), replay.Body.String())

	secondVersion, second := actualVersion(post(request("", map[string]any{"header": true}, []byte("name,price\npear,5\n"))))
	require.Greater(t, secondVersion, firstVersion)
	rowOne := 1
	require.Equal(t, models.CsvOptions{Header: true, HeaderRow: &rowOne, BOM: "strip", Charset: "UTF-8"}, readOptions(second))
	require.Equal(t, firstOptions, readOptions(first))

	for _, options := range []map[string]any{
		{"delimiter": "::"}, {"headerRow": -1}, {"charset": "bad"}, {"bom": "sometimes"},
	} {
		for _, key := range []string{"", "wp82-invalid-csv"} {
			rec := post(request(key, options, []byte("name,price\npear,5\n")))
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		}
	}
	wrongOptions := post(request("wp82-first-csv", map[string]any{"headerRow": -1}, raw))
	require.Equal(t, http.StatusConflict, wrongOptions.Code, wrongOptions.Body.String())

	var count int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM dataset_versions WHERE dataset_id = $1 AND transaction_id IS NOT NULL`,
		s.dataset.ID).Scan(&count))
	require.Equal(t, 2, count)
	var head string
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT head_transaction_id::text FROM dataset_branches WHERE dataset_id = $1 AND name = 'main'`,
		s.dataset.ID).Scan(&head))
	require.Equal(t, second.ID.String(), head)
	require.Equal(t, firstOptions, readOptions(first))
}
