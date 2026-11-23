package handlers_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
)

func makeEmptyParquetSchema(t *testing.T, fields ...arrow.Field) []byte {
	t.Helper()
	var out bytes.Buffer
	writer, err := pqarrow.NewFileWriter(arrow.NewSchema(fields, nil), &out,
		parquet.NewWriterProperties(parquet.WithVersion(parquet.V2_6)), pqarrow.NewArrowWriterProperties())
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return out.Bytes()
}

func historicalSchemaRequest(hs *scanHarness, query string, subject uuid.UUID) *httptest.ResponseRecorder {
	id := hs.dataset.ID.String()
	return do(hs.h.GetCurrentSchema, http.MethodGet, "/v1/datasets/"+id+"/schema?"+query, "",
		map[string]string{"rid": id}, &authmw.Claims{Sub: subject})
}

func TestHistoricalSchemaBindsPhysicalParquetFieldsToSnapshot(t *testing.T) {
	hs := newScanHarness(t)
	first := makeEmptyParquetSchema(t,
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "legacy", Type: arrow.BinaryTypes.String})
	second := makeEmptyParquetSchema(t,
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "amount", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "added", Type: arrow.FixedWidthTypes.Boolean})
	version1 := hs.commitSnapshotVersionFiles(t,
		stagedSpec{path: "a.parquet", content: first}, stagedSpec{path: "b.parquet", content: first})
	rec1 := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version1), hs.ownerID)
	require.Equal(t, http.StatusOK, rec1.Code, rec1.Body.String())
	require.JSONEq(t, fmt.Sprintf(`{
		"dataset_id": %q, "transaction_id": %q, "branch": "main", "version": %d,
		"fields": [
			{"name": "id", "field_type": "LONG", "nullable": false},
			{"name": "amount", "field_type": "LONG", "nullable": true},
			{"name": "legacy", "field_type": "STRING", "nullable": false}
		]
	}`, hs.dataset.ID, hs.store.versions[hs.dataset.ID][0].TransactionID, version1), rec1.Body.String())

	version2 := hs.commitSnapshotVersion(t, "part.parquet", second)
	rec2 := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version2), hs.ownerID)
	require.Equal(t, http.StatusOK, rec2.Code, rec2.Body.String())
	var result struct {
		Fields json.RawMessage `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &result))
	require.JSONEq(t, `[
		{"name": "id", "field_type": "LONG", "nullable": false},
		{"name": "amount", "field_type": "STRING", "nullable": true},
		{"name": "added", "field_type": "BOOLEAN", "nullable": false}
	]`, string(result.Fields))
	after := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version1), hs.ownerID)
	require.Equal(t, http.StatusOK, after.Code, after.Body.String())
	require.JSONEq(t, rec1.Body.String(), after.Body.String())
}

func TestHistoricalSchemaRejectsUnsupportedOrUnavailableSnapshot(t *testing.T) {
	left := makeEmptyParquetSchema(t, arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64})
	right := makeEmptyParquetSchema(t, arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32})
	nested := makeEmptyParquetSchema(t, arrow.Field{Name: "items", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)})
	tooMany := make([]stagedSpec, 17)
	for i := range tooMany {
		tooMany[i] = stagedSpec{path: fmt.Sprintf("%02d.parquet", i), content: left}
	}
	for _, tc := range []struct {
		name  string
		files []stagedSpec
		code  string
	}{
		{"missing second file", []stagedSpec{{path: "a.parquet", content: left}, {path: "b.parquet"}}, "dataset_file_bytes_missing"},
		{"different schemas", []stagedSpec{{path: "a.parquet", content: left}, {path: "b.parquet", content: right}}, "dataset_schema_files_mismatch"},
		{"unsupported format", []stagedSpec{{path: "a.ndjson", content: []byte("{}\n")}}, "dataset_schema_format_unsupported"},
		{"nested schema", []stagedSpec{{path: "a.parquet", content: nested}}, "dataset_schema_unsupported"},
		{"invalid footer", []stagedSpec{{path: "a.parquet", content: []byte("invalid")}}, "dataset_file_parse_failed"},
		{"too many files", tooMany, "dataset_schema_file_count_unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hs := newScanHarness(t)
			version := hs.commitSnapshotVersionFiles(t, tc.files...)
			rec := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version), hs.ownerID)
			require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), tc.code)
			require.NotContains(t, rec.Body.String(), `"fields"`)
		})
	}
}

func TestHistoricalSchemaRejectsUnboundRequests(t *testing.T) {
	hs := newScanHarness(t)
	version := hs.commitSnapshotVersion(t, "part.parquet", makeStagedOrdersParquet(t, 0, "orders.v1"))
	for _, query := range []string{
		"version=1",
		"branch=main&version=",
		"branch=main&version=0",
		"branch=main&version=2147483648",
		"branch=main&version=%ZZ",
		"branch=main&version=1&version=2",
		"branch=main&branch=other&version=1",
		"branch=main&version=1&limit=2",
	} {
		t.Run(query, func(t *testing.T) {
			rec := historicalSchemaRequest(hs, query, hs.ownerID)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}

	rec := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version), uuid.New())
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	delete(hs.store.branches, hs.dataset.ID)
	rec = historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version), hs.ownerID)
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "branch not found")
}
