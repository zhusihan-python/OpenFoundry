package handlers_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
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
	firstFields := `[{"name":"id","field_type":"LONG","nullable":false},{"name":"amount","field_type":"LONG","nullable":true},{"name":"legacy","field_type":"STRING","nullable":false}]`
	firstDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(firstFields)))
	require.JSONEq(t, fmt.Sprintf(`{
		"dataset_id": %q, "transaction_id": %q, "branch": "main", "version": %d,
		"content_hash": %q, "fields": %s
	}`, hs.dataset.ID, hs.store.versions[hs.dataset.ID][0].TransactionID, version1, firstDigest, firstFields), rec1.Body.String())

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
	match := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d&expected_content_hash=%s", version1, firstDigest), hs.ownerID)
	require.Equal(t, http.StatusOK, match.Code, match.Body.String())
	require.JSONEq(t, rec1.Body.String(), match.Body.String())
	mismatch := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d&expected_content_hash=%s", version2, firstDigest), hs.ownerID)
	require.Equal(t, http.StatusPreconditionFailed, mismatch.Code, mismatch.Body.String())
	require.JSONEq(t, `{"error": "dataset_schema_content_hash_mismatch: the committed schema does not match expected_content_hash",
		"code": "dataset_schema_content_hash_mismatch"}`, mismatch.Body.String())
	require.NotContains(t, mismatch.Body.String(), `"fields"`)

	republished := hs.commitSnapshotVersion(t, "repacked.parquet", first)
	same := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d&expected_content_hash=%s", republished, firstDigest), hs.ownerID)
	require.Equal(t, http.StatusOK, same.Code, same.Body.String())
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

func TestHistoricalSchemaHashCoversEveryPublicFieldProperty(t *testing.T) {
	hs := newScanHarness(t)
	id := arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64}
	amount := arrow.Field{Name: "amount", Type: arrow.PrimitiveTypes.Int64}
	baseline := hs.commitSnapshotVersion(t, "part.parquet", makeEmptyParquetSchema(t, id, amount))
	rec := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", baseline), hs.ownerID)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var result struct {
		ContentHash string `json:"content_hash"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
	require.Len(t, result.ContentHash, 64)
	for _, tc := range []struct {
		name   string
		fields []arrow.Field
	}{
		{"order", []arrow.Field{amount, id}},
		{"name", []arrow.Field{id, {Name: "total", Type: amount.Type}}},
		{"type", []arrow.Field{id, {Name: amount.Name, Type: arrow.PrimitiveTypes.Int32}}},
		{"nullability", []arrow.Field{id, {Name: amount.Name, Type: amount.Type, Nullable: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			version := hs.commitSnapshotVersion(t, "part.parquet", makeEmptyParquetSchema(t, tc.fields...))
			query := fmt.Sprintf("branch=main&version=%d&expected_content_hash=%s", version, result.ContentHash)
			rec := historicalSchemaRequest(hs, query, hs.ownerID)
			require.Equal(t, http.StatusPreconditionFailed, rec.Code, rec.Body.String())
		})
	}
}

func TestHistoricalSchemaCrossChecksTheTransactionThatCutTheVersion(t *testing.T) {
	hs := newScanHarness(t)
	id := arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64}
	first := hs.commitSnapshotVersion(t, "a.parquet", makeEmptyParquetSchema(t, id,
		arrow.Field{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true}))
	second := hs.commitSnapshotVersion(t, "b.parquet", makeEmptyParquetSchema(t, id,
		arrow.Field{Name: "amount", Type: arrow.BinaryTypes.String, Nullable: true}))
	txn := func(version int32) uuid.UUID { return *hs.store.versions[hs.dataset.ID][version-1].TransactionID }
	plain := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", first), hs.ownerID)
	require.Equal(t, http.StatusOK, plain.Code, plain.Body.String())
	var result struct {
		ContentHash string `json:"content_hash"`
	}
	require.NoError(t, json.Unmarshal(plain.Body.Bytes(), &result))

	for _, query := range []string{
		fmt.Sprintf("branch=main&version=%d&transaction_id=%s", first, txn(first)),
		fmt.Sprintf("branch=main&version=%d&expected_content_hash=%s&transaction_id=%s", first, result.ContentHash, txn(first)),
	} {
		rec := historicalSchemaRequest(hs, query, hs.ownerID)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.JSONEq(t, plain.Body.String(), rec.Body.String())
	}
	for _, tc := range []struct {
		name    string
		version int32
		txn     uuid.UUID
	}{
		{"another version's transaction", first, txn(second)},
		{"earlier version's transaction", second, txn(first)},
		{"unknown transaction", first, uuid.New()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d&transaction_id=%s", tc.version, tc.txn), hs.ownerID)
			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			var answer struct {
				Error string  `json:"error"`
				Code  *string `json:"code"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &answer))
			require.Equal(t, "dataset_schema_transaction_mismatch", *answer.Code)
			require.Contains(t, answer.Error, "dataset_schema_transaction_mismatch: ")
			require.Contains(t, answer.Error, txn(tc.version).String())
			require.Contains(t, answer.Error, tc.txn.String())
		})
	}
	// The pairing is answered before the schema precondition: a wrong pair
	// is a 409 even when the digest does not match either.
	rec := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d&expected_content_hash=%s&transaction_id=%s",
		second, result.ContentHash, txn(first)), hs.ownerID)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
}

func TestHistoricalSchemaRejectsUnboundRequests(t *testing.T) {
	hs := newScanHarness(t)
	version := hs.commitSnapshotVersion(t, "part.parquet", makeStagedOrdersParquet(t, 0, "orders.v1"))
	known := uuid.New().String()
	for _, query := range []string{
		"version=1",
		"branch=main&version=",
		"branch=main&version=0",
		"branch=main&version=2147483648",
		"branch=main&version=%ZZ",
		"branch=main&version=1&version=2",
		"branch=main&branch=other&version=1",
		"branch=main&version=1&limit=2",
		"branch=main&expected_content_hash=" + strings.Repeat("a", 64),
		"branch=main&version=1&expected_content_hash=",
		"branch=main&version=1&expected_content_hash=" + strings.Repeat("A", 64),
		"branch=main&version=1&expected_content_hash=" + strings.Repeat("a", 63),
		"branch=main&version=1&expected_content_hash=" + strings.Repeat("x", 64),
		"branch=main&version=1&expected_content_hash=" + strings.Repeat("a", 64) + "&expected_content_hash=" + strings.Repeat("a", 64),
		"branch=main&version=1&transaction_id=",
		"branch=main&version=1&transaction_id=not-a-uuid",
		"branch=main&version=1&transaction_id=00000000-0000-0000-0000-000000000000",
		"branch=main&version=1&transaction_id=" + known + "&transaction_id=" + known,
		"branch=main&version=not-a-number&transaction_id=" + known,
	} {
		t.Run(query, func(t *testing.T) {
			rec := historicalSchemaRequest(hs, query, hs.ownerID)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}

	// Without a version the request stays on the legacy current-schema
	// route: an extra transaction_id neither binds nor rejects it, the
	// answer is byte-for-byte the one the plain request gets.
	hs.store.views = map[uuid.UUID][]models.DatasetView{}
	legacy := historicalSchemaRequest(hs, "branch=main", hs.ownerID)
	unbound := historicalSchemaRequest(hs, "branch=main&transaction_id="+known, hs.ownerID)
	require.Equal(t, legacy.Code, unbound.Code, unbound.Body.String())
	require.Equal(t, legacy.Body.String(), unbound.Body.String())
	require.NotContains(t, unbound.Body.String(), "versioned schema")

	rec := historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version), uuid.New())
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	delete(hs.store.branches, hs.dataset.ID)
	rec = historicalSchemaRequest(hs, fmt.Sprintf("branch=main&version=%d", version), hs.ownerID)
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "branch not found")
}
