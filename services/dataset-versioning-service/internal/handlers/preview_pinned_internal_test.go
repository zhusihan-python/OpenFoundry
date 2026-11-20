package handlers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

func TestPinnedParquetPreviewTypesAndExactInt64(t *testing.T) {
	schema := &models.DatasetSchema{
		FileFormat: "PARQUET",
		Fields: []models.Field{
			{Name: "order_id", Type: models.FieldTypeLong},
			{Name: "note", Type: models.FieldTypeString, Nullable: true},
		},
	}
	page := datasetScanAllFilesResponse{Format: "parquet", SchemaVersion: "orders.v1", Schema: schema}
	columns, rows, err := pinnedPreviewRows(page)
	require.NoError(t, err)
	require.Len(t, columns, 2)
	require.Equal(t, "order_id", columns[0].Name)
	require.Equal(t, models.FieldTypeLong, columns[0].FieldType)
	require.False(t, *columns[0].Nullable)
	require.True(t, *columns[1].Nullable)
	require.NotNil(t, rows)
	require.Empty(t, rows)

	page.Rows = []json.RawMessage{json.RawMessage(`{"order_id":9223372036854775807,"amount":"99999999999999.9999","note":null}`)}
	_, rows, err = pinnedPreviewRows(page)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.JSONEq(t, `{"order_id":"9223372036854775807","amount":"99999999999999.9999","note":null}`, string(rows[0]))
	require.Contains(t, string(page.Rows[0]), `"order_id":9223372036854775807`)
}

func TestPinnedEmptyStagedNDJSONPreviewColumns(t *testing.T) {
	metadata := models.JSONValue(`{"staged_output":{"record_count":0},"schema":{"file_format":"ndjson","fields":[{"name":"sheet","type":"STRING","nullable":false},{"name":"code","type":"STRING","nullable":true}]}}`)
	schema := emptyStagedNDJSONSchema(metadata)
	require.NotNil(t, schema)
	page := datasetScanAllFilesResponse{Format: "ndjson", Schema: schema, Rows: []json.RawMessage{}}
	columns, rows, err := pinnedPreviewRows(page)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Len(t, columns, 2)
	require.Equal(t, "sheet", columns[0].Name)
	require.Equal(t, "code", columns[1].Name)
	require.Equal(t, models.FieldTypeString, columns[0].FieldType)
	require.False(t, *columns[0].Nullable)
	require.True(t, *columns[1].Nullable)
	require.Nil(t, emptyStagedNDJSONSchema(models.JSONValue(`{"schema":{"fields":[{"name":"old"}]}}`)))
	require.Nil(t, emptyStagedNDJSONSchema(models.JSONValue(`{"staged_output":{"record_count":1},"schema":{"fields":[{"name":"old"}]}}`)))
}
