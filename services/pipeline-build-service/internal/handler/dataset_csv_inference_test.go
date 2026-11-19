package handler

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCSVSchemaInferenceMergesRowsAndHeaders(t *testing.T) {
	var infer csvSchemaInference
	require.NoError(t, infer.addHeaders(json.RawMessage(`[{"name":"header_only"}]`)))
	require.NoError(t, infer.addRow(json.RawMessage(`{"amount":2,"code":123,"quantity":1,"note":null}`)))
	require.NoError(t, infer.addRow(json.RawMessage(`{"amount":2.5,"code":"001","quantity":4,"note":"x"}`)))
	require.NoError(t, infer.addRow(json.RawMessage(`{"amount":0,"code":"abc","quantity":5}`)))
	require.Equal(t, &datasetRowsSchema{FileFormat: "TEXT", Fields: []datasetRowsField{
		{Name: "amount", Type: "DOUBLE"},
		{Name: "code", Type: "STRING"},
		{Name: "header_only", Type: "STRING", Nullable: true},
		{Name: "note", Type: "STRING", Nullable: true},
		{Name: "quantity", Type: "LONG"},
	}}, infer.schema())

	var empty csvSchemaInference
	require.NoError(t, empty.addHeaders(json.RawMessage(`[{"name":"name"}]`)))
	require.Equal(t, []datasetRowsField{{Name: "name", Type: "STRING", Nullable: true}}, empty.schema().Fields)
}

func TestCSVSchemaInferenceBoundsColumnMetadata(t *testing.T) {
	var infer csvSchemaInference
	for i := range csvInferenceMaxColumns {
		_, err := infer.column(fmt.Sprint(i))
		require.NoError(t, err)
	}
	_, err := infer.column("extra")
	require.ErrorContains(t, err, "dataset_csv_schema_columns_exceeded")

	var wide csvSchemaInference
	_, err = wide.column(strings.Repeat("x", csvInferenceMaxNameBytes+1))
	require.ErrorContains(t, err, "dataset_csv_schema_names_exceeded")
}
