package handler

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// TestRealDatasetInputExecutionReadsImportedExcel drives the production
// dataset_input node path (lightweight runtime preview/FASTER execution)
// against a real dataset-versioning-service that has an Excel-imported
// dataset registered. Skipped unless the isolated-stack env vars are set:
//
//	P31_DVS_URL=http://p31-dvs:3932 P31_DATASET_RID=<rid> P31_BEARER_TOKEN=<jwt>
//
// This complements the captured-shape contract test: here the request,
// auth, and bytes are all real end to end.
func TestRealDatasetInputExecutionReadsImportedExcel(t *testing.T) {
	base := os.Getenv("P31_DVS_URL")
	rid := os.Getenv("P31_DATASET_RID")
	token := os.Getenv("P31_BEARER_TOKEN")
	if base == "" || rid == "" || token == "" {
		t.Skip("P31 real DVS integration env not set")
	}

	rt := newLightweightTableRuntime()
	rt.datasets = DatasetServiceRowReader{BaseURL: base, BearerToken: token}

	config := `{"dataset_rid":"` + rid + `"}`
	result, err := rt.Run(context.Background(), inputNodeContext("excel_in", config, nil), json.RawMessage(config), "dataset_input")
	require.NoError(t, err)

	assert.Equal(t, "ndjson", result.Metadata["dataset_file_format"])
	assert.Equal(t, "orders.ndjson", result.Metadata["dataset_file_path"])
	assert.Equal(t, false, result.Metadata["dataset_rows_truncated"])

	rows := result.Metadata["data_rows"].([]map[string]json.RawMessage)
	require.Len(t, rows, 2)
	assert.Equal(t, json.RawMessage(`"007"`), rows[0]["order_no"])
	assert.Equal(t, json.RawMessage(`"123456789012345678901234567890"`), rows[0]["code"])
	assert.Equal(t, json.RawMessage(`"富文本描述内容"`), rows[0]["desc"])
	assert.Equal(t, json.RawMessage(`"3"`), rows[0]["qty"])
	assert.Equal(t, json.RawMessage(`"008"`), rows[1]["order_no"])
	assert.Equal(t, json.RawMessage(`null`), rows[1]["desc"])
}

// TestRealPipelinePreviewEntryMatchesExecution drives the actual preview
// entry (executeLocalPipelinePreviewWithPorts, the same function the HTTP
// preview handler calls) against the real DVS and asserts the preview and
// execution paths surface identical column values for the imported dataset.
func TestRealPipelinePreviewEntryMatchesExecution(t *testing.T) {
	base := os.Getenv("P31_DVS_URL")
	rid := os.Getenv("P31_DATASET_RID")
	token := os.Getenv("P31_BEARER_TOKEN")
	if base == "" || rid == "" || token == "" {
		t.Skip("P31 real DVS integration env not set")
	}
	reader := DatasetServiceRowReader{BaseURL: base, BearerToken: token}

	nodesJSON := `[
		{"id":"ds_in","label":"Dataset input","transform_type":"dataset_input","config":{"dataset_rid":"` + rid + `"},"depends_on":[]}
	]`
	var nodes []models.PipelineNode
	require.NoError(t, json.Unmarshal([]byte(nodesJSON), &nodes))

	response, err := executeLocalPipelinePreviewWithPorts(context.Background(), uuid.New(), "ds_in", nodes, 10, ExecutionPorts{DatasetRows: reader})
	require.NoError(t, err)
	require.True(t, response.Fresh)
	require.Nil(t, response.Error)
	require.Len(t, response.Rows, 2)

	assert.Equal(t, json.RawMessage(`"007"`), response.Rows[0]["order_no"])
	assert.Equal(t, json.RawMessage(`"123456789012345678901234567890"`), response.Rows[0]["code"])
	assert.Equal(t, json.RawMessage(`"富文本描述内容"`), response.Rows[0]["desc"])
	assert.Equal(t, json.RawMessage(`"3"`), response.Rows[0]["qty"])
	assert.Equal(t, json.RawMessage(`null`), response.Rows[1]["desc"])
}
