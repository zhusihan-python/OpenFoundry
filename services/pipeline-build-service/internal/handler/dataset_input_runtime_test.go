package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

type fakeDatasetRowSource struct {
	calls  []DatasetRowsRequest
	result DatasetRowsResult
	err    error
}

func (f *fakeDatasetRowSource) ReadDatasetRows(_ context.Context, req DatasetRowsRequest) (DatasetRowsResult, error) {
	f.calls = append(f.calls, req)
	return f.result, f.err
}

func datasetRowsResult(t *testing.T, rows ...string) DatasetRowsResult {
	t.Helper()
	out := DatasetRowsResult{DatasetID: uuid.New().String(), Path: "orders.csv", Format: "csv", Source: "backing_file", Columns: []string{"sku", "qty"}, Truncated: false}
	out.Rows = make([]map[string]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		var parsed map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(row), &parsed))
		out.Rows = append(out.Rows, parsed)
	}
	return out
}

func inputNodeContext(id string, config string, metadata map[string]any) executor.NodeContext {
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadata["logic_kind"] = "transform"
	metadata["transform_type"] = "dataset_input"
	metadata["logic_payload"] = json.RawMessage(config)
	return executor.NodeContext{
		BuildID:     uuid.New(),
		BuildBranch: "master",
		Attempt:     1,
		Node:        executor.Node{ID: id, JobID: uuid.New(), Metadata: metadata},
	}
}

func TestInputNodeLoadsReferencedDatasetRows(t *testing.T) {
	source := &fakeDatasetRowSource{result: datasetRowsResult(t, `{"sku":"007","qty":2}`, `{"sku":"AB-9","qty":10}`)}
	rt := newLightweightTableRuntime()
	rt.datasets = source

	config := `{"dataset_rid":"ri.foundry.main.dataset.` + uuid.New().String() + `"}`
	result, err := rt.Run(context.Background(), inputNodeContext("orders_in", config, nil), json.RawMessage(config), "dataset_input")
	require.NoError(t, err)
	require.Len(t, source.calls, 1)
	assert.Equal(t, maxDatasetInputRows, source.calls[0].Limit) // no sample_size → full capped read
	assert.False(t, source.calls[0].AllowTruncation)            // formal execution refuses truncated input

	rows := result.Metadata["data_rows"].([]map[string]json.RawMessage)
	require.Len(t, rows, 2)
	assert.Equal(t, json.RawMessage(`"007"`), rows[0]["sku"]) // SKU leading zeros survive
	assert.Equal(t, json.RawMessage(`2`), rows[0]["qty"])
	assert.Equal(t, "csv", result.Metadata["dataset_file_format"])
	assert.NotEmpty(t, result.Metadata["dataset_rid"])
}

func TestInputNodeInlineRowsTakePriorityOverDataset(t *testing.T) {
	source := &fakeDatasetRowSource{result: datasetRowsResult(t, `{"sku":"007"}`)}
	rt := newLightweightTableRuntime()
	rt.datasets = source

	config := `{"dataset_id":"` + uuid.New().String() + `","rows":[{"a":1}]}`
	_, err := rt.Run(context.Background(), inputNodeContext("inline_in", config, nil), json.RawMessage(config), "dataset_input")
	require.NoError(t, err)
	assert.Empty(t, source.calls) // inline data wins; dataset never queried
}

func TestInputNodeWithoutReferenceKeepsSyntheticBehaviour(t *testing.T) {
	source := &fakeDatasetRowSource{}
	rt := newLightweightTableRuntime()
	rt.datasets = source

	config := `{"sample_size":3}`
	result, err := rt.Run(context.Background(), inputNodeContext("plain_in", config, nil), json.RawMessage(config), "dataset_input")
	require.NoError(t, err)
	assert.Empty(t, source.calls)
	rows := result.Metadata["data_rows"].([]map[string]json.RawMessage)
	require.Len(t, rows, 3)
	assert.Equal(t, json.RawMessage(`true`), rows[0]["synthetic"])
}

func TestInputNodeReferenceLoadFailureIsExplainableError(t *testing.T) {
	source := &fakeDatasetRowSource{err: errors.New("dataset_input_read_failed: dataset-versioning returned 404: dataset not found")}
	rt := newLightweightTableRuntime()
	rt.datasets = source

	config := `{"dataset_id":"` + uuid.New().String() + `"}`
	_, err := rt.Run(context.Background(), inputNodeContext("bad_in", config, nil), json.RawMessage(config), "dataset_input")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_input_load_failed")
	assert.Contains(t, err.Error(), "dataset not found")
	assert.NotContains(t, err.Error(), "synthetic")
}

func TestInputNodeReferenceWithoutConfiguredSourceFails(t *testing.T) {
	rt := newLightweightTableRuntime()

	config := `{"dataset_rid":"ri.foundry.main.dataset.` + uuid.New().String() + `"}`
	_, err := rt.Run(context.Background(), inputNodeContext("unwired_in", config, nil), json.RawMessage(config), "dataset_input")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_input_source_not_configured")
}

func TestInputNodeReferenceFromNodeMetadataInputDatasetIDs(t *testing.T) {
	source := &fakeDatasetRowSource{result: datasetRowsResult(t, `{"v":1}`)}
	rt := newLightweightTableRuntime()
	rt.datasets = source

	datasetID := uuid.New().String()
	_, err := rt.Run(context.Background(), inputNodeContext("meta_in", `{}`, map[string]any{"input_dataset_ids": []string{datasetID}}), json.RawMessage(`{}`), "dataset_input")
	require.NoError(t, err)
	require.Len(t, source.calls, 1)
	assert.Equal(t, datasetID, source.calls[0].DatasetRID)
}

func TestInputNodePinnedVersionIsRejectedExplicitly(t *testing.T) {
	source := &fakeDatasetRowSource{result: datasetRowsResult(t, `{"v":1}`)}
	rt := newLightweightTableRuntime()
	rt.datasets = source

	config := `{"dataset_id":"` + uuid.New().String() + `","dataset_version":"3"}`
	_, err := rt.Run(context.Background(), inputNodeContext("pinned_in", config, nil), json.RawMessage(config), "dataset_input")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_input_version_not_supported")
	assert.Empty(t, source.calls) // never falls back to reading the latest rows
}

func TestPassthroughWithoutDependenciesLoadsDataset(t *testing.T) {
	source := &fakeDatasetRowSource{result: datasetRowsResult(t, `{"sku":"0123"}`)}
	rt := newLightweightTableRuntime()
	rt.datasets = source

	config := `{"dataset_id":"` + uuid.New().String() + `"}`
	result, err := rt.Run(context.Background(), executor.NodeContext{Node: executor.Node{ID: "bind_ds", JobID: uuid.New(), Metadata: map[string]any{
		"logic_kind": "transform", "transform_type": "passthrough",
		"logic_payload": json.RawMessage(config),
	}}}, json.RawMessage(config), "passthrough")
	require.NoError(t, err)
	require.Len(t, source.calls, 1)
	rows := result.Metadata["data_rows"].([]map[string]json.RawMessage)
	require.Len(t, rows, 1)
	assert.Equal(t, json.RawMessage(`"0123"`), rows[0]["sku"])
}

func TestDatasetLoadedRowsStayNumericallyFilterable(t *testing.T) {
	source := &fakeDatasetRowSource{result: datasetRowsResult(t, `{"sku":"007","qty":2}`, `{"sku":"008","qty":5}`)}
	rt := newLightweightTableRuntime()
	rt.datasets = source

	// Source node.
	config := `{"dataset_rid":"ri.foundry.main.dataset.` + uuid.New().String() + `"}`
	_, err := rt.Run(context.Background(), inputNodeContext("ds_in", config, nil), json.RawMessage(config), "dataset_input")
	require.NoError(t, err)

	// Downstream numeric filter over the loaded rows.
	filtered, err := rt.Run(context.Background(), executor.NodeContext{Node: executor.Node{ID: "flt", JobID: uuid.New(), DependsOn: []string{"ds_in"}, Metadata: map[string]any{
		"logic_kind": "transform", "transform_type": "filter", "logic_payload": json.RawMessage(`{"predicate":"qty > 3"}`),
	}}}, json.RawMessage(`{"predicate":"qty > 3"}`), "filter")
	require.NoError(t, err)
	rows := filtered.Metadata["data_rows"].([]map[string]json.RawMessage)
	require.Len(t, rows, 1)
	assert.Equal(t, json.RawMessage(`"008"`), rows[0]["sku"])
}

func TestPreviewPipelineNodeReadsDatasetReferencedInput(t *testing.T) {
	source := &fakeDatasetRowSource{result: datasetRowsResult(t, `{"sku":"007","qty":2}`, `{"sku":"011","qty":9}`)}
	datasetRID := "ri.foundry.main.dataset." + uuid.New().String()
	nodesJSON := `[
		{"id":"ds_in","label":"Dataset input","transform_type":"dataset_input","config":{"dataset_rid":"` + datasetRID + `"},"depends_on":[]},
		{"id":"flt","label":"Filter","transform_type":"filter","config":{"predicate":"qty > 3"},"depends_on":["ds_in"]}
	]`
	var nodes []models.PipelineNode
	require.NoError(t, json.Unmarshal([]byte(nodesJSON), &nodes))

	response, err := executeLocalPipelinePreviewWithPorts(context.Background(), uuid.New(), "flt", nodes, 10, ExecutionPorts{DatasetRows: source})
	require.NoError(t, err)
	require.True(t, response.Fresh)
	require.Nil(t, response.Error)
	require.Equal(t, []string{"ds_in", "flt"}, response.SourceChain)
	require.Len(t, source.calls, 1)
	assert.Equal(t, datasetRID, source.calls[0].DatasetRID)
	assert.True(t, source.calls[0].AllowTruncation) // preview may truncate
	require.Len(t, response.Rows, 1)
	assert.Equal(t, json.RawMessage(`"011"`), response.Rows[0]["sku"])
	assert.Equal(t, json.RawMessage(`9`), response.Rows[0]["qty"])
}

func TestRuntimeNodeRunnerWiresDatasetSourceFromPorts(t *testing.T) {
	// The FASTER execution path builds its runner via newRuntimeNodeRunner(ports);
	// the dataset source must flow through that wiring.
	source := &fakeDatasetRowSource{result: datasetRowsResult(t, `{"sku":"0055"}`)}
	runner := newRuntimeNodeRunner(ExecutionPorts{DatasetRows: source})

	result, err := runner.Run(context.Background(), executor.NodeContext{
		BuildID: uuid.New(), BuildBranch: "master", Attempt: 1,
		Node: executor.Node{ID: "src", JobID: uuid.New(), Metadata: map[string]any{
			"logic_kind": "transform", "transform_type": "dataset_input",
			"logic_payload":     json.RawMessage(`{"dataset_id":"` + uuid.New().String() + `"}`),
			"input_dataset_ids": []string{},
		}},
	})
	require.NoError(t, err)
	require.Len(t, source.calls, 1)
	rows := result.Metadata["data_rows"].([]map[string]json.RawMessage)
	require.Len(t, rows, 1)
	assert.Equal(t, json.RawMessage(`"0055"`), rows[0]["sku"])
}

func TestDatasetServiceRowReaderRequestsRegisteredRows(t *testing.T) {
	var gotPath, gotQuery, gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.Query().Encode()
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"dataset_id":"` + uuid.New().String() + `","path":"orders.csv","format":"csv","source":"backing_file","columns":["sku","qty"],"rows":[{"sku":"007","qty":2}],"truncated":false}`))
	}))
	defer server.Close()

	reader := DatasetServiceRowReader{BaseURL: server.URL, BearerToken: "svc-token"}
	out, err := reader.ReadDatasetRows(context.Background(), DatasetRowsRequest{DatasetRID: "ri.foundry.main.dataset." + uuid.New().String(), Path: "orders.csv", Branch: "release", Limit: 25})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(gotPath, "/v1/datasets/ri.foundry.main.dataset."), gotPath)
	assert.Contains(t, gotQuery, "limit=25")
	assert.Contains(t, gotQuery, "mode=execution")
	assert.Contains(t, gotQuery, "path=orders.csv")
	assert.Contains(t, gotQuery, "branch=release")
	assert.Equal(t, "Bearer svc-token", gotAuth)
	assert.Equal(t, "orders.csv", out.Path)
	require.Len(t, out.Rows, 1)
	assert.Equal(t, json.RawMessage(`"007"`), out.Rows[0]["sku"])
}

func TestDatasetServiceRowReaderSendsPreviewMode(t *testing.T) {
	var gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Encode()
		_, _ = w.Write([]byte(`{"columns":[],"rows":[]}`))
	}))
	defer server.Close()

	reader := DatasetServiceRowReader{BaseURL: server.URL}
	_, err := reader.ReadDatasetRows(context.Background(), DatasetRowsRequest{DatasetRID: uuid.New().String(), AllowTruncation: true})
	require.NoError(t, err)
	assert.Contains(t, gotQuery, "mode=preview")
}

func TestDatasetServiceRowReaderClampsLimit(t *testing.T) {
	var gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Encode()
		_, _ = w.Write([]byte(`{"columns":[],"rows":[]}`))
	}))
	defer server.Close()

	reader := DatasetServiceRowReader{BaseURL: server.URL}
	_, err := reader.ReadDatasetRows(context.Background(), DatasetRowsRequest{DatasetRID: uuid.New().String(), Limit: 999999})
	require.NoError(t, err)
	assert.Contains(t, gotQuery, "limit=1000")
}

func TestDatasetServiceRowReaderRejectsInvalidReferences(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("server must not be called for invalid references")
	}))
	defer server.Close()

	reader := DatasetServiceRowReader{BaseURL: server.URL}
	for _, ref := range []string{"", "http://evil.example/dataset", "ri.foundry.main.dataset.not-a-uuid", "../../etc/passwd"} {
		_, err := reader.ReadDatasetRows(context.Background(), DatasetRowsRequest{DatasetRID: ref})
		require.Error(t, err, "ref %q", ref)
		assert.Contains(t, err.Error(), "dataset_input_invalid_reference")
	}
}

func TestDatasetServiceRowReaderWithoutBaseURLFails(t *testing.T) {
	reader := DatasetServiceRowReader{}
	_, err := reader.ReadDatasetRows(context.Background(), DatasetRowsRequest{DatasetRID: uuid.New().String()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_input_source_not_configured")
}

func TestDatasetServiceRowReaderSurfacesDatasetErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"dataset not found"}`))
	}))
	defer server.Close()

	reader := DatasetServiceRowReader{BaseURL: server.URL}
	_, err := reader.ReadDatasetRows(context.Background(), DatasetRowsRequest{DatasetRID: uuid.New().String()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_input_read_failed")
	assert.Contains(t, err.Error(), "dataset not found")
}

func TestDatasetServiceRowReaderRefusesRedirects(t *testing.T) {
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("redirect target must not be reached")
	}))
	defer redirectTarget.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget.URL+"/secret", http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	reader := DatasetServiceRowReader{BaseURL: server.URL}
	_, err := reader.ReadDatasetRows(context.Background(), DatasetRowsRequest{DatasetRID: uuid.New().String()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_input_read_failed")
}

func TestDatasetServiceRowReaderRefusesRedirectsWithInjectedClient(t *testing.T) {
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("redirect target must not be reached")
	}))
	defer redirectTarget.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget.URL+"/secret", http.StatusFound)
	}))
	defer server.Close()

	// An injected client without its own redirect policy must still be
	// shadowed by the reader's redirect refusal.
	reader := DatasetServiceRowReader{BaseURL: server.URL, Client: &http.Client{Timeout: 10 * time.Second}}
	_, err := reader.ReadDatasetRows(context.Background(), DatasetRowsRequest{DatasetRID: uuid.New().String()})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset_input_read_failed")
}

func TestDatasetServiceRowReaderForwardsCallerBearer(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"columns":[],"rows":[]}`))
	}))
	defer server.Close()

	// Without a caller identity the static service token applies.
	reader := DatasetServiceRowReader{BaseURL: server.URL, BearerToken: "svc-token"}
	_, err := reader.ReadDatasetRows(context.Background(), DatasetRowsRequest{DatasetRID: uuid.New().String()})
	require.NoError(t, err)
	assert.Equal(t, "Bearer svc-token", gotAuth)

	// A forwarded caller bearer wins over the service token: manual
	// preview/run reads happen as the user.
	ctx := contextWithCallerBearer(context.Background(), "user-token")
	_, err = reader.ReadDatasetRows(ctx, DatasetRowsRequest{DatasetRID: uuid.New().String()})
	require.NoError(t, err)
	assert.Equal(t, "Bearer user-token", gotAuth)
}

func TestCallerBearerFromRequest(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	assert.Empty(t, callerBearerFromRequest(req))
	req.Header.Set("Authorization", "Basic zmlsZQ==")
	assert.Empty(t, callerBearerFromRequest(req))
	req.Header.Set("Authorization", "Bearer abc.def")
	assert.Equal(t, "abc.def", callerBearerFromRequest(req))
}
