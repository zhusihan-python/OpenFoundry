package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

func TestPipelineAIPGenerateUsesInjectedGeneratorAndReturnsPreview(t *testing.T) {
	generator := &recordingPipelineAIPGenerator{
		result: PipelineAIPGenerateResult{
			Description: "Keep runnable trails.",
			Nodes: []models.PipelineNode{
				{ID: "aip_filter", Label: "Long trails", TransformType: "filter", Config: json.RawMessage(`{"predicate":"distance > 3"}`)},
			},
		},
	}
	restore := SetExecutionPorts(ExecutionPorts{AIP: generator})
	defer restore()

	body := []byte(`{
		"prompt": "Keep trails longer than three miles",
		"sample_size": 10,
		"selected_node_ids": ["source"],
		"nodes": [
			{
				"id": "source",
				"label": "Trail source",
				"transform_type": "external",
				"config": {
					"rows": [
						{"trail":"Anemone","distance":2.9},
						{"trail":"Mesa","distance":5.2}
					]
				}
			}
		]
	}`)
	rr := httptest.NewRecorder()
	PipelineAIPGenerate(rr, httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/aip/generate", bytes.NewReader(body)))
	require.Equal(t, http.StatusOK, rr.Code)

	var response PipelineAIPGenerateResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&response))
	require.Equal(t, "Keep runnable trails.", response.Description)
	require.Equal(t, []string{"source"}, response.SelectedNodeIDs)
	require.Len(t, response.Nodes, 1)
	require.Equal(t, []string{"source"}, response.Nodes[0].DependsOn)
	require.NotNil(t, response.Preview)
	require.Nil(t, response.PreviewError)
	require.Equal(t, "aip_filter", response.Preview.NodeID)
	require.Len(t, response.Preview.Rows, 1)
	require.JSONEq(t, `"Mesa"`, string(response.Preview.Rows[0]["trail"]))
	require.Equal(t, "Keep trails longer than three miles", generator.seen.Prompt)
	require.Len(t, generator.seen.Nodes, 1)
}

func TestAIServicePipelineAIPGeneratorParsesJSONNodeResponse(t *testing.T) {
	var sawPrompt string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/ai/chat/completions", r.URL.Path)
		var req aiChatCompletionRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		sawPrompt = req.UserMessage
		writeJSON(w, http.StatusOK, map[string]any{
			"reply": `{
				"description": "Generated an LLM summary node.",
				"nodes": [
					{
						"id": "aip_llm",
						"label": "Summarize trails",
						"transform_type": "llm",
						"depends_on": ["source"],
						"config": {"prompt":"Summarize {{trail}}","output_column":"summary"}
					}
				]
			}`,
			"provider_name": "mock-ai",
		})
	}))
	defer server.Close()

	generator := NewAIServicePipelineAIPGenerator(PipelineAIPGeneratorConfig{BaseURL: server.URL, Client: server.Client()})
	result, err := generator.GeneratePipelineTransform(context.Background(), PipelineAIPGenerateRequest{
		Prompt:          "Add an LLM summary",
		SelectedNodeIDs: []string{"source"},
		Nodes: []models.PipelineNode{
			{ID: "source", Label: "Trail source", TransformType: "external", Config: json.RawMessage(`{"rows":[{"trail":"Mesa"}]}`)},
		},
	})
	require.NoError(t, err)
	require.Contains(t, sawPrompt, "Add an LLM summary")
	require.Equal(t, "Generated an LLM summary node.", result.Description)
	require.Equal(t, "mock-ai", result.ProviderName)
	require.Len(t, result.Nodes, 1)
	require.Equal(t, "llm", result.Nodes[0].TransformType)
	require.JSONEq(t, `{"prompt":"Summarize {{trail}}","output_column":"summary"}`, string(result.Nodes[0].Config))
}

type recordingPipelineAIPGenerator struct {
	seen   PipelineAIPGenerateRequest
	result PipelineAIPGenerateResult
	err    error
}

func (g *recordingPipelineAIPGenerator) GeneratePipelineTransform(_ context.Context, req PipelineAIPGenerateRequest) (PipelineAIPGenerateResult, error) {
	g.seen = req
	return g.result, g.err
}

func TestAIPProviderProposalValidation(t *testing.T) {
	req := PipelineAIPGenerateRequest{
		Nodes:           []models.PipelineNode{{ID: "sales", TransformType: "external", Config: json.RawMessage(`{"rows":[{"quantity":9}]}`)}},
		SelectedNodeIDs: []string{"sales"},
	}
	for _, tc := range []struct{ name, nodes, reason string }{
		{"empty", `[]`, "empty_nodes"},
		{"invented id", `[{"id":"flt","transform_type":"filter","depends_on":["orders"],"config":{"predicate":"quantity > 1"}}]`, "missing_dependency"},
		{"colliding id", `[{"id":"sales","transform_type":"select","config":{"columns":["quantity"]}}]`, "duplicate_node_id"},
		{"cycle", `[{"id":"flt","depends_on":["flt"],"transform_type":"filter","config":{"predicate":"quantity > 1"}}]`, "cycle_detected"},
		{"missing type", `[{"id":"flt"}]`, "transform_type is required"},
		{"missing predicate", `[{"transform_type":"filter","config":{"condition":"quantity > 1"}}]`, "filter_missing_predicate"},
		{"unknown column", `[{"transform_type":"filter","config":{"predicate":"absent > 1"}}]`, "absent"},
		{"node as dataset", `[{"transform_type":"filter","input_dataset_ids":["sales"]}]`, "invalid_json"},
		{"unrequested uuid", `[{"transform_type":"filter","input_dataset_ids":["0f59ec85-47eb-4a7a-a4c1-e99600520b9c"]}]`, "use depends_on"},
		{"binding in config", `[{"transform_type":"passthrough","config":{"config":{"dataset_rid":"0f59ec85-47eb-4a7a-a4c1-e99600520b9c"}}}]`, "use depends_on"},
		{"binding in output", `[{"transform_type":"output_dataset","config":{"_output":{"dataset_rid":"0f59ec85-47eb-4a7a-a4c1-e99600520b9c"}}}]`, "unknown output dataset"},
		{"invalid config", `[{"transform_type":"filter","config":[{"predicate":"true"}]}]`, "config"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseAIPGenerateCompletion(`{"nodes":`+tc.nodes+`}`, req)
			require.ErrorContains(t, err, tc.reason)
		})
	}
}

func TestSavedAIPProposalPreviewsExistingSourceWithoutChangingSavedGraph(t *testing.T) {
	repo := newFakePipelineAuthoringRepo()
	restoreRepo := SetPipelineAuthoringRepository(repo)
	defer restoreRepo()
	w := httptest.NewRecorder()
	CreatePipeline(w, httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewBufferString(`{"name":"Volumes","nodes":[{"id":"sales","transform_type":"dataset_input","config":{"dataset_id":"168c0564-2751-44a8-9c59-7691e924b4b0"}}]}`)))
	require.Equal(t, http.StatusCreated, w.Code)
	var saved models.Pipeline
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &saved))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var prompt aiChatCompletionRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&prompt))
		require.Contains(t, prompt.UserMessage, `"selected_node_ids":["sales"]`)
		require.Contains(t, prompt.SystemPrompt, "Omit input_dataset_ids")
		writeJSON(w, 200, map[string]string{"reply": `{"nodes":[{"id":"large","label":"Large sales","transform_type":"filter","config":{"predicate":"qty >= 10"}}]}`})
	}))
	defer server.Close()
	rows := &aipPreviewRowSource{fakeDatasetRowSource: fakeDatasetRowSource{result: datasetRowsResult(t, `{"qty":2}`, `{"qty":11}`)}}
	restore := SetExecutionPorts(ExecutionPorts{DatasetRows: rows, AIP: NewAIServicePipelineAIPGenerator(PipelineAIPGeneratorConfig{BaseURL: server.URL, Client: server.Client()})})
	defer restore()
	w = httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+saved.ID.String()+"/aip/generate", bytes.NewBufferString(`{"prompt":"Retain large sales with qty at least ten"}`))
	request.Header.Set("Authorization", "Bearer requesting-user")
	PipelineAIPGenerateByID(w, request)
	require.Equal(t, "requesting-user", rows.bearer)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var result PipelineAIPGenerateResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.Nil(t, result.PreviewError)
	require.NotNil(t, result.Preview)
	require.Len(t, result.Preview.Rows, 1)
	require.JSONEq(t, "11", string(result.Preview.Rows[0]["qty"]))
	require.Equal(t, []string{"sales", "large"}, result.Preview.SourceChain)
	stored, err := repo.GetPipeline(t.Context(), saved.ID)
	require.NoError(t, err)
	nodes, err := stored.ParsedNodes()
	require.NoError(t, err)
	require.Len(t, nodes, 1)
}

func TestAIPDatasetBindingsMatchTheirExistingRoles(t *testing.T) {
	input := uuid.New()
	output := uuid.New()
	req := PipelineAIPGenerateRequest{
		Nodes: []models.PipelineNode{
			{ID: "source", TransformType: "dataset_input", Config: mustRuntimeJSON(map[string]any{"dataset_rid": datasetRIDPrefix + input.String()})},
			{ID: "target", TransformType: "output_dataset", DependsOn: []string{"source"}, OutputDatasetID: &output},
		},
		SelectedNodeIDs: []string{"source"},
	}
	node := models.PipelineNode{ID: "out", TransformType: "output_dataset", DependsOn: []string{"source"}, InputDatasetIDs: []uuid.UUID{input}, OutputDatasetID: &output}
	check := func(node models.PipelineNode) error {
		t.Helper()
		b, err := json.Marshal(PipelineAIPGenerateResult{Nodes: []models.PipelineNode{node}})
		require.NoError(t, err)
		_, err = parseAIPGenerateCompletion(string(b), req)
		return err
	}
	require.NoError(t, check(node))
	node.OutputDatasetID = &input
	require.ErrorContains(t, check(node), "unknown output dataset")
}

func TestAIPFencedArrayUsesSameGraphValidation(t *testing.T) {
	source := models.PipelineNode{ID: "input", TransformType: "external", Config: json.RawMessage(`{"rows":[{"n":5}]}`)}
	req := PipelineAIPGenerateRequest{Nodes: []models.PipelineNode{source}, SelectedNodeIDs: []string{"input"}}
	text := "```json\n" + `[{"id":"high","transform_type":"filter","depends_on":["input"],"config":{"predicate":"n > 1"}}]` + "\n```"
	result, err := parseAIPGenerateCompletion(text, req)
	require.NoError(t, err)
	require.Len(t, result.Nodes, 1)
	require.Equal(t, "high", result.Nodes[0].ID)
	_, err = parseAIPGenerateCompletion("[]", req)
	require.ErrorContains(t, err, "aip_generation_empty_nodes")
	_, err = parseAIPGenerateCompletion(`[{"id":"high","transform_type":"filter","depends_on":["absent"],"config":{"predicate":"n > 1"}}]`, req)
	require.ErrorContains(t, err, "missing_dependency")
}

type aipPreviewRowSource struct {
	fakeDatasetRowSource
	bearer string
}

func (s *aipPreviewRowSource) ReadDatasetRows(ctx context.Context, req DatasetRowsRequest) (DatasetRowsResult, error) {
	s.bearer = callerBearerFromContext(ctx)
	return s.fakeDatasetRowSource.ReadDatasetRows(ctx, req)
}
