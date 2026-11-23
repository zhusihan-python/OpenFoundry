//go:build integration

package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

func TestAIServiceLLMProviderSmokeGated(t *testing.T) {
	if os.Getenv("OPENFOUNDRY_PIPELINE_LLM_PROVIDER_SMOKE") != "1" {
		t.Skip("set OPENFOUNDRY_PIPELINE_LLM_PROVIDER_SMOKE=1 with AI_SERVICE_URL to run provider smoke")
	}
	baseURL := strings.TrimSpace(os.Getenv("AI_SERVICE_URL"))
	require.NotEmpty(t, baseURL, "AI_SERVICE_URL is required for provider smoke")
	runner := NewAIServiceLLMRunner(AIServiceLLMConfig{BaseURL: baseURL, AgentRuntime: os.Getenv("PIPELINE_LLM_PROTOCOL") == "agent-runtime", BearerToken: os.Getenv("AI_SERVICE_BEARER"), PurposeJustification: os.Getenv("PIPELINE_LLM_PURPOSE_JUSTIFICATION")})
	result, err := runner.RunLLMTransform(context.Background(), LLMTransformRequest{
		Node:          executor.NodeContext{Node: executor.Node{ID: "llm_provider_smoke"}},
		Payload:       json.RawMessage(`{"prompt":"Return exactly: ok","output_column":"result","max_rows":1,"max_tokens":8}`),
		TransformType: "llm",
		InputRows:     []map[string]json.RawMessage{{"trail": mustRuntimeJSON("Mesa")}},
	})
	require.NoError(t, err)
	require.Equal(t, 1, result.Metadata["rows_affected"])
	require.NotEmpty(t, rowsFromLLMResult(result))
}

func TestPipelineAIPProviderSmokeGated(t *testing.T) {
	if os.Getenv("OPENFOUNDRY_PIPELINE_AIP_PROVIDER_SMOKE") != "1" {
		t.Skip("set OPENFOUNDRY_PIPELINE_AIP_PROVIDER_SMOKE=1 with AI_SERVICE_URL to run provider smoke")
	}
	baseURL := strings.TrimSpace(os.Getenv("AI_SERVICE_URL"))
	require.NotEmpty(t, baseURL, "AI_SERVICE_URL is required for provider smoke")
	generator := NewAIServicePipelineAIPGenerator(PipelineAIPGeneratorConfig{BaseURL: baseURL, AgentRuntime: os.Getenv("PIPELINE_LLM_PROTOCOL") == "agent-runtime", BearerToken: os.Getenv("AI_SERVICE_BEARER"), PurposeJustification: os.Getenv("PIPELINE_LLM_PURPOSE_JUSTIFICATION")})
	restore := SetExecutionPorts(ExecutionPorts{AIP: generator})
	defer restore()
	body, err := json.Marshal(PipelineAIPGenerateRequest{
		Prompt:          "Create a filter node that keeps rows where distance is greater than 3.",
		SelectedNodeIDs: []string{"source"},
		Nodes: []models.PipelineNode{
			{ID: "source", Label: "Trail source", TransformType: "external", Config: json.RawMessage(`{"rows":[{"trail":"Pine","distance":2},{"trail":"Mesa","distance":5},{"trail":"Lake","distance":3}]}`)},
		},
	})
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	PipelineAIPGenerate(rr, httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/aip/generate", bytes.NewReader(body)))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var response PipelineAIPGenerateResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
	require.NotEmpty(t, response.Nodes)
	require.Nil(t, response.PreviewError)
	require.NotNil(t, response.Preview)
	require.Len(t, response.Preview.Rows, 1)
	require.JSONEq(t, `"Mesa"`, string(response.Preview.Rows[0]["trail"]))
	require.JSONEq(t, `5`, string(response.Preview.Rows[0]["distance"]))
}
