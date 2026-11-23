//go:build integration

package handler

import (
	"context"
	"encoding/json"
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
	runner := NewAIServiceLLMRunner(AIServiceLLMConfig{BaseURL: baseURL, AgentRuntime: os.Getenv("PIPELINE_LLM_PROTOCOL") == "agent-runtime", BearerToken: os.Getenv("AI_SERVICE_BEARER")})
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
	generator := NewAIServicePipelineAIPGenerator(PipelineAIPGeneratorConfig{BaseURL: baseURL, AgentRuntime: os.Getenv("PIPELINE_LLM_PROTOCOL") == "agent-runtime", BearerToken: os.Getenv("AI_SERVICE_BEARER")})
	result, err := generator.GeneratePipelineTransform(context.Background(), PipelineAIPGenerateRequest{
		Prompt:          "Create a filter node that keeps rows where distance is greater than 3.",
		SelectedNodeIDs: []string{"source"},
		Nodes: []models.PipelineNode{
			{ID: "source", Label: "Trail source", TransformType: "external", Config: json.RawMessage(`{"rows":[{"trail":"Mesa","distance":5}]}`)},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Nodes)
}
