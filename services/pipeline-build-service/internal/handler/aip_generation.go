package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

type PipelineAIPGenerateRequest struct {
	PipelineID       string                `json:"pipeline_id,omitempty"`
	Prompt           string                `json:"prompt"`
	DAG              json.RawMessage       `json:"dag,omitempty"`
	IR               *models.PipelineIR    `json:"ir,omitempty"`
	Nodes            []models.PipelineNode `json:"nodes,omitempty"`
	SelectedNodeIDs  []string              `json:"selected_node_ids,omitempty"`
	SampleSize       int                   `json:"sample_size,omitempty"`
	Model            string                `json:"model,omitempty"`
	MaxTokens        int                   `json:"max_tokens,omitempty"`
	ProviderMetadata map[string]any        `json:"provider_metadata,omitempty"`
}

type PipelineAIPGenerateResult struct {
	Description     string                `json:"description"`
	Nodes           []models.PipelineNode `json:"nodes"`
	Reasoning       string                `json:"reasoning,omitempty"`
	ProviderName    string                `json:"provider_name,omitempty"`
	RawResponse     string                `json:"raw_response,omitempty"`
	GeneratedNodeID string                `json:"generated_node_id,omitempty"`
}

type PipelineAIPGenerateResponse struct {
	Description     string                       `json:"description"`
	Nodes           []models.PipelineNode        `json:"nodes"`
	Prompt          string                       `json:"prompt"`
	SelectedNodeIDs []string                     `json:"selected_node_ids"`
	ProviderName    string                       `json:"provider_name,omitempty"`
	GeneratedAt     time.Time                    `json:"generated_at"`
	Preview         *pipelineNodePreviewResponse `json:"preview,omitempty"`
	PreviewError    *pipelineNodePreviewError    `json:"preview_error,omitempty"`
}

type PipelineAIPGenerator interface {
	GeneratePipelineTransform(ctx context.Context, req PipelineAIPGenerateRequest) (PipelineAIPGenerateResult, error)
}

type AIServicePipelineAIPGenerator struct {
	runner *AIServiceLLMRunner
}

type PipelineAIPGeneratorConfig = AIServiceLLMConfig

func NewAIServicePipelineAIPGenerator(cfg PipelineAIPGeneratorConfig) *AIServicePipelineAIPGenerator {
	return &AIServicePipelineAIPGenerator{runner: NewAIServiceLLMRunner(AIServiceLLMConfig(cfg))}
}

func (g *AIServicePipelineAIPGenerator) GeneratePipelineTransform(ctx context.Context, req PipelineAIPGenerateRequest) (PipelineAIPGenerateResult, error) {
	if g == nil || g.runner == nil || strings.TrimSpace(g.runner.cfg.BaseURL) == "" {
		return PipelineAIPGenerateResult{}, errors.New("ai_service_not_configured: set AI_SERVICE_URL to generate Pipeline Builder transforms")
	}
	userPrompt, err := buildAIPGeneratePrompt(req)
	if err != nil {
		return PipelineAIPGenerateResult{}, err
	}
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 900
	}
	completion, err := g.runner.complete(ctx, aiChatCompletionRequest{
		UserMessage:     userPrompt,
		SystemPrompt:    pipelineAIPSystemPrompt,
		Model:           req.Model,
		MaxTokens:       maxTokens,
		FallbackEnabled: true,
	})
	if err != nil {
		return PipelineAIPGenerateResult{}, err
	}
	result, err := parseAIPGenerateCompletion(completion.Text, req)
	if err != nil {
		return PipelineAIPGenerateResult{}, err
	}
	result.ProviderName = completion.ProviderName
	result.RawResponse = completion.Text
	return result, nil
}

const pipelineAIPSystemPrompt = `You author transforms for OpenFoundry Pipeline Builder. Respond with exactly one JSON object of this shape:
{"description":"<short description>","nodes":[{"id":"<new unique node name>","label":"<label>","transform_type":"<transform type>","depends_on":["<upstream graph node ID>"],"config":{}}]}
Replace placeholders and fill config to implement the user request. The root must contain the literal key "nodes" with at least one new node.
Wire the first transform to the selected node IDs and later transforms to the appropriate upstream nodes. Return only the new nodes.
Omit input_dataset_ids, output_dataset_id and dataset references in config for ordinary transforms. Link upstream nodes using depends_on. Only reuse an explicit dataset binding from the graph in the same input or output role. Never use a graph node ID as a dataset UUID.
Use the actual input columns and values from the request. Use filter with config.predicate as a single Boolean expression string; use select with config.columns as a list of column names; use llm with config.prompt and config.output_column. Structured sql and output_dataset are also supported. Return the description and nodes together in the JSON object.`

func PipelineAIPGenerate(w http.ResponseWriter, r *http.Request) {
	pipelineAIPGenerate(w, r, uuid.Nil)
}

func PipelineAIPGenerateByID(w http.ResponseWriter, r *http.Request) {
	id, err := pipelineIDFromRequest(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_pipeline_id", "detail": err.Error()})
		return
	}
	pipelineAIPGenerate(w, r, id)
}

func pipelineAIPGenerate(w http.ResponseWriter, r *http.Request, pipelineID uuid.UUID) {
	var req PipelineAIPGenerateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_json", "detail": err.Error()})
		return
	}
	if strings.TrimSpace(req.Prompt) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "aip_prompt_required", "detail": "prompt is required"})
		return
	}
	if pipelineID != uuid.Nil {
		req.PipelineID = pipelineID.String()
	}
	nodes, err := aipGenerationNodesForRequest(r.Context(), pipelineID, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_pipeline_graph", "detail": err.Error()})
		return
	}
	req.Nodes = nodes
	req.SelectedNodeIDs = normalizeAIPSelectedNodes(req.SelectedNodeIDs, nodes)

	ports, ok := currentExecutionPorts()
	if !ok || ports.AIP == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "pipeline_aip_generator_not_configured", "detail": "Pipeline AIP generation requires AI_SERVICE_URL-backed generator wiring"})
		return
	}
	result, err := ports.AIP.GeneratePipelineTransform(r.Context(), req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "pipeline_aip_generation_failed", "detail": err.Error()})
		return
	}
	generated := normalizeGeneratedPipelineNodes(result.Nodes, req.SelectedNodeIDs)
	response := PipelineAIPGenerateResponse{
		Description:     firstNonEmpty(result.Description, "Generated pipeline transform"),
		Nodes:           generated,
		Prompt:          req.Prompt,
		SelectedNodeIDs: req.SelectedNodeIDs,
		ProviderName:    result.ProviderName,
		GeneratedAt:     time.Now().UTC(),
	}
	previewID := firstNonEmpty(result.GeneratedNodeID, lastGeneratedNodeID(generated))
	if previewID != "" {
		previewCtx := contextWithCallerBearer(r.Context(), callerBearerFromRequest(r))
		preview, previewErr := executeLocalPipelinePreviewWithPorts(previewCtx, firstNonNilUUID(pipelineID), previewID, append(append([]models.PipelineNode(nil), nodes...), generated...), clampPipelinePreviewSampleSize(firstPositiveInt(req.SampleSize, defaultPipelinePreviewSampleSize)), ports)
		if previewErr == nil {
			response.Preview = &preview
		} else {
			var execErr previewExecutionError
			if errors.As(previewErr, &execErr) {
				response.PreviewError = &execErr.err
			} else {
				response.PreviewError = &pipelineNodePreviewError{Kind: "preview_failed", NodeID: previewID, Message: previewErr.Error()}
			}
		}
	}
	writeJSON(w, http.StatusOK, response)
}

func aipGenerationNodesForRequest(ctx context.Context, pipelineID uuid.UUID, req PipelineAIPGenerateRequest) ([]models.PipelineNode, error) {
	if req.IR != nil {
		return req.IR.Normalize().LegacyNodes(), nil
	}
	if len(req.Nodes) > 0 {
		return append([]models.PipelineNode(nil), req.Nodes...), nil
	}
	if len(bytes.TrimSpace(req.DAG)) > 0 {
		ir, err := models.ParsePipelineIR(req.DAG)
		if err != nil {
			return nil, err
		}
		return ir.LegacyNodes(), nil
	}
	if pipelineID == uuid.Nil {
		return nil, errors.New("dag, ir, nodes, or pipeline id is required")
	}
	repo, ok := currentPipelineAuthoringRepository()
	if !ok {
		return nil, errors.New("pipeline authoring repository is not configured")
	}
	pipeline, err := repo.GetPipeline(ctx, pipelineID)
	if err != nil {
		return nil, err
	}
	if pipeline == nil {
		return nil, errors.New("pipeline not found")
	}
	return pipeline.ParsedNodes()
}

func buildAIPGeneratePrompt(req PipelineAIPGenerateRequest) (string, error) {
	selected := selectedAIPNodes(req.Nodes, req.SelectedNodeIDs)
	contextJSON, err := json.Marshal(map[string]any{
		"pipeline_id":       req.PipelineID,
		"selected_node_ids": req.SelectedNodeIDs,
		"selected_nodes":    selected,
		"all_nodes":         summarizeAIPNodes(req.Nodes),
	})
	if err != nil {
		return "", err
	}
	return "User prompt:\n" + strings.TrimSpace(req.Prompt) + "\n\nPipeline context JSON:\n" + string(contextJSON) + "\n\nReturn JSON only.", nil
}

func parseAIPGenerateCompletion(text string, req PipelineAIPGenerateRequest) (PipelineAIPGenerateResult, error) {
	payload := extractAIPJSON(text)
	var result PipelineAIPGenerateResult
	var target any = &result
	// Some instruction models return the requested nodes as a bare array.
	// Decode both representations into the same proposal before validation.
	if strings.HasPrefix(payload, "[") {
		target = &result.Nodes
	}
	if err := json.Unmarshal([]byte(payload), target); err != nil {
		return result, fmt.Errorf("aip_generation_invalid_json: %w", err)
	}
	if len(result.Nodes) == 0 {
		return result, errors.New("aip_generation_empty_nodes")
	}
	known := make(map[aipDatasetBinding]bool)
	for _, node := range req.Nodes {
		bindings, _ := aipDatasetBindings(node)
		for _, binding := range bindings {
			known[binding] = true
		}
	}
	for _, node := range result.Nodes {
		if strings.TrimSpace(node.TransformType) == "" {
			return result, errors.New("aip_generation_invalid_node: transform_type is required")
		}
		bindings, err := aipDatasetBindings(node)
		if err != nil {
			return result, fmt.Errorf("aip_generation_invalid_node: %w", err)
		}
		for _, binding := range bindings {
			if !known[binding] {
				return result, fmt.Errorf("aip_generation_invalid_binding: unknown %s dataset %q; use depends_on for graph node IDs", binding.role, binding.id)
			}
		}
	}
	result.Nodes = normalizeGeneratedPipelineNodes(result.Nodes, req.SelectedNodeIDs)
	generated := make(map[string]bool, len(result.Nodes))
	for _, node := range result.Nodes {
		generated[node.ID] = true
	}
	if result.GeneratedNodeID != "" && !generated[result.GeneratedNodeID] {
		return result, errors.New("aip_generation_invalid_node: preview target must be a generated node")
	}
	combined := append(append([]models.PipelineNode(nil), req.Nodes...), result.Nodes...)
	report := validatePipelineIRStrict(req.PipelineID, models.NewPipelineIRFromNodes(combined))
	for _, failure := range report.Errors {
		if generated[failure.NodeID] || failure.Code == "cycle_detected" {
			return result, fmt.Errorf("aip_generation_invalid_node: %s: %s", failure.Code, failure.Message)
		}
	}
	return result, nil
}

// A read binding on an existing node does not authorize a generated write.
type aipDatasetBinding struct {
	role string
	id   string
}

func aipDatasetBindings(node models.PipelineNode) ([]aipDatasetBinding, error) {
	cfg, err := parseTableRuntimeConfig(node.Config)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(bytes.TrimSpace(node.Config), []byte("null")) {
		return nil, errors.New("config must be an object")
	}
	inputs := append(uuidStrings(node.InputDatasetIDs), cfg.InputDatasetIDs...)
	outputs := []string{}
	if node.OutputDatasetID != nil {
		outputs = append(outputs, node.OutputDatasetID.String())
	}
	if normaliseTableTransform(node.TransformType) == "output" || node.OutputDatasetID != nil {
		out := parseOutputDatasetConfig(node.Config)
		outputs = append(outputs, out.DatasetID, out.DatasetRID, out.OutputDatasetRID)
	} else {
		inputs = append(inputs, cfg.DatasetID, cfg.DatasetRID)
	}
	var bindings []aipDatasetBinding
	for _, refs := range []struct {
		role string
		ids  []string
	}{{"input", inputs}, {"output", outputs}} {
		for _, raw := range refs.ids {
			id, err := canonicalDatasetRIDForComparison(raw)
			if err != nil {
				return nil, err
			}
			if id != "" {
				bindings = append(bindings, aipDatasetBinding{refs.role, id})
			}
		}
	}
	return bindings, nil
}

func extractAIPJSON(text string) string {
	trimmed := strings.TrimSpace(text)
	if strings.HasPrefix(trimmed, "```") {
		trimmed = strings.TrimPrefix(trimmed, "```json")
		trimmed = strings.TrimPrefix(trimmed, "```")
		trimmed = strings.TrimSuffix(trimmed, "```")
		trimmed = strings.TrimSpace(trimmed)
	}
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		return trimmed
	}
	re := regexp.MustCompile(`(?s)\{.*\}`)
	if match := re.FindString(trimmed); match != "" {
		return match
	}
	return trimmed
}

func normalizeGeneratedPipelineNodes(nodes []models.PipelineNode, selected []string) []models.PipelineNode {
	out := make([]models.PipelineNode, 0, len(nodes))
	previousID := ""
	for i, node := range nodes {
		node.ID = strings.TrimSpace(node.ID)
		if node.ID == "" {
			node.ID = uuid.NewString()
		}
		if strings.TrimSpace(node.Label) == "" {
			node.Label = "AIP transform"
		}
		if strings.TrimSpace(node.TransformType) == "" {
			node.TransformType = "sql"
		}
		if len(bytes.TrimSpace(node.Config)) == 0 || !json.Valid(node.Config) {
			node.Config = json.RawMessage(`{}`)
		}
		if node.DependsOn == nil {
			switch {
			case i > 0 && previousID != "":
				node.DependsOn = []string{previousID}
			case len(selected) > 0:
				node.DependsOn = append([]string(nil), selected...)
			default:
				node.DependsOn = []string{}
			}
		}
		if node.InputDatasetIDs == nil {
			node.InputDatasetIDs = []uuid.UUID{}
		}
		out = append(out, node)
		previousID = node.ID
	}
	return out
}

func normalizeAIPSelectedNodes(selected []string, nodes []models.PipelineNode) []string {
	known := map[string]struct{}{}
	for _, node := range nodes {
		known[node.ID] = struct{}{}
	}
	out := []string{}
	for _, id := range selected {
		if _, ok := known[id]; ok {
			out = append(out, id)
		}
	}
	if len(out) > 0 || len(nodes) == 0 {
		return out
	}
	return []string{nodes[len(nodes)-1].ID}
}

func selectedAIPNodes(nodes []models.PipelineNode, selected []string) []map[string]any {
	selection := map[string]struct{}{}
	for _, id := range selected {
		selection[id] = struct{}{}
	}
	out := []map[string]any{}
	for _, node := range nodes {
		if _, ok := selection[node.ID]; !ok {
			continue
		}
		out = append(out, summarizeAIPNode(node))
	}
	return out
}

func summarizeAIPNodes(nodes []models.PipelineNode) []map[string]any {
	out := make([]map[string]any, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, summarizeAIPNode(node))
	}
	return out
}

func summarizeAIPNode(node models.PipelineNode) map[string]any {
	return map[string]any{
		"id":                node.ID,
		"label":             node.Label,
		"transform_type":    node.TransformType,
		"depends_on":        node.DependsOn,
		"config":            node.Config,
		"input_dataset_ids": node.InputDatasetIDs,
		"output_dataset_id": node.OutputDatasetID,
	}
}

func lastGeneratedNodeID(nodes []models.PipelineNode) string {
	if len(nodes) == 0 {
		return ""
	}
	return nodes[len(nodes)-1].ID
}

func firstNonNilUUID(id uuid.UUID) uuid.UUID {
	if id != uuid.Nil {
		return id
	}
	return uuid.New()
}
