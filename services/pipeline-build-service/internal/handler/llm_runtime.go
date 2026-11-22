package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	pipelineexpression "github.com/openfoundry/openfoundry-go/libs/pipeline-expression"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
)

type LLMTransformRequest struct {
	Node          executor.NodeContext
	Payload       json.RawMessage
	TransformType string
	InputRows     []map[string]json.RawMessage
	SampleSize    int
	Preview       bool
}

type LLMTransformRunner interface {
	RunLLMTransform(ctx context.Context, req LLMTransformRequest) (executor.NodeResult, error)
}

type AIServiceLLMConfig struct {
	AgentRuntime bool
	BaseURL      string
	BearerToken  string
	Client       *http.Client
	Timeout      time.Duration
}

type AIServiceLLMRunner struct {
	cfg AIServiceLLMConfig
}

func NewAIServiceLLMRunner(cfg AIServiceLLMConfig) *AIServiceLLMRunner {
	return &AIServiceLLMRunner{cfg: cfg}
}

type llmNodeConfig struct {
	Prompt       string   `json:"prompt,omitempty"`
	SystemPrompt string   `json:"system_prompt,omitempty"`
	Model        string   `json:"model,omitempty"`
	InputColumn  string   `json:"input_column,omitempty"`
	OutputColumn string   `json:"output_column,omitempty"`
	OutputType   string   `json:"output_type,omitempty"`
	MaxRows      int      `json:"max_rows,omitempty"`
	MaxTokens    int      `json:"max_tokens,omitempty"`
	Temperature  *float32 `json:"temperature,omitempty"`
}

func (r *AIServiceLLMRunner) RunLLMTransform(ctx context.Context, req LLMTransformRequest) (executor.NodeResult, error) {
	if strings.TrimSpace(r.cfg.BaseURL) == "" {
		return executor.NodeResult{}, errors.New("ai_service_not_configured: set AI_SERVICE_URL to execute Pipeline Builder LLM nodes")
	}
	cfg := parseLLMNodeConfig(req.Payload)
	if strings.TrimSpace(cfg.Prompt) == "" {
		return executor.NodeResult{}, errors.New("llm_prompt_required")
	}
	if _, err := cfg.fieldType(); err != nil {
		return executor.NodeResult{}, err
	}
	const maxModelCalls = 50
	rows := req.InputRows
	if len(rows) == 0 && len(req.Node.Node.DependsOn) == 0 {
		rows = []map[string]json.RawMessage{{}}
	}
	if req.Preview {
		limit := min(firstPositiveInt(cfg.MaxRows, req.SampleSize, 5), maxModelCalls)
		if len(rows) > limit {
			rows = rows[:limit]
		}
	} else if len(rows) > maxModelCalls {
		return executor.NodeResult{}, fmt.Errorf("llm_row_limit_exceeded: %d input rows exceed the build limit of %d", len(rows), maxModelCalls)
	}

	outRows := make([]map[string]json.RawMessage, 0, len(rows))
	totalPromptTokens := 0
	totalCompletionTokens := 0
	totalTokens := 0
	providerName := ""
	model := cfg.Model
	for _, row := range rows {
		prompt := renderLLMPrompt(cfg, row)
		completion, err := r.complete(ctx, aiChatCompletionRequest{
			UserMessage:     prompt,
			SystemPrompt:    cfg.SystemPrompt,
			Model:           cfg.Model,
			MaxTokens:       cfg.MaxTokens,
			Temperature:     cfg.Temperature,
			FallbackEnabled: true,
		})
		if err != nil {
			return executor.NodeResult{}, err
		}
		next := cloneRawRow(row)
		value, err := cfg.outputValue(completion.Text)
		if err != nil {
			return executor.NodeResult{}, fmt.Errorf("llm_invalid_output: row %d: %w", len(outRows)+1, err)
		}
		next[cfg.OutputColumn] = value
		outRows = append(outRows, next)
		totalPromptTokens += completion.PromptTokens
		totalCompletionTokens += completion.CompletionTokens
		totalTokens += completion.TotalTokens
		if completion.Model != "" {
			model = completion.Model
		}
		if completion.ProviderName != "" {
			providerName = completion.ProviderName
		}
	}

	metaRows := outRows
	if len(metaRows) > 5 {
		metaRows = metaRows[:5]
	}
	columns := inferRawColumns(outRows)
	meta := map[string]any{
		"runtime":        "llm",
		"engine":         "ai_service",
		"transform_type": req.TransformType,
		"rows_affected":  len(outRows),
		"columns":        columns,
		"sample_rows":    metaRows,
		"data_rows":      outRows,
		"prompt":         cfg.Prompt,
		"output_column":  cfg.OutputColumn,
		"output_type":    firstNonEmpty(cfg.OutputType, "string"),
		"provider_name":  providerName,
		"usage": map[string]int{
			"prompt_tokens":     totalPromptTokens,
			"completion_tokens": totalCompletionTokens,
			"total_tokens":      totalTokens,
		},
	}
	if model != "" {
		meta["model"] = model
	}
	payload, _ := json.Marshal(outRows)
	sum := sha256.Sum256(append([]byte(req.Node.Node.ID+"|llm|"), payload...))
	return executor.NodeResult{OutputContentHash: "sha256:" + hex.EncodeToString(sum[:]), Metadata: meta}, nil
}

func parseLLMNodeConfig(raw json.RawMessage) llmNodeConfig {
	cfg := decodeLLMNodeConfig(raw)
	if strings.TrimSpace(cfg.OutputColumn) == "" {
		cfg.OutputColumn = "llm_output"
	}
	cfg.OutputType = strings.ToLower(strings.TrimSpace(cfg.OutputType))
	if cfg.OutputType == "" {
		cfg.OutputType = "string"
	}
	return cfg
}

func decodeLLMNodeConfig(raw json.RawMessage) llmNodeConfig {
	var cfg llmNodeConfig
	if len(bytes.TrimSpace(raw)) == 0 {
		return cfg
	}
	_ = json.Unmarshal(raw, &cfg)
	if strings.TrimSpace(cfg.Prompt) != "" || strings.TrimSpace(cfg.OutputColumn) != "" {
		return cfg
	}
	var envelope struct {
		Config llmNodeConfig `json:"config"`
		LLM    llmNodeConfig `json:"llm"`
	}
	if json.Unmarshal(raw, &envelope) == nil {
		if strings.TrimSpace(envelope.Config.Prompt) != "" || strings.TrimSpace(envelope.Config.OutputColumn) != "" {
			return envelope.Config
		}
		if strings.TrimSpace(envelope.LLM.Prompt) != "" || strings.TrimSpace(envelope.LLM.OutputColumn) != "" {
			return envelope.LLM
		}
	}
	return cfg
}

type aiChatCompletionRequest struct {
	UserMessage     string   `json:"user_message"`
	SystemPrompt    string   `json:"system_prompt,omitempty"`
	Model           string   `json:"model,omitempty"`
	MaxTokens       int      `json:"max_tokens,omitempty"`
	Temperature     *float32 `json:"temperature,omitempty"`
	FallbackEnabled bool     `json:"fallback_enabled"`
}

type aiChatCompletion struct {
	Model            string
	Text             string
	ProviderName     string
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
}

func (r *AIServiceLLMRunner) complete(ctx context.Context, body aiChatCompletionRequest) (aiChatCompletion, error) {
	client := r.cfg.Client
	if client == nil {
		timeout := r.cfg.Timeout
		if timeout <= 0 {
			timeout = 45 * time.Second
		}
		client = &http.Client{Timeout: timeout}
	}
	var requestBody any = body
	path := "/api/v1/ai/chat/completions"
	if r.cfg.AgentRuntime {
		path = "/api/v1/agent-runtime/chat/completions"
		type message struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		}
		messages := make([]message, 0, 2)
		if body.SystemPrompt != "" {
			messages = append(messages, message{Role: "system", Content: body.SystemPrompt})
		}
		messages = append(messages, message{Role: "user", Content: body.UserMessage})
		requestBody = struct {
			Model       string    `json:"model,omitempty"`
			Messages    []message `json:"messages"`
			MaxTokens   int       `json:"max_tokens,omitempty"`
			Temperature *float32  `json:"temperature,omitempty"`
		}{body.Model, messages, body.MaxTokens, body.Temperature}
	}
	payload, err := json.Marshal(requestBody)
	if err != nil {
		return aiChatCompletion{}, err
	}
	url := strings.TrimRight(r.cfg.BaseURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return aiChatCompletion{}, err
	}
	req.Header.Set("content-type", "application/json")
	if token := strings.TrimSpace(r.cfg.BearerToken); token != "" {
		req.Header.Set("authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return aiChatCompletion{}, fmt.Errorf("ai-service request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return aiChatCompletion{}, fmt.Errorf("ai-service completion failed: %s", aiErrorMessage(raw, resp.Status))
	}
	return parseAICompletionResponse(raw)
}

func parseAICompletionResponse(raw []byte) (aiChatCompletion, error) {
	var shaped struct {
		Model        string          `json:"model"`
		Reply        string          `json:"reply"`
		ProviderName string          `json:"provider_name"`
		Usage        json.RawMessage `json:"usage"`
		Choices      []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &shaped); err != nil {
		return aiChatCompletion{}, fmt.Errorf("invalid ai-service response: %w", err)
	}
	text := strings.TrimSpace(shaped.Reply)
	if text == "" && len(shaped.Choices) > 0 {
		text = strings.TrimSpace(shaped.Choices[0].Message.Content)
	}
	if text == "" {
		return aiChatCompletion{}, errors.New("ai-service response did not include reply text")
	}
	completion := aiChatCompletion{Text: text, Model: shaped.Model, ProviderName: shaped.ProviderName}
	if len(shaped.Usage) > 0 {
		var usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		}
		_ = json.Unmarshal(shaped.Usage, &usage)
		completion.PromptTokens = usage.PromptTokens
		completion.CompletionTokens = usage.CompletionTokens
		completion.TotalTokens = usage.TotalTokens
	}
	return completion, nil
}

func aiErrorMessage(raw []byte, fallback string) string {
	var holder map[string]json.RawMessage
	if json.Unmarshal(raw, &holder) == nil {
		for _, key := range []string{"error", "detail", "message"} {
			if value, ok := holder[key]; ok {
				var s string
				if json.Unmarshal(value, &s) == nil && strings.TrimSpace(s) != "" {
					return s
				}
			}
		}
	}
	if trimmed := strings.TrimSpace(string(raw)); trimmed != "" {
		return trimmed
	}
	return fallback
}

func renderLLMPrompt(cfg llmNodeConfig, row map[string]json.RawMessage) string {
	prompt := cfg.Prompt
	if strings.Contains(prompt, "{{") {
		for key, raw := range row {
			prompt = strings.ReplaceAll(prompt, "{{"+key+"}}", rawToPromptValue(raw))
		}
	}
	if cfg.InputColumn != "" {
		if raw, ok := row[cfg.InputColumn]; ok {
			return prompt + "\n\nInput " + cfg.InputColumn + ": " + rawToPromptValue(raw)
		}
	}
	if len(row) > 0 {
		rowJSON, _ := json.Marshal(row)
		return prompt + "\n\nInput row JSON: " + string(rowJSON)
	}
	return prompt
}

func rawToPromptValue(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

func isLLMTransform(transformType string) bool {
	switch strings.ToLower(strings.TrimSpace(transformType)) {
	case "llm", "use_llm", "llm_prompt", "aip_llm":
		return true
	default:
		return false
	}
}

func (r runtimeNodeRunner) runLLM(ctx context.Context, node executor.NodeContext, payload json.RawMessage) (executor.NodeResult, error) {
	if r.LLM == nil {
		return executor.NodeResult{}, errors.New("ai_service_not_configured: set AI_SERVICE_URL to execute Pipeline Builder LLM nodes")
	}
	inputRows := []map[string]json.RawMessage{}
	if len(node.Node.DependsOn) > 0 {
		if r.Table == nil {
			return executor.NodeResult{}, errors.New("llm_input_not_ready: upstream table is unavailable")
		}
		rows, err := r.Table.firstDependencyRows(node)
		if err != nil {
			return executor.NodeResult{}, err
		}
		inputRows = rowsToMaps(rows)
	}
	result, err := r.LLM.RunLLMTransform(ctx, LLMTransformRequest{
		Node:          node,
		Payload:       payload,
		TransformType: metadataString(node.Node.Metadata, "transform_type"),
		InputRows:     inputRows,
		SampleSize:    metadataInt(node.Node.Metadata, "sample_size"),
		Preview:       r.Preview,
	})
	if err != nil {
		return executor.NodeResult{}, err
	}
	if r.Table != nil {
		rows := rowsFromLLMResult(result)
		if rows != nil {
			table := r.Table.runtimeTable(node.Node, "llm", "llm", payload, tableRuntimeConfig{}, nil, rows)
			r.Table.storeTable(node.Node.ID, table)
			r.Table.applyTypedOutput(node.Node.ID, table, result.Metadata, len(node.Node.Outputs) > 0)
		}
	}
	return result, nil
}

func rowsFromLLMResult(result executor.NodeResult) []pipelineexpression.Row {
	raw, ok := result.Metadata["data_rows"]
	if !ok || raw == nil {
		return nil
	}
	var rows []map[string]json.RawMessage
	switch value := raw.(type) {
	case []map[string]json.RawMessage:
		rows = cloneRawRows(value)
	case json.RawMessage:
		_ = json.Unmarshal(value, &rows)
	default:
		encoded, _ := json.Marshal(value)
		_ = json.Unmarshal(encoded, &rows)
	}
	if rows == nil {
		return nil
	}
	out := make([]pipelineexpression.Row, 0, len(rows))
	for _, row := range rows {
		out = append(out, pipelineexpression.Row(cloneRawRow(row)))
	}
	return out
}

func cloneRawRows(rows []map[string]json.RawMessage) []map[string]json.RawMessage {
	out := make([]map[string]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		out = append(out, cloneRawRow(row))
	}
	return out
}

func inferRawColumns(rows []map[string]json.RawMessage) []string {
	prRows := make([]pipelineexpression.Row, 0, len(rows))
	for _, row := range rows {
		prRows = append(prRows, pipelineexpression.Row(row))
	}
	return deriveRuntimeColumns(prRows)
}

func metadataInt(metadata map[string]any, key string) int {
	if v := metadataInt64(metadata, key); v != nil {
		return int(*v)
	}
	return 0
}
