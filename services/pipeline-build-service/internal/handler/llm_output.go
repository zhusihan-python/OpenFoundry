package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

func (cfg llmNodeConfig) fieldType() (string, error) {
	switch cfg.OutputType {
	case "string", "json":
		// A JSON document can have any shape. Preserve its text in a STRING
		// column, including for empty relations and downstream json_extract.
		return "STRING", nil
	case "boolean":
		return "BOOLEAN", nil
	case "double":
		return "DOUBLE", nil
	default:
		return "", fmt.Errorf("unsupported LLM output_type %q", cfg.OutputType)
	}
}

func (cfg llmNodeConfig) outputValue(text string) (json.RawMessage, error) {
	switch cfg.OutputType {
	case "json":
		if !json.Valid([]byte(text)) {
			return nil, errors.New("expected a valid JSON document")
		}
		fallthrough
	case "string":
		return json.Marshal(text)
	default:
		return columnCastValue(mustRuntimeJSON(text), cfg.OutputType)
	}
}

func validateLLMSchema(node models.PipelineIRNode, deps []pipelineStrictSchema, report *pipelineStrictValidationReport) pipelineStrictSchema {
	cfg := parseLLMNodeConfig(node.Config)
	base := firstSchema(deps)
	if len(deps) == 0 {
		base.Known = true
	}
	if strings.TrimSpace(cfg.Prompt) == "" {
		report.addError(node.ID, nil, "llm_prompt_required", "LLM node requires a prompt")
	}
	kind, err := cfg.fieldType()
	if err != nil {
		report.addError(node.ID, &cfg.OutputColumn, "llm_invalid_output_type", err.Error())
		return base
	}
	if !base.Known {
		return base
	}
	fields := cloneStrictFields(base.Fields)
	target := pipelineStrictValidationField{Name: cfg.OutputColumn, FieldType: kind, typedOutput: true}
	replaced := false
	for i := range fields {
		// The LLM changes one cell per row and retains every other field.
		fields[i].typedOutput = true
		if fields[i].Name == cfg.OutputColumn {
			fields[i] = target
			replaced = true
		}
	}
	if !replaced {
		fields = append(fields, target)
	}
	return pipelineStrictSchema{Known: true, Fields: fields}
}

// An LLM declaration may describe only the generated field. Like the rows,
// its schema must also carry through the other input fields.
func llmDeclaredSchema(declared, derived pipelineStrictSchema) pipelineStrictSchema {
	for _, field := range derived.Fields {
		if _, exists := declared.field(field.Name); !exists {
			declared.Fields = append(declared.Fields, field)
		}
	}
	return declared
}
