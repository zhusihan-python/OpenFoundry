package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

var (
	errManualOntologyOutput     = errors.New("object/link outputs require the graph run path (POST /api/v1/pipelines/{id}/runs)")
	errManualJobSpecUnavailable = errors.New("published manual JobSpec unavailable")
	errManualTargetInvalid      = errors.New("cannot determine manual build output")
)

// Keys are the backing dataset identities the graph executor would publish.
type manualOntologyTargets map[string]string

func manualGraphTargets(pipeline *models.Pipeline) (manualOntologyTargets, error) {
	ir, err := pipeline.RuntimeIR()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errManualTargetInvalid, err)
	}
	if report := ir.Validate(); !report.Valid() {
		return nil, fmt.Errorf("%w: %w", errManualTargetInvalid, report)
	}
	targets := manualOntologyTargets{}
	for _, node := range ir.LegacyNodes() {
		if node.OutputDatasetID == nil && normaliseTableTransform(node.TransformType) != "output" {
			continue
		}
		cfg, err := manualGraphOutputConfig(node.Config)
		if err != nil {
			return nil, fmt.Errorf("%w: node %s: %w", errManualTargetInvalid, node.ID, err)
		}
		if isManualOntologyKind(outputKindForNode(node, cfg)) {
			rid := outputDatasetRIDForPipelineNode(pipeline.ID, node, cfg)
			targets[manualDatasetKey(rid)] = node.ID
		}
	}
	return targets, nil
}

// Match the executor's nested/direct config precedence. Decode strictly so
// malformed declarations cannot disappear into parseOutputDatasetConfig's defaults.
func manualGraphOutputConfig(raw json.RawMessage) (outputDatasetConfig, error) {
	var envelope struct {
		Output outputDatasetConfig `json:"_output"`
	}
	if len(raw) == 0 {
		return envelope.Output, nil
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return outputDatasetConfig{}, err
	}
	if !envelope.Output.empty() {
		return envelope.Output, nil
	}
	var direct outputDatasetConfig
	err := json.Unmarshal(raw, &direct)
	return direct, err
}

func manualDatasetKey(raw string) string {
	if key, err := canonicalDatasetRIDForComparison(raw); err == nil {
		return key
	}
	// Legacy co-located repositories may use opaque dataset names.
	return strings.TrimSpace(raw)
}

func (targets manualOntologyTargets) checkOutputs(rids []string) error {
	for _, rid := range rids {
		if node, ok := targets[manualDatasetKey(rid)]; ok {
			return fmt.Errorf("%w: dataset %s is the backing output of node %s", errManualOntologyOutput, rid, node)
		}
	}
	return nil
}

func (targets manualOntologyTargets) checkSpecs(specs []models.JobSpec) error {
	for _, spec := range specs {
		if err := targets.checkOutputs(spec.OutputDatasetRIDs); err != nil {
			return err
		}
		if spec.PublishedLogicPayload != nil {
			if len(*spec.PublishedLogicPayload) == 0 {
				return fmt.Errorf("%w: %s", errManualJobSpecUnavailable, spec.RID)
			}
			if err := checkManualSpecTarget(spec.RID, "", *spec.PublishedLogicPayload); err != nil {
				return err
			}
		}
		if err := checkManualSpecTarget(spec.RID, "", spec.LogicPayload); err != nil {
			return err
		}
	}
	return nil
}

func isManualOntologyKind(kind string) bool {
	tx := executor.OutputTransaction{OutputKind: kind}
	return isLinkTypeOutput(tx) || isObjectTypeOutput(tx)
}

func checkManualSpecTarget(specRID, transformType string, raw json.RawMessage) error {
	var declaration struct {
		TransformType string               `json:"transform_type"`
		Kind          string               `json:"kind"`
		Output        *outputDatasetConfig `json:"_output"`
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && !json.Valid(raw) {
		return fmt.Errorf("%w: JobSpec %s contains invalid JSON", errManualTargetInvalid, specRID)
	}
	// A legacy scalar or list payload cannot declare a keyed output. Its
	// registered transform type, if present, still determines the output.
	if len(raw) > 0 && raw[0] == '{' {
		fields, nested := datasetOutputConfigFields(raw)
		if _, err := declaredOutputColumns(fields, nested); err != nil {
			return fmt.Errorf("JobSpec %s: %w", specRID, err)
		}
		if err := json.Unmarshal(raw, &declaration); err != nil {
			return fmt.Errorf("%w: JobSpec %s: %w", errManualTargetInvalid, specRID, err)
		}
	}
	kind := declaration.Kind
	if declaration.Output != nil && !declaration.Output.empty() {
		kind = declaration.Output.Kind
	}
	node := models.PipelineNode{TransformType: firstNonEmpty(transformType, declaration.TransformType)}
	if kind := outputKindForNode(node, outputDatasetConfig{Kind: kind}); isManualOntologyKind(kind) {
		return fmt.Errorf("%w: JobSpec %s declares %s", errManualOntologyOutput, specRID, kind)
	}
	return nil
}
