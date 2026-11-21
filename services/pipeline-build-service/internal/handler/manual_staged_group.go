package handler

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

const stagedManualGroupPath = "/internal/datasets/outputs:commit-staged-group"

type manualStagedGroupConfig struct {
	Manifest   batchedArtifactManifest `json:"manifest"`
	SpecHashes map[string]string       `json:"spec_hashes"`
	Excel      *manualExcelGroupConfig `json:"excel,omitempty"`
}

func stagedManualGroupConfig(specs []models.JobSpec) (*manualStagedGroupConfig, error) {
	var root *models.JobSpec
	for i := range specs {
		var fields map[string]json.RawMessage
		if json.Unmarshal(specs[i].LogicPayload, &fields) != nil {
			continue
		}
		if fields["staged_output_group"] == nil {
			if fields["excel_outputs"] != nil {
				return nil, errors.New("excel_outputs requires staged_output_group")
			}
			continue
		}
		var enabled bool
		if err := json.Unmarshal(fields["staged_output_group"], &enabled); err != nil || !enabled || root != nil {
			return nil, errors.New("staged_output_group must be true on exactly one input spec")
		}
		root = &specs[i]
	}
	if root == nil {
		return nil, nil
	}
	var options struct {
		TransformType string          `json:"transform_type"`
		ExcelOutputs  json.RawMessage `json:"excel_outputs"`
	}
	_ = json.Unmarshal(root.LogicPayload, &options)
	cfg, err := parseTableRuntimeConfig(root.LogicPayload)
	if err != nil {
		return nil, err
	}
	pin, err := parseDatasetInputSnapshot(cfg.DatasetSnapshot)
	if err != nil {
		return nil, err
	}
	if options.TransformType != "dataset_input" || pin == nil || len(root.Inputs) != 1 ||
		!validBatchedObjectPath(cfg.DatasetPath) || root.ContentHash == "" || len(root.OutputDatasetRIDs) == 0 {
		return nil, errors.New("staged output groups require one pinned dataset_input with an explicit file and a published JobSpec")
	}
	source, err := canonicalDatasetRIDForComparison(root.Inputs[0].DatasetRID)
	if err != nil {
		return nil, err
	}
	if source == "" {
		return nil, errors.New("staged group input dataset is required")
	}
	targets := make(map[string]bool)
	for _, spec := range specs {
		if len(spec.OutputDatasetRIDs) == 0 {
			return nil, errors.New("each staged group job must declare an output")
		}
		for _, target := range spec.OutputDatasetRIDs {
			target, err := canonicalDatasetRIDForComparison(target)
			if err != nil {
				return nil, err
			}
			if targets[target] || target == source {
				return nil, errors.New("staged group targets must be distinct and cannot replace the pinned input")
			}
			targets[target] = true
		}
	}
	if len(targets) < 2 || len(targets) > maxManualDatasetGroup {
		return nil, errors.New("staged output groups require 2 to 16 outputs")
	}
	config := &manualStagedGroupConfig{
		Manifest: batchedArtifactManifest{
			JobSpecRID: root.RID, JobSpecContentHash: root.ContentHash,
			Input: batchedArtifactSource{DatasetRID: datasetRIDPrefix + source, Branch: pin.Branch, Version: pin.Version,
				TransactionID: pin.TransactionID, Path: cfg.DatasetPath},
		},
		SpecHashes: make(map[string]string, len(specs)),
	}
	if len(options.ExcelOutputs) != 0 {
		if len(specs) != 1 || len(pin.Excel) != 0 || pin.FlattenJSON || !strings.HasSuffix(strings.ToLower(cfg.DatasetPath), ".xlsx") {
			return nil, errors.New("excel_outputs requires one XLSX JobSpec without a single-sheet config")
		}
		declared, err := canonicalDatasetRIDForComparison(cfg.DatasetRID)
		if err != nil || (declared != "" && declared != source) || cfg.DatasetID != "" ||
			cfg.DatasetBranch != "" || cfg.DatasetVersion != "" || cfg.Rows != nil ||
			cfg.SeedRows != nil || cfg.Records != nil || cfg.Data != nil {
			return nil, errors.New("excel_outputs input must match the single pinned JobSpec dataset")
		}
		excel, err := parseManualExcelGroup(options.ExcelOutputs, targets)
		if err != nil {
			return nil, err
		}
		config.Excel = excel
	}
	for _, spec := range specs {
		if spec.RID != root.RID {
			var shape struct {
				TransformType string `json:"transform_type"`
			}
			_ = json.Unmarshal(spec.LogicPayload, &shape)
			if (shape.TransformType != "filter" && shape.TransformType != "passthrough") || len(spec.Inputs) != 1 {
				return nil, errors.New("staged group downstream jobs must filter or pass through a single DAG input")
			}
			input, err := canonicalDatasetRIDForComparison(spec.Inputs[0].DatasetRID)
			if err != nil || !targets[input] {
				return nil, errors.New("staged group downstream jobs cannot read another dataset")
			}
		}
		config.SpecHashes[spec.RID] = manualGroupPayloadHash(spec.LogicPayload)
	}
	return config, nil
}

func manualGroupPayloadHash(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (c *manualStagedGroupConfig) verifyPlan(plan executor.Plan) error {
	if len(plan.Nodes) != len(c.SpecHashes) {
		return errors.New("staged group JobSpec set changed")
	}
	for _, node := range plan.Nodes {
		payload, _ := node.Metadata["logic_payload"].(json.RawMessage)
		root := node.ID == c.Manifest.JobSpecRID
		if (root && len(node.DependsOn) != 0) || (!root && len(node.DependsOn) != 1) {
			return errors.New("staged group plan must start from the single pinned input")
		}
		if c.SpecHashes[node.ID] != manualGroupPayloadHash(payload) {
			return errors.New("staged group JobSpec changed")
		}
	}
	return nil
}

func (m manualBuildExecution) stageGroup(ctx context.Context, group []manualGroupOutput, outcome executor.Outcome) error {
	if m.staged.Excel != nil {
		clientConfig, err := (BatchedClientConfig{BaseURL: m.dataset.BaseURL, Client: m.dataset.Client}).withDefaults()
		if err != nil {
			return err
		}
		for i := range group {
			if err := m.stageGroupMember(ctx, batchedDatasetClient{cfg: clientConfig}, &group[i], group[i].body.Schema); err != nil {
				return err
			}
		}
		return nil
	}
	root := outcome.Results[m.staged.Manifest.JobSpecRID]
	source := m.staged.Manifest.Input
	rid, _ := root.Metadata["dataset_rid"].(string)
	path, _ := root.Metadata["dataset_file_path"].(string)
	branch, _ := root.Metadata["dataset_branch"].(string)
	version, _ := root.Metadata["dataset_version"].(int32)
	txn, _ := root.Metadata["dataset_transaction_id"].(string)
	canonical, _ := canonicalDatasetRIDForComparison(rid)
	if datasetRIDPrefix+canonical != source.DatasetRID || path != source.Path || branch != source.Branch ||
		version != source.Version || txn != transactionRIDPrefix+source.TransactionID.String() {
		return errors.New("staged group result does not bind to the configured input snapshot")
	}
	sourceRows, ok := fullResultRows(root.Metadata)
	if !ok {
		return errors.New("staged group requires fully materialized source rows")
	}
	schema := inferOutputSchema(resultColumns(root.Metadata), sourceRows, normaliseOutputFileFormat("NDJSON"))
	cfg, err := (BatchedClientConfig{BaseURL: m.dataset.BaseURL, Client: m.dataset.Client}).withDefaults()
	if err != nil {
		return err
	}
	client := batchedDatasetClient{cfg: cfg}
	for i := range group {
		if err := m.stageGroupMember(ctx, client, &group[i], schema); err != nil {
			return err
		}
	}
	return nil
}

func (m manualBuildExecution) stageGroupMember(ctx context.Context, client batchedDatasetClient, member *manualGroupOutput, schema *datasetOutputSchema) error {
	body, remote := &member.body, member.remote
	if len(body.Files) != 1 || body.Files[0].ContentBase64 == nil || body.OutputSchemaVersion != "" ||
		body.CreateIfMissing || body.TransactionType != "SNAPSHOT" || !strings.HasSuffix(body.Files[0].LogicalPath, ".ndjson") {
		return errors.New("staged group requires a complete NDJSON snapshot for each output")
	}
	file := &body.Files[0]
	content := file.ContentBase64
	digest := manualGroupPayloadHash(content)
	req := batchedStageRequest{DatasetRID: remote.dataset, Branch: remote.branch,
		ReservationRID: member.owned.TransactionRID, BuildID: m.buildID, LogicalPath: file.LogicalPath, IdempotencyKey: remote.key}
	sealed, err := client.StageStream(ctx, req, func(_ context.Context, writer *bufio.Writer) error {
		_, err := writer.Write(content)
		return err
	})
	if err != nil {
		return err
	}
	id, err := uuid.Parse(sealed.StagedObjectID)
	if err != nil || id == uuid.Nil || sealed.DatasetRID != req.DatasetRID || sealed.Branch != req.Branch ||
		sealed.ReservationID != remote.binding.ReservationID.String() || sealed.BuildID != m.buildID.String() ||
		sealed.LogicalPath != req.LogicalPath || sealed.IdempotencyKey != req.IdempotencyKey ||
		sealed.SizeBytes != int64(len(content)) || sealed.SHA256 != digest || sealed.RecordCount != int64(bytes.Count(content, []byte{'\n'})) {
		return fmt.Errorf("staged group seal does not match dataset %s and its output bytes", remote.dataset)
	}
	file.StagedObjectID, file.SHA256, file.Operation = sealed.StagedObjectID, sealed.SHA256, "STAGED_ADD"
	file.StoragePath, file.ContentBase64 = "", nil
	body.ManualReservation, body.PreviewRows, body.PreviewColumns = nil, nil, nil
	body.ReservationID, body.BuildID = remote.binding.ReservationID.String(), m.buildID.String()
	if body.ArtifactManifest == nil {
		body.ArtifactManifest = &m.staged.Manifest
	}
	body.Schema = schema
	return nil
}
