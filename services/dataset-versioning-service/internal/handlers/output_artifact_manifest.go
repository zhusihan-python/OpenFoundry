package handlers

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

const artifactManifestKey = "artifact_manifest"

type artifactManifest struct {
	Version            int                           `json:"version"`
	BuildID            uuid.UUID                     `json:"build_id"`
	JobSpecRID         string                        `json:"job_spec_rid"`
	JobSpecContentHash string                        `json:"job_spec_content_hash,omitempty"`
	Input              models.PipelineArtifactSource `json:"input"`
	Output             artifactManifestOutput        `json:"output"`
}

type artifactManifestOutput struct {
	DatasetID     uuid.UUID `json:"dataset_id"`
	Branch        string    `json:"branch"`
	TransactionID uuid.UUID `json:"transaction_id"`
	Path          string    `json:"path"`
	Format        string    `json:"format"`
	SizeBytes     int64     `json:"size_bytes"`
	SHA256        string    `json:"sha256"`
	RecordCount   int64     `json:"record_count"`
}

func validateManifestSource(ctx context.Context, tx repo.OutputCommitTxStore, scope repo.TenantScope, manifest *models.PipelineArtifactManifestInput) error {
	src := manifest.Input
	if strings.TrimSpace(manifest.JobSpecRID) == "" || len(manifest.JobSpecRID) > 512 || len(manifest.JobSpecContentHash) > 256 ||
		strings.TrimSpace(src.DatasetRID) == "" || len(src.DatasetRID) > 512 || strings.TrimSpace(src.Branch) == "" || len(src.Branch) > 512 ||
		src.Version < 1 || src.TransactionID == uuid.Nil || (src.Path != "" && !validScanStagedPath(src.Path)) {
		return keyedFileError{errString("artifact_manifest_invalid: invalid input pin or JobSpec identity")}
	}
	if src.PredicateSHA256 != "" {
		if digest, err := hex.DecodeString(src.PredicateSHA256); err != nil || len(digest) != 32 {
			return keyedFileError{errString("artifact_manifest_invalid: predicate_sha256 must be a SHA-256 digest")}
		}
	}
	dataset, err := tx.ResolveDatasetForTenant(ctx, src.DatasetRID, scope)
	if err != nil {
		return err
	}
	if dataset == nil {
		return repo.ErrNotFound
	}
	branch, err := tx.GetRuntimeBranch(ctx, dataset.ID, src.Branch)
	if err != nil && !errors.Is(err, repo.ErrNotFound) {
		return err
	}
	if branch == nil {
		return keyedFileError{errString("artifact_manifest_input_mismatch: input branch is unavailable")}
	}
	version, err := tx.GetVersion(ctx, dataset.ID, src.Version)
	if err != nil {
		return err
	}
	if version == nil || version.TransactionID == nil || *version.TransactionID != src.TransactionID {
		return keyedFileError{errString("artifact_manifest_input_mismatch: input transaction does not match the published version")}
	}
	txn, err := tx.GetRuntimeTransaction(ctx, dataset.ID, src.TransactionID)
	if err != nil {
		return err
	}
	if txn == nil || txn.BranchID != branch.ID || txn.Status != models.TransactionStatusCommitted || txn.TxType != models.TransactionTypeSnapshot {
		return keyedFileError{errString("artifact_manifest_input_mismatch: input transaction is not a committed snapshot on the branch")}
	}
	if src.Path != "" {
		file, err := tx.GetTransactionStagedFile(ctx, txn.ID, src.Path)
		if err != nil {
			return err
		}
		if file == nil {
			return keyedFileError{errString("artifact_manifest_input_mismatch: input path is absent from the transaction")}
		}
	}
	return nil
}

func txArtifactManifest(datasetID, txnID uuid.UUID, obj *repo.StagedOutputObject, input *models.PipelineArtifactManifestInput) *artifactManifest {
	if input == nil {
		return nil
	}
	format := "ndjson"
	if strings.HasSuffix(obj.LogicalPath, stagedParquetExtension) {
		format = stagedParquetFormat
	}
	return &artifactManifest{
		Version: 1, BuildID: obj.BuildID, JobSpecRID: input.JobSpecRID, JobSpecContentHash: input.JobSpecContentHash, Input: input.Input,
		Output: artifactManifestOutput{
			DatasetID: datasetID, Branch: obj.BranchName, TransactionID: txnID, Path: obj.LogicalPath,
			Format: format, SizeBytes: obj.SizeBytes, SHA256: obj.SHA256, RecordCount: obj.RecordCount,
		},
	}
}
