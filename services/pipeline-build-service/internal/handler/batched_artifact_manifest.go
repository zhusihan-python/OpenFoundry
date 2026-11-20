package handler

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/google/uuid"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

type batchedArtifactManifest struct {
	JobSpecRID         string                `json:"job_spec_rid"`
	JobSpecContentHash string                `json:"job_spec_content_hash,omitempty"`
	Input              batchedArtifactSource `json:"input"`
}

type batchedArtifactSource struct {
	DatasetRID      string    `json:"dataset_rid"`
	Branch          string    `json:"branch"`
	Version         int32     `json:"version"`
	TransactionID   uuid.UUID `json:"transaction_id"`
	Path            string    `json:"path,omitempty"`
	PredicateSHA256 string    `json:"predicate_sha256,omitempty"`
}

func newBatchedArtifactManifest(cfg *models.BatchedBuildConfig, txn uuid.UUID) *batchedArtifactManifest {
	source := batchedArtifactSource{DatasetRID: cfg.InputDatasetRID, Branch: cfg.InputBranch,
		Version: cfg.InputVersion, TransactionID: txn, Path: cfg.InputPath}
	if cfg.Predicate != "" {
		sum := sha256.Sum256([]byte(cfg.Predicate))
		source.PredicateSHA256 = hex.EncodeToString(sum[:])
	}
	return &batchedArtifactManifest{
		JobSpecRID: cfg.JobSpecRID, JobSpecContentHash: cfg.JobSpecContentHash, Input: source,
	}
}
