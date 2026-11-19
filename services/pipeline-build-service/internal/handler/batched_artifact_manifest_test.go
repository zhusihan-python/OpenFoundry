package handler

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

func TestBatchedArtifactManifestUsesFrozenInput(t *testing.T) {
	cfg := &models.BatchedBuildConfig{
		JobSpecRID: "job-spec", JobSpecContentHash: "job-hash",
		InputDatasetRID: "dataset", InputBranch: "main", InputVersion: 2, InputPath: "part.ndjson",
		Predicate: `{"column":"status","op":"eq","value":"accepted"}`,
	}
	txn := uuid.New()
	manifest := newBatchedArtifactManifest(cfg, txn)
	require.Equal(t, cfg.JobSpecRID, manifest.JobSpecRID)
	require.Equal(t, cfg.JobSpecContentHash, manifest.JobSpecContentHash)
	require.Equal(t, batchedArtifactSource{
		DatasetRID: cfg.InputDatasetRID, Branch: cfg.InputBranch, Version: cfg.InputVersion, Path: cfg.InputPath,
		TransactionID: txn, PredicateSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(cfg.Predicate))),
	}, manifest.Input)
}
