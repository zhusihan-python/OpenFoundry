//go:build integration

package postgres

import (
	"context"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// TestBuildFlowPublishPinHashCollisionRejected proves the WP3.4 reuse
// guard on real Postgres: a colliding content hash must NOT reuse the
// canonical job_specs row for a different dataset_snapshot definition —
// build planning reads the branch mapping while LoadPlan restores the
// canonical logic_payload, so a silent reuse would bind the two sides to
// different pins. Identical pinned content still dedups, and unpinned
// collisions keep the historical reuse behavior.
func TestBuildFlowPublishPinHashCollisionRejected(t *testing.T) {
	ctx := context.Background()
	f := newBuildFlowFixture(ctx, t)
	org := uuidPtr(uuid.New())
	sub := uuid.New()
	scopeCtx := handler.ContextWithTenantScope(ctx, &handler.TenantScope{TenantID: org, Subject: sub})

	pipelineRID := "ri.foundry.main.pipeline." + uuid.NewString()
	outRID := "ri.foundry.main.dataset." + uuid.NewString()
	inRID := "ri.foundry.main.dataset." + uuid.NewString()
	fixedHash := "colliding-hash-0001"
	inputs := []models.InputSpec{{DatasetRID: inRID}}

	pinPayload := func(version int, txn string) []byte {
		return []byte(`{"dataset_id":"` + inRID + `","dataset_snapshot":{"branch":"main","version":` + strconv.Itoa(version) + `,"transaction_id":"ri.foundry.main.transaction.` + txn + `"}}`)
	}
	publish := func(payload []byte, hash string) (handler.PublishedJobSpec, error) {
		return f.repo.PublishJobSpec(scopeCtx, "TRANSFORM",
			handler.CreateJobSpecRequest{PipelineRID: pipelineRID, BranchName: "main", Inputs: inputs, OutputDatasetRIDs: []string{outRID}, LogicPayload: payload, ContentHash: &hash}, sub.String())
	}

	txnA := "11111111-1111-1111-1111-111111111111"
	txnB := "22222222-2222-2222-2222-222222222222"

	// Baseline: the pinned spec publishes normally.
	a, err := publish(pinPayload(3, txnA), fixedHash)
	require.NoError(t, err)

	// A DIFFERENT pin under the SAME content hash is rejected outright.
	_, err = publish(pinPayload(4, txnB), fixedHash)
	require.Error(t, err)
	require.Contains(t, err.Error(), "job_spec_content_conflict")

	// The canonical row and the branch mapping are untouched by the
	// rejected publish: exactly one spec row, still bound to the original
	// rid.
	var specRows int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM job_specs WHERE pipeline_rid=$1 AND branch_name='main'`, pipelineRID).Scan(&specRows))
	require.Equal(t, 1, specRows)
	var boundRID string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT job_spec_rid FROM pipeline_job_specs WHERE pipeline_rid=$1 AND output_dataset_rid=$2 AND branch_name='main'`, pipelineRID, outRID).Scan(&boundRID))
	require.Equal(t, a.RID, boundRID)

	// Identical pinned content (same hash, byte-equal payload+inputs)
	// still dedups to the same rid.
	aAgain, err := publish(pinPayload(3, txnA), fixedHash)
	require.NoError(t, err)
	require.Equal(t, a.RID, aAgain.RID)

	// Unpinned collisions keep the legacy reuse: same explicit hash, no
	// pin on either side, different payloads — the historical row is
	// returned (top-level behavior preserved).
	plain1, err := publish([]byte(`{"v":1}`), "colliding-hash-0002")
	require.NoError(t, err)
	plain2, err := publish([]byte(`{"v":2}`), "colliding-hash-0002")
	require.NoError(t, err)
	require.Equal(t, plain1.RID, plain2.RID)
}
