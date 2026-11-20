//go:build integration

package repo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

func TestOutputCommitGroupAtomicVisibilityAndReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool := bootPool(ctx, t)
	first, scope, owner := w1Setup(ctx, t, pool)
	r := &Repo{Pool: pool}
	second, err := r.CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "group-" + uuid.NewString()}, owner, scope.OrgID)
	require.NoError(t, err)

	datasets := []uuid.UUID{first.ID, second.ID}
	old := make([]*models.OutputCommitReceipt, len(datasets))
	group := make([]OutputCommitMember, len(datasets))
	for i, id := range datasets {
		old[i], _, err = r.WithIdempotentOutputCommit(ctx, scope, id, "previous", keyedFingerprint("previous"),
			keyedBuildFixture(id.String(), scope, "old.ndjson", owner))
		require.NoError(t, err)
		group[i] = OutputCommitMember{DatasetID: id, Key: "group",
			Build: keyedBuildFixture(id.String(), scope, "new.ndjson", owner)}
	}

	assertVisible := func(visible []*models.OutputCommitReceipt, versionCount int, path string, groupVisible bool) {
		t.Helper()
		for i, id := range datasets {
			branch, err := r.GetRuntimeBranch(ctx, id, "main")
			require.NoError(t, err)
			require.Equal(t, &visible[i].Transaction.ID, branch.HeadTransactionID)
			versions, err := r.ListVersions(ctx, id)
			require.NoError(t, err)
			require.Len(t, versions, versionCount)
			index, err := r.ListDatasetFileIndex(ctx, id)
			require.NoError(t, err)
			require.Len(t, index, 1)
			require.Equal(t, path, index[0].Path)
			receipt, err := r.GetOutputCommitReceipt(ctx, scope, id, group[i].Key)
			require.NoError(t, err)
			if groupVisible {
				require.NotNil(t, receipt)
				require.Equal(t, visible[i].ResponseBody, receipt.ResponseBody)
			} else {
				require.Nil(t, receipt)
			}
		}
	}

	// Fail after both inner publishes have already run, including after
	// the first member's receipt INSERT. A concurrent reader on another
	// connection must still see both old versions while we are parked.
	entered, release := make(chan struct{}), make(chan struct{})
	failed := append([]OutputCommitMember(nil), group...)
	forced := errors.New("second member failed after its inner publish")
	failed[1].Build = func(ctx context.Context, tx OutputCommitTxStore) (models.CommitDatasetOutputResponse, error) {
		result, err := group[1].Build(ctx, tx)
		if err != nil {
			return result, err
		}
		close(entered)
		select {
		case <-release:
			return result, forced
		case <-ctx.Done():
			return result, ctx.Err()
		}
	}
	failure := make(chan error, 1)
	go func() {
		_, _, err := r.WithIdempotentOutputCommitGroup(ctx, scope, keyedFingerprint("same group"), failed)
		failure <- err
	}()
	select {
	case <-entered:
	case err := <-failure:
		t.Fatalf("group stopped before publishing both members: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertVisible(old, 1, "old.ndjson", false)
	close(release)
	select {
	case err := <-failure:
		require.ErrorIs(t, err, forced)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	assertVisible(old, 1, "old.ndjson", false)

	published, replay, err := r.WithIdempotentOutputCommitGroup(ctx, scope, keyedFingerprint("same group"), group)
	require.NoError(t, err)
	require.False(t, replay)
	require.Len(t, published, 2)
	assertVisible(published, 2, "new.ndjson", true)

	unexpected := errors.New("replay ran a build callback")
	for i := range group {
		group[i].Build = func(context.Context, OutputCommitTxStore) (models.CommitDatasetOutputResponse, error) {
			return models.CommitDatasetOutputResponse{}, unexpected
		}
	}
	repeated, replay, err := r.WithIdempotentOutputCommitGroup(ctx, scope, keyedFingerprint("same group"), group)
	require.NoError(t, err)
	require.True(t, replay)
	for i := range repeated {
		require.Equal(t, published[i].ResponseBody, repeated[i].ResponseBody)
	}
	_, _, err = r.WithIdempotentOutputCommitGroup(ctx, scope, keyedFingerprint("changed body"), group)
	require.ErrorIs(t, err, ErrIdempotencyKeyConflict)
	changed := append([]OutputCommitMember(nil), group...)
	changed[1].Key = "different-member-key"
	_, _, err = r.WithIdempotentOutputCommitGroup(ctx, scope, keyedFingerprint("same group"), changed)
	require.ErrorIs(t, err, ErrIdempotencyKeyConflict)
	assertVisible(published, 2, "new.ndjson", true)

	foreign := TenantScope{Subject: uuid.New()}
	_, _, err = r.WithIdempotentOutputCommitGroup(ctx, foreign, keyedFingerprint("same group"), group)
	require.ErrorIs(t, err, ErrNotFound)

	// Simulate an incomplete receipt set in the disposable fixture. The
	// group must not fill in the missing receipt or publish again.
	_, err = pool.Exec(ctx, "DELETE FROM dataset_output_commit_receipts WHERE dataset_id=$1 AND idempotency_key=$2", datasets[1], group[1].Key)
	require.NoError(t, err)
	_, _, err = r.WithIdempotentOutputCommitGroup(ctx, scope, keyedFingerprint("same group"), group)
	require.ErrorIs(t, err, ErrIdempotencyKeyConflict)
	missing, err := r.GetOutputCommitReceipt(ctx, scope, datasets[1], group[1].Key)
	require.NoError(t, err)
	require.Nil(t, missing)
}
