//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPG_ExecutionReceiptRecoversRunningWithoutRerun(t *testing.T) {
	ctx := context.Background()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.admit(ctx, t)
	fence, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker", 120000000000)
	require.NoError(t, err)
	require.True(t, ok)
	job := uuid.New()
	_, err = f.pool.Exec(ctx, `INSERT INTO jobs(id,build_id,job_spec_rid,state) VALUES($1,$2,$3,'RUNNING')`, job, f.params.BuildID, seedSpecRID)
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `INSERT INTO scheduling.node_run_results(dispatch_event_id,node_id) VALUES($1,$2)`, f.params.EventID, seedSpecRID)
	require.NoError(t, err)
	_, err = f.pool.Exec(ctx, `INSERT INTO job_outputs(job_id,output_dataset_rid,transaction_rid) VALUES($1,'dataset','reservation'),($1,'dataset2','reservation2')`, job)
	require.NoError(t, err)
	key := scheduleevents.DeriveOutputPublishKey(f.params.EventID, seedSpecRID, "dataset")
	require.NoError(t, f.repo.RecordPendingReceipt(ctx, f.params.EventID, "worker", fence, key, seedSpecRID, "dataset", []byte(`{}`)))
	require.NoError(t, f.repo.CompleteScheduledPublish(ctx, f.params.EventID, "worker", fence, key, json.RawMessage(`{}`), "dataset", "", ""))
	var state string
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT state FROM jobs WHERE id=$1`, job).Scan(&state))
	require.Equal(t, "RUNNING", state)
	key2 := scheduleevents.DeriveOutputPublishKey(f.params.EventID, seedSpecRID, "dataset2")
	require.NoError(t, f.repo.RecordPendingReceipt(ctx, f.params.EventID, "worker", fence, key2, seedSpecRID, "dataset2", []byte(`{}`)))
	require.NoError(t, f.repo.CompleteScheduledPublish(ctx, f.params.EventID, "worker", fence, key2, json.RawMessage(`{}`), "dataset2", "reservation2", "ri.foundry.main.transaction."+uuid.NewString()))
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT state FROM jobs WHERE id=$1`, job).Scan(&state))
	require.Equal(t, "COMPLETED", state)
}

func TestPG_ExecutionGateUsesCurrentAuthorization(t *testing.T) {
	ctx := context.Background()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.admit(ctx, t)
	fence, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker", 120000000000)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, f.repo.AssertDispatchFence(ctx, f.params.EventID, "worker", fence))
	_, err = f.pool.Exec(ctx, `UPDATE scheduling.grants SET state='revoked' WHERE id=$1`, f.params.GrantRef)
	require.NoError(t, err)
	require.ErrorIs(t, f.repo.AssertDispatchFence(ctx, f.params.EventID, "worker", fence), ErrGrantStaleAtPublish)
}

func TestPG_ScheduledBookkeepingRejectsStaleOwner(t *testing.T) {
	ctx := context.Background()
	f := seedScheduling(ctx, t, scheduleevents.TenantKindOrg)
	f.admit(ctx, t)
	fence, ok, err := f.repo.ClaimDispatchForExecution(ctx, f.params.EventID, "worker", 120000000000)
	require.NoError(t, err)
	require.True(t, ok)
	state := f.repo.ScheduledBuildState(f.params.EventID, "worker", fence)
	require.NoError(t, state.RecordPendingTxn(ctx, f.params.BuildID, "ds", "main", "txn"))
	_, ok, err = state.TryAcquire(ctx, "ds", f.params.BuildID, "txn")
	require.NoError(t, err)
	require.True(t, ok)
	_, err = f.pool.Exec(ctx, `UPDATE scheduling.dispatches SET fence_token=fence_token+1 WHERE event_id=$1`, f.params.EventID)
	require.NoError(t, err)
	require.ErrorIs(t, state.ClearPendingTxn(ctx, f.params.BuildID, "txn"), ErrStaleFence)
	require.ErrorIs(t, state.ReleaseLock(ctx, "ds", f.params.BuildID), ErrStaleFence)
	var n int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM build_input_locks WHERE build_id=$1`, f.params.BuildID).Scan(&n))
	require.Equal(t, 1, n)
}
