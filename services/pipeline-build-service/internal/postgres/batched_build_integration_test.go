//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/resolver"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// WP3.3 3B batched store integration (real PG): the canonical entry's
// tenant boundary (org, personal owner, cross-org negatives), the
// prepare→pending freeze path, prepared/queued refusals, lease races,
// the abort/intent boundary, exact-replay re-entry and the atomic
// finalize (jobs trail, job_outputs repointed, locks and pending
// released) — all against the real repository.

func batchedSeedConfig(t *testing.T, outputRID string, actor uuid.UUID) *models.BatchedBuildConfig {
	t.Helper()
	return &models.BatchedBuildConfig{
		TransformType:    "batched_dataset_copy",
		InputDatasetRID:  "ri.foundry.main.dataset." + uuid.NewString(),
		InputBranch:      "main",
		InputVersion:     3,
		AllFiles:         true,
		OutputDatasetRID: outputRID,
		OutputBranch:     "main",
		LogicalPath:      "part-00000.ndjson",
		JobSpecRID:       "ri.foundry.main.job_spec." + uuid.NewString(),
		ActorSubject:     actor,
	}
}

func batchedSeedArgs(actor uuid.UUID) resolver.ResolveBuildArgs {
	return resolver.ResolveBuildArgs{
		PipelineRID: "ri.foundry.main.pipeline." + uuid.NewString(),
		BuildBranch: "main",
		RequestedBy: actor.String(),
		TriggerKind: "MANUAL",
	}
}

// seedBatchedBuild opens a build + PREPARING config (one transaction) and
// moves it to PENDING with a reservation and one WAITING job, mirroring a
// resolved canonical CreateBuild.
func seedBatchedBuild(ctx context.Context, t *testing.T, pool *pgxpool.Pool, repo *Repository, scope *handler.TenantScope, stage string) (*models.BatchedBuildConfig, uuid.UUID) {
	t.Helper()
	actor := scope.Subject
	outputRID := "ri.foundry.main.dataset." + uuid.NewString()
	buildID := uuid.New()
	cfg := batchedSeedConfig(t, outputRID, actor)
	require.NoError(t, repo.OpenBatchedPrepare(handler.ContextWithTenantScope(ctx, scope), batchedSeedArgs(actor), buildID, cfg))

	// Jobs row + pending reference + lock, exactly what PersistResolvedBuild
	// and AcquireLocks leave behind.
	jobID := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO jobs (id, build_id, job_spec_rid, state, output_transaction_rids)
VALUES ($1,$2,$3,'WAITING','{}') ON CONFLICT (id) DO NOTHING`, jobID, buildID, cfg.JobSpecRID)
	require.NoError(t, err)
	reservation := "ri.foundry.main.transaction." + uuid.NewString()
	_, err = pool.Exec(ctx, `INSERT INTO job_outputs (job_id, output_dataset_rid, transaction_rid) VALUES ($1,$2,$3)
ON CONFLICT (job_id, output_dataset_rid) DO UPDATE SET transaction_rid=EXCLUDED.transaction_rid`, jobID, outputRID, reservation)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO build_input_locks (output_dataset_rid, build_id, transaction_rid) VALUES ($1,$2,$3)
ON CONFLICT (output_dataset_rid) DO NOTHING`, outputRID, buildID, reservation)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO build_pending_output_txns (build_id, dataset_rid, branch, transaction_rid) VALUES ($1,$2,'main',$3)
ON CONFLICT DO NOTHING`, buildID, outputRID, reservation)
	require.NoError(t, err)

	switch stage {
	case models.BatchedStagePending:
		require.NoError(t, repo.PersistResolvedBuild(ctx, &models.ResolvedBuild{
			BuildID: buildID, State: models.BuildResolution,
			JobSpecs:           []models.JobSpec{{RID: cfg.JobSpecRID, OutputDatasetRIDs: []string{outputRID}}},
			OpenedTransactions: []models.OpenedTransaction{{DatasetRID: outputRID, TransactionRID: reservation}},
			Jobs:               []models.ResolvedJob{{ID: jobID, JobSpecRID: cfg.JobSpecRID, OutputTransactionRIDs: []string{reservation}}},
			BatchConfig:        cfg,
		}))
		cfg.ReservationRID = reservation
	}
	return cfg, buildID
}

func TestBatchedConfigTenantBoundaryIntegration(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	repo := NewRepositoryFromPool(pool)
	org := uuid.New()
	owner := uuid.New()

	t.Run("org scope sees the config", func(t *testing.T) {
		scope := &handler.TenantScope{TenantID: &org, Subject: owner}
		cfg, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
		loaded, err := repo.GetBatchedConfig(handler.ContextWithTenantScope(ctx, scope), buildID.String())
		require.NoError(t, err)
		require.NotNil(t, loaded)
		require.Equal(t, cfg.OutputDatasetRID, loaded.OutputDatasetRID)
		require.Equal(t, models.BatchedStagePending, loaded.Stage)
	})

	t.Run("cross org is invisible", func(t *testing.T) {
		scope := &handler.TenantScope{TenantID: &org, Subject: owner}
		_, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
		foreignOrg := uuid.New()
		loaded, err := repo.GetBatchedConfig(handler.ContextWithTenantScope(ctx, &handler.TenantScope{TenantID: &foreignOrg, Subject: uuid.New()}), buildID.String())
		require.NoError(t, err)
		require.Nil(t, loaded, "a foreign organization must not observe the batched configuration")
	})

	t.Run("personal domain owner sees it, non owner does not", func(t *testing.T) {
		scope := &handler.TenantScope{Subject: owner} // no TenantID: personal
		cfg, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
		loaded, err := repo.GetBatchedConfig(handler.ContextWithTenantScope(ctx, scope), buildID.String())
		require.NoError(t, err)
		require.NotNil(t, loaded, "the personal-domain owner reads their row (builds b maps to requested_by)")
		require.Equal(t, cfg.OutputDatasetRID, loaded.OutputDatasetRID)

		stranger := &handler.TenantScope{Subject: uuid.New()}
		loaded, err = repo.GetBatchedConfig(handler.ContextWithTenantScope(ctx, stranger), buildID.String())
		require.NoError(t, err)
		require.Nil(t, loaded, "a different personal subject must not observe it")
	})
}

func TestBatchedPrepareFreezeAndRefusalsIntegration(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	repo := NewRepositoryFromPool(pool)
	org := uuid.New()
	scope := &handler.TenantScope{TenantID: &org, Subject: uuid.New()}

	t.Run("preparing refuses execute and terminalizes on failure", func(t *testing.T) {
		_, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePreparing)
		_, err := repo.AcquireBatchedRun(handler.ContextWithTenantScope(ctx, scope), buildID, uuid.New(), time.Minute)
		require.ErrorIs(t, err, handler.ErrBatchedPreparing)
		require.NoError(t, repo.MarkBuildFailed(handler.ContextWithTenantScope(ctx, scope), buildID, "resolution failed"))
		cfg, err := repo.GetBatchedConfig(handler.ContextWithTenantScope(ctx, scope), buildID.String())
		require.NoError(t, err)
		require.Equal(t, models.BatchedStageFailed, cfg.Stage)
	})

	t.Run("queued build refuses the run entry (no empty-plan fallback)", func(t *testing.T) {
		cfg, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
		_, err := pool.Exec(ctx, `UPDATE builds SET state='BUILD_QUEUED', queued_at=NOW() WHERE id=$1`, buildID)
		require.NoError(t, err)
		_, err = repo.AcquireBatchedRun(handler.ContextWithTenantScope(ctx, scope), buildID, uuid.New(), time.Minute)
		require.ErrorIs(t, err, handler.ErrBatchedQueued)
		_ = cfg
	})
}

func TestBatchedLeaseRacesIntegration(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	repo := NewRepositoryFromPool(pool)
	org := uuid.New()
	scope := &handler.TenantScope{TenantID: &org, Subject: uuid.New()}
	scopedCtx := handler.ContextWithTenantScope(ctx, scope)

	t.Run("second acquire conflicts, expired lease is taken over", func(t *testing.T) {
		_, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
		first, err := repo.AcquireBatchedRun(scopedCtx, buildID, uuid.New(), time.Minute)
		require.NoError(t, err)
		require.Equal(t, models.BatchedStageStaging, first.Stage)

		_, err = repo.AcquireBatchedRun(scopedCtx, buildID, uuid.New(), time.Minute)
		require.ErrorIs(t, err, handler.ErrBatchedLeaseHeld)

		// Expire the lease: a fresh attempt takes over.
		_, err = pool.Exec(ctx, `UPDATE build_batched_configs SET lease_expires_at=NOW() - INTERVAL '1 second' WHERE build_id=$1`, buildID)
		require.NoError(t, err)
		second, err := repo.AcquireBatchedRun(scopedCtx, buildID, uuid.New(), time.Minute)
		require.NoError(t, err)
		require.Equal(t, models.BatchedStageStaging, second.Stage)
	})

	t.Run("fenced writes refuse a stale owner", func(t *testing.T) {
		_, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
		owner, err := repo.AcquireBatchedRun(scopedCtx, buildID, uuid.New(), time.Minute)
		require.NoError(t, err)
		require.NoError(t, repo.MarkBatchedStaged(scopedCtx, buildID, *owner.LeaseOwner, "obj", "sha", 1, 1))
		err = repo.MarkBatchedStaged(scopedCtx, buildID, uuid.New(), "obj2", "sha2", 2, 2)
		require.ErrorIs(t, err, handler.ErrBatchedFenceLost)
	})
}

func TestBatchedAbortIntentBoundaryIntegration(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	repo := NewRepositoryFromPool(pool)
	org := uuid.New()
	scope := &handler.TenantScope{TenantID: &org, Subject: uuid.New()}
	scopedCtx := handler.ContextWithTenantScope(ctx, scope)

	t.Run("idle takeover abort lands terminal with the lifecycle trail", func(t *testing.T) {
		cfg, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
		decision, err := repo.BeginBatchedAbort(scopedCtx, buildID.String(), uuid.New(), time.Minute)
		require.NoError(t, err)
		require.Equal(t, handler.BatchedAbortKindTakeover, decision.Kind)
		require.NoError(t, repo.CompleteBatchedAbort(scopedCtx, buildID, decision.Owner, false, cfg.OutputDatasetRID))

		final, err := repo.GetBatchedConfig(scopedCtx, buildID.String())
		require.NoError(t, err)
		require.Equal(t, models.BatchedStageAborted, final.Stage)
		var buildState string
		require.NoError(t, pool.QueryRow(ctx, `SELECT state FROM builds WHERE id=$1`, buildID).Scan(&buildState))
		require.Equal(t, "BUILD_ABORTED", buildState)
		var lockCount, pendingCount int
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_input_locks WHERE build_id=$1`, buildID).Scan(&lockCount))
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_pending_output_txns WHERE build_id=$1`, buildID).Scan(&pendingCount))
		require.Zero(t, lockCount)
		require.Zero(t, pendingCount)
		// The job walked WAITING → ABORTED (the legal direct edge).
		var jobState string
		require.NoError(t, pool.QueryRow(ctx, `SELECT state FROM jobs WHERE build_id=$1`, buildID).Scan(&jobState))
		require.Equal(t, "ABORTED", jobState)
		_ = cfg
	})

	t.Run("running job abort walks ABORT_PENDING and failure cleanup fails the build row", func(t *testing.T) {
		cfg, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
		// A lease would have driven the job to RUNNING.
		_, err := pool.Exec(ctx, `UPDATE jobs SET state='RUNNING' WHERE build_id=$1`, buildID)
		require.NoError(t, err)

		// Abort path: RUNNING → ABORT_PENDING → ABORTED.
		decision, err := repo.BeginBatchedAbort(scopedCtx, buildID.String(), uuid.New(), time.Minute)
		require.NoError(t, err)
		require.Equal(t, handler.BatchedAbortKindTakeover, decision.Kind)
		require.NoError(t, repo.CompleteBatchedAbort(scopedCtx, buildID, decision.Owner, false, cfg.OutputDatasetRID))
		trail := []string{}
		rows, err := pool.Query(ctx, `SELECT to_state FROM job_state_transitions t JOIN jobs j ON j.id=t.job_id WHERE j.build_id=$1 ORDER BY t.id`, buildID)
		require.NoError(t, err)
		for rows.Next() {
			var to string
			require.NoError(t, rows.Scan(&to))
			trail = append(trail, to)
		}
		rows.Close()
		require.Equal(t, []string{"ABORT_PENDING", "ABORTED"}, trail)
		var buildState string
		require.NoError(t, pool.QueryRow(ctx, `SELECT state FROM builds WHERE id=$1`, buildID).Scan(&buildState))
		require.Equal(t, "BUILD_ABORTED", buildState)

		// Failure-cleanup path (keepFailed): config/jobs/build FAILED and
		// references cleared — atomically, build row included.
		cfg2, buildID2 := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
		_, err = pool.Exec(ctx, `UPDATE jobs SET state='RUNNING' WHERE build_id=$1`, buildID2)
		require.NoError(t, err)
		decision2, err := repo.BeginBatchedAbort(scopedCtx, buildID2.String(), uuid.New(), time.Minute)
		require.NoError(t, err)
		require.NoError(t, repo.CompleteBatchedAbort(scopedCtx, buildID2, decision2.Owner, true, cfg2.OutputDatasetRID))
		var cfgStage string
		require.NoError(t, pool.QueryRow(ctx, `SELECT stage FROM build_batched_configs WHERE build_id=$1`, buildID2).Scan(&cfgStage))
		require.Equal(t, models.BatchedStageFailed, cfgStage)
		require.NoError(t, pool.QueryRow(ctx, `SELECT state FROM builds WHERE id=$1`, buildID2).Scan(&buildState))
		require.Equal(t, "BUILD_FAILED", buildState, "the failure cleanup must flip the build row in the same transaction")
		var pendingCount int
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_pending_output_txns WHERE build_id=$1`, buildID2).Scan(&pendingCount))
		require.Zero(t, pendingCount)
		// Both confirmed-cancel terminals flag the job_outputs row aborted
		// (and not committed) in the SAME transaction.
		for _, id := range []uuid.UUID{buildID, buildID2} {
			var aborted, committed bool
			require.NoError(t, pool.QueryRow(ctx, `SELECT jo.aborted, jo.committed FROM job_outputs jo JOIN jobs j ON j.id=jo.job_id WHERE j.build_id=$1`, id).Scan(&aborted, &committed))
			require.True(t, aborted)
			require.False(t, committed)
		}
		_ = cfg
		_ = cfg2
	})

	t.Run("abort flag refuses the intent write", func(t *testing.T) {
		_, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
		owner, err := repo.AcquireBatchedRun(scopedCtx, buildID, uuid.New(), time.Minute)
		require.NoError(t, err)
		require.NoError(t, repo.MarkBatchedStaged(scopedCtx, buildID, *owner.LeaseOwner, "obj", "sha", 1, 1))
		decision, err := repo.BeginBatchedAbort(scopedCtx, buildID.String(), uuid.New(), time.Minute)
		require.NoError(t, err)
		require.Equal(t, handler.BatchedAbortKindFlagged, decision.Kind)
		err = repo.PersistBatchedCommitIntent(scopedCtx, buildID, *owner.LeaseOwner, "wp33."+buildID.String(), []byte(`{}`))
		require.ErrorIs(t, err, handler.ErrBatchedPublishing, "a recorded abort must win the intent race")
	})

	t.Run("durable intent flips abort to a conflict", func(t *testing.T) {
		_, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
		owner, err := repo.AcquireBatchedRun(scopedCtx, buildID, uuid.New(), time.Minute)
		require.NoError(t, err)
		require.NoError(t, repo.MarkBatchedStaged(scopedCtx, buildID, *owner.LeaseOwner, "obj", "sha", 1, 1))
		require.NoError(t, repo.PersistBatchedCommitIntent(scopedCtx, buildID, *owner.LeaseOwner, "wp33."+buildID.String(), []byte(`{"intent":true}`)))
		decision, err := repo.BeginBatchedAbort(scopedCtx, buildID.String(), uuid.New(), time.Minute)
		require.NoError(t, err)
		require.Equal(t, handler.BatchedAbortKindPublishing, decision.Kind)
		// The exact body survives for replay.
		final, err := repo.GetBatchedConfig(scopedCtx, buildID.String())
		require.NoError(t, err)
		require.Equal(t, `{"intent":true}`, string(final.CommitRequestBody))
	})
}

func TestBatchedReplayAndFinalizeIntegration(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	repo := NewRepositoryFromPool(pool)
	org := uuid.New()
	scope := &handler.TenantScope{TenantID: &org, Subject: uuid.New()}
	scopedCtx := handler.ContextWithTenantScope(ctx, scope)

	_, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
	owner, err := repo.AcquireBatchedRun(scopedCtx, buildID, uuid.New(), time.Minute)
	require.NoError(t, err)
	require.NoError(t, repo.MarkBatchedStaged(scopedCtx, buildID, *owner.LeaseOwner, "obj-1", "sha-1", 10, 4))
	intent := []byte(`{"idempotency_key":"wp33.` + buildID.String() + `","transaction_type":"SNAPSHOT"}`)
	require.NoError(t, repo.PersistBatchedCommitIntent(scopedCtx, buildID, *owner.LeaseOwner, "wp33."+buildID.String(), intent))

	// Lost ACK: the outcome stays publish-pending with the lease released.
	require.NoError(t, repo.MarkBatchedOutcomeUnknown(scopedCtx, buildID, *owner.LeaseOwner, "transport: connection reset"))
	final, err := repo.GetBatchedConfig(scopedCtx, buildID.String())
	require.NoError(t, err)
	require.Equal(t, models.BatchedStageOutcomeUnknown, final.Stage)
	require.Nil(t, final.LeaseOwner, "the lease is released so replay is not blocked by the TTL")

	// A run entry on an intent-bearing build is refused; the replay entry
	// re-acquires and hands back the exact stored bytes.
	_, err = repo.AcquireBatchedRun(scopedCtx, buildID, uuid.New(), time.Minute)
	require.ErrorIs(t, err, handler.ErrBatchedPublishing)
	replayer, err := repo.AcquireBatchedReplay(scopedCtx, buildID, uuid.New(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, intent, replayer.CommitRequestBody)

	// Finalize: one transaction repoints job_outputs at the ACTUAL
	// published transaction, completes job/build and clears lock+pending.
	published := "ri.foundry.main.transaction." + uuid.NewString()
	require.NoError(t, repo.FinalizeBatchedCommit(scopedCtx, buildID, *replayer.LeaseOwner, []byte(`{"ok":true}`), published))
	final, err = repo.GetBatchedConfig(scopedCtx, buildID.String())
	require.NoError(t, err)
	require.Equal(t, models.BatchedStageDone, final.Stage)
	require.Equal(t, published, final.PublishedTxnRID)

	var jobState, buildState string
	require.NoError(t, pool.QueryRow(ctx, `SELECT state FROM jobs WHERE build_id=$1`, buildID).Scan(&jobState))
	require.NoError(t, pool.QueryRow(ctx, `SELECT state FROM builds WHERE id=$1`, buildID).Scan(&buildState))
	require.Equal(t, "COMPLETED", jobState)
	require.Equal(t, "BUILD_COMPLETED", buildState)

	var txnRID string
	var committed bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT jo.transaction_rid, jo.committed FROM job_outputs jo JOIN jobs j ON j.id=jo.job_id WHERE j.build_id=$1`, buildID).Scan(&txnRID, &committed))
	require.Equal(t, published, txnRID)
	require.True(t, committed)

	var lockCount, pendingCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_input_locks WHERE build_id=$1`, buildID).Scan(&lockCount))
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM build_pending_output_txns WHERE build_id=$1`, buildID).Scan(&pendingCount))
	require.Zero(t, lockCount)
	require.Zero(t, pendingCount)

	// The lifecycle trail recorded the full RUNNING → COMPLETED path.
	trail := []string{}
	rows, err := pool.Query(ctx, `SELECT to_state FROM job_state_transitions t JOIN jobs j ON j.id=t.job_id WHERE j.build_id=$1 ORDER BY t.id`, buildID)
	require.NoError(t, err)
	for rows.Next() {
		var to string
		require.NoError(t, rows.Scan(&to))
		trail = append(trail, to)
	}
	rows.Close()
	require.Equal(t, []string{"RUN_PENDING", "RUNNING", "COMPLETED"}, trail)

	// A stale owner cannot finalize twice.
	err = repo.FinalizeBatchedCommit(scopedCtx, buildID, *replayer.LeaseOwner, nil, published)
	require.ErrorIs(t, err, handler.ErrBatchedFenceLost)
}

func TestBatchedPrepareRefusesExistingBuildIntegration(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	repo := NewRepositoryFromPool(pool)
	org := uuid.New()
	scope := &handler.TenantScope{TenantID: &org, Subject: uuid.New()}
	scopedCtx := handler.ContextWithTenantScope(ctx, scope)

	cfg, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
	err := repo.OpenBatchedPrepare(scopedCtx, batchedSeedArgs(scope.Subject), buildID, cfg)
	require.Error(t, err, "attaching a batched configuration to an existing build must fail explicitly")
	require.Contains(t, err.Error(), "batched_prepare_build_conflict")
}

func TestBatchedFinalizeZeroRowsRollsBackIntegration(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	repo := NewRepositoryFromPool(pool)
	org := uuid.New()
	scope := &handler.TenantScope{TenantID: &org, Subject: uuid.New()}
	scopedCtx := handler.ContextWithTenantScope(ctx, scope)

	_, buildID := seedBatchedBuild(ctx, t, pool, repo, scope, models.BatchedStagePending)
	owner, err := repo.AcquireBatchedRun(scopedCtx, buildID, uuid.New(), time.Minute)
	require.NoError(t, err)
	require.NoError(t, repo.MarkBatchedStaged(scopedCtx, buildID, *owner.LeaseOwner, "obj", "sha", 1, 1))
	require.NoError(t, repo.PersistBatchedCommitIntent(scopedCtx, buildID, *owner.LeaseOwner, "wp33."+buildID.String(), []byte(`{}`)))

	// Tamper: the job_outputs row no longer holds the expected reservation.
	_, err = pool.Exec(ctx, `UPDATE job_outputs SET transaction_rid='ri.foundry.main.transaction.' || gen_random_uuid()::text WHERE job_id IN (SELECT id FROM jobs WHERE build_id=$1)`, buildID)
	require.NoError(t, err)

	published := "ri.foundry.main.transaction." + uuid.NewString()
	err = repo.FinalizeBatchedCommit(scopedCtx, buildID, *owner.LeaseOwner, []byte(`{}`), published)
	require.Error(t, err)
	require.Contains(t, err.Error(), "batched_finalize_")
	require.Contains(t, err.Error(), "mismatch", "the tampered bookkeeping must abort the finalize, whichever exactly-one check trips first")

	// Rollback kept EVERYTHING in the publish-pending state: the config
	// stage, the persisted intent, the (tampered) job_outputs row and the
	// build state — the exact body stays replayable.
	cfg, err := repo.GetBatchedConfig(scopedCtx, buildID.String())
	require.NoError(t, err)
	require.Equal(t, models.BatchedStagePublishing, cfg.Stage)
	require.NotEmpty(t, cfg.CommitRequestBody)
	var buildState string
	require.NoError(t, pool.QueryRow(ctx, `SELECT state FROM builds WHERE id=$1`, buildID).Scan(&buildState))
	require.Equal(t, "BUILD_RUNNING", buildState)
}

func TestBatchedBareUUIDCompatibilityIntegration(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	repo := NewRepositoryFromPool(pool)
	org := uuid.New()
	scope := &handler.TenantScope{TenantID: &org, Subject: uuid.New()}
	scopedCtx := handler.ContextWithTenantScope(ctx, scope)

	// The JobSpec declared a BARE UUID output: every bookkeeping row the
	// resolver wrote uses the verbatim spelling, while the frozen config
	// is canonical. Nothing may diverge into an empty reservation or a
	// zero-row finalize.
	outputUUID := uuid.New()
	bareOutput := outputUUID.String()
	canonicalOutput := "ri.foundry.main.dataset." + bareOutput
	buildID := uuid.New()
	cfg := batchedSeedConfig(t, canonicalOutput, scope.Subject)
	cfg.OutputDatasetRID = canonicalOutput
	require.NoError(t, repo.OpenBatchedPrepare(scopedCtx, batchedSeedArgs(scope.Subject), buildID, cfg))

	jobID := uuid.New()
	reservation := "ri.foundry.main.transaction." + uuid.NewString()
	_, err := pool.Exec(ctx, `INSERT INTO jobs (id, build_id, job_spec_rid, state, output_transaction_rids)
VALUES ($1,$2,$3,'WAITING',ARRAY[$4])`, jobID, buildID, cfg.JobSpecRID, reservation)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO job_outputs (job_id, output_dataset_rid, transaction_rid) VALUES ($1,$2,$3)`, jobID, bareOutput, reservation)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO build_input_locks (output_dataset_rid, build_id, transaction_rid) VALUES ($1,$2,$3)`, bareOutput, buildID, reservation)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO build_pending_output_txns (build_id, dataset_rid, branch, transaction_rid) VALUES ($1,$2,'main',$3)`, buildID, bareOutput, reservation)
	require.NoError(t, err)

	// PersistResolvedBuild binds the reservation through the UUID.
	require.NoError(t, repo.PersistResolvedBuild(scopedCtx, &models.ResolvedBuild{
		BuildID: buildID, State: models.BuildResolution,
		JobSpecs:           []models.JobSpec{{RID: cfg.JobSpecRID, OutputDatasetRIDs: []string{bareOutput}}},
		OpenedTransactions: []models.OpenedTransaction{{DatasetRID: bareOutput, TransactionRID: reservation}},
		Jobs:               []models.ResolvedJob{{ID: jobID, JobSpecRID: cfg.JobSpecRID, OutputTransactionRIDs: []string{reservation}}},
		BatchConfig:        cfg,
	}))
	loaded, err := repo.GetBatchedConfig(scopedCtx, buildID.String())
	require.NoError(t, err)
	require.Equal(t, reservation, loaded.ReservationRID, "UUID-equivalent spellings bind the reservation")

	// Full lease → intent → finalize: job_outputs repoints and the public
	// jobs.output_transaction_rids array flips in the SAME transaction.
	owner, err := repo.AcquireBatchedRun(scopedCtx, buildID, uuid.New(), time.Minute)
	require.NoError(t, err)
	require.NoError(t, repo.MarkBatchedStaged(scopedCtx, buildID, *owner.LeaseOwner, "obj", "sha", 1, 1))
	published := "ri.foundry.main.transaction." + uuid.NewString()
	require.NoError(t, repo.PersistBatchedCommitIntent(scopedCtx, buildID, *owner.LeaseOwner, "wp33."+buildID.String(), []byte(`{}`)))
	require.NoError(t, repo.FinalizeBatchedCommit(scopedCtx, buildID, *owner.LeaseOwner, []byte(`{}`), published))

	var txnRID string
	var committed bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT transaction_rid, committed FROM job_outputs WHERE job_id=$1`, jobID).Scan(&txnRID, &committed))
	require.Equal(t, published, txnRID, "finalize repoints job_outputs despite the bare-UUID spelling")
	require.True(t, committed)

	var jobTxns []string
	require.NoError(t, pool.QueryRow(ctx, `SELECT output_transaction_rids FROM jobs WHERE id=$1`, jobID).Scan(&jobTxns))
	require.Equal(t, []string{published}, jobTxns, "the public job surface now reports the actual published transaction")
}
