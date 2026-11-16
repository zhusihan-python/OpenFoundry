//go:build integration

// P2.5 发布后推进断点 — real PostgreSQL 16 regressions (PG-test track).
//
// Authority chain: root review-initial-contract.md + review-round1.md +
// review-pg-plan-round1.md + implementation-release.md (2026-11-14), the
// Primary implementation-contract.md v1.1 in tools/smoke/business-platform-p2-5/
// (frozen signatures), and pg-test-plan.md §4/§5 (matrix r4).
//
// STAGED FILE. Stage 2 (contract v1.1 + landed repo layer): the core
// checkpoint-aware surface is implemented in internal/repo/checkpoint.go and
// the public entrypoints route captured work (ClaimSyncRun / BeginPublishGate
// / CompleteLeasedSyncRun / RequestCancelSyncRun), so the contracted tests
// below exercise the LANDED code directly. Manual admission (EnqueueSyncRun
// integration), the scheduled busy path, the repo.go mutation fences and the
// handler wiring were NOT yet landed when this stage was written: those
// matrix rows (T9 fences, T10 no-lock-across-HTTP, T11 scheduled busy, the
// full T13 legacy/old6 sweep) stay in pg-test-plan §5 as pending names and
// must be added — not faked — when their surfaces land. The tests construct
// admission through the SAME ordered transaction the contract prescribes for
// the manual path (Source -> Def FOR UPDATE -> authoritative binding recompute
// -> AcquireCheckpointForAdmission -> stamp last) via the internal helper,
// which is exactly how the future wrapper will call it.
//
// Anti-fake-test discipline (review-pg-plan-round1): full-row zero-change
// snapshots include every timestamp (p25CpJSON / runRowJSON, no key
// subtraction); natural lease expiry NEVER mutates the parked rows; same-Def
// races use a pure Source-row barrier with real backend-PID block proofs;
// marker RAISE proves LOCAL rollback only; and every not-visible negative
// runs against a real foreign-owned row with a correct-tenant positive
// control.
package repo

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// ---------------------------------------------------------------------------
// Stage 1: the legacy mutation-fence baseline (kept verbatim; see pg-test-plan
// §4 T9 for the two-phase evidence contract).
// ---------------------------------------------------------------------------

// TestPGCheckpointLegacyMutationFenceBaseline pins the CURRENT mutation
// gates around sync runs — the fence semantics P2.5 must extend without
// regressing legacy (non-opt-in) defs:
//
//   - pending/running runs block Source config changes and Def retargets
//     (ErrActiveRuns, zero durable change — byte-equal rows);
//   - same-value retargets and pure name edits stay allowed while a run
//     is active (no revision bump, no data-position concept yet);
//   - SetCredential is today an INDEPENDENT upsert: it never consults run
//     state at all (the exact gap [PC-FENCE] must close for P25-captured
//     work only), while remaining tenant-scoped (invisible tenant ⇒ no
//     row attaches, with a positive control);
//   - a TERMINAL failed run blocks nothing — today's guards count only
//     pending/running, so a failed run lets config/retarget/credential
//     writes straight through. After P25 lands, THIS stays true for
//     legacy defs while the retained-occupied P25 variant must reject.
func TestPGCheckpointLegacyMutationFenceBaseline(t *testing.T) {
	r, _, tenant, sourceID, defID := leaseFixture(t)
	ctx := context.Background()

	requester := uuid.New()
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, requester)
	target := jobTarget(t, r, defID)

	t.Run("pending_blocks_input_and_target_writes", func(t *testing.T) {
		connBefore := tableRowJSON(t, r.Pool, "connections", sourceID)
		defBefore := tableRowJSON(t, r.Pool, "batch_sync_defs", defID)

		_, err := r.UpdateConnection(ctx, sourceID, tenant,
			&models.UpdateConnectionRequest{Config: []byte(`{}`)})
		assert.ErrorIs(t, err, ErrActiveRuns, "config change under a pending run")

		newTarget := uuid.New()
		_, err = r.UpdateSyncJob(ctx, defID,
			&models.UpdateSyncJobRequest{OutputDatasetID: &newTarget}, tenant)
		assert.ErrorIs(t, err, ErrActiveRuns, "retarget under a pending run")

		assert.Equal(t, connBefore, tableRowJSON(t, r.Pool, "connections", sourceID),
			"refused config change leaves the source row byte-equal")
		assert.Equal(t, defBefore, tableRowJSON(t, r.Pool, "batch_sync_defs", defID),
			"refused retarget leaves the def row byte-equal")
	})

	t.Run("pending_allows_pure_and_same_value_writes", func(t *testing.T) {
		name := "renamed-while-pending"
		updated, err := r.UpdateConnection(ctx, sourceID, tenant,
			&models.UpdateConnectionRequest{Name: &name})
		require.NoError(t, err, "name-only edit stays allowed under an active run")
		require.NotNil(t, updated)
		assert.Equal(t, name, updated.Name)

		revBefore := p24DefRevision(t, r.Pool, defID)
		job, err := r.UpdateSyncJob(ctx, defID,
			&models.UpdateSyncJobRequest{OutputDatasetID: &target}, tenant)
		require.NoError(t, err, "same-value retarget never consults the active-run guard")
		require.NotNil(t, job)
		assert.Equal(t, target, job.OutputDatasetID)
		assert.Equal(t, revBefore, p24DefRevision(t, r.Pool, defID),
			"same-value patch does not bump schedule_revision")
	})

	t.Run("setcredential_is_an_unfenced_upsert_today", func(t *testing.T) {
		// Positive control FIRST (§7-E: a live negative needs a real row):
		// the in-tenant write succeeds even while the run is pending —
		// today's UPSERT has no locking, no active protection, no epoch.
		cred, err := r.SetCredential(ctx, sourceID, tenant,
			"api_key", []byte("baseline-ciphertext"), "fp-baseline-1")
		require.NoError(t, err)
		require.NotNil(t, cred, "in-tenant SetCredential attaches")

		// Live negative against a REAL row owned by another tenant.
		foreign := Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}
		none, err := r.SetCredential(ctx, sourceID, foreign,
			"api_key", []byte("baseline-ciphertext-2"), "fp-baseline-2")
		require.NoError(t, err)
		assert.Nil(t, none, "foreign-tenant SetCredential is a clean no-op")

		var attached int
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM source_credentials WHERE source_id = $1`,
			sourceID).Scan(&attached))
		assert.Equal(t, 1, attached, "exactly the in-tenant credential row exists")
	})

	t.Run("terminal_failed_releases_every_mutation_gate", func(t *testing.T) {
		claim, err := r.ClaimSyncRun(ctx, runID, tenant,
			"fence-baseline-holder", "fence-baseline-token", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)

		msg := "baseline terminal failure"
		done, err := r.CompleteLeasedSyncRun(ctx, runID, tenant,
			"fence-baseline-holder", "fence-baseline-token", claim.Run.Generation,
			"failed", 0, 0, 0, &msg, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, done)
		assert.Equal(t, "failed", done.Status)

		// The pending/running-only guards let everything through now. This
		// is the recorded GAP: for P25-captured work a failed-but-retained
		// owner must reject these same surfaces ([PC-FENCE]); for legacy
		// defs this permissive truth is permanent.
		upd, err := r.UpdateConnection(ctx, sourceID, tenant,
			&models.UpdateConnectionRequest{Config: []byte(`{}`)})
		require.NoError(t, err, "terminal run unblocks config change (legacy truth)")
		require.NotNil(t, upd)

		retarget := uuid.New()
		job, err := r.UpdateSyncJob(ctx, defID,
			&models.UpdateSyncJobRequest{OutputDatasetID: &retarget}, tenant)
		require.NoError(t, err, "terminal run unblocks retarget (legacy truth)")
		require.NotNil(t, job)
		assert.Equal(t, retarget, job.OutputDatasetID)

		cred, err := r.SetCredential(ctx, sourceID, tenant,
			"api_key", []byte("baseline-ciphertext-3"), "fp-baseline-3")
		require.NoError(t, err)
		require.NotNil(t, cred)
	})
}

// ---------------------------------------------------------------------------
// Stage 2 fixtures and helpers (contract v1.1 shapes).
// ---------------------------------------------------------------------------

// p25IncrementalConfig is a minimal VALID rest_api source config with the
// cursor-only incremental opt-in (contract §2.1/§2.1b): request_param is
// distinct from every pagination key, auth is none (no query placement
// collision), and the completed-checkpoint dot path has nonempty segments.
func p25IncrementalConfig() []byte {
	return []byte(`{
		"domain": "inc.example.com",
		"auth": {"type": "none"},
		"runtime": {"worker": "foundry"},
		"permissions": {"syncable": true},
		"sync": {
			"resource_path": "/items",
			"records_path": "items",
			"pagination": {"type": "cursor", "cursor_param": "cursor_token", "next_cursor_path": "next_cursor"},
			"incremental": {"enabled": true, "request_param": "since_token", "completed_checkpoint_path": "completed"}
		}
	}`)
}

// p25Fixture boots a fresh migrated database with one org-tenant incremental
// source + def.
func p25Fixture(t *testing.T) (*Repo, string, Tenant, uuid.UUID, uuid.UUID) {
	t.Helper()
	r, dsn := bootRepoWithDSN(t)
	ctx := context.Background()
	require.NoError(t, Migrate(ctx, r.Pool))
	tenant := Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}
	conn, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
		Name: "inc-source", ConnectorType: "rest_api", Config: p25IncrementalConfig(),
	}, tenant)
	require.NoError(t, err)
	require.NotNil(t, conn)
	job, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
		SourceID: conn.ID, OutputDatasetID: uuid.New(),
	}, tenant)
	require.NoError(t, err)
	require.NotNil(t, job)
	return r, dsn, tenant, conn.ID, job.ID
}

// p25Admit performs the contract §5 MANUAL admission sequence on its own
// transaction: Source FOR UPDATE -> Def FOR UPDATE (strong lock) ->
// authoritative binding recompute (post-lock) -> AcquireCheckpointForAdmission
// -> admission stamp last -> commit. The caller's ctx (and its deadline)
// propagates through every statement (review-pg-code-round1 #3).
// fingerprintOverride (when nonempty) replaces the recomputed fingerprint to
// drive the binding_stale matrix. NOTE (#5): this exercises the repo-level
// Acquire contract directly; PUBLIC-entry proofs live in the enqueue
// subtests. Error returns keep the helper goroutine-safe (no require inside).
func p25Admit(ctx context.Context, r *Repo, tenant Tenant, sourceID, defID uuid.UUID, requestedBy uuid.UUID, fingerprintOverride string) (*models.SyncRun, *IncrementalRejection, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer rollbackAdmissionTx(tx)
	if err := lockSourceRowForUpdate(ctx, tx, sourceID, tenant); err != nil {
		return nil, nil, err
	}
	defPredicate, defArg := syncDefTenantPredicate(tenant, 2)
	var lockedDef uuid.UUID
	if err := tx.QueryRow(ctx,
		`SELECT d.id FROM batch_sync_defs d WHERE d.id = $1 AND `+defPredicate+` FOR UPDATE`,
		defID, defArg).Scan(&lockedDef); err != nil {
		return nil, nil, err
	}
	binding, fingerprint, err := readIncrementalBinding(ctx, tx, defID, sourceID, tenant)
	if err != nil {
		return nil, nil, err
	}
	if fingerprintOverride != "" {
		fingerprint = fingerprintOverride
	}
	run, rejection, err := r.AcquireCheckpointForAdmission(ctx, tx, defID, sourceID, tenant, requestedBy, binding, fingerprint)
	if err != nil || rejection != nil {
		return run, rejection, err
	}
	if err := advanceSyncLastAdmitted(ctx, tx, sourceID); err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}
	return run, nil, nil
}

// p25BoundedRollback closes a barrier transaction with its OWN 5-second
// budget on a detached context (root up-candidate review fix 2: a bare
// Rollback(context.Background()) can wait unbounded and hang Pool.Close
// after a Fatal). Used at EVERY new P25 barrier release point — cleanup
// callbacks (release→cancel→bounded-join) and hot-path releases alike.
func p25BoundedRollback(tx pgx.Tx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return tx.Rollback(ctx)
}

// p25RowJSONWithoutCp normalizes one sync_runs row_to_json by removing ONLY
// the five P2.5 cp_* keys and re-marshalling canonically (sorted keys), for
// cross-migration comparisons where the newly-added NULL columns are
// legitimate. It also reports whether every removed cp_* value was JSON
// null (the never-adopted invariant).
func p25RowJSONWithoutCp(t *testing.T, js string) (string, bool) {
	t.Helper()
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(js), &m))
	allNull := true
	for _, k := range []string{"cp_mode", "cp_base_epoch", "cp_base_value",
		"cp_binding_fingerprint", "cp_output_dataset_id"} {
		if v, ok := m[k]; ok {
			if strings.TrimSpace(string(v)) != "null" {
				allNull = false
			}
			delete(m, k)
		}
	}
	out, err := json.Marshal(m)
	require.NoError(t, err)
	return string(out), allNull
}

// p25CpJSON snapshots the COMPLETE sync_def_checkpoints row (every column,
// every timestamp — no key subtraction; review-pg-plan-round1 #1).
func p25CpJSON(t *testing.T, pool *pgxpool.Pool, defID uuid.UUID) string {
	t.Helper()
	var js string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT row_to_json(cp.*)::text FROM sync_def_checkpoints cp WHERE cp.sync_def_id = $1`,
		defID).Scan(&js))
	return js
}

// p25CpState reads the decision-relevant checkpoint columns read-only.
type p25CpState struct {
	Value        *string
	Epoch        int
	Owner        *uuid.UUID
	AdvancedBy   *uuid.UUID
	LastBatchTxn *uuid.UUID
}

func p25CpStateOf(t *testing.T, pool *pgxpool.Pool, defID uuid.UUID) p25CpState {
	t.Helper()
	var s p25CpState
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT checkpoint_value, confirmed_epoch, active_run_id, advanced_by_run, last_batch_txn
		   FROM sync_def_checkpoints WHERE sync_def_id = $1`, defID).
		Scan(&s.Value, &s.Epoch, &s.Owner, &s.AdvancedBy, &s.LastBatchTxn))
	return s
}

// p25LeaseExpiry reads the run's lease_expires_at for natural-expiry waits.
func p25LeaseExpiry(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) time.Time {
	t.Helper()
	var expires time.Time
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT lease_expires_at FROM sync_runs WHERE id = $1`, runID).Scan(&expires))
	return expires
}

// p25RunStatus reads one run's status read-only.
func p25RunStatus(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) string {
	t.Helper()
	var status string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status FROM sync_runs WHERE id = $1`, runID).Scan(&status))
	return status
}

// p25RunCount counts runs of one def (occupancy truth from SQL, never from
// the code under test).
func p25RunCount(t *testing.T, pool *pgxpool.Pool, defID uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM sync_runs WHERE sync_def_id = $1`, defID).Scan(&n))
	return n
}

// p25ClaimGate drives the public routed claim + publish gate for a captured
// run (release correction 2: routing happens BEFORE any Run-first lock).
func p25ClaimGate(t *testing.T, r *Repo, tenant Tenant, runID uuid.UUID, holder, token string) (*LeaseClaim, *PublishGateResult) {
	t.Helper()
	ctx := context.Background()
	claim, err := r.ClaimSyncRun(ctx, runID, tenant, holder, token, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claim, "captured incremental run claims through the routed path")
	require.Equal(t, CPModeCursor, claim.Run.CPMode, "claim carries the frozen cp_mode")

	gate, err := r.BeginPublishGate(ctx, runID, tenant, holder, token, claim.Run.Generation, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, gate)
	require.Equal(t, PublishGateEntered, gate.Outcome, "gate enters through the routed path")
	return claim, gate
}

// ---------------------------------------------------------------------------
// Stage 2 contracted tests (pg-test-plan §4/§5 r4).
// ---------------------------------------------------------------------------

// T1 — migration: empty DB / genuine pre-P2.5 DB / replay / concurrent /
// DB-level bounds (octet_length CHECK, PK single row).
func TestPGCheckpointMigrationEmptyOldRerunConcurrent(t *testing.T) {
	t.Run("empty_database_migrates_with_defaults", func(t *testing.T) {
		r := bootRepo(t)
		ctx := context.Background()
		require.NoError(t, Migrate(ctx, r.Pool))
		var tables int
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'sync_def_checkpoints'`).Scan(&tables))
		assert.Equal(t, 1, tables)
		var cpCols int
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM information_schema.columns
			  WHERE table_name = 'sync_runs'
			    AND column_name IN ('cp_mode','cp_base_epoch','cp_base_value','cp_binding_fingerprint','cp_output_dataset_id')`).Scan(&cpCols))
		assert.Equal(t, 5, cpCols, "the five frozen-baseline columns exist")
		// Replay: settled-schema whole rows stay byte-equal.
		require.NoError(t, Migrate(ctx, r.Pool))
	})

	t.Run("old_database_no_backfill_and_byte_preserved_replay", func(t *testing.T) {
		r := bootRepo(t)
		ctx := context.Background()
		skipped := p24ApplyMigrationsExcept(t, r.Pool, "sync_def_checkpoints")
		require.Len(t, skipped, 1, "exactly the P2.5 migration file is skipped")
		assert.Contains(t, skipped[0], "20261116100000", skipped[0])

		// Genuine pre-P2.5 state: legacy def + PENDING run (claiming must
		// wait for the migration: the routed claim pre-reads r.cp_mode,
		// which does not exist yet — enqueue itself is still pure legacy).
		tenant := Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}
		conn, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
			Name: "legacy", ConnectorType: "rest_api",
			Config: []byte(`{"domain":"legacy.example.com","auth":{"type":"none"},
				"runtime":{"worker":"foundry"},"permissions":{"syncable":true},
				"sync":{"resource_path":"/x","records_path":"items"}}`),
		}, tenant)
		require.NoError(t, err)
		require.NotNil(t, conn)
		job, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: conn.ID, OutputDatasetID: uuid.New()}, tenant)
		require.NoError(t, err)
		require.NotNil(t, job)
		run, err := r.EnqueueSyncRun(ctx, job.ID, conn.ID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, run)

		defBefore := tableRowJSON(t, r.Pool, "batch_sync_defs", job.ID)
		connBefore := tableRowJSON(t, r.Pool, "connections", conn.ID)
		runBeforeNorm, _ := p25RowJSONWithoutCp(t, runRowJSON(t, r.Pool, run.ID))

		// Full Migrate: no backfill, no adoption, field-preserved history.
		require.NoError(t, Migrate(ctx, r.Pool))
		var cpRows int
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM sync_def_checkpoints`).Scan(&cpRows))
		assert.Zero(t, cpRows, "no checkpoint row is backfilled for legacy defs")
		assert.Equal(t, defBefore, tableRowJSON(t, r.Pool, "batch_sync_defs", job.ID))
		assert.Equal(t, connBefore, tableRowJSON(t, r.Pool, "connections", conn.ID))
		// The migration legitimately ADDS the five cp_* NULL columns, so a
		// whole-row byte compare across the schema boundary is over-strict:
		// compare with cp_* stripped from BOTH sides, then positively assert
		// every cp_* IS NULL (never adopted) — first-run finding, fixed in
		// the PG-owned fixture only.
		runAfterNorm, cpAllNull := p25RowJSONWithoutCp(t, runRowJSON(t, r.Pool, run.ID))
		assert.Equal(t, runBeforeNorm, runAfterNorm,
			"legacy run survives the P2.5 migration field-for-field (cp_* excluded)")
		assert.True(t, cpAllNull, "every cp_* column is NULL after the migration")

		// The legacy claim path still works with the cp columns present
		// (cp_mode NULL routes to the unchanged legacy implementation).
		claim, err := r.ClaimSyncRun(ctx, run.ID, tenant, "old-holder", "old-token", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)

		// Replay on the settled schema: whole rows byte-equal.
		runJSON := runRowJSON(t, r.Pool, run.ID)
		defJSON := tableRowJSON(t, r.Pool, "batch_sync_defs", job.ID)
		require.NoError(t, Migrate(ctx, r.Pool))
		assert.Equal(t, runJSON, runRowJSON(t, r.Pool, run.ID))
		assert.Equal(t, defJSON, tableRowJSON(t, r.Pool, "batch_sync_defs", job.ID))
	})

	t.Run("db_bounds_octet_length_and_single_row", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		_, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)

		// Raw-BYTE bound (release correction 4): 300 CJK runes = 900 bytes
		// must violate the 512-BYTE CHECK even though runes < 512.
		oversize := strings.Repeat("世", 300)
		_, err = r.Pool.Exec(ctx,
			`UPDATE sync_def_checkpoints SET checkpoint_value = $1 WHERE sync_def_id = $2`,
			oversize, defID)
		require.Error(t, err, "octet_length bound is raw bytes, not runes")

		// PK single-row: a second row for the same def is a named PK violation.
		_, err = r.Pool.Exec(ctx,
			`INSERT INTO sync_def_checkpoints (sync_def_id, source_id, output_dataset_id, mode, binding_fingerprint)
			 VALUES ($1, $2, $3, 'cursor', 'x')`, defID, sourceID, uuid.New())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sync_def_checkpoints_pkey",
			"the violation is the PK constraint by name (no blanket 23505 swallow)")
	})
}

// T2 — admission owner in one transaction: FK order, frozen capture, busy,
// binding_stale, checkpoint-row contention (55P03 abort), stamp discipline.
func TestPGCheckpointAdmissionOwnerInTxOrder(t *testing.T) {
	t.Run("first_admission_freezes_base_and_sets_owner_after_run_insert", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()

		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		require.NotNil(t, run)
		assert.Equal(t, "pending", run.Status)

		// Owner set AFTER the run INSERT (immediate FK — no deferrable trick):
		// the table carries FKs, and NONE of them may be deferrable.
		var fks, deferrableFKs int
		require.NoError(t, r.Pool.QueryRow(ctx, `
			SELECT COUNT(*), COUNT(*) FILTER (WHERE condeferrable)
			  FROM pg_constraint
			 WHERE conrelid = 'sync_def_checkpoints'::regclass AND contype = 'f'`).Scan(&fks, &deferrableFKs))
		assert.GreaterOrEqual(t, fks, 1)
		assert.Zero(t, deferrableFKs, "no deferrable FK (root: no unstated deferred FK)")
		cp := p25CpStateOf(t, r.Pool, defID)
		require.NotNil(t, cp.Owner)
		assert.Equal(t, run.ID, *cp.Owner, "pending run owns the checkpoint")
		assert.Equal(t, 0, cp.Epoch)
		assert.Nil(t, cp.Value, "first run has no confirmed value yet")

		// Frozen capture on the run row (runInternalPreRead is the routing
		// surface; fields are immutable after admission).
		pre, err := r.runInternalPreRead(ctx, run.ID, tenant)
		require.NoError(t, err)
		require.NotNil(t, pre)
		assert.Equal(t, CPModeCursor, pre.CPMode)
		assert.Equal(t, 0, pre.CPBaseEpoch)
		assert.Empty(t, pre.CPBaseValue)
		assert.NotEmpty(t, pre.CPBindingFingerprint)
		assert.Equal(t, jobTarget(t, r, defID), pre.CPOutputDatasetID)
	})

	t.Run("second_admission_busy_zero_run_zero_stamp", func(t *testing.T) {
		ctx := context.Background()
		r, _, tenant, sourceID, defID := p25Fixture(t)

		first, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		stamp := p23MustStamp(t, r.Pool, sourceID)

		second, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, second)
		require.NotNil(t, rejection)
		assert.Equal(t, IncrementalBusy, rejection.Kind)
		assert.Equal(t, first.ID, rejection.ActiveRunID, "the rejection names the owning run")
		assert.Equal(t, 1, p25RunCount(t, r.Pool, defID), "no second run row")
		p23AssertStampUnchanged(t, r.Pool, sourceID, stamp, "busy rejection consumes no admission stamp")
	})

	t.Run("binding_stale_rejected_without_silent_reset", func(t *testing.T) {
		ctx := context.Background()
		r, _, tenant, sourceID, defID := p25Fixture(t)
		first, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		// review-pg-code-round1 #1: release the owner through a REAL path
		// (public routed pending cancel) — never a hand-cleared owner —
		// preserving the checkpoint row and its confirmed fingerprint.
		by := uuid.New()
		res, err := r.RequestCancelSyncRun(ctx, first.ID, defID, tenant, &by, "free the owner")
		require.NoError(t, err)
		require.Equal(t, CancelOutcomeAborted, res.Outcome)
		cpBefore := p25CpJSON(t, r.Pool, defID)
		assert.Nil(t, p25CpStateOf(t, r.Pool, defID).Owner)

		// Owner-free, fingerprint mismatched: the stale branch is REACHABLE.
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), sha256Hex("stale-binding-probe"))
		require.NoError(t, err)
		require.Nil(t, run)
		require.NotNil(t, rejection)
		assert.Equal(t, IncrementalBindingStale, rejection.Kind)
		assert.Equal(t, 1, p25RunCount(t, r.Pool, defID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID),
			"stale rejection leaves the checkpoint row byte-equal")
	})

	t.Run("public_enqueue_captures_and_serializes", func(t *testing.T) {
		// review-pg-code-round1 #5: the PRODUCTION manual entry (EnqueueSyncRun)
		// captures the baseline and serializes same-Def work with a typed
		// busy rejection and zero stamp side effect.
		ctx := context.Background()
		r, _, tenant, sourceID, defID := p25Fixture(t)
		run, err := r.EnqueueSyncRun(ctx, defID, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, run, "public enqueue admits the incremental def")
		assert.Equal(t, "pending", run.Status)

		pre, err := r.runInternalPreRead(ctx, run.ID, tenant)
		require.NoError(t, err)
		require.NotNil(t, pre)
		assert.Equal(t, CPModeCursor, pre.CPMode, "the public entry freezes the capture")
		assert.Equal(t, jobTarget(t, r, defID), pre.CPOutputDatasetID)
		cp := p25CpStateOf(t, r.Pool, defID)
		require.NotNil(t, cp.Owner)
		assert.Equal(t, run.ID, *cp.Owner)

		stamp := p23MustStamp(t, r.Pool, sourceID)
		second, err := r.EnqueueSyncRun(ctx, defID, sourceID, tenant, uuid.New())
		require.Error(t, err, "the second public enqueue is the typed busy refusal")
		assert.Nil(t, second)
		var busy *IncrementalRejection
		require.ErrorAs(t, err, &busy)
		assert.Equal(t, IncrementalBusy, busy.Kind)
		assert.Equal(t, run.ID, busy.ActiveRunID)
		assert.Equal(t, 1, p25RunCount(t, r.Pool, defID))
		p23AssertStampUnchanged(t, r.Pool, sourceID, stamp, "busy consumes no admission stamp")
	})

	t.Run("source_lock_wait_config_flip_routes_by_authoritative_read", func(t *testing.T) {
		// Contract §3.3 / release correction 2: the lockless config read is
		// a HINT only. A legacy→incremental config flip that lands while a
		// public enqueue waits on the Source lock must route INCREMENTALLY
		// (the authoritative post-lock reread decides), producing a captured
		// P25 run — never a legacy row on flipped config.
		r, dsn := bootRepoWithDSN(t)
		ctx := context.Background()
		require.NoError(t, Migrate(ctx, r.Pool))
		tenant := Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}
		legacyCfg := []byte(`{"domain":"flip.example.com","auth":{"type":"none"},
			"runtime":{"worker":"foundry"},"permissions":{"syncable":true},
			"sync":{"resource_path":"/x","records_path":"items"}}`)
		conn, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
			Name: "flip-source", ConnectorType: "rest_api", Config: legacyCfg,
		}, tenant)
		require.NoError(t, err)
		require.NotNil(t, conn)
		job, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: conn.ID, OutputDatasetID: uuid.New()}, tenant)
		require.NoError(t, err)
		require.NotNil(t, job)

		blocker := singleConnRepo(t, dsn)
		pidBlocker := backendPID(t, blocker.Pool)
		enq := singleConnRepo(t, dsn)
		pidEnq := backendPID(t, enq.Pool)

		btx, err := blocker.Pool.Begin(ctx)
		require.NoError(t, err)
		var lockedSource uuid.UUID
		require.NoError(t, btx.QueryRow(ctx,
			`SELECT id FROM connections WHERE id = $1 FOR UPDATE`, conn.ID).Scan(&lockedSource))

		op := p24Go(ctx, func(ctx context.Context) (*models.SyncRun, error) {
			return enq.EnqueueSyncRun(ctx, job.ID, conn.ID, tenant, uuid.New())
		})
		p23RegisterCleanup(t,
			func() { _ = p25BoundedRollback(btx) },
			func() { op.shutdown(t) })
		waitUntilBlockedBy(t, r.Pool, pidEnq, pidBlocker)

		// The Source-holding transaction flips the config, THEN releases.
		_, err = btx.Exec(ctx,
			`UPDATE connections SET config = $2 WHERE id = $1`, conn.ID, p25IncrementalConfig())
		require.NoError(t, err)
		require.NoError(t, btx.Commit(ctx))

		res := mustRecv(t, op.done, "public enqueue after the Source-lock flip")
		require.NoError(t, res.Err)
		require.NotNil(t, res.Val)

		pre, err := r.runInternalPreRead(ctx, res.Val.ID, tenant)
		require.NoError(t, err)
		require.NotNil(t, pre)
		assert.Equal(t, CPModeCursor, pre.CPMode,
			"the AUTHORITATIVE post-lock read routed the flipped config incrementally")
		cp := p25CpStateOf(t, r.Pool, job.ID)
		require.NotNil(t, cp.Owner)
		assert.Equal(t, res.Val.ID, *cp.Owner)
		assert.Equal(t, 1, p25RunCount(t, r.Pool, job.ID))
	})

	t.Run("def_lock_wait_capture_is_authoritative_post_lock", func(t *testing.T) {
		// review-repo-round1 #1 via the PRODUCTION entry
		// (review-pg-code-round1 #5): a retarget that lands while the
		// PUBLIC EnqueueSyncRun waits on the Def lock must be captured —
		// the post-lock recompute decides, so the admitted run freezes the
		// NEW target/input with no internally stale owner.
		r, dsn, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()

		blocker := singleConnRepo(t, dsn)
		pidBlocker := backendPID(t, blocker.Pool)
		enq := singleConnRepo(t, dsn)
		pidEnq := backendPID(t, enq.Pool)

		btx, err := blocker.Pool.Begin(ctx)
		require.NoError(t, err)
		var lockedDef uuid.UUID
		require.NoError(t, btx.QueryRow(ctx,
			`SELECT id FROM batch_sync_defs WHERE id = $1 FOR UPDATE`, defID).Scan(&lockedDef))

		op := p24Go(ctx, func(ctx context.Context) (*models.SyncRun, error) {
			return enq.EnqueueSyncRun(ctx, defID, sourceID, tenant, uuid.New())
		})
		// review-pg-code-round1 #3: release the barrier, then cancel and
		// bounded-join the op — a Fatal can never hang Pool.Close.
		p23RegisterCleanup(t,
			func() { _ = p25BoundedRollback(btx) },
			func() { op.shutdown(t) })
		waitUntilBlockedBy(t, r.Pool, pidEnq, pidBlocker)

		// The Def-holding transaction itself retargets, THEN releases.
		newTarget := uuid.New()
		_, err = btx.Exec(ctx,
			`UPDATE batch_sync_defs SET output_dataset_id = $2 WHERE id = $1`, defID, newTarget)
		require.NoError(t, err)
		require.NoError(t, btx.Commit(ctx))

		res := mustRecv(t, op.done, "public enqueue after Def-lock wait")
		require.NoError(t, res.Err)
		require.NotNil(t, res.Val)

		pre, err := r.runInternalPreRead(ctx, res.Val.ID, tenant)
		require.NoError(t, err)
		require.NotNil(t, pre)
		assert.Equal(t, newTarget, pre.CPOutputDatasetID,
			"the capture freezes the post-lock (NEW) target, not the pre-lock read")
		_, fpNow, err := readIncrementalBinding(ctx, r.Pool, defID, sourceID, tenant)
		require.NoError(t, err)
		assert.Equal(t, fpNow, pre.CPBindingFingerprint,
			"the capture fingerprint matches the recomputed authoritative binding")
		cp := p25CpStateOf(t, r.Pool, defID)
		require.NotNil(t, cp.Owner)
		assert.Equal(t, res.Val.ID, *cp.Owner)
		assert.Equal(t, 1, p25RunCount(t, r.Pool, defID))
	})

	t.Run("configured_initial_value_is_durably_captured", func(t *testing.T) {
		// review-repo-round1 #5: with no confirmed value, admission must
		// durably freeze the CONFIGURED InitialValue into run.cp_base_value
		// while the checkpoint confirmation stays NULL/epoch 0 — and no
		// configured initial means NULL, never rederived at claim time.
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()

		withInitial := strings.Replace(string(p25IncrementalConfig()),
			`"completed_checkpoint_path": "completed"`,
			`"completed_checkpoint_path": "completed", "initial_value": "init-token-7"`, 1)
		conn2, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
			Name: "inc-with-initial", ConnectorType: "rest_api", Config: []byte(withInitial),
		}, tenant)
		require.NoError(t, err)
		require.NotNil(t, conn2)
		job2, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: conn2.ID, OutputDatasetID: uuid.New()}, tenant)
		require.NoError(t, err)
		require.NotNil(t, job2)

		run, rejection, err := p25Admit(ctx, r, tenant, conn2.ID, job2.ID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		require.NotNil(t, run)

		var baseValue *string
		var confirmed *string
		var epoch int
		require.NoError(t, r.Pool.QueryRow(ctx, `
			SELECT r.cp_base_value, cp.checkpoint_value, cp.confirmed_epoch
			  FROM sync_runs r
			  JOIN sync_def_checkpoints cp ON cp.sync_def_id = r.sync_def_id
			 WHERE r.id = $1`, run.ID).Scan(&baseValue, &confirmed, &epoch))
		require.NotNil(t, baseValue, "the configured initial value IS the frozen input")
		assert.Equal(t, "init-token-7", *baseValue)
		assert.Nil(t, confirmed, "confirmation stays unset (epoch 0, NULL value)")
		assert.Equal(t, 0, epoch)

		// Without a configured initial: NULL capture, never an invented one.
		runNoInit, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT cp_base_value FROM sync_runs WHERE id = $1`, runNoInit.ID).Scan(&baseValue))
		assert.Nil(t, baseValue, "no configured initial ⇒ NULL frozen input")
	})

	t.Run("checkpoint_row_contention_aborts_without_tick_or_run", func(t *testing.T) {
		ctx := context.Background()
		r, dsn, tenant, sourceID, defID := p25Fixture(t)
		// Initialize the row first (contention targets the EXISTING row's
		// NOWAIT lock; the placeholder path is guarded by the Def lock),
		// then cancel the pending run so the owner slot is free again.
		firstRun, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		res, err := r.RequestCancelSyncRun(context.Background(), firstRun.ID, defID, tenant, nil, "release for contention probe")
		require.NoError(t, err)
		require.Equal(t, CancelOutcomeAborted, res.Outcome)

		blocker := singleConnRepo(t, dsn)
		btx, err := blocker.Pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = p25BoundedRollback(btx) }()
		var locked uuid.UUID
		require.NoError(t, btx.QueryRow(ctx,
			`SELECT sync_def_id FROM sync_def_checkpoints WHERE sync_def_id = $1 FOR UPDATE`,
			defID).Scan(&locked))

		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.Error(t, err, "the NOWAIT miss surfaces as contention, not a silent wait")
		assert.ErrorIs(t, err, errCheckpointLockContention)
		assert.Nil(t, run)
		assert.Nil(t, rejection)
	})
}

// T3 — same Def, two real pools: the Source row serializes admissions; the
// serialized loser answers busy with zero rows (root round-1 #3 causal fix).
func TestPGCheckpointSameDefTwoPoolOwnerUniqueness(t *testing.T) {
	r, dsn, tenant, sourceID, defID := p25Fixture(t)
	ctx := context.Background()

	enqA := singleConnRepo(t, dsn)
	pidA := backendPID(t, enqA.Pool) // warmed BEFORE the pool's only conn is taken (§7-D)
	enqB := singleConnRepo(t, dsn)
	pidB := backendPID(t, enqB.Pool)

	// A opens the admission transaction and takes the Source lock FIRST —
	// the causal barrier: B cannot reach the Def/Checkpoint locks at all.
	atx, err := enqA.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = p25BoundedRollback(atx) }()
	require.NoError(t, lockSourceRowForUpdate(ctx, atx, sourceID, tenant))
	defPredicate, defArg := syncDefTenantPredicate(tenant, 2)
	var lockedDef uuid.UUID
	require.NoError(t, atx.QueryRow(ctx,
		`SELECT d.id FROM batch_sync_defs d WHERE d.id = $1 AND `+defPredicate+` FOR UPDATE`,
		defID, defArg).Scan(&lockedDef))
	bindingA, fpA, err := readIncrementalBinding(ctx, atx, defID, sourceID, tenant)
	require.NoError(t, err)
	runA, rejection, err := enqA.AcquireCheckpointForAdmission(ctx, atx, defID, sourceID, tenant, uuid.New(), bindingA, fpA)
	require.NoError(t, err)
	require.Nil(t, rejection)
	require.NotNil(t, runA)
	require.NoError(t, advanceSyncLastAdmitted(ctx, atx, sourceID))

	// B launches its full admission; it can ONLY park on A's Source lock.
	op := p24Go(ctx, func(ctx context.Context) (*models.SyncRun, error) {
		return enqB.EnqueueSyncRun(ctx, defID, sourceID, tenant, uuid.New())
	})
	// review-pg-code-round1 #3: release (A's tx) → cancel → bounded join.
	p23RegisterCleanup(t,
		func() { _ = p25BoundedRollback(atx) },
		func() { op.shutdown(t) })
	waitUntilBlockedBy(t, r.Pool, pidB, pidA)

	// A commits (owner + run become durable); B then proceeds serialized.
	require.NoError(t, atx.Commit(ctx))
	res := mustRecv(t, op.done, "serialized second admission")
	require.Error(t, res.Err)
	var busy *IncrementalRejection
	require.ErrorAs(t, res.Err, &busy)
	assert.Equal(t, IncrementalBusy, busy.Kind)
	assert.Equal(t, runA.ID, busy.ActiveRunID)

	assert.Equal(t, 1, p25RunCount(t, r.Pool, defID), "exactly ONE run exists")
	cp := p25CpStateOf(t, r.Pool, defID)
	require.NotNil(t, cp.Owner)
	assert.Equal(t, runA.ID, *cp.Owner, "the committed winner owns the checkpoint")
}

// T4 — sibling Defs of one source stay independent once ownership is DURABLE
// (no DB lock held); tenant negatives run against real foreign rows.
func TestPGCheckpointSiblingDefIndependentNoLockHeld(t *testing.T) {
	r, _, tenant, sourceID, _ := p25Fixture(t)
	ctx := context.Background()

	job2, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
		SourceID: sourceID, OutputDatasetID: uuid.New()}, tenant)
	require.NoError(t, err)
	require.NotNil(t, job2)
	job1, err := r.GetSyncJob(ctx, mustDefOfSource(t, r, sourceID, tenant, job2.ID), tenant)
	require.NoError(t, err)
	require.NotNil(t, job1)

	// Def A holds DURABLE ownership and NO database lock (committed, no
	// executor running) — Def B admits freely.
	runA, rejection, err := p25Admit(ctx, r, tenant, sourceID, job1.ID, uuid.New(), "")
	require.NoError(t, err)
	require.Nil(t, rejection)
	runB, rejection, err := p25Admit(ctx, r, tenant, sourceID, job2.ID, uuid.New(), "")
	require.NoError(t, err)
	require.Nil(t, rejection)
	assert.Equal(t, 1, p25RunCount(t, r.Pool, job1.ID))
	assert.Equal(t, 1, p25RunCount(t, r.Pool, job2.ID))
	assert.NotEqual(t, runA.ID, runB.ID)

	cpA := p25CpStateOf(t, r.Pool, job1.ID)
	cpB := p25CpStateOf(t, r.Pool, job2.ID)
	require.NotNil(t, cpA.Owner)
	require.NotNil(t, cpB.Owner)
	assert.Equal(t, runA.ID, *cpA.Owner)
	assert.Equal(t, runB.ID, *cpB.Owner)

	// Live tenant negatives (real rows exist; positive controls above).
	foreign := Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}
	viewForeign, err := r.GetCheckpointView(ctx, job1.ID, foreign)
	require.NoError(t, err)
	assert.Nil(t, viewForeign, "foreign tenant sees no checkpoint view")
	preForeign, err := r.runInternalPreRead(ctx, runA.ID, foreign)
	require.NoError(t, err)
	assert.Nil(t, preForeign, "foreign tenant pre-read is a clean nil")
	viewOK, err := r.GetCheckpointView(ctx, job1.ID, tenant)
	require.NoError(t, err)
	require.NotNil(t, viewOK)
	assert.NotNil(t, viewOK.Owner, "correct tenant sees the owner (positive control)")
}

// mustDefOfSource returns the OTHER def of the source than the excluded one
// (the first-created def), failing loudly when the fixture shape breaks.
func mustDefOfSource(t *testing.T, r *Repo, sourceID uuid.UUID, tenant Tenant, exclude uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	require.NoError(t, r.Pool.QueryRow(context.Background(),
		`SELECT d.id FROM batch_sync_defs d WHERE d.source_id = $1 AND d.id <> $2
		  ORDER BY d.created_at LIMIT 1`, sourceID, exclude).Scan(&id))
	return id
}

// T5 — success advance is all-or-nothing: marker-RAISE on the checkpoint
// update rolls the WHOLE transaction back (run stays running, owner kept);
// a CAS miss writes NOTHING (no unilateral succeeded); the happy path
// advances epoch/value/forensics and releases the owner atomically.
func TestPGCheckpointAdvanceAllOrNothingRollback(t *testing.T) {
	t.Run("happy_path_atomic_advance_and_release", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, _ := p25ClaimGate(t, r, tenant, run.ID, "h", "tok")

		txn := uuid.New()
		done, err := r.CompleteIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok", claim.Run.Generation,
			IncrementalAdvance{CompletedCheckpoint: "cp-token-1", Rows: 7, Bytes: 128,
				ContentHash: sha256Hex("p25-batch-1"), TxnID: txn})
		require.NoError(t, err)
		require.NotNil(t, done)
		assert.Equal(t, "succeeded", done.Status)

		cp := p25CpStateOf(t, r.Pool, defID)
		require.NotNil(t, cp.Value)
		assert.Equal(t, "cp-token-1", *cp.Value)
		assert.Equal(t, 1, cp.Epoch)
		assert.Nil(t, cp.Owner, "owner released in the SAME transaction")
		require.NotNil(t, cp.AdvancedBy)
		assert.Equal(t, run.ID, *cp.AdvancedBy)
		require.NotNil(t, cp.LastBatchTxn)
		assert.Equal(t, txn, *cp.LastBatchTxn)

		// Safe projection: length + sha256 only, never the raw token.
		view, err := r.GetCheckpointView(ctx, defID, tenant)
		require.NoError(t, err)
		require.NotNil(t, view)
		assert.True(t, view.Initialized)
		assert.Equal(t, 1, view.ConfirmedEpoch)
		assert.Equal(t, len("cp-token-1"), view.CheckpointLen)
		assert.Equal(t, sha256Hex("cp-token-1"), view.CheckpointSHA256)
		require.NotNil(t, view.LastConfirmed)
		assert.Equal(t, txn, view.LastConfirmed.TxnID)
		assert.Nil(t, view.Owner)
	})

	t.Run("marker_raise_on_checkpoint_update_rolls_back_everything", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, _ := p25ClaimGate(t, r, tenant, run.ID, "h", "tok")

		runBefore := runRowJSON(t, r.Pool, run.ID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		_, err = r.Pool.Exec(ctx, `
			CREATE OR REPLACE FUNCTION p25_fail_cp_advance() RETURNS trigger AS $fn$
			BEGIN RAISE EXCEPTION 'p25 forced checkpoint advance failure' USING ERRCODE = 'P0001'; END $fn$ LANGUAGE plpgsql;
			CREATE TRIGGER p25_fail_cp_advance_trg BEFORE UPDATE ON sync_def_checkpoints
			  FOR EACH ROW WHEN (NEW.confirmed_epoch <> OLD.confirmed_epoch)
			  EXECUTE FUNCTION p25_fail_cp_advance();`)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = r.Pool.Exec(context.Background(),
				`DROP TRIGGER IF EXISTS p25_fail_cp_advance_trg ON sync_def_checkpoints;
				 DROP FUNCTION IF EXISTS p25_fail_cp_advance();`)
		})

		_, err = r.CompleteIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok", claim.Run.Generation,
			IncrementalAdvance{CompletedCheckpoint: "cp-token-x", Rows: 1, Bytes: 2,
				ContentHash: sha256Hex("p25-marker-x"), TxnID: uuid.New()})
		require.Error(t, err, "the forced failure must surface")
		// review-pg-code-round1 #2: the error must BE the trigger's RAISE —
		// exact message and SQLSTATE — proving the SQL UPDATE was reached
		// (a generic require.Error could mask a validation refusal).
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "P0001", pgErr.Code)
		assert.Contains(t, pgErr.Message, "p25 forced checkpoint advance failure")

		// marker RAISE proves LOCAL rollback ONLY (round-1 #8): run stays
		// running, checkpoint byte-equal, owner retained — all-old.
		assert.Equal(t, "running", p25RunStatus(t, r.Pool, run.ID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, run.ID))
	})

	t.Run("current_binding_drift_refuses_advance", func(t *testing.T) {
		// review-repo-round1 #4: under the ordered locks the CURRENT full
		// binding/target must be re-read and compared against the captured
		// run — an out-of-band source-config drift (fence bypassed by
		// future code, simulated with direct SQL) may NOT confirm the old
		// position even though cp.oldfp==run.oldfp still matches.
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, _ := p25ClaimGate(t, r, tenant, run.ID, "h", "tok")

		drifted := strings.Replace(string(p25IncrementalConfig()),
			`"resource_path": "/items"`, `"resource_path": "/drifted"`, 1)
		_, err = r.Pool.Exec(ctx,
			`UPDATE connections SET config = $2 WHERE id = $1`, sourceID, []byte(drifted))
		require.NoError(t, err)
		runBefore := runRowJSON(t, r.Pool, run.ID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		done, err := r.CompleteIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok",
			claim.Run.Generation, IncrementalAdvance{CompletedCheckpoint: "cp-drift",
				Rows: 1, Bytes: 2, ContentHash: sha256Hex("drift"), TxnID: uuid.New()})
		require.Error(t, err, "current-binding drift must refuse to confirm the old position")
		assert.Nil(t, done)
		assert.True(t, errors.Is(err, ErrIncrementalCheckpointCAS) || errors.Is(err, ErrLeaseLost),
			"the refusal is typed, got: %v", err)
		assert.Equal(t, "running", p25RunStatus(t, r.Pool, run.ID))
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, run.ID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
	})

	t.Run("checkpoint_cas_miss_writes_nothing_no_unilateral_succeeded", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, _ := p25ClaimGate(t, r, tenant, run.ID, "h", "tok")

		// Epoch drift out-of-band (the single-flight defense-in-depth case).
		_, err = r.Pool.Exec(ctx,
			`UPDATE sync_def_checkpoints SET confirmed_epoch = 9 WHERE sync_def_id = $1`, defID)
		require.NoError(t, err)
		runBefore := runRowJSON(t, r.Pool, run.ID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		done, err := r.CompleteIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok", claim.Run.Generation,
			IncrementalAdvance{CompletedCheckpoint: "cp-token-y", Rows: 1, Bytes: 2,
				ContentHash: sha256Hex("p25-cas-y"), TxnID: uuid.New()})
		require.Error(t, err, "CAS miss is a typed refusal")
		assert.Nil(t, done)
		assert.ErrorIs(t, err, ErrIncrementalCheckpointCAS)
		assert.Equal(t, "running", p25RunStatus(t, r.Pool, run.ID),
			"the run may NOT be succeeded alone (round-1 #3)")
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, run.ID))
	})
}

// T6 — natural lease expiry under a PURE lock wait (root round-1 #2): the
// parked row is never mutated; validity is proven at genuine block time; the
// DB clock crosses expiry on its own; the post-lock fresh clock rejects with
// every protected row byte-identical.
func TestPGCheckpointLeaseExpiryUnderLockWaitNatural(t *testing.T) {
	r, dsn, tenant, sourceID, defID := p25Fixture(t)
	ctx := context.Background()

	run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
	require.NoError(t, err)
	require.Nil(t, rejection)
	claim, err := r.ClaimSyncRun(ctx, run.ID, tenant, "h", "tok", 2*time.Second)
	require.NoError(t, err)
	require.NotNil(t, claim)

	// Pure blocker on the Def row — lock only, NEVER a row write.
	blocker := singleConnRepo(t, dsn)
	pidBlocker := backendPID(t, blocker.Pool)
	worker := singleConnRepo(t, dsn)
	pidWorker := backendPID(t, worker.Pool)
	btx, err := blocker.Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = p25BoundedRollback(btx) }()
	var locked uuid.UUID
	require.NoError(t, btx.QueryRow(ctx,
		`SELECT id FROM batch_sync_defs WHERE id = $1 FOR UPDATE`, defID).Scan(&locked))

	runBefore := runRowJSON(t, r.Pool, run.ID)
	cpBefore := p25CpJSON(t, r.Pool, defID)

	op := p24Go(ctx, func(ctx context.Context) (*models.SyncRun, error) {
		return worker.FailIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok",
			claim.Run.Generation, "failed", nil, nil)
	})
	p23RegisterCleanup(t,
		func() { _ = p25BoundedRollback(btx) },
		func() { op.shutdown(t) })
	waitUntilBlockedBy(t, r.Pool, pidWorker, pidBlocker)

	// Prove the lease is STILL VALID at genuine block time.
	var validAtBlock bool
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT clock_timestamp() < lease_expires_at FROM sync_runs WHERE id = $1`,
		run.ID).Scan(&validAtBlock))
	require.True(t, validAtBlock, "lease must be valid while the worker is genuinely blocked")

	// Let the DB clock cross expiry NATURALLY (no row mutation anywhere).
	p23WaitClockPast(t, r.Pool, p25LeaseExpiry(t, r.Pool, run.ID))
	require.NoError(t, p25BoundedRollback(btx))

	res := mustRecv(t, op.done, "expiry-under-wait failure finalize")
	require.Error(t, res.Err)
	assert.ErrorIs(t, res.Err, ErrLeaseLost,
		"the post-lock fresh clock rejects the now-expired lease")

	assert.Equal(t, runBefore, runRowJSON(t, r.Pool, run.ID), "run row byte-identical")
	assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID), "checkpoint byte-identical, owner retained")
	cp := p25CpStateOf(t, r.Pool, defID)
	require.NotNil(t, cp.Owner)
	assert.Equal(t, run.ID, *cp.Owner)
	p23AssertNoIdleInTx(t, r.Pool)
}

// TestPGCheckpointLeaseExpiryUnderLockWaitNatural — success-advance twin
// (review-pg-code-round1 #6): the SAME pure-lock natural-expiry discipline,
// but through a VALID gated run and the SUCCESS finalize. A stale-clock bug
// specific to the passed-SQL advance path would otherwise be uncovered.
func TestPGCheckpointAdvanceExpiryUnderLockWaitNatural(t *testing.T) {
	r, dsn, tenant, sourceID, defID := p25Fixture(t)
	ctx := context.Background()

	run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
	require.NoError(t, err)
	require.Nil(t, rejection)
	claim, err := r.ClaimSyncRun(ctx, run.ID, tenant, "h", "tok", 2*time.Second)
	require.NoError(t, err)
	require.NotNil(t, claim)
	gate, err := r.BeginPublishGate(ctx, run.ID, tenant, "h", "tok", claim.Run.Generation, 2*time.Second)
	require.NoError(t, err)
	require.Equal(t, PublishGateEntered, gate.Outcome)

	// Pure blocker on the Def row — lock only, NEVER a row write.
	blocker := singleConnRepo(t, dsn)
	pidBlocker := backendPID(t, blocker.Pool)
	worker := singleConnRepo(t, dsn)
	pidWorker := backendPID(t, worker.Pool)
	btx, err := blocker.Pool.Begin(ctx)
	require.NoError(t, err)
	var locked uuid.UUID
	require.NoError(t, btx.QueryRow(ctx,
		`SELECT id FROM batch_sync_defs WHERE id = $1 FOR UPDATE`, defID).Scan(&locked))

	runBefore := runRowJSON(t, r.Pool, run.ID)
	cpBefore := p25CpJSON(t, r.Pool, defID)

	adv := IncrementalAdvance{CompletedCheckpoint: "cp-expired",
		Rows: 3, Bytes: 9, ContentHash: sha256Hex("p25-expired-batch"), TxnID: uuid.New()}
	op := p24Go(ctx, func(ctx context.Context) (*models.SyncRun, error) {
		return worker.CompleteIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok",
			claim.Run.Generation, adv)
	})
	p23RegisterCleanup(t,
		func() { _ = p25BoundedRollback(btx) },
		func() { op.shutdown(t) })
	waitUntilBlockedBy(t, r.Pool, pidWorker, pidBlocker)

	// Valid at genuine block time; the DB clock then crosses expiry NATURALLY
	// (no parked row is ever mutated).
	var validAtBlock bool
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT clock_timestamp() < lease_expires_at FROM sync_runs WHERE id = $1`,
		run.ID).Scan(&validAtBlock))
	require.True(t, validAtBlock, "the lease must still be valid while blocked")
	p23WaitClockPast(t, r.Pool, p25LeaseExpiry(t, r.Pool, run.ID))
	require.NoError(t, p25BoundedRollback(btx))

	res := mustRecv(t, op.done, "success advance under natural expiry")
	require.Error(t, res.Err)
	assert.ErrorIs(t, res.Err, ErrLeaseLost,
		"the post-lock fresh clock rejects the advance CAS")

	assert.Equal(t, runBefore, runRowJSON(t, r.Pool, run.ID),
		"no succeeded write, no half-advance")
	assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
	cp := p25CpStateOf(t, r.Pool, defID)
	require.NotNil(t, cp.Owner)
	assert.Equal(t, run.ID, *cp.Owner)
	p23AssertNoIdleInTx(t, r.Pool)
}

// T7 — fencing matrix: every mismatch is a typed zero-write refusal with
// full-row equality on BOTH protected tables.
func TestPGCheckpointFencingMatrixZeroWrite(t *testing.T) {
	newClaimed := func(t *testing.T) (*Repo, Tenant, uuid.UUID, uuid.UUID, uuid.UUID, *LeaseClaim) {
		ctx := context.Background()
		r, _, tenant, sourceID, defID := p25Fixture(t)
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, err := r.ClaimSyncRun(context.Background(), run.ID, tenant, "h", "tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		return r, tenant, sourceID, defID, run.ID, claim
	}

	t.Run("wrong_token_and_wrong_generation_fence_zero_write", func(t *testing.T) {
		r, tenant, _, defID, runID, claim := newClaimed(t)
		ctx := context.Background()

		// review-pg-code-round2 #4: enter the gate CORRECTLY first — a NULL
		// gate would mask the intended success-finalize fence behind the
		// stage guard. Snapshots are taken AFTER the valid gate entry.
		gate, err := r.BeginPublishGate(ctx, runID, tenant, "h", "tok", claim.Run.Generation, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, gate)
		require.Equal(t, PublishGateEntered, gate.Outcome, "the gate must genuinely enter")
		runBefore := runRowJSON(t, r.Pool, runID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		// Gate negatives (kept as independent observations).
		gate, err = r.BeginPublishGate(ctx, runID, tenant, "h", "WRONG", claim.Run.Generation, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, gate)
		assert.Equal(t, PublishGateFenced, gate.Outcome)
		gate, err = r.BeginPublishGate(ctx, runID, tenant, "h", "tok", claim.Run.Generation+7, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, gate)
		assert.Equal(t, PublishGateFenced, gate.Outcome)

		// The SUCCESS finalize under wrong material, one variant at a time:
		// each must be a typed zero-write refusal (fully valid advance).
		done, err := r.CompleteIncrementalSyncRun(ctx, runID, tenant, "h", "WRONG",
			claim.Run.Generation, IncrementalAdvance{CompletedCheckpoint: "cp-w",
				Rows: 1, Bytes: 1, ContentHash: sha256Hex("p25-fence-w"), TxnID: uuid.New()})
		require.Error(t, err, "wrong-token Complete refuses")
		assert.Nil(t, done)
		assert.ErrorIs(t, err, ErrLeaseLost, "the success CAS fences wrong lease material")
		assert.Equal(t, "running", p25RunStatus(t, r.Pool, runID))
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))

		done, err = r.CompleteIncrementalSyncRun(ctx, runID, tenant, "h", "tok",
			claim.Run.Generation+7, IncrementalAdvance{CompletedCheckpoint: "cp-g",
				Rows: 1, Bytes: 1, ContentHash: sha256Hex("p25-fence-g"), TxnID: uuid.New()})
		require.Error(t, err, "wrong-generation Complete refuses")
		assert.Nil(t, done)
		assert.ErrorIs(t, err, ErrLeaseLost, "the success CAS fences stale generations")
		assert.Equal(t, "running", p25RunStatus(t, r.Pool, runID))
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
	})

	t.Run("owner_and_epoch_mismatch_are_typed_zero_write", func(t *testing.T) {
		r, tenant, _, defID, runID, claim := newClaimed(t)
		ctx := context.Background()
		_, err := r.BeginPublishGate(ctx, runID, tenant, "h", "tok", claim.Run.Generation, time.Minute)
		require.NoError(t, err) // entered

		// Owner cleared out-of-band with the epoch INTACT.
		_, err = r.Pool.Exec(ctx,
			`UPDATE sync_def_checkpoints SET active_run_id = NULL WHERE sync_def_id = $1`, defID)
		require.NoError(t, err)
		runBefore := runRowJSON(t, r.Pool, runID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		done, err := r.CompleteIncrementalSyncRun(ctx, runID, tenant, "h", "tok", claim.Run.Generation,
			IncrementalAdvance{CompletedCheckpoint: "cp-z", Rows: 1, Bytes: 1,
				ContentHash: sha256Hex("p25-fence-z"), TxnID: uuid.New()})
		require.Error(t, err)
		assert.Nil(t, done)
		assert.ErrorIs(t, err, ErrIncrementalCheckpointCAS)
		assert.Equal(t, "running", p25RunStatus(t, r.Pool, runID))
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
	})

	t.Run("wrong_epoch_with_correct_owner_is_typed_zero_write", func(t *testing.T) {
		// review-pg-code-round1 #4: an INDEPENDENT wrong-epoch variant where
		// the owner still points at this run — only the base epoch drifted.
		r, tenant, _, defID, runID, claim := newClaimed(t)
		ctx := context.Background()
		_, err := r.BeginPublishGate(ctx, runID, tenant, "h", "tok", claim.Run.Generation, time.Minute)
		require.NoError(t, err)
		_, err = r.Pool.Exec(ctx,
			`UPDATE sync_def_checkpoints SET confirmed_epoch = confirmed_epoch + 5 WHERE sync_def_id = $1`,
			defID)
		require.NoError(t, err)
		runBefore := runRowJSON(t, r.Pool, runID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		done, err := r.CompleteIncrementalSyncRun(ctx, runID, tenant, "h", "tok", claim.Run.Generation,
			IncrementalAdvance{CompletedCheckpoint: "cp-e", Rows: 1, Bytes: 1,
				ContentHash: sha256Hex("p25-fence-e"), TxnID: uuid.New()})
		require.Error(t, err, "correct owner does not excuse a wrong base epoch")
		assert.Nil(t, done)
		assert.ErrorIs(t, err, ErrIncrementalCheckpointCAS)
		assert.Equal(t, "running", p25RunStatus(t, r.Pool, runID))
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
	})

	t.Run("fingerprint_drift_gate_answers_binding_stale", func(t *testing.T) {
		r, tenant, _, defID, runID, claim := newClaimed(t)
		ctx := context.Background()
		_, err := r.Pool.Exec(ctx,
			`UPDATE sync_def_checkpoints SET binding_fingerprint = 'deadbeef' WHERE sync_def_id = $1`, defID)
		require.NoError(t, err)
		cpBefore := p25CpJSON(t, r.Pool, defID)
		runBefore := runRowJSON(t, r.Pool, runID)

		gate, err := r.BeginIncrementalPublishGate(ctx, runID, tenant, "h", "tok", claim.Run.Generation, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, gate)
		assert.Equal(t, IncrementalPublishGateBindingStale, gate.Outcome)
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID))
	})

	t.Run("ordinary_complete_may_not_write_p25_success", func(t *testing.T) {
		r, tenant, _, defID, runID, claim := newClaimed(t)
		ctx := context.Background()
		_, err := r.BeginPublishGate(ctx, runID, tenant, "h", "tok", claim.Run.Generation, time.Minute)
		require.NoError(t, err)
		runBefore := runRowJSON(t, r.Pool, runID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		_, err = r.CompleteLeasedSyncRun(ctx, runID, tenant, "h", "tok", claim.Run.Generation,
			"succeeded", 1, 1, 1, nil, nil, claim.Run.DatasetVersionID, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrIncrementalRequiresAdvance)
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
	})
}

// T8 — prepublish cancel/failure: the DB-asserted release boundary
// (publish_entered_at IS NULL) decides release vs retain; the ordered cancel
// routes through the public entrypoint.
func TestPGCheckpointPrepublishCancelReleaseOrdered(t *testing.T) {
	t.Run("pending_cancel_aborts_and_releases_same_tx", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)

		by := uuid.New()
		res, err := r.RequestCancelSyncRun(ctx, run.ID, defID, tenant, &by, "test cancel")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeAborted, res.Outcome)
		assert.Equal(t, "aborted", res.Run.Status)

		cp := p25CpStateOf(t, r.Pool, defID)
		assert.Nil(t, cp.Owner, "owner released in the same transaction")
		assert.Equal(t, 0, cp.Epoch, "no advance on cancel")
		assert.Nil(t, cp.Value)
		assert.Nil(t, cp.AdvancedBy, "no forensics on cancel")
	})

	t.Run("running_cancel_persists_request_owner_kept_then_pre_gate_fail_releases", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, err := r.ClaimSyncRun(ctx, run.ID, tenant, "h", "tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)

		by := uuid.New()
		res, err := r.RequestCancelSyncRun(ctx, run.ID, defID, tenant, &by, "test cancel")
		require.NoError(t, err)
		assert.Equal(t, CancelOutcomeRequested, res.Outcome)
		require.NotNil(t, p25CpStateOf(t, r.Pool, defID).Owner, "a running cancel retains the owner")

		// Pre-gate abort through the routed failure path: released.
		msg := "cancelled before the gate"
		done, err := r.FailIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok",
			claim.Run.Generation, "aborted", &msg, nil)
		require.NoError(t, err)
		require.NotNil(t, done)
		assert.Equal(t, "aborted", done.Status)
		assert.Nil(t, p25CpStateOf(t, r.Pool, defID).Owner,
			"pre-gate abort releases the owner (publish_entered_at IS NULL held)")
		assert.Equal(t, 0, p25CpStateOf(t, r.Pool, defID).Epoch)
	})

	t.Run("post_gate_failure_retains_owner_and_aborted_is_staleguarded", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, _ := p25ClaimGate(t, r, tenant, run.ID, "h", "tok")
		runBefore := runRowJSON(t, r.Pool, run.ID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		// review-pg-code-round1 #4: 'aborted' FIRST — the stage guard must
		// refuse it on a gate-entered run with Run+CP exactly unchanged.
		why := "cancel too late for aborted"
		done, err := r.FailIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok",
			claim.Run.Generation, "aborted", &why, nil)
		require.Error(t, err, "aborted is stage-guarded once the gate is entered")
		assert.Nil(t, done)
		assert.ErrorIs(t, err, ErrLeaseLost)
		assert.Equal(t, "running", p25RunStatus(t, r.Pool, run.ID))
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, run.ID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))

		// Gate entered, nothing dispatched: honest failed, owner RETAINED
		// (release correction 1 — gate ENTRY is the conservative boundary).
		msg := "work context died after the gate, before dispatch"
		done, err = r.FailIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok",
			claim.Run.Generation, "failed", &msg, nil)
		require.NoError(t, err)
		require.NotNil(t, done)
		assert.Equal(t, "failed", done.Status)
		cp := p25CpStateOf(t, r.Pool, defID)
		require.NotNil(t, cp.Owner, "post-gate failure RETAINS the owner (P26)")
		assert.Equal(t, run.ID, *cp.Owner)
		assert.Equal(t, 0, cp.Epoch)
		assert.Nil(t, cp.Value)

		// The retained FAILED owner is queryable (never disguised) with the
		// business-state wording (review-repo-round1 #8 API clarification).
		view, err := r.GetCheckpointView(ctx, defID, tenant)
		require.NoError(t, err)
		require.NotNil(t, view)
		require.NotNil(t, view.Owner)
		assert.Equal(t, "failed", view.Owner.RunStatus)
		assert.Equal(t, "requires_reconciliation", view.Owner.BlockedReason)
	})

	t.Run("post_gate_fail_wrong_owner_is_zero_write", func(t *testing.T) {
		// review-repo-round1 #3: the retain branch needs the SAME owner/
		// base-epoch/binding guard as the release branch — a Fail call for
		// a run that is NOT the current owner may never close anybody's
		// work, not even as failed+retain.
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, _ := p25ClaimGate(t, r, tenant, run.ID, "h", "tok")
		_, err = r.Pool.Exec(ctx,
			`UPDATE sync_def_checkpoints SET active_run_id = NULL WHERE sync_def_id = $1`, defID)
		require.NoError(t, err)
		runBefore := runRowJSON(t, r.Pool, run.ID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		msg := "owner drifted before failure finalize"
		done, err := r.FailIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok",
			claim.Run.Generation, "failed", &msg, nil)
		require.Error(t, err, "a wrong-owner Fail is a fenced/uncertain refusal")
		assert.Nil(t, done)
		assert.True(t, errors.Is(err, ErrIncrementalCheckpointCAS) || errors.Is(err, ErrLeaseLost),
			"the refusal is typed, got: %v", err)
		assert.Equal(t, "running", p25RunStatus(t, r.Pool, run.ID))
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, run.ID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
	})

	t.Run("cancel_wrong_parent_zero_writes_before_any_state_change", func(t *testing.T) {
		// review-repo-round1 #2: the ordered cancel must lock/verify the
		// BOUND Def (Source→Def→CP→Run); a wrong parent or an unknown run
		// answers not_visible with zero writes on every protected row.
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		run1, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		job2, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: sourceID, OutputDatasetID: uuid.New()}, tenant)
		require.NoError(t, err)
		run2, rejection, err := p25Admit(ctx, r, tenant, sourceID, job2.ID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)

		cp1Before := p25CpJSON(t, r.Pool, defID)
		cp2Before := p25CpJSON(t, r.Pool, job2.ID)
		run1Before := runRowJSON(t, r.Pool, run1.ID)
		run2Before := runRowJSON(t, r.Pool, run2.ID)

		// run2 addressed under def1's parent: invisible, zero-write.
		res, err := r.RequestCancelSyncRun(ctx, run2.ID, defID, tenant, nil, "wrong parent")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeNotVisible, res.Outcome)
		// Unknown run under the real def: same clean refusal.
		res, err = r.RequestCancelSyncRun(ctx, uuid.New(), defID, tenant, nil, "unknown run")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeNotVisible, res.Outcome)
		// review-source-round2 #1: the tenant predicate placeholder ($3) —
		// a FOREIGN-ORG tenant cancel against the real (run, def) pair must
		// stay invisible; a wrong placeholder binding would match the row.
		foreign := Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}
		res, err = r.RequestCancelSyncRun(ctx, run1.ID, defID, foreign, nil, "foreign org")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeNotVisible, res.Outcome)

		// Every negative above left all four protected rows byte-identical.
		assert.Equal(t, cp1Before, p25CpJSON(t, r.Pool, defID))
		assert.Equal(t, cp2Before, p25CpJSON(t, r.Pool, job2.ID))
		assert.Equal(t, run1Before, runRowJSON(t, r.Pool, run1.ID))
		assert.Equal(t, run2Before, runRowJSON(t, r.Pool, run2.ID))

		// Same-tenant positive control LAST (it legitimately mutates rows):
		// the correct tenant's cancel IS visible and aborts.
		res, err = r.RequestCancelSyncRun(ctx, run1.ID, defID, tenant, nil, "owner tenant")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeAborted, res.Outcome)
	})

	t.Run("live_second_batch_owner_not_mislabeled_by_last_advance", func(t *testing.T) {
		// review-repo-round1 #8: observability follows the CURRENT owner
		// run, never the historical advanced_by_run — a healthy second
		// batch (running owner, prior advance present) must NOT read as
		// unresolved.
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()

		run1, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim1, _ := p25ClaimGate(t, r, tenant, run1.ID, "h", "tok")
		_, err = r.CompleteIncrementalSyncRun(ctx, run1.ID, tenant, "h", "tok",
			claim1.Run.Generation, IncrementalAdvance{CompletedCheckpoint: "cp-1",
				Rows: 1, Bytes: 1, ContentHash: sha256Hex("one"), TxnID: uuid.New()})
		require.NoError(t, err)

		run2, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		_, err = r.ClaimSyncRun(ctx, run2.ID, tenant, "h2", "tok2", time.Minute)
		require.NoError(t, err)

		view, err := r.GetCheckpointView(ctx, defID, tenant)
		require.NoError(t, err)
		require.NotNil(t, view)
		require.NotNil(t, view.Owner, "the second batch's owner is visible")
		assert.Equal(t, "running", view.Owner.RunStatus)
		assert.Empty(t, view.Owner.BlockedReason,
			"a live running owner is NOT unresolved just because a previous batch advanced")
		require.NotNil(t, view.LastConfirmed, "the prior confirmed batch stays queryable")
		assert.Equal(t, run1.ID, view.LastConfirmed.RunID)
	})
}

// T9 — an unresolved (retained) owner fences EVERY protected mutation
// surface regardless of the owned run's terminal state, while pure
// name/status/cron/enable edits stay allowed and never reset data position
// (release correction 3); the takeover backdoor stays closed for owned
// running work; the documented recovery is a NEW def on the same source.
func TestPGCheckpointUnknownOwnerRetainedAcrossMutations(t *testing.T) {
	// p25RetainedFailed drives a captured run to failed-with-retained-owner:
	// gate ENTRY is the conservative unknown boundary (release correction 1).
	p25RetainedFailed := func(t *testing.T) (*Repo, Tenant, uuid.UUID, uuid.UUID, uuid.UUID, *models.SyncRun) {
		ctx := context.Background()
		r, _, tenant, sourceID, defID := p25Fixture(t)
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, _ := p25ClaimGate(t, r, tenant, run.ID, "h", "tok")
		msg := "dispatch outcome unknown"
		done, err := r.FailIncrementalSyncRun(context.Background(), run.ID, tenant,
			"h", "tok", claim.Run.Generation, "failed", &msg, nil)
		require.NoError(t, err)
		require.NotNil(t, done)
		return r, tenant, sourceID, defID, run.ID, done
	}
	// p25FenceConfig returns a binding-relevant variant of the incremental
	// config (resource_path changed): a real input mutation under EITHER
	// reading of "actual change" (raw bytes or effective subset — F-6).
	p25FenceConfig := func() []byte {
		return []byte(strings.Replace(string(p25IncrementalConfig()),
			`"resource_path": "/items"`, `"resource_path": "/items2"`, 1))
	}

	t.Run("retained_owner_fences_every_protected_surface", func(t *testing.T) {
		r, tenant, sourceID, defID, runID, done := p25RetainedFailed(t)
		ctx := context.Background()
		assert.Equal(t, "failed", done.Status)
		cpBefore := p25CpJSON(t, r.Pool, defID)
		runBefore := runRowJSON(t, r.Pool, runID)

		// Source config change (typed PATCH).
		_, err := r.UpdateConnection(ctx, sourceID, tenant,
			&models.UpdateConnectionRequest{Config: p25FenceConfig()})
		assert.ErrorIs(t, err, ErrIncrementalOwnerUnresolved, "typed config change is fenced")

		// Bare helper (whole-config writer).
		_, err = r.UpdateConnectionConfig(ctx, sourceID, p25FenceConfig())
		assert.ErrorIs(t, err, ErrIncrementalOwnerUnresolved, "bare config helper is fenced")

		// Credential write.
		_, err = r.SetCredential(ctx, sourceID, tenant, "api_key",
			[]byte("rotation-ciphertext"), "fp-rotated")
		assert.ErrorIs(t, err, ErrIncrementalOwnerUnresolved, "SetCredential is fenced")

		// Def input mutations: retarget AND file_glob.
		newTarget := uuid.New()
		_, err = r.UpdateSyncJob(ctx, defID,
			&models.UpdateSyncJobRequest{OutputDatasetID: &newTarget}, tenant)
		assert.ErrorIs(t, err, ErrIncrementalOwnerUnresolved, "retarget is fenced")
		glob := "*.ndjson"
		_, err = r.UpdateSyncJob(ctx, defID,
			&models.UpdateSyncJobRequest{FileGlob: &glob}, tenant)
		assert.ErrorIs(t, err, ErrIncrementalOwnerUnresolved, "file_glob change is fenced")

		// Cascade delete.
		_, err = r.DeleteConnection(ctx, sourceID, tenant)
		assert.ErrorIs(t, err, ErrIncrementalOwnerUnresolved, "source delete is fenced")

		// Nothing moved: full-row equality on BOTH protected tables.
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID))
	})

	t.Run("pure_edits_allowed_and_never_reset_data_position", func(t *testing.T) {
		r, tenant, sourceID, defID, _, _ := p25RetainedFailed(t)
		ctx := context.Background()
		cpBefore := p25CpJSON(t, r.Pool, defID)
		revBefore := p24DefRevision(t, r.Pool, defID)

		name := "renamed-under-retained-owner"
		upd, err := r.UpdateConnection(ctx, sourceID, tenant,
			&models.UpdateConnectionRequest{Name: &name})
		require.NoError(t, err, "pure name edit is never fenced")
		require.NotNil(t, upd)

		cron := "*/5 * * * *"
		job, err := r.UpdateSyncJob(ctx, defID,
			&models.UpdateSyncJobRequest{ScheduleCron: &cron}, tenant)
		require.NoError(t, err, "pure cron edit is never fenced")
		require.NotNil(t, job)
		assert.Equal(t, revBefore+1, p24DefRevision(t, r.Pool, defID),
			"cron change still bumps schedule_revision (grant behavior unchanged)")
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID),
			"pure edits leave the data checkpoint byte-identical")

		// Same-value retarget: no fence, no bump.
		same := jobTarget(t, r, defID)
		_, err = r.UpdateSyncJob(ctx, defID,
			&models.UpdateSyncJobRequest{OutputDatasetID: &same}, tenant)
		require.NoError(t, err, "same-value retarget is not an input change")
		assert.Equal(t, revBefore+1, p24DefRevision(t, r.Pool, defID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
	})

	t.Run("expired_owned_running_run_is_not_takeoverable", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, err := r.ClaimSyncRun(ctx, run.ID, tenant, "h", "tok", 2*time.Second)
		require.NoError(t, err)
		require.NotNil(t, claim)

		p23WaitClockPast(t, r.Pool, p25LeaseExpiry(t, r.Pool, run.ID))
		runBefore := runRowJSON(t, r.Pool, run.ID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		// The storage takeover is a closed backdoor for owned P25 work (R13):
		// no re-execution, no adoption, zero writes.
		took, err := r.TakeoverExpiredSyncRun(ctx, run.ID, tenant, "new-owner", "new-token", time.Minute)
		require.NoError(t, err)
		assert.Nil(t, took, "an owned run is never taken over, even expired")
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, run.ID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
	})

	t.Run("concurrent_protected_writers_fence_without_deadlock", func(t *testing.T) {
		r, dsn, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, _ := p25ClaimGate(t, r, tenant, run.ID, "h", "tok")
		msg := "dispatch outcome unknown"
		_, err = r.FailIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok",
			claim.Run.Generation, "failed", &msg, nil)
		require.NoError(t, err)

		w1 := singleConnRepo(t, dsn)
		w2 := singleConnRepo(t, dsn)
		cfg := p25FenceConfig()
		op1 := p24Go(ctx, func(ctx context.Context) (*models.Connection, error) {
			return w1.UpdateConnection(ctx, sourceID, tenant,
				&models.UpdateConnectionRequest{Config: cfg})
		})
		op2 := p24Go(ctx, func(ctx context.Context) (*models.CredentialResponse, error) {
			return w2.SetCredential(ctx, sourceID, tenant, "api_key",
				[]byte("race-ciphertext"), "fp-race")
		})
		res1 := mustRecv(t, op1.done, "concurrent config writer")
		res2 := mustRecv(t, op2.done, "concurrent credential writer")
		assert.ErrorIs(t, res1.Err, ErrIncrementalOwnerUnresolved)
		assert.ErrorIs(t, res2.Err, ErrIncrementalOwnerUnresolved)
		// The retained owner is exactly where it was.
		cp := p25CpStateOf(t, r.Pool, defID)
		require.NotNil(t, cp.Owner)
	})

	t.Run("documented_recovery_is_a_new_def", func(t *testing.T) {
		r, tenant, sourceID, defID, _, _ := p25RetainedFailed(t)
		ctx := context.Background()

		// The fenced def stays blocked (P26), but a NEW def on the same
		// source admits into its OWN checkpoint row immediately.
		job2, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: sourceID, OutputDatasetID: uuid.New()}, tenant)
		require.NoError(t, err)
		require.NotNil(t, job2)
		run2, rejection, err := p25Admit(ctx, r, tenant, sourceID, job2.ID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		require.NotNil(t, run2)
		assert.Equal(t, 1, p25RunCount(t, r.Pool, defID), "the fenced def keeps exactly its one run")
		assert.Equal(t, 1, p25RunCount(t, r.Pool, job2.ID))
	})
}

// T11 — scheduled incremental admission: LOGICAL owner-busy records a
// truthful skipped tick and advances the CRON cursor WITHOUT a run, WITHOUT
// the Source admission stamp and WITHOUT touching the DATA checkpoint;
// binding-stale is its own truthful outcome; PHYSICAL checkpoint-row
// contention is a contention SKIP with zero durable effects (never a tick
// on the aborted transaction).
func TestPGCheckpointScheduledBusyLogicalVsContention(t *testing.T) {
	// p25ScheduledFixture: the fixture's EXISTING incremental def with its
	// schedule enabled (a pure cron/enable edit — never fenced) and made
	// due, plus a live armed grant.
	p25ScheduledFixture := func(t *testing.T) (*Repo, string, Tenant, uuid.UUID, uuid.UUID, time.Time, *ScheduleArmClaim) {
		r, dsn, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		cron, tz, enabled := "*/1 * * * *", "UTC", true
		_, err := r.UpdateSyncJob(ctx, defID,
			&models.UpdateSyncJobRequest{ScheduleCron: &cron, ScheduleTimeZone: &tz, ScheduleEnabled: &enabled},
			tenant)
		require.NoError(t, err, "pure schedule edit on an owner-free def is allowed")
		// Force the slot due (P24 pattern: direct cursor move to the past).
		_, err = r.Pool.Exec(ctx,
			`UPDATE batch_sync_defs SET schedule_next_fire_at = clock_timestamp() - interval '5 minutes' WHERE id = $1`, defID)
		require.NoError(t, err)
		arm, view := p24Arm(t, r, tenant, defID, sourceID, jobTarget(t, r, defID), dbClock(t, r.Pool).Add(10*time.Minute))
		require.NotNil(t, view)
		scheduledFor := *p24Cursor(t, r.Pool, defID)
		return r, dsn, tenant, sourceID, defID, scheduledFor, arm
	}
	p25ClaimScheduled := func(t *testing.T, r *Repo, arm *ScheduleArmClaim, defID uuid.UUID, scheduledFor time.Time) (*ScheduleClaimResult, error) {
		t.Helper()
		cand := ScheduleCandidate{
			SyncDefID:    defID,
			SourceID:     arm.SourceID,
			Revision:     p24DefRevision(t, r.Pool, defID),
			ScheduledFor: scheduledFor,
		}
		return r.ClaimDueScheduleSlot(context.Background(), cand, arm)
	}
	p25TickCount := func(t *testing.T, r *Repo, defID uuid.UUID) int {
		t.Helper()
		var n int
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT COUNT(*) FROM sync_schedule_ticks WHERE sync_def_id = $1`, defID).Scan(&n))
		return n
	}

	t.Run("enqueued_positive_triple_uuid_identity", func(t *testing.T) {
		// review-source-round2 #2: a TRUE enqueued scheduled admission — the
		// tick's run_id, the RETURNED run's id and the checkpoint owner must
		// be ONE uuid (the double-ID bug writes an orphan outer id into the
		// tick and the FK/owner chain breaks).
		r, _, tenant, _, defID, scheduledFor, arm := p25ScheduledFixture(t)
		ctx := context.Background()
		// review-pg-code-round2 #3: the fresh source's stamp is NULL before
		// the first success — assert the nullable truth, never require it.
		stampBefore := p23Stamp(t, r.Pool, arm.SourceID)
		require.Nil(t, stampBefore, "a fresh source has no admission stamp yet")

		res, err := p25ClaimScheduled(t, r, arm, defID, scheduledFor)
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.True(t, res.Claimed)
		assert.Equal(t, TickOutcomeEnqueued, res.Outcome)
		require.NotNil(t, res.Run, "the scheduled path CREATES the incremental run")

		var tickRunID uuid.UUID
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT run_id FROM sync_schedule_ticks WHERE sync_def_id = $1`, defID).Scan(&tickRunID))
		cp := p25CpStateOf(t, r.Pool, defID)
		require.NotNil(t, cp.Owner, "the new run owns the checkpoint")
		assert.Equal(t, res.Run.ID, tickRunID, "tick.run_id == returned run id")
		assert.Equal(t, res.Run.ID, *cp.Owner, "checkpoint owner == returned run id")

		pre, err := r.runInternalPreRead(ctx, res.Run.ID, tenant)
		require.NoError(t, err)
		require.NotNil(t, pre)
		assert.Equal(t, CPModeCursor, pre.CPMode, "the scheduled capture is frozen")
		stampAfter := p23Stamp(t, r.Pool, arm.SourceID)
		require.NotNil(t, stampAfter, "the successful enqueue stamps admission")
	})

	t.Run("logical_busy_truthful_tick_cron_advances_data_frozen", func(t *testing.T) {
		ctx := context.Background()
		r, _, tenant, _, defID, scheduledFor, arm := p25ScheduledFixture(t)

		// A durable owner exists with NO executor lock held.
		ownerRun, rejection, err := p25Admit(ctx, r, tenant, arm.SourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		require.NotNil(t, ownerRun)
		stamp := p23MustStamp(t, r.Pool, arm.SourceID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		res, err := p25ClaimScheduled(t, r, arm, defID, scheduledFor)
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.True(t, res.Claimed)
		assert.Equal(t, TickOutcomeSkippedIncrementalBusy, res.Outcome)
		assert.Nil(t, res.Run, "busy creates NO run")

		// Truthful tick + cron cursor advanced …
		require.Equal(t, 1, p25TickCount(t, r, defID))
		var tickOutcome, tickReason string
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT outcome, COALESCE(reason,'') FROM sync_schedule_ticks WHERE sync_def_id = $1`, defID).
			Scan(&tickOutcome, &tickReason))
		assert.Equal(t, string(TickOutcomeSkippedIncrementalBusy), tickOutcome)
		assert.Equal(t, "incremental_busy", tickReason)
		cursor := p24Cursor(t, r.Pool, defID)
		require.NotNil(t, cursor)
		assert.True(t, cursor.After(dbClock(t, r.Pool).Add(-time.Second)),
			"the CRON cursor advanced to a future fire")

		// … while the DATA checkpoint and the admission stamp are untouched.
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
		p23AssertStampUnchanged(t, r.Pool, arm.SourceID, stamp, "busy consumes no admission stamp")
		assert.Equal(t, 1, p25RunCount(t, r.Pool, defID), "exactly the owner's run exists")
	})

	t.Run("binding_stale_is_its_own_truthful_outcome", func(t *testing.T) {
		r, _, tenant, _, defID, scheduledFor, arm := p25ScheduledFixture(t)
		ctx := context.Background()

		// Free owner, stale stored fingerprint.
		run, rejection, err := p25Admit(ctx, r, tenant, arm.SourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		by := uuid.New()
		res, err := r.RequestCancelSyncRun(ctx, run.ID, defID, tenant, &by, "free the owner")
		require.NoError(t, err)
		require.Equal(t, CancelOutcomeAborted, res.Outcome)
		_, err = r.Pool.Exec(ctx,
			`UPDATE sync_def_checkpoints SET binding_fingerprint = 'stale-beef' WHERE sync_def_id = $1`, defID)
		require.NoError(t, err)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		res2, err := p25ClaimScheduled(t, r, arm, defID, scheduledFor)
		require.NoError(t, err)
		require.NotNil(t, res2)
		assert.True(t, res2.Claimed)
		assert.Equal(t, TickOutcomeErrorBindingStale, res2.Outcome)
		assert.Nil(t, res2.Run)
		assert.Equal(t, 1, p25TickCount(t, r, defID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID),
			"the stale outcome never resets the stored binding")
		// review-pg-code-round2 #3: this subtest already created and ABORTED
		// one run to free the owner — that row IS the baseline. The stale
		// outcome creates no NEW run beyond it.
		assert.Equal(t, 1, p25RunCount(t, r.Pool, defID),
			"no NEW run beyond the aborted baseline row")
	})

	t.Run("physical_contention_is_a_skip_with_zero_durable_effects", func(t *testing.T) {
		r, dsn, tenant, _, defID, scheduledFor, arm := p25ScheduledFixture(t)
		ctx := context.Background()

		// Existing row, free owner, row lock held by a pure blocker.
		run, rejection, err := p25Admit(ctx, r, tenant, arm.SourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		by := uuid.New()
		res, err := r.RequestCancelSyncRun(ctx, run.ID, defID, tenant, &by, "free the owner")
		require.NoError(t, err)
		require.Equal(t, CancelOutcomeAborted, res.Outcome)
		blocker := singleConnRepo(t, dsn)
		btx, err := blocker.Pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = p25BoundedRollback(btx) }()
		var locked uuid.UUID
		require.NoError(t, btx.QueryRow(ctx,
			`SELECT sync_def_id FROM sync_def_checkpoints WHERE sync_def_id = $1 FOR UPDATE`,
			defID).Scan(&locked))
		cpBefore := p25CpJSON(t, r.Pool, defID)
		cursorBefore := p24Cursor(t, r.Pool, defID)
		stamp := p23MustStamp(t, r.Pool, arm.SourceID)

		res2, err := p25ClaimScheduled(t, r, arm, defID, scheduledFor)
		require.NoError(t, err)
		require.NotNil(t, res2)
		assert.False(t, res2.Claimed, "contention never claims")
		assert.Equal(t, ScheduleSkipContention, res2.Skip)
		assert.Zero(t, p25TickCount(t, r, defID),
			"NO tick on the aborted transaction (root Q7 ruling)")
		assert.Nil(t, res2.Run)
		assert.Equal(t, cursorBefore, p24Cursor(t, r.Pool, defID), "cursor untouched")
		p23AssertStampUnchanged(t, r.Pool, arm.SourceID, stamp, "contention consumes no stamp")
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
	})
}

// T13 — legacy and old6 compatibility: pre-P2.5 unknown/terminal rows never
// gain checkpoint state, survive replay byte-for-byte and are NOT adopted;
// the legacy non-opt-in lifecycle is byte-compatible (no checkpoint row, no
// cp_* capture, legacy concurrency and takeover semantics intact).
func TestPGCheckpointLegacySnapshotAndOld6Compat(t *testing.T) {
	t.Run("old6_shapes_survive_replay_unadopted_and_legacy_takeover_intact", func(t *testing.T) {
		r := bootRepo(t)
		ctx := context.Background()
		skipped := p24ApplyMigrationsExcept(t, r.Pool, "sync_def_checkpoints")
		require.Len(t, skipped, 1)

		tenant := Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}
		conn, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
			Name: "old6", ConnectorType: "rest_api",
			Config: []byte(`{"domain":"old6.example.com","auth":{"type":"none"},
				"runtime":{"worker":"foundry"},"permissions":{"syncable":true},
				"sync":{"resource_path":"/x","records_path":"items"}}`),
		}, tenant)
		require.NoError(t, err)
		require.NotNil(t, conn)
		job, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: conn.ID, OutputDatasetID: uuid.New()}, tenant)
		require.NoError(t, err)
		require.NotNil(t, job)

		// The sealed old6 are ALL RUNNING (review-pg-code-round2 #6) — the
		// P24 controlled cohort is live work with varied lease/gate shapes:
		// two expired-lease (B lease-loss), two live-lease (C
		// committed-unconfirmed), two gate-entered. Ordinary TERMINAL
		// compatibility is proven separately (baseline + legacy lifecycle)
		// and is NOT claimed as old6 pin coverage.
		seed := func(holder, token *string, expires *time.Time, gateEntered bool) uuid.UUID {
			id := uuid.New()
			var gateCol, gateVal string
			if gateEntered {
				gateCol, gateVal = ", publish_entered_at", ", clock_timestamp() - interval '30 minutes'"
			}
			_, err := r.Pool.Exec(ctx, `
				INSERT INTO sync_runs (id, sync_def_id, status, org_id, owner_id, control_mode,
				                       started_at, attempt, generation,
				                       lease_holder, lease_token, lease_expires_at, heartbeat_at`+gateCol+`)
				VALUES ($1, $2, 'running', $3, $4, 'controlled',
				        clock_timestamp() - interval '1 hour', 1, 1,
				        $5, $6, $7, clock_timestamp() - interval '1 hour'`+gateVal+`)`,
				id, job.ID, tenant.OrgID, tenant.OwnerID, holder, token, expires)
			require.NoError(t, err)
			return id
		}
		old6 := []uuid.UUID{
			// B-shape: lease-loss running — EXPIRED material still attached
			// (attempt/generation seeded at 1: a legacy takeover must land
			// on exactly attempt 2 / generation 2).
			seed(p25StrPtr("holder-b1"), p25StrPtr("token-b1"),
				p25TimePtr(dbClock(t, r.Pool).Add(-time.Minute)), false),
			seed(p25StrPtr("holder-b2"), p25StrPtr("token-b2"),
				p25TimePtr(dbClock(t, r.Pool).Add(-2*time.Minute)), false),
			// C-shape: committed-unconfirmed running — LIVE material.
			seed(p25StrPtr("holder-c1"), p25StrPtr("token-c1"),
				p25TimePtr(dbClock(t, r.Pool).Add(time.Hour)), false),
			seed(p25StrPtr("holder-c2"), p25StrPtr("token-c2"),
				p25TimePtr(dbClock(t, r.Pool).Add(2*time.Hour)), false),
			// Gate-entered running (dispatch-uncertain stage).
			seed(p25StrPtr("holder-g1"), p25StrPtr("token-g1"),
				p25TimePtr(dbClock(t, r.Pool).Add(time.Hour)), true),
			seed(p25StrPtr("holder-g2"), p25StrPtr("token-g2"),
				p25TimePtr(dbClock(t, r.Pool).Add(time.Hour)), true),
		}

		require.NoError(t, Migrate(ctx, r.Pool))
		var cpRows int
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM sync_def_checkpoints`).Scan(&cpRows))
		assert.Zero(t, cpRows, "old6 unknowns are never adopted or backfilled")

		for _, id := range old6 {
			pre, err := r.runInternalPreRead(ctx, id, tenant)
			require.NoError(t, err)
			require.NotNil(t, pre)
			assert.Empty(t, pre.CPMode, "old6 work is never captured P25 work")
		}

		// Replay: whole rows byte-equal on the settled schema.
		before := make([]string, len(old6))
		for i, id := range old6 {
			before[i] = runRowJSON(t, r.Pool, id)
		}
		require.NoError(t, Migrate(ctx, r.Pool))
		for i, id := range old6 {
			assert.Equal(t, before[i], runRowJSON(t, r.Pool, id), "old6 row %d byte-identical across replay", i)
		}

		// LEGACY takeover stays available for the non-owned expired B-shape
		// (the guard closes only OWNED P25 work, never legacy rows). The
		// seed carries attempt/generation 1, so the takeover lands on 2/2.
		took, err := r.TakeoverExpiredSyncRun(ctx, old6[0], tenant, "legacy-taker", "legacy-token", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, took, "legacy lease-loss takeover semantics are unchanged")
		assert.Equal(t, 2, took.Run.Attempt, "seeded attempt 1 ⇒ takeover attempt 2")
		assert.Equal(t, 2, took.Run.Generation, "seeded generation 1 ⇒ takeover generation 2")
	})

	t.Run("legacy_lifecycle_never_touches_checkpoint_state", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()

		// Replace the incremental opt-in with a plain legacy config: the
		// def keeps its identity but opts out (mode-off never bypasses —
		// here it simply has no owner yet, so the fence allows the change).
		legacyCfg := []byte(`{"domain":"legacy.example.com","auth":{"type":"none"},
			"runtime":{"worker":"foundry"},"permissions":{"syncable":true},
			"sync":{"resource_path":"/x","records_path":"items"}}`)
		_, err := r.UpdateConnection(ctx, sourceID, tenant,
			&models.UpdateConnectionRequest{Config: legacyCfg})
		require.NoError(t, err)

		// Legacy concurrency: TWO pending runs coexist (no per-Def
		// serialization without the opt-in).
		run1, err := r.EnqueueSyncRun(ctx, defID, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, run1)
		run2, err := r.EnqueueSyncRun(ctx, defID, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, run2)

		// Legacy lifecycle through the NON-routed predicates.
		claim, err := r.ClaimSyncRun(ctx, run1.ID, tenant, "lh", "lt", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		assert.Empty(t, claim.Run.CPMode, "legacy runs carry no cp capture")
		gate, err := r.BeginPublishGate(ctx, run1.ID, tenant, "lh", "lt", claim.Run.Generation, time.Minute)
		require.NoError(t, err)
		require.Equal(t, PublishGateEntered, gate.Outcome)
		version := uuid.New()
		done, err := r.CompleteLeasedSyncRun(ctx, run1.ID, tenant, "lh", "lt",
			claim.Run.Generation, "succeeded", 10, 1, 2, nil, nil, &version, nil)
		require.NoError(t, err)
		require.NotNil(t, done)
		assert.Equal(t, "succeeded", done.Status)

		by := uuid.New()
		res, err := r.RequestCancelSyncRun(ctx, run2.ID, defID, tenant, &by, "legacy cancel")
		require.NoError(t, err)
		assert.Equal(t, CancelOutcomeAborted, res.Outcome)

		var cpRows int
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM sync_def_checkpoints`).Scan(&cpRows))
		assert.Zero(t, cpRows, "the legacy lifecycle never creates checkpoint state")
	})

	t.Run("legacy_runsyncjob_cannot_bypass_checkpoint_owner", func(t *testing.T) {
		// review-source-round1 #4: the legacy unleased task creator is a
		// closed backdoor for opt-in incremental work — typed rejection,
		// zero new runs, zero stamp; a NON-opt-in legacy source keeps the
		// exact pre-P25 behavior (including the {}-config regression from
		// review-repo-round1 #9: blanket REST validation must not break
		// old empty-config fixtures).
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()

		// Incremental def with a retained owner.
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, _ := p25ClaimGate(t, r, tenant, run.ID, "h", "tok")
		msg := "unknown dispatch"
		_, err = r.FailIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok",
			claim.Run.Generation, "failed", &msg, nil)
		require.NoError(t, err)
		stamp := p23MustStamp(t, r.Pool, sourceID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		legacyRun, err := r.RunSyncJob(ctx, defID, tenant)
		require.Error(t, err, "the opt-in def refuses the legacy unleased path")
		assert.ErrorIs(t, err, ErrIncrementalRequiresControlled)
		assert.Nil(t, legacyRun)
		assert.Equal(t, 1, p25RunCount(t, r.Pool, defID), "no bypass run is created")
		p23AssertStampUnchanged(t, r.Pool, sourceID, stamp, "no admission stamp on the refusal")
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))

		// Legacy {}-config source: BOTH entrypoints keep exact old behavior.
		legacyConn, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
			Name: "empty-config-legacy", ConnectorType: "rest_api"}, tenant)
		require.NoError(t, err)
		require.NotNil(t, legacyConn)
		legacyDef, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: legacyConn.ID, OutputDatasetID: uuid.New()}, tenant)
		require.NoError(t, err)
		require.NotNil(t, legacyDef)

		enqueued, err := r.EnqueueSyncRun(ctx, legacyDef.ID, legacyConn.ID, tenant, uuid.New())
		require.NoError(t, err, "legacy {}-config enqueue is unchanged")
		require.NotNil(t, enqueued)

		ran, err := r.RunSyncJob(ctx, legacyDef.ID, tenant)
		require.NoError(t, err, "legacy {}-config RunSyncJob is unchanged")
		require.NotNil(t, ran)
		assert.Equal(t, "running", ran.Status)
		assert.Equal(t, "legacy", ran.ControlMode, "the migration DEFAULT is 'legacy' (review-source-round2)")
		assert.Empty(t, ran.CPMode, "legacy work is never captured P25 work")
		var legacyCP int
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM sync_def_checkpoints WHERE sync_def_id = $1`, legacyDef.ID).
			Scan(&legacyCP))
		assert.Zero(t, legacyCP)
	})
}

func p25StrPtr(v string) *string       { return &v }
func p25TimePtr(v time.Time) *time.Time { return &v }

// T12 — advance-side candidate validation and the empty-batch transition:
// invalid opaque candidates are refused with ZERO writes (the publish-side
// fetch errors are runtime-unit territory); a confirmed empty batch (same
// completed token) advances the epoch while keeping the token.
func TestPGCheckpointCandidateValidationTransitions(t *testing.T) {
	t.Run("invalid_candidates_refused_zero_write", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, _ := p25ClaimGate(t, r, tenant, run.ID, "h", "tok")
		runBefore := runRowJSON(t, r.Pool, run.ID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		// Every OTHER parameter stays legal so ONLY the candidate refuses
		// (review-pg-code-round1 #2: no parameter error can mask the case).
		for _, bad := range []string{"", strings.Repeat("a", 513), "ab\x01cd", "line\nbreak"} {
			_, err := r.CompleteIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok",
				claim.Run.Generation, IncrementalAdvance{CompletedCheckpoint: bad,
					Rows: 1, Bytes: 1, ContentHash: sha256Hex("p25-candidate-ctl"), TxnID: uuid.New()})
			require.Error(t, err, "candidate %q must be refused", bad)
			assert.Contains(t, err.Error(), "completed checkpoint",
				"the refusal names its own category (candidate), got: %v", err)
		}
		assert.Equal(t, "running", p25RunStatus(t, r.Pool, run.ID))
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, run.ID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))

		// Valid-candidate CONTROL: with everything legal the SAME call
		// succeeds — proving the refusals above were the candidate, not a
		// broken fixture (review-pg-code-round1 #2).
		done, err := r.CompleteIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok",
			claim.Run.Generation, IncrementalAdvance{CompletedCheckpoint: "cp-valid-ctl",
				Rows: 1, Bytes: 1, ContentHash: sha256Hex("p25-candidate-ctl"), TxnID: uuid.New()})
		require.NoError(t, err)
		require.NotNil(t, done)
		assert.Equal(t, "succeeded", done.Status)
		assert.Equal(t, 1, p25CpStateOf(t, r.Pool, defID).Epoch)
	})

	t.Run("bad_txn_counts_hash_refused_before_locks", func(t *testing.T) {
		// review-repo-round1 #4: nil txn, negative counts and a non-hex
		// content hash are rejected BEFORE any lock/write — zero durable
		// effects, the run stays running.
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()
		run, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		claim, _ := p25ClaimGate(t, r, tenant, run.ID, "h", "tok")
		runBefore := runRowJSON(t, r.Pool, run.ID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		bad := []struct {
			label    string
			contains string // the ACTUAL frozen business error class (review-pg-code-round2 #5)
			adv      IncrementalAdvance
		}{
			{"nil txn", "transaction id", IncrementalAdvance{CompletedCheckpoint: "cp-ok", Rows: 1, Bytes: 1,
				ContentHash: sha256Hex("p25-badtxn-ctl"), TxnID: uuid.Nil}},
			{"negative rows", "negative batch facts", IncrementalAdvance{CompletedCheckpoint: "cp-ok", Rows: -1, Bytes: 1,
				ContentHash: sha256Hex("p25-badtxn-ctl"), TxnID: uuid.New()}},
			{"negative bytes", "negative batch facts", IncrementalAdvance{CompletedCheckpoint: "cp-ok", Rows: 1, Bytes: -5,
				ContentHash: sha256Hex("p25-badtxn-ctl"), TxnID: uuid.New()}},
			{"non-hex hash", "content hash", IncrementalAdvance{CompletedCheckpoint: "cp-ok", Rows: 1, Bytes: 1,
				ContentHash: "not-a-hash", TxnID: uuid.New()}},
		}
		for _, probe := range bad {
			done, err := r.CompleteIncrementalSyncRun(ctx, run.ID, tenant, "h", "tok",
				claim.Run.Generation, probe.adv)
			require.Error(t, err, "%s must be refused", probe.label)
			assert.Nil(t, done)
			assert.Contains(t, err.Error(), probe.contains,
				"the refusal names its own category (%s), got: %v", probe.label, err)
		}
		assert.Equal(t, "running", p25RunStatus(t, r.Pool, run.ID))
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, run.ID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
	})

	t.Run("empty_batch_same_token_advances_epoch", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p25Fixture(t)
		ctx := context.Background()

		// Batch 1 confirms token "cp-token-1" (epoch 0 -> 1).
		run1, _, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		claim1, _ := p25ClaimGate(t, r, tenant, run1.ID, "h", "tok")
		_, err = r.CompleteIncrementalSyncRun(ctx, run1.ID, tenant, "h", "tok",
			claim1.Run.Generation, IncrementalAdvance{CompletedCheckpoint: "cp-token-1",
				Rows: 0, Bytes: 0, ContentHash: sha256Hex(""), TxnID: uuid.New()})
		require.NoError(t, err)

		// Batch 2 starts from the confirmed token and confirms the SAME
		// token again (empty batch): epoch advances, value stays.
		run2, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, err)
		require.Nil(t, rejection)
		pre, err := r.runInternalPreRead(ctx, run2.ID, tenant)
		require.NoError(t, err)
		require.NotNil(t, pre)
		assert.Equal(t, 1, pre.CPBaseEpoch, "the new run freezes the confirmed epoch")
		assert.Equal(t, "cp-token-1", pre.CPBaseValue, "the new run freezes the confirmed token")
		claim2, _ := p25ClaimGate(t, r, tenant, run2.ID, "h2", "tok2")
		_, err = r.CompleteIncrementalSyncRun(ctx, run2.ID, tenant, "h2", "tok2",
			claim2.Run.Generation, IncrementalAdvance{CompletedCheckpoint: "cp-token-1",
				Rows: 0, Bytes: 0, ContentHash: sha256Hex(""), TxnID: uuid.New()})
		require.NoError(t, err, "same-token reconfirm (empty batch) is the R14 transition")

		cp := p25CpStateOf(t, r.Pool, defID)
		require.NotNil(t, cp.Value)
		assert.Equal(t, "cp-token-1", *cp.Value)
		assert.Equal(t, 2, cp.Epoch)
		assert.Nil(t, cp.Owner)
	})
}
