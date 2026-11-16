//go:build integration

// P2.6 幂等发布与提交恢复 — real PostgreSQL 16 integration tests
// (SOURCE-ONLY stage: compiled, NOT yet executed — real-PG execution is a
// separately reviewed command category; nothing here fakes PG semantics).
//
// Authority: root-decisions-v2.md v2/v2.1/v2.2 directions + the W2 contract
// (docs/architecture/business-platform-p2-6-contract.md). Fixtures borrow the
// P25 harness verbatim (bootRepoWithDSN / leaseFixture family / singleConnRepo
// / backendPID / waitUntilBlockedBy / waitForDBExpiry / row-JSON snapshots) —
// no P25 test file or semantic is modified.
//
// Anti-fake discipline (inherited from review-pg-plan-round1): full-row
// zero-change snapshots include every timestamp; contention is proven through
// pg_stat_activity/pg_blocking_pids, not timeouts; rollback is proven with
// per-session marker-RAISE triggers on the TEST-OWNED fresh database; every
// negative that matters carries a positive control.
package repo

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// ---------------------------------------------------------------------------
// P26 fixtures (own configs; P25 helpers reused untouched)
// ---------------------------------------------------------------------------

// p26SnapshotConfig is a rest_api source with the recovery opt-in and NO
// incremental block — the v2.1 snapshot-protocol shape.
func p26SnapshotConfig() []byte {
	return []byte(`{
		"domain": "snap.example.com",
		"auth": {"type": "none"},
		"runtime": {"worker": "foundry"},
		"permissions": {"syncable": true},
		"sync": {
			"resource_path": "/snap",
			"records_path": "items",
			"pagination": {"type": "page"},
			"recovery": {"enabled": true}
		}
	}`)
}

// p26IncrementalConfig is the P25 incremental config plus the recovery
// opt-in — the v1 incremental shape.
func p26IncrementalConfig() []byte {
	return []byte(`{
		"domain": "inc.example.com",
		"auth": {"type": "none"},
		"runtime": {"worker": "foundry"},
		"permissions": {"syncable": true},
		"sync": {
			"resource_path": "/items",
			"records_path": "items",
			"pagination": {"type": "cursor", "cursor_param": "cursor_token", "next_cursor_path": "next_cursor"},
			"incremental": {"enabled": true, "request_param": "since_token", "completed_checkpoint_path": "completed"},
			"recovery": {"enabled": true}
		}
	}`)
}

// p26Fixture boots a fresh migrated org-tenant database with one source+def
// carrying the given (already typed-validated) config.
func p26Fixture(t *testing.T, config []byte) (*Repo, string, Tenant, uuid.UUID, uuid.UUID) {
	t.Helper()
	r, dsn := bootRepoWithDSN(t)
	ctx := context.Background()
	require.NoError(t, Migrate(ctx, r.Pool))
	tenant := Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}
	conn, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
		Name: "p26-source", ConnectorType: "rest_api", Config: config,
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

// p26IntentJSON snapshots the COMPLETE sync_run_publish_intents row (every
// column and timestamp — no key subtraction).
func p26IntentJSON(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) string {
	t.Helper()
	var js string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT row_to_json(i.*)::text FROM sync_run_publish_intents i WHERE i.run_id = $1`, runID).Scan(&js))
	return js
}

// p26CredJSON snapshots one source_credentials row.
func p26CredJSON(t *testing.T, pool *pgxpool.Pool, sourceID uuid.UUID, kind string) string {
	t.Helper()
	var js string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT row_to_json(c.*)::text FROM source_credentials c
		  WHERE c.source_id = $1 AND c.kind = $2`, sourceID, kind).Scan(&js))
	return js
}

// p26Claim performs the ordinary pending→running claim (the P25 statement).
func p26Claim(t *testing.T, r *Repo, runID uuid.UUID, tenant Tenant, ttl time.Duration) *LeaseClaim {
	t.Helper()
	claim, err := r.ClaimSyncRun(context.Background(), runID, tenant, "p26-exec", "p26-exec-token", ttl)
	require.NoError(t, err)
	require.NotNil(t, claim)
	return claim
}

// p26Gate builds a valid frozen material for the run's mode and drives the
// recoverable gate to Entered.
func p26Gate(t *testing.T, r *Repo, runID uuid.UUID, tenant Tenant, sourceID, defID uuid.UUID, lease *LeaseClaim) *LeaseClaim {
	t.Helper()
	var mat FrozenPublishMaterial
	if lease.Run.CPMode != "" {
		cand := "p26-cpc-" + runID.String()[:8]
		mat = material(t, matIDs{runID: runID, sourceID: sourceID, defID: defID}, "cursor", &cand, []byte("{\"p\":1}\n"), 1)
	} else {
		mat = material(t, matIDs{runID: runID, sourceID: sourceID, defID: defID}, "", nil, []byte("{\"p\":1}\n"), 1)
	}
	res, err := r.BeginRecoverablePublishGate(context.Background(), lease, tenant, mat)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, IncrementalPublishGateEntered, res.Outcome)
	require.NotNil(t, res.Claim)
	return res.Claim
}

// p26ForceExpire moves the run's lease into the past on the DATABASE clock.
// Fixture manipulation on the test-owned fresh database: it only ever
// simulates the passage of time for a lease nobody renews (the natural wait
// would be the gate's full 60s renewal), and every later assertion targets
// rows read after this point.
func p26ForceExpire(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) {
	t.Helper()
	tag, err := pool.Exec(context.Background(),
		`UPDATE sync_runs SET lease_expires_at = clock_timestamp() - interval '1 millisecond'
		  WHERE id = $1`, runID)
	require.NoError(t, err)
	require.Equal(t, int64(1), tag.RowsAffected())
}

// p26IndependentFixture boots a fresh migrated incremental-v1 fixture
// (own database) for subtests that must not share the enclosing test's
// checkpoint-owning def.
func p26IndependentFixture(t *testing.T) (*Repo, Tenant, uuid.UUID, uuid.UUID) {
	t.Helper()
	r, _, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
	return r, tenant, sourceID, defID
}

// p26NameSession tags the single-connection pool's session with an
// application_name. First real-PG run lesson (W6 test-connector-normal
// 04:25Z): the backendPID probe can observe a DIFFERENT pooled connection
// than the one that later performs the blocking statement — waiter identity
// must be discovered from pg_stat_activity by name, not by a pre-probed pid.
func p26NameSession(t *testing.T, r *Repo, name string) {
	t.Helper()
	_, err := r.Pool.Exec(context.Background(), `SELECT set_config('application_name', $1, false)`, name)
	require.NoError(t, err)
}

// p26WaitDirectBlocked polls (bounded) until the named session is in a Lock
// wait whose pg_blocking_pids DIRECTLY contains the barrier root. Used for
// the FIRST contender: a direct wait on the barrier is unambiguous.
func p26WaitDirectBlocked(t *testing.T, observer *Repo, session string, root int32) int32 {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var pid int32
		var blockers []int32
		err := observer.Pool.QueryRow(context.Background(),
			`SELECT pid, pg_blocking_pids(pid) FROM pg_stat_activity
			  WHERE application_name = $1 AND wait_event_type = 'Lock'`, session).Scan(&pid, &blockers)
		if err == nil && containsPID(blockers, root) {
			t.Logf("direct-block proof: session %q pid=%d directly blocked by root %d (blockers=%v)",
				session, pid, root, blockers)
			return pid
		}
		if time.Now().After(deadline) {
			t.Fatalf("session %q never DIRECTLY waited on barrier %d", session, root)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// p26WaitChainToRoot polls (bounded) until the named session is in a Lock
// wait and its ACTUAL finite blocking chain — pg_blocking_pids hard AND soft
// (wait-queue) blockers, expanded recursively — reaches the SAME root as the
// first contender. The observed chain is logged so the run raw records the
// real queue evidence (review-w2-pg-attempt1: PG16 soft blockers mean the
// second waiter may queue behind the first; the chain, not a direct edge,
// is its proof).
func p26WaitChainToRoot(t *testing.T, observer *Repo, session string, root int32) []int32 {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var pid int32
		var blockers []int32
		err := observer.Pool.QueryRow(context.Background(),
			`SELECT pid, pg_blocking_pids(pid) FROM pg_stat_activity
			  WHERE application_name = $1 AND wait_event_type = 'Lock'`, session).Scan(&pid, &blockers)
		if err == nil {
			chain := p26ExpandChain(observer, blockers, root, 4)
			if chain != nil {
				t.Logf("chain-block proof: session %q pid=%d chain %v reaches root %d",
					session, pid, chain, root)
				return chain
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("session %q never had a finite blocking chain to root %d", session, root)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// p26StillBlocked is the pre-release re-check: the session is STILL in a
// Lock wait whose chain still reaches the root — not merely any unrelated
// Lock wait. Single-shot, no polling.
func p26StillBlocked(t *testing.T, observer *Repo, session string, root int32) bool {
	t.Helper()
	var pid int32
	var blockers []int32
	err := observer.Pool.QueryRow(context.Background(),
		`SELECT pid, pg_blocking_pids(pid) FROM pg_stat_activity
		  WHERE application_name = $1 AND wait_event_type = 'Lock'`, session).Scan(&pid, &blockers)
	if err != nil {
		return false
	}
	return p26ExpandChain(observer, blockers, root, 4) != nil
}

// p26ExpandChain walks pg_blocking_pids (hard + soft wait-queue blockers)
// recursively, bounded by depth, and returns the observed pid chain to the
// target — or nil when it does not reach.
func p26ExpandChain(observer *Repo, blockers []int32, target int32, depth int) []int32 {
	for _, b := range blockers {
		if b == target {
			return []int32{target}
		}
	}
	if depth <= 0 {
		return nil
	}
	for _, b := range blockers {
		var next []int32
		err := observer.Pool.QueryRow(context.Background(),
			`SELECT pg_blocking_pids($1::int)`, b).Scan(&next)
		if err != nil {
			continue
		}
		if sub := p26ExpandChain(observer, next, target, depth-1); sub != nil {
			return append([]int32{b}, sub...)
		}
	}
	return nil
}

func containsPID(pids []int32, want int32) bool {
	for _, p := range pids {
		if p == want {
			return true
		}
	}
	return false
}

// p26RecoveryClaim performs a ClaimRecovery and requires the granted outcome.
func p26RecoveryClaim(t *testing.T, r *Repo, runID uuid.UUID, tenant Tenant) (*LeaseClaim, *RecoveryClaimResult) {
	t.Helper()
	res, err := r.ClaimRecovery(context.Background(), runID, tenant, "p26-rec", "p26-rec-token-"+uuid.NewString()[:8], time.Minute)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, RecoveryClaimGranted, res.Outcome)
	require.NotNil(t, res.Claim)
	return res.Claim, res
}

// p26ReceiptFor builds the exact-matching receipt of an intent.
func p26ReceiptFor(m PublishIntentMaterial) PublishReceipt {
	return PublishReceipt{
		IdempotencyKey:      m.PublishKey,
		RequestFingerprint:  m.RequestFingerprint,
		DatasetID:           m.OutputDatasetID,
		Branch:              m.Branch,
		TransactionID:       uuid.New(),
		Status:              "COMMITTED",
		MetadataContentHash: m.ContentHash,
		MetadataSyncRunID:   m.RunID,
		MetadataSourceID:    m.SourceID,
		MetadataSyncDefID:   m.SyncDefID,
	}
}

// p26MarkerTrigger installs a per-session marker-RAISE trigger on a table:
// UPDATEs on that table fail ONLY on sessions that set `p26.<marker>='1'`
// (P25's proven local-rollback probe shape, test-owned database only).
func p26MarkerTrigger(t *testing.T, pool *pgxpool.Pool, table, marker string) {
	t.Helper()
	ctx := context.Background()
	fn := "p26_probe_" + marker
	_, err := pool.Exec(ctx, fmt.Sprintf(
		`CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $fn$
		   BEGIN
		     IF current_setting('p26.%s', true) = '1' THEN
		       RAISE EXCEPTION 'p26 probe %s';
		     END IF;
		     RETURN NEW;
		   END
		 $fn$ LANGUAGE plpgsql`, fn, marker, marker))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`DROP TRIGGER IF EXISTS %s ON %s`, fn, table))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(
		`CREATE TRIGGER %s BEFORE UPDATE ON %s
		   FOR EACH ROW EXECUTE FUNCTION %s()`, fn, table, fn))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP TRIGGER IF EXISTS %s ON %s`, fn, table))
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP FUNCTION IF EXISTS %s()`, fn))
	})
}

// p26SetMarker turns the per-session marker on/off for EVERY connection of a
// single-connection pool (the gate/finalize session).
func p26SetMarker(t *testing.T, r *Repo, marker, value string) {
	t.Helper()
	_, err := r.Pool.Exec(context.Background(), fmt.Sprintf(`SET p26.%s = '%s'`, marker, value))
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// T1. Admission freezes both modes (and legacy stays NULL)
// ---------------------------------------------------------------------------

// TestPGP26AdmissionFreezesBothModes proves the public EnqueueSyncRun path
// freezes the v1 protocol and identity triple for BOTH modes, keeps legacy
// admissions byte-identical (NULL protocol/triple), and mirrors the cp
// binding into the triple for incremental runs.
func TestPGP26AdmissionFreezesBothModes(t *testing.T) {
	t.Run("snapshot_v1_freezes_triple_without_cp", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p26Fixture(t, p26SnapshotConfig())
		// Item 4: the PUBLIC enqueue projection carries the frozen marker
		// (an empty marker would re-enable legacy cleanup against the task).
		enqRun, err := r.EnqueueSyncRun(context.Background(), defID, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, enqRun)
		runID := enqRun.ID
		assert.Equal(t, models.RunRecoveryProtocolP26V1, enqRun.RecoveryProtocol,
			"runPublicColumns/scanSyncRun must carry the frozen protocol marker")
		var protocol *string
		var recSource, recOutput *uuid.UUID
		var recFingerprint *string
		var cpMode *string
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT recovery_protocol, recovery_source_id, recovery_output_dataset_id,
			        recovery_binding_fingerprint, cp_mode
			   FROM sync_runs WHERE id = $1`, runID).
			Scan(&protocol, &recSource, &recOutput, &recFingerprint, &cpMode))
		require.NotNil(t, protocol)
		assert.Equal(t, models.RunRecoveryProtocolP26V1, *protocol)
		require.NotNil(t, recSource)
		assert.Equal(t, sourceID, *recSource)
		require.NotNil(t, recOutput)
		assert.Equal(t, jobTarget(t, r, defID), *recOutput)
		require.NotNil(t, recFingerprint)
		assert.Len(t, *recFingerprint, 64)
		assert.Nil(t, cpMode, "snapshot v1 run must not carry a cp_mode")
		// No checkpoint row is ever created for a snapshot def.
		var cpCount int
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT COUNT(*) FROM sync_def_checkpoints WHERE sync_def_id = $1`, defID).Scan(&cpCount))
		assert.Zero(t, cpCount, "snapshot admission must not create a checkpoint row")
	})

	t.Run("incremental_v1_freezes_cp_and_mirrors_triple", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		var protocol, cpFingerprint, recFingerprint string
		var recOutput, cpOutput uuid.UUID
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT recovery_protocol, recovery_binding_fingerprint, recovery_output_dataset_id,
			        cp_binding_fingerprint, cp_output_dataset_id
			   FROM sync_runs WHERE id = $1`, runID).
			Scan(&protocol, &recFingerprint, &recOutput, &cpFingerprint, &cpOutput))
		assert.Equal(t, models.RunRecoveryProtocolP26V1, protocol)
		assert.Equal(t, cpFingerprint, recFingerprint, "the triple mirrors the frozen cp binding")
		assert.Equal(t, cpOutput, recOutput, "the triple mirrors the frozen cp target")
		// And the checkpoint owner is this run (P25 semantics untouched).
		var owner uuid.UUID
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT active_run_id FROM sync_def_checkpoints WHERE sync_def_id = $1`, defID).Scan(&owner))
		assert.Equal(t, runID, owner)
	})

	t.Run("legacy_admission_stays_null", func(t *testing.T) {
		r, _, tenant, sourceID, defID := leaseFixture(t) // P25 plain fixture, no recovery block
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		var protocol *string
		var recSource *uuid.UUID
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT recovery_protocol, recovery_source_id FROM sync_runs WHERE id = $1`, runID).
			Scan(&protocol, &recSource))
		assert.Nil(t, protocol, "legacy admission must never adopt the v1 protocol")
		assert.Nil(t, recSource)
	})
}

// ---------------------------------------------------------------------------
// T2. Gate/intent atomic fault rollback (marker-RAISE on the gate UPDATE)
// ---------------------------------------------------------------------------

// TestPGP26GateIntentAtomicRollback proves the intent INSERT and the
// publish_entered_at stamp commit or roll back TOGETHER: a forced failure of
// the gate UPDATE (per-session marker trigger on sync_runs) leaves BOTH
// absent, and the same session enters cleanly once the marker is released.
func TestPGP26GateIntentAtomicRollback(t *testing.T) {
	r, dsn, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())

	gateSession := singleConnRepo(t, dsn)
	lease := p26Claim(t, gateSession, runID, tenant, time.Minute)

	p26MarkerTrigger(t, r.Pool, "sync_runs", "gate_fail")
	runBefore := runRowJSON(t, r.Pool, runID)

	p26SetMarker(t, gateSession, "gate_fail", "1")
	cand := "cpc-rollback"
	mat := material(t, matIDs{runID: runID, sourceID: sourceID, defID: defID}, "cursor", &cand, []byte("{\"x\":1}\n"), 1)
	res, err := gateSession.BeginRecoverableIncrementalPublishGate(context.Background(), lease, tenant, mat)
	require.Error(t, err, "the marker must fail the gate UPDATE")
	assert.Nil(t, res)

	// All-old: no gate stamp, no intent row, run row untouched.
	assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID))
	var intentCount int
	require.NoError(t, r.Pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM sync_run_publish_intents WHERE run_id = $1`, runID).Scan(&intentCount))
	assert.Zero(t, intentCount)

	// Positive control: same session, marker released → clean entry, and the
	// intent now exists.
	p26SetMarker(t, gateSession, "gate_fail", "0")
	entered := p26Gate(t, gateSession, runID, tenant, sourceID, defID, lease)
	assert.Equal(t, runID, entered.Run.ID)
	require.NoError(t, r.Pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM sync_run_publish_intents WHERE run_id = $1`, runID).Scan(&intentCount))
	assert.Equal(t, 1, intentCount)
}

// ---------------------------------------------------------------------------
// T2b. Gate REFUSALS never persist the intent (Root review fix regression)
// ---------------------------------------------------------------------------

// TestPGP26GateRefusalLeavesNoIntent pins the Root review fix (2026-11-15):
// every non-entered gate outcome — cancel_requested, fenced (wrong token,
// expired lease) — must leave NO intent row and NO publish_entered_at stamp.
// Before the fix the cancel/fenced diagnosis branches returned nil from the
// chain callback, COMMITTING the already-executed intent INSERT with the
// gate unentered — permanently breaking the IFF (recovery then refuses the
// run as corrupted). Both modes are pinned.
func TestPGP26GateRefusalLeavesNoIntent(t *testing.T) {
	assertNoIntentAndNoGate := func(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) {
		t.Helper()
		var intentCount int
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT COUNT(*) FROM sync_run_publish_intents WHERE run_id = $1`, runID).Scan(&intentCount))
		assert.Zero(t, intentCount, "a refused gate must not persist the intent")
		var gate *time.Time
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT publish_entered_at FROM sync_runs WHERE id = $1`, runID).Scan(&gate))
		assert.Nil(t, gate, "a refused gate must not stamp publish_entered_at")
	}

	t.Run("incremental_cancel_requested", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		lease := p26Claim(t, r, runID, tenant, time.Minute)
		// Persist a durable cancel request on the RUNNING row (the gate's
		// cancel race loser).
		res, err := r.RequestCancelSyncRun(context.Background(), runID, defID, tenant, nil, "test cancel")
		require.NoError(t, err)
		require.Equal(t, CancelOutcomeRequested, res.Outcome)

		cand := "cpc-cancel"
		mat := material(t, matIDs{runID: runID, sourceID: sourceID, defID: defID}, "cursor", &cand, []byte("{\"c\":1}\n"), 1)
		gate, err := r.BeginRecoverableIncrementalPublishGate(context.Background(), lease, tenant, mat)
		require.NoError(t, err)
		require.NotNil(t, gate)
		assert.Equal(t, IncrementalPublishGateCancelled, gate.Outcome)
		assertNoIntentAndNoGate(t, r.Pool, runID)
	})

	t.Run("incremental_fenced_wrong_token", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		lease := p26Claim(t, r, runID, tenant, time.Minute)
		wrong := *lease
		wrong.Token = "wrong-token"
		cand := "cpc-fenced"
		mat := material(t, matIDs{runID: runID, sourceID: sourceID, defID: defID}, "cursor", &cand, []byte("{\"f\":1}\n"), 1)
		gate, err := r.BeginRecoverableIncrementalPublishGate(context.Background(), &wrong, tenant, mat)
		require.NoError(t, err)
		require.NotNil(t, gate)
		assert.Equal(t, IncrementalPublishGateFenced, gate.Outcome)
		assertNoIntentAndNoGate(t, r.Pool, runID)
	})

	t.Run("incremental_fenced_expired_lease", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		lease := p26Claim(t, r, runID, tenant, 120*time.Millisecond)
		waitForDBExpiry(t, r.Pool, runID)
		cand := "cpc-expired"
		mat := material(t, matIDs{runID: runID, sourceID: sourceID, defID: defID}, "cursor", &cand, []byte("{\"e\":1}\n"), 1)
		gate, err := r.BeginRecoverableIncrementalPublishGate(context.Background(), lease, tenant, mat)
		require.NoError(t, err)
		require.NotNil(t, gate)
		assert.Equal(t, IncrementalPublishGateFenced, gate.Outcome)
		assertNoIntentAndNoGate(t, r.Pool, runID)
	})

	t.Run("snapshot_cancel_requested", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p26Fixture(t, p26SnapshotConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		lease := p26Claim(t, r, runID, tenant, time.Minute)
		res, err := r.RequestCancelSyncRun(context.Background(), runID, defID, tenant, nil, "test cancel")
		require.NoError(t, err)
		require.Equal(t, CancelOutcomeRequested, res.Outcome)

		mat := material(t, matIDs{runID: runID, sourceID: sourceID, defID: defID}, "", nil, []byte("{\"s\":1}\n"), 1)
		gate, err := r.BeginRecoverablePublishGate(context.Background(), lease, tenant, mat)
		require.NoError(t, err)
		require.NotNil(t, gate)
		assert.Equal(t, IncrementalPublishGateCancelled, gate.Outcome)
		assertNoIntentAndNoGate(t, r.Pool, runID)
	})
}

// ---------------------------------------------------------------------------
// T3. ClaimRecovery shapes, two-connection contention and lock-wait expiry
// ---------------------------------------------------------------------------

// TestPGP26ClaimRecoveryContentionAndShapes covers the three claim shapes
// (pending pre-intent, expired pre-intent, expired post-intent) plus:
// exactly one winner between two recovery connections (busy for the loser),
// the ordinary task/recovery claim race over the SAME pending row, a real
// row-lock block proof via pg_blocking_pids, and a lock_timeout expiry that
// leaves every row untouched.
func TestPGP26ClaimRecoveryContentionAndShapes(t *testing.T) {
	t.Run("post_intent_two_connections_exactly_one_winner", func(t *testing.T) {
		r, dsn, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		lease := p26Claim(t, r, runID, tenant, time.Minute)
		p26Gate(t, r, runID, tenant, sourceID, defID, lease)
		p26ForceExpire(t, r.Pool, runID)

		// Root round-1: a REAL race — one blocker holds the Source row, TWO
		// independent claim sessions are both PROVEN waiting, then the
		// barrier releases and both proceed; exactly one grant, exactly one
		// generation increment. Waiter discovery is by application_name
		// (first real-PG run: the backendPID probe can observe a different
		// pooled connection than the one that ends up waiting), and the
		// blocking chain is matched DIRECTLY or through indirect queued
		// blockers (pg_blocking_pids) — the proof is not weakened.
		observer := singleConnRepo(t, dsn)
		blocker := singleConnRepo(t, dsn)
		blockerPID := backendPID(t, blocker.Pool)
		blockTx, err := blocker.Pool.Begin(context.Background())
		require.NoError(t, err)
		var locked uuid.UUID
		require.NoError(t, blockTx.QueryRow(context.Background(),
			`SELECT c.id FROM connections c WHERE c.id = $1 FOR UPDATE`, sourceID).Scan(&locked))
		// Pre-registered bounded release: a later Fatal must never hang
		// Pool.Close on the blocker's open transaction.
		release := func() { _ = p25BoundedRollback(blockTx) }
		defer release()

		connA := singleConnRepo(t, dsn)
		connB := singleConnRepo(t, dsn)
		p26NameSession(t, connA, "p26-rec-a")
		p26NameSession(t, connB, "p26-rec-b")

		type claimResult struct {
			res *RecoveryClaimResult
			err error
		}
		chA := make(chan claimResult, 1)
		chB := make(chan claimResult, 1)
		// Coherent bounded contexts with slack: claims 30s, observer waits
		// 15s, joins 20s — never a sleep as a barrier, and the release
		// always precedes pool close (defer order).
		start := func(ch chan claimResult, repo *Repo, holder, token string) {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				res, cerr := repo.ClaimRecovery(ctx, runID, tenant, holder, token, time.Minute)
				ch <- claimResult{res, cerr}
			}()
		}
		// Staggered proof (review-w2-pg-attempt1): A first — DIRECT block on
		// the barrier; then B — its finite chain (hard or soft wait-queue
		// blockers) must reach the SAME root; both must STILL be waiting on
		// that root right before the release.
		start(chA, connA, "rec-a", "tok-a")
		p26WaitDirectBlocked(t, observer, "p26-rec-a", blockerPID)
		start(chB, connB, "rec-b", "tok-b")
		p26WaitChainToRoot(t, observer, "p26-rec-b", blockerPID)
		require.True(t, p26StillBlocked(t, observer, "p26-rec-a", blockerPID),
			"contender A must still be barrier-blocked at release time")
		require.True(t, p26StillBlocked(t, observer, "p26-rec-b", blockerPID),
			"contender B must still be barrier-blocked at release time")
		release()

		var gotA, gotB claimResult
		select {
		case gotA = <-chA:
		case <-time.After(20 * time.Second):
			t.Fatal("contender A did not converge after the barrier released")
		}
		select {
		case gotB = <-chB:
		case <-time.After(20 * time.Second):
			t.Fatal("contender B did not converge after the barrier released")
		}
		require.NoError(t, gotA.err)
		require.NoError(t, gotB.err)
		require.NotNil(t, gotA.res)
		require.NotNil(t, gotB.res)
		grants := 0
		var winner, loser *RecoveryClaimResult
		for _, got := range []*RecoveryClaimResult{gotA.res, gotB.res} {
			switch got.Outcome {
			case RecoveryClaimGranted:
				grants++
				winner = got
			case RecoveryClaimBusyLiveHolder:
				loser = got
			default:
				t.Fatalf("unexpected outcome %q in the two-connection race", got.Outcome)
			}
		}
		require.Equal(t, 1, grants, "exactly one winner")
		require.NotNil(t, winner)
		require.NotNil(t, loser)
		assert.True(t, winner.IntentExists, "post-gate claim must surface the intent")
		// Exactly ONE generation increment over the pre-race row.
		var generation int
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT generation FROM sync_runs WHERE id = $1`, runID).Scan(&generation))
		assert.Equal(t, lease.Run.Generation+1, generation)
		// Review-w2-round1 item 2 regression: the granted claim carries the
		// frozen protocol marker (empty marker would mis-route W3 to legacy).
		assert.Equal(t, models.RunRecoveryProtocolP26V1, winner.Claim.Run.RecoveryProtocol)
	})

	t.Run("pending_recovery_vs_ordinary_claim_real_race", func(t *testing.T) {
		r, dsn, tenant, sourceID, defID := p26Fixture(t, p26SnapshotConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())

		// Both entry points serialize on the SAME pending run row: hold the
		// run row from a third session, prove both contenders wait, release,
		// then exactly one claim lands (the other sees status='running').
		observer := singleConnRepo(t, dsn)
		blocker := singleConnRepo(t, dsn)
		blockerPID := backendPID(t, blocker.Pool)
		blockTx, err := blocker.Pool.Begin(context.Background())
		require.NoError(t, err)
		var locked uuid.UUID
		require.NoError(t, blockTx.QueryRow(context.Background(),
			`SELECT r.id FROM sync_runs r WHERE r.id = $1 FOR UPDATE`, runID).Scan(&locked))
		release := func() { _ = p25BoundedRollback(blockTx) }
		defer release()

		recSession := singleConnRepo(t, dsn)
		ordSession := singleConnRepo(t, dsn)
		p26NameSession(t, recSession, "p26-rec-pend")
		p26NameSession(t, ordSession, "p26-ord-pend")

		type result struct {
			res *RecoveryClaimResult
			clm *LeaseClaim
			err error
		}
		chRec := make(chan result, 1)
		chOrd := make(chan result, 1)
		// Staggered proof, same discipline as the post-intent race: the
		// recovery contender first (direct block on the run-row barrier),
		// then the ordinary claim (finite chain to the SAME root), both
		// still barrier-blocked at release time. Bounded contexts with
		// slack; no sleep barriers.
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			res, cerr := recSession.ClaimRecovery(ctx, runID, tenant, "rec", "tok", time.Minute)
			chRec <- result{res: res, err: cerr}
		}()
		p26WaitDirectBlocked(t, observer, "p26-rec-pend", blockerPID)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			clm, cerr := ordSession.ClaimSyncRun(ctx, runID, tenant, "exec", "tok", time.Minute)
			chOrd <- result{clm: clm, err: cerr}
		}()
		p26WaitChainToRoot(t, observer, "p26-ord-pend", blockerPID)
		require.True(t, p26StillBlocked(t, observer, "p26-rec-pend", blockerPID),
			"the recovery contender must still be barrier-blocked at release time")
		require.True(t, p26StillBlocked(t, observer, "p26-ord-pend", blockerPID),
			"the ordinary contender must still be barrier-blocked at release time")
		release()

		var gotRec, gotOrd result
		select {
		case gotRec = <-chRec:
		case <-time.After(20 * time.Second):
			t.Fatal("recovery contender did not converge after the barrier released")
		}
		select {
		case gotOrd = <-chOrd:
		case <-time.After(20 * time.Second):
			t.Fatal("ordinary contender did not converge after the barrier released")
		}
		require.NoError(t, gotRec.err)
		require.NoError(t, gotOrd.err)
		require.NotNil(t, gotRec.res, "never a nil result with nil error")
		grants := 0
		if gotRec.res.Outcome == RecoveryClaimGranted {
			grants++
			assert.False(t, gotRec.res.IntentExists, "pre-gate pending claim has no intent")
			assert.Equal(t, models.RunRecoveryProtocolP26V1, gotRec.res.Claim.Run.RecoveryProtocol)
		}
		if gotOrd.clm != nil {
			grants++
			assert.Equal(t, models.RunRecoveryProtocolP26V1, gotOrd.clm.Run.RecoveryProtocol)
		}
		assert.Equal(t, 1, grants, "exactly one claimant wins the same pending row")
		if gotRec.res.Outcome != RecoveryClaimGranted {
			assert.Equal(t, RecoveryClaimBusyLiveHolder, gotRec.res.Outcome,
				"the recovery loser must observe the winner's live lease, never an unset result")
		}
	})

	t.Run("expired_pre_intent_grants_fresh_execution", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p26Fixture(t, p26SnapshotConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		p26Claim(t, r, runID, tenant, 120*time.Millisecond)
		waitForDBExpiry(t, r.Pool, runID)

		claim, res := p26RecoveryClaim(t, r, runID, tenant)
		assert.False(t, res.IntentExists)
		assert.Equal(t, "running", claim.Run.Status)
		var gate *time.Time
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT publish_entered_at FROM sync_runs WHERE id = $1`, runID).Scan(&gate))
		assert.Nil(t, gate, "pre-gate recovery claim must not stamp the gate")
	})

	t.Run("lock_wait_proof_and_lock_timeout_zero_write", func(t *testing.T) {
		r, dsn, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		lease := p26Claim(t, r, runID, tenant, time.Minute)
		p26Gate(t, r, runID, tenant, sourceID, defID, lease)
		p26ForceExpire(t, r.Pool, runID)

		observer := singleConnRepo(t, dsn)
		blocker := singleConnRepo(t, dsn)
		blockerPID := backendPID(t, blocker.Pool)
		blockTx, err := blocker.Pool.Begin(context.Background())
		require.NoError(t, err)
		var locked uuid.UUID
		require.NoError(t, blockTx.QueryRow(context.Background(),
			`SELECT c.id FROM connections c WHERE c.id = $1 FOR UPDATE`, sourceID).Scan(&locked))

		// Variant A (block proof → release → granted): the claim genuinely
		// enters the Source row-lock wait (pg_blocking_pids proof, not a
		// timeout), then completes when the barrier releases.
		claimSession := singleConnRepo(t, dsn)
		claimPID := backendPID(t, claimSession.Pool)
		type claimResult struct {
			res *RecoveryClaimResult
			err error
		}
		ch := make(chan claimResult, 1)
		go func() {
			res, cerr := claimSession.ClaimRecovery(context.Background(), runID, tenant, "rec", "tok", time.Minute)
			ch <- claimResult{res, cerr}
		}()
		waitUntilBlockedBy(t, observer.Pool, claimPID, blockerPID)
		require.NoError(t, p25BoundedRollback(blockTx))
		var claimA *LeaseClaim
		select {
		case got := <-ch:
			require.NoError(t, got.err)
			require.NotNil(t, got.res)
			require.Equal(t, RecoveryClaimGranted, got.res.Outcome)
			claimA = got.res.Claim
		case <-time.After(15 * time.Second):
			t.Fatal("claim never completed after the barrier released")
		}

		// Root round-1 correction: this def still owns runA's checkpoint, so
		// a second admission would answer incremental_busy. Converge runA
		// LEGITIMATELY first (verified-receipt finalize), then admit runB.
		intentA, err := claimSession.GetPublishIntentMaterial(context.Background(), runID, tenant)
		require.NoError(t, err)
		require.NotNil(t, intentA)
		_, err = claimSession.FinalizeRecoveredIncrementalRun(context.Background(), claimA, tenant, p26ReceiptFor(*intentA))
		require.NoError(t, err)

		// Variant B (lock_timeout expiry → error, zero writes): a SECOND
		// v1 run is gated and expired, the blocker re-holds the Source row,
		// and the claiming session carries lock_timeout='150ms' BEFORE the
		// call. The claim errors and every row stays byte-identical.
		runB := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		leaseB := p26Claim(t, r, runB, tenant, time.Minute)
		p26Gate(t, r, runB, tenant, sourceID, defID, leaseB)
		p26ForceExpire(t, r.Pool, runB)

		blockTx2, err := blocker.Pool.Begin(context.Background())
		require.NoError(t, err)
		defer func() { _ = p25BoundedRollback(blockTx2) }()
		require.NoError(t, blockTx2.QueryRow(context.Background(),
			`SELECT c.id FROM connections c WHERE c.id = $1 FOR UPDATE`, sourceID).Scan(&locked))

		claimSession2 := singleConnRepo(t, dsn)
		_, err = claimSession2.Pool.Exec(context.Background(), `SET lock_timeout = '150ms'`)
		require.NoError(t, err)

		runBBefore := runRowJSON(t, r.Pool, runB)
		cpBBefore := p25CpJSON(t, r.Pool, defID)
		resB, err := claimSession2.ClaimRecovery(context.Background(), runB, tenant, "rec-b", "tok-b", time.Minute)
		assert.Error(t, err, "the lock wait must expire on lock_timeout")
		assert.Nil(t, resB)
		assert.Equal(t, runBBefore, runRowJSON(t, r.Pool, runB), "a timed-out claim writes nothing")
		assert.Equal(t, cpBBefore, p25CpJSON(t, r.Pool, defID))
	})
}

// ---------------------------------------------------------------------------
// T4. Ordinary takeover/complete cannot bypass the v1 protocol
// ---------------------------------------------------------------------------

// TestPGP26TakeoverAndCompleteBypassRefused proves: TakeoverExpiredSyncRun
// refuses every v1 run (both modes, zero writes); the ordinary
// CompleteLeasedSyncRun refuses v1 success (typed) and post-gate
// failed/aborted (typed), while PRE-gate failures keep the original CAS.
func TestPGP26TakeoverAndCompleteBypassRefused(t *testing.T) {
	t.Run("incremental_post_gate", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		lease := p26Claim(t, r, runID, tenant, time.Minute)
		p26Gate(t, r, runID, tenant, sourceID, defID, lease)
		p26ForceExpire(t, r.Pool, runID)

		runBefore := runRowJSON(t, r.Pool, runID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		taken, err := r.TakeoverExpiredSyncRun(context.Background(), runID, tenant, "taker", "tok", time.Minute)
		require.NoError(t, err)
		assert.Nil(t, taken, "a v1 run is never generic-takeover eligible")
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID))
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))

		msg := "boom"
		_, err = r.CompleteLeasedSyncRun(context.Background(), runID, tenant,
			lease.Holder, lease.Token, lease.Run.Generation, "succeeded", 0, 0, 0, nil, nil, nil, nil)
		assert.ErrorIs(t, err, ErrRecoveryRequiresFinalize, "v1 success only via finalize")
		_, err = r.CompleteLeasedSyncRun(context.Background(), runID, tenant,
			lease.Holder, lease.Token, lease.Run.Generation, "failed", 0, 0, 0, &msg, nil, nil, nil)
		assert.ErrorIs(t, err, ErrRecoveryPostGateLocked, "post-gate failed must stay running")
		_, err = r.CompleteLeasedSyncRun(context.Background(), runID, tenant,
			lease.Holder, lease.Token, lease.Run.Generation, "aborted", 0, 0, 0, &msg, nil, nil, nil)
		assert.ErrorIs(t, err, ErrRecoveryPostGateLocked, "post-gate aborted must stay running")
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID), "every refused bypass writes nothing")
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
	})

	t.Run("snapshot_post_gate", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p26Fixture(t, p26SnapshotConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		lease := p26Claim(t, r, runID, tenant, time.Minute)
		p26Gate(t, r, runID, tenant, sourceID, defID, lease)
		p26ForceExpire(t, r.Pool, runID)

		runBefore := runRowJSON(t, r.Pool, runID)
		taken, err := r.TakeoverExpiredSyncRun(context.Background(), runID, tenant, "taker", "tok", time.Minute)
		require.NoError(t, err)
		assert.Nil(t, taken)
		_, err = r.CompleteLeasedSyncRun(context.Background(), runID, tenant,
			lease.Holder, lease.Token, lease.Run.Generation, "succeeded", 0, 0, 0, nil, nil, nil, nil)
		assert.ErrorIs(t, err, ErrRecoveryRequiresFinalize)
		msg := "boom"
		_, err = r.CompleteLeasedSyncRun(context.Background(), runID, tenant,
			lease.Holder, lease.Token, lease.Run.Generation, "failed", 0, 0, 0, &msg, nil, nil, nil)
		assert.ErrorIs(t, err, ErrRecoveryPostGateLocked)
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID))
	})

	t.Run("pre_gate_failure_keeps_original_cas", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p26Fixture(t, p26SnapshotConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		lease := p26Claim(t, r, runID, tenant, time.Minute)
		msg := "pre-gate honest failure"
		run, err := r.CompleteLeasedSyncRun(context.Background(), runID, tenant,
			lease.Holder, lease.Token, lease.Run.Generation, "failed", 0, 0, 0, &msg, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, run)
		assert.Equal(t, "failed", run.Status, "pre-gate v1 failure keeps the P25 CAS semantics")
	})
}

// ---------------------------------------------------------------------------
// T5. Budgets persist across claims; manual query never resets
// ---------------------------------------------------------------------------

// TestPGP26BudgetsAcrossClaimsTotalCaps proves the INDEPENDENT,
// restart-persistent budgets (review-w2-round1 item 7): send total 3 and
// automated query total 5 never reset across claims/waits/restarts, do NOT
// block each other, a manual query past the auto cap only increments the
// observable counter, and a genuinely committed receipt still finalizes
// from an exhausted state.
func TestPGP26BudgetsAcrossClaimsTotalCaps(t *testing.T) {
	r, _, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	lease := p26Claim(t, r, runID, tenant, time.Minute)
	p26Gate(t, r, runID, tenant, sourceID, defID, lease)
	p26ForceExpire(t, r.Pool, runID)

	claim1, _ := p26RecoveryClaim(t, r, runID, tenant)

	t.Run("send3_then_independent_auto_queries", func(t *testing.T) {
		for i := 1; i <= MaxPublishSendAttempts; i++ {
			b, err := r.ConsumePublishSendBudget(context.Background(), claim1, tenant)
			require.NoError(t, err)
			assert.True(t, b.Allowed)
		}
		b, err := r.ConsumePublishSendBudget(context.Background(), claim1, tenant)
		assert.ErrorIs(t, err, ErrIntentNotMutable)
		assert.False(t, b.Allowed)

		// The send exhaustion must NOT block the QUERY budget.
		for i := 1; i <= MaxRecoveryQueryAttempts; i++ {
			q, qerr := r.ConsumeRecoveryQueryAttempt(context.Background(), claim1, tenant, RecoveryWaitVerifyUnavailable)
			require.NoError(t, qerr, "send-exhausted run still verifies (attempt %d)", i)
			assert.True(t, q.Allowed)
		}
		_, err = r.ConsumeRecoveryQueryAttempt(context.Background(), claim1, tenant, RecoveryWaitVerifyUnavailable)
		assert.ErrorIs(t, err, ErrIntentNotMutable, "auto query cap is its own 5")
	})

	t.Run("totals_survive_new_claims_and_waits", func(t *testing.T) {
		p26ForceExpire(t, r.Pool, runID)
		claim2, res := p26RecoveryClaim(t, r, runID, tenant)
		require.True(t, res.IntentExists)
		_, err := r.ConsumePublishSendBudget(context.Background(), claim2, tenant)
		assert.ErrorIs(t, err, ErrIntentNotMutable, "send total never resets on a new claim")
		_, err = r.ConsumeRecoveryQueryAttempt(context.Background(), claim2, tenant, RecoveryWaitVerifyUnavailable)
		assert.ErrorIs(t, err, ErrIntentNotMutable, "auto query total never resets on a new claim")
		// A wait is still recordable (only finalization refuses it) and it
		// resets neither counter nor the exhausted summary.
		require.NoError(t, r.RecordRecoveryWait(context.Background(), claim2, tenant, RecoveryWaitVerifyUnavailable, time.Second))
	})

	t.Run("manual_query_past_cap_observable_non_resetting", func(t *testing.T) {
		p26ForceExpire(t, r.Pool, runID)
		claim3, _ := p26RecoveryClaim(t, r, runID, tenant)
		m, err := r.ConsumeRecoveryQueryAttemptManual(context.Background(), claim3, tenant, RecoveryWaitVerifyUnavailable)
		require.NoError(t, err)
		assert.True(t, m.Allowed)
		assert.Equal(t, MaxRecoveryQueryAttempts+1, m.Attempt)
		// Manual query NEVER bypasses the send budget.
		_, err = r.ConsumePublishSendBudget(context.Background(), claim3, tenant)
		assert.ErrorIs(t, err, ErrIntentNotMutable)
	})

	t.Run("reverse_order_query5_then_send_allowed_to_3", func(t *testing.T) {
		// First real-PG run fix (W6 raw 04:25Z): the shared def still owns
		// the main fixture run's checkpoint (the subtests above never
		// finalize it), so a second incremental admission answers busy —
		// boot an INDEPENDENT fixture instead. Production single-flight is
		// untouched. The subtest proves the OTHER ordering: query exhausts
		// first, the send budget still runs to its own cap of 3.
		r, tenant, sourceID, defID := p26IndependentFixture(t)
		run2 := leaseEnqueue(t, r, tenant, sourceID, defID, tenant.OwnerID)
		lease2 := p26Claim(t, r, run2, tenant, time.Minute)
		p26Gate(t, r, run2, tenant, sourceID, defID, lease2)
		p26ForceExpire(t, r.Pool, run2)
		claimRun2, _ := p26RecoveryClaim(t, r, run2, tenant)
		for i := 1; i <= MaxRecoveryQueryAttempts; i++ {
			q, qerr := r.ConsumeRecoveryQueryAttempt(context.Background(), claimRun2, tenant, RecoveryWaitVerifyUnavailable)
			require.NoError(t, qerr)
			assert.True(t, q.Allowed)
		}
		for i := 1; i <= MaxPublishSendAttempts; i++ {
			b, berr := r.ConsumePublishSendBudget(context.Background(), claimRun2, tenant)
			require.NoError(t, berr, "query-exhausted run may still send (attempt %d)", i)
			assert.True(t, b.Allowed)
		}
		_, berr := r.ConsumePublishSendBudget(context.Background(), claimRun2, tenant)
		assert.ErrorIs(t, berr, ErrIntentNotMutable)

		// Counters authoritative + first-exhausted summary preserved.
		var state string
		var sends, queries int
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT recovery_state, send_attempts, query_attempts
			   FROM sync_run_publish_intents WHERE run_id = $1`, run2).
			Scan(&state, &sends, &queries))
		assert.Equal(t, MaxPublishSendAttempts, sends)
		assert.Equal(t, MaxRecoveryQueryAttempts, queries)
		assert.Contains(t, []string{IntentStateQueryBudgetExhausted, IntentStateSendBudgetExhausted}, state,
			"an exhausted summary stays visible; the counters are authoritative")

		// A genuinely committed receipt still finalizes from the exhausted
		// state — budgets gate retries, never the truth.
		intent2, err := r.GetPublishIntentMaterial(context.Background(), run2, tenant)
		require.NoError(t, err)
		fin, err := r.FinalizeRecoveredIncrementalRun(context.Background(), claimRun2, tenant, p26ReceiptFor(*intent2))
		require.NoError(t, err)
		assert.Equal(t, RecoveryFinalizeCommitted, fin.Outcome)
	})
}

// ---------------------------------------------------------------------------
// T6. Receipt mismatch = zero writes (every identity field)
// ---------------------------------------------------------------------------

// TestPGP26ReceiptMismatchZeroWrite drives FinalizeRecoveredRun with a
// mismatched receipt for EVERY identity field and proves each refusal leaves
// the run, checkpoint and intent rows byte-identical.
func TestPGP26ReceiptMismatchZeroWrite(t *testing.T) {
	r, _, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	lease := p26Claim(t, r, runID, tenant, time.Minute)
	p26Gate(t, r, runID, tenant, sourceID, defID, lease)
	p26ForceExpire(t, r.Pool, runID)
	recClaim, _ := p26RecoveryClaim(t, r, runID, tenant)

	intent, err := r.GetPublishIntentMaterial(context.Background(), runID, tenant)
	require.NoError(t, err)
	require.NotNil(t, intent)
	base := p26ReceiptFor(*intent)

	mutations := map[string]func(*PublishReceipt){
		"status":         func(rc *PublishReceipt) { rc.Status = "OPEN" },
		"key":            func(rc *PublishReceipt) { rc.IdempotencyKey = DerivePublishKey(uuid.New()) },
		"dataset":        func(rc *PublishReceipt) { rc.DatasetID = uuid.New() },
		"branch":         func(rc *PublishReceipt) { rc.Branch = "dev" },
		"nil_txn":        func(rc *PublishReceipt) { rc.TransactionID = uuid.Nil },
		"fingerprint":    func(rc *PublishReceipt) { rc.RequestFingerprint = strings.Repeat("a", 64) },
		"content_hash":   func(rc *PublishReceipt) { rc.MetadataContentHash = strings.Repeat("b", 64) },
		"lineage_run":    func(rc *PublishReceipt) { rc.MetadataSyncRunID = uuid.New() },
		"lineage_source": func(rc *PublishReceipt) { rc.MetadataSourceID = uuid.New() },
		"lineage_def":    func(rc *PublishReceipt) { rc.MetadataSyncDefID = uuid.New() },
	}
	runBefore := runRowJSON(t, r.Pool, runID)
	cpBefore := p25CpJSON(t, r.Pool, defID)
	intentBefore := p26IntentJSON(t, r.Pool, runID)
	for name, mutate := range mutations {
		bad := base
		mutate(&bad)
		_, err := r.FinalizeRecoveredRun(context.Background(), recClaim, tenant, bad)
		assert.ErrorIs(t, err, ErrReceiptMismatch, "mutation %q must be refused", name)
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID), "mutation %q: run untouched", name)
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID), "mutation %q: checkpoint untouched", name)
		assert.Equal(t, intentBefore, p26IntentJSON(t, r.Pool, runID), "mutation %q: intent untouched", name)
	}
}

// ---------------------------------------------------------------------------
// T7. Finalize atomicity (marker-RAISE on the checkpoint advance)
// ---------------------------------------------------------------------------

// TestPGP26FinalizeAtomicRollback proves success + epoch advance + owner
// release + intent finalize land in ONE transaction: failing the checkpoint
// UPDATE (per-session marker) rolls the success write back with it, and the
// released marker converges everything in one shot.
func TestPGP26FinalizeAtomicRollback(t *testing.T) {
	r, dsn, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	lease := p26Claim(t, r, runID, tenant, time.Minute)
	p26Gate(t, r, runID, tenant, sourceID, defID, lease)
	p26ForceExpire(t, r.Pool, runID)

	finSession := singleConnRepo(t, dsn)
	recClaim, _ := p26RecoveryClaim(t, finSession, runID, tenant)
	intent, err := finSession.GetPublishIntentMaterial(context.Background(), runID, tenant)
	require.NoError(t, err)
	receipt := p26ReceiptFor(*intent)

	p26MarkerTrigger(t, r.Pool, "sync_def_checkpoints", "cp_fail")
	runBefore := runRowJSON(t, r.Pool, runID)
	cpBefore := p25CpJSON(t, r.Pool, defID)
	intentBefore := p26IntentJSON(t, r.Pool, runID)

	p26SetMarker(t, finSession, "cp_fail", "1")
	_, err = finSession.FinalizeRecoveredIncrementalRun(context.Background(), recClaim, tenant, receipt)
	require.Error(t, err, "the marker must fail the checkpoint advance — and roll everything back")
	assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID), "success write rolled back with the advance")
	assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
	assert.Equal(t, intentBefore, p26IntentJSON(t, r.Pool, runID))

	// Positive control: marker released → one-shot convergence.
	p26SetMarker(t, finSession, "cp_fail", "0")
	fin, err := finSession.FinalizeRecoveredIncrementalRun(context.Background(), recClaim, tenant, receipt)
	require.NoError(t, err)
	require.NotNil(t, fin)
	assert.Equal(t, RecoveryFinalizeCommitted, fin.Outcome)

	var status string
	var owner *uuid.UUID
	var epoch int
	var intentState string
	require.NoError(t, r.Pool.QueryRow(context.Background(),
		`SELECT r.status, cp.active_run_id, cp.confirmed_epoch, i.recovery_state
		   FROM sync_runs r
		   JOIN sync_def_checkpoints cp ON cp.sync_def_id = r.sync_def_id
		   JOIN sync_run_publish_intents i ON i.run_id = r.id
		  WHERE r.id = $1`, runID).Scan(&status, &owner, &epoch, &intentState))
	assert.Equal(t, "succeeded", status)
	assert.Nil(t, owner, "owner released in the same transaction")
	assert.Equal(t, 1, epoch, "epoch advanced exactly once")
	assert.Equal(t, IntentStateFinalized, intentState)
}

// ---------------------------------------------------------------------------
// T8. Duplicate finalize after a later batch = already_resolved, zero writes
// ---------------------------------------------------------------------------

// TestPGP26LaterBatchDuplicateFinalizeZeroWrite drives run A to a committed
// finalize, lets batch B advance the checkpoint and take (and release) the
// owner, then repeats A's finalize with the SAME receipt: the answer is
// already_resolved and B's advanced state is not rewritten.
func TestPGP26LaterBatchDuplicateFinalizeZeroWrite(t *testing.T) {
	r, _, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
	ctx := context.Background()

	// Batch A: admit → claim → gate → expire → recovery claim → finalize.
	runA := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	leaseA := p26Claim(t, r, runA, tenant, time.Minute)
	p26Gate(t, r, runA, tenant, sourceID, defID, leaseA)
	p26ForceExpire(t, r.Pool, runA)
	claimA, _ := p26RecoveryClaim(t, r, runA, tenant)
	intentA, err := r.GetPublishIntentMaterial(ctx, runA, tenant)
	require.NoError(t, err)
	receiptA := p26ReceiptFor(*intentA)
	finA, err := r.FinalizeRecoveredIncrementalRun(ctx, claimA, tenant, receiptA)
	require.NoError(t, err)
	assert.Equal(t, RecoveryFinalizeCommitted, finA.Outcome)

	// Batch B: a fresh admission owns the checkpoint now (P25 single-flight),
	// runs its own gate cycle and finalizes — the epoch advances to 2.
	runB, rejection, err := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
	require.NoError(t, err)
	require.Nil(t, rejection)
	require.NotNil(t, runB)
	leaseB := p26Claim(t, r, runB.ID, tenant, time.Minute)
	p26Gate(t, r, runB.ID, tenant, sourceID, defID, leaseB)
	p26ForceExpire(t, r.Pool, runB.ID)
	claimB, _ := p26RecoveryClaim(t, r, runB.ID, tenant)
	intentB, err := r.GetPublishIntentMaterial(ctx, runB.ID, tenant)
	require.NoError(t, err)
	_, err = r.FinalizeRecoveredIncrementalRun(ctx, claimB, tenant, p26ReceiptFor(*intentB))
	require.NoError(t, err)

	var epochAfterB int
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT confirmed_epoch FROM sync_def_checkpoints WHERE sync_def_id = $1`, defID).Scan(&epochAfterB))
	require.Equal(t, 2, epochAfterB)

	// Duplicate A finalize with the SAME receipt: already_resolved, zero
	// writes — even though the checkpoint has since advanced.
	runBefore := runRowJSON(t, r.Pool, runA)
	cpBefore := p25CpJSON(t, r.Pool, defID)
	intentBefore := p26IntentJSON(t, r.Pool, runA)
	dup, err := r.FinalizeRecoveredIncrementalRun(ctx, claimA, tenant, receiptA)
	require.NoError(t, err)
	require.NotNil(t, dup)
	assert.Equal(t, RecoveryFinalizeAlreadyResolved, dup.Outcome)
	assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runA))
	assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID), "B's advanced checkpoint is not rewritten")
	assert.Equal(t, intentBefore, p26IntentJSON(t, r.Pool, runA))
}

// ---------------------------------------------------------------------------
// T9. Credential / file_glob fence (v1) and legacy unaffected
// ---------------------------------------------------------------------------

// TestPGP26CredentialFileGlobFenceAndLegacy proves the P26-only fence: an
// EFFECTIVE credential change (different fingerprint) or file_glob/target
// change is refused while a v1 pending/running run exists; same-value
// rotations and pure metadata edits stay allowed; and legacy (non-v1) runs
// keep the exact pre-P26 behavior.
func TestPGP26CredentialFileGlobFenceAndLegacy(t *testing.T) {
	t.Run("v1_fence", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p26Fixture(t, p26SnapshotConfig())
		// Seed the baseline credential BEFORE the run exists (a first
		// attachment would itself be an effective change under the fence).
		_, err := r.Pool.Exec(context.Background(),
			`INSERT INTO source_credentials (id, source_id, kind, secret_ciphertext, fingerprint)
			 VALUES ($1, $2, 'api_key', $3, $4)`,
			uuid.New(), sourceID, []byte("ct"), strings.Repeat("a", 64))
		require.NoError(t, err)
		leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New()) // pending v1 run

		credBefore := p26CredJSON(t, r.Pool, sourceID, "api_key")
		defBefore := tableRowJSON(t, r.Pool, "batch_sync_defs", defID)

		_, err2 := r.SetCredential(context.Background(), sourceID, tenant, "api_key", []byte("ct2"), strings.Repeat("b", 64))
		assert.ErrorIs(t, err2, ErrRecoveryRunsActive, "effective credential change under a v1 run")
		assert.Equal(t, credBefore, p26CredJSON(t, r.Pool, sourceID, "api_key"))

		// Same-fingerprint rotation stays allowed (not an effective change).
		updated, err := r.SetCredential(context.Background(), sourceID, tenant, "api_key", []byte("ct-same"), strings.Repeat("a", 64))
		require.NoError(t, err)
		require.NotNil(t, updated)

		glob := "sync-*.ndjson"
		_, err = r.UpdateSyncJob(context.Background(), defID,
			&models.UpdateSyncJobRequest{FileGlob: &glob}, tenant)
		assert.ErrorIs(t, err, ErrRecoveryRunsActive, "effective file_glob change under a v1 run")
		assert.Equal(t, defBefore, tableRowJSON(t, r.Pool, "batch_sync_defs", defID))
	})

	t.Run("legacy_unaffected", func(t *testing.T) {
		r, _, tenant, sourceID, defID := leaseFixture(t)
		leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New()) // pending legacy run

		// Effective credential change: allowed (pre-P26 behavior).
		v, err := r.SetCredential(context.Background(), sourceID, tenant, "api_key", []byte("ct"), strings.Repeat("c", 64))
		require.NoError(t, err)
		require.NotNil(t, v, "positive control: the in-tenant write attaches")

		// Effective file_glob change: allowed for a legacy def without a CP
		// owner (P25 semantics byte-identical).
		glob := "legacy-*.ndjson"
		job, err := r.UpdateSyncJob(context.Background(), defID,
			&models.UpdateSyncJobRequest{FileGlob: &glob}, tenant)
		require.NoError(t, err)
		require.NotNil(t, job)
	})
}

// ---------------------------------------------------------------------------
// T10. Migration: empty database, pre-P26 rows, replay
// ---------------------------------------------------------------------------

// TestPGP26MigrationEmptyLegacyReplay proves: the P26 migration lands on an
// EMPTY database (kept as its own case), upgrades a REAL pre-P26 schema with
// an old row preserved (never adopted), and replays idempotently
// (review-w2-round1 correction: the old row must come from the OLD schema,
// not from a new-schema admission).
func TestPGP26MigrationEmptyLegacyReplay(t *testing.T) {
	schemaSnapshot := func(pool *pgxpool.Pool) string {
		rows, err := pool.Query(context.Background(), `
			SELECT table_name || ':' || column_name || ':' || data_type
			  FROM information_schema.columns
			 WHERE table_schema = 'public'
			   AND (table_name = 'sync_run_publish_intents'
			    OR (table_name = 'sync_runs' AND column_name LIKE 'recovery_%'))
			 ORDER BY 1`)
		require.NoError(t, err)
		defer rows.Close()
		var sb strings.Builder
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			sb.WriteString(line + "\n")
		}
		return sb.String()
	}

	t.Run("empty_database", func(t *testing.T) {
		r, _ := bootRepoWithDSN(t)
		require.NoError(t, Migrate(context.Background(), r.Pool))
		empty := schemaSnapshot(r.Pool)
		assert.Contains(t, empty, "sync_run_publish_intents:publish_body:bytea")
		assert.Contains(t, empty, "sync_runs:recovery_protocol:")
		assert.Contains(t, empty, "sync_runs:recovery_binding_fingerprint:")
		require.NoError(t, Migrate(context.Background(), r.Pool), "replay idempotent")
		assert.Equal(t, empty, schemaSnapshot(r.Pool))
	})

	t.Run("real_pre_p26_schema_upgrade_preserves_old_rows", func(t *testing.T) {
		r, _ := bootRepoWithDSN(t)
		ctx := context.Background()
		// Apply EVERY migration except the P26 one — a genuine pre-P26
		// database (the same ordering Migrate uses, from the same embed FS).
		files, err := fs.Glob(migrationsFS, "migrations/*.sql")
		require.NoError(t, err)
		sort.Strings(files)
		for _, name := range files {
			if strings.Contains(name, "20261117100000") {
				continue
			}
			raw, rerr := migrationsFS.ReadFile(name)
			require.NoError(t, rerr)
			_, err = r.Pool.Exec(ctx, string(raw))
			require.NoError(t, err, "pre-P26 migration %s", name)
		}
		// An OLD pending run on the OLD schema (raw insert — the new repo
		// scanners select recovery columns that do not exist yet).
		tenant := Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}
		conn, cerr := r.CreateConnection(ctx, &models.CreateConnectionRequest{Name: "old", ConnectorType: "rest_api"}, tenant)
		require.NoError(t, cerr)
		job, jerr := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{SourceID: conn.ID, OutputDatasetID: uuid.New()}, tenant)
		require.NoError(t, jerr)
		oldRun := uuid.New()
		_, err = r.Pool.Exec(ctx,
			`INSERT INTO sync_runs (id, sync_def_id, status, org_id, owner_id, requested_by, control_mode, started_at)
			 VALUES ($1, $2, 'pending', $3, $4, $5, 'controlled', clock_timestamp())`,
			oldRun, job.ID, tenant.OrgID, tenant.OwnerID, uuid.New())
		require.NoError(t, err)

		// Upgrade: the P26 migration lands, the old row survives untouched.
		require.NoError(t, Migrate(ctx, r.Pool))
		var protocol *string
		var recSource *uuid.UUID
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT recovery_protocol, recovery_source_id FROM sync_runs WHERE id = $1`, oldRun).
			Scan(&protocol, &recSource))
		assert.Nil(t, protocol, "an old-schema row is never adopted")
		assert.Nil(t, recSource)
		upgraded := schemaSnapshot(r.Pool)
		assert.Contains(t, upgraded, "sync_run_publish_intents:publish_body:bytea")

		// Replay on the upgraded database: schema stable, still not adopted.
		require.NoError(t, Migrate(ctx, r.Pool))
		assert.Equal(t, upgraded, schemaSnapshot(r.Pool))
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT recovery_protocol FROM sync_runs WHERE id = $1`, oldRun).Scan(&protocol))
		assert.Nil(t, protocol)
	})
}

// ---------------------------------------------------------------------------
// T11. OLD public entries refuse p26-v1 (zero writes); legacy unaffected
// ---------------------------------------------------------------------------

// TestPGP26OldPublicEntriesRefuseV1 pins review-w2-round1 item 3: the old
// gate/complete entries can never bypass the v1 protocol — no
// gate-without-intent, no receipt-less advance — while the legacy (NULL
// protocol) paths keep working (positive controls).
func TestPGP26OldPublicEntriesRefuseV1(t *testing.T) {
	v1Refused := func(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID, defID uuid.UUID) {
		t.Helper()
		var intentCount int
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT COUNT(*) FROM sync_run_publish_intents WHERE run_id = $1`, runID).Scan(&intentCount))
		assert.Zero(t, intentCount, "old entries must not create an intent")
		var gate *time.Time
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT publish_entered_at FROM sync_runs WHERE id = $1`, runID).Scan(&gate))
		assert.Nil(t, gate, "old entries must not stamp the gate")
		_ = defID
	}

	t.Run("v1_incremental_all_three_old_entries", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		lease := p26Claim(t, r, runID, tenant, time.Minute)

		runBefore := runRowJSON(t, r.Pool, runID)
		cpBefore := p25CpJSON(t, r.Pool, defID)

		_, err := r.BeginPublishGate(context.Background(), runID, tenant, lease.Holder, lease.Token, lease.Run.Generation, time.Minute)
		assert.ErrorIs(t, err, ErrRecoveryRequiresRecoverableGate)
		_, err = r.BeginIncrementalPublishGate(context.Background(), runID, tenant, lease.Holder, lease.Token, lease.Run.Generation, time.Minute)
		assert.ErrorIs(t, err, ErrRecoveryRequiresRecoverableGate)
		_, err = r.CompleteIncrementalSyncRun(context.Background(), runID, tenant, lease.Holder, lease.Token, lease.Run.Generation,
			IncrementalAdvance{CompletedCheckpoint: "cpc", Rows: 1, Bytes: 2, ContentHash: strings.Repeat("a", 64), TxnID: uuid.New()})
		assert.ErrorIs(t, err, ErrRecoveryRequiresFinalize, "a bare transaction UUID is not receipt evidence")

		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID), "every refused old entry writes nothing")
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID))
		v1Refused(t, r.Pool, runID, defID)
	})

	t.Run("v1_snapshot_old_gate_refused", func(t *testing.T) {
		r, _, tenant, sourceID, defID := p26Fixture(t, p26SnapshotConfig())
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		lease := p26Claim(t, r, runID, tenant, time.Minute)
		runBefore := runRowJSON(t, r.Pool, runID)
		_, err := r.BeginPublishGate(context.Background(), runID, tenant, lease.Holder, lease.Token, lease.Run.Generation, time.Minute)
		assert.ErrorIs(t, err, ErrRecoveryRequiresRecoverableGate)
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID))
		v1Refused(t, r.Pool, runID, defID)
	})

	t.Run("legacy_old_gate_still_works", func(t *testing.T) {
		r, _, tenant, sourceID, defID := leaseFixture(t)
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		lease := p26Claim(t, r, runID, tenant, time.Minute)
		gate, err := r.BeginPublishGate(context.Background(), runID, tenant, lease.Holder, lease.Token, lease.Run.Generation, time.Minute)
		require.NoError(t, err, "positive control: the legacy path is unchanged")
		require.NotNil(t, gate)
		assert.Equal(t, PublishGateEntered, gate.Outcome)
		// And legacy completion (non-v1) still works through the old entry.
		version := uuid.New()
		hash := strings.Repeat("b", 64)
		run, err := r.CompleteLeasedSyncRun(context.Background(), runID, tenant,
			lease.Holder, lease.Token, lease.Run.Generation, "succeeded", 1, 1, 1, nil, nil, &version, &hash)
		require.NoError(t, err)
		assert.Equal(t, "succeeded", run.Status)
	})
}

// ---------------------------------------------------------------------------
// T12. Claim terminal shapes (item 6): read-only already_resolved first,
// explicit zero-write not_eligible for every other terminal shape
// ---------------------------------------------------------------------------

// TestPGP26ClaimTerminalShapes pins review-w2-round1 item 6: a succeeded run
// with a finalized intent answers the READ-ONLY already_resolved outcome even
// after a later batch advanced the checkpoint and released the owner; failed/
// aborted terminal shapes are explicit zero-write not_eligible with NO lease
// mutation; ClaimRecovery never returns a nil result with nil error.
func TestPGP26ClaimTerminalShapes(t *testing.T) {
	r, _, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
	ctx := context.Background()

	// Run A: full committed cycle (gate → expire → claim → finalize).
	runA := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	leaseA := p26Claim(t, r, runA, tenant, time.Minute)
	p26Gate(t, r, runA, tenant, sourceID, defID, leaseA)
	p26ForceExpire(t, r.Pool, runA)
	claimA, _ := p26RecoveryClaim(t, r, runA, tenant)
	intentA, err := r.GetPublishIntentMaterial(ctx, runA, tenant)
	require.NoError(t, err)
	_, err = r.FinalizeRecoveredIncrementalRun(ctx, claimA, tenant, p26ReceiptFor(*intentA))
	require.NoError(t, err)

	t.Run("succeeded_finalized_already_resolved_after_later_batch", func(t *testing.T) {
		// Batch B advances the checkpoint (epoch 2) and releases the owner.
		runB, rejection, aerr := p25Admit(ctx, r, tenant, sourceID, defID, uuid.New(), "")
		require.NoError(t, aerr)
		require.Nil(t, rejection)
		leaseB := p26Claim(t, r, runB.ID, tenant, time.Minute)
		p26Gate(t, r, runB.ID, tenant, sourceID, defID, leaseB)
		p26ForceExpire(t, r.Pool, runB.ID)
		claimB, _ := p26RecoveryClaim(t, r, runB.ID, tenant)
		intentB, ierr := r.GetPublishIntentMaterial(ctx, runB.ID, tenant)
		require.NoError(t, ierr)
		_, err = r.FinalizeRecoveredIncrementalRun(ctx, claimB, tenant, p26ReceiptFor(*intentB))
		require.NoError(t, err)

		runBefore := runRowJSON(t, r.Pool, runA)
		cpBefore := p25CpJSON(t, r.Pool, defID)
		res, cerr := r.ClaimRecovery(ctx, runA, tenant, "late", "late-tok", time.Minute)
		require.NoError(t, cerr)
		require.NotNil(t, res, "never a nil result with nil error")
		assert.Equal(t, RecoveryClaimAlreadyResolved, res.Outcome,
			"recognized BEFORE the current (advanced) CP/owner/binding state")
		assert.Nil(t, res.Claim, "already_resolved installs no lease")
		assert.True(t, res.IntentExists)
		assert.Equal(t, IntentStateFinalized, res.Intent.RecoveryState)
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runA), "read-only: zero writes")
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID), "batch B's advanced checkpoint untouched")
	})

	t.Run("terminal_failed_with_intent_not_eligible_zero_write", func(t *testing.T) {
		// Out-of-band residual shape: a post-gate 'failed' written by a
		// pre-v2.1 executor build (today's public paths refuse it — the
		// UPDATE simulates the historical residue on the test-owned DB).
		runC := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
		leaseC := p26Claim(t, r, runC, tenant, time.Minute)
		p26Gate(t, r, runC, tenant, sourceID, defID, leaseC)
		_, terr := r.Pool.Exec(ctx,
			`UPDATE sync_runs SET status = 'failed', finished_at = clock_timestamp() WHERE id = $1`, runC)
		require.NoError(t, terr)

		runBefore := runRowJSON(t, r.Pool, runC)
		res, cerr := r.ClaimRecovery(ctx, runC, tenant, "late", "late-tok", time.Minute)
		require.NoError(t, cerr)
		require.NotNil(t, res)
		assert.Equal(t, RecoveryClaimNotEligible, res.Outcome,
			"terminal adoption logic is removed — explicit zero-write refusal")
		assert.Nil(t, res.Claim)
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runC),
			"no lease/attempt/generation mutation on a terminal row")
	})
}

// ---------------------------------------------------------------------------
// T13. Gate parent-identity anchor (item 5): zero-write refusal on mismatch
// ---------------------------------------------------------------------------

// TestPGP26GateMaterialParentMismatchZeroWrite proves the gate refuses —
// before any INSERT — a material whose lineage names other parents than the
// run's frozen source/def, even though the body itself is self-consistent.
func TestPGP26GateMaterialParentMismatchZeroWrite(t *testing.T) {
	r, _, tenant, sourceID, defID := p26Fixture(t, p26IncrementalConfig())
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	lease := p26Claim(t, r, runID, tenant, time.Minute)

	// Self-consistent material for a DIFFERENT (source, def, run) triple.
	foreign := ids(t)
	foreignMat := validIncrementalMaterial(t, foreign)
	// Re-key the body to THIS run so only the PARENTS mismatch.
	foreignMat.ExactBody = buildBody(t, DerivePublishKey(runID), foreignMat.Branch,
		foreignMat.LogicalPath, foreignMat.ContentHash, foreignMat.Rows,
		[]byte("{\"p\":1}\n"), foreign.sourceID, foreign.defID, runID)
	foreignMat.SourceID, foreignMat.SyncDefID = foreign.sourceID, foreign.defID

	runBefore := runRowJSON(t, r.Pool, runID)
	_, err := r.BeginRecoverableIncrementalPublishGate(context.Background(), lease, tenant, foreignMat)
	assert.ErrorIs(t, err, ErrRecoveryMaterialParentMismatch)

	assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID))
	var intentCount int
	require.NoError(t, r.Pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM sync_run_publish_intents WHERE run_id = $1`, runID).Scan(&intentCount))
	assert.Zero(t, intentCount)
	var gate *time.Time
	require.NoError(t, r.Pool.QueryRow(context.Background(),
		`SELECT publish_entered_at FROM sync_runs WHERE id = $1`, runID).Scan(&gate))
	assert.Nil(t, gate)
}
