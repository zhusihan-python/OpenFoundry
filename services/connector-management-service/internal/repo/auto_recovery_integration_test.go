//go:build integration

// P2.6 automatic recovery seam — real PostgreSQL 16 integration tests
// (SOURCE-ONLY stage: compiled, NOT executed; the real-PG run belongs to the
// separately reviewed W6 command). Fixtures reuse the P24 arm harness
// (p24Arm) and the P26 helpers verbatim; the SNAPSHOT v1 config is used so
// several runs may coexist on one def (no incremental single-flight).
package repo

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// p26AutoFixture boots a snapshot-v1 fixture and enqueues one pending run
// whose ORIGINAL requester is the tenant's owner (the arm actor identity).
func p26AutoFixture(t *testing.T) (*Repo, string, Tenant, uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()
	r, dsn, tenant, sourceID, defID := p26Fixture(t, p26SnapshotConfig())
	requester := tenant.OwnerID
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, requester)
	return r, dsn, tenant, sourceID, defID, runID
}

// p26ArmFor arms the def's schedule grant for the fixture actor. The arm's
// local ExpiresAt is re-anchored to the DB view's value: PG stores
// microseconds, time.Now() carries nanoseconds, and the production claim
// adjudicates EXACT equality (arm.ExpiresAt.Equal(grant.ExpiresAt)) — a
// fixture that skips this would depend on rounding luck.
func p26ArmFor(t *testing.T, r *Repo, tenant Tenant, defID, sourceID uuid.UUID) *ScheduleArmClaim {
	t.Helper()
	output := jobTarget(t, r, defID)
	arm, view := p24Arm(t, r, tenant, defID, sourceID, output, time.Now().Add(10*time.Minute))
	require.NotNil(t, view)
	arm.ExpiresAt = view.ExpiresAt
	return arm
}

// p26AutoZeroWrite asserts a typed refusal left every row byte-identical
// (cpBefore "" skips the checkpoint compare — snapshot defs have no CP row).
func p26AutoZeroWrite(t *testing.T, pool *pgxpool.Pool, runID, defID uuid.UUID, runBefore, cpBefore, intentBefore string) {
	t.Helper()
	assert.Equal(t, runBefore, runRowJSON(t, pool, runID), "refused auto claim: run untouched")
	if cpBefore != "" {
		assert.Equal(t, cpBefore, p25CpJSON(t, pool, defID), "refused auto claim: checkpoint untouched")
	}
	var intentCount int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM sync_run_publish_intents WHERE run_id = $1`, runID).Scan(&intentCount))
	if intentBefore == "" {
		assert.Zero(t, intentCount, "refused auto claim: no intent created")
	} else {
		assert.Equal(t, intentBefore, p26IntentJSON(t, pool, runID), "refused auto claim: intent untouched")
	}
}

// ---------------------------------------------------------------------------
// A. Scan: filters + deterministic keyset paging
// ---------------------------------------------------------------------------

// TestPGP26AutoRecoveryScanKeysetAndFilters pins the lockless scan contract:
// pending v1 eligible (ReadyAt=started_at), live-lease running excluded,
// not-yet-actionable and query-exhausted intents excluded, send-exhausted
// with query budget remaining INCLUDED, terminal and legacy rows excluded,
// and (ReadyAt, RunID) keyset paging with full coverage and no repeats.
func TestPGP26AutoRecoveryScanKeysetAndFilters(t *testing.T) {
	r, _, tenant, sourceID, defID, _ := p26AutoFixture(t)
	ctx := context.Background()

	// A legacy (protocol NULL) def+run in the SAME database.
	legacyTenant := Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}
	legacyConn, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{Name: "legacy", ConnectorType: "rest_api"}, legacyTenant)
	require.NoError(t, err)
	legacyJob, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{SourceID: legacyConn.ID, OutputDatasetID: uuid.New()}, legacyTenant)
	require.NoError(t, err)
	leaseEnqueue(t, r, legacyTenant, legacyConn.ID, legacyJob.ID, uuid.New())

	pending := leaseEnqueue(t, r, tenant, sourceID, defID, tenant.OwnerID)

	// running with a LIVE lease — excluded.
	live := leaseEnqueue(t, r, tenant, sourceID, defID, tenant.OwnerID)
	p26Claim(t, r, live, tenant, time.Minute)

	// running expired + intent waiting with next_action_at in the future —
	// excluded until due.
	deferred := leaseEnqueue(t, r, tenant, sourceID, defID, tenant.OwnerID)
	leaseD := p26Claim(t, r, deferred, tenant, time.Minute)
	p26Gate(t, r, deferred, tenant, sourceID, defID, leaseD)
	p26ForceExpire(t, r.Pool, deferred)
	_, err = r.Pool.Exec(ctx,
		`UPDATE sync_run_publish_intents SET recovery_state='waiting', next_action_at = clock_timestamp() + interval '1 hour'
		  WHERE run_id = $1`, deferred)
	require.NoError(t, err)

	// running expired + send-exhausted with query budget remaining —
	// INCLUDED (independent budgets).
	sendOnly := leaseEnqueue(t, r, tenant, sourceID, defID, tenant.OwnerID)
	leaseS := p26Claim(t, r, sendOnly, tenant, time.Minute)
	p26Gate(t, r, sendOnly, tenant, sourceID, defID, leaseS)
	p26ForceExpire(t, r.Pool, sendOnly)
	claimS, _ := p26RecoveryClaim(t, r, sendOnly, tenant)
	for i := 0; i < MaxPublishSendAttempts; i++ {
		_, err = r.ConsumePublishSendBudget(ctx, claimS, tenant)
		require.NoError(t, err)
	}
	p26ForceExpire(t, r.Pool, sendOnly)

	// terminal failed v1 (pre-gate close) — excluded.
	terminal := leaseEnqueue(t, r, tenant, sourceID, defID, tenant.OwnerID)
	leaseT := p26Claim(t, r, terminal, tenant, time.Minute)
	msg := "done"
	_, err = r.CompleteLeasedSyncRun(ctx, terminal, tenant, leaseT.Holder, leaseT.Token, leaseT.Run.Generation,
		"failed", 0, 0, 0, &msg, nil, nil, nil)
	require.NoError(t, err)

	candidates, err := r.ListRecoveryCandidates(ctx, time.Now().Add(time.Hour), nil, 100)
	require.NoError(t, err)
	got := map[uuid.UUID]bool{}
	for _, c := range candidates {
		got[c.RunID] = true
		assert.NotEqual(t, legacyJob.ID, c.SyncDefID, "legacy def never appears")
	}
	assert.True(t, got[pending], "pending v1 is a candidate")
	assert.True(t, got[sendOnly], "send-exhausted with query budget remaining is a candidate")
	assert.False(t, got[live], "live lease is not a candidate")
	assert.False(t, got[deferred], "not-yet-actionable intent is not a candidate")
	assert.False(t, got[terminal], "terminal run is not a candidate")
	assert.False(t, got[legacyRunID(t, r, legacyJob.ID)], "legacy run never appears")

	// Keyset paging with a tiny page: deterministic order, full coverage, no
	// duplicates across pages.
	paged := map[uuid.UUID]bool{}
	after := (*RecoveryScanKey)(nil)
	pages := 0
	for {
		page, perr := r.ListRecoveryCandidates(ctx, time.Now().Add(time.Hour), after, 1)
		require.NoError(t, perr)
		if len(page) == 0 {
			break
		}
		for _, c := range page {
			require.False(t, paged[c.RunID], "keyset paging must not repeat a run")
			paged[c.RunID] = true
		}
		last := page[len(page)-1]
		after = &RecoveryScanKey{ReadyAt: last.ReadyAt, RunID: last.RunID}
		pages++
		if pages > 50 {
			t.Fatal("keyset paging did not terminate")
		}
	}
	assert.Equal(t, got, paged, "single-page and paged scans agree exactly")
}

// legacyRunID reads the legacy def's pending run id (negative-scan assert).
func legacyRunID(t *testing.T, r *Repo, defID uuid.UUID) uuid.UUID {
	t.Helper()
	var runID uuid.UUID
	require.NoError(t, r.Pool.QueryRow(context.Background(),
		`SELECT r.id FROM sync_runs r WHERE r.sync_def_id = $1 ORDER BY r.started_at, r.id LIMIT 1`,
		defID).Scan(&runID))
	return runID
}

// ---------------------------------------------------------------------------
// B. ClaimAutomaticRecovery grant matrix (typed zero-write refusals)
// ---------------------------------------------------------------------------

// TestPGP26ClaimAutomaticRecoveryGrantMatrix covers: the correct fresh-arm
// claim (granted, pending fresh-execution shape) and every refusal — no
// grant row, wrong boot, wrong revision, foreign domain, local arm expiry,
// stale owner heartbeat, actor mismatch, DB grant expiry, not-due (future
// next_action_at / exhausted query budget) — each zero-write with the typed
// outcome and no grant material leaked.
func TestPGP26ClaimAutomaticRecoveryGrantMatrix(t *testing.T) {
	r, _, tenant, sourceID, defID, fixtureRun := p26AutoFixture(t)
	ctx := context.Background()

	t.Run("fresh_arm_grants_pending_fresh_execution", func(t *testing.T) {
		arm := p26ArmFor(t, r, tenant, defID, sourceID)
		res, err := r.ClaimAutomaticRecovery(ctx, fixtureRun, arm, "auto", "auto-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, RecoveryClaimGranted, res.Outcome)
		assert.False(t, res.IntentExists)
		assert.Equal(t, models.RunRecoveryProtocolP26V1, res.Claim.Run.RecoveryProtocol)
	})

	refusal := func(t *testing.T, name string, mutate func(arm *ScheduleArmClaim), db func(t *testing.T)) {
		t.Run(name, func(t *testing.T) {
			runID := leaseEnqueue(t, r, tenant, sourceID, defID, tenant.OwnerID)
			arm := p26ArmFor(t, r, tenant, defID, sourceID)
			if mutate != nil {
				mutate(arm)
			}
			if db != nil {
				db(t)
			}
			runBefore := runRowJSON(t, r.Pool, runID)
			res, err := r.ClaimAutomaticRecovery(ctx, runID, arm, "auto", "auto-tok", time.Minute)
			require.NoError(t, err)
			require.NotNil(t, res, "never a nil result with nil error")
			assert.Equal(t, RecoveryClaimAuthRequired, res.Outcome, "typed refusal")
			assert.Nil(t, res.Claim)
			assert.NotContains(t, string(res.Outcome), arm.GrantID.String(), "no grant material leaks")
			p26AutoZeroWrite(t, r.Pool, runID, defID, runBefore, "", "")
		})
	}

	// Each refusal arms a FRESH grant (revoked_grant removes the row first).
	refusal(t, "no_grant_row", nil, func(t *testing.T) {
		_, err := r.RevokeScheduleGrant(ctx, defID, tenant)
		require.NoError(t, err)
	})
	refusal(t, "wrong_boot", func(arm *ScheduleArmClaim) { arm.BootID = uuid.New() }, nil)
	refusal(t, "wrong_revision", func(arm *ScheduleArmClaim) { arm.Revision++ }, nil)
	refusal(t, "local_arm_expired", func(arm *ScheduleArmClaim) {
		arm.ExpiresAt = time.Now().Add(-time.Minute)
	}, nil)
	refusal(t, "stale_owner_heartbeat", nil, func(t *testing.T) {
		_, err := r.Pool.Exec(ctx,
			`UPDATE sync_schedule_grants SET last_seen_at = clock_timestamp() - interval '200 seconds'`)
		require.NoError(t, err)
	})

	t.Run("foreign_domain_tenant_is_not_visible", func(t *testing.T) {
		// A FULLY foreign tenant cannot even see the run — the honest answer
		// is NotVisible (the tenant predicate isolates before any grant
		// adjudication), NOT auth_required. Asserted separately from the
		// refusal helper; production isolation is untouched.
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, tenant.OwnerID)
		arm := p26ArmFor(t, r, tenant, defID, sourceID)
		foreignArm := *arm
		foreignArm.Tenant = Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}
		runBefore := runRowJSON(t, r.Pool, runID)
		res, err := r.ClaimAutomaticRecovery(ctx, runID, &foreignArm, "auto", "auto-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, RecoveryClaimNotVisible, res.Outcome,
			"a fully foreign tenant answers not_visible — isolation precedes grant adjudication")
		assert.Nil(t, res.Claim)
		p26AutoZeroWrite(t, r.Pool, runID, defID, runBefore, "", "")
	})

	t.Run("actor_mismatch_requested_by", func(t *testing.T) {
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New()) // NOT the arm actor
		arm := p26ArmFor(t, r, tenant, defID, sourceID)
		runBefore := runRowJSON(t, r.Pool, runID)
		res, err := r.ClaimAutomaticRecovery(ctx, runID, arm, "auto", "auto-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, RecoveryClaimAuthRequired, res.Outcome, "automatic adoption never crosses identities")
		p26AutoZeroWrite(t, r.Pool, runID, defID, runBefore, "", "")
	})

	t.Run("db_grant_expired", func(t *testing.T) {
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, tenant.OwnerID)
		output := jobTarget(t, r, defID)
		shortExpiry := time.Now().Add(150 * time.Millisecond)
		arm, view := p24Arm(t, r, tenant, defID, sourceID, output, shortExpiry)
		require.NotNil(t, view)
		arm.ExpiresAt = view.ExpiresAt // re-anchor to the DB microsecond value
		time.Sleep(300 * time.Millisecond)
		runBefore := runRowJSON(t, r.Pool, runID)
		res, err := r.ClaimAutomaticRecovery(ctx, runID, arm, "auto", "auto-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, RecoveryClaimAuthRequired, res.Outcome, "expired DB grant refuses adoption")
		p26AutoZeroWrite(t, r.Pool, runID, defID, runBefore, "", "")
	})

	notDue := func(t *testing.T, name string, intentSQL string) {
		t.Run(name, func(t *testing.T) {
			runID := leaseEnqueue(t, r, tenant, sourceID, defID, tenant.OwnerID)
			lease := p26Claim(t, r, runID, tenant, time.Minute)
			p26Gate(t, r, runID, tenant, sourceID, defID, lease)
			p26ForceExpire(t, r.Pool, runID)
			_, err := r.Pool.Exec(ctx, intentSQL, runID)
			require.NoError(t, err)
			arm := p26ArmFor(t, r, tenant, defID, sourceID)
			intentBefore := p26IntentJSON(t, r.Pool, runID)
			runBefore := runRowJSON(t, r.Pool, runID)
			res, err := r.ClaimAutomaticRecovery(ctx, runID, arm, "auto", "auto-tok", time.Minute)
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, RecoveryClaimNotDue, res.Outcome, "not actionable for the automated path")
			p26AutoZeroWrite(t, r.Pool, runID, defID, runBefore, "", intentBefore)
		})
	}
	notDue(t, "not_due_next_action_future",
		`UPDATE sync_run_publish_intents SET recovery_state='waiting', next_action_at = clock_timestamp() + interval '1 hour' WHERE run_id = $1`)
	notDue(t, "not_due_query_budget_exhausted",
		`UPDATE sync_run_publish_intents SET query_attempts = 5 WHERE run_id = $1`)
}

// ---------------------------------------------------------------------------
// C. Linearization: revoke vs claim race under the pinned lock order
// ---------------------------------------------------------------------------

// TestPGP26AutoRecoveryBlockedThenRevoke proves linearization through the
// pinned Source→Def lock order: while the automatic claim is BLOCKED on the
// Source row, a Def-only revoke commits; once the barrier releases, the
// claim observes the REVOKED grant and answers auth_required with zero
// writes — and the converse (an unblocked claim wins first; a later revoke
// never retroactively un-grants the installed claim lease).
func TestPGP26AutoRecoveryBlockedThenRevoke(t *testing.T) {
	r, dsn, tenant, sourceID, defID, fixtureRun := p26AutoFixture(t)
	ctx := context.Background()

	t.Run("revoke_wins_while_claim_blocked_on_source", func(t *testing.T) {
		arm := p26ArmFor(t, r, tenant, defID, sourceID)

		observer := singleConnRepo(t, dsn)
		blocker := singleConnRepo(t, dsn)
		blockerPID := backendPID(t, blocker.Pool)
		blockTx, err := blocker.Pool.Begin(ctx)
		require.NoError(t, err)
		var locked uuid.UUID
		require.NoError(t, blockTx.QueryRow(ctx,
			`SELECT c.id FROM connections c WHERE c.id = $1 FOR UPDATE`, sourceID).Scan(&locked))
		release := func() { _ = p25BoundedRollback(blockTx) }
		defer release()

		claimSession := singleConnRepo(t, dsn)
		claimPID := backendPID(t, claimSession.Pool)
		type res struct {
			out *RecoveryClaimResult
			err error
		}
		ch := make(chan res, 1)
		go func() {
			cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			out, cerr := claimSession.ClaimAutomaticRecovery(cctx, fixtureRun, arm, "auto", "auto-tok", time.Minute)
			ch <- res{out, cerr}
		}()
		waitUntilBlockedBy(t, observer.Pool, claimPID, blockerPID)

		// The revoke is Def-only: it linearizes BEFORE the blocked claim.
		_, err = r.RevokeScheduleGrant(ctx, defID, tenant)
		require.NoError(t, err)

		runBefore := runRowJSON(t, r.Pool, fixtureRun)
		release()
		got := <-ch
		require.NoError(t, got.err)
		require.NotNil(t, got.out)
		assert.Equal(t, RecoveryClaimAuthRequired, got.out.Outcome,
			"the claim must observe the revoke that linearized before it")
		assert.Nil(t, got.out.Claim)
		assert.Equal(t, runBefore, runRowJSON(t, r.Pool, fixtureRun), "zero writes")
	})

	t.Run("claim_wins_before_revoke", func(t *testing.T) {
		runID := leaseEnqueue(t, r, tenant, sourceID, defID, tenant.OwnerID)
		arm := p26ArmFor(t, r, tenant, defID, sourceID)
		res, err := r.ClaimAutomaticRecovery(ctx, runID, arm, "auto", "auto-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, RecoveryClaimGranted, res.Outcome)
		// The claim installed its lease; a later revoke removes FUTURE
		// authorizations but never retroactively un-grants the claim.
		_, err = r.RevokeScheduleGrant(ctx, defID, tenant)
		require.NoError(t, err)
		var generation int
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT generation FROM sync_runs WHERE id = $1`, runID).Scan(&generation))
		assert.Equal(t, res.Claim.Run.Generation, generation, "the installed claim is untouched by the later revoke")
	})
}
