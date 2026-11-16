//go:build integration

// Real PostgreSQL 16 regressions for the P2.1 durable sync-run task and its
// lease. Everything here proves DATABASE-level semantics against multiple
// independent sessions: two-phase post-lock clock re-checks (a blocker that
// only holds FOR UPDATE, never writes — verified through pg_stat_activity /
// pg_blocking_pids, not inferred from a timeout), TTLs anchored to the
// post-lock DB clock, exactly-one-winner claims, fencing of expired/taken
// over leases (no ABA), tenant isolation of every lease operation,
// legacy-path guards, active-period mutation guards, migration
// upgrade/re-run safety, and parameter validation that must not touch a
// single column.
package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// leaseFixtureDomain provisions a migrated database with one source and one
// sync def in the org domain (org=true) or the personal domain (org=false).
// Returns the DSN so tests can open additional independent sessions.
func leaseFixtureDomain(t *testing.T, org bool) (*Repo, string, Tenant, uuid.UUID, uuid.UUID) {
	t.Helper()
	r, dsn := bootRepoWithDSN(t)
	ctx := context.Background()
	require.NoError(t, Migrate(ctx, r.Pool))
	tenant := Tenant{OwnerID: uuid.New()}
	if org {
		tenant.OrgID = orgPtr(uuid.New())
	}
	conn, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{Name: "rest", ConnectorType: "rest_api"}, tenant)
	require.NoError(t, err)
	require.NotNil(t, conn)
	job, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{SourceID: conn.ID, OutputDatasetID: uuid.New()}, tenant)
	require.NoError(t, err)
	require.NotNil(t, job)
	return r, dsn, tenant, conn.ID, job.ID
}

func leaseFixture(t *testing.T) (*Repo, string, Tenant, uuid.UUID, uuid.UUID) {
	t.Helper()
	return leaseFixtureDomain(t, true)
}

// singleConnRepo opens an independent single-connection pool on the DSN: a
// dedicated PostgreSQL session (stable backend pid) for multi-connection
// races.
func singleConnRepo(t *testing.T, dsn string) *Repo {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return &Repo{Pool: pool}
}

// backendPID returns the backend pid of a single-connection pool; calling it
// also warms the connection so a later operation on the same pool uses the
// same session.
func backendPID(t *testing.T, pool *pgxpool.Pool) int32 {
	t.Helper()
	var pid int32
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&pid))
	return pid
}

// waitUntilBlockedBy polls pg_stat_activity/pg_blocking_pids until `pid` is
// actually waiting on a lock held by `blocker` — proof that the operation
// under test genuinely entered the row-lock wait (PG16 functions per
// postgres.org/docs/16/functions-info.html and monitoring-stats.html).
func waitUntilBlockedBy(t *testing.T, observer *pgxpool.Pool, pid, blocker int32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waitType *string
		var blockers []int32
		err := observer.QueryRow(context.Background(),
			`SELECT wait_event_type, pg_blocking_pids(pid) FROM pg_stat_activity WHERE pid = $1`,
			pid).Scan(&waitType, &blockers)
		if err == nil && waitType != nil && *waitType == "Lock" {
			for _, b := range blockers {
				if b == blocker {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid %d never waited on a lock held by %d", pid, blocker)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// dbClock reads the database clock through an observer session.
func dbClock(t *testing.T, pool *pgxpool.Pool) time.Time {
	t.Helper()
	var now time.Time
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT clock_timestamp()`).Scan(&now))
	return now
}

// runRowJSON snapshots the COMPLETE sync_runs row (every column, including
// lease columns) as comparable text.
func runRowJSON(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) string {
	t.Helper()
	var js string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT row_to_json(r.*)::text FROM sync_runs r WHERE r.id = $1`, runID).Scan(&js))
	return js
}

// tableRowJSON snapshots a full row of one of the whitelisted tables.
func tableRowJSON(t *testing.T, pool *pgxpool.Pool, table string, id uuid.UUID) string {
	t.Helper()
	switch table {
	case "connections", "batch_sync_defs", "sync_runs":
	default:
		t.Fatalf("table %q not allowed", table)
	}
	var js string
	require.NoError(t, pool.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT row_to_json(t.*)::text FROM %s t WHERE t.id = $1`, table), id).Scan(&js))
	return js
}

// waitForDBExpiry polls the DATABASE clock until it has moved past the run's
// lease_expires_at. Bounded, short sleeps only — never a long blind sleep.
func waitForDBExpiry(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var expired bool
		err := pool.QueryRow(context.Background(),
			`SELECT clock_timestamp() >= r.lease_expires_at FROM sync_runs r WHERE r.id = $1`,
			runID).Scan(&expired)
		require.NoError(t, err)
		if expired {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("lease never expired within the bounded wait")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func leaseEnqueue(t *testing.T, r *Repo, tenant Tenant, sourceID, defID uuid.UUID, requester uuid.UUID) uuid.UUID {
	t.Helper()
	run, err := r.EnqueueSyncRun(context.Background(), defID, sourceID, tenant, requester)
	require.NoError(t, err)
	require.NotNil(t, run)
	assert.Equal(t, "pending", run.Status)
	return run.ID
}

func jobTarget(t *testing.T, r *Repo, defID uuid.UUID) uuid.UUID {
	t.Helper()
	var target uuid.UUID
	require.NoError(t, r.Pool.QueryRow(context.Background(),
		`SELECT output_dataset_id FROM batch_sync_defs WHERE id = $1`, defID).Scan(&target))
	return target
}

func TestPGLeaseEnqueueCommitsAndProvenance(t *testing.T) {
	r, dsn, tenant, sourceID, defID := leaseFixture(t)
	ctx := context.Background()

	// A second session must see the pending row immediately — proves the
	// enqueue COMMITTED (a rolled-back insert would be invisible).
	requester := uuid.New() // a different subject than the source owner
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, requester)

	other := singleConnRepo(t, dsn)
	var status string
	var requestedBy, ownerID *uuid.UUID
	require.NoError(t, other.Pool.QueryRow(ctx,
		`SELECT status, requested_by, owner_id FROM sync_runs WHERE id = $1`, runID).
		Scan(&status, &requestedBy, &ownerID))
	assert.Equal(t, "pending", status)
	require.NotNil(t, requestedBy, "requested_by records the triggering JWT subject")
	assert.Equal(t, requester, *requestedBy)
	require.NotNil(t, ownerID, "owner_id snapshots the SOURCE owner")
	assert.Equal(t, tenant.OwnerID, *ownerID, "owner_id is the source owner, not the requester")

	// org_id follows the def; cross-tenant enqueue is a clean no-op.
	foreign := Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}
	none, err := r.EnqueueSyncRun(ctx, defID, sourceID, foreign, requester)
	require.NoError(t, err)
	assert.Nil(t, none)
}

func TestPGLeaseConcurrentClaimExactlyOneWinner(t *testing.T) {
	r, dsn, tenant, sourceID, defID := leaseFixture(t)
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())

	const contenders = 8
	results := make([]*LeaseClaim, contenders)
	errs := make([]error, contenders)
	start := make(chan struct{})
	var wg sync.WaitGroup
	repos := make([]*Repo, contenders)
	for i := range repos {
		repos[i] = singleConnRepo(t, dsn)
	}
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			claim, err := repos[i].ClaimSyncRun(ctx, runID, tenant,
				fmt.Sprintf("holder-%d", i), fmt.Sprintf("token-%d-%s", i, runID), time.Minute)
			results[i], errs[i] = claim, err
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	for i := 0; i < contenders; i++ {
		require.NoError(t, errs[i], "contender %d", i)
		if results[i] != nil {
			winners++
			assert.Equal(t, 1, results[i].Run.Attempt, "first claim is attempt 1")
			assert.Equal(t, 1, results[i].Run.Generation, "first claim is generation 1")
			assert.Equal(t, fmt.Sprintf("holder-%d", i), results[i].Holder)
		}
	}
	assert.Equal(t, 1, winners, "exactly one connection may claim a pending run")

	late := singleConnRepo(t, dsn)
	lateClaim, err := late.ClaimSyncRun(context.Background(), runID, tenant, "late", "late-token", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, lateClaim)
}

func TestPGLeaseRenewCompleteRejectWrongFencingMaterial(t *testing.T) {
	r, _, tenant, sourceID, defID := leaseFixture(t)
	ctx := context.Background()
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	claim, err := r.ClaimSyncRun(ctx, runID, tenant, "h1", "t1", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claim)
	before := runRowJSON(t, r.Pool, runID)

	cases := []struct {
		name       string
		holder     string
		token      string
		generation int
		tenant     Tenant
	}{
		{"wrong holder", "h2", "t1", 1, tenant},
		{"wrong token", "h1", "t2", 1, tenant},
		{"wrong generation", "h1", "t1", 2, tenant},
		{"wrong tenant", "h1", "t1", 1, Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}},
	}
	for _, tc := range cases {
		renewed, err := r.RenewSyncRunLease(ctx, runID, tc.tenant, tc.holder, tc.token, tc.generation, time.Minute)
		require.NoError(t, err, tc.name)
		assert.Nil(t, renewed, tc.name)
	}
	hash := "deadbeef"
	completed, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "h1", "t-wrong", 1, "failed", 0, 0, 0, nil, nil, nil, &hash)
	require.NoError(t, err)
	assert.Nil(t, completed, "complete must fence on token mismatch")

	assert.Equal(t, before, runRowJSON(t, r.Pool, runID), "every rejected CAS must leave the full row untouched")

	// The right material still works, then terminal closes exactly once.
	renewed, err := r.RenewSyncRunLease(ctx, runID, tenant, "h1", "t1", 1, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, renewed)
	// P2.2 stage machine: 'succeeded' is only legal after the publish gate was
	// entered (and no cancel won first). The legal precondition replaces the
	// old direct-complete arrangement; the fencing assertions above/below are
	// unchanged.
	gate, err := r.BeginPublishGate(ctx, runID, tenant, "h1", "t1", 1, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, gate)
	assert.Equal(t, PublishGateEntered, gate.Outcome)
	version := uuid.New()
	done, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "h1", "t1", 1, "succeeded", 10, 1, 2, nil, nil, &version, &hash)
	require.NoError(t, err)
	require.NotNil(t, done)
	assert.Equal(t, "succeeded", done.Status)
	again, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "h1", "t1", 1, "failed", 0, 0, 0, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, again, "terminal states are immutable")
}

// TestPGLeaseRowLockExpiryClockRegression is the core READ COMMITTED clock
// proof. A blocker holds the run row FOR UPDATE WITHOUT writing it; the
// holder's renew/complete enter the row-lock wait (verified via
// pg_stat_activity/pg_blocking_pids, with the DB clock still INSIDE the
// lease at wait entry — so a single-statement impl would already have
// evaluated and passed its clock qual); the lease expires while they wait;
// the blocker ROLLs BACK (no new row version, so EvalPlanQual never runs).
// The two-phase CAS must still reject via the post-lock clock_timestamp()
// re-check, and the full row must be unchanged.
func TestPGLeaseRowLockExpiryClockRegression(t *testing.T) {
	for _, op := range []string{"renew", "complete"} {
		t.Run(op, func(t *testing.T) {
			r, dsn, tenant, sourceID, defID := leaseFixture(t)
			ctx := context.Background()
			runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
			claim, err := r.ClaimSyncRun(ctx, runID, tenant, "h1", "t1", 2*time.Second)
			require.NoError(t, err)
			require.NotNil(t, claim)
			before := runRowJSON(t, r.Pool, runID)

			// Blocker session: lock the row, never write it.
			blocker, err := pgx.Connect(ctx, dsn)
			require.NoError(t, err)
			t.Cleanup(func() { _ = blocker.Close(ctx) })
			var blockerPID int32
			require.NoError(t, blocker.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID))
			txB, err := blocker.Begin(ctx)
			require.NoError(t, err)
			var locked uuid.UUID
			require.NoError(t, txB.QueryRow(ctx,
				`SELECT id FROM sync_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&locked))

			holder := singleConnRepo(t, dsn)
			holderPID := backendPID(t, holder.Pool)
			res := make(chan error, 1)
			go func() {
				opCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				switch op {
				case "renew":
					c, err := holder.RenewSyncRunLease(opCtx, runID, tenant, "h1", "t1", 1, time.Minute)
					switch {
					case err != nil:
						res <- err
					case c == nil:
						res <- nil // fenced, as required
					default:
						res <- fmt.Errorf("renew succeeded after expiry-behind-lock: must have been rejected")
					}
				case "complete":
					version := uuid.New()
					c, err := holder.CompleteLeasedSyncRun(opCtx, runID, tenant, "h1", "t1", 1, "succeeded", 1, 1, 1, nil, nil, &version, nil)
					switch {
					case err != nil:
						res <- err
					case c == nil:
						res <- nil
					default:
						res <- fmt.Errorf("complete succeeded after expiry-behind-lock: must have been rejected")
					}
				}
			}()

			// Prove the op genuinely waits on the blocker's lock...
			waitUntilBlockedBy(t, r.Pool, holderPID, blockerPID)
			// ...while the lease is still inside its window (otherwise the
			// scenario would be indistinguishable from a late start).
			var stillValid bool
			require.NoError(t, r.Pool.QueryRow(ctx,
				`SELECT clock_timestamp() < lease_expires_at FROM sync_runs WHERE id = $1`,
				runID).Scan(&stillValid))
			require.True(t, stillValid, "test invalid: lease already expired at wait entry")

			// Let the DB clock pass lease_expires_at, then release the lock
			// WITHOUT producing a new row version (rollback).
			waitForDBExpiry(t, r.Pool, runID)
			require.NoError(t, txB.Rollback(ctx))

			select {
			case err := <-res:
				require.NoError(t, err, "fenced op must return a clean rejection, not a storage error")
			case <-time.After(15 * time.Second):
				t.Fatal("operation never returned after lock release")
			}
			assert.Equal(t, before, runRowJSON(t, r.Pool, runID),
				"full row must be unchanged after the post-lock expiry rejection")
		})
	}
}

// TestPGLeaseTTLAnchoredToPostLockClock is the positive counterpart of the
// clock regression: a claim/renew that waits behind a row lock for LONGER
// than its own TTL must still install a lease whose expiry is anchored to the
// post-lock database clock — never born expired. A wait-time-anchored (or
// transaction-now-anchored) implementation would return an expiry that is
// already in the past once the lock is granted.
func TestPGLeaseTTLAnchoredToPostLockClock(t *testing.T) {
	for _, op := range []string{"renew", "claim"} {
		t.Run(op, func(t *testing.T) {
			r, dsn, tenant, sourceID, defID := leaseFixture(t)
			ctx := context.Background()
			newTTL := 400 * time.Millisecond

			// Both variants need a concrete row: the renew variant claims a
			// long-lived lease; the claim variant leaves the pending row
			// unclaimed so the blocked operation is the claim itself.
			runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
			if op == "renew" {
				claim, err := r.ClaimSyncRun(ctx, runID, tenant, "h1", "t1", 5*time.Second)
				require.NoError(t, err)
				require.NotNil(t, claim, "original lease must be long enough to survive the block")
			}

			blocker, err := pgx.Connect(ctx, dsn)
			require.NoError(t, err)
			t.Cleanup(func() { _ = blocker.Close(ctx) })
			var blockerPID int32
			require.NoError(t, blocker.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID))
			txB, err := blocker.Begin(ctx)
			require.NoError(t, err)
			_, err = txB.Exec(ctx, `SELECT id FROM sync_runs WHERE id = $1 FOR UPDATE`, runID)
			require.NoError(t, err)

			holder := singleConnRepo(t, dsn)
			holderPID := backendPID(t, holder.Pool)
			type out struct {
				claim *LeaseClaim
				run   *models.SyncRun
				err   error
			}
			res := make(chan out, 1)
			go func() {
				opCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				switch op {
				case "renew":
					c, err := holder.RenewSyncRunLease(opCtx, runID, tenant, "h1", "t1", 1, newTTL)
					res <- out{claim: c, err: err}
				case "claim":
					c, err := holder.ClaimSyncRun(opCtx, runID, tenant, "h1", "t1", newTTL)
					res <- out{claim: c, err: err}
				}
			}()

			waitUntilBlockedBy(t, r.Pool, holderPID, blockerPID)
			enteredAt := dbClock(t, r.Pool)
			// Hold the lock for LONGER than the new TTL before releasing.
			for dbClock(t, r.Pool).Sub(enteredAt) < 700*time.Millisecond {
				time.Sleep(20 * time.Millisecond)
			}
			unlockBefore := dbClock(t, r.Pool)
			require.NoError(t, txB.Rollback(ctx))

			select {
			case got := <-res:
				require.NoError(t, got.err)
				var expiresAt time.Time
				if op == "renew" {
					require.NotNil(t, got.claim, "renew behind a lock must succeed while its lease is valid")
					expiresAt = got.claim.ExpiresAt
				} else {
					require.NotNil(t, got.claim, "claim behind a lock must succeed on the pending row")
					expiresAt = got.claim.ExpiresAt
				}
				// Anchored to the POST-lock clock: the new lease outlives the
				// unlock instant by (almost) the full TTL. A wait-entry
				// anchored expiry would already be ~300ms in the past here.
				assert.True(t, expiresAt.After(unlockBefore.Add(newTTL-30*time.Millisecond)),
					"lease must be born unexpired and anchored to the post-lock DB clock: expires=%v unlock=%v", expiresAt, unlockBefore)
				var live bool
				require.NoError(t, r.Pool.QueryRow(ctx,
					`SELECT clock_timestamp() < lease_expires_at FROM sync_runs WHERE id = $1`,
					got.claim.Run.ID).Scan(&live))
				assert.True(t, live, "the just-installed lease must be live")
			case <-time.After(15 * time.Second):
				t.Fatal("operation never returned after lock release")
			}
		})
	}
}

// TestPGLeaseExpiryFenceBeforeTakeover proves the expiry window BEFORE anyone
// takes over: an expired lease can neither renew nor complete.
func TestPGLeaseExpiryFenceBeforeTakeover(t *testing.T) {
	r, _, tenant, sourceID, defID := leaseFixture(t)
	ctx := context.Background()
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	claim, err := r.ClaimSyncRun(ctx, runID, tenant, "h1", "t1", 120*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, claim)
	before := runRowJSON(t, r.Pool, runID)

	waitForDBExpiry(t, r.Pool, runID)

	renewed, err := r.RenewSyncRunLease(ctx, runID, tenant, "h1", "t1", 1, time.Minute)
	require.NoError(t, err)
	assert.Nil(t, renewed, "expired lease must not renew even before takeover")
	version := uuid.New()
	done, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "h1", "t1", 1, "succeeded", 1, 1, 1, nil, nil, &version, nil)
	require.NoError(t, err)
	assert.Nil(t, done, "expired lease must not complete even before takeover")
	assert.Equal(t, before, runRowJSON(t, r.Pool, runID))
}

// TestPGLeaseTakeoverFencingAndABA proves takeover rotates generation+token so
// every pre-takeover fencing pair is dead forever — including for the SAME
// holder string reclaiming later (ABA), and that a not-yet-expired lease can
// never be taken over.
func TestPGLeaseTakeoverFencingAndABA(t *testing.T) {
	r, _, tenant, sourceID, defID := leaseFixture(t)
	ctx := context.Background()
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	claim, err := r.ClaimSyncRun(ctx, runID, tenant, "h1", "t1", 120*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, claim)

	// Takeover while the lease is still valid is refused.
	early, err := r.TakeoverExpiredSyncRun(ctx, runID, tenant, "h2", "t2", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, early, "unexpired leases cannot be taken over")

	waitForDBExpiry(t, r.Pool, runID)
	over, err := r.TakeoverExpiredSyncRun(ctx, runID, tenant, "h2", "t2", 120*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, over)
	assert.Equal(t, 2, over.Run.Generation)
	assert.Equal(t, 2, over.Run.Attempt)
	assert.Equal(t, "t2", over.Token)

	// Old holder fenced on both operations.
	renewed, err := r.RenewSyncRunLease(ctx, runID, tenant, "h1", "t1", 1, time.Minute)
	require.NoError(t, err)
	assert.Nil(t, renewed)
	done, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "h1", "t1", 1, "succeeded", 0, 0, 0, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, done)

	// ABA: the ORIGINAL holder string takes over next. Its new claim works,
	// but the original token stays dead forever. Short TTL so the (final)
	// expiry wait stays bounded; only fencing assertions follow it.
	waitForDBExpiry(t, r.Pool, runID)
	again, err := r.TakeoverExpiredSyncRun(ctx, runID, tenant, "h1", "t3", time.Hour)
	require.NoError(t, err)
	require.NotNil(t, again)
	assert.Equal(t, 3, again.Run.Generation)
	zombie, err := r.RenewSyncRunLease(ctx, runID, tenant, "h1", "t1", 1, time.Minute)
	require.NoError(t, err)
	assert.Nil(t, zombie, "token from generation 1 must never revive, even for its own holder")
	live, err := r.RenewSyncRunLease(ctx, runID, tenant, "h1", "t3", 3, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, live, "the current generation's token renews normally")
}

// TestPGLegacyRowsGuarded proves the legacy paths cannot bypass P2.1: legacy
// lease-less running rows close exactly once via CompleteSyncRun, are immune
// to lease ops/takeover, and still block source deletion while running.
func TestPGLegacyRowsGuarded(t *testing.T) {
	r, _, tenant, sourceID, defID := leaseFixture(t)
	ctx := context.Background()

	legacy, err := r.RunSyncJob(ctx, defID, tenant)
	require.NoError(t, err)
	require.NotNil(t, legacy)
	assert.Equal(t, "running", legacy.Status)
	assert.Equal(t, 0, legacy.Generation)

	// Lease ops reject the lease-less row; takeover never claims it.
	renewed, err := r.RenewSyncRunLease(ctx, legacy.ID, tenant, "h", "t", 1, time.Minute)
	require.NoError(t, err)
	assert.Nil(t, renewed)
	over, err := r.TakeoverExpiredSyncRun(ctx, legacy.ID, tenant, "h", "t", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, over, "legacy rows without a lease are never takeover candidates")

	// Legacy completion closes it exactly once...
	version := uuid.New()
	closed, err := r.CompleteSyncRun(ctx, legacy.ID, tenant, "succeeded", 5, 1, 5, nil, nil, &version, nil)
	require.NoError(t, err)
	require.NotNil(t, closed)
	// ...and can never rewrite the terminal state.
	rewrite, err := r.CompleteSyncRun(ctx, legacy.ID, tenant, "failed", 0, 0, 0, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, rewrite)

	// A second legacy run still blocks source deletion while running (the
	// lock-protocol insert participates in the active-period guard).
	active, err := r.RunSyncJob(ctx, defID, tenant)
	require.NoError(t, err)
	require.NotNil(t, active)
	_, err = r.DeleteConnection(ctx, sourceID, tenant)
	assert.ErrorIs(t, err, ErrActiveRuns)
}

// TestPGLeaseTenantIsolationMatrix covers every lease operation against
// cross-domain callers: another personal owner, the same subject inside an
// org (and vice versa), and a foreign org — enqueue, claim, renew, complete
// and the page-gate check all refuse with the full row unchanged. The P1
// CRUD matrix does not substitute for the new lease SQL.
func TestPGLeaseTenantIsolationMatrix(t *testing.T) {
	t.Run("personal_domain_row", func(t *testing.T) {
		r, _, personal, sourceID, defID := leaseFixtureDomain(t, false)
		ctx := context.Background()
		runID := leaseEnqueue(t, r, personal, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, runID, personal, "h1", "t1", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		before := runRowJSON(t, r.Pool, runID)

		attackers := map[string]Tenant{
			"other personal owner":  {OwnerID: uuid.New()},
			"same subject in org A": {OwnerID: personal.OwnerID, OrgID: orgPtr(uuid.New())},
			"foreign org admin":     {OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())},
		}
		for name, attacker := range attackers {
			none, err := r.EnqueueSyncRun(ctx, defID, sourceID, attacker, uuid.New())
			require.NoError(t, err, name)
			assert.Nil(t, none, name+": cross-domain enqueue")

			lost, err := r.ClaimSyncRun(ctx, runID, attacker, "h1", "t1", time.Minute)
			require.NoError(t, err, name)
			assert.Nil(t, lost, name+": cross-domain claim")

			renewed, err := r.RenewSyncRunLease(ctx, runID, attacker, "h1", "t1", 1, time.Minute)
			require.NoError(t, err, name)
			assert.Nil(t, renewed, name+": cross-domain renew")

			done, err := r.CompleteLeasedSyncRun(ctx, runID, attacker, "h1", "t1", 1, "succeeded", 0, 0, 0, nil, nil, nil, nil)
			require.NoError(t, err, name)
			assert.Nil(t, done, name+": cross-domain complete")

			ok, err := r.CheckSyncRunLease(ctx, runID, attacker, "h1", "t1", 1)
			require.NoError(t, err, name)
			assert.False(t, ok, name+": cross-domain lease check")
		}
		assert.Equal(t, before, runRowJSON(t, r.Pool, runID))
	})

	t.Run("org_domain_row", func(t *testing.T) {
		r, _, orgTenant, sourceID, defID := leaseFixture(t)
		ctx := context.Background()
		runID := leaseEnqueue(t, r, orgTenant, sourceID, defID, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, runID, orgTenant, "h1", "t1", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		before := runRowJSON(t, r.Pool, runID)

		attackers := map[string]Tenant{
			"same subject personal": {OwnerID: orgTenant.OwnerID},
			"same subject in org B": {OwnerID: orgTenant.OwnerID, OrgID: orgPtr(uuid.New())},
			"other owner in org B":  {OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())},
		}
		for name, attacker := range attackers {
			none, err := r.EnqueueSyncRun(ctx, defID, sourceID, attacker, uuid.New())
			require.NoError(t, err, name)
			assert.Nil(t, none, name)

			lost, err := r.ClaimSyncRun(ctx, runID, attacker, "h1", "t1", time.Minute)
			require.NoError(t, err, name)
			assert.Nil(t, lost, name)

			renewed, err := r.RenewSyncRunLease(ctx, runID, attacker, "h1", "t1", 1, time.Minute)
			require.NoError(t, err, name)
			assert.Nil(t, renewed, name)

			done, err := r.CompleteLeasedSyncRun(ctx, runID, attacker, "h1", "t1", 1, "succeeded", 0, 0, 0, nil, nil, nil, nil)
			require.NoError(t, err, name)
			assert.Nil(t, done, name)

			ok, err := r.CheckSyncRunLease(ctx, runID, attacker, "h1", "t1", 1)
			require.NoError(t, err, name)
			assert.False(t, ok, name)
		}
		assert.Equal(t, before, runRowJSON(t, r.Pool, runID))
	})
}

func TestPGLeaseInvalidArgumentsTouchNothing(t *testing.T) {
	r, _, tenant, sourceID, defID := leaseFixture(t)
	ctx := context.Background()
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	claim, err := r.ClaimSyncRun(ctx, runID, tenant, "h1", "t1", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claim)
	before := runRowJSON(t, r.Pool, runID)

	badInstalls := []struct {
		name   string
		holder string
		token  string
		ttl    time.Duration
	}{
		{"blank holder", "  ", "t", time.Minute},
		{"blank token", "h", "", time.Minute},
		{"zero ttl", "h", "t", 0},
		{"sub-microsecond ttl rounds to zero", "h", "t", 300 * time.Nanosecond},
		{"over-max ttl", "h", "t", 25 * time.Hour},
	}
	for _, tc := range badInstalls {
		c, err := r.ClaimSyncRun(ctx, runID, tenant, tc.holder, tc.token, tc.ttl)
		assert.Error(t, err, tc.name)
		assert.Nil(t, c, tc.name)
		over, err := r.TakeoverExpiredSyncRun(ctx, runID, tenant, tc.holder, tc.token, tc.ttl)
		assert.Error(t, err, tc.name)
		assert.Nil(t, over, tc.name)
	}
	// Renew carries a TTL too — the same bounds apply, with otherwise valid
	// fencing material so ONLY the TTL is invalid.
	for _, tc := range []string{"renew zero ttl", "renew sub-microsecond ttl", "renew over-max ttl"} {
		var ttl time.Duration
		switch tc {
		case "renew zero ttl":
			ttl = 0
		case "renew sub-microsecond ttl":
			ttl = 300 * time.Nanosecond
		default:
			ttl = 25 * time.Hour
		}
		renewed, err := r.RenewSyncRunLease(ctx, runID, tenant, "h1", "t1", 1, ttl)
		assert.Error(t, err, tc)
		assert.Nil(t, renewed, tc)
	}

	badCAS := []struct {
		name       string
		holder     string
		token      string
		generation int
	}{
		{"renew blank holder", "", "t1", 1},
		{"renew blank token", "h1", " ", 1},
		{"renew generation zero", "h1", "t1", 0},
		{"renew negative generation", "h1", "t1", -3},
	}
	for _, tc := range badCAS {
		c, err := r.RenewSyncRunLease(ctx, runID, tenant, tc.holder, tc.token, tc.generation, time.Minute)
		assert.Error(t, err, tc.name)
		assert.Nil(t, c, tc.name)
		done, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, tc.holder, tc.token, tc.generation, "succeeded", 0, 0, 0, nil, nil, nil, nil)
		assert.Error(t, err, tc.name)
		assert.Nil(t, done, tc.name)
	}

	assert.Equal(t, before, runRowJSON(t, r.Pool, runID),
		"invalid arguments must not touch attempt/generation/heartbeat or ANY column")
}

func TestPGActiveGuardsBlockSourceAndTargetMutations(t *testing.T) {
	r, _, tenant, sourceID, defID := leaseFixture(t)
	ctx := context.Background()
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, uuid.New())
	claim, err := r.ClaimSyncRun(ctx, runID, tenant, "h1", "t1", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claim)

	retarget := uuid.New()
	mutations := []struct {
		name string
		call func() error
	}{
		{"delete source", func() error { _, err := r.DeleteConnection(ctx, sourceID, tenant); return err }},
		{"update connection config", func() error {
			_, err := r.UpdateConnection(ctx, sourceID, tenant, &models.UpdateConnectionRequest{Config: json.RawMessage(`{"changed":true}`)})
			return err
		}},
		{"update raw connection config", func() error {
			_, err := r.UpdateConnectionConfig(ctx, sourceID, json.RawMessage(`{"auto_register":{}}`))
			return err
		}},
		{"retarget sync def", func() error {
			_, err := r.UpdateSyncJob(ctx, defID, &models.UpdateSyncJobRequest{OutputDatasetID: &retarget}, tenant)
			return err
		}},
	}
	for _, m := range mutations {
		sourceBefore := tableRowJSON(t, r.Pool, "connections", sourceID)
		defBefore := tableRowJSON(t, r.Pool, "batch_sync_defs", defID)
		runBefore := tableRowJSON(t, r.Pool, "sync_runs", runID)

		require.ErrorIs(t, m.call(), ErrActiveRuns, m.name)

		assert.Equal(t, sourceBefore, tableRowJSON(t, r.Pool, "connections", sourceID), m.name+": source row untouched")
		assert.Equal(t, defBefore, tableRowJSON(t, r.Pool, "batch_sync_defs", defID), m.name+": def row untouched")
		assert.Equal(t, runBefore, tableRowJSON(t, r.Pool, "sync_runs", runID), m.name+": run row untouched")
	}

	// Name-only connection updates and same-target def writes stay allowed.
	renamed := "still-fine"
	updated, err := r.UpdateConnection(ctx, sourceID, tenant, &models.UpdateConnectionRequest{Name: &renamed})
	require.NoError(t, err)
	require.NotNil(t, updated)
	current := jobTarget(t, r, defID)
	same, err := r.UpdateSyncJob(ctx, defID, &models.UpdateSyncJobRequest{OutputDatasetID: &current}, tenant)
	require.NoError(t, err)
	require.NotNil(t, same)

	// After the run reaches a terminal state the pre-P2.1 semantics return.
	// P2.2 stage machine: enter the publish gate first so the succeeded
	// completion is legal; the guard assertions above are unchanged.
	gate, err := r.BeginPublishGate(ctx, runID, tenant, "h1", "t1", 1, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, gate)
	require.Equal(t, PublishGateEntered, gate.Outcome)
	done, err := r.CompleteLeasedSyncRun(ctx, runID, tenant, "h1", "t1", 1, "succeeded", 1, 1, 1, nil, nil, &[]uuid.UUID{uuid.New()}[0], nil)
	require.NoError(t, err)
	require.NotNil(t, done)
	updated, err = r.UpdateConnection(ctx, sourceID, tenant, &models.UpdateConnectionRequest{Config: json.RawMessage(`{"changed":true}`)})
	require.NoError(t, err)
	require.NotNil(t, updated)
	deleted, err := r.DeleteConnection(ctx, sourceID, tenant)
	require.NoError(t, err)
	assert.True(t, deleted)

	// The cascaded delete removed the durable task rows with the source —
	// expected legacy semantics AFTER terminal states.
	var runs int
	require.NoError(t, r.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM sync_runs`).Scan(&runs))
	assert.Equal(t, 0, runs)
}

// TestPGDefLockVsRetargetDeterministic proves the FK KEY SHARE vs FOR UPDATE
// serialization in a DETERMINISTIC order (a pure race could pass even without
// the guard): an uncommitted pending INSERT holds the def's KEY SHARE; the
// retarget genuinely waits on it (verified via pg_blocking_pids); the INSERT
// commits; the retarget is then refused with ErrActiveRuns and both rows are
// unchanged.
func TestPGDefLockVsRetargetDeterministic(t *testing.T) {
	r, dsn, tenant, sourceID, defID := leaseFixture(t)
	ctx := context.Background()

	sessionA, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sessionA.Close(ctx) })
	var pidA int32
	require.NoError(t, sessionA.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pidA))
	txA, err := sessionA.Begin(ctx)
	require.NoError(t, err)
	pendingID := uuid.New()
	// The enqueue's INSERT shape: takes KEY SHARE on the def parent row.
	_, err = txA.Exec(ctx,
		`INSERT INTO sync_runs (id, sync_def_id, status, org_id)
		 SELECT $1, d.id, 'pending', d.org_id FROM batch_sync_defs d WHERE d.id = $2 AND d.source_id = $3`,
		pendingID, defID, sourceID)
	require.NoError(t, err)

	upd := singleConnRepo(t, dsn)
	pidU := backendPID(t, upd.Pool)
	res := make(chan error, 1)
	newTarget := uuid.New()
	go func() {
		opCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		_, err := upd.UpdateSyncJob(opCtx, defID, &models.UpdateSyncJobRequest{OutputDatasetID: &newTarget}, tenant)
		res <- err
	}()

	waitUntilBlockedBy(t, r.Pool, pidU, pidA)
	defBefore := tableRowJSON(t, r.Pool, "batch_sync_defs", defID)

	require.NoError(t, txA.Commit(ctx)) // pending durable task becomes visible
	select {
	case err := <-res:
		require.ErrorIs(t, err, ErrActiveRuns, "retarget must observe the committed pending run post-lock")
	case <-time.After(15 * time.Second):
		t.Fatal("retarget never returned")
	}
	assert.Equal(t, defBefore, tableRowJSON(t, r.Pool, "batch_sync_defs", defID),
		"refused retarget leaves the def (and its target) unchanged")
	var status string
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT status FROM sync_runs WHERE id = $1`, pendingID).Scan(&status))
	assert.Equal(t, "pending", status, "the durable task survives the refused retarget")
}

// TestPGEnqueueVsDeleteBarrier proves the source FOR SHARE protocol for both
// trigger paths: either the delete wins and the trigger cleanly sees no
// source (nil run, no FK error), or the trigger wins and the delete is
// refused with ErrActiveRuns while source and run both survive. The legacy
// variant proves the FIXED legacy insert takes the same parent lock.
func TestPGEnqueueVsDeleteBarrier(t *testing.T) {
	for _, variant := range []string{"enqueue", "legacy_run"} {
		t.Run(variant, func(t *testing.T) {
			r, dsn, tenant, _, _ := leaseFixture(t)
			ctx := context.Background()
			for i := 0; i < 10; i++ {
				conn, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{Name: fmt.Sprintf("del-src-%d-%d", i, 0), ConnectorType: "rest_api"}, tenant)
				require.NoError(t, err)
				require.NotNil(t, conn)
				job, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{SourceID: conn.ID, OutputDatasetID: uuid.New()}, tenant)
				require.NoError(t, err)
				require.NotNil(t, job)

				trigger := singleConnRepo(t, dsn)
				del := singleConnRepo(t, dsn)
				start := make(chan struct{})
				type res struct {
					run *models.SyncRun
					err error
				}
				type delRes struct {
					deleted bool
					err     error
				}
				trigCh, delCh := make(chan res, 1), make(chan delRes, 1)
				go func() {
					<-start
					opCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
					defer cancel()
					switch variant {
					case "enqueue":
						run, err := trigger.EnqueueSyncRun(opCtx, job.ID, conn.ID, tenant, tenant.OwnerID)
						trigCh <- res{run, err}
					case "legacy_run":
						run, err := trigger.RunSyncJob(opCtx, job.ID, tenant)
						trigCh <- res{run, err}
					}
				}()
				go func() {
					<-start
					opCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
					defer cancel()
					deleted, err := del.DeleteConnection(opCtx, conn.ID, tenant)
					delCh <- delRes{deleted, err}
				}()
				close(start)

				var trig res
				select {
				case trig = <-trigCh:
				case <-time.After(20 * time.Second):
					t.Fatalf("iteration %d: trigger never returned", i)
				}
				require.NoError(t, trig.err, "iteration %d: trigger must never surface an FK violation", i)
				var outcome delRes
				select {
				case outcome = <-delCh:
				case <-time.After(20 * time.Second):
					t.Fatalf("iteration %d: delete never returned", i)
				}

				switch {
				case outcome.err == nil && outcome.deleted:
					assert.Nil(t, trig.run, "iteration %d: deleting source first must make the trigger a clean no-op", i)
				case errors.Is(outcome.err, ErrActiveRuns):
					require.NotNil(t, trig.run, "iteration %d: refused delete implies the trigger won", i)
					var runs int
					require.NoError(t, r.Pool.QueryRow(ctx,
						`SELECT COUNT(*) FROM sync_runs WHERE id = $1`, trig.run.ID).Scan(&runs))
					assert.Equal(t, 1, runs, "iteration %d: the durable task must survive the refused delete", i)
					_, err := r.Pool.Exec(ctx,
						`UPDATE sync_runs SET status = 'aborted', finished_at = clock_timestamp()
						  WHERE id = $1 AND status IN ('pending','running')`, trig.run.ID)
					require.NoError(t, err)
				default:
					t.Fatalf("iteration %d: unexpected delete outcome: deleted=%v err=%v", i, outcome.deleted, outcome.err)
				}
			}
		})
	}
}

// TestPGMigrateLegacyDBPreservesRowsAndLeaseOnRerun builds a REAL pre-P2.1
// database (only migrations before the lease file are applied; the lease
// columns provably do not exist yet), inserts legacy rows, upgrades, and then
// replays migrations over a live lease.
func TestPGMigrateLegacyDBPreservesRowsAndLeaseOnRerun(t *testing.T) {
	r, _ := bootRepoWithDSN(t)
	ctx := context.Background()
	applyMigrationsExcept(ctx, t, r.Pool, "20261113100000")

	// The P2.1 columns genuinely do not exist yet.
	var leaseCols int
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM information_schema.columns
		  WHERE table_name = 'sync_runs' AND column_name IN ('lease_token', 'generation', 'requested_by')`).Scan(&leaseCols))
	require.Equal(t, 0, leaseCols, "fixture must be a genuine pre-P2.1 database")

	legacyOwner := uuid.New()
	personal := Tenant{OwnerID: legacyOwner}
	legacySource := uuid.New()
	_, err := r.Pool.Exec(ctx,
		`INSERT INTO connections (id, name, connector_type, config, owner_id) VALUES ($1,'legacy','postgresql','{}',$2)`,
		legacySource, legacyOwner)
	require.NoError(t, err)
	legacyDef := uuid.New()
	_, err = r.Pool.Exec(ctx,
		`INSERT INTO batch_sync_defs (id, source_id, output_dataset_id) VALUES ($1,$2,$3)`,
		legacyDef, legacySource, uuid.New())
	require.NoError(t, err)
	legacyRun := uuid.New()
	legacyVersion := uuid.New()
	legacyHash := "legacy-hash-keep"
	_, err = r.Pool.Exec(ctx,
		`INSERT INTO sync_runs (id, sync_def_id, status, dataset_version_id, content_hash, bytes_written, rows_written)
		 VALUES ($1, $2, 'succeeded', $3, $4, 22766, 151)`, legacyRun, legacyDef, legacyVersion, legacyHash)
	require.NoError(t, err)
	legacyRunning := uuid.New()
	_, err = r.Pool.Exec(ctx,
		`INSERT INTO sync_runs (id, sync_def_id, status) VALUES ($1, $2, 'running')`, legacyRunning, legacyDef)
	require.NoError(t, err)

	// Upgrade.
	require.NoError(t, Migrate(ctx, r.Pool))
	require.NoError(t, Migrate(ctx, r.Pool), "replay stays idempotent")

	// Both legacy rows stay VISIBLE to the personal-domain owner — the
	// takeover/complete refusals below are about the missing lease, never
	// about domain invisibility.
	runs, err := r.ListSyncRuns(ctx, legacyDef, personal)
	require.NoError(t, err)
	require.Len(t, runs, 2, "legacy rows must remain visible to their owner")

	var gotVersion uuid.UUID
	var gotHash string
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT dataset_version_id, content_hash FROM sync_runs WHERE id = $1`, legacyRun).
		Scan(&gotVersion, &gotHash))
	assert.Equal(t, legacyVersion, gotVersion, "legacy terminal version preserved")
	assert.Equal(t, legacyHash, gotHash, "legacy terminal hash preserved")

	over, err := r.TakeoverExpiredSyncRun(ctx, legacyRunning, personal, "h", "t", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, over, "legacy running row is never taken over / re-run")
	var stillRunning int
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM sync_runs WHERE id = $1 AND status = 'running'`, legacyRunning).Scan(&stillRunning))
	assert.Equal(t, 1, stillRunning)

	// A live lease on a NEW durable task survives further migration replays.
	runID := leaseEnqueue(t, r, personal, legacySource, legacyDef, legacyOwner)
	claim, err := r.ClaimSyncRun(ctx, runID, personal, "h1", "t1", time.Hour)
	require.NoError(t, err)
	require.NotNil(t, claim)
	leasedRow := runRowJSON(t, r.Pool, runID)
	require.NoError(t, Migrate(ctx, r.Pool))
	require.NoError(t, Migrate(ctx, r.Pool))
	assert.Equal(t, leasedRow, runRowJSON(t, r.Pool, runID),
		"migration replay must not reset or touch a live lease")
}
