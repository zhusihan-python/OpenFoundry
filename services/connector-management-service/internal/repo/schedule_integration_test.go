//go:build integration

// P2.4 observable scheduling — real PostgreSQL 16 regressions.
//
// Authority chain: root review contract (handoffs/2026-11-14-p24), round-1
// implementation review (review-round1.md — the Arm defID/in.SyncDefID
// mismatch, Arm wait-past-expiry, two-phase Heartbeat, enqueued TickID,
// GetView Live-by-actual-revision corrections), design-draft v2 and
// implementation-contract.md in tools/smoke/business-platform-p2-4/.
// Tests target the CONTRACTED (post-correction) semantics; they are expected
// to FAIL against an unfixed implementation — that is their job as
// regressions, and no compile is attempted against signatures the primary
// GLM is still updating (RevokeScheduleGrant's returned-grant-id shape is
// flagged inline).
//
// Matrix (details in pg-test-plan.md §4/§5):
//   1. locked DB clock + candidate CAS (scan hint never authoritative)
//   2. same slot, two real pools ⇒ exactly one tick + one run
//   3. bounded NOWAIT loop + keyset fair scan (repo surface)
//   4. skip-decision atomicity: auth_required reasons, admission skips,
//      stamp never consumed on non-run paths, error_invalid_schedule halts
//   5. success commit: real TickID, cursor/tick/run together, stamp LAST
//      (in-tx visibility probe), retention prune
//   6. every-stage failure rollback (run/tick/cursor/stamp marker triggers)
//   7. honest dispatch residue (dispatch_failed patch, no re-touch)
//   8. grant generation: fresh grant_id per arm, expected-binding CAS,
//      defID/in.SyncDefID mismatch zero-write, arm wait-past-expiry,
//      revoke DB semantics
//   9. heartbeat: exact generation, two-phase lock-then-clock (P21 trap),
//      MVCC-only Def check (no reverse lock)
//  10. GetScheduleGrantView Live computed from the ACTUAL def revision
//  11. config semantics: revision bumps, cursor NULL/future, zero-write
//      validation, legacy invalid cron survival
//  12. migration replay: legacy disabled + NULL cursor, no backfill,
//      history/lease/stamp preserved byte for byte, no CONCURRENTLY
//  13. per-source independence + shared admission with the manual path
//
// Reused frozen helpers (same package, never edited): bootRepoWithDSN,
// Migrate, migrationsFS, singleConnRepo, backendPID, waitUntilBlockedBy,
// dbClock, tableRowJSON, snapshotSyncRun, assertSyncRunPersisted, mustRecv,
// p23Op, p23RegisterCleanup, p23WaitClockPast, p23AssertNoIdleInTx,
// p23Stamp, p23MustStamp, p23AssertStampUnchanged, p23ActiveCount,
// p23CapConfig, p23CapIntervalConfig, leaseEnqueue, p23CeilSeconds.
package repo

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
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

// ---------------------------------------------------------------------------
// p24 deterministic machinery
// ---------------------------------------------------------------------------

// p24BarrierStmts is the closed allowlist of barrier targets: fixed SQL text
// per table so a barrier can never interpolate an identifier.
var p24BarrierStmts = map[string]string{
	"connections":          `SELECT id FROM connections WHERE id = $1 FOR UPDATE`,
	"batch_sync_defs":      `SELECT id FROM batch_sync_defs WHERE id = $1 FOR UPDATE`,
	"sync_schedule_grants": `SELECT sync_def_id FROM sync_schedule_grants WHERE sync_def_id = $1 FOR UPDATE`,
}

// p24Bounded is the context budget every helper path uses — release, commit
// and in-barrier writes never wait on an unbounded Background context.
const p24Bounded = 5 * time.Second

// p24RowBarrier generalizes the p23SourceBarrier discipline to any
// allowlisted table: a dedicated session whose open transaction holds one row
// FOR UPDATE WITHOUT writing it (no new row version, so EvalPlanQual never
// runs for waiters). The pre-registered cleanup CLOSES the connection, so
// even a t.Fatal can never leave the lock behind. While held, no writer on
// that row can commit.
type p24RowBarrier struct {
	pid   int32
	tx    pgx.Tx
	table string
}

func p24HoldRowBarrier(t *testing.T, dsn, table string, id uuid.UUID) *p24RowBarrier {
	t.Helper()
	stmt, ok := p24BarrierStmts[table]
	require.True(t, ok, "barrier table %q is not allowlisted", table)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), p24Bounded)
		defer cancel()
		_ = conn.Close(closeCtx) // bounded; releases the row lock
	})
	var pid int32
	require.NoError(t, conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	var locked any
	require.NoError(t, tx.QueryRow(ctx, stmt, id).Scan(&locked))
	return &p24RowBarrier{pid: pid, tx: tx, table: table}
}

// release rolls the barrier transaction back, dropping the row lock.
func (b *p24RowBarrier) release(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), p24Bounded)
	defer cancel()
	require.NoError(t, b.tx.Rollback(ctx), "release %s barrier", b.table)
}

// commit makes the barrier transaction's writes durable and frees the row —
// for ordered-race cases where the blocker must WIN durably.
func (b *p24RowBarrier) commit(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), p24Bounded)
	defer cancel()
	require.NoError(t, b.tx.Commit(ctx), "commit %s barrier", b.table)
}

// exec writes inside the barrier transaction (used with commit() to make an
// in-tx rewrite authoritative for the waiter that acquires the lock next).
func (b *p24RowBarrier) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), p24Bounded)
	defer cancel()
	_, err := b.tx.Exec(ctx, sql, args...)
	require.NoError(t, err, "barrier %s write", b.table)
}

// boundedRelease is the best-effort bounded rollback for cleanup paths (an
// already finished transaction answers ErrTxClosed, which is ignored); the
// pre-registered conn.Close is the backstop.
func (b *p24RowBarrier) boundedRelease() {
	rbCtx, cancel := context.WithTimeout(context.Background(), p24Bounded)
	defer cancel()
	_ = b.tx.Rollback(rbCtx)
}

// p24TableBarrier holds a whole-table ACCESS EXCLUSIVE lock in a dedicated
// session. A row FOR UPDATE NOWAIT cannot bypass it: relation acquisition
// itself blocks (the P23-verified official PG16 behavior), which is how the
// tests park a claim BEFORE its row locks and clocks — the deterministic way
// to prove the post-lock clock is re-sampled after a genuine wait.
type p24TableBarrier struct {
	pid   int32
	tx    pgx.Tx
	table string
}

var p24TableLockStmts = map[string]string{
	"connections":     `LOCK TABLE connections IN ACCESS EXCLUSIVE MODE`,
	"batch_sync_defs": `LOCK TABLE batch_sync_defs IN ACCESS EXCLUSIVE MODE`,
}

func p24HoldTableBarrier(t *testing.T, dsn, table string) *p24TableBarrier {
	t.Helper()
	stmt, ok := p24TableLockStmts[table]
	require.True(t, ok, "table barrier %q is not allowlisted", table)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), p24Bounded)
		defer cancel()
		_ = conn.Close(closeCtx) // bounded; releases the table lock
	})
	var pid int32
	require.NoError(t, conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, stmt)
	require.NoError(t, err)
	return &p24TableBarrier{pid: pid, tx: tx, table: table}
}

func (b *p24TableBarrier) release(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), p24Bounded)
	defer cancel()
	require.NoError(t, b.tx.Rollback(ctx), "release %s table barrier", b.table)
}

func (b *p24TableBarrier) boundedRelease() {
	rbCtx, cancel := context.WithTimeout(context.Background(), p24Bounded)
	defer cancel()
	_ = b.tx.Rollback(rbCtx)
}

// p24Result carries one background operation's outcome; p24Go wraps any repo
// call in the p23Op discipline (HARD bounded context even when the caller's
// parent has no deadline, single-send-then-close channel, idempotent bounded
// shutdown; release-before-cleanup is the caller's p23RegisterCleanup duty).
type p24Result[T any] struct {
	Val T
	Err error
}

// p24Go runs fn in the background. fn must be a single repo call —
// composition belongs in the test body so every send is provably single.
func p24Go[T any](parent context.Context, fn func(context.Context) (T, error)) *p23Op[p24Result[T]] {
	ctx, cancel := context.WithTimeout(parent, 20*time.Second) // hard bound
	ch := make(chan p24Result[T], 1)
	go func() {
		defer cancel()
		val, err := fn(ctx)
		ch <- p24Result[T]{Val: val, Err: err}
		close(ch)
	}()
	return &p23Op[p24Result[T]]{cancel: cancel, done: ch}
}

// p24ApplyMigrationsExcept builds a genuine pre-P2.4 database without
// hard-coding file names: every embedded migration whose body does NOT carry
// the marker (a schema token unique to the P2.4 scheduling set) is applied
// in order; the skipped files are returned and must be exactly the P2.4 set.
func p24ApplyMigrationsExcept(t *testing.T, pool *pgxpool.Pool, marker string) []string {
	t.Helper()
	entries, err := migrationsFS.ReadDir("migrations")
	require.NoError(t, err)
	var names, skipped []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		body, err := migrationsFS.ReadFile("migrations/" + e.Name())
		require.NoError(t, err)
		if strings.Contains(string(body), marker) {
			skipped = append(skipped, e.Name())
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		body, err := migrationsFS.ReadFile("migrations/" + name)
		require.NoError(t, err)
		_, err = pool.Exec(context.Background(), string(body))
		require.NoError(t, err, "apply %s", name)
	}
	require.NotEmpty(t, skipped, "the P2.4 scheduling migrations must exist and be skippable")
	return skipped
}

// ---------------------------------------------------------------------------
// p24 fixtures and observers
// ---------------------------------------------------------------------------

// p24CronEveryMinute is a Unix5 every-minute expression (short fires keep
// tests fast without any sleep-based waiting).
const p24CronEveryMinute = "* * * * *"

// p24FixtureDomain provisions one source + one ENABLED every-minute def and
// returns everything the tests address. Config optionally carries a typed
// admission policy (reuse p23CapConfig / p23CapIntervalConfig).
func p24FixtureDomain(t *testing.T, org bool, config json.RawMessage) (*Repo, string, Tenant, uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()
	r, dsn := bootRepoWithDSN(t)
	ctx := context.Background()
	require.NoError(t, Migrate(ctx, r.Pool))
	tenant := Tenant{OwnerID: uuid.New()}
	if org {
		tenant.OrgID = orgPtr(uuid.New())
	}
	if len(config) == 0 {
		config = json.RawMessage(`{}`)
	}
	conn, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
		Name: "p24-src", ConnectorType: "rest_api", Config: config,
	}, tenant)
	require.NoError(t, err)
	require.NotNil(t, conn)
	out := uuid.New()
	enabled := true
	job, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
		SourceID: conn.ID, OutputDatasetID: out,
		ScheduleCron: ptrTo(p24CronEveryMinute), ScheduleEnabled: &enabled,
	}, tenant)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.True(t, job.ScheduleEnabled)
	require.Equal(t, 1, job.ScheduleRevision)
	require.NotNil(t, job.ScheduleNextFireAt, "an enabled schedule must compute a cursor")
	return r, dsn, tenant, conn.ID, job.ID, out
}

func ptrTo[V any](v V) *V { return &v }

// p24NextSeventhMinuteBoundary returns the first wall-clock minute strictly
// after t (UTC) whose minute-of-hour is ≡ 0 mod 7 — exactly the fire set of
// "*/7 * * * *" — used to match a recomputed cursor against the NEW cron's
// boundary calculation from the locked clock.
func p24NextSeventhMinuteBoundary(t time.Time) time.Time {
	b := t.UTC().Truncate(time.Minute).Add(time.Minute)
	for b.Minute()%7 != 0 {
		b = b.Add(time.Minute)
	}
	return b
}

// p24RangeStrings renders "lo".."hi" as decimal strings (oversize-fixture
// builder: legal cron value lists whose total length alone is illegal).
func p24RangeStrings(lo, hi int) []string {
	out := make([]string, 0, hi-lo+1)
	for v := lo; v <= hi; v++ {
		out = append(out, strconv.Itoa(v))
	}
	return out
}

// p24MakeDue force-marks the def overdue (fixture escape hatch: the config
// surfaces always compute a FUTURE cursor, which is exactly what production
// wants and what the tests must bypass deterministically).
func p24MakeDue(t *testing.T, pool *pgxpool.Pool, defID uuid.UUID) time.Time {
	t.Helper()
	var due time.Time
	require.NoError(t, pool.QueryRow(context.Background(),
		`UPDATE batch_sync_defs
		    SET schedule_next_fire_at = clock_timestamp() - interval '1 hour'
		  WHERE id = $1
		  RETURNING schedule_next_fire_at`, defID).Scan(&due))
	return due
}

// p24DefRevision / p24Cursor / p24TickCount / p24RunCount / p24GrantCount
// are independent-observer reads of the durable schedule state.
func p24DefRevision(t *testing.T, pool *pgxpool.Pool, defID uuid.UUID) int {
	t.Helper()
	var rev int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT schedule_revision FROM batch_sync_defs WHERE id = $1`, defID).Scan(&rev))
	return rev
}

func p24Cursor(t *testing.T, pool *pgxpool.Pool, defID uuid.UUID) *time.Time {
	t.Helper()
	var cur *time.Time
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT schedule_next_fire_at FROM batch_sync_defs WHERE id = $1`, defID).Scan(&cur))
	return cur
}

func p24TickCount(t *testing.T, pool *pgxpool.Pool, defID uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM sync_schedule_ticks WHERE sync_def_id = $1`, defID).Scan(&n))
	return n
}

func p24RunCount(t *testing.T, pool *pgxpool.Pool, defID uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM sync_runs WHERE sync_def_id = $1`, defID).Scan(&n))
	return n
}

func p24GrantCount(t *testing.T, pool *pgxpool.Pool, defID uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM sync_schedule_grants WHERE sync_def_id = $1`, defID).Scan(&n))
	return n
}

// p24GrantRow is the byte-truth of the def's grant row. The grants table is
// keyed by sync_def_id (no `id` column), so it needs its own static snapshot
// SQL — the frozen tableRowJSON helper stays untouched.
func p24GrantRow(t *testing.T, pool *pgxpool.Pool, defID uuid.UUID) string {
	t.Helper()
	var js string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT row_to_json(g.*)::text FROM sync_schedule_grants g WHERE g.sync_def_id = $1`, defID).Scan(&js))
	return js
}

// p24GrantImmutable snapshots every grant column EXCEPT last_seen_at — the
// columns a successful heartbeat must preserve byte for byte. Key removal is
// a jsonb-text operation (PG16 official JSON functions), so the composite is
// converted with to_jsonb before subtracting the key.
func p24GrantImmutable(t *testing.T, pool *pgxpool.Pool, defID uuid.UUID) string {
	t.Helper()
	var js string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT (to_jsonb(g) - 'last_seen_at')::text FROM sync_schedule_grants g WHERE g.sync_def_id = $1`, defID).Scan(&js))
	return js
}

// p24SetGrantCol rewrites one grant column directly (fixture escape hatch for
// expiry/freshness windows the arm surface cannot express).
func p24SetGrantCol(t *testing.T, pool *pgxpool.Pool, defID uuid.UUID, expr string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE sync_schedule_grants SET `+expr+` WHERE sync_def_id = $1`, defID)
	require.NoError(t, err)
}

// p24Arm arms the def and returns the matching local arm claim plus the DB
// view. expiresAt is the arm's absolute deadline. The actor IS the trusted
// tenant's verified caller subject (OwnerSubject) — the contract forbids a
// third identity; negative matrices below change exactly one field each.
func p24Arm(t *testing.T, r *Repo, tenant Tenant, defID, sourceID, output uuid.UUID, expiresAt time.Time) (*ScheduleArmClaim, *models.ScheduleGrantView) {
	t.Helper()
	rev := p24DefRevision(t, r.Pool, defID)
	grantID, bootID := uuid.New(), uuid.New()
	write := models.ScheduleGrantWrite{
		SyncDefID: defID, GrantID: grantID, BootID: bootID,
		ActorSubject: tenant.OwnerID, OwnerSubject: tenant.OwnerID, OrgID: tenant.OrgID,
		SourceID: sourceID, OutputDatasetID: output,
		Revision: rev, ExpiresAt: expiresAt,
	}
	view, err := r.ArmScheduleGrant(context.Background(), defID, tenant, write, rev, output)
	require.NoError(t, err)
	require.NotNil(t, view)
	return &ScheduleArmClaim{
		GrantID: grantID, BootID: bootID, Actor: tenant.OwnerID, Revision: rev,
		Tenant: tenant, SourceID: sourceID, OutputDatasetID: output,
		Authorization: "Bearer p24-test", ExpiresAt: expiresAt,
	}, view
}

// p24Cand builds the candidate for the def's CURRENT revision + cursor.
func p24Cand(t *testing.T, pool *pgxpool.Pool, defID, sourceID uuid.UUID) ScheduleCandidate {
	t.Helper()
	cur := p24Cursor(t, pool, defID)
	require.NotNil(t, cur, "candidate needs a non-NULL cursor")
	return ScheduleCandidate{
		SyncDefID: defID, SourceID: sourceID,
		Revision: p24DefRevision(t, pool, defID), ScheduledFor: *cur,
	}
}

// p24Claim runs one claim on a caller-owned repo.
func p24Claim(r *Repo, cand ScheduleCandidate, arm *ScheduleArmClaim) func(context.Context) (*ScheduleClaimResult, error) {
	return func(ctx context.Context) (*ScheduleClaimResult, error) {
		return r.ClaimDueScheduleSlot(ctx, cand, arm)
	}
}

// ---------------------------------------------------------------------------
// p24 stage-failure fixture (marker triggers toggled through a flag table)
// ---------------------------------------------------------------------------

// p24InstallStageFixture installs the toggles + triggers that force a
// deterministic failure at ONE chosen stage of the claim transaction, plus
// the stamp-order probe. Everything is idempotent and dropped on cleanup.
func p24InstallStageFixture(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS p24_flag (stage text PRIMARY KEY, fail boolean NOT NULL DEFAULT false);
INSERT INTO p24_flag (stage, fail) VALUES ('fail_run', false), ('fail_tick', false),
    ('fail_cursor', false), ('fail_stamp', false), ('probe_stamp', false)
ON CONFLICT (stage) DO UPDATE SET fail = EXCLUDED.fail;
CREATE TABLE IF NOT EXISTS p24_stamp_probe (id bigserial PRIMARY KEY, ticks int NOT NULL, run_ticks int NOT NULL, next_fire timestamptz);
CREATE OR REPLACE FUNCTION p24_stage_gate() RETURNS trigger AS $fn$
DECLARE s text := TG_ARGV[0]; f boolean;
BEGIN
  SELECT fail INTO f FROM p24_flag WHERE stage = s;
  IF COALESCE(f, false) THEN
    RAISE EXCEPTION 'p24 forced failure at %', s USING ERRCODE = 'P0001';
  END IF;
  RETURN NEW;
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION p24_stamp_probe_fn() RETURNS trigger AS $fn$
BEGIN
  IF COALESCE((SELECT fail FROM p24_flag WHERE stage = 'probe_stamp'), false) THEN
    INSERT INTO p24_stamp_probe (ticks, run_ticks, next_fire)
    SELECT (SELECT count(*) FROM sync_schedule_ticks),
           (SELECT count(*) FROM sync_schedule_ticks WHERE run_id IS NOT NULL),
           (SELECT max(schedule_next_fire_at) FROM batch_sync_defs WHERE schedule_next_fire_at IS NOT NULL);
  END IF;
  RETURN NEW;
END $fn$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS p24_fail_run ON sync_runs;
CREATE TRIGGER p24_fail_run BEFORE INSERT ON sync_runs
  FOR EACH ROW EXECUTE FUNCTION p24_stage_gate('fail_run');
DROP TRIGGER IF EXISTS p24_fail_tick ON sync_schedule_ticks;
CREATE TRIGGER p24_fail_tick BEFORE INSERT ON sync_schedule_ticks
  FOR EACH ROW EXECUTE FUNCTION p24_stage_gate('fail_tick');
DROP TRIGGER IF EXISTS p24_fail_cursor ON batch_sync_defs;
CREATE TRIGGER p24_fail_cursor BEFORE UPDATE ON batch_sync_defs
  FOR EACH ROW WHEN (NEW.schedule_last_tick_at IS DISTINCT FROM OLD.schedule_last_tick_at
                     OR NEW.schedule_next_fire_at IS DISTINCT FROM OLD.schedule_next_fire_at)
  EXECUTE FUNCTION p24_stage_gate('fail_cursor');
DROP TRIGGER IF EXISTS p24_fail_stamp ON connections;
CREATE TRIGGER p24_fail_stamp BEFORE UPDATE ON connections
  FOR EACH ROW WHEN (NEW.sync_last_admitted_at IS DISTINCT FROM OLD.sync_last_admitted_at
                     AND NEW.sync_last_admitted_at IS NOT NULL)
  EXECUTE FUNCTION p24_stage_gate('fail_stamp');
DROP TRIGGER IF EXISTS p24_stamp_probe_trg ON connections;
CREATE TRIGGER p24_stamp_probe_trg BEFORE UPDATE ON connections
  FOR EACH ROW WHEN (NEW.sync_last_admitted_at IS DISTINCT FROM OLD.sync_last_admitted_at
                     AND NEW.sync_last_admitted_at IS NOT NULL)
  EXECUTE FUNCTION p24_stamp_probe_fn();
`)
	require.NoError(t, err, "install p24 stage fixture")
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), p24Bounded)
		defer cancel()
		_, _ = pool.Exec(dropCtx, `
DROP TRIGGER IF EXISTS p24_fail_run ON sync_runs;
DROP TRIGGER IF EXISTS p24_fail_tick ON sync_schedule_ticks;
DROP TRIGGER IF EXISTS p24_fail_cursor ON batch_sync_defs;
DROP TRIGGER IF EXISTS p24_fail_stamp ON connections;
DROP TRIGGER IF EXISTS p24_stamp_probe_trg ON connections;
DROP FUNCTION IF EXISTS p24_stage_gate();
DROP FUNCTION IF EXISTS p24_stamp_probe_fn();
DROP TABLE IF EXISTS p24_stamp_probe;
DROP TABLE IF EXISTS p24_flag;`)
	})
}

// p24SetStage toggles one fixture stage (committed before the call returns,
// so the claim transaction's trigger reads see it).
func p24SetStage(t *testing.T, pool *pgxpool.Pool, stage string, on bool) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO p24_flag (stage, fail) VALUES ($1, $2)
		 ON CONFLICT (stage) DO UPDATE SET fail = EXCLUDED.fail`, stage, on)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// mandatory test 1 — locked clock & candidate CAS
// ---------------------------------------------------------------------------

// TestPGScheduleLockedClockAndCandidateCAS proves the caller clock is a scan
// hint ONLY: the claim re-adjudicates due-ness and candidate identity from
// the POST-LOCK database clock and the POST-LOCK def row, no matter what the
// scan believed.
func TestPGScheduleLockedClockAndCandidateCAS(t *testing.T) {
	t.Run("hint_narrows_scan_only_locked_clock_decides", func(t *testing.T) {
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))

		// The scan hint only narrows: a hint OLDER than the due instant hides
		// the row from the scan, a future hint surfaces it — but neither can
		// adjudicate due-ness; the claim's locked clock does.
		due := *p24Cursor(t, r.Pool, def)
		older, err := r.ListDueScheduleCandidates(context.Background(), due.Add(-time.Minute), nil, 8)
		require.NoError(t, err)
		assert.Empty(t, older, "an older hint hides the row from the scan")
		newer, err := r.ListDueScheduleCandidates(context.Background(), dbClock(t, r.Pool).Add(time.Hour), nil, 8)
		require.NoError(t, err)
		require.Len(t, newer, 1)
		assert.Equal(t, def, newer[0].SyncDefID)

		res, err := r.ClaimDueScheduleSlot(context.Background(), newer[0], arm)
		require.NoError(t, err)
		require.True(t, res.Claimed)
		require.Equal(t, TickOutcomeEnqueued, res.Outcome)
		require.NotNil(t, res.Run)

		// The OLD candidate no longer matches the advanced cursor: the
		// candidate CAS rejects it as stale_candidate BEFORE any due decision.
		resOld, err := r.ClaimDueScheduleSlot(context.Background(), newer[0], arm)
		require.NoError(t, err)
		require.False(t, resOld.Claimed)
		assert.Equal(t, ScheduleSkipStale, resOld.Skip)

		// Rebuild the candidate from the NEW (still future) cursor: a future
		// scan hint surfaces it, but the claim's locked clock still refuses —
		// no early firing despite the hint.
		futureHint := dbClock(t, r.Pool).Add(time.Hour)
		futureList, err := r.ListDueScheduleCandidates(context.Background(), futureHint, nil, 8)
		require.NoError(t, err)
		require.Len(t, futureList, 1, "a future hint surfaces the not-yet-due row")
		resFuture, err := r.ClaimDueScheduleSlot(context.Background(), futureList[0], arm)
		require.NoError(t, err)
		require.False(t, resFuture.Claimed)
		assert.Equal(t, ScheduleSkipNotDue, resFuture.Skip)
		assert.Equal(t, 1, p24RunCount(t, r.Pool, def))
		assert.Zero(t, p24TickCount(t, r.Pool, def)-1, "only the fired slot's tick exists")
	})

	t.Run("def_row_blocker_is_fast_contention_not_a_wait", func(t *testing.T) {
		r, dsn, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		cand := p24Cand(t, r.Pool, def, src)

		// The claim's Def lock is FOR UPDATE NOWAIT: a pure Def row blocker is
		// answered by an immediate contention skip — never a row-lock wait.
		bar := p24HoldRowBarrier(t, dsn, "batch_sync_defs", def)
		enq := singleConnRepo(t, dsn)
		start := time.Now()
		res, err := enq.ClaimDueScheduleSlot(context.Background(), cand, arm)
		elapsed := time.Since(start)
		bar.release(t)
		require.NoError(t, err)
		require.False(t, res.Claimed)
		assert.Equal(t, ScheduleSkipContention, res.Skip)
		assert.Less(t, elapsed, 3*time.Second, "Def NOWAIT must be fast, took %s", elapsed)
		assert.Zero(t, p24TickCount(t, r.Pool, def))
		assert.Zero(t, p24RunCount(t, r.Pool, def))
		p23AssertNoIdleInTx(t, r.Pool)

		// The SAME session claims successfully once the row is free.
		res2, err := enq.ClaimDueScheduleSlot(context.Background(), cand, arm)
		require.NoError(t, err)
		require.True(t, res2.Claimed)
		require.Equal(t, TickOutcomeEnqueued, res2.Outcome)
	})

	t.Run("post_lock_clock_authority_proven_by_table_wait", func(t *testing.T) {
		r, dsn, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		// Cursor set STRICTLY FUTURE: the slot is not due yet at launch.
		due := dbClock(t, r.Pool).Add(3 * time.Second)
		_, err := r.Pool.Exec(context.Background(),
			`UPDATE batch_sync_defs SET schedule_next_fire_at = $2 WHERE id = $1`, def, due)
		require.NoError(t, err)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		cand := p24Cand(t, r.Pool, def, src)

		// A whole-table ACCESS EXCLUSIVE lock parks the claim at RELATION
		// acquisition — before any row lock and before any clock read (a row
		// FOR UPDATE NOWAIT cannot bypass it; the P23-verified PG16 behavior).
		bar := p24HoldTableBarrier(t, dsn, "connections")
		enq := singleConnRepo(t, dsn)
		enqPID := backendPID(t, enq.Pool) // warm BEFORE launching
		op := p24Go(context.Background(), p24Claim(enq, cand, arm))
		p23RegisterCleanup(t, bar.boundedRelease, func() { op.shutdown(t) })
		waitUntilBlockedBy(t, r.Pool, enqPID, bar.pid)

		// Genuine block observed while the slot is STILL not due.
		require.True(t, dbClock(t, r.Pool).Before(due),
			"the wait must still precede the due boundary (else the proof is vacuous)")

		// Cross the due boundary while parked, then release.
		p23WaitClockPast(t, r.Pool, due.Add(200*time.Millisecond))
		bar.release(t)
		got := mustRecv(t, op.done, "claim after crossing due")

		// The claim re-samples the clock AFTER its locks: the slot is now due
		// and fires. An implementation judging by the launch-time/hint clock
		// would answer not_due here and fail this assertion.
		require.NoError(t, got.Err)
		require.True(t, got.Val.Claimed, "the fresh post-lock clock must adjudicate due-ness")
		require.Equal(t, TickOutcomeEnqueued, got.Val.Outcome)
		require.NotNil(t, got.Val.Run)
		// The committed decision times anchor at/after the due boundary.
		assert.False(t, got.Val.ScheduledFor.After(due.Add(time.Second)))
		var claimedAt time.Time
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT claimed_at FROM sync_schedule_ticks WHERE id = $1`, got.Val.TickID).Scan(&claimedAt))
		assert.False(t, claimedAt.Before(due), "claimed_at comes from the post-lock clock")
		p23AssertNoIdleInTx(t, r.Pool)
	})

	t.Run("cas_field_matrix_rejects_cleanly", func(t *testing.T) {
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		base := p24Cand(t, r.Pool, def, src)

		for name, cand := range map[string]ScheduleCandidate{
			"revision+1":   func() ScheduleCandidate { c := base; c.Revision++; return c }(),
			"revision-1":   func() ScheduleCandidate { c := base; c.Revision--; return c }(),
			"slot_shifted": func() ScheduleCandidate { c := base; c.ScheduledFor = c.ScheduledFor.Add(time.Second); return c }(),
		} {
			res, err := r.ClaimDueScheduleSlot(context.Background(), cand, arm)
			require.NoError(t, err, name)
			require.False(t, res.Claimed, name)
			assert.Equal(t, ScheduleSkipStale, res.Skip, name)
		}
		// A candidate with a zero ScheduledFor never matches a non-NULL cursor.
		zero := base
		zero.ScheduledFor = time.Time{}
		res, err := r.ClaimDueScheduleSlot(context.Background(), zero, arm)
		require.NoError(t, err)
		require.False(t, res.Claimed)
		assert.Equal(t, ScheduleSkipStale, res.Skip)
		assert.Zero(t, p24TickCount(t, r.Pool, def))
		assert.Zero(t, p24RunCount(t, r.Pool, def))
	})

	t.Run("cursor_null_reaches_candidate_cas_first", func(t *testing.T) {
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		_, err := r.Pool.Exec(context.Background(),
			`UPDATE batch_sync_defs SET schedule_next_fire_at = NULL WHERE id = $1`, def)
		require.NoError(t, err)
		// The candidate CAS runs BEFORE the due check: a zero-time candidate
		// against a NULL cursor is a slot mismatch ⇒ stale_candidate.
		res, err := r.ClaimDueScheduleSlot(context.Background(), ScheduleCandidate{
			SyncDefID: def, SourceID: src, Revision: p24DefRevision(t, r.Pool, def),
			ScheduledFor: time.Time{},
		}, arm)
		require.NoError(t, err)
		require.False(t, res.Claimed)
		assert.Equal(t, ScheduleSkipStale, res.Skip)
		assert.Zero(t, p24TickCount(t, r.Pool, def))
	})
}

// ---------------------------------------------------------------------------
// mandatory test 2 — same slot, two real pools
// ---------------------------------------------------------------------------

// TestPGScheduleSameSlotSingleWinnerTwoPools races two independent
// single-connection sessions over the same (def, revision, scheduled_for):
// whatever the interleaving, exactly ONE tick and ONE run commit, and the
// loser answers a clean structured skip with zero durable effects.
func TestPGScheduleSameSlotSingleWinnerTwoPools(t *testing.T) {
	t.Run("sequential_loser_after_winner_is_clean_noop", func(t *testing.T) {
		r, dsn, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		cand := p24Cand(t, r.Pool, def, src)

		a, b := singleConnRepo(t, dsn), singleConnRepo(t, dsn)
		resA, errA := a.ClaimDueScheduleSlot(context.Background(), cand, arm)
		require.NoError(t, errA)
		require.True(t, resA.Claimed)
		require.Equal(t, TickOutcomeEnqueued, resA.Outcome)
		require.NotNil(t, resA.Run)
		stampA := p23MustStamp(t, r.Pool, src)

		resB, errB := b.ClaimDueScheduleSlot(context.Background(), cand, arm)
		require.NoError(t, errB)
		require.False(t, resB.Claimed)
		assert.Contains(t, []ScheduleClaimSkip{ScheduleSkipStale, ScheduleSkipNotDue}, resB.Skip,
			"the already-claimed slot must answer a clean skip")
		assert.Equal(t, 1, p24TickCount(t, r.Pool, def), "exactly one tick")
		assert.Equal(t, 1, p24RunCount(t, r.Pool, def), "exactly one run")
		p23AssertStampUnchanged(t, r.Pool, src, stampA, "a skipped claim never consumes admission")
		p23AssertNoIdleInTx(t, r.Pool)
	})

	t.Run("concurrent_same_instant_exactly_one_commit", func(t *testing.T) {
		r, dsn, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		cand := p24Cand(t, r.Pool, def, src)

		const contenders = 4
		repos := make([]*Repo, contenders)
		for i := range repos {
			repos[i] = singleConnRepo(t, dsn)
		}
		results := make([]p24Result[*ScheduleClaimResult], contenders)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < contenders; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				res, err := repos[i].ClaimDueScheduleSlot(ctx, cand, arm)
				results[i] = p24Result[*ScheduleClaimResult]{Val: res, Err: err}
			}(i)
		}
		t.Cleanup(func() {
			joined := make(chan struct{})
			go func() { wg.Wait(); close(joined) }()
			select {
			case <-joined:
			case <-time.After(20 * time.Second):
				t.Errorf("claim contenders did not return within the bounded join")
			}
		})
		close(start)
		wg.Wait()

		winners := 0
		for i := 0; i < contenders; i++ {
			require.NoError(t, results[i].Err, "contender %d", i)
			if results[i].Val.Claimed {
				winners++
				assert.Equal(t, TickOutcomeEnqueued, results[i].Val.Outcome, "contender %d", i)
				require.NotNil(t, results[i].Val.Run, "contender %d", i)
			} else {
				assert.NotEmpty(t, results[i].Val.Skip, "contender %d: structured skip", i)
			}
		}
		assert.Equal(t, 1, winners, "exactly one winner")
		assert.Equal(t, 1, p24TickCount(t, r.Pool, def), "exactly one committed tick")
		assert.Equal(t, 1, p24RunCount(t, r.Pool, def), "exactly one committed run")
		// The tick's slot identity is exactly the candidate's.
		var rev int
		var slot time.Time
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT schedule_revision, scheduled_for FROM sync_schedule_ticks WHERE sync_def_id = $1`, def).
			Scan(&rev, &slot))
		assert.Equal(t, cand.Revision, rev)
		assert.True(t, slot.Equal(cand.ScheduledFor))
		p23AssertNoIdleInTx(t, r.Pool)
	})
}

// ---------------------------------------------------------------------------
// mandatory test 3 — bounded loop & keyset fair scan (repo surface)
// ---------------------------------------------------------------------------

// TestPGScheduleLoopBoundedFairProgress proves the scheduler-facing repo
// surface is bounded and fair: a held Source answers fast structured
// contention (never a park), and the keyset scan cursor lets a later
// eligible def proceed past contended older rows.
func TestPGScheduleLoopBoundedFairProgress(t *testing.T) {
	t.Run("nowait_source_contention_is_fast_and_rowless", func(t *testing.T) {
		r, dsn, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		cand := p24Cand(t, r.Pool, def, src)

		bar := p24HoldRowBarrier(t, dsn, "connections", src)
		enq := singleConnRepo(t, dsn)
		start := time.Now()
		res, err := enq.ClaimDueScheduleSlot(context.Background(), cand, arm)
		elapsed := time.Since(start)
		bar.release(t)
		require.NoError(t, err)
		require.False(t, res.Claimed)
		assert.Equal(t, ScheduleSkipContention, res.Skip)
		assert.Less(t, elapsed, 3*time.Second, "scheduler NOWAIT must be fast, took %s", elapsed)
		assert.Zero(t, p24TickCount(t, r.Pool, def))
		assert.Zero(t, p24RunCount(t, r.Pool, def))
		assert.Nil(t, p23Stamp(t, r.Pool, src), "contention writes nothing")
		p23AssertNoIdleInTx(t, r.Pool)

		// The SAME session immediately succeeds once the row is free.
		res2, err2 := enq.ClaimDueScheduleSlot(context.Background(), cand, arm)
		require.NoError(t, err2)
		require.True(t, res2.Claimed)
		require.Equal(t, TickOutcomeEnqueued, res2.Outcome)
	})

	t.Run("keyset_scan_passes_contended_head_to_later_def", func(t *testing.T) {
		r, dsn, tenant, srcA, defA, _ := p24FixtureDomain(t, true, nil)
		ctx := context.Background()
		// defB lives on a SEPARATE source so holding srcA cannot touch it.
		connB, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
			Name: "p24-src-b", ConnectorType: "rest_api"}, tenant)
		require.NoError(t, err)
		jobB, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: connB.ID, OutputDatasetID: uuid.New(),
			ScheduleCron: ptrTo(p24CronEveryMinute), ScheduleEnabled: ptrTo(true),
		}, tenant)
		require.NoError(t, err)
		// defA strictly older than defB.
		_, err = r.Pool.Exec(ctx,
			`UPDATE batch_sync_defs SET schedule_next_fire_at = clock_timestamp() - interval '2 hours' WHERE id = $1`, defA)
		require.NoError(t, err)
		p24MakeDue(t, r.Pool, jobB.ID)

		// srcA is externally held: any claim on defA is fast contention, but
		// the lockless scan still surfaces BOTH defs ordered by
		// (next_fire_at, id), and the `after` key continues past the head.
		bar := p24HoldRowBarrier(t, dsn, "connections", srcA)
		defer bar.release(t)

		hint := dbClock(t, r.Pool)
		page, err := r.ListDueScheduleCandidates(ctx, hint, nil, 8)
		require.NoError(t, err)
		require.Len(t, page, 2)
		require.Equal(t, defA, page[0].SyncDefID)
		require.Equal(t, jobB.ID, page[1].SyncDefID)
		afterHead, err := r.ListDueScheduleCandidates(ctx, hint,
			&ScheduleScanKey{NextFireAt: page[0].ScheduledFor, DefID: page[0].SyncDefID}, 8)
		require.NoError(t, err)
		require.Len(t, afterHead, 1)
		assert.Equal(t, jobB.ID, afterHead[0].SyncDefID, "the after-key continues past the contended head")

		armB, _ := p24Arm(t, r, tenant, jobB.ID, connB.ID, jobB.OutputDatasetID, hint.Add(time.Hour))
		claimB, err := r.ClaimDueScheduleSlot(ctx, page[1], armB)
		require.NoError(t, err)
		require.True(t, claimB.Claimed)
		require.Equal(t, TickOutcomeEnqueued, claimB.Outcome,
			"a later eligible def must progress while an older def's source is contended")
	})
}

// ---------------------------------------------------------------------------
// mandatory test 4 — skip-decision atomicity
// ---------------------------------------------------------------------------

// TestPGScheduleSkipDecisionAtomicity proves every no-run decision commits
// atomically as ONE tick row + cursor advance, with the admission stamp
// untouched and no run — for every auth_required reason and both typed
// admission refusals — and that an unreadable schedule halts the cursor.
func TestPGScheduleSkipDecisionAtomicity(t *testing.T) {
	// reasonTick reads the EXACT decision the claim returned (by TickID), so
	// sequential decisions in one subtest can never be conflated.
	reasonTick := func(t *testing.T, r *Repo, tickID uuid.UUID) (string, *string) {
		t.Helper()
		var outcome string
		var reason *string
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT outcome, reason FROM sync_schedule_ticks WHERE id = $1`, tickID).
			Scan(&outcome, &reason))
		return outcome, reason
	}

	t.Run("auth_required_reason_matrix", func(t *testing.T) {
		cases := []struct {
			name     string
			arrange  func(t *testing.T) (*Repo, Tenant, uuid.UUID, uuid.UUID, uuid.UUID, *ScheduleArmClaim)
			expected string
		}{
			{"never_armed", func(t *testing.T) (*Repo, Tenant, uuid.UUID, uuid.UUID, uuid.UUID, *ScheduleArmClaim) {
				r, _, tenant, src, def, _ := p24FixtureDomain(t, true, nil)
				p24MakeDue(t, r.Pool, def)
				return r, tenant, src, def, uuid.Nil, nil
			}, TickReasonNeverArmed},
			{"grant_expired", func(t *testing.T) (*Repo, Tenant, uuid.UUID, uuid.UUID, uuid.UUID, *ScheduleArmClaim) {
				r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
				p24MakeDue(t, r.Pool, def)
				p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
				// Healthy same-generation grant whose DB expiry genuinely
				// passed. Per the contract's reason list, grant_expired is
				// the DB-row truth ("行.expires_at≤dbNow"); the claim arrives
				// WITHOUT a local arm — any replica adjudicates the dead
				// owner from the row alone. (An arm PRESENT with a matching
				// expired expiry maps to window_too_short first — that branch
				// has its own exact-equality subtest below.)
				p24SetGrantCol(t, r.Pool, def, `expires_at = clock_timestamp() - interval '1 second'`)
				return r, tenant, src, def, out, nil
			}, TickReasonGrantExpired},
			{"owner_stale", func(t *testing.T) (*Repo, Tenant, uuid.UUID, uuid.UUID, uuid.UUID, *ScheduleArmClaim) {
				r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
				p24MakeDue(t, r.Pool, def)
				arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
				p24SetGrantCol(t, r.Pool, def, `last_seen_at = clock_timestamp() - interval '91 seconds'`)
				return r, tenant, src, def, out, arm
			}, TickReasonOwnerStale},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
			r, _, src, def, _, arm := tc.arrange(t)
			// Deterministic time anchor captured BEFORE the claim: the
			// advanced cursor is strictly after the claim's own locked clock,
			// which is ≥ this anchor. Comparing against a post-hoc dbClock
			// read would flake when the next fire lands within the observer's
			// read latency.
			preClaim := dbClock(t, r.Pool)
			res, err := r.ClaimDueScheduleSlot(context.Background(), p24Cand(t, r.Pool, def, src), arm)
			require.NoError(t, err)
			require.True(t, res.Claimed, "auth_required IS a claimed decision (tick+cursor commit)")
			require.Equal(t, TickOutcomeAuthRequired, res.Outcome)
			require.Nil(t, res.Run)
			require.NotEqual(t, uuid.Nil, res.TickID)
			outcome, reason := reasonTick(t, r, res.TickID)
			assert.Equal(t, string(TickOutcomeAuthRequired), outcome)
			require.NotNil(t, reason)
			assert.Equal(t, tc.expected, *reason)
			// Cursor advanced to a strictly future slot; no run; no stamp.
			require.NotNil(t, res.NextFireAt)
			assert.True(t, res.NextFireAt.After(preClaim), "the advanced cursor is strictly after the pre-claim clock")
			cur := p24Cursor(t, r.Pool, def)
			require.NotNil(t, cur)
			assert.True(t, cur.Equal(*res.NextFireAt))
			assert.Zero(t, p24RunCount(t, r.Pool, def))
				assert.Nil(t, p23Stamp(t, r.Pool, src), "auth_required never consumes admission")
			})
		}
	})

	t.Run("same_id_exact_expiry_expired_answers_window_too_short", func(t *testing.T) {
		// The canonical-µs fixture root prescribed: ONE expiry value, written
		// to the DB row and the local arm EXACTLY equal (read back from the
		// row — two separate host-side clock reads differ at microsecond
		// precision and would silently test a mismatched snapshot instead).
		// With the arm present, same grant_id and arm.ExpiresAt ≤ dbNow, the
		// contract's reason order maps this to window_too_short (the
		// arm-window branch precedes the DB grant_expired branch).
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		p24SetGrantCol(t, r.Pool, def, `expires_at = clock_timestamp() - interval '1 second'`)
		var dbExpires time.Time
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT expires_at FROM sync_schedule_grants WHERE sync_def_id = $1`, def).Scan(&dbExpires))
		arm.ExpiresAt = dbExpires // EXACT canonical value, not a second clock read

		res, err := r.ClaimDueScheduleSlot(context.Background(), p24Cand(t, r.Pool, def, src), arm)
		require.NoError(t, err)
		require.True(t, res.Claimed)
		require.Equal(t, TickOutcomeAuthRequired, res.Outcome)
		require.Nil(t, res.Run)
		require.NotEqual(t, uuid.Nil, res.TickID)
		var reason *string
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT reason FROM sync_schedule_ticks WHERE id = $1`, res.TickID).Scan(&reason))
		require.NotNil(t, reason)
		assert.Equal(t, TickReasonWindowTooShort, *reason,
			"an arm-present expired window maps to window_too_short even when the expiry equals the row's")
		assert.Zero(t, p24RunCount(t, r.Pool, def))
		assert.Nil(t, p23Stamp(t, r.Pool, src), "window_too_short consumes no admission")
	})

	t.Run("stale_revision_then_mismatched_local_expiry_fails_closed", func(t *testing.T) {
		// stale_revision: the grant predates a config change that bumped the
		// def revision (arming again is the repair; the OLD grant must not
		// keep firing).
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		cron := "*/2 * * * *"
		_, err := r.UpdateSyncJob(context.Background(), def, &models.UpdateSyncJobRequest{ScheduleCron: &cron}, tenant)
		require.NoError(t, err)
		require.Greater(t, p24DefRevision(t, r.Pool, def), arm.Revision)
		// The revision bump recomputed a FUTURE cursor: force it due again to
		// isolate the grant adjudication from the CAS.
		p24MakeDue(t, r.Pool, def)
		res, err := r.ClaimDueScheduleSlot(context.Background(), p24Cand(t, r.Pool, def, src), arm)
		require.NoError(t, err)
		require.True(t, res.Claimed)
		require.Equal(t, TickOutcomeAuthRequired, res.Outcome)
		require.NotEqual(t, uuid.Nil, res.TickID)
		outcome, reason := reasonTick(t, r, res.TickID)
		assert.Equal(t, string(TickOutcomeAuthRequired), outcome)
		require.NotNil(t, reason)
		assert.Equal(t, TickReasonStaleRevision, *reason)
		assert.Zero(t, p24RunCount(t, r.Pool, def))

		// ROOT RULING (replaces the old window_too_short auth_required
		// expectation): a local arm whose ExpiresAt differs from the SAME
		// grant_id's immutable DB expiry is an illegal/mismatched snapshot.
		// It must FAIL CLOSED — a skip (remote_owner / invalid-snapshot
		// class) with ZERO run, ZERO tick and NO cursor advance — and may
		// never consume the still-live DB owner's slot.
		p24MakeDue(t, r.Pool, def)
		healthyArm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		curBefore := p24Cursor(t, r.Pool, def)
		stampBefore := p23Stamp(t, r.Pool, src)
		ticksBefore := p24TickCount(t, r.Pool, def)
		mismatched := *healthyArm
		mismatched.ExpiresAt = dbClock(t, r.Pool).Add(-time.Minute) // differs from the DB row's future expiry
		res2, err2 := r.ClaimDueScheduleSlot(context.Background(), p24Cand(t, r.Pool, def, src), &mismatched)
		require.NoError(t, err2)
		require.False(t, res2.Claimed, "a mismatched local snapshot must fail closed, not record a decision")
		assert.NotEmpty(t, res2.Skip, "the refusal is a structured skip")
		assert.Contains(t, []ScheduleClaimSkip{ScheduleSkipRemoteOwner}, res2.Skip,
			"mismatched snapshot answers the fail-closed skip (never auth_required)")
		assert.Zero(t, p24RunCount(t, r.Pool, def))
		assert.Equal(t, ticksBefore, p24TickCount(t, r.Pool, def), "zero tick")
		curAfter := p24Cursor(t, r.Pool, def)
		require.NotNil(t, curAfter)
		assert.True(t, curAfter.Equal(*curBefore), "no cursor advance on a fail-closed skip")
		assert.Equal(t, stampBefore, p23Stamp(t, r.Pool, src), "no stamp change")
	})

	t.Run("skipped_admission_capacity_and_interval_keep_stamp", func(t *testing.T) {
		// Capacity: the shared P2.3 gate refuses the scheduled slot too.
		rCap, _, tenant, src, def, out := p24FixtureDomain(t, true, p23CapConfig(1))
		p24MakeDue(t, rCap.Pool, def)
		manual, err := rCap.EnqueueSyncRun(context.Background(), def, src, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, manual)
		stamp := p23MustStamp(t, rCap.Pool, src)
		arm, _ := p24Arm(t, rCap, tenant, def, src, out, dbClock(t, rCap.Pool).Add(time.Hour))
		res, err := rCap.ClaimDueScheduleSlot(context.Background(), p24Cand(t, rCap.Pool, def, src), arm)
		require.NoError(t, err)
		require.True(t, res.Claimed)
		require.Equal(t, TickOutcomeSkippedAdmissionCapacity, res.Outcome)
		require.Nil(t, res.Run)
		require.NotNil(t, res.NextFireAt, "a skipped slot still advances the cursor")
		// The refusal means NO ADDITIONAL run: the pre-existing manual run is
		// preserved exactly (count and identity).
		assert.Equal(t, 1, p24RunCount(t, rCap.Pool, def), "the manual run is still the only run")
		var onlyRun uuid.UUID
		require.NoError(t, rCap.Pool.QueryRow(context.Background(),
			`SELECT id FROM sync_runs WHERE sync_def_id = $1`, def).Scan(&onlyRun))
		assert.Equal(t, manual.ID, onlyRun)
		p23AssertStampUnchanged(t, rCap.Pool, src, stamp, "skipped_admission_capacity")
		// Freeing capacity (legal terminal) lets the next due slot enqueue.
		claim, err := rCap.ClaimSyncRun(context.Background(), manual.ID, tenant, "p24h", "p24t", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		term, err := rCap.CompleteLeasedSyncRun(context.Background(), manual.ID, tenant, "p24h", "p24t", claim.Run.Generation,
			"aborted", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, term)
		p24MakeDue(t, rCap.Pool, def)
		res2, err2 := rCap.ClaimDueScheduleSlot(context.Background(), p24Cand(t, rCap.Pool, def, src), arm)
		require.NoError(t, err2)
		require.True(t, res2.Claimed)
		assert.Equal(t, TickOutcomeEnqueued, res2.Outcome)

		// Interval: a fresh admission stamp inside min_interval refuses the
		// scheduled slot without consuming the stamp again.
		rIv, _, tenant2, src2, def2, out2 := p24FixtureDomain(t, true, p23IntervalConfig(60_000))
		p24MakeDue(t, rIv.Pool, def2)
		_, err = rIv.EnqueueSyncRun(context.Background(), def2, src2, tenant2, uuid.New())
		require.NoError(t, err)
		stamp2 := p23MustStamp(t, rIv.Pool, src2)
		arm2, _ := p24Arm(t, rIv, tenant2, def2, src2, out2, dbClock(t, rIv.Pool).Add(time.Hour))
		resIv, errIv := rIv.ClaimDueScheduleSlot(context.Background(), p24Cand(t, rIv.Pool, def2, src2), arm2)
		require.NoError(t, errIv)
		require.True(t, resIv.Claimed)
		assert.Equal(t, TickOutcomeSkippedAdmissionInterval, resIv.Outcome)
		assert.Nil(t, resIv.Run)
		assert.NotNil(t, resIv.NextFireAt)
		p23AssertStampUnchanged(t, rIv.Pool, src2, stamp2, "skipped_admission_min_interval")
		assert.Equal(t, 1, p24RunCount(t, rIv.Pool, def2))
	})

	t.Run("error_invalid_schedule_halts_cursor_pending_repair", func(t *testing.T) {
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		// Out-of-band corruption — the only reachable path (write surfaces
		// validate).
		_, err := r.Pool.Exec(context.Background(),
			`UPDATE batch_sync_defs SET schedule_cron = 'not a cron' WHERE id = $1`, def)
		require.NoError(t, err)
		res, err := r.ClaimDueScheduleSlot(context.Background(), p24Cand(t, r.Pool, def, src), arm)
		require.NoError(t, err)
		require.True(t, res.Claimed)
		require.Equal(t, TickOutcomeErrorInvalidSchedule, res.Outcome)
		require.Nil(t, res.Run)
		require.Nil(t, res.NextFireAt, "the cursor is parked NULL pending repair")
		assert.Nil(t, p24Cursor(t, r.Pool, def))
		assert.Zero(t, p24RunCount(t, r.Pool, def))
		// The halted def never surfaces in the scan; repair re-arms it.
		due, err := r.ListDueScheduleCandidates(context.Background(), dbClock(t, r.Pool).Add(time.Hour), nil, 8)
		require.NoError(t, err)
		for _, c := range due {
			assert.NotEqual(t, def, c.SyncDefID, "a halted schedule must not be scanned")
		}
		cron := p24CronEveryMinute
		_, err = r.UpdateSyncJob(context.Background(), def, &models.UpdateSyncJobRequest{ScheduleCron: &cron}, tenant)
		require.NoError(t, err)
		require.NotNil(t, p24Cursor(t, r.Pool, def), "repair recomputes a cursor")
	})
}

// itoa was replaced by strconv.Itoa above.

// ---------------------------------------------------------------------------
// mandatory test 5 — success commit: real TickID, stamp LAST, prune
// ---------------------------------------------------------------------------

// TestPGScheduleSuccessCommitStampLastAndPrune proves the enqueued success
// path: the returned TickID is the real committed tick row; run/cursor/tick
// commit together with the next cursor strictly future; the admission stamp
// advance is the transaction's LAST statement (in-transaction visibility
// probe); and the retention prune bounds the tick history inside the same
// transaction.
func TestPGScheduleSuccessCommitStampLastAndPrune(t *testing.T) {
	t.Run("enqueued_returns_real_tick_id_and_future_cursor", func(t *testing.T) {
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		cand := p24Cand(t, r.Pool, def, src)

		res, err := r.ClaimDueScheduleSlot(context.Background(), cand, arm)
		require.NoError(t, err)
		require.True(t, res.Claimed)
		require.Equal(t, TickOutcomeEnqueued, res.Outcome)

		// Round-1 regression: the committed tick ID must come back.
		require.NotEqual(t, uuid.Nil, res.TickID, "the enqueued result must carry the real tick id")
		var dbOutcome string
		var dbRun *uuid.UUID
		var dbRev int
		var dbSlot, dbClaimed time.Time
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT outcome, run_id, schedule_revision, scheduled_for, claimed_at
			   FROM sync_schedule_ticks WHERE id = $1`, res.TickID).
			Scan(&dbOutcome, &dbRun, &dbRev, &dbSlot, &dbClaimed))
		assert.Equal(t, string(TickOutcomeEnqueued), dbOutcome)
		require.NotNil(t, dbRun)
		require.NotNil(t, res.Run)
		assert.Equal(t, res.Run.ID, *dbRun)
		assert.Equal(t, cand.Revision, dbRev)
		assert.True(t, dbSlot.Equal(cand.ScheduledFor))

		// The run is the honest pending durable admission (not execution).
		assert.Equal(t, "pending", res.Run.Status)
		assert.Equal(t, "controlled", res.Run.ControlMode)
		require.NotNil(t, res.Run.RequestedBy)
		assert.Equal(t, arm.Actor, *res.Run.RequestedBy)
		require.NotNil(t, res.Run.OrgID)
		assert.Equal(t, tenant.OrgID, res.Run.OrgID)

		// Cursor advanced to a strictly future slot; last-fire bookkeeping set.
		require.NotNil(t, res.NextFireAt)
		cur := p24Cursor(t, r.Pool, def)
		require.NotNil(t, cur)
		assert.True(t, cur.Equal(*res.NextFireAt))
		var lastFire, lastTick *time.Time
		var lastOutcome *string
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT schedule_last_fire_at, schedule_last_tick_at, schedule_last_tick_outcome
			   FROM batch_sync_defs WHERE id = $1`, def).
			Scan(&lastFire, &lastTick, &lastOutcome))
		require.NotNil(t, lastFire)
		assert.True(t, lastFire.Equal(cand.ScheduledFor))
		require.NotNil(t, lastTick)
		require.NotNil(t, lastOutcome)
		assert.Equal(t, string(TickOutcomeEnqueued), *lastOutcome)

		// The stamp advanced (consumed) — at/after the tick's claimed_at, the
		// write-time clock of the SAME transaction.
		stamp := p23MustStamp(t, r.Pool, src)
		assert.False(t, stamp.Before(dbClaimed), "the stamp anchors at write time, not before the tick")

		// The tick surfaces through the tenant-scoped read API.
		ticks, err := r.ListScheduleTicks(context.Background(), def, tenant, 50)
		require.NoError(t, err)
		require.Len(t, ticks, 1)
		assert.Equal(t, res.TickID, ticks[0].ID)
		assert.Equal(t, string(TickOutcomeEnqueued), ticks[0].Outcome)
	})

	t.Run("stamp_advance_runs_after_tick_cursor_and_prune_in_same_tx", func(t *testing.T) {
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24InstallStageFixture(t, r.Pool)
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		// Seed the FULL retention history in the SAME probe fixture: with 500
		// pre-existing ticks the claim's own tick insert brings the in-tx
		// count to 501, and the prune must have already run it back down to
		// 500 by the time the stamp fires — proving prune precedes stamp.
		_, err := r.Pool.Exec(context.Background(), `
INSERT INTO sync_schedule_ticks (id, sync_def_id, schedule_revision, scheduled_for, claimed_at, outcome, created_at)
SELECT gen_random_uuid(), $1, 1,
       clock_timestamp() - ((g + 10) || ' minutes')::interval,
       clock_timestamp() - ((g + 10) || ' minutes')::interval,
       'auth_required',
       clock_timestamp() - ((g + 10) || ' minutes')::interval
  FROM generate_series(1, 500) g`, def)
		require.NoError(t, err)
		require.Equal(t, 500, p24TickCount(t, r.Pool, def))
		p24SetStage(t, r.Pool, "probe_stamp", true)

		res, err := r.ClaimDueScheduleSlot(context.Background(), p24Cand(t, r.Pool, def, src), arm)
		require.NoError(t, err)
		require.True(t, res.Claimed)
		require.Equal(t, TickOutcomeEnqueued, res.Outcome)

		// The probe row was written by the stamp UPDATE's own trigger; its
		// snapshot of the transaction must already contain THIS claim's
		// enqueued tick (run_id IS NOT NULL — the 500 seeds are
		// auth_required/no-run), the advanced cursor AND the pruned history
		// (500 INCLUDING the new tick, not 501). Total+cursor alone could
		// still allow a cursor→stamp→tick→prune false green (stamp would see
		// the 500 OLD ticks); the run-tick count closes that hole.
		var ticks, runTicks int
		var nextFire *time.Time
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT ticks, run_ticks, next_fire FROM p24_stamp_probe ORDER BY id DESC LIMIT 1`).
			Scan(&ticks, &runTicks, &nextFire))
		assert.Equal(t, 1, runTicks, "the stamp must run after THIS claim's enqueued run-tick INSERT")
		assert.Equal(t, 500, ticks, "the stamp must run after tick INSERT and prune (500 incl. the new tick)")
		require.NotNil(t, nextFire)
		assert.True(t, nextFire.Equal(*res.NextFireAt), "the stamp must run after the cursor UPDATE")
		var kept int
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT count(*) FROM sync_schedule_ticks WHERE id = $1`, res.TickID).Scan(&kept))
		assert.Equal(t, 1, kept, "the new tick survived the prune")
	})

	t.Run("retention_prune_bounds_history_in_same_tx", func(t *testing.T) {
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		// 500 pre-existing ticks with strictly decreasing ages (no created_at
		// ties — prune order is deterministic).
		_, err := r.Pool.Exec(context.Background(), `
INSERT INTO sync_schedule_ticks (id, sync_def_id, schedule_revision, scheduled_for, claimed_at, outcome, created_at)
SELECT gen_random_uuid(), $1, 1,
       clock_timestamp() - ((g + 10) || ' minutes')::interval,
       clock_timestamp() - ((g + 10) || ' minutes')::interval,
       'auth_required',
       clock_timestamp() - ((g + 10) || ' minutes')::interval
  FROM generate_series(1, 500) g`, def)
		require.NoError(t, err)
		require.Equal(t, 500, p24TickCount(t, r.Pool, def))

		res, err := r.ClaimDueScheduleSlot(context.Background(), p24Cand(t, r.Pool, def, src), arm)
		require.NoError(t, err)
		require.True(t, res.Claimed)
		require.Equal(t, TickOutcomeEnqueued, res.Outcome)
		assert.Equal(t, 500, p24TickCount(t, r.Pool, def), "retention keeps at most 500 including the new tick")
		// The new tick survived; the oldest history row was the one pruned.
		var kept int
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT count(*) FROM sync_schedule_ticks WHERE id = $1`, res.TickID).Scan(&kept))
		assert.Equal(t, 1, kept)
	})
}

// ---------------------------------------------------------------------------
// mandatory test 6 — every-stage failure full rollback
// ---------------------------------------------------------------------------

// TestPGScheduleStageFailureFullRollback forces a deterministic failure at
// each write stage of the enqueued claim (run INSERT, tick INSERT, cursor
// UPDATE, stamp UPDATE): every stage rolls the WHOLE transaction back with
// zero durable delta, the failed session stays usable, and no connection
// lingers in an abandoned transaction.
func TestPGScheduleStageFailureFullRollback(t *testing.T) {
	for _, stage := range []string{"fail_run", "fail_tick", "fail_cursor", "fail_stamp"} {
		t.Run(stage, func(t *testing.T) {
			r, dsn, tenant, src, def, out := p24FixtureDomain(t, true, nil)
			p24InstallStageFixture(t, r.Pool)
			// Establish REAL prior admission history first: a manual enqueue
			// leaves a non-NULL stamp and one pending run, so every injected
			// failure below proves the OLD history survives byte for byte
			// (a nil-start fixture could not distinguish rollback from
			// "nothing happened yet").
			manual, err := r.EnqueueSyncRun(context.Background(), def, src, tenant, uuid.New())
			require.NoError(t, err)
			require.NotNil(t, manual)
			priorStamp := p23MustStamp(t, r.Pool, src)
			p24MakeDue(t, r.Pool, def)
			arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
			cand := p24Cand(t, r.Pool, def, src)
			defBefore := tableRowJSON(t, r.Pool, "batch_sync_defs", def)
			srcBefore := tableRowJSON(t, r.Pool, "connections", src)
			due := *p24Cursor(t, r.Pool, def)

			enq := singleConnRepo(t, dsn)
			p24SetStage(t, r.Pool, stage, true)
			res, err := enq.ClaimDueScheduleSlot(context.Background(), cand, arm)
			require.Error(t, err, "%s: the forced failure must surface", stage)
			assert.Nil(t, res, "%s: no result on failure", stage)
			// The forced failure is a genuine DB error, never a typed refusal.
			var rej *AdmissionRejection
			assert.False(t, errors.As(err, &rej), "%s: not masked as a typed admission refusal", stage)
			p24SetStage(t, r.Pool, stage, false)

			// ZERO durable delta: no tick, no ADDITIONAL run (the manual run
			// is preserved by id), cursor byte-identical, def/source rows
			// unchanged, and the PRIOR non-NULL stamp survives.
			assert.Zero(t, p24TickCount(t, r.Pool, def), stage)
			assert.Equal(t, 1, p24RunCount(t, r.Pool, def), "%s: only the manual run", stage)
			var onlyRun uuid.UUID
			require.NoError(t, r.Pool.QueryRow(context.Background(),
				`SELECT id FROM sync_runs WHERE sync_def_id = $1`, def).Scan(&onlyRun), stage)
			assert.Equal(t, manual.ID, onlyRun, "%s: the prior run survives", stage)
			cur := p24Cursor(t, r.Pool, def)
			require.NotNil(t, cur)
			assert.True(t, cur.Equal(due), "%s: cursor must not move", stage)
			assert.Equal(t, defBefore, tableRowJSON(t, r.Pool, "batch_sync_defs", def), stage)
			assert.Equal(t, srcBefore, tableRowJSON(t, r.Pool, "connections", src), stage)
			p23AssertStampUnchanged(t, r.Pool, src, priorStamp, stage+": prior admission history preserved")
			p23AssertNoIdleInTx(t, r.Pool)

			// The SAME session recovers and the claim then commits.
			res2, err2 := enq.ClaimDueScheduleSlot(context.Background(), cand, arm)
			require.NoError(t, err2, "%s: same session must be usable", stage)
			require.True(t, res2.Claimed)
			require.Equal(t, TickOutcomeEnqueued, res2.Outcome)
			assert.Equal(t, 1, p24TickCount(t, r.Pool, def))
			assert.Equal(t, 2, p24RunCount(t, r.Pool, def), "manual + scheduled runs")
		})
	}
}

// ---------------------------------------------------------------------------
// mandatory test 7 — honest dispatch residue
// ---------------------------------------------------------------------------

// TestPGScheduleHonestDispatchResidue proves the post-commit dispatch bookkeeping:
// the dispatch_failed patch flips ONLY the tick outcome (bounded detail), a
// repeat or a wrong slot touches nothing, and a later claim never adopts or
// re-touches the earlier pending run.
func TestPGScheduleHonestDispatchResidue(t *testing.T) {
	r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
	p24MakeDue(t, r.Pool, def)
	arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
	cand := p24Cand(t, r.Pool, def, src)
	res, err := r.ClaimDueScheduleSlot(context.Background(), cand, arm)
	require.NoError(t, err)
	require.True(t, res.Claimed)
	require.Equal(t, TickOutcomeEnqueued, res.Outcome)

	runBefore := snapshotSyncRun(t, r.Pool, res.Run.ID)

	// A later claim (next slot forced due) must not adopt or re-touch the
	// earlier pending run: it enqueues its OWN run.
	p24MakeDue(t, r.Pool, def)
	res2, err := r.ClaimDueScheduleSlot(context.Background(), p24Cand(t, r.Pool, def, src), arm)
	require.NoError(t, err)
	require.True(t, res2.Claimed)
	assert.Equal(t, TickOutcomeEnqueued, res2.Outcome)
	assert.NotEqual(t, res.Run.ID, res2.Run.ID, "no automatic adoption/replay")
	assertSyncRunPersisted(t, runBefore, snapshotSyncRun(t, r.Pool, res.Run.ID), "first pending run untouched")

	// Snapshots AFTER the second claim: the dispatch-failure patch must not
	// move anything the claim legitimately moved.
	defBefore := tableRowJSON(t, r.Pool, "batch_sync_defs", def)
	srcBefore := tableRowJSON(t, r.Pool, "connections", src)

	// The dispatch-failure patch: bounded detail, slot-precise, single-shot.
	ok, err := r.RecordScheduleTickDispatchFailure(context.Background(), def, cand.Revision, cand.ScheduledFor,
		strings.Repeat("x", 300))
	require.NoError(t, err)
	assert.True(t, ok)
	var outcome, detail *string
	var runID *uuid.UUID
	require.NoError(t, r.Pool.QueryRow(context.Background(),
		`SELECT outcome, detail, run_id FROM sync_schedule_ticks WHERE id = $1`, res.TickID).
		Scan(&outcome, &detail, &runID))
	require.NotNil(t, outcome)
	assert.Equal(t, string(TickOutcomeDispatchFailed), *outcome)
	require.NotNil(t, detail)
	assert.Len(t, *detail, 256, "detail is bounded to the column limit")
	require.NotNil(t, runID)
	assert.Equal(t, res.Run.ID, *runID, "the run link survives")
	assert.Equal(t, defBefore, tableRowJSON(t, r.Pool, "batch_sync_defs", def), "cursor/def untouched")
	assert.Equal(t, srcBefore, tableRowJSON(t, r.Pool, "connections", src), "stamp untouched")

	// Repeat (outcome no longer enqueued) and a wrong slot are clean no-ops.
	ok, err = r.RecordScheduleTickDispatchFailure(context.Background(), def, cand.Revision, cand.ScheduledFor, "again")
	require.NoError(t, err)
	assert.False(t, ok)
	ok, err = r.RecordScheduleTickDispatchFailure(context.Background(), def, cand.Revision, cand.ScheduledFor.Add(time.Minute), "wrong")
	require.NoError(t, err)
	assert.False(t, ok)
	p23AssertNoIdleInTx(t, r.Pool)
}

// ---------------------------------------------------------------------------
// mandatory test 8 — grant generation, arm CAS, mismatch & expiry (round-1)
// ---------------------------------------------------------------------------

// TestPGScheduleGrantGenerationCASAndArmRevoke covers the grant write
// surface: fresh grant_id per arm with cursor/revision untouched, the
// expected revision/output CAS, the round-1 defID/in.SyncDefID mismatch
// zero-write regression (cross-def and cross-tenant), the arm that waits on
// the Def lock past its own expiry (never persists an expired grant, never
// answers Live), and the revoke DB semantics.
//
// NOTE: RevokeScheduleGrant now carries the actually-deleted grant id
// (round-1 #8 safe-Drop shape, (*uuid.UUID, error)); the revoke subtest
// asserts the returned id IS the deleted generation.
func TestPGScheduleGrantGenerationCASAndArmRevoke(t *testing.T) {
	t.Run("fresh_grant_id_per_arm_cursor_and_revision_untouched", func(t *testing.T) {
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		expires := dbClock(t, r.Pool).Add(time.Hour)
		arm1, view1 := p24Arm(t, r, tenant, def, src, out, expires)
		curBefore := p24Cursor(t, r.Pool, def)
		revBefore := p24DefRevision(t, r.Pool, def)

		arm2, view2 := p24Arm(t, r, tenant, def, src, out, expires)
		assert.NotEqual(t, arm1.GrantID, arm2.GrantID, "every arm mints a fresh grant id")
		assert.NotEqual(t, view1.GrantID, view2.GrantID)
		assert.Equal(t, 1, p24GrantCount(t, r.Pool, def), "the def's grant row is replaced, not stacked")
		assert.True(t, curBefore.Equal(*p24Cursor(t, r.Pool, def)), "arm never moves the cursor (cannot re-fire a slot)")
		assert.Equal(t, revBefore, p24DefRevision(t, r.Pool, def), "arm never bumps the revision")
		assert.True(t, view2.Live, "a just-armed grant is live")
	})

	t.Run("expected_revision_or_output_drift_is_clean_conflict", func(t *testing.T) {
		r, dsn, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		ctx := context.Background()
		rev := p24DefRevision(t, r.Pool, def)
		write := models.ScheduleGrantWrite{
			SyncDefID: def, GrantID: uuid.New(), BootID: uuid.New(),
			ActorSubject: tenant.OwnerID, OwnerSubject: tenant.OwnerID, OrgID: tenant.OrgID,
			SourceID: src, OutputDatasetID: out, Revision: rev,
			ExpiresAt: dbClock(t, r.Pool).Add(time.Hour),
		}
		// The write INPUT disagreeing with its own expectations is refused
		// pre-DB as the typed input-inconsistency error (the source's newer
		// precheck layer — nothing is locked, nothing is written).
		view, err := r.ArmScheduleGrant(ctx, def, tenant, write, rev+1, out)
		assert.ErrorIs(t, err, ErrScheduleArmInput)
		assert.Nil(t, view)
		view, err = r.ArmScheduleGrant(ctx, def, tenant, write, rev, uuid.New())
		assert.ErrorIs(t, err, ErrScheduleArmInput)
		assert.Nil(t, view)
		assert.Zero(t, p24GrantCount(t, r.Pool, def), "a refused arm writes nothing")

		// The REAL target-changed races: the input material AND the expected
		// values are BOTH bound to the same precheck-old (revision, output);
		// then the LOCKED DEF's ACTUAL state is changed under the parked arm —
		// once by revision bump, once by output retarget — and the post-lock
		// CAS must answer ErrScheduleArmTargetChanged with zero writes.
		enq := singleConnRepo(t, dsn)
		enqPID := backendPID(t, enq.Pool) // warm BEFORE the sole connection goes busy

		bar := p24HoldRowBarrier(t, dsn, "batch_sync_defs", def)
		op := p24Go(ctx, func(ctx context.Context) (*models.ScheduleGrantView, error) {
			return enq.ArmScheduleGrant(ctx, def, tenant, write, rev, out)
		})
		p23RegisterCleanup(t, bar.boundedRelease, func() { op.shutdown(t) })
		waitUntilBlockedBy(t, r.Pool, enqPID, bar.pid)
		bar.exec(t, `UPDATE batch_sync_defs SET schedule_revision = schedule_revision + 1 WHERE id = $1`, def)
		bar.commit(t)
		got := mustRecv(t, op.done, "arm after the def revision moved")
		assert.ErrorIs(t, got.Err, ErrScheduleArmTargetChanged,
			"a consistent arm whose locked def revision moved is a clean conflict")
		assert.Nil(t, got.Val)
		assert.Zero(t, p24GrantCount(t, r.Pool, def))

		// Output-retarget variant on the CURRENT revision: material and
		// expectations stay bound to the precheck-old output; only the
		// locked def's actual output changes under the parked arm.
		revNow := p24DefRevision(t, r.Pool, def)
		writeO := write
		writeO.GrantID = uuid.New()
		writeO.Revision = revNow
		barO := p24HoldRowBarrier(t, dsn, "batch_sync_defs", def)
		opO := p24Go(ctx, func(ctx context.Context) (*models.ScheduleGrantView, error) {
			return enq.ArmScheduleGrant(ctx, def, tenant, writeO, revNow, out)
		})
		p23RegisterCleanup(t, barO.boundedRelease, func() { opO.shutdown(t) })
		waitUntilBlockedBy(t, r.Pool, enqPID, barO.pid)
		barO.exec(t, `UPDATE batch_sync_defs SET output_dataset_id = $2 WHERE id = $1`, def, uuid.New())
		barO.commit(t)
		gotO := mustRecv(t, opO.done, "arm after the def output moved")
		assert.ErrorIs(t, gotO.Err, ErrScheduleArmTargetChanged,
			"a consistent arm whose locked def output retargeted is a clean conflict")
		assert.Nil(t, gotO.Val)
		assert.Zero(t, p24GrantCount(t, r.Pool, def))

		// The correct expectations succeed on the same session afterwards.
		rev2 := p24DefRevision(t, r.Pool, def)
		out2 := uuid.New()
		var outNow uuid.UUID
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT output_dataset_id FROM batch_sync_defs WHERE id = $1`, def).Scan(&outNow))
		out2 = outNow
		write2 := writeO
		write2.GrantID = uuid.New()
		write2.Revision = rev2
		write2.OutputDatasetID = out2
		view2, err2 := enq.ArmScheduleGrant(ctx, def, tenant, write2, rev2, out2)
		require.NoError(t, err2)
		require.NotNil(t, view2)
	})

	t.Run("trusted_input_negative_matrix_each_field_exactly_one", func(t *testing.T) {
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		ctx := context.Background()
		rev := p24DefRevision(t, r.Pool, def)
		// A healthy CURRENT grant exists; every refusal below must leave it,
		// the def, the cursor, the stamp and the tick/run population intact.
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		grantBefore := p24GrantRow(t, r.Pool, def)
		defBefore := tableRowJSON(t, r.Pool, "batch_sync_defs", def)
		srcBefore := tableRowJSON(t, r.Pool, "connections", src)
		curBefore := p24Cursor(t, r.Pool, def)
		stampBefore := p23Stamp(t, r.Pool, src)

		valid := func() models.ScheduleGrantWrite {
			return models.ScheduleGrantWrite{
				SyncDefID: def, GrantID: uuid.New(), BootID: uuid.New(),
				ActorSubject: tenant.OwnerID, OwnerSubject: tenant.OwnerID, OrgID: tenant.OrgID,
				SourceID: src, OutputDatasetID: out, Revision: rev,
				ExpiresAt: dbClock(t, r.Pool).Add(time.Hour),
			}
		}
		other := uuid.New()
		mutations := map[string]func(w *models.ScheduleGrantWrite){
			"org_id wrong":       func(w *models.ScheduleGrantWrite) { w.OrgID = &other },
			"owner_subject wrong": func(w *models.ScheduleGrantWrite) { w.OwnerSubject = other },
			"actor_subject wrong": func(w *models.ScheduleGrantWrite) { w.ActorSubject = other },
			"revision wrong":     func(w *models.ScheduleGrantWrite) { w.Revision = rev + 1 },
			"output wrong":       func(w *models.ScheduleGrantWrite) { w.OutputDatasetID = uuid.New() },
			// A renew reusing the CURRENT grant_id — with EVERY other field
			// fully valid — is still refused: every arm must mint a FRESH
			// grant id. (Reusing the id while also changing another field
			// would let the owner-mismatch check alone carry the refusal.)
			"same grant id otherwise valid": func(w *models.ScheduleGrantWrite) {
				w.GrantID = arm.GrantID
			},
		}
		for name, mutate := range mutations {
			w := valid()
			mutate(&w)
			view, err := r.ArmScheduleGrant(ctx, def, tenant, w, rev, out)
			assert.Error(t, err, name+": illegal trusted input must be refused")
			assert.Nil(t, view, name)
		}
		// Everything is byte-unchanged; no new run or tick appeared.
		assert.Equal(t, grantBefore, p24GrantRow(t, r.Pool, def), "the current grant survives every refusal")
		assert.Equal(t, defBefore, tableRowJSON(t, r.Pool, "batch_sync_defs", def))
		assert.Equal(t, srcBefore, tableRowJSON(t, r.Pool, "connections", src))
		assert.True(t, p24Cursor(t, r.Pool, def).Equal(*curBefore))
		assert.Equal(t, stampBefore, p23Stamp(t, r.Pool, src))
		assert.Zero(t, p24TickCount(t, r.Pool, def))
		assert.Zero(t, p24RunCount(t, r.Pool, def))
	})

	t.Run("defid_input_mismatch_zero_write_crossdef_and_crosstenant", func(t *testing.T) {
		r, _, tenant, src, defA, outA := p24FixtureDomain(t, true, nil)
		ctx := context.Background()
		enabled := true
		// A second def of the same source/tenant.
		jobB, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: src, OutputDatasetID: uuid.New(),
			ScheduleCron: ptrTo(p24CronEveryMinute), ScheduleEnabled: &enabled,
		}, tenant)
		require.NoError(t, err)
		defB := jobB.ID
		// A def in a DIFFERENT tenant (personal domain, other owner).
		otherTenant := Tenant{OwnerID: uuid.New()}
		connC, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
			Name: "p24-src-c", ConnectorType: "rest_api"}, otherTenant)
		require.NoError(t, err)
		jobC, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: connC.ID, OutputDatasetID: uuid.New(),
			ScheduleCron: ptrTo(p24CronEveryMinute), ScheduleEnabled: &enabled,
		}, otherTenant)
		require.NoError(t, err)

		revA := p24DefRevision(t, r.Pool, defA)
		rowA := tableRowJSON(t, r.Pool, "batch_sync_defs", defA)
		rowB := tableRowJSON(t, r.Pool, "batch_sync_defs", defB)
		makeWrite := func(def uuid.UUID) models.ScheduleGrantWrite {
			return models.ScheduleGrantWrite{
				SyncDefID: def, GrantID: uuid.New(), BootID: uuid.New(),
				ActorSubject: tenant.OwnerID, OwnerSubject: tenant.OwnerID, OrgID: tenant.OrgID,
				SourceID: src, OutputDatasetID: outA, Revision: revA,
				ExpiresAt: dbClock(t, r.Pool).Add(time.Hour),
			}
		}

		// Cross-def: parameter defID is defA, the write targets defB.
		view, err := r.ArmScheduleGrant(ctx, defA, tenant, makeWrite(defB), revA, outA)
		require.Error(t, err, "defID/in.SyncDefID mismatch must be rejected before any write")
		assert.Nil(t, view)
		// Cross-tenant: the write targets defC (invisible in this tenant).
		view, err = r.ArmScheduleGrant(ctx, defA, tenant, makeWrite(jobC.ID), revA, outA)
		require.Error(t, err)
		assert.Nil(t, view)

		assert.Zero(t, p24GrantCount(t, r.Pool, defA), "no grant row may appear for the locked def")
		assert.Zero(t, p24GrantCount(t, r.Pool, defB), "no grant row may appear for the input def")
		assert.Zero(t, p24GrantCount(t, r.Pool, jobC.ID))
		assert.Equal(t, rowA, tableRowJSON(t, r.Pool, "batch_sync_defs", defA))
		assert.Equal(t, rowB, tableRowJSON(t, r.Pool, "batch_sync_defs", defB))
	})

	t.Run("arm_waiting_on_def_lock_past_its_own_expiry_errors_cleanly", func(t *testing.T) {
		r, dsn, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		expires := dbClock(t, r.Pool).Add(2500 * time.Millisecond) // inside the 5s op budget
		rev := p24DefRevision(t, r.Pool, def)
		write := models.ScheduleGrantWrite{
			SyncDefID: def, GrantID: uuid.New(), BootID: uuid.New(),
			ActorSubject: tenant.OwnerID, OwnerSubject: tenant.OwnerID, OrgID: tenant.OrgID,
			SourceID: src, OutputDatasetID: out, Revision: rev, ExpiresAt: expires,
		}
		defBefore := tableRowJSON(t, r.Pool, "batch_sync_defs", def)
		// The Def row lock is held; the arm parks on it. PID warmed BEFORE the
		// sole connection goes busy (a later query would self-deadlock).
		bar := p24HoldRowBarrier(t, dsn, "batch_sync_defs", def)
		enq := singleConnRepo(t, dsn)
		enqPID := backendPID(t, enq.Pool)
		op := p24Go(context.Background(), func(ctx context.Context) (*models.ScheduleGrantView, error) {
			return enq.ArmScheduleGrant(ctx, def, tenant, write, rev, out)
		})
		p23RegisterCleanup(t, bar.boundedRelease, func() { op.shutdown(t) })
		waitUntilBlockedBy(t, r.Pool, enqPID, bar.pid)
		// The deadline is STILL VALID at genuine-block time — the proof would
		// be vacuous if the arm were already expired when it parked.
		require.True(t, dbClock(t, r.Pool).Before(expires), "block observed while the input is still valid")
		p23WaitClockPast(t, r.Pool, expires.Add(200*time.Millisecond))
		bar.release(t)
		got := mustRecv(t, op.done, "arm after its own expiry")

		// The arm input expired while waiting on the Def lock: the typed
		// ErrScheduleArmExpired, nothing persisted, nothing live.
		assert.ErrorIs(t, got.Err, ErrScheduleArmExpired,
			"an arm whose deadline passed on the Def wait must answer the typed expiry error")
		assert.Nil(t, got.Val)
		assert.Zero(t, p24GrantCount(t, r.Pool, def), "no expired grant row may be persisted")
		assert.Equal(t, defBefore, tableRowJSON(t, r.Pool, "batch_sync_defs", def))
		p23AssertNoIdleInTx(t, r.Pool)

		// The same session arms fine with a fresh future deadline.
		write2 := write
		write2.GrantID = uuid.New()
		write2.ExpiresAt = dbClock(t, r.Pool).Add(time.Hour)
		view2, err2 := enq.ArmScheduleGrant(context.Background(), def, tenant, write2, rev, out)
		require.NoError(t, err2)
		require.NotNil(t, view2)
		assert.True(t, view2.Live)
	})

	t.Run("arm_waiting_on_grant_row_past_its_own_expiry_errors_cleanly", func(t *testing.T) {
		// The SECOND wait an arm can hit: the Def lock is FREE (acquired
		// immediately), but the grant-row UPSERT parks on an externally held
		// pure FOR UPDATE of the existing grant row. Crossing the deadline
		// during THAT wait must still answer ErrScheduleArmExpired and leave
		// the pre-existing grant byte-identical.
		r, dsn, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		grantBefore := p24GrantRow(t, r.Pool, def)
		defBefore := tableRowJSON(t, r.Pool, "batch_sync_defs", def)
		expires := dbClock(t, r.Pool).Add(2500 * time.Millisecond)
		rev := p24DefRevision(t, r.Pool, def)
		write := models.ScheduleGrantWrite{
			SyncDefID: def, GrantID: uuid.New(), BootID: uuid.New(),
			ActorSubject: tenant.OwnerID, OwnerSubject: tenant.OwnerID, OrgID: tenant.OrgID,
			SourceID: src, OutputDatasetID: out, Revision: rev, ExpiresAt: expires,
		}
		bar := p24HoldRowBarrier(t, dsn, "sync_schedule_grants", def)
		enq := singleConnRepo(t, dsn)
		enqPID := backendPID(t, enq.Pool) // warm BEFORE launching
		op := p24Go(context.Background(), func(ctx context.Context) (*models.ScheduleGrantView, error) {
			return enq.ArmScheduleGrant(ctx, def, tenant, write, rev, out)
		})
		p23RegisterCleanup(t, bar.boundedRelease, func() { op.shutdown(t) })
		waitUntilBlockedBy(t, r.Pool, enqPID, bar.pid)
		require.True(t, dbClock(t, r.Pool).Before(expires), "block observed while the input is still valid")
		p23WaitClockPast(t, r.Pool, expires.Add(200*time.Millisecond))
		bar.release(t)
		got := mustRecv(t, op.done, "arm after expiry on the grant-row wait")

		assert.ErrorIs(t, got.Err, ErrScheduleArmExpired,
			"crossing the deadline on the grant-row wait must answer the typed expiry error")
		assert.Nil(t, got.Val)
		// Full prior state preserved: the OLD grant survives byte for byte.
		assert.Equal(t, grantBefore, p24GrantRow(t, r.Pool, def))
		assert.Equal(t, defBefore, tableRowJSON(t, r.Pool, "batch_sync_defs", def))
		assert.Equal(t, 1, p24GrantCount(t, r.Pool, def))
		p23AssertNoIdleInTx(t, r.Pool)

		// The same session arms fine once the input deadline is healthy.
		write2 := write
		write2.GrantID = uuid.New()
		write2.ExpiresAt = dbClock(t, r.Pool).Add(time.Hour)
		view2, err2 := enq.ArmScheduleGrant(context.Background(), def, tenant, write2, rev, out)
		require.NoError(t, err2)
		require.NotNil(t, view2)
		assert.True(t, view2.Live)
	})

	t.Run("revoke_returns_exactly_the_deleted_generation", func(t *testing.T) {
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		require.Equal(t, 1, p24GrantCount(t, r.Pool, def))

		// The revoke returns the ACTUALLY deleted grant id — the round-1 #8
		// safe-Drop contract (a local Drop may only remove that generation).
		revoked, err := r.RevokeScheduleGrant(context.Background(), def, tenant)
		require.NoError(t, err)
		require.NotNil(t, revoked)
		assert.Equal(t, arm.GrantID, *revoked)
		assert.Zero(t, p24GrantCount(t, r.Pool, def))
		// Idempotent: nothing left to delete answers nil.
		revoked, err = r.RevokeScheduleGrant(context.Background(), def, tenant)
		require.NoError(t, err)
		assert.Nil(t, revoked)
		// An invisible def answers nil without touching anything.
		other := Tenant{OwnerID: uuid.New()}
		revoked, err = r.RevokeScheduleGrant(context.Background(), def, other)
		require.NoError(t, err)
		assert.Nil(t, revoked)

		// After revocation the due slot answers auth_required/never_armed.
		res, err := r.ClaimDueScheduleSlot(context.Background(), p24Cand(t, r.Pool, def, src), nil)
		require.NoError(t, err)
		require.True(t, res.Claimed)
		assert.Equal(t, TickOutcomeAuthRequired, res.Outcome)
	})
}

// ---------------------------------------------------------------------------
// mandatory test 9 — heartbeat exact generation, two-phase clock, MVCC def
// ---------------------------------------------------------------------------

// TestPGScheduleHeartbeatExactGeneration covers the round-1 heartbeat
// corrections: only the exact generation updates; the update happens AFTER a
// row lock plus a FRESH clock read on the same connection (a pure FOR UPDATE
// blocker that crosses the expiry or freshness window must yield false with
// the row byte-identical — the P2.1 EvalPlanQual trap a single UPDATE WHERE
// clock falls into); and the post-lock Def check is an MVCC read that never
// takes the Def lock (no Grant→Def reverse order).
func TestPGScheduleHeartbeatExactGeneration(t *testing.T) {
	t.Run("exact_generation_updates_only_last_seen", func(t *testing.T) {
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		var seenBefore time.Time
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT last_seen_at FROM sync_schedule_grants WHERE sync_def_id = $1`, def).Scan(&seenBefore))
		immutableBefore := p24GrantImmutable(t, r.Pool, def)

		ok, err := r.HeartbeatScheduleGrant(context.Background(), def, arm.GrantID, arm.BootID, arm.Actor,
			arm.Revision, 90*time.Second)
		require.NoError(t, err)
		assert.True(t, ok)

		// EVERY column except last_seen_at is byte-identical after a success
		// (expires_at is immutable to heartbeats among them).
		var lastSeen time.Time
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT last_seen_at FROM sync_schedule_grants WHERE sync_def_id = $1`, def).Scan(&lastSeen))
		assert.True(t, lastSeen.After(seenBefore), "the heartbeat advanced last_seen_at")
		assert.Equal(t, immutableBefore, p24GrantImmutable(t, r.Pool, def),
			"a successful heartbeat changes ONLY last_seen_at")

		// Every field of the generation must match exactly; each mismatch is
		// a zero-row no-op with the row byte-identical.
		before2 := p24GrantRow(t, r.Pool, def)
		wrong := []struct {
			grantID, bootID, actor uuid.UUID
			revision               int
		}{
			{uuid.New(), arm.BootID, arm.Actor, arm.Revision},
			{arm.GrantID, uuid.New(), arm.Actor, arm.Revision},
			{arm.GrantID, arm.BootID, uuid.New(), arm.Revision},
			{arm.GrantID, arm.BootID, arm.Actor, arm.Revision + 1},
		}
		for i, w := range wrong {
			ok, err := r.HeartbeatScheduleGrant(context.Background(), def, w.grantID, w.bootID, w.actor, w.revision, 90*time.Second)
			require.NoError(t, err, "mismatch %d", i)
			assert.False(t, ok, "mismatch %d: wrong generation must not heartbeat", i)
		}
		assert.Equal(t, before2, p24GrantRow(t, r.Pool, def), "failed heartbeats leave the row byte-identical")

		// The def's CURRENT binding is re-validated per heartbeat: a revision
		// bump or an output retarget invalidates the grant generation and the
		// heartbeat refuses with the row preserved (round-2 #11 semantics).
		cron := "*/4 * * * *"
		_, err = r.UpdateSyncJob(context.Background(), def, &models.UpdateSyncJobRequest{ScheduleCron: &cron}, tenant)
		require.NoError(t, err)
		before3 := p24GrantRow(t, r.Pool, def)
		ok, err = r.HeartbeatScheduleGrant(context.Background(), def, arm.GrantID, arm.BootID, arm.Actor,
			arm.Revision, 90*time.Second)
		require.NoError(t, err)
		assert.False(t, ok, "a revision bump invalidates the heartbeat generation")
		assert.Equal(t, before3, p24GrantRow(t, r.Pool, def), "the invalidated heartbeat leaves the row untouched")

		// Re-arm at the current revision, then retarget the output: the
		// mismatched output binding also refuses, row preserved.
		arm2, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		retarget := uuid.New()
		_, err = r.UpdateSyncJob(context.Background(), def, &models.UpdateSyncJobRequest{OutputDatasetID: &retarget}, tenant)
		require.NoError(t, err)
		before4 := p24GrantRow(t, r.Pool, def)
		ok, err = r.HeartbeatScheduleGrant(context.Background(), def, arm2.GrantID, arm2.BootID, arm2.Actor,
			arm2.Revision, 90*time.Second)
		require.NoError(t, err)
		assert.False(t, ok, "an output retarget invalidates the heartbeat generation")
		assert.Equal(t, before4, p24GrantRow(t, r.Pool, def), "the invalidated heartbeat leaves the row untouched")
	})

	t.Run("two_phase_clock_pure_blocker_crossing_expiry", func(t *testing.T) {
		r, dsn, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(2500*time.Millisecond))
		rowBefore := p24GrantRow(t, r.Pool, def)

		// A pure FOR UPDATE blocker on the grant row: no new row version, so
		// a single-statement UPDATE would evaluate its clock predicates
		// BEFORE the wait and still succeed after the deadline (the P2.1
		// trap). The two-phase fix must re-read the clock AFTER the lock.
		bar := p24HoldRowBarrier(t, dsn, "sync_schedule_grants", def)
		enq := singleConnRepo(t, dsn)
		enqPID := backendPID(t, enq.Pool) // warm BEFORE the sole connection goes busy
		op := p24Go(context.Background(), func(ctx context.Context) (bool, error) {
			return enq.HeartbeatScheduleGrant(ctx, def, arm.GrantID, arm.BootID, arm.Actor,
				arm.Revision, 90*time.Second)
		})
		p23RegisterCleanup(t, bar.boundedRelease, func() { op.shutdown(t) })
		waitUntilBlockedBy(t, r.Pool, enqPID, bar.pid)
		// The grant is STILL VALID at genuine-block time — otherwise an
		// implementation rejecting an already-expired input would pass vacuously.
		require.True(t, dbClock(t, r.Pool).Before(arm.ExpiresAt), "block observed while the grant is still live")
		p23WaitClockPast(t, r.Pool, arm.ExpiresAt.Add(200*time.Millisecond))
		bar.release(t)
		got := mustRecv(t, op.done, "heartbeat after crossing expiry")

		require.NoError(t, got.Err)
		assert.False(t, got.Val, "a grant that expired while the heartbeat waited must refuse")
		assert.Equal(t, rowBefore, p24GrantRow(t, r.Pool, def), "the refused heartbeat must not touch the row")
		p23AssertNoIdleInTx(t, r.Pool)

		// The same session recovers: re-arm a healthy generation and the
		// heartbeat succeeds on it.
		arm2, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		ok, err := enq.HeartbeatScheduleGrant(context.Background(), def, arm2.GrantID, arm2.BootID, arm2.Actor,
			arm2.Revision, 90*time.Second)
		require.NoError(t, err)
		assert.True(t, ok, "the same session must heartbeat a healthy generation afterwards")
	})

	t.Run("two_phase_clock_pure_blocker_crossing_freshness", func(t *testing.T) {
		r, dsn, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
		// last_seen sits 89.5s in the past: valid at send time, stale shortly
		// after — the wait must push it across the 90s freshness boundary.
		p24SetGrantCol(t, r.Pool, def, `last_seen_at = clock_timestamp() - interval '89.5 seconds'`)
		var lastSeen time.Time
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT last_seen_at FROM sync_schedule_grants WHERE sync_def_id = $1`, def).Scan(&lastSeen))
		rowBefore := p24GrantRow(t, r.Pool, def)

		bar := p24HoldRowBarrier(t, dsn, "sync_schedule_grants", def)
		enq := singleConnRepo(t, dsn)
		enqPID := backendPID(t, enq.Pool) // warm BEFORE the sole connection goes busy
		op := p24Go(context.Background(), func(ctx context.Context) (bool, error) {
			return enq.HeartbeatScheduleGrant(ctx, def, arm.GrantID, arm.BootID, arm.Actor,
				arm.Revision, 90*time.Second)
		})
		p23RegisterCleanup(t, bar.boundedRelease, func() { op.shutdown(t) })
		waitUntilBlockedBy(t, r.Pool, enqPID, bar.pid)
		// Freshness is STILL VALID at genuine-block time (last_seen + 90s is
		// still ahead of the clock); the proof is not vacuous.
		require.True(t, dbClock(t, r.Pool).Before(lastSeen.Add(90*time.Second)),
			"block observed while the owner is still fresh")
		p23WaitClockPast(t, r.Pool, lastSeen.Add(90*time.Second+200*time.Millisecond))
		bar.release(t)
		got := mustRecv(t, op.done, "heartbeat after crossing freshness")

		require.NoError(t, got.Err)
		assert.False(t, got.Val, "an owner that went stale while the heartbeat waited must refuse")
		assert.Equal(t, rowBefore, p24GrantRow(t, r.Pool, def), "the refused heartbeat must not touch the row")
		p23AssertNoIdleInTx(t, r.Pool)
	})

	t.Run("def_check_is_mvcc_no_reverse_lock", func(t *testing.T) {
		r, dsn, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))

		// The DEF row is externally locked; a heartbeat whose grant row is
		// free must still complete (it may only MVCC-read the def, never take
		// the Def lock after the Grant lock — the reverse order is forbidden).
		bar := p24HoldRowBarrier(t, dsn, "batch_sync_defs", def)
		start := time.Now()
		ok, err := r.HeartbeatScheduleGrant(context.Background(), def, arm.GrantID, arm.BootID, arm.Actor,
			arm.Revision, 90*time.Second)
		elapsed := time.Since(start)
		bar.release(t)
		require.NoError(t, err)
		assert.True(t, ok, "a valid heartbeat must not need the Def lock")
		assert.Less(t, elapsed, 3*time.Second, "MVCC read must not block on the Def lock, took %s", elapsed)
		var expires time.Time
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT expires_at FROM sync_schedule_grants WHERE sync_def_id = $1`, def).Scan(&expires))
		assert.True(t, expires.Equal(arm.ExpiresAt), "heartbeat never extends expires_at")
	})
}

// ---------------------------------------------------------------------------
// mandatory test 10 — GetScheduleGrantView Live by ACTUAL def revision
// ---------------------------------------------------------------------------

// TestPGScheduleGetViewLiveByActualRevision is the round-1 #5 regression:
// liveness must be computed against the ACTUAL joined Def revision inside
// the same SQL, so a stale caller defRevision argument can never resurrect a
// grant whose def revision moved.
func TestPGScheduleGetViewLiveByActualRevision(t *testing.T) {
	r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
	arm, view := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
	rev := arm.Revision
	require.True(t, view.Live)

	// A real config change bumps the def revision; the grant now predates it.
	cron := "*/5 * * * *"
	_, err := r.UpdateSyncJob(context.Background(), def, &models.UpdateSyncJobRequest{ScheduleCron: &cron}, tenant)
	require.NoError(t, err)
	newRev := p24DefRevision(t, r.Pool, def)
	require.Greater(t, newRev, rev)

	// The round-1 #5 fix removed the caller defRevision argument entirely:
	// liveness is computed against the ACTUAL joined def revision inside the
	// SQL, so no stale caller input can resurrect a moved grant. The revision
	// bump above must therefore read Live=false.
	got, err := r.GetScheduleGrantView(context.Background(), def, tenant)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.Live, "liveness must follow the ACTUAL def revision")
	assert.Equal(t, arm.GrantID, got.GrantID)
	assert.Equal(t, rev, got.Revision, "the view still projects the grant row's own revision")

	// ...and re-arming at the current revision restores liveness.
	arm2, view2 := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))
	require.True(t, view2.Live)
	got, err = r.GetScheduleGrantView(context.Background(), def, tenant)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.Live)
	assert.Equal(t, arm2.GrantID, got.GrantID)

	// The invisible-tenant check runs WHILE the live grant exists (after the
	// revoke it would prove nothing): another tenant sees no grant at all.
	other := Tenant{OwnerID: uuid.New()}
	gotOther, err := r.GetScheduleGrantView(context.Background(), def, other)
	require.NoError(t, err)
	assert.Nil(t, gotOther, "an invisible def projects no grant even while a live one exists")

	// After a revoke the owner tenant also sees no row.
	revokedID, err := r.RevokeScheduleGrant(context.Background(), def, tenant)
	require.NoError(t, err)
	require.NotNil(t, revokedID)
	assert.Equal(t, arm2.GrantID, *revokedID)
	got, err = r.GetScheduleGrantView(context.Background(), def, tenant)
	require.NoError(t, err)
	assert.Nil(t, got)
}

// ---------------------------------------------------------------------------
// mandatory test 11 — config semantics & validation zero-write
// ---------------------------------------------------------------------------

// TestPGScheduleConfigSemanticsZeroWrite covers the revision/cursor config
// semantics and the zero-write validation discipline of every schedule write
// surface at repo level.
func TestPGScheduleConfigSemanticsZeroWrite(t *testing.T) {
	t.Run("revision_bumps_on_actual_change_only_and_cursor_rules", func(t *testing.T) {
		r, _, tenant, _, def, _ := p24FixtureDomain(t, true, nil)
		ctx := context.Background()
		base := p24DefRevision(t, r.Pool, def)

		// Same-value PATCH: no bump.
		same := p24CronEveryMinute
		job, err := r.UpdateSyncJob(ctx, def, &models.UpdateSyncJobRequest{ScheduleCron: &same}, tenant)
		require.NoError(t, err)
		assert.Equal(t, base, p24DefRevision(t, r.Pool, def), "same-value patch does not bump")

		// Actual cron change: bump + cursor recomputed from the LOCKED clock
		// as the NEW cron's own boundary math. NO new≠old inequality: an
		// every-minute → */7 change legitimately recomputes to the SAME next
		// minute whenever that minute is ≡0 mod 7 (root's example: minute 49
		// satisfies both expressions), so a must-differ assertion was simply
		// wrong. The deterministic proof is: cursor is strictly after the
		// pre-patch clock, sits exactly ON a */7 minute boundary (UTC wall
		// clock, second/nanosecond zero), and equals one of the two candidate
		// boundaries reachable from a locked clock inside [preChange, commit]
		// (the second candidate only if the lock itself crossed the first).
		preChange := dbClock(t, r.Pool)
		changed := "*/7 * * * *"
		job, err = r.UpdateSyncJob(ctx, def, &models.UpdateSyncJobRequest{ScheduleCron: &changed}, tenant)
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, base+1, p24DefRevision(t, r.Pool, def))
		cur := p24Cursor(t, r.Pool, def)
		require.NotNil(t, cur)
		assert.True(t, cur.After(preChange), "recomputed cursor is strictly future (pre-patch anchor)")
		curU := cur.UTC()
		assert.Zero(t, curU.Second())
		assert.Zero(t, curU.Nanosecond())
		assert.Zero(t, curU.Minute()%7, "cursor must be a */7 minute boundary")
		c1 := p24NextSeventhMinuteBoundary(preChange)
		assert.True(t, curU.Equal(c1) || curU.Equal(c1.Add(7*time.Minute)),
			"cursor must equal the NEW cron's boundary computed from the locked clock: got %v, candidates %v / %v",
			curU, c1, c1.Add(7*time.Minute))

		// file_glob actual change bumps too.
		glob := "*.csv"
		_, err = r.UpdateSyncJob(ctx, def, &models.UpdateSyncJobRequest{FileGlob: &glob}, tenant)
		require.NoError(t, err)
		assert.Equal(t, base+2, p24DefRevision(t, r.Pool, def))

		// Disable: cursor NULL.
		disabled := false
		_, err = r.UpdateSyncJob(ctx, def, &models.UpdateSyncJobRequest{ScheduleEnabled: &disabled}, tenant)
		require.NoError(t, err)
		assert.Equal(t, base+3, p24DefRevision(t, r.Pool, def))
		assert.Nil(t, p24Cursor(t, r.Pool, def), "disable parks the cursor NULL")

		// Re-enable with the stored */7 cron: strictly-future cursor that
		// again matches the SAME boundary math (the stored expression did not
		// change, so this proves persistence + recompute-from-locked-clock).
		preEnable := dbClock(t, r.Pool)
		enabled := true
		_, err = r.UpdateSyncJob(ctx, def, &models.UpdateSyncJobRequest{ScheduleEnabled: &enabled}, tenant)
		require.NoError(t, err)
		cur = p24Cursor(t, r.Pool, def)
		require.NotNil(t, cur)
		assert.True(t, cur.After(preEnable), "re-enabled cursor is strictly future (pre-patch anchor)")
		curU = cur.UTC()
		assert.Zero(t, curU.Minute()%7, "re-enabled cursor is a */7 boundary")
		e1 := p24NextSeventhMinuteBoundary(preEnable)
		assert.True(t, curU.Equal(e1) || curU.Equal(e1.Add(7*time.Minute)),
			"re-enabled cursor must equal the stored cron's boundary from the locked clock: got %v, candidates %v / %v",
			curU, e1, e1.Add(7*time.Minute))

		// Explicit empty cron clears + disables + bumps.
		empty := ""
		_, err = r.UpdateSyncJob(ctx, def, &models.UpdateSyncJobRequest{ScheduleCron: &empty}, tenant)
		require.NoError(t, err)
		assert.Nil(t, p24Cursor(t, r.Pool, def))
		var en bool
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT schedule_enabled FROM batch_sync_defs WHERE id = $1`, def).Scan(&en))
		assert.False(t, en)
	})

	t.Run("invalid_input_zero_write_matrix", func(t *testing.T) {
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, nil)
		ctx := context.Background()
		rowBefore := tableRowJSON(t, r.Pool, "batch_sync_defs", def)

		// A 5-field expression whose TRIMMED length really exceeds 256 (legal
		// value lists, only the length is illegal) — trailing whitespace
		// alone would trim back to a legal expression.
		minutes := strings.TrimSuffix(strings.Join(p24RangeStrings(0, 59), ","), ",")
		hours := strings.TrimSuffix(strings.Join(p24RangeStrings(0, 23), ","), ",")
		days := strings.TrimSuffix(strings.Join(p24RangeStrings(1, 31), ","), ",")
		oversize := minutes + " " + hours + " " + days + " * *"
		require.Greater(t, len(strings.TrimSpace(oversize)), 256, "fixture: trimmed oversize must exceed 256")

		badCrons := map[string]string{
			"four fields":  "* * * *",
			"seven fields": "* * * * * * *",
			"oversize":     oversize,
		}
		for name, cron := range badCrons {
			c := cron
			_, err := r.UpdateSyncJob(ctx, def, &models.UpdateSyncJobRequest{ScheduleCron: &c}, tenant)
			assert.Error(t, err, name+": invalid cron must be refused")
			var inv *models.InvalidScheduleError
			assert.True(t, errors.As(err, &inv), name+": stable invalid_schedule error")
		}
		// Normal SURROUNDING whitespace stays acceptable (trim discipline).
		padded := "  " + p24CronEveryMinute + "  "
		_, err := r.UpdateSyncJob(ctx, def, &models.UpdateSyncJobRequest{ScheduleCron: &padded}, tenant)
		require.NoError(t, err, "surrounding whitespace is trimmed and accepted")
		rowBefore = tableRowJSON(t, r.Pool, "batch_sync_defs", def)

		badTZ := "Not/AZone"
		_, err = r.UpdateSyncJob(ctx, def, &models.UpdateSyncJobRequest{ScheduleTimeZone: &badTZ}, tenant)
		assert.Error(t, err)
		var inv *models.InvalidScheduleError
		assert.True(t, errors.As(err, &inv))

		// Create-side validation refuses the same shapes with zero rows.
		var defs int
		require.NoError(t, r.Pool.QueryRow(ctx, `SELECT count(*) FROM batch_sync_defs`).Scan(&defs))
		enabled := true
		_, err = r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: src, OutputDatasetID: out,
			ScheduleCron: ptrTo("* * * *"), ScheduleEnabled: &enabled,
		}, tenant)
		assert.Error(t, err)
		var defsAfter int
		require.NoError(t, r.Pool.QueryRow(ctx, `SELECT count(*) FROM batch_sync_defs`).Scan(&defsAfter))
		assert.Equal(t, defs, defsAfter, "refused create writes zero rows")

		// Everything above left the row byte-identical.
		assert.Equal(t, rowBefore, tableRowJSON(t, r.Pool, "batch_sync_defs", def))
	})

	t.Run("enable_without_cron_refused_on_a_cronless_def", func(t *testing.T) {
		// A dedicated DISABLED, NO-CRON def: enabling without a cron must be
		// refused with zero durable change. (The main fixture already carries
		// a valid cron — re-enabling it would be a legal same-value patch.)
		r, _, tenant, src, _, _ := p24FixtureDomain(t, true, nil)
		ctx := context.Background()
		job, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: src, OutputDatasetID: uuid.New(),
		}, tenant)
		require.NoError(t, err)
		require.False(t, job.ScheduleEnabled)
		require.Nil(t, job.ScheduleCron)
		rowBefore := tableRowJSON(t, r.Pool, "batch_sync_defs", job.ID)

		enabled := true
		_, err = r.UpdateSyncJob(ctx, job.ID, &models.UpdateSyncJobRequest{ScheduleEnabled: &enabled}, tenant)
		require.Error(t, err, "enable requires a valid nonempty cron")
		var inv *models.InvalidScheduleError
		assert.True(t, errors.As(err, &inv))
		assert.Equal(t, rowBefore, tableRowJSON(t, r.Pool, "batch_sync_defs", job.ID), "refused enable leaves the row untouched")
		assert.Nil(t, p24Cursor(t, r.Pool, job.ID))
	})

	t.Run("legacy_invalid_cron_survives_untouched_edits", func(t *testing.T) {
		r, _, tenant, _, def, _ := p24FixtureDomain(t, true, nil)
		ctx := context.Background()
		// Out-of-band legacy junk (pre-P2.4 saved string), disabled.
		_, err := r.Pool.Exec(ctx,
			`UPDATE batch_sync_defs SET schedule_cron = 'legacy junk', schedule_enabled = false, file_glob = '*.parquet' WHERE id = $1`, def)
		require.NoError(t, err)
		rev := p24DefRevision(t, r.Pool, def)

		// A SAME-VALUE glob PATCH must NOT re-validate the stored legacy cron
		// (compatibility) and must not bump the revision.
		sameGlob := "*.parquet"
		_, err = r.UpdateSyncJob(ctx, def, &models.UpdateSyncJobRequest{FileGlob: &sameGlob}, tenant)
		require.NoError(t, err, "untouched legacy invalid cron stays compatible")
		assert.Equal(t, rev, p24DefRevision(t, r.Pool, def), "same-value patch does not bump")
		var cron string
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT schedule_cron FROM batch_sync_defs WHERE id = $1`, def).Scan(&cron))
		assert.Equal(t, "legacy junk", cron)

		// An ACTUAL glob change is a real config change: it bumps the
		// revision (still without re-validating the stored junk cron).
		newGlob := "*.csv"
		_, err = r.UpdateSyncJob(ctx, def, &models.UpdateSyncJobRequest{FileGlob: &newGlob}, tenant)
		require.NoError(t, err, "the glob change itself is legal")
		assert.Equal(t, rev+1, p24DefRevision(t, r.Pool, def), "an actual glob change bumps the revision")
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT schedule_cron FROM batch_sync_defs WHERE id = $1`, def).Scan(&cron))
		assert.Equal(t, "legacy junk", cron, "the junk cron survives without revalidation")

		// Enabling the legacy junk must be refused (explicit new validation).
		enabled := true
		_, err = r.UpdateSyncJob(ctx, def, &models.UpdateSyncJobRequest{ScheduleEnabled: &enabled}, tenant)
		assert.Error(t, err)

		// Replacing it with a valid cron validates, bumps again and arms a cursor.
		valid := "*/3 * * * *"
		_, err = r.UpdateSyncJob(ctx, def, &models.UpdateSyncJobRequest{ScheduleCron: &valid, ScheduleEnabled: &enabled}, tenant)
		require.NoError(t, err)
		assert.Equal(t, rev+2, p24DefRevision(t, r.Pool, def))
		require.NotNil(t, p24Cursor(t, r.Pool, def))
	})

	t.Run("quartz6_and_utc_default_and_iana_tz", func(t *testing.T) {
		r, _, tenant, src, _, _ := p24FixtureDomain(t, true, nil)
		ctx := context.Background()
		// Six fields ⇒ Quartz6 flavour accepted.
		enabled := true
		job, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: src, OutputDatasetID: uuid.New(),
			ScheduleCron: ptrTo("0 * * * * *"), ScheduleEnabled: &enabled,
		}, tenant)
		require.NoError(t, err)
		assert.Equal(t, "UTC", job.ScheduleTimeZone, "default timezone is UTC")

		// An explicit IANA timezone is accepted and persisted.
		tz := "America/New_York"
		job2, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: src, OutputDatasetID: uuid.New(),
			ScheduleCron: ptrTo(p24CronEveryMinute), ScheduleTimeZone: &tz, ScheduleEnabled: &enabled,
		}, tenant)
		require.NoError(t, err)
		assert.Equal(t, tz, job2.ScheduleTimeZone)
	})
}

// ---------------------------------------------------------------------------
// mandatory test 12 — migration replay & history immutability
// ---------------------------------------------------------------------------

// TestPGScheduleMigrationReplayLegacyDisabled proves the P2.4 migration
// story: legacy saved schedule_cron rows migrate to disabled + NULL cursor
// with NO backfill/implicit activation; historical runs, a live lease and
// the admission stamp survive replay byte for byte; the new file stays in
// the serialized batch form.
func TestPGScheduleMigrationReplayLegacyDisabled(t *testing.T) {
	r, _ := bootRepoWithDSN(t)
	ctx := context.Background()
	skipped := p24ApplyMigrationsExcept(t, r.Pool, "sync_schedule_ticks")
	require.Len(t, skipped, 1, "exactly the P2.4 scheduling migration is skipped")
	assert.Contains(t, skipped[0], "sync_schedule", "unexpected skip set: %v", skipped)

	// A genuine pre-P2.4 population: legacy defs (valid + invalid saved
	// crons), a terminal run with artifacts, and a legacy running row.
	owner := uuid.New()
	personal := Tenant{OwnerID: owner}
	legacySource := uuid.New()
	_, err := r.Pool.Exec(ctx,
		`INSERT INTO connections (id, name, connector_type, config, owner_id) VALUES ($1,'p24-legacy','rest_api','{}',$2)`,
		legacySource, owner)
	require.NoError(t, err)
	defValid, defInvalid := uuid.New(), uuid.New()
	for def, cron := range map[uuid.UUID]string{defValid: "*/10 * * * *", defInvalid: "legacy junk"} {
		_, err = r.Pool.Exec(ctx,
			`INSERT INTO batch_sync_defs (id, source_id, output_dataset_id, schedule_cron) VALUES ($1,$2,$3,$4)`,
			def, legacySource, uuid.New(), cron)
		require.NoError(t, err)
	}
	terminalRun := uuid.New()
	version := uuid.New()
	hash := "p24-legacy-hash-keep"
	_, err = r.Pool.Exec(ctx,
		`INSERT INTO sync_runs (id, sync_def_id, status, dataset_version_id, content_hash, bytes_written, rows_written, finished_at)
		 VALUES ($1,$2,'succeeded',$3,$4,22766,151,now())`, terminalRun, defValid, version, hash)
	require.NoError(t, err)
	legacyRunning := uuid.New()
	_, err = r.Pool.Exec(ctx,
		`INSERT INTO sync_runs (id, sync_def_id, status, started_at) VALUES ($1,$2,'running',now())`,
		legacyRunning, defValid)
	require.NoError(t, err)

	legacyValidJSON := p24LegacyDefJSON(t, r.Pool, defValid)
	legacyInvalidJSON := p24LegacyDefJSON(t, r.Pool, defInvalid)
	terminalBefore := snapshotSyncRun(t, r.Pool, terminalRun)
	legacyRunningBefore := snapshotSyncRun(t, r.Pool, legacyRunning)

	// First migrate: columns appear, legacy rows stay disabled + NULL cursor.
	require.NoError(t, Migrate(ctx, r.Pool))
	for _, def := range []uuid.UUID{defValid, defInvalid} {
		var enabled bool
		var rev int
		var next *time.Time
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT schedule_enabled, schedule_revision, schedule_next_fire_at FROM batch_sync_defs WHERE id = $1`, def).
			Scan(&enabled, &rev, &next))
		assert.False(t, enabled, "legacy schedules migrate disabled")
		assert.Equal(t, 1, rev, "no revision backfill")
		assert.Nil(t, next, "no cursor backfill or implicit activation")
	}
		assert.Equal(t, legacyValidJSON, p24LegacyDefJSON(t, r.Pool, defValid), "legacy columns byte-preserved")
		assert.Equal(t, legacyInvalidJSON, p24LegacyDefJSON(t, r.Pool, defInvalid))
		assertSyncRunPersisted(t, terminalBefore, snapshotSyncRun(t, r.Pool, terminalRun), "legacy terminal run after first upgrade")

		// Live state on the upgraded database: admission stamp + a held lease.
		_, err = r.Pool.Exec(ctx,
			`UPDATE connections SET sync_last_admitted_at = clock_timestamp() - interval '5 minutes' WHERE id = $1`, legacySource)
		require.NoError(t, err)
		liveRun, err := r.EnqueueSyncRun(ctx, defValid, legacySource, personal, owner)
		require.NoError(t, err)
		require.NotNil(t, liveRun)
		claim, err := r.ClaimSyncRun(ctx, liveRun.ID, personal, "p24mig", "p24migtok", time.Hour)
		require.NoError(t, err)
		require.NotNil(t, claim)
		stamp := p23MustStamp(t, r.Pool, legacySource)
		srcBefore := tableRowJSON(t, r.Pool, "connections", legacySource)
		defRowBefore := tableRowJSON(t, r.Pool, "batch_sync_defs", defValid)
		runBefore := snapshotSyncRun(t, r.Pool, liveRun.ID)

		// Replay twice over the live state: everything byte-identical —
		// including BOTH seeded legacy runs (terminal AND still-running),
		// compared against their pre-upgrade snapshots after the FINAL replay.
		require.NoError(t, Migrate(ctx, r.Pool))
		require.NoError(t, Migrate(ctx, r.Pool))
		assert.Equal(t, srcBefore, tableRowJSON(t, r.Pool, "connections", legacySource), "stamp survives replay")
		assert.Equal(t, defRowBefore, tableRowJSON(t, r.Pool, "batch_sync_defs", defValid))
		assertSyncRunPersisted(t, runBefore, snapshotSyncRun(t, r.Pool, liveRun.ID), "leased run across replay")
		assertSyncRunPersisted(t, terminalBefore, snapshotSyncRun(t, r.Pool, terminalRun), "legacy terminal run after final replays")
		assertSyncRunPersisted(t, legacyRunningBefore, snapshotSyncRun(t, r.Pool, legacyRunning), "legacy running row after final replays")
		leaseOK, err := r.CheckSyncRunLease(ctx, liveRun.ID, personal, "p24mig", "p24migtok", claim.Run.Generation)
	require.NoError(t, err)
	assert.True(t, leaseOK, "the live lease stays valid across replays")
	p23AssertStampUnchanged(t, r.Pool, legacySource, stamp, "migration replay")
	var ticks, grants int
	require.NoError(t, r.Pool.QueryRow(ctx, `SELECT count(*) FROM sync_schedule_ticks`).Scan(&ticks))
	require.NoError(t, r.Pool.QueryRow(ctx, `SELECT count(*) FROM sync_schedule_grants`).Scan(&grants))
	assert.Zero(t, ticks)
	assert.Zero(t, grants)

	// The new file stays in the serialized batch form (no CONCURRENTLY on the
	// executable SQL).
	body, err := migrationsFS.ReadFile("migrations/" + skipped[0])
	require.NoError(t, err)
	assert.NotContains(t, strings.ToUpper(p23StripSQLComments(string(body))), "CONCURRENTLY")
}

// p24LegacyDefJSON snapshots exactly the pre-P2.4 def columns (the schedule
// columns legitimately appear with their defaults after the upgrade).
func p24LegacyDefJSON(t *testing.T, pool *pgxpool.Pool, defID uuid.UUID) string {
	t.Helper()
	var js string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT json_build_object('id', id, 'source_id', source_id, 'output_dataset_id', output_dataset_id,
		        'file_glob', file_glob, 'schedule_cron', schedule_cron, 'created_at', created_at)
		   FROM batch_sync_defs WHERE id = $1`, defID).Scan(&js))
	return js
}

// ---------------------------------------------------------------------------
// mandatory test 13 — per-source independence + shared admission
// ---------------------------------------------------------------------------

// TestPGScheduleIndependentSourcesProgress proves one hot Source never parks
// another Source's progress, and that the scheduled path shares the P2.3
// admission gate with the manual path (occupancy, typed refusals, interval).
func TestPGScheduleIndependentSourcesProgress(t *testing.T) {
	t.Run("hot_source_a_never_blocks_source_b", func(t *testing.T) {
		r, dsn, tenant, srcA, defA, outA := p24FixtureDomain(t, true, nil)
		ctx := context.Background()
		// Source B in the same org with its own due def + arm.
		connB, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
			Name: "p24-src-b", ConnectorType: "rest_api"}, tenant)
		require.NoError(t, err)
		jobB, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{
			SourceID: connB.ID, OutputDatasetID: uuid.New(),
			ScheduleCron: ptrTo(p24CronEveryMinute), ScheduleEnabled: ptrTo(true),
		}, tenant)
		require.NoError(t, err)
		p24MakeDue(t, r.Pool, defA)
		p24MakeDue(t, r.Pool, jobB.ID)
		armA, _ := p24Arm(t, r, tenant, defA, srcA, outA, dbClock(t, r.Pool).Add(time.Hour))
		armB, _ := p24Arm(t, r, tenant, jobB.ID, connB.ID, jobB.OutputDatasetID, dbClock(t, r.Pool).Add(time.Hour))
		candA := p24Cand(t, r.Pool, defA, srcA)
		candB := p24Cand(t, r.Pool, jobB.ID, connB.ID)

		// srcA is externally held. The scheduler's Source lock is NOWAIT, so
		// A's claim answers FAST structured contention (it never parks), and
		// B's claim on its own source commits fully in the meantime.
		bar := p24HoldRowBarrier(t, dsn, "connections", srcA)
		enqA := singleConnRepo(t, dsn)
		start := time.Now()
		resA, err := enqA.ClaimDueScheduleSlot(ctx, candA, armA)
		elapsed := time.Since(start)
		require.NoError(t, err)
		require.False(t, resA.Claimed)
		assert.Equal(t, ScheduleSkipContention, resA.Skip)
		assert.Less(t, elapsed, 3*time.Second, "NOWAIT contention must be fast, took %s", elapsed)
		assert.Zero(t, p24TickCount(t, r.Pool, defA))
		assert.Zero(t, p24RunCount(t, r.Pool, defA))

		resB, err := r.ClaimDueScheduleSlot(ctx, candB, armB)
		require.NoError(t, err)
		require.True(t, resB.Claimed)
		require.Equal(t, TickOutcomeEnqueued, resB.Outcome)
		require.NotNil(t, resB.Run)

		// Once the hot source frees, A's slot fires too — nothing queued or
		// starved behind B.
		bar.release(t)
		resA2, err := enqA.ClaimDueScheduleSlot(ctx, candA, armA)
		require.NoError(t, err)
		require.True(t, resA2.Claimed)
		require.Equal(t, TickOutcomeEnqueued, resA2.Outcome)
		assert.Equal(t, 1, p24RunCount(t, r.Pool, defA))
		assert.Equal(t, 1, p24RunCount(t, r.Pool, jobB.ID))
		p23AssertNoIdleInTx(t, r.Pool)
	})

	t.Run("scheduled_and_manual_share_the_gate", func(t *testing.T) {
		r, _, tenant, src, def, out := p24FixtureDomain(t, true, p23CapConfig(1))
		ctx := context.Background()
		p24MakeDue(t, r.Pool, def)
		arm, _ := p24Arm(t, r, tenant, def, src, out, dbClock(t, r.Pool).Add(time.Hour))

		// The scheduled claim fills the single slot.
		res, err := r.ClaimDueScheduleSlot(ctx, p24Cand(t, r.Pool, def, src), arm)
		require.NoError(t, err)
		require.True(t, res.Claimed)
		require.Equal(t, TickOutcomeEnqueued, res.Outcome)
		stamp := p23MustStamp(t, r.Pool, src)

		// The manual path at the same instant answers the typed P2.3 refusal
		// with zero side effects — the gate is shared, not duplicated.
		run, err := r.EnqueueSyncRun(ctx, def, src, tenant, uuid.New())
		require.Error(t, err)
		assert.Nil(t, run)
		rej, ok := p23Rejection(err)
		require.True(t, ok)
		assert.Equal(t, AdmissionReasonCapacity, rej.Reason)
		p23AssertStampUnchanged(t, r.Pool, src, stamp, "shared gate: capacity refusal")
		assert.Equal(t, 1, p24RunCount(t, r.Pool, def))

		// And the next scheduled slot is skipped_admission_capacity while the
		// manual run still occupies the source.
		p24MakeDue(t, r.Pool, def)
		res2, err := r.ClaimDueScheduleSlot(ctx, p24Cand(t, r.Pool, def, src), arm)
		require.NoError(t, err)
		require.True(t, res2.Claimed)
		assert.Equal(t, TickOutcomeSkippedAdmissionCapacity, res2.Outcome)
		assert.Nil(t, res2.Run)
		p23AssertStampUnchanged(t, r.Pool, src, stamp, "skipped slot never consumes admission")
	})
}
