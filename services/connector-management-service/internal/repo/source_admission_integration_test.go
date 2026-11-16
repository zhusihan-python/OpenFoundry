//go:build integration

// Real PostgreSQL 16 regressions for the P2.3 per-source persistent sync
// admission (admission.go + the shared gate inside EnqueueSyncRun/RunSyncJob).
// Everything is proven at the DATABASE level against multiple independent
// single-connection sessions, never through nil==nil pass-throughs:
//
//   - capacity bursts across the same source's different sync defs and actors:
//     a same-instant burst provably commits BETWEEN 1 and cap admissions (the
//     NOWAIT losers of the first in-flight admission transaction are typed
//     contentions — filling the cap is NOT a concurrency guarantee, so the
//     remaining slots are filled SEQUENTIALLY afterwards, and the next
//     request is then capacity-refused with zero side effects), with the
//     committed run population and run-ID set as the evidence,
//   - the NOWAIT fast-contention contract for admission-enabled sources (an
//     externally held source row lock is a quick typed busy, zero runs, zero
//     sync_last_admitted_at movement) and per-source independence (no global
//     serialization between sources or tenants),
//   - the post-lock policy + clock decision: the lockless pre-reads (parent
//     chain + mode selection) can never adjudicate — a policy updated inside
//     the blocking lock holder's transaction governs the waiter after it
//     commits, in BOTH boundary orders, with the RetryAfter ceiling derived
//     from the real post-lock DB clock,
//   - the release-state matrix: pending-aborted and legal terminal states
//     free capacity; running with a persisted cancel request, a gate-entered
//     running row, an expired lease and legacy running rows keep occupying;
//     fenced CAS attempts never release; a cancel never refunds the interval,
//   - tenant/parent binding ordered BEFORE quota and stamp: foreign callers
//     and wrong def/source pairs see the clean (nil, nil) contract even
//     against a full or externally locked source, and the legacy RunSyncJob
//     passes through the same gate,
//   - mutation/delete vs admission lock ordering in both deterministic
//     directions (queue proof via pg_blocking_pids), with the pre-existing
//     active-run guards intact and illegal admission configs refused without
//     touching the row,
//   - same-transaction failure atomicity (an INSERT failure rolls run AND
//     stamp back), honest error classification (cancelled contexts never
//     surface as the typed 429), and migration replay/upgrade/restart safety
//     with no backfill of sync_last_admitted_at and no CONCURRENTLY batch.
//
// Stamp-preservation assertions are only ever taken in serial windows (a
// sequential probe, or behind a barrier under which no successful admission
// can commit) — never across a phase where concurrent admissions might
// legitimately advance the timestamp.
//
// Cleanup discipline: every barrier/operation registers its failure-safe
// teardown AT LAUNCH, and t.Cleanup's LIFO order makes it run BEFORE the
// pools' own Close — bounded best-effort barrier release, then cancel, then
// the closed-channel idempotent bounded join. No cleanup path ever drains an
// operation's own 15s bound, and every result channel is closed after its
// single send.
//
// Lease-internal material (tokens) is compared internally only; any printed
// row snapshot goes through the lease-token-redacting helpers of this
// package.
package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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

// ---------- file-local helpers (p23-prefixed to avoid collisions) ----------

// p23AdmissionColumn is the private connections column the P2.3 migration
// adds; it is also the content marker that identifies the P2.3 migration
// file for the legacy-upgrade fixture.
const p23AdmissionColumn = "sync_last_admitted_at"

// p23FixtureDomain provisions a fresh migrated database with one org (or
// personal) tenant, one source and TWO sync defs on it, plus the DSN for
// independent sessions. The source starts with the given raw config.
func p23FixtureDomain(t *testing.T, org bool, config json.RawMessage) (*Repo, string, Tenant, uuid.UUID, uuid.UUID, uuid.UUID) {
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
		Name: "p23-src", ConnectorType: "rest_api", Config: config,
	}, tenant)
	require.NoError(t, err)
	require.NotNil(t, conn)
	job1, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{SourceID: conn.ID, OutputDatasetID: uuid.New()}, tenant)
	require.NoError(t, err)
	require.NotNil(t, job1)
	job2, err := r.CreateSyncJob(ctx, &models.CreateSyncJobRequest{SourceID: conn.ID, OutputDatasetID: uuid.New()}, tenant)
	require.NoError(t, err)
	require.NotNil(t, job2)
	return r, dsn, tenant, conn.ID, job1.ID, job2.ID
}

// p23Fixture is p23FixtureDomain in the org domain with an empty config.
func p23Fixture(t *testing.T) (*Repo, string, Tenant, uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()
	return p23FixtureDomain(t, true, nil)
}

// p23CapConfig / p23IntervalConfig / p23CapIntervalConfig build source
// configs whose only sync subtree is the typed admission block.
func p23CapConfig(cap int) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"sync":{"admission":{"max_inflight_runs":%d}}}`, cap))
}

func p23IntervalConfig(ms int) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"sync":{"admission":{"min_interval_ms":%d}}}`, ms))
}

func p23CapIntervalConfig(cap, ms int) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"sync":{"admission":{"max_inflight_runs":%d,"min_interval_ms":%d}}}`, cap, ms))
}

// p23RawAdmissionConfig wraps a raw admission value (null, {}, malformed...)
// into the full sync subtree.
func p23RawAdmissionConfig(block string) json.RawMessage {
	return json.RawMessage(`{"sync":{"admission":` + block + `}}`)
}

// p23Stamp reads the source's private admission stamp.
func p23Stamp(t *testing.T, pool *pgxpool.Pool, sourceID uuid.UUID) *time.Time {
	t.Helper()
	var stamp *time.Time
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT sync_last_admitted_at FROM connections WHERE id = $1`, sourceID).Scan(&stamp))
	return stamp
}

// p23MustStamp requires a NON-NULL stamp first (anti nil==nil: preservation
// assertions below it can then never pass on two NULLs).
func p23MustStamp(t *testing.T, pool *pgxpool.Pool, sourceID uuid.UUID) time.Time {
	t.Helper()
	stamp := p23Stamp(t, pool, sourceID)
	require.NotNil(t, stamp, "precondition: stamp must already be set (anti nil==nil)")
	return *stamp
}

// p23ActiveCount is the REAL pending+running run count of the source (the
// exact population the admission cap bounds), read through an independent
// observer session.
func p23ActiveCount(t *testing.T, pool *pgxpool.Pool, sourceID uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM sync_runs r
		   JOIN batch_sync_defs d ON d.id = r.sync_def_id
		  WHERE d.source_id = $1 AND r.status IN ('pending','running')`, sourceID).Scan(&n))
	return n
}

// p23RunIDSet returns every run id of the source (any status).
func p23RunIDSet(t *testing.T, pool *pgxpool.Pool, sourceID uuid.UUID) map[uuid.UUID]bool {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT r.id FROM sync_runs r
		   JOIN batch_sync_defs d ON d.id = r.sync_def_id
		  WHERE d.source_id = $1`, sourceID)
	require.NoError(t, err)
	defer rows.Close()
	out := map[uuid.UUID]bool{}
	for rows.Next() {
		var id uuid.UUID
		require.NoError(t, rows.Scan(&id))
		out[id] = true
	}
	require.NoError(t, rows.Err())
	return out
}

// p23SetConfigDirect rewrites the source config with plain SQL. Fixture-only
// escape hatch for policies that must change while runs are active (every
// business write surface legitimately refuses that).
func p23SetConfigDirect(t *testing.T, pool *pgxpool.Pool, sourceID uuid.UUID, config json.RawMessage) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE connections SET config = $2 WHERE id = $1`, sourceID, config)
	require.NoError(t, err)
}

// p23Rejection extracts the typed P2.3 refusal (a typed refusal is returned
// AS the error with a nil run — it is a value, not a transport failure).
func p23Rejection(err error) (*AdmissionRejection, bool) {
	var rej *AdmissionRejection
	if errors.As(err, &rej) {
		return rej, true
	}
	return nil, false
}

// p23ReasonIn reports whether err is a typed refusal with one of the reasons.
func p23ReasonIn(err error, reasons ...AdmissionRejectionReason) bool {
	rej, ok := p23Rejection(err)
	if !ok {
		return false
	}
	for _, want := range reasons {
		if rej.Reason == want {
			return true
		}
	}
	return false
}

// p23CeilSeconds mirrors the business RetryAfter rounding: whole seconds,
// clamped to at least one.
func p23CeilSeconds(d time.Duration) time.Duration {
	if d <= 0 {
		return time.Second
	}
	s := (d + time.Second - 1) / time.Second
	return s * time.Second
}

// p23SourceBarrier is a dedicated session whose open transaction holds the
// source row FOR UPDATE WITHOUT writing it (the strongest blocker: no new
// row version, so EvalPlanQual never runs for waiters). The pre-registered
// cleanup CLOSES the connection, so even a t.Fatal can never leave the lock
// behind; release() rolls back, commit() makes an in-tx config rewrite
// authoritative for the waiter that acquires the lock afterwards. While the
// barrier is held NO successful admission can commit on the source — the
// serial window for stamp-preservation assertions.
type p23SourceBarrier struct {
	pid int32
	tx  pgx.Tx
}

func p23HoldSourceBarrier(t *testing.T, dsn string, sourceID uuid.UUID) *p23SourceBarrier {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx) // bounded; releases the row lock
	})
	var pid int32
	require.NoError(t, conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	var locked uuid.UUID
	require.NoError(t, tx.QueryRow(ctx,
		`SELECT id FROM connections WHERE id = $1 FOR UPDATE`, sourceID).Scan(&locked))
	return &p23SourceBarrier{pid: pid, tx: tx}
}

func (b *p23SourceBarrier) release(t *testing.T) {
	t.Helper()
	require.NoError(t, b.tx.Rollback(context.Background()))
}

func (b *p23SourceBarrier) commit(t *testing.T) {
	t.Helper()
	require.NoError(t, b.tx.Commit(context.Background()))
}

// boundedRelease is the best-effort, BOUNDED rollback for cleanup paths (an
// already released/committed transaction answers ErrTxClosed, which is
// ignored); the barrier's pre-registered conn.Close is the backstop.
func (b *p23SourceBarrier) boundedRelease() {
	rbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = b.tx.Rollback(rbCtx)
}

// p23EnqueueCall/p23MutateCall/p23DeleteCall carry one
// background operation's result; every helper sends exactly once and then
// CLOSES the channel, so a later idempotent cleanup join can never block.
type p23EnqueueCall struct {
	run *models.SyncRun
	err error
}

type p23MutateCall struct {
	conn *models.Connection
	err  error
}

type p23DeleteCall struct {
	deleted bool
	err     error
}

// p23Op ties one background repo operation to its cancellable context and
// its result channel (single send, then closed): the hot path receives the
// value through done; the pre-registered cleanup's idempotent join returns
// immediately on the closed channel instead of draining the operation's own
// timeout bound.
type p23Op[T any] struct {
	cancel context.CancelFunc
	done   <-chan T
}

// shutdown cancels the operation and bounded-joins it — the cleanup-path
// twin of the hot path's mustRecv. Safe to run after the hot path already
// consumed the result (channel closed ⇒ immediate return).
func (op *p23Op[T]) shutdown(t *testing.T) {
	op.cancel()
	select {
	case <-op.done:
	case <-time.After(15 * time.Second):
		t.Errorf("background operation did not return within the bounded join")
	}
}

// p23RegisterCleanup pre-registers the failure-safe teardown for background
// operations AT LAUNCH: t.Cleanup is LIFO, so — because the pools were
// created (and registered their own Close) BEFORE the operations launched —
// this runs BEFORE any Pool.Close: bounded best-effort barrier release first
// (the barrier's pre-registered conn.Close is the backstop), then cancel,
// then the idempotent bounded join per operation. No cleanup path ever waits
// out an operation's own 15s context bound.
func p23RegisterCleanup(t *testing.T, release func(), shutdowns ...func()) {
	t.Cleanup(func() {
		if release != nil {
			release()
		}
		for _, s := range shutdowns {
			s()
		}
	})
}

// p23GoEnqueue runs one EnqueueSyncRun in the background on a bounded
// context. Used with single-connection repos so the backend PID (and its
// lock-wait state) is deterministic.
func p23GoEnqueue(r *Repo, syncID, sourceID uuid.UUID, tenant Tenant, requester uuid.UUID) *p23Op[p23EnqueueCall] {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	ch := make(chan p23EnqueueCall, 1)
	go func() {
		defer cancel()
		run, err := r.EnqueueSyncRun(ctx, syncID, sourceID, tenant, requester)
		ch <- p23EnqueueCall{run, err}
		close(ch)
	}()
	return &p23Op[p23EnqueueCall]{cancel: cancel, done: ch}
}

// p23GoEnqueueCtx is p23GoEnqueue on a CALLER-owned context (the caller
// keeps its own cancel for hot-path cancellation); the returned op's cancel
// composes with it.
func p23GoEnqueueCtx(parent context.Context, r *Repo, syncID, sourceID uuid.UUID, tenant Tenant, requester uuid.UUID) *p23Op[p23EnqueueCall] {
	ctx, cancel := context.WithCancel(parent)
	ch := make(chan p23EnqueueCall, 1)
	go func() {
		defer cancel()
		run, err := r.EnqueueSyncRun(ctx, syncID, sourceID, tenant, requester)
		ch <- p23EnqueueCall{run, err}
		close(ch)
	}()
	return &p23Op[p23EnqueueCall]{cancel: cancel, done: ch}
}

// p23GoUpdateConnection runs one config mutation in the background.
func p23GoUpdateConnection(r *Repo, sourceID uuid.UUID, tenant Tenant, config json.RawMessage) *p23Op[p23MutateCall] {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	ch := make(chan p23MutateCall, 1)
	go func() {
		defer cancel()
		conn, err := r.UpdateConnection(ctx, sourceID, tenant, &models.UpdateConnectionRequest{Config: config})
		ch <- p23MutateCall{conn, err}
		close(ch)
	}()
	return &p23Op[p23MutateCall]{cancel: cancel, done: ch}
}

func p23GoDeleteConnection(r *Repo, sourceID uuid.UUID, tenant Tenant) *p23Op[p23DeleteCall] {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	ch := make(chan p23DeleteCall, 1)
	go func() {
		defer cancel()
		deleted, err := r.DeleteConnection(ctx, sourceID, tenant)
		ch <- p23DeleteCall{deleted, err}
		close(ch)
	}()
	return &p23Op[p23DeleteCall]{cancel: cancel, done: ch}
}

// p23WaitClockPast bounded-polls the DATABASE clock (never a blind sleep)
// until it has moved past the deadline.
func p23WaitClockPast(t *testing.T, pool *pgxpool.Pool, deadline time.Time) {
	t.Helper()
	limit := time.Now().Add(15 * time.Second)
	for dbClock(t, pool).Before(deadline) {
		if time.Now().After(limit) {
			t.Fatalf("db clock never passed %v", deadline)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// p23NewPool opens a brand-new pool on the DSN (restart/rejoin proof).
func p23NewPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// p23AssertNoIdleInTx proves no session on this database lingers inside an
// abandoned transaction (bounded-rollback discipline of every failure path).
func p23AssertNoIdleInTx(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
		   AND state IN ('idle in transaction', 'idle in transaction (aborted)')`).Scan(&n))
	assert.Zero(t, n, "no connection may linger inside an abandoned transaction")
}

// p23ApplyMigrationsExceptAdmission applies every embedded migration whose
// body does NOT carry the P2.3 admission column — building a genuine
// pre-P2.3 database without hard-coding the file name — and returns the
// skipped file names (must be exactly the P2.3 set).
func p23ApplyMigrationsExceptAdmission(t *testing.T, pool *pgxpool.Pool) []string {
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
		if strings.Contains(string(body), p23AdmissionColumn) {
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
	require.NotEmpty(t, skipped, "the P2.3 admission migration must exist and be skippable")
	return skipped
}

// p23ColumnPresent reports whether connections carries the admission stamp.
func p23ColumnPresent(t *testing.T, pool *pgxpool.Pool) bool {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.columns
		  WHERE table_name = 'connections' AND column_name = $1`, p23AdmissionColumn).Scan(&n))
	return n == 1
}

// p23StripSQLComments removes `--` line comments and /* */ block comments so
// keyword assertions run on the executable SQL only (the migration files
// DOCUMENT rules like the missing CONCURRENTLY inside their own comment
// blocks — those words must not trip the check).
func p23StripSQLComments(body string) string {
	lines := strings.Split(body, "\n")
	for i, line := range lines {
		if idx := strings.Index(line, "--"); idx >= 0 {
			lines[i] = line[:idx]
		}
	}
	joined := strings.Join(lines, "\n")
	for { // block comments, non-nested (PG semantics)
		open := strings.Index(joined, "/*")
		if open < 0 {
			break
		}
		closed := strings.Index(joined[open:], "*/")
		if closed < 0 {
			joined = joined[:open]
			break
		}
		joined = joined[:open] + joined[open+closed+2:]
	}
	return joined
}

// p23InsertBarrierKey is an opaque session-advisory-lock key for the
// slow-INSERT fixture (distinct from every business advisory key, which the
// migration-concurrency regressions pin).
const p23InsertBarrierKey int64 = 0x70323331

// p23WaitAdvisoryQueued polls the lock-queue truth until the waiter's INSERT
// is genuinely parked on the fixture advisory barrier (ungranted advisory
// request, wait_event 'advisory') — never a sleep-based guess.
func p23WaitAdvisoryQueued(t *testing.T, observer *pgxpool.Pool, waiter int32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waitEvent string
		var queued int
		err := observer.QueryRow(context.Background(),
			`SELECT wait_event,
			        (SELECT count(*) FROM pg_locks l WHERE l.pid = $1 AND l.locktype = 'advisory' AND NOT l.granted)
			   FROM pg_stat_activity WHERE pid = $1`, waiter).Scan(&waitEvent, &queued)
		if err == nil && waitEvent == "advisory" && queued >= 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid %d never queued on the insert barrier advisory lock (wait_event=%q queued=%d)",
				waiter, waitEvent, queued)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// p23AssertStampUnchanged requires the stamp to stay exactly at its known
// non-NULL value across a refusal (never a nil==nil comparison). Only call
// inside a serial window: a sequential probe, or behind a barrier under
// which no successful admission can commit on this source.
func p23AssertStampUnchanged(t *testing.T, pool *pgxpool.Pool, sourceID uuid.UUID, want time.Time, msg string) {
	t.Helper()
	got := p23Stamp(t, pool, sourceID)
	require.NotNil(t, got, msg+": stamp must not be cleared")
	assert.True(t, got.Equal(want), msg+": stamp must be unchanged")
}

// p23Burst races `contenders` independent single-connection sessions against
// one freshly capped source (alternating its two defs, distinct actors of
// the same org) released from a common start gate, and proves the honest
// burst invariant: at least one and at most `cap` admissions commit (NOWAIT
// contention makes "the burst fills the cap" NOT a guarantee — losers of the
// first in-flight admission transaction are typed contentions and never
// queue or retry), every loser is exactly a typed capacity/contention
// refusal, and the committed database population IS the reported winner set.
// The remaining capacity is then filled SEQUENTIALLY (post-burst, when no
// admission transaction is in flight) up to exactly `cap`, proving the cap
// itself is reachable by ordinary requests.
type p23BurstResult struct {
	r        *Repo
	dsn      string
	tenant   Tenant
	sourceID uuid.UUID
	defs     []uuid.UUID
	winners  map[uuid.UUID]bool
}

func p23Burst(t *testing.T, cap, contenders int) *p23BurstResult {
	t.Helper()
	r, dsn, tenant, sourceID, def1, def2 := p23FixtureDomain(t, true, p23CapConfig(cap))
	require.Nil(t, p23Stamp(t, r.Pool, sourceID), "fresh source starts unstamped")

	defs := []uuid.UUID{def1, def2}
	repos := make([]*Repo, contenders)
	for i := range repos {
		repos[i] = singleConnRepo(t, dsn)
	}
	type slot struct {
		run *models.SyncRun
		err error
	}
	results := make([]slot, contenders)
	start := make(chan struct{})
	parentCtx, parentCancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(parentCtx, 15*time.Second)
			defer cancel()
			// Distinct verified subjects of the SAME org share the quota;
			// both defs of the source are exercised.
			run, err := repos[i].EnqueueSyncRun(ctx, defs[i%len(defs)], sourceID, tenant, uuid.New())
			results[i] = slot{run, err}
		}(i)
	}
	// Failure-safe teardown registered AT LAUNCH (LIFO: runs before the
	// contenders' pools Close): cancel the shared parent, then bounded-join
	// every contender — never drain a 15s bound to clean up.
	t.Cleanup(func() {
		parentCancel()
		joined := make(chan struct{})
		go func() { wg.Wait(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(15 * time.Second):
			t.Errorf("burst contenders did not return within the bounded join")
		}
	})
	close(start)
	wg.Wait()

	winners := map[uuid.UUID]bool{}
	for i := 0; i < contenders; i++ {
		if results[i].run != nil {
			require.NoError(t, results[i].err, "contender %d: an admission carries no error", i)
			assert.Equal(t, "pending", results[i].run.Status, "contender %d", i)
			winners[results[i].run.ID] = true
			continue
		}
		// run == nil: must be EXACTLY a typed refusal (never a bare nil —
		// every contender is a legitimate caller of a visible source).
		rej, ok := p23Rejection(results[i].err)
		require.True(t, ok, "contender %d: loser must be a typed admission refusal, got err=%v", i, results[i].err)
		assert.Contains(t, []AdmissionRejectionReason{AdmissionReasonCapacity, AdmissionReasonContention},
			rej.Reason, "contender %d", i)
	}
	require.GreaterOrEqual(t, len(winners), 1, "the row starts unlocked: at least one contender must admit")
	require.LessOrEqual(t, len(winners), cap, "the source cap bounds committed admissions")

	// Database truth, not the return values: the committed run population IS
	// the winner set and respects the cap.
	assert.Equal(t, len(winners), p23ActiveCount(t, r.Pool, sourceID),
		"real pending+running count must equal the burst winners")
	assert.Equal(t, winners, p23RunIDSet(t, r.Pool, sourceID),
		"committed run ids must be exactly the reported winners")
	require.NotNil(t, p23Stamp(t, r.Pool, sourceID), "a successful admission must stamp the source")

	// Post-burst sequential fill: with no admission transaction in flight,
	// ordinary requests reach exactly the cap (no queueing, no retries —
	// plain sequential calls).
	for guard := 0; p23ActiveCount(t, r.Pool, sourceID) < cap; guard++ {
		require.Less(t, guard, cap+2, "sequential fill must converge on the cap")
		run, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, run, "sequential request into a free slot must admit")
	}
	assert.Equal(t, cap, p23ActiveCount(t, r.Pool, sourceID))
	return &p23BurstResult{r: r, dsn: dsn, tenant: tenant, sourceID: sourceID, defs: defs, winners: winners}
}

// ---------- mandatory test 1: concurrent capacity ----------

// TestPGSourceAdmissionConcurrentCapacity bounds run creation per SOURCE
// (every sync def, every actor of the tenant): a same-instant burst commits
// between 1 and cap admissions with typed refusals for everyone else and the
// committed rows as evidence; once the source is at its cap, ANY further
// request — different actor, different def — is capacity-refused with zero
// side effects (serial before→refuse→after stamp proof); and an
// admission-enabled source whose row is held by an external session fails
// FAST with the typed contention refusal (the NOWAIT contract), after which
// the same session succeeds.
func TestPGSourceAdmissionConcurrentCapacity(t *testing.T) {
	t.Run("cap1_burst_exactly_one_then_capacity_zero_side_effects", func(t *testing.T) {
		res := p23Burst(t, 1, 6)
		require.Equal(t, 1, p23ActiveCount(t, res.r.Pool, res.sourceID))
		stamp := p23MustStamp(t, res.r.Pool, res.sourceID)

		// Serial cross-actor probe: a DIFFERENT verified subject of the same
		// org shares the quota (deterministic — the burst already filled it).
		other := singleConnRepo(t, res.dsn)
		run, err := other.EnqueueSyncRun(context.Background(), res.defs[1], res.sourceID, res.tenant, uuid.New())
		rej, ok := p23Rejection(err)
		require.True(t, ok, "expected the typed capacity refusal, got run=%v err=%v", run, err)
		assert.Nil(t, run)
		assert.Equal(t, AdmissionReasonCapacity, rej.Reason)
		assert.Equal(t, 1, p23ActiveCount(t, res.r.Pool, res.sourceID))
		assert.Equal(t, res.winners, p23RunIDSet(t, res.r.Pool, res.sourceID), "no new run may exist")
		p23AssertStampUnchanged(t, res.r.Pool, res.sourceID, stamp, "capacity refusal (cross-actor)")

		// Serial cross-def probe: the source's OTHER def shares the same cap.
		run2, err2 := other.EnqueueSyncRun(context.Background(), res.defs[0], res.sourceID, res.tenant, uuid.New())
		rej2, ok2 := p23Rejection(err2)
		require.True(t, ok2, "expected the typed capacity refusal, got run=%v err=%v", run2, err2)
		assert.Nil(t, run2)
		assert.Equal(t, AdmissionReasonCapacity, rej2.Reason)
		p23AssertStampUnchanged(t, res.r.Pool, res.sourceID, stamp, "capacity refusal (cross-def)")
	})

	t.Run("cap2_burst_between_one_and_two_then_sequential_fill_reaches_cap", func(t *testing.T) {
		res := p23Burst(t, 2, 5)
		stamp := p23MustStamp(t, res.r.Pool, res.sourceID)
		ids := p23RunIDSet(t, res.r.Pool, res.sourceID)

		// At exactly cap, the next sequential request is refused with zero
		// side effects (serial stamp window).
		run, err := res.r.EnqueueSyncRun(context.Background(), res.defs[1], res.sourceID, res.tenant, uuid.New())
		rej, ok := p23Rejection(err)
		require.True(t, ok, "expected the typed capacity refusal, got run=%v err=%v", run, err)
		assert.Nil(t, run)
		assert.Equal(t, AdmissionReasonCapacity, rej.Reason)
		assert.Equal(t, 2, p23ActiveCount(t, res.r.Pool, res.sourceID))
		assert.Equal(t, ids, p23RunIDSet(t, res.r.Pool, res.sourceID))
		p23AssertStampUnchanged(t, res.r.Pool, res.sourceID, stamp, "capacity refusal at cap")
	})

	t.Run("enabled_source_under_external_row_lock_fast_contention_then_same_pool_succeeds", func(t *testing.T) {
		r, dsn, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		// No admission yet: stamp NULL, zero runs (both proven).
		require.Nil(t, p23Stamp(t, r.Pool, sourceID))
		require.Zero(t, p23ActiveCount(t, r.Pool, sourceID))

		bar := p23HoldSourceBarrier(t, dsn, sourceID)
		enq := singleConnRepo(t, dsn)

		start := time.Now()
		run, err := enq.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		elapsed := time.Since(start)
		rej, ok := p23Rejection(err)
		require.True(t, ok, "held source row must answer the typed refusal, got run=%v err=%v", run, err)
		assert.Nil(t, run)
		assert.Equal(t, AdmissionReasonContention, rej.Reason)
		assert.Less(t, elapsed, 3*time.Second,
			"NOWAIT contention must be fast, not a lock wait (took %s)", elapsed)
		assert.Equal(t, time.Second, rej.RetryAfter, "contention carries the advisory retry hint")

		// Zero side effects while locked (serial barrier window).
		assert.Zero(t, p23ActiveCount(t, r.Pool, sourceID))
		assert.Nil(t, p23Stamp(t, r.Pool, sourceID), "refused admission must not stamp")

		bar.release(t)
		// The SAME pool/session succeeds immediately afterwards: the failed
		// transaction was rolled back in a bounded way and nothing retried
		// behind the caller's back.
		run2, err2 := enq.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err2)
		require.NotNil(t, run2)
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, sourceID))
		require.NotNil(t, p23Stamp(t, r.Pool, sourceID))
		p23AssertNoIdleInTx(t, r.Pool)
	})
}

// ---------- mandatory test 2: independent sources & contention ----------

// TestPGSourceAdmissionIndependentSourcesAndContention proves the admission
// state is per-SOURCE: while source A is full or externally row-locked
// (fast typed contention), source B — same org or a different tenant —
// genuinely inserts and stamps. No global gate, no cross-source queue.
func TestPGSourceAdmissionIndependentSourcesAndContention(t *testing.T) {
	t.Run("locked_or_full_source_A_never_blocks_source_B", func(t *testing.T) {
		// A carries an admission policy: the locked-row probe below answers
		// the FAST typed contention (a policy-off source would legitimately
		// block on FOR UPDATE instead).
		r, dsn, tenant, srcA, defA, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		// B: second source (own cap) in the same tenant.
		connB, err := r.CreateConnection(context.Background(), &models.CreateConnectionRequest{
			Name: "p23-src-b", ConnectorType: "rest_api", Config: p23CapConfig(1)}, tenant)
		require.NoError(t, err)
		require.NotNil(t, connB)
		jobB, err := r.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{
			SourceID: connB.ID, OutputDatasetID: uuid.New()}, tenant)
		require.NoError(t, err)
		require.NotNil(t, jobB)

		// Fill A to its cap and lock its row externally.
		fill, err := r.EnqueueSyncRun(context.Background(), defA, srcA, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, fill)
		stampA := p23MustStamp(t, r.Pool, srcA)
		bar := p23HoldSourceBarrier(t, dsn, srcA)

		// A: enqueue under the held row lock → fast typed contention.
		enqA := singleConnRepo(t, dsn)
		opA := p23GoEnqueue(enqA, defA, srcA, tenant, uuid.New())
		// B: a genuinely independent source inserts WHILE A is locked.
		enqB := singleConnRepo(t, dsn)
		opB := p23GoEnqueue(enqB, jobB.ID, connB.ID, tenant, uuid.New())
		p23RegisterCleanup(t, bar.boundedRelease,
			func() { opA.shutdown(t) }, func() { opB.shutdown(t) })

		gotA := mustRecv(t, opA.done, "enqueue on locked source A")
		rejA, okA := p23Rejection(gotA.err)
		require.True(t, okA, "locked A must answer the typed refusal, got run=%v err=%v", gotA.run, gotA.err)
		assert.Nil(t, gotA.run)
		assert.Equal(t, AdmissionReasonContention, rejA.Reason)

		gotB := mustRecv(t, opB.done, "enqueue on independent source B")
		require.NoError(t, gotB.err)
		require.NotNil(t, gotB.run, "source B must really insert while A is locked")

		bar.release(t)
		// A untouched (its fill run and stamp only — serial barrier window
		// for A); B committed and stamped.
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, srcA))
		p23AssertStampUnchanged(t, r.Pool, srcA, stampA, "locked source A")
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, connB.ID))
		require.NotNil(t, p23Stamp(t, r.Pool, connB.ID), "source B must be stamped by its own admission")
		assert.True(t, p23RunIDSet(t, r.Pool, connB.ID)[gotB.run.ID], "B's run row is real")
	})

	t.Run("four_concurrent_contenders_all_fast_contention_zero_side_effects", func(t *testing.T) {
		r, dsn, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(2))
		bar := p23HoldSourceBarrier(t, dsn, sourceID)

		const n = 4
		ops := make([]*p23Op[p23EnqueueCall], n)
		shutdowns := make([]func(), 0, n)
		for i := 0; i < n; i++ {
			ops[i] = p23GoEnqueue(singleConnRepo(t, dsn), def1, sourceID, tenant, uuid.New())
			shutdowns = append(shutdowns, func() { ops[i].shutdown(t) })
		}
		p23RegisterCleanup(t, bar.boundedRelease, shutdowns...)
		start := time.Now()
		for i := 0; i < n; i++ {
			got := mustRecv(t, ops[i].done, "contender under external lock")
			require.True(t, p23ReasonIn(got.err, AdmissionReasonContention),
				"contender %d must be contention, got run=%v err=%v", i, got.run, got.err)
			assert.Nil(t, got.run)
		}
		assert.Less(t, time.Since(start), 5*time.Second, "all four must fail fast, not queue")

		bar.release(t)
		// Serial barrier window held for the whole burst: zero movement.
		assert.Zero(t, p23ActiveCount(t, r.Pool, sourceID))
		assert.Nil(t, p23Stamp(t, r.Pool, sourceID), "no contender may have stamped")
	})

	t.Run("cross_tenant_sources_independent", func(t *testing.T) {
		r, dsn, tenantA, srcA, defA, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		tenantB := Tenant{OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())}
		connB, err := r.CreateConnection(context.Background(), &models.CreateConnectionRequest{
			Name: "p23-src-tb", ConnectorType: "rest_api", Config: p23CapConfig(1)}, tenantB)
		require.NoError(t, err)
		require.NotNil(t, connB)
		jobB, err := r.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{
			SourceID: connB.ID, OutputDatasetID: uuid.New()}, tenantB)
		require.NoError(t, err)
		require.NotNil(t, jobB)

		bar := p23HoldSourceBarrier(t, dsn, srcA)
		enqA := singleConnRepo(t, dsn)
		opA := p23GoEnqueue(enqA, defA, srcA, tenantA, uuid.New())
		enqB := singleConnRepo(t, dsn)
		opB := p23GoEnqueue(enqB, jobB.ID, connB.ID, tenantB, uuid.New())
		p23RegisterCleanup(t, bar.boundedRelease,
			func() { opA.shutdown(t) }, func() { opB.shutdown(t) })

		gotA := mustRecv(t, opA.done, "tenant A enqueue on its locked source")
		require.True(t, p23ReasonIn(gotA.err, AdmissionReasonContention),
			"locked source must answer contention, got run=%v err=%v", gotA.run, gotA.err)
		assert.Nil(t, gotA.run)
		gotB := mustRecv(t, opB.done, "tenant B enqueue while tenant A's source is locked")
		require.NoError(t, gotB.err)
		require.NotNil(t, gotB.run, "another tenant's source must not be serialized behind A's lock")

		bar.release(t)
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, connB.ID))
		require.NotNil(t, p23Stamp(t, r.Pool, connB.ID))
	})
}

// ---------- mandatory test 3: interval decided by the post-lock clock ----------

// TestPGSourceAdmissionIntervalPostLockClock pins the two-phase decision
// itself. The lockless pre-read only picks the lock MODE (blocking FOR UPDATE
// while the policy is off), so the waiter is provably INSIDE the row-lock
// wait (pg_blocking_pids) while the holder — inside its locked transaction —
// rewrites the policy and commits. The waiter must then adjudicate with the
// POST-LOCK policy and the POST-LOCK DB clock, in both boundary orders:
// admitting once the boundary was crossed while waiting, refusing (with the
// real ceil'd RetryAfter) when it was not. The pre-read can never adjudicate.
func TestPGSourceAdmissionIntervalPostLockClock(t *testing.T) {
	const interval = 3000 * time.Millisecond

	t.Run("boundary_crossed_while_waiting_admits_on_postlock_clock", func(t *testing.T) {
		r, dsn, tenant, sourceID, def1, _ := p23Fixture(t) // policy off
		// First admission stamps T0 (policy-off admissions still stamp).
		first, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, first)
		t0 := p23MustStamp(t, r.Pool, sourceID)
		eligibleAt := t0.Add(interval)

		bar := p23HoldSourceBarrier(t, dsn, sourceID)
		enq := singleConnRepo(t, dsn)
		enqPID := backendPID(t, enq.Pool)
		op := p23GoEnqueue(enq, def1, sourceID, tenant, uuid.New())
		p23RegisterCleanup(t, bar.boundedRelease, func() { op.shutdown(t) })

		// The waiter is genuinely parked on the row lock while the boundary
		// has NOT elapsed yet (test validity: a pre-wait clock decision would
		// refuse; only the post-lock clock can admit).
		waitUntilBlockedBy(t, r.Pool, enqPID, bar.pid)
		clockAtBlock := dbClock(t, r.Pool)
		require.Greater(t, eligibleAt.Sub(clockAtBlock), 500*time.Millisecond,
			"test invalid: boundary already (nearly) crossed when the waiter parked")

		// Inside the held lock: enable the interval policy, hold past the
		// boundary on the DB clock, then COMMIT (authoritative for the
		// waiter's post-lock re-read).
		_, err = bar.tx.Exec(context.Background(),
			`UPDATE connections SET config = $2 WHERE id = $1`, sourceID,
			p23IntervalConfig(int(interval.Milliseconds())))
		require.NoError(t, err)
		p23WaitClockPast(t, r.Pool, eligibleAt)
		commitClock := dbClock(t, r.Pool)
		bar.commit(t)

		got := mustRecv(t, op.done, "enqueue behind the policy-flipping holder")
		require.NoError(t, got.err)
		require.NotNil(t, got.run, "post-lock clock is past the boundary: must admit")
		assert.True(t, p23RunIDSet(t, r.Pool, sourceID)[got.run.ID], "the admitted run is real")

		// The stamp is written at commit time inside the waiter's
		// post-lock transaction: past the boundary AND past the holder's
		// commit — a wait-entry-anchored decision could show neither.
		newStamp := p23MustStamp(t, r.Pool, sourceID)
		assert.True(t, newStamp.After(eligibleAt), "the decision used a post-boundary clock")
		assert.False(t, newStamp.Before(commitClock),
			"the stamp must be anchored after the holder's commit, not at wait entry")
	})

	t.Run("new_policy_rejects_before_boundary_no_preread_bypass", func(t *testing.T) {
		r, dsn, tenant, sourceID, def1, _ := p23Fixture(t) // policy off
		first, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, first)
		t1 := p23MustStamp(t, r.Pool, sourceID)

		bar := p23HoldSourceBarrier(t, dsn, sourceID)
		enq := singleConnRepo(t, dsn)
		enqPID := backendPID(t, enq.Pool)
		op := p23GoEnqueue(enq, def1, sourceID, tenant, uuid.New())
		p23RegisterCleanup(t, bar.boundedRelease, func() { op.shutdown(t) })
		waitUntilBlockedBy(t, r.Pool, enqPID, bar.pid)

		// Enable the interval policy INSIDE the lock and commit immediately —
		// far before t1+interval. The waiter pre-read the OFF policy; only
		// the post-lock re-read can refuse.
		_, err = bar.tx.Exec(context.Background(),
			`UPDATE connections SET config = $2 WHERE id = $1`, sourceID,
			p23IntervalConfig(int(interval.Milliseconds())))
		require.NoError(t, err)
		commitClock := dbClock(t, r.Pool)
		require.Less(t, commitClock.Sub(t1), interval-500*time.Millisecond,
			"test invalid: boundary already crossed before the holder committed")
		bar.commit(t)

		got := mustRecv(t, op.done, "enqueue behind the policy-flipping holder")
		rej, ok := p23Rejection(got.err)
		require.True(t, ok, "refusal must be typed, got run=%v err=%v", got.run, got.err)
		assert.Nil(t, got.run, "the pre-read OFF policy must not bypass the new interval")
		assert.Equal(t, AdmissionReasonMinInterval, rej.Reason)

		// RetryAfter is the ceiling of the post-lock remaining time: the
		// decision instant is bracketed by the holder commit and the return.
		afterClock := dbClock(t, r.Pool)
		eligibleAt := t1.Add(interval)
		lo, hi := eligibleAt.Sub(afterClock), eligibleAt.Sub(commitClock)
		assert.GreaterOrEqual(t, rej.RetryAfter, p23CeilSeconds(lo),
			"RetryAfter under-estimates the real remaining window")
		assert.LessOrEqual(t, rej.RetryAfter, p23CeilSeconds(hi),
			"RetryAfter over-estimates the real remaining window")

		// Nothing moved (serial barrier window): no run, stamp still exactly t1.
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, sourceID))
		p23AssertStampUnchanged(t, r.Pool, sourceID, t1, "interval refusal")

		// After the real window elapses (DB clock), admission resumes and
		// stamps past the boundary.
		p23WaitClockPast(t, r.Pool, eligibleAt)
		run2, err2 := enq.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err2)
		require.NotNil(t, run2, "admission must resume after the retry window")
		stamp2 := p23MustStamp(t, r.Pool, sourceID)
		assert.True(t, stamp2.After(eligibleAt))
	})

	t.Run("slow_insert_reanchors_stamp_at_write_time", func(t *testing.T) {
		const interval = 1500 * time.Millisecond
		r, dsn, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23IntervalConfig(int(interval.Milliseconds())))
		ctx := context.Background()

		// A session-advisory barrier the BEFORE INSERT trigger waits on: the
		// run INSERT of ONE marked subject parks INSIDE the transaction AFTER
		// the gate has already admitted, while the DB clock keeps running.
		holder, err := pgx.Connect(ctx, dsn)
		require.NoError(t, err)
		t.Cleanup(func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = holder.Close(closeCtx) // session advisory lock dies with it
		})
		_, err = holder.Exec(ctx, `SELECT pg_advisory_lock($1)`, p23InsertBarrierKey)
		require.NoError(t, err)

		marker := uuid.New()
		_, err = r.Pool.Exec(ctx, fmt.Sprintf(`
			CREATE OR REPLACE FUNCTION p23_insert_barrier() RETURNS trigger AS $fn$
			BEGIN
			  PERFORM pg_advisory_lock(%d);
			  PERFORM pg_advisory_unlock(%d);
			  RETURN NEW;
			END $fn$ LANGUAGE plpgsql;
			CREATE TRIGGER p23_insert_barrier_trg BEFORE INSERT ON sync_runs
			  FOR EACH ROW WHEN (NEW.requested_by = '%s') EXECUTE FUNCTION p23_insert_barrier();`,
			p23InsertBarrierKey, p23InsertBarrierKey, marker))
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = r.Pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS p23_insert_barrier_trg ON sync_runs;
				DROP FUNCTION IF EXISTS p23_insert_barrier()`)
		})

		enq := singleConnRepo(t, dsn)
		enqPID := backendPID(t, enq.Pool)
		startClock := dbClock(t, r.Pool)
		op := p23GoEnqueue(enq, def1, sourceID, tenant, marker)
		// Bounded best-effort barrier release for the cleanup path (the
		// holder's pre-registered conn.Close — which kills the session
		// advisory lock — is the backstop).
		p23RegisterCleanup(t, func() {
			rbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = holder.Exec(rbCtx, `SELECT pg_advisory_unlock($1)`, p23InsertBarrierKey)
		}, func() { op.shutdown(t) })

		// Lock-queue truth (never a sleep): the INSERT is genuinely parked
		// on the advisory barrier behind the holder.
		p23WaitAdvisoryQueued(t, r.Pool, enqPID)

		// Hold the INSERT until the DB clock is PAST start+interval+margin:
		// the cooldown the eventual stamp encodes must start at the WRITE,
		// not at the long-gone gate decision that pre-dates the barrier.
		p23WaitClockPast(t, r.Pool, startClock.Add(interval+600*time.Millisecond))
		releaseClock := dbClock(t, r.Pool)
		_, err = holder.Exec(ctx, `SELECT pg_advisory_unlock($1)`, p23InsertBarrierKey)
		require.NoError(t, err)

		got := mustRecv(t, op.done, "enqueue behind the insert barrier")
		require.NoError(t, got.err)
		require.NotNil(t, got.run, "the gate admitted before the barrier: the insert must complete")
		stamp := p23MustStamp(t, r.Pool, sourceID)
		assert.False(t, stamp.Before(releaseClock),
			"the stamp must be anchored at WRITE time (after the barrier release), not re-planted from the pre-insert gate clock")

		// An immediate follow-up is STILL interval-refused: the whole barrier
		// wait cannot be donated into a free cooldown. A gate-clock-replant
		// implementation would stamp before the barrier and admit here.
		second, err2 := enq.EnqueueSyncRun(ctx, def1, sourceID, tenant, uuid.New())
		rej, ok := p23Rejection(err2)
		require.True(t, ok, "expected the typed min_interval refusal, got run=%v err=%v", second, err2)
		assert.Nil(t, second)
		assert.Equal(t, AdmissionReasonMinInterval, rej.Reason)
		assert.Greater(t, rej.RetryAfter, time.Duration(0))
		p23AssertStampUnchanged(t, r.Pool, sourceID, stamp, "post-write interval refusal")
	})

	t.Run("interval_zero_disables_interval_cap_still_enforces", func(t *testing.T) {
		r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapIntervalConfig(1, 0))
		first, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, first)
		stamp := p23MustStamp(t, r.Pool, sourceID)

		// Immediately again (serial): interval 0 must NOT hold the second
		// admission back on interval grounds; the cap does refuse it.
		second, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		rej, ok := p23Rejection(err)
		require.True(t, ok, "expected the typed refusal, got run=%v err=%v", second, err)
		assert.Nil(t, second)
		assert.Equal(t, AdmissionReasonCapacity, rej.Reason,
			"interval 0 is off; only the cap dimension refuses")
		p23AssertStampUnchanged(t, r.Pool, sourceID, stamp, "capacity refusal under interval-0 policy")
	})
}

// ---------- mandatory test 4: release-state matrix ----------

// TestPGSourceAdmissionReleaseStateMatrix walks every run state and proves
// which ones still occupy the source's capacity: pending-aborted and legal
// terminal commits release; running with a persisted cancel request, a
// gate-entered running row, an expired (untaken) lease and legacy running
// rows keep occupying; fenced CAS attempts never release; and a cancel
// never refunds the minimum interval. Every refusal probe is sequential
// (serial stamp window).
func TestPGSourceAdmissionReleaseStateMatrix(t *testing.T) {
	// mustAdmit/mustRefuse are compact sequential probes on the fixture
	// repo; every refusal also pins the stamp (before→refuse→after).
	mustAdmit := func(t *testing.T, pool *pgxpool.Pool, r *Repo, def, source uuid.UUID, tenant Tenant) *models.SyncRun {
		t.Helper()
		before := p23Stamp(t, pool, source)
		run, err := r.EnqueueSyncRun(context.Background(), def, source, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, run, "expected an admission, got err=%v", err)
		after := p23MustStamp(t, pool, source)
		if before != nil {
			assert.False(t, after.Before(*before), "stamp must never move backwards")
		}
		return run
	}
	mustRefuse := func(t *testing.T, pool *pgxpool.Pool, r *Repo, def, source uuid.UUID, tenant Tenant, reason AdmissionRejectionReason, msg string) {
		t.Helper()
		before := p23MustStamp(t, pool, source)
		run, err := r.EnqueueSyncRun(context.Background(), def, source, tenant, uuid.New())
		rej, ok := p23Rejection(err)
		require.True(t, ok, "expected typed refusal %s, got run=%v err=%v", reason, run, err)
		assert.Nil(t, run)
		assert.Equal(t, reason, rej.Reason)
		p23AssertStampUnchanged(t, pool, source, before, string(reason)+" refusal"+msg)
	}

	t.Run("pending_occupies_then_pending_aborted_releases", func(t *testing.T) {
		r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		run := mustAdmit(t, r.Pool, r, def1, sourceID, tenant)
		mustRefuse(t, r.Pool, r, def1, sourceID, tenant, AdmissionReasonCapacity, "")

		res, err := r.RequestCancelSyncRun(context.Background(), run.ID, def1, tenant, nil, "release matrix")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeAborted, res.Outcome)
		// Anti nil==nil: the aborted row carries a real cancel timestamp.
		require.NotNil(t, res.Run.CancelRequestedAt)

		mustAdmit(t, r.Pool, r, def1, sourceID, tenant)
	})

	t.Run("claimed_running_occupies", func(t *testing.T) {
		r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		run := mustAdmit(t, r.Pool, r, def1, sourceID, tenant)
		claim, err := r.ClaimSyncRun(context.Background(), run.ID, tenant, "rm-hold", "rm-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		mustRefuse(t, r.Pool, r, def1, sourceID, tenant, AdmissionReasonCapacity, "")
	})

	t.Run("running_cancel_request_still_occupies", func(t *testing.T) {
		r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		run := mustAdmit(t, r.Pool, r, def1, sourceID, tenant)
		claim, err := r.ClaimSyncRun(context.Background(), run.ID, tenant, "cr-hold", "cr-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		res, err := r.RequestCancelSyncRun(context.Background(), run.ID, def1, tenant, nil, "still running")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeRequested, res.Outcome)
		require.NotNil(t, res.Run.CancelRequestedAt, "precondition: request really persisted")

		mustRefuse(t, r.Pool, r, def1, sourceID, tenant, AdmissionReasonCapacity,
			": a persisted cancel request alone frees nothing")
	})

	t.Run("gate_entered_still_occupies", func(t *testing.T) {
		r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		run := mustAdmit(t, r.Pool, r, def1, sourceID, tenant)
		claim, err := r.ClaimSyncRun(context.Background(), run.ID, tenant, "ge-hold", "ge-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)
		gate, err := r.BeginPublishGate(context.Background(), run.ID, tenant, "ge-hold", "ge-tok", 1, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, gate)
		require.Equal(t, PublishGateEntered, gate.Outcome)
		require.NotNil(t, gate.Claim.Run.PublishEnteredAt, "precondition: gate really entered")

		mustRefuse(t, r.Pool, r, def1, sourceID, tenant, AdmissionReasonCapacity,
			": a gate-entered running run still occupies the source")
	})

	t.Run("expired_lease_still_occupies", func(t *testing.T) {
		r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		run := mustAdmit(t, r.Pool, r, def1, sourceID, tenant)
		claim, err := r.ClaimSyncRun(context.Background(), run.ID, tenant, "ex-hold", "ex-tok", 120*time.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, claim)
		require.NotNil(t, claim.ExpiresAt, "precondition: this run carried a lease")
		waitForDBExpiry(t, r.Pool, run.ID) // DB-clock proof the lease is expired

		mustRefuse(t, r.Pool, r, def1, sourceID, tenant, AdmissionReasonCapacity,
			": an expired-but-untaken running run still occupies the source")
	})

	t.Run("legacy_running_row_still_occupies", func(t *testing.T) {
		r, _, tenant, sourceID, def1, _ := p23Fixture(t) // policy off
		legacy, err := r.RunSyncJob(context.Background(), def1, tenant)
		require.NoError(t, err)
		require.NotNil(t, legacy)
		require.Equal(t, models.RunControlLegacy, legacy.ControlMode, "precondition: genuine legacy row")
		require.Equal(t, "running", legacy.Status)

		// Enable the cap while the legacy run is active (direct fixture SQL:
		// every business write surface refuses config changes mid-run).
		p23SetConfigDirect(t, r.Pool, sourceID, p23CapConfig(1))
		mustRefuse(t, r.Pool, r, def1, sourceID, tenant, AdmissionReasonCapacity,
			": a legacy running row of unknown progress still counts")
	})

	t.Run("fenced_cas_never_releases_legal_terminal_does", func(t *testing.T) {
		r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		run := mustAdmit(t, r.Pool, r, def1, sourceID, tenant)
		claim, err := r.ClaimSyncRun(context.Background(), run.ID, tenant, "fc-hold", "fc-tok", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claim)

		// Wrong-token / wrong-generation CAS attempts: row-inert refusals.
		done, err := r.CompleteLeasedSyncRun(context.Background(), run.ID, tenant, "fc-hold", "fc-wrong", 1,
			"aborted", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		assert.Nil(t, done, "wrong token must fence")
		mustRefuse(t, r.Pool, r, def1, sourceID, tenant, AdmissionReasonCapacity,
			": a fenced complete freed nothing")
		renewed, err := r.RenewSyncRunLease(context.Background(), run.ID, tenant, "fc-hold", "fc-tok", 7, time.Minute)
		require.NoError(t, err)
		assert.Nil(t, renewed, "wrong generation must fence")
		mustRefuse(t, r.Pool, r, def1, sourceID, tenant, AdmissionReasonCapacity,
			": a fenced renew freed nothing")

		// The legal terminal (pre-gate abort with the REAL material) frees.
		aborted, err := r.CompleteLeasedSyncRun(context.Background(), run.ID, tenant, "fc-hold", "fc-tok", 1,
			"aborted", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, aborted)
		mustAdmit(t, r.Pool, r, def1, sourceID, tenant)
	})

	t.Run("legal_terminal_each_status_releases", func(t *testing.T) {
		for _, status := range []string{"succeeded", "failed", "aborted"} {
			t.Run(status, func(t *testing.T) {
				r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
				run := mustAdmit(t, r.Pool, r, def1, sourceID, tenant)
				claim, err := r.ClaimSyncRun(context.Background(), run.ID, tenant, "lt-hold", "lt-tok", time.Minute)
				require.NoError(t, err)
				require.NotNil(t, claim)
				if status == "succeeded" {
					gate, err := r.BeginPublishGate(context.Background(), run.ID, tenant, "lt-hold", "lt-tok", 1, time.Minute)
					require.NoError(t, err)
					require.Equal(t, PublishGateEntered, gate.Outcome)
				}
				done, err := r.CompleteLeasedSyncRun(context.Background(), run.ID, tenant, "lt-hold", "lt-tok", 1,
					status, 1, 1, 1, nil, nil, nil, nil)
				require.NoError(t, err)
				require.NotNil(t, done, status+" must complete legally here")
				mustAdmit(t, r.Pool, r, def1, sourceID, tenant)
			})
		}
	})

	t.Run("cancel_never_refunds_the_interval", func(t *testing.T) {
		const interval = 3000 * time.Millisecond
		r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true,
			p23CapIntervalConfig(models.SyncAdmissionMaxInflightRuns, int(interval.Milliseconds())))
		first := mustAdmit(t, r.Pool, r, def1, sourceID, tenant)
		t1 := p23MustStamp(t, r.Pool, sourceID)

		// Abort the pending run immediately — releasing CAPACITY is legal,
		// but the interval is measured from the last successful admission.
		res, err := r.RequestCancelSyncRun(context.Background(), first.ID, def1, tenant, nil, "instant regret")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeAborted, res.Outcome)
		assert.Zero(t, p23ActiveCount(t, r.Pool, sourceID), "capacity really is free")

		second, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		rej, ok := p23Rejection(err)
		require.True(t, ok, "expected typed refusal, got run=%v err=%v", second, err)
		assert.Nil(t, second, "an abort must not refund the interval")
		assert.Equal(t, AdmissionReasonMinInterval, rej.Reason)
		assert.Greater(t, rej.RetryAfter, time.Duration(0))
		p23AssertStampUnchanged(t, r.Pool, sourceID, t1, "interval not refunded by cancel")

		p23WaitClockPast(t, r.Pool, t1.Add(interval))
		mustAdmit(t, r.Pool, r, def1, sourceID, tenant)
	})
}

// ---------- mandatory test 5: tenant binding & the legacy gate ----------

// TestPGSourceAdmissionTenantBindingAndLegacy proves the tenant/parent
// binding is evaluated BEFORE quota and stamp: foreign callers get the clean
// (nil, nil) contract against a full OR externally locked enabled source,
// with zero row/stamp movement; the legacy RunSyncJob passes through the
// same admission gate; absent/null/empty policies keep the exact old
// contract; and a corrupted policy subtree fails closed with an honest
// error.
func TestPGSourceAdmissionTenantBindingAndLegacy(t *testing.T) {
	t.Run("org_domain_source_foreign_callers_nil_before_quota", func(t *testing.T) {
		r, dsn, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		// Fill the source to its cap, then hold its row externally: a foreign
		// caller must still see nil — the lockless parent-chain pre-check
		// and the tenant-predicated lock never reach quota/lock answers.
		fill, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, fill)
		stamp := p23MustStamp(t, r.Pool, sourceID)
		bar := p23HoldSourceBarrier(t, dsn, sourceID)

		for name, attacker := range map[string]Tenant{
			"same subject personal domain": {OwnerID: tenant.OwnerID},
			"same subject other org":       {OwnerID: tenant.OwnerID, OrgID: orgPtr(uuid.New())},
			"other owner other org":        {OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())},
			"other personal owner":         {OwnerID: uuid.New()},
		} {
			attackerRepo := singleConnRepo(t, dsn)
			start := time.Now()
			run, err := attackerRepo.EnqueueSyncRun(context.Background(), def1, sourceID, attacker, uuid.New())
			require.NoError(t, err, name)
			assert.Nil(t, run, name+": foreign caller must get the clean nil, never quota/lock answers")
			_, isRej := p23Rejection(err)
			assert.False(t, isRej, name+": no typed refusal may surface for an invisible source")
			assert.Less(t, time.Since(start), 3*time.Second, name+": tenant filtering must not wait on the lock")
		}
		// The legacy trigger honours the same binding under the held lock.
		legacy, err := r.RunSyncJob(context.Background(), def1,
			Tenant{OwnerID: tenant.OwnerID, OrgID: orgPtr(uuid.New())})
		require.NoError(t, err)
		assert.Nil(t, legacy, "foreign caller through the legacy path is also a clean nil")

		bar.release(t)
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, sourceID))
		p23AssertStampUnchanged(t, r.Pool, sourceID, stamp, "foreign callers")
	})

	t.Run("personal_domain_source_foreign_callers_nil_before_quota", func(t *testing.T) {
		r, _, personal, sourceID, def1, _ := p23FixtureDomain(t, false, p23CapConfig(1))
		fill, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, personal, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, fill)
		stamp := p23MustStamp(t, r.Pool, sourceID)

		for name, attacker := range map[string]Tenant{
			"same subject in an org": {OwnerID: personal.OwnerID, OrgID: orgPtr(uuid.New())},
			"other owner in an org":  {OwnerID: uuid.New(), OrgID: orgPtr(uuid.New())},
			"other personal owner":   {OwnerID: uuid.New()},
		} {
			run, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, attacker, uuid.New())
			require.NoError(t, err, name)
			assert.Nil(t, run, name)
		}
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, sourceID))
		p23AssertStampUnchanged(t, r.Pool, sourceID, stamp, "personal-domain foreign callers")
	})

	t.Run("wrong_source_def_parent_pair_nil", func(t *testing.T) {
		r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		// A second source+def in the SAME tenant: def/source crossed pairs
		// are invisible even for a legitimate caller (pre-check, no lock) —
		// here proven against a FULL source, so the nil precedes quota.
		conn2, err := r.CreateConnection(context.Background(), &models.CreateConnectionRequest{
			Name: "p23-src2", ConnectorType: "rest_api", Config: p23CapConfig(1)}, tenant)
		require.NoError(t, err)
		require.NotNil(t, conn2)
		job2, err := r.CreateSyncJob(context.Background(), &models.CreateSyncJobRequest{
			SourceID: conn2.ID, OutputDatasetID: uuid.New()}, tenant)
		require.NoError(t, err)
		require.NotNil(t, job2)
		fill, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, fill)
		stamp := p23MustStamp(t, r.Pool, sourceID)

		run, err := r.EnqueueSyncRun(context.Background(), job2.ID, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		assert.Nil(t, run, "def of source2 presented under source1 must be nil")
		run, err = r.EnqueueSyncRun(context.Background(), def1, conn2.ID, tenant, uuid.New())
		require.NoError(t, err)
		assert.Nil(t, run, "def of source1 presented under source2 must be nil")
		assert.Zero(t, p23ActiveCount(t, r.Pool, conn2.ID))
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, sourceID))
		p23AssertStampUnchanged(t, r.Pool, sourceID, stamp, "wrong parent pairs")
		// A missing def is equally invisible.
		run, err = r.EnqueueSyncRun(context.Background(), uuid.New(), sourceID, tenant, uuid.New())
		require.NoError(t, err)
		assert.Nil(t, run)
	})

	t.Run("legacy_runsyncjob_shares_the_gate", func(t *testing.T) {
		r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		fill, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, fill)
		stamp := p23MustStamp(t, r.Pool, sourceID)

		legacy, err := r.RunSyncJob(context.Background(), def1, tenant)
		rej, ok := p23Rejection(err)
		require.True(t, ok, "legacy refusal must be the typed rejection, got run=%v err=%v", legacy, err)
		assert.Nil(t, legacy, "the legacy trigger is bounded by the same gate")
		assert.Equal(t, AdmissionReasonCapacity, rej.Reason)
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, sourceID))
		p23AssertStampUnchanged(t, r.Pool, sourceID, stamp, "legacy refusal")

		// Free the slot; the legacy path now admits, stamps, and still
		// produces its unchanged legacy row shape.
		res, err := r.RequestCancelSyncRun(context.Background(), fill.ID, def1, tenant, nil, "free the slot")
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, CancelOutcomeAborted, res.Outcome)
		legacy2, err := r.RunSyncJob(context.Background(), def1, tenant)
		require.NoError(t, err)
		require.NotNil(t, legacy2)
		assert.Equal(t, "running", legacy2.Status)
		assert.Equal(t, models.RunControlLegacy, legacy2.ControlMode)
		var leaseless bool
		require.NoError(t, r.Pool.QueryRow(context.Background(),
			`SELECT lease_token IS NULL FROM sync_runs WHERE id = $1`, legacy2.ID).Scan(&leaseless))
		assert.True(t, leaseless, "the legacy row stays lease-less")
		stamp2 := p23MustStamp(t, r.Pool, sourceID)
		assert.False(t, stamp2.Before(stamp), "legacy admission advances the stamp too")
	})

	t.Run("legacy_runsyncjob_on_single_connection_pool", func(t *testing.T) {
		// The legacy path resolves its source INSIDE the transaction and
		// pre-reads the lock mode on the SAME connection (never a second
		// pool connection): a MaxConns=1 session must admit positively,
		// answer typed refusals, and — after the refusal released the
		// source lock — succeed again on the very same session. Regression
		// guard against the single-connection self-deadlock family.
		r, dsn, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		enq := singleConnRepo(t, dsn)
		ctx := context.Background()

		first, err := enq.RunSyncJob(ctx, def1, tenant)
		require.NoError(t, err)
		require.NotNil(t, first, "the legacy path must admit on a single-connection pool")
		require.NotNil(t, p23Stamp(t, r.Pool, sourceID))

		second, err2 := enq.RunSyncJob(ctx, def1, tenant)
		rej, ok := p23Rejection(err2)
		require.True(t, ok, "expected the typed capacity refusal, got run=%v err=%v", second, err2)
		assert.Nil(t, second)
		assert.Equal(t, AdmissionReasonCapacity, rej.Reason)

		// The typed refusal released the lock and closed its transaction:
		// close the legacy row (its only legal terminal is the no-token
		// CompleteSyncRun — a cancel is Unsupported by P2.2 semantics) and
		// the SAME session succeeds again.
		closed1, err := enq.CompleteSyncRun(ctx, first.ID, tenant, "succeeded", 1, 1, 1, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, closed1)
		third, err3 := enq.RunSyncJob(ctx, def1, tenant)
		require.NoError(t, err3)
		require.NotNil(t, third)
		p23AssertNoIdleInTx(t, r.Pool)

		// Close the legacy row so the source is empty again, then the NOWAIT
		// contention leg on the same single connection.
		closed, err := enq.CompleteSyncRun(ctx, third.ID, tenant, "succeeded", 1, 1, 1, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, closed)
		bar := p23HoldSourceBarrier(t, dsn, sourceID)
		busy, errB := enq.RunSyncJob(ctx, def1, tenant)
		rejB, okB := p23Rejection(errB)
		require.True(t, okB, "expected the typed contention refusal, got run=%v err=%v", busy, errB)
		assert.Nil(t, busy)
		assert.Equal(t, AdmissionReasonContention, rejB.Reason)
		bar.release(t)
		fourth, err4 := enq.RunSyncJob(ctx, def1, tenant)
		require.NoError(t, err4)
		require.NotNil(t, fourth, "after the barrier released, the same session must succeed")
		p23AssertNoIdleInTx(t, r.Pool)
	})

	t.Run("absent_null_empty_policy_keeps_old_contract", func(t *testing.T) {
		for name, config := range map[string]json.RawMessage{
			"no sync block": {},
			"admission null":       p23RawAdmissionConfig(`null`),
			"admission empty obj":  p23RawAdmissionConfig(`{}`),
			"sync block empty obj": json.RawMessage(`{"sync":{}}`),
			// Generic-compat (pinned): a non-object `sync` value cannot carry
			// an admission subtree, so the dimension is OFF — the config is
			// ACCEPTED passthrough, never an illegal subtree.
			"sync not an object (passthrough off)": json.RawMessage(`{"sync":"x"}`),
		} {
			t.Run(name, func(t *testing.T) {
				r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, config)
				// Old contract: unlimited immediate admissions, no interval.
				first, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
				require.NoError(t, err)
				require.NotNil(t, first)
				stamp1 := p23MustStamp(t, r.Pool, sourceID)
				second, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
				require.NoError(t, err)
				require.NotNil(t, second, "no admission dimension is enabled")
				stamp2 := p23MustStamp(t, r.Pool, sourceID)
				assert.False(t, stamp2.Before(stamp1), "every successful insert advances the stamp")
				legacy, err := r.RunSyncJob(context.Background(), def1, tenant)
				require.NoError(t, err)
				require.NotNil(t, legacy, "legacy path keeps the old contract too")
				assert.Equal(t, 3, p23ActiveCount(t, r.Pool, sourceID))
			})
		}
	})

	t.Run("corrupted_policy_fails_closed", func(t *testing.T) {
		r, _, tenant, sourceID, def1, _ := p23Fixture(t)
		first, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, first)
		stamp := p23MustStamp(t, r.Pool, sourceID)

		// Out-of-band corruption (every write surface validates): the gate
		// must fail CLOSED — an honest error, never a typed 429, never a
		// silent admission.
		p23SetConfigDirect(t, r.Pool, sourceID, p23RawAdmissionConfig(`{"max_inflight_runs":0}`))
		run, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.Error(t, err, "an unreadable policy must surface an error")
		assert.Nil(t, run)
		_, isRej := p23Rejection(err)
		assert.False(t, isRej, "corruption must not be masked as a typed refusal")
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, sourceID))
		p23AssertStampUnchanged(t, r.Pool, sourceID, stamp, "unreadable policy")

		// Restoring a valid policy unblocks the same session immediately (cap2:
		// one policy-off run is already active, so one slot stays open).
		p23SetConfigDirect(t, r.Pool, sourceID, p23CapConfig(2))
		run2, err2 := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err2)
		require.NotNil(t, run2, "the freed slot must admit under the restored policy")
	})
}

// ---------- mandatory test 6: mutation/delete vs admission ordering ----------

// TestPGSourceAdmissionMutationAndDelete pins the deterministic lock orders
// between run creation and the guarded source mutations (queue proven via
// pg_blocking_pids — never random races): an admission that commits first
// makes the config mutation and the delete lose with ErrActiveRuns; a delete
// that wins first leaves the trigger a clean no-row nil; an enabled
// admission never waits behind a mutation's lock (fast contention); and
// illegal admission configs change nothing on the config write surface.
func TestPGSourceAdmissionMutationAndDelete(t *testing.T) {
	t.Run("mutation_waits_behind_admit_then_refused", func(t *testing.T) {
		r, dsn, tenant, sourceID, def1, _ := p23Fixture(t) // policy off → blocking lock path
		bar := p23HoldSourceBarrier(t, dsn, sourceID)

		enqRepo := singleConnRepo(t, dsn)
		enqPID := backendPID(t, enqRepo.Pool)
		opEnq := p23GoEnqueue(enqRepo, def1, sourceID, tenant, uuid.New())
		waitUntilBlockedBy(t, r.Pool, enqPID, bar.pid)

		mutRepo := singleConnRepo(t, dsn)
		mutPID := backendPID(t, mutRepo.Pool)
		newCfg := p23CapConfig(2)
		opMut := p23GoUpdateConnection(mutRepo, sourceID, tenant, newCfg)
		p23RegisterCleanup(t, bar.boundedRelease,
			func() { opEnq.shutdown(t) }, func() { opMut.shutdown(t) })
		// The mutation queues strictly BEHIND the admission on the source row.
		waitUntilBlockedBy(t, r.Pool, mutPID, enqPID)

		bar.release(t)
		gotEnq := mustRecv(t, opEnq.done, "enqueue behind barrier")
		require.NoError(t, gotEnq.err)
		require.NotNil(t, gotEnq.run, "admission won the source row first")

		gotMut := mustRecv(t, opMut.done, "mutation queued behind the admission")
		require.ErrorIs(t, gotMut.err, ErrActiveRuns, "the committed pending run must refuse the config change")
		assert.Nil(t, gotMut.conn)
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, sourceID))
		require.NotNil(t, p23Stamp(t, r.Pool, sourceID))
		// The ZERO-WRITE proof of the refusal is the dedicated SERIAL subtest
		// below: two post-hoc reads around an already-finished background
		// mutation would prove nothing.
	})

	t.Run("serial_config_mutation_refused_zero_write", func(t *testing.T) {
		// Serial before→refuse→after with NO background operation: a LEGAL
		// new config is refused with ErrActiveRuns because a pending run is
		// active, and the FULL row — config, updated_at and the private
		// stamp included — stays byte-identical.
		r, _, tenant, sourceID, def1, _ := p23Fixture(t) // policy off
		run, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, run)
		stamp := p23MustStamp(t, r.Pool, sourceID)
		rowBefore := tableRowJSON(t, r.Pool, "connections", sourceID)

		updated, err := r.UpdateConnection(context.Background(), sourceID, tenant,
			&models.UpdateConnectionRequest{Config: p23CapConfig(2)})
		require.ErrorIs(t, err, ErrActiveRuns, "an active pending run must refuse the config change")
		assert.Nil(t, updated)

		assert.Equal(t, rowBefore, tableRowJSON(t, r.Pool, "connections", sourceID),
			"the refused mutation must not touch ANY column (config, updated_at, stamp)")
		p23AssertStampUnchanged(t, r.Pool, sourceID, stamp, "refused config mutation")
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, sourceID))
	})

	t.Run("delete_waits_behind_admit_then_refused_run_survives", func(t *testing.T) {
		r, dsn, tenant, sourceID, def1, _ := p23Fixture(t)
		bar := p23HoldSourceBarrier(t, dsn, sourceID)

		enqRepo := singleConnRepo(t, dsn)
		enqPID := backendPID(t, enqRepo.Pool)
		opEnq := p23GoEnqueue(enqRepo, def1, sourceID, tenant, uuid.New())
		waitUntilBlockedBy(t, r.Pool, enqPID, bar.pid)

		delRepo := singleConnRepo(t, dsn)
		delPID := backendPID(t, delRepo.Pool)
		opDel := p23GoDeleteConnection(delRepo, sourceID, tenant)
		p23RegisterCleanup(t, bar.boundedRelease,
			func() { opEnq.shutdown(t) }, func() { opDel.shutdown(t) })
		waitUntilBlockedBy(t, r.Pool, delPID, enqPID)

		bar.release(t)
		gotEnq := mustRecv(t, opEnq.done, "enqueue behind barrier")
		require.NoError(t, gotEnq.err)
		require.NotNil(t, gotEnq.run)

		gotDel := mustRecv(t, opDel.done, "delete queued behind the admission")
		require.ErrorIs(t, gotDel.err, ErrActiveRuns)
		assert.False(t, gotDel.deleted)
		assert.True(t, p23RunIDSet(t, r.Pool, sourceID)[gotEnq.run.ID],
			"the durable task must survive the refused delete")
	})

	t.Run("admit_waits_behind_delete_clean_nil_no_fk", func(t *testing.T) {
		r, dsn, tenant, sourceID, def1, _ := p23Fixture(t)
		bar := p23HoldSourceBarrier(t, dsn, sourceID)

		// Delete queues FIRST; the trigger queues strictly behind it.
		delRepo := singleConnRepo(t, dsn)
		delPID := backendPID(t, delRepo.Pool)
		opDel := p23GoDeleteConnection(delRepo, sourceID, tenant)
		waitUntilBlockedBy(t, r.Pool, delPID, bar.pid)

		enqRepo := singleConnRepo(t, dsn)
		enqPID := backendPID(t, enqRepo.Pool)
		opEnq := p23GoEnqueue(enqRepo, def1, sourceID, tenant, uuid.New())
		p23RegisterCleanup(t, bar.boundedRelease,
			func() { opDel.shutdown(t) }, func() { opEnq.shutdown(t) })
		waitUntilBlockedBy(t, r.Pool, enqPID, delPID)

		bar.release(t)
		gotDel := mustRecv(t, opDel.done, "delete behind barrier")
		require.NoError(t, gotDel.err)
		assert.True(t, gotDel.deleted, "no active run existed when the delete's check ran")

		gotEnq := mustRecv(t, opEnq.done, "enqueue queued behind the delete")
		require.NoError(t, gotEnq.err, "a delete that won must leave the trigger a clean nil, never an FK error")
		assert.Nil(t, gotEnq.run)
		var runs int
		require.NoError(t, r.Pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM sync_runs`).Scan(&runs))
		assert.Zero(t, runs)
	})

	t.Run("enabled_admit_vs_held_mutation_lock_fast_contention", func(t *testing.T) {
		r, dsn, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		// Simulate a mutation parked between its source lock and its check.
		bar := p23HoldSourceBarrier(t, dsn, sourceID)
		enq := singleConnRepo(t, dsn)
		start := time.Now()
		run, err := enq.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		rej, ok := p23Rejection(err)
		require.True(t, ok, "expected typed contention, got run=%v err=%v", run, err)
		assert.Nil(t, run)
		assert.Equal(t, AdmissionReasonContention, rej.Reason,
			"enabled admission must not queue behind a mutation's lock")
		assert.Less(t, time.Since(start), 3*time.Second)
		bar.release(t)

		// The mutation surface itself still works afterwards on the same
		// source (name-only change is legal with no active runs).
		renamed := "p23-after-contention"
		updated, err := r.UpdateConnection(context.Background(), sourceID, tenant,
			&models.UpdateConnectionRequest{Name: &renamed})
		require.NoError(t, err)
		require.NotNil(t, updated)
	})

	t.Run("invalid_admission_config_rejected_row_unchanged", func(t *testing.T) {
		r, _, _, sourceID, _, _ := p23Fixture(t)
		rowBefore := tableRowJSON(t, r.Pool, "connections", sourceID)
		for name, cfg := range map[string]json.RawMessage{
			"cap zero":                  p23RawAdmissionConfig(`{"max_inflight_runs":0}`),
			"cap negative":              p23RawAdmissionConfig(`{"max_inflight_runs":-2}`),
			"cap over max":              p23RawAdmissionConfig(`{"max_inflight_runs":4097}`),
			"interval negative":         p23RawAdmissionConfig(`{"min_interval_ms":-1}`),
			"interval over max":         p23RawAdmissionConfig(`{"min_interval_ms":86400001}`),
			"cap wrong json type":       p23RawAdmissionConfig(`{"max_inflight_runs":"x"}`),
			"admission wrong json type": p23RawAdmissionConfig(`"x"`),
		} {
			updated, err := r.UpdateConnectionConfig(context.Background(), sourceID, cfg)
			assert.Error(t, err, name+": illegal policy must be refused by the config surface")
			assert.Nil(t, updated, name)
			assert.Equal(t, rowBefore, tableRowJSON(t, r.Pool, "connections", sourceID),
				name+": refused config change leaves the row untouched")
		}
		// A legal policy writes fine (and later governs admissions).
		updated, err := r.UpdateConnectionConfig(context.Background(), sourceID, p23CapConfig(1))
		require.NoError(t, err)
		require.NotNil(t, updated)
	})

	t.Run("create_and_typed_update_validate_zero_write", func(t *testing.T) {
		// Direct REPO callers must not be able to persist an illegal policy
		// through ANY config surface — CreateConnection and the typed
		// UpdateConnection validate the NEW config themselves (name-only
		// updates never re-validate the old stored config).
		r, _, tenant, sourceID, _, _ := p23Fixture(t)
		ctx := context.Background()
		illegal := p23RawAdmissionConfig(`{"max_inflight_runs":0}`)
		rowBefore := tableRowJSON(t, r.Pool, "connections", sourceID)
		var sources int
		require.NoError(t, r.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM connections`).Scan(&sources))

		created, err := r.CreateConnection(ctx, &models.CreateConnectionRequest{
			Name: "p23-bad-create", ConnectorType: "rest_api", Config: illegal}, tenant)
		assert.Error(t, err, "CreateConnection must refuse an illegal policy at the repo layer")
		assert.Nil(t, created)
		var sourcesAfter int
		require.NoError(t, r.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM connections`).Scan(&sourcesAfter))
		assert.Equal(t, sources, sourcesAfter, "refused create writes zero rows")

		updated, err := r.UpdateConnection(ctx, sourceID, tenant,
			&models.UpdateConnectionRequest{Config: illegal})
		assert.Error(t, err, "typed UpdateConnection must refuse an illegal policy at the repo layer")
		assert.Nil(t, updated)
		assert.Equal(t, rowBefore, tableRowJSON(t, r.Pool, "connections", sourceID),
			"refused typed update leaves the row untouched")

		// Name-only updates carry no config and never re-validate the old
		// one: the legal path stays open exactly as before P2.3.
		renamed := "p23-name-only-fine"
		renamedConn, err := r.UpdateConnection(ctx, sourceID, tenant,
			&models.UpdateConnectionRequest{Name: &renamed})
		require.NoError(t, err)
		require.NotNil(t, renamedConn)
		assert.Equal(t, renamed, renamedConn.Name)
	})

	t.Run("delete_guard_unchanged_under_policy", func(t *testing.T) {
		r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		run, err := r.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, run)
		_, err = r.DeleteConnection(context.Background(), sourceID, tenant)
		assert.ErrorIs(t, err, ErrActiveRuns, "the pre-existing active guard holds under an admission policy")
		// Terminal state restores the old delete semantics.
		res, err := r.RequestCancelSyncRun(context.Background(), run.ID, def1, tenant, nil, "allow delete")
		require.NoError(t, err)
		require.Equal(t, CancelOutcomeAborted, res.Outcome)
		deleted, err := r.DeleteConnection(context.Background(), sourceID, tenant)
		require.NoError(t, err)
		assert.True(t, deleted)
	})
}

// ---------- mandatory test 7: rollback atomicity & migration replay ----------

// TestPGSourceAdmissionRollbackAndMigrationReplay proves the run INSERT and
// the stamp advance are ONE transaction (an INSERT failure rolls both back
// and the same session recovers), cancellation/errors are never masked as
// the typed 429 with no residual locks or abandoned transactions, and the
// migration story holds: idempotent replay without CONCURRENTLY, a genuine
// pre-P2.3 upgrade that backfills nothing and preserves every old field,
// replay over live admission state, and state survival across a brand-new
// pool.
func TestPGSourceAdmissionRollbackAndMigrationReplay(t *testing.T) {
	t.Run("insert_failure_rolls_back_run_and_stamp_same_pool_recovers", func(t *testing.T) {
		r, dsn, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(4))
		enq := singleConnRepo(t, dsn)
		first, err := enq.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, first)
		stamp := p23MustStamp(t, r.Pool, sourceID)

		// Deterministic in-transaction INSERT failure: a BEFORE INSERT
		// trigger raises for one marker subject (fixture-only DDL).
		marker := uuid.New()
		_, err = r.Pool.Exec(context.Background(), fmt.Sprintf(`
			CREATE OR REPLACE FUNCTION p23_fail_insert() RETURNS trigger AS $fn$
			BEGIN RAISE EXCEPTION 'p23 forced insert failure' USING ERRCODE = 'P0001'; END $fn$ LANGUAGE plpgsql;
			CREATE TRIGGER p23_fail_sync_runs_insert BEFORE INSERT ON sync_runs
			  FOR EACH ROW WHEN (NEW.requested_by = '%s') EXECUTE FUNCTION p23_fail_insert();`, marker))
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = r.Pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS p23_fail_sync_runs_insert ON sync_runs;
				DROP FUNCTION IF EXISTS p23_fail_insert()`)
		})

		run, err := enq.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, marker)
		require.Error(t, err, "the forced INSERT failure must surface")
		assert.Nil(t, run)
		_, isRej := p23Rejection(err)
		assert.False(t, isRej, "a genuine DB error must not be masked as the typed 429")
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, sourceID), "no orphan run may survive the rollback")
		p23AssertStampUnchanged(t, r.Pool, sourceID, stamp, "rolled-back admission")

		_, err = r.Pool.Exec(context.Background(), `DROP TRIGGER p23_fail_sync_runs_insert ON sync_runs`)
		require.NoError(t, err)
		run2, err2 := enq.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err2, "the SAME session must be usable after the failed transaction")
		require.NotNil(t, run2)
		assert.Equal(t, 2, p23ActiveCount(t, r.Pool, sourceID))
		stamp2 := p23MustStamp(t, r.Pool, sourceID)
		assert.False(t, stamp2.Before(stamp))
		p23AssertNoIdleInTx(t, r.Pool)
	})

	t.Run("cancelled_context_is_not_contention", func(t *testing.T) {
		r, dsn, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		enq := singleConnRepo(t, dsn)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		run, err := enq.EnqueueSyncRun(ctx, def1, sourceID, tenant, uuid.New())
		require.Error(t, err)
		assert.Nil(t, run)
		_, isRej := p23Rejection(err)
		assert.False(t, isRej, "a cancelled context must never be translated into the typed contention 429")
		assert.True(t, isBoundedMigrateCancel(err), "error stays the honest context class: %v", err)
		assert.Zero(t, p23ActiveCount(t, r.Pool, sourceID))
		// Same session, fresh context: exactly one admission — no retries.
		run2, err2 := enq.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err2)
		require.NotNil(t, run2)
		assert.Equal(t, 1, p23ActiveCount(t, r.Pool, sourceID))
	})

	t.Run("blocked_lock_wait_cancelled_no_fake_contention_no_residue", func(t *testing.T) {
		r, dsn, tenant, sourceID, def1, _ := p23Fixture(t) // blocking lock path
		bar := p23HoldSourceBarrier(t, dsn, sourceID)

		enq := singleConnRepo(t, dsn)
		enqPID := backendPID(t, enq.Pool)
		ctx, cancel := context.WithCancel(context.Background())
		op := p23GoEnqueueCtx(ctx, enq, def1, sourceID, tenant, uuid.New())
		p23RegisterCleanup(t, bar.boundedRelease, func() { op.shutdown(t) })
		waitUntilBlockedBy(t, r.Pool, enqPID, bar.pid)
		cancel()

		got := mustRecv(t, op.done, "cancelled lock wait")
		require.Error(t, got.err)
		assert.Nil(t, got.run)
		_, isRej := p23Rejection(got.err)
		assert.False(t, isRej, "57014/cancel must not be faked as contention")
		assert.True(t, isBoundedMigrateCancel(got.err), "classifiable bounded cancel, got %v", got.err)

		// Backend teardown is eventual: no residual locks, whether the
		// backend exited or went idle.
		waitBackendLocksCleared(t, r.Pool, enqPID, "transactionid")
		waitBackendLocksCleared(t, r.Pool, enqPID, "relation")
		bar.release(t)
		// Only AFTER the barrier released (it is legitimately idle-in-tx
		// while holding the lock) may no connection linger in a transaction.
		p23AssertNoIdleInTx(t, r.Pool)

		// The SAME pool admits under a fresh context once the row is free.
		run, err := enq.EnqueueSyncRun(context.Background(), def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, run)
		require.NotNil(t, p23Stamp(t, r.Pool, sourceID))
	})

	t.Run("fresh_migrate_idempotent_column_present_no_concurrently", func(t *testing.T) {
		r, _ := bootRepoWithDSN(t)
		ctx := context.Background()
		require.NoError(t, Migrate(ctx, r.Pool))
		require.NoError(t, Migrate(ctx, r.Pool), "replay stays idempotent")
		require.True(t, p23ColumnPresent(t, r.Pool), "the admission stamp column must exist")

		// The P2.3 batch file itself never uses CONCURRENTLY (it must replay
		// inside the P22-style single advisory-locked transaction). The word
		// check runs on the SQL only — comments are stripped first (the file
		// DOCUMENTS the no-CONCURRENTLY rule in its own comment block).
		entries, err := migrationsFS.ReadDir("migrations")
		require.NoError(t, err)
		found := 0
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
				continue
			}
			body, err := migrationsFS.ReadFile("migrations/" + e.Name())
			require.NoError(t, err)
			if !strings.Contains(string(body), p23AdmissionColumn) {
				continue
			}
			found++
			sqlOnly := p23StripSQLComments(string(body))
			assert.NotContains(t, strings.ToUpper(sqlOnly), "CONCURRENTLY",
				"%s must stay in the serialized batch form", e.Name())
		}
		assert.Equal(t, 1, found, "exactly the admission migration touches the stamp column")
	})

	t.Run("legacy_upgrade_no_backfill_old_rows_preserved", func(t *testing.T) {
		r, _ := bootRepoWithDSN(t)
		ctx := context.Background()
		p23ApplyMigrationsExceptAdmission(t, r.Pool)
		require.False(t, p23ColumnPresent(t, r.Pool), "fixture must be a genuine pre-P2.3 database")

		// Pre-P2.3 rows: a source with a finished run (real artifacts) and a
		// legacy running row.
		legacyOwner := uuid.New()
		personal := Tenant{OwnerID: legacyOwner}
		legacySource := uuid.New()
		_, err := r.Pool.Exec(ctx,
			`INSERT INTO connections (id, name, connector_type, config, owner_id) VALUES ($1,'p23-legacy','postgresql','{}',$2)`,
			legacySource, legacyOwner)
		require.NoError(t, err)
		legacyDef := uuid.New()
		_, err = r.Pool.Exec(ctx,
			`INSERT INTO batch_sync_defs (id, source_id, output_dataset_id) VALUES ($1,$2,$3)`,
			legacyDef, legacySource, uuid.New())
		require.NoError(t, err)
		legacyTerminal := uuid.New()
		legacyVersion := uuid.New()
		legacyHash := "p23-legacy-hash-keep"
		_, err = r.Pool.Exec(ctx,
			`INSERT INTO sync_runs (id, sync_def_id, status, dataset_version_id, content_hash, bytes_written, rows_written, finished_at)
			 VALUES ($1, $2, 'succeeded', $3, $4, 22766, 151, now())`, legacyTerminal, legacyDef, legacyVersion, legacyHash)
		require.NoError(t, err)
		legacyRunning := uuid.New()
		_, err = r.Pool.Exec(ctx,
			`INSERT INTO sync_runs (id, sync_def_id, status, started_at) VALUES ($1, $2, 'running', now())`,
			legacyRunning, legacyDef)
		require.NoError(t, err)

		terminalBefore := snapshotSyncRun(t, r.Pool, legacyTerminal)
		runningBefore := snapshotSyncRun(t, r.Pool, legacyRunning)
		require.NotNil(t, terminalBefore.version, "anti nil==nil: real artifacts on the legacy terminal row")

		require.NoError(t, Migrate(ctx, r.Pool))
		require.NoError(t, Migrate(ctx, r.Pool), "replay after upgrade stays idempotent")
		require.True(t, p23ColumnPresent(t, r.Pool))

		// Old rows are NOT backfilled: the stamp stays NULL for them.
		var nullStamps int
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM connections WHERE sync_last_admitted_at IS NULL`).Scan(&nullStamps))
		assert.Equal(t, 1, nullStamps, "no historical backfill of the private stamp")
		assertSyncRunPersisted(t, terminalBefore, snapshotSyncRun(t, r.Pool, legacyTerminal), "legacy terminal row")
		assertSyncRunPersisted(t, runningBefore, snapshotSyncRun(t, r.Pool, legacyRunning), "legacy running row")

		// The upgraded database admits and stamps — both creation paths (the
		// legacy running row predates the policy: old contract admits).
		run, err := r.EnqueueSyncRun(ctx, legacyDef, legacySource, personal, legacyOwner)
		require.NoError(t, err)
		require.NotNil(t, run)
		require.NotNil(t, p23Stamp(t, r.Pool, legacySource))
		legacy2, err := r.RunSyncJob(ctx, legacyDef, personal)
		require.NoError(t, err)
		require.NotNil(t, legacy2)
	})

	t.Run("replay_preserves_state_quota_and_release", func(t *testing.T) {
		r, _, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		ctx := context.Background()
		run := leaseEnqueue(t, r, tenant, sourceID, def1, uuid.New())
		claim, err := r.ClaimSyncRun(ctx, run, tenant, "rp-hold", "rp-tok", time.Hour)
		require.NoError(t, err)
		require.NotNil(t, claim)
		stamp := p23MustStamp(t, r.Pool, sourceID)
		runBefore := snapshotSyncRun(t, r.Pool, run)
		sourceBefore := tableRowJSON(t, r.Pool, "connections", sourceID)
		// Historical terminal runs exist and must never count as occupancy.
		for i := 0; i < 3; i++ {
			_, err := r.Pool.Exec(ctx,
				`INSERT INTO sync_runs (id, sync_def_id, status, finished_at)
				 VALUES ($1, $2, $3, now())`, uuid.New(), def1,
				[]string{"succeeded", "failed", "aborted"}[i])
			require.NoError(t, err)
		}

		require.NoError(t, Migrate(ctx, r.Pool))
		require.NoError(t, Migrate(ctx, r.Pool), "replay over live admission state")

		assertSyncRunPersisted(t, runBefore, snapshotSyncRun(t, r.Pool, run), "live run across replay")
		assert.Equal(t, sourceBefore, tableRowJSON(t, r.Pool, "connections", sourceID),
			"source row (stamp included) must survive the replay byte for byte")
		p23AssertStampUnchanged(t, r.Pool, sourceID, stamp, "migration replay")

		// The quota still bites after the replay, and old terminal runs do
		// not occupy it.
		rejRun, rejErr := r.EnqueueSyncRun(ctx, def1, sourceID, tenant, uuid.New())
		rej, ok := p23Rejection(rejErr)
		require.True(t, ok, "expected typed capacity refusal, got run=%v err=%v", rejRun, rejErr)
		assert.Nil(t, rejRun)
		assert.Equal(t, AdmissionReasonCapacity, rej.Reason)
		aborted, err := r.CompleteLeasedSyncRun(ctx, run, tenant, "rp-hold", "rp-tok", 1,
			"aborted", 0, 0, 0, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, aborted)
		run2, err := r.EnqueueSyncRun(ctx, def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, run2, "release still works after the replay; terminal history never counts")
	})

	t.Run("new_pool_restart_state_survives", func(t *testing.T) {
		r, dsn, tenant, sourceID, def1, _ := p23FixtureDomain(t, true, p23CapConfig(1))
		ctx := context.Background()
		run, err := r.EnqueueSyncRun(ctx, def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, run)
		stamp := p23MustStamp(t, r.Pool, sourceID)

		// A brand-new pool (fresh process shape) sees the same durable state.
		pool2 := p23NewPool(t, dsn)
		p23AssertStampUnchanged(t, pool2, sourceID, stamp, "new pool reads the durable stamp")
		assert.Equal(t, 1, p23ActiveCount(t, pool2, sourceID))
		repo2 := &Repo{Pool: pool2}
		rejRun, rejErr := repo2.EnqueueSyncRun(ctx, def1, sourceID, tenant, uuid.New())
		rej, ok := p23Rejection(rejErr)
		require.True(t, ok, "expected typed capacity refusal, got run=%v err=%v", rejRun, rejErr)
		assert.Nil(t, rejRun)
		assert.Equal(t, AdmissionReasonCapacity, rej.Reason,
			"quota is enforced by the database, not by pool memory")

		// And the release path works from the new pool.
		res, err := repo2.RequestCancelSyncRun(ctx, run.ID, def1, tenant, nil, "new pool")
		require.NoError(t, err)
		require.Equal(t, CancelOutcomeAborted, res.Outcome)
		run2, err := repo2.EnqueueSyncRun(ctx, def1, sourceID, tenant, uuid.New())
		require.NoError(t, err)
		require.NotNil(t, run2)
	})
}
