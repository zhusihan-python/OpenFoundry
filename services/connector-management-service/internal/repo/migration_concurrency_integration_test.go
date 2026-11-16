//go:build integration

// Real PostgreSQL 16 concurrency regressions for the serialized Migrate
// (three-replica restart exposure): two INDEPENDENT sessions running Migrate
// on the same database serialize on the Connector-specific transaction-level
// advisory lock taken BEFORE any DDL — proven through pg_stat_activity /
// pg_locks with real backend PIDs (never random sleeps, never fabricated
// ordering SQL). The populated variant proves the advisory lock spans the
// WHOLE DDL batch: an external session holding only an ACCESS EXCLUSIVE
// table lock on sync_runs stalls migrate A mid-batch (advisory granted,
// relation wait), while migrate B cannot even acquire the advisory lock
// (ungranted advisory request, blocked-by A). The cancellation variant
// cancels a Migrate that is genuinely waiting on the advisory lock: a
// classifiable bounded error, zero DDL while waiting, no residual locks or
// leaked backend, and the same pool migrates successfully afterwards.
package repo

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- file-local helpers ----------

// externalBarrier is a dedicated session whose open transaction holds either
// the migrations advisory lock or an ACCESS EXCLUSIVE table lock, blocking
// real Migrates. The pre-registered cleanup CLOSES the connection, so even a
// t.Fatal can never leave the transaction-scoped lock behind (xact advisory
// locks die with the session); release() is the graceful path.
type externalBarrier struct {
	conn *pgx.Conn
	pid  int32
	tx   pgx.Tx
}

// holdMigrationsAdvisory takes THE advisory lock Repo.Migrate itself takes —
// same exported key constant, same one-arg xact form; the key is never
// copied, so it cannot drift — before any Migrate starts.
func holdMigrationsAdvisory(t *testing.T, dsn string, observer *pgxpool.Pool) *externalBarrier {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		rbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(rbCtx) // bounded; releases the xact advisory lock
	})
	var pid int32
	require.NoError(t, conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, MigrationsAdvisoryLockKey)
	require.NoError(t, err, "holder must take the exact business advisory lock")
	var granted int
	require.NoError(t, observer.QueryRow(ctx,
		`SELECT count(*) FROM pg_locks WHERE pid = $1 AND locktype = 'advisory' AND granted`, pid).Scan(&granted))
	require.Equal(t, 1, granted, "holder must hold the granted migrations advisory lock")
	return &externalBarrier{conn: conn, pid: pid, tx: tx}
}

// holdSyncRunsExclusive holds ONLY an ACCESS EXCLUSIVE lock on sync_runs — it
// never writes the table — so a Migrate that already owns the advisory lock
// stalls mid-batch on the first sync_runs DDL statement.
func holdSyncRunsExclusive(t *testing.T, dsn string, observer *pgxpool.Pool) *externalBarrier {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		rbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(rbCtx) // bounded; releases the table lock
	})
	var pid int32
	require.NoError(t, conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `LOCK TABLE sync_runs IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)
	var modes []string
	rows, err := observer.Query(ctx,
		`SELECT mode FROM pg_locks WHERE pid = $1 AND locktype = 'relation'
		   AND relation = 'sync_runs'::regclass AND granted`, pid)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var m string
		require.NoError(t, rows.Scan(&m))
		modes = append(modes, m)
	}
	require.NoError(t, rows.Err())
	require.Contains(t, modes, "AccessExclusiveLock",
		"barrier must hold ACCESS EXCLUSIVE on sync_runs (lock only, never a write)")
	return &externalBarrier{conn: conn, pid: pid, tx: tx}
}

func (b *externalBarrier) release(t *testing.T) {
	t.Helper()
	require.NoError(t, b.tx.Rollback(context.Background()))
}

// migrateHandle ties one background Migrate to a cancellable bounded context
// and its result channel: the hot path awaits done, the failure path cancels
// and joins deterministically — never by draining the whole 45s bound through
// a pool Close blocked behind a still-held barrier.
type migrateHandle struct {
	cancel context.CancelFunc
	done   <-chan error
}

// goMigrate runs one Migrate on a dedicated bounded context in the
// background and returns the handle for awaiting / cancelling it. The result
// channel is CLOSED after the single send: the hot path consumes the error,
// and the cleanup's second (idempotent) join then returns immediately on the
// closed channel instead of blocking forever.
func goMigrate(t *testing.T, pool *pgxpool.Pool) *migrateHandle {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	ch := make(chan error, 1)
	go func() {
		ch <- Migrate(ctx, pool)
		close(ch)
	}()
	return &migrateHandle{cancel: cancel, done: ch}
}

// joinBounded waits for a Migrate goroutine with a bound; used on the
// cleanup path (the hot path joins via mustRecv).
func joinBounded(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Errorf("migrate goroutine did not return within the bounded join")
	}
}

// registerMigrateCleanup pre-registers the failure-safe teardown for ONE
// in-flight Migrate, IMMEDIATELY at launch (t.Cleanup is LIFO, so it runs
// BEFORE the pools' own Close cleanups — which would otherwise block for the
// full 45s context on a Migrate stuck behind a still-held lock): best-effort
// release the barrier (the barrier's pre-registered conn.Close is the
// backstop), cancel the operation, then bounded-join the goroutine. Every
// Fatal/require path therefore reaches a prompt release; nothing waits out
// the context bound to clean up.
func registerMigrateCleanup(t *testing.T, barrier *externalBarrier, h *migrateHandle) {
	t.Helper()
	t.Cleanup(func() {
		if barrier != nil {
			rbCtx, rbCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = barrier.tx.Rollback(rbCtx) // best-effort and BOUNDED; the barrier's pre-registered conn.Close is the backstop
			rbCancel()
		}
		h.cancel()
		joinBounded(t, h.done)
	})
}

// waitMigrateQueuedOnAdvisory proves the Migrate backend is really waiting on
// the ADVISORY lock (wait_event='advisory') behind holder, with its advisory
// request queued and NOT granted — lock-queue truth from pg_locks. A Migrate
// that does not take the lock fails here deterministically (the designed
// repro — no reliance on random DDL interleaving).
func waitMigrateQueuedOnAdvisory(t *testing.T, observer *pgxpool.Pool, waiter, holder int32) {
	t.Helper()
	waitUntilBlockedBy(t, observer, waiter, holder)
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waitEvent string
		var granted, queued int
		err := observer.QueryRow(context.Background(),
			`SELECT wait_event,
			        (SELECT count(*) FROM pg_locks l WHERE l.pid = $1 AND l.locktype = 'advisory' AND l.granted),
			        (SELECT count(*) FROM pg_locks l WHERE l.pid = $1 AND l.locktype = 'advisory' AND NOT l.granted)
			   FROM pg_stat_activity WHERE pid = $1`, waiter).Scan(&waitEvent, &granted, &queued)
		if err == nil && waitEvent == "advisory" && granted == 0 && queued >= 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("migrate pid %d never queued on the migrations advisory lock (wait_event=%q granted=%d queued=%d)",
				waiter, waitEvent, granted, queued)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitMigrateStalledOnSyncRuns proves the Migrate backend owns the GRANTED
// advisory lock yet waits on a RELATION lock held by blocker: it is stalled
// mid-batch, so the advisory lock provably covers DDL execution, not just
// batch start.
func waitMigrateStalledOnSyncRuns(t *testing.T, observer *pgxpool.Pool, waiter, blocker int32) {
	t.Helper()
	waitUntilBlockedBy(t, observer, waiter, blocker)
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waitEvent string
		var advisoryGranted int
		err := observer.QueryRow(context.Background(),
			`SELECT wait_event,
			        (SELECT count(*) FROM pg_locks l WHERE l.pid = $1 AND l.locktype = 'advisory' AND l.granted)
			   FROM pg_stat_activity WHERE pid = $1`, waiter).Scan(&waitEvent, &advisoryGranted)
		if err == nil && waitEvent == "relation" && advisoryGranted == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("migrate pid %d never stalled on the sync_runs lock mid-batch (wait_event=%q advisory_granted=%d)",
				waiter, waitEvent, advisoryGranted)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitMigrateQueuedBehindMigrate proves the second Migrate cannot acquire the
// advisory lock the first one holds mid-batch: ungranted advisory request,
// blocked-by the FIRST migrate's pid.
func waitMigrateQueuedBehindMigrate(t *testing.T, observer *pgxpool.Pool, waiter, ahead int32) {
	t.Helper()
	waitUntilBlockedBy(t, observer, waiter, ahead)
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waitEvent string
		var granted, queued int
		err := observer.QueryRow(context.Background(),
			`SELECT wait_event,
			        (SELECT count(*) FROM pg_locks l WHERE l.pid = $1 AND l.locktype = 'advisory' AND l.granted),
			        (SELECT count(*) FROM pg_locks l WHERE l.pid = $1 AND l.locktype = 'advisory' AND NOT l.granted)
			   FROM pg_stat_activity WHERE pid = $1`, waiter).Scan(&waitEvent, &granted, &queued)
		if err == nil && waitEvent == "advisory" && granted == 0 && queued >= 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("migrate pid %d never queued behind migrate pid %d on the advisory lock (wait_event=%q granted=%d queued=%d)",
				waiter, ahead, waitEvent, granted, queued)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// assertNoBusinessTables proves zero business DDL became visible (fresh
// database): the advisory lock precedes every schema change.
func assertNoBusinessTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	for _, tbl := range []string{"connections", "batch_sync_defs", "sync_runs"} {
		var reg *string
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT to_regclass($1)::text`, "public."+tbl).Scan(&reg))
		require.Nil(t, reg, "no business table may exist before the migrations lock is granted (%s exists)", tbl)
	}
}

// assertSchemaComplete proves the full business schema (core tables plus the
// complete P2.1 lease and P2.2 control column set) exists.
func assertSchemaComplete(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	for _, tbl := range []string{"connections", "batch_sync_defs", "sync_runs"} {
		var reg *string
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT to_regclass($1)::text`, "public."+tbl).Scan(&reg))
		require.NotNil(t, reg, "table %s must exist", tbl)
	}
	var cols int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.columns WHERE table_name = 'sync_runs'
		   AND column_name IN ('lease_holder','lease_token','lease_expires_at','heartbeat_at','requested_by','owner_id',
		                       'attempt','generation','control_mode','cancel_requested_at','cancelled_by','publish_entered_at')`).Scan(&cols))
	require.Equal(t, 12, cols, "sync_runs must carry the full P2.1+P2.2 column set")
	// The original three-replica failure chain ended on the legacy sync_jobs
	// runtime table (its ALTERs interleaving with the DROP): the completed
	// batch must leave it finally DROPPED.
	var oldReg *string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT to_regclass($1)::text`, "public.sync_jobs").Scan(&oldReg))
	require.Nil(t, oldReg, "the legacy sync_jobs table must be dropped by the full batch")
}

func requireNoLocksOfType(t *testing.T, observer *pgxpool.Pool, pid int32, lockType string) {
	t.Helper()
	var n int
	require.NoError(t, observer.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_locks WHERE pid = $1 AND locktype = $2`, pid, lockType).Scan(&n))
	require.Zero(t, n, "backend %d must hold no %s locks", pid, lockType)
}

// waitBackendLocksCleared polls until the backend holds no locks of the given
// type within a bound: after a cancelled lock wait, pgx closes the connection
// and the server-side teardown (statement cancel → transaction abort →
// backend exit, releasing the queued advisory request) is asynchronous, so
// "no residual lock" is an eventual property. A lock that never clears
// within the bound is a REAL leak and fails here — the poll is a wait, not a
// relaxation.
func waitBackendLocksCleared(t *testing.T, observer *pgxpool.Pool, pid int32, lockType string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		require.NoError(t, observer.QueryRow(context.Background(),
			`SELECT count(*) FROM pg_locks WHERE pid = $1 AND locktype = $2`, pid, lockType).Scan(&n))
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("backend %d still holds %d %s lock(s) after the bounded wait: leak",
				pid, n, lockType)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// isBoundedMigrateCancel classifies the acceptable bounded outcomes of a
// cancelled in-wait Migrate: a context error surfaced by pgx, or PG's own
// query_canceled (SQLSTATE 57014).
func isBoundedMigrateCancel(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) && pgErr.SQLState() == "57014" {
		return true
	}
	return strings.Contains(err.Error(), "canceling statement due to")
}

// syncRunSnapshot captures every safely-assertable persisted field of one
// sync_runs row plus the full row JSON (compared raw; printed only in
// lease-token-redacted form via assertCancelRowEqual).
type syncRunSnapshot struct {
	rowJSON                 string
	status, control, holder string
	token                   *string
	expiresAt               *time.Time
	cancelAt                *time.Time
	cancelledBy             *uuid.UUID
	publishAt               *time.Time
	attempt, generation     int
	version                 *uuid.UUID
	hash                    *string
	bytes, files, rows      int64
}

func snapshotSyncRun(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) *syncRunSnapshot {
	t.Helper()
	s := &syncRunSnapshot{}
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status, control_mode, COALESCE(lease_holder,''), lease_token, lease_expires_at,
		        cancel_requested_at, cancelled_by, publish_entered_at, attempt, generation,
		        dataset_version_id, content_hash, bytes_written, files_written, rows_written
		   FROM sync_runs WHERE id = $1`, runID).
		Scan(&s.status, &s.control, &s.holder, &s.token, &s.expiresAt,
			&s.cancelAt, &s.cancelledBy, &s.publishAt, &s.attempt, &s.generation,
			&s.version, &s.hash, &s.bytes, &s.files, &s.rows))
	s.rowJSON = runRowJSON(t, pool, runID)
	return s
}

// assertSyncRunPersisted requires every persisted field to be identical
// across a concurrent migration replay. Lease tokens are compared internally
// and NEVER printed; nil-ness is asserted explicitly first so an all-NULL
// "nil==nil" pass-through can never fake preservation.
func assertSyncRunPersisted(t *testing.T, before, after *syncRunSnapshot, label string) {
	t.Helper()
	require.NotNil(t, after, label)
	assert.Equal(t, before.status, after.status, label+": status")
	assert.Equal(t, before.control, after.control, label+": control_mode")
	assert.Equal(t, before.holder, after.holder, label+": lease_holder")
	if before.token != nil {
		require.NotNil(t, after.token, label+": lease token must not be reset to NULL")
		require.True(t, *before.token == *after.token, label+": lease token unchanged (values withheld)")
	} else {
		assert.Nil(t, after.token, label+": lease-less row must stay lease-less")
	}
	if before.expiresAt != nil {
		require.NotNil(t, after.expiresAt, label+": lease expiry must not be reset to NULL")
		assert.True(t, before.expiresAt.Equal(*after.expiresAt), label+": lease_expires_at unchanged")
	}
	if before.cancelAt != nil {
		require.NotNil(t, after.cancelAt, label+": cancel request must not be cleared")
		assert.True(t, before.cancelAt.Equal(*after.cancelAt), label+": cancel_requested_at unchanged")
	} else {
		assert.Nil(t, after.cancelAt, label+": no cancel request before means none after")
	}
	if before.cancelledBy != nil {
		require.NotNil(t, after.cancelledBy, label+": cancelled_by must not be cleared")
		assert.Equal(t, *before.cancelledBy, *after.cancelledBy, label+": cancelled_by unchanged")
	}
	if before.publishAt != nil {
		require.NotNil(t, after.publishAt, label+": gate marker must not be cleared")
		assert.True(t, before.publishAt.Equal(*after.publishAt), label+": publish_entered_at unchanged")
	} else {
		assert.Nil(t, after.publishAt, label+": no gate before means none after")
	}
	assert.Equal(t, before.attempt, after.attempt, label+": attempt")
	assert.Equal(t, before.generation, after.generation, label+": generation")
	if before.version != nil {
		require.NotNil(t, after.version, label+": dataset version must not be cleared")
		assert.Equal(t, *before.version, *after.version, label+": dataset_version_id unchanged")
	}
	if before.hash != nil {
		require.NotNil(t, after.hash, label+": content hash must not be cleared")
		assert.Equal(t, *before.hash, *after.hash, label+": content_hash unchanged")
	}
	assert.Equal(t, before.bytes, after.bytes, label+": bytes_written")
	assert.Equal(t, before.files, after.files, label+": files_written")
	assert.Equal(t, before.rows, after.rows, label+": rows_written")
	assertCancelRowEqual(t, before.rowJSON, after.rowJSON, label+": full row")
}

// ---------- mandatory test 1: concurrent Migrates serialize ----------

func TestPGConcurrentMigrateSerialized(t *testing.T) {
	t.Run("fresh_database_two_migrates_queue_then_both_succeed", func(t *testing.T) {
		r, dsn := bootRepoWithDSN(t)
		ctx := context.Background()

		// External session holds THE business advisory lock before any
		// Migrate starts; both Migrates must queue on it.
		holder := holdMigrationsAdvisory(t, dsn, r.Pool)

		m1 := singleConnRepo(t, dsn)
		pid1 := backendPID(t, m1.Pool)
		m2 := singleConnRepo(t, dsn)
		pid2 := backendPID(t, m2.Pool)
		h1 := goMigrate(t, m1.Pool)
		registerMigrateCleanup(t, holder, h1)
		h2 := goMigrate(t, m2.Pool)
		registerMigrateCleanup(t, holder, h2)

		waitMigrateQueuedOnAdvisory(t, r.Pool, pid1, holder.pid)
		waitMigrateQueuedOnAdvisory(t, r.Pool, pid2, holder.pid)

		// While both wait, ZERO business DDL is visible — the advisory lock
		// precedes every schema statement.
		assertNoBusinessTables(t, r.Pool)

		holder.release(t)
		require.NoError(t, mustRecv(t, h1.done, "migrate 1 behind advisory holder"))
		require.NoError(t, mustRecv(t, h2.done, "migrate 2 behind advisory holder"))

		assertSchemaComplete(t, r.Pool)
		require.NoError(t, Migrate(ctx, r.Pool), "repeated migration after the concurrent pair")
		require.NoError(t, Migrate(ctx, r.Pool), "second repeat stays idempotent")
	})

	t.Run("populated_rerun_lock_covers_entire_ddl_batch", func(t *testing.T) {
		r, dsn := bootRepoWithDSN(t)
		ctx := context.Background()
		require.NoError(t, Migrate(ctx, r.Pool)) // build the schema once

		// Fixture: legacy rows + the full controlled stage set, so replay
		// preservation is asserted on NON-trivial values (never nil==nil).
		legacyOwner := uuid.New()
		personal := Tenant{OwnerID: legacyOwner}
		legacySource := uuid.New()
		_, err := r.Pool.Exec(ctx,
			`INSERT INTO connections (id, name, connector_type, config, owner_id) VALUES ($1,'mc-legacy','postgresql','{}',$2)`,
			legacySource, legacyOwner)
		require.NoError(t, err)
		legacyDef := uuid.New()
		_, err = r.Pool.Exec(ctx,
			`INSERT INTO batch_sync_defs (id, source_id, output_dataset_id) VALUES ($1,$2,$3)`,
			legacyDef, legacySource, uuid.New())
		require.NoError(t, err)
		legacyTerminal := uuid.New()
		_, err = r.Pool.Exec(ctx,
			`INSERT INTO sync_runs (id, sync_def_id, status, dataset_version_id, content_hash, bytes_written, rows_written, finished_at)
			 VALUES ($1, $2, 'succeeded', $3, 'mc-legacy-hash', 22766, 151, now())`,
			legacyTerminal, legacyDef, uuid.New())
		require.NoError(t, err)
		legacyRunning := uuid.New()
		_, err = r.Pool.Exec(ctx,
			`INSERT INTO sync_runs (id, sync_def_id, status, started_at) VALUES ($1, $2, 'running', now())`,
			legacyRunning, legacyDef)
		require.NoError(t, err)

		runLease := leaseEnqueue(t, r, personal, legacySource, legacyDef, legacyOwner)
		leaseClaim, err := r.ClaimSyncRun(ctx, runLease, personal, "mc-lease-hold", "mc-lease-tok", time.Hour)
		require.NoError(t, err)
		require.NotNil(t, leaseClaim, "active controlled lease")
		runCancel := leaseEnqueue(t, r, personal, legacySource, legacyDef, legacyOwner)
		cancelClaim, err := r.ClaimSyncRun(ctx, runCancel, personal, "mc-cancel-hold", "mc-cancel-tok", time.Hour)
		require.NoError(t, err)
		require.NotNil(t, cancelClaim, "cancel-flagged controlled lease")
		cancelRes, err := r.RequestCancelSyncRun(ctx, runCancel, legacyDef, personal, nil, "mc cancel")
		require.NoError(t, err)
		require.NotNil(t, cancelRes)
		require.Equal(t, CancelOutcomeRequested, cancelRes.Outcome)
		runGate := leaseEnqueue(t, r, personal, legacySource, legacyDef, legacyOwner)
		gateClaim, err := r.ClaimSyncRun(ctx, runGate, personal, "mc-gate-hold", "mc-gate-tok", time.Hour)
		require.NoError(t, err)
		require.NotNil(t, gateClaim, "gate-entered controlled lease")
		gateRes, err := r.BeginPublishGate(ctx, runGate, personal, "mc-gate-hold", "mc-gate-tok", 1, time.Hour)
		require.NoError(t, err)
		require.NotNil(t, gateRes)
		require.Equal(t, PublishGateEntered, gateRes.Outcome)

		before := map[uuid.UUID]*syncRunSnapshot{}
		for _, id := range []uuid.UUID{legacyTerminal, legacyRunning, runLease, runCancel, runGate} {
			before[id] = snapshotSyncRun(t, r.Pool, id)
		}
		// Anti nil==nil: the rows whose preservation matters carry real
		// non-NULL lease/cancel/gate values at snapshot time.
		require.NotNil(t, before[runLease].token)
		require.NotNil(t, before[runLease].expiresAt)
		require.NotNil(t, before[runCancel].cancelAt)
		require.NotNil(t, before[runGate].publishAt)
		require.NotNil(t, before[legacyTerminal].version)
		require.NotNil(t, before[legacyTerminal].hash)

		// External session holds ONLY an ACCESS EXCLUSIVE table lock on
		// sync_runs (never writes). Migrate A acquires the advisory lock,
		// runs DDL, and stalls mid-batch on the first sync_runs statement —
		// proving the advisory lock covers the ENTIRE batch.
		barrier := holdSyncRunsExclusive(t, dsn, r.Pool)

		a := singleConnRepo(t, dsn)
		pidA := backendPID(t, a.Pool)
		hA := goMigrate(t, a.Pool)
		registerMigrateCleanup(t, barrier, hA)
		waitMigrateStalledOnSyncRuns(t, r.Pool, pidA, barrier.pid)

		// Migrate B starts while A is stalled mid-batch: B must NOT get the
		// advisory lock and must be queued BEHIND A (blocked-by A).
		b := singleConnRepo(t, dsn)
		pidB := backendPID(t, b.Pool)
		hB := goMigrate(t, b.Pool)
		registerMigrateCleanup(t, barrier, hB)
		waitMigrateQueuedBehindMigrate(t, r.Pool, pidB, pidA)

		barrier.release(t)
		require.NoError(t, mustRecv(t, hA.done, "migrate A stalled mid-batch"))
		require.NoError(t, mustRecv(t, hB.done, "migrate B queued behind A"))

		// Every persisted field of every row is identical (tokens compared
		// internally, never printed); the active lease is still live.
		for _, id := range []uuid.UUID{legacyTerminal, legacyRunning, runLease, runCancel, runGate} {
			assertSyncRunPersisted(t, before[id], snapshotSyncRun(t, r.Pool, id), "row "+id.String())
		}
		var live bool
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT clock_timestamp() < lease_expires_at FROM sync_runs WHERE id = $1`, runLease).Scan(&live))
		assert.True(t, live, "the active controlled lease must survive the concurrent replay")

		assertSchemaComplete(t, r.Pool)
		require.NoError(t, Migrate(ctx, r.Pool), "repeated migration after the serialized rerun")
	})
}

// ---------- mandatory test 2: bounded cancellation of the lock wait ----------

func TestPGMigrateLockWaitCancellation(t *testing.T) {
	r, dsn := bootRepoWithDSN(t)
	ctx := context.Background()

	// Pre-registered bounded cleanup comes FIRST: whatever fails below, the
	// holder connection is closed (releasing the advisory lock) and the
	// migrate context is cancelled — no lock can outlive this test.
	holder := holdMigrationsAdvisory(t, dsn, r.Pool)

	m := singleConnRepo(t, dsn)
	pid := backendPID(t, m.Pool)

	h := goMigrate(t, m.Pool)
	registerMigrateCleanup(t, holder, h)

	// The Migrate must genuinely wait on the advisory lock before anything
	// is cancelled — this is the deterministic repro point (a lock-less
	// Migrate FAILS here instead of racing on random DDL interleaving).
	waitMigrateQueuedOnAdvisory(t, r.Pool, pid, holder.pid)

	// Zero DDL while waiting: no business tables exist, and the waiting
	// backend holds NO relation locks at all.
	assertNoBusinessTables(t, r.Pool)
	requireNoLocksOfType(t, r.Pool, pid, "relation")

	h.cancel()
	err := mustRecv(t, h.done, "cancelled Migrate")
	require.Error(t, err, "a cancelled lock wait must surface an error, not success")
	require.True(t, isBoundedMigrateCancel(err),
		"cancel must surface a classifiable bounded error (context/pgx cancel or SQLSTATE 57014), got: %v", err)

	// No residual locks on that backend — bounded eventual check (server-side
	// teardown after the client vanished is asynchronous; a lock that never
	// clears is a real leak and fails at the bound).
	waitBackendLocksCleared(t, r.Pool, pid, "advisory")
	waitBackendLocksCleared(t, r.Pool, pid, "relation")
	// The backend is either gone (pgx destroyed the cancelled connection) or
	// idle — never an abandoned transaction.
	var state *string
	stateErr := r.Pool.QueryRow(ctx,
		`SELECT state FROM pg_stat_activity WHERE pid = $1`, pid).Scan(&state)
	if stateErr == nil {
		assert.Equal(t, "idle", *state, "a surviving backend must be idle, never an abandoned transaction")
	} else {
		require.ErrorIs(t, stateErr, pgx.ErrNoRows,
			"backend must be either gone (conn destroyed by the cancel) or readable")
	}

	// Release the holder; the SAME pool migrates successfully under a fresh
	// context.
	holder.release(t)
	require.NoError(t, Migrate(context.Background(), m.Pool))
	assertSchemaComplete(t, r.Pool)
	// No connection anywhere on this database lingers inside an abandoned
	// transaction.
	var abandoned int
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
		   AND state IN ('idle in transaction', 'idle in transaction (aborted)')`).Scan(&abandoned))
	assert.Zero(t, abandoned, "no connection may linger inside an abandoned transaction")
}
