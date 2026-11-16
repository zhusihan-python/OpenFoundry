//go:build integration

// P2.6 W1b — Dataset COMMIT ACK-loss wire proof (real PostgreSQL).
//
// REQUIRED NAME (frozen for the W6 execution pass):
// TestPGP26DatasetWireCommitAckLossReplaysReceipt
//
// NOT executed in W1b: this phase delivers compile/vet plus the DB-free
// mechanics pure tests (output_commit_ackloss_wire_test.go); the real-PG
// run happens in the W6 unified pass after Root review.
//
// Proof shape — real socket ACK suppression, never a mock
// Commit-succeeds-then-errors at the application boundary:
//
//   - a fault pool (MaxConns=1) whose single connection is wrapped by the
//     W1b net.Conn DialFunc wrapper; it reassembles frontend frames across
//     fragmented Writes and, on the exact simple-query `commit\0` frame
//     pgx v5.7.4 emits for Tx.Commit (no-arg Exec → simple query), arms ACK
//     suppression BEFORE forwarding the frame — the server receives and
//     executes the COMMIT;
//   - an independent LISTEN connection (LISTEN completed before the fault
//     transaction starts) receives the test-owned trigger notification —
//     PG16 delivers NOTIFY only after the outer transaction commits — and
//     both the payload (payloadUUID:pg_backend_pid) and the
//     NotificationResponse sender PID must match the cached fault backend
//     PID: that is the real-COMMIT evidence, and it also proves the fault
//     pool never silently changed backend;
//   - only after that validated notification does the test cut the fault
//     socket; the in-flight Tx.Commit then surfaces EOF and
//     WithIdempotentOutputCommit must return ErrCommitOutcomeUnknown with
//     no fabricated receipt (commitFrameSeen + suppressActivated +
//     EOFDelivered all asserted true);
//   - the observer pool sees the durable projection: one receipt, one
//     COMMITTED transaction, one version, head/index/lineage published;
//   - the same key+fingerprint retry on an independent pool replays the
//     exact stored bytes, the build callback is never entered again, and
//     the projection and version/receipt counts stay at one.
//
// Every wait is bounded. Failure cleanup cuts the fault socket first,
// then releases the notifier gates (listener cancel + close), then joins
// the worker bounded — all before the earlier-registered cleanups (trigger
// drop, pools) run via t.Cleanup LIFO.
package repo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// w1bConnectListener opens the independent LISTEN connection — a plain
// single pgx.Conn with TLS explicitly disabled for this isolated
// test-owned connection only — and completes LISTEN before returning.
func w1bConnectListener(ctx context.Context, t *testing.T, connString, channel string) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(connString)
	if err != nil {
		t.Fatalf("parse listener connstring: %v", err)
	}
	cfg.TLSConfig = nil // explicit sslmode=disable, test-owned connection only
	cfg.Fallbacks = nil
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect listener: %v", err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(cctx)
	})
	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		t.Fatalf("LISTEN %s must complete before the fault transaction starts: %v", channel, err)
	}
	return conn
}

// w1bFaultPool builds the fault pool: MaxConns=1, no background
// lifetime/idle recycling (one stable backend PID), plaintext-only, and
// every dialed socket wrapped by the W1b fault conn so the exact COMMIT
// frame can be detected and its acknowledgement suppressed on the wire.
func w1bFaultPool(ctx context.Context, t *testing.T, connString string, state *w1bFaultState) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		t.Fatalf("parse fault connstring: %v", err)
	}
	cfg.MaxConns = 1
	cfg.MinConns = 0
	// Keep ParseConfig's POSITIVE defaults for MaxConnLifetime (1h) and
	// MaxConnIdleTime (30m): pgx v5.7.4 treats a zero lifetime as "expire
	// now", so Release after the pg_backend_pid() warm-up would Destroy
	// the connection and the publish would run on a NEW backend PID,
	// breaking the cached-PID proof (review round 1 blocker 1).
	cfg.ConnConfig.TLSConfig = nil // explicit sslmode=disable, test-owned connection only
	cfg.ConnConfig.Fallbacks = nil
	baseDial := cfg.ConnConfig.DialFunc
	cfg.ConnConfig.DialFunc = func(dctx context.Context, network, addr string) (net.Conn, error) {
		raw, derr := baseDial(dctx, network, addr)
		if derr != nil {
			return nil, derr
		}
		return state.wrap(raw), nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("fault pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// w1bSafeSQLLiteral reports whether s is confined to the generated
// identifier/payload charset (no quotes, backslashes, dollar signs, NULs),
// keeping the trigger DDL interpolation provably safe on the test DB.
func w1bSafeSQLLiteral(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-' || r == '.' || r == '_' || r == ':':
		default:
			return false
		}
	}
	return true
}

// w1bInstallAckTrigger installs the test-owned COMMIT-evidence trigger on
// dataset_output_commit_receipts, scoped to exactly this fixture row
// (dataset + idempotency key), and drops it in cleanup. The notification
// payload carries a fresh UUID plus the inserting backend PID, so the
// listener can prove the COMMIT executed on the cached fault connection.
func w1bInstallAckTrigger(ctx context.Context, t *testing.T, pool *pgxpool.Pool, datasetID uuid.UUID, key, channel, payloadID string) {
	t.Helper()
	for what, s := range map[string]string{"key": key, "channel": channel, "payload": payloadID} {
		if !w1bSafeSQLLiteral(s) {
			t.Fatalf("%s %q contains a SQL quoting metacharacter", what, s)
		}
	}
	suffix := uuid.NewString()[:8]
	fn := "w1b_ack_notify_" + suffix
	trg := "w1b_ack_trg_" + suffix
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $fn$
		BEGIN
			PERFORM pg_notify('%s', '%s:' || pg_backend_pid()::text);
			RETURN NEW;
		END $fn$`, fn, channel, payloadID)); err != nil {
		t.Fatalf("create ACK-loss notify function: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER %s AFTER INSERT ON dataset_output_commit_receipts
		FOR EACH ROW
		WHEN (NEW.dataset_id = '%s'::uuid AND NEW.idempotency_key = '%s')
		EXECUTE FUNCTION %s()`, trg, datasetID, key, fn)); err != nil {
		t.Fatalf("create ACK-loss notify trigger: %v", err)
	}
	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(dctx, `DROP TRIGGER IF EXISTS `+trg+` ON dataset_output_commit_receipts`)
		_, _ = pool.Exec(dctx, `DROP FUNCTION IF EXISTS `+fn+`()`)
	})
}

// w1bAwaitNotification waits (bounded) for the trigger notification and
// validates it: channel match, payload UUID + fault backend PID match, and
// NotificationResponse sender PID match. Any miss — including a timeout —
// fails the test: without the notification the real COMMIT was never
// proven, and this proof never falls back to an artificial error.
func w1bAwaitNotification(t *testing.T, notifCh <-chan *pgconn.Notification, errCh <-chan error, timeout time.Duration, channel, payloadID string, faultPID int32) *pgconn.Notification {
	t.Helper()
	select {
	case n := <-notifCh:
		if n.Channel != channel {
			t.Fatalf("notification channel %q does not match %q", n.Channel, channel)
		}
		want := payloadID + ":" + strconv.FormatInt(int64(faultPID), 10)
		if n.Payload != want {
			t.Fatalf("notification payload %q does not match %q (fault backend changed PID?)", n.Payload, want)
		}
		if n.PID != uint32(faultPID) {
			t.Fatalf("notification sender PID %d is not the cached fault backend PID %d", n.PID, faultPID)
		}
		return n
	case err := <-errCh:
		t.Fatalf("independent LISTEN connection failed before the COMMIT notification: %v", err)
	case <-time.After(timeout):
		t.Fatalf("no validated notification within %s — real COMMIT not proven (no notify/timeout/no trigger fails, never a fallback)", timeout)
	}
	return nil
}

// TestPGP26DatasetWireCommitAckLossReplaysReceipt: a normal keyed
// outputs:commit builds and publishes through a real PG connection whose
// socket then loses the acknowledgement of the outer COMMIT. The original
// call must surface ErrCommitOutcomeUnknown (no fabricated receipt); an
// independent observer proves the COMMIT actually landed (trigger
// notification on an independent LISTEN connection matching the fault
// backend PID, plus the durable projection); and retrying the same
// key+fingerprint replays the exact stored bytes without ever entering the
// build again, leaving one version, one receipt and the projection
// unchanged.
func TestPGP26DatasetWireCommitAckLossReplaysReceipt(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t) // observer/setup pool (existing W1 fixture)
	dataset, scope, owner := w1Setup(ctx, t, pool)
	key := "of-p26-pub-v1:" + uuid.NewString()
	fingerprint := keyedFingerprint(keyedFixtureBody(t))
	channel := "w1b_ch_" + uuid.NewString()[:8]
	payloadID := uuid.NewString()
	lineageTarget := "ri.foundry.main.dataset." + uuid.NewString()

	// Independent LISTEN connection; PG16 only delivers NOTIFY emitted by
	// transactions that commit AFTER the LISTEN completed.
	listener := w1bConnectListener(ctx, t, pool.Config().ConnString(), channel)
	notifCtx, cancelNotif := context.WithCancel(context.Background())
	t.Cleanup(cancelNotif)
	notifCh := make(chan *pgconn.Notification, 1)
	listenerErrCh := make(chan error, 1)
	go func() {
		n, err := listener.WaitForNotification(notifCtx)
		if err != nil {
			listenerErrCh <- err
			return
		}
		notifCh <- n
	}()

	// Fault pool with the wire wrapper; cache its single backend PID before
	// the fault transaction starts.
	state := newW1bFaultState()
	t.Cleanup(state.CloseAll)
	faultPool := w1bFaultPool(ctx, t, pool.Config().ConnString(), state)
	var faultPID int32
	if err := faultPool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&faultPID); err != nil {
		t.Fatalf("cache fault backend pid: %v", err)
	}
	if faultPID == 0 {
		t.Fatal("pg_backend_pid returned 0")
	}
	if n := state.connCount(); n != 1 {
		t.Fatalf("fault pool must hold exactly one connection (MaxConns=1), dialed %d", n)
	}

	// Test-owned trigger, scoped to exactly this fixture row.
	w1bInstallAckTrigger(ctx, t, pool, dataset.ID, key, channel, payloadID)

	var buildCalls atomic.Int32
	inner := w1Build(w1BuildOpts{
		rawRef:    dataset.ID.String(),
		scope:     scope,
		branch:    "main",
		file:      "part-00000.ndjson",
		lineage:   []string{lineageTarget},
		startedBy: owner,
	})
	build := func(bctx context.Context, tx OutputCommitTxStore) (models.CommitDatasetOutputResponse, error) {
		buildCalls.Add(1)
		return inner(bctx, tx)
	}

	done := make(chan w1Result, 1)
	workerExited := make(chan struct{})
	t.Cleanup(func() {
		// Failure-path ordering (review round 1): cut the fault socket
		// FIRST, then release the notifier gates (cancel the listener
		// context and close the listener), then join the worker bounded —
		// all before the earlier-registered cleanups drop the trigger and
		// close the pools. workerExited is CLOSED once, so this join can
		// observe it again after a successful run without consuming the
		// single result value in done.
		state.CloseAll()
		cancelNotif()
		lctx, lcancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = listener.Close(lctx)
		lcancel()
		select {
		case <-workerExited:
		case <-time.After(10 * time.Second):
			t.Errorf("fault commit worker did not return after the socket cut")
		}
	})
	go func() {
		r, replayed, err := (&Repo{Pool: faultPool}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, fingerprint, build)
		done <- w1Result{receipt: r, replayed: replayed, err: err}
		close(workerExited)
	}()

	// 1 — the exact `commit\0` simple-query frame must be detected on the wire.
	if !state.waitCommitFrameSeen(20 * time.Second) {
		select {
		case r := <-done:
			t.Fatalf("test commit frame never detected on the fault connection; attempt returned early: %v", r.err)
		default:
		}
		t.Fatal("test commit frame never detected on the fault connection within 20s")
	}

	// 2 — real-COMMIT proof on the independent LISTEN connection; channel,
	// payload UUID and PID must all match the cached fault backend PID.
	w1bAwaitNotification(t, notifCh, listenerErrCh, 20*time.Second, channel, payloadID, faultPID)

	// 3 — COMMIT proven durable server-side; only now cut the fault socket
	// so the in-flight Tx.Commit surfaces EOF to the original caller.
	state.CloseAll()
	res := w1RecvT(t, done, "fault commit result")
	if !errors.Is(res.err, ErrCommitOutcomeUnknown) {
		t.Fatalf("WithIdempotentOutputCommit must surface ErrCommitOutcomeUnknown after wire ACK loss, got %v", res.err)
	}
	if res.receipt != nil || res.replayed {
		t.Fatalf("no receipt may be fabricated on unknown outcome: receipt=%v replayed=%v", res.receipt != nil, res.replayed)
	}
	seen, suppress, eof := state.flags()
	if !seen || !suppress || !eof {
		t.Fatalf("wire evidence incomplete: commitFrameSeen=%v suppressActivated=%v EOFDelivered=%v", seen, suppress, eof)
	}
	if n := state.connCount(); n != 1 {
		t.Fatalf("fault pool dialed %d connections; the PID proof assumes exactly one", n)
	}
	if n := buildCalls.Load(); n != 1 {
		t.Fatalf("build must run exactly once on the faulted first attempt, ran %d", n)
	}

	// 4 — observer projection on the normal pool: the COMMIT really landed.
	got, err := (&Repo{Pool: pool}).GetOutputCommitReceipt(ctx, scope, dataset.ID, key)
	if err != nil || got == nil {
		t.Fatalf("observer must see the committed receipt: %v", err)
	}
	if got.IdempotencyKey != key || got.RequestFingerprint != fingerprint {
		t.Fatalf("receipt binding mismatch: key=%q fingerprint=%q", got.IdempotencyKey, got.RequestFingerprint)
	}
	if got.Transaction.Status != models.TransactionStatusCommitted {
		t.Fatalf("receipt transaction status = %q, want COMMITTED", got.Transaction.Status)
	}
	var txnStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM dataset_transactions WHERE dataset_id=$1`, dataset.ID).Scan(&txnStatus); err != nil {
		t.Fatalf("observer transaction status: %v", err)
	}
	if txnStatus != string(models.TransactionStatusCommitted) {
		t.Fatalf("dataset transaction status = %q, want COMMITTED", txnStatus)
	}
	if n := w1Count(t, pool, `SELECT COUNT(*) FROM dataset_transactions WHERE dataset_id=$1`, dataset.ID); n != 1 {
		t.Fatalf("expected exactly 1 transaction, got %d", n)
	}
	if n := w1Count(t, pool, `SELECT COUNT(*) FROM dataset_output_commit_receipts WHERE dataset_id=$1 AND idempotency_key=$2`, dataset.ID, key); n != 1 {
		t.Fatalf("expected exactly 1 receipt, got %d", n)
	}
	if n := w1Count(t, pool, `SELECT COUNT(*) FROM dataset_files WHERE dataset_id=$1`, dataset.ID); n != 1 {
		t.Fatalf("expected exactly 1 trigger-projected file row, got %d", n)
	}
	published := w1SnapshotPublished(t, pool, dataset.ID)
	if published.versions != 1 {
		t.Fatalf("expected exactly 1 version, got %d", published.versions)
	}
	if published.head != got.Transaction.ID {
		t.Fatalf("branch head %v is not the committed transaction %v", published.head, got.Transaction.ID)
	}
	if !slices.Equal(published.indexPaths, []string{"part-00000.ndjson"}) {
		t.Fatalf("index projection = %v, want [part-00000.ndjson]", published.indexPaths)
	}
	if !slices.Equal(published.lineage, []string{lineageTarget}) {
		t.Fatalf("lineage projection = %v, want [%s]", published.lineage, lineageTarget)
	}

	// 5 — exact-receipt replay on an independent pool: stored bytes, build
	// never entered again, projection and counts unchanged.
	poolB := w1SecondPool(ctx, t, pool)
	replay, replayed, err := (&Repo{Pool: poolB}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, fingerprint, build)
	if err != nil || !replayed {
		t.Fatalf("retry after ACK loss must replay the stored receipt: replayed=%v err=%v", replayed, err)
	}
	if string(replay.ResponseBody) != string(got.ResponseBody) {
		t.Fatal("replay must return the exact stored response bytes")
	}
	if n := buildCalls.Load(); n != 1 {
		t.Fatalf("build must not be entered again on replay, ran %d times", n)
	}
	if after := w1SnapshotPublished(t, pool, dataset.ID); !after.equal(published) {
		t.Fatalf("published surface changed after replay: %+v vs %+v", after, published)
	}
	if n := w1Count(t, pool, `SELECT COUNT(*) FROM dataset_versions WHERE dataset_id=$1`, dataset.ID); n != 1 {
		t.Fatalf("replay must not cut another version, got %d", n)
	}
	if n := w1Count(t, pool, `SELECT COUNT(*) FROM dataset_output_commit_receipts WHERE dataset_id=$1 AND idempotency_key=$2`, dataset.ID, key); n != 1 {
		t.Fatalf("replay must not insert another receipt, got %d", n)
	}
}
