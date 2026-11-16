//go:build integration

// P2.6 W1 — real-Postgres interleave/rollback acceptance sources (REQUIRED
// NAMES for the W6 execution pass; NOT executed in W1 — compile/vet only).
//
// Concurrency is driven by deterministic barriers inside the build callback
// — the point where the keyed transaction already holds the datasets row
// lock — and every wait is PROVEN through pg_stat_activity + pg_blocking_pids
// against the captured backend PID of the lock holder: an unrelated lock
// waiter can never satisfy a barrier. Every barrier/release and every
// channel receive is bounded, and the idempotent release is pre-registered
// via t.Cleanup (LIFO: workers unpark before the pools close) so no failure
// path can hang the test or pin a connection.
//
//	TestW1KeyedLockInterleavesWithReplay             A parks holding the lock → B proven blocked by A's PID → replay, build not called
//	TestW1KeyedLockWaitCancelZeroWrite               waiter ctx-cancelled → zero writes, first unaffected, later replay stable
//	TestW1KeyedReceiptInsertFailureRollsBackPublish  receipt INSERT fails 23514 on the named constraint AFTER the publish completed → full outer rollback
//	TestW1KeyedCommitAThenBReplayStable              A then B → replay/GET A keep original bytes; head/current_version/full lineage/index exactly B's, checked after B and after each A read
//	TestW1KeyedLegacySharedLockOrderNoDeadlock       keyed(main) vs legacy commit(dev): legacy proven blocked by keyed's PID → shared lock order, no 40P01
//	TestW1GetOutputCommitReceiptOwnForeignUnknown    JOIN path: own receipt, foreign nil, unknown nil
//
// These tests commit metadata-only files — valid for transaction/projection
// proofs; downloadable bytes/hash acceptance happens later over HTTP.
//
// W6 exact run (see W1-DATASET-INTERFACE.md §5):
//
//	go test -tags integration -count=1 -race -v -run 'W1|WithIdempotentOutputCommit' ./services/dataset-versioning-service/internal/repo/
package repo

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// w1Setup applies the migration set (idempotent) and creates one org-owned
// dataset, returning it with its tenant scope.
func w1Setup(ctx context.Context, t *testing.T, pool *pgxpool.Pool) (models.Dataset, TenantScope, uuid.UUID) {
	t.Helper()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	org, owner := uuid.New(), uuid.New()
	dataset, err := (&Repo{Pool: pool}).CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "w1-" + uuid.NewString()[:8]}, owner, &org)
	if err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	if dataset == nil {
		t.Fatal("create dataset returned nil")
	}
	return *dataset, TenantScope{OrgID: &org, Subject: owner}, owner
}

// w1BuildOpts configures the keyed build fixture. entered/release form the
// deterministic barrier (entered fires once the outer transaction holds the
// datasets row lock; the callback then parks until release or ctx end).
// called marks build ENTRY (used to prove a replay never enters the build);
// published marks completion of the inner publish (CommitTransactionWithPublish
// succeeded) — only that proves the walked-publish-then-rollback property.
// backendPID, when set, captures pg_backend_pid() on the transaction's own
// connection at the barrier, identifying the lock holder exactly.
type w1BuildOpts struct {
	rawRef     string
	scope      TenantScope
	branch     string
	file       string
	lineage    []string
	startedBy  uuid.UUID
	entered    chan struct{}
	release    chan struct{}
	called     *atomic.Bool
	published  *atomic.Bool
	backendPID *int32
}

func w1Build(o w1BuildOpts) func(context.Context, OutputCommitTxStore) (models.CommitDatasetOutputResponse, error) {
	return func(ctx context.Context, tx OutputCommitTxStore) (models.CommitDatasetOutputResponse, error) {
		var zero models.CommitDatasetOutputResponse
		if o.called != nil {
			o.called.Store(true)
		}
		if o.backendPID != nil {
			if rp, ok := tx.(*Repo); ok {
				_ = rp.Pool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(o.backendPID)
			}
		}
		if o.entered != nil {
			o.entered <- struct{}{}
		}
		if o.release != nil {
			select {
			case <-o.release:
			case <-ctx.Done():
				return zero, ctx.Err()
			}
		}
		dataset, err := tx.ResolveDatasetForTenant(ctx, o.rawRef, o.scope)
		if err != nil {
			return zero, err
		}
		if dataset == nil {
			return zero, ErrNotFound
		}
		if err := tx.EnsureDefaultBranch(ctx, dataset); err != nil {
			return zero, err
		}
		branchRow, err := tx.GetRuntimeBranch(ctx, dataset.ID, o.branch)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return zero, err
		}
		if branchRow == nil {
			return zero, ErrNotFound
		}
		txn, err := tx.StartTransaction(ctx, dataset.ID, branchRow.ID, branchRow.Name, models.TransactionTypeSnapshot, "w1 build", models.JSONValue([]byte(`{}`)), o.startedBy)
		if err != nil {
			return zero, err
		}
		staged := []models.StageTransactionFile{{LogicalPath: o.file, PhysicalPath: "datasets/" + dataset.ID.String() + "/out/" + o.file, SizeBytes: 3, Operation: models.FileOperationAdd}}
		if err := tx.StageTransactionFiles(ctx, dataset.ID, txn.ID, staged); err != nil {
			return zero, err
		}
		if err := tx.MergeTransactionMetadata(ctx, dataset.ID, txn.ID, models.JSONValue([]byte(`{"pipeline_output":true}`))); err != nil {
			return zero, err
		}
		links := make([]models.PutDatasetLineageLink, 0, len(o.lineage))
		for _, target := range o.lineage {
			links = append(links, models.PutDatasetLineageLink{Direction: "upstream", TargetRID: target})
		}
		snapshot, err := tx.CommitTransactionWithPublish(ctx, dataset.ID, txn.ID, []models.PutDatasetFileIndexEntry{{Path: o.file, StoragePath: "datasets/" + dataset.ID.String() + "/out/" + o.file, SizeBytes: ptrInt64(3)}}, links)
		if err != nil {
			return zero, err
		}
		if o.published != nil {
			o.published.Store(true)
		}
		return models.CommitDatasetOutputResponse{
			DatasetID:   dataset.ID,
			DatasetRID:  "ri.foundry.main.dataset." + dataset.ID.String(),
			Branch:      branchRow.Name,
			Transaction: snapshot.Transaction,
			Files:       snapshot.Files,
			Preview:     snapshot.Preview,
		}, nil
	}
}

// w1Recv/w1RecvT bound every channel receive so a worker that died before
// signaling fails the test instead of hanging it; the pre-registered
// release cleanup (see tests) unparks any parked worker afterwards.
func w1Recv(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func w1RecvT[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	var zero T
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return zero
	}
}

// w1SecondPool opens an independent pool ("second instance") on the same
// database as poolA.
func w1SecondPool(ctx context.Context, t *testing.T, poolA *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	poolB, err := pgxpool.New(ctx, poolA.Config().ConnString())
	if err != nil {
		t.Fatalf("second pool: %v", err)
	}
	t.Cleanup(poolB.Close)
	return poolB
}

// w1Barrier registers the idempotent release for a barrier BEFORE any
// goroutine launches. t.Cleanup runs LIFO and bootPool/poolB registered
// their Close cleanups earlier, so the release always unparks workers
// BEFORE the pools close — no failure path can pin a connection.
func w1Barrier(t *testing.T) (entered chan struct{}, release chan struct{}, releaseFn func()) {
	t.Helper()
	entered = make(chan struct{}, 1)
	release = make(chan struct{})
	var once sync.Once
	releaseFn = func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseFn)
	return entered, release, releaseFn
}

// w1AwaitBlockedByPID polls until some session is waiting on a heavy lock
// AND pg_blocking_pids proves the given backend is its blocker, returning
// the waiter's PID. An unrelated lock waiter can never satisfy this.
func w1AwaitBlockedByPID(t *testing.T, pool *pgxpool.Pool, blocker int32) int32 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var waiter int32
		err := pool.QueryRow(context.Background(), `
			SELECT w.pid FROM pg_stat_activity w
			 WHERE w.wait_event_type = 'Lock'
			   AND w.datname = current_database()
			   AND pg_blocking_pids(w.pid) @> ARRAY[$1::int]
			 LIMIT 1`, blocker).Scan(&waiter)
		if err == nil && waiter != 0 {
			return waiter
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no session observed blocked by backend pid %d", blocker)
	return 0
}

func w1Count(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	return n
}

// w1Result carries one WithIdempotentOutputCommit outcome across goroutines.
type w1Result struct {
	receipt  *models.OutputCommitReceipt
	replayed bool
	err      error
}

// TestW1KeyedLockInterleavesWithReplay: instance A parks inside build
// holding the datasets row lock; instance B is proven blocked by A's
// captured backend PID (pg_blocking_pids), and once A commits, B returns
// the stored receipt with replayed=true and its build never ran.
func TestW1KeyedLockInterleavesWithReplay(t *testing.T) {
	ctx := context.Background()
	poolA := bootPool(ctx, t)
	poolB := w1SecondPool(ctx, t, poolA)

	dataset, scope, owner := w1Setup(ctx, t, poolA)
	key := "of-p26-pub-v1:" + uuid.NewString()
	fingerprint := keyedFingerprint(keyedFixtureBody(t))

	entered, release, releaseFn := w1Barrier(t)
	var aPID int32
	firstDone := make(chan w1Result, 1)
	go func() {
		r, replayed, cerr := (&Repo{Pool: poolA}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, fingerprint, w1Build(w1BuildOpts{
			rawRef: dataset.ID.String(), scope: scope, branch: "main", file: "part-00000.ndjson",
			startedBy: owner, entered: entered, release: release, backendPID: &aPID,
		}))
		firstDone <- w1Result{r, replayed, cerr}
	}()
	w1Recv(t, entered, "first build barrier")
	if aPID == 0 {
		t.Fatal("failed to capture the first instance's backend pid")
	}

	var bCalled atomic.Bool
	secondDone := make(chan w1Result, 1)
	go func() {
		r, replayed, cerr := (&Repo{Pool: poolB}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, fingerprint, w1Build(w1BuildOpts{
			rawRef: dataset.ID.String(), scope: scope, branch: "main", file: "part-00000.ndjson",
			startedBy: owner, called: &bCalled,
		}))
		secondDone <- w1Result{r, replayed, cerr}
	}()

	waiter := w1AwaitBlockedByPID(t, poolA, aPID)
	if waiter == aPID {
		t.Fatal("waiter must be a different backend than the holder")
	}
	select {
	case <-secondDone:
		t.Fatal("second instance must not finish while the first holds the lock")
	default:
	}

	releaseFn()
	first := w1RecvT(t, firstDone, "first commit result")
	if first.err != nil || first.replayed {
		t.Fatalf("first: replayed=%v err=%v", first.replayed, first.err)
	}
	second := w1RecvT(t, secondDone, "second commit result")
	if second.err != nil {
		t.Fatalf("second: %v", second.err)
	}
	if !second.replayed {
		t.Fatal("second must be a replay of the stored receipt")
	}
	if bCalled.Load() {
		t.Fatal("second instance's build must never run on replay")
	}
	if string(second.receipt.ResponseBody) != string(first.receipt.ResponseBody) {
		t.Fatal("replay bytes differ from the first response")
	}
	if n := w1Count(t, poolA, `SELECT COUNT(*) FROM dataset_output_commit_receipts WHERE dataset_id=$1`, dataset.ID); n != 1 {
		t.Fatalf("expected exactly 1 receipt, got %d", n)
	}
}

// TestW1KeyedLockWaitCancelZeroWrite: a caller cancelled while waiting on
// the datasets row lock (proven blocked by the holder's PID) writes
// nothing, does not disturb the lock holder, and a later fresh request
// replays the first response byte-stably.
func TestW1KeyedLockWaitCancelZeroWrite(t *testing.T) {
	ctx := context.Background()
	poolA := bootPool(ctx, t)
	poolB := w1SecondPool(ctx, t, poolA)

	dataset, scope, owner := w1Setup(ctx, t, poolA)
	key := "of-p26-pub-v1:" + uuid.NewString()
	fingerprint := keyedFingerprint(keyedFixtureBody(t))

	entered, release, releaseFn := w1Barrier(t)
	var aPID int32
	firstDone := make(chan error, 1)
	go func() {
		_, _, cerr := (&Repo{Pool: poolA}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, fingerprint, w1Build(w1BuildOpts{
			rawRef: dataset.ID.String(), scope: scope, branch: "main", file: "part-00000.ndjson",
			startedBy: owner, entered: entered, release: release, backendPID: &aPID,
		}))
		firstDone <- cerr
	}()
	w1Recv(t, entered, "first build barrier")
	if aPID == 0 {
		t.Fatal("failed to capture the first instance's backend pid")
	}

	waitCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	waitDone := make(chan error, 1)
	go func() {
		_, _, cerr := (&Repo{Pool: poolB}).WithIdempotentOutputCommit(waitCtx, scope, dataset.ID, key, fingerprint, w1Build(w1BuildOpts{
			rawRef: dataset.ID.String(), scope: scope, branch: "main", file: "part-00000.ndjson",
			startedBy: owner,
		}))
		waitDone <- cerr
	}()
	w1AwaitBlockedByPID(t, poolA, aPID)
	cancel()
	if err := w1RecvT(t, waitDone, "cancelled waiter result"); err == nil {
		t.Fatal("cancelled waiter must surface an error, not a silent success")
	}

	// Zero writes from the cancelled waiter; the holder is still parked.
	if n := w1Count(t, poolA, `SELECT COUNT(*) FROM dataset_transactions WHERE dataset_id=$1`, dataset.ID); n != 0 {
		t.Fatalf("cancelled waiter left %d transaction rows", n)
	}
	select {
	case err := <-firstDone:
		t.Fatalf("first must still be parked, but it returned %v", err)
	default:
	}

	releaseFn()
	if err := w1RecvT(t, firstDone, "first commit result"); err != nil {
		t.Fatalf("first commit after release: %v", err)
	}
	later, replayed, err := (&Repo{Pool: poolB}).WithIdempotentOutputCommit(context.Background(), scope, dataset.ID, key, fingerprint, w1Build(w1BuildOpts{
		rawRef: dataset.ID.String(), scope: scope, branch: "main", file: "part-00000.ndjson",
		startedBy: owner,
	}))
	if err != nil || !replayed {
		t.Fatalf("later replay: replayed=%v err=%v", replayed, err)
	}
	first, err := (&Repo{Pool: poolA}).GetOutputCommitReceipt(context.Background(), scope, dataset.ID, key)
	if err != nil || first == nil {
		t.Fatalf("get first receipt: %v", err)
	}
	if string(later.ResponseBody) != string(first.ResponseBody) {
		t.Fatal("replay after cancelled waiter must keep the original bytes")
	}
}

// TestW1KeyedReceiptInsertFailureRollsBackPublish: the receipt INSERT fails
// with 23514 on the fingerprint CHECK constraint (invisible to the receipt
// SELECT because no row exists) AFTER the inner publish COMPLETED (flag set
// only past CommitTransactionWithPublish). The single outer transaction
// must roll everything back — receipt, transactions, staged rows, version,
// branch head, file index, trigger-projected dataset_files and lineage —
// and a later valid commit still succeeds as the true first.
func TestW1KeyedReceiptInsertFailureRollsBackPublish(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)
	dataset, scope, owner := w1Setup(ctx, t, pool)
	key := "of-p26-pub-v1:" + uuid.NewString()
	lineageTargets := []string{
		"ri.foundry.main.dataset." + uuid.NewString(),
		"ri.foundry.main.dataset." + uuid.NewString(),
	}

	var published atomic.Bool
	_, _, err := (&Repo{Pool: pool}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, "not-hex-not-64", w1Build(w1BuildOpts{
		rawRef: dataset.ID.String(), scope: scope, branch: "main", file: "part-00000.ndjson",
		lineage: lineageTargets, startedBy: owner, published: &published,
	}))
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != "dataset_output_commit_receipts_fingerprint_hex" {
		t.Fatalf("expected 23514 on dataset_output_commit_receipts_fingerprint_hex, got %+v", err)
	}
	if !published.Load() {
		t.Fatal("the inner publish must have COMPLETED before the receipt INSERT failure")
	}

	for name, n := range map[string]int{
		"receipts":      w1Count(t, pool, `SELECT COUNT(*) FROM dataset_output_commit_receipts WHERE dataset_id=$1`, dataset.ID),
		"transactions":  w1Count(t, pool, `SELECT COUNT(*) FROM dataset_transactions WHERE dataset_id=$1`, dataset.ID),
		"staged":        w1Count(t, pool, `SELECT COUNT(*) FROM dataset_transaction_files f JOIN dataset_transactions t ON t.id=f.transaction_id WHERE t.dataset_id=$1`, dataset.ID),
		"versions":      w1Count(t, pool, `SELECT COUNT(*) FROM dataset_versions WHERE dataset_id=$1`, dataset.ID),
		"file_index":    w1Count(t, pool, `SELECT COUNT(*) FROM dataset_file_index WHERE dataset_id=$1`, dataset.ID),
		"dataset_files": w1Count(t, pool, `SELECT COUNT(*) FROM dataset_files WHERE dataset_id=$1`, dataset.ID),
		"lineage":       w1Count(t, pool, `SELECT COUNT(*) FROM dataset_lineage_links WHERE dataset_id=$1`, dataset.ID),
	} {
		if n != 0 {
			t.Fatalf("rollback left %s=%d behind", name, n)
		}
	}
	var headSet int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_branches WHERE dataset_id=$1 AND head_transaction_id IS NOT NULL`, dataset.ID).Scan(&headSet); err != nil {
		t.Fatalf("branch head probe: %v", err)
	}
	if headSet != 0 {
		t.Fatalf("branch head advanced despite rollback (%d branches)", headSet)
	}

	// The key stayed unbound: the valid retry is the true first commit.
	_, replayed, err := (&Repo{Pool: pool}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, keyedFingerprint(keyedFixtureBody(t)), w1Build(w1BuildOpts{
		rawRef: dataset.ID.String(), scope: scope, branch: "main", file: "part-00000.ndjson",
		startedBy: owner,
	}))
	if err != nil || replayed {
		t.Fatalf("valid retry after rollback: replayed=%v err=%v", replayed, err)
	}
}

// w1PublishedState is the exact published surface of a dataset: the main
// branch head transaction, datasets.current_version, the version count, the
// full lineage target set and the full file-index path set (both sorted).
type w1PublishedState struct {
	head           uuid.UUID
	currentVersion int32
	versions       int
	lineage        []string
	indexPaths     []string
}

func w1SnapshotPublished(t *testing.T, pool *pgxpool.Pool, datasetID uuid.UUID) w1PublishedState {
	t.Helper()
	var s w1PublishedState
	if err := pool.QueryRow(context.Background(), `
		SELECT b.head_transaction_id, d.current_version,
		       (SELECT COUNT(*) FROM dataset_versions v WHERE v.dataset_id = d.id)
		  FROM datasets d
		  JOIN dataset_branches b ON b.dataset_id = d.id AND b.name = 'main'
		 WHERE d.id = $1`, datasetID).Scan(&s.head, &s.currentVersion, &s.versions); err != nil {
		t.Fatalf("published-state snapshot: %v", err)
	}
	rows, err := pool.Query(context.Background(), `SELECT target_rid FROM dataset_lineage_links WHERE dataset_id=$1 ORDER BY target_rid`, datasetID)
	if err != nil {
		t.Fatalf("lineage snapshot: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var target string
		if err := rows.Scan(&target); err != nil {
			t.Fatalf("lineage scan: %v", err)
		}
		s.lineage = append(s.lineage, target)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("lineage rows: %v", err)
	}
	rows, err = pool.Query(context.Background(), `SELECT path FROM dataset_file_index WHERE dataset_id=$1 ORDER BY path`, datasetID)
	if err != nil {
		t.Fatalf("index snapshot: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			t.Fatalf("index scan: %v", err)
		}
		s.indexPaths = append(s.indexPaths, path)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("index rows: %v", err)
	}
	return s
}

func (s w1PublishedState) equal(other w1PublishedState) bool {
	return s.head == other.head &&
		s.currentVersion == other.currentVersion &&
		s.versions == other.versions &&
		slices.Equal(s.lineage, other.lineage) &&
		slices.Equal(s.indexPaths, other.indexPaths)
}

// TestW1KeyedCommitAThenBReplayStable: A commits (file a, two lineage
// links), then B commits (file b, one lineage link). Replaying A and GET-ing
// A return A's original bytes, and the ENTIRE published surface — branch
// head, current_version, version count, full lineage set, full index set —
// is snapshot right after B and re-snap after the A replay and after the
// GET, and must be exactly B's state at all three points.
func TestW1KeyedCommitAThenBReplayStable(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)
	dataset, scope, owner := w1Setup(ctx, t, pool)
	lineageA := []string{
		"ri.foundry.main.dataset." + uuid.NewString(),
		"ri.foundry.main.dataset." + uuid.NewString(),
	}
	lineageB := []string{"ri.foundry.main.dataset." + uuid.NewString()}
	keyA := "of-p26-pub-v1:" + uuid.NewString()
	keyB := "of-p26-pub-v1:" + uuid.NewString()
	build := func(file string, lineage []string) func(context.Context, OutputCommitTxStore) (models.CommitDatasetOutputResponse, error) {
		return w1Build(w1BuildOpts{rawRef: dataset.ID.String(), scope: scope, branch: "main", file: file, lineage: lineage, startedBy: owner})
	}

	firstA, replayed, err := (&Repo{Pool: pool}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, keyA, keyedFingerprint(keyedFixtureBody(t)), build("a.ndjson", lineageA))
	if err != nil || replayed {
		t.Fatalf("A: replayed=%v err=%v", replayed, err)
	}
	bReceipt, replayed, err := (&Repo{Pool: pool}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, keyB, keyedFingerprint(keyedFixtureBody(t)), build("b.ndjson", lineageB))
	if err != nil || replayed {
		t.Fatalf("B: replayed=%v err=%v", replayed, err)
	}

	afterB := w1SnapshotPublished(t, pool, dataset.ID)
	if !slices.Contains(afterB.lineage, lineageB[0]) || len(afterB.lineage) != 1 {
		t.Fatalf("after B the lineage set must be exactly B's single link, got %v", afterB.lineage)
	}
	if !slices.Equal(afterB.indexPaths, []string{"b.ndjson"}) {
		t.Fatalf("after B the index must hold exactly b.ndjson, got %v", afterB.indexPaths)
	}
	if afterB.versions != 2 {
		t.Fatalf("A+B must cut exactly 2 versions, got %d", afterB.versions)
	}

	replayA, replayed, err := (&Repo{Pool: pool}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, keyA, firstA.RequestFingerprint, build("a.ndjson", lineageA))
	if err != nil || !replayed {
		t.Fatalf("replay A: replayed=%v err=%v", replayed, err)
	}
	if string(replayA.ResponseBody) != string(firstA.ResponseBody) {
		t.Fatal("replay A must return A's original bytes")
	}
	if state := w1SnapshotPublished(t, pool, dataset.ID); !state.equal(afterB) {
		t.Fatalf("published surface changed after replay A: %+v vs %+v", state, afterB)
	}

	gotA, err := (&Repo{Pool: pool}).GetOutputCommitReceipt(ctx, scope, dataset.ID, keyA)
	if err != nil || gotA == nil || string(gotA.ResponseBody) != string(firstA.ResponseBody) {
		t.Fatalf("GET A must return A's original bytes: %v", err)
	}
	if state := w1SnapshotPublished(t, pool, dataset.ID); !state.equal(afterB) {
		t.Fatalf("published surface changed after GET A: %+v vs %+v", state, afterB)
	}

	if n := w1Count(t, pool, `SELECT COUNT(*) FROM dataset_output_commit_receipts WHERE dataset_id=$1`, dataset.ID); n != 2 {
		t.Fatalf("expected exactly 2 receipts (A and B), got %d", n)
	}
	_ = bReceipt // B's receipt exists; its bytes are irrelevant to the invariants above
}

// TestW1KeyedLegacySharedLockOrderNoDeadlock: a keyed commit parked on main
// (holding the datasets row lock) races a plain legacy commit on dev. The
// legacy commit is PROVEN blocked by the keyed instance's backend PID;
// commitTransactionSteps takes the same datasets lock first, so both finish
// with no deadlock (40P01), one version each, and no OPEN leftovers.
func TestW1KeyedLegacySharedLockOrderNoDeadlock(t *testing.T) {
	ctx := context.Background()
	poolA := bootPool(ctx, t)
	poolB := w1SecondPool(ctx, t, poolA)

	dataset, scope, owner := w1Setup(ctx, t, poolA)
	repoA := &Repo{Pool: poolA}
	repoB := &Repo{Pool: poolB}
	// main must exist BEFORE dev: once any branch row exists the keyed
	// build's EnsureDefaultBranch becomes a no-op, and dev alone would
	// leave main missing for the keyed side. dev derives from main — with
	// any branch already present CreateRuntimeBranch requires an explicit
	// parent_branch or source (validation caught by the first real-PG run).
	if err := repoA.EnsureDefaultBranch(ctx, &models.Dataset{ID: dataset.ID, CurrentVersion: dataset.CurrentVersion}); err != nil {
		t.Fatalf("ensure main: %v", err)
	}
	parentMain := "main"
	dev, err := repoA.CreateRuntimeBranch(ctx, dataset.ID, &models.CreateBranchBody{Name: "dev", ParentBranch: &parentMain}, owner)
	if err != nil {
		t.Fatalf("create dev branch: %v", err)
	}

	// Legacy OPEN transaction staged on dev BEFORE the keyed commit starts
	// (its StartTransaction is autocommit, exactly like the no-key path).
	legacyTxn, err := repoB.StartTransaction(ctx, dataset.ID, dev.ID, dev.Name, models.TransactionTypeAppend, "legacy side", models.JSONValue([]byte(`{}`)), owner)
	if err != nil {
		t.Fatalf("legacy start: %v", err)
	}
	if err := repoB.StageTransactionFiles(ctx, dataset.ID, legacyTxn.ID, []models.StageTransactionFile{{LogicalPath: "legacy.ndjson", PhysicalPath: "datasets/" + dataset.ID.String() + "/out/legacy.ndjson", SizeBytes: 3, Operation: models.FileOperationAdd}}); err != nil {
		t.Fatalf("legacy stage: %v", err)
	}

	entered, release, releaseFn := w1Barrier(t)
	var aPID int32
	keyedDone := make(chan error, 1)
	key := "of-p26-pub-v1:" + uuid.NewString()
	go func() {
		_, _, cerr := repoA.WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, keyedFingerprint(keyedFixtureBody(t)), w1Build(w1BuildOpts{
			rawRef: dataset.ID.String(), scope: scope, branch: "main", file: "keyed.ndjson",
			startedBy: owner, entered: entered, release: release, backendPID: &aPID,
		}))
		keyedDone <- cerr
	}()
	w1Recv(t, entered, "keyed build barrier")
	if aPID == 0 {
		t.Fatal("failed to capture the keyed instance's backend pid")
	}

	legacyDone := make(chan error, 1)
	go func() {
		legacyDone <- repoB.CommitTransaction(ctx, dataset.ID, legacyTxn.ID)
	}()
	if waiter := w1AwaitBlockedByPID(t, poolA, aPID); waiter == aPID {
		t.Fatal("waiter must be a different backend than the holder")
	}
	select {
	case err := <-legacyDone:
		t.Fatalf("legacy commit must wait for the keyed lock, returned %v", err)
	default:
	}

	releaseFn()
	if err := w1RecvT(t, keyedDone, "keyed commit result"); err != nil {
		t.Fatalf("keyed commit: %v", err)
	}
	if err := w1RecvT(t, legacyDone, "legacy commit result"); err != nil {
		msg := err.Error()
		if strings.Contains(msg, "40P01") || strings.Contains(strings.ToLower(msg), "deadlock") {
			t.Fatalf("deadlock detected despite shared lock order: %v", err)
		}
		t.Fatalf("legacy commit: %v", err)
	}
	if n := w1Count(t, poolA, `SELECT COUNT(*) FROM dataset_versions WHERE dataset_id=$1`, dataset.ID); n != 2 {
		t.Fatalf("expected one version per commit, got %d", n)
	}
	if n := w1Count(t, poolA, `SELECT COUNT(*) FROM dataset_transactions WHERE dataset_id=$1 AND status='OPEN'`, dataset.ID); n != 0 {
		t.Fatalf("no OPEN leftovers, got %d", n)
	}
	if n := w1Count(t, poolA, `SELECT COUNT(*) FROM dataset_output_commit_receipts WHERE dataset_id=$1`, dataset.ID); n != 1 {
		t.Fatalf("expected exactly 1 receipt, got %d", n)
	}
}

// TestW1GetOutputCommitReceiptOwnForeignUnknown drives the JOIN read path
// on a real database: own scope returns the stored bytes, a foreign org
// scope and an unknown key both return (nil, nil).
func TestW1GetOutputCommitReceiptOwnForeignUnknown(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)
	dataset, scope, owner := w1Setup(ctx, t, pool)
	key := "of-p26-pub-v1:" + uuid.NewString()
	first, replayed, err := (&Repo{Pool: pool}).WithIdempotentOutputCommit(ctx, scope, dataset.ID, key, keyedFingerprint(keyedFixtureBody(t)), w1Build(w1BuildOpts{
		rawRef: dataset.ID.String(), scope: scope, branch: "main", file: "part-00000.ndjson",
		startedBy: owner,
	}))
	if err != nil || replayed {
		t.Fatalf("seed commit: replayed=%v err=%v", replayed, err)
	}

	own, err := (&Repo{Pool: pool}).GetOutputCommitReceipt(ctx, scope, dataset.ID, key)
	if err != nil || own == nil {
		t.Fatalf("own receipt: %v", err)
	}
	if string(own.ResponseBody) != string(first.ResponseBody) || own.Branch != first.Branch || own.Transaction.ID != first.Transaction.ID {
		t.Fatal("own receipt must carry the stored response payload")
	}

	foreignOrg := uuid.New()
	foreign, err := (&Repo{Pool: pool}).GetOutputCommitReceipt(ctx, TenantScope{OrgID: &foreignOrg, Subject: uuid.New()}, dataset.ID, key)
	if err != nil || foreign != nil {
		t.Fatalf("foreign scope must get (nil,nil), got (%v,%v)", foreign, err)
	}
	unknown, err := (&Repo{Pool: pool}).GetOutputCommitReceipt(ctx, scope, dataset.ID, "of-p26-pub-v1:"+uuid.NewString())
	if err != nil || unknown != nil {
		t.Fatalf("unknown key must get (nil,nil), got (%v,%v)", unknown, err)
	}
}
