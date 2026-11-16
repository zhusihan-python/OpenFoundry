//go:build integration

// P2.6 wire-level COMMIT ACK-loss proof for the Connector finalize
// (wire-commit-ackloss-tests.md + review-wire-round1 W2 fixes). Required
// name: TestPGP26FinalizeWireCommitAckLossOnce.
//
// What is proven HERE: the LOCAL PostgreSQL commit of FinalizeRecoveredRun
// executes on the server while its ACK is lost on the wire — the method
// returns an error, the observer pool sees the full converged state, and the
// duplicate finalize/claim on a NORMAL connection answer already_resolved
// with zero writes. The receipt input is p26ReceiptFor(intent) — a synthetic
// matching DVS receipt — so this case proves LOCAL-PG ACK uncertainty only,
// NOT a remote Dataset commit in the same case; the real remote-HTTP proof
// belongs to W7 and is never claimed here.
//
// Mechanism (root-verified): pgx v5.7.4 Tx.Commit issues the exact
// simple-Query `commit\0`; a test-only DialFunc wrapper passes the PG
// startup through UNARMED and is Armed explicitly only after setup, PID
// warm, LISTEN (completed BEFORE the target transaction can notify) and the
// test-owned trigger are in place; the exact commit frame is forwarded with
// suppression armed, every ACK byte is held back (Read before/after checks),
// and the suppressed Read resolves to EOF only after the INDEPENDENT LISTEN
// connection validated the real notification (channel + payload UUID + the
// SAME fault backend PID — both the payload PID and the protocol
// NotificationResponse PID). A watchdog and every notifier failure path
// close the fault wire so nothing can hang; a timeout is an honest FAIL,
// never a fabricated validated ACK.
package repo

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jackc/pgx/v5"
)

// p26AckLossFaultPool builds a MaxConns=1 pool whose single connection runs
// through the (initially passthrough) ACK-loss wire wrapper, forces the
// connection NOW (wrapper cached, PID warm) and returns both. The wire's
// frame parser is NOT armed here — the caller arms it after LISTEN/trigger
// setup (review-wire-round1 item 1).
func p26AckLossFaultPool(t *testing.T, dsn string) (*pgxpool.Pool, *ackLossWire) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	inner := cfg.ConnConfig.DialFunc
	var wire *ackLossWire
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, cerr := inner(ctx, network, addr)
		if cerr != nil {
			return nil, cerr
		}
		w := newAckLossWire(c)
		wire = w
		return w, nil
	}
	cfg.MaxConns = 1
	cfg.MinConns = 0
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	// Registered FIRST so it runs LAST (LIFO): every gate/socket cleanup
	// below runs before the pool closes.
	t.Cleanup(pool.Close)
	// Warm the single connection now (passthrough phase).
	var pid int32
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&pid))
	require.NotNil(t, wire, "the fault pool's single connection must be wrapped")
	return pool, wire
}

// p26AckLossNotifyTrigger installs a test-owned trigger whose FUNCTION BODY
// uses TG_ARGV only (review-wire-round1 item 3: $n inside a dollar-quoted
// body is body text, NOT an Exec bind — the run/channel/payload constants
// are bound at CREATE TRIGGER ... EXECUTE FUNCTION fn($1,$2,$3), where $n
// ARE pool.Exec parameters). Fires ONLY on the exact owned run's
// running→succeeded transition. Cleanup drops both objects.
func p26AckLossNotifyTrigger(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID, channel, payloadUUID string) {
	t.Helper()
	ctx := context.Background()
	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	fn := "p26_ackloss_fn_" + suffix
	trg := "p26_ackloss_trg_" + suffix
	_, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION `+fn+`() RETURNS trigger AS $body$
		BEGIN
		  IF NEW.id = TG_ARGV[0]::uuid AND OLD.status = 'running' AND NEW.status = 'succeeded' THEN
		    PERFORM pg_notify(TG_ARGV[1], TG_ARGV[2] || ':' || pg_backend_pid());
		  END IF;
		  RETURN NEW;
		END
		$body$ LANGUAGE plpgsql`)
	require.NoError(t, err, "trigger function creation must succeed BEFORE any ACK fault — a setup failure is never an ACK proof")
	// DDL literal constants (review followup 13:00): CREATE TRIGGER args
	// are literal constants, not bind expressions — assemble the DDL
	// SERVER-SIDE with format(%I/%L) from parameterized inputs. (The %I/%L
	// placeholders are PG format() specifiers — this string is a plain pgx
	// SQL parameter, NOT a Go fmt template, so they stay SINGLE-percent; the
	// inputs are cast ::text so format's variadic "any" resolution is
	// unambiguous.)
	var ddl string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT format('CREATE TRIGGER %I BEFORE UPDATE ON sync_runs
		                 FOR EACH ROW EXECUTE FUNCTION %I(%L, %L, %L)',
		               $1::text, $2::text, $3::text, $4::text, $5::text)`,
		trg, fn, runID.String(), channel, payloadUUID).Scan(&ddl))
	_, err = pool.Exec(ctx, ddl)
	require.NoError(t, err, "trigger creation must succeed BEFORE any ACK fault")
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS `+trg+` ON sync_runs`)
		_, _ = pool.Exec(context.Background(), `DROP FUNCTION IF EXISTS `+fn+`()`)
	})
}

// p26AckLossNotification carries BOTH pids of the validated proof: the PID
// parsed from the payload (the notifying backend inside the trigger) and
// the protocol NotificationResponse PID — both must equal the fault PID.
type p26AckLossNotification struct {
	payloadPID int32
	notifPID   uint32
}

// p26AckLossListen arms an INDEPENDENT LISTEN connection FIRST (the LISTEN
// completes before the target transaction can notify it) under the caller's
// shared life context. It returns the proof channel AND the notify-worker
// done channel: cleanup JOINS the worker (bounded) before closing the
// connection with a fresh bounded context — never a Close while the
// WaitForNotification is still active.
func p26AckLossListen(t *testing.T, lifeCtx context.Context, dsn, channel, payloadUUID string, expectedFaultPID int32) (<-chan p26AckLossNotification, <-chan struct{}) {
	t.Helper()
	conn, err := pgx.Connect(lifeCtx, dsn)
	require.NoError(t, err)
	_, err = conn.Exec(lifeCtx, "LISTEN "+channel)
	require.NoError(t, err, "LISTEN must complete before the target transaction")
	out := make(chan p26AckLossNotification, 1)
	notifyDone := make(chan struct{})
	t.Cleanup(func() {
		// Join the notification worker FIRST (lifeCtx cancellation bounds
		// its WaitForNotification), then a fresh bounded Close.
		select {
		case <-notifyDone:
		case <-time.After(2 * time.Second):
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer closeCancel()
		_ = conn.Close(closeCtx)
	})

	go func() {
		defer close(notifyDone)
		waitCtx, cancel := context.WithTimeout(lifeCtx, 25*time.Second)
		defer cancel()
		n, werr := conn.WaitForNotification(waitCtx)
		if werr != nil {
			out <- p26AckLossNotification{}
			return
		}
		if n.Channel != channel || !strings.HasPrefix(n.Payload, payloadUUID+":") {
			out <- p26AckLossNotification{}
			return
		}
		pidStr, ok := strings.CutPrefix(n.Payload, payloadUUID+":")
		pid, perr := strconv.ParseInt(pidStr, 10, 64)
		if !ok || perr != nil || pid != int64(expectedFaultPID) || n.PID != uint32(expectedFaultPID) {
			out <- p26AckLossNotification{}
			return
		}
		out <- p26AckLossNotification{payloadPID: int32(pid), notifPID: n.PID}
	}()
	return out, notifyDone
}

// p26AckLossRunCase drives one mode's full wire-loss scenario.
func p26AckLossRunCase(t *testing.T, incremental bool) {
	r, dsn, tenant, sourceID, defID := p26Fixture(t, func() []byte {
		if incremental {
			return p26IncrementalConfig()
		}
		return p26SnapshotConfig()
	}())
	ctx := context.Background()
	runID := leaseEnqueue(t, r, tenant, sourceID, defID, tenant.OwnerID)
	lease := p26Claim(t, r, runID, tenant, time.Minute)
	p26Gate(t, r, runID, tenant, sourceID, defID, lease)
	p26ForceExpire(t, r.Pool, runID)

	// Recovery claim + synthetic receipt from the frozen intent (this case
	// proves LOCAL-PG ACK uncertainty, not a remote Dataset commit).
	recClaim, _ := p26RecoveryClaim(t, r, runID, tenant)
	intent, err := r.GetPublishIntentMaterial(ctx, runID, tenant)
	require.NoError(t, err)
	receipt := p26ReceiptFor(*intent)

	// Fault session: passthrough phase — wrapper installed, connection
	// warmed, backend PID cached. The frame parser is armed only AFTER the
	// trigger and the (already completed) LISTEN are in place.
	faultPool, wire := p26AckLossFaultPool(t, dsn)
	var faultPID int32
	require.NoError(t, faultPool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&faultPID))
	faultRepo := &Repo{Pool: faultPool}
	// Bounded abort registered IMMEDIATELY (after pool.Close → runs before
	// it, LIFO): any later Fatal/test end closes the fault socket first.
	t.Cleanup(func() { _ = wire.Close() })

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	t.Cleanup(lifeCancel)
	channel := "p26_ackloss_" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	payloadUUID := uuid.New().String()
	p26AckLossNotifyTrigger(t, r.Pool, runID, channel, payloadUUID)
	notified, notifyDone := p26AckLossListen(t, lifeCtx, dsn, channel, payloadUUID, faultPID)
	// Arm LAST: from here on the only typed traffic is the session's own
	// statements, whose final `commit\0` frame is the fault point.
	wire.ArmFrameParser()

	// Watchdog + notifier-failure discipline (review-wire-round1 item 4):
	// every failure path (mismatch/timeout) CLOSES the fault wire so the
	// finalize can never hang on the unbounded holdBack channel; the
	// validated state is tracked SEPARATELY — an abort EOF is never proof.
	confirmed := make(chan p26AckLossNotification, 1)
	watchdogDone := make(chan struct{})
	go func() {
		defer close(watchdogDone)
		select {
		case n := <-notified:
			if n.payloadPID != 0 {
				// Real validated proof: release the suppressed Read to EOF.
				wire.ackLossReleased()
			} else {
				// Validation FAILED: bounded abort; the honest failure is
				// asserted below on the confirmed channel.
				_ = wire.Close()
			}
			confirmed <- n
		case <-time.After(25 * time.Second):
			_ = wire.Close()
			confirmed <- p26AckLossNotification{}
		case <-lifeCtx.Done():
			// Cleanup path: exit silently; the joins below observe us.
		}
	}()
	// MASTER failure-path cleanup (runs FIRST, LIFO — before the listener
	// join/close, the wire Close and the pool Close): cancel the shared life
	// context, then bounded-join BOTH the notification worker and the
	// watchdog. Registered immediately, not only on the happy path.
	t.Cleanup(func() {
		lifeCancel()
		select {
		case <-notifyDone:
		case <-time.After(2 * time.Second):
		}
		select {
		case <-watchdogDone:
		case <-time.After(2 * time.Second):
		}
	})

	// The finalize itself: its final COMMIT reaches the server, its ACK is
	// lost — the method must return an error and NO fabricated result.
	finCtx, finCancel := context.WithTimeout(ctx, 20*time.Second)
	defer finCancel()
	fin, ferr := faultRepo.FinalizeRecoveredRun(finCtx, recClaim, tenant, receipt)
	require.Error(t, ferr, "the lost COMMIT ACK must surface as an error")
	assert.Nil(t, fin, "no fabricated finalize result")

	// Wire proof flags: the exact commit frame was seen, suppression armed,
	// and the terminal EOF was delivered to the caller.
	deadline := time.Now().Add(5 * time.Second)
	for {
		seen, supp, eof := wire.flags()
		if seen && supp && eof {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("wire flags incomplete: seen=%v suppress=%v eof=%v", seen, supp, eof)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The independent notification validated the REAL commit — payload PID
	// AND protocol PID equal the fault backend, channel+UUID matched. A
	// timeout/validation failure FAILS here; the abort EOF is never
	// accepted as proof.
	select {
	case n := <-confirmed:
		require.NotZero(t, n.payloadPID, "the LISTEN proof must validate (channel+UUID+payload PID+notification PID)")
		require.Equal(t, faultPID, n.payloadPID)
		require.Equal(t, uint32(faultPID), n.notifPID, "the protocol NotificationResponse PID must equal the fault backend (uint32 vs uint32)")
	case <-time.After(5 * time.Second):
		t.Fatal("the validated notification never arrived — refusing any artificial pass")
	}
	// Joins complete before the cleanups run (pool close is last, LIFO).
	<-watchdogDone

	// Observer (normal connection): the server DID commit the full
	// convergence — status, intent, txn binding AND the run's own
	// dataset_version_id/counts mirror the frozen intent exactly.
	var status, intentState string
	var txnID *uuid.UUID
	var datasetVersionID *uuid.UUID
	var rowsWritten, bytesWritten, filesWritten int64
	require.NoError(t, r.Pool.QueryRow(ctx, `
		SELECT r.status, i.recovery_state, i.txn_id, r.dataset_version_id,
		       r.rows_written, r.bytes_written, r.files_written
		  FROM sync_runs r JOIN sync_run_publish_intents i ON i.run_id = r.id
		 WHERE r.id = $1`, runID).
		Scan(&status, &intentState, &txnID, &datasetVersionID, &rowsWritten, &bytesWritten, &filesWritten))
	assert.Equal(t, "succeeded", status)
	assert.Equal(t, IntentStateFinalized, intentState)
	require.NotNil(t, txnID)
	assert.Equal(t, receipt.TransactionID, *txnID)
	require.NotNil(t, datasetVersionID, "the run records the committed transaction as its dataset version")
	assert.Equal(t, receipt.TransactionID, *datasetVersionID)
	assert.Equal(t, intent.Rows, rowsWritten)
	assert.Equal(t, intent.Bytes, bytesWritten)
	assert.Equal(t, int64(1), filesWritten)
	var contentHash string
	require.NoError(t, r.Pool.QueryRow(ctx,
		`SELECT content_hash FROM sync_runs WHERE id = $1`, runID).Scan(&contentHash))
	assert.Equal(t, intent.ContentHash, contentHash)

	if incremental {
		var epoch int
		var owner *uuid.UUID
		var checkpointValue *string
		require.NoError(t, r.Pool.QueryRow(ctx, `
			SELECT confirmed_epoch, active_run_id, checkpoint_value
			  FROM sync_def_checkpoints WHERE sync_def_id = $1`,
			defID).Scan(&epoch, &owner, &checkpointValue))
		assert.Equal(t, 1, epoch, "exactly one epoch advance")
		assert.Nil(t, owner, "owner released atomically")
		require.NotNil(t, checkpointValue, "the advanced checkpoint stores the intent's candidate")
		require.NotNil(t, intent.CompletedCheckpoint)
		assert.Equal(t, *intent.CompletedCheckpoint, *checkpointValue)
	} else {
		var cpRows int
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM sync_def_checkpoints WHERE sync_def_id = $1`, defID).Scan(&cpRows))
		assert.Zero(t, cpRows, "snapshot mode never creates checkpoint rows")
	}

	// Duplicate finalize + claim on the NORMAL connection: already_resolved,
	// zero writes — no lease generation or budget churn.
	runBefore := runRowJSON(t, r.Pool, runID)
	intentBefore := p26IntentJSON(t, r.Pool, runID)
	var cpBefore string
	if incremental {
		cpBefore = p25CpJSON(t, r.Pool, defID)
	}
	dup, derr := r.FinalizeRecoveredRun(ctx, recClaim, tenant, receipt)
	require.NoError(t, derr)
	require.NotNil(t, dup)
	assert.Equal(t, RecoveryFinalizeAlreadyResolved, dup.Outcome)
	claimRes, cerr := r.ClaimRecovery(ctx, runID, tenant, "dup", "dup-tok", time.Minute)
	require.NoError(t, cerr)
	require.NotNil(t, claimRes)
	assert.Equal(t, RecoveryClaimAlreadyResolved, claimRes.Outcome)
	assert.Nil(t, claimRes.Claim)
	assert.Equal(t, runBefore, runRowJSON(t, r.Pool, runID), "duplicate: run untouched")
	assert.Equal(t, intentBefore, p26IntentJSON(t, r.Pool, runID), "duplicate: intent (budgets included) untouched")
	if incremental {
		assert.Equal(t, cpBefore, p25CpJSON(t, r.Pool, defID), "duplicate: checkpoint untouched")
	} else {
		var cpAfter int
		require.NoError(t, r.Pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM sync_def_checkpoints WHERE sync_def_id = $1`, defID).Scan(&cpAfter))
		assert.Zero(t, cpAfter, "snapshot duplicate: still zero checkpoint rows")
	}
}

// TestPGP26FinalizeWireCommitAckLossOnce is the W2 required name: the real
// FinalizeRecoveredRun commit reaches PostgreSQL and loses ONLY its ACK on
// the wire; the run converges server-side exactly once and every duplicate
// path is idempotent with zero writes. Local-PG ACK-uncertainty proof — the
// remote Dataset commit is W7's separate case.
func TestPGP26FinalizeWireCommitAckLossOnce(t *testing.T) {
	t.Run("incremental", func(t *testing.T) { p26AckLossRunCase(t, true) })
	t.Run("snapshot", func(t *testing.T) { p26AckLossRunCase(t, false) })
}
