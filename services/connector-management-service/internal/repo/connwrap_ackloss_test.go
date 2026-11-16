// P2.6 wire-level COMMIT ACK-loss mechanics (test-only, package-private).
//
// The wrapper implements the root-verified wire contract
// (wire-commit-ackloss-tests.md + review-wire-round1 W2 fixes): a net.Conn
// shim installed through pgxpool.ConnConfig.DialFunc that
//
//   - passes everything through UNARMED — PG startup/SSLRequest frames have
//     no type byte, so frame parsing may only start after the caller
//     explicitly Arms the parser (setup/PID warm/LISTEN/trigger completed);
//   - once armed, parses OUTGOING PostgreSQL protocol frames (type byte +
//     int32 length, then payload) across ARBITRARY Write fragmentation —
//     never substring matching, never one-Write-one-frame — with a strict
//     accepted-buffer contract: Write returns len(p) when the bytes are
//     stored, forwards every COMPLETE frame with write-all semantics, and
//     never returns n > len(p) or an empty (0, nil) while a handshake awaits;
//   - arms the ACK suppression flag BEFORE forwarding the complete frame of
//     the exact simple-Query payload `commit\0` (pgx v5.7.4 Tx.Commit) so
//     the server DOES receive and execute the commit;
//   - on Read, checks the suppress flag BEFORE the underlying read AND again
//     after it returns (discarding both n and err) — once suppressed, no ACK
//     byte is ever exposed; the blocked read resolves to EOF only after the
//     independent validated notification releases the gate (Close aborts,
//     bounded).
//
// This file is pure mechanics: fakeConn-based tests below prove passthrough,
// frame parsing under fragmentation with the n-contract, exact-payload-only
// triggering, both Read checks (with the Read-entered-barrier scheduling
// fixed), and bounded cancellation. No database involved.
package repo

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// ackLossMaxFrame bounds the parser buffer for the small test statements.
const ackLossMaxFrame = 1 << 20

// ackLossWire is the suppressing net.Conn shim.
type ackLossWire struct {
	net.Conn
	mu sync.Mutex
	// armed: frame parsing active. False (pure passthrough) from dial until
	// the caller completes setup and calls ArmFrameParser — PG startup has
	// no type byte, and an unarmed parser would deadlock the handshake.
	armed bool
	// suppress: armed exactly when the complete `commit\0` simple-Query
	// frame has been parsed; from then on no ACK byte is exposed.
	suppress bool
	// commitFrameSeen: the exact commit frame was observed and forwarded.
	commitFrameSeen bool
	// eofDelivered: the caller-side Read got the terminal EOF.
	eofDelivered bool
	// wbuf: outgoing frame accumulator (fragmented Writes), armed only.
	wbuf []byte
	// release: closed by the notifier once the independent LISTEN proof
	// validates; unblocks suppressed Reads into EOF.
	release chan struct{}
	// abort: closed by Close to bound every blocked Read.
	abort chan struct{}
	closeOnce sync.Once
}

func newAckLossWire(inner net.Conn) *ackLossWire {
	return &ackLossWire{
		Conn:    inner,
		release: make(chan struct{}),
		abort:   make(chan struct{}),
	}
}

// ArmFrameParser switches the wrapper from pure passthrough to protocol
// frame parsing (setup, PID warm, LISTEN and the trigger are complete; the
// only typed traffic from here on is the session's statements).
func (w *ackLossWire) ArmFrameParser() {
	w.mu.Lock()
	w.armed = true
	w.wbuf = nil
	w.mu.Unlock()
}

// ackLossReleased closes the release gate (the validated notification).
func (w *ackLossWire) ackLossReleased() { w.closeChan(w.release) }

func (w *ackLossWire) closeChan(ch chan struct{}) {
	w.mu.Lock()
	select {
	case <-ch:
	default:
		close(ch)
	}
	w.mu.Unlock()
}

// flags snapshots the proof flags.
func (w *ackLossWire) flags() (commitFrameSeen, suppressActivated, eofDelivered bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.commitFrameSeen, w.suppress, w.eofDelivered
}

// writeAll forwards with short-write/error handling (net.Conn contract).
func (w *ackLossWire) writeAll(b []byte) error {
	for len(b) > 0 {
		n, err := w.Conn.Write(b)
		if n > 0 {
			b = b[n:]
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Write honors the accepted-buffer contract: it returns len(p) once the
// bytes are stored (passthrough or armed buffer) — never more than len(p),
// never an empty (0, nil) while the caller awaits completion. In armed mode
// only COMPLETE frames are forwarded (write-all); the suppress flag is set
// BEFORE the exact commit frame is forwarded — the server must receive the
// request; only the ACK side is lost.
func (w *ackLossWire) Write(p []byte) (int, error) {
	w.mu.Lock()
	armed := w.armed
	if !armed {
		w.mu.Unlock()
		if err := w.writeAll(p); err != nil {
			return len(p), err // bytes were handed to the socket contract-wise
		}
		return len(p), nil
	}
	if len(w.wbuf)+len(p) > ackLossMaxFrame {
		w.mu.Unlock()
		return 0, errors.New("ackloss: parser buffer bound exceeded")
	}
	w.wbuf = append(w.wbuf, p...)
	buf := w.wbuf
	forward := 0
	var fwd []byte
	for len(buf)-forward >= 5 {
		total := 1 + int(binary.BigEndian.Uint32(buf[forward+1:forward+5]))
		if total <= 4 || total > ackLossMaxFrame || len(buf)-forward < total {
			break // incomplete frame — wait for more fragments
		}
		frame := buf[forward : forward+total]
		if frame[0] == 'Q' && string(frame[5:]) == "commit\x00" {
			// Arm BEFORE forwarding: the server must still receive it.
			// (The caller's lock is ALREADY held here — set fields directly.)
			w.commitFrameSeen = true
			w.suppress = true
		}
		fwd = append(fwd, frame...)
		forward += total
	}
	w.wbuf = append([]byte(nil), buf[forward:]...)
	w.mu.Unlock()
	if len(fwd) > 0 {
		if err := w.writeAll(fwd); err != nil {
			return len(p), err // accepted; forward error surfaced honestly
		}
	}
	return len(p), nil
}

// Read implements BOTH suppress checks: before touching the underlying
// connection, and — unconditionally — after the underlying read returns,
// re-reading the CURRENT armed/suppress state (a Read that started UNARMED
// at warm-up may race ArmFrameParser + commit suppression while parked on
// the bottom connection; the entry snapshot must never decide the outcome).
// Bytes AND error observed while suppression is armed are discarded, never
// exposed; the terminal EOF is delivered only on release (validated
// notification) or Close (bounded abort).
func (w *ackLossWire) Read(p []byte) (int, error) {
	w.mu.Lock()
	supp := w.armed && w.suppress
	rel, abt := w.release, w.abort
	w.mu.Unlock()
	if supp {
		return w.holdEOF(rel, abt) // pre-check: never touch underlying bytes
	}
	n, err := w.Conn.Read(p)
	w.mu.Lock()
	supp = w.armed && w.suppress // ALWAYS re-read CURRENT state
	w.mu.Unlock()
	if !supp {
		return n, err
	}
	// AFTER-check: n>0 AND/OR err != nil raced with arming — discard both.
	return w.holdEOF(rel, abt)
}

// holdEOF resolves a suppressed Read: EOF on release, closed error on abort.
func (w *ackLossWire) holdEOF(rel, abt <-chan struct{}) (int, error) {
	if w.holdBack(rel, abt) {
		w.markEOFDelivered()
		return 0, io.EOF
	}
	w.markEOFDelivered()
	return 0, net.ErrClosed
}

// holdBack waits for release or abort; true = released (EOF), false =
// aborted.
func (w *ackLossWire) holdBack(rel, abt <-chan struct{}) bool {
	select {
	case <-rel:
		return true
	case <-abt:
		return false
	}
}

func (w *ackLossWire) markEOFDelivered() {
	w.mu.Lock()
	w.eofDelivered = true
	w.mu.Unlock()
}

// Close bounds every blocked Read (cleanup/watchdog path) and closes the
// socket. Idempotent.
func (w *ackLossWire) Close() error {
	w.closeOnce.Do(func() { w.closeChan(w.abort) })
	return w.Conn.Close()
}

// ---------------------------------------------------------------------------
// fakeConn: deterministic in-memory net.Conn for the pure mechanics tests.
// ---------------------------------------------------------------------------

// fakeConn feeds its Reads from a pushable buffer and records Writes.
type fakeConn struct {
	mu        sync.Mutex
	readBuf   []byte
	readErr   error
	written   []byte
	closed    bool
	readBlock chan struct{}
	parked    chan struct{} // closed once a Read has parked on an empty buffer
	parkedOnce sync.Once
}

func newFakeConn() *fakeConn {
	return &fakeConn{readBlock: make(chan struct{}), parked: make(chan struct{})}
}

func (f *fakeConn) push(b []byte) {
	f.mu.Lock()
	f.readBuf = append(f.readBuf, b...)
	f.mu.Unlock()
	select {
	case <-f.readBlock:
	default:
		close(f.readBlock)
	}
}

func (f *fakeConn) pushErr(err error) { f.mu.Lock(); f.readErr = err; f.mu.Unlock() }

// waitParked blocks until a Read call has actually parked on the empty
// buffer (the scheduling barrier the after-check test needs).
func (f *fakeConn) waitParked(t *testing.T) {
	t.Helper()
	select {
	case <-f.parked:
	case <-time.After(2 * time.Second):
		t.Fatal("no Read ever parked on the fake connection")
	}
}

func (f *fakeConn) Read(p []byte) (int, error) {
	for {
		f.mu.Lock()
		if len(f.readBuf) > 0 {
			n := copy(p, f.readBuf)
			f.readBuf = f.readBuf[n:]
			f.mu.Unlock()
			return n, nil
		}
		err := f.readErr
		f.mu.Unlock()
		if err != nil {
			return 0, err
		}
		f.parkedOnce.Do(func() { close(f.parked) })
		select {
		case <-f.readBlock:
			f.mu.Lock()
			f.readBlock = make(chan struct{})
			f.mu.Unlock()
		case <-time.After(2 * time.Second):
			return 0, errors.New("fakeConn: read timeout")
		}
	}
}

func (f *fakeConn) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written = append(f.written, p...)
	return len(p), nil
}

func (f *fakeConn) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}
func (f *fakeConn) LocalAddr() net.Addr               { return nil }
func (f *fakeConn) RemoteAddr() net.Addr              { return nil }
func (f *fakeConn) SetDeadline(t time.Time) error     { return nil }
func (f *fakeConn) SetReadDeadline(t time.Time) error { return nil }
func (f *fakeConn) SetWriteDeadline(t time.Time) error { return nil }

var _ net.Conn = (*fakeConn)(nil)

// pgFrame builds one protocol frame (type + length + payload).
func pgFrame(typ byte, payload string) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = typ
	binary.BigEndian.PutUint32(out[1:5], uint32(4+len(payload)))
	copy(out[5:], payload)
	return out
}

// ---------------------------------------------------------------------------
// Pure mechanics tests
// ---------------------------------------------------------------------------

// TestAckLossPassthroughUntilArmed proves the pre-arm contract: PG startup
// traffic (no type byte) flows through byte-identical with n == len(p), and
// nothing parses or triggers until ArmFrameParser.
func TestAckLossPassthroughUntilArmed(t *testing.T) {
	inner := newFakeConn()
	w := newAckLossWire(inner)

	startup := []byte{0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f} // length-prefixed, NO type byte
	for _, frag := range [][]byte{startup[:3], startup[3:]} {
		n, err := w.Write(frag)
		if err != nil || n != len(frag) {
			t.Fatalf("passthrough write: n=%d (want %d) err=%v", n, len(frag), err)
		}
	}
	// A commit frame BEFORE arming must pass through untriggered.
	commit := pgFrame('Q', "commit\x00")
	if n, err := w.Write(commit); err != nil || n != len(commit) {
		t.Fatalf("pre-arm commit write: n=%d err=%v", n, err)
	}
	if seen, supp, _ := w.flags(); seen || supp {
		t.Fatal("pre-arm traffic must never arm suppression")
	}
	if string(inner.written) != string(startup)+string(commit) {
		t.Fatal("pre-arm bytes must pass through byte-identical")
	}

	w.ArmFrameParser()
	if n, err := w.Write(pgFrame('Q', "commit\x00")); err != nil || n != 5+7 {
		t.Fatalf("armed commit write: n=%d err=%v", n, err)
	}
	if seen, supp, _ := w.flags(); !seen || !supp {
		t.Fatal("armed exact commit frame must trigger")
	}
}

// TestAckLossFrameParsingAcrossFragmentedWrites proves the parser detects
// the exact `commit\0` simple-Query frame only when its LAST fragment lands
// (no substring matching), honors the accepted-buffer n-contract on every
// fragmented Write, and forwards complete frames byte-identically.
func TestAckLossFrameParsingAcrossFragmentedWrites(t *testing.T) {
	inner := newFakeConn()
	w := newAckLossWire(inner)
	w.ArmFrameParser()

	commit := pgFrame('Q', "commit\x00")
	prefix := pgFrame('Q', "select 1\x00")
	fragments := [][]byte{prefix[:3], prefix[3:], commit[:5], commit[5:6], commit[6:]}
	var sent []byte
	for _, frag := range fragments {
		n, err := w.Write(frag)
		if err != nil {
			t.Fatalf("fragment write: %v", err)
		}
		if n != len(frag) {
			t.Fatalf("n-contract violated: n=%d want len(frag)=%d", n, len(frag))
		}
		sent = append(sent, frag...)
		seen, supp, _ := w.flags()
		wantTrig := string(sent) == string(prefix)+string(commit)
		if seen != wantTrig || supp != wantTrig {
			t.Fatalf("trigger state after fragment: seen=%v supp=%v want=%v", seen, supp, wantTrig)
		}
	}
	// The server received BOTH complete frames, byte-identical — no partial
	// frame ever left the wrapper.
	if string(inner.written) != string(prefix)+string(commit) {
		t.Fatalf("server must receive exactly the complete frames; got %q", inner.written)
	}
}

// TestAckLossOnlyExactCommitPayloadTriggers pins the exact-payload rule.
func TestAckLossOnlyExactCommitPayloadTriggers(t *testing.T) {
	for name, payload := range map[string]string{
		"select containing word": "select 'commit'\x00",
		"commit with space":      "commit \x00",
		"commit transaction":     "commit transaction\x00",
		"begin":                  "begin\x00",
		"case differs":           "COMMIT\x00",
	} {
		inner := newFakeConn()
		w := newAckLossWire(inner)
		w.ArmFrameParser()
		if n, err := w.Write(pgFrame('Q', payload)); err != nil || n != 5+len(payload) {
			t.Fatalf("%s: write n=%d err=%v", name, n, err)
		}
		if seen, supp, _ := w.flags(); seen || supp {
			t.Fatalf("%s (%q) must not trigger", name, payload)
		}
	}
	inner := newFakeConn()
	w := newAckLossWire(inner)
	w.ArmFrameParser()
	if _, err := w.Write(pgFrame('P', "commit\x00")); err != nil {
		t.Fatalf("parse message write: %v", err)
	}
	if seen, supp, _ := w.flags(); seen || supp {
		t.Fatal("only message type Q with payload commit\\0 triggers")
	}
}

// TestAckLossReadChecksBeforeAndAfterUnderlying proves BOTH Read checks with
// the scheduling fixed: the after-check test FIRST proves the Read has
// entered the underlying barrier (parked) — only then are ACK bytes pushed
// and suppression armed, so the read-winner race cannot flip the outcome.
func TestAckLossReadChecksBeforeAndAfterUnderlying(t *testing.T) {
	t.Run("before_check_holds_then_eof_on_release", func(t *testing.T) {
		inner := newFakeConn()
		w := newAckLossWire(inner)
		w.ArmFrameParser()
		inner.push([]byte("server-ack-bytes"))
		w.mu.Lock()
		w.suppress = true
		w.mu.Unlock()

		type readRes struct {
			n   int
			err error
		}
		ch := make(chan readRes, 1)
		go func() {
			var buf [8]byte
			n, err := w.Read(buf[:])
			ch <- readRes{n, err}
		}()
		select {
		case r := <-ch:
			t.Fatalf("suppressed Read must block until release, got n=%d err=%v", r.n, r.err)
		case <-time.After(150 * time.Millisecond):
		}
		w.ackLossReleased()
		r := <-ch
		if r.err != io.EOF || r.n != 0 {
			t.Fatalf("released Read must EOF, got n=%d err=%v", r.n, r.err)
		}
		if _, _, eof := w.flags(); !eof {
			t.Fatal("eofDelivered must be set")
		}
	})

	t.Run("after_check_swallows_inflight_bytes_and_error", func(t *testing.T) {
		inner := newFakeConn()
		w := newAckLossWire(inner)
		w.ArmFrameParser()
		type readRes struct {
			n   int
			err error
		}
		ch := make(chan readRes, 1)
		go func() {
			var buf [16]byte
			n, err := w.Read(buf[:])
			ch <- readRes{n, err}
		}()
		// Scheduling barrier FIRST (Read parked), THEN suppress, THEN the
		// racing bytes/error — the order never gambles on Read scheduling.
		inner.waitParked(t)
		w.mu.Lock()
		w.suppress = true
		w.mu.Unlock()
		inner.push([]byte("zombie-ack"))
		inner.pushErr(io.EOF) // n>0 AND err!=nil must both be discarded
		select {
		case r := <-ch:
			t.Fatalf("in-flight bytes/error must be swallowed, got n=%d err=%v", r.n, r.err)
		case <-time.After(150 * time.Millisecond):
		}
		w.ackLossReleased()
		r := <-ch
		if r.err != io.EOF || r.n != 0 {
			t.Fatalf("released Read must EOF, got n=%d err=%v", r.n, r.err)
		}
	})

	t.Run("unarmed_read_raced_by_arm_and_commit_holds_ack", func(t *testing.T) {
		// The wire-round followup case: a Read that STARTED during the
		// passthrough warm-up (entry snapshot unarmed) parks on the bottom
		// connection; ArmFrameParser + the exact commit frame arm
		// suppression; only then do the ACK bytes arrive. The CURRENT-state
		// post-check must hold them back — the entry snapshot may never
		// decide the outcome.
		inner := newFakeConn()
		w := newAckLossWire(inner)
		type readRes struct {
			n   int
			err error
		}
		ch := make(chan readRes, 1)
		go func() {
			var buf [16]byte
			n, err := w.Read(buf[:]) // starts UNARMED
			ch <- readRes{n, err}
		}()
		inner.waitParked(t)
		w.ArmFrameParser()
		if _, err := w.Write(pgFrame('Q', "commit\x00")); err != nil {
			t.Fatalf("armed commit write: %v", err)
		}
		if seen, supp, _ := w.flags(); !seen || !supp {
			t.Fatal("the exact commit frame must arm suppression")
		}
		inner.push([]byte("commit-ack"))
		select {
		case r := <-ch:
			t.Fatalf("an unarmed-started Read must still be held once suppression armed, got n=%d err=%v", r.n, r.err)
		case <-time.After(150 * time.Millisecond):
		}
		w.ackLossReleased()
		r := <-ch
		if r.err != io.EOF || r.n != 0 {
			t.Fatalf("released Read must EOF, got n=%d err=%v", r.n, r.err)
		}
	})
}

// TestAckLossCloseBoundsBlockedRead proves the cleanup path: Close aborts a
// suppressed Read promptly (bounded cancellation — a lost notification can
// never hang the test/pool shutdown).
func TestAckLossCloseBoundsBlockedRead(t *testing.T) {
	inner := newFakeConn()
	w := newAckLossWire(inner)
	w.ArmFrameParser()
	w.mu.Lock()
	w.suppress = true
	w.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		var buf [4]byte
		_, err := w.Read(buf[:])
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-done:
		if err == nil || err == io.EOF {
			t.Fatalf("abort must surface a closed error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close must bound the blocked Read")
	}
}
