// P2.6 W1b — pure wire-fault mechanics for the COMMIT ACK-loss proof.
//
// This file is the W1b-owned private helper half of
// output_commit_ackloss_integration_test.go: the PostgreSQL frontend
// frame parser and the net.Conn wrapper that suppresses the server's
// acknowledgement of the outer COMMIT. It has NO build tag and touches no
// database, so the mechanics are proven by the TestW1b* tests below in
// every plain `go test ./internal/repo/` run (and under -race), while the
// full real-PostgreSQL wire proof
// (TestPGP26DatasetWireCommitAckLossReplaysReceipt) stays behind the
// integration tag for the W6 execution pass.
//
// Wire contract this helper implements (verified against pgx v5.7.4):
// pgx's Tx.Commit calls the no-arg Exec("commit"), and a no-arg Exec uses
// the simple query protocol — exactly one 'Q' frontend frame whose payload
// is "commit\x00". The wrapper arms ACK suppression the moment that exact
// frame is reassembled, BEFORE forwarding the frame, so the server still
// receives and executes the COMMIT; everything the server sends back is
// then held back until the test (after independent proof that the COMMIT
// really landed) closes the socket and the caller sees EOF.
package repo

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// PostgreSQL special pre-startup protocol codes (see PG16 protocol docs).
const (
	w1bSSLRequestCode    = 80877103
	w1bGSSENCRequestCode = 80877104
	w1bCancelRequestCode = 80877102
)

// w1bMaxParserBuf bounds the parser's reassembly buffer. The commit proof
// only ever sees small test statements; a stream that would buffer more
// than this is flushed and passed through un-parsed (the test then fails
// honestly via commitFrameSeen=false) — memory stays bounded either way.
const w1bMaxParserBuf = 1 << 20

// w1bCommitQueryPayload is the exact simple-query payload of pgx v5.7.4's
// Tx.Commit. Byte-for-byte equality — never a substring match.
const w1bCommitQueryPayload = "commit\x00"

// w1bFrame is one complete frontend protocol frame handed to the
// forwarding layer. raw holds the exact wire bytes (type/length prefix
// included, or the full pre-startup message); isCommit marks the one
// exact 'Q' "commit\0" frame.
type w1bFrame struct {
	raw      []byte
	isCommit bool
}

// w1bFrameParser reassembles PostgreSQL frontend protocol frames from an
// arbitrary byte stream: Write fragments do NOT align with frame
// boundaries and one Write may carry several frames. It understands the
// connection preamble (SSLRequest/GSSENCRequest 8-byte messages,
// CancelRequest 16-byte messages, the typeless startup message) and then
// switches to the regular type(1)+length(4) framing.
type w1bFrameParser struct {
	buf        []byte
	sawStartup bool
	desynced   bool
}

// feed absorbs one Write chunk and returns every frame it completed.
// Bytes belonging to a still-incomplete frame stay buffered (they are NOT
// forwarded yet — that is what lets the wrapper arm suppression before
// the server sees the last byte of the COMMIT frame).
func (p *w1bFrameParser) feed(chunk []byte) []w1bFrame {
	if p.desynced {
		// Past the bound or malformed: pass through untouched, no commit
		// detection is possible any more.
		return []w1bFrame{{raw: append([]byte(nil), chunk...)}}
	}
	p.buf = append(p.buf, chunk...)
	if len(p.buf) > w1bMaxParserBuf {
		flushed := p.buf
		p.buf = nil
		p.desynced = true
		return []w1bFrame{{raw: flushed}}
	}
	var frames []w1bFrame
	for {
		if !p.sawStartup {
			if len(p.buf) < 4 {
				break
			}
			length := int(binary.BigEndian.Uint32(p.buf[0:4]))
			if length < 8 || length > w1bMaxParserBuf {
				p.desync(len(p.buf), &frames)
				return frames
			}
			if length == 8 || length == 16 {
				if len(p.buf) < length {
					break
				}
				code := binary.BigEndian.Uint32(p.buf[4:8])
				if (length == 8 && (code == w1bSSLRequestCode || code == w1bGSSENCRequestCode)) ||
					(length == 16 && code == w1bCancelRequestCode) {
					// Special pre-startup message: forwarded as-is, the
					// connection stays in the preamble phase (a cancel
					// connection sends nothing else at all).
					frames = append(frames, w1bFrame{raw: w1bCopyTake(p.buf, length)})
					p.buf = p.buf[length:]
					continue
				}
			}
			if len(p.buf) < length {
				break
			}
			// Startup message (typeless): protocol version + parameters.
			frames = append(frames, w1bFrame{raw: w1bCopyTake(p.buf, length)})
			p.buf = p.buf[length:]
			p.sawStartup = true
			continue
		}
		if len(p.buf) < 5 {
			break
		}
		typ := p.buf[0]
		length := int(binary.BigEndian.Uint32(p.buf[1:5]))
		if length < 4 || 1+length > w1bMaxParserBuf {
			p.desync(len(p.buf), &frames)
			return frames
		}
		frameLen := 1 + length
		if len(p.buf) < frameLen {
			break
		}
		isCommit := typ == 'Q' && string(p.buf[5:frameLen]) == w1bCommitQueryPayload
		frames = append(frames, w1bFrame{raw: w1bCopyTake(p.buf, frameLen), isCommit: isCommit})
		p.buf = p.buf[frameLen:]
	}
	return frames
}

// desync flushes everything buffered as one pass-through frame.
func (p *w1bFrameParser) desync(n int, frames *[]w1bFrame) {
	*frames = append(*frames, w1bFrame{raw: w1bCopyTake(p.buf, n)})
	p.buf = nil
	p.desynced = true
}

// w1bCopyTake returns a copy of the first n bytes of buf.
func w1bCopyTake(buf []byte, n int) []byte {
	return append([]byte(nil), buf[:n]...)
}

// w1bFaultState is shared by every fault connection of one fault pool
// (MaxConns=1, so exactly one carries the transaction). It tracks the
// wire evidence the test asserts: the exact COMMIT frame was seen, ACK
// suppression was armed, and EOF was finally delivered to the caller.
type w1bFaultState struct {
	mu              sync.Mutex
	conns           []*w1bFaultConn
	commitFrameSeen bool
	suppress        bool
	eofDelivered    bool
	commitSeenCh    chan struct{}
	commitOnce      sync.Once
}

func newW1bFaultState() *w1bFaultState {
	return &w1bFaultState{commitSeenCh: make(chan struct{})}
}

// wrap registers a newly dialed raw connection with this state.
func (s *w1bFaultState) wrap(raw net.Conn) *w1bFaultConn {
	c := &w1bFaultConn{Conn: raw, state: s, eofCh: make(chan struct{})}
	s.mu.Lock()
	s.conns = append(s.conns, c)
	s.mu.Unlock()
	return c
}

// armSuppress flips the suppression latch and publishes commitFrameSeen.
// Called by the wrapper BEFORE forwarding the complete COMMIT frame.
func (s *w1bFaultState) armSuppress() {
	s.mu.Lock()
	s.commitFrameSeen = true
	s.suppress = true
	s.mu.Unlock()
	s.commitOnce.Do(func() { close(s.commitSeenCh) })
}

func (s *w1bFaultState) suppressActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.suppress
}

// waitCommitFrameSeen blocks until the exact COMMIT frame is detected or
// the bound expires; it never waits forever.
func (s *w1bFaultState) waitCommitFrameSeen(d time.Duration) bool {
	select {
	case <-s.commitSeenCh:
		return true
	case <-time.After(d):
		return false
	}
}

// connCount reports how many sockets the fault pool has dialed through
// the wrapped DialFunc. The PID proof assumes exactly one.
func (s *w1bFaultState) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

func (s *w1bFaultState) markEOFDelivered() {
	s.mu.Lock()
	s.eofDelivered = true
	s.mu.Unlock()
}

// flags returns the three wire-evidence bits the test asserts together.
func (s *w1bFaultState) flags() (commitFrameSeen, suppressActivated, eofDelivered bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitFrameSeen, s.suppress, s.eofDelivered
}

// CloseAll cuts every tracked fault socket. Idempotent; unblocks any
// caller parked in the wrapper's Read.
func (s *w1bFaultState) CloseAll() {
	s.mu.Lock()
	conns := append([]*w1bFaultConn(nil), s.conns...)
	s.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

// w1bFaultConn wraps the raw TCP connection of the fault pool. Write
// reassembles frontend frames and forwards only complete frames; the
// moment the exact simple-query "commit\0" frame is complete it arms ACK
// suppression BEFORE those bytes reach the server. Read holds back every
// byte once suppression is armed — checked both before entering the
// underlying read and again after the read returns, before any byte is
// exposed — and parks the caller until the test closes the socket.
type w1bFaultConn struct {
	net.Conn  // underlying socket; address/deadline methods pass through
	state     *w1bFaultState
	parser    w1bFrameParser
	writeMu   sync.Mutex
	eofCh     chan struct{}
	closeOnce sync.Once
}

func (c *w1bFaultConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	for _, f := range c.parser.feed(p) {
		if f.isCommit {
			// Suppress FIRST, forward second: the server must still
			// receive and execute the COMMIT request.
			c.state.armSuppress()
		}
		if _, err := c.Conn.Write(f.raw); err != nil {
			return len(p), err
		}
	}
	return len(p), nil
}

func (c *w1bFaultConn) Read(p []byte) (int, error) {
	// Pre-check: hold back before even touching the socket.
	if c.state.suppressActive() {
		return c.awaitEOF()
	}
	n, err := c.Conn.Read(p)
	// Post-check: recheck UNCONDITIONALLY after the underlying read —
	// net.Conn allows n>0 together with a non-nil error (including EOF),
	// so whenever suppression armed while the read was in flight, BOTH
	// the returned bytes and the error are discarded and the caller parks
	// until the validated notification closes the socket (review round 1
	// blocker 2).
	if c.state.suppressActive() {
		return c.awaitEOF()
	}
	return n, err
}

// awaitEOF parks until Close and then surfaces EOF to the pgx reader.
func (c *w1bFaultConn) awaitEOF() (int, error) {
	<-c.eofCh
	c.state.markEOFDelivered()
	return 0, io.EOF
}

func (c *w1bFaultConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.eofCh)
		c.Conn.Close()
	})
	return nil
}

// --- pure test helpers -------------------------------------------------

// w1bStartupFrame builds the typeless frontend startup message with the
// given "key\0value\0" parameter pairs and the final terminator.
func w1bStartupFrame(params ...string) []byte {
	body := binary.BigEndian.AppendUint32(nil, 196608) // protocol 3.0
	for _, p := range params {
		body = append(body, p...)
		body = append(body, 0)
	}
	body = append(body, 0)
	out := binary.BigEndian.AppendUint32(nil, uint32(len(body)+4))
	return append(out, body...)
}

// w1bTypedFrame builds a regular type(1)+length(4) frontend frame; for a
// simple query the payload is the SQL string including its trailing NUL.
func w1bTypedFrame(typ byte, payload string) []byte {
	out := []byte{typ}
	out = binary.BigEndian.AppendUint32(out, uint32(len(payload)+4))
	return append(out, payload...)
}

// w1bSpecialFrame builds the 8-byte SSLRequest/GSSENCRequest message.
func w1bSpecialFrame(code uint32) []byte {
	out := binary.BigEndian.AppendUint32(nil, 8)
	return binary.BigEndian.AppendUint32(out, code)
}

// w1bCancelFrame builds the 16-byte CancelRequest message.
func w1bCancelFrame(pid, secret uint32) []byte {
	out := binary.BigEndian.AppendUint32(nil, 16)
	out = binary.BigEndian.AppendUint32(out, w1bCancelRequestCode)
	out = binary.BigEndian.AppendUint32(out, pid)
	return binary.BigEndian.AppendUint32(out, secret)
}

func w1bConcat(parts ...[]byte) []byte {
	var all []byte
	for _, p := range parts {
		all = append(all, p...)
	}
	return all
}

// w1bScriptRead is one scripted underlying Read result: bytes and an
// optional error returned TOGETHER (net.Conn allows n>0 with err != nil).
type w1bScriptRead struct {
	b   []byte
	err error
}

// w1bScriptConn is a deterministic in-memory net.Conn for the pure
// mechanics tests: every Write is recorded verbatim, Read returns only
// results explicitly pushed by the test (blocking otherwise, signalling
// entry on readEntered), and Close unblocks a pending Read.
type w1bScriptConn struct {
	mu          sync.Mutex
	writes      []byte
	reads       chan w1bScriptRead
	readEntered chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once
}

func newW1bScriptConn() *w1bScriptConn {
	return &w1bScriptConn{
		reads:       make(chan w1bScriptRead, 8),
		readEntered: make(chan struct{}, 64),
		closed:      make(chan struct{}),
	}
}

func (f *w1bScriptConn) Write(p []byte) (int, error) {
	select {
	case <-f.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	f.mu.Lock()
	f.writes = append(f.writes, p...)
	f.mu.Unlock()
	return len(p), nil
}

func (f *w1bScriptConn) Read(p []byte) (int, error) {
	select {
	case f.readEntered <- struct{}{}:
	default:
	}
	select {
	case r := <-f.reads:
		return copy(p, r.b), r.err
	case <-f.closed:
		return 0, io.EOF
	}
}

func (f *w1bScriptConn) Close() error {
	f.closeOnce.Do(func() { close(f.closed) })
	return nil
}

func (f *w1bScriptConn) forwarded() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.writes...)
}

func (f *w1bScriptConn) LocalAddr() net.Addr              { return w1bAddr("script-local") }
func (f *w1bScriptConn) RemoteAddr() net.Addr             { return w1bAddr("script-remote") }
func (f *w1bScriptConn) SetDeadline(time.Time) error      { return nil }
func (f *w1bScriptConn) SetReadDeadline(time.Time) error  { return nil }
func (f *w1bScriptConn) SetWriteDeadline(time.Time) error { return nil }

type w1bAddr string

func (a w1bAddr) Network() string { return "w1b" }
func (a w1bAddr) String() string  { return string(a) }

// w1bWaitEntered blocks until the script conn reports a Read entry.
func w1bWaitEntered(t *testing.T, f *w1bScriptConn, what string) {
	t.Helper()
	select {
	case <-f.readEntered:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestW1bCommitFrameDetectedAcrossFragmentedWrites feeds the full client
// stream — startup, an ordinary simple query, the exact `commit\0` simple
// query and a trailing PARTIAL frame — one byte per Write. The exact
// COMMIT frame must arm suppression at precisely the byte that completes
// it (before the trailing partial frame is forwarded anywhere), every
// byte must be forwarded exactly once in order, and the trailing partial
// frame must still be withheld.
func TestW1bCommitFrameDetectedAcrossFragmentedWrites(t *testing.T) {
	stream := w1bConcat(
		w1bStartupFrame("user\x00w1b"),
		w1bTypedFrame('Q', "SET TRANSACTION ISOLATION LEVEL READ COMMITTED\x00"),
		w1bTypedFrame('Q', w1bCommitQueryPayload),
		[]byte{'Q', 0, 0, 0, 9, 's', 'e'}, // trailing partial frame
	)
	commitEnd := len(stream) - 7 // end offset of the complete commit frame

	state := newW1bFaultState()
	raw := newW1bScriptConn()
	conn := state.wrap(raw)

	armedAt := -1
	for i := 0; i < len(stream); i++ {
		if _, err := conn.Write(stream[i : i+1]); err != nil {
			t.Fatalf("write byte %d: %v", i, err)
		}
		if state.suppressActive() && armedAt < 0 {
			armedAt = i + 1 // bytes fed so far
		}
	}
	if armedAt != commitEnd {
		t.Fatalf("suppress armed after %d bytes, want exactly at the commit frame end %d", armedAt, commitEnd)
	}
	seen, suppress, _ := state.flags()
	if !seen || !suppress {
		t.Fatalf("wire evidence bits: commitFrameSeen=%v suppressActivated=%v", seen, suppress)
	}
	forwarded := raw.forwarded()
	if got, want := len(forwarded), commitEnd; got != want {
		t.Fatalf("forwarded %d bytes, want %d (complete frames only; trailing partial withheld)", got, want)
	}
	if string(forwarded) != string(stream[:commitEnd]) {
		t.Fatal("forwarded bytes differ from the stream prefix — loss or reorder")
	}
}

// TestW1bNonExactCommitPayloadsNeverArmSuppress proves the trigger is an
// exact frame match, not a substring: uppercase COMMIT, trailing
// whitespace/semicolon, the word commit inside longer SQL, and an
// extended-protocol Parse frame carrying the query text "commit" all pass
// through without arming suppression.
func TestW1bNonExactCommitPayloadsNeverArmSuppress(t *testing.T) {
	stream := w1bConcat(
		w1bStartupFrame("user\x00w1b"),
		w1bTypedFrame('Q', "COMMIT\x00"),
		w1bTypedFrame('Q', "commit;\x00"),
		w1bTypedFrame('Q', "commit \x00"),
		w1bTypedFrame('Q', " commit\x00"),
		w1bTypedFrame('Q', "SET TRANSACTION ISOLATION LEVEL READ COMMITTED\x00"),
		w1bTypedFrame('Q', "select 'commit' as word\x00"),
		w1bTypedFrame('Q', "release savepoint tx_1\x00"),
		// Extended protocol Parse: query text "commit", parameter count 0 —
		// same bytes, different frame type.
		w1bTypedFrame('P', "commit\x00\x00\x00\x00\x00"),
		w1bTypedFrame('Q', "rollback\x00"),
	)
	state := newW1bFaultState()
	raw := newW1bScriptConn()
	conn := state.wrap(raw)
	// Feed in awkward 3-byte fragments (frames split and merged).
	for i := 0; i < len(stream); i += 3 {
		end := min(i+3, len(stream))
		if _, err := conn.Write(stream[i:end]); err != nil {
			t.Fatalf("write fragment: %v", err)
		}
	}
	if seen, suppress, _ := state.flags(); seen || suppress {
		t.Fatalf("non-exact payloads must never arm suppression: commitFrameSeen=%v suppressActivated=%v", seen, suppress)
	}
	if forwarded := raw.forwarded(); string(forwarded) != string(stream) {
		t.Fatal("all bytes must still be forwarded exactly once")
	}
}

// TestW1bReadPreCheckHoldsBackUntilClose arms suppression before any read
// and proves the wrapper's Read parks (pre-check) instead of touching the
// socket, surfacing EOF only after Close, with eofDelivered recorded.
func TestW1bReadPreCheckHoldsBackUntilClose(t *testing.T) {
	state := newW1bFaultState()
	raw := newW1bScriptConn()
	conn := state.wrap(raw)
	state.armSuppress()

	type readResult struct {
		n   int
		err error
	}
	res := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 64)
		n, err := conn.Read(buf)
		res <- readResult{n, err}
	}()
	select {
	case r := <-res:
		t.Fatalf("Read must park while suppressed, returned (%d, %v)", r.n, r.err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case r := <-res:
		if r.err != io.EOF || r.n != 0 {
			t.Fatalf("after Close the caller must see EOF, got (%d, %v)", r.n, r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read never returned after Close")
	}
	if _, _, eof := state.flags(); !eof {
		t.Fatal("eofDelivered must be recorded once EOF is returned")
	}
}

// TestW1bReadPostCheckSwallowsBytesAfterSuppressArms proves the second
// Read check: a read already blocked in the underlying socket when
// suppression arms must NOT expose the bytes it returns — they are
// swallowed and the caller parks until Close delivers EOF.
func TestW1bReadPostCheckSwallowsBytesAfterSuppressArms(t *testing.T) {
	state := newW1bFaultState()
	raw := newW1bScriptConn()
	conn := state.wrap(raw)

	type readResult struct {
		n   int
		err error
	}
	res := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 64)
		n, err := conn.Read(buf)
		res <- readResult{n, err}
	}()
	w1bWaitEntered(t, raw, "underlying Read entry")
	state.armSuppress()
	// Server "acknowledges": CommandComplete/ReadyForQuery bytes arrive
	// from the socket after suppression is armed.
	raw.reads <- w1bScriptRead{b: []byte("C\x00\x00\x00\x0bCOMMIT\x00")}
	select {
	case r := <-res:
		t.Fatalf("post-check missing: Read exposed %d ACK bytes", r.n)
	case <-time.After(150 * time.Millisecond):
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case r := <-res:
		if r.err != io.EOF || r.n != 0 {
			t.Fatalf("after Close the caller must see EOF, got (%d, %v)", r.n, r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read never returned after Close")
	}
}

// TestW1bReadPostCheckSwallowsBytesWithEOFAfterSuppressArms is the review
// round-1 counter-example: net.Conn allows one underlying Read to return
// ACK bytes AND a non-nil error (here io.EOF) after suppression armed —
// both must be discarded, the caller parks until Close and only then sees
// a clean EOF.
func TestW1bReadPostCheckSwallowsBytesWithEOFAfterSuppressArms(t *testing.T) {
	state := newW1bFaultState()
	raw := newW1bScriptConn()
	conn := state.wrap(raw)

	type readResult struct {
		n   int
		err error
	}
	res := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 64)
		n, err := conn.Read(buf)
		res <- readResult{n, err}
	}()
	w1bWaitEntered(t, raw, "underlying Read entry")
	state.armSuppress()
	// The socket returns ACK bytes and EOF in the SAME read.
	raw.reads <- w1bScriptRead{b: []byte("Z\x00\x00\x00\x05I"), err: io.EOF}
	select {
	case r := <-res:
		t.Fatalf("post-check missing: Read exposed (%d, %v) despite suppression", r.n, r.err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case r := <-res:
		if r.err != io.EOF || r.n != 0 {
			t.Fatalf("after Close the caller must see a clean EOF, got (%d, %v)", r.n, r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read never returned after Close")
	}
	if _, _, eof := state.flags(); !eof {
		t.Fatal("eofDelivered must be recorded once EOF is returned")
	}
}

// TestW1bStartupSSLAndCancelFramesPassthrough proves the parser survives
// the connection preamble: an SSLRequest followed by startup keeps the
// regular framing in sync (a later exact commit frame still triggers),
// and a CancelRequest-only connection (what pgconn dials for query
// cancellation) is forwarded untouched without ever matching.
func TestW1bStartupSSLAndCancelFramesPassthrough(t *testing.T) {
	stream := w1bConcat(
		w1bSpecialFrame(w1bSSLRequestCode),
		w1bStartupFrame("user\x00w1b"),
		w1bTypedFrame('Q', w1bCommitQueryPayload),
	)
	state := newW1bFaultState()
	raw := newW1bScriptConn()
	conn := state.wrap(raw)
	for i := 0; i < len(stream); i += 5 {
		end := min(i+5, len(stream))
		if _, err := conn.Write(stream[i:end]); err != nil {
			t.Fatalf("write fragment: %v", err)
		}
	}
	if seen, suppress, _ := state.flags(); !seen || !suppress {
		t.Fatalf("commit frame after SSLRequest+startup must arm: commitFrameSeen=%v suppressActivated=%v", seen, suppress)
	}
	if forwarded := raw.forwarded(); string(forwarded) != string(stream) {
		t.Fatal("SSLRequest+startup+commit stream must be forwarded verbatim")
	}

	// A separate cancel-request connection: exactly 16 bytes, no startup,
	// no commit frame, everything forwarded.
	cancelState := newW1bFaultState()
	cancelRaw := newW1bScriptConn()
	cancelConn := cancelState.wrap(cancelRaw)
	frame := w1bCancelFrame(4242, 77)
	if _, err := cancelConn.Write(frame); err != nil {
		t.Fatalf("cancel write: %v", err)
	}
	if seen, suppress, _ := cancelState.flags(); seen || suppress {
		t.Fatalf("CancelRequest must never arm: commitFrameSeen=%v suppressActivated=%v", seen, suppress)
	}
	if got := cancelRaw.forwarded(); string(got) != string(frame) {
		t.Fatal("CancelRequest must be forwarded verbatim")
	}
}
