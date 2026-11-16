// REAL-transport disconnect regression (P2.2 verification concern, no
// production changes): the default synchronous POST with an UNREAD request
// body may not propagate a client TCP disconnect to r.Context() on Go's
// HTTP/1 server (per the go1.26.3 source, the connection-level background
// read — the thing that notices a closed socket — only starts once the
// request body has been consumed to EOF; see net/http server.go
// requestBodyRemains/registerOnHitEOF). These tests drive the REAL handler
// over a REAL TCP socket, close only the CLIENT side mid-pull, and assert
// the propagation the sync contract relies on:
//
//   - the parked upstream page's context cancels within a SHORT observation
//     window (2–3s) — not after the 30s task deadline or a 60s source
//     timeout masquerading as a disconnect;
//   - the run converges to a persisted `failed` whose cause is the context
//     cancellation;
//   - no CommitOutput and no second page request ever happen.
//
// The two cases are Content-Length: 0 (expected to propagate immediately)
// and Content-Length: 2 with body `{}` (the suspected gap: the handler never
// reads r.Body). Claims are injected as trusted test middleware; the body is
// never read and the request context is never cancelled manually — only the
// transport may do it.
//
// Every cleanup is pre-registered and idempotent, ordered so a failure still
// converges: coordinator Stop (cancels the service root, which cancels the
// execution through its AfterFunc link) runs FIRST, then the upstream gate
// release, then the server close. No path can wedge Server.Close.
package handlers_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
)

// disconnectEnv is a manual rig (not newAsyncEnv) so the cleanup order can
// follow the failure rule: Stop the coordinator root BEFORE releasing the
// upstream, then close the server (LIFO registration).
type disconnectEnv struct {
	store     *fakeStore
	h         *handlers.Handlers
	coord     *handlers.RunCoordinator
	committer *fakeCommitPort
	upstream  *gatedUpstream
	srv       *httptest.Server
	job       *models.SyncJob
	owner     uuid.UUID
}

func newDisconnectEnv(t *testing.T) *disconnectEnv {
	t.Helper()
	owner := uuid.New()
	up := &gatedUpstream{bodies: []string{
		// Page 1 carries a FULL page (2 records, page_size 2) so a second
		// page WOULD be requested if the pull survived the disconnect.
		`{"data":[{"sku":"00100","qty":1},{"sku":"00200","qty":2}]}`,
		`{"data":[]}`,
	}}
	up.block() // park every page until released
	sourceSrv := httptest.NewServer(http.HandlerFunc(up.handler))
	// Cleanup registration is REVERSED into LIFO execution. Desired order on
	// any exit (including require failures): coordinator Stop FIRST (cancels
	// the service root, converging the executor through its AfterFunc link),
	// then the upstream gate release, then BOTH HTTP server closes. So the
	// closes register first and Stop registers LAST.
	t.Cleanup(sourceSrv.Close)
	store, source := newSyncTestStore(t, owner, restSyncSourceConfigWithTimeout(sourceSrv.URL, 60000))
	storeSourceCredential(t, store, source.ID, "oauth_token", "tok")
	job, err := store.CreateSyncJob(t.Context(), &models.CreateSyncJobRequest{SourceID: source.ID, OutputDatasetID: uuid.New()}, repo.Tenant{OwnerID: owner})
	require.NoError(t, err)
	committer := &fakeCommitPort{}
	h := newSyncHandlers(store, committer)
	h.Config.SyncCancelPollInterval = 10 * time.Millisecond
	// The task deadline sits FAR outside the observation window (3s): if the
	// disconnect does not propagate, only the deadline would eventually stop
	// the pull — which these assertions must not mistake for propagation.
	coord := handlers.NewRunCoordinator(context.Background(), handlers.CoordinatorConfig{
		QueueDepth: 4, Workers: 1,
		AuthMemoryTTL: 30 * time.Second, TaskDeadline: 30 * time.Second,
		StopBudget: 5 * time.Second,
	}, h)
	h.Coordinator = coord

	// REAL HTTP server: the actual RunSyncJob behind trusted-claims test
	// middleware. The body is never read here; nothing cancels the request
	// context — only the transport may.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("sync_id", job.ID.String())
		ctx := context.WithValue(r.Context(), chi.RouteCtxKey, rctx)
		ctx = authmw.ContextWithClaims(ctx, &authmw.Claims{Sub: owner})
		h.RunSyncJob(w, r.WithContext(ctx))
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(up.release)                  // runs after Stop, before the closes
	t.Cleanup(func() { _ = coord.Stop() }) // runs FIRST (registered last)
	return &disconnectEnv{store: store, h: h, coord: coord, committer: committer, upstream: up, srv: srv, job: job, owner: owner}
}

// rawPost opens a REAL TCP connection and writes the POST by hand (Content-
// Length exactly per case), leaving the socket open without reading the
// response — the client half of the disconnect experiment.
func rawPost(t *testing.T, srvURL, jobID, body string) net.Conn {
	t.Helper()
	host := strings.TrimPrefix(srvURL, "http://")
	conn, err := net.Dial("tcp", host)
	require.NoError(t, err)
	req := "POST /api/v1/data-connection/syncs/" + jobID + "/run HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Authorization: Bearer caller-platform-jwt\r\n" +
		"Content-Length: " + itoa(len(body)) + "\r\n" +
		"Connection: keep-alive\r\n\r\n" +
		body
	_, err = conn.Write([]byte(req))
	require.NoError(t, err)
	return conn
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestRealClientDisconnectCancelsSyncExecution(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"content_length_0", ""},
		{"content_length_2_json_object", "{}"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newDisconnectEnv(t)

			conn := rawPost(t, env.srv.URL, env.job.ID.String(), tc.body)
			t.Cleanup(func() { _ = conn.Close() }) // every conn pre-registered
			// The pull is genuinely in flight: page 1 is parked upstream.
			require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 },
				5*time.Second, 10*time.Millisecond, "page 1 must be parked before the disconnect")

			// The ONLY intervention: close the client TCP socket.
			require.NoError(t, conn.Close())
			closedAt := time.Now()

			// Propagation window: 3s — far below the 30s task deadline. The
			// parked page's context must observe the disconnect here.
			propagated := true
			deadline := time.Now().Add(3 * time.Second)
			for env.upstream.clientCtxDoneNanos() == 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if env.upstream.clientCtxDoneNanos() == 0 {
				propagated = false
			}
			require.True(t, propagated,
				"client TCP close did NOT propagate to the request context within 3s (upstream page context never cancelled) — transport gap confirmed for this case")

			// Convergence: the run persists `failed` with the context-cancellation
			// cause (the P2.1 transport mapping renders context.Canceled as
			// "timed out" — both phrasings accepted); nothing published; no
			// second page. Read the row under the fake store's lock: the legacy
			// unlocked ListSyncRuns is not a synchronization edge and may miss
			// concurrent worker writes.
			// Cause text: the runtime now distinguishes cancellation from
			// deadline — a client disconnect surfaces as the exact, URL-free
			// "context canceled" wording (never "timed out").
			require.Eventually(t, func() bool {
				env.store.mu.Lock()
				defer env.store.mu.Unlock()
				rows := env.store.runs[env.job.ID]
				if len(rows) != 1 {
					return false
				}
				r := rows[0]
				if r.Status != "failed" || r.Error == nil {
					return false
				}
				return strings.Contains(*r.Error, "context canceled") && !strings.Contains(*r.Error, "timed out")
			}, 5*time.Second, 15*time.Millisecond, "run must converge to failed with an explicit context-canceled cause")

			assert.Less(t, time.Since(closedAt), 8*time.Second, "convergence must be prompt, not deadline-driven (30s task deadline must not be the stopper)")
			reqs, _ := env.committer.snapshot()
			assert.Empty(t, reqs, "a disconnected client's run must never publish")
			// "No second page" is proven by the TOTAL request count (parked
			// requests included) staying at 1 — servedPages/peak only prove
			// non-completion, not absence of a next request. Re-checked after
			// a settle window so a straggler page cannot slip through.
			assert.Equal(t, int32(1), env.upstream.attemptsPages(), "exactly one page request may ever happen")
			time.Sleep(250 * time.Millisecond)
			assert.Equal(t, int32(1), env.upstream.attemptsPages(), "no second page may be requested after the disconnect (settle re-check)")
		})
	}
}

// readResponse parses one HTTP response off the raw client connection.
func readResponse(t *testing.T, conn net.Conn) (*http.Response, string) {
	t.Helper()
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, string(body)
}

func lockedRunCount(t *testing.T, env *disconnectEnv) (int, *models.SyncRun) {
	t.Helper()
	env.store.mu.Lock()
	defer env.store.mu.Unlock()
	rows := env.store.runs[env.job.ID]
	if len(rows) == 0 {
		return 0, nil
	}
	r := rows[0]
	return len(rows), &r
}

// TestRealBodyOverCapIs413WithZeroRows: a body beyond the 1 MiB drain cap is
// refused with 413 before any durable row exists; the upstream is never
// touched.
func TestRealBodyOverCapIs413WithZeroRows(t *testing.T) {
	env := newDisconnectEnv(t)
	body := strings.Repeat("x", (1<<20)+1)
	conn := rawPost(t, env.srv.URL, env.job.ID.String(), body)
	t.Cleanup(func() { _ = conn.Close() })

	resp, respBody := readResponse(t, conn)
	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode, respBody)
	// Per official go1.26.3 transfer.go (shouldClose → header.Del), the
	// client CONSUMES "Connection: close" and maps it to resp.Close.
	assert.True(t, resp.Close, "refusals close the connection")
	n, _ := lockedRunCount(t, env)
	assert.Zero(t, n, "over-cap body must create zero durable rows")
	assert.Equal(t, int32(0), env.upstream.attemptsPages(), "upstream untouched")
}

// TestRealSlowBodyIsBoundedWithZeroRows: a trickling body cannot hold the
// trigger hostage — the route-local 5s read deadline aborts the drain, the
// request is refused with 400, and no durable row exists. Total bound ≈ the
// read budget, far below any test-level timeout.
func TestRealSlowBodyIsBoundedWithZeroRows(t *testing.T) {
	env := newDisconnectEnv(t)
	host := strings.TrimPrefix(env.srv.URL, "http://")
	conn, err := net.Dial("tcp", host)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// Headers declare 4 KiB; only a fragment is ever written, then the
	// socket stalls (no close — a slow trickler).
	head := "POST /api/v1/data-connection/syncs/" + env.job.ID.String() + "/run HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Authorization: Bearer caller-platform-jwt\r\n" +
		"Content-Length: 4096\r\n\r\n"
	_, err = conn.Write([]byte(head + "frag"))
	require.NoError(t, err)

	start := time.Now()
	resp, respBody := readResponseWithDeadline(t, conn, 10*time.Second)
	elapsed := time.Since(start)
	// The route-local read budget is 5s (handlers.syncTriggerBodyReadBudget,
	// unexported); the refusal must be caused by it (408) and bounded near it.
	assert.Equal(t, http.StatusRequestTimeout, resp.StatusCode, respBody)
	// Consumed hop-by-hop header → resp.Close (see transfer.go note above).
	assert.True(t, resp.Close, "refusals close the connection so net/http never auto-drains the stalled body")
	assert.GreaterOrEqual(t, elapsed, 4*time.Second, "the read deadline must be what stops the drain")
	assert.Less(t, elapsed, 9*time.Second, "the refusal must be bounded by the route-local read budget")
	n, _ := lockedRunCount(t, env)
	assert.Zero(t, n, "slow body must create zero durable rows")
	assert.Equal(t, int32(0), env.upstream.attemptsPages(), "upstream untouched")
}

// TestRealFastBodyThenLongPullNotHurtByReadDeadline: after a fast body drain
// the read deadline is CLEARED — a pull that outlasts the 5s drain budget by
// design still succeeds end to end.
func TestRealFastBodyThenLongPullNotHurtByReadDeadline(t *testing.T) {
	env := newDisconnectEnv(t)
	conn := rawPost(t, env.srv.URL, env.job.ID.String(), "{}")
	t.Cleanup(func() { _ = conn.Close() })

	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 },
		5*time.Second, 10*time.Millisecond, "page 1 must park before the long hold")
	// Hold the page clearly beyond the 5s drain budget, then release.
	time.AfterFunc(6500*time.Millisecond, env.upstream.release)

	resp, respBody := readResponseWithDeadline(t, conn, 20*time.Second)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode, respBody)
	assert.Contains(t, respBody, `"succeeded"`, "a long pull after a drained body must complete normally: %s", respBody)
	n, run := lockedRunCount(t, env)
	require.Equal(t, 1, n)
	assert.Equal(t, "succeeded", run.Status)
	reqs, _ := env.committer.snapshot()
	assert.Len(t, reqs, 1, "the run publishes exactly once")
}

// readResponseWithDeadline bounds the client-side read of the response.
func readResponseWithDeadline(t *testing.T, conn net.Conn, budget time.Duration) (*http.Response, string) {
	t.Helper()
	type result struct {
		resp *http.Response
		body string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			ch <- result{err: err}
			return
		}
		body, err := io.ReadAll(resp.Body)
		ch <- result{resp: resp, body: string(body), err: err}
	}()
	select {
	case r := <-ch:
		require.NoError(t, r.err)
		return r.resp, r.body
	case <-time.After(budget):
		t.Fatalf("response did not arrive within %s", budget)
		return nil, ""
	}
}

// TestRealChunkedBodyOverCapIs413WithZeroRows: Transfer-Encoding: chunked
// bodies are drained under the same byte cap — over-cap chunked is 413 with
// Connection: close and zero rows.
func TestRealChunkedBodyOverCapIs413WithZeroRows(t *testing.T) {
	env := newDisconnectEnv(t)
	host := strings.TrimPrefix(env.srv.URL, "http://")
	conn, err := net.Dial("tcp", host)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	payload := strings.Repeat("y", (1<<20)+1)
	head := "POST /api/v1/data-connection/syncs/" + env.job.ID.String() + "/run HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Authorization: Bearer caller-platform-jwt\r\n" +
		"Transfer-Encoding: chunked\r\n\r\n"
	_, err = conn.Write([]byte(head + fmt.Sprintf("%x\r\n", len(payload))))
	require.NoError(t, err)
	_, err = conn.Write([]byte(payload))
	require.NoError(t, err)

	resp, respBody := readResponseWithDeadline(t, conn, 10*time.Second)
	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode, respBody)
	assert.True(t, resp.Close, "refusals close the connection")
	n, _ := lockedRunCount(t, env)
	assert.Zero(t, n, "chunked over-cap must create zero durable rows")
	assert.Equal(t, int32(0), env.upstream.attemptsPages(), "upstream untouched")
}

// TestRealChiCompressChainBodyEOFThenLongPullAliveThenDisconnect: through a
// REAL chi v5 Compress middleware chain (whose writer Unwraps to the
// connection), a drained body's deadline is cleared: the pull stays alive
// WELL past the 5s drain budget, and a subsequent client TCP close still
// propagates (cause context canceled, never a leaked-deadline timeout).
func TestRealChiCompressChainBodyEOFThenLongPullAliveThenDisconnect(t *testing.T) {
	env := newDisconnectEnv(t)

	r := chi.NewRouter()
	r.Use(middleware.Compress(5))
	r.Post("/api/v1/data-connection/syncs/{sync_id}/run", func(w http.ResponseWriter, r *http.Request) {
		// The REAL router supplies the {sync_id} param; claims are injected
		// as trusted test middleware. Body unread, no manual cancellation.
		ctx := authmw.ContextWithClaims(r.Context(), &authmw.Claims{Sub: env.owner})
		env.h.RunSyncJob(w, r.WithContext(ctx))
	})
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	t.Cleanup(env.upstream.release)
	t.Cleanup(func() { _ = env.coord.Stop() })

	conn := rawPost(t, srv.URL, env.job.ID.String(), "{}")
	t.Cleanup(func() { _ = conn.Close() })
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 },
		5*time.Second, 10*time.Millisecond, "page 1 must park through the compress chain")

	// Alive well past the 5s drain budget. attempts==1 alone cannot exclude
	// a leaked deadline having ALREADY cancelled the page — so also require:
	// the parked page's context is still live, the run is still running, and
	// nothing has been published. Only then does the client socket close.
	time.Sleep(5500 * time.Millisecond)
	assert.Equal(t, int32(1), env.upstream.attemptsPages(), "the parked page is still the only request; no second page while parked")
	require.Zero(t, env.upstream.clientCtxDoneNanos(), "the parked page must still be ALIVE past the drain budget (a leaked deadline would have cancelled it)")
	// Copy under the lock, assert AFTER unlocking: a failing require calls
	// GoExit, which would skip the Unlock and deadlock any later cleanup that
	// takes the same mutex.
	env.store.mu.Lock()
	rowCount, rowStatus := 0, ""
	if rows := env.store.runs[env.job.ID]; len(rows) == 1 {
		rowCount, rowStatus = 1, rows[0].Status
	}
	env.store.mu.Unlock()
	require.Equal(t, 1, rowCount, "one durable row while parked")
	require.Equal(t, "running", rowStatus, "still executing past the drain budget")
	parkedReqs, _ := env.committer.snapshot()
	require.Empty(t, parkedReqs, "nothing published while parked")

	// NOW the disconnect: must propagate through the compress chain.
	require.NoError(t, conn.Close())
	deadline := time.Now().Add(3 * time.Second)
	for env.upstream.clientCtxDoneNanos() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.NotZero(t, env.upstream.clientCtxDoneNanos(), "disconnect must propagate through the chi Compress chain within 3s")

	require.Eventually(t, func() bool {
		env.store.mu.Lock()
		defer env.store.mu.Unlock()
		rows := env.store.runs[env.job.ID]
		if len(rows) != 1 {
			return false
		}
		r := rows[0]
		return r.Status == "failed" && r.Error != nil &&
			strings.Contains(*r.Error, "context canceled") && !strings.Contains(*r.Error, "timed out")
	}, 5*time.Second, 15*time.Millisecond, "run must converge to failed with an explicit context-canceled cause (a leaked drain deadline would surface as timed out)")

	reqs, _ := env.committer.snapshot()
	assert.Empty(t, reqs)
	assert.Equal(t, int32(1), env.upstream.attemptsPages(), "exactly one page request overall")
}

// deadlineWriter wraps the ResponseRecorder with a CONTROLLABLE read
// deadline: supported-mode records the installs/clears; unsupported-mode
// mimics a writer chain without deadline support.
type deadlineWriter struct {
	*httptest.ResponseRecorder
	unsupported bool
	installs    []time.Time
	clears      int
}

func (d *deadlineWriter) SetReadDeadline(t time.Time) error {
	if d.unsupported {
		return http.ErrNotSupported
	}
	if t.IsZero() {
		d.clears++
	} else {
		d.installs = append(d.installs, t)
	}
	return nil
}

// TestDrainRequiresDeadlineCapableWriterZeroRows: a writer chain WITHOUT
// read-deadline support (bare recorder) is refused immediately — Connection
// close, 500, zero durable rows — never a silent unbounded fallback.
func TestDrainRequiresDeadlineCapableWriterZeroRows(t *testing.T) {
	env := newDisconnectEnv(t)
	dw := &deadlineWriter{ResponseRecorder: httptest.NewRecorder(), unsupported: true}
	req := withRouteParam(authedReq(http.MethodPost, "/syncs/"+env.job.ID.String()+"/run", "{}", env.owner), "sync_id", env.job.ID.String())
	req.Header.Set("Authorization", "Bearer caller-platform-jwt")
	env.h.RunSyncJob(dw, req)

	assert.Equal(t, http.StatusInternalServerError, dw.Code, dw.Body.String())
	assert.Equal(t, "close", dw.Header().Get("Connection"))
	n, _ := lockedRunCount(t, env)
	assert.Zero(t, n, "unsupported writer must create zero durable rows")
}

// TestDrainClearsDeadlineOnlyOnTrueEOF: with a deadline-capable writer, a
// within-cap body drains to EOF, the deadline is cleared exactly once, and
// the trigger proceeds normally. The handler is called SYNCHRONOUSLY with
// ?async=true (identical drain path; 202 returns right after dispatch), so
// the writer bookkeeping is stable when read — no goroutine race, no join.
func TestDrainClearsDeadlineOnlyOnTrueEOF(t *testing.T) {
	env := newDisconnectEnv(t)
	dw := &deadlineWriter{ResponseRecorder: httptest.NewRecorder()}
	req := withRouteParam(authedReq(http.MethodPost, "/syncs/"+env.job.ID.String()+"/run?async=true", "{}", env.owner), "sync_id", env.job.ID.String())
	req.Header.Set("Authorization", "Bearer caller-platform-jwt")
	env.h.RunSyncJob(dw, req)

	require.Equal(t, http.StatusAccepted, dw.Code, dw.Body.String())
	require.Len(t, dw.installs, 1, "drain installs the read deadline exactly once")
	require.Equal(t, 1, dw.clears, "a true EOF within the cap clears the deadline exactly once")
	n, _ := lockedRunCount(t, env)
	assert.Equal(t, 1, n, "the trigger proceeds to a durable row after a clean drain")
	// The background worker executes the dispatched task; the page parks on
	// the gated upstream (alive proof for the cleared deadline).
	require.Eventually(t, func() bool { return env.upstream.peakInFlight() == 1 },
		5*time.Second, 10*time.Millisecond, "the async task must reach the parked upstream page")
}
