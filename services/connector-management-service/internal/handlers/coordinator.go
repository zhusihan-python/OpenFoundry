// P2.2 run coordinator: the service-lifecycle-managed home for interruptible
// sync executions. Every execution — the default synchronous path AND async
// admissions — passes through one linearized admission gate with bounded
// capacity (queued + executing combined), so shutdown tracking and memory
// bounds cover both paths uniformly.
//
// Provable invariants (each has a dedicated test):
//
//	A. Reserve/Dispatch/beginExecution serialize against Stop's gate close
//	   under gateMu; every successful Reserve adds to gateWG BEFORE any
//	   database write, so Stop's bounded join covers the whole
//	   reserve→enqueue→dispatch/execute window; nothing registers after the
//	   gate closes except holders of pre-stop reservations.
//	B. Stop is idempotent and concurrent-safe (all callers observe the same
//	   completion): close gate → drain queue snapshot → IMMEDIATELY cancel
//	   the root (interrupting every live execution; their bounded detached
//	   terminal writes converge while the DB pool is still open) → close
//	   queued tasks and join under ONE shared drain budget → bounded worker
//	   join. Budget-exhausted rows are reported honestly, never fabricated.
//	C. The queue carries run handles only. Caller Authorization strings live
//	   in a private registry keyed by run id, are actively cleared at expiry
//	   (sweeper) and deleted on every exit path (done/cancel/reject/stop);
//	   an executing task's context deadline is min(auth expiry, task
//	   deadline). No JWT is ever persisted or logged.
//	D. A failed claim attempts a bounded durable close; when the database
//	   itself is unavailable the run is reported as honestly unconfirmed —
//	   it stays pending/queryable and cancellable, never silently dropped
//	   and never given a fabricated terminal state.
//	E. beginExecution keeps BOTH the base cancel and the deadline cancel;
//	   finish always calls both (plus the AfterFunc stop, whose callback is
//	   only an idempotent cancel), so no child registration outlives the
//	   execution on the root context.
//
// On restart the coordinator deliberately adopts NOTHING from the database:
// pending or running rows from a previous process are never scanned, claimed
// or replayed (P2.1 rule); they remain queryable and cancellable only.
//
// Shutdown ordering in main is testable: shutdownSequence runs the
// coordinator Stop (bounded join) BEFORE pool.Close so terminal writes
// always see a live database.
package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/repo"
)

// Coordinator bounds. Capacity bounds memory AND concurrent work (queued +
// executing, sync + async, combined); the auth memory TTL bounds how long a
// caller Authorization may live in the registry; the task deadline bounds
// the WHOLE task from admission (queue wait included). The stop budget is
// the single shared drain/join budget. Every value is overridable; main
// validates env overrides into range.
const (
	// Exported defaults double as the fallbacks main substitutes for unset
	// env overrides; the exported bounds are the single source config.FromEnv
	// validates explicit overrides against (an explicit out-of-range value —
	// including 0 — is a startup error, never a silent default).
	DefaultCoordinatorQueueDepth   = 64
	DefaultCoordinatorWorkers      = 1
	DefaultCoordinatorAuthTTL      = 300 * time.Second
	DefaultCoordinatorTaskDeadline = 300 * time.Second
	MinCoordinatorQueueDepth       = 1
	MaxCoordinatorQueueDepth       = 4096
	MinCoordinatorWorkers          = 1
	MaxCoordinatorWorkers          = 64
	MinCoordinatorAuthTTL          = time.Second
	MaxCoordinatorAuthTTL          = 24 * time.Hour
	MinCoordinatorTaskDeadline     = time.Second
	MaxCoordinatorTaskDeadline     = 24 * time.Hour
	defaultCoordinatorStopBudget   = 75 * time.Second // publish 30s + final write 30s + drain + slack
	defaultCoordinatorCancelPoll   = time.Second
)

// CoordinatorConfig carries validated bounds; NewRunCoordinator fills zero
// fields with the defaults above.
type CoordinatorConfig struct {
	QueueDepth    int
	Workers       int
	AuthMemoryTTL time.Duration
	TaskDeadline  time.Duration
	// StopBudget overrides the shared shutdown drain/join budget (tests).
	StopBudget time.Duration
}

func (c CoordinatorConfig) withDefaults() CoordinatorConfig {
	if c.QueueDepth <= 0 {
		c.QueueDepth = DefaultCoordinatorQueueDepth
	}
	if c.Workers <= 0 {
		c.Workers = DefaultCoordinatorWorkers
	}
	if c.AuthMemoryTTL <= 0 {
		c.AuthMemoryTTL = DefaultCoordinatorAuthTTL
	}
	if c.TaskDeadline <= 0 {
		c.TaskDeadline = DefaultCoordinatorTaskDeadline
	}
	if c.StopBudget <= 0 {
		c.StopBudget = defaultCoordinatorStopBudget
	}
	return c
}

// coordinatorTask is the queue payload: run handles only. The caller's
// Authorization, its expiry and the task deadline live in the auth registry
// record keyed by run id — never in the channel, never persisted.
//
// syncWait distinguishes the two waiting modes over ONE executor: a sync task
// carries the triggering request's context (client disconnect interrupts the
// execution; P2.1 no-terminal-write semantics on pre-publish cancellation)
// and a buffered result channel the HTTP handler waits on; an async task has
// neither and the worker is the only owner. Concurrency for BOTH is bounded
// by the same worker pool.
type coordinatorTask struct {
	runID, syncID, sourceID uuid.UUID
	tenant                  repo.Tenant
	requestedBy             uuid.UUID

	syncWait  bool
	parentCtx context.Context       // triggering request context (sync only)
	resultCh  chan<- SyncExecResult // buffered(1); sync only
}

// SyncExecResult is the worker's answer to a synchronously waiting handler.
type SyncExecResult struct {
	Run *models.SyncRun
	Err error
}

// errSyncClaimLost reports that another execution claimed the durable task
// first (claim CAS lost): the run is owned elsewhere, nothing was executed
// here, and no close is attempted.
var errSyncClaimLost = errors.New("sync run was claimed by another execution")

// claimFailureError reports that an admitted task never reached execution
// (or its claim failed). Aftermath describes the DURABLE aftermath precisely,
// derived from the real cancel outcome — never from the mere absence of an
// RPC error: "closed as aborted" is only ever said when the row actually
// transitioned to aborted; a persisted request that has not been confirmed,
// a too-late refusal or a failed close attempt each get their own sentence.
type claimFailureError struct {
	cause     error
	aftermath string
}

func (e *claimFailureError) Error() string {
	return e.cause.Error() + "; durable aftermath: " + e.aftermath
}
func (e *claimFailureError) Unwrap() error { return e.cause }

// cancelAftermath describes the durable aftermath of a bounded close attempt
// from the REAL outcome — the single place "aborted" may be claimed.
func cancelAftermath(outcome repo.CancelOutcome, closeErr error) string {
	switch {
	case closeErr != nil:
		return "the close attempt itself failed; the run stays honestly unconfirmed (pending or running, queryable)"
	case outcome == cancelOutcomeV1LeftForRecovery:
		return "the p26-v1 run was deliberately left uncancelled (pending or running) for fresh-auth recovery"
	case outcome == repo.CancelOutcomeAborted:
		return "the admitted run was closed as aborted"
	case outcome == repo.CancelOutcomeAlreadyTerminal:
		return "the run is already in a terminal state"
	case outcome == repo.CancelOutcomeRequested:
		return "a cancel request is now durably persisted; the run's final state is not yet confirmed"
	case outcome == repo.CancelOutcomeTooLate:
		return "the publish gate had already been entered; the outcome is uncertain (too late to cancel)"
	case outcome == repo.CancelOutcomeNotVisible:
		return "the run is not visible in this tenant"
	default:
		return "unknown close outcome"
	}
}

// admissionRecord is the registry entry for one async admission: the only
// place a caller Authorization may exist in memory. release hands the
// reserved slot + lifecycle count back exactly once, whichever path finishes
// the task (worker, stop drain, rejection).
type admissionRecord struct {
	mu             sync.Mutex
	authorization  string
	authExpiresAt  time.Time
	deadline       time.Time
	expiredCleared bool
	releaseOnce    sync.Once
	release        func()
}

// clearAuth zeroes the Authorization reference at expiry while keeping the
// record (and its release) alive for whoever eventually handles the task.
func (r *admissionRecord) clearAuth() {
	r.mu.Lock()
	r.authorization = ""
	r.expiredCleared = true
	r.mu.Unlock()
}

func (r *admissionRecord) snapshot() (auth string, authExpiresAt, deadline time.Time, expired bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.authorization, r.authExpiresAt, r.deadline, r.expiredCleared
}

// RunCoordinator coordinates admissions, the bounded queue, live executions
// and workers for one process. It is safe for concurrent use.
type RunCoordinator struct {
	h   *Handlers
	cfg CoordinatorConfig

	rootCtx    context.Context
	rootCancel context.CancelFunc

	// gateMu linearizes admission (Reserve/Dispatch) against shutdown (Stop).
	// A reservation = one sem slot + one gateWG count, acquired together
	// under the lock, so Stop's close-out can never miss an in-flight
	// admission and nothing enqueues after the gate closes.
	gateMu  sync.Mutex
	stopped bool
	sem     chan struct{}
	tasks   chan coordinatorTask
	gateWG  sync.WaitGroup // full lifecycle of every reservation

	authMu  sync.Mutex
	authReg map[uuid.UUID]*admissionRecord

	activeMu sync.Mutex
	active   map[uuid.UUID]*execHandle

	// P2.6 recovery tickets: an independent per-run registry. They share the
	// capacity accounting (sem + gateWG) so recovery stays bounded with every
	// other admission, but never touch authReg/active — root v2 item 9.
	recoveryMu      sync.Mutex
	recoveryTickets map[uuid.UUID]*recoveryTicket

	stopOnce sync.Once
	stopDone chan struct{}
	stopErr  error

	workerWG sync.WaitGroup
	sweepWG  sync.WaitGroup
}

// NewRunCoordinator starts the workers and the auth-expiry sweeper under the
// given service root context. h is the handler set whose executor and store
// the workers use.
func NewRunCoordinator(ctx context.Context, cfg CoordinatorConfig, h *Handlers) *RunCoordinator {
	cfg = cfg.withDefaults()
	rootCtx, rootCancel := context.WithCancel(ctx)
	c := &RunCoordinator{
		h:               h,
		cfg:             cfg,
		rootCtx:         rootCtx,
		rootCancel:      rootCancel,
		sem:             make(chan struct{}, cfg.QueueDepth),
		tasks:           make(chan coordinatorTask, cfg.QueueDepth),
		authReg:         map[uuid.UUID]*admissionRecord{},
		active:          map[uuid.UUID]*execHandle{},
		recoveryTickets: map[uuid.UUID]*recoveryTicket{},
		stopDone:        make(chan struct{}),
	}
	for i := 0; i < cfg.Workers; i++ {
		c.workerWG.Add(1)
		go c.workerLoop()
	}
	c.sweepWG.Add(1)
	go c.authSweeper()
	return c
}

// Config exposes the effective bounds (read-only use).
func (c *RunCoordinator) Config() CoordinatorConfig { return c.cfg }

// Admission is a reserved capacity slot plus its lifecycle count. The
// absolute auth/task deadlines are anchored at Reserve time — later database
// latency (enqueue, re-reads) can only consume the budget, never reset it.
// Dispatch transfers ownership to the queue/registry; Release returns it
// when the task never got queued. Both are idempotent.
type Admission struct {
	c          *RunCoordinator
	reservedAt time.Time
	mu         sync.Mutex
	released   bool
}

// CloseAdmission refuses NEW admissions at signal time without interrupting
// anything live: workers keep draining the queue and in-flight executions
// keep running, so a graceful HTTP drain can finish them. The full sequence
// (queue drain, root cancel, join) belongs to Stop.
func (c *RunCoordinator) CloseAdmission() {
	c.gateMu.Lock()
	c.stopped = true
	c.gateMu.Unlock()
}

// Reserve attempts to reserve one capacity slot without touching the
// database. It returns nil when capacity is exhausted, the coordinator has
// stopped, or the root context is already cancelled (a root without live
// workers must never accept work) — callers must answer 503 BEFORE creating
// any durable row. The gateWG count is taken under the same lock as Stop's
// close-out, so a reservation can never slip past a completed Stop.
func (c *RunCoordinator) Reserve() *Admission {
	c.gateMu.Lock()
	defer c.gateMu.Unlock()
	if c.stopped || c.rootCtx.Err() != nil {
		return nil
	}
	select {
	case c.sem <- struct{}{}:
		c.gateWG.Add(1)
		return &Admission{c: c, reservedAt: time.Now()}
	default:
		return nil
	}
}

// Deadlines returns the absolute authorization-memory and task deadlines
// anchored at Reserve time.
func (a *Admission) Deadlines() (authExpiresAt, taskDeadline time.Time) {
	return a.reservedAt.Add(a.c.cfg.AuthMemoryTTL), a.reservedAt.Add(a.c.cfg.TaskDeadline)
}

// releaseSlot returns one capacity slot and its lifecycle count. Exactly-once
// per reservation is guaranteed by the callers (Admission/record Once).
func (c *RunCoordinator) releaseSlot() {
	select {
	case <-c.sem:
	default:
	}
	c.gateWG.Done()
}

// Release returns a reserved slot (used when admission failed before or at
// enqueue). It is a no-op after a successful Dispatch.
func (a *Admission) Release() {
	if a == nil {
		return
	}
	a.mu.Lock()
	if a.released {
		a.mu.Unlock()
		return
	}
	a.released = true
	a.mu.Unlock()
	a.c.releaseSlot()
}

// Dispatch queues the task and stores its Authorization in the private
// registry under expiry anchored at Reserve time. It fails ONLY in the
// shutdown race after a successful Reserve (the durable row exists but will
// not be executed by this process). On failure ownership is NOT released
// here: the permit holder must first close the durable row (bounded) and
// only then call Release — otherwise Stop could observe a zero count,
// return, and have main close the pool before the cleanup runs.
func (a *Admission) Dispatch(t coordinatorTask, authorization string) bool {
	return a.dispatchCapped(t, authorization, time.Time{})
}

// DispatchWithJWTExpiry is Dispatch with the caller's ALREADY-VERIFIED JWT
// expiry as an absolute cap on the auth-memory and task deadlines (review
// followup: the trusted claims expiry must bound the work, not just the
// local TTL). A zero cap behaves like Dispatch.
func (a *Admission) DispatchWithJWTExpiry(t coordinatorTask, authorization string, jwtExpiry time.Time) bool {
	return a.dispatchCapped(t, authorization, jwtExpiry)
}

// clampDeadlines applies an optional absolute cap (the schedule grant's
// expiry) to the Reserve-anchored authorization-memory and task deadlines:
// each becomes min(anchored, cap). A zero cap is a no-op. Pure function.
func clampDeadlines(authExpiresAt, taskDeadline, cap time.Time) (time.Time, time.Time) {
	if cap.IsZero() {
		return authExpiresAt, taskDeadline
	}
	if cap.Before(authExpiresAt) {
		authExpiresAt = cap
	}
	if cap.Before(taskDeadline) {
		taskDeadline = cap
	}
	return authExpiresAt, taskDeadline
}

// DispatchScheduled queues a scheduler-derived task. deadlineCap is the
// schedule grant's ABSOLUTE expiry: both the authorization-memory expiry and
// the task deadline are clamped to min(Reserve-anchored bound, deadlineCap),
// so a scheduled execution may never outlive the grant that authorized it —
// including the queue wait, which consumes the same budget. A zero cap
// behaves exactly like Dispatch. The manual HTTP paths are unchanged.
func (a *Admission) DispatchScheduled(runID, syncID, sourceID uuid.UUID, tenant repo.Tenant, requestedBy uuid.UUID, authorization string, deadlineCap time.Time) bool {
	return a.dispatchCapped(coordinatorTask{
		runID:       runID,
		syncID:      syncID,
		sourceID:    sourceID,
		tenant:      tenant,
		requestedBy: requestedBy,
	}, authorization, deadlineCap)
}

func (a *Admission) dispatchCapped(t coordinatorTask, authorization string, deadlineCap time.Time) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	if a.released {
		a.mu.Unlock()
		return false
	}
	c := a.c
	anchoredAuthExpiry, anchoredDeadline := a.Deadlines()
	authExpiresAt, taskDeadline := clampDeadlines(anchoredAuthExpiry, anchoredDeadline, deadlineCap)
	rec := &admissionRecord{
		authorization: authorization,
		authExpiresAt: authExpiresAt,
		deadline:      taskDeadline,
	}
	rec.release = func() { rec.releaseOnce.Do(c.releaseSlot) }
	c.gateMu.Lock()
	if c.stopped {
		// Ownership stays with the caller: they close the row, then Release.
		c.gateMu.Unlock()
		a.mu.Unlock()
		return false
	}
	a.released = true // ownership transfers to the registry record
	c.authMu.Lock()
	c.authReg[t.runID] = rec
	c.authMu.Unlock()
	// Invariant: a slot is held, so the buffered channel has room.
	c.tasks <- t
	c.gateMu.Unlock()
	a.mu.Unlock()
	return true
}

// takeRecord removes and returns the registry record for a run (worker side).
func (c *RunCoordinator) takeRecord(runID uuid.UUID) *admissionRecord {
	c.authMu.Lock()
	rec := c.authReg[runID]
	delete(c.authReg, runID)
	c.authMu.Unlock()
	return rec
}

// DropAuthRef clears the Authorization reference of a run whose durable row
// is already terminal (e.g. a queued task cancelled straight to aborted): a
// terminal row will never execute or publish, so the header is pure garbage.
// The record itself (and its slot release) stays with the task's consumer;
// only the secret is dropped early.
func (c *RunCoordinator) DropAuthRef(runID uuid.UUID) {
	c.authMu.Lock()
	rec := c.authReg[runID]
	c.authMu.Unlock()
	if rec != nil {
		rec.clearAuth()
	}
}

// finishRecord drops the registry entry and releases its slot (stop drain /
// rejection paths). Idempotent.
func (c *RunCoordinator) finishRecord(runID uuid.UUID) {
	c.authMu.Lock()
	rec := c.authReg[runID]
	delete(c.authReg, runID)
	c.authMu.Unlock()
	if rec != nil {
		rec.release()
	}
}

// authSweeper actively clears expired Authorization references so a header
// never outlives its TTL while a task sits queued or runs long.
func (c *RunCoordinator) authSweeper() {
	defer c.sweepWG.Done()
	ticker := time.NewTicker(defaultCoordinatorCancelPoll)
	defer ticker.Stop()
	for {
		select {
		case <-c.rootCtx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			c.authMu.Lock()
			for _, rec := range c.authReg {
				if auth, exp, _, _ := rec.snapshot(); auth != "" && now.After(exp) {
					rec.clearAuth()
				}
			}
			c.authMu.Unlock()
		}
	}
}

// beginExecution registers a live execution under the coordinator: its
// context derives from the service root (so shutdown interrupts it), carries
// the given deadline, and — when parent is a request context — is cancelled
// when that request goes away. The returned finish func MUST be called on
// every exit path; it releases the registry entry and both context cancels
// (invariant E: no child registration outlives the execution on the root).
// execHandle identifies a registered execution so finish only removes its
// own registration (pointer identity; funcs are not comparable).
type execHandle struct {
	cancel context.CancelFunc
}

func (c *RunCoordinator) beginExecution(parent context.Context, runID uuid.UUID, deadline time.Time) (context.Context, func()) {
	var baseCtx context.Context
	var baseCancel context.CancelFunc
	var stopLink func() bool
	if parent != nil {
		// Synchronous request→execution cancellation: the execution context
		// derives from the triggering request, so a client disconnect is
		// observed by the very next ctx.Err() check (a known-dead request
		// must never START new remote publish work). The service root is
		// linked the other way round via AfterFunc: shutdown tolerates the
		// small async propagation latency because Stop joins on execution
		// completion anyway. The callback only calls baseCancel (idempotent),
		// so finish's stop is sufficient.
		baseCtx, baseCancel = context.WithCancel(parent)
		stopLink = context.AfterFunc(c.rootCtx, baseCancel)
	} else {
		baseCtx, baseCancel = context.WithCancel(c.rootCtx)
	}
	execCtx, deadlineCancel := context.WithDeadline(baseCtx, deadline)
	handle := &execHandle{cancel: baseCancel}
	c.activeMu.Lock()
	if _, dup := c.active[runID]; !dup {
		c.active[runID] = handle
	}
	c.activeMu.Unlock()
	return execCtx, func() {
		if stopLink != nil {
			stopLink()
		}
		deadlineCancel()
		baseCancel()
		c.activeMu.Lock()
		if c.active[runID] == handle {
			delete(c.active, runID)
		}
		c.activeMu.Unlock()
	}
}

// ActiveExecutions reports how many live executions are registered (tests
// assert it drains to zero).
func (c *RunCoordinator) ActiveExecutions() int {
	c.activeMu.Lock()
	defer c.activeMu.Unlock()
	return len(c.active)
}

// AuthRefCount reports how many admission records still exist — i.e. tasks
// still QUEUED (the worker removes the record when it starts executing).
// A bounded-registry probe that never exposes the secret.
func (c *RunCoordinator) AuthRefCount() int {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	return len(c.authReg)
}

// OutstandingAdmissions reports the number of live reservations — queued,
// executing, or still inside the reserve→dispatch window — from the admission
// semaphore itself. This is the true resource bound (≤ capacity).
func (c *RunCoordinator) OutstandingAdmissions() int {
	return len(c.sem)
}

// AuthRefCleared reports whether the run's Authorization reference has been
// removed or actively cleared (expiry sweep / DropAuthRef / consumed).
func (c *RunCoordinator) AuthRefCleared(runID uuid.UUID) bool {
	c.authMu.Lock()
	rec := c.authReg[runID]
	c.authMu.Unlock()
	if rec == nil {
		return true
	}
	_, _, _, expired := rec.snapshot()
	auth, _, _, _ := rec.snapshot()
	return expired || auth == ""
}

// workerLoop pulls admitted tasks one at a time and executes them under the
// coordinator lifecycle. It exits when the root context is cancelled (after
// its current task converges).
func (c *RunCoordinator) workerLoop() {
	defer c.workerWG.Done()
	for {
		select {
		case <-c.rootCtx.Done():
			return
		case t := <-c.tasks:
			c.executeTask(t)
		}
	}
}

// requestCancelBounded is the single durable-close path, budgeted by ctx.
func (c *RunCoordinator) requestCancelBounded(ctx context.Context, t coordinatorTask, cause string) (repo.CancelOutcome, error) {
	res, err := c.h.Repo.RequestCancelSyncRun(ctx, t.runID, t.syncID, t.tenant, nil, cause)
	if err != nil {
		return "", err
	}
	return res.Outcome, nil
}

// cancelOutcomeV1LeftForRecovery reports the P2.6 skip decision of
// closeTaskDurable: a p26-v1 row deliberately NOT durably cancelled.
const cancelOutcomeV1LeftForRecovery = repo.CancelOutcome("v1_left_for_recovery")

// closeTaskDurable closes a not-yet-executed durable task through the
// legacy cancel path — EXCEPT p26-v1 rows (review round1 item 4): a v1 run
// whose execution never started (Stop, auth/task expiry, claim failure,
// parent reads, shutdown races) must stay pending/running for fresh-auth
// recovery; the unsigned nil-generation cancel that would strand the
// protocol is refused. The local slot/auth release happens with the caller
// either way — only the durable close is skipped.
func (c *RunCoordinator) closeTaskDurable(ctx context.Context, t coordinatorTask, cause string) (repo.CancelOutcome, error) {
	// FAIL CLOSED (review followup): a failed probe never authorizes the
	// unsigned close — the read error is returned verbatim (the run stays
	// honestly unconfirmed) and an invisible run answers not-visible with
	// zero writes.
	run, rerr := c.h.Repo.GetSyncRunForTenant(ctx, t.syncID, t.runID, t.tenant)
	if rerr != nil {
		return "", rerr
	}
	if run == nil {
		return repo.CancelOutcomeNotVisible, nil
	}
	if run.RecoveryProtocol == models.RunRecoveryProtocolP26V1 {
		slog.Info("coordinator left p26-v1 task for fresh-auth recovery instead of durable close",
			slog.String("sync_run_id", t.runID.String()))
		return cancelOutcomeV1LeftForRecovery, nil
	}
	return c.requestCancelBounded(ctx, t, cause)
}

// systemClose deterministically closes a not-yet-executed task through the
// same durable cancel path users go through. Cause is recorded verbatim on
// an aborted pending row. When the database itself is unavailable the run is
// reported honestly (it stays pending/queryable) — never fabricated (D).
func (c *RunCoordinator) systemClose(ctx context.Context, t coordinatorTask, cause string) {
	outcome, err := c.requestCancelBounded(ctx, t, cause)
	switch {
	case err != nil:
		slog.Error("coordinator could not close admitted task; run stays honestly unconfirmed",
			slog.String("error", err.Error()), slog.String("sync_run_id", t.runID.String()))
	case outcome == repo.CancelOutcomeNotVisible:
		slog.Warn("coordinator close: run not visible", slog.String("sync_run_id", t.runID.String()))
	default:
		slog.Info("coordinator closed admitted task", slog.String("outcome", string(outcome)), slog.String("sync_run_id", t.runID.String()))
	}
}

// executeTask turns one admitted durable task into a leased execution — the
// single executor path for BOTH waiting modes. Every path converges, always
// releases the record and always answers a synchronously waiting handler
// (buffered channel, so a handler that already gave up never blocks the
// worker): auth/deadline expiry closes the pending row, a lost claim race
// means somebody else owns the row, a claim storage error attempts a bounded
// durable close (and reports honestly when even that is impossible), and the
// executor's terminal write is the record. Sync tasks run under the
// triggering request's context (client disconnect interrupts; P2.1
// no-terminal-write semantics for pre-publish cancellation); async tasks
// finalize every interruption.
func (c *RunCoordinator) executeTask(t coordinatorTask) {
	var result SyncExecResult
	if t.resultCh != nil {
		defer func() {
			// Always exactly one answer, even on the early-exit paths below.
			t.resultCh <- result
		}()
	}
	rec := c.takeRecord(t.runID)
	if rec == nil {
		// Unreachable by construction (records are created before the queue
		// send and removed exactly once by the task's consumer). Loud, never
		// silent.
		slog.Error("coordinator worker found task without admission record", slog.String("sync_run_id", t.runID.String()))
		result = SyncExecResult{Err: fmt.Errorf("internal coordinator error: task without admission record")}
		return
	}
	defer rec.release()

	auth, authExpiresAt, taskDeadline, expiredCleared := rec.snapshot()
	// closeNow performs the bounded durable close and reports the REAL
	// outcome; only cancelAftermath may translate it into words.
	closeNow := func(cause string) (repo.CancelOutcome, error) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(c.rootCtx), syncLeaseCallTimeout)
		defer cancel()
		outcome, err := c.closeTaskDurable(ctx, t, cause)
		switch {
		case err != nil:
			slog.Error("coordinator could not close admitted task; run stays honestly unconfirmed",
				slog.String("error", err.Error()), slog.String("sync_run_id", t.runID.String()))
		case outcome == repo.CancelOutcomeNotVisible:
			slog.Warn("coordinator close: run not visible", slog.String("sync_run_id", t.runID.String()))
		default:
			slog.Info("coordinator closed admitted task", slog.String("outcome", string(outcome)), slog.String("sync_run_id", t.runID.String()))
		}
		return outcome, err
	}
	preExecFailure := func(cause, msg string) error {
		outcome, closeErr := closeNow(cause)
		return &claimFailureError{cause: errors.New(msg), aftermath: cancelAftermath(outcome, closeErr)}
	}
	if expiredCleared || time.Now().After(authExpiresAt) {
		result = SyncExecResult{Err: preExecFailure("aborted: caller authorization memory TTL expired before execution", "caller authorization memory TTL expired before execution")}
		return
	}
	if time.Now().After(taskDeadline) {
		result = SyncExecResult{Err: preExecFailure("aborted: task deadline expired while queued", "task deadline expired while queued")}
		return
	}
	// The execution may not outlive its authorization memory TTL nor the
	// task deadline (both anchored at Reserve time).
	execDeadline := taskDeadline
	if authExpiresAt.Before(execDeadline) {
		execDeadline = authExpiresAt
	}
	execCtx, finish := c.beginExecution(t.parentCtx, t.runID, execDeadline)
	defer finish()

	bindFailure := func(cause, msg string, err error) error {
		outcome, closeErr := closeNow(cause)
		if err != nil {
			msg = fmt.Sprintf("%s: %v", msg, err)
		}
		return &claimFailureError{cause: errors.New(msg), aftermath: cancelAftermath(outcome, closeErr)}
	}
	job, err := c.h.Repo.GetSyncJob(execCtx, t.syncID, t.tenant)
	if err != nil {
		result = SyncExecResult{Err: bindFailure("aborted: sync definition could not be read before claim: "+err.Error(), "sync definition could not be read", err)}
		return
	}
	if job == nil {
		result = SyncExecResult{Err: bindFailure("aborted: sync definition is no longer visible in tenant before claim", "sync definition no longer visible before claim", nil)}
		return
	}
	conn, err := c.h.Repo.GetConnectionForTenant(execCtx, job.SourceID, t.tenant)
	if err != nil {
		result = SyncExecResult{Err: bindFailure("aborted: source could not be read before claim: "+err.Error(), "source could not be read", err)}
		return
	}
	if conn == nil {
		result = SyncExecResult{Err: bindFailure("aborted: source is no longer visible in tenant before claim", "source no longer visible before claim", nil)}
		return
	}
	token, err := newLeaseToken()
	if err != nil {
		result = SyncExecResult{Err: bindFailure("aborted: lease token could not be minted", "lease token could not be minted", nil)}
		return
	}
	lease, err := c.h.Repo.ClaimSyncRun(execCtx, t.runID, t.tenant, c.h.leaseHolder(), token, syncLeaseTTL)
	if err != nil {
		// D: a claim storage error leaves the durable outcome genuinely
		// uncertain (the claim may even have committed). Attempt a bounded
		// durable close; whatever it returns — aborted, a persisted but
		// unconfirmed request, too-late, or its own failure — is described
		// verbatim by cancelAftermath. Never fabricated, never silently
		// dropped.
		outcome, closeErr := closeNow("aborted after uncertain claim failure: " + err.Error())
		result = SyncExecResult{Err: &claimFailureError{cause: fmt.Errorf("claim failed: %w", err), aftermath: cancelAftermath(outcome, closeErr)}}
		return
	}
	if lease == nil {
		// The row left pending under us (cancelled in the lock race, or
		// claimed elsewhere): its own state is the record.
		slog.Info("coordinator claim lost", slog.String("sync_run_id", t.runID.String()))
		result = SyncExecResult{Err: errSyncClaimLost}
		return
	}
	run, execErr := c.h.executeSnapshotSync(execCtx, auth, t.tenant, lease, job, conn, !t.syncWait)
	if execErr != nil {
		slog.Warn("sync run did not reach a persisted terminal state",
			slog.String("error", execErr.Error()), slog.String("sync_run_id", t.runID.String()), slog.Bool("async", !t.syncWait))
	} else if run != nil {
		slog.Info("sync run finished", slog.String("status", run.Status), slog.String("sync_run_id", t.runID.String()), slog.Bool("async", !t.syncWait))
	}
	result = SyncExecResult{Run: run, Err: execErr}
}

// Stop performs the shutdown sequence and returns an explicit result shared
// by every concurrent/repeated caller (B): close the admission gate →
// snapshot+drain the queue → IMMEDIATELY cancel the root so every live
// execution (sync requests included) is interrupted and its bounded detached
// terminal write converges → close queued tasks and join all reservations
// under ONE shared budget → bounded worker join. The returned error is nil
// ONLY when everything joined inside the budget; otherwise it names exactly
// what stayed unconfirmed (queued rows left pending, unjoined reservations,
// workers still running) — an honest report, never a fabricated graceful
// completion. Budget-exhausted rows are never given a fake terminal state.
func (c *RunCoordinator) Stop() error {
	c.stopOnce.Do(func() {
		defer close(c.stopDone)
		c.stopErr = c.stopNow()
	})
	<-c.stopDone
	return c.stopErr
}

func (c *RunCoordinator) stopNow() error {
	// 1. Close the gate and snapshot the queued tasks under one lock. From
	// here Reserve/Dispatch refuse; pre-stop reservations remain tracked by
	// gateWG.
	c.gateMu.Lock()
	c.stopped = true
	var queued []coordinatorTask
	for {
		select {
		case t := <-c.tasks:
			queued = append(queued, t)
			continue
		default:
		}
		break
	}
	c.gateMu.Unlock()

	// 2. Interrupt everything live immediately — before any slow queued-close
	// work spends the budget. Every execution context is a child of rootCtx;
	// their terminal writes are detached and bounded, so they converge while
	// the database pool is still open (main closes it only after Stop).
	c.rootCancel()

	// 3. ONE shared drain/join budget for queued closes, the reservation join
	// and the worker join.
	deadline := time.Now().Add(c.cfg.StopBudget)
	var unclosed []uuid.UUID
	// answerSyncWaiter releases a synchronously waiting HTTP handler exactly
	// once (buffered channel): a queued task drained by Stop must never leave
	// its waiter hanging for the full task TTL.
	answerSyncWaiter := func(t coordinatorTask, aftermath string) {
		if t.resultCh != nil {
			select {
			case t.resultCh <- SyncExecResult{Err: &claimFailureError{cause: errors.New("service shutdown before dispatch"), aftermath: aftermath}}:
			default:
			}
		}
	}
	for _, t := range queued {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			// Honest path (D): budget exhausted. Drop the auth reference and
			// the slot; the row stays pending and queryable, cancellable by
			// its owner.
			c.finishRecord(t.runID)
			unclosed = append(unclosed, t.runID)
			slog.Error("coordinator stop budget exhausted before closing queued task; run stays honestly unconfirmed",
				slog.String("sync_run_id", t.runID.String()))
			answerSyncWaiter(t, cancelAftermath("", context.Canceled))
			continue
		}
		perCall := syncLeaseCallTimeout
		if remaining < perCall {
			perCall = remaining
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(c.rootCtx), perCall)
		outcome, err := c.closeTaskDurable(ctx, t, "aborted: service shutdown before dispatch")
		cancel()
		switch {
		case err != nil:
			slog.Error("coordinator could not close queued task during stop; run stays honestly unconfirmed",
				slog.String("error", err.Error()), slog.String("sync_run_id", t.runID.String()))
			unclosed = append(unclosed, t.runID)
		case outcome == repo.CancelOutcomeNotVisible:
			slog.Warn("coordinator stop close: run not visible", slog.String("sync_run_id", t.runID.String()))
		default:
			slog.Info("coordinator closed queued task during stop", slog.String("outcome", string(outcome)), slog.String("sync_run_id", t.runID.String()))
		}
		c.finishRecord(t.runID)
		answerSyncWaiter(t, cancelAftermath(outcome, err))
	}

	// Every wait below obeys the same shared deadline; a wait whose budget is
	// already exhausted reports failure immediately instead of blocking.
	waitBounded := func(ch <-chan struct{}, what string) bool {
		if remaining := time.Until(deadline); remaining > 0 {
			select {
			case <-ch:
				return true
			case <-time.After(remaining):
			}
		}
		slog.Warn("coordinator stop: did not join within the shared budget; reporting honestly", slog.String("what", what))
		return false
	}
	joinedCh := make(chan struct{})
	go func() {
		c.gateWG.Wait()
		close(joinedCh)
	}()
	joined := waitBounded(joinedCh, "reservations")

	wdone := make(chan struct{})
	go func() {
		c.workerWG.Wait()
		close(wdone)
	}()
	workersExited := waitBounded(wdone, "workers")
	c.sweepWG.Wait()

	if len(unclosed) == 0 && joined && workersExited {
		return nil
	}
	var parts []string
	if len(unclosed) > 0 {
		ids := make([]string, 0, len(unclosed))
		for _, id := range unclosed {
			ids = append(ids, id.String())
		}
		parts = append(parts, fmt.Sprintf("%d queued tasks could not be closed and stay pending/queryable: %v", len(unclosed), ids))
	}
	if !joined {
		parts = append(parts, "some reservations (executions) did not join within the stop budget")
	}
	if !workersExited {
		parts = append(parts, "some workers did not exit within the stop budget")
	}
	return errors.New("coordinator stop incomplete: " + strings.Join(parts, "; "))
}

// ensureCoordinator is the lazy default used by handler tests and embedders
// that never wire a service context: a background-rooted coordinator with
// default bounds. Production main.go always installs an explicit one.
func (h *Handlers) ensureCoordinator() *RunCoordinator {
	h.coordinatorOnce.Do(func() {
		if h.Coordinator != nil {
			return
		}
		h.Coordinator = NewRunCoordinator(context.Background(), CoordinatorConfig{}, h)
	})
	return h.Coordinator
}

// coordinatorFor returns the installed coordinator, constructing the lazy
// default when none was wired.
func (h *Handlers) coordinatorFor() *RunCoordinator {
	if h.Coordinator != nil {
		return h.Coordinator
	}
	return h.ensureCoordinator()
}
