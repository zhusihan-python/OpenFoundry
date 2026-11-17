package scheduling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/postgres"
)

type fakeSource struct {
	mu        sync.Mutex
	queue     []fakeMsg
	committed []string
	pollErr   error
}

type fakeMsg struct {
	raw       []byte
	topic     string
	partition int
	offset    int64
}

func (f *fakeSource) Poll(ctx context.Context) ([]byte, string, int, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pollErr != nil {
		return nil, "", 0, 0, f.pollErr
	}
	if len(f.queue) == 0 {
		return nil, "", 0, 0, ErrNoMessage
	}
	m := f.queue[0]
	f.queue = f.queue[1:]
	return m.raw, m.topic, m.partition, m.offset, nil
}

func (f *fakeSource) Commit(ctx context.Context, topic string, partition int, offset int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = append(f.committed, key(topic, partition, offset))
	return nil
}

func key(topic string, partition int, offset int64) string {
	return fmt.Sprintf("%s/%d/%d", topic, partition, offset)
}

type fakeStore struct {
	poisonStateOverride func() (string, int, error)
	lookupIDs           []uuid.UUID

	mu         sync.Mutex
	admitRes   *postgres.AdmissionResult
	admitErr   error
	poison     map[string]int
	dlqed      []string
	dlqErr     error
	runnerHit  int
	admitCalls int
	getCalls   func() (*postgres.SchedulingDispatch, error)
	pstate     map[string]string // durable poison state by key
	pattempts  map[string]int    // durable attempts by key
}

func (f *fakeStore) AdmitDispatch(ctx context.Context, p postgres.AdmitParams, hook postgres.OutcomeHook) (*postgres.AdmissionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.admitCalls++
	if f.admitErr != nil {
		return nil, f.admitErr
	}
	return f.admitRes, nil
}
func (f *fakeStore) ClaimDispatchForExecution(ctx context.Context, id uuid.UUID, owner string, ttl time.Duration) (int64, bool, error) {
	return 1, true, nil
}
func (f *fakeStore) FinishDispatch(ctx context.Context, id uuid.UUID, owner string, fence int64, state, msg string, extra postgres.ExtraTx) error {
	return nil
}
func (f *fakeStore) RecordPoisonAttempt(ctx context.Context, topic string, partition int, offset int64, lastErr string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.poison == nil {
		f.poison = map[string]int{}
	}
	if f.pattempts == nil {
		f.pattempts = map[string]int{}
	}
	f.poison[key(topic, partition, offset)]++
	f.pattempts[key(topic, partition, offset)] = f.poison[key(topic, partition, offset)]
	return f.poison[key(topic, partition, offset)], nil
}
func (f *fakeStore) GetPoisonState(ctx context.Context, topic string, partition int, offset int64) (string, int, error) {
	f.mu.Lock()
	override := f.poisonStateOverride
	f.mu.Unlock()
	if override != nil {
		return override()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pstate == nil {
		f.pstate = map[string]string{}
	}
	if f.pattempts == nil {
		f.pattempts = map[string]int{}
	}
	return f.pstate[key(topic, partition, offset)], f.pattempts[key(topic, partition, offset)], nil
}
func (f *fakeStore) EnqueueDLQViaOutbox(ctx context.Context, topic string, partition int, offset int64, raw []byte, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dlqErr != nil {
		return f.dlqErr
	}
	if f.pstate == nil {
		f.pstate = map[string]string{}
	}
	if f.pstate[key(topic, partition, offset)] == "dlq_enqueued" {
		return nil // idempotent
	}
	f.pstate[key(topic, partition, offset)] = "dlq_enqueued"
	f.dlqed = append(f.dlqed, key(topic, partition, offset))
	return nil
}
func (f *fakeStore) ExpireStaleExecutions(ctx context.Context, now time.Time) ([]postgres.SchedulingDispatch, error) {
	return nil, nil
}
func (f *fakeStore) ReconcileDispatch(ctx context.Context, id uuid.UUID, owner string, ttl time.Duration) (int64, bool, error) {
	return 0, false, nil
}
func (f *fakeStore) HeartbeatDispatch(ctx context.Context, id uuid.UUID, owner string, fence int64, ttl time.Duration) error {
	return nil
}
func (f *fakeStore) GetDispatch(ctx context.Context, id uuid.UUID) (*postgres.SchedulingDispatch, error) {
	f.mu.Lock()
	f.lookupIDs = append(f.lookupIDs, id)
	getter := f.getCalls
	f.mu.Unlock()
	if getter != nil {
		return getter()
	}
	return nil, fmt.Errorf("load dispatch: %w", pgx.ErrNoRows)
}
func (f *fakeStore) GetGrantByRef(ctx context.Context, ref uuid.UUID) (*postgres.SchedulingGrant, error) {
	return nil, nil
}

func newTestConsumer(t *testing.T, store *fakeStore, src *fakeSource, runner Runner) *Consumer {
	t.Helper()
	if runner == nil {
		runner = func(ctx context.Context, d postgres.SchedulingDispatch, g *postgres.SchedulingGrant, stop <-chan struct{}) error {
			store.mu.Lock()
			store.runnerHit++
			store.mu.Unlock()
			return nil
		}
	}
	return NewConsumer(store, src, func() postgres.OutcomeHook { return nil }, "test-worker", time.Minute, 10*time.Millisecond, 5, slog.Default())
}

func validTriggerMessage(t *testing.T) ([]byte, postgres.AdmissionResult) {
	t.Helper()
	tenant := scheduleevents.TenantRef{Kind: scheduleevents.TenantKindOrg, ID: uuid.MustParse("33333333-3333-3333-3333-333333333333")}
	ts := time.Date(2026, 11, 18, 2, 0, 0, 0, time.UTC)
	payload := scheduleevents.TriggerPayload{
		TenantKind: "org", TenantID: tenant.ID.String(), ScheduleKey: "nightly",
		DefinitionRevision: 1, ScheduledFor: ts,
		PipelineRID: "ri.foundry.main.pipeline.11111111-1111-1111-1111-111111111111",
		BuildBranch: "main", GrantRef: uuid.MustParse("22222222-2222-2222-2222-222222222222"), GrantGeneration: 1,
	}
	env, err := scheduleevents.BuildTriggerEnvelope(payload, ts, "c", "k")
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	eventID := scheduleevents.DeriveEventID(tenant, "nightly", 1, ts, scheduleevents.EventTypeTriggered)
	return raw, postgres.AdmissionResult{Outcome: postgres.AdmitAdmitted,
		Dispatch: postgres.SchedulingDispatch{EventID: eventID, State: postgres.DispatchStateAdmitted}}
}

func TestConsumerAdmittedAcksWithoutInlineRunner(t *testing.T) {
	store := &fakeStore{}
	raw, res := validTriggerMessage(t)
	store.admitRes = &res
	src := &fakeSource{queue: []fakeMsg{{raw: raw, topic: "pipeline.builds.v1", partition: 0, offset: 1}}}
	c := newTestConsumer(t, store, src, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = c.Run(ctx)
	src.mu.Lock()
	commits := len(src.committed)
	src.mu.Unlock()
	if commits != 1 {
		t.Fatalf("durable admission must ack exactly once, got %d", commits)
	}
	if store.runnerHit != 0 {
		t.Fatal("the message loop must NOT run the runner inline (execution worker owns it)")
	}
}

func TestConsumerAdmissionFailureDoesNotCommitBeforeBudget(t *testing.T) {
	store := &fakeStore{admitErr: errors.New("db down")}
	raw, _ := validTriggerMessage(t)
	src := &fakeSource{queue: []fakeMsg{{raw: raw, topic: "t", partition: 0, offset: 1}}}
	c := newTestConsumer(t, store, src, nil)
	// Short window: only a couple of held retries happen — well below
	// the budget of 5 — so nothing may be committed or dead-lettered.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_ = c.Run(ctx)
	if len(src.committed) != 0 {
		t.Fatal("below-budget admission failures must not commit the offset")
	}
	if len(store.dlqed) != 0 {
		t.Fatal("below-budget admission failures must not dead-letter")
	}
	if store.runnerHit != 0 {
		t.Fatal("runner must not fire on admission failure")
	}
}

func TestConsumerDuplicateSkipsRunnerButCommits(t *testing.T) {
	raw, _ := validTriggerMessage(t)
	store := &fakeStore{admitRes: &postgres.AdmissionResult{Outcome: postgres.AdmitDuplicateTerminal}}
	src := &fakeSource{queue: []fakeMsg{{raw: raw, topic: "t", partition: 0, offset: 7}}}
	c := newTestConsumer(t, store, src, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = c.Run(ctx)
	if store.runnerHit != 0 {
		t.Fatal("duplicate must skip the runner")
	}
	if len(src.committed) != 1 {
		t.Fatalf("duplicate should commit once, got %d", len(src.committed))
	}
}

func TestConsumerPoisonDLQAfterBudget(t *testing.T) {
	store := &fakeStore{}
	// ONE bad delivery: the consumer must HOLD it, retrying in-process
	// until the durable budget is spent, then DLQ+ack. Kafka never
	// redelivers a committed offset — an early commit would lose it.
	src := &fakeSource{queue: []fakeMsg{
		{raw: []byte("garbage"), topic: "pipeline.builds.v1", partition: 3, offset: 42},
	}}
	c := NewConsumer(store, src, func() postgres.OutcomeHook { return nil }, "w", time.Minute, 5*time.Millisecond, 2, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = c.Run(ctx)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.poison[key("pipeline.builds.v1", 3, 42)] != 2 {
		t.Fatalf("budget of 2 spent on the SAME message, got %d", store.poison[key("pipeline.builds.v1", 3, 42)])
	}
	if len(store.dlqed) != 1 {
		t.Fatalf("exactly one DLQ after budget, got %d", len(store.dlqed))
	}
	if len(src.committed) != 1 {
		t.Fatalf("single commit, only after DLQ: got %d", len(src.committed))
	}
	if store.runnerHit != 0 {
		t.Fatal("poison must never reach the runner")
	}
}

// The PG-transient sequence: admission fails (hold+retry, NO ack, no
// advance past the offset), then succeeds; the NEXT message of the
// same partition processes normally afterward.
func TestConsumerTransientAdmissionFailureHoldsThenProceeds(t *testing.T) {
	store := &fakeStore{}
	raw, res := validTriggerMessage(t)
	nextRaw, nextRes := validTriggerMessage(t) // different tuple => distinct event
	nextRes.Outcome = postgres.AdmitDuplicateTerminal
	src := &fakeSource{queue: []fakeMsg{
		{raw: raw, topic: "pipeline.builds.v1", partition: 0, offset: 5},
		{raw: nextRaw, topic: "pipeline.builds.v1", partition: 0, offset: 6},
	}}
	c := newTestConsumer(t, store, src, nil)

	// First admission attempt fails transiently.
	store.mu.Lock()
	store.admitErr = errors.New("db down")
	store.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(ctx) }()

	// Let it hold+retry the first message once, then heal.
	time.Sleep(30 * time.Millisecond)
	store.mu.Lock()
	store.admitErr = nil
	store.admitRes = &res
	store.mu.Unlock()

	deadline := time.After(3 * time.Second)
	for {
		src.mu.Lock()
		committed := append([]string(nil), src.committed...)
		src.mu.Unlock()
		if len(committed) == 2 { // offset 5 (after heal) then 6
			cancel()
			<-done
			if committed[0] != key("pipeline.builds.v1", 0, 5) || committed[1] != key("pipeline.builds.v1", 0, 6) {
				t.Fatalf("commit order must follow offsets 5 then 6: %v", committed)
			}
			return
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("held-then-healed sequence did not complete: %v", committed)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Budget covers valid-but-erroring admissions: exactly maxAttempts
// AdmitDispatch calls, then application processing STOPS (no further
// admission calls), no dispatch exists, DLQ commits, offset ACKed once.
func TestConsumerAdmissionErrorBudgetThenDLQ(t *testing.T) {
	store := &fakeStore{admitErr: errors.New("db flaky")}
	raw, _ := validTriggerMessage(t)
	src := &fakeSource{queue: []fakeMsg{{raw: raw, topic: "t", partition: 0, offset: 9}}}
	store.getCalls = func() (*postgres.SchedulingDispatch, error) { return nil, nil } // no dispatch
	c := NewConsumer(store, src, func() postgres.OutcomeHook { return nil }, "w", time.Minute, 5*time.Millisecond, 3, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.Run(ctx)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.admitCalls != 3 {
		t.Fatalf("admission must be attempted exactly maxAttempts=3 times, got %d", store.admitCalls)
	}
	if len(store.dlqed) != 1 {
		t.Fatalf("exactly one DLQ, got %d", len(store.dlqed))
	}
	src.mu.Lock()
	commits := len(src.committed)
	src.mu.Unlock()
	if commits != 1 {
		t.Fatalf("single ACK after DLQ, got %d", commits)
	}
}

// Terminal-phase recovery: budget spent, but a durable admission
// EXISTS (commit-then-crash before ACK) — ACK directly, never DLQ.
func TestConsumerBudgetSpentButDurableAdmissionRecovered(t *testing.T) {
	raw, res := validTriggerMessage(t)
	store := &fakeStore{admitErr: errors.New("db flaky")}
	src := &fakeSource{queue: []fakeMsg{{raw: raw, topic: "t", partition: 1, offset: 3}}}
	store.getCalls = func() (*postgres.SchedulingDispatch, error) {
		d := res.Dispatch
		return &d, nil // durable admission exists
	}
	c := NewConsumer(store, src, func() postgres.OutcomeHook { return nil }, "w", time.Minute, 5*time.Millisecond, 2, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.Run(ctx)
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.dlqed) != 0 {
		t.Fatal("recovered admission must NOT be dead-lettered")
	}
	src.mu.Lock()
	commits := len(src.committed)
	src.mu.Unlock()
	if commits != 1 {
		t.Fatalf("recovered admission ACKs once, got %d", commits)
	}
}

// Terminal-phase persistence failure (DB down for DLQ write): message
// held; after healing the idempotent DLQ commits and the offset ACKs —
// and application processing never resumes.
func TestConsumerTerminalPhaseHoldsAcrossOutage(t *testing.T) {
	raw, _ := validTriggerMessage(t)
	store := &fakeStore{admitErr: errors.New("db down")}
	src := &fakeSource{queue: []fakeMsg{{raw: raw, topic: "t", partition: 2, offset: 4}}}
	store.getCalls = func() (*postgres.SchedulingDispatch, error) { return nil, nil }
	c := NewConsumer(store, src, func() postgres.OutcomeHook { return nil }, "w", time.Minute, 5*time.Millisecond, 2, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(ctx) }()
	// Spend the budget with DLQ persistently failing.
	store.mu.Lock()
	store.dlqErr = errors.New("outbox unreachable")
	store.mu.Unlock()
	time.Sleep(60 * time.Millisecond)
	store.mu.Lock()
	admitsDuringOutage := store.admitCalls
	dlqErr := store.dlqErr
	store.dlqErr = nil
	store.mu.Unlock()
	if admitsDuringOutage > 2 {
		t.Fatalf("application processing must stop at budget, got %d calls", admitsDuringOutage)
	}
	_ = dlqErr
	deadline := time.After(3 * time.Second)
	for {
		store.mu.Lock()
		dlqed := len(store.dlqed)
		store.mu.Unlock()
		src.mu.Lock()
		commits := len(src.committed)
		src.mu.Unlock()
		if dlqed == 1 && commits == 1 {
			cancel()
			<-done
			store.mu.Lock()
			finalAdmits := store.admitCalls
			store.mu.Unlock()
			if finalAdmits > 2 {
				t.Fatalf("no admission retry after budget, got %d", finalAdmits)
			}
			return
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("healed terminal phase did not complete: dlq=%d commits=%d", dlqed, commits)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Restart semantics: budget spent, DLQ write failed; a FRESH consumer
// (new process, same durable poison state) must NOT re-enter
// admission — it goes straight to the terminal phase, and once the
// DLQ intent finally commits, only the ACK remains.
func TestConsumerRestartAfterTerminalFailureSkipsAdmission(t *testing.T) {
	raw, _ := validTriggerMessage(t)
	store := &fakeStore{admitErr: errors.New("db flaky")}
	src := &fakeSource{queue: []fakeMsg{{raw: raw, topic: "t", partition: 4, offset: 11}}}
	mk := func() *Consumer {
		return NewConsumer(store, src, func() postgres.OutcomeHook { return nil }, "w", time.Minute, 5*time.Millisecond, 2, slog.Default())
	}
	// Phase 1: burn the budget; DLQ persistently fails -> held.
	store.dlqErr = errors.New("outbox down")
	ctx1, cancel1 := context.WithTimeout(context.Background(), 120*time.Millisecond)
	_ = mk().Run(ctx1)
	cancel1()
	store.mu.Lock()
	admitsAfterPhase1 := store.admitCalls
	store.dlqErr = nil
	store.mu.Unlock()
	if admitsAfterPhase1 != 2 {
		t.Fatalf("budget=2 admissions expected, got %d", admitsAfterPhase1)
	}
	// Phase 2: "restart" — fresh consumer, same durable state.
	src.queue = append(src.queue, fakeMsg{raw: raw, topic: "t", partition: 4, offset: 11})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel2()
	_ = mk().Run(ctx2)
	store.mu.Lock()
	admitsAfterPhase2 := store.admitCalls
	dlqed := len(store.dlqed)
	store.mu.Unlock()
	if admitsAfterPhase2 != admitsAfterPhase1 {
		t.Fatalf("restart must not re-run admission: %d -> %d", admitsAfterPhase1, admitsAfterPhase2)
	}
	if dlqed != 1 {
		t.Fatalf("exactly one DLQ after heal, got %d", dlqed)
	}
}

// A Kafka TOMBSTONE (nil value, valid offset) must never be skipped
// past: it consumes the poison budget like any undecodable payload and
// is ACKed only after the DLQ intent commits.
func TestConsumerTombstoneConsumesBudget(t *testing.T) {
	store := &fakeStore{}
	src := &fakeSource{queue: []fakeMsg{
		{raw: nil, topic: "pipeline.builds.v1", partition: 5, offset: 77},
	}}
	c := NewConsumer(store, src, func() postgres.OutcomeHook { return nil }, "w", time.Minute, 5*time.Millisecond, 2, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_ = c.Run(ctx)
	store.mu.Lock()
	poisonCount := store.poison[key("pipeline.builds.v1", 5, 77)]
	dlqed := len(store.dlqed)
	store.mu.Unlock()
	src.mu.Lock()
	commits := len(src.committed)
	src.mu.Unlock()
	if poisonCount != 2 {
		t.Fatalf("tombstone must spend the budget, got %d", poisonCount)
	}
	if dlqed != 1 {
		t.Fatalf("tombstone must dead-letter once, got %d", dlqed)
	}
	if commits != 1 {
		t.Fatalf("tombstone ACKs once after DLQ, got %d", commits)
	}
}

// Regression for the stale-cached-id P1: durable budget already spent,
// a REAL dispatch exists, and the first phase/lookup hits a transient
// infrastructure error. After recovery the terminal phase must use the
// RE-PARSED real event id (never a cached uuid.Nil): Admit 0, DLQ 0,
// ACK 1.
func TestConsumerRecoveryUsesReparsedEventID(t *testing.T) {
	raw, res := validTriggerMessage(t)
	realID := res.Dispatch.EventID
	store := &fakeStore{admitErr: errors.New("should never be called")}
	src := &fakeSource{queue: []fakeMsg{{raw: raw, topic: "t", partition: 6, offset: 21}}}
	// Durable budget already spent for this offset.
	store.pattempts = map[string]int{key("t", 6, 21): 5}
	store.pstate = map[string]string{key("t", 6, 21): "counting"}
	// GetPoisonState fails ONCE (transient), then reports the spent budget.
	poisonFails := int32(1)
	store.poisonStateOverride = func() (string, int, error) {
		if atomic.LoadInt32(&poisonFails) > 0 {
			atomic.StoreInt32(&poisonFails, 0)
			return "", 0, errors.New("db blink")
		}
		return "counting", 5, nil
	}
	// GetDispatch: first call infrastructure error, then the real row.
	// Record every id it was called with.
	lookupFails := int32(1)
	store.getCalls = func() (*postgres.SchedulingDispatch, error) {
		if atomic.LoadInt32(&lookupFails) > 0 {
			atomic.StoreInt32(&lookupFails, 0)
			return nil, errors.New("db blink")
		}
		d := res.Dispatch
		return &d, nil
	}
	store.lookupIDs = nil
	c := NewConsumer(store, src, func() postgres.OutcomeHook { return nil }, "w", time.Minute, 5*time.Millisecond, 5, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_ = c.Run(ctx)

	store.mu.Lock()
	admits, dlqed, lookups := store.admitCalls, len(store.dlqed), append([]uuid.UUID(nil), store.lookupIDs...)
	store.mu.Unlock()
	src.mu.Lock()
	commits := len(src.committed)
	src.mu.Unlock()
	if admits != 0 {
		t.Fatalf("durable budget spent: admission must never run, got %d", admits)
	}
	if dlqed != 0 {
		t.Fatal("a real dispatch exists: must NOT be dead-lettered")
	}
	if commits != 1 {
		t.Fatalf("recovered admission ACKs exactly once, got %d", commits)
	}
	if len(lookups) == 0 {
		t.Fatal("terminal phase must have looked the dispatch up")
	}
	for _, id := range lookups {
		if id != realID {
			t.Fatalf("lookup used stale id %v, want real %v", id, realID)
		}
	}
}
