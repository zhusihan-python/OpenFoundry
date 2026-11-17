// The T8 scheduling consumer. Two loops (T8 decision §4, frozen):
//
//   - The MESSAGE loop admits opportunities: a message is HELD until it
//     reaches a durable outcome (admission decision committed, or the
//     poison budget exhausted + DLQ outbox intent committed). On
//     infrastructure failure the same message is retried with backoff —
//     the loop never polls past an unresolved offset and never commits
//     across one. Kafka does not redeliver committed offsets, so
//     "count then commit" would silently drop poison; the budget is
//     spent retrying THIS message, durably counted by stable
//     (topic,partition,offset) identity.
//   - The EXECUTION loop is an independent persistent worker: it scans
//     admitted/executing/unknown dispatches, claims each under a fresh
//     fence with its CURRENT grant, heartbeats, and drives the runner.
//     Business execution failures are dispatch outcomes, never message
//     retry budget.
package scheduling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"

	"github.com/openfoundry/openfoundry-go/libs/outbox"
	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/postgres"
)

// Runner executes one claimed dispatch through the canonical chain. It
// must finish the dispatch (terminal or unknown_publish) including
// fenced child writes and the outcome outbox event. stop is closed
// when the heartbeat loses the fence (cancel any external work).
type Runner func(ctx context.Context, d postgres.SchedulingDispatch, grant *postgres.SchedulingGrant, stop <-chan struct{}) error

// ConsumerStore is the persistence contract (implemented by
// *postgres.Repository).
type ConsumerStore interface {
	AdmitDispatch(ctx context.Context, p postgres.AdmitParams, outcomeHook postgres.OutcomeHook) (*postgres.AdmissionResult, error)
	ClaimDispatchForExecution(ctx context.Context, eventID uuid.UUID, owner string, ttl time.Duration) (int64, bool, error)
	HeartbeatDispatch(ctx context.Context, eventID uuid.UUID, owner string, fence int64, ttl time.Duration) error
	RecordPoisonAttempt(ctx context.Context, topic string, partition int, offset int64, lastErr string) (int, error)
	GetPoisonState(ctx context.Context, topic string, partition int, offset int64) (state string, attempts int, err error)
	EnqueueDLQViaOutbox(ctx context.Context, topic string, partition int, offset int64, raw []byte, reason string) error
	ExpireStaleExecutions(ctx context.Context, now time.Time) ([]postgres.SchedulingDispatch, error)
	ReconcileDispatch(ctx context.Context, eventID uuid.UUID, owner string, ttl time.Duration) (int64, bool, error)
	GetDispatch(ctx context.Context, eventID uuid.UUID) (*postgres.SchedulingDispatch, error)
	GetGrantByRef(ctx context.Context, grantRef uuid.UUID) (*postgres.SchedulingGrant, error)
}

// ErrNoMessage is returned by MessageSource.Poll when no message is
// currently available (a blocking source waits; a non-blocking one
// idles). It is NOT a tombstone: a tombstone is a MESSAGE with a nil
// value and a valid topic/partition/offset — it must flow into the
// poison budget like any undecodable payload, never be skipped past.
var ErrNoMessage = errors.New("no message available")

// MessageSource is the Kafka subscriber surface. Commit advances the
// durable cursor; a committed offset is never redelivered. Poll
// returns ErrNoMessage (or a ctx error) when there is nothing to
// deliver; a nil raw WITH a valid offset is a tombstone message.
type MessageSource interface {
	Poll(ctx context.Context) (raw []byte, topic string, partition int, offset int64, err error)
	Commit(ctx context.Context, topic string, partition int, offset int64) error
}

// OutcomeHookBuilder builds the same-transaction outcome hook used by
// admission when it records skipped/blocked terminals itself.
type OutcomeHookBuilder func() postgres.OutcomeHook

// Consumer hosts the message loop; ExecutionWorker is the execution
// loop started separately.
type Consumer struct {
	store       ConsumerStore
	source      MessageSource
	hookBuilder OutcomeHookBuilder
	owner       string
	leaseTTL    time.Duration
	heartbeat   time.Duration
	maxAttempts uint32
	log         *slog.Logger
}

func NewConsumer(store ConsumerStore, source MessageSource, hook OutcomeHookBuilder, owner string, leaseTTL, heartbeat time.Duration, maxAttempts uint32, log *slog.Logger) *Consumer {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if maxAttempts > 5 {
		maxAttempts = 5
	}
	return &Consumer{store: store, source: source, hookBuilder: hook, owner: owner,
		leaseTTL: leaseTTL, heartbeat: heartbeat, maxAttempts: maxAttempts, log: log}
}

type message struct {
	raw       []byte
	topic     string
	partition int
	offset    int64
}

// Run polls until ctx is cancelled. Each message is fully resolved
// (durable admission / DLQ) before the next poll; retryable failures
// back off on the SAME message — the loop never advances past an
// unresolved offset.
func (c *Consumer) Run(ctx context.Context) error {
	backoff := 10 * time.Millisecond
	const maxBackoff = 200 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		raw, topic, partition, offset, err := c.source.Poll(ctx)
		if err != nil {
			if errors.Is(err, ErrNoMessage) {
				// Nothing to deliver: idle briefly (blocking sources
				// block inside Poll and rarely land here).
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(20 * time.Millisecond):
				}
				continue
			}
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return ctx.Err()
			}
			c.log.Error("scheduler poll failed", "err", err.Error())
			continue
		}
		if topic == "" && offset == 0 {
			// Defensive: a source returning (nil,"",0,0,nil) violates the
			// contract; treat as no-message rather than skipping.
			continue
		}
		// raw==nil here is a TOMBSTONE: a real message with a nil value.
		// It flows into handle -> parse failure -> poison budget, and is
		// only ever committed after a durable outcome — never skipped.
		msg := message{raw: raw, topic: topic, partition: partition, offset: offset}
		for {
			done, herr := c.handle(ctx, msg)
			if done {
				backoff = 10 * time.Millisecond
				break
			}
			// The durable poison row is the ONLY phase memory: on the
			// next pass handle() re-reads it (and re-parses for the
			// event id) — never in-message cached state.
			c.log.Warn("holding message for retry", "err", errStr(herr), "partition", msg.partition, "offset", msg.offset)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
		}
	}
}

// handle resolves one message. done=true means the offset was
// committed here; done=false means retryable (held — never polled or
// committed past).
//
// Budget semantics (ADR-0038 §5): the maxAttempts budget counts
// APPLICATION processing attempts by stable (topic,partition,offset)
// identity — decode failures AND admission errors alike (a valid
// message whose admission keeps erroring must not retry forever).
// Once the budget is spent, application processing STOPS and only the
// terminal phase retries: verify a durable admission already exists
// (an earlier attempt may have committed but the ACK was lost), else
// commit the (idempotent) DLQ intent, then ACK. If the database is
// unreachable even for that, the message is held — never skipped.
func (c *Consumer) handle(ctx context.Context, msg message) (bool, error) {
	// DURABLE phase check first: the poison row (stable
	// topic/partition/offset identity) survives restarts and held
	// retries. Once the budget is spent, application processing never
	// re-runs; once the DLQ intent is committed, only the ACK remains.
	state, attempts, serr := c.store.GetPoisonState(ctx, msg.topic, msg.partition, msg.offset)
	if serr != nil {
		return false, fmt.Errorf("poison phase read: %w", serr) // hold
	}
	if state == "dlq_enqueued" || state == "dlq_published" {
		if cerr := c.source.Commit(ctx, msg.topic, msg.partition, msg.offset); cerr != nil {
			return false, fmt.Errorf("commit after enqueued dlq: %w", cerr)
		}
		return true, nil
	}
	if uint32(attempts) >= c.maxAttempts {
		// Budget spent: application processing never re-runs. Re-parse
		// (pure decode, not an attempt) to recover the REAL event id —
		// a cached id from a failed pass must never substitute for it.
		if env, _, perr := scheduleevents.ParseAndValidateTrigger(msg.raw); perr == nil {
			return c.terminalPhase(ctx, msg, env.EventID)
		}
		// Undecodable (incl. tombstones): straight to DLQ + ACK.
		return c.dlqAndAck(ctx, msg, "budget spent (undecodable)")
	}
	env, payload, perr := scheduleevents.ParseAndValidateTrigger(msg.raw)
	if perr == nil {
		if _, terr := payload.Tenant(); terr != nil {
			perr = terr
		}
	}
	if perr != nil {
		return c.poison(ctx, msg, perr)
	}
	tenant, _ := payload.Tenant()
	params := postgres.AdmitParams{
		EventID:            env.EventID,
		Tenant:             tenant,
		ScheduleKey:        payload.ScheduleKey,
		DefinitionRevision: payload.DefinitionRevision,
		ScheduledFor:       payload.ScheduledFor,
		PipelineRID:        payload.PipelineRID,
		BuildBranch:        payload.BuildBranch,
		GrantRef:           payload.GrantRef,
		GrantGeneration:    payload.GrantGeneration,
		BuildID:            scheduleevents.DeriveBuildID(env.EventID),
	}

	var hook postgres.OutcomeHook
	if c.hookBuilder != nil {
		hook = c.hookBuilder()
	}
	res, aerr := c.store.AdmitDispatch(ctx, params, hook)
	if aerr == nil {
		// Durable decision (admitted/duplicate/skipped/blocked): the
		// execution worker owns admitted dispatches from here.
		if cerr := c.source.Commit(ctx, msg.topic, msg.partition, msg.offset); cerr != nil {
			return false, fmt.Errorf("commit after admission: %w", cerr)
		}
		c.log.Info("admission resolved", "outcome", string(res.Outcome))
		return true, nil
	}

	// Admission errored: spend one budget unit by stable identity.
	attempts, cerr := c.store.RecordPoisonAttempt(ctx, msg.topic, msg.partition, msg.offset, "admission error: "+aerr.Error())
	if cerr != nil {
		// Cannot even account: hold (DB unreachable) — never advance.
		return false, fmt.Errorf("admission: %v (budget accounting: %w)", aerr, cerr)
	}
	if uint32(attempts) < c.maxAttempts {
		return false, fmt.Errorf("admission attempt %d/%d: %s", attempts, c.maxAttempts, aerr.Error())
	}
	return c.terminalPhase(ctx, msg, env.EventID)
}

// terminalPhase runs ONLY after the processing budget is spent: no more
// application processing. First verify a durable admission already
// exists for this opportunity (an earlier attempt may have committed
// before the error was surfaced — e.g. commit-then-crash on the ACK
// path); if so the offset is simply ACKed. Otherwise commit the
// idempotent DLQ intent, then ACK.
func (c *Consumer) terminalPhase(ctx context.Context, msg message, eventID uuid.UUID) (bool, error) {
	d, derr := c.store.GetDispatch(ctx, eventID)
	switch {
	case derr == nil && d != nil:
		// Durable admission already recorded — ACK, never DLQ.
		if cerr := c.source.Commit(ctx, msg.topic, msg.partition, msg.offset); cerr != nil {
			return false, fmt.Errorf("commit after recovered admission: %w", cerr)
		}
		c.log.Warn("budget spent but durable admission recovered; acked", "partition", msg.partition, "offset", msg.offset)
		return true, nil
	case errors.Is(derr, pgx.ErrNoRows):
		// No dispatch for this event id: safe to dead-letter.
	case derr == nil && d == nil:
		// Fake-store legacy shape; treat as absent.
	default:
		// Lookup itself failed (including cancellation): hold, never
		// guess between dropping and dead-lettering.
		return false, fmt.Errorf("recovery lookup: %w", derr)
	}
	return c.dlqAndAck(ctx, msg, "budget spent")
}

// dlqAndAck commits the idempotent DLQ intent and only then ACKs.
func (c *Consumer) dlqAndAck(ctx context.Context, msg message, reason string) (bool, error) {
	if derr := c.store.EnqueueDLQViaOutbox(ctx, msg.topic, msg.partition, msg.offset, msg.raw, reason); derr != nil {
		return false, fmt.Errorf("dlq enqueue: %w", derr)
	}
	if cerr := c.source.Commit(ctx, msg.topic, msg.partition, msg.offset); cerr != nil {
		return false, fmt.Errorf("commit after dlq: %w", cerr)
	}
	c.log.Error("message dead-lettered after budget", "partition", msg.partition, "offset", msg.offset)
	return true, nil
}

// poison spends the durable budget retrying THIS message; only after
// the DLQ outbox intent commits is the offset committed.
func (c *Consumer) poison(ctx context.Context, msg message, reason error) (bool, error) {
	attempts, err := c.store.RecordPoisonAttempt(ctx, msg.topic, msg.partition, msg.offset, reason.Error())
	if err != nil {
		return false, fmt.Errorf("poison count: %w", err)
	}
	if uint32(attempts) < c.maxAttempts {
		return false, fmt.Errorf("poison attempt %d/%d: %s", attempts, c.maxAttempts, reason.Error())
	}
	// Decode failures have no dispatch to recover — straight to the
	// idempotent DLQ + ACK phase.
	if derr := c.store.EnqueueDLQViaOutbox(ctx, msg.topic, msg.partition, msg.offset, msg.raw, reason.Error()); derr != nil {
		return false, fmt.Errorf("dlq enqueue: %w", derr)
	}
	if cerr := c.source.Commit(ctx, msg.topic, msg.partition, msg.offset); cerr != nil {
		return false, fmt.Errorf("commit after dlq: %w", cerr)
	}
	c.log.Error("poison message dead-lettered", "partition", msg.partition, "offset", msg.offset)
	return true, nil
}

// ExecutionWorker is the independent persistent execution loop: claim
// each recoverable dispatch under a fresh fence with its CURRENT
// grant, heartbeat while the runner works, re-drive reconciliation for
// unknown publishes. Never opens new opportunities.
func (c *Consumer) ExecutionWorker(ctx context.Context, runner Runner) error {
	for {
		if err := c.scanOnce(ctx, runner); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.log.Error("execution scan failed", "err", err.Error())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.heartbeat):
		}
	}
}

func (c *Consumer) scanOnce(ctx context.Context, runner Runner) error {
	stale, err := c.store.ExpireStaleExecutions(ctx, time.Now().UTC())
	if err != nil {
		return err
	}
	for _, d := range stale {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		c.drive(ctx, d, runner)
	}
	return nil
}

func (c *Consumer) drive(ctx context.Context, d postgres.SchedulingDispatch, runner Runner) {
	var fence int64
	var ok bool
	var err error
	if d.State == postgres.DispatchStateUnknownPublish || d.State == postgres.DispatchStateReconcile {
		fence, ok, err = c.store.ReconcileDispatch(ctx, d.EventID, c.owner, c.leaseTTL)
	} else {
		fence, ok, err = c.store.ClaimDispatchForExecution(ctx, d.EventID, c.owner, c.leaseTTL)
	}
	if err != nil || !ok {
		return
	}
	// Fresh state + CURRENT grant under the new fence.
	fresh, gerr := c.store.GetDispatch(ctx, d.EventID)
	if gerr != nil || fresh == nil {
		c.log.Error("reload dispatch after claim failed", "err", errStr(gerr))
		return
	}
	grant, gerr := c.store.GetGrantByRef(ctx, fresh.GrantRef)
	if gerr != nil {
		return
	}

	// Heartbeat until the runner returns; a lost fence stops the world.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := make(chan struct{})
	go func() {
		defer close(stop)
		t := time.NewTicker(c.heartbeat)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				if hbErr := c.store.HeartbeatDispatch(runCtx, d.EventID, c.owner, fence, c.leaseTTL); hbErr != nil {
					c.log.Error("heartbeat lost fence; stopping runner", "err", hbErr.Error())
					cancel()
					return
				}
			}
		}
	}()
	if rerr := runner(runCtx, *fresh, grant, stop); rerr != nil {
		c.log.Error("runner failed", "event", d.EventID.String(), "err", rerr.Error())
	}
}

func errStr(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// OutcomeEnqueueHook is the REAL admission hook (T8 decision §4):
// when admission itself records a terminal skipped/blocked state, the
// outcome event enqueues through the transactional outbox in the SAME
// transaction. Recoverable states never publish.
func OutcomeEnqueueHook(cipher *TokenCipher) func() postgres.OutcomeHook {
	return func() postgres.OutcomeHook {
		return func(ctx context.Context, tx pgx.Tx, res postgres.AdmissionResult) error {
			state := scheduleevents.OutcomeSkipped
			if res.Outcome == postgres.AdmitBlockedOverlap {
				state = scheduleevents.OutcomeBlocked
			}
			d := res.Dispatch
			payload, err := scheduleevents.BuildOutcomeEnvelope(scheduleevents.OutcomePayload{
				TenantKind:         d.TenantKind,
				TenantID:           d.TenantID.String(),
				ScheduleKey:        d.ScheduleKey,
				DefinitionRevision: d.DefinitionRevision,
				ScheduledFor:       d.ScheduledFor,
				BuildID:            d.BuildID,
				State:              state,
				Error:              deref(d.Error),
				TriggerEvent:       d.EventID,
			}, time.Now().UTC(), d.EventID.String(), d.EventID.String())
			if err != nil {
				return fmt.Errorf("outcome envelope: %w", err)
			}
			raw, err := jsonMarshalEnvelope(payload)
			if err != nil {
				return err
			}
			event := outbox.New(payload.EventID, scheduleevents.AggregateType,
				payload.AggregateID, scheduleevents.TopicOutcomes, raw)
			return outbox.Enqueue(ctx, tx, event)
		}
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func jsonMarshalEnvelope(v any) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal outcome: %w", err)
	}
	return raw, nil
}
