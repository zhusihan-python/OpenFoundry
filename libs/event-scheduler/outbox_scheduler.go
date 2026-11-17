package eventscheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openfoundry/openfoundry-go/libs/outbox"
	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
)

// Outbox dispatch mode — T8 decision §1 transaction ①.
//
// The legacy [Scheduler.Tick] publishes the raw payload_template
// straight to Kafka (direct mode). This scheduler instead fires
// scheduling-mode definitions through the transactional outbox:
//
//	ONE Postgres transaction per opportunity:
//	  claim (FOR UPDATE OF d SKIP LOCKED)
//	  → INSERT scheduling.dispatch_audit        (permanent record —
//	    the outbox row is deleted in this same tx, so the audit table
//	    is the durable proof of what fired; its PK is NOT a dedup
//	    store for the outbox path)
//	  → outbox.Enqueue(BuildTriggerEnvelope(...))  (INSERT+DELETE in
//	    the same tx; Debezium relays the WAL INSERT to Kafka)
//	  → advance next_run_at / stamp last_run_at
//	  → COMMIT
//
// Failure policy (root decision 2026-11-17):
//   - PERMANENT bad definition (cron/flavor/tz unparseable, no future
//     fire, incomplete binding, missing grant row, envelope schema
//     violation) → same-transaction quarantine: enabled=false +
//     quarantined_at + sanitized quarantine_reason, commit, continue
//     with the other due rows (V7: no starvation).
//   - REGISTRY transient failure (unreachable / auth / not registered
//     / content drift) → no quarantine: the tick aborts before
//     claiming (fingerprints verified up front), every row stays due,
//     the CronJob retries next minute.
//   - Infra DB error → tick aborts; claimed-but-uncommitted rows stay
//     due (rollback), committed progress is preserved.
//
// The producer never reads scheduling.grants.token_ciphertext — only
// grant_ref and the current generation metadata.

// OutboxTickResult reports what one [OutboxScheduler.TickOutbox] run
// did. Quarantined is informational: quarantining is a SUCCESS action
// for the tick (the bad row is isolated and the rest kept firing), so
// it is not an error.
type OutboxTickResult struct {
	// Fired is the number of opportunities enqueued this tick.
	Fired int
	// Quarantined is the number of definitions isolated this tick.
	Quarantined int
	// FiredEventIDs lists the event ids enqueued (bounded by Fired).
	FiredEventIDs []uuid.UUID
	// QuarantinedIDs lists the quarantined definition ids.
	QuarantinedIDs []uuid.UUID
}

// OutboxScheduler fires dispatch_mode='pipeline_outbox_v1' definitions.
// Stateless across TickOutbox calls — build once, invoke once per
// CronJob run, same operating model as [Scheduler].
type OutboxScheduler struct {
	pg            *pgxpool.Pool
	verifier      SchemaRegistryVerifier
	registryGroup string
	// cdcSlotName, when set, is an additional fail-closed gate: the
	// tick refuses to fire while the CDC replication slot is missing
	// or not active. The outbox INSERT+DELETE pattern is WAL-only —
	// an event enqueued before the connector is streaming cannot be
	// recovered by a later snapshot (snapshot.mode=never and the row
	// is already deleted), so due definitions must NEVER fire before
	// CDC is live (root review 2026-11-17).
	cdcSlotName string
}

// NewOutboxScheduler builds an OutboxScheduler. registryGroup may be
// empty — it then falls back to [DefaultRegistryGroup].
func NewOutboxScheduler(pg *pgxpool.Pool, verifier SchemaRegistryVerifier, registryGroup string) *OutboxScheduler {
	if registryGroup == "" {
		registryGroup = DefaultRegistryGroup
	}
	return &OutboxScheduler{pg: pg, verifier: verifier, registryGroup: registryGroup}
}

// WithCDCSlot opts into the replication-slot readiness gate: the tick
// fails closed with ErrCDCNotReady until pg_replication_slots shows
// slotName active. Returns the scheduler for chaining.
func (s *OutboxScheduler) WithCDCSlot(slotName string) *OutboxScheduler {
	s.cdcSlotName = slotName
	return s
}

// outboxDefinitionRow is one claimed schedules.definitions row in the
// scheduling-mode shape. Binding columns are nullable in the table
// (legacy rows have NULL); a new-mode row with NULL bindings is a bad
// definition to quarantine, not a scan error — hence the pointers.
type outboxDefinitionRow struct {
	id                 uuid.UUID
	name               string
	cronExpr           string
	cronFlavor         string
	timeZone           string
	tenantKind         *string
	tenantID           *uuid.UUID
	scheduleKey        *string
	definitionRevision *int
	pipelineRID        *string
	buildBranch        *string
	grantRef           *uuid.UUID
	nextRunAt          time.Time
	// grantGeneration is NULL when the grant row referenced by
	// grant_ref does not exist (yet) — a broken binding.
	grantGeneration *int
}

// TickOutbox runs one outbox tick: verify registry fingerprints, then
// claim-and-fire every due scheduling-mode definition. See the type
// comment for the exact transaction and failure policy.
func (s *OutboxScheduler) TickOutbox(ctx context.Context, now time.Time) (OutboxTickResult, error) {
	var res OutboxTickResult

	// Registry first, before any claim: a transient registry problem
	// must leave ALL due rows due (retryable), never quarantine.
	// UNCONDITIONAL (root review round 1): a nil verifier is a
	// misconfiguration that fails closed inside the check itself —
	// the call is never skipped.
	if err := s.VerifyRegistryFingerprints(ctx); err != nil {
		return res, err
	}

	// CDC readiness gate (root review round 2): the outbox pattern is
	// WAL-only; an enqueue with no streaming reader is an event lost
	// to a snapshot forever. Opt-in via WithCDCSlot.
	if s.cdcSlotName != "" {
		if err := s.requireCDCSlotActive(ctx); err != nil {
			return res, err
		}
	}

	tickID := uuid.New()
	for {
		tx, err := s.pg.Begin(ctx)
		if err != nil {
			return res, &SchedulerError{Kind: ErrDB, Cause: err}
		}

		row, err := claimOutboxDefinition(ctx, tx, now)
		if err != nil {
			_ = tx.Rollback(ctx)
			return res, classifyPgSchemaError(err)
		}
		if row == nil {
			if err := tx.Commit(ctx); err != nil {
				return res, &SchedulerError{Kind: ErrDB, Cause: err}
			}
			return res, nil
		}

		scheduledFor := row.nextRunAt

		// Permanent per-row failures quarantine inside THIS
		// transaction and continue; infrastructure failures roll
		// back and abort the tick.
		env, next, badReason, infraErr := s.prepareFire(row, scheduledFor, now, tickID)
		if infraErr != nil {
			_ = tx.Rollback(ctx)
			return res, infraErr
		}
		if badReason != "" {
			if err := quarantineDefinition(ctx, tx, row.id, badReason); err != nil {
				_ = tx.Rollback(ctx)
				return res, classifyPgSchemaError(err)
			}
			if err := tx.Commit(ctx); err != nil {
				return res, &SchedulerError{Kind: ErrDB, Cause: err}
			}
			res.Quarantined++
			res.QuarantinedIDs = append(res.QuarantinedIDs, row.id)
			slog.Warn("scheduling definition quarantined",
				slog.String("definition_id", row.id.String()),
				slog.String("name", row.name),
				slog.String("reason", badReason))
			continue
		}

		// Permanent opportunity record. ON CONFLICT DO NOTHING: an
		// operator reset of next_run_at replays the same opportunity
		// (same deterministic event_id) and the audit row already
		// exists — the re-enqueued event is a duplicate the CONSUMER
		// deduplicates via scheduling.dispatches(event_id).
		if _, err := tx.Exec(ctx,
			`INSERT INTO scheduling.dispatch_audit
			   (event_id, definition_id, tenant_kind, tenant_id, schedule_key,
			    definition_revision, scheduled_for)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)
			 ON CONFLICT (event_id) DO NOTHING`,
			env.EventID, row.id, string(tenantKindOf(row)), tenantIDOf(row),
			scheduleKeyOf(row), revisionOf(row), scheduledFor,
		); err != nil {
			_ = tx.Rollback(ctx)
			return res, classifyPgSchemaError(err)
		}

		headers := lineageHeaderMap(row.name, scheduledFor)
		if err := outbox.Enqueue(ctx, tx, &outbox.OutboxEvent{
			EventID:     env.EventID,
			Aggregate:   env.AggregateType,
			AggregateID: env.AggregateID,
			Topic:       scheduleevents.TopicTriggers,
			Payload:     mustMarshalEnvelope(env),
			Headers:     headers,
		}); err != nil {
			_ = tx.Rollback(ctx)
			return res, classifyPgSchemaError(err)
		}

		if _, err := tx.Exec(ctx,
			`UPDATE schedules.definitions
			    SET next_run_at = $1, last_run_at = $2, updated_at = now()
			  WHERE id = $3`,
			next, scheduledFor, row.id,
		); err != nil {
			_ = tx.Rollback(ctx)
			return res, classifyPgSchemaError(err)
		}

		if err := tx.Commit(ctx); err != nil {
			return res, &SchedulerError{Kind: ErrDB, Cause: err}
		}
		res.Fired++
		res.FiredEventIDs = append(res.FiredEventIDs, env.EventID)
	}
}

// requireCDCSlotActive fails closed unless the configured replication
// slot exists AND is active (a connected streaming reader). Checked
// before any claim so a not-yet-running connector leaves every due
// definition due.
//
// pg_replication_slots is CLUSTER-wide: slot_name alone could match a
// same-named slot owned by a different database, so database (vs
// current_database()), slot_type and plugin are all verified (root
// review round 3). Two deliberately distinct situations:
//
//   - slot NEVER created (or mismatched db/type/plugin): outbox
//     INSERT+DELETE events committed meanwhile are unrecoverable —
//     WAL retention was never armed for this slot. Hard deployment
//     ordering: the connector must create the slot and be RUNNING
//     before the first tick fires.
//   - slot exists and matches but is INACTIVE: the slot RETAINS WAL,
//     so events enqueued now are delivered once the connector
//     resumes — this is a conservative wait-state, not data loss.
func (s *OutboxScheduler) requireCDCSlotActive(ctx context.Context) error {
	var currentDB, database, slotType, plugin string
	var active bool
	err := s.pg.QueryRow(ctx,
		`SELECT (SELECT current_database()), database, slot_type, plugin, active
		   FROM pg_replication_slots
		  WHERE slot_name = $1`,
		s.cdcSlotName,
	).Scan(&currentDB, &database, &slotType, &plugin, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		return &SchedulerError{
			Kind: ErrCDCNotReady,
			Name: s.cdcSlotName,
			Cause: fmt.Errorf(
				"replication slot %q not found — if never created, outbox events committed in the meantime are unrecoverable (WAL retention never armed): bring the Debezium connector to RUNNING (slot created) before the first scheduling tick", s.cdcSlotName),
		}
	}
	if err != nil {
		return &SchedulerError{Kind: ErrDB, Cause: err}
	}
	if database != currentDB || slotType != "logical" || plugin != "pgoutput" {
		return &SchedulerError{
			Kind: ErrCDCNotReady,
			Name: s.cdcSlotName,
			Cause: fmt.Errorf(
				"slot %q exists but is not this database's outbox slot (database=%s type=%s plugin=%s; want database=%s type=logical plugin=pgoutput)",
				s.cdcSlotName, database, slotType, plugin, currentDB),
		}
	}
	if !active {
		return &SchedulerError{
			Kind: ErrCDCNotReady,
			Name: s.cdcSlotName,
			Cause: fmt.Errorf(
				"slot %q is correct but inactive (no streaming reader). WAL IS retained by the slot — events enqueued now are delivered when the connector resumes; refusing to fire is a conservative wait-state, not data loss", s.cdcSlotName),
		}
	}
	return nil
}

// prepareFire performs the per-row steps that can fail: cron
// recompute, binding validation, envelope construction. It returns
//
//	env      — the ready trigger envelope (nil if badReason != "")
//	next     — the advanced next_run_at (zero if badReason != "")
//	badReason— non-empty for a PERMANENT bad definition (quarantine)
//	err      — non-nil for INFRASTRUCTURE failures (abort tick)
func (s *OutboxScheduler) prepareFire(row *outboxDefinitionRow, scheduledFor, now time.Time, tickID uuid.UUID) (env *scheduleevents.Envelope, next time.Time, badReason string, err error) {
	def := &ScheduleDefinition{
		ID:         row.id,
		Name:       row.name,
		CronExpr:   row.cronExpr,
		CronFlavor: row.cronFlavor,
		TimeZone:   row.timeZone,
		Enabled:    true,
	}
	// ComputeNextFire surfaces ErrInvalidCron / ErrUnknownFlavor /
	// ErrInvalidTimeZone / ErrNoFutureFire — all permanent.
	nextFire, err := ComputeNextFire(def, now)
	if err != nil {
		return nil, time.Time{}, sanitizeReason(fmt.Sprintf("cron recompute failed: %v", err)), nil
	}

	payload, reason := buildTriggerPayload(row)
	if reason != "" {
		return nil, time.Time{}, reason, nil
	}

	envelope, err := scheduleevents.BuildTriggerEnvelope(
		*payload,
		now.UTC(), // occurred_at = tick time
		fmt.Sprintf("schedule:%s", row.id),
		fmt.Sprintf("tick:%s", tickID),
	)
	if err != nil {
		// Envelope/schema violations (schedule_key pattern, revision
		// bounds, uuid fields...) are permanent definition problems.
		return nil, time.Time{}, sanitizeReason(fmt.Sprintf("trigger envelope invalid: %v", err)), nil
	}
	return envelope, nextFire, "", nil
}

// buildTriggerPayload validates the binding columns and reads the
// grant generation metadata (never the token ciphertext). A missing
// grant row is a broken binding: grant_generation is required by the
// frozen schema and cannot be fabricated.
func buildTriggerPayload(row *outboxDefinitionRow) (p *scheduleevents.TriggerPayload, badReason string) {
	if row.tenantKind == nil || row.tenantID == nil || row.scheduleKey == nil ||
		row.definitionRevision == nil || row.pipelineRID == nil ||
		row.buildBranch == nil || row.grantRef == nil {
		return nil, sanitizeReason("binding incomplete: pipeline_outbox_v1 row missing tenant_kind/tenant_id/schedule_key/definition_revision/pipeline_rid/build_branch/grant_ref")
	}
	if row.grantGeneration == nil {
		return nil, sanitizeReason(fmt.Sprintf("grant row missing for grant_ref %s (cannot read generation)", *row.grantRef))
	}
	kind := scheduleevents.TenantKind(*row.tenantKind)
	if !kind.Valid() {
		return nil, sanitizeReason(fmt.Sprintf("invalid tenant_kind %q", *row.tenantKind))
	}
	return &scheduleevents.TriggerPayload{
		TenantKind:         string(kind),
		TenantID:           row.tenantID.String(),
		ScheduleKey:        *row.scheduleKey,
		DefinitionRevision: *row.definitionRevision,
		ScheduledFor:       row.nextRunAt,
		PipelineRID:        *row.pipelineRID,
		BuildBranch:        *row.buildBranch,
		GrantRef:           *row.grantRef,
		GrantGeneration:    *row.grantGeneration,
	}, ""
}

// claimOutboxDefinition claims the earliest due scheduling-mode row.
// Returns (nil, nil) when nothing is due.
func claimOutboxDefinition(ctx context.Context, tx pgx.Tx, now time.Time) (*outboxDefinitionRow, error) {
	r := tx.QueryRow(ctx, `
		SELECT d.id, d.name, d.cron_expr, d.cron_flavor, d.time_zone,
		       d.tenant_kind, d.tenant_id, d.schedule_key, d.definition_revision,
		       d.pipeline_rid, d.build_branch, d.grant_ref, d.next_run_at,
		       g.generation
		  FROM schedules.definitions d
		  LEFT JOIN scheduling.grants g ON g.id = d.grant_ref
		 WHERE d.dispatch_mode = 'pipeline_outbox_v1'
		   AND d.enabled
		   AND d.next_run_at <= $1
		   AND d.quarantined_at IS NULL
		 ORDER BY d.next_run_at
		 LIMIT 1
		   FOR UPDATE OF d SKIP LOCKED`, now)

	var row outboxDefinitionRow
	err := r.Scan(&row.id, &row.name, &row.cronExpr, &row.cronFlavor, &row.timeZone,
		&row.tenantKind, &row.tenantID, &row.scheduleKey, &row.definitionRevision,
		&row.pipelineRID, &row.buildBranch, &row.grantRef, &row.nextRunAt,
		&row.grantGeneration)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// quarantineDefinition isolates a permanently bad definition in the
// SAME transaction that claimed it: enabled=false plus the quarantine
// columns with a sanitized reason. Cleared only by a verified
// definition update / explicit restore (the definition API, worker A).
func quarantineDefinition(ctx context.Context, tx pgx.Tx, id uuid.UUID, reason string) error {
	_, err := tx.Exec(ctx,
		`UPDATE schedules.definitions
		    SET enabled = false,
		        quarantined_at = now(),
		        quarantine_reason = $2,
		        updated_at = now()
		  WHERE id = $1`,
		id, reason,
	)
	return err
}

// classifyPgSchemaError maps Postgres undefined-object errors from the
// NEW queries onto ErrSchemaMissing with the original error as Cause,
// so an unmigrated database fails closed with an actionable message
// instead of a raw column-not-found.
func classifyPgSchemaError(err error) error {
	if err == nil {
		return nil
	}
	var se *SchedulerError
	if errors.As(err, &se) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) &&
		(pgErr.Code == "42703" /* undefined_column */ || pgErr.Code == "42P01" /* undefined_table */) {
		return &SchedulerError{Kind: ErrSchemaMissing, Cause: err}
	}
	if err != nil {
		return &SchedulerError{Kind: ErrDB, Cause: err}
	}
	return nil
}

// sanitizeReason bounds and flattens a quarantine reason. The producer
// never reads secrets (token ciphertext is not selected), and this
// keeps any future message shape from leaking multi-line payloads into
// the column.
func sanitizeReason(reason string) string {
	r := strings.TrimSpace(reason)
	r = strings.ReplaceAll(r, "\n", " ")
	r = strings.ReplaceAll(r, "\r", " ")
	if len(r) > 512 {
		r = r[:512]
	}
	return r
}

// lineageHeaderMap rebuilds the OpenLineage headers for one fire as a
// plain string map for the outbox row's headers column.
//
// WIRE SHAPE (verified against Debezium 2.7 EventRouter semantics —
// do not "flatten" expectations): with the connector's
// `table.fields.additional.placement: "headers:header"` the map is
// emitted as ONE Kafka record header whose value is the JSON object
// ({"ol-namespace":...,"ol-run-id":...}), NOT as one header per ol-*
// key. Consumers that want lineage must parse that single header.
func lineageHeaderMap(name string, scheduledFor time.Time) map[string]string {
	lineage := BuildLineage(name, scheduledFor)
	out := make(map[string]string, 6)
	for _, h := range lineage.ToKafkaHeaders() {
		out[h.Key] = string(h.Value)
	}
	return out
}

func mustMarshalEnvelope(env *scheduleevents.Envelope) json.RawMessage {
	raw, err := json.Marshal(env)
	if err != nil {
		// Envelope fields are all JSON-marshallable (uuid, time,
		// string, raw message); a failure here is a programming bug.
		panic(fmt.Sprintf("eventscheduler: marshal trigger envelope: %v", err))
	}
	return raw
}

func tenantKindOf(row *outboxDefinitionRow) scheduleevents.TenantKind {
	return scheduleevents.TenantKind(derefString(row.tenantKind))
}

func tenantIDOf(row *outboxDefinitionRow) uuid.UUID {
	if row.tenantID == nil {
		return uuid.Nil
	}
	return *row.tenantID
}

func scheduleKeyOf(row *outboxDefinitionRow) string {
	return derefString(row.scheduleKey)
}

func revisionOf(row *outboxDefinitionRow) int {
	if row.definitionRevision == nil {
		return 0
	}
	return *row.definitionRevision
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
