// Package scheduleevents is the shared event contract for the T8
// pipeline-scheduling wiring (ADR-0037/0038; see
// docs/architecture/data-integration-t8-scheduling-decision.md §2).
//
// It is the single source of truth for BOTH sides of the wiring:
//
//   - the producer side (schedules-tick CLI, owned by the producer
//     worker) imports BuildTriggerEnvelope to construct the event it
//     publishes, and
//   - the consumer side (pipeline-build-service scheduling consumer,
//     owned by the main WP3.2 worker) imports ParseAndValidateTrigger
//     to verify inbound events.
//
// Delivery semantics: at-least-once. Determinism of DeriveEventID is
// what makes redelivery safe: the same (tenant, schedule, revision,
// scheduled_for) opportunity always maps to the same event_id, and the
// consumer deduplicates through its scheduling.dispatches admission
// row — NOT through a record-before-process idempotency store.
//
// Tenant domains (S1 compatibility, T8 decision §2): every identifier
// is qualified by a TenantRef — org (claims OrgID) or personal (claims
// Sub) — so an OrgID and a personal subject that happen to share a
// UUID can never mix domains. Build and publish-key identities derive
// from the event id and inherit the isolation automatically.
//
// Topic split (T8 decision §2, Codex review point 6): trigger events
// ride the pre-provisioned pipeline.builds.v1 topic; outcome events go
// to a dedicated pipeline.schedule.outcomes.v1 topic so a scheduling
// consumer never consumes its own outputs.
package scheduleevents

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v5"
)

// TopicTriggers carries schedule trigger events (pre-provisioned,
// infra/helm/infra/kafka-cluster/values.yaml).
const TopicTriggers = "pipeline.builds.v1"

// TopicOutcomes carries schedule outcome events. Provisioning of this
// topic and its DLQ belongs to the infra worker; the constant here is
// the frozen wire name both sides compile against.
const TopicOutcomes = "pipeline.schedule.outcomes.v1"

// Event types carried on the two topics.
const (
	EventTypeTriggered = "pipeline.schedule.triggered.v1"
	EventTypeOutcome   = "pipeline.schedule.outcome.v1"
)

// AggregateType identifies the schedule aggregate in ADR-0038 terms.
const AggregateType = "pipeline_schedule"

// Out-of-band dispatch terminal states. blocked is TERMINAL (Forbid
// semantics for cross-slot overlap: the opportunity is skipped and is
// never automatically replayed). unknown_publish/reconcile are
// internal-only states that never become outcome events.
const (
	OutcomeSucceeded      = "succeeded"
	OutcomeFailed         = "failed"
	OutcomeSkipped        = "skipped"
	OutcomeBlocked        = "blocked"
	OutcomeUnknownPublish = "unknown_publish"
	OutcomeReconcile      = "reconcile"
)

// TenantKind distinguishes the two S1 tenant domains.
type TenantKind string

const (
	// TenantKindOrg is the organization domain: tenant id = claims OrgID.
	TenantKindOrg TenantKind = "org"
	// TenantKindPersonal is the personal domain: tenant id = claims Sub.
	TenantKindPersonal TenantKind = "personal"
)

// Valid reports whether k is one of the defined kinds.
func (k TenantKind) Valid() bool {
	return k == TenantKindOrg || k == TenantKindPersonal
}

// TenantRef is a fully-qualified tenant identity. The same UUID under
// different kinds denotes DIFFERENT tenants and must never be mixed.
type TenantRef struct {
	Kind TenantKind
	ID   uuid.UUID
}

// String renders the canonical wire form "org:<uuid>" / "personal:<uuid>".
func (r TenantRef) String() string {
	return string(r.Kind) + ":" + r.ID.String()
}

// ParseTenantRef parses the canonical "kind:uuid" form and rejects the
// nil UUID (same rule as TriggerPayload.Tenant).
func ParseTenantRef(s string) (TenantRef, error) {
	kind, id, ok := strings.Cut(s, ":")
	if !ok {
		return TenantRef{}, fmt.Errorf("scheduleevents: tenant ref %q missing kind", s)
	}
	k := TenantKind(kind)
	if !k.Valid() {
		return TenantRef{}, fmt.Errorf("scheduleevents: unknown tenant kind %q", kind)
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return TenantRef{}, fmt.Errorf("scheduleevents: tenant ref id: %w", err)
	}
	if parsed == uuid.Nil {
		return TenantRef{}, errors.New("scheduleevents: tenant id must not be nil")
	}
	return TenantRef{Kind: k, ID: parsed}, nil
}

// uuidNamespace is the well-known OID namespace (RFC 4122 §C), same
// namespace family the event-scheduler lineage run_id uses. The input
// domain below is disjoint from the lineage run_id domain, so the two
// identifiers never collide.
var uuidNamespace = uuid.MustParse("6ba7b812-9dad-11d1-80b4-00c04fd430c8")

//go:embed schema/*.json
var schemaFS embed.FS

var (
	schemaOnce    sync.Once
	triggerSchema *jsonschema.Schema
	outcomeSchema *jsonschema.Schema
	schemaErr     error
)

func compiledSchemas() (trigger, outcome *jsonschema.Schema, err error) {
	schemaOnce.Do(func() {
		t, terr := compileSchema("schema/pipeline.schedule.triggered.v1.json")
		if terr != nil {
			schemaErr = fmt.Errorf("scheduleevents: trigger schema: %w", terr)
			return
		}
		o, oerr := compileSchema("schema/pipeline.schedule.outcome.v1.json")
		if oerr != nil {
			schemaErr = fmt.Errorf("scheduleevents: outcome schema: %w", oerr)
			return
		}
		triggerSchema, outcomeSchema = t, o
	})
	return triggerSchema, outcomeSchema, schemaErr
}

func compileSchema(name string) (*jsonschema.Schema, error) {
	raw, err := schemaFS.ReadFile(name)
	if err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(name, strings.NewReader(string(raw))); err != nil {
		return nil, err
	}
	return compiler.Compile(name)
}

func validateAgainst(schema *jsonschema.Schema, raw []byte) error {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return fmt.Errorf("scheduleevents: decode: %w", err)
	}
	if err := schema.Validate(v); err != nil {
		return fmt.Errorf("scheduleevents: schema violation: %w", err)
	}
	return nil
}

// TriggerPayload is the payload of EventTypeTriggered.
type TriggerPayload struct {
	// TenantKind + TenantID fully qualify the tenant (S1 domains).
	TenantKind         string    `json:"tenant_kind"`
	TenantID           string    `json:"tenant_id"`
	ScheduleKey        string    `json:"schedule_key"`
	DefinitionRevision int       `json:"definition_revision"`
	ScheduledFor       time.Time `json:"scheduled_for"`
	PipelineRID        string    `json:"pipeline_rid"`
	BuildBranch        string    `json:"build_branch"`
	// GrantRef identifies the persistent scheduling grant the producer
	// bound at definition-enabling time, and GrantGeneration is its
	// generation at that moment. The consumer treats the generation as
	// an EXACT match requirement against the grant's current
	// generation: re-granting invalidates every queued/replayed event
	// carrying the old generation. Neither field is trusted on its
	// own — the consumer re-verifies tenant/pipeline/subject/revision
	// and the JWT from its own database state.
	GrantRef        uuid.UUID `json:"grant_ref"`
	GrantGeneration int       `json:"grant_generation"`
}

// Tenant resolves and validates the payload's tenant fields into a
// TenantRef. It errors on unknown kinds, non-UUID ids, or the nil UUID.
func (p *TriggerPayload) Tenant() (TenantRef, error) {
	kind := TenantKind(p.TenantKind)
	if !kind.Valid() {
		return TenantRef{}, fmt.Errorf("scheduleevents: invalid tenant_kind %q", p.TenantKind)
	}
	id, err := uuid.Parse(p.TenantID)
	if err != nil {
		return TenantRef{}, fmt.Errorf("scheduleevents: invalid tenant_id: %w", err)
	}
	if id == uuid.Nil {
		return TenantRef{}, errors.New("scheduleevents: tenant_id must not be nil")
	}
	return TenantRef{Kind: kind, ID: id}, nil
}

// OutcomePayload is the payload of EventTypeOutcome. An outcome event
// is published EXACTLY ONCE per opportunity, and ONLY for a confirmed
// terminal state (succeeded/failed/skipped/blocked).
// unknown_publish/reconcile are INTERNAL observable dispatch states —
// they never become outcome events, so a later recovered success can
// never reuse an event id with a different payload.
type OutcomePayload struct {
	TenantKind         string    `json:"tenant_kind"`
	TenantID           string    `json:"tenant_id"`
	ScheduleKey        string    `json:"schedule_key"`
	DefinitionRevision int       `json:"definition_revision"`
	ScheduledFor       time.Time `json:"scheduled_for"`
	BuildID            uuid.UUID `json:"build_id"`
	State              string    `json:"state"`
	Error              string    `json:"error,omitempty"`
	TriggerEvent       uuid.UUID `json:"trigger_event_id"`
}

// Envelope is the ADR-0038 eight-field event envelope.
type Envelope struct {
	EventID       uuid.UUID       `json:"event_id"`
	EventType     string          `json:"event_type"`
	AggregateID   string          `json:"aggregate_id"`
	AggregateType string          `json:"aggregate_type"`
	OccurredAt    time.Time       `json:"occurred_at"`
	CorrelationID string          `json:"correlation_id"`
	CausationID   string          `json:"causation_id"`
	Payload       json.RawMessage `json:"payload"`
}

// formatScheduledFor renders an instant deterministically for UUID
// derivation: explicit UTC with fixed nanosecond precision (no
// RFC3339Nano trailing-zero trimming, always Z-suffixed).
func formatScheduledFor(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z07:00")
}

// AggregateID returns the canonical aggregate id
// "<kind>:<tenant_uuid>/<schedule>".
func AggregateID(tenant TenantRef, scheduleKey string) string {
	return tenant.String() + "/" + scheduleKey
}

// eventIDInput joins the ADR-0038 tuple parts with unit separators so
// concatenated field values can never alias a different tuple.
func eventIDInput(tenant TenantRef, scheduleKey string, revision int, scheduledFor time.Time, eventType string) string {
	return strings.Join([]string{
		AggregateType,
		AggregateID(tenant, scheduleKey),
		fmt.Sprintf("%d@%s", revision, formatScheduledFor(scheduledFor)),
		eventType,
	}, "\x1f")
}

// DeriveEventID computes the deterministic event UUID v5 for a trigger
// or outcome event. Producer and consumer derive it independently from
// the same tuple, which is what lets the consumer reject forged
// event_ids. The tenant kind is part of the tuple: the same UUID under
// org and personal derives different ids.
func DeriveEventID(tenant TenantRef, scheduleKey string, revision int, scheduledFor time.Time, eventType string) uuid.UUID {
	return uuid.NewSHA1(uuidNamespace, []byte(eventIDInput(tenant, scheduleKey, revision, scheduledFor, eventType)))
}

// DeriveBuildID derives the deterministic build identity for one
// scheduling opportunity FROM the trigger event id. Deriving from the
// event id (rather than from a bare tenant/schedule/scheduled_for
// tuple) guarantees the definition revision AND the tenant kind are
// baked in. The input domain is disjoint from DeriveEventID's.
func DeriveBuildID(triggerEventID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuidNamespace, append([]byte("pipeline_schedule_build\x1f"), triggerEventID[:]...))
}

// DerivePublishKey computes the deterministic keyed-commit key for one
// (opportunity, node) output publish as a FIXED-LENGTH identifier:
// "sch:" + UUIDv5 over (trigger event id, node id). Deriving from the
// trigger event id bakes in the definition revision and tenant kind;
// the fixed UUID form always satisfies the DVS keyed-commit key
// contract (^[A-Za-z0-9._~:=-]{1,200}$ — dataset-versioning
// output_commit.go:25-30) regardless of how long the schedule key or
// node id is. Retries MUST reuse this key byte-for-byte; a changed key
// would break DVS keyed idempotency.
func DerivePublishKey(triggerEventID uuid.UUID, nodeID string) string {
	derived := uuid.NewSHA1(uuidNamespace, []byte(strings.Join([]string{
		"pipeline_schedule_publish",
		triggerEventID.String(),
		nodeID,
	}, "\x1f")))
	return "sch:" + derived.String()
}

// commonValidation enforces the payload semantics shared by the
// constructor and the parser, so a forged message cannot pass checks
// the producer side would have rejected.
func (p *TriggerPayload) commonValidation() error {
	if _, err := p.Tenant(); err != nil {
		return err
	}
	if p.ScheduleKey == "" {
		return errors.New("scheduleevents: schedule_key is required")
	}
	if p.GrantRef == uuid.Nil {
		return errors.New("scheduleevents: grant_ref is required")
	}
	return nil
}

// BuildTriggerEnvelope constructs the fully-enveloped trigger event for
// one scheduling opportunity. The producer MUST call this per fire —
// a statically pre-filled envelope is never re-published as-is across
// opportunities (T8 decision §2).
func BuildTriggerEnvelope(payload TriggerPayload, occurredAt time.Time, correlationID, causationID string) (*Envelope, error) {
	if err := payload.commonValidation(); err != nil {
		return nil, err
	}
	tenant, err := payload.Tenant()
	if err != nil {
		return nil, err
	}
	eventID := DeriveEventID(tenant, payload.ScheduleKey, payload.DefinitionRevision, payload.ScheduledFor, EventTypeTriggered)
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("scheduleevents: marshal payload: %w", err)
	}
	env := &Envelope{
		EventID:       eventID,
		EventType:     EventTypeTriggered,
		AggregateID:   AggregateID(tenant, payload.ScheduleKey),
		AggregateType: AggregateType,
		OccurredAt:    occurredAt,
		CorrelationID: correlationID,
		CausationID:   causationID,
		Payload:       json.RawMessage(rawPayload),
	}
	if err := validateEnvelope(env, EventTypeTriggered); err != nil {
		return nil, err
	}
	return env, nil
}

// ParseAndValidateTrigger decodes an inbound message, validates it
// against the embedded JSON Schema, and re-derives event_id to reject
// forged identifiers (the consumer trusts only events whose event_id
// matches the payload tuple, kind included).
func ParseAndValidateTrigger(raw []byte) (*Envelope, *TriggerPayload, error) {
	trigger, _, err := compiledSchemas()
	if err != nil {
		return nil, nil, err
	}
	if err := validateAgainst(trigger, raw); err != nil {
		return nil, nil, err
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, nil, fmt.Errorf("scheduleevents: decode envelope: %w", err)
	}
	var payload TriggerPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return nil, nil, fmt.Errorf("scheduleevents: decode payload: %w", err)
	}
	if err := payload.commonValidation(); err != nil {
		return nil, nil, err
	}
	tenant, err := payload.Tenant()
	if err != nil {
		return nil, nil, err
	}
	expected := DeriveEventID(tenant, payload.ScheduleKey, payload.DefinitionRevision, payload.ScheduledFor, EventTypeTriggered)
	if env.EventID != expected {
		return nil, nil, fmt.Errorf("scheduleevents: event_id %s does not match derived %s (forged or mismatched tuple)", env.EventID, expected)
	}
	if env.AggregateID != AggregateID(tenant, payload.ScheduleKey) {
		return nil, nil, fmt.Errorf("scheduleevents: aggregate_id %q does not match payload tenant/schedule", env.AggregateID)
	}
	return &env, &payload, nil
}

// BuildOutcomeEnvelope constructs the outcome event the consumer
// publishes (via the transactional outbox, to TopicOutcomes) when a
// dispatch reaches a CONFIRMED terminal state. Callers must not
// publish outcome events for unknown_publish/reconcile dispatches.
// The event id is derived with the real definition revision, so
// opportunities of different revisions never collide on one outcome id.
func BuildOutcomeEnvelope(payload OutcomePayload, occurredAt time.Time, correlationID, causationID string) (*Envelope, error) {
	kind := TenantKind(payload.TenantKind)
	if !kind.Valid() {
		return nil, fmt.Errorf("scheduleevents: invalid tenant_kind %q", payload.TenantKind)
	}
	id, err := uuid.Parse(payload.TenantID)
	if err != nil {
		return nil, fmt.Errorf("scheduleevents: invalid tenant_id %q: %w", payload.TenantID, err)
	}
	if id == uuid.Nil {
		return nil, errors.New("scheduleevents: tenant_id must not be nil")
	}
	tenant := TenantRef{Kind: kind, ID: id}
	// Identity invariants: the referenced trigger event must be the one
	// derived from THIS tuple (revision/kind included), and the build id
	// must derive from that trigger event. Together they block zero
	// values, arbitrary ids, and cross-revision/cross-tenant identities.
	expectedTrigger := DeriveEventID(tenant, payload.ScheduleKey, payload.DefinitionRevision, payload.ScheduledFor, EventTypeTriggered)
	if payload.TriggerEvent != expectedTrigger {
		return nil, fmt.Errorf("scheduleevents: trigger_event_id %s does not match derived %s", payload.TriggerEvent, expectedTrigger)
	}
	if payload.BuildID != DeriveBuildID(expectedTrigger) {
		return nil, fmt.Errorf("scheduleevents: build_id %s does not derive from trigger event %s", payload.BuildID, expectedTrigger)
	}
	eventID := DeriveEventID(tenant, payload.ScheduleKey, payload.DefinitionRevision, payload.ScheduledFor, EventTypeOutcome)
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("scheduleevents: marshal payload: %w", err)
	}
	env := &Envelope{
		EventID:       eventID,
		EventType:     EventTypeOutcome,
		AggregateID:   AggregateID(tenant, payload.ScheduleKey),
		AggregateType: AggregateType,
		OccurredAt:    occurredAt,
		CorrelationID: correlationID,
		CausationID:   causationID,
		Payload:       json.RawMessage(rawPayload),
	}
	if err := validateEnvelope(env, EventTypeOutcome); err != nil {
		return nil, err
	}
	return env, nil
}

func validateEnvelope(env *Envelope, eventType string) error {
	if env.EventType != eventType {
		return fmt.Errorf("scheduleevents: event_type %q != %q", env.EventType, eventType)
	}
	if env.AggregateType != AggregateType {
		return fmt.Errorf("scheduleevents: aggregate_type %q != %q", env.AggregateType, AggregateType)
	}
	whole, err := json.Marshal(env)
	if err != nil {
		return err
	}
	var trigger, outcome *jsonschema.Schema
	trigger, outcome, err = compiledSchemas()
	if err != nil {
		return err
	}
	if eventType == EventTypeTriggered {
		return validateAgainst(trigger, whole)
	}
	return validateAgainst(outcome, whole)
}

// DeriveOutputPublishKey identifies one node dataset output independently.
// JSON tuple encoding preserves boundaries even for arbitrary identifier text.
func DeriveOutputPublishKey(eventID uuid.UUID, nodeID, datasetRID string) string {
	tuple, _ := json.Marshal([]string{"pipeline_schedule_output_publish_v1", eventID.String(), nodeID, datasetRID})
	return "sch:" + uuid.NewSHA1(uuidNamespace, tuple).String()
}
