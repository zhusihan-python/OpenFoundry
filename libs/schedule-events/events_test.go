package scheduleevents

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("parse time %q: %v", s, err)
	}
	return ts
}

// sharedUUID is used verbatim under BOTH tenant kinds in the domain
// isolation tests: the same UUID under org and personal is two
// DIFFERENT tenants.
var sharedUUID = uuid.MustParse("33333333-3333-3333-3333-333333333333")

func sampleTenant() TenantRef {
	return TenantRef{Kind: TenantKindOrg, ID: sharedUUID}
}

func samplePayload() TriggerPayload {
	return TriggerPayload{
		TenantKind:         "org",
		TenantID:           sharedUUID.String(),
		ScheduleKey:        "nightly-refresh",
		DefinitionRevision: 3,
		ScheduledFor:       time.Date(2026, 11, 18, 2, 0, 0, 0, time.UTC),
		PipelineRID:        "ri.foundry.main.pipeline.11111111-1111-1111-1111-111111111111",
		BuildBranch:        "main",
		GrantRef:           uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		GrantGeneration:    2,
	}
}

func TestBuildAndParseRoundTrip(t *testing.T) {
	payload := samplePayload()
	now := time.Date(2026, 11, 18, 2, 0, 1, 500, time.UTC)
	env, err := BuildTriggerEnvelope(payload, now, "corr-1", "cause-1")
	if err != nil {
		t.Fatalf("BuildTriggerEnvelope: %v", err)
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	parsedEnv, parsedPayload, err := ParseAndValidateTrigger(raw)
	if err != nil {
		t.Fatalf("ParseAndValidateTrigger: %v", err)
	}
	if parsedEnv.EventID != env.EventID {
		t.Fatalf("event id mismatch: %s vs %s", parsedEnv.EventID, env.EventID)
	}
	if parsedPayload.ScheduleKey != payload.ScheduleKey {
		t.Fatalf("payload mismatch: %+v", parsedPayload)
	}
	if !parsedPayload.ScheduledFor.Equal(payload.ScheduledFor) {
		t.Fatalf("scheduled_for mismatch: %v vs %v", parsedPayload.ScheduledFor, payload.ScheduledFor)
	}
	if parsedPayload.GrantRef != payload.GrantRef {
		t.Fatalf("grant ref mismatch: %v", parsedPayload.GrantRef)
	}
}

func TestBuildRequiresGrantRefAndTenantFields(t *testing.T) {
	p := samplePayload()
	p.GrantRef = uuid.Nil
	if _, err := BuildTriggerEnvelope(p, time.Now().UTC(), "c", "k"); err == nil {
		t.Fatal("nil grant_ref must be rejected at construction")
	}
	p = samplePayload()
	p.TenantKind = "team"
	if _, err := BuildTriggerEnvelope(p, time.Now().UTC(), "c", "k"); err == nil {
		t.Fatal("unknown tenant kind must be rejected at construction")
	}
	p = samplePayload()
	p.TenantID = "not-a-uuid"
	if _, err := BuildTriggerEnvelope(p, time.Now().UTC(), "c", "k"); err == nil {
		t.Fatal("non-uuid tenant id must be rejected at construction")
	}
	p = samplePayload()
	p.TenantID = uuid.Nil.String()
	if _, err := BuildTriggerEnvelope(p, time.Now().UTC(), "c", "k"); err == nil {
		t.Fatal("nil tenant uuid must be rejected at construction")
	}
}

// TestTenantKindDomainIsolation covers ⑫: the same UUID under org and
// personal are different tenants and can never collide on aggregate id,
// event id, or parse acceptance of a cross-kind aggregate.
func TestTenantKindDomainIsolation(t *testing.T) {
	orgRef := TenantRef{Kind: TenantKindOrg, ID: sharedUUID}
	personalRef := TenantRef{Kind: TenantKindPersonal, ID: sharedUUID}
	ts := mustTime(t, "2026-11-18T02:00:00Z")

	if AggregateID(orgRef, "s") == AggregateID(personalRef, "s") {
		t.Fatal("org and personal aggregate ids must differ for the same UUID")
	}
	eo := DeriveEventID(orgRef, "s", 1, ts, EventTypeTriggered)
	ep := DeriveEventID(personalRef, "s", 1, ts, EventTypeTriggered)
	if eo == ep {
		t.Fatal("org and personal event ids must differ for the same UUID")
	}
	if bo, bp := DeriveBuildID(eo), DeriveBuildID(ep); bo == bp {
		t.Fatal("build ids inherit the kind isolation via the event id")
	}

	// Wire form: an org event must not validate with a personal
	// aggregate_id, and vice versa.
	orgEnv, err := BuildTriggerEnvelope(samplePayload(), ts, "c", "k")
	if err != nil {
		t.Fatalf("build org envelope: %v", err)
	}
	raw, _ := json.Marshal(orgEnv)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	m["aggregate_id"] = AggregateID(personalRef, "nightly-refresh")
	cross, _ := json.Marshal(m)
	if _, _, err := ParseAndValidateTrigger(cross); err == nil {
		t.Fatal("cross-kind aggregate_id must be rejected")
	}

	// Round-trip through the canonical string form.
	parsed, err := ParseTenantRef(orgRef.String())
	if err != nil || parsed != orgRef {
		t.Fatalf("tenant ref round trip: %v %+v", err, parsed)
	}
	if _, err := ParseTenantRef("team:" + sharedUUID.String()); err == nil {
		t.Fatal("unknown kind must fail ParseTenantRef")
	}
	if _, err := ParseTenantRef("org:" + uuid.Nil.String()); err == nil {
		t.Fatal("nil uuid must fail ParseTenantRef")
	}
}

func TestParseRejectsZeroGrantRef(t *testing.T) {
	payload := samplePayload()
	env, err := BuildTriggerEnvelope(payload, time.Now().UTC(), "c", "k")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	raw, _ := json.Marshal(env)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	var p map[string]any
	_ = json.Unmarshal(env.Payload, &p)
	// event_id does NOT include grant_ref, so only the shared semantic
	// validation can catch this forgery.
	p["grant_ref"] = uuid.Nil.String()
	pj, _ := json.Marshal(p)
	m["payload"] = json.RawMessage(pj)
	tampered, _ := json.Marshal(m)
	if _, _, err := ParseAndValidateTrigger(tampered); err == nil {
		t.Fatal("zero grant_ref must be rejected by the parser")
	}
}

func TestParseRejectsForgedEventID(t *testing.T) {
	payload := samplePayload()
	env, err := BuildTriggerEnvelope(payload, time.Now().UTC(), "c", "k")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	raw, _ := json.Marshal(env)
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var p map[string]any
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	p["schedule_key"] = "other-schedule"
	pj, _ := json.Marshal(p)
	m["payload"] = json.RawMessage(pj)
	tampered, _ := json.Marshal(m)
	if _, _, err := ParseAndValidateTrigger(tampered); err == nil {
		t.Fatal("expected forged event_id to be rejected")
	}
}

func TestParseRejectsSchemaViolation(t *testing.T) {
	raw := []byte(`{"event_id":"not-a-uuid"}`)
	if _, _, err := ParseAndValidateTrigger(raw); err == nil {
		t.Fatal("expected schema violation to be rejected")
	}
}

func TestDeriveEventIDDeterministicAndDisjoint(t *testing.T) {
	ref := sampleTenant()
	ts := mustTime(t, "2026-11-18T02:00:00.123456789Z")
	a := DeriveEventID(ref, "nightly", 1, ts, EventTypeTriggered)
	b := DeriveEventID(ref, "nightly", 1, ts, EventTypeTriggered)
	if a != b {
		t.Fatalf("derivation not deterministic: %s vs %s", a, b)
	}
	if c := DeriveEventID(ref, "nightly", 1, ts.Add(time.Minute), EventTypeTriggered); a == c {
		t.Fatal("different scheduled_for must derive a different event id")
	}
	if d := DeriveEventID(ref, "nightly", 2, ts, EventTypeTriggered); a == d {
		t.Fatal("different revision must derive a different event id")
	}
	if e := DeriveEventID(ref, "nightly", 1, ts, EventTypeOutcome); a == e {
		t.Fatal("trigger and outcome ids must be distinct")
	}
}

func TestDeriveBuildIDIncludesRevisionViaEventID(t *testing.T) {
	ref := sampleTenant()
	ts := mustTime(t, "2026-11-18T02:00:00Z")
	rev1 := DeriveEventID(ref, "s", 1, ts, EventTypeTriggered)
	rev2 := DeriveEventID(ref, "s", 2, ts, EventTypeTriggered)
	b1 := DeriveBuildID(rev1)
	b2 := DeriveBuildID(rev2)
	if b1 == b2 {
		t.Fatal("same opportunity of different revisions must derive different build ids")
	}
	if b3 := DeriveBuildID(rev1); b3 != b1 {
		t.Fatal("build id derivation must be deterministic")
	}
	if b1 == rev1 || b1 == rev2 {
		t.Fatal("build id must never equal an event id")
	}
}

func TestScheduledForFormattingStableAcrossZeroNanos(t *testing.T) {
	withNanos := time.Date(2026, 11, 18, 2, 0, 0, 123000000, time.UTC)
	zero := time.Date(2026, 11, 18, 2, 0, 0, 0, time.UTC)
	s1 := formatScheduledFor(withNanos)
	s2 := formatScheduledFor(zero)
	if !strings.HasSuffix(s1, ".123000000Z") {
		t.Fatalf("unexpected format: %s", s1)
	}
	if !strings.HasSuffix(s2, ".000000000Z") {
		t.Fatalf("unexpected format: %s", s2)
	}
}

func TestDerivePublishKeyFixedLengthAndRevisionSensitive(t *testing.T) {
	ref := sampleTenant()
	ts := mustTime(t, "2026-11-18T02:00:00Z")
	rev1 := DeriveEventID(ref, "s", 1, ts, EventTypeTriggered)
	rev2 := DeriveEventID(ref, "s", 2, ts, EventTypeTriggered)
	k1 := DerivePublishKey(rev1, "node-1")
	k2 := DerivePublishKey(rev1, "node-1")
	if k1 != k2 {
		t.Fatal("publish key must be deterministic")
	}
	if k3 := DerivePublishKey(rev1, "node-2"); k1 == k3 {
		t.Fatal("different nodes must derive different keys")
	}
	if k4 := DerivePublishKey(rev2, "node-1"); k1 == k4 {
		t.Fatal("different revisions must derive different publish keys")
	}
	// DVS keyed-commit key contract: ^[A-Za-z0-9._~:=-]{1,200}$.
	if len(k1) != len("sch:")+36 || len(k1) > 200 {
		t.Fatalf("publish key must be fixed short length, got %d (%q)", len(k1), k1)
	}
	if !strings.HasPrefix(k1, "sch:") || strings.ContainsAny(k1, "/+@ ") {
		t.Fatalf("publish key violates DVS key pattern: %q", k1)
	}
	// Long node ids must not change the key shape.
	long := strings.Repeat("n", 500)
	kl := DerivePublishKey(rev1, long)
	if len(kl) != len(k1) {
		t.Fatalf("long node id changed key length: %d vs %d", len(kl), len(k1))
	}
}

// derivedOutcome builds an OutcomePayload whose TriggerEvent/BuildID
// are the legitimate derived identities for (kind, revision).
func derivedOutcome(kind TenantKind, revision int) OutcomePayload {
	ref := TenantRef{Kind: kind, ID: sharedUUID}
	ts := time.Date(2026, 11, 18, 2, 0, 0, 0, time.UTC)
	trigger := DeriveEventID(ref, "nightly", revision, ts, EventTypeTriggered)
	return OutcomePayload{
		TenantKind:         string(kind),
		TenantID:           sharedUUID.String(),
		ScheduleKey:        "nightly",
		DefinitionRevision: revision,
		ScheduledFor:       ts,
		BuildID:            DeriveBuildID(trigger),
		State:              OutcomeSucceeded,
		TriggerEvent:       trigger,
	}
}

func TestOutcomeEnvelopeValidatesAndRevisionDistinct(t *testing.T) {
	base := derivedOutcome(TenantKindOrg, 1)
	env1, err := BuildOutcomeEnvelope(base, time.Now().UTC(), "c", "k")
	if err != nil {
		t.Fatalf("BuildOutcomeEnvelope: %v", err)
	}
	rev2 := derivedOutcome(TenantKindOrg, 2)
	env2, err := BuildOutcomeEnvelope(rev2, time.Now().UTC(), "c", "k")
	if err != nil {
		t.Fatalf("BuildOutcomeEnvelope rev2: %v", err)
	}
	if env1.EventID == env2.EventID {
		t.Fatal("outcome event ids must differ across definition revisions")
	}
	// Kind isolation on outcome ids too.
	personal := derivedOutcome(TenantKindPersonal, 1)
	env3, err := BuildOutcomeEnvelope(personal, time.Now().UTC(), "c", "k")
	if err != nil {
		t.Fatalf("BuildOutcomeEnvelope personal: %v", err)
	}
	if env1.EventID == env3.EventID {
		t.Fatal("outcome event ids must differ across tenant kinds")
	}
	// Identity invariants: the trigger ref must derive from THIS tuple.
	crossRev := base
	crossRev.TriggerEvent = triggerFrom(TenantKindOrg, 2) // rev-2 trigger under a rev-1 payload
	if _, err := BuildOutcomeEnvelope(crossRev, time.Now().UTC(), "c", "k"); err == nil {
		t.Fatal("cross-revision trigger_event_id must be rejected")
	}
	arbitraryBuild := base
	arbitraryBuild.BuildID = uuid.New()
	if _, err := BuildOutcomeEnvelope(arbitraryBuild, time.Now().UTC(), "c", "k"); err == nil {
		t.Fatal("non-derived build_id must be rejected")
	}
	// Internal-only states never validate as outcome events.
	for _, state := range []string{OutcomeUnknownPublish, OutcomeReconcile, "not-a-state"} {
		bad := base
		bad.State = state
		if _, err := BuildOutcomeEnvelope(bad, time.Now().UTC(), "c", "k"); err == nil {
			t.Fatalf("state %q must fail outcome schema validation", state)
		}
	}
}

func triggerFrom(kind TenantKind, revision int) uuid.UUID {
	ref := TenantRef{Kind: kind, ID: sharedUUID}
	return DeriveEventID(ref, "nightly", revision, time.Date(2026, 11, 18, 2, 0, 0, 0, time.UTC), EventTypeTriggered)
}

func TestTopicSplitFrozen(t *testing.T) {
	if TopicTriggers != "pipeline.builds.v1" {
		t.Fatalf("trigger topic renamed: %q", TopicTriggers)
	}
	if TopicOutcomes != "pipeline.schedule.outcomes.v1" {
		t.Fatalf("outcome topic renamed: %q", TopicOutcomes)
	}
}

func TestOutputPublishKeysSeparateDatasets(t *testing.T) {
	event := uuid.New()
	a := DeriveOutputPublishKey(event, "node", "dataset-a")
	b := DeriveOutputPublishKey(event, "node", "dataset-b")
	if a == b || len(a) != 40 {
		t.Fatal("dataset publish keys collide or length changed")
	}
	if a != DeriveOutputPublishKey(event, "node", "dataset-a") {
		t.Fatal("retry key changed")
	}
}
