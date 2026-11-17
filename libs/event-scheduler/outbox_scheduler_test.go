package eventscheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	databus "github.com/openfoundry/openfoundry-go/libs/event-bus-data"
)

// fakeVerifier pins registry content hashes without network access.
type fakeVerifier struct {
	hashes map[string]string
	err    error
}

func (f *fakeVerifier) ContentSHA256(_ context.Context, _ string, artifactID string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return f.hashes[artifactID], nil
}

func okFakeVerifier() *fakeVerifier {
	return &fakeVerifier{hashes: map[string]string{
		TriggerSchemaArtifactID: TriggerSchemaSHA256,
		OutcomeSchemaArtifactID: OutcomeSchemaSHA256,
	}}
}

// TestSchemaFingerprintsMatchM1Files is the drift guard between the
// producer's pinned fingerprints and the frozen M1 schema bytes. A
// mismatch means libs/schedule-events changed and the pinned constants
// (and the registered artifacts) must be updated — never the other
// way around.
func TestSchemaFingerprintsMatchM1Files(t *testing.T) {
	for artifact, path := range map[string]string{
		TriggerSchemaArtifactID: "../schedule-events/schema/pipeline.schedule.triggered.v1.json",
		OutcomeSchemaArtifactID: "../schedule-events/schema/pipeline.schedule.outcome.v1.json",
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		sum := sha256.Sum256(raw)
		got := hex.EncodeToString(sum[:])
		want := map[string]string{
			TriggerSchemaArtifactID: TriggerSchemaSHA256,
			OutcomeSchemaArtifactID: OutcomeSchemaSHA256,
		}[artifact]
		if got != want {
			t.Errorf("artifact %s: file sha256 %s != pinned %s — M1 schema changed, re-pin before shipping", artifact, got, want)
		}
	}
}

// TestTickOutboxNilVerifierFailClosed asserts through the PUBLIC entry
// point (root review round 2): a nil verifier must produce a clear
// ErrRegistryUnreachable — never a silent skip, never a panic. The
// registry check runs before anything touches Postgres, so a nil pool
// is safe here too.
func TestTickOutboxNilVerifierFailClosed(t *testing.T) {
	s := NewOutboxScheduler(nil, nil, "")
	res, err := s.TickOutbox(context.Background(), time.Now().UTC())
	if err == nil {
		t.Fatal("nil verifier must fail closed")
	}
	if !IsKind(err, ErrRegistryUnreachable) {
		t.Fatalf("want ErrRegistryUnreachable, got %v", err)
	}
	if res.Fired != 0 || res.Quarantined != 0 {
		t.Fatalf("no rows may fire on a failed gate, got %+v", res)
	}
}

func TestVerifyRegistryFingerprintsOutcomes(t *testing.T) {
	ctx := context.Background()
	if err := NewOutboxScheduler(nil, okFakeVerifier(), "").VerifyRegistryFingerprints(ctx); err != nil {
		t.Fatalf("happy path: unexpected error %v", err)
	}

	mismatch := NewOutboxScheduler(nil, &fakeVerifier{hashes: map[string]string{
		TriggerSchemaArtifactID: TriggerSchemaSHA256,
		OutcomeSchemaArtifactID: strings.Repeat("0", 64),
	}}, "")
	if err := mismatch.VerifyRegistryFingerprints(ctx); !IsKind(err, ErrRegistryContentMismatch) {
		t.Fatalf("want ErrRegistryContentMismatch, got %v", err)
	}

	unreachable := NewOutboxScheduler(nil, &fakeVerifier{err: errors.New("boom")}, "")
	if err := unreachable.VerifyRegistryFingerprints(ctx); !IsKind(err, ErrRegistryUnreachable) {
		t.Fatalf("want ErrRegistryUnreachable, got %v", err)
	}
}

// TestApicurioRegistryContentSHA256 pins the v2 wire contract against
// a local HTTP server: GET /apis/registry/v2/groups/{g}/artifacts/{id}
// returns the raw content bytes (getLatestArtifact in the 2.6.13.Final
// OpenAPI spec).
func TestApicurioRegistryContentSHA256(t *testing.T) {
	payload := []byte(`{"schema":"pin-me"}`)
	want := sha256.Sum256(payload)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/registry/v2/groups/default/artifacts/pipeline.schedule.triggered.v1" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	reg := NewApicurioRegistry(srv.URL, srv.Client())
	got, err := reg.ContentSHA256(context.Background(), "default", TriggerSchemaArtifactID)
	if err != nil {
		t.Fatalf("ContentSHA256: %v", err)
	}
	if got != hex.EncodeToString(want[:]) {
		t.Fatalf("sha mismatch: %s != %s", got, hex.EncodeToString(want[:]))
	}
	if _, err := reg.ContentSHA256(context.Background(), "default", "no.such.artifact"); err == nil {
		t.Fatal("404 must surface as an error (retryable registry state)")
	}
}

func TestSanitizeReason(t *testing.T) {
	in := "line1\nline2\r\n" + strings.Repeat("x", 600)
	out := sanitizeReason(in)
	if strings.ContainsAny(out, "\n\r") {
		t.Fatalf("newlines survived: %q", out)
	}
	if len(out) != 512 {
		t.Fatalf("want truncated to 512, got %d", len(out))
	}
}

func goodRow() *outboxDefinitionRow {
	kind := "org"
	tenant := uuid.MustParse("11111111-2222-4111-8111-333333333333")
	key := "nightly-build"
	rev := 1
	rid := "ri.foundry.main.pipeline.11111111-1111-4111-8111-111111111111"
	branch := "main"
	grant := uuid.New()
	gen := 2
	return &outboxDefinitionRow{
		id: uuid.New(), name: "n", cronExpr: "* * * * *", cronFlavor: "unix5", timeZone: "UTC",
		tenantKind: &kind, tenantID: &tenant, scheduleKey: &key, definitionRevision: &rev,
		pipelineRID: &rid, buildBranch: &branch, grantRef: &grant, grantGeneration: &gen,
	}
}

// TestBuildTriggerPayloadBindingValidation covers the permanent bad
// definition families that must quarantine (never abort the tick).
func TestBuildTriggerPayloadBindingValidation(t *testing.T) {
	t.Run("good binding builds payload", func(t *testing.T) {
		p, reason := buildTriggerPayload(goodRow())
		if reason != "" || p == nil {
			t.Fatalf("want payload, got reason=%q", reason)
		}
		if p.GrantGeneration != 2 || p.TenantKind != "org" || p.BuildBranch != "main" {
			t.Fatalf("payload fields wrong: %+v", p)
		}
	})
	t.Run("missing grant row quarantines", func(t *testing.T) {
		row := goodRow()
		row.grantGeneration = nil
		if _, reason := buildTriggerPayload(row); !strings.Contains(reason, "grant row missing") {
			t.Fatalf("want grant-missing reason, got %q", reason)
		}
	})
	t.Run("incomplete binding quarantines", func(t *testing.T) {
		row := goodRow()
		row.buildBranch = nil
		if _, reason := buildTriggerPayload(row); !strings.Contains(reason, "binding incomplete") {
			t.Fatalf("want binding-incomplete reason, got %q", reason)
		}
	})
	t.Run("invalid tenant kind quarantines", func(t *testing.T) {
		row := goodRow()
		bad := "team"
		row.tenantKind = &bad
		if _, reason := buildTriggerPayload(row); !strings.Contains(reason, "invalid tenant_kind") {
			t.Fatalf("want invalid-kind reason, got %q", reason)
		}
	})
}

// TestPrepareFireEnvelopeValid — the happy-path envelope from a real
// row passes M1 validation (deterministic event_id included).
func TestPrepareFireEnvelopeValid(t *testing.T) {
	row := goodRow()
	row.nextRunAt = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	s := &OutboxScheduler{}
	env, next, bad, err := s.prepareFire(row, row.nextRunAt, row.nextRunAt.Add(time.Minute), uuid.New())
	if err != nil || bad != "" || env == nil {
		t.Fatalf("prepareFire failed: bad=%q err=%v", bad, err)
	}
	if !next.After(row.nextRunAt) {
		t.Fatalf("next fire must advance, got %v", next)
	}
	if env.EventType != "pipeline.schedule.triggered.v1" || env.AggregateType != "pipeline_schedule" {
		t.Fatalf("envelope constants wrong: %+v", env)
	}
	if !strings.HasPrefix(env.AggregateID, "org:") || !strings.Contains(env.AggregateID, "/nightly-build") {
		t.Fatalf("aggregate id must carry the tenant domain: %s", env.AggregateID)
	}
}

// ─── PG-backed integration (env-gated via OFWP32_PG_DSN) ────────────
//
// The runner in tools/smoke/data-integration-wp3-2 provides a
// disposable Postgres 16 and sets the env; these tests apply the
// library migrations themselves.

func newPgxPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("OFWP32_PG_DSN")
	if dsn == "" {
		t.Skip("OFWP32_PG_DSN not set — skipping PG-backed outbox tick tests")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func execSQL(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
}

func queryInt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return n
}

// createLogicalSlot creates a pgoutput logical slot via the official
// admin function (pg_create_logical_replication_slot — the documented
// name since PostgreSQL 9.4, PG16 §9.27.6 Functions-Replication).
func createLogicalSlot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) {
	t.Helper()
	execSQL(t, ctx, pool, "create logical slot "+name,
		`SELECT pg_create_logical_replication_slot($1,'pgoutput')`, name)
}

func resetLibDB(t *testing.T, ctx context.Context, pool *pgxpool.Pool, withOutboxMigration bool) {
	t.Helper()
	execSQL(t, ctx, pool, "drop leftover state", `
		DROP SCHEMA IF EXISTS scheduling CASCADE;
		DROP SCHEMA IF EXISTS outbox CASCADE;
		DROP SCHEMA IF EXISTS schedules CASCADE;`)
	raw1, err := os.ReadFile("migrations/0001_schedules_definitions.sql")
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, ctx, pool, "apply 0001", string(raw1))
	if withOutboxMigration {
		raw2, err := os.ReadFile("migrations/0002_schedules_outbox_dispatch.sql")
		if err != nil {
			t.Fatal(err)
		}
		execSQL(t, ctx, pool, "apply 0002", string(raw2))
	}
}

// insertFixture creates one grant + three definitions: a good outbox
// row, a bad-cron outbox row, and a due legacy (direct-mode) row.
func insertFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (goodID, badID, legacyID uuid.UUID) {
	t.Helper()
	grantID := uuid.New()
	tenantID := uuid.MustParse("11111111-2222-4111-8111-444444444444")
	subject := uuid.MustParse("11111111-2222-4111-8111-555555555555")
	execSQL(t, ctx, pool, "insert grant", `
		INSERT INTO scheduling.grants
		  (id, tenant_kind, tenant_id, org_id, subject, schedule_key, pipeline_rid,
		   generation, state, expires_at, token_ciphertext, created_by)
		VALUES ($1,'org',$2,$2,$3,'nightly-build',
		        'ri.foundry.main.pipeline.11111111-1111-4111-8111-111111111111',
		        2,'active', now()+interval '1 day', '\x00'::bytea, $3)`,
		grantID, tenantID, subject)

	goodID, badID, legacyID = uuid.New(), uuid.New(), uuid.New()
	const insertDef = `
		INSERT INTO schedules.definitions
		  (id, name, cron_expr, cron_flavor, time_zone, enabled, topic, payload_template,
		   next_run_at, dispatch_mode, tenant_kind, tenant_id, schedule_key,
		   definition_revision, pipeline_rid, build_branch, grant_ref)
		VALUES ($1,$2,$3,'unix5','UTC',true,'pipeline.builds.v1','{"k":"v"}'::jsonb,
		        now()-interval '1 minute', $4,$5,$6,$7,1,
		        'ri.foundry.main.pipeline.11111111-1111-4111-8111-111111111111','main',$8)`
	execSQL(t, ctx, pool, "insert good def", insertDef,
		goodID, "wp32-good", "*/5 * * * *", "pipeline_outbox_v1", "org", tenantID, "nightly-build", grantID)
	execSQL(t, ctx, pool, "insert bad def", insertDef,
		badID, "wp32-badcron", "not-a-cron", "pipeline_outbox_v1", "org", tenantID, "broken-one", grantID)
	// Legacy row: NULL dispatch_mode (old semantics — only the direct
	// tick may ever fire it).
	execSQL(t, ctx, pool, "insert legacy def", insertDef,
		legacyID, "wp32-legacy", "* * * * *", nil, nil, nil, nil, nil)
	return goodID, badID, legacyID
}

// TestTickOutboxIntegration drives the real SQL path against PG16:
// firing, in-transaction quarantine isolation (V7), legacy-row
// exclusion, registry fail-closed behavior, replay audit semantics,
// and the CDC slot gate.
func TestTickOutboxIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newPgxPool(t, ctx)
	resetLibDB(t, ctx, pool, true)
	goodID, badID, legacyID := insertFixture(t, ctx, pool)

	s := NewOutboxScheduler(pool, okFakeVerifier(), "")
	now := time.Now().UTC()

	res, err := s.TickOutbox(ctx, now)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if res.Fired != 1 || res.Quarantined != 1 {
		t.Fatalf("want fired=1 quarantined=1, got %+v", res)
	}

	// Good row: exactly one audit row, advanced, still enabled.
	if n := queryInt(t, ctx, pool, `SELECT count(*) FROM scheduling.dispatch_audit WHERE definition_id=$1`, goodID); n != 1 {
		t.Fatalf("want 1 audit row for good definition, got %d", n)
	}
	var enabled bool
	var nextRun time.Time
	if err := pool.QueryRow(ctx,
		`SELECT enabled, next_run_at FROM schedules.definitions WHERE id=$1`, goodID).Scan(&enabled, &nextRun); err != nil {
		t.Fatal(err)
	}
	if !enabled {
		t.Fatal("good definition must stay enabled")
	}
	if !nextRun.After(now) {
		t.Fatalf("next_run_at must advance past now, got %v", nextRun)
	}
	// Outbox table stays empty in steady state (INSERT+DELETE same tx).
	if n := queryInt(t, ctx, pool, `SELECT count(*) FROM outbox.events`); n != 0 {
		t.Fatalf("outbox must be empty after commit, got %d rows", n)
	}

	// Bad row: quarantined + disabled in the SAME tx, reason recorded.
	var qReason *string
	if err := pool.QueryRow(ctx,
		`SELECT quarantine_reason FROM schedules.definitions WHERE id=$1 AND enabled=false AND quarantined_at IS NOT NULL`, badID).
		Scan(&qReason); err != nil {
		t.Fatalf("bad row must be disabled+quarantined: %v", err)
	}
	if qReason == nil || !strings.Contains(*qReason, "cron") {
		t.Fatalf("quarantine reason must mention cron, got %v", qReason)
	}

	// Legacy row untouched by TickOutbox (double-dispatch exclusion).
	if n := queryInt(t, ctx, pool, `SELECT count(*) FROM scheduling.dispatch_audit WHERE definition_id=$1`, legacyID); n != 0 {
		t.Fatal("legacy (non-outbox) row must never be fired by TickOutbox")
	}

	// Second tick: nothing left due.
	res2, err := s.TickOutbox(ctx, now.Add(time.Second))
	if err != nil || res2.Fired != 0 || res2.Quarantined != 0 {
		t.Fatalf("second tick must be a no-op, got res=%+v err=%v", res2, err)
	}

	// Registry down: fail-closed before claiming; rows stay due, no
	// quarantine side effects.
	execSQL(t, ctx, pool, "reset bad row for registry test",
		`UPDATE schedules.definitions SET enabled=true, quarantined_at=NULL, quarantine_reason=NULL, next_run_at=now()-interval '1 minute' WHERE id=$1`, badID)
	down := NewOutboxScheduler(pool, &fakeVerifier{err: errors.New("registry down")}, "")
	if _, err := down.TickOutbox(ctx, time.Now().UTC()); !IsKind(err, ErrRegistryUnreachable) {
		t.Fatalf("want ErrRegistryUnreachable, got %v", err)
	}
	if n := queryInt(t, ctx, pool,
		`SELECT count(*) FROM schedules.definitions WHERE id=$1 AND enabled AND quarantined_at IS NULL`, badID); n != 1 {
		t.Fatal("registry-down tick must leave the due row due and unquarantined")
	}

	// CDC slot gate: configured slot absent → ErrCDCNotReady, rows stay due.
	gated := NewOutboxScheduler(pool, okFakeVerifier(), "").WithCDCSlot("ofwp32_missing_slot")
	if _, err := gated.TickOutbox(ctx, time.Now().UTC()); !IsKind(err, ErrCDCNotReady) {
		t.Fatalf("want ErrCDCNotReady, got %v", err)
	}
	if n := queryInt(t, ctx, pool,
		`SELECT count(*) FROM schedules.definitions WHERE id=$1 AND enabled AND quarantined_at IS NULL`, badID); n != 1 {
		t.Fatal("CDC gate must leave due rows untouched")
	}

	// Replay semantics: reset the good row to fire the SAME slot again
	// — the deterministic event_id re-enqueues (consumer dedups) while
	// dispatch_audit stays at one row per opportunity.
	var eventIDStr string
	if err := pool.QueryRow(ctx,
		`SELECT event_id::text FROM scheduling.dispatch_audit WHERE definition_id=$1`, goodID).Scan(&eventIDStr); err != nil {
		t.Fatal(err)
	}
	// SAME-SLOT replay: reset next_run_at to the exact fired slot
	// (last_run_at), not to a fresh now()-offset — a different instant
	// would legitimately be a NEW opportunity with a new event_id.
	execSQL(t, ctx, pool, "reset good row for replay",
		`UPDATE schedules.definitions SET next_run_at=last_run_at WHERE id=$1`, goodID)
	res3, err := s.TickOutbox(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("replay tick: %v", err)
	}
	if res3.Fired != 1 || len(res3.FiredEventIDs) != 1 || res3.FiredEventIDs[0].String() != eventIDStr {
		t.Fatalf("replay must re-fire the SAME event_id, got %v want %s", res3.FiredEventIDs, eventIDStr)
	}
	if n := queryInt(t, ctx, pool, `SELECT count(*) FROM scheduling.dispatch_audit WHERE definition_id=$1`, goodID); n != 1 {
		t.Fatalf("audit must stay 1 row per opportunity, got %d", n)
	}
}

// TestTickOutboxCDCSlotGateVariants — the gate distinguishes a missing
// slot, a same-named slot owned by ANOTHER database (cluster-wide
// pg_replication_slots, root review round 3), and a correct-but-
// inactive slot (wait-state, not data loss). Needs wal_level=logical
// to create slots; skips otherwise.
func TestTickOutboxCDCSlotGateVariants(t *testing.T) {
	ctx := context.Background()
	pool := newPgxPool(t, ctx)
	resetLibDB(t, ctx, pool, true)
	insertFixture(t, ctx, pool)

	// Missing slot.
	s0 := NewOutboxScheduler(pool, okFakeVerifier(), "").WithCDCSlot("ofwp32_gate_missing")
	_, err := s0.TickOutbox(ctx, time.Now().UTC())
	if !IsKind(err, ErrCDCNotReady) || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing slot: want ErrCDCNotReady/not found, got %v", err)
	}

	var wal string
	if err := pool.QueryRow(ctx, `SHOW wal_level`).Scan(&wal); err != nil || wal != "logical" {
		t.Skipf("PG wal_level=%q (err=%v) — need logical to create slots in-test", wal, err)
	}

	// Correct slot, inactive: rejection wording must distinguish the
	// wait-state from permanent loss.
	createLogicalSlot(t, ctx, pool, "ofwp32_gate_inactive")
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `SELECT pg_drop_replication_slot('ofwp32_gate_inactive')`) })
	s1 := NewOutboxScheduler(pool, okFakeVerifier(), "").WithCDCSlot("ofwp32_gate_inactive")
	_, err = s1.TickOutbox(ctx, time.Now().UTC())
	if !IsKind(err, ErrCDCNotReady) {
		t.Fatalf("inactive slot: want ErrCDCNotReady, got %v", err)
	}
	if !strings.Contains(err.Error(), "retained") || strings.Contains(err.Error(), "unrecoverable") {
		t.Fatalf("inactive slot message must say WAL retained (wait-state), got: %v", err)
	}

	// Same-named slot owned by ANOTHER database: rejected as mismatch.
	execSQL(t, ctx, pool, "create foreign db", `CREATE DATABASE ofwp32_other`)
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DROP DATABASE ofwp32_other WITH (FORCE)`) })
	foreignDSN := strings.TrimSuffix(os.Getenv("OFWP32_PG_DSN"), "/ofwp32") + "/ofwp32_other"
	foreign, err := pgxpool.New(ctx, foreignDSN)
	if err != nil {
		t.Fatalf("connect foreign db: %v", err)
	}
	createLogicalSlot(t, ctx, foreign, "ofwp32_gate_foreign")
	t.Cleanup(func() {
		_, _ = foreign.Exec(ctx, `SELECT pg_drop_replication_slot('ofwp32_gate_foreign')`)
		foreign.Close()
	})
	s2 := NewOutboxScheduler(pool, okFakeVerifier(), "").WithCDCSlot("ofwp32_gate_foreign")
	_, err = s2.TickOutbox(ctx, time.Now().UTC())
	if !IsKind(err, ErrCDCNotReady) || !strings.Contains(err.Error(), "not this database's outbox slot") {
		t.Fatalf("foreign-db slot: want mismatch rejection, got %v", err)
	}
	// Due rows untouched by every rejected gate attempt.
	if n := queryInt(t, ctx, pool, `SELECT count(*) FROM scheduling.dispatch_audit`); n != 0 {
		t.Fatalf("no opportunity may fire while the CDC gate rejects, audit rows=%d", n)
	}
}

// TestTickOutboxSchemaMissingFailClosed proves an unmigrated database
// surfaces ErrSchemaMissing (not a raw 42703) for the new queries.
func TestTickOutboxSchemaMissingFailClosed(t *testing.T) {
	ctx := context.Background()
	pool := newPgxPool(t, ctx)
	resetLibDB(t, ctx, pool, false) // 0001 only.

	s := NewOutboxScheduler(pool, okFakeVerifier(), "")
	_, err := s.TickOutbox(ctx, time.Now().UTC())
	if !IsKind(err, ErrSchemaMissing) {
		t.Fatalf("want ErrSchemaMissing, got %v", err)
	}
}

// TestTickLegacyExcludesOutboxRows — the direct-mode tick must skip
// pipeline_outbox_v1 rows even when they are the earliest due
// (double-dispatch rule, T8 decision §3): with the exclusion in the
// claim query it fires ONLY the legacy row.
func TestTickLegacyExcludesOutboxRows(t *testing.T) {
	ctx := context.Background()
	pool := newPgxPool(t, ctx)
	resetLibDB(t, ctx, pool, true)
	goodID, badID, legacyID := insertFixture(t, ctx, pool)

	pub := &recordingPublisher{}
	legacy := NewScheduler(pool, pub)
	fired, err := legacy.Tick(ctx, time.Now().UTC())
	if err != nil || fired != 1 {
		t.Fatalf("legacy tick must fire exactly the one direct-mode row, fired=%d err=%v", fired, err)
	}
	if len(pub.topics) != 1 {
		t.Fatalf("exactly one publish expected, got %v", pub.topics)
	}
	if n := queryInt(t, ctx, pool, `SELECT count(*) FROM scheduling.dispatch_audit`); n != 0 {
		t.Fatalf("legacy tick must not write dispatch_audit, got %d", n)
	}
	for _, id := range []uuid.UUID{goodID, badID} {
		var lastRun *time.Time
		if err := pool.QueryRow(ctx, `SELECT last_run_at FROM schedules.definitions WHERE id=$1`, id).Scan(&lastRun); err != nil {
			t.Fatal(err)
		}
		if lastRun != nil {
			t.Fatalf("legacy tick must not fire outbox-mode row %s", id)
		}
	}
	var legacyLastRun *time.Time
	if err := pool.QueryRow(ctx, `SELECT last_run_at FROM schedules.definitions WHERE id=$1`, legacyID).Scan(&legacyLastRun); err != nil {
		t.Fatal(err)
	}
	if legacyLastRun == nil {
		t.Fatal("legacy tick must have fired the direct-mode row")
	}
}

// recordingPublisher satisfies databus.Publisher and records topics.
type recordingPublisher struct {
	topics []string
}

func (p *recordingPublisher) Publish(_ context.Context, topic string, _, _ []byte, _ *databus.OpenLineageHeaders) error {
	p.topics = append(p.topics, topic)
	return nil
}

func (p *recordingPublisher) Flush(_ context.Context) error { return nil }

func (p *recordingPublisher) Close() error { return nil }
