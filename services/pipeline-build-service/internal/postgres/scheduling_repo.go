// Scheduling persistence for the T8 pipeline-scheduling consumer
// (WP3.2). See docs/architecture/data-integration-t8-scheduling-decision.md
// §3/§4 for the frozen contracts these queries implement:
//
//   - AdmitDispatch is the single-transaction admission: per-opportunity
//     dedup (event_id PK), exact-generation grant check, cross-opportunity
//     activity slot (Forbid semantics), and an in-tx build row creation.
//   - Grant generations are EXACT-match: a re-granted schedule
//     invalidates every queued/replayed event of the old generation.
//   - Blocked dispatches are terminal (Forbid skip, never auto-replayed);
//     unknown_publish/reconcile keep the activity slot and are recoverable.
package postgres

import (
	"context"
	"strings"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
	"github.com/openfoundry/openfoundry-go/libs/outbox"
)

// SchedulingGrant mirrors a scheduling.grants row. CreatedBy is the
// operator who granted (distinct from Subject, the identity the grant
// runs as).
type SchedulingGrant struct {
	ID              uuid.UUID
	TenantKind      string
	TenantID        uuid.UUID
	OrgID           *uuid.UUID
	Subject         uuid.UUID
	ScheduleKey     string
	PipelineRID     string
	Generation      int
	State           string
	ExpiresAt       time.Time
	TokenCiphertext []byte
	CreatedBy       uuid.UUID
}

// SchedulingDispatch mirrors a scheduling.dispatches row.
type SchedulingDispatch struct {
	EventID            uuid.UUID
	TenantKind         string
	TenantID           uuid.UUID
	ScheduleKey        string
	DefinitionRevision int
	ScheduledFor       time.Time
	PipelineRID        string
	BuildBranch        string
	GrantRef           uuid.UUID
	GrantGeneration    int
	BuildID            uuid.UUID
	State              string
	Attempts           int
	LeaseOwner         *string
	FenceToken         int64
	LeaseExpiresAt     *time.Time
	Error              *string
}

// Dispatch terminal states (mirror scheduleevents constants on the DB
// wire; blocked is terminal, unknown_publish/reconcile are recoverable).
const (
	DispatchStateAdmitted       = "admitted"
	DispatchStateExecuting      = "executing"
	DispatchStateSucceeded      = "succeeded"
	DispatchStateFailed         = "failed"
	DispatchStateSkipped        = "skipped"
	DispatchStateBlocked        = "blocked"
	DispatchStateUnknownPublish = "unknown_publish"
	DispatchStateReconcile      = "reconcile"
)

func dispatchTerminal(state string) bool {
	switch state {
	case DispatchStateSucceeded, DispatchStateFailed, DispatchStateSkipped, DispatchStateBlocked:
		return true
	}
	return false
}

func dispatchRecoverable(state string) bool {
	return state == DispatchStateUnknownPublish || state == DispatchStateReconcile
}

// AdmitOutcome classifies the result of an admission attempt.
type AdmitOutcome string

const (
	AdmitAdmitted           AdmitOutcome = "admitted"
	AdmitDuplicateTerminal  AdmitOutcome = "duplicate_terminal"
	AdmitDuplicateInFlight  AdmitOutcome = "duplicate_in_flight"
	AdmitDuplicateRecover   AdmitOutcome = "duplicate_recoverable"
	AdmitSkippedGrant       AdmitOutcome = "skipped_grant"
	AdmitBlockedOverlap     AdmitOutcome = "blocked_overlap"
)

// ExpectedOutput is one frozen (node, output dataset) expectation of
// the resolved plan, persisted at admission and enforced by the
// succeeded-finish completeness check.
type ExpectedOutput struct {
	NodeID     string `json:"node_id"`
	DatasetRID string `json:"dataset_rid"`
}

// AdmitParams carries the validated trigger tuple into admission.
// GrantSubject is filled by admission itself from the verified grant
// row (NOT from the event) so the in-tx build row can record
// requested_by (personal-domain owner visibility relies on it).
//
// Sealing semantics (T8 review): ExpectedOutputs/ExpectedNodes NULL
// means NOT YET SEALED — success is impossible until the resolver has
// persisted the durable plan (M3); an explicit empty ExpectedOutputs
// slice is a SEALED decision "no outputs". Same for ExpectedNodes.
type AdmitParams struct {
	EventID            uuid.UUID
	Tenant             scheduleevents.TenantRef
	ScheduleKey        string
	DefinitionRevision int
	ScheduledFor       time.Time
	PipelineRID        string
	BuildBranch        string
	GrantRef           uuid.UUID
	GrantGeneration    int
	BuildID            uuid.UUID // DeriveBuildID(triggerEventID)
	GrantSubject       uuid.UUID // set during admission from the grant row
	ExpectedOutputs    []ExpectedOutput
	ExpectedOutputsSealed bool
	ExpectedNodes      []string
	ExpectedNodesSealed bool
}

// sealedJSON renders a set as a JSON ARRAY, distinguishing "not sealed"
// (SQL NULL) from "sealed empty" ('[]'). A sealed nil/empty slice must
// serialize as '[]' — JSON null would bypass the SQL IS NULL gate.
func sealedJSON(sealed bool, value any) any {
	if !sealed {
		return nil
	}
	raw, err := json.Marshal(value)
	if err != nil || string(raw) == "null" {
		return []byte("[]")
	}
	return raw
}

func (p *AdmitParams) expectedOutputsJSON() any {
	return sealedJSON(p.ExpectedOutputsSealed, p.ExpectedOutputs)
}

func (p *AdmitParams) expectedNodesJSON() any {
	return sealedJSON(p.ExpectedNodesSealed, p.ExpectedNodes)
}

// OutcomeHook runs inside the admission transaction when admission
// itself records a terminal state (skipped/blocked) — used to enqueue
// the outcome event atomically.
type OutcomeHook func(ctx context.Context, tx pgx.Tx, res AdmissionResult) error

// ExtraTx is an extra same-transaction write during a fenced finish
// (e.g. the outcome outbox enqueue).
type ExtraTx func(ctx context.Context, tx pgx.Tx) error

// AdmissionResult reports what admission decided for the event.
type AdmissionResult struct {
	Outcome AdmitOutcome
	Dispatch SchedulingDispatch
	// Grant is populated whenever a grant row was found (even rejected
	// ones, for observability); nil when none exists.
	Grant *SchedulingGrant
}

// AdmitDispatch runs the single-transaction admission protocol (T8
// decision §4). It never executes the pipeline; callers drive the
// canonical chain only on AdmitAdmitted. outcomeHook, when set, runs in
// the SAME transaction whenever admission itself records a terminal
// state (skipped/blocked), so the outcome event commits atomically with
// the dispatch decision.
func (r *Repository) AdmitDispatch(ctx context.Context, p AdmitParams, outcomeHook OutcomeHook) (*AdmissionResult, error) {
	beginner, ok := r.db.(txBeginner)
	if !ok {
		// pgxmock pools in unit tests satisfy txBeginner; a DB that
		// cannot begin transactions cannot run admission at all.
		return nil, errors.New("scheduling admission requires a transaction-capable database")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin admission tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	res, err := admitDispatchInTx(ctx, tx, p)
	if err != nil {
		return nil, err
	}
	if outcomeHook != nil && (res.Outcome == AdmitSkippedGrant || res.Outcome == AdmitBlockedOverlap) {
		if err := outcomeHook(ctx, tx, *res); err != nil {
			return nil, fmt.Errorf("admission outcome hook: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit admission tx: %w", err)
	}
	return res, nil
}

// admitDispatchInTx is the admission protocol body, parameterized over
// the transaction so tests can drive it against a mocked tx.
func admitDispatchInTx(ctx context.Context, db DB, p AdmitParams) (*AdmissionResult, error) {
	// 1. Per-opportunity dedup: insert-once on the event id. With ON
	// CONFLICT DO NOTHING, a lost race yields no RETURNING row
	// (ErrNoRows); the explicit read below then routes on state.
	var insertedState string
	err := db.QueryRow(ctx,
		`INSERT INTO scheduling.dispatches
		   (event_id, tenant_kind, tenant_id, schedule_key, definition_revision,
		    scheduled_for, pipeline_rid, build_branch, grant_ref, grant_generation,
		    build_id, expected_outputs, expected_nodes, state)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'admitted')
		 ON CONFLICT (event_id) DO NOTHING
		 RETURNING state`,
		p.EventID, string(p.Tenant.Kind), p.Tenant.ID, p.ScheduleKey, p.DefinitionRevision,
		p.ScheduledFor, p.PipelineRID, p.BuildBranch, p.GrantRef, p.GrantGeneration, p.BuildID,
		p.expectedOutputsJSON(), p.expectedNodesJSON(),
	).Scan(&insertedState)
	if errors.Is(err, pgx.ErrNoRows) {
		state, d, lerr := loadDispatch(ctx, db, p.EventID)
		if lerr != nil {
			return nil, lerr
		}
		switch {
		case dispatchTerminal(state):
			return &AdmissionResult{Outcome: AdmitDuplicateTerminal, Dispatch: *d}, nil
		case dispatchRecoverable(state):
			return &AdmissionResult{Outcome: AdmitDuplicateRecover, Dispatch: *d}, nil
		default: // admitted / executing elsewhere
			return &AdmissionResult{Outcome: AdmitDuplicateInFlight, Dispatch: *d}, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("insert dispatch: %w", err)
	}

	dispatch := SchedulingDispatch{
		EventID: p.EventID, TenantKind: string(p.Tenant.Kind), TenantID: p.Tenant.ID,
		ScheduleKey: p.ScheduleKey, DefinitionRevision: p.DefinitionRevision,
		ScheduledFor: p.ScheduledFor, PipelineRID: p.PipelineRID, BuildBranch: p.BuildBranch,
		GrantRef: p.GrantRef, GrantGeneration: p.GrantGeneration, BuildID: p.BuildID,
		State: DispatchStateAdmitted,
	}

	// 2. Exact-generation grant check against CURRENT database state.
	grant, gerr := loadActiveGrant(ctx, db, p.Tenant, p.ScheduleKey)
	if gerr != nil {
		return nil, gerr
	}
	if grant == nil || grant.Generation != p.GrantGeneration || grant.PipelineRID != p.PipelineRID ||
		grant.ID != p.GrantRef || grant.ExpiresAt.Before(time.Now().UTC()) {
		reason := "grant_missing"
		switch {
		case grant != nil && grant.ID != p.GrantRef:
			reason = "grant_ref_mismatch"
		case grant != nil && grant.Generation != p.GrantGeneration:
			reason = "grant_generation_mismatch"
		case grant != nil && grant.PipelineRID != p.PipelineRID:
			reason = "grant_pipeline_mismatch"
		case grant != nil && grant.ExpiresAt.Before(time.Now().UTC()):
			reason = "grant_expired"
		}
		if err := setDispatchState(ctx, db, p.EventID, DispatchStateSkipped, reason); err != nil {
			return nil, err
		}
		dispatch.State = DispatchStateSkipped
		e := reason
		dispatch.Error = &e
		return &AdmissionResult{Outcome: AdmitSkippedGrant, Dispatch: dispatch, Grant: grant}, nil
	}

	// 2b. Definition-binding closure: the CURRENT pipeline_outbox_v1
	// binding must still match this event's tuple (enabled, revision,
	// pipeline, branch, grant ref). A stale event of an older revision
	// (definition edited since) is skipped — never executed against the
	// old shape.
	var defEnabled bool
	var defRev int
	var defPipeline, defBranch, defGrant string
	err = db.QueryRow(ctx,
		`SELECT enabled, definition_revision, pipeline_rid, build_branch, grant_ref::text
		   FROM schedules.definitions
		  WHERE dispatch_mode='pipeline_outbox_v1'
		    AND tenant_kind=$1 AND tenant_id=$2 AND schedule_key=$3`,
		string(p.Tenant.Kind), p.Tenant.ID, p.ScheduleKey,
	).Scan(&defEnabled, &defRev, &defPipeline, &defBranch, &defGrant)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := setDispatchState(ctx, db, p.EventID, DispatchStateSkipped, "definition_unbound"); err != nil {
			return nil, err
		}
		dispatch.State = DispatchStateSkipped
		e := "definition_unbound"
		dispatch.Error = &e
		return &AdmissionResult{Outcome: AdmitSkippedGrant, Dispatch: dispatch, Grant: grant}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load definition binding: %w", err)
	}
	if !defEnabled || defRev != p.DefinitionRevision || defPipeline != p.PipelineRID ||
		defBranch != p.BuildBranch || defGrant != p.GrantRef.String() {
		if err := setDispatchState(ctx, db, p.EventID, DispatchStateSkipped, "definition_mismatch"); err != nil {
			return nil, err
		}
		dispatch.State = DispatchStateSkipped
		e := "definition_mismatch"
		dispatch.Error = &e
		return &AdmissionResult{Outcome: AdmitSkippedGrant, Dispatch: dispatch, Grant: grant}, nil
	}

	// 3. Cross-opportunity activity slot (Forbid semantics: overlap is
	// a terminal skip, never preemption).
	acquired, aerr := acquireActivity(ctx, db, p, dispatch)
	if aerr != nil {
		return nil, aerr
	}
	if !acquired {
		if err := setDispatchState(ctx, db, p.EventID, DispatchStateBlocked, "overlap"); err != nil {
			return nil, err
		}
		dispatch.State = DispatchStateBlocked
		e := "overlap"
		dispatch.Error = &e
		return &AdmissionResult{Outcome: AdmitBlockedOverlap, Dispatch: dispatch, Grant: grant}, nil
	}

	// 4. In-tx build row (deterministic id, idempotent re-entry).
	// requested_by comes from the VERIFIED grant subject — builds rows
	// carry NOT NULL requested_by, and the personal-domain owner is
	// identified through it.
	p.GrantSubject = grant.Subject
	if err := openBuildTx(ctx, db, p); err != nil {
		return nil, err
	}
	return &AdmissionResult{Outcome: AdmitAdmitted, Dispatch: dispatch, Grant: grant}, nil
}

func loadDispatch(ctx context.Context, db DB, eventID uuid.UUID) (string, *SchedulingDispatch, error) {
	var d SchedulingDispatch
	var tenantID uuid.UUID
	err := db.QueryRow(ctx,
		`SELECT event_id, tenant_kind, tenant_id, schedule_key, definition_revision,
		        scheduled_for, pipeline_rid, build_branch, grant_ref, grant_generation,
		        build_id, state, attempts, fence_token
		   FROM scheduling.dispatches WHERE event_id=$1`, eventID,
	).Scan(&d.EventID, &d.TenantKind, &tenantID, &d.ScheduleKey, &d.DefinitionRevision,
		&d.ScheduledFor, &d.PipelineRID, &d.BuildBranch, &d.GrantRef, &d.GrantGeneration,
		&d.BuildID, &d.State, &d.Attempts, &d.FenceToken)
	if err != nil {
		return "", nil, fmt.Errorf("load dispatch: %w", err)
	}
	d.TenantID = tenantID
	return d.State, &d, nil
}

func setDispatchState(ctx context.Context, db DB, eventID uuid.UUID, state, reason string) error {
	_, err := db.Exec(ctx,
		`UPDATE scheduling.dispatches SET state=$2, error=$3, updated_at=NOW() WHERE event_id=$1`,
		eventID, state, reason)
	if err != nil {
		return fmt.Errorf("set dispatch state %s: %w", state, err)
	}
	return nil
}

func loadActiveGrant(ctx context.Context, db DB, tenant scheduleevents.TenantRef, scheduleKey string) (*SchedulingGrant, error) {
	var g SchedulingGrant
	var id, tenantID, subject, orgID string
	err := db.QueryRow(ctx,
		`SELECT id::text, tenant_kind, tenant_id::text, COALESCE(org_id::text,''), subject::text, schedule_key, pipeline_rid,
		        generation, state, expires_at, token_ciphertext
		   FROM scheduling.grants
		  WHERE tenant_kind=$1 AND tenant_id=$2 AND schedule_key=$3 AND state='active'`,
		string(tenant.Kind), tenant.ID, scheduleKey,
	).Scan(&id, &g.TenantKind, &tenantID, &orgID, &subject, &g.ScheduleKey, &g.PipelineRID,
		&g.Generation, &g.State, &g.ExpiresAt, &g.TokenCiphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load grant: %w", err)
	}
	if g.ID, err = uuid.Parse(id); err != nil {
		return nil, fmt.Errorf("load grant id: %w", err)
	}
	if g.TenantID, err = uuid.Parse(tenantID); err != nil {
		return nil, fmt.Errorf("load grant tenant: %w", err)
	}
	if g.Subject, err = uuid.Parse(subject); err != nil {
		return nil, fmt.Errorf("load grant subject: %w", err)
	}
	if orgID != "" {
		parsed, perr := uuid.Parse(orgID)
		if perr != nil {
			return nil, fmt.Errorf("load grant org: %w", perr)
		}
		g.OrgID = &parsed
	}
	return &g, nil
}

// acquireActivity inserts or re-acquires the (tenant, pipeline) slot.
// Re-acquisition is only valid for the SAME dispatch (idempotent
// redelivery of an admitted-but-unclaimed event); any other live slot
// blocks (Forbid).
func acquireActivity(ctx context.Context, db DB, p AdmitParams, d SchedulingDispatch) (bool, error) {
	tag, err := db.Exec(ctx,
		`INSERT INTO scheduling.pipeline_activity
		   (tenant_kind, tenant_id, pipeline_rid, active_dispatch_id, fence_token)
		 VALUES ($1,$2,$3,$4,1)
		 ON CONFLICT (tenant_kind, tenant_id, pipeline_rid) DO NOTHING`,
		string(p.Tenant.Kind), p.Tenant.ID, p.PipelineRID, p.EventID)
	if err != nil {
		return false, fmt.Errorf("acquire activity: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	// Conflict: same dispatch idempotent re-entry re-acquires; anything
	// else (including an expired lease of another dispatch) blocks.
	var active uuid.UUID
	err = db.QueryRow(ctx,
		`SELECT active_dispatch_id FROM scheduling.pipeline_activity
		  WHERE tenant_kind=$1 AND tenant_id=$2 AND pipeline_rid=$3`,
		string(p.Tenant.Kind), p.Tenant.ID, p.PipelineRID).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		// Raced with a release; retry once by inserting again.
		tag, err := db.Exec(ctx,
			`INSERT INTO scheduling.pipeline_activity
			   (tenant_kind, tenant_id, pipeline_rid, active_dispatch_id, fence_token)
			 VALUES ($1,$2,$3,$4,1)
			 ON CONFLICT (tenant_kind, tenant_id, pipeline_rid) DO NOTHING`,
			string(p.Tenant.Kind), p.Tenant.ID, p.PipelineRID, p.EventID)
		if err != nil {
			return false, fmt.Errorf("re-acquire activity: %w", err)
		}
		return tag.RowsAffected() == 1, nil
	}
	if err != nil {
		return false, fmt.Errorf("load activity: %w", err)
	}
	return active == p.EventID, nil
}

// openBuildTx creates the build row INSIDE the admission transaction
// (deterministic id, SCHEDULED trigger, explicit branch, requested_by =
// verified grant subject — the column is NOT NULL in the real schema).
// Mirrors Repository.OpenBuild's INSERT but on the caller's tx — the
// pool-level OpenBuild must not be used inside admission (it would open
// a second transaction).
func openBuildTx(ctx context.Context, db DB, p AdmitParams) error {
	_, err := db.Exec(ctx,
		`INSERT INTO builds (id, pipeline_rid, build_branch, job_spec_fallback, state, trigger_kind, force_build, requested_by, abort_policy, tenant_id)
VALUES ($1,$2,$3,'{}','BUILD_RESOLUTION','SCHEDULED',false,$4,'DEPENDENT_ONLY',$5)
ON CONFLICT (id) DO NOTHING`,
		p.BuildID, p.PipelineRID, p.BuildBranch, p.GrantSubject.String(), tenantIDForScheduling(p))
	if err != nil {
		return fmt.Errorf("open build in admission tx: %w", err)
	}
	return nil
}

func tenantIDForScheduling(p AdmitParams) any {
	if p.Tenant.Kind == scheduleevents.TenantKindOrg {
		return p.Tenant.ID
	}
	// Personal domain: builds.tenant_id stays NULL (tenancy.go: personal
	// rows key on tenant_id NULL + subject), matching the legacy P1.1
	// mapping; the subject travels with the grant via requested_by.
	return nil
}

// sha256Hex fingerprints receipt request bodies.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ClaimDispatchForExecution transitions admitted -> executing in ONE
// transaction: row lock, claimability, CURRENT grant validity (same id,
// EXACT generation, active, unexpired — a revoked/re-granted grant can
// no longer claim), activity ownership, new fence on BOTH the dispatch
// and its activity row. ok=false means not claimable.
func (r *Repository) ClaimDispatchForExecution(ctx context.Context, eventID uuid.UUID, owner string, ttl time.Duration) (int64, bool, error) {
	beginner, ok := r.db.(txBeginner)
	if !ok {
		return 0, false, errors.New("claim requires a transaction-capable database")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("begin claim tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var (
		state, grantID string
		gen            int
		leaseExpires   *time.Time
	)
	err = tx.QueryRow(ctx,
		`SELECT d.state, d.grant_ref::text, d.grant_generation, d.lease_expires_at
		   FROM scheduling.dispatches d WHERE d.event_id=$1 FOR UPDATE`,
		eventID).Scan(&state, &grantID, &gen, &leaseExpires)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("lock dispatch for claim: %w", err)
	}
	// Post-lock DB wall clock: judge claimability with the clock read
	// after the row lock, not a host-side timestamp from before.
	var lockNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&lockNow); err != nil {
		return 0, false, fmt.Errorf("claim lock clock: %w", err)
	}
	claimable := state == DispatchStateAdmitted && (leaseExpires == nil || leaseExpires.Before(lockNow)) ||
		state == DispatchStateExecuting && leaseExpires != nil && leaseExpires.Before(lockNow)
	if !claimable {
		return 0, false, nil
	}
	// Current grant must still match id + EXACT generation, be active
	// and unexpired — under a FOR SHARE row lock so a concurrent revoke
	// cannot slip between the check and the claim commit. The wall
	// clock is read in a SEPARATE statement AFTER all locks are held:
	// PG's LockRows evaluates the FOR SHARE targetlist before waiting
	// on the tuple lock, so a clock_timestamp() in the same SELECT
	// could predate the wait (REL_16 nodeLockRows.c).
	var gState string
	var gExpiry time.Time
	err = tx.QueryRow(ctx,
		`SELECT state, expires_at FROM scheduling.grants
		  WHERE id::text=$1 AND generation=$2 FOR SHARE`, grantID, gen).Scan(&gState, &gExpiry)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("verify grant at claim: %w", err)
	}
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return 0, false, fmt.Errorf("claim wall clock: %w", err)
	}
	if gState != "active" || !gExpiry.After(dbNow) {
		return 0, false, nil
	}
	// Exactly one activity row, bound to THIS dispatch.
	var active uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT active_dispatch_id FROM scheduling.pipeline_activity
		  WHERE active_dispatch_id=$1`, eventID).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("verify activity at claim: %w", err)
	}

	var fence int64
	if err := tx.QueryRow(ctx,
		`UPDATE scheduling.dispatches
		    SET state='executing', fence_token=fence_token+1,
		        lease_owner=$2, lease_expires_at=clock_timestamp()+$3::interval, updated_at=NOW()
		  WHERE event_id=$1 RETURNING fence_token`,
		eventID, owner, ttl.String()).Scan(&fence); err != nil {
		return 0, false, fmt.Errorf("claim dispatch: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE scheduling.pipeline_activity SET fence_token=$2, heartbeat_at=NOW()
		  WHERE active_dispatch_id=$1`, eventID, fence); err != nil {
		return 0, false, fmt.Errorf("sync activity fence: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, false, fmt.Errorf("commit claim tx: %w", err)
	}
	return fence, true, nil
}

// ReconcileDispatch claims a dispatch sitting in unknown_publish or
// reconcile for reconciliation: same row-lock + CURRENT-grant
// (id/exact generation/active/unexpired, FOR SHARE) + activity binding
// verification as ClaimDispatchForExecution, then a fresh fence/lease.
// The reconciler MUST reconcile external publishes by stable publish
// key and the ORIGINAL persisted request bytes — never re-run nodes,
// never mint a new key. A missing/revoked/mismatched grant refuses the
// claim (revoked schedules stay parked until re-granted or expired).
func (r *Repository) ReconcileDispatch(ctx context.Context, eventID uuid.UUID, owner string, ttl time.Duration) (int64, bool, error) {
	beginner, ok := r.db.(txBeginner)
	if !ok {
		return 0, false, errors.New("reconcile requires a transaction-capable database")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("begin reconcile tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var state, grantID string
	var gen int
	err = tx.QueryRow(ctx,
		`SELECT state, grant_ref::text, grant_generation
		   FROM scheduling.dispatches WHERE event_id=$1 FOR UPDATE`,
		eventID).Scan(&state, &grantID, &gen)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("lock dispatch for reconcile: %w", err)
	}
	if state != DispatchStateUnknownPublish && state != DispatchStateReconcile {
		return 0, false, nil
	}
	// Grant row lock first (FOR SHARE, no clock in the targetlist —
	// same nodeLockRows rationale as claim), wall clock in a separate
	// statement once all locks are held.
	var gState string
	var gExpiry time.Time
	err = tx.QueryRow(ctx,
		`SELECT state, expires_at FROM scheduling.grants
		  WHERE id::text=$1 AND generation=$2 FOR SHARE`, grantID, gen).Scan(&gState, &gExpiry)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("verify grant at reconcile: %w", err)
	}
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return 0, false, fmt.Errorf("reconcile wall clock: %w", err)
	}
	if gState != "active" || !gExpiry.After(dbNow) {
		return 0, false, nil
	}
	var active uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT active_dispatch_id FROM scheduling.pipeline_activity
		  WHERE active_dispatch_id=$1`, eventID).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("verify activity at reconcile: %w", err)
	}

	var fence int64
	if err := tx.QueryRow(ctx,
		`UPDATE scheduling.dispatches
		    SET state='executing', fence_token=fence_token+1,
		        lease_owner=$2, lease_expires_at=clock_timestamp()+$3::interval, updated_at=NOW()
		  WHERE event_id=$1 RETURNING fence_token`,
		eventID, owner, ttl.String()).Scan(&fence); err != nil {
		return 0, false, fmt.Errorf("reconcile dispatch: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE scheduling.pipeline_activity SET fence_token=$2, heartbeat_at=NOW()
		  WHERE active_dispatch_id=$1`, eventID, fence); err != nil {
		return 0, false, fmt.Errorf("sync activity fence at reconcile: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, false, fmt.Errorf("commit reconcile tx: %w", err)
	}
	return fence, true, nil
}

// HeartbeatDispatch extends the lease iff event, owner, fence, state
// AND an unexpired lease all match — an expired lease cannot renew
// itself (the recovery flow must re-claim with a new fence).
func (r *Repository) HeartbeatDispatch(ctx context.Context, eventID uuid.UUID, owner string, fence int64, ttl time.Duration) error {
	beginner, ok := r.db.(txBeginner)
	if !ok {
		return errors.New("heartbeat requires a transaction-capable database")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin heartbeat tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := lockDispatchForFencedOps(ctx, tx, eventID, owner, fence); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE scheduling.dispatches
		    SET lease_expires_at=clock_timestamp()+$2::interval, updated_at=NOW()
		  WHERE event_id=$1`, eventID, ttl.String()); err != nil {
		return fmt.Errorf("heartbeat dispatch: %w", err)
	}
	return tx.Commit(ctx)
}

// lockDispatchForFencedOps is the shared POST-LOCK verification for
// fenced writers (FencedWrite/Heartbeat/Finish): lock the dispatch row
// by event_id ONLY (a WHERE on lease columns would be evaluated by
// ExecQual BEFORE LockRows waits — an unmodified tuple returns the
// stale slot with no EPQ recheck), then read the DB wall clock in a
// separate statement, and only then verify owner/fence/executing and a
// still-live lease in code. The lock is held to the caller's commit.
func lockDispatchForFencedOps(ctx context.Context, db DB, eventID uuid.UUID, owner string, fence int64) (uuid.UUID, error) {
	var state, lockOwner, buildIDStr string
	var ft int64
	var leaseExpiry time.Time
	// COALESCE flattens both nullables (owner '' / lease epoch-0) —
	// staleness is judged below from the values, never from scan
	// targets.
	err := db.QueryRow(ctx,
		`SELECT state, COALESCE(lease_owner,''), fence_token,
		        COALESCE(lease_expires_at, to_timestamp(0)), build_id::text
		   FROM scheduling.dispatches WHERE event_id=$1 FOR UPDATE`,
		eventID).Scan(&state, &lockOwner, &ft, &leaseExpiry, &buildIDStr)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, ErrStaleFence
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("lock dispatch for fenced op: %w", err)
	}
	var dbNow time.Time
	if err := db.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return uuid.Nil, fmt.Errorf("fenced op wall clock: %w", err)
	}
	if state != DispatchStateExecuting || lockOwner != owner || ft != fence ||
		!leaseExpiry.After(dbNow) { // NULL flattened to epoch => stale
		return uuid.Nil, ErrStaleFence
	}
	buildID, err := uuid.Parse(buildIDStr)
	if err != nil {
		return uuid.Nil, fmt.Errorf("fenced op build id: %w", err)
	}
	return buildID, nil
}

// ErrStaleFence reports that a fenced write no longer owns the dispatch.
var ErrStaleFence = errors.New("scheduling dispatch fence is stale")

// ErrFinishIncomplete reports that a succeeded finish was rejected by
// the same-tx completeness check (jobs not all COMPLETED, or
// pending/unknown receipts). The caller must transition the dispatch to
// unknown_publish for reconciliation instead of forcing success.
var ErrFinishIncomplete = errors.New("scheduling finish completeness check failed")

// FinishDispatch records a terminal (or recoverable-unknown) state in
// ONE fenced transaction: CAS on (owner, fence, executing, lease still
// live per clock_timestamp — NOW() is the tx start and can be stale
// after lock waits); for terminal outcomes also build terminal state,
// the caller's extra writes (e.g. the outcome outbox enqueue), and the
// activity release gated on completeness. Recoverable outcomes
// (unknown_publish/reconcile) ONLY advance the dispatch state — no
// build terminal write, no extra writes, slot retained. A stale fence
// yields ErrStaleFence with ZERO changes.
func (r *Repository) FinishDispatch(ctx context.Context, eventID uuid.UUID, owner string, fence int64, state, errMsg string, extra ExtraTx) error {
	if !dispatchTerminal(state) && !dispatchRecoverable(state) {
		return fmt.Errorf("invalid finish state %q", state)
	}
	beginner, ok := r.db.(txBeginner)
	if !ok {
		return errors.New("finish dispatch requires a transaction-capable database")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin finish tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Post-lock fence verification first (shared with FencedWrite/
	// Heartbeat): a WHERE on the lease columns of the UPDATE itself
	// would be evaluated before the row lock wait (ExecQual precedes
	// LockRows; an unmodified tuple returns without EPQ recheck).
	buildID, err := lockDispatchForFencedOps(ctx, tx, eventID, owner, fence)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE scheduling.dispatches
		    SET state=$3, error=NULLIF($4,''), lease_owner=NULL, lease_expires_at=NULL, updated_at=NOW()
		  WHERE event_id=$1 AND fence_token=$2`,
		eventID, fence, state, errMsg); err != nil {
		return fmt.Errorf("finish dispatch: %w", err)
	}

	if dispatchRecoverable(state) {
		// Unknown/reconcile: dispatch-only. The build keeps its current
		// state, nothing terminal is published, the slot is retained.
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit recoverable finish: %w", err)
		}
		return nil
	}

	// Terminal outcomes are CONFIRMED states: neither success nor
	// failure may be confirmed while any publish of this dispatch is
	// pending/unknown — the caller must transition to unknown_publish
	// for reconciliation instead.
	var hasUnconfirmed bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM scheduling.publish_receipts
		    WHERE dispatch_event_id=$1 AND state <> 'committed')`,
		eventID).Scan(&hasUnconfirmed); err != nil {
		return fmt.Errorf("finish receipt gate: %w", err)
	}
	if hasUnconfirmed {
		return ErrFinishIncomplete
	}

	// Succeeded additionally requires SAME-TX completeness:
	//   * both expectation sets SEALED as real arrays (NULL = not sealed;
	//     JSON null / non-array shapes are rejected too),
	//   * every job of the build COMPLETED and at least one exists
	//     (extra jobs beyond the expectation still have to be COMPLETED),
	//   * UNIVERSAL node coverage: EVERY expected job spec RID (the
	//     resolver's stable per-node job identity) maps to a real,
	//     COMPLETED job of this build — an unrelated completed job must
	//     never satisfy the expectation,
	//   * UNIVERSAL output coverage: EVERY expected (node, dataset) pair
	//     has a committed receipt bound to this dispatch. Counting is
	//     forbidden — duplicates must not mask a missing pair.
	if state == DispatchStateSucceeded {
		var incomplete bool
		if err := tx.QueryRow(ctx,
			`SELECT
			   d.expected_outputs IS NULL
			   OR d.expected_nodes IS NULL
			   OR jsonb_typeof(d.expected_outputs) <> 'array'
			   OR jsonb_typeof(d.expected_nodes) <> 'array'
			   OR (SELECT COUNT(*) FROM jobs j WHERE j.build_id=$1 AND j.state <> 'COMPLETED') > 0
			   OR (SELECT COUNT(*) FROM jobs j WHERE j.build_id=$1) = 0
			   OR EXISTS (
			       SELECT 1 FROM jsonb_array_elements_text(d.expected_nodes) n
			        WHERE NOT EXISTS (
			            SELECT 1 FROM jobs j
			             WHERE j.build_id=$1 AND j.job_spec_rid=n AND j.state='COMPLETED'))
			   OR EXISTS (
			       SELECT 1 FROM jsonb_array_elements(d.expected_outputs) e
			        WHERE NOT EXISTS (
			            SELECT 1 FROM scheduling.publish_receipts pr
			             WHERE pr.dispatch_event_id=d.event_id AND pr.state='committed'
			               AND pr.node_id=(e->>'node_id')
			               AND pr.dataset_rid=(e->>'dataset_rid')))
			 FROM scheduling.dispatches d WHERE d.event_id=$2`,
			buildID, eventID).Scan(&incomplete); err != nil {
			return fmt.Errorf("finish completeness check: %w", err)
		}
		if incomplete {
			return ErrFinishIncomplete
		}
	}

	// Build terminal state mirrors the dispatch outcome in the same tx.
	buildState := "BUILD_FAILED"
	if state == DispatchStateSucceeded {
		buildState = "BUILD_COMPLETED"
	}
	if _, err := tx.Exec(ctx,
		`UPDATE builds SET state=$3, error_message=NULLIF($2,''), finished_at=NOW() WHERE id=$1`,
		buildID, errMsg, buildState); err != nil {
		return fmt.Errorf("finish build: %w", err)
	}

	if extra != nil {
		if err := extra(ctx, tx); err != nil {
			return fmt.Errorf("finish extra writes: %w", err)
		}
	}

	// Release the activity slot only for outcomes that ran the pipeline
	// and confirmed (or safely terminated) every publish. Skipped and
	// blocked never acquired the slot at claim time.
	if state != DispatchStateSkipped && state != DispatchStateBlocked {
		if _, err := tx.Exec(ctx,
			`DELETE FROM scheduling.pipeline_activity a
			  USING scheduling.dispatches d
			  WHERE a.active_dispatch_id=d.event_id
			    AND d.event_id=$1 AND d.state=$2
			    AND NOT EXISTS (
			        SELECT 1 FROM scheduling.publish_receipts pr
			         WHERE pr.dispatch_event_id=$1 AND pr.state <> 'committed')`,
			eventID, state); err != nil {
			return fmt.Errorf("release activity: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit finish tx: %w", err)
	}
	return nil
}

// FencedWrite runs fn inside a transaction that first locks the
// dispatch row and verifies the SAME ownership predicate as the claim
// (owner + fence + executing + unexpired lease). Execution-time child
// writes (receipts, node state) must go through this — never a bare
// read-then-write. External DVS calls stay OUTSIDE transactions: their
// safety is the stable publish key + same-bytes replay + reconcile.
func (r *Repository) FencedWrite(ctx context.Context, eventID uuid.UUID, owner string, fence int64, fn ExtraTx) error {
	beginner, ok := r.db.(txBeginner)
	if !ok {
		return errors.New("fenced write requires a transaction-capable database")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin fenced write tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := lockDispatchForFencedOps(ctx, tx, eventID, owner, fence); err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit fenced write tx: %w", err)
	}
	return nil
}

// ExpireStaleExecutions is the recovery-worker scan input. It covers:
//   - admitted with no lease (admission committed + event acked but the
//     process crashed before claiming — redelivery will NOT come),
//   - executing with an expired lease (worker died mid-run),
//   - unknown_publish/reconcile rows (their finish cleared the lease
//     and the event was acked, so ONLY this scan brings them back).
// Recovery resumes the ORIGINAL dispatch (never a new opportunity) via
// ClaimDispatchForExecution / ReconcileDispatch respectively, then
// reconciles publishes by stable key + original request bytes.
func (r *Repository) ExpireStaleExecutions(ctx context.Context, now time.Time) ([]SchedulingDispatch, error) {
	rows, err := r.db.Query(ctx,
		`SELECT event_id, tenant_kind, tenant_id, schedule_key, definition_revision,
		        scheduled_for, pipeline_rid, build_branch, grant_ref, grant_generation,
		        build_id, state, attempts, fence_token, lease_expires_at
		   FROM scheduling.dispatches
		  WHERE (state IN ('admitted','executing')
		           AND (lease_expires_at IS NULL OR lease_expires_at < $1))
		     OR state IN ('unknown_publish','reconcile')
		  ORDER BY lease_expires_at NULLS FIRST, updated_at`, now)
	if err != nil {
		return nil, fmt.Errorf("scan stale executions: %w", err)
	}
	defer rows.Close()
	var out []SchedulingDispatch
	for rows.Next() {
		var d SchedulingDispatch
		var tenantID uuid.UUID
		if err := rows.Scan(&d.EventID, &d.TenantKind, &tenantID, &d.ScheduleKey, &d.DefinitionRevision,
			&d.ScheduledFor, &d.PipelineRID, &d.BuildBranch, &d.GrantRef, &d.GrantGeneration,
			&d.BuildID, &d.State, &d.Attempts, &d.FenceToken, &d.LeaseExpiresAt); err != nil {
			return nil, err
		}
		d.TenantID = tenantID
		out = append(out, d)
	}
	return out, rows.Err()
}

// ─── Poison messages ──────────────────────────────────────────────────

// RecordPoisonAttempt increments the persistent counter for a
// (topic,partition,offset) identity and returns the new count.
func (r *Repository) RecordPoisonAttempt(ctx context.Context, topic string, partition int, offset int64, lastErr string) (int, error) {
	var attempts int
	err := r.db.QueryRow(ctx,
		`INSERT INTO scheduling.poison_messages (topic, partition_id, partition_offset, attempts, last_error, state)
		 VALUES ($1,$2,$3,1,$4,'counting')
		 ON CONFLICT (topic, partition_id, partition_offset) DO UPDATE
		   SET attempts=poison_messages.attempts+1, last_attempt_at=NOW(), last_error=$4
		 RETURNING attempts`,
		topic, partition, offset, lastErr).Scan(&attempts)
	if err != nil {
		return 0, fmt.Errorf("record poison attempt: %w", err)
	}
	return attempts, nil
}

// MarkPoisonDLQEnqueued flips a poison record to dlq_enqueued after the
// DLQ outbox intent committed (the offset may only then be acked).
func (r *Repository) MarkPoisonDLQEnqueued(ctx context.Context, topic string, partition int, offset int64) error {
	_, err := r.db.Exec(ctx,
		`UPDATE scheduling.poison_messages SET state='dlq_enqueued'
		  WHERE topic=$1 AND partition_id=$2 AND partition_offset=$3`,
		topic, partition, offset)
	if err != nil {
		return fmt.Errorf("mark poison dlq: %w", err)
	}
	return nil
}

// ─── Publish receipts (fenced writes; never bare pool writes) ────────

// ErrReceiptKeyConflict reports a same-key receipt re-use attempt whose
// bytes/dispatch/node binding differs from the persisted original.
var ErrReceiptKeyConflict = errors.New("publish receipt key re-use with different binding")

// RecordPendingReceipt persists the ORIGINAL request bytes BEFORE the
// first POST (T8 decision §3), inside a fenced write: the dispatch row
// is locked and ownership (owner/fence/live lease) verified in the SAME
// transaction. A key conflict is only tolerated when the existing row
// carries the identical dispatch/node binding and byte hash — a new
// body under an existing key is rejected outright.
func (r *Repository) RecordPendingReceipt(ctx context.Context, eventID uuid.UUID, owner string, fence int64, key, nodeID, datasetRID string, body []byte) error {
	dispatch := eventID
	return r.FencedWrite(ctx, eventID, owner, fence, func(ctx context.Context, tx pgx.Tx) error {
		sum := sha256Hex(body)
		tag, err := tx.Exec(ctx,
			`INSERT INTO scheduling.publish_receipts
			   (key, dispatch_event_id, node_id, dataset_rid, request_body, request_sha256, state)
			 VALUES ($1,$2,$3,$4,$5,$6,'pending')
			 ON CONFLICT (key) DO NOTHING`,
			key, dispatch, nodeID, datasetRID, body, sum)
		if err != nil {
			return fmt.Errorf("insert pending receipt: %w", err)
		}
		if tag.RowsAffected() == 0 {
			var existingDispatch, existingNode, existingSum, existingDataset string
			if err := tx.QueryRow(ctx,
				`SELECT dispatch_event_id::text, node_id, request_sha256, dataset_rid
				   FROM scheduling.publish_receipts WHERE key=$1`, key,
			).Scan(&existingDispatch, &existingNode, &existingSum, &existingDataset); err != nil {
				return fmt.Errorf("verify receipt key conflict: %w", err)
			}
			if existingDispatch != dispatch.String() || existingNode != nodeID ||
				existingSum != sum || existingDataset != datasetRID {
				return ErrReceiptKeyConflict
			}
		}
		return nil
	})
}

// MarkReceiptCommitted records a usableACK receipt (fenced). Committing
// an already-committed key with the same receipt is idempotent; the
// committed state is terminal for downgrade purposes.
func (r *Repository) MarkReceiptCommitted(ctx context.Context, eventID uuid.UUID, owner string, fence int64, key string, receipt json.RawMessage) error {
	return r.FencedWrite(ctx, eventID, owner, fence, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE scheduling.publish_receipts
			    SET state='committed', receipt=$2, updated_at=NOW()
			  WHERE key=$1 AND state <> 'committed'`,
			key, receipt)
		if err != nil {
			return fmt.Errorf("mark receipt committed: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// Either already committed (fine) or unknown key (bug).
			var state string
			if err := tx.QueryRow(ctx,
				`SELECT state FROM scheduling.publish_receipts WHERE key=$1`, key).Scan(&state); err != nil {
				return fmt.Errorf("verify receipt committed: %w", err)
			}
			if state != "committed" {
				return fmt.Errorf("receipt %s in state %s cannot be committed", key, state)
			}
		}
		return nil
	})
}

// MarkReceiptUnknown flags an unconfirmable publish for reconciliation
// (fenced). A committed receipt NEVER downgrades to unknown.
func (r *Repository) MarkReceiptUnknown(ctx context.Context, eventID uuid.UUID, owner string, fence int64, key string, reason string) error {
	return r.FencedWrite(ctx, eventID, owner, fence, func(ctx context.Context, tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE scheduling.publish_receipts
			    SET state='unknown', updated_at=NOW(),
			        receipt=COALESCE(receipt, jsonb_build_object('reason',$2))
			  WHERE key=$1 AND state <> 'committed'`,
			key, reason)
		if err != nil {
			return fmt.Errorf("mark receipt unknown: %w", err)
		}
		if tag.RowsAffected() == 0 {
			var state string
			if err := tx.QueryRow(ctx,
				`SELECT state FROM scheduling.publish_receipts WHERE key=$1`, key).Scan(&state); err != nil {
				return fmt.Errorf("verify receipt unknown: %w", err)
			}
			if state == "committed" {
				return fmt.Errorf("committed receipt %s cannot downgrade to unknown", key)
			}
		}
		return nil
	})
}


// EnqueueDLQViaOutbox records the dead-letter intent through the
// transactional outbox and flips the poison row to dlq_enqueued in the
// SAME transaction. IDEMPOTENT: the DLQ event id is derived from
// (topic,partition,offset), and the poison row is locked and inspected
// first — if a previous attempt already committed the intent (crash
// before the Kafka ACK, restart re-handling the same offset), the call
// returns nil without writing a second event. Only the FIRST call
// writes the outbox row and the state/audit (dlq_event_id).
func (r *Repository) EnqueueDLQViaOutbox(ctx context.Context, topic string, partition int, offset int64, raw []byte, reason string) error {
	beginner, ok := r.db.(txBeginner)
	if !ok {
		return errors.New("dlq enqueue requires a transaction-capable database")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin dlq tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Lock the poison row — it MUST exist (the caller counted first);
	// a stateless DLQ write would have no durable dedup. Already-
	// enqueued means idempotent success (crash between DLQ commit and
	// the Kafka ACK re-lands here and only ACKs).
	var state string
	err = tx.QueryRow(ctx,
		`SELECT state FROM scheduling.poison_messages
		  WHERE topic=$1 AND partition_id=$2 AND partition_offset=$3
		  FOR UPDATE`, topic, partition, offset).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("dlq refused: poison row missing (record attempts first)")
	}
	if err != nil {
		return fmt.Errorf("lock poison row for dlq: %w", err)
	}
	if state == "dlq_enqueued" || state == "dlq_published" {
		return tx.Commit(ctx) // idempotent no-op
	}

	dlqID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("scheduling_dlq%s%d%d", topic, partition, offset)))
	dlqEvent := outbox.New(dlqID, scheduleevents.AggregateType, fmt.Sprintf("poison:%s:%d:%d", topic, partition, offset), "__dlq."+topic,
		mustJSON(map[string]any{
			"topic": topic, "partition": partition, "offset": offset,
			"reason": sanitizeReason(reason), "raw_message": string(raw),
		}))
	if err := outbox.Enqueue(ctx, tx, dlqEvent); err != nil {
		return fmt.Errorf("enqueue dlq outbox: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE scheduling.poison_messages SET state='dlq_enqueued', dlq_event_id=$4
		  WHERE topic=$1 AND partition_id=$2 AND partition_offset=$3`,
		topic, partition, offset, dlqID); err != nil {
		return fmt.Errorf("mark poison dlq: %w", err)
	}
	return tx.Commit(ctx)
}

func mustJSON(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

// sanitizeReason strips control characters so DLQ payloads stay
// log-safe; raw secrets never appear here (reasons come from decode
// failures, not credentials).
func sanitizeReason(reason string) string {
	const max = 500
	if len(reason) > max {
		reason = reason[:max]
	}
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, reason)
}

// GetDispatch loads a dispatch by event id (fresh state for the
// execution worker after a claim).
func (r *Repository) GetDispatch(ctx context.Context, eventID uuid.UUID) (*SchedulingDispatch, error) {
	_, d, err := loadDispatch(ctx, r.db, eventID)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// GetGrantByRef loads a grant row by id (the dispatch's bound grant).
func (r *Repository) GetGrantByRef(ctx context.Context, grantRef uuid.UUID) (*SchedulingGrant, error) {
	var g SchedulingGrant
	var id, tenantID, subject, orgID string
	err := r.db.QueryRow(ctx,
		`SELECT id::text, tenant_kind, tenant_id::text, COALESCE(org_id::text,''), subject::text,
		        schedule_key, pipeline_rid, generation, state, expires_at, token_ciphertext
		   FROM scheduling.grants WHERE id=$1`, grantRef,
	).Scan(&id, &g.TenantKind, &tenantID, &orgID, &subject, &g.ScheduleKey, &g.PipelineRID,
		&g.Generation, &g.State, &g.ExpiresAt, &g.TokenCiphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get grant by ref: %w", err)
	}
	if g.ID, err = uuid.Parse(id); err != nil {
		return nil, err
	}
	if g.TenantID, err = uuid.Parse(tenantID); err != nil {
		return nil, err
	}
	if g.Subject, err = uuid.Parse(subject); err != nil {
		return nil, err
	}
	if orgID != "" {
		parsed, perr := uuid.Parse(orgID)
		if perr != nil {
			return nil, perr
		}
		g.OrgID = &parsed
	}
	return &g, nil
}


// GetPoisonState reads the durable per-offset poison phase: state ''
// with attempts 0 means never counted. The consumer consults this
// BEFORE any application processing so a restart (or a held message
// after a failed terminal phase) never re-enters admission once the
// budget is spent, and only ACKs once the DLQ intent is committed.
func (r *Repository) GetPoisonState(ctx context.Context, topic string, partition int, offset int64) (state string, attempts int, err error) {
	err = r.db.QueryRow(ctx,
		`SELECT state, attempts FROM scheduling.poison_messages
		  WHERE topic=$1 AND partition_id=$2 AND partition_offset=$3`,
		topic, partition, offset).Scan(&state, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, fmt.Errorf("get poison state: %w", err)
	}
	return state, attempts, nil
}
