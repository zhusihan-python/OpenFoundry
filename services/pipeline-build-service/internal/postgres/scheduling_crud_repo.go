// Scheduling CRUD for the T8 HTTP API (WP3.2, worker D). See
// docs/architecture/data-integration-t8-scheduling-decision.md §3/§4/§6.
//
// The *As methods are the ONLY entry points the HTTP surface may use:
// every one of them verifies the TRUSTED caller identity (derived from
// validated JWT claims, never the request body) against the resource
// owner INSIDE the same transaction that locks and writes the row:
//
//   - org domain:  the owner (grant created_by) or an org admin;
//   - personal:    ONLY the owner — an admin claim never widens it;
//   - a resource outside the caller's tenant domain is indistinguish-
//     able from a missing one (ErrSchedulingNotFound, no cross-domain
//     oracle).
//
// UpsertGrant remains a thin trusted fixture adapter to the same implementation.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	eventscheduler "github.com/openfoundry/openfoundry-go/libs/event-scheduler"
	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
)

// SchedulingActor is the trusted caller identity for scheduling CRUD.
// It is built exclusively from VALIDATED JWT claims at the HTTP edge
// (claims OrgID -> org domain; no OrgID -> personal domain); the
// request body never contributes to it.
type SchedulingActor struct {
	Tenant  scheduleevents.TenantRef
	Subject uuid.UUID
	Admin   bool // claims.HasRole("admin"); ignored in the personal domain
}

// MayManage reports whether the actor may manage a resource owned by
// `owner` inside tenant domain `kind`:
//   - personal: only the owner (subject == owner); an admin claim is
//     deliberately ignored — the personal domain has no wider role;
//   - org: the owner or an org admin;
//   - owner nil (ownership not establishable, e.g. the grant row is
//     gone): org admins only, personal always denies.
func (a SchedulingActor) MayManage(kind scheduleevents.TenantKind, owner *uuid.UUID) bool {
	if kind == scheduleevents.TenantKindPersonal {
		return owner != nil && a.Subject == *owner
	}
	if owner != nil && a.Subject == *owner {
		return true
	}
	return a.Admin
}

// CRUD error sentinels. The HTTP layer maps them onto status codes;
// wrap context is safe to carry in-process but must not leak resource
// existence across tenant domains.
var (
	// ErrSchedulingNotFound — the resource does not exist in the
	// caller's tenant domain (cross-domain access is identical).
	ErrSchedulingNotFound = errors.New("scheduling resource not found in caller domain")
	// ErrSchedulingForbidden — in-domain resource, actor is neither
	// owner nor (org domain) admin.
	ErrSchedulingForbidden = errors.New("actor may not manage this scheduling resource")
	// ErrGrantGenerationMismatch — the caller pinned a grant
	// generation that differs from the row's current one.
	ErrGrantGenerationMismatch = errors.New("grant generation mismatch")
	// ErrGrantNotActive — the grant row is not active (revoked/
	// suspended) or no longer satisfies the binding invariants.
	ErrGrantNotActive = errors.New("grant is not active")
	// ErrGrantExpired — the grant's stored expiry has passed (judged
	// on the post-lock DB wall clock).
	ErrGrantExpired = errors.New("grant expired")
	// ErrGrantPipelineMismatch — the grant is bound to a different
	// pipeline_rid than the definition being enabled.
	ErrGrantPipelineMismatch = errors.New("grant bound to a different pipeline")
	// ErrInvalidCron — the cron expression failed the same parser the
	// producer tick uses (unix5 flavor, UTC).
	ErrInvalidCron = errors.New("invalid cron expression")
)

// lockLatestGrantTx locks the newest grant row (any state) of
// (tenant, scheduleKey) FOR UPDATE and returns its created_by owner
// and generation. nil row means the key has no grant history at all.
// The FOR UPDATE serializes concurrent re-grants/revoke decisions; the
// first-ever grant for a key has no row to lock and is serialized by
// the partial unique active index instead.
func lockLatestGrantTx(ctx context.Context, tx pgx.Tx, tenant scheduleevents.TenantRef, scheduleKey string) (*uuid.UUID, int, error) {
	if err := lockSchedulingIdentity(ctx, tx, tenant, scheduleKey); err != nil {
		return nil, 0, err
	}
	var owner uuid.UUID
	var generation int
	err := tx.QueryRow(ctx,
		`SELECT created_by, generation FROM scheduling.grants
		  WHERE tenant_kind=$1 AND tenant_id=$2 AND schedule_key=$3
		  ORDER BY generation DESC LIMIT 1 FOR UPDATE`,
		string(tenant.Kind), tenant.ID, scheduleKey,
	).Scan(&owner, &generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("lock latest grant: %w", err)
	}
	return &owner, generation, nil
}

// UpsertGrantAs re-grants a schedule on behalf of a verified actor:
// the previous active rows are revoked and a fresh row with the next
// generation is inserted in ONE transaction. When the key already has
// grant history, the newest row is locked FOR UPDATE and the actor
// must be its owner (org admin override); a brand-new key needs no
// prior ownership. The caller has ALREADY validated the JWT (sub,
// exp, domain derivation) and encrypted the token.
func (r *Repository) UpsertGrantAs(ctx context.Context, actor SchedulingActor, g SchedulingGrant) (uuid.UUID, int, error) {
	beginner, ok := r.db.(txBeginner)
	if !ok {
		return uuid.Nil, 0, errors.New("upsert grant requires a transaction-capable database")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return uuid.Nil, 0, fmt.Errorf("begin grant tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	owner, _, err := lockLatestGrantTx(ctx, tx, actor.Tenant, g.ScheduleKey)
	if err != nil {
		return uuid.Nil, 0, err
	}
	if owner != nil && !actor.MayManage(actor.Tenant.Kind, owner) {
		return uuid.Nil, 0, ErrSchedulingForbidden
	}
	id, generation, err := upsertGrantCore(ctx, tx, g)
	if err != nil {
		return uuid.Nil, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, 0, fmt.Errorf("commit grant tx: %w", err)
	}
	return id, generation, nil
}

// upsertGrantCore is the shared persistence body (no actor checks):
// revoke the previous active rows, derive the next generation from the
// FULL history (not just active rows) and insert the fresh grant.
func upsertGrantCore(ctx context.Context, tx pgx.Tx, g SchedulingGrant) (uuid.UUID, int, error) {
	if _, err := tx.Exec(ctx,
		`UPDATE scheduling.grants SET state='revoked', updated_at=NOW()
		  WHERE tenant_kind=$1 AND tenant_id=$2 AND schedule_key=$3 AND state='active'`,
		g.TenantKind, g.TenantID, g.ScheduleKey); err != nil {
		return uuid.Nil, 0, fmt.Errorf("revoke previous grant: %w", err)
	}
	var generation int
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(generation),0)+1 FROM scheduling.grants
		  WHERE tenant_kind=$1 AND tenant_id=$2 AND schedule_key=$3`,
		g.TenantKind, g.TenantID, g.ScheduleKey).Scan(&generation); err != nil {
		return uuid.Nil, 0, fmt.Errorf("next grant generation: %w", err)
	}
	id := uuid.New()
	if _, err := tx.Exec(ctx,
		`INSERT INTO scheduling.grants
		   (id, tenant_kind, tenant_id, org_id, subject, schedule_key, pipeline_rid,
		    generation, state, expires_at, token_ciphertext, created_by)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'active',$9,$10,$11)`,
		id, g.TenantKind, g.TenantID, g.OrgID, g.Subject, g.ScheduleKey, g.PipelineRID,
		generation, g.ExpiresAt, g.TokenCiphertext, g.CreatedBy); err != nil {
		return uuid.Nil, 0, fmt.Errorf("insert grant: %w", err)
	}
	return id, generation, nil
}

// RevokeGrantAs revokes every active/suspended grant of the key on
// behalf of a verified actor. Idempotent: revoking an already-revoked
// key (by an authorized actor) is a no-op success. A key with no grant
// history in the caller's domain is ErrSchedulingNotFound — the same
// answer a foreign-domain key would produce.
func (r *Repository) RevokeGrantAs(ctx context.Context, actor SchedulingActor, scheduleKey string) error {
	beginner, ok := r.db.(txBeginner)
	if !ok {
		return errors.New("revoke grant requires a transaction-capable database")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin revoke tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	owner, _, err := lockLatestGrantTx(ctx, tx, actor.Tenant, scheduleKey)
	if err != nil {
		return err
	}
	if owner == nil {
		return ErrSchedulingNotFound
	}
	if !actor.MayManage(actor.Tenant.Kind, owner) {
		return ErrSchedulingForbidden
	}
	if _, err := tx.Exec(ctx,
		`UPDATE scheduling.grants SET state='revoked', updated_at=NOW()
		  WHERE tenant_kind=$1 AND tenant_id=$2 AND schedule_key=$3 AND state IN ('active','suspended')`,
		string(actor.Tenant.Kind), actor.Tenant.ID, scheduleKey); err != nil {
		return fmt.Errorf("revoke grant: %w", err)
	}
	return tx.Commit(ctx)
}

// definitionBinding is the locked view of a bound definition row plus
// its (possibly missing) owning grant.
type definitionBinding struct {
	DefinitionID uuid.UUID
	CronExpr     string
	PipelineRID  string
	GrantRef     *uuid.UUID
	GrantOwner   *uuid.UUID
}

// lockDefinitionBindingTx locks the pipeline_outbox_v1 definition row
// of (tenant, scheduleKey) FOR UPDATE and resolves its owning grant
// (grant created_by is the schedule's owner — the grantor and the
// run-as subject coincide on every API-created grant). Missing rows
// report ErrSchedulingNotFound: the tenant-scoped WHERE makes a
// foreign-domain key indistinguishable from an absent one.
func lockDefinitionBindingTx(ctx context.Context, tx pgx.Tx, tenant scheduleevents.TenantRef, scheduleKey string) (*definitionBinding, error) {
	var b definitionBinding
	err := tx.QueryRow(ctx,
		`SELECT d.id, d.cron_expr, d.pipeline_rid, d.grant_ref, g.created_by
		   FROM schedules.definitions d
		   LEFT JOIN scheduling.grants g ON g.id = d.grant_ref
		  WHERE d.dispatch_mode='pipeline_outbox_v1'
		    AND d.tenant_kind=$1 AND d.tenant_id=$2 AND d.schedule_key=$3
		  FOR UPDATE OF d`,
		string(tenant.Kind), tenant.ID, scheduleKey,
	).Scan(&b.DefinitionID, &b.CronExpr, &b.PipelineRID, &b.GrantRef, &b.GrantOwner)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSchedulingNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock definition binding: %w", err)
	}
	return &b, nil
}

// EnableSchedulingDefinitionAs binds (or rebinds) a definition for
// pipeline_outbox_v1 dispatch on behalf of a verified actor, in ONE
// transaction:
//
//   - the referenced grant row is locked FOR SHARE and must satisfy
//     id + EXACT generation + tenant domain + schedule_key + pipeline_rid
//   - active + unexpired (post-lock DB wall clock), and the actor
//     must be the grantor or an org admin — so a grant for pipeline P1
//     can never arm a definition for P2, and a non-owner in the same
//     org can never arm someone else's grant;
//   - the first next_run_at is the schedule's REAL next cron instant
//     (eventscheduler.ComputeNextFire, unix5/UTC — the same parser the
//     producer tick uses) computed from the post-lock DB clock, never
//     a fixed now()+1min;
//   - definition_revision advances atomically (ON CONFLICT DO UPDATE
//     +1). The INSERT branch floors the revision at the highest
//     revision ever audited for this definition id, so a re-created
//     row can never re-mint a revision whose event ids already exist.
//
// It returns the durable definition id, the resulting revision and the
// computed next fire instant.
func (r *Repository) EnableSchedulingDefinitionAs(ctx context.Context, actor SchedulingActor, scheduleKey, pipelineRID, buildBranch, cronExpr string, grantRef uuid.UUID, grantGeneration int) (uuid.UUID, int, time.Time, error) {
	beginner, ok := r.db.(txBeginner)
	if !ok {
		return uuid.Nil, 0, time.Time{}, errors.New("enable definition requires a transaction-capable database")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return uuid.Nil, 0, time.Time{}, fmt.Errorf("begin definition tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockSchedulingIdentity(ctx, tx, actor.Tenant, scheduleKey); err != nil {
		return uuid.Nil, 0, time.Time{}, err
	}

	// Situational grant check under lock. Tenant/schedule_key terms are
	// part of the WHERE so a foreign-domain grant id is simply absent.
	var gPipelineRID, gState string
	var gGeneration int
	var gExpiry time.Time
	var gOwner uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT pipeline_rid, generation, state, expires_at, created_by
		   FROM scheduling.grants
		  WHERE id=$1 AND tenant_kind=$2 AND tenant_id=$3 AND schedule_key=$4 AND subject=created_by
		  FOR SHARE`,
		grantRef, string(actor.Tenant.Kind), actor.Tenant.ID, scheduleKey,
	).Scan(&gPipelineRID, &gGeneration, &gState, &gExpiry, &gOwner)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, 0, time.Time{}, ErrSchedulingNotFound
	}
	if err != nil {
		return uuid.Nil, 0, time.Time{}, fmt.Errorf("verify grant for definition: %w", err)
	}
	if gGeneration != grantGeneration {
		return uuid.Nil, 0, time.Time{}, ErrGrantGenerationMismatch
	}
	if gPipelineRID != pipelineRID {
		return uuid.Nil, 0, time.Time{}, ErrGrantPipelineMismatch
	}
	if !actor.MayManage(actor.Tenant.Kind, &gOwner) {
		return uuid.Nil, 0, time.Time{}, ErrSchedulingForbidden
	}
	// Lock an existing binding before evaluating expiry; an upsert may wait here.
	if _, err := lockDefinitionBindingTx(ctx, tx, actor.Tenant, scheduleKey); err != nil && !errors.Is(err, ErrSchedulingNotFound) {
		return uuid.Nil, 0, time.Time{}, err
	}
	// Post-lock DB wall clock in a SEPARATE statement (same LockRows
	// rationale as the claim path): expiry is judged after all locks.
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return uuid.Nil, 0, time.Time{}, fmt.Errorf("definition wall clock: %w", err)
	}
	if gState != "active" {
		return uuid.Nil, 0, time.Time{}, ErrGrantNotActive
	}
	if !gExpiry.After(dbNow) {
		return uuid.Nil, 0, time.Time{}, ErrGrantExpired
	}

	// First/next fire instant from the SAME cron engine the producer
	// tick uses (unix5, UTC) — an unparseable expression fails the
	// enablement before any write.
	nextRunAt, err := computeNextRunAt(scheduleKey, cronExpr, dbNow)
	if err != nil {
		return uuid.Nil, 0, time.Time{}, err
	}

	definitionID := uuid.NewSHA1(uuid.NameSpaceOID,
		[]byte("sch:def:"+actor.Tenant.String()+":"+scheduleKey))
	var revision int
	err = tx.QueryRow(ctx,
		`INSERT INTO schedules.definitions
		   (id, name, cron_expr, cron_flavor, time_zone, enabled, topic,
		    payload_template, next_run_at, dispatch_mode, tenant_kind, tenant_id,
		    schedule_key, definition_revision, pipeline_rid, build_branch, grant_ref)
		 VALUES ($1,$2,$3,'unix5','UTC',true,$4,'{}'::jsonb,$5,
		         'pipeline_outbox_v1',$6,$7,$8,
		         (SELECT COALESCE(MAX(a.definition_revision),0)+1
		            FROM scheduling.dispatch_audit a WHERE a.definition_id=$1),
		         $9,$10,$11)
		 ON CONFLICT (id) DO UPDATE SET
		   enabled=true,
		   cron_expr=EXCLUDED.cron_expr,
		   topic=EXCLUDED.topic,
		   dispatch_mode='pipeline_outbox_v1',
		   tenant_kind=EXCLUDED.tenant_kind,
		   tenant_id=EXCLUDED.tenant_id,
		   schedule_key=EXCLUDED.schedule_key,
		   definition_revision=schedules.definitions.definition_revision+1,
		   pipeline_rid=EXCLUDED.pipeline_rid,
		   build_branch=EXCLUDED.build_branch,
		   grant_ref=EXCLUDED.grant_ref,
		   quarantined_at=NULL,
		   quarantine_reason=NULL,
		   next_run_at=EXCLUDED.next_run_at,
		   updated_at=NOW()
		 RETURNING definition_revision, next_run_at`,
		definitionID, "sch:"+actor.Tenant.String()+":"+scheduleKey, cronExpr,
		scheduleevents.TopicTriggers, nextRunAt,
		string(actor.Tenant.Kind), actor.Tenant.ID, scheduleKey,
		pipelineRID, buildBranch, grantRef).Scan(&revision, &nextRunAt)
	if err != nil {
		return uuid.Nil, 0, time.Time{}, fmt.Errorf("upsert definition binding: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, 0, time.Time{}, fmt.Errorf("commit definition tx: %w", err)
	}
	return definitionID, revision, nextRunAt, nil
}

// SetSchedulingDefinitionStateAs pauses or resumes a bound definition
// on behalf of a verified actor, bumping definition_revision on every
// transition. Pausing keeps dispatch_mode (the row never falls back to
// the legacy tick) and leaves next_run_at untouched. Resuming is an
// EXPLICIT RECOVERY: it re-validates the CURRENT grant inside the same
// transaction (active, unexpired on the post-lock DB clock, pipeline
// matching the binding), clears quarantine (T8 decision §3) and rearms
// next_run_at at the schedule's next future cron instant — a schedule
// paused across its fire slot resumes at the NEXT slot, never a
// retroactive catch-up fire.
func (r *Repository) SetSchedulingDefinitionStateAs(ctx context.Context, actor SchedulingActor, scheduleKey string, enabled bool) (int, error) {
	beginner, ok := r.db.(txBeginner)
	if !ok {
		return 0, errors.New("set definition state requires a transaction-capable database")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin definition state tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockSchedulingIdentity(ctx, tx, actor.Tenant, scheduleKey); err != nil {
		return 0, err
	}

	b, err := lockDefinitionBindingTx(ctx, tx, actor.Tenant, scheduleKey)
	if err != nil {
		return 0, err
	}
	if !actor.MayManage(actor.Tenant.Kind, b.GrantOwner) {
		return 0, ErrSchedulingForbidden
	}

	var nextRunAt *time.Time
	if enabled {
		// Re-validate the bound grant NOW (same tx, row locked FOR
		// SHARE so a concurrent revoke cannot slip in between).
		var gState, gPipelineRID string
		var gExpiry time.Time
		err = tx.QueryRow(ctx,
			`SELECT state, expires_at, pipeline_rid FROM scheduling.grants
			  WHERE id=$1 AND tenant_kind=$2 AND tenant_id=$3 AND schedule_key=$4 AND subject=created_by FOR SHARE`, b.GrantRef, string(actor.Tenant.Kind), actor.Tenant.ID, scheduleKey,
		).Scan(&gState, &gExpiry, &gPipelineRID)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("%w: bound grant vanished", ErrGrantNotActive)
		}
		if err != nil {
			return 0, fmt.Errorf("verify grant at resume: %w", err)
		}
		var dbNow time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
			return 0, fmt.Errorf("resume wall clock: %w", err)
		}
		if gState != "active" {
			return 0, ErrGrantNotActive
		}
		if !gExpiry.After(dbNow) {
			return 0, ErrGrantExpired
		}
		if gPipelineRID != b.PipelineRID {
			return 0, ErrGrantPipelineMismatch
		}
		next, err := computeNextRunAt(scheduleKey, b.CronExpr, dbNow)
		if err != nil {
			return 0, err
		}
		nextRunAt = &next
	}

	var revision int
	var query string
	var args []any
	if nextRunAt != nil {
		query = `UPDATE schedules.definitions
		            SET enabled=true, next_run_at=$4, definition_revision=definition_revision+1,
		                quarantined_at=NULL, quarantine_reason=NULL, updated_at=NOW()
		          WHERE id=$1 AND tenant_kind=$2 AND tenant_id=$3
		          RETURNING definition_revision`
		args = []any{b.DefinitionID, string(actor.Tenant.Kind), actor.Tenant.ID, *nextRunAt}
	} else {
		query = `UPDATE schedules.definitions
		            SET enabled=$4, definition_revision=definition_revision+1, updated_at=NOW()
		          WHERE id=$1 AND tenant_kind=$2 AND tenant_id=$3
		          RETURNING definition_revision`
		args = []any{b.DefinitionID, string(actor.Tenant.Kind), actor.Tenant.ID, enabled}
	}
	if err := tx.QueryRow(ctx, query, args...).Scan(&revision); err != nil {
		return 0, fmt.Errorf("set definition state: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit definition state tx: %w", err)
	}
	return revision, nil
}

// DisableSchedulingDefinitionAs removes the binding on behalf of a
// verified actor: dispatch_mode back to NULL AND enabled=false in one
// revision-advancing transaction — the row is dead for BOTH ticks (the
// new tick needs mode='pipeline_outbox_v1', the legacy tick needs
// enabled=true), so a disabled schedule never falls back to legacy
// dispatch. Revision stays monotonic for audit; re-enabling goes
// through EnableSchedulingDefinitionAs.
func (r *Repository) DisableSchedulingDefinitionAs(ctx context.Context, actor SchedulingActor, scheduleKey string) (int, error) {
	beginner, ok := r.db.(txBeginner)
	if !ok {
		return 0, errors.New("disable definition requires a transaction-capable database")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin disable tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := lockSchedulingIdentity(ctx, tx, actor.Tenant, scheduleKey); err != nil {
		return 0, err
	}

	b, err := lockDefinitionBindingTx(ctx, tx, actor.Tenant, scheduleKey)
	if err != nil {
		return 0, err
	}
	if !actor.MayManage(actor.Tenant.Kind, b.GrantOwner) {
		return 0, ErrSchedulingForbidden
	}

	var revision int
	if err := tx.QueryRow(ctx,
		`UPDATE schedules.definitions
		    SET dispatch_mode=NULL, enabled=false, definition_revision=definition_revision+1, updated_at=NOW()
		  WHERE id=$1 AND tenant_kind=$2 AND tenant_id=$3
		  RETURNING definition_revision`,
		b.DefinitionID, string(actor.Tenant.Kind), actor.Tenant.ID,
	).Scan(&revision); err != nil {
		return 0, fmt.Errorf("disable definition: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit disable tx: %w", err)
	}
	return revision, nil
}

// computeNextRunAt derives the next UTC fire instant strictly after
// `after` with the producer tick's own engine (unix5 flavor). Wrap +
// re-expose as ErrInvalidCron so the API maps parse failures to 400
// without importing the scheduler error types at the HTTP edge.
func computeNextRunAt(scheduleKey, cronExpr string, after time.Time) (time.Time, error) {
	def := eventscheduler.ScheduleDefinition{
		Name: scheduleKey, CronExpr: cronExpr,
		CronFlavor: "unix5", TimeZone: "UTC",
	}
	next, err := eventscheduler.ComputeNextFire(&def, after)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %v", ErrInvalidCron, err)
	}
	return next, nil
}

// UpsertGrant is a trusted fixture adapter to the same authorized persistence path.
func (r *Repository) UpsertGrant(ctx context.Context, g SchedulingGrant) (uuid.UUID, int, error) {
	return r.UpsertGrantAs(ctx, SchedulingActor{Tenant: scheduleevents.TenantRef{Kind: scheduleevents.TenantKind(g.TenantKind), ID: g.TenantID}, Subject: g.CreatedBy, Admin: true}, g)
}

func lockSchedulingIdentity(ctx context.Context, tx pgx.Tx, tenant scheduleevents.TenantRef, scheduleKey string) error {
	// Serialize the identity before reading history, including an absent first row.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, string(tenant.Kind)+":"+tenant.ID.String()+":"+scheduleKey); err != nil {
		return fmt.Errorf("lock grant identity: %w", err)
	}
	return nil
}
