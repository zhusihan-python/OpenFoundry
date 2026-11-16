// Package repo holds SQL queries + embedded migrations for
// connector-management-service.
package repo

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// MigrationsAdvisoryLockKey is the stable, Connector-specific cluster lock
// that serializes migration replays across concurrently starting replicas.
// It is the big-endian int64 of "OFCONNMG" — an opaque, never-reused value:
// changing it would let two versions of this service migrate the same
// database concurrently. Test-visible on purpose (the migration-concurrency
// regression asserts on it).
const MigrationsAdvisoryLockKey int64 = 0x4F46434F4E4E4D47

// migrationsRollbackBudget bounds the best-effort rollback that cleans up a
// failed migration transaction (its context is detached from the caller's,
// which may already be cancelled).
const migrationsRollbackBudget = 30 * time.Second

// Migrate replays every embedded migration on startup. Concurrent replicas
// (the three-copy acceptance starts them simultaneously) must not interleave
// DDL: file N of one process can run between file N-1 and N of another —
// e.g. the 20260424 ALTERs of sync_jobs interleaving with another process's
// 20260503 DROP — because every file is historically written idempotent for
// SEQUENTIAL replay, not for concurrent cross-process replay.
//
// Therefore the whole sorted batch runs inside ONE pool transaction on ONE
// connection, guarded by a TRANSACTION-scoped advisory lock
// (pg_advisory_xact_lock) acquired BEFORE any DDL: the second replica's
// Migrate blocks until the first commits, then replays the same idempotent
// SQL on the settled schema. The xact lock releases automatically with the
// transaction (commit or rollback) — no session-lock leak. Migration errors
// are returned verbatim (never swallowed, never retried); the historical SQL
// files are untouched.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	// Read every file BEFORE opening the transaction: an FS failure must not
	// take (or hold) the advisory lock.
	bodies := make(map[string][]byte, len(names))
	for _, name := range names {
		body, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		bodies[name] = body
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migrations transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			// Best-effort bounded rollback on a detached context (the
			// caller's ctx may be cancelled); releasing the transaction also
			// releases the xact advisory lock. The PRIMARY error is what the
			// caller sees — a rollback failure never masks it.
			rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), migrationsRollbackBudget)
			defer cancel()
			if rbErr := tx.Rollback(rbCtx); rbErr != nil {
				slog.Warn("migrations rollback after failure", slog.String("error", rbErr.Error()))
			}
		}
	}()

	// Serialize against every other replica BEFORE any DDL, on the same
	// connection that will run the batch. Blocking is intended: startup waits
	// for the current holder to commit; the caller's ctx still bounds the
	// wait.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, MigrationsAdvisoryLockKey); err != nil {
		return fmt.Errorf("acquire migrations advisory lock: %w", err)
	}
	for _, name := range names {
		if _, err := tx.Exec(ctx, string(bodies[name])); err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	committed = true
	return nil
}

type Repo struct{ Pool *pgxpool.Pool }

// Tenant is the trusted access domain derived from verified JWT claims.
// OrgID != nil scopes access to rows belonging to that organization; OrgID ==
// nil is the personal domain where only rows with a NULL org owned by OwnerID
// are visible. Client-supplied headers or body fields never widen it.
type Tenant struct {
	OwnerID uuid.UUID
	OrgID   *uuid.UUID
}

// connectionTenantPredicate returns the SQL predicate (against alias `c`)
// plus its argument for the tenant, using argPos as the $N placeholder.
func connectionTenantPredicate(t Tenant, argPos int) (string, any) {
	if t.OrgID != nil {
		return fmt.Sprintf("c.org_id = $%d", argPos), *t.OrgID
	}
	return fmt.Sprintf("(c.org_id IS NULL AND c.owner_id = $%d)", argPos), t.OwnerID
}

const connectionSelect = `SELECT c.id, c.name, c.connector_type, c.config, c.status,
	c.owner_id, c.org_id, c.last_sync_at, c.created_at, c.updated_at FROM connections c`

func (r *Repo) ListConnections(ctx context.Context, t Tenant, ownerFilter *uuid.UUID) ([]models.Connection, error) {
	predicate, arg := connectionTenantPredicate(t, 1)
	query := connectionSelect + ` WHERE ` + predicate
	args := []any{arg}
	if ownerFilter != nil {
		args = append(args, *ownerFilter)
		query += fmt.Sprintf(` AND owner_id = $%d`, len(args))
	}
	rows, err := r.Pool.Query(ctx, query+` ORDER BY created_at DESC LIMIT 500`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]models.Connection, 0)
	for rows.Next() {
		v, err := scanConnection(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

// ListAllConnections enumerates every connection for background maintenance
// sweeps (auto-registration). It is not exposed on any request surface; the
// HTTP list endpoints stay tenant-scoped via ListConnections.
func (r *Repo) ListAllConnections(ctx context.Context) ([]models.Connection, error) {
	rows, err := r.Pool.Query(ctx, connectionSelect+` ORDER BY created_at DESC LIMIT 500`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]models.Connection, 0)
	for rows.Next() {
		v, err := scanConnection(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

func (r *Repo) GetConnection(ctx context.Context, id uuid.UUID) (*models.Connection, error) {
	row := r.Pool.QueryRow(ctx, connectionSelect+` WHERE id = $1`, id)
	v, err := scanConnection(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

// GetConnectionForTenant loads a connection only when it belongs to the
// caller's tenant domain.
func (r *Repo) GetConnectionForTenant(ctx context.Context, id uuid.UUID, t Tenant) (*models.Connection, error) {
	predicate, arg := connectionTenantPredicate(t, 2)
	row := r.Pool.QueryRow(ctx, connectionSelect+` WHERE id = $1 AND `+predicate, id, arg)
	v, err := scanConnection(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

func (r *Repo) CreateConnection(ctx context.Context, body *models.CreateConnectionRequest, t Tenant) (*models.Connection, error) {
	// P2.3: the repository validates the sync.admission subtree of every NEW
	// config itself — the HTTP normalization already does, but direct repo
	// callers must not be able to persist an illegal policy. An absent block
	// (also the `{}` fallback for an empty body) is legal and enables
	// nothing. Illegal policy → zero rows written.
	if len(body.Config) > 0 {
		if _, err := models.ParseSyncAdmissionPolicy(body.Config); err != nil {
			return nil, err
		}
	}
	id := uuid.New()
	cfg := body.Config
	if len(cfg) == 0 {
		cfg = []byte(`{}`)
	}
	row := r.Pool.QueryRow(ctx,
		`INSERT INTO connections (id, name, connector_type, config, owner_id, org_id)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id, name, connector_type, config, status, owner_id, org_id,
		           last_sync_at, created_at, updated_at`,
		id, strings.TrimSpace(body.Name), body.ConnectorType, cfg, t.OwnerID, t.OrgID,
	)
	return scanConnection(row)
}

// UpdateConnection mutates a source inside a two-phase transaction: the
// source row is locked FOR UPDATE first, then — if the request carries a
// config mutation — the P2.3 sync.admission subtree of the NEW config is
// validated (repo-side, so direct callers cannot persist an illegal policy;
// illegal → zero writes) and the pending/running run check is re-evaluated
// after the lock, so a run enqueued concurrently with this update is actually
// observed and the mutation is refused with ErrActiveRuns. Name/status-only
// updates stay allowed while runs are active and never re-validate the OLD
// stored config.
func (r *Repo) UpdateConnection(ctx context.Context, id uuid.UUID, t Tenant, body *models.UpdateConnectionRequest) (*models.Connection, error) {
	// Validate the NEW config's admission subtree before any lock or write:
	// an illegal policy must not even take the source row lock (which would
	// momentarily block admissions), and a refused update changes nothing.
	if len(body.Config) > 0 {
		if _, err := models.ParseSyncAdmissionPolicy(body.Config); err != nil {
			return nil, err
		}
	}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	predicate, arg := connectionTenantPredicate(t, 2)
	var locked uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT c.id FROM connections c WHERE c.id = $1 AND `+predicate+` FOR UPDATE`,
		id, arg).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(body.Config) > 0 {
		active, err := txSourceHasActiveRuns(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if active {
			return nil, ErrActiveRuns
		}
	}
	// Re-read the current row under the held lock and merge the patch.
	row := tx.QueryRow(ctx, connectionSelect+` WHERE c.id = $1`, id)
	current, err := scanConnection(row)
	if err != nil {
		return nil, err
	}
	// P2.5 fence: an effective config change (byte-different) is a protected
	// input mutation. Def locks are taken sorted BEFORE the protected row is
	// mutated, so the Def->Checkpoint->Run finalize chain can reread the
	// binding without a reverse Source lock; any unresolved checkpoint owner
	// refuses the edit. Same-value config writes skip the fence; pure
	// name/status edits never hit it.
	if len(body.Config) > 0 && !bytes.Equal(current.Config, body.Config) {
		defIDs, err := lockSourceDefsForProtection(ctx, tx, id, t)
		if err != nil {
			return nil, err
		}
		if err := assertCheckpointOwnersFreeTx(ctx, tx, defIDs); err != nil {
			return nil, err
		}
	}
	name := current.Name
	if body.Name != nil {
		name = *body.Name
	}
	cfg := current.Config
	if len(body.Config) > 0 {
		cfg = body.Config
	}
	status := current.Status
	if body.Status != nil {
		status = *body.Status
	}
	updated := tx.QueryRow(ctx,
		`UPDATE connections c SET name = $2, config = $3, status = $4, updated_at = $5
		 WHERE c.id = $1
		 RETURNING c.id, c.name, c.connector_type, c.config, c.status, c.owner_id, c.org_id,
		           c.last_sync_at, c.created_at, c.updated_at`,
		id, name, cfg, status, time.Now().UTC(),
	)
	out, err := scanConnection(updated)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Repo) DeleteConnection(ctx context.Context, id uuid.UUID, t Tenant) (bool, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	predicate, arg := connectionTenantPredicate(t, 2)
	var locked uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT c.id FROM connections c WHERE c.id = $1 AND `+predicate+` FOR UPDATE`,
		id, arg).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Post-lock re-check: an enqueue that committed while we waited for the
	// FOR UPDATE (it holds FOR SHARE through its insert) is visible here, and
	// the cascade delete must not silently swallow its pending run.
	active, err := txSourceHasActiveRuns(ctx, tx, id)
	if err != nil {
		return false, err
	}
	if active {
		return false, ErrActiveRuns
	}
	// P2.5 fence: an unresolved checkpoint owner blocks the cascade (its
	// checkpoint row would vanish under the owner). Def locks are taken
	// sorted before the protected delete.
	defIDs, err := lockSourceDefsForProtection(ctx, tx, id, t)
	if err != nil {
		return false, err
	}
	if err := assertCheckpointOwnersFreeTx(ctx, tx, defIDs); err != nil {
		return false, err
	}
	cmd, err := tx.Exec(ctx, `DELETE FROM connections WHERE id = $1`, id)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return cmd.RowsAffected() > 0, nil
}

type rowLikeT interface{ Scan(...any) error }

func scanConnection(r rowLikeT) (*models.Connection, error) {
	v := &models.Connection{}
	if err := r.Scan(&v.ID, &v.Name, &v.ConnectorType, &v.Config, &v.Status,
		&v.OwnerID, &v.OrgID, &v.LastSyncAt, &v.CreatedAt, &v.UpdatedAt); err != nil {
		return nil, err
	}
	return v, nil
}

func (r *Repo) ListCredentials(ctx context.Context, sourceID uuid.UUID, t Tenant) ([]models.CredentialResponse, error) {
	predicate, arg := connectionTenantPredicate(t, 2)
	rows, err := r.Pool.Query(ctx,
		`SELECT sc.id, sc.source_id, sc.kind, sc.fingerprint, sc.created_at
		 FROM source_credentials sc JOIN connections c ON c.id = sc.source_id
		 WHERE sc.source_id = $1 AND `+predicate+` ORDER BY sc.created_at DESC`, sourceID, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]models.CredentialResponse, 0)
	for rows.Next() {
		v := models.CredentialResponse{}
		if err := rows.Scan(&v.ID, &v.SourceID, &v.Kind, &v.Fingerprint, &v.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// SetCredential stores (or rotates) a source credential. P2.5: this used to
// be a lockless single-statement UPSERT; it is now a Source-locked
// transaction that takes the sorted Def locks BEFORE mutating the protected
// credential row and refuses while any def of the source holds an unresolved
// checkpoint owner (root implementation-release correction 3). Same-value
// rotation is NOT special-cased: the credential content fingerprint is
// unchanged for identical plaintext, so the binding stays stable and nothing
// downstream is invalidated; a real value change changes the fingerprint,
// making the stored checkpoint binding stale (recovery: newDef-only).
func (r *Repo) SetCredential(ctx context.Context, sourceID uuid.UUID, t Tenant, kind string, ciphertext []byte, fingerprint string) (*models.CredentialResponse, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollbackCheckpointTx(tx)
	if err := lockSourceRowForUpdate(ctx, tx, sourceID, t); err != nil {
		if errors.Is(err, errCPNotVisible) {
			// Source invisible in this tenant: nothing attaches, same shape
			// as the other tenant-scoped writers.
			return nil, nil
		}
		return nil, err
	}
	defIDs, err := lockSourceDefsForProtection(ctx, tx, sourceID, t)
	if err != nil {
		return nil, err
	}
	if err := assertCheckpointOwnersFreeTx(ctx, tx, defIDs); err != nil {
		return nil, err
	}
	// P2.6 fence (v2.1): an EFFECTIVE credential change (a different content
	// fingerprint — same-value rotations keep the fingerprint and stay
	// allowed) is refused while any def of this source holds a pending or
	// running p26-v1 run: the frozen recovery bindings (incremental AND
	// snapshot) hash credential fingerprints. Pre-P26 runs never trip this.
	var storedFingerprint *string
	if ferr := tx.QueryRow(ctx,
		`SELECT sc.fingerprint FROM source_credentials sc
		  WHERE sc.source_id = $1 AND sc.kind = $2`, sourceID, kind).Scan(&storedFingerprint); ferr != nil && !errors.Is(ferr, pgx.ErrNoRows) {
		return nil, ferr
	}
	if storedFingerprint == nil || *storedFingerprint != fingerprint {
		if err := assertNoRecoveryProtocolRunsTx(ctx, tx, defIDs); err != nil {
			return nil, err
		}
	}
	predicate, arg := connectionTenantPredicate(t, 6)
	row := tx.QueryRow(ctx,
		`INSERT INTO source_credentials (id, source_id, kind, secret_ciphertext, fingerprint)
		 SELECT $1, c.id, $3, $4, $5 FROM connections c WHERE c.id = $2 AND `+predicate+`
		 ON CONFLICT (source_id, kind) DO UPDATE
		   SET secret_ciphertext = EXCLUDED.secret_ciphertext,
		       fingerprint = EXCLUDED.fingerprint,
		       created_at = NOW()
		 RETURNING id, source_id, kind, fingerprint, created_at`,
		uuid.New(), sourceID, kind, ciphertext, fingerprint, arg)
	v := &models.CredentialResponse{}
	if err := row.Scan(&v.ID, &v.SourceID, &v.Kind, &v.Fingerprint, &v.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return v, nil
}

// GetSourceCredentialCiphertext returns the encrypted secret stored for the
// given credential kind on a source inside the caller's tenant, or nil when
// either the source or the credential does not exist in that domain.
func (r *Repo) GetSourceCredentialCiphertext(ctx context.Context, sourceID uuid.UUID, t Tenant, kind string) ([]byte, error) {
	predicate, arg := connectionTenantPredicate(t, 3)
	var ciphertext []byte
	err := r.Pool.QueryRow(ctx,
		`SELECT sc.secret_ciphertext
		 FROM source_credentials sc JOIN connections c ON c.id = sc.source_id
		 WHERE sc.source_id = $1 AND sc.kind = $2 AND `+predicate,
		sourceID, kind, arg).Scan(&ciphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return ciphertext, nil
}

func (r *Repo) ListSourcePolicies(ctx context.Context, sourceID uuid.UUID, t Tenant) ([]models.SourcePolicyBindingResponse, error) {
	predicate, arg := connectionTenantPredicate(t, 2)
	rows, err := r.Pool.Query(ctx,
		`SELECT b.source_id, b.policy_id, b.kind
		 FROM source_policy_bindings b JOIN connections c ON c.id = b.source_id
		 WHERE b.source_id = $1 AND `+predicate+` ORDER BY b.created_at DESC`, sourceID, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]models.SourcePolicyBindingResponse, 0)
	for rows.Next() {
		v := models.SourcePolicyBindingResponse{}
		if err := rows.Scan(&v.SourceID, &v.PolicyID, &v.Kind); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (r *Repo) AttachPolicy(ctx context.Context, sourceID uuid.UUID, t Tenant, policyID uuid.UUID, kind string) (*models.SourcePolicyBindingResponse, error) {
	predicate, arg := connectionTenantPredicate(t, 4)
	row := r.Pool.QueryRow(ctx,
		`INSERT INTO source_policy_bindings (source_id, policy_id, kind)
		 SELECT c.id, $2, $3 FROM connections c WHERE c.id = $1 AND `+predicate+`
		 ON CONFLICT (source_id, policy_id) DO UPDATE SET kind = EXCLUDED.kind
		 RETURNING source_id, policy_id, kind`, sourceID, policyID, kind, arg)
	v := &models.SourcePolicyBindingResponse{}
	if err := row.Scan(&v.SourceID, &v.PolicyID, &v.Kind); err != nil {
		return nil, err
	}
	return v, nil
}

func (r *Repo) DetachPolicy(ctx context.Context, sourceID uuid.UUID, t Tenant, policyID uuid.UUID) (bool, error) {
	predicate, arg := connectionTenantPredicate(t, 3)
	cmd, err := r.Pool.Exec(ctx,
		`DELETE FROM source_policy_bindings b USING connections c
		 WHERE b.source_id = c.id AND b.source_id = $1 AND `+predicate+` AND b.policy_id = $2`,
		sourceID, policyID, arg)
	if err != nil {
		return false, err
	}
	return cmd.RowsAffected() > 0, nil
}

const syncJobSelect = `SELECT d.id, d.source_id, d.output_dataset_id, d.file_glob, d.schedule_cron,
	d.schedule_time_zone, d.schedule_enabled, d.schedule_revision, d.schedule_next_fire_at,
	d.schedule_last_fire_at, d.schedule_last_tick_at, d.schedule_last_tick_outcome, d.org_id, d.created_at
	FROM batch_sync_defs d`

// syncDefTenantPredicate scopes batch_sync_defs (alias `d`) by its persisted
// org ownership; legacy NULL-org defs fall back to the source owner domain.
func syncDefTenantPredicate(t Tenant, argPos int) (string, any) {
	if t.OrgID != nil {
		return fmt.Sprintf("d.org_id = $%d", argPos), *t.OrgID
	}
	return fmt.Sprintf("(d.org_id IS NULL AND EXISTS (SELECT 1 FROM connections pc WHERE pc.id = d.source_id AND pc.owner_id = $%d))", argPos), t.OwnerID
}

func (r *Repo) ListSyncJobs(ctx context.Context, sourceID uuid.UUID, t Tenant) ([]models.SyncJob, error) {
	predicate, arg := syncDefTenantPredicate(t, 2)
	rows, err := r.Pool.Query(ctx, syncJobSelect+` WHERE d.source_id = $1 AND `+predicate+` ORDER BY d.created_at DESC`, sourceID, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]models.SyncJob, 0)
	for rows.Next() {
		v, err := scanSyncJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

func (r *Repo) GetSyncJob(ctx context.Context, id uuid.UUID, t Tenant) (*models.SyncJob, error) {
	predicate, arg := syncDefTenantPredicate(t, 2)
	row := r.Pool.QueryRow(ctx, syncJobSelect+` WHERE d.id = $1 AND `+predicate, id, arg)
	v, err := scanSyncJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

// CreateSyncJob persists a new def with its P2.4 schedule state. The merged
// (cron, time zone, enabled) spec is validated here authoritatively (the
// handler pre-validates for a clean 400 — the P2.3 dual-layer discipline); an
// invalid spec refuses with *models.InvalidScheduleError before any write.
// The first cursor, when the def is created enabled, is computed from the
// transaction's database clock (the first fire is strictly in the future);
// every def starts at schedule_revision = 1.
func (r *Repo) CreateSyncJob(ctx context.Context, body *models.CreateSyncJobRequest, t Tenant) (*models.SyncJob, error) {
	cron := ""
	if body.ScheduleCron != nil {
		cron = *body.ScheduleCron
	}
	tz := ""
	if body.ScheduleTimeZone != nil {
		tz = *body.ScheduleTimeZone
	}
	enabled := body.ScheduleEnabled != nil && *body.ScheduleEnabled
	spec, err := models.NormalizeScheduleSpec(cron, tz, enabled)
	if err != nil {
		return nil, err
	}
	var cronArg *string
	if spec.Cron != "" {
		cronArg = &spec.Cron
	}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return nil, err
	}
	var next *time.Time
	if spec.Enabled {
		n, err := models.NextScheduleFire(spec.Cron, spec.TimeZone, dbNow)
		if err != nil {
			return nil, err
		}
		next = &n
	}
	predicate, arg := connectionTenantPredicate(t, 6)
	row := tx.QueryRow(ctx,
		`INSERT INTO batch_sync_defs
		       (id, source_id, output_dataset_id, file_glob, schedule_cron, org_id,
		        schedule_time_zone, schedule_enabled, schedule_revision, schedule_next_fire_at)
		 SELECT $1, c.id, $3, $4, $5, c.org_id, $7, $8, 1, $9
		   FROM connections c WHERE c.id = $2 AND `+predicate+`
		 RETURNING id, source_id, output_dataset_id, file_glob, schedule_cron,
		           schedule_time_zone, schedule_enabled, schedule_revision, schedule_next_fire_at,
		           schedule_last_fire_at, schedule_last_tick_at, schedule_last_tick_outcome, org_id, created_at`,
		uuid.New(), body.SourceID, body.OutputDatasetID, body.FileGlob, cronArg, arg,
		spec.TimeZone, spec.Enabled, next,
	)
	v, err := scanSyncJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return v, nil
}

// UpdateSyncJob mutates a sync def inside a two-phase transaction: the def
// row is locked FOR UPDATE first; when the request retargets the output
// dataset (a different id than the current one) the pending/running check is
// re-evaluated after the lock and the mutation is refused with ErrActiveRuns,
// so an active durable task can never be silently repointed. Same-target
// writes and file_glob/schedule changes stay allowed.
//
// P2.4 schedule semantics (root contract): omitted fields preserve their
// values; the merged (cron, time zone, enabled) spec is validated
// authoritatively here (*models.InvalidScheduleError refuses with zero rows
// written); an empty cron explicitly clears/disables. An ACTUAL change to
// cron / time zone / enabled / output target / file_glob bumps
// schedule_revision (invalidating every outstanding schedule grant) and, while
// enabled, recomputes the cursor from the locked database time (disable parks
// the cursor NULL). Same-value patches neither bump the revision nor touch
// the cursor.
func (r *Repo) UpdateSyncJob(ctx context.Context, id uuid.UUID, body *models.UpdateSyncJobRequest, t Tenant) (*models.SyncJob, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	predicate, arg := syncDefTenantPredicate(t, 2)
	var locked uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT d.id FROM batch_sync_defs d WHERE d.id = $1 AND `+predicate+` FOR UPDATE`,
		id, arg).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	row := tx.QueryRow(ctx, syncJobSelect+` WHERE d.id = $1`, id)
	current, err := scanSyncJob(row)
	if err != nil {
		return nil, err
	}
	// P2.5 fence: an effective output-target or file_glob change is a
	// protected def-input mutation. The def row is already locked FOR UPDATE
	// above (this single-def assert needs no additional locks and cannot
	// deadlock against the sorted multi-def writers); any unresolved
	// checkpoint owner — regardless of the owned run's state — refuses the
	// edit. Pure cron/enable/name edits never hit this fence.
	//
	// P2.6 fence (v2.1): an effective def-input change is ALSO refused while
	// a pending/running p26-v1 run exists for this def — the snapshot
	// recovery binding hashes the target and file_glob, and the incremental
	// binding already covered them; the v1 fence extends the protection to
	// snapshot runs (which hold no checkpoint owner). Pre-P26 runs never trip
	// it.
	defInputChanged := (body.OutputDatasetID != nil && *body.OutputDatasetID != current.OutputDatasetID) ||
		(body.FileGlob != nil && !strPtrEqual(body.FileGlob, current.FileGlob))
	if defInputChanged {
		if err := assertCheckpointOwnersFreeTx(ctx, tx, []uuid.UUID{id}); err != nil {
			return nil, err
		}
		if err := assertNoRecoveryProtocolRunsTx(ctx, tx, []uuid.UUID{id}); err != nil {
			return nil, err
		}
	}
	output := current.OutputDatasetID
	if body.OutputDatasetID != nil {
		if *body.OutputDatasetID != current.OutputDatasetID {
			active, err := txDefHasActiveRuns(ctx, tx, id)
			if err != nil {
				return nil, err
			}
			if active {
				return nil, ErrActiveRuns
			}
		}
		output = *body.OutputDatasetID
	}
	fileGlob := current.FileGlob
	if body.FileGlob != nil {
		fileGlob = body.FileGlob
	}
	currentCron := ""
	if current.ScheduleCron != nil {
		currentCron = *current.ScheduleCron
	}
	cron := currentCron
	if body.ScheduleCron != nil {
		cron = *body.ScheduleCron
	}
	tz := current.ScheduleTimeZone
	if body.ScheduleTimeZone != nil {
		tz = *body.ScheduleTimeZone
	}
	enabled := current.ScheduleEnabled
	if body.ScheduleEnabled != nil {
		enabled = *body.ScheduleEnabled
	}
	// Explicit clear: an empty cron clears AND disables the schedule (cursor
	// NULL, revision bump below). A contradictory explicit enabled=true in
	// the same request keeps `enabled` true and fails the validation below
	// ("enable requires a valid non-empty cron").
	if body.ScheduleCron != nil && strings.TrimSpace(*body.ScheduleCron) == "" && body.ScheduleEnabled == nil {
		enabled = false
	}
	// Legacy-compatibility validation (root round-1 rule): validate what this
	// request actually supplies — an explicitly supplied NEW non-empty cron
	// and/or time zone must be valid even while disabled; enabling requires
	// the MERGED cron to be valid and non-empty. An untouched legacy invalid
	// disabled cron survives untouched (name-only/file_glob patches never
	// re-validate stored schedule data), and every merged state that will be
	// persisted with a non-empty cron is stored only in valid form.
	newCronSupplied := body.ScheduleCron != nil && strings.TrimSpace(*body.ScheduleCron) != ""
	newTZSupplied := body.ScheduleTimeZone != nil && strings.TrimSpace(*body.ScheduleTimeZone) != ""
	if newTZSupplied {
		if _, err := models.ResolveScheduleTimeZone(*body.ScheduleTimeZone); err != nil {
			return nil, err
		}
	}
	if newCronSupplied {
		effectiveTZ := tz
		if body.ScheduleTimeZone != nil {
			effectiveTZ = *body.ScheduleTimeZone
		}
		if _, err := models.ParseScheduleCron(*body.ScheduleCron, effectiveTZ); err != nil {
			return nil, err
		}
	}
	if enabled {
		spec, err := models.NormalizeScheduleSpec(cron, tz, true)
		if err != nil {
			return nil, err
		}
		cron = spec.Cron
		tz = spec.TimeZone
	}

	// Storage values: trimmed cron (empty = NULL) and a non-empty time zone.
	storeCron := strings.TrimSpace(cron)
	storeTZ := strings.TrimSpace(tz)
	if storeTZ == "" {
		storeTZ = "UTC"
	}

	// Actual-change detection: same-value patches keep the revision (and the
	// arm bound to it) intact.
	changed := currentCron != storeCron ||
		current.ScheduleTimeZone != storeTZ ||
		current.ScheduleEnabled != enabled ||
		output != current.OutputDatasetID ||
		!strPtrEqual(fileGlob, current.FileGlob)
	newRevision := current.ScheduleRevision
	next := current.ScheduleNextFireAt
	if changed {
		newRevision++
		if enabled {
			// The enabled merged spec was validated above; the cursor comes
			// from the locked database clock.
			var dbNow time.Time
			if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
				return nil, err
			}
			n, err := models.NextScheduleFire(storeCron, storeTZ, dbNow)
			if err != nil {
				return nil, err
			}
			next = &n
		} else {
			next = nil
		}
	}
	var cronArg *string
	if storeCron != "" {
		cronArg = &storeCron
	}
	updated := tx.QueryRow(ctx,
		`UPDATE batch_sync_defs d
		    SET output_dataset_id = $2, file_glob = $3, schedule_cron = $4,
		        schedule_time_zone = $5, schedule_enabled = $6,
		        schedule_revision = $7, schedule_next_fire_at = $8
		  WHERE d.id = $1
		  RETURNING d.id, d.source_id, d.output_dataset_id, d.file_glob, d.schedule_cron,
		            d.schedule_time_zone, d.schedule_enabled, d.schedule_revision, d.schedule_next_fire_at,
		            d.schedule_last_fire_at, d.schedule_last_tick_at, d.schedule_last_tick_outcome, d.org_id, d.created_at`,
		id, output, fileGlob, cronArg, storeTZ, enabled, newRevision, next,
	)
	out, err := scanSyncJob(updated)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// strPtrEqual compares two optional strings by value (nil == nil, nil != "").
func strPtrEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// runPublicColumns is the public sync_runs projection (no lease columns —
// the lease token must never leave the repo layer). Keep in sync with
// scanSyncRun.
const runPublicColumns = `r.id, r.sync_def_id, r.status, r.org_id, r.started_at, r.finished_at,
	r.bytes_written, r.files_written, r.rows_written, r.error, r.ingest_job_id,
	r.dataset_version_id, r.content_hash, r.requested_by, r.owner_id, r.attempt, r.generation,
	r.control_mode, r.cancel_requested_at, r.cancelled_by, r.publish_entered_at,
	COALESCE(r.recovery_protocol, '')`

// txSourceHasActiveRuns reports pending/running runs for a source inside an
// open transaction. The caller must already hold the source row lock: then
// this read observes every enqueue that committed before the lock was
// acquired.
func txSourceHasActiveRuns(ctx context.Context, tx pgx.Tx, sourceID uuid.UUID) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		    SELECT 1 FROM sync_runs r
		      JOIN batch_sync_defs d ON d.id = r.sync_def_id
		     WHERE d.source_id = $1 AND r.status IN ('pending','running'))`,
		sourceID).Scan(&exists)
	return exists, err
}

// txDefHasActiveRuns is txSourceHasActiveRuns scoped to one sync def.
func txDefHasActiveRuns(ctx context.Context, tx pgx.Tx, defID uuid.UUID) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		    SELECT 1 FROM sync_runs r
		     WHERE r.sync_def_id = $1 AND r.status IN ('pending','running'))`,
		defID).Scan(&exists)
	return exists, err
}

// RunSyncJob is the legacy pre-P2.1 trigger, retained for compatibility with
// existing callers/tests: it produces a *running* row with no lease that
// CompleteSyncRun can still close exactly once. It shares the P2.3 admission
// protocol with EnqueueSyncRun — trusted-tenant def resolution first (clean
// nil on an invisible def, before any quota), the mode pre-read executed ON
// THE TRANSACTION's own connection (never a second pool connection: a
// single-conn pool would self-deadlock), FOR UPDATE / FOR UPDATE NOWAIT on
// the resolved source, post-lock def-binding re-verification (FOR KEY SHARE,
// the INSERT's own Source→Def order), admissionGate, then INSERT →
// pending→running → write-time-anchored sync_last_admitted_at advance, all in
// one transaction. Both run-creation paths are therefore bounded by ONE
// per-source policy; a rejected legacy trigger returns the typed
// *AdmissionRejection and creates no row. All legacy row semantics are
// unchanged: no lease, no control_mode ('legacy' default), and the def row
// stays protected by the insert's FK KEY SHARE against UpdateSyncJob's FOR
// UPDATE — no def→source lock order is introduced.
func (r *Repo) RunSyncJob(ctx context.Context, id uuid.UUID, t Tenant) (*models.SyncRun, error) {
	defPredicate, defArg := syncDefTenantPredicate(t, 2)
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollbackAdmissionTx(tx)
	// Resolve the def's source (trusted tenant + parent chain): an invisible
	// def answers the clean legacy nil before any lock or quota state.
	var sourceID uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT d.source_id FROM batch_sync_defs d WHERE d.id = $1 AND `+defPredicate,
		id, defArg).Scan(&sourceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// Mode pre-read on the SAME connection (see the doc comment): lockless
	// w.r.t. the source row, and never a second pool connection.
	nowait := admissionNowait(ctx, tx, sourceID, t)
	err = lockSourceForAdmission(ctx, tx, sourceID, t, nowait)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		if nowait && isLockNotAvailable(err) {
			return nil, &AdmissionRejection{Reason: AdmissionReasonContention, RetryAfter: advisoryRetryAfter}
		}
		return nil, err
	}
	// P2.5 (source review item 4): this legacy path creates an UNLEASED
	// running row outside checkpoint ownership. An opt-in incremental def
	// must never gain such a bypass run — reject explicitly, with zero new
	// rows and zero admission stamp, before the def lock/gate/insert. The
	// probe is deliberately lightweight (no full REST parse: legacy `{}` and
	// non-rest_api sources answer false unchanged — repo review item 9).
	incremental, perr := sourceIncrementalEnabled(ctx, tx, sourceID, t)
	if perr != nil {
		return nil, perr
	}
	if incremental {
		return nil, ErrIncrementalRequiresControlled
	}
	// Parent chain before quota, post-lock: the def must still be bound to
	// the locked source and visible in the tenant.
	bound, err := lockDefBoundToSource(ctx, tx, id, sourceID, t)
	if err != nil {
		return nil, err
	}
	if !bound {
		return nil, nil
	}
	if err := admissionGate(ctx, tx, sourceID); err != nil {
		return nil, err
	}
	runID := uuid.New()
	_, err = tx.Exec(ctx,
		`INSERT INTO sync_runs (id, sync_def_id, status, org_id)
		 SELECT $1, d.id, 'pending', d.org_id FROM batch_sync_defs d
		 WHERE d.id = $2 AND d.source_id = $3`,
		runID, id, sourceID)
	if err != nil {
		return nil, err
	}
	row := tx.QueryRow(ctx,
		`UPDATE sync_runs r
		    SET status = 'running', started_at = clock_timestamp()
		  WHERE r.id = $1 AND r.status = 'pending'
		  RETURNING `+runPublicColumns,
		runID)
	v, err := scanSyncRun(row)
	if err != nil {
		return nil, err
	}
	if err := advanceSyncLastAdmitted(ctx, tx, sourceID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return v, nil
}

// CompleteSyncRun is the legacy no-token completion path. Its predicates
// confine it to lease-less legacy running rows: it can neither bypass a
// leased run (lease_token IS NOT NULL is filtered out) nor rewrite a
// terminal state (status must still be 'running'), nor close a P2.1 pending
// durable task without claiming it, nor — P2.5 (source review item 4) —
// finish a captured incremental run through the no-token path (cp_mode IS
// NULL guard).
func (r *Repo) CompleteSyncRun(ctx context.Context, runID uuid.UUID, t Tenant, status string, bytesWritten int64, filesWritten int64, rowsWritten int64, errMsg *string, ingestJobID *string, datasetVersionID *uuid.UUID, contentHash *string) (*models.SyncRun, error) {
	predicate, arg := syncRunTenantPredicate(t, 10)
	row := r.Pool.QueryRow(ctx,
		`UPDATE sync_runs r
		    SET status = $2,
		        finished_at = NOW(),
		        bytes_written = $3,
		        files_written = $4,
		        rows_written = $5,
		        error = $6,
		        ingest_job_id = $7,
		        dataset_version_id = $8,
		        content_hash = $9
		  WHERE r.id = $1 AND r.status = 'running' AND r.lease_token IS NULL
		    AND r.cp_mode IS NULL AND `+predicate+`
		  RETURNING `+runPublicColumns,
		runID, status, bytesWritten, filesWritten, rowsWritten, errMsg, ingestJobID, datasetVersionID, contentHash, arg,
	)
	v, err := scanSyncRun(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

// syncRunTenantPredicate scopes sync_runs (alias `r`) by its persisted org
// ownership; legacy NULL-org runs fall back to the owning sync def's source
// owner domain.
func syncRunTenantPredicate(t Tenant, argPos int) (string, any) {
	if t.OrgID != nil {
		return fmt.Sprintf("r.org_id = $%d", argPos), *t.OrgID
	}
	return fmt.Sprintf("(r.org_id IS NULL AND EXISTS (SELECT 1 FROM batch_sync_defs d JOIN connections pc ON pc.id = d.source_id WHERE d.id = r.sync_def_id AND pc.owner_id = $%d))", argPos), t.OwnerID
}

func (r *Repo) ListSyncRuns(ctx context.Context, syncID uuid.UUID, t Tenant) ([]models.SyncRun, error) {
	predicate, arg := syncRunTenantPredicate(t, 2)
	rows, err := r.Pool.Query(ctx,
		`SELECT `+runPublicColumns+`
		 FROM sync_runs r
		 WHERE r.sync_def_id = $1 AND `+predicate+`
		 ORDER BY r.started_at DESC LIMIT 500`, syncID, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]models.SyncRun, 0)
	for rows.Next() {
		v, err := scanSyncRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

func scanSyncJob(r rowLikeT) (*models.SyncJob, error) {
	v := &models.SyncJob{}
	if err := r.Scan(&v.ID, &v.SourceID, &v.OutputDatasetID, &v.FileGlob, &v.ScheduleCron,
		&v.ScheduleTimeZone, &v.ScheduleEnabled, &v.ScheduleRevision, &v.ScheduleNextFireAt,
		&v.ScheduleLastFireAt, &v.ScheduleLastTickAt, &v.ScheduleLastTickOutcome, &v.OrgID, &v.CreatedAt); err != nil {
		return nil, err
	}
	return v, nil
}

func scanSyncRun(r rowLikeT) (*models.SyncRun, error) {
	v := &models.SyncRun{}
	if err := r.Scan(&v.ID, &v.SyncDefID, &v.Status, &v.OrgID, &v.StartedAt, &v.FinishedAt, &v.BytesWritten,
		&v.FilesWritten, &v.RowsWritten, &v.Error, &v.IngestJobID, &v.DatasetVersionID, &v.ContentHash,
		&v.RequestedBy, &v.OwnerID, &v.Attempt, &v.Generation,
		&v.ControlMode, &v.CancelRequestedAt, &v.CancelledBy, &v.PublishEnteredAt,
		// Root review-w2-round1 item 4: the frozen protocol marker is part of
		// the public projection scan (json:"-" keeps the wire shape) so every
		// enqueue/admission return carries it — an empty marker must never
		// re-enable legacy cleanup against a p26-v1 task.
		&v.RecoveryProtocol); err != nil {
		return nil, err
	}
	return v, nil
}

var validVirtualProviders = map[string]bool{
	"AMAZON_S3": true, "AZURE_ABFS": true, "BIGQUERY": true, "DATABRICKS": true,
	"FOUNDRY_ICEBERG": true, "GCS": true, "SNOWFLAKE": true,
}

var validVirtualTableTypes = map[string]bool{
	"TABLE": true, "VIEW": true, "MATERIALIZED_VIEW": true, "EXTERNAL_DELTA": true,
	"MANAGED_DELTA": true, "MANAGED_ICEBERG": true, "PARQUET_FILES": true,
	"AVRO_FILES": true, "CSV_FILES": true, "OTHER": true,
}

var ErrConflict = errors.New("conflict")

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "SQLSTATE 23505")
}

func (r *Repo) EnableVirtualTableSource(ctx context.Context, sourceRID string, body *models.EnableVirtualTableSourceRequest) (*models.VirtualTableSourceLink, error) {
	provider := strings.TrimSpace(body.Provider)
	if !validVirtualProviders[provider] {
		return nil, fmt.Errorf("invalid provider: %s", body.Provider)
	}
	cfg := body.IcebergCatalogConfig
	if len(cfg) == 0 {
		cfg = []byte(`null`)
	}
	row := r.Pool.QueryRow(ctx,
		`INSERT INTO virtual_table_sources_link (source_rid, provider, virtual_tables_enabled, iceberg_catalog_kind, iceberg_catalog_config)
		 VALUES ($1, $2, TRUE, $3, $4)
		 ON CONFLICT (source_rid) DO UPDATE SET virtual_tables_enabled = TRUE, provider = EXCLUDED.provider,
		     iceberg_catalog_kind = EXCLUDED.iceberg_catalog_kind, iceberg_catalog_config = EXCLUDED.iceberg_catalog_config,
		     updated_at = NOW()
		 RETURNING source_rid, provider, virtual_tables_enabled, code_imports_enabled, export_controls,
		           auto_register_project_rid, auto_register_enabled, auto_register_interval_seconds,
		           auto_register_tag_filters, iceberg_catalog_kind, iceberg_catalog_config, created_at, updated_at`,
		sourceRID, provider, body.IcebergCatalogKind, cfg,
	)
	return scanVirtualTableSourceLink(row)
}

func (r *Repo) CreateVirtualTable(ctx context.Context, sourceRID string, actorID string, body *models.CreateVirtualTableRequest) (*models.VirtualTable, error) {
	if !validVirtualTableTypes[body.TableType] {
		return nil, fmt.Errorf("invalid table_type: %s", body.TableType)
	}
	name := ""
	if body.Name != nil {
		name = strings.TrimSpace(*body.Name)
	}
	if name == "" {
		name = body.Locator.DefaultDisplayName()
	}
	if name == "" || strings.TrimSpace(body.ProjectRID) == "" {
		return nil, fmt.Errorf("project_rid and name/locator are required")
	}
	locator, err := body.Locator.CanonicalJSON()
	if err != nil {
		return nil, err
	}
	actor := actorID
	row := r.Pool.QueryRow(ctx,
		`INSERT INTO virtual_tables (id, source_rid, project_rid, name, parent_folder_rid, locator, table_type,
		     schema_inferred, capabilities, markings, properties, created_by)
		 SELECT $1, l.source_rid, $3, $4, $5, $6::jsonb, $7, '[]'::jsonb, '{}'::jsonb, $8, '{}'::jsonb, $9
		 FROM virtual_table_sources_link l WHERE l.source_rid = $2 AND l.virtual_tables_enabled
		 RETURNING id, rid, source_rid, project_rid, name, parent_folder_rid, locator, table_type,
		           schema_inferred, capabilities, update_detection_enabled, update_detection_interval_seconds,
		           last_observed_version, last_polled_at,
		           COALESCE(update_detection_consecutive_failures, 0), update_detection_next_poll_at,
		           markings, properties, created_by, created_at, updated_at`,
		uuid.New(), sourceRID, strings.TrimSpace(body.ProjectRID), name, body.ParentFolderRID,
		locator, body.TableType, body.Markings, actor,
	)
	v, err := scanVirtualTable(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if isUniqueViolation(err) {
		return nil, ErrConflict
	}
	return v, err
}

func (r *Repo) ListVirtualTables(ctx context.Context, ownerID string, project, source string, limit int) ([]models.VirtualTable, error) {
	if limit < 1 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	query := virtualTableSelect + ` WHERE created_by = $1`
	args := []any{ownerID}
	if project != "" {
		args = append(args, project)
		query += fmt.Sprintf(` AND project_rid = $%d`, len(args))
	}
	if source != "" {
		args = append(args, source)
		query += fmt.Sprintf(` AND source_rid = $%d`, len(args))
	}
	args = append(args, limit)
	query += fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d`, len(args))
	rows, err := r.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]models.VirtualTable, 0)
	for rows.Next() {
		v, err := scanVirtualTable(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

const virtualTableSelect = `SELECT id, rid, source_rid, project_rid, name, parent_folder_rid, locator, table_type,
	schema_inferred, capabilities, update_detection_enabled, update_detection_interval_seconds,
	last_observed_version, last_polled_at, COALESCE(update_detection_consecutive_failures, 0), update_detection_next_poll_at,
	markings, properties, created_by, created_at, updated_at FROM virtual_tables`

func (r *Repo) GetVirtualTable(ctx context.Context, rid string, ownerID string) (*models.VirtualTable, error) {
	row := r.Pool.QueryRow(ctx, virtualTableSelect+` WHERE rid = $1 AND created_by = $2`, rid, ownerID)
	v, err := scanVirtualTable(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

func scanVirtualTableSourceLink(r rowLikeT) (*models.VirtualTableSourceLink, error) {
	v := &models.VirtualTableSourceLink{}
	if err := r.Scan(&v.SourceRID, &v.Provider, &v.VirtualTablesEnabled, &v.CodeImportsEnabled,
		&v.ExportControls, &v.AutoRegisterProjectRID, &v.AutoRegisterEnabled, &v.AutoRegisterIntervalSeconds,
		&v.AutoRegisterTagFilters, &v.IcebergCatalogKind, &v.IcebergCatalogConfig, &v.CreatedAt, &v.UpdatedAt); err != nil {
		return nil, err
	}
	return v, nil
}

func scanVirtualTable(r rowLikeT) (*models.VirtualTable, error) {
	v := &models.VirtualTable{}
	if err := r.Scan(&v.ID, &v.RID, &v.SourceRID, &v.ProjectRID, &v.Name, &v.ParentFolderRID,
		&v.Locator, &v.TableType, &v.SchemaInferred, &v.Capabilities, &v.UpdateDetectionEnabled,
		&v.UpdateDetectionIntervalSeconds, &v.LastObservedVersion, &v.LastPolledAt,
		&v.UpdateDetectionConsecutiveFailures, &v.UpdateDetectionNextPollAt, &v.Markings,
		&v.Properties, &v.CreatedBy, &v.CreatedAt, &v.UpdatedAt); err != nil {
		return nil, err
	}
	if v.Markings == nil {
		v.Markings = []string{}
	}
	return v, nil
}

const mediaSetSyncSelect = `SELECT m.id, m.source_id, m.sync_type, m.target_media_set_rid,
	m.subfolder, m.filters, m.schedule_cron, m.created_at
	FROM media_set_syncs m JOIN connections c ON c.id = m.source_id`

func (r *Repo) ListMediaSetSyncs(ctx context.Context, sourceID uuid.UUID, ownerID uuid.UUID) ([]models.MediaSetSync, error) {
	rows, err := r.Pool.Query(ctx, mediaSetSyncSelect+` WHERE m.source_id = $1 AND c.owner_id = $2 ORDER BY m.created_at DESC`, sourceID, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]models.MediaSetSync, 0)
	for rows.Next() {
		v, err := scanMediaSetSync(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

func (r *Repo) GetMediaSetSync(ctx context.Context, id uuid.UUID, ownerID uuid.UUID) (*models.MediaSetSync, error) {
	row := r.Pool.QueryRow(ctx, mediaSetSyncSelect+` WHERE m.id = $1 AND c.owner_id = $2`, id, ownerID)
	v, err := scanMediaSetSync(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

func (r *Repo) CreateMediaSetSync(ctx context.Context, sourceID uuid.UUID, body *models.CreateMediaSetSyncRequest, ownerID uuid.UUID) (*models.MediaSetSync, error) {
	filters, err := json.Marshal(body.Filters)
	if err != nil {
		return nil, err
	}
	row := r.Pool.QueryRow(ctx,
		`INSERT INTO media_set_syncs (id, source_id, sync_type, target_media_set_rid, subfolder, filters, schedule_cron)
		 SELECT $1, c.id, $3, $4, $5, $6, $7 FROM connections c WHERE c.id = $2 AND c.owner_id = $8
		 RETURNING id, source_id, sync_type, target_media_set_rid, subfolder, filters, schedule_cron, created_at`,
		uuid.New(), sourceID, string(body.Kind), strings.TrimSpace(body.TargetMediaSetRID), strings.Trim(body.Subfolder, "/"), filters, body.ScheduleCron, ownerID,
	)
	v, err := scanMediaSetSync(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

func (r *Repo) UpdateMediaSetSync(ctx context.Context, id uuid.UUID, body *models.UpdateMediaSetSyncRequest, ownerID uuid.UUID) (*models.MediaSetSync, error) {
	current, err := r.GetMediaSetSync(ctx, id, ownerID)
	if err != nil || current == nil {
		return current, err
	}
	kind := current.Kind
	if body.Kind != nil {
		kind = *body.Kind
	}
	target := current.TargetMediaSetRID
	if body.TargetMediaSetRID != nil {
		target = strings.TrimSpace(*body.TargetMediaSetRID)
	}
	subfolder := current.Subfolder
	if body.Subfolder != nil {
		subfolder = strings.Trim(*body.Subfolder, "/")
	}
	filters := current.Filters
	if body.Filters != nil {
		filters = *body.Filters
	}
	schedule := current.ScheduleCron
	if body.ScheduleCron != nil {
		schedule = body.ScheduleCron
	}
	if errs := models.ValidateMediaSetSyncConfig(kind, target, filters, schedule); len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "; "))
	}
	filtersJSON, err := json.Marshal(filters)
	if err != nil {
		return nil, err
	}
	row := r.Pool.QueryRow(ctx,
		`UPDATE media_set_syncs m SET sync_type = $2, target_media_set_rid = $3, subfolder = $4, filters = $5, schedule_cron = $6
		 FROM connections c WHERE m.source_id = c.id AND m.id = $1 AND c.owner_id = $7
		 RETURNING m.id, m.source_id, m.sync_type, m.target_media_set_rid, m.subfolder, m.filters, m.schedule_cron, m.created_at`,
		id, string(kind), target, subfolder, filtersJSON, schedule, ownerID,
	)
	return scanMediaSetSync(row)
}

func scanMediaSetSync(r rowLikeT) (*models.MediaSetSync, error) {
	v := &models.MediaSetSync{}
	var kind string
	var filters []byte
	if err := r.Scan(&v.ID, &v.SourceID, &kind, &v.TargetMediaSetRID, &v.Subfolder, &filters, &v.ScheduleCron, &v.CreatedAt); err != nil {
		return nil, err
	}
	v.Kind = models.MediaSetSyncKind(kind)
	if len(filters) == 0 {
		filters = []byte(`{}`)
	}
	if err := json.Unmarshal(filters, &v.Filters); err != nil {
		return nil, err
	}
	return v, nil
}

const registrationSelect = `SELECT id, connection_id, selector, display_name, source_kind,
	registration_mode, auto_sync, update_detection, target_dataset_id, last_source_signature,
	last_dataset_version, metadata, created_at, updated_at FROM connection_registrations`

func (r *Repo) ListRegistrations(ctx context.Context, sourceID uuid.UUID) ([]models.ConnectionRegistration, error) {
	rows, err := r.Pool.Query(ctx, registrationSelect+` WHERE connection_id = $1 ORDER BY created_at DESC`, sourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.ConnectionRegistration{}
	for rows.Next() {
		v, err := scanRegistration(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

func (r *Repo) UpsertRegistration(ctx context.Context, sourceID uuid.UUID, source models.DiscoveredSource, mode string, autoSync bool, updateDetection bool, targetDatasetID *uuid.UUID, metadata json.RawMessage) (*models.ConnectionRegistration, error) {
	if len(metadata) == 0 || string(metadata) == "null" {
		metadata = []byte(`{}`)
	}
	row := r.Pool.QueryRow(ctx, `WITH upserted AS (
		INSERT INTO connection_registrations (id, connection_id, selector, display_name, source_kind, registration_mode, auto_sync, update_detection, target_dataset_id, last_source_signature, metadata)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (connection_id, selector) DO UPDATE SET display_name = EXCLUDED.display_name, source_kind = EXCLUDED.source_kind,
			registration_mode = EXCLUDED.registration_mode, auto_sync = EXCLUDED.auto_sync, update_detection = EXCLUDED.update_detection,
			target_dataset_id = EXCLUDED.target_dataset_id, last_source_signature = EXCLUDED.last_source_signature,
			metadata = EXCLUDED.metadata, updated_at = NOW()
		RETURNING id, connection_id, selector, display_name, source_kind, registration_mode, auto_sync, update_detection, target_dataset_id, last_source_signature, last_dataset_version, metadata, created_at, updated_at)
		SELECT id, connection_id, selector, display_name, source_kind, registration_mode, auto_sync, update_detection, target_dataset_id, last_source_signature, last_dataset_version, metadata, created_at, updated_at FROM upserted`, uuid.New(), sourceID, source.Selector, source.DisplayName, source.SourceKind, mode, autoSync, updateDetection, targetDatasetID, source.SourceSignature, metadata)
	v, err := scanRegistration(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

func (r *Repo) GetRegistration(ctx context.Context, sourceID uuid.UUID, registrationID uuid.UUID) (*models.ConnectionRegistration, error) {
	row := r.Pool.QueryRow(ctx, registrationSelect+` WHERE connection_id = $1 AND id = $2`, sourceID, registrationID)
	v, err := scanRegistration(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

func (r *Repo) DeleteRegistration(ctx context.Context, sourceID uuid.UUID, registrationID uuid.UUID) (bool, error) {
	ct, err := r.Pool.Exec(ctx, `DELETE FROM connection_registrations WHERE connection_id = $1 AND id = $2`, sourceID, registrationID)
	return ct.RowsAffected() > 0, err
}

// UpdateConnectionConfig mutates a source's raw config (auto-registration
// settings surface). It participates in the P2.1 active-period policy: the
// source row is locked FOR UPDATE and the pending/running check is re-run
// post-lock, so a config change is refused with ErrActiveRuns while a durable
// task is active — same protection as the tenant-scoped UpdateConnection.
func (r *Repo) UpdateConnectionConfig(ctx context.Context, id uuid.UUID, config json.RawMessage) (*models.Connection, error) {
	// P2.3: this surface rewrites the WHOLE config, so its sync.admission
	// subtree must pass the same validation as the typed handler paths —
	// before any lock is taken, an illegal policy is a plain error and
	// changes nothing.
	if _, err := models.ParseSyncAdmissionPolicy(config); err != nil {
		return nil, err
	}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var locked uuid.UUID
	err = tx.QueryRow(ctx, `SELECT c.id FROM connections c WHERE c.id = $1 FOR UPDATE`, id).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	active, err := txSourceHasActiveRuns(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if active {
		return nil, ErrActiveRuns
	}
	// P2.5 fence: this surface rewrites the WHOLE config — always a
	// protected input mutation. Sorted Def locks (tenant-less internal
	// surface, same row set) precede the protected row.
	defIDs, err := lockSourceDefsForProtectionInternal(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if err := assertCheckpointOwnersFreeTx(ctx, tx, defIDs); err != nil {
		return nil, err
	}
	row := tx.QueryRow(ctx, `UPDATE connections SET config = $2, updated_at = NOW() WHERE id = $1 RETURNING id, name, connector_type, config, status, owner_id, org_id, last_sync_at, created_at, updated_at`, id, config)
	v, err := scanConnection(row)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return v, nil
}

func (r *Repo) ListIcebergNamespaces(ctx context.Context) ([]models.Connection, error) {
	rows, err := r.Pool.Query(ctx, connectionSelect+` WHERE EXISTS (SELECT 1 FROM connection_registrations reg WHERE reg.connection_id = c.id AND COALESCE((reg.metadata->>'supports_zero_copy')::bool, false) = true) ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.Connection{}
	for rows.Next() {
		v, err := scanConnection(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

func (r *Repo) GetIcebergConnection(ctx context.Context, namespace string) (*models.Connection, error) {
	row := r.Pool.QueryRow(ctx, connectionSelect+` WHERE name = $1 OR regexp_replace(name, '[^A-Za-z0-9_-]', '_', 'g') = $1 LIMIT 1`, namespace)
	v, err := scanConnection(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

func (r *Repo) ListIcebergTables(ctx context.Context, connectionID uuid.UUID) ([]models.ConnectionRegistration, error) {
	rows, err := r.Pool.Query(ctx, registrationSelect+` WHERE connection_id = $1 AND COALESCE((metadata->>'supports_zero_copy')::bool, false) = true ORDER BY selector`, connectionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.ConnectionRegistration{}
	for rows.Next() {
		v, err := scanRegistration(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

func scanRegistration(r rowLikeT) (*models.ConnectionRegistration, error) {
	v := &models.ConnectionRegistration{}
	if err := r.Scan(&v.ID, &v.ConnectionID, &v.Selector, &v.DisplayName, &v.SourceKind, &v.RegistrationMode, &v.AutoSync, &v.UpdateDetection, &v.TargetDatasetID, &v.LastSourceSignature, &v.LastDatasetVersion, &v.Metadata, &v.CreatedAt, &v.UpdatedAt); err != nil {
		return nil, err
	}
	return v, nil
}

func (r *Repo) GetRegistrationSignature(ctx context.Context, sourceID uuid.UUID, selector string) (*string, error) {
	row := r.Pool.QueryRow(ctx, `SELECT last_source_signature FROM connection_registrations WHERE connection_id = $1 AND selector = $2`, sourceID, selector)
	var sig *string
	if err := row.Scan(&sig); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return sig, nil
}

func (r *Repo) RecordRegistrationSignature(ctx context.Context, sourceID uuid.UUID, selector string, signature *string) error {
	if signature == nil {
		return nil
	}
	_, err := r.Pool.Exec(ctx, `UPDATE connection_registrations SET last_source_signature = $3, updated_at = NOW() WHERE connection_id = $1 AND selector = $2`, sourceID, selector, *signature)
	return err
}

// RunDueSyncJobs (the pre-P2.4 compatibility no-op) was removed: the P2.4
// scheduler lives in internal/repo/schedule.go (ListDueScheduleCandidates +
// ClaimDueScheduleSlot) and the loop in handlers.ScheduleRunner.

const connectorAgentSelect = `SELECT id, name, agent_url, owner_id, status,
	capabilities, metadata, last_heartbeat_at, created_at, updated_at FROM connector_agents`

func (r *Repo) ListConnectorAgents(ctx context.Context, ownerID uuid.UUID) ([]models.ConnectorAgent, error) {
	rows, err := r.Pool.Query(ctx, connectorAgentSelect+` WHERE owner_id = $1 ORDER BY created_at DESC`, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.ConnectorAgent{}
	for rows.Next() {
		agent, err := scanConnectorAgent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *agent)
	}
	return out, rows.Err()
}

func (r *Repo) RegisterConnectorAgent(ctx context.Context, body *models.RegisterAgentRequest, ownerID uuid.UUID) (*models.ConnectorAgent, error) {
	id := uuid.New()
	row := r.Pool.QueryRow(ctx,
		`INSERT INTO connector_agents
			(id, name, agent_url, owner_id, status, capabilities, metadata, last_heartbeat_at)
		 VALUES ($1, $2, $3, $4, 'online', $5, $6, NOW())
		 ON CONFLICT (agent_url) DO UPDATE SET
			name = EXCLUDED.name,
			owner_id = EXCLUDED.owner_id,
			status = 'online',
			capabilities = EXCLUDED.capabilities,
			metadata = EXCLUDED.metadata,
			last_heartbeat_at = NOW(),
			updated_at = NOW()
		 WHERE connector_agents.owner_id = EXCLUDED.owner_id
		 RETURNING id, name, agent_url, owner_id, status,
		           capabilities, metadata, last_heartbeat_at, created_at, updated_at`,
		id, strings.TrimSpace(body.Name), strings.TrimSpace(body.AgentURL), ownerID, body.Capabilities, body.Metadata,
	)
	agent, err := scanConnectorAgent(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return agent, err
}

func (r *Repo) HeartbeatConnectorAgent(ctx context.Context, id uuid.UUID, body *models.AgentHeartbeatRequest, ownerID uuid.UUID) (*models.ConnectorAgent, error) {
	row := r.Pool.QueryRow(ctx,
		`UPDATE connector_agents
		 SET status = 'online',
		     capabilities = $3,
		     metadata = $4,
		     last_heartbeat_at = NOW(),
		     updated_at = NOW()
		 WHERE id = $1 AND owner_id = $2
		 RETURNING id, name, agent_url, owner_id, status,
		           capabilities, metadata, last_heartbeat_at, created_at, updated_at`,
		id, ownerID, body.Capabilities, body.Metadata,
	)
	agent, err := scanConnectorAgent(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return agent, err
}

func (r *Repo) DeleteConnectorAgent(ctx context.Context, id uuid.UUID, ownerID uuid.UUID) (bool, error) {
	cmd, err := r.Pool.Exec(ctx, `DELETE FROM connector_agents WHERE id = $1 AND owner_id = $2`, id, ownerID)
	if err != nil {
		return false, err
	}
	return cmd.RowsAffected() > 0, nil
}

func (r *Repo) GetConnectorAgent(ctx context.Context, id uuid.UUID) (*models.ConnectorAgent, error) {
	row := r.Pool.QueryRow(ctx, connectorAgentSelect+` WHERE id = $1`, id)
	agent, err := scanConnectorAgent(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return agent, err
}

func scanConnectorAgent(r rowLikeT) (*models.ConnectorAgent, error) {
	agent := &models.ConnectorAgent{}
	if err := r.Scan(&agent.ID, &agent.Name, &agent.AgentURL, &agent.OwnerID, &agent.Status, &agent.Capabilities, &agent.Metadata, &agent.LastHeartbeatAt, &agent.CreatedAt, &agent.UpdatedAt); err != nil {
		return nil, err
	}
	return agent, nil
}

const webhookHistorySelect = `SELECT id, source_id, user_id, status, http_status,
	input_policy, inputs, output_parameters, error, call_count, started_at,
	finished_at, duration_ms, retention_expires_at, created_at
	FROM webhook_invocation_history`

func (r *Repo) AppendWebhookHistory(ctx context.Context, body *models.CreateWebhookHistoryEntry) (*models.WebhookHistoryEntry, error) {
	if body == nil {
		return nil, errors.New("webhook history entry is nil")
	}
	if body.StartedAt.IsZero() {
		body.StartedAt = time.Now().UTC()
	}
	if body.FinishedAt.IsZero() {
		body.FinishedAt = time.Now().UTC()
	}
	if body.RetentionExpiresAt.IsZero() {
		body.RetentionExpiresAt = body.FinishedAt.Add(30 * 24 * time.Hour)
	}
	durationMS := body.FinishedAt.Sub(body.StartedAt).Milliseconds()
	if durationMS < 0 {
		durationMS = 0
	}
	policy, err := json.Marshal(body.InputPolicy)
	if err != nil {
		return nil, err
	}
	var httpStatus any
	if body.HTTPStatus != nil {
		httpStatus = int(*body.HTTPStatus)
	}
	id := uuid.New()
	row := r.Pool.QueryRow(ctx,
		`INSERT INTO webhook_invocation_history
			(id, source_id, user_id, status, http_status, input_policy, inputs,
			 output_parameters, error, call_count, started_at, finished_at,
			 duration_ms, retention_expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		 RETURNING id, source_id, user_id, status, http_status, input_policy,
		           inputs, output_parameters, error, call_count, started_at,
		           finished_at, duration_ms, retention_expires_at, created_at`,
		id, body.SourceID, body.UserID, body.Status, httpStatus, policy,
		nullableJSON(body.Inputs), nullableJSON(body.OutputParameters), body.Error,
		body.CallCount, body.StartedAt, body.FinishedAt, durationMS, body.RetentionExpiresAt,
	)
	return scanWebhookHistory(row)
}

func (r *Repo) ListWebhookHistory(ctx context.Context, sourceID uuid.UUID, limit int) ([]models.WebhookHistoryEntry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	_, _ = r.Pool.Exec(ctx, `DELETE FROM webhook_invocation_history WHERE retention_expires_at < NOW()`)
	rows, err := r.Pool.Query(ctx, webhookHistorySelect+`
		WHERE source_id = $1 AND retention_expires_at >= NOW()
		ORDER BY created_at DESC
		LIMIT $2`, sourceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.WebhookHistoryEntry{}
	for rows.Next() {
		row, err := scanWebhookHistory(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *row)
	}
	return out, rows.Err()
}

func nullableJSON(raw json.RawMessage) any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	return raw
}

func scanWebhookHistory(r rowLikeT) (*models.WebhookHistoryEntry, error) {
	entry := &models.WebhookHistoryEntry{}
	var policy json.RawMessage
	var httpStatus pgtype.Int4
	if err := r.Scan(
		&entry.ID,
		&entry.SourceID,
		&entry.UserID,
		&entry.Status,
		&httpStatus,
		&policy,
		&entry.Inputs,
		&entry.OutputParameters,
		&entry.Error,
		&entry.CallCount,
		&entry.StartedAt,
		&entry.FinishedAt,
		&entry.DurationMS,
		&entry.RetentionExpiresAt,
		&entry.CreatedAt,
	); err != nil {
		return nil, err
	}
	if len(policy) > 0 {
		_ = json.Unmarshal(policy, &entry.InputPolicy)
	}
	if httpStatus.Valid {
		converted := uint16(httpStatus.Int32)
		entry.HTTPStatus = &converted
	}
	if len(entry.Inputs) == 0 {
		entry.Inputs = nil
	}
	if len(entry.OutputParameters) == 0 {
		entry.OutputParameters = nil
	}
	return entry, nil
}

const inboundListenerEventSelect = `SELECT id, source_id, listener_id, event_id, status,
	signature_verified, payload, headers, destination, created_at
	FROM inbound_listener_events`

func (r *Repo) AppendInboundListenerEvent(ctx context.Context, body *models.CreateInboundListenerEvent) (*models.InboundListenerEvent, error) {
	if body == nil {
		return nil, errors.New("inbound listener event is nil")
	}
	if strings.TrimSpace(body.ListenerID) == "" {
		return nil, errors.New("listener_id is required")
	}
	status := strings.TrimSpace(body.Status)
	if status == "" {
		status = "accepted"
	}
	destination, err := json.Marshal(body.Destination)
	if err != nil {
		return nil, err
	}
	id := uuid.New()
	row := r.Pool.QueryRow(ctx,
		`INSERT INTO inbound_listener_events
			(id, source_id, listener_id, event_id, status, signature_verified,
			 payload, headers, destination)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 RETURNING id, source_id, listener_id, event_id, status, signature_verified,
		           payload, headers, destination, created_at`,
		id, body.SourceID, body.ListenerID, nullableText(body.EventID), status,
		body.SignatureVerified, nullableJSON(body.Payload), nullableJSON(body.Headers), destination,
	)
	return scanInboundListenerEvent(row)
}

func (r *Repo) ListInboundListenerEvents(ctx context.Context, sourceID uuid.UUID, limit int) ([]models.InboundListenerEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := r.Pool.Query(ctx, inboundListenerEventSelect+`
		WHERE source_id = $1
		ORDER BY created_at DESC
		LIMIT $2`, sourceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.InboundListenerEvent{}
	for rows.Next() {
		row, err := scanInboundListenerEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *row)
	}
	return out, rows.Err()
}

func nullableText(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func scanInboundListenerEvent(r rowLikeT) (*models.InboundListenerEvent, error) {
	event := &models.InboundListenerEvent{}
	var eventID pgtype.Text
	var destination json.RawMessage
	if err := r.Scan(
		&event.ID,
		&event.SourceID,
		&event.ListenerID,
		&eventID,
		&event.Status,
		&event.SignatureVerified,
		&event.Payload,
		&event.Headers,
		&destination,
		&event.CreatedAt,
	); err != nil {
		return nil, err
	}
	if eventID.Valid {
		event.EventID = eventID.String
	}
	if len(destination) > 0 {
		_ = json.Unmarshal(destination, &event.Destination)
	}
	if len(event.Payload) == 0 {
		event.Payload = nil
	}
	if len(event.Headers) == 0 {
		event.Headers = nil
	}
	return event, nil
}
