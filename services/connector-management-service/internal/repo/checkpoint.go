// P2.5 per-def incremental completion checkpoints: durable single-flight
// ownership, checkpoint-aware claim/gate/finalize, and the mutation fences.
//
// Global lock order (implementation-contract §5): connections(Source) ->
// batch_sync_defs(Def) -> sync_def_checkpoints(Checkpoint) -> sync_runs(Run).
// Checkpoint is always a leaf. Every checkpoint-aware path takes the Def
// strong lock (FOR UPDATE) BEFORE the checkpoint row, and the run row last;
// protected writers (UpdateConnection/UpdateConnectionConfig/SetCredential/
// deletes) hold the Source lock and the sorted Def locks BEFORE touching the
// protected input/credential rows, so the Def->Checkpoint->Run chain can
// reread the authoritative binding without a reverse Source lock.
//
// Conservative unknown boundary (implementation-release correction 1): the
// publish-gate ENTRY is P25's boundary. Every owner RELEASE asserts
// publish_entered_at IS NULL inside the same transaction that writes the
// terminal state; after gate entry a failure is written as honest 'failed'
// with the owner RETAINED (P26 resolves), never aborted, never released.
//
// The run's cp_* columns are written exactly once (at admission) and are
// therefore immutable: the executor treats run.cp_mode as the single
// authority for claim/execute/cancel/finalize and never consults the current
// Source config.
package repo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

const (
	// checkpointOpBudget bounds every checkpoint-aware transaction (same
	// family as scheduleOpBudget / syncLeaseCallTimeout).
	checkpointOpBudget = 5 * time.Second
	// CPModeCursor is the only P2.5 mode.
	CPModeCursor = "cursor"
)

// ErrLeaseLost reports a fenced/expired lease CAS miss: nothing was written
// and the caller must not fabricate a terminal state.
var ErrLeaseLost = errors.New("sync run lease was lost or fenced")

// sha256Hex is the safe digest used for opaque-value exposure (lengths and
// hashes only — never the raw token).
func sha256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

// rollbackCheckpointTx bounds the deferred cleanup rollback of a checkpoint
// transaction independently of the caller's (possibly already dead) context —
// the same discipline as rollbackAdmissionTx (repo review item 7). The
// statement budgets themselves live in the waiting contexts; the rollback
// must never escape indefinitely.
func rollbackCheckpointTx(tx pgx.Tx) {
	rollbackCtx, cancel := context.WithTimeout(context.Background(), checkpointOpBudget)
	defer cancel()
	_ = tx.Rollback(rollbackCtx)
}

// IncrementalRejectionKind enumerates typed, zero-side-effect admission
// refusals for an incremental def.
type IncrementalRejectionKind string

const (
	// IncrementalBusy: the def's checkpoint row is owned by another run
	// (pending counts; failed-unknown retains).
	IncrementalBusy IncrementalRejectionKind = "busy"
	// IncrementalBindingStale: the stored binding fingerprint does not match
	// the authoritative current binding (recovery: newDef-only reset).
	IncrementalBindingStale IncrementalRejectionKind = "binding_stale"
)

// IncrementalRejection is a typed admission refusal. ActiveRunID is only
// surfaced to callers already parent-visible (handler rule).
type IncrementalRejection struct {
	Kind        IncrementalRejectionKind
	ActiveRunID uuid.UUID
	ActiveSince time.Time
}

func (e *IncrementalRejection) Error() string {
	switch e.Kind {
	case IncrementalBusy:
		return "sync def already has an active incremental run"
	case IncrementalBindingStale:
		return "sync def incremental binding is stale for the current source configuration"
	default:
		return "incremental admission rejected"
	}
}

var (
	// ErrIncrementalOwnerUnresolved: a protected mutation was refused because
	// a def's checkpoint owner is unresolved (pending/running/failed-unknown).
	ErrIncrementalOwnerUnresolved = errors.New("sync def has an unresolved incremental checkpoint owner")
	// ErrIncrementalCheckpointCAS: an advance/release predicate missed; the
	// transaction wrote NOTHING (all-or-nothing rule).
	ErrIncrementalCheckpointCAS = errors.New("incremental checkpoint CAS mismatch; nothing written")
	// ErrIncrementalRequiresAdvance: the ordinary complete path may not write
	// a P25 run's success — only CompleteIncrementalSyncRun may.
	ErrIncrementalRequiresAdvance = errors.New("incremental run success must go through CompleteIncrementalSyncRun")
	// ErrIncrementalRequiresControlled: the legacy unleased RunSyncJob path
	// refuses opt-in incremental defs (they must go through the controlled
	// leased admission — source review item 4).
	ErrIncrementalRequiresControlled = errors.New("incremental sync definitions must run through the controlled leased path")
	// errCheckpointLockContention: 55P03 on the checkpoint row NOWAIT lock —
	// the transaction is aborted; callers roll back and answer contention.
	errCheckpointLockContention = errors.New("checkpoint row lock contention")
)

// CheckpointBase is the admission-frozen baseline captured on the run row.
type CheckpointBase struct {
	Mode            string
	BaseEpoch       int
	BaseValue       string
	HasBaseValue    bool
	Fingerprint     string
	OutputDatasetID uuid.UUID
}

// checkpointRow mirrors one sync_def_checkpoints row.
type checkpointRow struct {
	SyncDefID, SourceID, OutputDatasetID uuid.UUID
	Mode                                 string
	BindingFingerprint                   string
	CheckpointValue                      *string
	ConfirmedEpoch                       int
	ActiveRunID                          *uuid.UUID
	ActiveSince                          *time.Time
	AdvancedByRun                        *uuid.UUID
	AdvancedAt                           *time.Time
	LastBatchRows                        *int64
	LastBatchBytes                       *int64
	LastBatchHash                        *string
	LastBatchTxn                         *uuid.UUID
	CreatedAt, UpdatedAt                 time.Time
}

const checkpointColumns = `cp.sync_def_id, cp.source_id, cp.output_dataset_id, cp.mode,
	cp.binding_fingerprint, cp.checkpoint_value, cp.confirmed_epoch,
	cp.active_run_id, cp.active_since, cp.advanced_by_run, cp.advanced_at,
	cp.last_batch_rows, cp.last_batch_bytes, cp.last_batch_hash, cp.last_batch_txn,
	cp.created_at, cp.updated_at`

func scanCheckpointRow(row pgx.Row) (*checkpointRow, error) {
	v := &checkpointRow{}
	err := row.Scan(&v.SyncDefID, &v.SourceID, &v.OutputDatasetID, &v.Mode,
		&v.BindingFingerprint, &v.CheckpointValue, &v.ConfirmedEpoch,
		&v.ActiveRunID, &v.ActiveSince, &v.AdvancedByRun, &v.AdvancedAt,
		&v.LastBatchRows, &v.LastBatchBytes, &v.LastBatchHash, &v.LastBatchTxn,
		&v.CreatedAt, &v.UpdatedAt)
	return v, err
}

// cpQuerier is the read surface shared by pool and transaction paths.
type cpQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// runInternalRow is the lockless trusted pre-read used for CP-aware routing
// and for the (immutable) run-side baseline. Never serialized.
type runInternalRow struct {
	RunID                      uuid.UUID
	SyncDefID, SourceID        uuid.UUID
	OrgID                      *uuid.UUID
	CPMode                     string // "" = non-incremental / legacy
	CPBaseEpoch                int
	CPBaseValue                string
	CPBindingFingerprint       string
	CPOutputDatasetID          uuid.UUID
	RecoveryProtocol           string // "" = pre-P26; models.RunRecoveryProtocolP26V1 = P2.6 path
	RecoverySourceID           uuid.UUID
	RecoveryOutputDatasetID    uuid.UUID
	RecoveryBindingFingerprint string
}

// runInternalPreRead reads the run's immutable cp/recovery fields plus the
// def->source parent link, by id + tenant, without any lock. Returns (nil,
// nil) when the run is not visible.
func (r *Repo) runInternalPreRead(ctx context.Context, runID uuid.UUID, t Tenant) (*runInternalRow, error) {
	predicate, arg := syncRunTenantPredicate(t, 2)
	var v runInternalRow
	var cpBaseValue *string
	err := r.Pool.QueryRow(ctx,
		`SELECT r.id, r.sync_def_id, d.source_id, r.org_id,
		        COALESCE(r.cp_mode, ''), COALESCE(r.cp_base_epoch, 0),
		        r.cp_base_value, COALESCE(r.cp_binding_fingerprint, ''),
		        COALESCE(r.cp_output_dataset_id, '00000000-0000-0000-0000-000000000000'::uuid),
		        COALESCE(r.recovery_protocol, ''),
		        COALESCE(r.recovery_source_id, '00000000-0000-0000-0000-000000000000'::uuid),
		        COALESCE(r.recovery_output_dataset_id, '00000000-0000-0000-0000-000000000000'::uuid),
		        COALESCE(r.recovery_binding_fingerprint, '')
		   FROM sync_runs r JOIN batch_sync_defs d ON d.id = r.sync_def_id
		  WHERE r.id = $1 AND `+predicate,
		runID, arg).Scan(&v.RunID, &v.SyncDefID, &v.SourceID, &v.OrgID, &v.CPMode, &v.CPBaseEpoch,
		&cpBaseValue, &v.CPBindingFingerprint, &v.CPOutputDatasetID, &v.RecoveryProtocol,
		&v.RecoverySourceID, &v.RecoveryOutputDatasetID, &v.RecoveryBindingFingerprint)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if cpBaseValue != nil {
		v.CPBaseValue = *cpBaseValue
	}
	return &v, nil
}

// isIncrementalRun reports whether a pre-read row is captured P25 work.
func isIncrementalRun(pre *runInternalRow) bool {
	return pre != nil && strings.TrimSpace(pre.CPMode) != ""
}

// ---------------------------------------------------------------------------
// Binding recompute (authoritative, under locks)
// ---------------------------------------------------------------------------

// readIncrementalBinding recomputes the def's binding from the CURRENT def /
// source config / credential fingerprints through the given querier. Callers
// that hold the Def lock get race-free reads (protected writers take the same
// Def locks before mutating the protected rows).
func readIncrementalBinding(ctx context.Context, q cpQuerier, syncID, sourceID uuid.UUID, t Tenant) (models.IncrementalBinding, string, error) {
	defPredicate, defArg := syncDefTenantPredicate(t, 2)
	var (
		outputDatasetID uuid.UUID
		fileGlob        *string
	)
	err := q.QueryRow(ctx,
		`SELECT d.output_dataset_id, d.file_glob
		   FROM batch_sync_defs d
		  WHERE d.id = $1 AND d.source_id = $3 AND `+defPredicate,
		syncID, defArg, sourceID).Scan(&outputDatasetID, &fileGlob)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.IncrementalBinding{}, "", errNoIncrementalDef
	}
	if err != nil {
		return models.IncrementalBinding{}, "", err
	}
	connPredicate, connArg := connectionTenantPredicate(t, 2)
	var rawConfig []byte
	err = q.QueryRow(ctx,
		`SELECT c.config FROM connections c WHERE c.id = $1 AND `+connPredicate,
		sourceID, connArg).Scan(&rawConfig)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.IncrementalBinding{}, "", errNoIncrementalDef
	}
	if err != nil {
		return models.IncrementalBinding{}, "", err
	}
	source, err := models.ParseRESTAPISourceConfig(rawConfig)
	if err != nil {
		return models.IncrementalBinding{}, "", fmt.Errorf("incremental binding: source config unreadable: %w", err)
	}
	if source.Sync == nil || source.Sync.Incremental == nil || !source.Sync.Incremental.Enabled {
		return models.IncrementalBinding{}, "", errNoIncrementalDef
	}
	if err := models.ValidateRESTAPISourceIncremental(source); err != nil {
		return models.IncrementalBinding{}, "", err
	}
	// Credential fingerprints of the kinds referenced by the auth config.
	kinds := referencedCredentialKinds(source.Auth)
	creds := []models.CredFingerprint{}
	if len(kinds) > 0 {
		rows, err := q.Query(ctx,
			`SELECT sc.kind, sc.fingerprint FROM source_credentials sc
			  WHERE sc.source_id = $1 AND sc.kind = ANY($2)`, sourceID, kinds)
		if err != nil {
			return models.IncrementalBinding{}, "", err
		}
		defer rows.Close()
		for rows.Next() {
			var cf models.CredFingerprint
			if err := rows.Scan(&cf.Kind, &cf.Fingerprint); err != nil {
				return models.IncrementalBinding{}, "", err
			}
			creds = append(creds, cf)
		}
		if err := rows.Err(); err != nil {
			return models.IncrementalBinding{}, "", err
		}
	}
	binding := models.IncrementalBinding{
		SourceID:        sourceID,
		OutputDatasetID: outputDatasetID,
		FileGlob:        fileGlob,
		BaseURL:         source.BaseURL,
		ResourcePath:    source.Sync.ResourcePath,
		RecordsPath:     source.Sync.RecordsPath,
		PaginationType:  source.Sync.Pagination.Type,
		CursorParam:     source.Sync.Pagination.CursorParam,
		NextCursorPath:  source.Sync.Pagination.NextCursorPath,
		Incremental:     *source.Sync.Incremental,
		QueryParams:     source.Sync.QueryParams,
		Headers:         source.Sync.Headers,
		Auth:            source.Auth,
		Credentials:     creds,
	}
	return binding, models.IncrementalBindingFingerprint(binding), nil
}

// referencedCredentialKinds lists the credential kinds the auth config reads.
func referencedCredentialKinds(auth models.RESTAPIAuthConfig) []string {
	seen := map[string]bool{}
	out := []string{}
	add := func(kind string) {
		kind = strings.TrimSpace(kind)
		if kind != "" && !seen[kind] {
			seen[kind] = true
			out = append(out, kind)
		}
	}
	add(auth.SecretRef)
	add(auth.UsernameSecretRef)
	add(auth.PasswordSecretRef)
	return out
}

var errNoIncrementalDef = errors.New("sync def is not incremental in the current configuration")

// sourceIncrementalEnabled is the LIGHTWEIGHT mode probe (repo review item 9
// / source review item 4): under the held Source lock it decides ONLY whether
// this source is a rest_api source with an explicitly ENABLED incremental
// block. It performs NO full REST config parse or validation — legacy `{}`
// configs and non-rest_api connector types answer (false, nil) unchanged, so
// every pre-P25 admission behavior stays byte-identical (old P21/P23/P24
// fixtures with empty or minimal configs are untouched). An explicitly
// enabled but INVALID config still fails closed later, inside
// readIncrementalBinding's authoritative full parse (reached only for
// probed-true sources).
func sourceIncrementalEnabled(ctx context.Context, q cpQuerier, sourceID uuid.UUID, t Tenant) (bool, error) {
	connPredicate, connArg := connectionTenantPredicate(t, 2)
	var connectorType string
	var rawConfig []byte
	err := q.QueryRow(ctx,
		`SELECT c.connector_type, c.config FROM connections c WHERE c.id = $1 AND `+connPredicate,
		sourceID, connArg).Scan(&connectorType, &rawConfig)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !strings.EqualFold(strings.ReplaceAll(strings.TrimSpace(connectorType), "-", "_"), "rest_api") {
		return false, nil
	}
	if len(strings.TrimSpace(string(rawConfig))) == 0 {
		return false, nil
	}
	// Round2 fix 3: layer-by-layer RawMessage probing. A typed struct would
	// ERROR on legacy-valid shapes the frozen P23 suite explicitly allows
	// (e.g. rest_api sources with a scalar `sync: "x"`) and break their
	// admissions. Any non-object root/sync, any missing/non-object
	// incremental block, or a non-boolean enabled means LEGACY OFF (false,
	// nil) — never an error. Only an explicit boolean `true` activates the
	// full REST parse (which fails closed for malformed enabled configs).
	var root map[string]json.RawMessage
	if err := json.Unmarshal(rawConfig, &root); err != nil {
		return false, nil
	}
	syncRaw, ok := root["sync"]
	if !ok {
		return false, nil
	}
	var syncBlock map[string]json.RawMessage
	if err := json.Unmarshal(syncRaw, &syncBlock); err != nil {
		return false, nil // legacy scalar/shape — not expressible as incremental
	}
	incRaw, ok := syncBlock["incremental"]
	if !ok {
		return false, nil
	}
	var incBlock map[string]json.RawMessage
	if err := json.Unmarshal(incRaw, &incBlock); err != nil {
		return false, nil
	}
	enabledRaw, ok := incBlock["enabled"]
	if !ok {
		return false, nil
	}
	var enabled bool
	if err := json.Unmarshal(enabledRaw, &enabled); err != nil {
		return false, nil // non-boolean enabled is not an expressible opt-in
	}
	return enabled, nil
}

// sourceRecoveryEnabled is the P2.6 opt-in probe (root-decisions-v2 item 2):
// the same lockless-in-tx, layer-by-layer raw walk as
// sourceIncrementalEnabled. It answers ONLY whether the source config says
// sync.recovery.enabled == true. Legacy shapes — empty config, scalar sync,
// missing/non-object recovery block, non-boolean enabled — answer (false,
// nil) and every pre-P26 admission stays byte-identical. The authoritative
// full parse (ParseRESTAPISourceConfig -> NormalizeRESTAPISyncConfig) fails
// closed for an enabled-but-invalid combination; callers only reach this
// probe after sourceIncrementalEnabled already returned true.
func sourceRecoveryEnabled(ctx context.Context, q cpQuerier, sourceID uuid.UUID, t Tenant) bool {
	connPredicate, connArg := connectionTenantPredicate(t, 2)
	var connectorType string
	var rawConfig []byte
	err := q.QueryRow(ctx,
		`SELECT c.connector_type, c.config FROM connections c WHERE c.id = $1 AND `+connPredicate,
		sourceID, connArg).Scan(&connectorType, &rawConfig)
	if err != nil {
		return false
	}
	if !strings.EqualFold(strings.ReplaceAll(strings.TrimSpace(connectorType), "-", "_"), "rest_api") {
		return false
	}
	if len(strings.TrimSpace(string(rawConfig))) == 0 {
		return false
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(rawConfig, &root); err != nil {
		return false
	}
	syncRaw, ok := root["sync"]
	if !ok {
		return false
	}
	var syncBlock map[string]json.RawMessage
	if err := json.Unmarshal(syncRaw, &syncBlock); err != nil {
		return false // legacy scalar/shape — not expressible as an opt-in
	}
	recRaw, ok := syncBlock["recovery"]
	if !ok {
		return false
	}
	var recBlock map[string]json.RawMessage
	if err := json.Unmarshal(recRaw, &recBlock); err != nil {
		return false
	}
	enabledRaw, ok := recBlock["enabled"]
	if !ok {
		return false
	}
	var enabled bool
	if err := json.Unmarshal(enabledRaw, &enabled); err != nil {
		return false
	}
	return enabled
}

// ---------------------------------------------------------------------------
// Admission ownership (runs inside the caller's open admission transaction)
// ---------------------------------------------------------------------------

// AcquireCheckpointForAdmission runs INSIDE an open admission transaction
// that already holds the Source lock and the Def FOR UPDATE strong lock (and
// has passed the P2.3 admission gate). It locks-or-initializes the checkpoint
// row, verifies owner/binding, INSERTs the pending run with the frozen base,
// and sets the owner to that run. The caller stamps admission LAST.
//
// errCheckpointLockContention means the transaction is aborted: roll back and
// answer contention (manual: AdmissionRejection{contention}; scheduled:
// contention skip — never finishNoRun on the aborted transaction).
func (r *Repo) AcquireCheckpointForAdmission(ctx context.Context, tx pgx.Tx,
	syncID, sourceID uuid.UUID, t Tenant, requestedBy uuid.UUID,
	binding models.IncrementalBinding, bindingFingerprint string) (*models.SyncRun, *IncrementalRejection, error) {

	// 1. Lock-or-init the checkpoint row.
	row, err := lockOrInitCheckpoint(ctx, tx, syncID, sourceID, bindingFingerprint)
	if errors.Is(err, errCheckpointLockContention) {
		return nil, nil, err
	}
	if err != nil {
		return nil, nil, err
	}

	// 2. Owner/binding verification under the held lock.
	if row.ActiveRunID != nil {
		rejection := &IncrementalRejection{Kind: IncrementalBusy, ActiveRunID: *row.ActiveRunID}
		if row.ActiveSince != nil {
			rejection.ActiveSince = *row.ActiveSince
		}
		return nil, rejection, nil
	}
	if row.BindingFingerprint != bindingFingerprint {
		return nil, &IncrementalRejection{Kind: IncrementalBindingStale}, nil
	}

	// 3. Frozen baseline from the locked row. A never-confirmed checkpoint
	// falls back to the configured initial_value when present (the first
	// run's fixed input token); otherwise the input token is absent and the
	// incremental request parameter is omitted entirely.
	base := CheckpointBase{
		Mode:            CPModeCursor,
		BaseEpoch:       row.ConfirmedEpoch,
		Fingerprint:     bindingFingerprint,
		OutputDatasetID: row.OutputDatasetID,
	}
	switch {
	case row.CheckpointValue != nil:
		base.BaseValue = *row.CheckpointValue
		base.HasBaseValue = true
	case binding.Incremental.InitialValue != nil:
		base.BaseValue = *binding.Incremental.InitialValue
		base.HasBaseValue = true
	}

	// 4. Pending run with the captured baseline (FK target for the owner).
	runID := uuid.New()
	run, err := insertPendingSyncRunWithBase(ctx, tx, runID, syncID, sourceID, t, requestedBy, base)
	if err != nil {
		return nil, nil, err
	}
	if run == nil {
		return nil, nil, nil // parent chain invisible under this tenant
	}

	// 5. Owner now references the INSERTed run (root correction: never
	// before). The active_run_id IS NULL guard is a defense-in-depth recheck.
	tag, err := tx.Exec(ctx,
		`UPDATE sync_def_checkpoints
		    SET active_run_id = $2, active_since = clock_timestamp(), updated_at = clock_timestamp()
		  WHERE sync_def_id = $1 AND active_run_id IS NULL`,
		syncID, runID)
	if err != nil {
		return nil, nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, nil, fmt.Errorf("incremental owner set affected %d rows (expected 1)", tag.RowsAffected())
	}
	return run, nil, nil
}

// lockOrInitCheckpoint locks the def's checkpoint row (NOWAIT on the existing
// row; plain FOR UPDATE after initializing the placeholder — the Def strong
// lock excludes concurrent initialization). Returns the locked row.
func lockOrInitCheckpoint(ctx context.Context, tx pgx.Tx, syncID, sourceID uuid.UUID, bindingFingerprint string) (*checkpointRow, error) {
	row, err := scanCheckpointRow(tx.QueryRow(ctx,
		`SELECT `+checkpointColumns+` FROM sync_def_checkpoints cp
		  WHERE cp.sync_def_id = $1 FOR UPDATE NOWAIT`, syncID))
	if err == nil {
		return row, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		if isLockNotAvailable(err) {
			return nil, errCheckpointLockContention
		}
		return nil, err
	}
	// Absent: initialize the placeholder (idempotent under the Def strong
	// lock; DO NOTHING keeps a concurrent winner authoritative).
	if _, err := tx.Exec(ctx,
		`INSERT INTO sync_def_checkpoints (sync_def_id, source_id, output_dataset_id, mode, binding_fingerprint)
		 SELECT $1, $2,
		        (SELECT d.output_dataset_id FROM batch_sync_defs d WHERE d.id = $1),
		        $3, $4
		 ON CONFLICT (sync_def_id) DO NOTHING`,
		syncID, sourceID, CPModeCursor, bindingFingerprint); err != nil {
		return nil, err
	}
	return scanCheckpointRow(tx.QueryRow(ctx,
		`SELECT `+checkpointColumns+` FROM sync_def_checkpoints cp
		  WHERE cp.sync_def_id = $1 FOR UPDATE`, syncID))
}

// insertPendingSyncRunWithBase is insertPendingSyncRun (schedule.go) plus the
// admission-frozen baseline columns. Shared by manual and scheduled admission.
func insertPendingSyncRunWithBase(ctx context.Context, tx pgx.Tx, runID, syncID, sourceID uuid.UUID, t Tenant, requestedBy uuid.UUID, base CheckpointBase) (*models.SyncRun, error) {
	defPredicate, defArg := syncDefTenantPredicate(t, 4)
	var cpBaseValue *string
	if base.HasBaseValue {
		cpBaseValue = &base.BaseValue
	}
	// P2.6: freeze the recovery protocol AND the v1 identity triple from the
	// same locked rows the admission already trusts (root-decisions-v2 item
	// 2 + v2.1). Write-once, never updated; pre-P26 rows keep NULL. For an
	// incremental v1 run the triple mirrors the cp_* freeze (source =
	// def.source_id, output = base target, binding = incremental
	// fingerprint).
	recoveryProtocol := ""
	var recSource, recOutput, recBinding any
	if sourceRecoveryEnabled(ctx, tx, sourceID, t) {
		recoveryProtocol = models.RunRecoveryProtocolP26V1
		recSource = sourceID
		recOutput = base.OutputDatasetID
		recBinding = base.Fingerprint
	}
	var protocolArg any
	if recoveryProtocol != "" {
		protocolArg = recoveryProtocol
	}
	row := tx.QueryRow(ctx,
		`INSERT INTO sync_runs AS r (id, sync_def_id, status, org_id, owner_id, requested_by, control_mode,
		     cp_mode, cp_base_epoch, cp_base_value, cp_binding_fingerprint, cp_output_dataset_id,
		     recovery_protocol, recovery_source_id, recovery_output_dataset_id, recovery_binding_fingerprint)
		 SELECT $1, d.id, 'pending', d.org_id, c.owner_id, $3, 'controlled', $6, $7, $8, $9, $10,
		        $11, $12, $13, $14
		   FROM batch_sync_defs d JOIN connections c ON c.id = d.source_id
		  WHERE d.id = $2 AND d.source_id = $5 AND `+defPredicate+`
		 RETURNING `+runPublicColumns,
		runID, syncID, requestedBy, defArg, sourceID, base.Mode, base.BaseEpoch, cpBaseValue, base.Fingerprint, base.OutputDatasetID, protocolArg, recSource, recOutput, recBinding)
	run, err := scanSyncRun(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return run, nil
}

// ---------------------------------------------------------------------------
// Checkpoint-aware claim
// ---------------------------------------------------------------------------

// claimIncrementalSyncRun claims a pending run that owns a checkpoint:
// ordered Def(FOR UPDATE) -> Checkpoint(FOR UPDATE) -> Run (FOR UPDATE) with
// the post-lock DB clock, verifying owner and binding before the run CAS.
// Returns (nil, nil) when the run is not visible / not pending / verification
// missed (the run stays pending; the executor reports a lost/unclaimable
// claim honestly).
func (r *Repo) claimIncrementalSyncRun(ctx context.Context, runID uuid.UUID, t Tenant, holder, token string, ttl time.Duration, pre *runInternalRow) (*LeaseClaim, error) {
	if err := validateLeaseInstall(holder, token, ttl); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, checkpointOpBudget)
	defer cancel()
	var out *LeaseClaim
	err := r.withDefCheckpointRunLock(ctx, pre, t, func(tx pgx.Tx) error {
		cp, err := lockCheckpointForUpdate(ctx, tx, pre.SyncDefID)
		if err != nil {
			return err
		}
		binding, fingerprint, err := readIncrementalBinding(ctx, tx, pre.SyncDefID, pre.SourceID, t)
		if err != nil {
			return err
		}
		// Repo review item 5: claim verifies the FULL captured provenance —
		// owner, current binding == run's captured binding == checkpoint's
		// stored binding, base epoch, AND the captured output target. The
		// actual input token itself is the frozen run.cp_base_value (admission
		// durably stored the configured InitialValue there when no confirmed
		// value existed); it is never rederived from current config here.
		if cp.ActiveRunID == nil || *cp.ActiveRunID != runID ||
			cp.BindingFingerprint != fingerprint || fingerprint != pre.CPBindingFingerprint ||
			cp.ConfirmedEpoch != pre.CPBaseEpoch ||
			binding.OutputDatasetID != pre.CPOutputDatasetID {
			return nil // verification miss: not claimable here
		}
		claim, err := claimLockedPendingRun(ctx, tx, runID, holder, token, ttl)
		if err != nil || claim == nil {
			return err
		}
		out = claim
		return nil
	})
	if errors.Is(err, errCPNotVisible) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// claimLockedPendingRun performs the ClaimSyncRun UPDATE on a run row the
// caller already locked (same statement, same CAS discipline).
func claimLockedPendingRun(ctx context.Context, tx pgx.Tx, runID uuid.UUID, holder, token string, ttl time.Duration) (*LeaseClaim, error) {
	row := tx.QueryRow(ctx,
		`UPDATE sync_runs r
		    SET status = 'running',
		        started_at = clock_timestamp(),
		        attempt = r.attempt + 1,
		        generation = r.generation + 1,
		        lease_holder = $2,
		        lease_token = $3,
		        lease_expires_at = clock_timestamp() + make_interval(secs => $4::float8),
		        heartbeat_at = clock_timestamp()
		  WHERE r.id = $1 AND r.status = 'pending'
		  RETURNING `+leasedRunColumns,
		runID, holder, token, ttl.Seconds())
	claim, err := scanLeasedRun(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return claim, nil
}

// lockCheckpointForUpdate locks (FOR UPDATE) and returns the def's checkpoint
// row inside the caller's transaction. errCPNotVisible when absent.
func lockCheckpointForUpdate(ctx context.Context, tx pgx.Tx, defID uuid.UUID) (*checkpointRow, error) {
	row, err := scanCheckpointRow(tx.QueryRow(ctx,
		`SELECT `+checkpointColumns+` FROM sync_def_checkpoints cp
		  WHERE cp.sync_def_id = $1 FOR UPDATE`, defID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errCPNotVisible
	}
	if err != nil {
		return nil, err
	}
	return row, nil
}

var errCPNotVisible = errors.New("checkpoint row not visible")

// errCapturedBaselineMismatch is the defense-consistency guard of
// withDefCheckpointRunLock (repo review item 6): the locked run row's
// immutable captured baseline differs from the caller's trusted pre-read.
// Zero writes; callers map it to their fenced/not-visible outcomes.
var errCapturedBaselineMismatch = errors.New("run captured incremental baseline mismatch under locks")

// withDefCheckpointRunLock opens a transaction taking the ordered chain
// Def(FOR UPDATE) -> Checkpoint(FOR UPDATE) -> Run(FOR UPDATE) and runs fn.
// The run SELECT asserts r.sync_def_id matches the locked Def, and the
// captured baseline is REREAD under the locks and compared against the
// caller's trusted pre-read (item 6 — the cp_* columns are write-once, so a
// mismatch can only be corruption or a routing bug; it fences everything).
func (r *Repo) withDefCheckpointRunLock(ctx context.Context, pre *runInternalRow, t Tenant, fn func(tx pgx.Tx) error) error {
	runID := pre.RunID
	defID := pre.SyncDefID
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackCheckpointTx(tx)
	defPredicate, defArg := syncDefTenantPredicate(t, 2)
	var lockedDef uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT d.id FROM batch_sync_defs d WHERE d.id = $1 AND `+defPredicate+` FOR UPDATE`,
		defID, defArg).Scan(&lockedDef)
	if errors.Is(err, pgx.ErrNoRows) {
		return errCPNotVisible
	}
	if err != nil {
		return err
	}
	if _, err := lockCheckpointForUpdate(ctx, tx, defID); err != nil {
		return err
	}
	runPredicate, runArg := syncRunTenantPredicate(t, 2)
	var (
		lockedRun, lockedDefOfRun uuid.UUID
		cpMode                    string
		cpBaseEpoch               int
		cpBaseValue               *string
		cpFingerprint             string
		cpOutput                  uuid.UUID
		recoveryProtocol          string
		recSource, recOutput      uuid.UUID
		recFingerprint            string
	)
	err = tx.QueryRow(ctx,
		`SELECT r.id, r.sync_def_id,
		        COALESCE(r.cp_mode, ''), COALESCE(r.cp_base_epoch, 0),
		        r.cp_base_value, COALESCE(r.cp_binding_fingerprint, ''),
		        COALESCE(r.cp_output_dataset_id, '00000000-0000-0000-0000-000000000000'::uuid),
		        COALESCE(r.recovery_protocol, ''),
		        COALESCE(r.recovery_source_id, '00000000-0000-0000-0000-000000000000'::uuid),
		        COALESCE(r.recovery_output_dataset_id, '00000000-0000-0000-0000-000000000000'::uuid),
		        COALESCE(r.recovery_binding_fingerprint, '')
		   FROM sync_runs r WHERE r.id = $1 AND `+runPredicate+` FOR UPDATE`,
		runID, runArg).Scan(&lockedRun, &lockedDefOfRun, &cpMode, &cpBaseEpoch,
		&cpBaseValue, &cpFingerprint, &cpOutput, &recoveryProtocol,
		&recSource, &recOutput, &recFingerprint)
	if errors.Is(err, pgx.ErrNoRows) {
		return errCPNotVisible
	}
	if err != nil {
		return err
	}
	if lockedDefOfRun != defID {
		return errCPNotVisible
	}
	lockedBaseValue := ""
	if cpBaseValue != nil {
		lockedBaseValue = *cpBaseValue
	}
	if cpMode != pre.CPMode || cpBaseEpoch != pre.CPBaseEpoch || lockedBaseValue != pre.CPBaseValue ||
		cpFingerprint != pre.CPBindingFingerprint || cpOutput != pre.CPOutputDatasetID ||
		recoveryProtocol != pre.RecoveryProtocol ||
		recSource != pre.RecoverySourceID || recOutput != pre.RecoveryOutputDatasetID ||
		recFingerprint != pre.RecoveryBindingFingerprint {
		return errCapturedBaselineMismatch
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ---------------------------------------------------------------------------
// Publish gate (checkpoint aware)
// ---------------------------------------------------------------------------

// IncrementalPublishGateOutcome is the machine-readable gate decision.
type IncrementalPublishGateOutcome string

const (
	IncrementalPublishGateEntered      IncrementalPublishGateOutcome = "entered"
	IncrementalPublishGateCancelled    IncrementalPublishGateOutcome = "cancel_requested"
	IncrementalPublishGateFenced       IncrementalPublishGateOutcome = "fenced"
	IncrementalPublishGateBindingStale IncrementalPublishGateOutcome = "binding_stale"
	IncrementalPublishGateOwnerLost    IncrementalPublishGateOutcome = "owner_lost"
)

// IncrementalPublishGateResult reports the decision. Claim is set only for
// Entered; Run carries the row for the cancelled case.
type IncrementalPublishGateResult struct {
	Outcome IncrementalPublishGateOutcome
	Claim   *LeaseClaim
	Run     *models.SyncRun
}

// BeginIncrementalPublishGate is the checkpoint-aware publish-entry decision:
// ordered Def -> Checkpoint -> Run with the post-lock clock. Binding is
// re-verified from the authoritative (locked) def/source/credential reads
// against the run's frozen baseline BEFORE the gate CAS; the gate CAS itself
// is the BeginPublishGate statement verbatim.
func (r *Repo) BeginIncrementalPublishGate(ctx context.Context, runID uuid.UUID, t Tenant, holder, token string, generation int, ttl time.Duration) (*IncrementalPublishGateResult, error) {
	pre, err := r.runInternalPreRead(ctx, runID, t)
	if err != nil {
		return nil, err
	}
	if !isIncrementalRun(pre) {
		return nil, errors.New("BeginIncrementalPublishGate on a non-incremental run")
	}
	if pre.RecoveryProtocol == models.RunRecoveryProtocolP26V1 {
		// P2.6 (review-w2-round1 item 3): the old entry refuses p26-v1 —
		// the recoverable gate owns intent+stamp atomicity. Zero writes.
		return nil, ErrRecoveryRequiresRecoverableGate
	}
	if err := validateLeaseCAS(holder, token, generation, ttl); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, checkpointOpBudget)
	defer cancel()
	var out *IncrementalPublishGateResult
	err = r.withDefCheckpointRunLock(ctx, pre, t, func(tx pgx.Tx) error {
		cp, err := lockCheckpointForUpdate(ctx, tx, pre.SyncDefID)
		if err != nil {
			return err
		}
		binding, fingerprint, err := readIncrementalBinding(ctx, tx, pre.SyncDefID, pre.SourceID, t)
		if err != nil {
			return err
		}
		_ = binding
		if binding.OutputDatasetID != pre.CPOutputDatasetID ||
			fingerprint != pre.CPBindingFingerprint ||
			cp.BindingFingerprint != fingerprint {
			out = &IncrementalPublishGateResult{Outcome: IncrementalPublishGateBindingStale}
			return nil
		}
		if cp.ActiveRunID == nil || *cp.ActiveRunID != runID || cp.ConfirmedEpoch != pre.CPBaseEpoch {
			// Should be impossible under single-flight; treat as uncertain.
			out = &IncrementalPublishGateResult{Outcome: IncrementalPublishGateOwnerLost}
			return nil
		}
		// Gate CAS (BeginPublishGate statement verbatim) — the run row is
		// already locked by the chain, satisfying the two-phase protocol.
		claim, err := scanLeasedRun(tx.QueryRow(ctx,
			`UPDATE sync_runs r
			    SET publish_entered_at = clock_timestamp(),
			        lease_expires_at = clock_timestamp() + make_interval(secs => $5::float8),
			        heartbeat_at = clock_timestamp()
			  WHERE r.id = $1 AND r.status = 'running' AND r.control_mode = 'controlled'
			    AND r.lease_holder = $2 AND r.lease_token = $3 AND r.generation = $4
			    AND clock_timestamp() < r.lease_expires_at
			    AND r.cancel_requested_at IS NULL
			    AND r.publish_entered_at IS NULL
			  RETURNING `+leasedRunColumns,
			runID, holder, token, generation, ttl.Seconds()))
		if err == nil {
			out = &IncrementalPublishGateResult{Outcome: IncrementalPublishGateEntered, Claim: claim}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		// Diagnosis under the held locks (mirrors cancel.go's BeginPublishGate):
		// a persisted cancel only counts as Cancelled when the caller's lease
		// material is still the live, unexpired, controlled, gate-not-entered
		// holder; otherwise the refusal is a fencing failure.
		var (
			cancelRequestedAt *time.Time
			materialOK        bool
		)
		err = tx.QueryRow(ctx,
			`SELECT r.cancel_requested_at,
			       COALESCE(r.status = 'running' AND r.control_mode = 'controlled'
			         AND r.lease_holder = $2 AND r.lease_token = $3 AND r.generation = $4
			         AND clock_timestamp() < r.lease_expires_at
			         AND r.publish_entered_at IS NULL, false) AS material_ok
			  FROM sync_runs r WHERE r.id = $1`,
			runID, holder, token, generation).Scan(&cancelRequestedAt, &materialOK)
		if errors.Is(err, pgx.ErrNoRows) {
			out = &IncrementalPublishGateResult{Outcome: IncrementalPublishGateFenced}
			return nil
		}
		if err != nil {
			return err
		}
		if !materialOK || cancelRequestedAt == nil {
			out = &IncrementalPublishGateResult{Outcome: IncrementalPublishGateFenced}
			return nil
		}
		run, rerr := lockedRunByID(ctx, tx, runID)
		if rerr != nil {
			return rerr
		}
		out = &IncrementalPublishGateResult{Outcome: IncrementalPublishGateCancelled, Run: run}
		return nil
	})
	if errors.Is(err, errCPNotVisible) {
		return &IncrementalPublishGateResult{Outcome: IncrementalPublishGateFenced}, nil
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Success finalize (all-or-nothing)
// ---------------------------------------------------------------------------

// IncrementalAdvance carries the confirmed batch facts for the advance.
type IncrementalAdvance struct {
	CompletedCheckpoint string
	Rows                int64
	Bytes               int64
	ContentHash         string
	TxnID               uuid.UUID
}

// completeIncrementalLocked is the shared success core of the P25 and P2.6
// finalize paths. The caller holds the ordered Def->Checkpoint->Run locks
// (withDefCheckpointRunLock) and has ALREADY made its path-specific decision
// (the P25 entry re-verifies the current binding; the P2.6 recovery finalize
// instead proves a committed DVS receipt and may be answering an
// already-succeeded run). This function performs:
//
//   - the success CAS (CompleteLeasedSyncRun predicates verbatim: running +
//     lease material + unexpired + gate entered + no cancel request),
//   - the checkpoint advance + owner release in the SAME transaction,
//   - the optional finalize of the run's publish intent row (P2.6; nil for
//     pre-P26 runs — a run without an intent row simply skips it).
//
// Any predicate miss writes NOTHING and returns a typed error.
func completeIncrementalLocked(ctx context.Context, tx pgx.Tx, pre *runInternalRow, holder, token string, generation int, adv IncrementalAdvance, finalizeIntentTxn uuid.UUID) (*models.SyncRun, error) {
	runID := pre.RunID
	// Success CAS — CompleteLeasedSyncRun predicates verbatim.
	run, err := scanLeasedRun(tx.QueryRow(ctx,
		`UPDATE sync_runs r
		    SET status = 'succeeded',
		        finished_at = clock_timestamp(),
		        bytes_written = $5,
		        files_written = $6,
		        rows_written = $7,
		        error = NULL,
		        ingest_job_id = NULL,
		        dataset_version_id = $8,
		        content_hash = $9,
		        heartbeat_at = clock_timestamp()
		  WHERE r.id = $1 AND r.status = 'running'
		    AND r.lease_holder = $2 AND r.lease_token = $3 AND r.generation = $4
		    AND clock_timestamp() < r.lease_expires_at
		    AND (r.control_mode = 'legacy'
		      OR ('succeeded' = 'succeeded' AND r.publish_entered_at IS NOT NULL AND r.cancel_requested_at IS NULL))
		  RETURNING `+leasedRunColumns,
		runID, holder, token, generation, adv.Bytes, int64(1), adv.Rows, adv.TxnID, adv.ContentHash))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrLeaseLost // nothing written; honest fencing
	}
	if err != nil {
		return nil, err
	}
	// Advance + release in the same transaction (predicates identical for
	// every caller — epoch advances at most once per run).
	tag, err := tx.Exec(ctx,
		`UPDATE sync_def_checkpoints
		    SET confirmed_epoch = confirmed_epoch + 1,
		        checkpoint_value = $2,
		        active_run_id = NULL,
		        active_since = NULL,
		        advanced_by_run = $3,
		        advanced_at = clock_timestamp(),
		        last_batch_rows = $4,
		        last_batch_bytes = $5,
		        last_batch_hash = $6,
		        last_batch_txn = $7,
		        updated_at = clock_timestamp()
		  WHERE sync_def_id = $1
		    AND active_run_id = $3
		    AND confirmed_epoch = $8
		    AND binding_fingerprint = $9`,
		pre.SyncDefID, adv.CompletedCheckpoint, runID, adv.Rows, adv.Bytes,
		adv.ContentHash, adv.TxnID, pre.CPBaseEpoch, pre.CPBindingFingerprint)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrIncrementalCheckpointCAS // rolls the success write back too
	}
	// P2.6 intent finalize: same transaction as the success + advance +
	// release. Existing/finalized state is validated by the caller (the
	// recovery path); here the row — when one exists and is not yet
	// finalized — must atomically become finalized with the receipt's txn.
	// A no-op when the run has no intent row (every pre-P26 run).
	itag, err := tx.Exec(ctx,
		`UPDATE sync_run_publish_intents
		    SET recovery_state = 'finalized',
		        txn_id = $2,
		        resolved_at = clock_timestamp(),
		        updated_at = clock_timestamp()
		  WHERE run_id = $1 AND recovery_state <> 'finalized'`,
		runID, finalizeIntentTxn)
	if err != nil {
		return nil, err
	}
	if itag.RowsAffected() > 1 {
		return nil, ErrIncrementalCheckpointCAS // PK-guarded; unreachable shape
	}
	return &run.Run, nil
}

// CompleteIncrementalSyncRun writes the run's success AND advances+releases
// the checkpoint in ONE transaction — all-or-nothing. Any predicate miss
// (lease/owner/epoch/fingerprint) writes NOTHING and returns a typed error;
// the run stays running as an honest uncertain outcome (P26). A lost COMMIT
// response is reported as an error: the database is either all-old or all-new
// (the owner may already be cleared) — never repaired or guessed here.
func (r *Repo) CompleteIncrementalSyncRun(ctx context.Context, runID uuid.UUID, t Tenant, holder, token string, generation int, adv IncrementalAdvance) (*models.SyncRun, error) {
	// P2.6 (review-w2-round1 item 3): the old P25 completion entry refuses
	// p26-v1 runs outright — a bare transaction UUID is NOT receipt evidence,
	// and a v1 success/advance may only be written by
	// FinalizeRecoveredIncrementalRun (verified receipt + intent finalize in
	// the same transaction). Pre-gate v1 failures keep their path through
	// CompleteLeasedSyncRun → FailIncrementalSyncRun. Zero writes.
	if pre, err := r.runInternalPreRead(ctx, runID, t); err != nil {
		return nil, err
	} else if pre != nil && pre.RecoveryProtocol == models.RunRecoveryProtocolP26V1 {
		return nil, ErrRecoveryRequiresFinalize
	}
	// Repo review item 4: validate the advance payload BEFORE any lock —
	// genuine facts only (non-nil txn, non-negative counts, 64-hex sha256).
	if err := models.ValidateOpaqueCheckpointValue(adv.CompletedCheckpoint, "completed checkpoint"); err != nil {
		return nil, fmt.Errorf("refusing to advance with an invalid completed checkpoint: %w", err)
	}
	if adv.TxnID == uuid.Nil {
		return nil, errors.New("refusing to advance without a transaction id")
	}
	if adv.Rows < 0 || adv.Bytes < 0 {
		return nil, errors.New("refusing to advance with negative batch facts")
	}
	if len(adv.ContentHash) != 64 {
		return nil, errors.New("refusing to advance without a 64-hex content hash")
	}
	for _, c := range adv.ContentHash {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return nil, errors.New("refusing to advance without a lowercase-hex content hash")
		}
	}
	pre, err := r.runInternalPreRead(ctx, runID, t)
	if err != nil {
		return nil, err
	}
	if !isIncrementalRun(pre) {
		return nil, errors.New("CompleteIncrementalSyncRun on a non-incremental run")
	}
	if strings.TrimSpace(holder) == "" || strings.TrimSpace(token) == "" || generation < 1 {
		return nil, errors.New("invalid lease fencing material for incremental completion")
	}
	ctx, cancel := context.WithTimeout(ctx, checkpointOpBudget)
	defer cancel()
	var out *models.SyncRun
	err = r.withDefCheckpointRunLock(ctx, pre, t, func(tx pgx.Tx) error {
		cp, err := lockCheckpointForUpdate(ctx, tx, pre.SyncDefID)
		if err != nil {
			return err
		}
		// Item 4: re-read the CURRENT full binding/target under the ordered
		// locks and compare against BOTH the captured run baseline AND the
		// stored checkpoint — never merely cp.oldfp == run.oldfp. Current
		// source-auth/Def drift must not confirm an old position.
		binding, fingerprint, err := readIncrementalBinding(ctx, tx, pre.SyncDefID, pre.SourceID, t)
		if err != nil {
			return err
		}
		if cp.ActiveRunID == nil || *cp.ActiveRunID != runID ||
			cp.ConfirmedEpoch != pre.CPBaseEpoch || cp.BindingFingerprint != pre.CPBindingFingerprint ||
			fingerprint != pre.CPBindingFingerprint ||
			binding.OutputDatasetID != pre.CPOutputDatasetID {
			return ErrIncrementalCheckpointCAS // nothing written; run stays running
		}
		out, err = completeIncrementalLocked(ctx, tx, pre, holder, token, generation, adv, uuid.Nil)
		return err
	})
	if errors.Is(err, errCPNotVisible) {
		return nil, ErrIncrementalCheckpointCAS
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Failure finalize with conditional release (gate boundary is authoritative)
// ---------------------------------------------------------------------------

// FailIncrementalSyncRun writes a failed/aborted terminal state for an
// incremental run through the ordered Def->Checkpoint->Run chain and clears
// the owner ONLY when the DB-asserted conservative boundary allows it:
// publish_entered_at IS NULL. After gate entry the run is still closed as an
// honest 'failed' (when the lease CAS still holds) but the owner is RETAINED
// for P26. 'aborted' keeps the stage guard (gate must not have been entered).
func (r *Repo) FailIncrementalSyncRun(ctx context.Context, runID uuid.UUID, t Tenant, holder, token string, generation int, status string, errMsg *string, contentHash *string) (*models.SyncRun, error) {
	switch status {
	case "failed", "aborted":
	default:
		return nil, errors.New("invalid terminal status for incremental failure: " + status)
	}
	if strings.TrimSpace(holder) == "" || strings.TrimSpace(token) == "" || generation < 1 {
		return nil, errors.New("invalid lease fencing material for incremental failure")
	}
	pre, err := r.runInternalPreRead(ctx, runID, t)
	if err != nil {
		return nil, err
	}
	if !isIncrementalRun(pre) {
		return nil, errors.New("FailIncrementalSyncRun on a non-incremental run")
	}
	ctx, cancel := context.WithTimeout(ctx, checkpointOpBudget)
	defer cancel()
	var out *models.SyncRun
	err = r.withDefCheckpointRunLock(ctx, pre, t, func(tx pgx.Tx) error {
		// P2.6 (v2.1): a p26-v1 run past the publish gate may NOT be closed
		// as failed/aborted — the unknown outcome must stay `running` until
		// recovery converges it (RecordRecoveryWait / FinalizeRecoveredRun).
		// Zero writes; pre-gate failures keep the original P25 semantics.
		if pre.RecoveryProtocol == models.RunRecoveryProtocolP26V1 {
			var gate *time.Time
			if gerr := tx.QueryRow(ctx,
				`SELECT r.publish_entered_at FROM sync_runs r WHERE r.id = $1`, runID).Scan(&gate); gerr != nil {
				return gerr
			}
			if gate != nil {
				return ErrRecoveryPostGateLocked
			}
		}
		cp, err := lockCheckpointForUpdate(ctx, tx, pre.SyncDefID)
		if err != nil {
			return err
		}
		// Repo review item 3: the FULL owner/baseepoch/binding guard gates
		// EVERY terminal mutation — including the post-gate retain branch. A
		// mismatch is a fenced/uncertain zero-write, never permission to
		// close somebody else's captured work.
		binding, fingerprint, err := readIncrementalBinding(ctx, tx, pre.SyncDefID, pre.SourceID, t)
		if err != nil {
			return err
		}
		if cp.ActiveRunID == nil || *cp.ActiveRunID != runID ||
			cp.ConfirmedEpoch != pre.CPBaseEpoch || cp.BindingFingerprint != pre.CPBindingFingerprint ||
			fingerprint != pre.CPBindingFingerprint ||
			binding.OutputDatasetID != pre.CPOutputDatasetID {
			return ErrIncrementalCheckpointCAS
		}
		run, err := scanLeasedRun(tx.QueryRow(ctx,
			`UPDATE sync_runs r
			    SET status = $5,
			        finished_at = clock_timestamp(),
			        bytes_written = 0,
			        files_written = 0,
			        rows_written = 0,
			        error = $6,
			        ingest_job_id = NULL,
			        dataset_version_id = NULL,
			        content_hash = $7,
			        heartbeat_at = clock_timestamp()
			  WHERE r.id = $1 AND r.status = 'running'
			    AND r.lease_holder = $2 AND r.lease_token = $3 AND r.generation = $4
			    AND clock_timestamp() < r.lease_expires_at
			    AND (r.control_mode = 'legacy'
			      OR ($5 = 'succeeded' AND r.publish_entered_at IS NOT NULL AND r.cancel_requested_at IS NULL)
			      OR ($5 = 'aborted' AND r.publish_entered_at IS NULL)
			      OR ($5 = 'failed'))
			  RETURNING `+leasedRunColumns,
			runID, holder, token, generation, status, errMsg, contentHash))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost // nothing written
		}
		if err != nil {
			return err
		}
		// DB-asserted release boundary: only a never-gated run may release.
		if run.Run.PublishEnteredAt == nil {
			tag, err := tx.Exec(ctx,
				`UPDATE sync_def_checkpoints
				    SET active_run_id = NULL, active_since = NULL, updated_at = clock_timestamp()
				  WHERE sync_def_id = $1 AND active_run_id = $2`,
				pre.SyncDefID, runID)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return ErrIncrementalCheckpointCAS
			}
		}
		// Gate entered: honest failed, owner retained (P26).
		out = &run.Run
		return nil
	})
	if errors.Is(err, errCPNotVisible) {
		return nil, ErrIncrementalCheckpointCAS
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Cancel (checkpoint aware)
// ---------------------------------------------------------------------------

// RequestCancelIncrementalSyncRun is the ordered cancel for captured P25
// runs: Source -> Def -> Checkpoint -> Run. pending -> aborted + owner
// release in one transaction (a pending run never entered the gate; the
// release boundary assertion holds trivially but is still enforced by the
// shared stage guard). running -> persist the cancel request only. Outcome
// semantics mirror RequestCancelSyncRun.
func (r *Repo) RequestCancelIncrementalSyncRun(ctx context.Context, runID, syncID, sourceID uuid.UUID, t Tenant, cancelledBy *uuid.UUID, cause string) (*CancelResult, error) {
	ctx, cancel := context.WithTimeout(ctx, checkpointOpBudget)
	defer cancel()
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollbackCheckpointTx(tx)
	// Source -> Def -> Checkpoint -> Run (repo review item 2: the Def lock
	// was missing — the claimed order is now the actual order). A wrong
	// parent/tenant answers a zero-write NotVisible before any cancellation
	// or release.
	if err := lockSourceRowForUpdate(ctx, tx, sourceID, t); err != nil {
		if errors.Is(err, errCPNotVisible) {
			return &CancelResult{Outcome: CancelOutcomeNotVisible}, nil
		}
		return nil, err
	}
	// Round2 fix 1: the SQL already binds $1=def and $2=source, so the tenant
	// predicate placeholder must start at $3 (argPos 2 would collide with
	// sourceID and bind the source UUID as the tenant value on real PG).
	defPredicate, defArg := syncDefTenantPredicate(t, 3)
	var lockedDef uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT d.id FROM batch_sync_defs d
		  WHERE d.id = $1 AND d.source_id = $2 AND `+defPredicate+` FOR UPDATE`,
		syncID, sourceID, defArg).Scan(&lockedDef)
	if errors.Is(err, pgx.ErrNoRows) {
		return &CancelResult{Outcome: CancelOutcomeNotVisible}, nil
	}
	if err != nil {
		return nil, err
	}
	if _, err := lockCheckpointForUpdate(ctx, tx, syncID); err != nil {
		if errors.Is(err, errCPNotVisible) {
			return &CancelResult{Outcome: CancelOutcomeNotVisible}, nil
		}
		return nil, err
	}
	predicate, arg := syncRunTenantPredicate(t, 3)
	var (
		status, controlMode                 string
		cancelRequestedAt, publishEnteredAt *time.Time
	)
	err = tx.QueryRow(ctx,
		`SELECT r.status, r.control_mode, r.cancel_requested_at, r.publish_entered_at
		   FROM sync_runs r
		  WHERE r.id = $1 AND r.sync_def_id = $2 AND `+predicate+`
		  FOR UPDATE`,
		runID, syncID, arg).Scan(&status, &controlMode, &cancelRequestedAt, &publishEnteredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return &CancelResult{Outcome: CancelOutcomeNotVisible}, nil
	}
	if err != nil {
		return nil, err
	}
	finish := func(outcome CancelOutcome) (*CancelResult, error) {
		run, err := lockedRunByID(ctx, tx, runID)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &CancelResult{Outcome: outcome, Run: run}, nil
	}
	if controlMode != models.RunControlControlled {
		return finish(CancelOutcomeUnsupported)
	}
	switch status {
	case "succeeded", "failed", "aborted":
		return finish(CancelOutcomeAlreadyTerminal)
	}
	if publishEnteredAt != nil {
		return finish(CancelOutcomeTooLate)
	}
	if status == "pending" {
		row := tx.QueryRow(ctx,
			`UPDATE sync_runs r
			    SET status = 'aborted',
			        finished_at = clock_timestamp(),
			        error = $2,
			        cancel_requested_at = COALESCE(r.cancel_requested_at, clock_timestamp()),
			        cancelled_by = COALESCE(r.cancelled_by, $3),
			        heartbeat_at = clock_timestamp()
			  WHERE r.id = $1 AND r.status = 'pending' AND r.control_mode = 'controlled'
			  RETURNING `+runPublicColumns,
			runID, cause, cancelledBy)
		run, err := scanSyncRun(row)
		if errors.Is(err, pgx.ErrNoRows) {
			// A claim won the race; fall through to the request-persist path
			// below by re-reading the row's live state.
			live, rerr := lockedRunByID(ctx, tx, runID)
			if rerr != nil {
				return nil, rerr
			}
			run = live
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		if run.Status == "aborted" {
			// Provable zero-dispatch: pending never entered the gate. The
			// release boundary assertion (publish_entered_at IS NULL) is
			// enforced by the pending guard itself.
			tag, terr := tx.Exec(ctx,
				`UPDATE sync_def_checkpoints
				    SET active_run_id = NULL, active_since = NULL, updated_at = clock_timestamp()
				  WHERE sync_def_id = $1 AND active_run_id = $2`,
				syncID, runID)
			if terr != nil {
				return nil, terr
			}
			if tag.RowsAffected() != 1 {
				return nil, ErrIncrementalCheckpointCAS
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, err
			}
			return &CancelResult{Outcome: CancelOutcomeAborted, Run: run}, nil
		}
		// Raced to running: persist the request (below) using the fresh row.
	}
	if status == "running" || status == "pending" {
		row := tx.QueryRow(ctx,
			`UPDATE sync_runs r
			    SET cancel_requested_at = COALESCE(r.cancel_requested_at, clock_timestamp()),
			        cancelled_by = COALESCE(r.cancelled_by, $2)
			  WHERE r.id = $1 AND r.status = 'running' AND r.control_mode = 'controlled'
			    AND r.publish_entered_at IS NULL AND r.cancel_requested_at IS NULL
			  RETURNING `+runPublicColumns,
			runID, cancelledBy)
		run, err := scanSyncRun(row)
		if errors.Is(err, pgx.ErrNoRows) {
			// Already requested, already gated or already terminal: report
			// the row's honest state.
			live, rerr := lockedRunByID(ctx, tx, runID)
			if rerr != nil {
				return nil, rerr
			}
			switch {
			case live.Status == "running" && live.PublishEnteredAt != nil:
				return finishFromLive(ctx, tx, CancelOutcomeTooLate, live)
			default:
				return finishFromLive(ctx, tx, CancelOutcomeRequested, live)
			}
		}
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &CancelResult{Outcome: CancelOutcomeRequested, Run: run}, nil
	}
	return nil, fmt.Errorf("unexpected incremental run status %q during cancel", status)
}

func finishFromLive(ctx context.Context, tx pgx.Tx, outcome CancelOutcome, run *models.SyncRun) (*CancelResult, error) {
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &CancelResult{Outcome: outcome, Run: run}, nil
}

// lockSourceRowForUpdate locks the source row (id + tenant) FOR UPDATE inside
// an open transaction. Returns errCPNotVisible when absent.
func lockSourceRowForUpdate(ctx context.Context, tx pgx.Tx, sourceID uuid.UUID, t Tenant) error {
	predicate, arg := connectionTenantPredicate(t, 2)
	var locked uuid.UUID
	err := tx.QueryRow(ctx,
		`SELECT c.id FROM connections c WHERE c.id = $1 AND `+predicate+` FOR UPDATE`,
		sourceID, arg).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return errCPNotVisible
	}
	return err
}

// ---------------------------------------------------------------------------
// Mutation fences
// ---------------------------------------------------------------------------

// lockSourceDefsForProtection locks (FOR UPDATE, sorted by def id) every def
// of the source visible under the tenant and returns their ids. Protected
// writers MUST hold these locks before mutating connections.config or
// source_credentials rows.
func lockSourceDefsForProtection(ctx context.Context, tx pgx.Tx, sourceID uuid.UUID, t Tenant) ([]uuid.UUID, error) {
	predicate, arg := syncDefTenantPredicate(t, 2)
	rows, err := tx.Query(ctx,
		`SELECT d.id FROM batch_sync_defs d
		  WHERE d.source_id = $1 AND `+predicate+`
		  ORDER BY d.id FOR UPDATE`,
		sourceID, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// lockSourceDefsForProtectionInternal is the tenant-less variant for internal
// system surfaces (auto-registration config rewrite) that carry no tenant:
// same sorted FOR UPDATE ordering, same row set.
func lockSourceDefsForProtectionInternal(ctx context.Context, tx pgx.Tx, sourceID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := tx.Query(ctx,
		`SELECT d.id FROM batch_sync_defs d WHERE d.source_id = $1 ORDER BY d.id FOR UPDATE`, sourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// assertCheckpointOwnersFreeTx errors with ErrIncrementalOwnerUnresolved when
// any listed def still owns a checkpoint — regardless of the owned run's
// state (pending/running/failed-unknown/expired). Run inside a transaction
// holding the Def locks.
func assertCheckpointOwnersFreeTx(ctx context.Context, tx pgx.Tx, defIDs []uuid.UUID) error {
	if len(defIDs) == 0 {
		return nil
	}
	idStrings := make([]string, len(defIDs))
	for i, id := range defIDs {
		idStrings[i] = id.String()
	}
	rows, err := tx.Query(ctx,
		`SELECT cp.sync_def_id, cp.active_run_id, cp.active_since
		   FROM sync_def_checkpoints cp
		  WHERE cp.active_run_id IS NOT NULL AND cp.sync_def_id = ANY($1)`, idStrings)
	if err != nil {
		return err
	}
	defer rows.Close()
	held := []string{}
	for rows.Next() {
		var defID, runID uuid.UUID
		var since *time.Time
		if err := rows.Scan(&defID, &runID, &since); err != nil {
			return err
		}
		held = append(held, fmt.Sprintf("def=%s run=%s", defID, runID))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(held) > 0 {
		return fmt.Errorf("%w: %s", ErrIncrementalOwnerUnresolved, strings.Join(held, ", "))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Public view
// ---------------------------------------------------------------------------

// CheckpointView is the safe public projection of a def's checkpoint state.
// The opaque stored value never appears — only its length and sha256.
type CheckpointView struct {
	Mode             string                       `json:"mode"`
	Initialized      bool                         `json:"initialized"`
	CheckpointLen    int                          `json:"checkpoint_len,omitempty"`
	CheckpointSHA256 string                       `json:"checkpoint_sha256,omitempty"`
	ConfirmedEpoch   int                          `json:"confirmed_epoch"`
	Owner            *CheckpointOwnerView         `json:"owner,omitempty"`
	LastConfirmed    *CheckpointLastConfirmedView `json:"last_confirmed,omitempty"`
}

// CheckpointOwnerView names the durable owner and why it blocks.
type CheckpointOwnerView struct {
	RunID         uuid.UUID `json:"run_id"`
	RunStatus     string    `json:"run_status"`
	Since         time.Time `json:"since"`
	BlockedReason string    `json:"blocked_reason"` // "requires_reconciliation" | "" (business state; internal phase names never leak)
}

// CheckpointLastConfirmedView summarizes the last confirmed batch.
type CheckpointLastConfirmedView struct {
	RunID       uuid.UUID `json:"run_id"`
	TxnID       uuid.UUID `json:"txn_id"`
	Rows        int64     `json:"rows"`
	Bytes       int64     `json:"bytes"`
	ContentHash string    `json:"content_hash"`
	At          time.Time `json:"at"`
}

// SyncRunIncrementalInfo returns the safe public incremental projection for
// a run (mode + base epoch), or nil for non-incremental/legacy runs.
func (r *Repo) SyncRunIncrementalInfo(ctx context.Context, runID uuid.UUID, t Tenant) (*models.RunIncrementalView, error) {
	pre, err := r.runInternalPreRead(ctx, runID, t)
	if err != nil {
		return nil, err
	}
	if !isIncrementalRun(pre) {
		return nil, nil
	}
	return &models.RunIncrementalView{Mode: pre.CPMode, BaseEpoch: pre.CPBaseEpoch}, nil
}

// GetCheckpointView returns the def's checkpoint view under the trusted
// parent-chain tenant check (syncDefTenantPredicate join).
func (r *Repo) GetCheckpointView(ctx context.Context, defID uuid.UUID, t Tenant) (*CheckpointView, error) {
	predicate, arg := syncDefTenantPredicate(t, 2)
	cp, err := scanCheckpointRow(r.Pool.QueryRow(ctx,
		`SELECT `+checkpointColumns+` FROM sync_def_checkpoints cp
		  JOIN batch_sync_defs d ON d.id = cp.sync_def_id
		 WHERE cp.sync_def_id = $1 AND `+predicate, defID, arg))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	view := &CheckpointView{Mode: cp.Mode, ConfirmedEpoch: cp.ConfirmedEpoch}
	if cp.CheckpointValue != nil {
		view.Initialized = true
		view.CheckpointLen = len(*cp.CheckpointValue)
		view.CheckpointSHA256 = sha256Hex(*cp.CheckpointValue)
	}
	if cp.ActiveRunID != nil {
		owner := &CheckpointOwnerView{RunID: *cp.ActiveRunID}
		if cp.ActiveSince != nil {
			owner.Since = *cp.ActiveSince
		}
		// Repo review item 8: owner observability is based on the CURRENT
		// owner run's status and its ACTUAL lease liveness (SQL-side clock
		// comparison) — never on the historical advanced_by_run marker. A
		// read error fails CLOSED (propagates), never a silent blank status.
		var (
			runStatus string
			leaseLive *bool
			gateKnown *time.Time
		)
		runPredicate, runArg := syncRunTenantPredicate(t, 2)
		err := r.Pool.QueryRow(ctx,
			`SELECT r.status,
			        (r.lease_expires_at IS NOT NULL AND r.lease_expires_at > clock_timestamp()) AS lease_live,
			        r.publish_entered_at
			   FROM sync_runs r WHERE r.id = $1 AND `+runPredicate,
			*cp.ActiveRunID, runArg).Scan(&runStatus, &leaseLive, &gateKnown)
		if err != nil {
			return nil, fmt.Errorf("incremental checkpoint owner run unreadable: %w", err)
		}
		owner.RunStatus = runStatus
		switch {
		case runStatus == "failed":
			// Post-dispatch unknown outcome: the owner is an unconfirmed result requiring reconciliation (business state).
			owner.BlockedReason = "requires_reconciliation"
		case runStatus == "running" && leaseLive != nil && !*leaseLive:
			// Expired holder with no takeover for captured P25 work: the
			// owner is an unconfirmed result requiring reconciliation.
			owner.BlockedReason = "requires_reconciliation"
		default:
			// pending / live running: queryably owned, no claim about this
			// instance executing it.
			owner.BlockedReason = ""
		}
		view.Owner = owner
	}
	if cp.AdvancedByRun != nil && cp.AdvancedAt != nil {
		last := &CheckpointLastConfirmedView{
			RunID: *cp.AdvancedByRun,
			At:    *cp.AdvancedAt,
		}
		if cp.LastBatchTxn != nil {
			last.TxnID = *cp.LastBatchTxn
		}
		if cp.LastBatchRows != nil {
			last.Rows = *cp.LastBatchRows
		}
		if cp.LastBatchBytes != nil {
			last.Bytes = *cp.LastBatchBytes
		}
		if cp.LastBatchHash != nil {
			last.ContentHash = *cp.LastBatchHash
		}
		view.LastConfirmed = last
	}
	return view, nil
}
