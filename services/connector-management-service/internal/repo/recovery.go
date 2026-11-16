// P2.6 recoverable publish gate + fenced recovery state machine
// (root-decisions-v2 items 3–6 + v2.1 generalization).
//
// Every persistent transition of a p26-v1 run's intent is fenced by the full
// lease material (holder + token + generation + unexpired + status/gate
// guards) — there are deliberately NO unfenced state markers. Post-gate
// unknown outcomes keep the run `running` with a recorded wait reason and
// backoff; nothing but a verified committed DVS receipt may finalize, and no
// verification failure (404/ABORTED/OPEN/5xx/bad ACK/budget exhaustion)
// releases the checkpoint owner. Pre-gate v1 failures keep the exact P25
// release semantics through the existing paths.
//
// v2.1: the protocol covers BOTH modes — incremental (cp_mode set, ordered
// Def→CP→Run→Intent) and snapshot (cp_mode empty, ordered Def→Run→Intent, no
// checkpoint row, no epoch, last-commit-wins snapshot semantics preserved).
// The generic entries (BeginRecoverablePublishGate / FinalizeRecoveredRun /
// ClaimRecovery) route on the run's FROZEN mode; the ...Incremental names
// remain as thin wrappers for the first slice's callers.
//
// Lock order is the pinned chain everywhere: Source -> Def -> (CP, only when
// the def's protocol work is incremental) -> Run, the intent row taken last
// under the held Run lock.
package repo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// RecoveryGateLeaseTTL is the full-lease TTL the recoverable gate renews for
// (mirrors the executor's syncLeaseTTL of 60s — the gate precedes a bounded
// publish dispatch plus fenced replays, each of which renews again).
const RecoveryGateLeaseTTL = 60 * time.Second

// Typed P2.6 errors. All carry zero-write semantics at the SQL layer.
var (
	// ErrRecoveryRequiresFinalize: a p26-v1 run's success may only be
	// written by FinalizeRecoveredRun (intent finalized in the same
	// transaction).
	ErrRecoveryRequiresFinalize = errors.New("p26-v1 run success must go through FinalizeRecoveredRun")
	// ErrRecoveryPostGateLocked: a p26-v1 run past the publish gate must
	// stay `running` (unknown) — failure/abort writes are refused until
	// recovery converges the run.
	ErrRecoveryPostGateLocked = errors.New("p26-v1 run past the publish gate must stay running until recovery converges it")
	// ErrRecoveryRunsActive: the P26-only mutation fence — an effective
	// credential or def-input (file_glob/target) change is refused while a
	// v1 pending/running run exists for the source/def.
	ErrRecoveryRunsActive = errors.New("source has active p26-v1 recovery runs; effective protected change refused")
	// ErrRecoveryRequiresRecoverableGate: the OLD public gate entries
	// (BeginPublishGate / BeginIncrementalPublishGate) refuse p26-v1 runs —
	// a v1 gate may only be entered through BeginRecoverablePublishGate,
	// where the intent and the gate stamp commit atomically (review-w2-round1
	// item 3: no gate-without-intent bypass). Zero writes.
	ErrRecoveryRequiresRecoverableGate = errors.New("p26-v1 run must enter the publish gate through BeginRecoverablePublishGate")
	// ErrRecoveryMaterialParentMismatch: the frozen material's source/def
	// identity does not match the run's trusted frozen parents (review-w2-
	// round1 item 5). Trust is anchored at the gate against the pre-read —
	// a caller-self-consistent body whose lineage names OTHER parents must
	// never reach the intent INSERT. Zero writes.
	ErrRecoveryMaterialParentMismatch = errors.New("publish material parent identity does not match the run's frozen source/def")
)

// validateFrozenMaterialParents is the gate-side identity anchor
// (review-w2-round1 item 5): the material (and with it the serialized body's
// lineage triple, already cross-checked by ValidateFrozenPublishMaterial)
// must name EXACTLY the run's trusted frozen parents.
func validateFrozenMaterialParents(sourceID, defID uuid.UUID, mat FrozenPublishMaterial) error {
	if mat.SourceID != sourceID || mat.SyncDefID != defID {
		return ErrRecoveryMaterialParentMismatch
	}
	return nil
}

// RecoveryClaimOutcome is the machine-readable result of ClaimRecovery.
type RecoveryClaimOutcome string

const (
	// RecoveryClaimGranted: recovery lease installed (generation+1, rotated
	// token); the claimer owns the next verification/replay/finalize step —
	// or, for a pre-gate claim (no intent), a fresh execution of the run.
	RecoveryClaimGranted RecoveryClaimOutcome = "granted"
	// RecoveryClaimBusyLiveHolder: the run's lease is still live — recovery
	// must not touch the run (a live execution or another recovery owns it).
	// Zero writes.
	RecoveryClaimBusyLiveHolder RecoveryClaimOutcome = "busy_live_holder"
	// RecoveryClaimBindingStale: the current source/def binding no longer
	// matches the run's frozen fingerprint. Zero writes; the P25 newDef-only
	// reset guidance applies.
	RecoveryClaimBindingStale RecoveryClaimOutcome = "binding_stale"
	// RecoveryClaimNotEligible: the run is not a p26-v1 run, or is in a
	// shape recovery refuses (gate-entered without an intent — corruption —
	// or an unfinalized-but-released owner). Zero writes.
	RecoveryClaimNotEligible RecoveryClaimOutcome = "not_eligible"
	// RecoveryClaimNotVisible: no such run in this tenant.
	RecoveryClaimNotVisible RecoveryClaimOutcome = "not_visible"
	// RecoveryClaimAlreadyResolved: the run is already succeeded with a
	// finalized intent — the read-only idempotent answer, recognized BEFORE
	// any current CP/owner/binding eligibility (review-w2-round1 item 6).
	// No claim is installed; W3 answers from the intent/receipt facts.
	RecoveryClaimAlreadyResolved RecoveryClaimOutcome = "already_resolved"
)

// RecoveryWaitReason enumerates why recovery deferred (recorded durably).
type RecoveryWaitReason string

const (
	// RecoveryWaitAuthRequired: no fresh authorization available for the
	// remote verification/replay. Records the wait WITHOUT consuming any
	// attempt budget (root-decisions-v2 item 5).
	RecoveryWaitAuthRequired RecoveryWaitReason = "auth_required"
	// RecoveryWaitVerifyUnavailable: the DVS verification attempt failed as a
	// dependency error. Consumes one query attempt.
	RecoveryWaitVerifyUnavailable RecoveryWaitReason = "verify_unavailable"
	// RecoveryWaitReceiptOpen: the remote transaction is still OPEN — the
	// outcome is genuinely undecided. No budget consumption (time-based
	// re-check).
	RecoveryWaitReceiptOpen RecoveryWaitReason = "receipt_open"
	// RecoveryWaitSendDeferred: a same-byte replay attempt failed as a
	// dependency error. Consumes one send attempt.
	RecoveryWaitSendDeferred RecoveryWaitReason = "send_deferred"
)

// validRecoveryWaitReason bounds the durable reason strings.
func validRecoveryWaitReason(reason RecoveryWaitReason) bool {
	switch reason {
	case RecoveryWaitAuthRequired, RecoveryWaitVerifyUnavailable,
		RecoveryWaitReceiptOpen, RecoveryWaitSendDeferred:
		return true
	}
	return false
}

// RecoveryClaimResult reports the claim decision. Claim is set only for
// Granted (a full LeaseClaim — the fencing material of the recovery lease;
// Generation is the row's own counter, deliberately not duplicated).
// Intent/IntentExists distinguish the post-gate shape (intent present: only
// verification or same-byte replay allowed) from the pre-gate shape (no
// intent: a fresh execution may re-pull the source on the frozen baseline).
type RecoveryClaimResult struct {
	Outcome         RecoveryClaimOutcome
	Claim           *LeaseClaim
	Intent          *PublishIntentMaterial
	IntentExists    bool
	CancelRequested bool
}

// validateRecoveryFencing rejects blank fencing material before anything is
// locked or written.
func validateRecoveryFencing(holder, token string, generation int) error {
	if strings.TrimSpace(holder) == "" {
		return errors.New("recovery lease holder must not be blank")
	}
	if strings.TrimSpace(token) == "" {
		return errors.New("recovery lease token must not be blank")
	}
	if generation < 1 {
		return fmt.Errorf("recovery lease generation %d is never valid: claims start at 1", generation)
	}
	return nil
}

// errRecoveryGateAlreadyEntered marks the conflict branch of the intent
// INSERT: an intent already exists, so the run cannot enter the gate a second
// time. Zero writes (the whole transaction rolls back).
var errRecoveryGateAlreadyEntered = errors.New("publish intent already exists for this run")

// errRecoveryGateRefused carries a typed gate outcome out of the lock-chain
// callback AS AN ERROR so the chain helper ROLLS THE TRANSACTION BACK. Root
// review fix (2026-11-15): every non-entered branch of a recoverable gate —
// cancelled, fenced (wrong token/generation, expired, terminal), binding
// stale, owner lost — must leave NOTHING behind; returning nil from the
// callback would COMMIT the already-executed intent INSERT while
// publish_entered_at stays NULL, permanently breaking the gate⟺intent IFF
// (ClaimRecovery then refuses the run as corrupted). Only Entered commits.
type errRecoveryGateRefused struct{ result *IncrementalPublishGateResult }

func (e *errRecoveryGateRefused) Error() string {
	return "recovery gate refused entry: transaction rolled back, no intent persisted"
}

// refused returns the sentinel for a typed refusal outcome.
func refused(outcome IncrementalPublishGateOutcome) error {
	return &errRecoveryGateRefused{result: &IncrementalPublishGateResult{Outcome: outcome}}
}

// refusedRun returns the sentinel for a typed refusal outcome carrying the
// run row (the cancelled case).
func refusedRun(outcome IncrementalPublishGateOutcome, run *models.SyncRun) error {
	return &errRecoveryGateRefused{result: &IncrementalPublishGateResult{Outcome: outcome, Run: run}}
}

// mapRecoveryGateErr translates the chain-helper error of a recoverable gate
// into its typed public answer: sentinel refusals (rolled back, zero writes)
// and already-entered / not-visible fences become outcomes; real errors stay
// errors.
func mapRecoveryGateErr(err error) (*IncrementalPublishGateResult, error, bool) {
	var refusal *errRecoveryGateRefused
	if errors.As(err, &refusal) {
		return refusal.result, nil, true
	}
	if errors.Is(err, errRecoveryGateAlreadyEntered) {
		return &IncrementalPublishGateResult{Outcome: IncrementalPublishGateFenced}, nil, true
	}
	if errors.Is(err, errCPNotVisible) {
		return &IncrementalPublishGateResult{Outcome: IncrementalPublishGateFenced}, nil, true
	}
	return nil, err, false
}

// ---------------------------------------------------------------------------
// Snapshot recovery binding (admission freeze + authoritative recompute)
// ---------------------------------------------------------------------------

// readSnapshotRecoveryBinding recomputes a def's SNAPSHOT recovery binding
// from the CURRENT def/source/credential rows through the given querier.
// Callers hold the Source+Def strong locks (admission) or the recovery chain
// locks, so the reads are race-free against the protected writers. It
// performs the full REST parse — only reached for sources the lightweight
// probe already marked recovery-enabled, so legacy shapes never get here.
func readSnapshotRecoveryBinding(ctx context.Context, q cpQuerier, syncID, sourceID uuid.UUID, t Tenant) (models.SnapshotRecoveryBinding, string, error) {
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
		return models.SnapshotRecoveryBinding{}, "", errNoIncrementalDef
	}
	if err != nil {
		return models.SnapshotRecoveryBinding{}, "", err
	}
	connPredicate, connArg := connectionTenantPredicate(t, 2)
	var rawConfig []byte
	err = q.QueryRow(ctx,
		`SELECT c.config FROM connections c WHERE c.id = $1 AND `+connPredicate,
		sourceID, connArg).Scan(&rawConfig)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.SnapshotRecoveryBinding{}, "", errNoIncrementalDef
	}
	if err != nil {
		return models.SnapshotRecoveryBinding{}, "", err
	}
	source, err := models.ParseRESTAPISourceConfig(rawConfig)
	if err != nil {
		return models.SnapshotRecoveryBinding{}, "", fmt.Errorf("snapshot recovery binding: source config unreadable: %w", err)
	}
	if source.Sync == nil {
		return models.SnapshotRecoveryBinding{}, "", errors.New("snapshot recovery binding: source has no sync configuration")
	}
	kinds := referencedCredentialKinds(source.Auth)
	creds := []models.CredFingerprint{}
	if len(kinds) > 0 {
		rows, err := q.Query(ctx,
			`SELECT sc.kind, sc.fingerprint FROM source_credentials sc
			  WHERE sc.source_id = $1 AND sc.kind = ANY($2)`, sourceID, kinds)
		if err != nil {
			return models.SnapshotRecoveryBinding{}, "", err
		}
		defer rows.Close()
		for rows.Next() {
			var cf models.CredFingerprint
			if err := rows.Scan(&cf.Kind, &cf.Fingerprint); err != nil {
				return models.SnapshotRecoveryBinding{}, "", err
			}
			creds = append(creds, cf)
		}
		if err := rows.Err(); err != nil {
			return models.SnapshotRecoveryBinding{}, "", err
		}
	}
	binding := models.SnapshotRecoveryBinding{
		SourceID:        sourceID,
		OutputDatasetID: outputDatasetID,
		FileGlob:        fileGlob,
		BaseURL:         source.BaseURL,
		ResourcePath:    source.Sync.ResourcePath,
		RecordsPath:     source.Sync.RecordsPath,
		PaginationType:  source.Sync.Pagination.Type,
		PageParam:       source.Sync.Pagination.PageParam,
		PageSizeParam:   source.Sync.Pagination.PageSizeParam,
		PageSize:        source.Sync.Pagination.PageSize,
		StartPage:       source.Sync.Pagination.StartPage,
		CursorParam:     source.Sync.Pagination.CursorParam,
		NextCursorPath:  source.Sync.Pagination.NextCursorPath,
		QueryParams:     source.Sync.QueryParams,
		Headers:         source.Sync.Headers,
		Auth:            source.Auth,
		Credentials:     creds,
	}
	return binding, models.SnapshotRecoveryBindingFingerprint(binding), nil
}

// insertPendingSyncRunWithRecovery is the snapshot-v1 admission INSERT: the
// plain pending shape plus the frozen recovery identity triple. Shared by the
// manual (EnqueueSyncRun) and scheduled claim paths; no checkpoint row, no
// owner, no epoch — snapshot single-flight semantics are unchanged.
func insertPendingSyncRunWithRecovery(ctx context.Context, tx pgx.Tx, runID, syncID, sourceID uuid.UUID, t Tenant, requestedBy uuid.UUID, binding models.SnapshotRecoveryBinding, fingerprint string) (*models.SyncRun, error) {
	defPredicate, defArg := syncDefTenantPredicate(t, 4)
	row := tx.QueryRow(ctx,
		`INSERT INTO sync_runs AS r (id, sync_def_id, status, org_id, owner_id, requested_by, control_mode,
		     recovery_protocol, recovery_source_id, recovery_output_dataset_id, recovery_binding_fingerprint)
		 SELECT $1, d.id, 'pending', d.org_id, c.owner_id, $3, 'controlled', $6, $7, $8, $9
		   FROM batch_sync_defs d JOIN connections c ON c.id = d.source_id
		  WHERE d.id = $2 AND d.source_id = $5 AND `+defPredicate+`
		 RETURNING `+runPublicColumns,
		runID, syncID, requestedBy, defArg, sourceID,
		models.RunRecoveryProtocolP26V1, sourceID, binding.OutputDatasetID, fingerprint)
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
// Ordered lock chains
// ---------------------------------------------------------------------------

// withDefRunLock opens the SNAPSHOT v1 chain — Def(FOR UPDATE) ->
// Run(FOR UPDATE) — verifies the frozen baseline under the locks (including
// the recovery triple) and runs fn. No checkpoint row exists for snapshot
// runs, so the chain deliberately stops at Run; the intent row is fn's own
// last write under the held Run lock.
func (r *Repo) withDefRunLock(ctx context.Context, pre *runInternalRow, t Tenant, fn func(tx pgx.Tx) error) error {
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
	if err := verifyLockedRunBaseline(ctx, tx, pre, t); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// verifyLockedRunBaseline takes the run row FOR UPDATE by id + tenant and
// compares every frozen field (cp_* + recovery triple) against the trusted
// pre-read. Shared by withDefRunLock and withRecoveryChainLock — one shape,
// no drift.
func verifyLockedRunBaseline(ctx context.Context, tx pgx.Tx, pre *runInternalRow, t Tenant) error {
	runPredicate, runArg := syncRunTenantPredicate(t, 2)
	var (
		lockedRun, lockedDefOfRun uuid.UUID
		orgID                     *uuid.UUID
		cpMode                    string
		cpBaseEpoch               int
		cpBaseValue               *string
		cpFingerprint             string
		cpOutput                  uuid.UUID
		recoveryProtocol          string
		recSource, recOutput      uuid.UUID
		recFingerprint            string
	)
	err := tx.QueryRow(ctx,
		`SELECT r.id, r.sync_def_id, r.org_id,
		        COALESCE(r.cp_mode, ''), COALESCE(r.cp_base_epoch, 0),
		        r.cp_base_value, COALESCE(r.cp_binding_fingerprint, ''),
		        COALESCE(r.cp_output_dataset_id, '00000000-0000-0000-0000-000000000000'::uuid),
		        COALESCE(r.recovery_protocol, ''),
		        COALESCE(r.recovery_source_id, '00000000-0000-0000-0000-000000000000'::uuid),
		        COALESCE(r.recovery_output_dataset_id, '00000000-0000-0000-0000-000000000000'::uuid),
		        COALESCE(r.recovery_binding_fingerprint, '')
		   FROM sync_runs r WHERE r.id = $1 AND `+runPredicate+` FOR UPDATE`,
		pre.RunID, runArg).Scan(&lockedRun, &lockedDefOfRun, &orgID, &cpMode, &cpBaseEpoch,
		&cpBaseValue, &cpFingerprint, &cpOutput, &recoveryProtocol,
		&recSource, &recOutput, &recFingerprint)
	if errors.Is(err, pgx.ErrNoRows) {
		return errCPNotVisible
	}
	if err != nil {
		return err
	}
	if lockedDefOfRun != pre.SyncDefID {
		return errCPNotVisible
	}
	lockedBaseValue := ""
	if cpBaseValue != nil {
		lockedBaseValue = *cpBaseValue
	}
	if lockedRun != pre.RunID || (orgID == nil) != (pre.OrgID == nil) ||
		(orgID != nil && pre.OrgID != nil && *orgID != *pre.OrgID) ||
		cpMode != pre.CPMode || cpBaseEpoch != pre.CPBaseEpoch || lockedBaseValue != pre.CPBaseValue ||
		cpFingerprint != pre.CPBindingFingerprint || cpOutput != pre.CPOutputDatasetID ||
		recoveryProtocol != pre.RecoveryProtocol ||
		recSource != pre.RecoverySourceID || recOutput != pre.RecoveryOutputDatasetID ||
		recFingerprint != pre.RecoveryBindingFingerprint {
		return errCapturedBaselineMismatch
	}
	return nil
}

// withRecoveryChainLock opens the ordered Source -> Def -> (CP, incremental
// only) -> Run chain and runs fn with everything locked, handing it the
// trusted pre-read and (for incremental runs) the locked checkpoint row. The
// run's frozen baseline is verified under the locks; a mismatch fences
// everything with zero writes.
func (r *Repo) withRecoveryChainLock(ctx context.Context, pre *runInternalRow, t Tenant, fn func(tx pgx.Tx, cp *checkpointRow) error) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackCheckpointTx(tx)
	// 1. Source (id + tenant) FOR UPDATE.
	if err := lockSourceRowForUpdate(ctx, tx, pre.SourceID, t); err != nil {
		return err
	}
	// 2. Def (bound to this source, tenant-checked) FOR UPDATE.
	defPredicate, defArg := syncDefTenantPredicate(t, 3)
	var lockedDef uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT d.id FROM batch_sync_defs d
		  WHERE d.id = $1 AND d.source_id = $2 AND `+defPredicate+` FOR UPDATE`,
		pre.SyncDefID, pre.SourceID, defArg).Scan(&lockedDef)
	if errors.Is(err, pgx.ErrNoRows) {
		return errCPNotVisible
	}
	if err != nil {
		return err
	}
	// 3. Checkpoint FOR UPDATE — ONLY for incremental runs (a snapshot run
	// has no checkpoint row by construction).
	var cp *checkpointRow
	if isIncrementalRun(pre) {
		cp, err = lockCheckpointForUpdate(ctx, tx, pre.SyncDefID)
		if err != nil {
			return err
		}
	}
	// 4. Run FOR UPDATE + frozen-baseline verification.
	if err := verifyLockedRunBaseline(ctx, tx, pre, t); err != nil {
		return err
	}
	if err := fn(tx, cp); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ---------------------------------------------------------------------------
// Recoverable publish gate (generic, routes on the frozen mode)
// ---------------------------------------------------------------------------

// BeginRecoverablePublishGate is the GENERIC P2.6 gate: it routes on the
// run's frozen mode — incremental runs take the Def→CP→Run chain, snapshot
// runs the Def→Run chain — and installs the durable publish intent in the
// SAME transaction that stamps publish_entered_at. The material must satisfy
// the mode's candidate pairing (incremental: non-nil; snapshot: nil); the
// input lease must be the live execution claim and is renewed for a full TTL
// on entry. See BeginRecoverableIncrementalPublishGate for the incremental
// detail; the snapshot branch verifies the CURRENT snapshot binding against
// the run's frozen recovery_binding_fingerprint before the gate CAS.
func (r *Repo) BeginRecoverablePublishGate(ctx context.Context, lease *LeaseClaim, t Tenant, mat FrozenPublishMaterial) (*IncrementalPublishGateResult, error) {
	if lease == nil {
		return nil, errors.New("BeginRecoverablePublishGate requires a live lease claim")
	}
	pre, err := r.runInternalPreRead(ctx, lease.Run.ID, t)
	if err != nil {
		return nil, err
	}
	if pre == nil {
		return nil, errors.New("BeginRecoverablePublishGate on an invisible run")
	}
	if pre.RecoveryProtocol != models.RunRecoveryProtocolP26V1 {
		return nil, errors.New("BeginRecoverablePublishGate on a run not frozen for the p26-v1 protocol")
	}
	if isIncrementalRun(pre) {
		return r.BeginRecoverableIncrementalPublishGate(ctx, lease, t, mat)
	}
	// Snapshot branch.
	runID := lease.Run.ID
	mat.CPMode = pre.CPMode // "" — validated pairing: candidate must be nil
	if err := validateFrozenMaterialParents(pre.SourceID, pre.SyncDefID, mat); err != nil {
		return nil, err
	}
	if err := ValidateFrozenPublishMaterial(runID, mat); err != nil {
		return nil, err
	}
	if err := validateLeaseCAS(lease.Holder, lease.Token, lease.Run.Generation, minLeaseTTL); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, checkpointOpBudget)
	defer cancel()
	var out *IncrementalPublishGateResult
	err = r.withDefRunLock(ctx, pre, t, func(tx pgx.Tx) error {
		// Authoritative binding recompute under the Def lock: drift refuses
		// the gate — sentinel error ⇒ rollback, zero writes.
		binding, fingerprint, err := readSnapshotRecoveryBinding(ctx, tx, pre.SyncDefID, pre.SourceID, t)
		if err != nil {
			return err
		}
		if fingerprint != pre.RecoveryBindingFingerprint ||
			binding.OutputDatasetID != pre.RecoveryOutputDatasetID ||
			binding.SourceID != pre.RecoverySourceID {
			return refused(IncrementalPublishGateBindingStale)
		}
		// Intent first, gate CAS second — and only an ENTERED CAS may commit:
		// every later refusal returns an error so the whole transaction
		// (intent included) rolls back (IFF invariant).
		intentInsert, ierr := tx.Exec(ctx,
			`INSERT INTO sync_run_publish_intents
			    (run_id, sync_def_id, source_id, org_id, recovery_protocol, publish_key,
			     request_fingerprint, publish_body, output_dataset_id, branch, logical_path,
			     content_hash, rows_written, bytes_written, completed_checkpoint,
			     cp_mode, cp_base_epoch, cp_base_value, cp_binding_fingerprint, recovery_state)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NULL,
			         '', 0, NULL, $15, 'armed')
			 ON CONFLICT (run_id) DO NOTHING`,
			runID, pre.SyncDefID, pre.SourceID, pre.OrgID, pre.RecoveryProtocol,
			DerivePublishKey(runID), RequestFingerprintOf(mat.ExactBody), mat.ExactBody,
			pre.RecoveryOutputDatasetID, mat.Branch, mat.LogicalPath,
			mat.ContentHash, mat.Rows, mat.Bytes, pre.RecoveryBindingFingerprint)
		if ierr != nil {
			return ierr
		}
		if intentInsert.RowsAffected() != 1 {
			return errRecoveryGateAlreadyEntered
		}
		// Gate CAS (BeginPublishGate statement verbatim, full TTL renewal).
		claim, gerr := scanLeasedRun(tx.QueryRow(ctx,
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
			runID, lease.Holder, lease.Token, lease.Run.Generation, RecoveryGateLeaseTTL.Seconds()))
		if gerr == nil {
			out = &IncrementalPublishGateResult{Outcome: IncrementalPublishGateEntered, Claim: claim}
			return nil // the ONLY committing path
		}
		if !errors.Is(gerr, pgx.ErrNoRows) {
			return gerr
		}
		var (
			cancelRequestedAt *time.Time
			materialOK        bool
		)
		derr := tx.QueryRow(ctx,
			`SELECT r.cancel_requested_at,
			       COALESCE(r.status = 'running' AND r.control_mode = 'controlled'
			         AND r.lease_holder = $2 AND r.lease_token = $3 AND r.generation = $4
			         AND clock_timestamp() < r.lease_expires_at
			         AND r.publish_entered_at IS NULL, false) AS material_ok
			  FROM sync_runs r WHERE r.id = $1`,
			runID, lease.Holder, lease.Token, lease.Run.Generation).Scan(&cancelRequestedAt, &materialOK)
		if errors.Is(derr, pgx.ErrNoRows) {
			return refused(IncrementalPublishGateFenced)
		}
		if derr != nil {
			return derr
		}
		if !materialOK || cancelRequestedAt == nil {
			return refused(IncrementalPublishGateFenced)
		}
		run, rerr := lockedRunByID(ctx, tx, runID)
		if rerr != nil {
			return rerr
		}
		return refusedRun(IncrementalPublishGateCancelled, run)
	})
	if mapped, merr, ok := mapRecoveryGateErr(err); ok {
		return mapped, merr
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BeginRecoverableIncrementalPublishGate is the incremental-mode variant of
// BeginRecoverablePublishGate (kept as the first slice's explicit entry).
func (r *Repo) BeginRecoverableIncrementalPublishGate(ctx context.Context, lease *LeaseClaim, t Tenant, mat FrozenPublishMaterial) (*IncrementalPublishGateResult, error) {
	if lease == nil {
		return nil, errors.New("BeginRecoverableIncrementalPublishGate requires a live lease claim")
	}
	runID := lease.Run.ID
	pre, err := r.runInternalPreRead(ctx, runID, t)
	if err != nil {
		return nil, err
	}
	if !isIncrementalRun(pre) {
		return nil, errors.New("BeginRecoverableIncrementalPublishGate on a non-incremental run")
	}
	if pre.RecoveryProtocol != models.RunRecoveryProtocolP26V1 {
		return nil, errors.New("BeginRecoverableIncrementalPublishGate on a run not frozen for the p26-v1 protocol")
	}
	mat.CPMode = pre.CPMode
	if err := validateFrozenMaterialParents(pre.SourceID, pre.SyncDefID, mat); err != nil {
		return nil, err
	}
	if err := ValidateFrozenPublishMaterial(runID, mat); err != nil {
		return nil, err
	}
	if err := validateLeaseCAS(lease.Holder, lease.Token, lease.Run.Generation, minLeaseTTL); err != nil {
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
		if binding.OutputDatasetID != pre.CPOutputDatasetID ||
			fingerprint != pre.CPBindingFingerprint ||
			cp.BindingFingerprint != fingerprint {
			return refused(IncrementalPublishGateBindingStale) // sentinel ⇒ rollback
		}
		if cp.ActiveRunID == nil || *cp.ActiveRunID != runID || cp.ConfirmedEpoch != pre.CPBaseEpoch {
			// Should be impossible under single-flight; treat as uncertain.
			return refused(IncrementalPublishGateOwnerLost)
		}
		// Intent first, gate CAS second — and only an ENTERED CAS may commit:
		// every later refusal returns an error so the whole transaction
		// (intent included) rolls back (IFF invariant).
		intentInsert, ierr := tx.Exec(ctx,
			`INSERT INTO sync_run_publish_intents
			    (run_id, sync_def_id, source_id, org_id, recovery_protocol, publish_key,
			     request_fingerprint, publish_body, output_dataset_id, branch, logical_path,
			     content_hash, rows_written, bytes_written, completed_checkpoint,
			     cp_mode, cp_base_epoch, cp_base_value, cp_binding_fingerprint, recovery_state)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
			         $16, $17, $18, $19, 'armed')
			 ON CONFLICT (run_id) DO NOTHING`,
			runID, pre.SyncDefID, pre.SourceID, pre.OrgID, pre.RecoveryProtocol,
			DerivePublishKey(runID), RequestFingerprintOf(mat.ExactBody), mat.ExactBody,
			pre.CPOutputDatasetID, mat.Branch, mat.LogicalPath,
			mat.ContentHash, mat.Rows, mat.Bytes, derefString(mat.CompletedCheckpoint),
			pre.CPMode, pre.CPBaseEpoch, nullableCPBaseValue(pre.CPBaseValue), pre.CPBindingFingerprint)
		if ierr != nil {
			return ierr
		}
		if intentInsert.RowsAffected() != 1 {
			return errRecoveryGateAlreadyEntered
		}
		// Gate CAS (BeginIncrementalPublishGate statement verbatim, full
		// TTL renewal).
		claim, gerr := scanLeasedRun(tx.QueryRow(ctx,
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
			runID, lease.Holder, lease.Token, lease.Run.Generation, RecoveryGateLeaseTTL.Seconds()))
		if gerr == nil {
			out = &IncrementalPublishGateResult{Outcome: IncrementalPublishGateEntered, Claim: claim}
			return nil // the ONLY committing path
		}
		if !errors.Is(gerr, pgx.ErrNoRows) {
			return gerr
		}
		var (
			cancelRequestedAt *time.Time
			materialOK        bool
		)
		derr := tx.QueryRow(ctx,
			`SELECT r.cancel_requested_at,
			       COALESCE(r.status = 'running' AND r.control_mode = 'controlled'
			         AND r.lease_holder = $2 AND r.lease_token = $3 AND r.generation = $4
			         AND clock_timestamp() < r.lease_expires_at
			         AND r.publish_entered_at IS NULL, false) AS material_ok
			  FROM sync_runs r WHERE r.id = $1`,
			runID, lease.Holder, lease.Token, lease.Run.Generation).Scan(&cancelRequestedAt, &materialOK)
		if errors.Is(derr, pgx.ErrNoRows) {
			return refused(IncrementalPublishGateFenced)
		}
		if derr != nil {
			return derr
		}
		if !materialOK || cancelRequestedAt == nil {
			return refused(IncrementalPublishGateFenced)
		}
		run, rerr := lockedRunByID(ctx, tx, runID)
		if rerr != nil {
			return rerr
		}
		return refusedRun(IncrementalPublishGateCancelled, run)
	})
	if mapped, merr, ok := mapRecoveryGateErr(err); ok {
		return mapped, merr
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// derefString maps a nil candidate to SQL NULL (snapshot) — used only by the
// incremental insert where the validated pairing guarantees non-nil.
func derefString(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

// nullableCPBaseValue maps the P25 absence rule ("" means the input token was
// absent) back to SQL NULL for the mirrored baseline column.
func nullableCPBaseValue(v string) any {
	if v == "" {
		return nil
	}
	return v
}

// ---------------------------------------------------------------------------
// Snapshot v1 terminal fence (CompleteLeasedSyncRun routing target)
// ---------------------------------------------------------------------------

// CompleteRecoverableSnapshotRun is the terminal-write router target for a
// SNAPSHOT p26-v1 run: success is refused outright (the recovery finalize
// owns it), and failed/aborted are only provable BEFORE the publish gate —
// a post-gate close would fabricate an outcome for a dispatched publish, so
// it is refused with ErrRecoveryPostGateLocked and the run stays `running`.
// Pre-gate closes use the CompleteLeasedSyncRun CAS verbatim (no checkpoint
// side effects exist for snapshot runs).
func (r *Repo) CompleteRecoverableSnapshotRun(ctx context.Context, runID uuid.UUID, t Tenant, holder, token string, generation int, status string, bytesWritten, filesWritten, rowsWritten int64, errMsg *string, ingestJobID *string, datasetVersionID *uuid.UUID, contentHash *string) (*models.SyncRun, error) {
	switch status {
	case "failed", "aborted":
	case "succeeded":
		return nil, ErrRecoveryRequiresFinalize
	default:
		return nil, errors.New("invalid terminal status for snapshot recovery completion: " + status)
	}
	if strings.TrimSpace(holder) == "" || strings.TrimSpace(token) == "" || generation < 1 {
		return nil, errors.New("invalid lease fencing material for snapshot recovery completion")
	}
	pre, err := r.runInternalPreRead(ctx, runID, t)
	if err != nil {
		return nil, err
	}
	if pre == nil || pre.RecoveryProtocol != models.RunRecoveryProtocolP26V1 || isIncrementalRun(pre) {
		return nil, errors.New("CompleteRecoverableSnapshotRun on a run that is not a snapshot p26-v1 run")
	}
	ctx, cancel := context.WithTimeout(ctx, checkpointOpBudget)
	defer cancel()
	var out *models.SyncRun
	err = r.withDefRunLock(ctx, pre, t, func(tx pgx.Tx) error {
		var gate *time.Time
		if gerr := tx.QueryRow(ctx,
			`SELECT r.publish_entered_at FROM sync_runs r WHERE r.id = $1`, runID).Scan(&gate); gerr != nil {
			return gerr
		}
		if gate != nil {
			return ErrRecoveryPostGateLocked
		}
		// Pre-gate close: CompleteLeasedSyncRun UPDATE predicates verbatim.
		run, uerr := scanLeasedRun(tx.QueryRow(ctx,
			`UPDATE sync_runs r
			    SET status = $5,
			        finished_at = clock_timestamp(),
			        bytes_written = $6,
			        files_written = $7,
			        rows_written = $8,
			        error = $9,
			        ingest_job_id = $10,
			        dataset_version_id = $11,
			        content_hash = $12,
			        heartbeat_at = clock_timestamp()
			  WHERE r.id = $1 AND r.status = 'running'
			    AND r.lease_holder = $2 AND r.lease_token = $3 AND r.generation = $4
			    AND clock_timestamp() < r.lease_expires_at
			    AND (r.control_mode = 'legacy'
			      OR ($5 = 'succeeded' AND r.publish_entered_at IS NOT NULL AND r.cancel_requested_at IS NULL)
			      OR ($5 = 'aborted' AND r.publish_entered_at IS NULL)
			      OR ($5 = 'failed'))
			  RETURNING `+leasedRunColumns,
			runID, holder, token, generation, status, bytesWritten, filesWritten,
			rowsWritten, errMsg, ingestJobID, datasetVersionID, contentHash))
		if errors.Is(uerr, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if uerr != nil {
			return uerr
		}
		out = &run.Run
		return nil
	})
	if errors.Is(err, errCPNotVisible) {
		return nil, ErrLeaseLost
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Recovery claim (generic: pre-gate pending, pre-gate expired, post-gate)
// ---------------------------------------------------------------------------

// ClaimRecovery installs a recovery lease on a p26-v1 run (both modes). It
// performs the ordered Source->Def->(CP, incremental only)->Run locking and
// covers three shapes (root-decisions-v2.1):
//
//   - PENDING, never claimed: the admission's coordinator ticket died at
//     restart. Granted WITHOUT an intent — the claimer may freshly EXECUTE
//     the run (re-pull allowed: provably nothing was dispatched). Fresh auth
//     is the entry; this is the v1 replacement for a bare takeover.
//   - RUNNING with an EXPIRED lease and NO intent (crashed before the gate):
//     granted for a fresh execution on the frozen baseline, same as above.
//   - RUNNING with an EXPIRED lease and an intent (crashed after the gate):
//     granted for verification / same-byte replay / finalize ONLY — the
//     source may never be pulled again for this run.
//
// A live lease answers busy_live_holder; binding drift answers binding_stale;
// everything else writes exactly nothing. The run's status is never touched
// (an unknown outcome stays `running`).
func (r *Repo) ClaimRecovery(ctx context.Context, runID uuid.UUID, t Tenant, holder, token string, ttl time.Duration) (*RecoveryClaimResult, error) {
	if err := validateLeaseInstall(holder, token, ttl); err != nil {
		return nil, err
	}
	pre, err := r.runInternalPreRead(ctx, runID, t)
	if err != nil {
		return nil, err
	}
	if pre == nil {
		return &RecoveryClaimResult{Outcome: RecoveryClaimNotVisible}, nil
	}
	if pre.RecoveryProtocol != models.RunRecoveryProtocolP26V1 {
		return &RecoveryClaimResult{Outcome: RecoveryClaimNotEligible}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, checkpointOpBudget)
	defer cancel()
	var out *RecoveryClaimResult
	err = r.withRecoveryChainLock(ctx, pre, t, func(tx pgx.Tx, cp *checkpointRow) error {
		res, derr := recoveryClaimDecision(ctx, tx, pre, cp, t, holder, token, ttl)
		if derr != nil {
			return derr
		}
		out = res
		return nil
	})
	if errors.Is(err, errRunNotVisible) || errors.Is(err, errCPNotVisible) {
		return &RecoveryClaimResult{Outcome: RecoveryClaimNotVisible}, nil
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// recoveryClaimDecision is the SHARED locked core of the manual and
// automatic recovery claims (auto-recovery-seam: the automatic wrapper must
// not verify the grant separately and then call the public entry — the same
// transaction decides). Everything runs under the held Source→Def→(CP,
// incremental only)→Run chain; zero writes on every non-granted outcome.
func recoveryClaimDecision(ctx context.Context, tx pgx.Tx, pre *runInternalRow, cp *checkpointRow, t Tenant, holder, token string, ttl time.Duration) (*RecoveryClaimResult, error) {
	runID := pre.RunID
	res := &RecoveryClaimResult{IntentExists: false}
	intent, err := publishIntentByID(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	if intent != nil {
		res.Intent = intent
		res.IntentExists = true
	}
	status, gateEntered, cancelRequested, err := runRecoveryShape(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	res.CancelRequested = cancelRequested
	// The IFF invariant: an intent exists iff the gate was entered. A
	// violation is corruption — refuse with zero writes.
	if intent != nil != gateEntered {
		res.Outcome = RecoveryClaimNotEligible
		return res, nil
	}
	// Root review-w2-round1 item 6: a SUCCEEDED run with a FINALIZED
	// intent is ALREADY RESOLVED — recognized BEFORE any current
	// CP/owner/binding eligibility, so a later batch that advanced the
	// checkpoint, took a new owner or drifted the binding can never
	// block the idempotent answer for the old run. Read-only: no lease,
	// attempt or generation is ever touched on a terminal row.
	if status == "succeeded" && intent != nil && intent.RecoveryState == IntentStateFinalized {
		res.Outcome = RecoveryClaimAlreadyResolved
		return res, nil
	}
	// Every other terminal shape (failed/aborted, with or without an
	// intent, succeeded-but-unfinalized) is out of scope for a claim:
	// the pre-v2.1 terminal-adoption logic is deliberately REMOVED — a
	// recovery lease on a terminal row was unauthorized.
	switch status {
	case "succeeded", "failed", "aborted":
		res.Outcome = RecoveryClaimNotEligible
		return res, nil
	}
	// Owner/baseline guards for the incremental mode (a snapshot run has
	// no checkpoint owner; its identity lives in the frozen triple).
	if isIncrementalRun(pre) {
		if cp == nil || cp.ActiveRunID == nil || *cp.ActiveRunID != runID ||
			cp.ConfirmedEpoch != pre.CPBaseEpoch || cp.BindingFingerprint != pre.CPBindingFingerprint {
			res.Outcome = RecoveryClaimNotEligible
			return res, nil
		}
		if intent != nil && (intent.CPBaseEpoch != pre.CPBaseEpoch ||
			intent.CPBindingFingerprint != pre.CPBindingFingerprint) {
			res.Outcome = RecoveryClaimNotEligible
			return res, nil
		}
	}
	// Current binding must still match the run's frozen fingerprint
	// (mode-routed recompute).
	if isIncrementalRun(pre) {
		binding, fingerprint, err := readIncrementalBinding(ctx, tx, pre.SyncDefID, pre.SourceID, t)
		if err != nil {
			return nil, err
		}
		if fingerprint != pre.CPBindingFingerprint || binding.OutputDatasetID != pre.CPOutputDatasetID {
			res.Outcome = RecoveryClaimBindingStale
			return res, nil
		}
	} else {
		binding, fingerprint, err := readSnapshotRecoveryBinding(ctx, tx, pre.SyncDefID, pre.SourceID, t)
		if err != nil {
			return nil, err
		}
		if fingerprint != pre.RecoveryBindingFingerprint ||
			binding.OutputDatasetID != pre.RecoveryOutputDatasetID ||
			binding.SourceID != pre.RecoverySourceID {
			res.Outcome = RecoveryClaimBindingStale
			return res, nil
		}
	}
	switch {
	case status == "pending" && intent == nil:
		// Fresh-execution shape (durable admission, dead ticket).
		claim, cerr := claimLockedPendingRun(ctx, tx, runID, holder, token, ttl)
		if cerr != nil {
			return nil, cerr
		}
		if claim == nil {
			// Lost the pending CAS race (another ordinary/recovery claim
			// won): report the row's live shape — never an unset result.
			live, lerr := runLeaseLive(ctx, tx, runID)
			if lerr != nil {
				return nil, lerr
			}
			if live {
				res.Outcome = RecoveryClaimBusyLiveHolder
			} else {
				res.Outcome = RecoveryClaimNotEligible
			}
			return res, nil
		}
		res.Outcome = RecoveryClaimGranted
		res.Claim = claim
		return res, nil
	case status == "running":
		// Live lease is untouchable (execution or another recovery).
		live, lerr := runLeaseLive(ctx, tx, runID)
		if lerr != nil {
			return nil, lerr
		}
		if live {
			res.Outcome = RecoveryClaimBusyLiveHolder
			return res, nil
		}
		claim, cerr := claimLockedRunningRun(ctx, tx, runID, holder, token, ttl)
		if cerr != nil {
			return nil, cerr
		}
		if claim == nil {
			// A renewal landed between the liveness check and the CAS:
			// the current holder is live, report busy — never unset.
			res.Outcome = RecoveryClaimBusyLiveHolder
			return res, nil
		}
		res.Outcome = RecoveryClaimGranted
		res.Claim = claim
		return res, nil
	}
	return nil, fmt.Errorf("unexpected recovery claim shape: status %q", status)
}

// runRecoveryShape reads the run's status and gate/cancel markers under the
// held chain locks.
func runRecoveryShape(ctx context.Context, tx pgx.Tx, runID uuid.UUID) (status string, gateEntered, cancelRequested bool, err error) {
	var gate, cancel *time.Time
	err = tx.QueryRow(ctx,
		`SELECT r.status, r.publish_entered_at, r.cancel_requested_at FROM sync_runs r WHERE r.id = $1`,
		runID).Scan(&status, &gate, &cancel)
	return status, gate != nil, cancel != nil, err
}

// claimLockedTerminalForVerify was REMOVED (root review-w2-round1 item 6):
// installing a recovery lease on a TERMINAL row was unauthorized — a
// succeeded+finalized run answers the read-only already_resolved outcome
// (recognized before any CP/owner/binding eligibility), and every other
// terminal shape is an explicit zero-write not_eligible.

// ---------------------------------------------------------------------------
// Fenced waits and budgets
// ---------------------------------------------------------------------------

// RecordRecoveryWait defers recovery with a durable reason and the next
// action time, fully fenced by the recovery lease. It consumes NO attempt
// budget — budget consumption is a separate, explicit decision
// (root-decisions-v2 item 5: missing fresh auth never spends an attempt).
// The wait is refused (typed, zero-write) on a finalized or budget-exhausted
// (sticky) intent: those states no longer take automated transitions.
func (r *Repo) RecordRecoveryWait(ctx context.Context, lease *LeaseClaim, t Tenant, reason RecoveryWaitReason, retryAfter time.Duration) error {
	if lease == nil {
		return errors.New("RecordRecoveryWait requires a live recovery lease")
	}
	if !validRecoveryWaitReason(reason) {
		return errors.New("invalid recovery wait reason")
	}
	if retryAfter <= 0 {
		retryAfter = time.Second
	}
	pre, err := r.runInternalPreRead(ctx, lease.Run.ID, t)
	if err != nil {
		return err
	}
	if pre == nil || pre.RecoveryProtocol != models.RunRecoveryProtocolP26V1 {
		return errors.New("RecordRecoveryWait on a non-p26-v1 run")
	}
	ctx, cancel := context.WithTimeout(ctx, checkpointOpBudget)
	defer cancel()
	lockChain := r.withDefCheckpointRunLock
	if !isIncrementalRun(pre) {
		lockChain = r.withDefRunLock
	}
	return lockChain(ctx, pre, t, func(tx pgx.Tx) error {
		if err := assertRecoveryLeaseHeld(ctx, tx, lease); err != nil {
			return err
		}
		// INDEPENDENT budgets (review-w2-round1 item 7): a wait is refused
		// only by finalization. An exhausted marker stays visible as the
		// safe summary (the state only flips to 'waiting' from a live
		// armed/waiting state) while the OTHER budget's worker keeps
		// deferring; the authoritative truth is the counters.
		tag, err := tx.Exec(ctx,
			`UPDATE sync_run_publish_intents
			    SET recovery_state = CASE WHEN recovery_state IN ('armed','waiting')
			             THEN 'waiting' ELSE recovery_state END,
			        last_wait_reason = $2,
			        next_action_at = clock_timestamp() + make_interval(secs => $3::float8),
			        updated_at = clock_timestamp()
			  WHERE run_id = $1 AND recovery_state <> 'finalized'`,
			lease.Run.ID, string(reason), retryAfter.Seconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			intent, ierr := publishIntentByID(ctx, tx, lease.Run.ID)
			if ierr != nil {
				return ierr
			}
			if intent == nil {
				return ErrRecoveryIntentMissing
			}
			return ErrIntentNotMutable
		}
		return nil
	})
}

// BudgetResult reports a budget decision: the attempt number just consumed
// (0 when refused) and the remaining attempts after it (negative-never; the
// manual query path reports Remaining 0 past the automated cap).
type BudgetResult struct {
	Allowed   bool
	Attempt   int
	Remaining int
}

// consumeBudget is the fenced budget core. Automated paths are capped and
// flip the sticky exhausted state atomically in the SAME statement (the
// increment and the sticky transition can never diverge). The manual QUERY
// path increments the observable counter past the automated cap without
// touching the sticky state (root-decisions-v2.1: one explicit fresh request
// = at most one query, count stays observable; the send budget is NEVER
// bypassable — manual included — and finalize from an exhausted state stays
// possible for a genuinely committed receipt).
func (r *Repo) consumeBudget(ctx context.Context, lease *LeaseClaim, t Tenant, send bool, manual bool, reason RecoveryWaitReason) (BudgetResult, error) {
	if lease == nil {
		return BudgetResult{}, errors.New("budget consumption requires a live recovery lease")
	}
	pre, err := r.runInternalPreRead(ctx, lease.Run.ID, t)
	if err != nil {
		return BudgetResult{}, err
	}
	if pre == nil || pre.RecoveryProtocol != models.RunRecoveryProtocolP26V1 {
		return BudgetResult{}, errors.New("budget consumption on a non-p26-v1 run")
	}
	column := "query_attempts"
	maxAttempts := MaxRecoveryQueryAttempts
	exhaustedState := IntentStateQueryBudgetExhausted
	if send {
		column = "send_attempts"
		maxAttempts = MaxPublishSendAttempts
		exhaustedState = IntentStateSendBudgetExhausted
	}
	ctx, cancel := context.WithTimeout(ctx, checkpointOpBudget)
	defer cancel()
	lockChain := r.withDefCheckpointRunLock
	if !isIncrementalRun(pre) {
		lockChain = r.withDefRunLock
	}
	var result BudgetResult
	err = lockChain(ctx, pre, t, func(tx pgx.Tx) error {
		if err := assertRecoveryLeaseHeld(ctx, tx, lease); err != nil {
			return err
		}
		var consumed int
		var err error
		if manual && !send {
			// Manual query: observable increment, no cap, no state change.
			err = tx.QueryRow(ctx,
				`UPDATE sync_run_publish_intents
				    SET query_attempts = query_attempts + 1,
				        last_wait_reason = $2,
				        updated_at = clock_timestamp()
				  WHERE run_id = $1 AND recovery_state <> 'finalized'
				  RETURNING query_attempts`,
				lease.Run.ID, string(reason)).Scan(&consumed)
			if err == nil {
				result = BudgetResult{Allowed: true, Attempt: consumed, Remaining: 0}
			}
		} else {
			// Capped consumption — INDEPENDENT budgets (review-w2-round1
			// item 7): an operation is refused only by finalization or its
			// OWN counter reaching its cap, never because the OTHER budget
			// exhausted. The exhausted marker flips only from a live state
			// (armed/waiting), so the first exhausted summary stays visible
			// while the actual counters remain authoritative.
			err = tx.QueryRow(ctx,
				`UPDATE sync_run_publish_intents
				    SET `+column+` = `+column+` + 1,
				        recovery_state = CASE WHEN `+column+` + 1 >= $2
				             AND recovery_state IN ('armed','waiting')
				               THEN $3::text ELSE recovery_state END,
				        last_wait_reason = $4,
				        updated_at = clock_timestamp()
				  WHERE run_id = $1
				    AND recovery_state <> 'finalized'
				    AND `+column+` < $2
				  RETURNING `+column,
				lease.Run.ID, maxAttempts, exhaustedState, string(reason)).Scan(&consumed)
			if err == nil {
				result = BudgetResult{Allowed: true, Attempt: consumed, Remaining: maxAttempts - consumed}
			}
		}
		if errors.Is(err, pgx.ErrNoRows) {
			intent, ierr := publishIntentByID(ctx, tx, lease.Run.ID)
			if ierr != nil {
				return ierr
			}
			if intent == nil {
				return ErrRecoveryIntentMissing
			}
			return ErrIntentNotMutable
		}
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return BudgetResult{}, err
	}
	return result, nil
}

// ConsumePublishSendBudget spends one send attempt (first dispatch or a
// same-byte replay) before the bytes go out, fenced by the recovery/live
// lease. EVERY send consumes it — manual recover included — and the cap (3,
// restart-persistent) is absolute (root-decisions-v2.1). Refusal is the
// typed ErrIntentNotMutable with zero writes.
func (r *Repo) ConsumePublishSendBudget(ctx context.Context, lease *LeaseClaim, t Tenant) (BudgetResult, error) {
	return r.consumeBudget(ctx, lease, t, true, false, RecoveryWaitSendDeferred)
}

// ConsumeRecoveryQueryAttempt spends one AUTOMATED remote verification
// attempt; on exhaustion the intent flips to the sticky
// query_budget_exhausted state. Fenced and restart-persistent.
func (r *Repo) ConsumeRecoveryQueryAttempt(ctx context.Context, lease *LeaseClaim, t Tenant, reason RecoveryWaitReason) (BudgetResult, error) {
	if !validRecoveryWaitReason(reason) || reason == RecoveryWaitAuthRequired {
		return BudgetResult{}, errors.New("invalid query-attempt reason (auth_required never consumes a budget)")
	}
	return r.consumeBudget(ctx, lease, t, false, false, reason)
}

// ConsumeRecoveryQueryAttemptManual records ONE explicit fresh-request
// verification query (W4 enforces the at-most-once-per-request rule): the
// observable query_attempts counter increments past the automated cap, the
// sticky state is neither reset nor widened, and the committed-receipt
// finalize remains available.
func (r *Repo) ConsumeRecoveryQueryAttemptManual(ctx context.Context, lease *LeaseClaim, t Tenant, reason RecoveryWaitReason) (BudgetResult, error) {
	if !validRecoveryWaitReason(reason) || reason == RecoveryWaitAuthRequired {
		return BudgetResult{}, errors.New("invalid manual query-attempt reason (auth_required never consumes a budget)")
	}
	return r.consumeBudget(ctx, lease, t, false, true, reason)
}

// ---------------------------------------------------------------------------
// Receipt verification + finalize (generic, routes on the frozen mode)
// ---------------------------------------------------------------------------

// PublishReceipt is the immutable DVS receipt evidence (fetched by key or
// transaction id through the runtime port in a later work package; the repo
// layer only consumes it). The repo INDEPENDENTLY re-verifies every identity
// field against the frozen intent (root v2.2 direction): key, fingerprint,
// dataset, branch AND the full lineage triple — the runtime port supplies
// them, but the receipt is never trusted on declaration alone.
type PublishReceipt struct {
	IdempotencyKey      string
	RequestFingerprint  string
	DatasetID           uuid.UUID
	Branch              string
	TransactionID       uuid.UUID
	Status              string // "COMMITTED" is the only finalizable status
	MetadataContentHash string
	MetadataSyncRunID   uuid.UUID
	MetadataSourceID    uuid.UUID
	MetadataSyncDefID   uuid.UUID
}

// ReceiptMatchesIntent is the pure two-layer verification: the receipt must
// be a COMMITTED receipt of THIS run's fixed key, this dataset, this branch,
// this exact request fingerprint, with transaction metadata (content hash +
// the full lineage triple source/def/run) matching the intent. Any mismatch
// is a typed error that never echoes body/candidate values.
func ReceiptMatchesIntent(receipt PublishReceipt, m PublishIntentMaterial) error {
	if receipt.Status != "COMMITTED" {
		return fmt.Errorf("receipt status %q is not COMMITTED", receipt.Status)
	}
	if receipt.IdempotencyKey != m.PublishKey {
		return errors.New("receipt idempotency key does not match the intent")
	}
	if receipt.DatasetID != m.OutputDatasetID {
		return errors.New("receipt dataset does not match the intent target")
	}
	if receipt.Branch != m.Branch {
		return errors.New("receipt branch does not match the intent")
	}
	if receipt.TransactionID == uuid.Nil {
		return errors.New("receipt has no transaction id")
	}
	if receipt.RequestFingerprint != m.RequestFingerprint {
		return errors.New("receipt request fingerprint does not match the intent body")
	}
	if receipt.MetadataContentHash != m.ContentHash {
		return errors.New("receipt content hash does not match the intent")
	}
	if receipt.MetadataSyncRunID != m.RunID {
		return errors.New("receipt lineage run does not match the intent run")
	}
	if receipt.MetadataSourceID != m.SourceID {
		return errors.New("receipt lineage source does not match the intent source")
	}
	if receipt.MetadataSyncDefID != m.SyncDefID {
		return errors.New("receipt lineage def does not match the intent def")
	}
	return nil
}

// RecoveryFinalizeOutcome reports the finalize decision.
type RecoveryFinalizeOutcome string

const (
	// RecoveryFinalizeCommitted: success (+ checkpoint advance and owner
	// release for incremental) + intent finalized landed in one transaction.
	RecoveryFinalizeCommitted RecoveryFinalizeOutcome = "finalized_committed"
	// RecoveryFinalizeAlreadyResolved: the run is already succeeded with a
	// finalized intent whose receipt facts match — zero writes (the
	// idempotent duplicate answer, checked BEFORE any owner/epoch state).
	RecoveryFinalizeAlreadyResolved RecoveryFinalizeOutcome = "already_resolved"
)

// RecoveryFinalizeResult pairs the outcome with the run row.
type RecoveryFinalizeResult struct {
	Outcome RecoveryFinalizeOutcome
	Run     *models.SyncRun
}

// ErrReceiptMismatch is the typed receipt-vs-intent verification failure.
var ErrReceiptMismatch = errors.New("committed receipt does not match the frozen publish intent")

// ErrRecoveryIntentMissing: the run has no intent row (pre-gate or legacy) —
// recovery finalize refuses to guess.
var ErrRecoveryIntentMissing = errors.New("run has no publish intent")

// FinalizeRecoveredRun is the GENERIC convergence: it routes on the run's
// frozen mode. The caller supplies only the receipt; the
// candidate/hash/counts come from the DB's immutable intent row, and the
// receipt is two-layer verified against the intent first. Duplicate recovery
// is recognized FIRST (already_resolved, zero writes) whatever the current
// checkpoint/owner state is. Finalize is allowed from exhausted budget
// states — a genuinely committed receipt always converges.
func (r *Repo) FinalizeRecoveredRun(ctx context.Context, lease *LeaseClaim, t Tenant, receipt PublishReceipt) (*RecoveryFinalizeResult, error) {
	if lease == nil {
		return nil, errors.New("FinalizeRecoveredRun requires a live recovery lease")
	}
	pre, err := r.runInternalPreRead(ctx, lease.Run.ID, t)
	if err != nil {
		return nil, err
	}
	if pre == nil || pre.RecoveryProtocol != models.RunRecoveryProtocolP26V1 {
		return nil, errors.New("FinalizeRecoveredRun on a non-p26-v1 run")
	}
	if isIncrementalRun(pre) {
		return r.FinalizeRecoveredIncrementalRun(ctx, lease, t, receipt)
	}
	if err := validateRecoveryFencing(lease.Holder, lease.Token, lease.Run.Generation); err != nil {
		return nil, err
	}
	runID := lease.Run.ID
	ctx, cancel := context.WithTimeout(ctx, checkpointOpBudget)
	defer cancel()
	var out *RecoveryFinalizeResult
	err = r.withDefRunLock(ctx, pre, t, func(tx pgx.Tx) error {
		intent, err := publishIntentByID(ctx, tx, runID)
		if err != nil {
			return err
		}
		if intent == nil {
			return ErrRecoveryIntentMissing
		}
		if err := ReceiptMatchesIntent(receipt, *intent); err != nil {
			return fmt.Errorf("%w: %v", ErrReceiptMismatch, err)
		}
		// Idempotent duplicate FIRST.
		status, err := runStatusUnderLock(ctx, tx, runID)
		if err != nil {
			return err
		}
		if status == "succeeded" && intent.RecoveryState == IntentStateFinalized {
			if intent.TxnID != nil && *intent.TxnID == receipt.TransactionID {
				run, rerr := lockedRunByID(ctx, tx, runID)
				if rerr != nil {
					return rerr
				}
				out = &RecoveryFinalizeResult{Outcome: RecoveryFinalizeAlreadyResolved, Run: run}
				return nil
			}
			return ErrIncrementalCheckpointCAS
		}
		if err := assertRecoveryLeaseHeld(ctx, tx, lease); err != nil {
			return err
		}
		// Snapshot success CAS — CompleteLeasedSyncRun success predicates
		// verbatim (gate entered, no cancel), facts from the intent row, NO
		// checkpoint side effects. The intent finalizes in the same tx.
		run, uerr := scanLeasedRun(tx.QueryRow(ctx,
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
			runID, lease.Holder, lease.Token, lease.Run.Generation,
			intent.Bytes, int64(1), intent.Rows, receipt.TransactionID, intent.ContentHash))
		if errors.Is(uerr, pgx.ErrNoRows) {
			// The unknown-stays-running run was closed out-of-band between
			// the claim and here, or the lease fenced. Zero writes.
			return ErrLeaseLost
		}
		if uerr != nil {
			return uerr
		}
		itag, ierr := tx.Exec(ctx,
			`UPDATE sync_run_publish_intents
			    SET recovery_state = 'finalized',
			        txn_id = $2,
			        resolved_at = clock_timestamp(),
			        updated_at = clock_timestamp()
			  WHERE run_id = $1 AND recovery_state <> 'finalized'`,
			runID, receipt.TransactionID)
		if ierr != nil {
			return ierr
		}
		if itag.RowsAffected() != 1 {
			return ErrIncrementalCheckpointCAS // rolls the success back with it
		}
		out = &RecoveryFinalizeResult{Outcome: RecoveryFinalizeCommitted, Run: &run.Run}
		return nil
	})
	if errors.Is(err, errCPNotVisible) {
		return nil, ErrLeaseLost
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// FinalizeRecoveredIncrementalRun is the incremental-mode convergence:
// success CAS + checkpoint advance + owner release + intent finalized, all
// in ONE transaction (completeIncrementalLocked), duplicate-recognized
// first. See FinalizeRecoveredRun for the shared contract.
func (r *Repo) FinalizeRecoveredIncrementalRun(ctx context.Context, lease *LeaseClaim, t Tenant, receipt PublishReceipt) (*RecoveryFinalizeResult, error) {
	if lease == nil {
		return nil, errors.New("FinalizeRecoveredIncrementalRun requires a live recovery lease")
	}
	if err := validateRecoveryFencing(lease.Holder, lease.Token, lease.Run.Generation); err != nil {
		return nil, err
	}
	runID := lease.Run.ID
	pre, err := r.runInternalPreRead(ctx, runID, t)
	if err != nil {
		return nil, err
	}
	if !isIncrementalRun(pre) {
		return nil, errors.New("FinalizeRecoveredIncrementalRun on a non-incremental run")
	}
	if pre.RecoveryProtocol != models.RunRecoveryProtocolP26V1 {
		return nil, errors.New("FinalizeRecoveredIncrementalRun on a run not frozen for the p26-v1 protocol")
	}
	ctx, cancel := context.WithTimeout(ctx, checkpointOpBudget)
	defer cancel()
	var out *RecoveryFinalizeResult
	err = r.withDefCheckpointRunLock(ctx, pre, t, func(tx pgx.Tx) error {
		intent, err := publishIntentByID(ctx, tx, runID)
		if err != nil {
			return err
		}
		if intent == nil {
			return ErrRecoveryIntentMissing
		}
		if err := ReceiptMatchesIntent(receipt, *intent); err != nil {
			return fmt.Errorf("%w: %v", ErrReceiptMismatch, err)
		}
		// Idempotent duplicate FIRST: a succeeded run with a finalized intent
		// and matching receipt facts is already resolved, whatever the
		// checkpoint/owner state is now.
		status, err := runStatusUnderLock(ctx, tx, runID)
		if err != nil {
			return err
		}
		if status == "succeeded" && intent.RecoveryState == IntentStateFinalized {
			if intent.TxnID != nil && *intent.TxnID == receipt.TransactionID {
				run, rerr := lockedRunByID(ctx, tx, runID)
				if rerr != nil {
					return rerr
				}
				out = &RecoveryFinalizeResult{Outcome: RecoveryFinalizeAlreadyResolved, Run: run}
				return nil
			}
			// Finalized with a DIFFERENT transaction: out-of-band corruption
			// of the intent row. Refuse rather than rewrite.
			return ErrIncrementalCheckpointCAS
		}
		// Fenced finalize: this recovery lease must still be the live holder.
		if err := assertRecoveryLeaseHeld(ctx, tx, lease); err != nil {
			return err
		}
		// The candidate/hash/counts come from the immutable intent row.
		var candidate string
		if intent.CompletedCheckpoint != nil {
			candidate = *intent.CompletedCheckpoint
		}
		adv := IncrementalAdvance{
			CompletedCheckpoint: candidate,
			Rows:                intent.Rows,
			Bytes:               intent.Bytes,
			ContentHash:         intent.ContentHash,
			TxnID:               receipt.TransactionID,
		}
		run, ferr := completeIncrementalLocked(ctx, tx, pre, lease.Holder, lease.Token, lease.Run.Generation, adv, receipt.TransactionID)
		if ferr != nil {
			return ferr
		}
		out = &RecoveryFinalizeResult{Outcome: RecoveryFinalizeCommitted, Run: run}
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
// P26-only mutation fence (SetCredential / effective def-input change)
// ---------------------------------------------------------------------------

// assertNoRecoveryProtocolRunsTx is the v2.1 fence core: it errors with
// ErrRecoveryRunsActive when any def still holds a PENDING or RUNNING
// p26-v1 run. Run inside a transaction holding the Source/Def locks.
// Pre-P26 rows (NULL protocol) never trip it, so legacy behavior is
// byte-identical.
func assertNoRecoveryProtocolRunsTx(ctx context.Context, tx pgx.Tx, defIDs []uuid.UUID) error {
	if len(defIDs) == 0 {
		return nil
	}
	idStrings := make([]string, len(defIDs))
	for i, id := range defIDs {
		idStrings[i] = id.String()
	}
	rows, err := tx.Query(ctx,
		`SELECT r.sync_def_id, r.id, r.status
		   FROM sync_runs r
		  WHERE r.recovery_protocol = 'p26-v1'
		    AND r.status IN ('pending','running')
		    AND r.sync_def_id = ANY($1)`, idStrings)
	if err != nil {
		return err
	}
	defer rows.Close()
	held := []string{}
	for rows.Next() {
		var defID, runID uuid.UUID
		var status string
		if err := rows.Scan(&defID, &runID, &status); err != nil {
			return err
		}
		held = append(held, fmt.Sprintf("def=%s run=%s status=%s", defID, runID, status))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(held) > 0 {
		return fmt.Errorf("%w: %s", ErrRecoveryRunsActive, strings.Join(held, ", "))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Locked helpers (run-row reads/writes under the held chain locks)
// ---------------------------------------------------------------------------

// runLeaseLive reports whether the run row currently holds an unexpired
// running lease, evaluated with the post-lock DB clock.
func runLeaseLive(ctx context.Context, tx pgx.Tx, runID uuid.UUID) (bool, error) {
	var live bool
	err := tx.QueryRow(ctx,
		`SELECT COALESCE(r.status = 'running'
		      AND r.lease_expires_at IS NOT NULL
		      AND clock_timestamp() < r.lease_expires_at, false)
		   FROM sync_runs r WHERE r.id = $1`, runID).Scan(&live)
	return live, err
}

// claimLockedRunningRun installs a fresh lease on a RUNNING run row the
// caller already locked (attempt/generation+1, rotated token) — the recovery
// takeover shape. The status deliberately stays 'running' (unknown outcome).
func claimLockedRunningRun(ctx context.Context, tx pgx.Tx, runID uuid.UUID, holder, token string, ttl time.Duration) (*LeaseClaim, error) {
	claim, err := scanLeasedRun(tx.QueryRow(ctx,
		`UPDATE sync_runs r
		    SET attempt = r.attempt + 1,
		        generation = r.generation + 1,
		        lease_holder = $2,
		        lease_token = $3,
		        lease_expires_at = clock_timestamp() + make_interval(secs => $4::float8),
		        heartbeat_at = clock_timestamp()
		  WHERE r.id = $1 AND r.status = 'running'
		    AND (r.lease_expires_at IS NULL OR clock_timestamp() >= r.lease_expires_at)
		  RETURNING `+leasedRunColumns,
		runID, holder, token, ttl.Seconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // lost the race (a renewal landed between check and CAS)
	}
	if err != nil {
		return nil, err
	}
	return claim, nil
}

// claimLockedPendingRun (checkpoint.go) is reused verbatim for the pending
// fresh-execution claim — the standard P25 pending→running CAS.

// assertRecoveryLeaseHeld verifies — under the held run lock, with the
// post-lock clock — that the given lease material is the CURRENT live holder
// of the running run. Returns ErrLeaseLost on any mismatch (zero writes).
func assertRecoveryLeaseHeld(ctx context.Context, tx pgx.Tx, lease *LeaseClaim) error {
	var ok bool
	err := tx.QueryRow(ctx,
		`SELECT COALESCE(r.status = 'running'
		      AND r.lease_holder = $2 AND r.lease_token = $3 AND r.generation = $4
		      AND clock_timestamp() < r.lease_expires_at, false)
		   FROM sync_runs r WHERE r.id = $1`,
		lease.Run.ID, lease.Holder, lease.Token, lease.Run.Generation).Scan(&ok)
	if err != nil {
		return err
	}
	if !ok {
		return ErrLeaseLost
	}
	return nil
}

// runStatusUnderLock reads only the status under the held run lock.
func runStatusUnderLock(ctx context.Context, tx pgx.Tx, runID uuid.UUID) (string, error) {
	var status string
	err := tx.QueryRow(ctx,
		`SELECT r.status FROM sync_runs r WHERE r.id = $1`, runID).Scan(&status)
	return status, err
}
