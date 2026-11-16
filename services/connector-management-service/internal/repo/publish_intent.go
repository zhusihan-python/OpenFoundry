// P2.6 durable publish intents (root-decisions-v2 items 1, 3, 5).
//
// A p26-v1 run's publish material is frozen ONCE, in the same ordered
// Def->Checkpoint->Run transaction that stamps sync_runs.publish_entered_at:
// the EXACT serialized HTTP JSON body (single serialization — the DVS byte
// contract), the opaque completion-checkpoint candidate, the explicit branch,
// the run-scoped idempotency key, the sha256 request fingerprint, the content
// hash/counts and the run's frozen baseline. From the moment the intent
// exists the source may never be pulled again for this run: recovery may only
// VERIFY the remote outcome by key or replay the exact same bytes.
//
// The key is FIXED and run-scoped ("of-p26-pub-v1:<runUUID>") — generation,
// attempt and content are deliberately NOT part of it. Target/branch/body are
// bound by the request fingerprint instead, so no retry can evade a conflict
// by swapping the key, and no identity drift can silently republish.
//
// Authorization material never enters this file's storage, views, errors or
// logs. The raw body, the opaque candidate, the binding fingerprint and the
// key itself never appear in any public projection.
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

// Bounds of the P2.6 intent protocol (frozen by root-decisions-v2 item 5).
const (
	// MaxPublishIntentBodyBytes bounds the persisted exact publish body. It
	// equals the P25 default max_total_bytes ceiling: a fetch that stayed
	// within its limits always fits, and a DB CHECK mirrors the bound.
	MaxPublishIntentBodyBytes = 64 << 20
	// MaxPublishSendAttempts is the total send budget (first dispatch + same
	// -byte replays), persisted across restarts.
	MaxPublishSendAttempts = 3
	// MaxRecoveryQueryAttempts bounds automated verification attempts.
	MaxRecoveryQueryAttempts = 5
	// PublishKeyPrefix is the fixed v1 identity prefix.
	PublishKeyPrefix = "of-p26-pub-v1:"
	// Recovery states (recovery_state column).
	IntentStateArmed                = "armed"
	IntentStateWaiting              = "waiting"
	IntentStateSendBudgetExhausted  = "send_budget_exhausted"
	IntentStateQueryBudgetExhausted = "query_budget_exhausted"
	IntentStateFinalized            = "finalized"
)

// Recovery state error: an intent is already finalized or sticky-exhausted
// and refuses further state mutations through the wait/budget paths.
var ErrIntentNotMutable = errors.New("publish intent is finalized or budget-exhausted and refuses further automated transitions")

// DerivePublishKey returns the fixed run-scoped DVS idempotency key of a run
// (root-decisions-v2 item 1). Deterministic and stable for the run's whole
// life; deliberately independent of generation/attempt/content.
func DerivePublishKey(runID uuid.UUID) string {
	return PublishKeyPrefix + runID.String()
}

// RecoveryBackoff is the capped exponential query backoff (1s, 2s, 4s, 8s,
// then 16s): attempt counts start at 1 after the first consumption.
func RecoveryBackoff(attempt int) time.Duration {
	switch {
	case attempt <= 1:
		return time.Second
	case attempt == 2:
		return 2 * time.Second
	case attempt == 3:
		return 4 * time.Second
	case attempt == 4:
		return 8 * time.Second
	default:
		return 16 * time.Second
	}
}

// FrozenPublishMaterial is the executor's gate-time input: everything the
// intent row persists, assembled by the single serialization site (W3).
type FrozenPublishMaterial struct {
	SourceID, SyncDefID uuid.UUID
	Branch              string
	LogicalPath         string
	// ExactBody is the serialized HTTP JSON body (already including the
	// idempotency_key field). It is persisted verbatim and MUST be replayed
	// verbatim — the DVS fingerprint is over these exact bytes. The size
	// bound applies to THIS serialized body, not to any decoded payload.
	ExactBody []byte
	// CompletedCheckpoint is the opaque data-checkpoint candidate. REQUIRED
	// non-nil for incremental runs; REQUIRED nil for snapshot runs (enforced
	// against the run's frozen mode by ValidatePublishCandidateForMode at the
	// gate — CPMode is set by the gate caller from the frozen run row).
	CompletedCheckpoint *string
	ContentHash         string
	Rows                int64
	Bytes               int64
	// CPMode is the run's frozen mode ("" = snapshot); it is filled by the
	// gate from the trusted pre-read, never by the executor, and drives the
	// candidate pairing validation only.
	CPMode string
}

// validateHex64 reports whether s is exactly 64 lowercase hex chars.
func validateHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// ValidatePublishCandidateForMode enforces the mode pairing of the candidate
// (root-decisions-v2.1): an incremental run (cpMode != "") requires a valid
// non-nil opaque candidate; a snapshot run (cpMode == "") requires nil.
func ValidatePublishCandidateForMode(cpMode string, candidate *string) error {
	if cpMode != "" {
		if candidate == nil {
			return errors.New("incremental publish material requires a completed checkpoint candidate")
		}
		if err := models.ValidateOpaqueCheckpointValue(*candidate, "completed checkpoint"); err != nil {
			return fmt.Errorf("publish material candidate invalid: %w", err)
		}
		return nil
	}
	if candidate != nil {
		return errors.New("snapshot publish material must not carry a completed checkpoint candidate")
	}
	return nil
}

// ValidateFrozenPublishMaterial performs the pure, lock-free validation of a
// gate-time material: non-blank branch/logical path, bounded non-nil body
// that parses as the commit-request JSON carrying the EXACT run-scoped
// idempotency key — and, v2.1, cross-checks the ACTUAL inline content rather
// than trusting declared metadata: the base64 file content is decoded, its
// length must equal both the declared file size and mat.Bytes, and its sha256
// must equal mat.ContentHash (which must also match the metadata
// declaration). The metadata lineage triple (source/def/run) must equal the
// material's identity, and the candidate must satisfy the mode pairing.
// Errors never echo body bytes or the candidate value.
func ValidateFrozenPublishMaterial(runID uuid.UUID, mat FrozenPublishMaterial) error {
	if strings.TrimSpace(mat.Branch) == "" {
		return errors.New("publish material requires an explicit branch")
	}
	if strings.TrimSpace(mat.LogicalPath) == "" {
		return errors.New("publish material requires a logical path")
	}
	if mat.SourceID == uuid.Nil || mat.SyncDefID == uuid.Nil {
		return errors.New("publish material requires the frozen source and def identity")
	}
	if len(mat.ExactBody) < 2 || len(mat.ExactBody) > MaxPublishIntentBodyBytes {
		return fmt.Errorf("publish material body must be between 2 and %d bytes (serialized JSON body bound)", MaxPublishIntentBodyBytes)
	}
	if err := ValidatePublishCandidateForMode(mat.CPMode, mat.CompletedCheckpoint); err != nil {
		return err
	}
	if !validateHex64(mat.ContentHash) {
		return errors.New("publish material requires a 64-lowercase-hex content hash")
	}
	if mat.Rows < 0 || mat.Bytes < 0 {
		return errors.New("publish material counts must be non-negative")
	}
	var body struct {
		IdempotencyKey string `json:"idempotency_key"`
		Branch         string `json:"branch"`
		Files          []struct {
			LogicalPath   string `json:"logical_path"`
			SizeBytes     int64  `json:"size_bytes"`
			ContentBase64 []byte `json:"content_base64"`
		} `json:"files"`
		Metadata struct {
			ContentHash string `json:"content_hash"`
			Rows        int64  `json:"rows"`
			Bytes       int64  `json:"bytes"`
			Lineage     *struct {
				SourceID  uuid.UUID `json:"source_id"`
				SyncDefID uuid.UUID `json:"sync_def_id"`
				SyncRunID uuid.UUID `json:"sync_run_id"`
			} `json:"lineage"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(mat.ExactBody, &body); err != nil {
		return errors.New("publish material body is not the expected commit request JSON")
	}
	if body.IdempotencyKey != DerivePublishKey(runID) {
		return errors.New("publish material body does not carry the run-scoped idempotency key")
	}
	if body.Branch != mat.Branch {
		return errors.New("publish material branch does not match the serialized body")
	}
	if len(body.Files) != 1 {
		return errors.New("publish material body must carry exactly one inline file")
	}
	if body.Files[0].LogicalPath != mat.LogicalPath {
		return errors.New("publish material logical path does not match the serialized body")
	}
	// v2.1: the declared size/hash are never trusted alone — the actual
	// decoded bytes must carry them.
	if body.Files[0].ContentBase64 == nil {
		return errors.New("publish material body file has no inline content")
	}
	if int64(len(body.Files[0].ContentBase64)) != mat.Bytes {
		return errors.New("publish material byte count does not match the actual inline content length")
	}
	if body.Files[0].SizeBytes != mat.Bytes {
		return errors.New("publish material byte count does not match the declared file size")
	}
	actualHash := fmt.Sprintf("%x", sha256.Sum256(body.Files[0].ContentBase64))
	if actualHash != mat.ContentHash {
		return errors.New("publish material content hash does not match the actual inline content")
	}
	if body.Metadata.ContentHash != mat.ContentHash {
		return errors.New("publish material content hash does not match the serialized body metadata")
	}
	if body.Metadata.Rows != mat.Rows {
		return errors.New("publish material row count does not match the serialized body metadata")
	}
	if body.Metadata.Bytes != mat.Bytes {
		return errors.New("publish material byte count does not match the serialized body metadata")
	}
	if body.Metadata.Lineage == nil {
		return errors.New("publish material body metadata has no lineage triple")
	}
	if body.Metadata.Lineage.SourceID != mat.SourceID ||
		body.Metadata.Lineage.SyncDefID != mat.SyncDefID ||
		body.Metadata.Lineage.SyncRunID != runID {
		return errors.New("publish material lineage does not match the frozen source/def/run identity")
	}
	return nil
}

// RequestFingerprintOf is the sha256 hex over the exact request body bytes —
// the same value the DVS computes itself (root-decisions-v2, Dataset W1).
func RequestFingerprintOf(exactBody []byte) string {
	digest := sha256.Sum256(exactBody)
	return hex.EncodeToString(digest[:])
}

// PublishIntentMaterial is the full internal projection of one intent row
// (repo-internal: it carries the exact body and the opaque candidate and is
// never serialized to public JSON).
type PublishIntentMaterial struct {
	RunID, SyncDefID, SourceID uuid.UUID
	OrgID                      *uuid.UUID
	RecoveryProtocol           string
	PublishKey                 string
	RequestFingerprint         string
	ExactBody                  []byte
	OutputDatasetID            uuid.UUID
	Branch, LogicalPath        string
	ContentHash                string
	Rows, Bytes                int64
	// CompletedCheckpoint is nil for snapshot intents, non-nil for
	// incremental intents (mirrors the SQL NULL rule).
	CompletedCheckpoint  *string
	CPMode               string
	CPBaseEpoch          int
	CPBaseValue          string
	CPBindingFingerprint string
	RecoveryState        string
	LastWaitReason       *string
	NextActionAt         *time.Time
	SendAttempts         int
	QueryAttempts        int
	TxnID                *uuid.UUID
	ResolvedAt           *time.Time
}

const publishIntentColumns = `pi.run_id, pi.sync_def_id, pi.source_id, pi.org_id,
	pi.recovery_protocol, pi.publish_key, pi.request_fingerprint, pi.publish_body,
	pi.output_dataset_id, pi.branch, pi.logical_path, pi.content_hash,
	pi.rows_written, pi.bytes_written, pi.completed_checkpoint,
	pi.cp_mode, pi.cp_base_epoch, pi.cp_base_value, pi.cp_binding_fingerprint,
	pi.recovery_state, pi.last_wait_reason, pi.next_action_at,
	pi.send_attempts, pi.query_attempts, pi.txn_id, pi.resolved_at`

func scanPublishIntent(row pgx.Row) (*PublishIntentMaterial, error) {
	v := &PublishIntentMaterial{}
	var cpBaseValue *string
	err := row.Scan(&v.RunID, &v.SyncDefID, &v.SourceID, &v.OrgID,
		&v.RecoveryProtocol, &v.PublishKey, &v.RequestFingerprint, &v.ExactBody,
		&v.OutputDatasetID, &v.Branch, &v.LogicalPath, &v.ContentHash,
		&v.Rows, &v.Bytes, &v.CompletedCheckpoint,
		&v.CPMode, &v.CPBaseEpoch, &cpBaseValue, &v.CPBindingFingerprint,
		&v.RecoveryState, &v.LastWaitReason, &v.NextActionAt,
		&v.SendAttempts, &v.QueryAttempts, &v.TxnID, &v.ResolvedAt)
	if err != nil {
		return nil, err
	}
	if cpBaseValue != nil {
		v.CPBaseValue = *cpBaseValue
	}
	// Snapshot intents carry a NULL candidate by contract; the pairing is
	// enforced at the gate, so a non-nil candidate on a snapshot-mode intent
	// is corruption and fails loudly here instead of flowing downstream.
	if v.CompletedCheckpoint != nil && v.CPMode == "" {
		return nil, errors.New("snapshot publish intent carries a non-NULL candidate")
	}
	return v, nil
}

// publishIntentByID loads one intent row under the caller's open transaction,
// taking it FOR UPDATE first. Returns (nil, nil) when absent.
func publishIntentByID(ctx context.Context, tx pgx.Tx, runID uuid.UUID) (*PublishIntentMaterial, error) {
	v, err := scanPublishIntent(tx.QueryRow(ctx,
		`SELECT `+publishIntentColumns+` FROM sync_run_publish_intents pi
		  WHERE pi.run_id = $1 FOR UPDATE`, runID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

// GetPublishIntentMaterial returns the run's intent (trusted parent-chain
// tenant check through the run join), or (nil, nil) when the run has none.
// Repo-internal material — callers must not expose it verbatim.
func (r *Repo) GetPublishIntentMaterial(ctx context.Context, runID uuid.UUID, t Tenant) (*PublishIntentMaterial, error) {
	predicate, arg := syncRunTenantPredicate(t, 2)
	v, err := scanPublishIntent(r.Pool.QueryRow(ctx,
		`SELECT `+publishIntentColumns+` FROM sync_run_publish_intents pi
		  JOIN sync_runs r ON r.id = pi.run_id
		 WHERE pi.run_id = $1 AND `+predicate, runID, arg))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

// PublishIntentView is the SAFE public projection of an intent: state,
// attempt counters, timestamps, reason, the recorded transaction and
// length-only body/checkpoint facts. The raw body, the opaque candidate, the
// binding fingerprint and the key never appear.
type PublishIntentView struct {
	Protocol       string     `json:"protocol"`
	State          string     `json:"state"`
	LastWaitReason *string    `json:"last_wait_reason,omitempty"`
	NextActionAt   *time.Time `json:"next_action_at,omitempty"`
	SendAttempts   int        `json:"send_attempts"`
	SendBudget     int        `json:"send_budget"`
	QueryAttempts  int        `json:"query_attempts"`
	QueryBudget    int        `json:"query_budget"`
	BodyBytes      int        `json:"body_bytes"`
	CheckpointLen  int        `json:"checkpoint_len"`
	TransactionID  *uuid.UUID `json:"transaction_id,omitempty"`
	ResolvedAt     *time.Time `json:"resolved_at,omitempty"`
}

// PublishIntentSafeView projects material into the safe view shape.
func PublishIntentSafeView(m *PublishIntentMaterial) *PublishIntentView {
	checkpointLen := 0
	if m.CompletedCheckpoint != nil {
		checkpointLen = len(*m.CompletedCheckpoint)
	}
	return &PublishIntentView{
		Protocol:       m.RecoveryProtocol,
		State:          m.RecoveryState,
		LastWaitReason: m.LastWaitReason,
		NextActionAt:   m.NextActionAt,
		SendAttempts:   m.SendAttempts,
		SendBudget:     MaxPublishSendAttempts,
		QueryAttempts:  m.QueryAttempts,
		QueryBudget:    MaxRecoveryQueryAttempts,
		BodyBytes:      len(m.ExactBody),
		CheckpointLen:  checkpointLen,
		TransactionID:  m.TxnID,
		ResolvedAt:     m.ResolvedAt,
	}
}

// GetPublishIntentView returns the safe view of the run's intent, or (nil,
// nil) when the run has none (including every pre-P26 run).
func (r *Repo) GetPublishIntentView(ctx context.Context, runID uuid.UUID, t Tenant) (*PublishIntentView, error) {
	m, err := r.GetPublishIntentMaterial(ctx, runID, t)
	if err != nil || m == nil {
		return nil, err
	}
	return PublishIntentSafeView(m), nil
}
