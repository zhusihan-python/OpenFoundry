// P2.6 recoverable publish client (root-decisions-v2 + W3 interface freeze).
//
// The keyed commit protocol serializes the publish body exactly ONCE
// (PrepareRecoverableCommit), persists those bytes, and every later dispatch
// — first attempt or same-byte replay after an unknown outcome — sends the
// persisted bytes verbatim. The DVS fingerprints the raw request bytes
// itself, so any re-serialization (field order, metadata drift) would be a
// different operation: 409, not idempotency. The client therefore never
// re-marshals, never pulls the source, and never retries on its own —
// send/query budgets, heartbeat and fencing belong to the executor.
//
// Authorization is a fresh caller token that only ever reaches the request
// header; it never enters the body, an error, or a log line. Error text
// never echoes the raw response body either: non-2xx replies are classified
// by status alone.
package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Protocol bounds. maxPreparedCommitBodyBytes mirrors
// repo.MaxPublishIntentBodyBytes; the constant is restated here because the
// runtime port package must stay independent of the repo layer. It caps the
// SERIALIZED exact JSON body (base64 inflation included) — it is NOT a claim
// that a 64 MiB NDJSON snapshot always fits: the actual serialized length is
// measured and over-sized prepares are refused, never truncated
// (root-decisions-v2.1). Receipt reads are bounded well above anything a
// legitimate commit response (file index + bounded preview, never inline
// content) can produce.
const (
	maxPreparedCommitBodyBytes = 64 << 20
	maxCommitReceiptBytes      = 8 << 20
	maxIdempotencyKeyLen       = 128
)

// ErrCommitReceiptNotFound: GET /outputs/commits/{key} answered 404 — no
// committed receipt is visible for the key YET. It is never proof that
// nothing is in flight (root-decisions-v2, Dataset W1) and never a fake
// success: callers keep the owner and continue verification.
var ErrCommitReceiptNotFound = errors.New("no committed receipt is visible for the idempotency key")

// ErrCommitIdempotencyConflict: the same idempotency key is already
// committed with a DIFFERENT request body (fingerprint mismatch, HTTP 409).
// Swapping the key cannot evade it — target/branch/body are fingerprint
// bound. The typed response code W1-DATASET-INTERFACE will specify refines
// this classification later; the status split lands now.
var ErrCommitIdempotencyConflict = errors.New("idempotency key is already committed with a different request body")

// ReceiptMismatchError reports a structurally valid committed receipt that
// fails the strict match against the prepared expectations. Check names the
// failing comparison only; it never carries body bytes or tokens.
type ReceiptMismatchError struct {
	Check string
}

func (e *ReceiptMismatchError) Error() string {
	return "commit receipt failed the strict prepared-commit match: " + e.Check
}

// DatasetLineageIdentity is the typed lineage triple embedded in the publish
// metadata and echoed back inside transaction metadata. It is the identity
// half of the strict receipt match.
type DatasetLineageIdentity struct {
	SourceID  uuid.UUID `json:"source_id"`
	SyncDefID uuid.UUID `json:"sync_def_id"`
	SyncRunID uuid.UUID `json:"sync_run_id"`
}

// PreparedCommit is the single-serialization result: everything the executor
// persists at the gate (body, key, branch, fingerprint, hash, counts) plus
// the lineage expectations the receipt must echo. Body is the exact JSON
// the DVS will fingerprint; mutating it after the fact only makes the send
// refuse (CommitPreparedOutput re-checks sha256(Body)==Fingerprint), because
// what goes on the wire must always be the bytes that were fingerprinted.
type PreparedCommit struct {
	DatasetID   uuid.UUID
	Key         string
	Branch      string
	LogicalPath string
	Body        []byte
	Fingerprint string
	ContentHash string
	Rows        int64
	Bytes       int64
	Lineage     DatasetLineageIdentity
}

// DatasetCommitReceipt is the typed, strictly-validated view of the
// immutable response saved by the DVS for a keyed commit (returned verbatim
// by both the commit replay and the GET-by-key endpoint). Replay is set by
// CommitPreparedOutput only: an HTTP 200 same-key replay vs the 201 first
// commit; a GET cannot invent it.
type DatasetCommitReceipt struct {
	DatasetID          uuid.UUID
	Branch             string
	IdempotencyKey     string
	RequestFingerprint string
	TransactionID      uuid.UUID
	Status             string
	ContentHash        string
	Lineage            DatasetLineageIdentity
	Replay             bool
}

// DatasetRecoveryPort is the P2.6 keyed-commit client port. It is separate
// from DatasetVersioningPort on purpose: the legacy port, its fakes and the
// P25 handler paths stay byte-for-byte compatible; only protocol-v1 runs
// flow through the recoverable surface. Both operations are single-shot —
// the executor owns retry budgets, fencing and stickiness.
type DatasetRecoveryPort interface {
	// CommitPreparedOutput POSTs the persisted body verbatim under a fresh
	// caller Authorization header and only returns a receipt that satisfies
	// the strict match (ValidateReceipt) — a 2xx with a wrong or unknown
	// receipt is rejected, never a usable ACK.
	CommitPreparedOutput(ctx context.Context, prepared PreparedCommit, authorization string) (*DatasetCommitReceipt, error)
	// GetCommitReceipt fetches the saved immutable receipt by key. 404 is
	// ErrCommitReceiptNotFound (not committed-visible-yet); 200 still has to
	// pass structural validation, and the executor matches it against the
	// prepared expectations with ValidateReceipt.
	GetCommitReceipt(ctx context.Context, datasetID uuid.UUID, key string, authorization string) (*DatasetCommitReceipt, error)
}

// The concrete client implements both ports; the legacy surface is untouched.
var (
	_ DatasetVersioningPort = HTTPDatasetVersioningClient{}
	_ DatasetRecoveryPort   = HTTPDatasetVersioningClient{}
)

// recoverableCommitFile and recoverableCommitBody fix the exact wire shape.
// Top-level order comes from the struct declaration; the DVS decodes by
// field name, and the fingerprint is over the produced bytes, so the order
// only has to be stable — which a struct guarantees. Metadata is embedded
// verbatim from the caller (map iteration order is irrelevant: encoding/json
// sorts map keys).
type recoverableCommitFile struct {
	LogicalPath   string `json:"logical_path"`
	SizeBytes     int64  `json:"size_bytes"`
	ContentType   string `json:"content_type"`
	Operation     string `json:"operation"`
	ContentBase64 []byte `json:"content_base64"` // empty slice => "" (zero-row inline output)
}

type recoverableCommitBody struct {
	CreateIfMissing bool                    `json:"create_if_missing"`
	Format          string                  `json:"format"`
	Branch          string                  `json:"branch"`
	TransactionType string                  `json:"transaction_type"`
	Summary         string                  `json:"summary"`
	Files           []recoverableCommitFile `json:"files"`
	Metadata        map[string]any          `json:"metadata"`
	IdempotencyKey  string                  `json:"idempotency_key"`
}

// PrepareRecoverableCommit serializes the publish body exactly once and
// derives the request fingerprint. It is pure: no I/O, no clock, no source
// pull. The keyed protocol is frozen to an explicit branch with
// create_if_missing=false and exactly one inline file (zero bytes allowed),
// metadata carrying content_hash/rows plus the lineage triple; the raw
// checkpoint candidate and any authorization material must never appear.
//
// The (body, material) pair leaving this function is guaranteed to satisfy
// repo.ValidateFrozenPublishMaterial for matching inputs: branch, logical
// path, size, content hash and row count are cross-checked against the
// serialized body here, at the single serialization site. Errors never echo
// content or metadata values.
func PrepareRecoverableCommit(req DatasetCommitRequest, idempotencyKey string) (*PreparedCommit, error) {
	if err := validateIdempotencyKey(idempotencyKey); err != nil {
		return nil, err
	}
	if req.DatasetID == uuid.Nil {
		return nil, errors.New("recoverable commit requires a target dataset id")
	}
	branch := strings.TrimSpace(req.Branch)
	if branch == "" {
		// The keyed protocol must name its branch: an empty branch would let
		// dataset-versioning silently resolve active/main (or fall back to
		// main on a typo), which is exactly the ambiguity the fingerprint
		// exists to forbid.
		return nil, errors.New("recoverable commit requires an explicit branch")
	}
	if req.CreateIfMissing {
		return nil, errors.New("recoverable commit requires create_if_missing=false")
	}
	logicalPath := strings.TrimSpace(req.LogicalPath)
	if logicalPath == "" {
		return nil, errors.New("recoverable commit requires a logical path")
	}
	if req.RowCount < 0 {
		return nil, errors.New("recoverable commit row count must be non-negative")
	}
	content := req.Content
	if content == nil {
		content = []byte{}
	}
	contentHash := strings.TrimSpace(req.ContentHash)
	if !isHex64(contentHash) {
		return nil, errors.New("recoverable commit requires a 64-lowercase-hex content hash")
	}
	if digest := sha256.Sum256(content); hex.EncodeToString(digest[:]) != contentHash {
		// Freeze only a CONSISTENT (content, hash) pair: a mismatched pair
		// would persist an intent whose metadata can never be re-verified.
		return nil, errors.New("recoverable commit content hash does not match the content bytes")
	}
	lineage, err := recoverableCommitMetadata(req.Metadata, contentHash, req.RowCount, int64(len(content)))
	if err != nil {
		return nil, err
	}
	contentType := strings.TrimSpace(req.ContentType)
	if contentType == "" {
		contentType = "application/x-ndjson"
	}
	transactionType := strings.TrimSpace(req.TransactionType)
	if transactionType == "" {
		transactionType = "SNAPSHOT"
	}
	body, err := json.Marshal(recoverableCommitBody{
		CreateIfMissing: false,
		Format:          req.Format,
		Branch:          branch,
		TransactionType: transactionType,
		Summary:         req.Summary,
		Files: []recoverableCommitFile{{
			LogicalPath:   logicalPath,
			SizeBytes:     int64(len(content)),
			ContentType:   contentType,
			Operation:     "ADD",
			ContentBase64: content,
		}},
		Metadata:       req.Metadata,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return nil, fmt.Errorf("recoverable commit body serialization failed: %w", err)
	}
	if len(body) < 2 || len(body) > maxPreparedCommitBodyBytes {
		return nil, fmt.Errorf("recoverable commit body must be between 2 and %d bytes", maxPreparedCommitBodyBytes)
	}
	return &PreparedCommit{
		DatasetID:   req.DatasetID,
		Key:         idempotencyKey,
		Branch:      branch,
		LogicalPath: logicalPath,
		Body:        body,
		Fingerprint: RequestFingerprintOfBody(body),
		ContentHash: contentHash,
		Rows:        req.RowCount,
		Bytes:       int64(len(content)),
		Lineage:     lineage,
	}, nil
}

// recoverableCommitMetadata validates the caller metadata embedded in the
// body: content_hash must equal the request hash, rows must equal the
// request row count and bytes must equal the actual content length (all are
// repo.ValidateFrozenPublishMaterial cross checks, enforced here at the
// single serialization site), and lineage must carry the three parseable
// identity UUIDs that the receipt must echo.
func recoverableCommitMetadata(metadata map[string]any, contentHash string, rowCount, byteCount int64) (DatasetLineageIdentity, error) {
	var zero DatasetLineageIdentity
	if metadata == nil {
		return zero, errors.New("recoverable commit metadata is required (content_hash, rows, bytes, lineage)")
	}
	rawHash, ok := metadata["content_hash"]
	if !ok {
		return zero, errors.New("recoverable commit metadata is missing content_hash")
	}
	hashStr, ok := rawHash.(string)
	if !ok || hashStr != contentHash {
		return zero, errors.New("recoverable commit metadata content_hash must be the request content hash")
	}
	rawRows, ok := metadata["rows"]
	if !ok {
		return zero, errors.New("recoverable commit metadata is missing rows")
	}
	rows, ok := coerceInt64(rawRows)
	if !ok || rows != rowCount {
		return zero, errors.New("recoverable commit metadata rows must equal the request row count")
	}
	rawBytes, ok := metadata["bytes"]
	if !ok {
		return zero, errors.New("recoverable commit metadata is missing bytes")
	}
	bytesCount, ok := coerceInt64(rawBytes)
	if !ok || bytesCount != byteCount {
		return zero, errors.New("recoverable commit metadata bytes must equal the actual content byte count")
	}
	rawLineage, ok := metadata["lineage"]
	if !ok {
		return zero, errors.New("recoverable commit metadata is missing lineage")
	}
	lineageMap, ok := rawLineage.(map[string]any)
	if !ok {
		return zero, errors.New("recoverable commit metadata lineage must be an object")
	}
	return recoverableLineageFrom(lineageMap)
}

// recoverableLineageFrom parses the lineage triple. Values arrive as
// uuid.UUID (the P25 handler embeds them typed) or as strings.
func recoverableLineageFrom(lineage map[string]any) (DatasetLineageIdentity, error) {
	identity := DatasetLineageIdentity{}
	for name, target := range map[string]*uuid.UUID{
		"source_id":   &identity.SourceID,
		"sync_def_id": &identity.SyncDefID,
		"sync_run_id": &identity.SyncRunID,
	} {
		raw, ok := lineage[name]
		if !ok {
			return identity, fmt.Errorf("recoverable commit metadata lineage is missing %s", name)
		}
		switch v := raw.(type) {
		case uuid.UUID:
			*target = v
		case string:
			parsed, err := uuid.Parse(strings.TrimSpace(v))
			if err != nil {
				return identity, fmt.Errorf("recoverable commit metadata lineage %s is not a uuid", name)
			}
			*target = parsed
		default:
			return identity, fmt.Errorf("recoverable commit metadata lineage %s must be a uuid", name)
		}
	}
	if identity.SourceID == uuid.Nil || identity.SyncDefID == uuid.Nil || identity.SyncRunID == uuid.Nil {
		return identity, errors.New("recoverable commit metadata lineage ids must be non-nil")
	}
	return identity, nil
}

// validateIdempotencyKey accepts the fixed v1 shape surface: non-blank,
// within 128 bytes, restricted to unreserved characters plus ':' so the key
// can travel in a path segment without escaping. The exact derivation
// (of-p26-pub-v1:<runUUID>) lives with the repo layer.
func validateIdempotencyKey(key string) error {
	if key == "" || strings.TrimSpace(key) != key {
		return errors.New("recoverable commit requires a non-blank idempotency key")
	}
	if len(key) > maxIdempotencyKeyLen {
		return fmt.Errorf("recoverable commit idempotency key exceeds %d bytes", maxIdempotencyKeyLen)
	}
	for _, c := range key {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == ':' || c == '-' || c == '_' || c == '.' || c == '~':
		default:
			return errors.New("recoverable commit idempotency key has invalid characters")
		}
	}
	return nil
}

// isHex64 reports exactly 64 lowercase hex characters.
func isHex64(s string) bool {
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

// coerceInt64 accepts the numeric Go kinds a handler-built metadata map can
// carry (int64 from snapshot counters, int page counts, whole float64 from
// JSON round-trips).
func coerceInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case float64:
		if n == float64(int64(n)) {
			return int64(n), true
		}
	case json.Number:
		if parsed, err := n.Int64(); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

// RequestFingerprintOfBody is the sha256 hex over the exact request bytes —
// the value the DVS computes itself over the raw body. Restated from the repo
// layer (which owns the intent projection) so the runtime package stays
// self-contained; the two definitions are the frozen byte contract.
func RequestFingerprintOfBody(exactBody []byte) string {
	digest := sha256.Sum256(exactBody)
	return hex.EncodeToString(digest[:])
}

// ValidateReceipt is the strict prepared-expectations match. CommitPrepared
// Output applies it internally before reporting success; the executor re
// applies it to receipts fetched via GetCommitReceipt, and the handler layer
// will verify again — a transaction UUID alone is never a usable ACK.
func (p *PreparedCommit) ValidateReceipt(r *DatasetCommitReceipt) error {
	mismatch := func(check string) error {
		return &ReceiptMismatchError{Check: check}
	}
	if r == nil {
		return mismatch("receipt is nil")
	}
	if r.DatasetID != p.DatasetID {
		return mismatch("dataset")
	}
	if r.Branch != p.Branch {
		return mismatch("branch")
	}
	if r.IdempotencyKey != p.Key {
		return mismatch("idempotency_key")
	}
	if r.RequestFingerprint != p.Fingerprint {
		return mismatch("request_fingerprint")
	}
	if r.TransactionID == uuid.Nil {
		return mismatch("transaction_id")
	}
	if strings.TrimSpace(r.Status) != "COMMITTED" {
		return mismatch("status")
	}
	if r.ContentHash != p.ContentHash {
		return mismatch("content_hash")
	}
	if r.Lineage != p.Lineage {
		return mismatch("lineage")
	}
	return nil
}

// validateForSend guards the wire path: the bytes about to leave must be the
// bytes that were fingerprinted (and therefore the bytes that were
// persisted), and the body must still agree with the prepared material —
// verified against the ACTUAL serialized bytes, never against the metadata's
// own declarations (root-decisions-v2.1). The strict receipt match stays the
// second, independent layer on top.
func (p *PreparedCommit) validateForSend() error {
	if p.DatasetID == uuid.Nil {
		return errors.New("recoverable commit requires a target dataset id")
	}
	if err := validateIdempotencyKey(p.Key); err != nil {
		return err
	}
	if strings.TrimSpace(p.Branch) == "" {
		return errors.New("recoverable commit requires an explicit branch")
	}
	if len(p.Body) < 2 || len(p.Body) > maxPreparedCommitBodyBytes {
		return fmt.Errorf("recoverable commit body must be between 2 and %d bytes", maxPreparedCommitBodyBytes)
	}
	if !isHex64(p.Fingerprint) || RequestFingerprintOfBody(p.Body) != p.Fingerprint {
		return errors.New("recoverable commit body no longer matches its request fingerprint; refusing to send drifted bytes")
	}
	if !isHex64(p.ContentHash) {
		return errors.New("recoverable commit requires a 64-lowercase-hex content hash")
	}
	if p.Rows < 0 || p.Bytes < 0 {
		return errors.New("recoverable commit counts must be non-negative")
	}
	if p.Lineage == (DatasetLineageIdentity{}) {
		return errors.New("recoverable commit requires lineage expectations for receipt validation")
	}
	return p.verifyBodyMaterial()
}

// verifyBodyMaterial re-derives the material facts from the exact bytes:
// key, branch, the single inline file (logical path, decoded byte length,
// sha256) and the metadata content_hash/rows/lineage must all agree with the
// prepared fields. A prepared value assembled from the wrong row — or a body
// edited after the fact in a way that preserves the fingerprint — is caught
// here, before anything reaches the wire. Errors never echo body values.
func (p *PreparedCommit) verifyBodyMaterial() error {
	var body struct {
		IdempotencyKey string `json:"idempotency_key"`
		Branch         string `json:"branch"`
		Files          []struct {
			LogicalPath   string `json:"logical_path"`
			SizeBytes     int64  `json:"size_bytes"`
			ContentBase64 []byte `json:"content_base64"`
		} `json:"files"`
		Metadata struct {
			ContentHash string                 `json:"content_hash"`
			Rows        int64                  `json:"rows"`
			Bytes       int64                  `json:"bytes"`
			Lineage     DatasetLineageIdentity `json:"lineage"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(p.Body, &body); err != nil {
		return errors.New("recoverable commit body is not the expected commit request JSON")
	}
	if body.IdempotencyKey != p.Key {
		return errors.New("recoverable commit body does not carry the prepared idempotency key")
	}
	if body.Branch != p.Branch {
		return errors.New("recoverable commit body branch does not match the prepared branch")
	}
	if len(body.Files) != 1 {
		return errors.New("recoverable commit body must carry exactly one inline file")
	}
	if body.Files[0].LogicalPath != p.LogicalPath {
		return errors.New("recoverable commit body logical path does not match the prepared logical path")
	}
	if body.Files[0].SizeBytes != p.Bytes || int64(len(body.Files[0].ContentBase64)) != p.Bytes {
		return errors.New("recoverable commit body file size does not match the decoded content length")
	}
	if digest := sha256.Sum256(body.Files[0].ContentBase64); hex.EncodeToString(digest[:]) != p.ContentHash {
		return errors.New("recoverable commit body content does not hash to the prepared content hash")
	}
	if body.Metadata.ContentHash != p.ContentHash {
		return errors.New("recoverable commit body metadata content hash does not match the prepared content hash")
	}
	if body.Metadata.Rows != p.Rows {
		return errors.New("recoverable commit body metadata rows do not match the prepared row count")
	}
	if body.Metadata.Bytes != p.Bytes {
		return errors.New("recoverable commit body metadata bytes do not match the prepared byte count")
	}
	if body.Metadata.Lineage != p.Lineage {
		return errors.New("recoverable commit body metadata lineage does not match the prepared lineage")
	}
	return nil
}

// CommitPreparedOutput posts the persisted body verbatim. The fresh caller
// authorization travels in the header only. Redirects are not followed: a
// keyed commit is bound to exactly one target endpoint, and a redirect
// (which for 301/302/303 would even downgrade the replay to a GET) must
// surface as a typed dependency failure, never a silent cross-target send.
func (c HTTPDatasetVersioningClient) CommitPreparedOutput(ctx context.Context, prepared PreparedCommit, authorization string) (*DatasetCommitReceipt, error) {
	const op = "commit_prepared_output"
	// De-alias FIRST: fingerprint validation, material verification and the
	// wire reader all operate on one private copy of the bytes. A caller (or
	// an in-flight RoundTripper) mutating the original backing array after
	// the hash check can never change what is actually sent — the validated
	// bytes and the wire bytes are the same array by construction.
	dispatch := prepared
	dispatch.Body = append([]byte(nil), prepared.Body...)
	if err := dispatch.validateForSend(); err != nil {
		return nil, err
	}
	base := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if base == "" {
		return nil, &DependencyError{Op: op, Detail: "url is not configured"}
	}
	httpClient := noRedirectClient(c.Client)
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/api/v1/datasets/%s/outputs:commit", base, dispatch.DatasetID),
		bytes.NewReader(dispatch.Body))
	if err != nil {
		return nil, &DependencyError{Op: op, Detail: SanitizeSyncError(err.Error())}
	}
	hreq.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(authorization) != "" {
		hreq.Header.Set("Authorization", authorization)
	}
	resp, err := httpClient.Do(hreq)
	if err != nil {
		return nil, transportDependencyError(op, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		return nil, ErrDatasetUnauthorized
	case resp.StatusCode == http.StatusForbidden:
		return nil, ErrDatasetForbidden
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrDatasetNotFound
	case resp.StatusCode == http.StatusConflict:
		// Only the typed keyed-path conflict is a PERMANENT mismatch (same
		// key already committed with different bytes). A plain 409 — e.g. an
		// open-transaction/branch conflict from the legacy path — is
		// dependency-busy: unknown outcome, keep owner, retryable per
		// budget. The decision reads ONLY the bounded typed code envelope;
		// the error message itself never surfaces.
		conflict, classificationErr := classifyIdempotencyConflict(resp.Body)
		if classificationErr != nil {
			return nil, &DependencyError{Op: op, Detail: classificationErr.Error()}
		}
		if conflict {
			return nil, ErrCommitIdempotencyConflict
		}
		return nil, &DependencyError{Op: op, Status: http.StatusConflict}
	case resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated:
		// Includes 5xx, unfollowed 3xx and any non-frozen 2xx (202/204): the
		// keyed protocol only defines 200 (replay) and 201 (first commit).
		return nil, &DependencyError{Op: op, Status: resp.StatusCode}
	}
	receipt, err := decodeCommitReceipt(resp.Body)
	if err != nil {
		return nil, &DependencyError{Op: op, Detail: err.Error()}
	}
	if err := prepared.ValidateReceipt(receipt); err != nil {
		return nil, err
	}
	receipt.Replay = resp.StatusCode == http.StatusOK
	return receipt, nil
}

// GetCommitReceipt fetches the immutable receipt saved for the key. The
// response must identify the requested dataset and key; 404 is the typed
// "not committed-visible-yet" answer, never a success and never proof that
// no dispatch is in flight.
func (c HTTPDatasetVersioningClient) GetCommitReceipt(ctx context.Context, datasetID uuid.UUID, key string, authorization string) (*DatasetCommitReceipt, error) {
	const op = "get_commit_receipt"
	if datasetID == uuid.Nil {
		return nil, errors.New("commit receipt lookup requires a target dataset id")
	}
	if err := validateIdempotencyKey(key); err != nil {
		return nil, err
	}
	base := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if base == "" {
		return nil, &DependencyError{Op: op, Detail: "url is not configured"}
	}
	httpClient := noRedirectClient(c.Client)
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/api/v1/datasets/%s/outputs/commits/%s", base, datasetID, key), nil)
	if err != nil {
		return nil, &DependencyError{Op: op, Detail: SanitizeSyncError(err.Error())}
	}
	hreq.Header.Set("Accept", "application/json")
	if strings.TrimSpace(authorization) != "" {
		hreq.Header.Set("Authorization", authorization)
	}
	resp, err := httpClient.Do(hreq)
	if err != nil {
		return nil, transportDependencyError(op, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrCommitReceiptNotFound
	case resp.StatusCode == http.StatusUnauthorized:
		return nil, ErrDatasetUnauthorized
	case resp.StatusCode == http.StatusForbidden:
		return nil, ErrDatasetForbidden
	case resp.StatusCode != http.StatusOK:
		return nil, &DependencyError{Op: op, Status: resp.StatusCode}
	}
	receipt, err := decodeCommitReceipt(resp.Body)
	if err != nil {
		return nil, &DependencyError{Op: op, Detail: err.Error()}
	}
	if receipt.DatasetID != datasetID {
		return nil, &DependencyError{Op: op, Detail: "receipt does not identify the requested dataset"}
	}
	if receipt.IdempotencyKey != key {
		return nil, &DependencyError{Op: op, Detail: "receipt does not carry the requested idempotency key"}
	}
	return receipt, nil
}

// commitReceiptEnvelope is the decode shape of the saved keyed response. It
// keeps the fields the strict match needs; everything else (files, preview,
// schema, lineage links) is deliberately ignored.
type commitReceiptEnvelope struct {
	DatasetID          uuid.UUID `json:"dataset_id"`
	Branch             string    `json:"branch"`
	IdempotencyKey     string    `json:"idempotency_key"`
	RequestFingerprint string    `json:"request_fingerprint"`
	Transaction        struct {
		ID       uuid.UUID       `json:"id"`
		Status   string          `json:"status"`
		Metadata json.RawMessage `json:"metadata"`
	} `json:"transaction"`
}

type commitReceiptMetadata struct {
	ContentHash string                 `json:"content_hash"`
	Lineage     DatasetLineageIdentity `json:"lineage"`
}

// decodeCommitReceipt reads a bounded response body and applies the
// structural half of the contract: a keyed receipt always carries dataset,
// branch, key, fingerprint, a non-nil COMMITTED transaction and the
// content_hash + lineage inside transaction metadata. Anything else is an
// unknown/invalid receipt — a typed dependency failure, never a success.
func decodeCommitReceipt(body io.Reader) (*DatasetCommitReceipt, error) {
	raw, err := io.ReadAll(io.LimitReader(body, maxCommitReceiptBytes+1))
	if err != nil {
		return nil, errors.New(SanitizeSyncError(err.Error()))
	}
	if len(raw) > maxCommitReceiptBytes {
		return nil, errors.New("commit receipt exceeds the size bound")
	}
	var envelope commitReceiptEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, errors.New("commit receipt is not valid JSON")
	}
	if envelope.DatasetID == uuid.Nil {
		return nil, errors.New("commit receipt has no dataset id")
	}
	if strings.TrimSpace(envelope.Branch) == "" {
		return nil, errors.New("commit receipt has no branch")
	}
	if envelope.IdempotencyKey == "" {
		return nil, errors.New("commit receipt has no idempotency key")
	}
	if envelope.RequestFingerprint == "" {
		return nil, errors.New("commit receipt has no request fingerprint")
	}
	if envelope.Transaction.ID == uuid.Nil {
		return nil, errors.New("commit receipt has no transaction id")
	}
	if strings.TrimSpace(envelope.Transaction.Status) != "COMMITTED" {
		return nil, errors.New("commit receipt transaction is not COMMITTED")
	}
	var metadata commitReceiptMetadata
	if len(envelope.Transaction.Metadata) == 0 {
		return nil, errors.New("commit receipt transaction metadata is missing")
	}
	if err := json.Unmarshal(envelope.Transaction.Metadata, &metadata); err != nil {
		return nil, errors.New("commit receipt transaction metadata is malformed")
	}
	if metadata.ContentHash == "" {
		return nil, errors.New("commit receipt metadata has no content hash")
	}
	if metadata.Lineage.SourceID == uuid.Nil || metadata.Lineage.SyncDefID == uuid.Nil || metadata.Lineage.SyncRunID == uuid.Nil {
		return nil, errors.New("commit receipt metadata lineage is incomplete")
	}
	return &DatasetCommitReceipt{
		DatasetID:          envelope.DatasetID,
		Branch:             envelope.Branch,
		IdempotencyKey:     envelope.IdempotencyKey,
		RequestFingerprint: envelope.RequestFingerprint,
		TransactionID:      envelope.Transaction.ID,
		Status:             strings.TrimSpace(envelope.Transaction.Status),
		ContentHash:        metadata.ContentHash,
		Lineage:            metadata.Lineage,
	}, nil
}

// idempotencyConflictCode is the W1-DATASET-INTERFACE typed code for a keyed
// replay whose bytes differ from the committed receipt.
const idempotencyConflictCode = "IDEMPOTENCY_KEY_CONFLICT"

// classifyIdempotencyConflict reads a bounded slice of a 409 body and reports
// whether it carries the typed keyed-conflict code. Unparseable, oversized or
// code-less bodies are NOT treated as permanent conflicts — a misread busy
// 409 must stay an unknown dependency outcome, never a false terminal state.
func classifyIdempotencyConflict(body io.Reader) (bool, error) {
	raw, err := io.ReadAll(io.LimitReader(body, 4096))
	if err != nil {
		return false, errors.New("conflict response body could not be read")
	}
	var envelope struct {
		Code *string `json:"code"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return false, nil // untyped 409: dependency-busy, not a typed conflict
	}
	return envelope.Code != nil && *envelope.Code == idempotencyConflictCode, nil
}

// noRedirectClient shallow-clones the client with redirects disabled. The
// clone shares the transport and timeouts; only the redirect policy differs.
func noRedirectClient(configured *http.Client) *http.Client {
	client := configured
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	clone := *client
	clone.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &clone
}
