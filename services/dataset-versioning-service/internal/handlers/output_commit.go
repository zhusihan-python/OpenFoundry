package handlers

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
	"regexp"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

// idempotencyKeyPattern bounds the keyed-commit key: URL-path safe,
// printable, at most 200 chars. The same pattern guards the GET route's
// {key} parameter.
var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._~:=-]{1,200}$`)

func validIdempotencyKey(key string) bool { return idempotencyKeyPattern.MatchString(key) }

// Keyed-commit specific errors. The first two answer 400 before any
// transaction work; the others classify build failures that must not map
// through the generic transaction error rules.
var (
	errKeyedCreateIfMissing = errors.New("idempotent output commit requires create_if_missing=false")
	errKeyedBranchRequired  = errors.New("idempotent output commit requires an explicit branch")
	errKeyedBranchMissing   = errors.New("branch not found")
	errKeyedLineageTarget   = errors.New("lineage target dataset not found")
)

// keyedFileError marks build failures that map through the output commit
// file-error rules (400/413/503) rather than the transaction rules.
type keyedFileError struct{ err error }

func (e keyedFileError) Error() string { return e.err.Error() }
func (e keyedFileError) Unwrap() error { return e.err }

// keyedObjectError marks physical object-write failures (503 backing
// unavailable / 500) from the keyed path.
type keyedObjectError struct{ err error }

func (e keyedObjectError) Error() string { return e.err.Error() }
func (e keyedObjectError) Unwrap() error { return e.err }

// requestFingerprint is the P2.6 keyed-commit fingerprint: SHA256 hex over
// the exact raw HTTP JSON body — every original byte, including the
// idempotency_key field. Computed only by this service; caller-supplied
// hashes are never trusted.
func requestFingerprint(rawBody []byte) string {
	sum := sha256.Sum256(rawBody)
	return hex.EncodeToString(sum[:])
}

func writeRawJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func strPtr(v string) *string { return &v }

func (h *Handlers) CommitDatasetOutput(w http.ResponseWriter, r *http.Request) {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if !canWriteDataset(claims) {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "forbidden", "required_scope": "dataset.write", "dataset_rid": datasetIDParam(r)})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, h.maxOutputCommitBodyBytes())
	// The decoder streams off the request body through a tee that captures
	// every byte read: a no-key commit keeps the historical semantics —
	// decode the first JSON value, never require the trailing data —
	// exactly like the pre-P2.6 handler, with no up-front whole-body
	// buffering. The keyed single-value gate below must reuse THIS decoder
	// instance: a fresh one would not see a second value sitting in this
	// decoder's read-ahead buffer.
	var captured bytes.Buffer
	tee := io.TeeReader(r.Body, &captured)
	dec := json.NewDecoder(tee)
	var body models.CommitDatasetOutputRequest
	if err := dec.Decode(&body); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSONErr(w, http.StatusRequestEntityTooLarge, "request body exceeds the output commit size limit")
			return
		}
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if body.IdempotencyKey == "" {
		// The staged path exists only as a keyed commit: without a key it
		// must fail closed here, never fall through to the legacy
		// pipeline (whose staged-file CHECK would only fail later, after
		// opening a transaction).
		if stagedCommitRequested(body) {
			writeJSONErr(w, http.StatusBadRequest, "staged commit requires idempotency_key")
			return
		}
		h.commitDatasetOutputLegacy(w, r, body, claims)
		return
	}
	// Keyed: the fingerprint binds ALL original bytes, so the body must be
	// exactly one JSON value followed by nothing. More() cannot express
	// that: at the top level a trailing ']' or '}' reads as "no more
	// elements" and would slip through. A second Decode on the SAME
	// decoder must hit io.EOF — a decoded second value, a syntax error or
	// any trailing garbage is an invalid body, a size overrun is 413. EOF
	// here also means the decoder consumed the whole body, so everything
	// has flowed through the tee and the captured buffer is the complete
	// raw request.
	var extra json.RawMessage
	switch err := dec.Decode(&extra); {
	case errors.Is(err, io.EOF):
		// exactly one JSON value — proceed with the captured raw bytes
	case err == nil:
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	default:
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSONErr(w, http.StatusRequestEntityTooLarge, "request body exceeds the output commit size limit")
			return
		}
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	h.commitDatasetOutputKeyed(w, r, captured.Bytes(), body, claims)
}

// commitDatasetOutputLegacy is the historical non-idempotent outputs:commit:
// create-if-missing resolution, active-branch/main fallback, publish via the
// global Repo with a deferred abort of the transaction on failure. Kept
// byte-for-byte in behavior — a request without idempotency_key must be
// indistinguishable from before P2.6.
func (h *Handlers) commitDatasetOutputLegacy(w http.ResponseWriter, r *http.Request, body models.CommitDatasetOutputRequest, claims *authmw.Claims) {
	dataset, err := h.resolveOrCreateOutputDataset(r, &body, claims)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			writeJSONErr(w, http.StatusNotFound, "dataset not found")
			return
		}
		if repo.IsConflict(err) {
			writeJSONErr(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if dataset == nil {
		writeJSONErr(w, http.StatusNotFound, "dataset not found")
		return
	}
	// Cross-dataset lineage references must stay inside the caller's
	// tenant; a foreign target dataset is rejected like an unknown one
	// before any transaction work begins. Link shape (direction, target)
	// is validated up front too so no deterministic rejection can land
	// after bytes or rows have been written.
	if len(body.LineageLinks) > 0 {
		for _, link := range body.LineageLinks {
			if err := validateLineageLink(link.Direction, link.TargetRID); err != nil {
				writeJSONErr(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		if err := h.validateLineageTargetsInTenant(r.Context(), body.LineageLinks, tenantScopeFromClaims(claims)); err != nil {
			if errors.Is(err, repo.ErrNotFound) {
				writeJSONErr(w, http.StatusNotFound, "lineage target dataset not found")
				return
			}
			writeJSONErr(w, http.StatusInternalServerError, "failed to validate lineage targets")
			return
		}
	}
	if err := h.Repo.EnsureDefaultBranch(r.Context(), dataset); err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to ensure default branch")
		return
	}
	branchName := strings.TrimSpace(body.Branch)
	if branchName == "" {
		branchName = dataset.ActiveBranch
	}
	if branchName == "" {
		branchName = "main"
	}
	branch, err := h.Repo.GetRuntimeBranch(r.Context(), dataset.ID, branchName)
	if err != nil || branch == nil {
		if branchName != "main" {
			if fallback, fallbackErr := h.Repo.GetRuntimeBranch(r.Context(), dataset.ID, "main"); fallbackErr == nil && fallback != nil {
				branchName = "main"
				branch = fallback
				err = nil
			}
		}
	}
	if err != nil || branch == nil {
		writeBranchError(w, firstErr(err, repo.ErrNotFound))
		return
	}

	txType := body.TransactionType
	if txType == "" {
		txType = models.TransactionTypeSnapshot
	}
	summary := strings.TrimSpace(body.Summary)
	if summary == "" {
		summary = "Pipeline dataset output commit"
	}
	provenance := body.Provenance
	if len(provenance) == 0 {
		provenance = models.JSONValue([]byte(`{"source":"pipeline-build-service"}`))
	}
	txn, err := h.Repo.StartTransaction(r.Context(), dataset.ID, branch.ID, branch.Name, txType, summary, provenance, claims.Sub)
	if err != nil {
		writeTransactionError(w, err)
		return
	}
	committed := false
	defer func() {
		if !committed {
			_ = h.Repo.AbortTransaction(r.Context(), dataset.ID, txn.ID)
		}
	}()

	backing, err := h.outputCommitBackingFor(commitBodyHasContent(body))
	if err != nil {
		writeOutputCommitFileError(w, err)
		return
	}
	staged, indexEntries, writes, err := outputCommitFiles(dataset.ID, txn.ID, body, backing)
	if err != nil {
		writeOutputCommitFileError(w, err)
		return
	}
	// Caller-supplied storage paths may only reference objects inside
	// this dataset's namespace: the commit's own transaction is always
	// allowed, earlier transactions of the same dataset resolve through
	// the store, and other datasets' namespaces are rejected. The base
	// dir comes from the configured backing store — content-free commits
	// skip the backing setup, but their paths live in the same tree.
	commitBaseDir := backing.baseDir
	if commitBaseDir == "" && h.BackingFS != nil {
		commitBaseDir = h.BackingFS.BaseDirectory()
	}
	ownTxn := h.datasetTxnOwner(r.Context(), dataset.ID)
	for _, stagedFile := range staged {
		if err := h.validateStorageRefForDataset(r.Context(), stagedFile.PhysicalPath, commitBaseDir, dataset.ID, &txn.ID, ownTxn); err != nil {
			writeOutputCommitFileError(w, err)
			return
		}
	}
	for _, entry := range indexEntries {
		if err := h.validateStorageRefForDataset(r.Context(), entry.StoragePath, commitBaseDir, dataset.ID, &txn.ID, ownTxn); err != nil {
			writeOutputCommitFileError(w, err)
			return
		}
	}
	if err := h.writeOutputCommitObjects(writes); err != nil {
		writeOutputCommitObjectError(w, err)
		return
	}
	if err := h.Repo.StageTransactionFiles(r.Context(), dataset.ID, txn.ID, staged); err != nil {
		writeTransactionError(w, err)
		return
	}
	metadataPatch, err := outputCommitMetadata(body, len(indexEntries))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.Repo.MergeTransactionMetadata(r.Context(), dataset.ID, txn.ID, metadataPatch); err != nil {
		writeTransactionError(w, err)
		return
	}
	// The version cut, file-index publication, lineage publication and
	// every response read land in one database transaction, and the
	// method returns an in-memory snapshot of the response. A failure in
	// any publish or snapshot step rolls the whole thing back, so no
	// half-published version is ever visible, the previous snapshot stays
	// authoritative, and a failed request can never describe an already
	// published version. On rollback the transaction row survives as
	// OPEN, so the deferred abort above then finalises it as ABORTED —
	// no OPEN leftovers.
	snapshot, err := h.Repo.CommitTransactionWithPublish(r.Context(), dataset.ID, txn.ID, indexEntries, body.LineageLinks)
	if err != nil {
		writeTransactionError(w, err)
		return
	}
	committed = true
	writeJSON(w, http.StatusCreated, models.CommitDatasetOutputResponse{
		DatasetID:    dataset.ID,
		DatasetRID:   "ri.foundry.main.dataset." + dataset.ID.String(),
		Branch:       branch.Name,
		Transaction:  snapshot.Transaction,
		Schema:       body.Schema,
		Files:        snapshot.Files,
		Preview:      snapshot.Preview,
		LineageLinks: snapshot.LineageLinks,
	})
}

// commitDatasetOutputKeyed is the P2.6 idempotent outputs:commit. The
// service fingerprints the exact raw body, then runs one outer transaction
// (see repo.WithIdempotentOutputCommit): a committed receipt with the same
// fingerprint replays the stored immutable response bytes (200) without
// touching branch/backing/lineage or head/index; a different fingerprint
// under the same key is a typed 409 with zero side effects; otherwise the
// full Start/Stage/Merge/Publish pipeline runs inside that transaction and
// the receipt becomes durable in its single COMMIT (201). Any failure rolls
// the whole thing back — no OPEN transaction, staged rows or receipt
// survive; only the attempt's unreferenced physical objects may orphan
// under its fresh random txnUUID.
func (h *Handlers) commitDatasetOutputKeyed(w http.ResponseWriter, r *http.Request, raw []byte, body models.CommitDatasetOutputRequest, claims *authmw.Claims) {
	key := body.IdempotencyKey
	if !validIdempotencyKey(key) {
		writeJSONErr(w, http.StatusBadRequest, "invalid idempotency_key")
		return
	}
	// Deterministic, body-intrinsic validations run before any transaction:
	// they are pure functions of the bytes, so a byte-identical replay can
	// never fail them after the fingerprint check short-circuits.
	if body.CreateIfMissing {
		writeJSONErr(w, http.StatusBadRequest, errKeyedCreateIfMissing.Error())
		return
	}
	branchName := strings.TrimSpace(body.Branch)
	if branchName == "" {
		writeJSONErr(w, http.StatusBadRequest, errKeyedBranchRequired.Error())
		return
	}
	// Staged commits carry their own deterministic shape contract; it is a
	// pure function of the bytes, so a byte-identical replay can never
	// fail it after the fingerprint short-circuit.
	if stagedCommitRequested(body) {
		if err := validateStagedCommitRequest(body); err != nil {
			writeJSONErr(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	for _, link := range body.LineageLinks {
		if err := validateLineageLink(link.Direction, link.TargetRID); err != nil {
			writeJSONErr(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	// Keyed commits never create the dataset: the reference must resolve to
	// an existing dataset id, which the outer transaction's tenant-scoped
	// row lock then verifies.
	datasetID, err := outputDatasetIDFromRID(datasetIDParam(r))
	if err != nil {
		writeJSONErr(w, http.StatusNotFound, "dataset not found")
		return
	}
	scope := tenantScopeFromClaims(claims)
	fingerprint := requestFingerprint(raw)
	var build func(context.Context, repo.OutputCommitTxStore) (models.CommitDatasetOutputResponse, error)
	if stagedCommitRequested(body) {
		build = h.stagedKeyedOutputCommitBuild(r, body, branchName, datasetID, scope, claims)
	} else {
		build = h.keyedOutputCommitBuild(r, body, branchName, datasetID, scope, claims)
	}
	receipt, replayed, err := h.Repo.WithIdempotentOutputCommit(r.Context(), scope, datasetID, key, fingerprint, build)
	if err != nil {
		writeKeyedCommitError(w, err)
		return
	}
	status := http.StatusCreated
	if replayed {
		status = http.StatusOK
	}
	// Serve the stored bytes verbatim — first response, replay and GET all
	// return this exact serialization.
	writeRawJSON(w, status, receipt.ResponseBody)
}

// keyedOutputCommitBuild assembles the transaction-scoped build callback:
// every store call goes through the tx view handed in (never the global
// Repo), physical object writes use this attempt's fresh txnUUID, and the
// returned response is assembled purely from the in-memory publish snapshot
// so no post-commit read can fail.
func (h *Handlers) keyedOutputCommitBuild(r *http.Request, body models.CommitDatasetOutputRequest, branchName string, datasetID uuid.UUID, scope repo.TenantScope, claims *authmw.Claims) func(context.Context, repo.OutputCommitTxStore) (models.CommitDatasetOutputResponse, error) {
	txType := body.TransactionType
	if txType == "" {
		txType = models.TransactionTypeSnapshot
	}
	summary := strings.TrimSpace(body.Summary)
	if summary == "" {
		summary = "Pipeline dataset output commit"
	}
	provenance := body.Provenance
	if len(provenance) == 0 {
		provenance = models.JSONValue([]byte(`{"source":"pipeline-build-service"}`))
	}
	rawRef := datasetIDParam(r)
	return func(ctx context.Context, tx repo.OutputCommitTxStore) (models.CommitDatasetOutputResponse, error) {
		var zero models.CommitDatasetOutputResponse
		// Lineage targets must stay inside the caller's tenant — checked on
		// the transaction's snapshot so a rejected link can never land
		// after bytes or rows have been written.
		if len(body.LineageLinks) > 0 {
			if err := validateLineageTargetsInTx(ctx, tx, body.LineageLinks, scope); err != nil {
				if errors.Is(err, repo.ErrNotFound) {
					return zero, errKeyedLineageTarget
				}
				return zero, err
			}
		}
		dataset, err := tx.ResolveDatasetForTenant(ctx, rawRef, scope)
		if err != nil {
			return zero, err
		}
		if dataset == nil {
			return zero, repo.ErrNotFound
		}
		if err := tx.EnsureDefaultBranch(ctx, dataset); err != nil {
			return zero, err
		}
		// Explicit branch only: no active-branch/main fallback — a stale
		// active-branch change never redirects a keyed commit, and a missing
		// branch is a plain 404. GetRuntimeBranch reports a missing branch
		// as (nil, ErrNotFound), which must not surface as the generic
		// dataset-not-found.
		branch, err := tx.GetRuntimeBranch(ctx, dataset.ID, branchName)
		if err != nil && !errors.Is(err, repo.ErrNotFound) {
			return zero, err
		}
		if branch == nil {
			return zero, errKeyedBranchMissing
		}
		txn, err := tx.StartTransaction(ctx, dataset.ID, branch.ID, branch.Name, txType, summary, provenance, claims.Sub)
		if err != nil {
			return zero, err
		}
		backing, err := h.outputCommitBackingFor(commitBodyHasContent(body))
		if err != nil {
			return zero, keyedFileError{err}
		}
		staged, indexEntries, writes, ferr := outputCommitFiles(dataset.ID, txn.ID, body, backing)
		if ferr != nil {
			return zero, keyedFileError{ferr}
		}
		commitBaseDir := backing.baseDir
		if commitBaseDir == "" && h.BackingFS != nil {
			commitBaseDir = h.BackingFS.BaseDirectory()
		}
		for _, stagedFile := range staged {
			if err := h.validateStorageRefInTx(ctx, tx, stagedFile.PhysicalPath, commitBaseDir, dataset.ID, &txn.ID); err != nil {
				return zero, keyedFileError{err}
			}
		}
		for _, entry := range indexEntries {
			if err := h.validateStorageRefInTx(ctx, tx, entry.StoragePath, commitBaseDir, dataset.ID, &txn.ID); err != nil {
				return zero, keyedFileError{err}
			}
		}
		if err := h.writeOutputCommitObjects(writes); err != nil {
			return zero, keyedObjectError{err}
		}
		if err := tx.StageTransactionFiles(ctx, dataset.ID, txn.ID, staged); err != nil {
			return zero, err
		}
		metadataPatch, err := outputCommitMetadata(body, len(indexEntries))
		if err != nil {
			return zero, keyedFileError{err}
		}
		if err := tx.MergeTransactionMetadata(ctx, dataset.ID, txn.ID, metadataPatch); err != nil {
			return zero, err
		}
		// Publish and response snapshot inside the same outer transaction
		// (a SAVEPOINT under it); any failure rolls everything back.
		snapshot, err := tx.CommitTransactionWithPublish(ctx, dataset.ID, txn.ID, indexEntries, body.LineageLinks)
		if err != nil {
			return zero, err
		}
		return models.CommitDatasetOutputResponse{
			DatasetID:    dataset.ID,
			DatasetRID:   "ri.foundry.main.dataset." + dataset.ID.String(),
			Branch:       branch.Name,
			Transaction:  snapshot.Transaction,
			Schema:       body.Schema,
			Files:        snapshot.Files,
			Preview:      snapshot.Preview,
			LineageLinks: snapshot.LineageLinks,
		}, nil
	}
}

// validateLineageTargetsInTx is validateLineageTargetsInTenant over the
// keyed commit's transaction-scoped store view.
func validateLineageTargetsInTx(ctx context.Context, tx repo.OutputCommitTxStore, links []models.PutDatasetLineageLink, scope repo.TenantScope) error {
	for _, l := range links {
		kind := "dataset"
		if l.TargetKind != nil && *l.TargetKind != "" {
			kind = *l.TargetKind
		}
		if kind != "dataset" {
			continue
		}
		target, err := tx.ResolveDatasetForTenant(ctx, l.TargetRID, scope)
		if err != nil {
			return err
		}
		if target == nil {
			return repo.ErrNotFound
		}
	}
	return nil
}

// validateStorageRefInTx is validateStorageRefForDataset with the
// transaction-ownership probe answered by the tx view, so the check sees
// the same snapshot the commit writes to.
func (h *Handlers) validateStorageRefInTx(ctx context.Context, tx repo.OutputCommitTxStore, ref string, baseDir string, datasetID uuid.UUID, currentTxn *uuid.UUID) error {
	return h.validateStorageRefForDataset(ctx, ref, baseDir, datasetID, currentTxn, func(txnID uuid.UUID) bool {
		_, found, err := tx.GetTransactionStatus(ctx, datasetID, txnID)
		return err == nil && found
	})
}

// GetDatasetOutputCommit serves GET /datasets/{rid}/outputs/commits/{key}:
// the stored immutable response bytes of the first committed keyed commit.
// A 404 only reports that no committed receipt is currently visible for
// (dataset, key) in the caller's tenant — it does not prove that no
// operation is in flight.
func (h *Handlers) GetDatasetOutputCommit(w http.ResponseWriter, r *http.Request) {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	key := chi.URLParam(r, "key")
	if !validIdempotencyKey(key) {
		writeJSONErr(w, http.StatusBadRequest, "invalid idempotency_key")
		return
	}
	datasetID, err := outputDatasetIDFromRID(datasetIDParam(r))
	if err != nil {
		writeJSONErr(w, http.StatusNotFound, "dataset not found")
		return
	}
	receipt, err := h.Repo.GetOutputCommitReceipt(r.Context(), tenantScopeFromClaims(claims), datasetID, key)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if receipt == nil {
		writeJSONErr(w, http.StatusNotFound, "output commit receipt not found")
		return
	}
	writeRawJSON(w, http.StatusOK, receipt.ResponseBody)
}

// writeKeyedCommitError maps keyed-path failures to their HTTP contracts.
func writeKeyedCommitError(w http.ResponseWriter, err error) {
	var fileErr keyedFileError
	var objectErr keyedObjectError
	if writeStagedCommitError(w, err) {
		return
	}
	switch {
	case errors.Is(err, repo.ErrIdempotencyKeyConflict):
		writeJSON(w, http.StatusConflict, models.ErrorEnvelope{Error: err.Error(), Code: strPtr("IDEMPOTENCY_KEY_CONFLICT")})
	case errors.Is(err, repo.ErrCommitOutcomeUnknown):
		writeJSON(w, http.StatusServiceUnavailable, models.ErrorEnvelope{Error: repo.ErrCommitOutcomeUnknown.Error(), Code: strPtr("OUTCOME_UNKNOWN"), RetryAfter: int64Ptr(1)})
	case errors.Is(err, errKeyedBranchMissing):
		writeJSONErr(w, http.StatusNotFound, errKeyedBranchMissing.Error())
	case errors.Is(err, errKeyedLineageTarget):
		writeJSONErr(w, http.StatusNotFound, errKeyedLineageTarget.Error())
	case errors.As(err, &fileErr):
		writeOutputCommitFileError(w, fileErr.err)
	case errors.As(err, &objectErr):
		writeOutputCommitObjectError(w, objectErr.err)
	case errors.Is(err, repo.ErrNotFound):
		writeJSONErr(w, http.StatusNotFound, "dataset not found")
	default:
		writeTransactionError(w, err)
	}
}

func int64Ptr(v int64) *int64 { return &v }

// resolveOrCreateOutputDataset resolves the target dataset under the
// caller's tenant scope. An existing dataset in another organization is
// indistinguishable from a missing one (ErrNotFound), and create_if_missing
// never resurrects it: a dataset id that exists globally but outside the
// caller's tenant is rejected instead of created-or-overwritten. Newly
// created datasets inherit the caller's org (or personal domain) from the
// verified claims.
func (h *Handlers) resolveOrCreateOutputDataset(r *http.Request, body *models.CommitDatasetOutputRequest, claims *authmw.Claims) (*models.Dataset, error) {
	raw := datasetIDParam(r)
	scope := repo.TenantScope{OrgID: claims.OrgID, Subject: claims.Sub}
	dataset, err := h.Repo.ResolveDatasetForTenant(r.Context(), raw, scope)
	if err == nil && dataset != nil {
		return dataset, nil
	}
	if err != nil && !errors.Is(err, repo.ErrNotFound) {
		return nil, err
	}
	if !body.CreateIfMissing {
		return nil, repo.ErrNotFound
	}
	id, parseErr := outputDatasetIDFromRID(raw)
	if parseErr != nil {
		return nil, repo.ErrNotFound
	}
	// The reference resolves in no tenant-visible dataset. If it exists
	// globally it belongs to another tenant: 404 without attempting the
	// insert (which would otherwise surface as a PK conflict and leak
	// existence).
	if exists, exErr := h.Repo.DatasetExists(r.Context(), id); exErr != nil {
		return nil, exErr
	} else if exists {
		return nil, repo.ErrNotFound
	}
	name := strings.TrimSpace(body.DatasetName)
	if name == "" {
		name = "Pipeline output " + id.String()[:8]
	}
	format := "parquet"
	if body.Format != nil && strings.TrimSpace(*body.Format) != "" {
		format = strings.TrimSpace(*body.Format)
	}
	health := "unknown"
	return h.Repo.CreateDatasetForTenant(r.Context(), &models.CreateDatasetRequest{ID: &id, Name: name, Description: body.Description, Format: &format, HealthStatus: &health, Metadata: body.Metadata}, claims.Sub, claims.OrgID)
}

func outputDatasetIDFromRID(raw string) (uuid.UUID, error) {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimPrefix(trimmed, "ri.foundry.main.dataset.")
	return uuid.Parse(trimmed)
}

func commitBodyHasContent(body models.CommitDatasetOutputRequest) bool {
	for _, file := range body.Files {
		if file.ContentBase64 != nil {
			return true
		}
	}
	return false
}

func outputCommitFiles(datasetID uuid.UUID, txnID uuid.UUID, body models.CommitDatasetOutputRequest, backing outputCommitBacking) ([]models.StageTransactionFile, []models.PutDatasetFileIndexEntry, []outputObjectWrite, error) {
	now := time.Now().UTC()
	files := body.Files
	if len(files) == 0 {
		files = []models.CommitOutputFile{{LogicalPath: "part-00000.ndjson", SizeBytes: int64(len(body.PreviewRows))}}
	}
	staged := make([]models.StageTransactionFile, 0, len(files))
	indexEntries := make([]models.PutDatasetFileIndexEntry, 0, len(files))
	writes := make([]outputObjectWrite, 0, len(files))
	enforceUniquePaths := commitBodyHasContent(body)
	seenPaths := make(map[string]struct{}, len(files))
	var contentBytes int64
	for _, file := range files {
		logical := strings.Trim(strings.TrimSpace(file.LogicalPath), "/")
		if logical == "" {
			return nil, nil, nil, errString("logical_path is required")
		}
		op := file.Operation
		if op == "" {
			op = models.FileOperationAdd
		}
		if enforceUniquePaths {
			if _, dup := seenPaths[logical]; dup {
				return nil, nil, nil, errString("duplicate logical_path in output commit: " + logical)
			}
			seenPaths[logical] = struct{}{}
		}
		storagePath := strings.TrimSpace(file.StoragePath)
		if storagePath == "" {
			storagePath = "outputs/" + txnID.String() + "/" + logical
		}
		indexStoragePath := storagePath
		if file.ContentBase64 != nil {
			if op != models.FileOperationAdd {
				return nil, nil, nil, errString("content_base64 requires operation ADD, got " + string(op))
			}
			if !safeObjectKey(logical) {
				return nil, nil, nil, errString("invalid logical_path for content-bearing file")
			}
			if file.SizeBytes != int64(len(file.ContentBase64)) {
				return nil, nil, nil, errString("size_bytes must match content_base64 length when content is provided")
			}
			contentBytes += int64(len(file.ContentBase64))
			if contentBytes > backing.limit {
				return nil, nil, nil, fmt.Errorf("%w: %d bytes exceeds %d", errOutputCommitContentTooLarge, contentBytes, backing.limit)
			}
			// The server owns the physical location for content-bearing
			// files: the caller's storage_path is ignored so requests can
			// never direct writes at arbitrary backend paths.
			storagePath = outputCommitObjectKey(backing.baseDir, datasetID, txnID, logical)
			indexStoragePath = physicalURIForOutputObject(backing.fsid, storagePath)
			writes = append(writes, outputObjectWrite{Key: storagePath, Content: file.ContentBase64})
		}
		entryType := models.FileEntryTypeFile
		if err := validateFileIndexEntry(logical, storagePath, entryType, file.SizeBytes); err != nil {
			return nil, nil, nil, err
		}
		meta, err := outputFileMetadata(datasetID, txnID, file, body)
		if err != nil {
			return nil, nil, nil, err
		}
		staged = append(staged, models.StageTransactionFile{LogicalPath: logical, PhysicalPath: storagePath, SizeBytes: file.SizeBytes, Operation: op})
		indexEntries = append(indexEntries, models.PutDatasetFileIndexEntry{Path: logical, StoragePath: indexStoragePath, EntryType: &entryType, SizeBytes: &file.SizeBytes, ContentType: file.ContentType, Metadata: meta, LastModified: &now})
	}
	return staged, indexEntries, writes, nil
}

func outputFileMetadata(datasetID uuid.UUID, txnID uuid.UUID, file models.CommitOutputFile, body models.CommitDatasetOutputRequest) (models.JSONValue, error) {
	meta := map[string]any{}
	if len(file.Metadata) > 0 {
		if err := json.Unmarshal(file.Metadata, &meta); err != nil {
			return nil, err
		}
	}
	meta["dataset_id"] = datasetID.String()
	meta["transaction_id"] = txnID.String()
	meta["preview_columns"] = body.PreviewColumns
	meta["preview_rows"] = body.PreviewRows
	if body.Schema != nil {
		meta["schema"] = body.Schema
	}
	raw, err := json.Marshal(meta)
	return models.JSONValue(raw), err
}

func outputCommitMetadata(body models.CommitDatasetOutputRequest, fileCount int) (models.JSONValue, error) {
	meta := map[string]any{"pipeline_output": true}
	if len(body.Metadata) > 0 {
		if err := json.Unmarshal(body.Metadata, &meta); err != nil {
			return nil, err
		}
		meta["pipeline_output"] = true
	}
	if body.Schema != nil {
		meta["schema"] = body.Schema
	}
	meta["preview_columns"] = body.PreviewColumns
	meta["preview_rows"] = body.PreviewRows
	meta["file_count"] = fileCount
	raw, err := json.Marshal(meta)
	return models.JSONValue(raw), err
}

func firstErr(err error, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}
