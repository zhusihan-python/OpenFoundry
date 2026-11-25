// Package handlers wires the HTTP endpoints for dataset-versioning-service.
package handlers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

type Store interface {
	ListDatasets(ctx context.Context, ownerID *uuid.UUID, limit int) ([]models.Dataset, error)
	ListDatasetsForTenant(ctx context.Context, scope repo.TenantScope, ownerID *uuid.UUID, limit int) ([]models.Dataset, error)
	GetDataset(ctx context.Context, id uuid.UUID) (*models.Dataset, error)
	GetDatasetForTenant(ctx context.Context, id uuid.UUID, scope repo.TenantScope) (*models.Dataset, error)
	GetDatasetForOwner(ctx context.Context, id uuid.UUID, ownerID uuid.UUID) (*models.Dataset, error)
	ResolveDatasetForTenant(ctx context.Context, raw string, scope repo.TenantScope) (*models.Dataset, error)
	GetCatalogFacets(ctx context.Context) (*models.CatalogFacets, error)
	GetCatalogFacetsForTenant(ctx context.Context, scope repo.TenantScope) (*models.CatalogFacets, error)
	GetInternalDatasetMetadata(ctx context.Context, datasetID uuid.UUID) (*models.InternalDatasetMetadata, error)
	CreateDataset(ctx context.Context, body *models.CreateDatasetRequest, ownerID uuid.UUID) (*models.Dataset, error)
	CreateDatasetForTenant(ctx context.Context, body *models.CreateDatasetRequest, ownerID uuid.UUID, orgID *uuid.UUID) (*models.Dataset, error)
	UpdateDataset(ctx context.Context, id uuid.UUID, body *models.UpdateDatasetRequest) (*models.Dataset, error)
	DeleteDataset(ctx context.Context, id uuid.UUID) (bool, error)
	ListVersions(ctx context.Context, datasetID uuid.UUID) ([]models.DatasetVersion, error)
	GetVersion(ctx context.Context, datasetID uuid.UUID, version int32) (*models.DatasetVersion, error)
	GetPublishedVersionAt(ctx context.Context, datasetID uuid.UUID, branch string, at time.Time) (int32, error)
	CreateVersion(ctx context.Context, datasetID uuid.UUID, body *models.CreateDatasetVersionRequest) (*models.DatasetVersion, error)
	EnsureDefaultBranch(ctx context.Context, dataset *models.Dataset) error
	ListBranches(ctx context.Context, datasetID uuid.UUID) ([]models.DatasetBranch, error)
	GetBranch(ctx context.Context, datasetID uuid.UUID, name string) (*models.DatasetBranch, error)
	CreateBranch(ctx context.Context, dataset *models.Dataset, body *models.CreateDatasetBranchRequest) (*models.DatasetBranch, error)
	ListFiles(ctx context.Context, datasetID uuid.UUID, branch string, prefix string) ([]models.DatasetFile, error)
	ListTransactionFiles(ctx context.Context, datasetID uuid.UUID, transactionID uuid.UUID, prefix string) ([]models.DatasetFile, error)
	GetFile(ctx context.Context, datasetID uuid.UUID, fileID uuid.UUID) (*models.DatasetFile, error)
	GetTransactionStatus(ctx context.Context, datasetID uuid.UUID, transactionID uuid.UUID) (string, bool, error)
	ResolveDatasetID(ctx context.Context, raw string) (uuid.UUID, error)
	DatasetExists(ctx context.Context, datasetID uuid.UUID) (bool, error)
	DatasetViewBelongsToDataset(ctx context.Context, datasetID uuid.UUID, viewID uuid.UUID) (bool, error)
	GetDatasetViewOwner(ctx context.Context, viewID uuid.UUID) (*uuid.UUID, error)
	GetCatalogDataset(ctx context.Context, datasetID uuid.UUID) (*models.CatalogDataset, error)
	GetDatasetRichModel(ctx context.Context, datasetID uuid.UUID) (*models.DatasetRichModel, error)
	PatchDatasetMetadata(ctx context.Context, datasetID uuid.UUID, body *models.DatasetMetadataPatch) (*models.CatalogDataset, error)
	ListDatasetMarkings(ctx context.Context, datasetID uuid.UUID) ([]models.EffectiveMarking, error)
	ReplaceDatasetMarkings(ctx context.Context, datasetID uuid.UUID, markings []uuid.UUID, appliedBy uuid.UUID) error
	ListDatasetPermissions(ctx context.Context, datasetID uuid.UUID) ([]models.DatasetPermissionEdge, error)
	ReplaceDatasetPermissions(ctx context.Context, datasetID uuid.UUID, permissions []models.PutDatasetPermissionEdge) error
	ListDatasetLineageLinks(ctx context.Context, datasetID uuid.UUID) ([]models.DatasetLineageLink, error)
	ReplaceDatasetLineageLinks(ctx context.Context, datasetID uuid.UUID, links []models.PutDatasetLineageLink) error
	ListDatasetFileIndex(ctx context.Context, datasetID uuid.UUID) ([]models.DatasetFileIndexEntry, error)
	ReplaceDatasetFileIndex(ctx context.Context, datasetID uuid.UUID, files []models.PutDatasetFileIndexEntry) error
	ReplaceDatasetFileIndexWithStats(ctx context.Context, datasetID uuid.UUID, files []models.PutDatasetFileIndexEntry, rowCount, sizeBytes int64) error
	ListActiveRuntimeBranches(ctx context.Context, datasetID uuid.UUID) ([]models.RuntimeBranch, error)
	CreateRuntimeBranch(ctx context.Context, datasetID uuid.UUID, body *models.CreateBranchBody, actor uuid.UUID) (*models.RuntimeBranch, error)
	GetRuntimeBranch(ctx context.Context, datasetID uuid.UUID, branch string) (*models.RuntimeBranch, error)
	PreviewDeleteBranch(ctx context.Context, datasetID uuid.UUID, branch string) (*models.BranchDeletePreview, error)
	DeleteRuntimeBranch(ctx context.Context, datasetID uuid.UUID, branch string) (*models.BranchDeleteResponse, error)
	ReparentRuntimeBranch(ctx context.Context, datasetID uuid.UUID, branch string, newParent *string) (*models.RuntimeBranch, error)
	BranchAncestry(ctx context.Context, datasetID uuid.UUID, branch string) ([]models.RuntimeBranch, error)
	UpdateBranchRetention(ctx context.Context, datasetID uuid.UUID, branch string, policy models.RetentionPolicy, ttlDays *int32) (*models.RuntimeBranch, error)
	RestoreBranch(ctx context.Context, datasetID uuid.UUID, branch string) (*models.RuntimeBranch, error)
	ListBranchMarkings(ctx context.Context, branchID uuid.UUID) ([]models.BranchMarking, error)
	CompareBranches(ctx context.Context, datasetID uuid.UUID, base string, compare string) (*models.BranchCompareResponse, error)
	RollbackBranch(ctx context.Context, datasetID uuid.UUID, branch string, body *models.RollbackBody, actor uuid.UUID) (map[string]any, error)
	ListFallbacks(ctx context.Context, branchID uuid.UUID) ([]models.RuntimeFallbackEntry, error)
	ReplaceFallbacks(ctx context.Context, branchID uuid.UUID, fallbackNames []string) error
	ListViews(ctx context.Context, datasetID uuid.UUID) ([]models.DatasetView, error)
	CreateView(ctx context.Context, datasetID uuid.UUID, body *models.CreateDatasetViewRequest) (*models.DatasetView, error)
	GetDatasetView(ctx context.Context, datasetID uuid.UUID, viewOrName string) (*models.DatasetView, error)
	RefreshDatasetView(ctx context.Context, datasetID uuid.UUID, viewOrName string) (*models.DatasetView, error)
	GetCurrentView(ctx context.Context, datasetID uuid.UUID, branch string) (*models.ViewOut, error)
	GetViewAt(ctx context.Context, datasetID uuid.UUID, branch string, at *time.Time, transactionID *uuid.UUID) (*models.ViewOut, error)
	CompareViews(ctx context.Context, datasetID uuid.UUID, baseBranch string, targetBranch string, baseTransaction *uuid.UUID, targetTransaction *uuid.UUID) (*models.CompareOut, error)
	ListViewFiles(ctx context.Context, datasetID uuid.UUID, viewID uuid.UUID) ([]models.RuntimeViewFile, error)
	GetViewSchema(ctx context.Context, viewID uuid.UUID) (*models.SchemaResponse, error)
	PutViewSchema(ctx context.Context, viewID uuid.UUID, datasetID uuid.UUID, branch *string, schema models.DatasetSchema, contentHash string) (*models.SchemaResponse, error)
	GetCurrentSchema(ctx context.Context, datasetID uuid.UUID, branch string) (*models.SchemaResponse, error)
	PreviewData(ctx context.Context, datasetID uuid.UUID, viewID *uuid.UUID, q models.PreviewQuery) (*models.PreviewDataResponse, error)
	ValidateSchema(ctx context.Context, datasetID uuid.UUID, schema models.DatasetSchema) (*models.ValidateResponse, error)
	StorageDetails(ctx context.Context, datasetID uuid.UUID, fsID string, driver string, baseDir string, ttlSeconds uint64) (*models.StorageDetailsOut, error)
	StartTransaction(ctx context.Context, datasetID uuid.UUID, branchID uuid.UUID, branchName string, txType models.TransactionType, summary string, providence models.JSONValue, startedBy uuid.UUID) (*models.RuntimeTransaction, error)
	StageTransactionFiles(ctx context.Context, datasetID uuid.UUID, transactionID uuid.UUID, files []models.StageTransactionFile) error
	GetTransactionStagedFile(ctx context.Context, transactionID uuid.UUID, logicalPath string) (*models.StageTransactionFile, error)
	ListTransactionStagedFilesFrom(ctx context.Context, transactionID uuid.UUID, fromPath string, limit int) ([]models.StageTransactionFile, error)
	TransactionStagedFileFormats(ctx context.Context, transactionID uuid.UUID) (hasParquet, hasOther, allDelimited, allNDJSON bool, err error)
	MergeTransactionMetadata(ctx context.Context, datasetID uuid.UUID, transactionID uuid.UUID, metadata models.JSONValue) error
	GetRuntimeTransaction(ctx context.Context, datasetID uuid.UUID, txnID uuid.UUID) (*models.RuntimeTransaction, error)
	ListRuntimeTransactions(ctx context.Context, datasetID uuid.UUID, branch *string, before *time.Time, limit int) ([]models.RuntimeTransaction, error)
	CommitTransaction(ctx context.Context, datasetID uuid.UUID, txnID uuid.UUID) error
	CommitTransactionWithPublish(ctx context.Context, datasetID uuid.UUID, txnID uuid.UUID, files []models.PutDatasetFileIndexEntry, links []models.PutDatasetLineageLink) (*repo.PublishSnapshot, error)
	AbortTransaction(ctx context.Context, datasetID uuid.UUID, txnID uuid.UUID) error
	WithIdempotentOutputCommit(ctx context.Context, scope repo.TenantScope, datasetID uuid.UUID, key string, fingerprint string, build func(ctx context.Context, tx repo.OutputCommitTxStore) (models.CommitDatasetOutputResponse, error)) (*models.OutputCommitReceipt, bool, error)
	GetOutputCommitReceipt(ctx context.Context, scope repo.TenantScope, datasetID uuid.UUID, key string) (*models.OutputCommitReceipt, error)
	GetTransactionOutputCommit(ctx context.Context, datasetID, transactionID uuid.UUID, branch string) (*models.CommitDatasetOutputResponse, error)
	AcquireStagedUploadLease(ctx context.Context, scope repo.TenantScope, binding repo.StagedBinding, attemptID uuid.UUID, leaseTTL time.Duration) (*repo.StagedOutputObject, *uuid.UUID, bool, error)
	SealStagedOutput(ctx context.Context, scope repo.TenantScope, binding repo.StagedBinding, attemptID uuid.UUID, sizeBytes int64, sha256Hex string, recordCount int64, sealedPath string, ttl time.Duration) (*repo.StagedOutputObject, error)
	ReleaseStagedLease(ctx context.Context, attemptID uuid.UUID) error
	GetDatasetQuality(ctx context.Context, datasetID uuid.UUID) (*models.DatasetQualityResponse, error)
	UpsertQualityRule(ctx context.Context, datasetID uuid.UUID, body *models.CreateQualityRuleRequest) (*models.DatasetQualityRule, error)
	UpdateQualityRule(ctx context.Context, datasetID uuid.UUID, ruleID uuid.UUID, body *models.UpdateQualityRuleRequest) error
	DeleteQualityRule(ctx context.Context, datasetID uuid.UUID, ruleID uuid.UUID) error
	DatasetLintSummary(ctx context.Context, datasetID uuid.UUID) (*models.DatasetLintSummary, error)
	GetDatasetHealth(ctx context.Context, datasetRID string) (*models.DatasetHealth, error)
}

type Handlers struct {
	Repo       Store
	BackingFS  storageabstraction.BackingFS
	PresignTTL time.Duration
	AuditSink  func(context.Context, AuditEvent)
	// MaxOutputCommitContentBytes caps the total content bytes accepted in a
	// single outputs:commit request. Zero falls back to
	// DefaultMaxOutputCommitContentBytes.
	MaxOutputCommitContentBytes int64
	// StagedOutputRoot is the absolute path of the private staging
	// directory for WP3.3 staged output objects. It must live on the same
	// filesystem as the backing store but outside the backing object
	// namespace (checked per use on the resolved paths). Empty disables
	// the staged-output feature: outputs:stage and STAGED_ADD commits
	// fail closed with 503.
	StagedOutputRoot string
	// StagedOutputMaxBytes caps one streaming stage attempt's total body
	// bytes. Zero falls back to DefaultStagedOutputMaxBytes.
	StagedOutputMaxBytes int64
	// StagedOutputMaxRecords caps the record count of one staged object.
	// Zero falls back to DefaultStagedOutputMaxRecords.
	StagedOutputMaxRecords int64
	// StagedOutputLeaseTTL bounds the single in-flight upload lease of a
	// reservation. Zero falls back to DefaultStagedOutputLeaseTTL.
	StagedOutputLeaseTTL time.Duration
	// StagedOutputTTL bounds how long a sealed staged object stays
	// committable before it expires (no server-side GC in v1 — expired
	// artifacts are retained and listable). Zero falls back to
	// DefaultStagedOutputTTL.
	StagedOutputTTL time.Duration
	// Resolver computes effective dataset markings by walking lineage
	// upstream. When nil, marking endpoints fall back to the direct rows
	// returned by Repo.ListDatasetMarkings — keeps tests light and lets
	// the service start before lineage-service is reachable.
	Resolver *MarkingResolver
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSONErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

const dependencyUnavailableStatus = 503

func writeDependencyUnavailable(w http.ResponseWriter, code string, msg string) {
	writeJSON(w, dependencyUnavailableStatus, map[string]string{"code": code, "error": msg})
}

func datasetIDParam(r *http.Request) string {
	if id := chi.URLParam(r, "id"); id != "" {
		return id
	}
	return chi.URLParam(r, "rid")
}

func transactionIDParam(r *http.Request) string {
	if id := chi.URLParam(r, "txn"); id != "" {
		return id
	}
	return chi.URLParam(r, "txn_id")
}

func (h *Handlers) ListDatasets(w http.ResponseWriter, r *http.Request) {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	var ownerID *uuid.UUID
	if raw := r.URL.Query().Get("owner_id"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			writeJSONErr(w, http.StatusBadRequest, "invalid owner_id")
			return
		}
		ownerID = &id
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			limit = n
		}
	}
	// The tenant predicate is always applied; owner_id can only narrow it.
	items, err := h.Repo.ListDatasetsForTenant(r.Context(), tenantScopeFromClaims(claims), ownerID, limit)
	if err != nil {
		slog.Error("list datasets", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to list datasets")
		return
	}
	writeJSON(w, http.StatusOK, models.ListResponse[models.Dataset]{Items: items})
}

func (h *Handlers) GetDataset(w http.ResponseWriter, r *http.Request) {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	id, err := uuid.Parse(datasetIDParam(r))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	// Tenant visibility runs before anything is returned: a dataset in
	// another organization (or another user's personal domain) is
	// indistinguishable from a missing one.
	v, err := h.Repo.GetDatasetForTenant(r.Context(), id, tenantScopeFromClaims(claims))
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "dataset not found")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

// CreateDataset ports Rust `create_dataset`: RBAC gate, name/format/health
// validation, declarative defaults and a `dataset.create` audit emission.
func (h *Handlers) CreateDataset(w http.ResponseWriter, r *http.Request) {
	caller, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if !canWriteDataset(caller) {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "forbidden", "required_scope": "dataset.write", "dataset_rid": "new"})
		return
	}
	var body models.CreateDatasetRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if err := validateDatasetName(body.Name); err != nil {
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	format := "parquet"
	if body.Format != nil && *body.Format != "" {
		format = strings.ToLower(*body.Format)
	}
	if err := validateDatasetFormat(format); err != nil {
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	body.Format = &format
	health := "unknown"
	if body.HealthStatus != nil && *body.HealthStatus != "" {
		health = *body.HealthStatus
	}
	if err := validateHealthStatus(health); err != nil {
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	body.HealthStatus = &health
	// Tenant ownership is assigned from the verified claims only — the
	// request body carries no org field to honor.
	v, err := h.Repo.CreateDatasetForTenant(r.Context(), &body, caller.Sub, caller.OrgID)
	if err != nil {
		slog.Error("create dataset", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "create failed")
		return
	}
	emitDatasetAudit(caller.Sub.String(), "dataset.create", v.StoragePath, map[string]any{
		"dataset_id":      v.ID,
		"name":            v.Name,
		"format":          v.Format,
		"tags":            v.Tags,
		"runtime_owner":   "dataset-versioning-service",
		"versioning_mode": "declarative-only",
	})
	writeJSON(w, http.StatusCreated, v)
}

// UpdateDataset ports Rust `update_dataset`: tenant visibility, RBAC
// gate, name/health validation and a `dataset.update` audit emission
// listing changed fields.
func (h *Handlers) UpdateDataset(w http.ResponseWriter, r *http.Request) {
	caller, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	id, err := uuid.Parse(datasetIDParam(r))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	// Tenant check first: cross-tenant datasets 404 regardless of the
	// caller's scopes. Body-supplied owner_id changes can never widen
	// visibility because org ownership is not request-settable.
	existing, err := h.Repo.GetDatasetForTenant(r.Context(), id, tenantScopeFromClaims(caller))
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if existing == nil {
		writeJSONErr(w, http.StatusNotFound, "dataset not found")
		return
	}
	if !canWriteDataset(caller) {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "forbidden", "required_scope": "dataset.write", "dataset_rid": id.String()})
		return
	}
	var body models.UpdateDatasetRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if body.Name != nil {
		if err := validateDatasetName(*body.Name); err != nil {
			writeJSONErr(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if body.HealthStatus != nil {
		if err := validateHealthStatus(*body.HealthStatus); err != nil {
			writeJSONErr(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	v, err := h.Repo.UpdateDataset(r.Context(), id, &body)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "dataset not found")
		return
	}
	emitDatasetAudit(caller.Sub.String(), "dataset.update", v.StoragePath, map[string]any{
		"dataset_id":     v.ID,
		"fields_changed": updateChangedFields(&body),
	})
	writeJSON(w, http.StatusOK, v)
}

func (h *Handlers) DeleteDataset(w http.ResponseWriter, r *http.Request) {
	caller, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	id, err := uuid.Parse(datasetIDParam(r))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid id")
		return
	}
	// Tenant check first: cross-tenant datasets 404 regardless of the
	// caller's scopes (admins included).
	existing, err := h.Repo.GetDatasetForTenant(r.Context(), id, tenantScopeFromClaims(caller))
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if existing == nil {
		writeJSONErr(w, http.StatusNotFound, "dataset not found")
		return
	}
	if !canWriteDataset(caller) {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "forbidden", "required_scope": "dataset.write", "dataset_rid": id.String()})
		return
	}
	storagePath := existing.StoragePath
	deleted, err := h.Repo.DeleteDataset(r.Context(), id)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !deleted {
		writeJSONErr(w, http.StatusNotFound, "dataset not found")
		return
	}
	emitDatasetAudit(caller.Sub.String(), "dataset.delete", storagePath, map[string]any{
		"dataset_id": id,
	})
	w.WriteHeader(http.StatusNoContent)
}

func canWriteDataset(claims *authmw.Claims) bool {
	if claims == nil {
		return false
	}
	if claims.HasRole("admin") {
		return true
	}
	return claims.HasPermissionKey("dataset.write") || claims.HasPermission("dataset", "write")
}

// updateChangedFields lists the columns the PATCH body explicitly targeted,
// matching Rust's audit `fields_changed` array. The list keeps Rust's
// declaration order so audit consumers can compare records byte-for-byte.
func updateChangedFields(body *models.UpdateDatasetRequest) []string {
	out := make([]string, 0, 7)
	if body.Name != nil {
		out = append(out, "name")
	}
	if body.Description != nil {
		out = append(out, "description")
	}
	if body.Tags != nil {
		out = append(out, "tags")
	}
	if body.OwnerID != nil {
		out = append(out, "owner_id")
	}
	if len(body.Metadata) > 0 {
		out = append(out, "metadata")
	}
	if body.HealthStatus != nil {
		out = append(out, "health_status")
	}
	if body.CurrentViewID != nil {
		out = append(out, "current_view_id")
	}
	return out
}

func emitDatasetAudit(actor, action, datasetRID string, extra map[string]any) {
	encoded, err := json.Marshal(extra)
	if err != nil {
		encoded = []byte(`{}`)
	}
	slog.Info("audit",
		slog.String("actor", actor),
		slog.String("action", action),
		slog.String("dataset_rid", datasetRID),
		slog.String("details", string(encoded)),
	)
}

func parsePage(r *http.Request) (offset int, limit int) {
	limit = 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			limit = n
		}
	}
	if limit < 1 {
		limit = 1
	}
	if limit > 500 {
		limit = 500
	}
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		if decoded, err := base64.RawURLEncoding.DecodeString(raw); err == nil {
			if text := string(decoded); strings.HasPrefix(text, "of:") {
				if n, err := strconv.Atoi(strings.TrimPrefix(text, "of:")); err == nil && n > 0 {
					offset = n
				}
			}
		}
	}
	return offset, limit
}

func encodeCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte("of:" + strconv.Itoa(offset)))
}

// ownedDataset loads the route's dataset under the tenant rule. Personal
// (org-less) datasets keep their owner-only read boundary — the tenant
// predicate requires the caller to be the owner. Organization datasets
// are reachable by any caller of the same org, matching the tenant model
// where the organization, not the individual, is the access domain.
// Downstream write paths still apply their own scope checks.
func (h *Handlers) ownedDataset(w http.ResponseWriter, r *http.Request) (*authmw.Claims, *models.Dataset, bool) {
	caller, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return nil, nil, false
	}
	id, err := uuid.Parse(datasetIDParam(r))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid id")
		return nil, nil, false
	}
	dataset, err := h.Repo.GetDatasetForTenant(r.Context(), id, tenantScopeFromClaims(caller))
	if err != nil {
		slog.Error("load dataset", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to load dataset")
		return nil, nil, false
	}
	if dataset == nil {
		writeJSONErr(w, http.StatusNotFound, "dataset not found")
		return nil, nil, false
	}
	return caller, dataset, true
}

func (h *Handlers) ListVersions(w http.ResponseWriter, r *http.Request) {
	_, dataset, ok := h.ownedDataset(w, r)
	if !ok {
		return
	}
	versions, err := h.Repo.ListVersions(r.Context(), dataset.ID)
	if err != nil {
		slog.Error("list versions", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to list versions")
		return
	}
	offset, limit := parsePage(r)
	if offset > len(versions) {
		offset = len(versions)
	}
	end := offset + limit
	if end > len(versions) {
		end = len(versions)
	}
	hasMore := end < len(versions)
	var next *string
	if hasMore {
		v := encodeCursor(offset + limit)
		next = &v
	}
	writeJSON(w, http.StatusOK, models.Page[models.DatasetVersion]{Data: versions[offset:end], NextCursor: next, HasMore: hasMore})
}

func (h *Handlers) GetVersion(w http.ResponseWriter, r *http.Request) {
	_, dataset, ok := h.ownedDataset(w, r)
	if !ok {
		return
	}
	n, err := strconv.Atoi(chi.URLParam(r, "version"))
	if err != nil || n < 1 {
		writeJSONErr(w, http.StatusBadRequest, "invalid version")
		return
	}
	v, err := h.Repo.GetVersion(r.Context(), dataset.ID, int32(n))
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "version not found")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handlers) CreateVersion(w http.ResponseWriter, r *http.Request) {
	_, dataset, ok := h.ownedDataset(w, r)
	if !ok {
		return
	}
	var body models.CreateDatasetVersionRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if strings.TrimSpace(body.StoragePath) == "" {
		writeJSONErr(w, http.StatusBadRequest, "storage_path required")
		return
	}
	v, err := h.Repo.CreateVersion(r.Context(), dataset.ID, &body)
	if repo.IsConflict(err) {
		writeJSONErr(w, http.StatusConflict, "version already exists")
		return
	}
	if err != nil {
		slog.Error("create version", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to create version")
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func (h *Handlers) ListBranches(w http.ResponseWriter, r *http.Request) {
	raw := datasetIDParam(r)
	if _, err := uuid.Parse(raw); err != nil {
		datasetID, ok := h.resolveDatasetForCatalog(w, r)
		if !ok {
			return
		}
		branches, err := h.Repo.ListActiveRuntimeBranches(r.Context(), datasetID)
		if err != nil {
			writeJSONErr(w, http.StatusInternalServerError, "failed to list branches")
			return
		}
		offset, limit := parsePage(r)
		if offset > len(branches) {
			offset = len(branches)
		}
		end := offset + limit
		if end > len(branches) {
			end = len(branches)
		}
		hasMore := end < len(branches)
		var next *string
		if hasMore {
			v := encodeCursor(offset + limit)
			next = &v
		}
		writeJSON(w, http.StatusOK, models.Page[models.RuntimeBranch]{Data: branches[offset:end], NextCursor: next, HasMore: hasMore})
		return
	}
	_, dataset, ok := h.ownedDataset(w, r)
	if !ok {
		return
	}
	if err := h.Repo.EnsureDefaultBranch(r.Context(), dataset); err != nil {
		slog.Error("ensure default branch", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to ensure default branch")
		return
	}
	branches, err := h.Repo.ListBranches(r.Context(), dataset.ID)
	if err != nil {
		slog.Error("list branches", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to list branches")
		return
	}
	writeJSON(w, http.StatusOK, branches)
}

func (h *Handlers) GetBranch(w http.ResponseWriter, r *http.Request) {
	raw := datasetIDParam(r)
	name := chi.URLParam(r, "branch")
	if _, err := uuid.Parse(raw); err != nil {
		datasetID, ok := h.resolveDatasetForCatalog(w, r)
		if !ok {
			return
		}
		v, err := h.Repo.GetRuntimeBranch(r.Context(), datasetID, name)
		if err != nil {
			if errors.Is(err, repo.ErrNotFound) {
				writeJSONErr(w, http.StatusNotFound, "branch not found")
				return
			}
			writeJSONErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, v)
		return
	}
	_, dataset, ok := h.ownedDataset(w, r)
	if !ok {
		return
	}
	v, err := h.Repo.GetBranch(r.Context(), dataset.ID, name)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if v == nil {
		writeJSONErr(w, http.StatusNotFound, "branch not found")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handlers) CreateBranch(w http.ResponseWriter, r *http.Request) {
	raw := datasetIDParam(r)
	if _, err := uuid.Parse(raw); err != nil {
		datasetID, ok := h.resolveDatasetForCatalog(w, r)
		if !ok {
			return
		}
		claims, ok := h.requireDatasetWrite(w, r, datasetID)
		if !ok {
			return
		}
		var body models.CreateBranchBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSONErr(w, http.StatusBadRequest, "invalid body")
			return
		}
		v, err := h.Repo.CreateRuntimeBranch(r.Context(), datasetID, &body, claims.Sub)
		if err != nil {
			writeBranchError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, v)
		return
	}
	_, dataset, ok := h.ownedDataset(w, r)
	if !ok {
		return
	}
	var body models.CreateDatasetBranchRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if strings.TrimSpace(body.Name) == "" {
		writeJSONErr(w, http.StatusBadRequest, "branch name is required")
		return
	}
	if err := h.Repo.EnsureDefaultBranch(r.Context(), dataset); err != nil {
		slog.Error("ensure default branch", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to ensure default branch")
		return
	}
	v, err := h.Repo.CreateBranch(r.Context(), dataset, &body)
	if repo.IsConflict(err) {
		writeJSONErr(w, http.StatusConflict, "branch already exists")
		return
	}
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "source version does not exist") {
			status = http.StatusBadRequest
		}
		writeJSONErr(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func writeBranchError(w http.ResponseWriter, err error) {
	if errors.Is(err, repo.ErrNotFound) {
		writeJSONErr(w, http.StatusNotFound, "branch not found")
		return
	}
	if errors.Is(err, repo.ErrConflict) || repo.IsConflict(err) {
		writeJSONErr(w, http.StatusConflict, err.Error())
		return
	}
	if errors.Is(err, repo.ErrPreconditionFailed) {
		writeJSONErr(w, http.StatusPreconditionFailed, err.Error())
		return
	}
	if errors.Is(err, repo.ErrValidation) {
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if errors.Is(err, repo.ErrInvalidTransition) {
		writeJSONErr(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSONErr(w, http.StatusInternalServerError, err.Error())
}

// ListFiles serves GET /datasets/{id}/files. Without transaction_id it lists the files of the
// branch head, one per logical path. With transaction_id it lists the active files that one
// committed transaction of the branch holds instead, so a version that is no longer the head
// stays reachable by the transaction that cut it. The filter never falls back to the head: a
// transaction that is not a committed transaction of this dataset on this branch, before the
// head of that branch, is not found.
func (h *Handlers) ListFiles(w http.ResponseWriter, r *http.Request) {
	_, dataset, ok := h.ownedDataset(w, r)
	if !ok {
		return
	}
	branch := strings.TrimSpace(r.URL.Query().Get("branch"))
	if branch == "" {
		branch = "main"
	}
	prefix := strings.TrimLeft(strings.TrimSpace(r.URL.Query().Get("prefix")), "/")
	transactionID, err := listFilesTransaction(r)
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if transactionID != nil && !h.committedTransactionOfBranch(w, r, dataset.ID, branch, *transactionID) {
		return
	}
	var files []models.DatasetFile
	if transactionID == nil {
		files, err = h.Repo.ListFiles(r.Context(), dataset.ID, branch, prefix)
	} else {
		files, err = h.Repo.ListTransactionFiles(r.Context(), dataset.ID, *transactionID, prefix)
	}
	if err != nil {
		slog.Error("list files", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to list files")
		return
	}
	writeJSON(w, http.StatusOK, models.ListDatasetFilesResponse{Branch: branch, Total: len(files), Files: files})
}

// listFilesTransaction reads the optional transaction_id filter of the files listing. It
// returns nil when the request does not name a transaction, and a request without the filter
// is read exactly as before.
func listFilesTransaction(r *http.Request) (*uuid.UUID, error) {
	if _, present := r.URL.Query()["transaction_id"]; !present && !namesTransactionFilter(r.URL.RawQuery) {
		return nil, nil
	}
	// The convenience method silently drops a pair with an invalid percent-escape, which would
	// turn a malformed filter into a request without one. A query that names the filter is
	// therefore read strictly, as a whole.
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, errors.New("invalid query encoding")
	}
	values, present := query["transaction_id"]
	if !present || len(values) != 1 {
		return nil, errors.New("transaction_id must appear at most once")
	}
	transactionID, err := uuid.Parse(strings.TrimSpace(values[0]))
	if err != nil {
		return nil, errors.New("transaction_id must be a UUID")
	}
	return &transactionID, nil
}

// namesTransactionFilter reports whether the raw query holds a transaction_id pair, with the
// name as written or encoded, as in transaction%5Fid. URL.Query() drops a whole pair whose value
// does not decode, so a filter named that way with such a value is found here and nowhere else.
func namesTransactionFilter(raw string) bool {
	for _, pair := range strings.Split(raw, "&") {
		name, _, _ := strings.Cut(pair, "=")
		if decoded, err := url.QueryUnescape(name); err == nil && decoded == "transaction_id" {
			return true
		}
	}
	return false
}

// committedTransactionOfBranch reports whether the transaction is a committed transaction of
// this dataset on this branch that lies before the head of the branch. It writes the response
// when it is not. A deleted or archived branch does not resolve, as for rows:scan. A
// transaction of a parent branch is not one of a branch created from it, as for rows:scan:
// the branch inherits the files to read, not the commit.
func (h *Handlers) committedTransactionOfBranch(w http.ResponseWriter, r *http.Request, datasetID uuid.UUID, branch string, transactionID uuid.UUID) bool {
	notFound := func() bool {
		writeJSONErr(w, http.StatusNotFound, "transaction not found")
		return false
	}
	branchRow, err := h.Repo.GetRuntimeBranch(r.Context(), datasetID, branch)
	if err != nil && !errors.Is(err, repo.ErrNotFound) {
		writeJSONErr(w, http.StatusInternalServerError, "failed to load branch")
		return false
	}
	if err != nil || branchRow == nil {
		return notFound()
	}
	txn, err := h.Repo.GetRuntimeTransaction(r.Context(), datasetID, transactionID)
	if err != nil {
		writeJSONErr(w, http.StatusInternalServerError, "failed to load transaction")
		return false
	}
	if txn == nil || txn.Status != models.TransactionStatusCommitted || txn.BranchID != branchRow.ID {
		return notFound()
	}
	// The window of a branch ends at its head. A transaction of the branch that was committed
	// after the head is outside it, and the head listing leaves its files out as well.
	if branchRow.HeadTransactionID != nil {
		head, err := h.Repo.GetRuntimeTransaction(r.Context(), datasetID, *branchRow.HeadTransactionID)
		if err != nil {
			writeJSONErr(w, http.StatusInternalServerError, "failed to load transaction")
			return false
		}
		if head != nil && transactionAt(txn).After(transactionAt(head)) {
			return notFound()
		}
	}
	return true
}

// transactionAt is when a transaction took its place on a branch: its commit, or else its start.
func transactionAt(txn *models.RuntimeTransaction) time.Time {
	if txn.CommittedAt != nil {
		return *txn.CommittedAt
	}
	return txn.StartedAt
}

func (h *Handlers) DownloadFile(w http.ResponseWriter, r *http.Request) {
	_, dataset, ok := h.ownedDataset(w, r)
	if !ok {
		return
	}
	fileID, err := uuid.Parse(chi.URLParam(r, "file_id"))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid file_id")
		return
	}
	file, err := h.Repo.GetFile(r.Context(), dataset.ID, fileID)
	if err != nil {
		slog.Error("get file", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to load file")
		return
	}
	if file == nil {
		writeJSONErr(w, http.StatusNotFound, "file not found")
		return
	}
	if file.DeletedAt != nil {
		writeJSONErr(w, http.StatusGone, "file is soft-deleted")
		return
	}
	if h.BackingFS == nil {
		writeDependencyUnavailable(w, "backing_filesystem_unavailable", "backing filesystem not configured")
		return
	}
	ttl := h.PresignTTL
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	signed, err := h.BackingFS.PresignedURL(storageabstraction.ParsePhysicalURI(file.PhysicalURI), ttl)
	if err != nil {
		slog.Error("presign file", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to presign file")
		return
	}
	claims, _ := authmw.FromContext(r.Context())
	actor := "anonymous"
	if claims != nil {
		actor = claims.Sub.String()
	}
	h.emitAudit(r.Context(), AuditEvent{Actor: actor, Action: "files.download", DatasetRID: dataset.ID.String(), Details: map[string]any{
		"file_id": file.ID.String(), "logical_path": file.LogicalPath, "size_bytes": file.SizeBytes, "physical_uri": file.PhysicalURI, "presign_ttl_seconds": uint64(ttl / time.Second), "expires_at": signed.ExpiresAt,
	}})
	w.Header().Set("Location", signed.URL)
	w.Header().Set("Cache-Control", "private, max-age=0, must-revalidate")
	w.WriteHeader(http.StatusFound)
}

func (h *Handlers) CreateFileUploadURL(w http.ResponseWriter, r *http.Request) {
	_, dataset, ok := h.ownedDataset(w, r)
	if !ok {
		return
	}
	txnID, err := uuid.Parse(transactionIDParam(r))
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid transaction id")
		return
	}
	var body models.CreateDatasetFileUploadURLRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	logical := strings.TrimLeft(strings.TrimSpace(body.LogicalPath), "/")
	if logical == "" {
		writeJSONErr(w, http.StatusBadRequest, "logical_path required")
		return
	}
	if !safeObjectKey(logical) {
		writeJSONErr(w, http.StatusBadRequest, "invalid logical_path")
		return
	}
	status, found, err := h.Repo.GetTransactionStatus(r.Context(), dataset.ID, txnID)
	if err != nil {
		slog.Error("get transaction status", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to load transaction")
		return
	}
	if !found {
		writeJSONErr(w, http.StatusNotFound, "transaction not found")
		return
	}
	if !strings.EqualFold(status, "OPEN") {
		writeJSONErr(w, http.StatusConflict, "transaction is not OPEN")
		return
	}
	if h.BackingFS == nil {
		writeDependencyUnavailable(w, "backing_filesystem_unavailable", "backing filesystem not configured")
		return
	}
	ttl := h.PresignTTL
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	physical := storageabstraction.PhysicalLocation{
		FSID:          h.BackingFS.FSID(),
		BaseDirectory: h.BackingFS.BaseDirectory(),
		RelativePath:  "transactions/" + txnID.String() + "/" + logical,
	}
	signed, err := h.BackingFS.PresignedURL(physical, ttl)
	if err != nil {
		slog.Error("presign upload", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusInternalServerError, "failed to presign upload")
		return
	}
	method := signed.Method
	if method == "" || method == "GET" {
		method = "PUT"
	}
	claims, _ := authmw.FromContext(r.Context())
	actor := "anonymous"
	if claims != nil {
		actor = claims.Sub.String()
	}
	h.emitAudit(r.Context(), AuditEvent{Actor: actor, Action: "files.upload_url", DatasetRID: dataset.ID.String(), Details: map[string]any{
		"transaction_id": txnID.String(), "logical_path": physical.RelativePath, "content_type": body.ContentType, "sha256": body.SHA256, "physical_uri": physical.URI(), "presign_ttl_seconds": uint64(ttl / time.Second), "expires_at": signed.ExpiresAt,
	}})
	writeJSON(w, http.StatusOK, models.CreateDatasetFileUploadURLResponse{URL: signed.URL, PhysicalURI: physical.URI(), ExpiresAt: signed.ExpiresAt, Method: method})
}
