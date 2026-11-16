// Central tenant guard — P1.1 dataset tenant isolation.
//
// Every dataset-addressed route funnels through resolveTenantDataset (via
// resolveDatasetForCatalog, ownedDataset or the direct CRUD handlers), so
// the tenant rule in repo.TenantScope.Covers executes for each request at
// the resource-resolution layer, not per test entry. The tenant identity
// comes exclusively from the verified JWT claims; request parameters,
// bodies and forwarded headers are never consulted.

package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

// tenantScopeFromClaims projects validated JWT claims onto the repo's
// tenant scope. Claims with an OrgID act for that organization; claims
// without one act in the subject's personal domain.
func tenantScopeFromClaims(claims *authmw.Claims) repo.TenantScope {
	return repo.TenantScope{OrgID: claims.OrgID, Subject: claims.Sub}
}

// resolveTenantDataset loads the dataset named by the id/rid route param
// and enforces tenant visibility. Cross-tenant and unknown references
// both answer 404, so dataset existence is not leaked across tenants.
func (h *Handlers) resolveTenantDataset(w http.ResponseWriter, r *http.Request) (*models.Dataset, bool) {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return nil, false
	}
	dataset, err := h.Repo.ResolveDatasetForTenant(r.Context(), datasetIDParam(r), tenantScopeFromClaims(claims))
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			writeJSONErr(w, http.StatusNotFound, "dataset not found")
		} else {
			writeJSONErr(w, http.StatusInternalServerError, "failed to resolve dataset")
		}
		return nil, false
	}
	if dataset == nil {
		writeJSONErr(w, http.StatusNotFound, "dataset not found")
		return nil, false
	}
	return dataset, true
}

// validateLineageTargetsInTenant rejects lineage links whose target
// dataset resolves outside the caller's tenant; a target in another
// organization is indistinguishable from an unknown one — including pure
// UUID targets, which the resolver also reports as ErrNotFound when not
// tenant-visible. Non-dataset targets (e.g. pipelines) carry no dataset
// reference and are exempt. A nil dataset with a nil error is treated as
// not-found regardless of the Store implementation, so the guard never
// depends on one contract nuance alone.
func (h *Handlers) validateLineageTargetsInTenant(ctx context.Context, links []models.PutDatasetLineageLink, scope repo.TenantScope) error {
	for _, l := range links {
		kind := "dataset"
		if l.TargetKind != nil && *l.TargetKind != "" {
			kind = *l.TargetKind
		}
		if kind != "dataset" {
			continue
		}
		target, err := h.Repo.ResolveDatasetForTenant(ctx, l.TargetRID, scope)
		if err != nil {
			if errors.Is(err, repo.ErrNotFound) {
				return repo.ErrNotFound
			}
			return err
		}
		if target == nil {
			return repo.ErrNotFound
		}
	}
	return nil
}

// viewForeignToDataset reports whether a view id is positively known to
// belong to a different dataset: a dataset_views row (the authoritative
// view registry — the same signal DatasetViewBelongsToDataset encodes) or
// a stored schema row owned by another dataset. View ids are global, so
// without this check a caller could read, preview into, or overwrite
// (PutViewSchema upserts on view_id) another dataset's view/schema even
// after passing the parent-dataset tenant guard — the "dataset A parent +
// dataset B view" mismatch. Unknown views (no row in either table) stay
// usable for schema-only flows.
func (h *Handlers) viewForeignToDataset(ctx context.Context, datasetID uuid.UUID, viewID uuid.UUID) (bool, error) {
	owner, err := h.Repo.GetDatasetViewOwner(ctx, viewID)
	if err != nil {
		return false, err
	}
	if owner != nil && *owner != datasetID {
		return true, nil
	}
	schema, err := h.Repo.GetViewSchema(ctx, viewID)
	if err != nil {
		return false, err
	}
	return schema != nil && schema.DatasetID != datasetID, nil
}

// validateStorageRefForDataset rejects storage references whose local
// object key reaches into another dataset's namespace. File-index entries
// and staged transaction files carry caller-supplied storage paths, and
// the local backing store maps every scheme's key onto the same object
// tree (ParsePhysicalURI treats bare paths and s3/hdfs URIs alike), so a
// forged reference would let a later download or rows read serve another
// dataset's bytes.
//
// The rule, applied after optionally stripping one leading base-directory
// segment (uploads and content objects carry it; trigger-derived
// transaction paths do not):
//
//	datasets/{X}/...     → X must be the dataset being written
//	transactions/{t}/... → t must be a transaction of that dataset
//	anything else        → external placeholder path; every dataset-local
//	                       object lives under one of the two prefixes
//	                       above, so such a key cannot collide with one
//
// Legacy upload layouts ({baseDir}/datasets/{D}/...) and pipeline output
// placeholders (outputs/{txn}/...) pass unchanged. isOwnTxn decides
// transaction ownership; nil accepts only currentTxn.
func (h *Handlers) validateStorageRefForDataset(ctx context.Context, ref string, baseDir string, datasetID uuid.UUID, currentTxn *uuid.UUID, isOwnTxn func(uuid.UUID) bool) error {
	location := storageabstraction.ParsePhysicalURI(ref)
	key := strings.TrimLeft(location.RelativePath, "/")
	if trimmed := strings.Trim(baseDir, "/"); trimmed != "" {
		key = strings.TrimPrefix(key, trimmed+"/")
	}
	if key == "" {
		return nil
	}
	segments := strings.Split(key, "/")
	switch segments[0] {
	case "datasets":
		if len(segments) < 2 {
			return fmt.Errorf("storage_path %q must reference an object under datasets/{%s}/", ref, datasetID)
		}
		if segments[1] != datasetID.String() {
			return fmt.Errorf("storage_path %q references another dataset's object namespace", ref)
		}
	case "transactions":
		if len(segments) < 2 {
			return fmt.Errorf("storage_path %q must reference an object under this dataset's transactions", ref)
		}
		txnID, err := uuid.Parse(segments[1])
		if err != nil {
			return fmt.Errorf("storage_path %q has an invalid transaction segment", ref)
		}
		if currentTxn != nil && txnID == *currentTxn {
			return nil
		}
		if isOwnTxn == nil || !isOwnTxn(txnID) {
			return fmt.Errorf("storage_path %q references another dataset's transaction object", ref)
		}
	}
	return nil
}

// datasetTxnOwner builds the transaction-ownership probe used by the
// registration-time storage-ref validation.
func (h *Handlers) datasetTxnOwner(ctx context.Context, datasetID uuid.UUID) func(uuid.UUID) bool {
	return func(txnID uuid.UUID) bool {
		_, found, err := h.Repo.GetTransactionStatus(ctx, datasetID, txnID)
		return err == nil && found
	}
}
