// Tenant-scoped dataset access — P1.1 dataset tenant isolation.
//
// The tenant identity always originates from validated JWT claims (see
// handlers.tenantScopeFromClaims): the OrgID claim for organization
// requests, or the subject's personal domain when OrgID is absent.
// Request parameters, bodies and forwarded headers never influence it.

package repo

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// TenantScope is the trusted tenant identity of a caller, projected from
// verified JWT claims.
type TenantScope struct {
	// OrgID is the caller's organization; nil means the caller acts in
	// their personal domain.
	OrgID *uuid.UUID
	// Subject is the caller's sub claim.
	Subject uuid.UUID
}

// Covers implements the single dataset visibility rule for the service:
//
//   - an organization caller may reach datasets whose org_id equals the
//     caller's OrgID (the org is the tenant boundary — role or admin
//     status never widens it);
//   - a personal caller may reach only org-less (legacy) datasets they
//     own. Organization requests therefore cannot claim legacy rows, and
//     a personal caller never sees organization data.
func (s TenantScope) Covers(datasetOrg *uuid.UUID, ownerID uuid.UUID) bool {
	if s.OrgID != nil {
		return datasetOrg != nil && *datasetOrg == *s.OrgID
	}
	return datasetOrg == nil && ownerID == s.Subject
}

// tenantVisibility renders the SQL form of TenantScope.Covers over the
// datasets table. With a NULL scope org, `org_id = <org>` is never true
// and the second arm requires both an org-less row and the owner match;
// with a concrete scope org only the equality arm can match, so an org
// caller never reaches personal rows even when they own them.
func tenantVisibility(orgParam, subjectParam string) string {
	return fmt.Sprintf("(org_id = %s OR (org_id IS NULL AND owner_id = %s AND %s IS NULL))", orgParam, subjectParam, orgParam)
}

// GetDatasetForTenant loads a dataset by ID, returning nil when the
// caller's tenant scope does not cover it — indistinguishable from a
// missing row so handlers answer 404 without leaking cross-tenant
// existence.
func (r *Repo) GetDatasetForTenant(ctx context.Context, id uuid.UUID, scope TenantScope) (*models.Dataset, error) {
	row := r.Pool.QueryRow(ctx,
		datasetSelect+` WHERE id = $1 AND `+tenantVisibility("$2", "$3"), id, scope.OrgID, scope.Subject)
	v, err := scanDataset(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

// ResolveDatasetForTenant resolves a raw dataset reference (UUID or RID)
// within the caller's tenant scope. A reference that exists in another
// tenant — or that simply does not exist — resolves to ErrNotFound for
// both reference forms; callers never need to treat (nil, nil) as a
// third outcome.
func (r *Repo) ResolveDatasetForTenant(ctx context.Context, raw string, scope TenantScope) (*models.Dataset, error) {
	if id, err := uuid.Parse(raw); err == nil {
		v, err := r.GetDatasetForTenant(ctx, id, scope)
		if err != nil || v == nil {
			if err == nil {
				err = ErrNotFound
			}
			return nil, err
		}
		return v, nil
	}
	row := r.Pool.QueryRow(ctx,
		datasetSelect+` WHERE rid = $1 AND `+tenantVisibility("$2", "$3"), raw, scope.OrgID, scope.Subject)
	v, err := scanDataset(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return v, err
}

// ListDatasetsForTenant lists datasets visible to the caller's tenant
// scope, optionally narrowed by an owner filter. The filter intersects
// the tenant predicate, so it can only narrow — never widen — visibility.
func (r *Repo) ListDatasetsForTenant(ctx context.Context, scope TenantScope, ownerID *uuid.UUID, limit int) ([]models.Dataset, error) {
	if limit < 1 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	rows, err := r.Pool.Query(ctx, datasetSelect+`
		WHERE `+tenantVisibility("$1", "$2")+`
		  AND ($4::uuid IS NULL OR owner_id = $4)
		ORDER BY created_at DESC LIMIT $3`, scope.OrgID, scope.Subject, limit, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]models.Dataset, 0)
	for rows.Next() {
		v, err := scanDataset(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

// GetCatalogFacetsForTenant computes tag/owner facets over only the
// datasets visible to the caller's tenant scope.
func (r *Repo) GetCatalogFacetsForTenant(ctx context.Context, scope TenantScope) (*models.CatalogFacets, error) {
	tagRows, err := r.Pool.Query(ctx, `SELECT tag AS value, COUNT(*) AS count
		FROM datasets, unnest(tags) AS tag
		WHERE `+tenantVisibility("$1", "$2")+`
		GROUP BY tag
		ORDER BY count DESC, tag ASC`, scope.OrgID, scope.Subject)
	if err != nil {
		return nil, err
	}
	defer tagRows.Close()
	facets := &models.CatalogFacets{Tags: []models.CatalogTagFacet{}, Owners: []models.CatalogOwnerFacet{}}
	for tagRows.Next() {
		var tag models.CatalogTagFacet
		if err := tagRows.Scan(&tag.Value, &tag.Count); err != nil {
			return nil, err
		}
		facets.Tags = append(facets.Tags, tag)
	}
	if err := tagRows.Err(); err != nil {
		return nil, err
	}

	ownerRows, err := r.Pool.Query(ctx, `SELECT owner_id, COUNT(*) AS count
		FROM datasets
		WHERE `+tenantVisibility("$1", "$2")+`
		GROUP BY owner_id
		ORDER BY count DESC, owner_id ASC`, scope.OrgID, scope.Subject)
	if err != nil {
		return nil, err
	}
	defer ownerRows.Close()
	for ownerRows.Next() {
		var owner models.CatalogOwnerFacet
		if err := ownerRows.Scan(&owner.OwnerID, &owner.Count); err != nil {
			return nil, err
		}
		facets.Owners = append(facets.Owners, owner)
	}
	if err := ownerRows.Err(); err != nil {
		return nil, err
	}
	return facets, nil
}

// CreateDatasetForTenant inserts a dataset with its tenant ownership set
// from the caller's verified claims: orgID may be nil, which keeps the
// dataset in the owner's personal domain. Request bodies carry no org
// field to honor.
func (r *Repo) CreateDatasetForTenant(ctx context.Context, body *models.CreateDatasetRequest, ownerID uuid.UUID, orgID *uuid.UUID) (*models.Dataset, error) {
	id := uuid.New()
	if body.ID != nil && *body.ID != uuid.Nil {
		id = *body.ID
	}
	format := "parquet"
	if body.Format != nil && *body.Format != "" {
		format = strings.ToLower(*body.Format)
	}
	tags := body.Tags
	if tags == nil {
		tags = []string{}
	}
	description := ""
	if body.Description != nil {
		description = *body.Description
	}
	metadata := []byte(`{}`)
	if len(body.Metadata) > 0 {
		metadata = body.Metadata
	}
	health := "unknown"
	if body.HealthStatus != nil && *body.HealthStatus != "" {
		health = *body.HealthStatus
	}
	storagePath := BuildDatasetStoragePath("bronze", id)
	row := r.Pool.QueryRow(ctx,
		`INSERT INTO datasets
		    (id, rid, name, description, format, storage_path, owner_id, org_id, tags,
		     active_branch, metadata, health_status)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'main', $10, $11)
		 RETURNING id, name, description, format, storage_path, size_bytes,
		           row_count, owner_id, org_id, tags, current_version, active_branch,
		           metadata, health_status, current_view_id, created_at, updated_at`,
		id, "ri.foundry.main.dataset."+id.String(), strings.TrimSpace(body.Name), description,
		format, storagePath, ownerID, orgID, tags, metadata, health,
	)
	return scanDataset(row)
}
