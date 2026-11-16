//go:build integration

package repo

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// TestDatasetOrgTenancyMigrationAndVisibility locks the P1.1 tenancy work
// against a real Postgres 16:
//
//  1. the migration set (including 20261113000000_dataset_org_tenancy)
//     applies on a fresh database and re-runs unchanged — startup replays
//     the whole set, so every statement must be idempotent;
//  2. rows that predate tenancy (org_id NULL, inserted like a legacy
//     deployment would) keep org_id NULL across a migration re-run —
//     legacy data stays in the personal domain until explicitly moved;
//  3. the same dataset name coexists in two organizations;
//  4. the tenant visibility predicate behaves exactly like
//     TenantScope.Covers at the SQL level for org, cross-org, personal
//     and legacy rows across Get/Resolve/List/Facets.
func TestDatasetOrgTenancyMigrationAndVisibility(t *testing.T) {
	ctx := context.Background()
	pool := bootPool(ctx, t)

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("first Migrate on empty DB: %v", err)
	}

	// org_id exists, is nullable, and the partial lookup index is present.
	var nullable string
	if err := pool.QueryRow(ctx,
		`SELECT is_nullable FROM information_schema.columns
		  WHERE table_name = 'datasets' AND column_name = 'org_id'`).Scan(&nullable); err != nil {
		t.Fatalf("probe datasets.org_id: %v", err)
	}
	if nullable != "YES" {
		t.Fatalf("datasets.org_id is_nullable = %s, want YES", nullable)
	}
	var idxExists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_indexes
		  WHERE indexname = 'idx_datasets_org_id')`).Scan(&idxExists); err != nil {
		t.Fatalf("probe idx_datasets_org_id: %v", err)
	}
	if !idxExists {
		t.Fatal("idx_datasets_org_id missing")
	}

	r := &Repo{Pool: pool}

	// A row shaped like a pre-tenancy deployment: inserted without any
	// org knowledge. A second Migrate re-run must leave it NULL.
	legacyOwner := uuid.New()
	legacy, err := r.CreateDataset(ctx, &models.CreateDatasetRequest{Name: "legacy-orders"}, legacyOwner)
	if err != nil {
		t.Fatalf("legacy CreateDataset: %v", err)
	}
	if legacy.OrgID != nil {
		t.Fatalf("legacy dataset org_id = %v, want nil", legacy.OrgID)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate re-run with data: %v", err)
	}
	var legacyOrg *uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT org_id FROM datasets WHERE id = $1`, legacy.ID).Scan(&legacyOrg); err != nil {
		t.Fatalf("reload legacy row: %v", err)
	}
	if legacyOrg != nil {
		t.Fatalf("legacy row org_id = %v after re-run, want NULL", legacyOrg)
	}

	// Same subject, same dataset name, two organizations.
	user := uuid.New()
	orgA, orgB := uuid.New(), uuid.New()
	inA, err := r.CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "orders"}, user, &orgA)
	if err != nil {
		t.Fatalf("CreateDatasetForTenant orgA: %v", err)
	}
	if inA.OrgID == nil || *inA.OrgID != orgA {
		t.Fatalf("orgA dataset org_id = %v, want %s", inA.OrgID, orgA)
	}
	inB, err := r.CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "orders"}, user, &orgB)
	if err != nil {
		t.Fatalf("CreateDatasetForTenant orgB (same name must coexist): %v", err)
	}

	orgAScope := TenantScope{OrgID: &orgA, Subject: user}
	orgBScope := TenantScope{OrgID: &orgB, Subject: user}
	personalScope := TenantScope{Subject: legacyOwner}

	// Direct tenant-scoped loads.
	if got, err := r.GetDatasetForTenant(ctx, inA.ID, orgAScope); err != nil || got == nil {
		t.Fatalf("orgA get own-org dataset: %v, %v", got, err)
	}
	if got, _ := r.GetDatasetForTenant(ctx, inB.ID, orgAScope); got != nil {
		t.Fatal("orgA caller must not load orgB dataset")
	}
	if got, _ := r.GetDatasetForTenant(ctx, inA.ID, personalScope); got != nil {
		t.Fatal("personal caller must not load org dataset")
	}
	// The same owner WITH an org context cannot claim their legacy row…
	userOrgScope := TenantScope{OrgID: &orgA, Subject: legacyOwner}
	if got, _ := r.GetDatasetForTenant(ctx, legacy.ID, userOrgScope); got != nil {
		t.Fatal("org request must not reach personal-domain legacy row")
	}
	// …and neither can a foreign personal caller.
	if got, _ := r.GetDatasetForTenant(ctx, legacy.ID, TenantScope{Subject: uuid.New()}); got != nil {
		t.Fatal("foreign personal caller must not load legacy row")
	}
	// …but the owner without an org still can.
	if got, err := r.GetDatasetForTenant(ctx, legacy.ID, personalScope); err != nil || got == nil {
		t.Fatalf("legacy owner personal get: %v, %v", got, err)
	}

	// RID resolution under the tenant rule.
	ridA := "ri.foundry.main.dataset." + inA.ID.String()
	if got, err := r.ResolveDatasetForTenant(ctx, ridA, orgAScope); err != nil || got == nil {
		t.Fatalf("orgA resolve own RID: %v, %v", got, err)
	}
	if got, err := r.ResolveDatasetForTenant(ctx, ridA, orgBScope); err != ErrNotFound || got != nil {
		t.Fatalf("orgB resolve foreign RID = (%v, %v), want (nil, ErrNotFound)", got, err)
	}

	// List scoping: each org sees exactly its own row; the personal list
	// sees only the legacy row; the owner filter only narrows.
	listIDsFor := func(scope TenantScope, owner *uuid.UUID) map[uuid.UUID]bool {
		t.Helper()
		rows, err := r.ListDatasetsForTenant(ctx, scope, owner, 100)
		if err != nil {
			t.Fatalf("ListDatasetsForTenant: %v", err)
		}
		out := map[uuid.UUID]bool{}
		for _, d := range rows {
			out[d.ID] = true
		}
		return out
	}
	aIDs := listIDsFor(orgAScope, nil)
	if !aIDs[inA.ID] || aIDs[inB.ID] || aIDs[legacy.ID] {
		t.Fatalf("orgA list = %v, want exactly the orgA dataset", aIDs)
	}
	bIDs := listIDsFor(orgBScope, nil)
	if !bIDs[inB.ID] || bIDs[inA.ID] {
		t.Fatalf("orgB list = %v, want exactly the orgB dataset", bIDs)
	}
	pIDs := listIDsFor(personalScope, nil)
	if !pIDs[legacy.ID] || pIDs[inA.ID] {
		t.Fatalf("personal list = %v, want exactly the legacy dataset", pIDs)
	}
	// owner_id filter pointing at the other org's owner cannot widen.
	if ids := listIDsFor(orgAScope, &user); ids[inB.ID] {
		t.Fatal("owner_id filter must not surface cross-org datasets")
	}

	// Facets count only tenant-visible datasets.
	facetsA, err := r.GetCatalogFacetsForTenant(ctx, orgAScope)
	if err != nil {
		t.Fatalf("GetCatalogFacetsForTenant orgA: %v", err)
	}
	var aOwnerCount int64
	for _, o := range facetsA.Owners {
		aOwnerCount += o.Count
	}
	if aOwnerCount != 1 {
		t.Fatalf("orgA facet owner count = %d, want 1", aOwnerCount)
	}
}
