// P1.1 tenant-isolation regressions. These exercise the real handler
// stack (resource resolvers included) against the in-memory fakeStore,
// mirroring repo.TenantScope.Covers. PostgreSQL-level enforcement of the
// same predicate is covered by the migration integration tests.
package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

// tenantIsolationHarness wires a handler over a fakeStore whose seed
// dataset (index 0) is a personal-domain row owned by the given user.
type tenantIsolationHarness struct {
	h     *handlers.Handlers
	store *fakeStore
}

func newTenantIsolationHarness(t *testing.T) *tenantIsolationHarness {
	t.Helper()
	store := newFakeStore(uuid.New())
	store.datasets[0].Name = "seed"
	return &tenantIsolationHarness{h: &handlers.Handlers{Repo: store}, store: store}
}

func (th *tenantIsolationHarness) addDataset(name string, owner uuid.UUID, org *uuid.UUID, tags ...string) models.Dataset {
	d := models.Dataset{
		ID: uuid.New(), Name: name, Format: "parquet",
		StoragePath: "bronze/" + name, OwnerID: owner, OrgID: org,
		ActiveBranch: "main", Tags: tags, Metadata: []byte(`{}`),
		CurrentVersion: 1, HealthStatus: "unknown",
	}
	th.store.datasets = append(th.store.datasets, d)
	return d
}

func orgPtr(id uuid.UUID) *uuid.UUID { return &id }

// do executes a request against a named handler with route params and
// claims already attached.
func do(h http.HandlerFunc, method, target, body string, params map[string]string, claims *authmw.Claims) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	for k, v := range params {
		req = withRouteParam(req, k, v)
	}
	req = req.WithContext(authmw.ContextWithClaims(req.Context(), claims))
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func claimsFor(sub uuid.UUID, org *uuid.UUID, roles ...string) *authmw.Claims {
	return &authmw.Claims{Sub: sub, OrgID: org, Roles: roles, Permissions: []string{"dataset.write"}}
}

func listIDs(t *testing.T, rec *httptest.ResponseRecorder) []uuid.UUID {
	t.Helper()
	var out models.ListResponse[models.Dataset]
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	ids := make([]uuid.UUID, 0, len(out.Items))
	for _, d := range out.Items {
		ids = append(ids, d.ID)
	}
	return ids
}

func containsID(ids []uuid.UUID, id uuid.UUID) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

func storeHasOrg(store *fakeStore, id uuid.UUID, org *uuid.UUID) bool {
	for _, d := range store.datasets {
		if d.ID == id {
			return d.OrgID != nil && org != nil && *d.OrgID == *org
		}
	}
	return false
}

// TestTenantSameUserTwoOrgsSameName: one subject, two organizations. The
// same dataset name must work in both orgs, each org only ever sees its
// own row, and cross-org reads 404.
func TestTenantSameUserTwoOrgsSameName(t *testing.T) {
	t.Parallel()
	th := newTenantIsolationHarness(t)
	user := uuid.New()
	orgA, orgB := uuid.New(), uuid.New()

	createInOrg := func(org uuid.UUID) models.Dataset {
		rec := do(th.h.CreateDataset, http.MethodPost, "/v1/datasets",
			`{"name":"orders"}`, nil, claimsFor(user, &org))
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
		var d models.Dataset
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &d))
		require.NotNil(t, d.OrgID)
		assert.Equal(t, org, *d.OrgID, "org must be assigned from the verified claim")
		return d
	}
	inA := createInOrg(orgA)
	inB := createInOrg(orgB)
	assert.NotEqual(t, inA.ID, inB.ID, "same name must coexist per org")

	// Each org's list contains its own dataset and never the other's.
	rec := do(th.h.ListDatasets, http.MethodGet, "/v1/datasets", "", nil, claimsFor(user, orgPtr(orgA)))
	require.Equal(t, http.StatusOK, rec.Code)
	ids := listIDs(t, rec)
	assert.True(t, containsID(ids, inA.ID))
	assert.False(t, containsID(ids, inB.ID), "org A list must not surface org B datasets")

	// Direct cross-org GET by id is 404. The RID form is rejected by
	// GetDataset's uuid parse for every caller (pre-existing contract,
	// no existence leak), so RID-form isolation is asserted through a
	// RID-capable sub-resource instead (below).
	rec = do(th.h.GetDataset, http.MethodGet, "/v1/datasets/"+inB.ID.String(), "", map[string]string{"rid": inB.ID.String()}, claimsFor(user, orgPtr(orgA)))
	assert.Equal(t, http.StatusNotFound, rec.Code, "cross-org get via id")

	bRID := "ri.foundry.main.dataset." + inB.ID.String()
	rec = do(th.h.GetDataset, http.MethodGet, "/v1/datasets/"+bRID, "", map[string]string{"rid": bRID}, claimsFor(user, orgPtr(orgA)))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "RID form on GetDataset is a parse error for every caller")
	rec = do(th.h.ListTransactions, http.MethodGet, "/v1/datasets/"+bRID+"/transactions", "", map[string]string{"rid": bRID}, claimsFor(user, orgPtr(orgA)))
	assert.Equal(t, http.StatusNotFound, rec.Code, "cross-org sub-resource via RID")

	// The personal-domain seed dataset is invisible to org contexts even
	// for its owner.
	seed := th.store.datasets[0]
	rec = do(th.h.GetDataset, http.MethodGet, "/v1/datasets/"+seed.ID.String(), "", map[string]string{"rid": seed.ID.String()}, claimsFor(user, orgPtr(orgA)))
	assert.Equal(t, http.StatusNotFound, rec.Code, "org request must not reach personal-domain data")
}

// TestTenantPersonalDomainOnlyOriginalOwner: legacy rows (org NULL) stay
// reachable only for the owner calling without an OrgID.
func TestTenantPersonalDomainOnlyOriginalOwner(t *testing.T) {
	t.Parallel()
	th := newTenantIsolationHarness(t)
	owner := th.store.datasets[0].OwnerID
	legacy := th.store.datasets[0]
	require.Nil(t, legacy.OrgID)

	// Owner without org keeps full access (legacy behavior).
	rec := do(th.h.GetDataset, http.MethodGet, "/v1/datasets/"+legacy.ID.String(), "", map[string]string{"rid": legacy.ID.String()}, claimsFor(owner, nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// The same owner WITH an org context is a different tenant: org
	// requests never auto-claim legacy rows.
	org := uuid.New()
	rec = do(th.h.GetDataset, http.MethodGet, "/v1/datasets/"+legacy.ID.String(), "", map[string]string{"rid": legacy.ID.String()}, claimsFor(owner, orgPtr(org)))
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Other users — personal or any org — get 404 as well.
	other := uuid.New()
	rec = do(th.h.GetDataset, http.MethodGet, "/v1/datasets/"+legacy.ID.String(), "", map[string]string{"rid": legacy.ID.String()}, claimsFor(other, nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
	rec = do(th.h.GetDataset, http.MethodGet, "/v1/datasets/"+legacy.ID.String(), "", map[string]string{"rid": legacy.ID.String()}, claimsFor(other, orgPtr(org)))
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// The owner's personal list shows the legacy row; the org list does not.
	rec = do(th.h.ListDatasets, http.MethodGet, "/v1/datasets", "", nil, claimsFor(owner, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, containsID(listIDs(t, rec), legacy.ID))
	rec = do(th.h.ListDatasets, http.MethodGet, "/v1/datasets", "", nil, claimsFor(owner, orgPtr(org)))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.False(t, containsID(listIDs(t, rec), legacy.ID))
}

// TestTenantSameOrgDifferentUsers: within an organization the org — not
// the individual owner — is the access domain; across orgs everything
// 404s. Personal-domain datasets stay owner-only.
func TestTenantSameOrgDifferentUsers(t *testing.T) {
	t.Parallel()
	th := newTenantIsolationHarness(t)
	orgA, orgB := uuid.New(), uuid.New()
	alice, bob := uuid.New(), uuid.New()

	shared := th.addDataset("orders", alice, orgPtr(orgA))

	// Same org, different user, not the owner: reads pass (org tenant).
	rec := do(th.h.GetDataset, http.MethodGet, "/v1/datasets/"+shared.ID.String(), "", map[string]string{"rid": shared.ID.String()}, claimsFor(bob, orgPtr(orgA)))
	require.Equal(t, http.StatusOK, rec.Code)

	// Sub-resources follow their dataset: rows resolve through the same
	// tenant guard. With no registered files the in-org read gets the
	// upstream 422 (guard passed), while the cross-org read 404s at the
	// guard itself.
	rec = do(th.h.ReadDatasetFileRows, http.MethodGet, "/v1/datasets/"+shared.ID.String()+"/files/rows", "", map[string]string{"rid": shared.ID.String()}, claimsFor(bob, orgPtr(orgA)))
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	rec = do(th.h.ReadDatasetFileRows, http.MethodGet, "/v1/datasets/"+shared.ID.String()+"/files/rows", "", map[string]string{"rid": shared.ID.String()}, claimsFor(bob, orgPtr(orgB)))
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// The same user in org B is isolated from org A's dataset.
	rec = do(th.h.GetDataset, http.MethodGet, "/v1/datasets/"+shared.ID.String(), "", map[string]string{"rid": shared.ID.String()}, claimsFor(bob, orgPtr(orgB)))
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Bob's personal-domain dataset is invisible to org A (even to
	// Alice, who shares the org) and to other personal callers.
	personal := th.addDataset("private", bob, nil)
	rec = do(th.h.GetDataset, http.MethodGet, "/v1/datasets/"+personal.ID.String(), "", map[string]string{"rid": personal.ID.String()}, claimsFor(alice, orgPtr(orgA)))
	assert.Equal(t, http.StatusNotFound, rec.Code)
	rec = do(th.h.GetDataset, http.MethodGet, "/v1/datasets/"+personal.ID.String(), "", map[string]string{"rid": personal.ID.String()}, claimsFor(alice, nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestTenantAdminCannotCrossOrganizations: the admin role does not widen
// the tenant boundary — reads, writes, deletes, output commits, file rows,
// versions and transactions all 404 across orgs, while in-org admin
// access keeps working.
func TestTenantAdminCannotCrossOrganizations(t *testing.T) {
	t.Parallel()
	th := newTenantIsolationHarness(t)
	orgA, orgB := uuid.New(), uuid.New()
	target := th.addDataset("b-orders", uuid.New(), orgPtr(orgB))
	adminA := claimsFor(uuid.New(), orgPtr(orgA), "admin")
	rid := "ri.foundry.main.dataset." + target.ID.String()
	params := map[string]string{"rid": rid, "id": target.ID.String()}

	rec := do(th.h.GetDataset, http.MethodGet, "/v1/datasets/"+rid, "", params, adminA)
	assert.Equal(t, http.StatusNotFound, rec.Code, "admin cross-org GET")
	rec = do(th.h.UpdateDataset, http.MethodPatch, "/v1/datasets/"+rid, `{"description":"x"}`, params, adminA)
	assert.Equal(t, http.StatusNotFound, rec.Code, "admin cross-org PATCH")
	rec = do(th.h.DeleteDataset, http.MethodDelete, "/v1/datasets/"+rid, "", params, adminA)
	assert.Equal(t, http.StatusNotFound, rec.Code, "admin cross-org DELETE")
	rec = do(th.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", `{"branch":"main"}`, params, adminA)
	assert.Equal(t, http.StatusNotFound, rec.Code, "admin cross-org outputs:commit")
	rec = do(th.h.ReadDatasetFileRows, http.MethodGet, "/v1/datasets/"+rid+"/files/rows", "", params, adminA)
	assert.Equal(t, http.StatusNotFound, rec.Code, "admin cross-org file rows")
	rec = do(th.h.ListVersions, http.MethodGet, "/v1/datasets/"+rid+"/versions", "", params, adminA)
	assert.Equal(t, http.StatusNotFound, rec.Code, "admin cross-org versions")
	rec = do(th.h.ListTransactions, http.MethodGet, "/v1/datasets/"+rid+"/transactions", "", params, adminA)
	assert.Equal(t, http.StatusNotFound, rec.Code, "admin cross-org transactions")

	// In-org admin still works: update passes the tenant check and the
	// scope check (admin role).
	adminB := claimsFor(uuid.New(), orgPtr(orgB), "admin")
	rec = do(th.h.UpdateDataset, http.MethodPatch, "/v1/datasets/"+rid, `{"description":"ok"}`, params, adminB)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

// TestTenantOutputCommitIsolation: outputs:commit resolves its target
// under the tenant rule; create_if_missing neither reaches into another
// tenant nor overwrites it, and created outputs inherit the caller's org.
func TestTenantOutputCommitIsolation(t *testing.T) {
	t.Parallel()
	th := newTenantIsolationHarness(t)
	orgA, orgB := uuid.New(), uuid.New()
	user := uuid.New()
	foreign := th.addDataset("foreign", uuid.New(), orgPtr(orgB))
	foreignRID := "ri.foundry.main.dataset." + foreign.ID.String()

	// Committing into another org's existing dataset is 404.
	rec := do(th.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+foreignRID+"/outputs:commit",
		`{"branch":"main","files":[{"logical_path":"part.ndjson","size_bytes":4}]}`, map[string]string{"rid": foreignRID}, claimsFor(user, orgPtr(orgA)))
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Empty(t, th.store.runtimeTransactions, "no transaction may be started for a foreign dataset")

	// create_if_missing with the foreign dataset's RID must not
	// create-or-overwrite it: the id exists globally, so 404.
	rec = do(th.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+foreignRID+"/outputs:commit",
		`{"create_if_missing":true,"branch":"main"}`, map[string]string{"rid": foreignRID}, claimsFor(user, orgPtr(orgA)))
	require.Equal(t, http.StatusNotFound, rec.Code)
	assert.False(t, storeHasOrg(th.store, foreign.ID, orgPtr(orgA)), "foreign dataset must not be re-owned")

	// A fresh RID commits and lands in the caller's org.
	fresh := uuid.New()
	freshRID := "ri.foundry.main.dataset." + fresh.String()
	rec = do(th.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+freshRID+"/outputs:commit",
		`{"create_if_missing":true,"branch":"main","files":[{"logical_path":"part.ndjson","size_bytes":4}]}`, map[string]string{"rid": freshRID}, claimsFor(user, orgPtr(orgA)))
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	assert.True(t, storeHasOrg(th.store, fresh, orgPtr(orgA)), "created output must carry the caller's org")
}

// TestTenantListAndParamsCannotWiden: owner_id filters, spoofed org
// headers and body-borne org fields only ever narrow or get ignored —
// visibility stays pinned to the verified claims.
func TestTenantListAndParamsCannotWiden(t *testing.T) {
	t.Parallel()
	th := newTenantIsolationHarness(t)
	orgA, orgB := uuid.New(), uuid.New()
	alice := uuid.New()
	bobInB := uuid.New()
	inA := th.addDataset("a-orders", alice, orgPtr(orgA))
	inB := th.addDataset("b-orders", bobInB, orgPtr(orgB))

	// owner_id pointing at the org-B owner still cannot surface org-B
	// datasets for an org-A caller.
	rec := do(th.h.ListDatasets, http.MethodGet, "/v1/datasets?owner_id="+bobInB.String(), "", nil, claimsFor(alice, orgPtr(orgA)))
	require.Equal(t, http.StatusOK, rec.Code)
	ids := listIDs(t, rec)
	assert.False(t, containsID(ids, inB.ID), "owner_id must not bypass the tenant filter")
	assert.False(t, containsID(ids, inA.ID), "owner filter narrows within the tenant")

	// Spoofed tenant headers are not part of the trust chain.
	req := httptest.NewRequest(http.MethodGet, "/v1/datasets", nil)
	req.Header.Set("X-Openfoundry-Tenant-Id", orgB.String())
	req.Header.Set("X-Org-Id", orgB.String())
	req = req.WithContext(authmw.ContextWithClaims(req.Context(), claimsFor(alice, orgPtr(orgA))))
	rec = httptest.NewRecorder()
	th.h.ListDatasets(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.False(t, containsID(listIDs(t, rec), inB.ID), "org headers must never widen visibility")

	// A body-borne org field is ignored: creation assigns the claim's org.
	rec = do(th.h.CreateDataset, http.MethodPost, "/v1/datasets",
		`{"name":"spoof","org_id":"`+orgB.String()+`","tenant_id":"`+orgB.String()+`"}`, nil, claimsFor(alice, orgPtr(orgA)))
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var created models.Dataset
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	require.NotNil(t, created.OrgID)
	assert.Equal(t, orgA, *created.OrgID, "org ownership comes from claims, not the body")
}

// TestTenantFacetsAndInternalMetadataAreScoped: aggregate surfaces (facet
// counts, internal metadata) leak no cross-tenant existence either.
func TestTenantFacetsAndInternalMetadataAreScoped(t *testing.T) {
	t.Parallel()
	th := newTenantIsolationHarness(t)
	orgA, orgB := uuid.New(), uuid.New()
	alice := uuid.New()
	th.addDataset("a1", alice, orgPtr(orgA), "alpha")
	b1 := th.addDataset("b1", uuid.New(), orgPtr(orgB), "beta")

	rec := do(th.h.GetCatalogFacets, http.MethodGet, "/v1/catalog/facets", "", nil, claimsFor(alice, orgPtr(orgA)))
	require.Equal(t, http.StatusOK, rec.Code)
	var facets models.CatalogFacets
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &facets))
	names := map[string]bool{}
	for _, tag := range facets.Tags {
		names[tag.Value] = true
	}
	assert.True(t, names["alpha"])
	assert.False(t, names["beta"], "facets must not count cross-org datasets")

	rid := "ri.foundry.main.dataset." + b1.ID.String()
	rec = do(th.h.GetDatasetMetadata, http.MethodGet, "/internal/datasets/"+rid+"/metadata", "", map[string]string{"rid": rid}, claimsFor(alice, orgPtr(orgA)))
	assert.Equal(t, http.StatusNotFound, rec.Code, "internal metadata must respect the tenant boundary")
}

// TestTenantScopeCoversRule pins the pure visibility rule the SQL
// predicate mirrors.
func TestTenantScopeCoversRule(t *testing.T) {
	t.Parallel()
	orgA, orgB := uuid.New(), uuid.New()
	alice, bob := uuid.New(), uuid.New()
	orgADataset := orgPtr(orgA)

	assert.True(t, repo.TenantScope{OrgID: orgPtr(orgA), Subject: alice}.Covers(orgADataset, alice))
	assert.True(t, repo.TenantScope{OrgID: orgPtr(orgA), Subject: bob}.Covers(orgADataset, alice), "org membership, not ownership, gates org datasets")
	assert.False(t, repo.TenantScope{OrgID: orgPtr(orgB), Subject: alice}.Covers(orgADataset, alice))
	assert.False(t, repo.TenantScope{Subject: alice}.Covers(orgADataset, alice), "personal callers never see org data")
	assert.True(t, repo.TenantScope{Subject: alice}.Covers(nil, alice))
	assert.False(t, repo.TenantScope{Subject: bob}.Covers(nil, alice), "personal data is owner-only")
	assert.False(t, repo.TenantScope{OrgID: orgPtr(orgA), Subject: alice}.Covers(nil, alice), "org requests never claim legacy rows")
}

// TestTenantLineageTargetsStayInTenant: cross-dataset lineage references
// (PUT and outputs:commit) reject foreign or unknown target datasets.
func TestTenantLineageTargetsStayInTenant(t *testing.T) {
	t.Parallel()
	th := newTenantIsolationHarness(t)
	orgA, orgB := uuid.New(), uuid.New()
	alice := uuid.New()
	own := th.addDataset("own", alice, orgPtr(orgA))
	foreign := th.addDataset("foreign", uuid.New(), orgPtr(orgB))
	ownRID := "ri.foundry.main.dataset." + own.ID.String()
	foreignRID := "ri.foundry.main.dataset." + foreign.ID.String()

	body := `{"links":[{"direction":"upstream","target_rid":"` + foreignRID + `"}]}`
	rec := do(th.h.PutDatasetLineageLinks, http.MethodPut, "/v1/datasets/"+ownRID+"/lineage-links", body, map[string]string{"rid": ownRID}, claimsFor(alice, orgPtr(orgA)))
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Empty(t, th.store.lineageLinks[own.ID], "foreign lineage link must not be stored")

	unknown := `{"links":[{"direction":"upstream","target_rid":"ri.foundry.main.dataset.` + uuid.NewString() + `"}]}`
	rec = do(th.h.PutDatasetLineageLinks, http.MethodPut, "/v1/datasets/"+ownRID+"/lineage-links", unknown, map[string]string{"rid": ownRID}, claimsFor(alice, orgPtr(orgA)))
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Same-tenant target is accepted.
	sibling := th.addDataset("sibling", uuid.New(), orgPtr(orgA))
	ok := `{"links":[{"direction":"upstream","target_rid":"ri.foundry.main.dataset.` + sibling.ID.String() + `"}]}`
	rec = do(th.h.PutDatasetLineageLinks, http.MethodPut, "/v1/datasets/"+ownRID+"/lineage-links", ok, map[string]string{"rid": ownRID}, claimsFor(alice, orgPtr(orgA)))
	require.Equal(t, http.StatusOK, rec.Code)

	// outputs:commit with a foreign lineage target is rejected before any
	// transaction starts.
	th.store.lineageLinks[own.ID] = nil
	commit := `{"branch":"main","lineage_links":[{"direction":"upstream","target_rid":"` + foreignRID + `"}]}`
	rec = do(th.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+ownRID+"/outputs:commit", commit, map[string]string{"rid": ownRID}, claimsFor(alice, orgPtr(orgA)))
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Empty(t, th.store.lineageLinks[own.ID], "commit must not leave foreign lineage links")
}
