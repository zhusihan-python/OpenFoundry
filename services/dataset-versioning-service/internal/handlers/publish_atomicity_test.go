// P1 repair regressions: lineage-target UUID semantics and atomic
// outputs:commit publishing.
//
// The lineage guard previously only inspected the resolver's error, so a
// pure-UUID target that the resolver reported as (nil, nil) — invisible
// to the caller's tenant — slipped through. These cases pin the fixed
// contract (ErrNotFound for both UUID and RID forms) across PUT lineage
// and outputs:commit, and prove a publish-step failure leaves the
// previous snapshot fully intact and retryable.
package handlers_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

func linksBody(direction, target string) string {
	return `{"links":[{"direction":"` + direction + `","target_rid":"` + target + `"}]}`
}

// TestLineageTargetsAcrossUUIDAndRIDForms: the tenant guard rejects
// foreign or unknown targets in BOTH reference forms and accepts
// same-tenant ones, for PUT lineage and outputs:commit alike.
func TestLineageTargetsAcrossUUIDAndRIDForms(t *testing.T) {
	t.Parallel()
	th := newTenantIsolationHarness(t)
	orgA, orgB := uuid.New(), uuid.New()
	alice := uuid.New()
	own := th.addDataset("own", alice, orgPtr(orgA))
	sameOrgOtherUser := th.addDataset("sibling", uuid.New(), orgPtr(orgA))
	foreign := th.addDataset("foreign", uuid.New(), orgPtr(orgB))
	legacy := th.addDataset("legacy", alice, nil) // own old personal domain
	ownRID := "ri.foundry.main.dataset." + own.ID.String()
	claims := claimsFor(alice, orgPtr(orgA))

	cases := []struct {
		name   string
		target string
		want   int
	}{
		{"cross-org UUID", foreign.ID.String(), http.StatusNotFound},
		{"unknown UUID", uuid.NewString(), http.StatusNotFound},
		{"cross-org RID", "ri.foundry.main.dataset." + foreign.ID.String(), http.StatusNotFound},
		{"own legacy personal UUID", legacy.ID.String(), http.StatusNotFound},
		{"same-org other-user UUID", sameOrgOtherUser.ID.String(), http.StatusOK},
	}
	for _, tc := range cases {
		rec := do(th.h.PutDatasetLineageLinks, http.MethodPut, "/v1/datasets/"+ownRID+"/lineage-links",
			linksBody("upstream", tc.target), map[string]string{"rid": ownRID}, claims)
		assert.Equal(t, tc.want, rec.Code, "PUT lineage %s", tc.name)
		if tc.want == http.StatusNotFound {
			assert.Empty(t, th.store.lineageLinks[own.ID], "rejected target %s must not be stored", tc.name)
		}
	}

	// outputs:commit applies the same rule before any transaction work.
	for _, tc := range []struct {
		name   string
		target string
		want   int
	}{
		{"cross-org UUID", foreign.ID.String(), http.StatusNotFound},
		{"unknown UUID", uuid.NewString(), http.StatusNotFound},
		{"own legacy personal UUID", legacy.ID.String(), http.StatusNotFound},
	} {
		body := `{"branch":"main","lineage_links":[{"direction":"upstream","target_rid":"` + tc.target + `"}]}`
		rec := do(th.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+ownRID+"/outputs:commit", body, map[string]string{"rid": ownRID}, claims)
		assert.Equal(t, tc.want, rec.Code, "outputs:commit lineage %s", tc.name)
	}
	require.Empty(t, th.store.runtimeTransactions, "rejected commits must not leave transactions behind")

	// A personal caller may lineage-reference their own legacy dataset
	// by UUID (the legacy dataset itself serves as a valid same-tenant
	// upstream).
	legacyRID := "ri.foundry.main.dataset." + legacy.ID.String()
	rec := do(th.h.PutDatasetLineageLinks, http.MethodPut,
		"/v1/datasets/"+legacyRID+"/lineage-links",
		linksBody("upstream", legacy.ID.String()),
		map[string]string{"rid": legacyRID}, claimsFor(alice, nil))
	assert.Equal(t, http.StatusOK, rec.Code, "personal caller may reference own legacy dataset by UUID")
}

// TestCommitDatasetOutputRejectsInvalidDirection: deterministic link
// validation runs before any transaction or object write.
func TestCommitDatasetOutputRejectsInvalidDirection(t *testing.T) {
	t.Parallel()
	th := newTenantIsolationHarness(t)
	org := uuid.New()
	owner := uuid.New()
	target := th.addDataset("orders", owner, orgPtr(org))
	rid := "ri.foundry.main.dataset." + target.ID.String()

	body := `{"branch":"main","lineage_links":[{"direction":"sideways","target_rid":"` + uuid.NewString() + `"}]}`
	rec := do(th.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", body, map[string]string{"rid": rid}, claimsFor(owner, orgPtr(org)))
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Empty(t, th.store.runtimeTransactions, "invalid direction must not start a transaction")

	rec = do(th.h.PutDatasetLineageLinks, http.MethodPut, "/v1/datasets/"+rid+"/lineage-links", linksBody("sideways", target.ID.String()), map[string]string{"rid": rid}, claimsFor(owner, orgPtr(org)))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "PUT lineage invalid direction")
}

// TestCommitDatasetOutputPublishFailureIsAtomic: a publish-step failure
// (after the commit core) leaves the previous version, file index,
// lineage links and dataset_files untouched, the failed transaction
// ends ABORTED, and retrying the same commit succeeds once the fault
// clears — the old snapshot stays usable throughout.
func TestCommitDatasetOutputPublishFailureIsAtomic(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	store := &triggerMirrorStore{newFakeStore(owner)}
	fs := storageabstraction.NewLocalBackingFS("http://files.local", "store", []byte("publish-atomicity-test"))
	fs.RootDir = t.TempDir()
	h := &handlers.Handlers{Repo: store, BackingFS: fs}
	org := uuid.New()
	claims := claimsFor(owner, orgPtr(org))
	target := store.datasets[0]
	target.OrgID = orgPtr(org)
	target.ActiveBranch = "main" // mirrors the real table default
	store.datasets[0] = target
	rid := "ri.foundry.main.dataset." + target.ID.String()
	params := map[string]string{"rid": rid}

	commitBody := func(path string) string {
		return `{"branch":"main","files":[{"logical_path":"` + path + `","size_bytes":6,"storage_path":"datasets/` + target.ID.String() + `/out/` + path + `"}],"lineage_links":[{"direction":"upstream","target_rid":"` + target.ID.String() + `"}]}`
	}

	// v1: a successful publish establishes the old snapshot.
	rec := do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", commitBody("v1.ndjson"), params, claims)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var first models.CommitDatasetOutputResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &first))
	require.Equal(t, models.TransactionStatusCommitted, first.Transaction.Status)
	require.Len(t, store.fileIndex[target.ID], 1)
	require.Len(t, store.lineageLinks[target.ID], 1)
	require.Len(t, store.files[target.ID], 1)
	v1Version := store.datasets[0].CurrentVersion

	// Fault injection at the publish step.
	store.publishErr = errPublishInjected{}
	rec = do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", commitBody("v2.ndjson"), params, claims)
	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())

	// The previous snapshot is untouched and fully usable.
	assert.Equal(t, v1Version, store.datasets[0].CurrentVersion, "version must not advance on failed publish")
	require.Len(t, store.fileIndex[target.ID], 1, "file index must keep the old snapshot")
	assert.Equal(t, "v1.ndjson", store.fileIndex[target.ID][0].Path)
	require.Len(t, store.lineageLinks[target.ID], 1, "lineage must keep the old snapshot")
	require.Len(t, store.files[target.ID], 1, "dataset_files must keep the old snapshot")
	// The failed transaction ended aborted — no OPEN leftovers.
	var openLeft int
	for _, tx := range store.runtimeTransactions {
		if tx.DatasetID == target.ID && tx.Status == models.TransactionStatusOpen {
			openLeft++
		}
	}
	assert.Zero(t, openLeft, "failed publish must not leave an OPEN transaction")

	// Retry after the fault clears: the same commit lands — the version
	// advances by exactly one, a second COMMITTED transaction exists, the
	// index carries the new file and the trigger projection appended it.
	store.publishErr = nil
	rec = do(h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+rid+"/outputs:commit", commitBody("v2.ndjson"), params, claims)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	assert.Equal(t, v1Version+1, store.datasets[0].CurrentVersion, "retry must advance the version")
	committedTxns := 0
	for _, tx := range store.runtimeTransactions {
		if tx.DatasetID == target.ID && tx.Status == models.TransactionStatusCommitted {
			committedTxns++
		}
	}
	assert.Equal(t, 2, committedTxns, "retry must produce a second committed transaction")
	require.Len(t, store.fileIndex[target.ID], 1)
	assert.Equal(t, "v2.ndjson", store.fileIndex[target.ID][0].Path)
	require.Len(t, store.files[target.ID], 2, "trigger projection appends the retried commit")

	// The old snapshot stays listed after the failed publish and the
	// successful retry (byte-level download round-trips are verified
	// against the real backing store in output_commit_content_test).
	rec = do(h.ListFiles, http.MethodGet, "/v1/datasets/"+target.ID.String()+"/files", "", map[string]string{"id": target.ID.String()}, claims)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, strings.Contains(rec.Body.String(), "v1.ndjson"), "old snapshot file must remain listed")
}

type errPublishInjected struct{}

func (errPublishInjected) Error() string { return "injected publish failure" }
