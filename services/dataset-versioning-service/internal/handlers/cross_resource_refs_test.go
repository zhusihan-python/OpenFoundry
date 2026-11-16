// Cross-resource reference guards — review follow-ups for P1.1:
//
//   - view-id sub-resources must match their parent dataset (the
//     "dataset A parent + dataset B view" mismatch, possible even inside
//     one tenant) across schema read/write, view files and preview;
//   - file-index entries and staged output files must not register
//     storage URIs reaching into another dataset's local object
//     namespace, whatever URI scheme carries the key, while legacy
//     upload layouts keep working.
package handlers_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
)

// crossRefHarness: same-org caller with a local backing store (baseDir
// "store") and two datasets to build parent/child mismatches.
type crossRefHarness struct {
	*tenantIsolationHarness
	backing  *storageabstraction.LocalBackingFS
	datasetA models.Dataset
	datasetB models.Dataset
	claims   *authmw.Claims
}

func newCrossRefHarness(t *testing.T) *crossRefHarness {
	t.Helper()
	th := newTenantIsolationHarness(t)
	org := uuid.New()
	owner := uuid.New()
	a := th.addDataset("parent-a", owner, orgPtr(org))
	b := th.addDataset("child-b", owner, orgPtr(org))
	backing := &storageabstraction.LocalBackingFS{BaseURL: "http://dataset-versioning.local", BaseDir: "store", RootDir: t.TempDir()}
	th.h.BackingFS = backing
	return &crossRefHarness{
		tenantIsolationHarness: th,
		backing:                backing,
		datasetA:               a,
		datasetB:               b,
		claims:                 claimsFor(owner, orgPtr(org)),
	}
}

func (ch *crossRefHarness) viewIn(dataset models.Dataset) (uuid.UUID, models.DatasetView) {
	view := models.DatasetView{ID: uuid.New(), DatasetID: dataset.ID, Name: "v-" + dataset.Name, CreatedAt: time.Now().UTC()}
	ch.store.views[dataset.ID] = append(ch.store.views[dataset.ID], view)
	return view.ID, view
}

// TestViewSubresourcesRejectCrossDatasetViewID: a view id owned by
// dataset B is rejected on every dataset-A view surface — even though
// both datasets share the tenant, so the parent guard alone passes.
func TestViewSubresourcesRejectCrossDatasetViewID(t *testing.T) {
	t.Parallel()
	ch := newCrossRefHarness(t)
	foreignView, _ := ch.viewIn(ch.datasetB)
	ch.store.schemas[foreignView] = models.SchemaResponse{ViewID: foreignView, DatasetID: ch.datasetB.ID}

	aRID := "ri.foundry.main.dataset." + ch.datasetA.ID.String()
	params := map[string]string{"rid": aRID, "view_id": foreignView.String()}

	rec := do(ch.h.GetViewSchema, http.MethodGet, "/v1/datasets/"+aRID+"/views/"+foreignView.String()+"/schema", "", params, ch.claims)
	assert.Equal(t, http.StatusNotFound, rec.Code, "GetViewSchema with foreign view")

	schemaBody := `{"schema":{"file_format":"PARQUET","fields":[{"name":"a","type":"STRING","nullable":false}]}}`
	rec = do(ch.h.PutViewSchema, http.MethodPost, "/v1/datasets/"+aRID+"/views/"+foreignView.String()+"/schema", schemaBody, params, ch.claims)
	assert.Equal(t, http.StatusNotFound, rec.Code, "PutViewSchema onto foreign view")
	assert.Equal(t, ch.datasetB.ID, ch.store.schemas[foreignView].DatasetID, "foreign schema must stay untouched")

	rec = do(ch.h.ListViewFiles, http.MethodGet, "/v1/datasets/"+aRID+"/views/"+foreignView.String()+"/files", "", params, ch.claims)
	assert.Equal(t, http.StatusNotFound, rec.Code, "ListViewFiles with foreign view")

	rec = do(ch.h.PreviewViewData, http.MethodGet, "/v1/datasets/"+aRID+"/views/"+foreignView.String()+"/data", "", params, ch.claims)
	assert.Equal(t, http.StatusNotFound, rec.Code, "PreviewViewData with foreign view")

	// A view actually owned by dataset A is served.
	ownView, _ := ch.viewIn(ch.datasetA)
	ch.store.schemas[ownView] = models.SchemaResponse{ViewID: ownView, DatasetID: ch.datasetA.ID}
	ownParams := map[string]string{"rid": aRID, "view_id": ownView.String()}
	rec = do(ch.h.GetViewSchema, http.MethodGet, "/v1/datasets/"+aRID+"/views/"+ownView.String()+"/schema", "", ownParams, ch.claims)
	assert.Equal(t, http.StatusOK, rec.Code, "own view schema")

	// Schema-only views (no dataset_views row) keep their flows: a
	// foreign schema row is still rejected, an unknown view id is
	// writable (schema-only creation) and then readable.
	foreignSchemaOnly := uuid.New()
	ch.store.schemas[foreignSchemaOnly] = models.SchemaResponse{ViewID: foreignSchemaOnly, DatasetID: ch.datasetB.ID}
	rec = do(ch.h.GetViewSchema, http.MethodGet, "/v1/datasets/"+aRID+"/views/"+foreignSchemaOnly.String()+"/schema", "", map[string]string{"rid": aRID, "view_id": foreignSchemaOnly.String()}, ch.claims)
	assert.Equal(t, http.StatusNotFound, rec.Code, "schema-only foreign view")

	fresh := uuid.New()
	freshParams := map[string]string{"rid": aRID, "view_id": fresh.String()}
	rec = do(ch.h.PutViewSchema, http.MethodPost, "/v1/datasets/"+aRID+"/views/"+fresh.String()+"/schema", schemaBody, freshParams, ch.claims)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	rec = do(ch.h.GetViewSchema, http.MethodGet, "/v1/datasets/"+aRID+"/views/"+fresh.String()+"/schema", "", freshParams, ch.claims)
	assert.Equal(t, http.StatusOK, rec.Code, "schema-only view round-trip")
}

// TestFileIndexRejectsForeignStoragePaths: registered index entries must
// stay inside the dataset's own object namespace — across local URIs,
// other-scheme URIs (whose keys the local backing serves verbatim), bare
// paths and transaction-scoped paths — while legacy layouts pass.
func TestFileIndexRejectsForeignStoragePaths(t *testing.T) {
	t.Parallel()
	ch := newCrossRefHarness(t)
	dRID := "ri.foundry.main.dataset." + ch.datasetA.ID.String()
	params := map[string]string{"rid": dRID}

	putIndex := func(storagePath string) *int {
		body := `{"files":[{"path":"p","storage_path":` + strconv.Quote(storagePath) + `,"size_bytes":1}]}`
		rec := do(ch.h.PutDatasetFileIndex, http.MethodPut, "/v1/datasets/"+dRID+"/files/index", body, params, ch.claims)
		return &rec.Code
	}

	// Forged references into dataset B's namespace — every scheme form.
	for _, forged := range []string{
		"local:///datasets/" + ch.datasetB.ID.String() + "/uploads/secret.csv",
		"s3://attacker/store/datasets/" + ch.datasetB.ID.String() + "/uploads/secret.csv",
		"datasets/" + ch.datasetB.ID.String() + "/uploads/secret.csv",
		"store/datasets/" + ch.datasetB.ID.String() + "/uploads/secret.csv",
	} {
		code := putIndex(forged)
		assert.Equal(t, http.StatusBadRequest, *code, "forged storage_path %q", forged)
	}

	// A transaction belonging to dataset B must not be reachable either.
	foreignTxn := uuid.New()
	ch.store.runtimeTransactions[foreignTxn] = models.RuntimeTransaction{ID: foreignTxn, DatasetID: ch.datasetB.ID, BranchName: "main", Status: models.TransactionStatusOpen}
	code := putIndex("transactions/" + foreignTxn.String() + "/part.csv")
	assert.Equal(t, http.StatusBadRequest, *code, "foreign transaction object")

	// Legal layouts: this dataset's upload namespace (with and without
	// the base-dir prefix), external placeholders, and this dataset's own
	// transaction objects.
	ownTxn := uuid.New()
	ch.store.runtimeTransactions[ownTxn] = models.RuntimeTransaction{ID: ownTxn, DatasetID: ch.datasetA.ID, BranchName: "main", Status: models.TransactionStatusOpen}
	for _, legal := range []string{
		"store/datasets/" + ch.datasetA.ID.String() + "/uploads/orders.csv",
		"datasets/" + ch.datasetA.ID.String() + "/uploads/orders.csv",
		"s3://bucket/sales",
		"outputs/run-42/part-000.ndjson",
		"transactions/" + ownTxn.String() + "/part.csv",
	} {
		code := putIndex(legal)
		require.Equal(t, http.StatusOK, *code, "legal storage_path %q", legal)
	}
}

// TestOutputCommitRejectsForeignStoragePaths: outputs:commit stages
// caller-supplied storage paths; forged references into another
// dataset's namespace are rejected before any object write or staging,
// while explicit paths inside the dataset's own namespace commit fine.
func TestOutputCommitRejectsForeignStoragePaths(t *testing.T) {
	t.Parallel()
	ch := newCrossRefHarness(t)
	dRID := "ri.foundry.main.dataset." + ch.datasetA.ID.String()
	params := map[string]string{"rid": dRID}

	foreignTxn := uuid.New()
	ch.store.runtimeTransactions[foreignTxn] = models.RuntimeTransaction{ID: foreignTxn, DatasetID: ch.datasetB.ID, BranchName: "main", Status: models.TransactionStatusOpen}

	for _, forged := range []string{
		"local:///datasets/" + ch.datasetB.ID.String() + "/secret.csv",
		"datasets/" + ch.datasetB.ID.String() + "/secret.csv",
		"s3://attacker/store/datasets/" + ch.datasetB.ID.String() + "/secret.csv",
		"transactions/" + foreignTxn.String() + "/part.csv",
	} {
		body := `{"branch":"main","files":[{"logical_path":"part.ndjson","size_bytes":4,"storage_path":` + strconv.Quote(forged) + `}]}`
		rec := do(ch.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+dRID+"/outputs:commit", body, params, ch.claims)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "forged outputs:commit storage_path %q", forged)
	}
	// Nothing was staged by the rejected commits.
	require.Empty(t, ch.store.stagedFiles)

	// Explicit path inside this dataset's own namespace commits.
	body := `{"branch":"main","files":[{"logical_path":"part.ndjson","size_bytes":4,"storage_path":"store/datasets/` + ch.datasetA.ID.String() + `/custom/part.ndjson"}]}`
	rec := do(ch.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+dRID+"/outputs:commit", body, params, ch.claims)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var out models.CommitDatasetOutputResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Equal(t, models.TransactionStatusCommitted, out.Transaction.Status)
}
