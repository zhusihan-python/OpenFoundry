//go:build integration

// Real-PG16 handler/API regressions for the schema/branch support
// repair. Before this round the schema API referenced
// dataset_view_schemas columns that no migration created (SQLSTATE
// 42703 on every read — masked by regex-matched pgxmock and the
// in-memory fake), and GetCurrentView joined views across branches.
// These tests drive the real HTTP handlers against a real database.
package repo_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

func schemaViaHandler(t *testing.T, h *handlers.Handlers, rid string, query string, claims *authmw.Claims) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/datasets/"+rid+"/schema"+query, nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("rid", rid)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	req = req.WithContext(authmw.ContextWithClaims(req.Context(), claims))
	rec := httptest.NewRecorder()
	h.GetCurrentSchema(rec, req)
	return rec
}

func errorField(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	return body["error"]
}

const schemaA = `{"file_format":"PARQUET","fields":[{"name":"sku","type":"STRING","nullable":false},{"name":"qty","type":"INTEGER","nullable":true}]}`
const schemaB = `{"file_format":"PARQUET","fields":[{"name":"sku","type":"STRING","nullable":false},{"name":"amount","type":"DOUBLE","nullable":false}]}`

// TestSchemaBranchHTTPRoundTrip: a commit carrying a schema publishes it
// readable through GET /schema per branch — main and feature keep
// independent schemas (the cross-branch join bug), a schema-less dataset
// answers a distinguishable 404, and cross-tenant parents 404 before any
// schema lookup.
func TestSchemaBranchHTTPRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool := faultPool(ctx, t)
	require.NoError(t, repo.Migrate(ctx, pool))

	// The ownership columns exist (the pre-repair database lacked them).
	for _, col := range []string{"dataset_id", "branch", "updated_at"} {
		var exists bool
		require.NoError(t, pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns
			WHERE table_name = 'dataset_view_schemas' AND column_name = $1)`, col).Scan(&exists))
		require.True(t, exists, "dataset_view_schemas.%s must exist after migration", col)
	}

	r := &repo.Repo{Pool: pool}
	h := &handlers.Handlers{Repo: r}
	orgA, orgB := uuid.New(), uuid.New()
	owner := uuid.New()
	orgAClaims := &authmw.Claims{Sub: owner, OrgID: &orgA, Permissions: []string{"dataset.write"}}
	orgBClaims := &authmw.Claims{Sub: uuid.New(), OrgID: &orgB, Permissions: []string{"dataset.write"}}

	target, err := r.CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "schema-http"}, owner, &orgA)
	require.NoError(t, err)
	require.NoError(t, r.EnsureDefaultBranch(ctx, target))
	rid := "ri.foundry.main.dataset." + target.ID.String()

	// main: commit with schema A through the real outputs:commit handler.
	commitBody := `{"branch":"main","schema":` + schemaA + `,
		"files":[{"logical_path":"part-000.ndjson","size_bytes":4,"storage_path":"datasets/` + target.ID.String() + `/out/part-000.ndjson"}]}`
	rec := commitViaHandler(t, h, rid, commitBody, orgAClaims)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	// GET schema?branch=main reads the committed schema back.
	rec = schemaViaHandler(t, h, rid, "?branch=main", orgAClaims)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var gotA models.SchemaResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &gotA))
	require.NotNil(t, gotA.ViewID)
	assert.Equal(t, target.ID, gotA.DatasetID, "schema ownership must point at the dataset")
	require.NotNil(t, gotA.Branch, "branch must be recorded on the schema row")
	assert.Equal(t, "main", *gotA.Branch)
	require.Len(t, gotA.Schema.Fields, 2)
	assert.Equal(t, "sku", gotA.Schema.Fields[0].Name)
	assert.Equal(t, "qty", gotA.Schema.Fields[1].Name)
	assert.NotEmpty(t, gotA.ContentHash)

	// feature: a second branch commits schema B through the runtime
	// transaction path (same trigger).
	feature, err := r.CreateRuntimeBranch(ctx, target.ID, &models.CreateBranchBody{Name: "feature", ParentBranch: strPtr("main")}, owner)
	require.NoError(t, err)
	metaB := models.JSONValue([]byte(`{"schema":` + schemaB + `}`))
	txnB, err := r.StartTransaction(ctx, target.ID, feature.ID, feature.Name, models.TransactionTypeSnapshot, "feature schema", metaB, owner)
	require.NoError(t, err)
	require.NoError(t, r.StageTransactionFiles(ctx, target.ID, txnB.ID, []models.StageTransactionFile{
		{LogicalPath: "feature.ndjson", PhysicalPath: "datasets/" + target.ID.String() + "/out/feature.ndjson", SizeBytes: 4, Operation: models.FileOperationAdd},
	}))
	require.NoError(t, r.MergeTransactionMetadata(ctx, target.ID, txnB.ID, metaB))
	require.NoError(t, r.CommitTransaction(ctx, target.ID, txnB.ID))

	// main keeps schema A; feature serves schema B — no cross-branch leak.
	rec = schemaViaHandler(t, h, rid, "?branch=main", orgAClaims)
	require.Equal(t, http.StatusOK, rec.Code)
	var mainAgain models.SchemaResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &mainAgain))
	require.Len(t, mainAgain.Schema.Fields, 2)
	assert.Equal(t, "qty", mainAgain.Schema.Fields[1].Name, "main schema must stay schema A")

	rec = schemaViaHandler(t, h, rid, "?branch=feature", orgAClaims)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var featureSchema models.SchemaResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &featureSchema))
	require.Len(t, featureSchema.Schema.Fields, 2)
	assert.Equal(t, "amount", featureSchema.Schema.Fields[1].Name, "feature schema must be schema B")
	assert.NotNil(t, featureSchema.Branch)
	assert.Equal(t, "feature", *featureSchema.Branch)

	// Same-branch tie-break: a legacy/manual NULL-head view created LATER
	// than the branch's current head commit must not shadow it —
	// PostgreSQL's DESC default is NULLS FIRST, so the order key must be
	// explicit. The NULL-head view carries a decoy schema (schema B) that
	// would be served if the NULL-head row won.
	mainBranch, err := r.GetRuntimeBranch(ctx, target.ID, "main")
	require.NoError(t, err)
	var decoyViewID uuid.UUID
	err = pool.QueryRow(ctx, `INSERT INTO dataset_views (id, dataset_id, branch_id, head_transaction_id, computed_at, file_count, size_bytes, created_at)
		VALUES ($1, $2, $3, NULL, NOW(), 0, 0, NOW() + interval '1 hour') RETURNING id`,
		uuid.New(), target.ID, mainBranch.ID).Scan(&decoyViewID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO dataset_view_schemas
		(view_id, dataset_id, branch, schema_json, file_format, content_hash, created_at, updated_at)
		VALUES ($1, $2, 'main', $3::jsonb, 'PARQUET', md5($4::text), NOW(), NOW())`,
		decoyViewID, target.ID, schemaB, schemaB)
	require.NoError(t, err)
	var headViewID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT id FROM dataset_views
		WHERE dataset_id = $1 AND head_transaction_id = $2`, target.ID, mainBranch.HeadTransactionID).Scan(&headViewID))

	rec = schemaViaHandler(t, h, rid, "?branch=main", orgAClaims)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var tieBreak models.SchemaResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tieBreak))
	assert.Equal(t, "qty", tieBreak.Schema.Fields[1].Name, "current head schema must win over a later NULL-head view")
	assert.Equal(t, headViewID, tieBreak.ViewID, "the branch head's view must be selected")

	// Schema-less dataset: distinguishable 404 (schema not found), not a
	// 5xx and not a silent empty schema.
	bare, err := r.CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "schema-less"}, owner, &orgA)
	require.NoError(t, err)
	require.NoError(t, r.EnsureDefaultBranch(ctx, bare))
	bareRID := "ri.foundry.main.dataset." + bare.ID.String()
	rec = schemaViaHandler(t, h, bareRID, "?branch=main", orgAClaims)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "schema not found", errorField(t, rec))

	// Cross-tenant parent: 404 dataset not found — before any schema work.
	rec = schemaViaHandler(t, h, rid, "?branch=main", orgBClaims)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "dataset not found", errorField(t, rec))
}

// TestSchemaOwnershipCannotBeClaimed: a schema row whose ownership is
// NULL (unbackfilled / manually orphaned) is never claimable — reads
// through the view-id guard and PUT overwrites both answer 404 without
// touching the row.
func TestSchemaOwnershipCannotBeClaimed(t *testing.T) {
	ctx := context.Background()
	pool := faultPool(ctx, t)
	require.NoError(t, repo.Migrate(ctx, pool))

	r := &repo.Repo{Pool: pool}
	h := &handlers.Handlers{Repo: r}
	org := uuid.New()
	owner := uuid.New()
	claims := &authmw.Claims{Sub: owner, OrgID: &org, Permissions: []string{"dataset.write"}}
	target, err := r.CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "orphan-schema"}, owner, &org)
	require.NoError(t, err)
	require.NoError(t, r.EnsureDefaultBranch(ctx, target))
	rid := "ri.foundry.main.dataset." + target.ID.String()

	// Publish a schema so a view+schema row exists.
	rec := commitViaHandler(t, h, rid, `{"branch":"main","schema":`+schemaA+`}`, claims)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	// Degrade the ownership snapshot (unbackfilled / manually NULLed).
	var viewID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT s.view_id FROM dataset_view_schemas s
		JOIN dataset_views v ON v.id = s.view_id
		WHERE v.dataset_id = $1 ORDER BY v.created_at DESC LIMIT 1`, target.ID).Scan(&viewID))
	_, err = pool.Exec(ctx, `UPDATE dataset_view_schemas SET dataset_id = NULL WHERE view_id = $1`, viewID)
	require.NoError(t, err)

	// Branch-level GET still reports ownership through the AUTHORITATIVE
	// view relation (dataset_views.dataset_id), not the NULL snapshot.
	rec = schemaViaHandler(t, h, rid, "?branch=main", claims)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var got models.SchemaResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, target.ID, got.DatasetID, "branch schema ownership comes from dataset_views")
	assert.Len(t, got.Schema.Fields, 2)

	// A FOREIGN dataset's route must not overwrite this dataset's schema
	// row: PUT through dataset B's path with this view id answers 404 and
	// leaves the row (and its authoritative schema) intact.
	other, err := r.CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "other"}, uuid.New(), &org)
	require.NoError(t, err)
	otherRID := "ri.foundry.main.dataset." + other.ID.String()
	putReq := httptest.NewRequest(http.MethodPost, "/v1/datasets/"+otherRID+"/views/"+viewID.String()+"/schema",
		jsonBody(`{"schema":`+schemaB+`}`))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("rid", otherRID)
	rctx.URLParams.Add("view_id", viewID.String())
	putReq = putReq.WithContext(context.WithValue(putReq.Context(), chi.RouteCtxKey, rctx))
	putReq = putReq.WithContext(authmw.ContextWithClaims(putReq.Context(), claims))
	putRec := httptest.NewRecorder()
	h.PutViewSchema(putRec, putReq)
	require.Equal(t, http.StatusNotFound, putRec.Code, "foreign dataset route must not overwrite the schema row")
	assert.Equal(t, "view not found", errorField(t, putRec))

	var schemaRaw string
	require.NoError(t, pool.QueryRow(ctx, `SELECT schema_json::text FROM dataset_view_schemas WHERE view_id = $1`, viewID).Scan(&schemaRaw))
	assert.Contains(t, schemaRaw, "qty", "original schema content must be intact")
}

// TestSchemaColumnsRepairLegacyDatabase: a database shaped like the
// pre-repair era (columns missing) is repaired by a migration re-run,
// with ownership backfilled from the authoritative relations — and the
// schema API keeps working across the repair.
func TestSchemaColumnsRepairLegacyDatabase(t *testing.T) {
	ctx := context.Background()
	pool := faultPool(ctx, t)
	require.NoError(t, repo.Migrate(ctx, pool))

	r := &repo.Repo{Pool: pool}
	h := &handlers.Handlers{Repo: r}
	org := uuid.New()
	owner := uuid.New()
	claims := &authmw.Claims{Sub: owner, OrgID: &org, Permissions: []string{"dataset.write"}}
	target, err := r.CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "legacy-repair"}, owner, &org)
	require.NoError(t, err)
	require.NoError(t, r.EnsureDefaultBranch(ctx, target))
	rid := "ri.foundry.main.dataset." + target.ID.String()
	rec := commitViaHandler(t, h, rid, `{"branch":"main","schema":`+schemaA+`}`, claims)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	// Rewind to the pre-repair shape: drop the ownership columns (this
	// also drops their FK), like a database migrated by 20260503000001
	// alone. The views/schemas themselves survive.
	for _, col := range []string{"dataset_id", "branch", "updated_at"} {
		_, err := pool.Exec(ctx, `ALTER TABLE dataset_view_schemas DROP COLUMN IF EXISTS `+col)
		require.NoError(t, err)
	}
	// The schema API is broken in this shape (42703) — exactly the
	// read-only review's finding.
	rec = schemaViaHandler(t, h, rid, "?branch=main", claims)
	require.Equal(t, http.StatusInternalServerError, rec.Code, "legacy shape must fail with 5xx, not serve data")

	// The migration set repairs it; a second re-run is a no-op.
	require.NoError(t, repo.Migrate(ctx, pool))
	require.NoError(t, repo.Migrate(ctx, pool))

	rec = schemaViaHandler(t, h, rid, "?branch=main", claims)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var got models.SchemaResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, target.ID, got.DatasetID, "ownership backfilled from dataset_views")
	require.NotNil(t, got.Branch)
	assert.Equal(t, "main", *got.Branch, "branch backfilled from dataset_branches")

	// Backfilled rows are verifiably authoritative.
	var ownerCount, nullOwners int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FILTER (WHERE s.dataset_id = v.dataset_id),
		COUNT(*) FILTER (WHERE s.dataset_id IS NULL)
		FROM dataset_view_schemas s JOIN dataset_views v ON v.id = s.view_id`).Scan(&ownerCount, &nullOwners))
	assert.NotZero(t, ownerCount)
	assert.Zero(t, nullOwners, "every view-backed schema row must carry its dataset")
}

func strPtr(s string) *string { return &s }

func jsonBody(s string) *strings.Reader { return strings.NewReader(s) }
