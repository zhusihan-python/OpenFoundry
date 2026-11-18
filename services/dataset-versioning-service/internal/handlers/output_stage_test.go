// WP3.3 increment 3A unit coverage for outputs:stage and the STAGED_ADD
// keyed commit: streaming validation (scan-identical record cap incl
// newline, JSON-object rule), the single-artifact lease model
// (re-transmission reuse/conflict, in-flight, consumed), fail-closed
// configuration and namespace boundaries, request-shape negatives, and
// the full staged commit against the in-memory store (ADD conversion,
// reservation finalization, descriptor consumption, install, replay and
// fingerprint conflict). Real-PostgreSQL semantics live in the
// integration suite (staged_output_commit_integration_test.go).

package handlers_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
)

type stageFixture struct {
	store       *fakeStore
	h           *handlers.Handlers
	rid         string
	datasetID   uuid.UUID
	org         uuid.UUID
	claims      *authmw.Claims
	stageRoot   string
	backingRoot string
	reservation uuid.UUID
	build       uuid.UUID
}

func newStageFixture(t *testing.T) *stageFixture {
	t.Helper()
	owner, org := uuid.New(), uuid.New()
	store := newFakeStore(owner)
	backingRoot := t.TempDir()
	stageRoot := t.TempDir()
	fs := storageabstraction.NewLocalBackingFS("http://files.local", "store", []byte("stage-test"))
	fs.RootDir = backingRoot
	h := &handlers.Handlers{Repo: store, BackingFS: fs, StagedOutputRoot: stageRoot}
	target := store.datasets[0]
	target.OrgID = orgPtr(org)
	target.ActiveBranch = "main"
	store.datasets[0] = target
	rid := "ri.foundry.main.dataset." + target.ID.String()
	claims := claimsFor(owner, orgPtr(org))
	reservation, build := uuid.New(), uuid.New()
	sub := claims.Sub
	require.NoError(t, store.EnsureDefaultBranch(context.Background(), &target))
	mainBranch := store.branches[target.ID][0]
	store.runtimeTransactions[reservation] = models.RuntimeTransaction{
		ID: reservation, DatasetID: target.ID, BranchID: mainBranch.ID, BranchName: "main",
		TxType: models.TransactionTypeUpdate, Status: models.TransactionStatusOpen,
		StartedBy:  &sub,
		Providence: models.JSONValue([]byte(fmt.Sprintf(`{"origin":"pipeline-build-service","purpose":"wp33-staged-output","build_id":%q}`, build))),
	}
	return &stageFixture{
		store: store, h: h, rid: rid, datasetID: target.ID, org: org, claims: claims,
		stageRoot: stageRoot, backingRoot: backingRoot,
		reservation: reservation, build: build,
	}
}

func (fx *stageFixture) stageURL(overrides map[string]string) string {
	q := map[string]string{
		"branch":          "main",
		"reservation_id":  fx.reservation.String(),
		"build_id":        fx.build.String(),
		"logical_path":    "part-00000.ndjson",
		"idempotency_key": "of-wp33-stage-v1:" + fx.reservation.String(),
	}
	for k, v := range overrides {
		if v == "" {
			delete(q, k)
			continue
		}
		q[k] = v
	}
	values := make(url.Values, len(q))
	for k, v := range q {
		values.Set(k, v)
	}
	return "/v1/datasets/" + fx.rid + "/outputs:stage?" + values.Encode()
}

func (fx *stageFixture) stage(t *testing.T, body string, overrides map[string]string) *stageReply {
	t.Helper()
	rec := do(fx.h.StageDatasetOutput, http.MethodPost, fx.stageURL(overrides), body, map[string]string{"rid": fx.rid}, fx.claims)
	reply := &stageReply{code: rec.Code, raw: rec.Body.String()}
	if rec.Code == http.StatusCreated || rec.Code == http.StatusOK {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &reply.resp), rec.Body.String())
	}
	if rec.Code >= 400 {
		_ = json.Unmarshal(rec.Body.Bytes(), &reply.err)
	}
	return reply
}

type stageReply struct {
	code int
	raw  string
	resp models.StageDatasetOutputResponse
	err  models.ErrorEnvelope
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func stageSealedPath(fx *stageFixture, stagedObjectID string) string {
	return filepath.Join(fx.stageRoot, "datasets", fx.datasetID.String(), "reservations", fx.reservation.String(), stagedObjectID+".ndjson")
}

func TestStageOutputSealsRecordsAndDigest(t *testing.T) {
	fx := newStageFixture(t)
	body := `{"a":1}` + "\n" + `{"b":"x"}` + "\n" + `{"c":3}` // last record without trailing newline
	reply := fx.stage(t, body, nil)
	require.Equal(t, http.StatusCreated, reply.code, reply.raw)
	assert.Equal(t, sha256Hex([]byte(body)), reply.resp.SHA256)
	assert.Equal(t, int64(len(body)), reply.resp.SizeBytes)
	assert.Equal(t, int64(3), reply.resp.RecordCount)
	assert.NotEqual(t, uuid.Nil.String(), reply.resp.StagedObjectID)
	sealed, err := os.ReadFile(stageSealedPath(fx, reply.resp.StagedObjectID))
	require.NoError(t, err)
	assert.Equal(t, body, string(sealed), "sealed bytes must be the exact streamed bytes")
	obj := fx.store.stagedObjects[uuid.MustParse(reply.resp.StagedObjectID)]
	require.NotNil(t, obj)
	assert.Equal(t, repo.StagedObjectStatusSealed, obj.Status)
}

func TestStageOutputEmptyBodyZeroRows(t *testing.T) {
	fx := newStageFixture(t)
	reply := fx.stage(t, "", nil)
	require.Equal(t, http.StatusCreated, reply.code, reply.raw)
	assert.Equal(t, int64(0), reply.resp.SizeBytes)
	assert.Equal(t, int64(0), reply.resp.RecordCount)
	assert.Equal(t, sha256Hex(nil), reply.resp.SHA256)
	sealed, err := os.ReadFile(stageSealedPath(fx, reply.resp.StagedObjectID))
	require.NoError(t, err)
	assert.Empty(t, sealed)
}

func TestStageOutputRecordCapMatchesScan(t *testing.T) {
	fx := newStageFixture(t)
	// The scan boundary: a record of exactly datasetFileRowsMaxLineBytes
	// INCLUDING its newline is accepted; one byte over is rejected.
	record := func(contentLen int) string {
		b := make([]byte, 0, contentLen+1)
		b = append(b, `{"p":"`...)
		for len(b) < contentLen-2 {
			b = append(b, '1')
		}
		b = append(b, `"}`...)
		b = append(b, '\n')
		return string(b)
	}
	ok := fx.stage(t, record(1024*1024-1), nil) // content + newline = 1MiB
	require.Equal(t, http.StatusCreated, ok.code, ok.raw)

	reservation2, build2 := uuid.New(), uuid.New()
	sub := fx.claims.Sub
	fx.store.runtimeTransactions[reservation2] = models.RuntimeTransaction{
		ID: reservation2, DatasetID: fx.datasetID, BranchName: "main",
		TxType: models.TransactionTypeUpdate, Status: models.TransactionStatusOpen,
		StartedBy:  &sub,
		Providence: models.JSONValue([]byte(fmt.Sprintf(`{"purpose":"wp33-staged-output","build_id":%q}`, build2))),
	}
	bad := fx.stage(t, record(1024*1024), map[string]string{ // content + newline = 1MiB+1
		"reservation_id":  reservation2.String(),
		"build_id":        build2.String(),
		"idempotency_key": "of-wp33-stage-v1:" + reservation2.String(),
	})
	require.Equal(t, http.StatusRequestEntityTooLarge, bad.code, bad.raw)
	require.NotNil(t, bad.err.Code)
	assert.Equal(t, "STAGED_RECORD_TOO_LARGE", *bad.err.Code)
	for _, obj := range fx.store.stagedObjects {
		assert.NotEqual(t, reservation2, obj.ReservationID, "over-cap attempt must leave no descriptor")
	}
}

func TestStageOutputInvalidRecord(t *testing.T) {
	fx := newStageFixture(t)
	for name, body := range map[string]string{
		"array record":   `[1,2]` + "\n",
		"scalar record":  `42` + "\n",
		"blank line":     "\n",
		"whitespace":     "   \n",
		"broken json":    `{"a":` + "\n",
		"nested garbage": `{"a":1}` + "\n" + `nope` + "\n",
	} {
		reply := fx.stage(t, body, nil)
		assert.Equal(t, http.StatusBadRequest, reply.code, name)
		require.NotNil(t, reply.err.Code, name)
		assert.Equal(t, "STAGED_RECORD_INVALID", *reply.err.Code, name)
	}
	assert.Empty(t, fx.store.stagedObjects)
	// No temp or sealed leftovers under the reservation directory.
	entries, err := os.ReadDir(filepath.Join(fx.stageRoot, "datasets", fx.datasetID.String(), "reservations", fx.reservation.String()))
	require.NoError(t, err)
	assert.Empty(t, entries, "failed attempts must clean their spool files")
}

func TestStageOutputRetransmission(t *testing.T) {
	fx := newStageFixture(t)
	body := `{"n":1}` + "\n" + `{"n":2}` + "\n"
	first := fx.stage(t, body, nil)
	require.Equal(t, http.StatusCreated, first.code, first.raw)
	sealedBefore, err := os.ReadFile(stageSealedPath(fx, first.resp.StagedObjectID))
	require.NoError(t, err)

	same := fx.stage(t, body, nil)
	require.Equal(t, http.StatusOK, same.code, same.raw)
	assert.Equal(t, first.resp.StagedObjectID, same.resp.StagedObjectID)
	assert.Equal(t, first.resp.SHA256, same.resp.SHA256)
	assert.Len(t, fx.store.stagedObjects, 1, "reuse must not create a second artifact")

	other := fx.stage(t, `{"n":3}`+"\n", nil)
	require.Equal(t, http.StatusConflict, other.code, other.raw)
	require.NotNil(t, other.err.Code)
	assert.Equal(t, "STAGED_OBJECT_CONFLICT", *other.err.Code)
	sealedAfter, err := os.ReadFile(stageSealedPath(fx, first.resp.StagedObjectID))
	require.NoError(t, err)
	assert.Equal(t, sealedBefore, sealedAfter, "conflict must never touch the sealed file")
}

func TestStageOutputInFlightAndConsumed(t *testing.T) {
	fx := newStageFixture(t)
	fx.store.stagedObjects = map[uuid.UUID]*repo.StagedOutputObject{}
	// A live lease held by another attempt blocks the slot.
	other := uuid.New()
	live := time.Now().UTC().Add(10 * time.Minute)
	fx.store.stagedObjects[other] = &repo.StagedOutputObject{
		ID: other, DatasetID: fx.datasetID, ReservationID: fx.reservation,
		Status: repo.StagedObjectStatusStaging, LeaseExpiresAt: &live,
	}
	reply := fx.stage(t, `{"a":1}`+"\n", nil)
	require.Equal(t, http.StatusConflict, reply.code, reply.raw)
	require.NotNil(t, reply.err.Code)
	assert.Equal(t, "STAGED_UPLOAD_IN_FLIGHT", *reply.err.Code)

	// Consumed is terminal: re-staging is rejected outright.
	delete(fx.store.stagedObjects, other)
	consumed := &repo.StagedOutputObject{
		ID: other, DatasetID: fx.datasetID, ReservationID: fx.reservation,
		Status: repo.StagedObjectStatusConsumed,
	}
	fx.store.stagedObjects[other] = consumed
	reply = fx.stage(t, `{"a":1}`+"\n", nil)
	require.Equal(t, http.StatusConflict, reply.code, reply.raw)
	require.NotNil(t, reply.err.Code)
	assert.Equal(t, "STAGED_OBJECT_ALREADY_PUBLISHED", *reply.err.Code)
}

func TestStageOutputParamAndConfigNegatives(t *testing.T) {
	fx := newStageFixture(t)
	cases := []struct {
		name      string
		overrides map[string]string
		wantCode  int
	}{
		{"missing key", map[string]string{"idempotency_key": ""}, 400},
		{"bad reservation", map[string]string{"reservation_id": "nope"}, 400},
		{"bad build", map[string]string{"build_id": "nope"}, 400},
		{"invalid key charset", map[string]string{"idempotency_key": "has space!"}, 400},
		{"logical not ndjson", map[string]string{"logical_path": "part.csv"}, 400},
		{"logical traversal", map[string]string{"logical_path": "../escape.ndjson"}, 400},
		{"reserved subtree", map[string]string{"logical_path": "transactions/" + uuid.NewString() + "/x.ndjson"}, 400},
		{"non-active branch", map[string]string{"branch": "master"}, 400},
	}
	for _, tc := range cases {
		reply := fx.stage(t, `{"a":1}`+"\n", tc.overrides)
		assert.Equal(t, tc.wantCode, reply.code, tc.name+": "+reply.raw)
	}

	// Unconfigured staging root fails closed with 503.
	noRoot := fx.h
	noRoot.StagedOutputRoot = ""
	reply := do(noRoot.StageDatasetOutput, http.MethodPost, fx.stageURL(nil), `{"a":1}`+"\n", map[string]string{"rid": fx.rid}, fx.claims)
	assert.Equal(t, http.StatusServiceUnavailable, reply.Code, reply.Body.String())

	// A staging root inside the backing filesystem root is rejected on
	// resolved paths (fail closed, nothing written).
	inside := fx.h
	inside.StagedOutputRoot = filepath.Join(fx.backingRoot, "stage")
	reply = do(inside.StageDatasetOutput, http.MethodPost, fx.stageURL(nil), `{"a":1}`+"\n", map[string]string{"rid": fx.rid}, fx.claims)
	assert.Equal(t, http.StatusServiceUnavailable, reply.Code, reply.Body.String())
}

func TestStageOutputReservationBindingNegatives(t *testing.T) {
	// Wrong build marker: a reservation without the staged purpose can
	// never be claimed.
	fx := newStageFixture(t)
	sub := fx.claims.Sub
	legacy := uuid.New()
	fx.store.runtimeTransactions[legacy] = models.RuntimeTransaction{
		ID: legacy, DatasetID: fx.datasetID, BranchName: "main",
		TxType: models.TransactionTypeUpdate, Status: models.TransactionStatusOpen,
		StartedBy:  &sub,
		Providence: models.JSONValue([]byte(`{"origin":"pipeline-build-service"}`)),
	}
	reply := fx.stage(t, `{"a":1}`+"\n", map[string]string{"reservation_id": legacy.String(), "build_id": fx.build.String(), "idempotency_key": "of-wp33-stage-v1:" + legacy.String()})
	require.Equal(t, http.StatusBadRequest, reply.code, reply.raw)

	// A SNAPSHOT reservation (the PBS pre-open shape) is not a staged
	// reservation either.
	snapshot := uuid.New()
	fx.store.runtimeTransactions[snapshot] = models.RuntimeTransaction{
		ID: snapshot, DatasetID: fx.datasetID, BranchName: "main",
		TxType: models.TransactionTypeSnapshot, Status: models.TransactionStatusOpen,
		StartedBy:  &sub,
		Providence: models.JSONValue([]byte(fmt.Sprintf(`{"purpose":"wp33-staged-output","build_id":%q}`, fx.build))),
	}
	reply = fx.stage(t, `{"a":1}`+"\n", map[string]string{"reservation_id": snapshot.String(), "idempotency_key": "of-wp33-stage-v1:" + snapshot.String()})
	require.Equal(t, http.StatusBadRequest, reply.code, reply.raw)

	// Unknown reservation: 404 without side effects.
	reply = fx.stage(t, `{"a":1}`+"\n", map[string]string{"reservation_id": uuid.NewString()})
	assert.Equal(t, http.StatusNotFound, reply.code, reply.raw)
}

func stagedCommitBody(key string, stagedObjectID string, size int64, sha string, reservation, build uuid.UUID, mutate func(map[string]any)) string {
	m := map[string]any{
		"idempotency_key":  key,
		"branch":           "main",
		"transaction_type": "SNAPSHOT",
		"reservation_id":   reservation.String(),
		"build_id":         build.String(),
		"files": []map[string]any{{
			"logical_path":     "part-00000.ndjson",
			"operation":        "STAGED_ADD",
			"staged_object_id": stagedObjectID,
			"size_bytes":       size,
			"sha256":           sha,
		}},
	}
	if mutate != nil {
		mutate(m)
	}
	raw, _ := json.Marshal(m)
	return string(raw)
}

func TestStagedCommitMixedShapeNegatives(t *testing.T) {
	fx := newStageFixture(t)
	staged := fx.stage(t, `{"a":1}`+"\n", nil)
	require.Equal(t, http.StatusCreated, staged.code, staged.raw)
	key := "of-wp33-stage-v1:" + fx.reservation.String()

	// A staged reference without the STAGED_ADD operation must never fall
	// through to an ordinary or metadata-only publish that silently
	// ignores it — keyed or legacy alike.
	mutateDropOp := func(m map[string]any) { delete(m["files"].([]map[string]any)[0], "operation") }
	cases := []struct {
		name  string
		body  string
		noKey bool
	}{
		{"staged_object_id without operation", stagedCommitBody(key, staged.resp.StagedObjectID, staged.resp.SizeBytes, staged.resp.SHA256, fx.reservation, fx.build, mutateDropOp), false},
		{"sha256 without staged reference", stagedCommitBody(key, "", staged.resp.SizeBytes, staged.resp.SHA256, fx.reservation, fx.build, func(m map[string]any) {
			file := m["files"].([]map[string]any)[0]
			delete(file, "operation")
			delete(file, "staged_object_id")
		}), false},
		{"staged_object_id without operation or key", stagedCommitBody(key, staged.resp.StagedObjectID, staged.resp.SizeBytes, staged.resp.SHA256, fx.reservation, fx.build, mutateDropOp), true},
	}
	for _, tc := range cases {
		body := tc.body
		if tc.noKey {
			body = strings.Replace(body, `"idempotency_key":"`+key+`",`, "", 1)
		}
		rec := do(fx.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+fx.rid+"/outputs:commit", body, map[string]string{"rid": fx.rid}, fx.claims)
		assert.Equal(t, http.StatusBadRequest, rec.Code, tc.name+": "+rec.Body.String())
	}
	// Nothing was published through any path.
	assert.Empty(t, fx.store.outputCommitReceipts)
	assert.Equal(t, models.TransactionStatusOpen, fx.store.runtimeTransactions[fx.reservation].Status)
}

func TestStagedCommitLegacyWithoutKeyRejected(t *testing.T) {
	fx := newStageFixture(t)
	body := strings.Replace(stagedCommitBody("k", uuid.NewString(), 1, sha256Hex([]byte("x")), fx.reservation, fx.build, nil), `"idempotency_key":"k",`, "", 1)
	rec := do(fx.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+fx.rid+"/outputs:commit", body, map[string]string{"rid": fx.rid}, fx.claims)
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "staged commit requires idempotency_key")
}

func TestStagedCommitRequestShapeNegatives(t *testing.T) {
	fx := newStageFixture(t)
	staged := fx.stage(t, `{"a":1}`+"\n", nil)
	require.Equal(t, http.StatusCreated, staged.code, staged.raw)
	key := "of-wp33-stage-v1:" + fx.reservation.String() // must equal the stage's key: the descriptor binds it
	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"storage_path", func(m map[string]any) { m["files"].([]map[string]any)[0]["storage_path"] = "outputs/x.ndjson" }},
		{"content_base64", func(m map[string]any) { m["files"].([]map[string]any)[0]["content_base64"] = "" }},
		{"two files", func(m map[string]any) {
			m["files"] = append(m["files"].([]map[string]any), map[string]any{"logical_path": "b.ndjson", "operation": "STAGED_ADD", "staged_object_id": staged.resp.StagedObjectID, "size_bytes": 0, "sha256": staged.resp.SHA256})
		}},
		{"bad digest len", func(m map[string]any) { m["files"].([]map[string]any)[0]["sha256"] = "deadbeef" }},
		{"non-uuid staged id", func(m map[string]any) { m["files"].([]map[string]any)[0]["staged_object_id"] = "nope" }},
		{"append type", func(m map[string]any) { m["transaction_type"] = "APPEND" }},
		{"preview rows", func(m map[string]any) { m["preview_rows"] = [][]any{{"x"}} }},
		{"missing reservation", func(m map[string]any) { delete(m, "reservation_id") }},
	}
	for _, tc := range cases {
		body := stagedCommitBody(key, staged.resp.StagedObjectID, staged.resp.SizeBytes, staged.resp.SHA256, fx.reservation, fx.build, tc.mutate)
		rec := do(fx.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+fx.rid+"/outputs:commit", body, map[string]string{"rid": fx.rid}, fx.claims)
		assert.Equal(t, http.StatusBadRequest, rec.Code, tc.name+": "+rec.Body.String())
	}
	// Digest mismatch against the sealed authority is a 400 after the
	// shape check.
	body := stagedCommitBody(key, staged.resp.StagedObjectID, staged.resp.SizeBytes+1, staged.resp.SHA256, fx.reservation, fx.build, nil)
	rec := do(fx.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+fx.rid+"/outputs:commit", body, map[string]string{"rid": fx.rid}, fx.claims)
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "size or sha256")
}

func TestStagedCommitKeyedHappyPathReplayConflict(t *testing.T) {
	fx := newStageFixture(t)
	staged := fx.stage(t, `{"a":1}`+"\n"+`{"a":2}`+"\n", nil)
	require.Equal(t, http.StatusCreated, staged.code, staged.raw)
	key := "of-wp33-stage-v1:" + fx.reservation.String() // must equal the stage's key: the descriptor binds it
	body := stagedCommitBody(key, staged.resp.StagedObjectID, staged.resp.SizeBytes, staged.resp.SHA256, fx.reservation, fx.build, nil)

	first := do(fx.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+fx.rid+"/outputs:commit", body, map[string]string{"rid": fx.rid}, fx.claims)
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	var decoded models.CommitDatasetOutputResponse
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &decoded))

	// The reservation is finalized, the descriptor consumed, and the
	// staged-file row landed as ADD — never STAGED_ADD.
	resv := fx.store.runtimeTransactions[fx.reservation]
	assert.Equal(t, models.TransactionStatusAborted, resv.Status)
	obj := fx.store.stagedObjects[uuid.MustParse(staged.resp.StagedObjectID)]
	require.NotNil(t, obj)
	assert.Equal(t, repo.StagedObjectStatusConsumed, obj.Status)
	assert.NotNil(t, obj.ConsumedByTransaction)
	for _, files := range fx.store.stagedFiles {
		for _, f := range files {
			assert.NotEqual(t, models.FileOperation("STAGED_ADD"), f.Operation, "STAGED_ADD must be converted to ADD before staging")
		}
	}

	// The sealed bytes are installed at the server-named transaction
	// object key inside the backing root.
	installedRel := strings.TrimPrefix(decoded.Files[0].StoragePath, "local:///")
	installed, err := os.ReadFile(filepath.Join(fx.backingRoot, installedRel))
	require.NoError(t, err)
	assert.Equal(t, `{"a":1}`+"\n"+`{"a":2}`+"\n", string(installed))

	versionAfterFirst := fx.store.datasets[0].CurrentVersion
	replay := do(fx.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+fx.rid+"/outputs:commit", body, map[string]string{"rid": fx.rid}, fx.claims)
	require.Equal(t, http.StatusOK, replay.Code, replay.Body.String())
	assert.Equal(t, first.Body.String(), replay.Body.String())
	assert.Equal(t, versionAfterFirst, fx.store.datasets[0].CurrentVersion, "replay must not advance the version")

	conflict := do(fx.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+fx.rid+"/outputs:commit", stagedCommitBody(key, staged.resp.StagedObjectID, staged.resp.SizeBytes, staged.resp.SHA256, fx.reservation, fx.build, func(m map[string]any) { m["summary"] = "different bytes" }), map[string]string{"rid": fx.rid}, fx.claims)
	assert.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())
}

func TestStagedCommitWrongActorAndBranchNegatives(t *testing.T) {
	fx := newStageFixture(t)
	staged := fx.stage(t, `{"a":1}`+"\n", nil)
	require.Equal(t, http.StatusCreated, staged.code, staged.raw)
	key := "of-wp33-stage-v1:" + fx.reservation.String() // must equal the stage's key: the descriptor binds it
	body := stagedCommitBody(key, staged.resp.StagedObjectID, staged.resp.SizeBytes, staged.resp.SHA256, fx.reservation, fx.build, nil)

	// Another principal (same tenant, write scope) cannot commit the
	// reservation: the actor binding fails before anything moves.
	otherClaims := claimsFor(uuid.New(), orgPtr(fx.org))
	rec := do(fx.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+fx.rid+"/outputs:commit", body, map[string]string{"rid": fx.rid}, otherClaims)
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

	// A non-active branch is rejected by shape-level branch semantics.
	offBranch := stagedCommitBody(key, staged.resp.StagedObjectID, staged.resp.SizeBytes, staged.resp.SHA256, fx.reservation, fx.build, func(m map[string]any) { m["branch"] = "master" })
	rec = do(fx.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+fx.rid+"/outputs:commit", offBranch, map[string]string{"rid": fx.rid}, fx.claims)
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "active branch")
}

// Final-review round 2: JSON null body/file metadata must not crash the
// merge, and the server-stamped fields survive caller attempts to forge
// them (the caller's keys are written first, the server's last).
func TestStagedCommitNullMetadataAndServerStamps(t *testing.T) {
	fx := newStageFixture(t)
	staged := fx.stage(t, `{"m":1}`+"\n", nil)
	require.Equal(t, http.StatusCreated, staged.code, staged.raw)
	// Case 1: JSON null body and file metadata must not crash the merge.
	key := "of-wp33-stage-v1:" + fx.reservation.String()
	body := stagedCommitBody(key, staged.resp.StagedObjectID, staged.resp.SizeBytes, staged.resp.SHA256, fx.reservation, fx.build, func(m map[string]any) {
		m["metadata"] = nil // JSON null unmarshals into a nil map
		m["files"].([]map[string]any)[0]["metadata"] = nil
	})
	rec := do(fx.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+fx.rid+"/outputs:commit", body, map[string]string{"rid": fx.rid}, fx.claims)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	// Case 2 (fresh reservation + stage): the caller forges every
	// server-stamped key; the stamps must still win.
	build2 := uuid.New()
	reservation2, staged2 := newStageFixtureReservation(t, fx, build2)
	forge := stagedCommitBody("of-wp33-stage-v1:"+reservation2.String(), staged2.StagedObjectID, staged2.SizeBytes, staged2.SHA256, reservation2, build2, func(m map[string]any) {
		m["metadata"] = map[string]any{
			"staged_output": "forged",
			"file_count":    99,
			"sha256":        "caller-forged",
		}
		m["files"].([]map[string]any)[0]["metadata"] = map[string]any{"sha256": "file-forged"}
	})
	rec = do(fx.h.CommitDatasetOutput, http.MethodPost, "/v1/datasets/"+fx.rid+"/outputs:commit", forge, map[string]string{"rid": fx.rid}, fx.claims)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	// The transaction metadata carries the server-stamped values.
	txnMeta := map[string]any{}
	require.NoError(t, json.Unmarshal(fx.store.runtimeTransactions[lastStagedTxnID(fx)].Metadata, &txnMeta))
	assert.Equal(t, float64(1), txnMeta["file_count"])
	stamp, _ := txnMeta["staged_output"].(map[string]any)
	require.NotNil(t, stamp)
	assert.Equal(t, staged2.StagedObjectID, stamp["staged_object_id"])
	assert.Equal(t, staged2.SHA256, stamp["sha256"])
	assert.NotEqual(t, "forged", txnMeta["staged_output"])

	// The file index metadata keeps the server identity and digest.
	entries := fx.store.fileIndex[fx.datasetID]
	require.NotEmpty(t, entries)
	fileMeta := map[string]any{}
	require.NoError(t, json.Unmarshal(entries[len(entries)-1].Metadata, &fileMeta))
	assert.Equal(t, staged2.StagedObjectID, fileMeta["staged_object_id"])
	assert.Equal(t, staged2.SHA256, fileMeta["sha256"])
}

// newStageFixtureReservation aborts nothing: it registers a second OPEN
// reservation is impossible on one branch, so it finalizes the fixture's
// reservation through the fake and opens a fresh one for buildID.
func newStageFixtureReservation(t *testing.T, fx *stageFixture, build uuid.UUID) (uuid.UUID, models.StageDatasetOutputResponse) {
	t.Helper()
	old := fx.store.runtimeTransactions[fx.reservation]
	old.Status = models.TransactionStatusAborted
	fx.store.runtimeTransactions[fx.reservation] = old
	reservation := uuid.New()
	sub := fx.claims.Sub
	fx.store.runtimeTransactions[reservation] = models.RuntimeTransaction{
		ID: reservation, DatasetID: fx.datasetID, BranchID: old.BranchID, BranchName: "main",
		TxType: models.TransactionTypeUpdate, Status: models.TransactionStatusOpen,
		StartedBy:  &sub,
		Providence: models.JSONValue([]byte(fmt.Sprintf(`{"purpose":"wp33-staged-output","build_id":%q}`, build))),
	}
	fx.reservation = reservation
	fx.build = build
	reply := fx.stage(t, `{"m":1}`+"\n", nil)
	require.Equal(t, http.StatusCreated, reply.code, reply.raw)
	return reservation, reply.resp
}

func lastStagedTxnID(fx *stageFixture) uuid.UUID {
	var newest uuid.UUID
	var newestAt time.Time
	for id, txn := range fx.store.runtimeTransactions {
		if txn.DatasetID == fx.datasetID && txn.TxType == models.TransactionTypeSnapshot && txn.StartedAt.After(newestAt) {
			newest, newestAt = id, txn.StartedAt
		}
	}
	return newest
}

// Final-review round 2: the overlap verdict comes BEFORE creation (a
// rejected configuration leaves nothing behind, even for a deep
// not-yet-existing path), a backing root of "/" rejects everything,
// symlinked ancestors are seen through, and no absolute path is echoed.
func TestStagedOutputRootBoundaryOrderAndRedaction(t *testing.T) {
	fx := newStageFixture(t)
	countDirs := func(root string) int {
		n := 0
		filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
			if err == nil && d.IsDir() {
				n++
			}
			return nil
		})
		return n
	}
	before := countDirs(fx.backingRoot)

	// Deep non-existent staging path inside the backing root: rejected
	// with nothing created anywhere under the backing root.
	inside := fx.h
	inside.StagedOutputRoot = filepath.Join(fx.backingRoot, "inner", "deep", "stage")
	rec := do(inside.StageDatasetOutput, http.MethodPost, fx.stageURL(nil), `{"a":1}`+"\n", map[string]string{"rid": fx.rid}, fx.claims)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "overlaps")
	assert.NotContains(t, rec.Body.String(), fx.backingRoot, "no absolute path may be echoed")
	assert.NotContains(t, rec.Body.String(), "/private/tmp", "no absolute path may be echoed")
	assert.Equal(t, before, countDirs(fx.backingRoot), "rejection must not create anything under the backing root")

	// A symlinked staging path pointing inside the backing root is seen
	// through and rejected, again without creating anything.
	link := filepath.Join(t.TempDir(), "stage-link")
	require.NoError(t, os.Symlink(filepath.Join(fx.backingRoot, "via-link"), link))
	inside.StagedOutputRoot = link
	rec = do(inside.StageDatasetOutput, http.MethodPost, fx.stageURL(nil), `{"a":1}`+"\n", map[string]string{"rid": fx.rid}, fx.claims)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Equal(t, before, countDirs(fx.backingRoot))
	_ = os.Remove(link)

	// A backing root of "/" rejects every staging root.
	slashRoot := fx.h
	slashFS := *fx.h.BackingFS.(*storageabstraction.LocalBackingFS)
	slashFS.RootDir = "/"
	slashRoot.BackingFS = &slashFS
	rec = do(slashRoot.StageDatasetOutput, http.MethodPost, fx.stageURL(nil), `{"a":1}`+"\n", map[string]string{"rid": fx.rid}, fx.claims)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "overlaps")
}
