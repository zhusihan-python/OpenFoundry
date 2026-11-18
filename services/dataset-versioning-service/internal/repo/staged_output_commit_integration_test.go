//go:build integration

// WP3.3 increment 3A real-stack acceptance: the streaming outputs:stage
// endpoint and the STAGED_ADD keyed outputs:commit driven through the
// real chi router (auth middleware + test JWTs) against a real PostgreSQL
// database, a real local backing filesystem root and a sibling private
// staging root. Reservations are created through the real transactions
// route with the wp33-staged-output providence marker; publications go
// through the real outputs:commit; verification reads go through the real
// rows:scan endpoint and direct SQL on the authoritative tables.
//
// Honest scope note: the "restart" case rebuilds the handler and router
// over the same pool and directories — it is not a full process restart.
package repo_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	observability "github.com/openfoundry/openfoundry-go/libs/observability"
	storageabstraction "github.com/openfoundry/openfoundry-go/libs/storage-abstraction"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/config"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/handlers"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/models"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/repo"
	"github.com/openfoundry/openfoundry-go/services/dataset-versioning-service/internal/server"
)

type stagedStack struct {
	srv             *http.Server
	pool            *pgxpool.Pool
	backing         *storageabstraction.LocalBackingFS
	stageRoot       string
	jwt             *authmw.JWTConfig
	token           string // owner, dataset.write
	tokenNoWrite    string
	tokenPeerOrg    string
	tokenOtherActor string // same org, dataset.write, different sub
	claims          *authmw.Claims
	dataset         models.Dataset
	rid             string
}

func newStagedStack(t *testing.T, mutate func(h *handlers.Handlers)) *stagedStack {
	t.Helper()
	ctx := context.Background()
	pool := faultPool(ctx, t)
	require.NoError(t, repo.Migrate(ctx, pool))

	r := &repo.Repo{Pool: pool}
	backing := &storageabstraction.LocalBackingFS{BaseURL: "http://dataset-versioning.local", BaseDir: "store", RootDir: t.TempDir()}
	stageRoot := t.TempDir() // sibling private root on the same filesystem, outside the backing root
	h := &handlers.Handlers{Repo: r, BackingFS: backing, StagedOutputRoot: stageRoot}
	if mutate != nil {
		mutate(h)
	}

	jwt := authmw.NewJWTConfig("wp33-staged-router-secret")
	cfg := &config.Config{}
	cfg.Service.Name = "dataset-versioning-service"
	cfg.Service.Version = "test"
	cfg.Server.Host = "127.0.0.1"
	cfg.Server.Port = 0
	srv := server.New(cfg, jwt, h, observability.NewMetrics())

	owner := uuid.New()
	org := uuid.New()
	s := &stagedStack{
		srv: srv, pool: pool, backing: backing, stageRoot: stageRoot, jwt: jwt,
		token:           scanToken(t, jwt, owner, &org),
		tokenNoWrite:    stagedToken(t, jwt, uuid.New(), &org, nil),
		tokenPeerOrg:    scanToken(t, jwt, uuid.New(), func() *uuid.UUID { o := uuid.New(); return &o }()),
		tokenOtherActor: scanToken(t, jwt, uuid.New(), &org),
	}
	dataset, err := r.CreateDatasetForTenant(ctx, &models.CreateDatasetRequest{Name: "wp33-staged"}, owner, &org)
	require.NoError(t, err)
	require.NoError(t, r.EnsureDefaultBranch(ctx, dataset))
	s.dataset = *dataset
	s.rid = "ri.foundry.main.dataset." + dataset.ID.String()
	s.claims = &authmw.Claims{Sub: owner, OrgID: &org, Permissions: []string{"dataset.write"}}
	return s
}

func stagedToken(t *testing.T, jwt *authmw.JWTConfig, sub uuid.UUID, org *uuid.UUID, permissions []string) string {
	t.Helper()
	now := time.Now()
	claims := &authmw.Claims{
		Sub: sub, IAT: now.Unix(), EXP: now.Add(time.Hour).Unix(), JTI: uuid.New(),
		Email: "wp33@example.com", Name: "WP33 Staged",
		Permissions: permissions, OrgID: org,
	}
	tok, err := authmw.EncodeToken(jwt, claims)
	require.NoError(t, err)
	return tok
}

func (s *stagedStack) do(t *testing.T, method, target, contentType string, body io.Reader, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(rec, req)
	return rec
}

// reserve opens a real UPDATE reservation carrying the wp33 marker.
func (s *stagedStack) reserve(t *testing.T, build uuid.UUID, txType string, purpose string) uuid.UUID {
	t.Helper()
	body := fmt.Sprintf(`{"type":%q,"providence":{"origin":"pipeline-build-service","purpose":%q,"build_id":%q}}`, txType, purpose, build)
	rec := s.do(t, http.MethodPost, "/v1/datasets/"+s.rid+"/branches/main/transactions", "application/json", strings.NewReader(body), s.token)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var txn models.RuntimeTransaction
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &txn))
	return txn.ID
}

func (s *stagedStack) stageURL(reservation, build uuid.UUID, key string) string {
	q := url.Values{}
	q.Set("branch", "main")
	q.Set("reservation_id", reservation.String())
	q.Set("build_id", build.String())
	q.Set("logical_path", "part-00000.ndjson")
	q.Set("idempotency_key", key)
	return "/v1/datasets/" + s.rid + "/outputs:stage?" + q.Encode()
}

func (s *stagedStack) stage(t *testing.T, reservation, build uuid.UUID, key, body string) (*httptest.ResponseRecorder, models.StageDatasetOutputResponse) {
	t.Helper()
	rec := s.do(t, http.MethodPost, s.stageURL(reservation, build, key), "application/x-ndjson", strings.NewReader(body), s.token)
	if rec.Code == http.StatusCreated || rec.Code == http.StatusOK {
		var resp models.StageDatasetOutputResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
		return rec, resp
	}
	return rec, models.StageDatasetOutputResponse{}
}

func stagedCommitJSON(key string, stagedID string, size int64, sha string, reservation, build uuid.UUID, mutate func(map[string]any)) string {
	m := map[string]any{
		"idempotency_key":  key,
		"branch":           "main",
		"transaction_type": "SNAPSHOT",
		"reservation_id":   reservation.String(),
		"build_id":         build.String(),
		"files": []map[string]any{{
			"logical_path":     "part-00000.ndjson",
			"operation":        "STAGED_ADD",
			"staged_object_id": stagedID,
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

func (s *stagedStack) commit(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	return s.do(t, http.MethodPost, "/v1/datasets/"+s.rid+"/outputs:commit", "application/json", strings.NewReader(body), s.token)
}

func (s *stagedStack) currentVersion(t *testing.T) int32 {
	t.Helper()
	var v int32
	require.NoError(t, s.pool.QueryRow(context.Background(), `SELECT current_version FROM datasets WHERE id = $1`, s.dataset.ID).Scan(&v))
	return v
}

func (s *stagedStack) stagedRow(t *testing.T, stagedID uuid.UUID) (status string, consumedBy *uuid.UUID) {
	t.Helper()
	require.NoError(t, s.pool.QueryRow(context.Background(),
		`SELECT status, consumed_by_transaction FROM dataset_staged_output_objects WHERE id = $1`, stagedID).Scan(&status, &consumedBy))
	return
}

func (s *stagedStack) txnStatus(t *testing.T, txnID uuid.UUID) string {
	t.Helper()
	var status string
	require.NoError(t, s.pool.QueryRow(context.Background(), `SELECT status FROM dataset_transactions WHERE id = $1`, txnID).Scan(&status))
	return status
}

// scanAll walks the all_files rows:scan of one version to its end and
// returns the raw row JSON.
func (s *stagedStack) scanAll(t *testing.T, version int32) []json.RawMessage {
	t.Helper()
	var rows []json.RawMessage
	cursor := ""
	for {
		q := url.Values{}
		q.Set("branch", "main")
		q.Set("version", fmt.Sprint(version))
		q.Set("all_files", "true")
		q.Set("limit", "3")
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		rec := s.do(t, http.MethodGet, "/v1/datasets/"+s.rid+"/files/rows:scan?"+q.Encode(), "", nil, s.token)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var page allFilesHTTPResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
		rows = append(rows, page.Rows...)
		if page.NextCursor == nil || *page.NextCursor == "" {
			return rows
		}
		cursor = *page.NextCursor
	}
}

func assertRowsEqualValueByValue(t *testing.T, want []string, got []json.RawMessage) {
	t.Helper()
	require.Len(t, got, len(want), "row count mismatch")
	for i, w := range want {
		assert.JSONEq(t, w, string(got[i]), "row %d must match value-by-value", i)
	}
}

// A1 + A11 regression: a staged commit publishes one sealed object,
// value-comparable through the real scan endpoint, while the inline keyed
// path keeps working unchanged alongside it.
func TestStagedOutputEndToEndValueByValue(t *testing.T) {
	s := newStagedStack(t, nil)
	ctx := context.Background()

	// v1: the ordinary inline keyed path still publishes (regression).
	seed := `{"seed":true}` + "\n"
	inline := fmt.Sprintf(`{"idempotency_key":"of-v1","branch":"main","files":[{"logical_path":"seed.ndjson","content_base64":%q,"size_bytes":%d}]}`,
		base64.StdEncoding.EncodeToString([]byte(seed)), len(seed))
	rec := s.commit(t, inline)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	v1 := s.currentVersion(t)

	// Reserve, stage, publish v2 through the staged path.
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-a1:" + reservation.String()
	rows := []string{
		`{"id":1,"name":"alpha","tags":["a","b"]}`,
		`{"id":2,"name":"bravo","note":"日本語テキスト","ok":true}`,
		`{"id":3,"name":"charlie","ratio":1.5,"missing":null}`,
		`{"id":4,"name":"","big":9007199254740993}`,
		`{"id":5,"name":"echo","nested":{"k":[1,{"z":null}]}}`,
	}
	body := strings.Join(rows, "\n") + "\n"
	stageRec, staged := s.stage(t, reservation, build, key, body)
	require.Equal(t, http.StatusCreated, stageRec.Code, stageRec.Body.String())
	wantDigest := sha256.Sum256([]byte(body))
	assert.Equal(t, hex.EncodeToString(wantDigest[:]), staged.SHA256)
	assert.Equal(t, int64(len(body)), staged.SizeBytes)
	assert.Equal(t, int64(len(rows)), staged.RecordCount)

	commitBody := stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, nil)
	rec = s.commit(t, commitBody)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var published models.CommitDatasetOutputResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &published))
	require.Equal(t, models.TransactionStatusCommitted, published.Transaction.Status)

	v2 := s.currentVersion(t)
	assert.Equal(t, v1+1, v2, "exactly one new version")

	// Value-by-value through the real scan endpoint.
	assertRowsEqualValueByValue(t, rows, s.scanAll(t, v2))

	// Authoritative state: reservation finalized, descriptor consumed by
	// the publishing transaction, one receipt, one ndjson index entry.
	assert.Equal(t, "ABORTED", s.txnStatus(t, reservation))
	status, consumedBy := s.stagedRow(t, uuid.MustParse(staged.StagedObjectID))
	assert.Equal(t, "CONSUMED", status)
	require.NotNil(t, consumedBy)
	assert.Equal(t, published.Transaction.ID, *consumedBy)
	var receipts, indexRows int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_output_commit_receipts WHERE dataset_id=$1 AND idempotency_key=$2`, s.dataset.ID, key).Scan(&receipts))
	assert.Equal(t, 1, receipts)
	// The durable receipt fingerprint is the service's SHA256 over the
	// exact raw request bytes — verifiable against the bytes we sent.
	var durableFingerprint string
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT request_fingerprint FROM dataset_output_commit_receipts WHERE dataset_id=$1 AND idempotency_key=$2`, s.dataset.ID, key).Scan(&durableFingerprint))
	requestDigest := sha256.Sum256([]byte(commitBody))
	assert.Equal(t, hex.EncodeToString(requestDigest[:]), durableFingerprint)
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_file_index WHERE dataset_id=$1 AND path='part-00000.ndjson'`, s.dataset.ID).Scan(&indexRows))
	assert.Equal(t, 1, indexRows)
	var physical string
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT physical_uri FROM dataset_files WHERE dataset_id=$1 AND logical_path='part-00000.ndjson'`, s.dataset.ID).Scan(&physical))
	assert.True(t, strings.HasPrefix(physical, "local:///store/datasets/"+s.dataset.ID.String()+"/transactions/"), "physical_uri must be the server-named transaction object: %s", physical)

	// The installed bytes equal the sealed bytes exactly.
	installed, err := s.backing.ReadLocalObject(strings.TrimPrefix(physical, "local:///"))
	require.NoError(t, err)
	assert.Equal(t, body, string(installed))

	// v1 (inline) stays readable: the old version's rows scan unchanged.
	assertRowsEqualValueByValue(t, []string{`{"seed":true}`}, s.scanAll(t, v1))
}

// A2: an empty NDJSON body publishes a real zero-row object.
func TestStagedOutputEmptyObjectPublishes(t *testing.T) {
	s := newStagedStack(t, nil)
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-a2:" + reservation.String()
	stageRec, staged := s.stage(t, reservation, build, key, "")
	require.Equal(t, http.StatusCreated, stageRec.Code, stageRec.Body.String())
	assert.Equal(t, int64(0), staged.SizeBytes)
	assert.Equal(t, int64(0), staged.RecordCount)

	rec := s.commit(t, stagedCommitJSON(key, staged.StagedObjectID, 0, staged.SHA256, reservation, build, nil))
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	version := s.currentVersion(t)
	assert.Empty(t, s.scanAll(t, version), "zero-row object scans to zero rows")
	var size int64
	require.NoError(t, s.pool.QueryRow(context.Background(), `SELECT size_bytes FROM dataset_file_index WHERE dataset_id=$1 AND path='part-00000.ndjson'`, s.dataset.ID).Scan(&size))
	assert.Equal(t, int64(0), size, "the zero-row file must be registered, not dropped")
}

// A3: mid-stream failures reject explicitly, leave no descriptor and no
// spool residue.
func TestStagedOutputStreamFailures(t *testing.T) {
	s := newStagedStack(t, nil)
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-a3:" + reservation.String()

	// Bad record.
	rec, _ := s.stage(t, reservation, build, key, `{"a":1}`+"\nnope\n")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "STAGED_RECORD_INVALID")

	// Over-cap record (1MiB newline-included bound, scan-identical).
	big := make([]byte, 0, 1024*1024+1)
	big = append(big, `{"p":"`...)
	for len(big) < 1024*1024-2 {
		big = append(big, '1')
	}
	big = append(big, `"}`...)
	big = append(big, '\n')
	require.Equal(t, 1024*1024+1, len(big))
	rec, _ = s.stage(t, reservation, build, key, string(big))
	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	assert.Contains(t, rec.Body.String(), "STAGED_RECORD_TOO_LARGE")

	// Truncated transport: the body errors mid-record.
	rec = s.do(t, http.MethodPost, s.stageURL(reservation, build, key), "application/x-ndjson", io.MultiReader(strings.NewReader(`{"a":1}`+"\n"), &errReader{}), s.token)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid staged body")

	var descriptors int
	require.NoError(t, s.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM dataset_staged_output_objects WHERE reservation_id=$1`, reservation).Scan(&descriptors))
	assert.Equal(t, 0, descriptors, "failed attempts must leave no descriptor")
	entries, err := os.ReadDir(filepath.Join(s.stageRoot, "datasets", s.dataset.ID.String(), "reservations", reservation.String()))
	require.NoError(t, err)
	assert.Empty(t, entries, "failed attempts must clean their spool files")
	// The reservation stays OPEN and reusable.
	assert.Equal(t, "OPEN", s.txnStatus(t, reservation))
}

type errReader struct{}

func (e *errReader) Read([]byte) (int, error) { return 0, os.ErrDeadlineExceeded }

// A14/A15/A16: re-transmission reuse/conflict, in-flight exclusion and
// expired-lease takeover with bounded residue.
func TestStagedOutputRetransmitInFlightTakeover(t *testing.T) {
	s := newStagedStack(t, nil)
	ctx := context.Background()
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-a4:" + reservation.String()
	body := `{"n":1}` + "\n" + `{"n":2}` + "\n"

	_, first := s.stage(t, reservation, build, key, body)
	sealedPath := filepath.Join(s.stageRoot, "datasets", s.dataset.ID.String(), "reservations", reservation.String(), first.StagedObjectID+".ndjson")
	sealedBefore, err := os.ReadFile(sealedPath)
	require.NoError(t, err)

	// Byte-identical re-transmission reuses the descriptor.
	rec, again := s.stage(t, reservation, build, key, body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, first.StagedObjectID, again.StagedObjectID)
	var count int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_staged_output_objects WHERE reservation_id=$1`, reservation).Scan(&count))
	assert.Equal(t, 1, count, "reuse must not add rows")

	// Different content conflicts; the sealed file is untouched.
	rec, _ = s.stage(t, reservation, build, key, `{"n":3}`+"\n")
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "STAGED_OBJECT_CONFLICT")
	after, err := os.ReadFile(sealedPath)
	require.NoError(t, err)
	assert.Equal(t, sealedBefore, after)

	// A live foreign lease excludes concurrent uploads.
	_, err = s.pool.Exec(ctx, `UPDATE dataset_staged_output_objects SET status='STAGING', lease_expires_at = NOW() + interval '10 minutes' WHERE id=$1`, first.StagedObjectID)
	require.NoError(t, err)
	rec, _ = s.stage(t, reservation, build, key, body)
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "STAGED_UPLOAD_IN_FLIGHT")

	// Expired lease takeover on a fresh reservation: model a crashed
	// attempt the way the real code leaves it — a STAGING row under the
	// dead attempt's id with its own .tmp/.ndjson spool residue. The
	// takeover must hand the slot over and clear exactly those files.
	// (The first reservation is finalized through the real :abort route —
	// the branch admits one OPEN transaction at a time.)
	rec = s.do(t, http.MethodPost, "/v1/datasets/"+s.rid+"/branches/main/transactions/"+reservation.String()+":abort", "", nil, s.token)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	dead := uuid.New()
	reservation2 := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key2 := "of-wp33-a4b:" + reservation2.String()
	deadDir := filepath.Join(s.stageRoot, "datasets", s.dataset.ID.String(), "reservations", reservation2.String())
	require.NoError(t, os.MkdirAll(deadDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(deadDir, dead.String()+".tmp"), []byte("stale tmp"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(deadDir, dead.String()+".ndjson"), []byte("stale sealed"), 0o600))
	_, err = s.pool.Exec(ctx, `INSERT INTO dataset_staged_output_objects
		(id, dataset_id, branch_name, reservation_id, build_id, actor_id, idempotency_key, logical_path, status, lease_expires_at)
		VALUES ($1,$2,'main',$3,$4,$5,$6,'part-00000.ndjson','STAGING', NOW() - interval '1 minute')`,
		dead, s.dataset.ID, reservation2, build, s.dataset.OwnerID, key2)
	require.NoError(t, err)

	var fresh models.StageDatasetOutputResponse
	rec, fresh = s.stage(t, reservation2, build, key2, body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	_, err = os.Stat(filepath.Join(deadDir, dead.String()+".tmp"))
	assert.True(t, os.IsNotExist(err), "takeover must remove the dead attempt's temp spool")
	_, err = os.Stat(filepath.Join(deadDir, dead.String()+".ndjson"))
	assert.True(t, os.IsNotExist(err), "takeover must remove the dead attempt's sealed residue")
	entries, err := os.ReadDir(deadDir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "the reservation directory holds exactly the live artifact")
	_, err = os.Stat(filepath.Join(deadDir, fresh.StagedObjectID+".ndjson"))
	require.NoError(t, err)
	// The first reservation's live sealed artifact is untouched.
	after2, err := os.ReadFile(sealedPath)
	require.NoError(t, err)
	assert.Equal(t, sealedBefore, after2)
}

// A4: binding negatives on the commit side, each with zero side effects.
func TestStagedCommitBindingNegatives(t *testing.T) {
	s := newStagedStack(t, nil)
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-a5:" + reservation.String()
	_, staged := s.stage(t, reservation, build, key, `{"a":1}`+"\n")
	version := s.currentVersion(t)

	cases := []struct {
		name   string
		body   string
		token  string
		status int
		frag   string
	}{
		{"wrong key", stagedCommitJSON("other-key", staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, nil), s.token, 400, "binding"},
		{"wrong build", stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, uuid.New(), nil), s.token, 400, "reservation"},
		{"declared digest mismatch", stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes+1, staged.SHA256, reservation, build, nil), s.token, 400, "size or sha256"},
		{"other actor same tenant", stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, nil), s.tokenOtherActor, 400, "reservation"},
		{"other tenant", stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, nil), s.tokenPeerOrg, 404, ""},
		{"digest-only mismatch", stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, strings.Repeat("f", 64), reservation, build, nil), s.token, 400, "size or sha256"},
	}
	for _, tc := range cases {
		rec := s.do(t, http.MethodPost, "/v1/datasets/"+s.rid+"/outputs:commit", "application/json", strings.NewReader(tc.body), tc.token)
		assert.Equal(t, tc.status, rec.Code, tc.name+": "+rec.Body.String())
		if tc.frag != "" {
			assert.Contains(t, rec.Body.String(), tc.frag, tc.name)
		}
	}
	// A still-STAGING object is not committable.
	_, err := s.pool.Exec(context.Background(), `UPDATE dataset_staged_output_objects SET status='STAGING', lease_expires_at=NOW()+interval '10 minutes' WHERE id=$1`, staged.StagedObjectID)
	require.NoError(t, err)
	rec := s.commit(t, stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, nil))
	assert.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	_, err = s.pool.Exec(context.Background(), `UPDATE dataset_staged_output_objects SET status='SEALED' WHERE id=$1`, staged.StagedObjectID)
	require.NoError(t, err)

	// Expired sealed object.
	_, err = s.pool.Exec(context.Background(), `UPDATE dataset_staged_output_objects SET expires_at = NOW() - interval '1 second' WHERE id=$1`, staged.StagedObjectID)
	require.NoError(t, err)
	rec = s.commit(t, stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, nil))
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "expired")
	_, err = s.pool.Exec(context.Background(), `UPDATE dataset_staged_output_objects SET expires_at = NOW() + interval '1 hour' WHERE id=$1`, staged.StagedObjectID)
	require.NoError(t, err)

	// Zero side effects across the board.
	assert.Equal(t, version, s.currentVersion(t))
	assert.Equal(t, "OPEN", s.txnStatus(t, reservation))
	status, _ := s.stagedRow(t, uuid.MustParse(staged.StagedObjectID))
	assert.Equal(t, "SEALED", status)

	// A legacy (marker-less) reservation is never claimable. The branch
	// admits one OPEN transaction: finalize the staged one first through
	// the real :abort route.
	abortRec := s.do(t, http.MethodPost, "/v1/datasets/"+s.rid+"/branches/main/transactions/"+reservation.String()+":abort", "", nil, s.token)
	require.Equal(t, http.StatusOK, abortRec.Code, abortRec.Body.String())
	legacy := s.reserve(t, uuid.New(), "UPDATE", "")
	rec = s.commit(t, stagedCommitJSON("k2", staged.StagedObjectID, staged.SizeBytes, staged.SHA256, legacy, build, nil))
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}

// A5: an injected publish failure after the install keeps the old version
// authoritative, rolls the reservation back to OPEN and the descriptor to
// SEALED, and the same key+body retry then publishes exactly once.
func TestStagedCommitPublishFaultKeepsOldVersion(t *testing.T) {
	s := newStagedStack(t, nil)
	ctx := context.Background()
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-a6:" + reservation.String()
	_, staged := s.stage(t, reservation, build, key, `{"v":2}`+"\n")
	commitBody := stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, nil)

	rec := s.commit(t, commitBody)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	v2 := s.currentVersion(t)

	// Second staged publish on a fresh reservation; the injected
	// constraint breaks the file-index rewrite inside the publish
	// transaction — after the sealed file was installed.
	build2 := uuid.New()
	reservation2 := s.reserve(t, build2, "UPDATE", "wp33-staged-output")
	key2 := "of-wp33-a6b:" + reservation2.String()
	_, staged2 := s.stage(t, reservation2, build2, key2, `{"v":3}`+"\n")
	commit2 := stagedCommitJSON(key2, staged2.StagedObjectID, staged2.SizeBytes, staged2.SHA256, reservation2, build2, nil)

	_, err := s.pool.Exec(ctx, `ALTER TABLE dataset_file_index ADD CONSTRAINT fault_entry_type CHECK (entry_type = 'directory') NOT VALID`)
	require.NoError(t, err)
	rec = s.commit(t, commit2)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, "injected publish failure must 5xx, got %s", rec.Body.String())
	_, err = s.pool.Exec(ctx, `ALTER TABLE dataset_file_index DROP CONSTRAINT fault_entry_type`)
	require.NoError(t, err)
	// The failed attempt leaves no receipt and no version row at all.
	var failedReceipts, versionsAfterFault int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_output_commit_receipts WHERE dataset_id=$1 AND idempotency_key=$2`, s.dataset.ID, key2).Scan(&failedReceipts))
	assert.Equal(t, 0, failedReceipts, "a failed commit must leave no receipt")
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_versions WHERE dataset_id=$1`, s.dataset.ID).Scan(&versionsAfterFault))
	assert.Equal(t, int(v2), versionsAfterFault, "a failed commit must cut no version row")

	// Old version stays authoritative and byte-readable.
	assert.Equal(t, v2, s.currentVersion(t))
	assertRowsEqualValueByValue(t, []string{`{"v":2}`}, s.scanAll(t, v2))
	assert.Equal(t, "OPEN", s.txnStatus(t, reservation2), "the reservation must roll back to OPEN")
	status, _ := s.stagedRow(t, uuid.MustParse(staged2.StagedObjectID))
	assert.Equal(t, "SEALED", status, "the descriptor must roll back to SEALED")

	// The failed attempt happened BEFORE the outer COMMIT: its freshly
	// installed target is uniquely ours and has been removed again by the
	// build's cleanup — the backing namespace holds only the successful
	// v2 object (an orphan could survive only if the removal itself
	// failed, which is logged and listed as bounded residue).
	installedFiles, err := filepath.Glob(filepath.Join(s.backing.RootDir, "store", "datasets", s.dataset.ID.String(), "transactions", "*", "part-00000.ndjson"))
	require.NoError(t, err)
	assert.Len(t, installedFiles, 1, "the failed attempt must clean its own install target")

	var versionsBeforeRetry int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_versions WHERE dataset_id=$1`, s.dataset.ID).Scan(&versionsBeforeRetry))
	rec = s.commit(t, commit2)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	assert.Equal(t, v2+1, s.currentVersion(t), "retry publishes exactly one further version")
	var versionsAfterRetry int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_versions WHERE dataset_id=$1`, s.dataset.ID).Scan(&versionsAfterRetry))
	assert.Equal(t, versionsBeforeRetry+1, versionsAfterRetry, "the retry makes exactly one version visible")
	assertRowsEqualValueByValue(t, []string{`{"v":3}`}, s.scanAll(t, v2+1))
	assert.Equal(t, "ABORTED", s.txnStatus(t, reservation2))
}

// A6/A7/A8: an explicit REPLAY after a seen 201 is byte-stable and adds no
// second version; a different body under the same key is a typed 409; a
// rebuilt handler (same pool and directories) still serves receipts and
// can commit a stage sealed before the rebuild. Honest scope: this does
// NOT simulate a lost HTTP ACK or a lost PG COMMIT acknowledgement — the
// first commit is fully acknowledged here; 3C exercises real
// process-restart and dropped-ACK paths.
func TestStagedCommitReplayAndHandlerRebuild(t *testing.T) {
	s := newStagedStack(t, nil)
	ctx := context.Background()
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-a7:" + reservation.String()
	_, staged := s.stage(t, reservation, build, key, `{"r":1}`+"\n")
	commitBody := stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, nil)

	first := s.commit(t, commitBody)
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	version := s.currentVersion(t)

	replay := s.commit(t, commitBody)
	require.Equal(t, http.StatusOK, replay.Code)
	assert.Equal(t, first.Body.String(), replay.Body.String(), "replay serves the stored bytes verbatim")
	assert.Equal(t, version, s.currentVersion(t), "replay adds no version")
	var committed int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_transactions WHERE dataset_id=$1 AND status='COMMITTED' AND tx_type='SNAPSHOT'`, s.dataset.ID).Scan(&committed))
	assert.Equal(t, 1, committed, "replay creates no second transaction")

	conflict := s.commit(t, stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, func(m map[string]any) { m["summary"] = "different bytes" }))
	assert.Equal(t, http.StatusConflict, conflict.Code)
	assert.Contains(t, conflict.Body.String(), "IDEMPOTENCY_KEY_CONFLICT")

	// Rebuild handler + router over the same pool, backing and staging
	// roots; honest scope: not a full process restart.
	build2 := uuid.New()
	reservation2 := s.reserve2(t, build2)
	key2 := "of-wp33-a7b:" + reservation2.String()
	_, staged2 := s.stage(t, reservation2, build2, key2, `{"r":2}`+"\n")
	fresh := newStagedStackOver(t, s)
	get := fresh.do(t, http.MethodGet, "/v1/datasets/"+s.rid+"/outputs/commits/"+key, "", nil, fresh.token)
	require.Equal(t, http.StatusOK, get.Code, get.Body.String())
	assert.Equal(t, first.Body.String(), get.Body.String(), "receipt survives the rebuild")

	commit2 := stagedCommitJSON(key2, staged2.StagedObjectID, staged2.SizeBytes, staged2.SHA256, reservation2, build2, nil)
	rec := fresh.commit(t, commit2)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	assert.Equal(t, version+1, fresh.currentVersion(t))
	assertRowsEqualValueByValue(t, []string{`{"r":2}`}, fresh.scanAll(t, version+1))
}

func (s *stagedStack) reserve2(t *testing.T, build uuid.UUID) uuid.UUID {
	t.Helper()
	return s.reserve(t, build, "UPDATE", "wp33-staged-output")
}

func newStagedStackOver(t *testing.T, prev *stagedStack) *stagedStack {
	t.Helper()
	r := &repo.Repo{Pool: prev.pool}
	h := &handlers.Handlers{Repo: r, BackingFS: prev.backing, StagedOutputRoot: prev.stageRoot}
	jwt := authmw.NewJWTConfig("wp33-staged-router-secret")
	cfg := &config.Config{}
	cfg.Service.Name = "dataset-versioning-service"
	cfg.Service.Version = "test"
	cfg.Server.Host = "127.0.0.1"
	cfg.Server.Port = 0
	srv := server.New(cfg, jwt, h, observability.NewMetrics())
	owner := uuid.New()
	org := uuid.New()
	fresh := &stagedStack{srv: srv, pool: prev.pool, backing: prev.backing, stageRoot: prev.stageRoot, jwt: jwt,
		token: stagedToken(t, jwt, owner, &org, []string{"dataset.write"})}
	// Same dataset as before — resolve by RID; tokens minted with the
	// stack's own secret (same secret string) are accepted.
	fresh.dataset = prev.dataset
	fresh.rid = prev.rid
	// The dataset's tenant is the ORIGINAL owner/org; re-mint with those.
	var orgID *uuid.UUID
	require.NoError(t, prev.pool.QueryRow(context.Background(), `SELECT org_id FROM datasets WHERE id=$1`, prev.dataset.ID).Scan(&orgID))
	var ownerID uuid.UUID
	require.NoError(t, prev.pool.QueryRow(context.Background(), `SELECT owner_id FROM datasets WHERE id=$1`, prev.dataset.ID).Scan(&ownerID))
	fresh.token = scanToken(t, jwt, ownerID, orgID)
	return fresh
}

// A9: the shipped database CHECK keeps STAGED_ADD out of staged-file rows
// — the fail-closed mechanism for older services that predate the
// staged-commit branch.
func TestStagedAddOperationRejectedByShippedCheck(t *testing.T) {
	s := newStagedStack(t, nil)
	ctx := context.Background()
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	_, err := s.pool.Exec(ctx, `INSERT INTO dataset_transaction_files (transaction_id, logical_path, physical_path, size_bytes, op)
		VALUES ($1, 'x.ndjson', 'outputs/x', 1, 'STAGED_ADD')`, reservation)
	require.Error(t, err, "the shipped CHECK must reject STAGED_ADD outright")
	assert.Contains(t, err.Error(), "check constraint", "unexpected error: %v", err)
}

// A10/A12/A17: the private namespace is unreachable through public routes,
// overlapping staging roots fail closed, and an unconfigured root disables
// only the staged feature.
func TestStagedNamespaceIsolationAndConfigBoundaries(t *testing.T) {
	s := newStagedStack(t, nil)
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-a8:" + reservation.String()
	_, staged := s.stage(t, reservation, build, key, `{"x":1}`+"\n")

	// The presign proxy resolves keys against RootDir and cannot reach the
	// private staging root (lexical containment rejects the escape).
	sealedRel := "datasets/" + s.dataset.ID.String() + "/reservations/" + reservation.String() + "/" + staged.StagedObjectID + ".ndjson"
	escape := strings.Repeat("../", 6) + filepath.Base(s.stageRoot) + "/" + sealedRel
	rec := s.do(t, http.MethodGet, "/v1/_internal/local-fs/"+escape+"?expires=9999999999&sig=deadbeef", "", nil, "")
	assert.Equal(t, http.StatusBadRequest, rec.Code, "proxy must reject keys escaping its root, got %s", rec.Body.String())

	// An overlapping staging root (inside the backing root) fails closed.
	overlap := newStagedStack(t, func(h *handlers.Handlers) {
		h.StagedOutputRoot = filepath.Join(h.BackingFS.(*storageabstraction.LocalBackingFS).RootDir, "stage")
	})
	rec = overlap.do(t, http.MethodPost, overlap.stageURL(reservation, build, key), "application/x-ndjson", strings.NewReader(`{"a":1}`+"\n"), overlap.token)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "overlaps")

	// Without a staging root the staged feature is disabled (503) while
	// other routes stay up.
	noRoot := newStagedStack(t, func(h *handlers.Handlers) { h.StagedOutputRoot = "" })
	rec = noRoot.do(t, http.MethodPost, noRoot.stageURL(reservation, build, key), "application/x-ndjson", strings.NewReader(`{"a":1}`+"\n"), noRoot.token)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	// The commit targets the ORIGINAL dataset (shared pool, same JWT
	// secret): it resolves, then the unconfigured staging root fails the
	// STAGED_ADD commit closed.
	rec = noRoot.do(t, http.MethodPost, "/v1/datasets/"+s.rid+"/outputs:commit", "application/json",
		strings.NewReader(stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, nil)), s.token)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "unconfigured staging must fail the STAGED_ADD commit closed: "+rec.Body.String())
	rec = noRoot.do(t, http.MethodGet, "/healthz", "", nil, "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// A18: a sealed source that is not a regular file (planted symlink) is
// never installed.
func TestStagedCommitRejectsSymlinkedSeal(t *testing.T) {
	s := newStagedStack(t, nil)
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-a9:" + reservation.String()
	_, staged := s.stage(t, reservation, build, key, `{"a":1}`+"\n")
	sealedPath := filepath.Join(s.stageRoot, "datasets", s.dataset.ID.String(), "reservations", reservation.String(), staged.StagedObjectID+".ndjson")
	decoy := filepath.Join(t.TempDir(), "decoy.ndjson")
	require.NoError(t, os.WriteFile(decoy, []byte(`{"evil":true}`), 0o600))
	require.NoError(t, os.Remove(sealedPath))
	require.NoError(t, os.Symlink(decoy, sealedPath))

	rec := s.commit(t, stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "sealed staged object file is missing")
	// Nothing was published and no symlink crossed into the backing root.
	symlinks, err := filepath.Glob(filepath.Join(s.backing.RootDir, "store", "datasets", s.dataset.ID.String(), "transactions", "*", "part-00000.ndjson"))
	require.NoError(t, err)
	for _, p := range symlinks {
		fi, err := os.Lstat(p)
		require.NoError(t, err)
		assert.False(t, fi.Mode()&os.ModeSymlink != 0, "no symlink may be installed into the backing namespace")
	}
}

// Scope/auth order negatives for the stage route: 403 scope before tenant
// resolution, cross-tenant 404 without existence leaks.
func TestStagedOutputScopeAndTenantOrder(t *testing.T) {
	s := newStagedStack(t, nil)
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-a10:" + reservation.String()

	rec := s.do(t, http.MethodPost, s.stageURL(reservation, build, key), "application/x-ndjson", strings.NewReader(`{"a":1}`+"\n"), s.tokenNoWrite)
	assert.Equal(t, http.StatusForbidden, rec.Code, "scope check must precede tenant resolution")

	rec = s.do(t, http.MethodPost, s.stageURL(reservation, build, key), "application/x-ndjson", strings.NewReader(`{"a":1}`+"\n"), s.tokenPeerOrg)
	assert.Equal(t, http.StatusNotFound, rec.Code, "cross-tenant stage must 404 like a missing dataset")

	rec = s.do(t, http.MethodPost, s.stageURL(reservation, build, key), "application/x-ndjson", strings.NewReader(`{"a":1}`+"\n"), "")
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

// Final-review guards: a re-transmission may only reuse the live sealed
// artifact of the exact same binding (key / logical path / actor), only
// within its TTL, and only when the sealed file itself is still present,
// regular and of the sealed size — every check fires before a single body
// byte is streamed.
func TestStagedOutputRetransmitBindingAndSealedFileGuards(t *testing.T) {
	s := newStagedStack(t, nil)
	ctx := context.Background()
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-final-r:" + reservation.String()
	body := `{"g":1}` + "\n"
	_, first := s.stage(t, reservation, build, key, body)
	sealedPath := filepath.Join(s.stageRoot, "datasets", s.dataset.ID.String(), "reservations", reservation.String(), first.StagedObjectID+".ndjson")
	status, _ := s.stagedRow(t, uuid.MustParse(first.StagedObjectID))

	stageWith := func(token, keyOverride, logicalOverride string) *httptest.ResponseRecorder {
		q := url.Values{}
		q.Set("branch", "main")
		q.Set("reservation_id", reservation.String())
		q.Set("build_id", build.String())
		q.Set("logical_path", "part-00000.ndjson")
		if logicalOverride != "" {
			q.Set("logical_path", logicalOverride)
		}
		q.Set("idempotency_key", key)
		if keyOverride != "" {
			q.Set("idempotency_key", keyOverride)
		}
		return s.do(t, http.MethodPost, "/v1/datasets/"+s.rid+"/outputs:stage?"+q.Encode(), "application/x-ndjson", strings.NewReader(body), token)
	}

	// Wrong key, wrong logical path and a different actor are all binding
	// mismatches rejected before the body is read; the artifact survives.
	rec := stageWith(s.token, "of-wp33-final-r:other", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "binding")
	rec = stageWith(s.token, "", "other-file.ndjson")
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "binding")
	rec = stageWith(s.tokenOtherActor, "", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "binding")
	after, err := os.ReadFile(sealedPath)
	require.NoError(t, err)
	assert.Equal(t, body, string(after))

	// An expired SEALED artifact is not re-transmittable.
	_, err = s.pool.Exec(ctx, `UPDATE dataset_staged_output_objects SET expires_at = NOW() - interval '1 second' WHERE id=$1`, first.StagedObjectID)
	require.NoError(t, err)
	rec, _ = s.stage(t, reservation, build, key, body)
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "expired")
	_, err = s.pool.Exec(ctx, `UPDATE dataset_staged_output_objects SET expires_at = NOW() + interval '1 hour' WHERE id=$1`, first.StagedObjectID)
	require.NoError(t, err)

	// Missing, truncated and symlinked sealed files are refused before
	// streaming (server-side inconsistency, 500), and the row stays SEALED.
	require.NoError(t, os.Remove(sealedPath))
	rec, _ = s.stage(t, reservation, build, key, body)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	require.NoError(t, os.WriteFile(sealedPath, []byte(body[:4]), 0o600))
	rec, _ = s.stage(t, reservation, build, key, body)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "bytes on disk")
	require.NoError(t, os.Remove(sealedPath))
	require.NoError(t, os.Symlink(filepath.Join(t.TempDir(), "x"), sealedPath))
	rec, _ = s.stage(t, reservation, build, key, body)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	require.NoError(t, os.Remove(sealedPath))
	require.NoError(t, os.WriteFile(sealedPath, []byte(body), 0o600))
	assert.Equal(t, status, "SEALED")

	// With everything intact the byte-identical re-transmission reuses.
	rec, again := s.stage(t, reservation, build, key, body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, first.StagedObjectID, again.StagedObjectID)
}

// Final-review guard: the install verifies the sealed file's ACTUAL size
// against the sealed size — a truncated artifact aborts the commit before
// anything lands in the database.
func TestStagedCommitInstallSizeGuard(t *testing.T) {
	s := newStagedStack(t, nil)
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-final-i:" + reservation.String()
	_, staged := s.stage(t, reservation, build, key, `{"i":1}`+"\n")
	version := s.currentVersion(t)
	sealedPath := filepath.Join(s.stageRoot, "datasets", s.dataset.ID.String(), "reservations", reservation.String(), staged.StagedObjectID+".ndjson")
	require.NoError(t, os.WriteFile(sealedPath, []byte(`{"i":`), 0o600)) // truncated on disk

	rec := s.commit(t, stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	// Fixed message only — the size detail (and any path) goes to the
	// log, never into the HTTP response.
	assert.Contains(t, rec.Body.String(), "sealed staged object file is missing")
	assert.NotContains(t, rec.Body.String(), s.stageRoot, "no absolute path may be echoed")
	assert.Equal(t, version, s.currentVersion(t), "a size-mismatched artifact must publish nothing")
	assert.Equal(t, "OPEN", s.txnStatus(t, reservation))
	objStatus, _ := s.stagedRow(t, uuid.MustParse(staged.StagedObjectID))
	assert.Equal(t, "SEALED", objStatus)
}

// Final-review guard: the seal moment and the lease deadline are judged by
// the DATABASE wall clock read under the locks — an upload that outlives
// its lease cannot seal even though the lease was live when it started.
func TestStageSealUsesDatabaseClock(t *testing.T) {
	s := newStagedStack(t, func(h *handlers.Handlers) { h.StagedOutputLeaseTTL = time.Nanosecond })
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-final-c:" + reservation.String()
	rec, _ := s.stage(t, reservation, build, key, `{"c":1}`+"\n"+`{"c":2}`+"\n")
	assert.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "lease")
	var descriptors int
	require.NoError(t, s.pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM dataset_staged_output_objects WHERE reservation_id=$1`, reservation).Scan(&descriptors))
	assert.Equal(t, 0, descriptors, "an out-of-lease upload must not seal")
	entries, err := os.ReadDir(filepath.Join(s.stageRoot, "datasets", s.dataset.ID.String(), "reservations", reservation.String()))
	require.NoError(t, err)
	assert.Empty(t, entries, "the failed attempt cleans its spool")
}

// Final-review guard: the active-branch identity is checked against the
// LIVE branch row (soft-deleted rows filtered) under the locks — a branch
// soft-deleted and recreated under the same name can never legitimize an
// old reservation.
func TestStagedSoftDeletedBranchSameNameRejected(t *testing.T) {
	s := newStagedStack(t, nil)
	ctx := context.Background()
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-final-b:" + reservation.String()

	_, err := s.pool.Exec(ctx, `UPDATE dataset_branches SET deleted_at = NOW() WHERE dataset_id=$1 AND name='main'`, s.dataset.ID)
	require.NoError(t, err)
	rec, _ := s.stage(t, reservation, build, key, `{"b":1}`+"\n")
	assert.Equal(t, http.StatusBadRequest, rec.Code, "a soft-deleted active branch must fail the binding, got "+rec.Body.String())

	// Recreate "main" (partial unique index allows it while the old row is
	// soft-deleted): the reservation still points at the dead branch id.
	freshBranch := uuid.New()
	datasetRID := "ri.foundry.main.dataset." + s.dataset.ID.String()
	_, err = s.pool.Exec(ctx, `INSERT INTO dataset_branches
		(id, dataset_id, dataset_rid, name, labels, fallback_chain, is_default)
		VALUES ($1, $2, $3, 'main', '{}'::jsonb, '{}'::text[], false)`,
		freshBranch, s.dataset.ID, datasetRID)
	require.NoError(t, err)
	rec, _ = s.stage(t, reservation, build, key, `{"b":1}`+"\n")
	assert.Equal(t, http.StatusBadRequest, rec.Code, "a same-name recreated branch must not legitimize the old reservation, got "+rec.Body.String())
	var descriptors int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_staged_output_objects WHERE reservation_id=$1`, reservation).Scan(&descriptors))
	assert.Equal(t, 0, descriptors)
}

// Final-review round 2 (real stack): JSON null metadata publishes without
// crashing, and every server-stamped field (file_count, staged_output
// block, file-level staged identity and digest) survives caller attempts
// to forge it — verified against the authoritative tables.
func TestStagedCommitMetadataNullAndStampIntegrity(t *testing.T) {
	s := newStagedStack(t, nil)
	ctx := context.Background()
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-final-m0:" + reservation.String()
	_, staged := s.stage(t, reservation, build, key, `{"m":1}`+"\n")
	nullMeta := stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, func(m map[string]any) {
		m["metadata"] = nil
		m["files"].([]map[string]any)[0]["metadata"] = nil
	})
	rec := s.commit(t, nullMeta)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	assertStamped := func(txnMeta, fileMeta map[string]any, wantID, wantSHA string) {
		t.Helper()
		assert.Equal(t, float64(1), txnMeta["file_count"])
		stamp, ok := txnMeta["staged_output"].(map[string]any)
		require.True(t, ok, "staged_output must stay an object")
		assert.Equal(t, wantID, stamp["staged_object_id"])
		assert.Equal(t, wantSHA, stamp["sha256"])
		assert.Equal(t, wantID, fileMeta["staged_object_id"])
		assert.Equal(t, wantSHA, fileMeta["sha256"])
	}
	var txnMetaRaw, fileMetaRaw []byte
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT metadata::text FROM dataset_transactions WHERE id = (SELECT consumed_by_transaction FROM dataset_staged_output_objects WHERE id=$1)`, staged.StagedObjectID).Scan(&txnMetaRaw))
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT metadata::text FROM dataset_file_index WHERE dataset_id=$1 AND path='part-00000.ndjson'`, s.dataset.ID).Scan(&fileMetaRaw))
	txnMeta, fileMeta := map[string]any{}, map[string]any{}
	require.NoError(t, json.Unmarshal(txnMetaRaw, &txnMeta))
	require.NoError(t, json.Unmarshal(fileMetaRaw, &fileMeta))
	assertStamped(txnMeta, fileMeta, staged.StagedObjectID, staged.SHA256)

	// Fresh reservation: forge every stamped key; the stamps must win.
	build2 := uuid.New()
	rec = s.do(t, http.MethodPost, "/v1/datasets/"+s.rid+"/branches/main/transactions/"+reservation.String()+":abort", "", nil, s.token)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	reservation2 := s.reserve(t, build2, "UPDATE", "wp33-staged-output")
	key2 := "of-wp33-final-m1:" + reservation2.String()
	_, staged2 := s.stage(t, reservation2, build2, key2, `{"m":2}`+"\n")
	forge := stagedCommitJSON(key2, staged2.StagedObjectID, staged2.SizeBytes, staged2.SHA256, reservation2, build2, func(m map[string]any) {
		m["metadata"] = map[string]any{"staged_output": "forged", "file_count": 99, "sha256": "caller-forged"}
		m["files"].([]map[string]any)[0]["metadata"] = map[string]any{"sha256": "file-forged"}
	})
	rec = s.commit(t, forge)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT metadata::text FROM dataset_transactions WHERE id = (SELECT consumed_by_transaction FROM dataset_staged_output_objects WHERE id=$1)`, staged2.StagedObjectID).Scan(&txnMetaRaw))
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT metadata::text FROM dataset_file_index WHERE dataset_id=$1 AND path='part-00000.ndjson'`, s.dataset.ID).Scan(&fileMetaRaw))
	txnMeta, fileMeta = map[string]any{}, map[string]any{}
	require.NoError(t, json.Unmarshal(txnMetaRaw, &txnMeta))
	require.NoError(t, json.Unmarshal(fileMetaRaw, &fileMeta))
	assertStamped(txnMeta, fileMeta, staged2.StagedObjectID, staged2.SHA256)
}

// Independent-review guard: the final storage key embeds the CONFIGURED
// BaseDir, so a hostile BaseDir ("../escape") or a BaseDir component that
// is already a symlink out of the backing root must fail the install —
// rooted mkdir/link plus the containment re-check — with nothing written
// anywhere outside the backing root and no side effects in the database.
func TestStagedCommitHostileBaseDirRejected(t *testing.T) {
	s := newStagedStack(t, nil)
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-bd:" + reservation.String()
	_, staged := s.stage(t, reservation, build, key, `{"b":1}`+"\n")
	version := s.currentVersion(t)

	stageRoot := s.stageRoot
	backing := *s.backing

	// Case 1: traversal BaseDir — the storage key fails the lexical
	// object-key rule before anything is created.
	evil := backing
	evil.BaseDir = "../escape"
	h := &handlers.Handlers{Repo: &repo.Repo{Pool: s.pool}, BackingFS: &evil, StagedOutputRoot: stageRoot}
	body := stagedCommitJSON(key, staged.StagedObjectID, staged.SizeBytes, staged.SHA256, reservation, build, nil)
	rec := commitViaHandler(t, h, s.rid, body, s.claims)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "failed to install sealed staged object")
	assert.NotContains(t, rec.Body.String(), stageRoot, "no absolute path may be echoed")

	// Case 2: a BaseDir component that is already a symlink pointing out
	// of the backing root — the rooted mkdir refuses the escape.
	outside := t.TempDir()
	linked := backing
	linked.BaseDir = "linkout"
	require.NoError(t, os.Symlink(outside, filepath.Join(backing.RootDir, "linkout")))
	h = &handlers.Handlers{Repo: &repo.Repo{Pool: s.pool}, BackingFS: &linked, StagedOutputRoot: stageRoot}
	rec = commitViaHandler(t, h, s.rid, body, s.claims)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())

	// Nothing escaped: no file under the outside directory, no new
	// version, the reservation stays OPEN and the descriptor SEALED.
	entries, err := os.ReadDir(outside)
	require.NoError(t, err)
	assert.Empty(t, entries, "nothing may be written outside the backing root")
	assert.Equal(t, version, s.currentVersion(t))
	assert.Equal(t, "OPEN", s.txnStatus(t, reservation))
	objStatus, _ := s.stagedRow(t, uuid.MustParse(staged.StagedObjectID))
	assert.Equal(t, "SEALED", objStatus)
}

// midUploadExpirer streams one record, then expires the attempt's lease
// mid-upload (simulating an upload that outlives its lease while the
// request is in flight), then ends the body.
type midUploadExpirer struct {
	pool        *pgxpool.Pool
	reservation uuid.UUID
	phase       int
}

func (m *midUploadExpirer) Read(p []byte) (int, error) {
	switch m.phase {
	case 0:
		m.phase = 1
		copy(p, []byte(`{"a":1}`+"\n"))
		return len(`{"a":1}` + "\n"), nil
	case 1:
		m.phase = 2
		_, _ = m.pool.Exec(context.Background(),
			`UPDATE dataset_staged_output_objects SET lease_expires_at = NOW() - interval '1 second' WHERE reservation_id=$1 AND status='STAGING'`, m.reservation)
		time.Sleep(250 * time.Millisecond)
		return 0, io.EOF
	default:
		return 0, io.EOF
	}
}

// Fourth-round clock guard: a lease that expires WHILE the request waits
// on the datasets row lock must be judged by a clock read AFTER the lock
// (clock_timestamp) — the takeover path — and not by the transaction-start
// NOW(), which would keep the dead lease looking live forever.
func TestStageAcquireClockAfterLockWait(t *testing.T) {
	s := newStagedStack(t, nil)
	ctx := context.Background()
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-clk1:" + reservation.String()

	dead := uuid.New()
	_, err := s.pool.Exec(ctx, `INSERT INTO dataset_staged_output_objects
		(id, dataset_id, branch_name, reservation_id, build_id, actor_id, idempotency_key, logical_path, status, lease_expires_at)
		VALUES ($1,$2,'main',$3,$4,$5,$6,'part-00000.ndjson','STAGING', NOW() + interval '2 seconds')`,
		dead, s.dataset.ID, reservation, build, s.dataset.OwnerID, key)
	require.NoError(t, err)
	deadDir := filepath.Join(s.stageRoot, "datasets", s.dataset.ID.String(), "reservations", reservation.String())
	require.NoError(t, os.MkdirAll(deadDir, 0o700))
	deadFile := filepath.Join(deadDir, dead.String()+".ndjson")
	require.NoError(t, os.WriteFile(deadFile, []byte("stale"), 0o600))

	// Hold the datasets row lock well past the lease deadline.
	lockTx, err := s.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = lockTx.Exec(ctx, `SELECT id FROM datasets WHERE id=$1 FOR UPDATE`, s.dataset.ID)
	require.NoError(t, err)
	released := make(chan struct{})
	go func() {
		defer close(released)
		time.Sleep(3500 * time.Millisecond)
		_ = lockTx.Commit(ctx)
	}()

	recCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recCh <- s.do(t, http.MethodPost, s.stageURL(reservation, build, key), "application/x-ndjson", strings.NewReader(`{"c":1}`+"\n"), s.token)
	}()
	time.Sleep(400 * time.Millisecond) // the request is up and waiting on the lock
	<-released                         // lease (2s) has expired by now (3.5s)
	rec := <-recCh

	require.Equal(t, http.StatusCreated, rec.Code,
		"after the lock wait the expired lease must be taken over (clock read post-lock), got %s", rec.Body.String())
	_, err = os.Stat(deadFile)
	assert.True(t, os.IsNotExist(err), "takeover clears the dead attempt's files")
}

// Fourth-round clock guard: a lease that expires MID-UPLOAD (while the
// request streams) is rejected at the seal by the post-lock database
// clock — not by a timestamp captured before the upload.
func TestStageSealClockAfterMidUploadLeaseExpiry(t *testing.T) {
	s := newStagedStack(t, nil)
	ctx := context.Background()
	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-clk2:" + reservation.String()
	slow := &midUploadExpirer{pool: s.pool, reservation: reservation}
	rec := s.do(t, http.MethodPost, s.stageURL(reservation, build, key), "application/x-ndjson", slow, s.token)
	assert.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "lease")
	var descriptors int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dataset_staged_output_objects WHERE reservation_id=$1`, reservation).Scan(&descriptors))
	assert.Equal(t, 0, descriptors, "an out-of-lease upload must not seal")
}

// Integration-boundary guard: a source record within the 1MiB scan bound
// but full of & < > must come back from rows:scan WITHOUT HTML-entity
// inflation (&amp; &lt; &gt;), so the compact row bytes stay within the
// identical outputs:stage per-record cap and round-trip byte-faithfully.
func TestScanCompactHTMLRowsStageRoundTrip(t *testing.T) {
	s := newStagedStack(t, nil)
	// Build a legal NDJSON record just under the 1MiB (newline-included)
	// bound whose payload is dominated by & < > characters.
	filler := strings.Repeat("&<>", 8)
	record := `{"p":"` + filler
	for len(record) < 1024*1024-64 {
		record += filler
	}
	record += `"}` // content within the bound
	record += "\n" // newline included in the 1MiB cap
	require.Less(t, len(record), 1024*1024+1)
	// Without the fix, the scan response row would encode every & < > as
	// an entity and the compact row would exceed the stage cap.
	recordBytes := []byte(record)

	// Publish v1 through the real inline keyed path (regression surface).
	seed := base64.StdEncoding.EncodeToString(recordBytes)
	body := fmt.Sprintf(`{"idempotency_key":"of-wp33-esc-v1","branch":"main","files":[{"logical_path":"part-00000.ndjson","content_base64":%q,"size_bytes":%d}]}`, seed, len(recordBytes))
	rec := s.commit(t, body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	version := s.currentVersion(t)

	// Scan: the wire rows must not carry HTML entities.
	scanRec := s.do(t, http.MethodGet, "/v1/datasets/"+s.rid+"/files/rows:scan"+allFilesQuery(version, "", 0), "", nil, s.token)
	require.Equal(t, http.StatusOK, scanRec.Code, scanRec.Body.String())
	assert.NotContains(t, scanRec.Body.String(), "&amp;")
	assert.NotContains(t, scanRec.Body.String(), "&lt;")
	var page allFilesHTTPResponse
	require.NoError(t, json.Unmarshal(scanRec.Body.Bytes(), &page))
	require.Len(t, page.Rows, 1)
	// Value semantics survive: the scanned row equals the source record.
	assert.JSONEq(t, strings.TrimSuffix(record, "\n"), string(page.Rows[0]))

	// Re-spool the compact row the way PBS would (non-escaping encoder,
	// newline included) — it must still fit the identical stage cap and
	// seal.
	var compact bytes.Buffer
	enc := json.NewEncoder(&compact)
	enc.SetEscapeHTML(false)
	require.NoError(t, enc.Encode(page.Rows[0]))
	compactLine := append(bytes.TrimRight(compact.Bytes(), "\n"), '\n')
	require.LessOrEqual(t, len(compactLine), 1024*1024, "the compact row must stay within the stage per-record cap")

	build := uuid.New()
	reservation := s.reserve(t, build, "UPDATE", "wp33-staged-output")
	key := "of-wp33-esc:" + reservation.String()
	stageRec, staged := s.stage(t, reservation, build, key, string(compactLine))
	require.Equal(t, http.StatusCreated, stageRec.Code, stageRec.Body.String())
	sealedBytes, err := os.ReadFile(filepath.Join(s.stageRoot, "datasets", s.dataset.ID.String(), "reservations", reservation.String(), staged.StagedObjectID+".ndjson"))
	require.NoError(t, err)
	assert.JSONEq(t, strings.TrimSuffix(record, "\n"), string(bytes.TrimRight(sealedBytes, "\n")), "sealed bytes must preserve the row's value semantics")
}
