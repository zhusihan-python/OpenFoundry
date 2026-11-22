package handler

// WP3.3 3B batched build tests: the canonical happy path over a fake
// dataset wire (rows:scan paging, streaming outputs:stage with the
// same-key descriptor rule, keyed outputs:commit receipts with 201-first
// / 200-replay), an in-memory lease-fenced store, and the negative
// controls for grammar, freeze, actor binding, replay exactness, abort
// races and scan page validation.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/resolver"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

// ---------------------------------------------------------------------------
// Fake dataset wire (mirrors the 3A contract the PBS client depends on).
// ---------------------------------------------------------------------------

type fakeStagedDescriptor struct {
	key         string
	datasetRID  string
	branch      string
	reservation string
	buildID     string
	logicalPath string
	size        int64
	sha         string
	records     int64
	consumed    bool
}

type fakeCommitReceipt struct {
	fingerprint string
	response    []byte
}

type fakeDatasetService struct {
	t  *testing.T
	mu sync.Mutex

	// Input fixture served by rows:scan (ndjson records, no newlines).
	scanRecords [][]byte
	// scanBehavior overrides the scan response when set (negative controls).
	scanBehavior func(r *http.Request) (int, []byte)

	// Stage side.
	stageEarlyReject *struct {
		Status int
		Body   string
	}
	stageReceiptMutator func(receipt map[string]any)
	stageDescriptors    map[string]*fakeStagedDescriptor // by reservation UUID
	stageBodies         map[string][]byte                // reservation -> sealed body
	stageTruncated      map[string]bool
	stageCalls          int

	// Commit side (keyed receipts).
	receipts           map[string]fakeCommitReceipt
	commits            []string // committed request bodies in order
	commitTxns         []uuid.UUID
	failCommitOnceWith *int // override the first success response after the commit
	commitCalls        int

	// Abort side.
	aborted map[string]string // txn UUID -> "ABORTED"

	server *httptest.Server
}

func newFakeBatchedDVS(t *testing.T, records [][]byte) *fakeDatasetService {
	f := &fakeDatasetService{
		t:                t,
		scanRecords:      records,
		stageDescriptors: map[string]*fakeStagedDescriptor{},
		stageBodies:      map[string][]byte{},
		stageTruncated:   map[string]bool{},
		receipts:         map[string]fakeCommitReceipt{},
		aborted:          map[string]string{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/datasets/", func(w http.ResponseWriter, r *http.Request) {
		require.NotEqual(t, "", strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), "batched calls never run without the caller JWT")
		switch {
		case strings.HasSuffix(r.URL.Path, "/files/rows:scan"):
			f.handleScan(w, r)
		case strings.HasSuffix(r.URL.Path, "/outputs:stage"):
			f.handleStage(w, r)
		case strings.HasSuffix(r.URL.Path, "/outputs:commit"):
			f.handleCommit(w, r)
		case strings.HasSuffix(r.URL.Path, ":abort"):
			f.handleAbort(w, r)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeDatasetService) handleScan(w http.ResponseWriter, r *http.Request) {
	if f.scanBehavior != nil {
		status, body := f.scanBehavior(r)
		w.WriteHeader(status)
		_, _ = w.Write(body)
		return
	}
	q := r.URL.Query()
	limit := 2 // small pages exercise the cursor loop
	if l := q.Get("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}
	offset := 0
	if c := q.Get("cursor"); c != "" {
		fmt.Sscanf(c, "off%d", &offset)
	}
	datasetUUID := bareDatasetUUID(r.URL.Path, "/files/rows:scan")
	end := offset + limit
	if end > len(f.scanRecords) {
		end = len(f.scanRecords)
	}
	rows := f.scanRecords[offset:end]
	var buf bytes.Buffer
	buf.WriteString(`{"dataset_id":"` + datasetUUID + `","branch":` + jsonString(q.Get("branch")) + `,"version":` + q.Get("version") + `,"transaction_id":"11111111-1111-1111-1111-111111111111","format":"ndjson","mode":"all_files","rows":[`)
	for i, row := range rows {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(row)
	}
	buf.WriteString(`],"row_count":` + fmt.Sprint(len(rows)))
	if end < len(f.scanRecords) {
		buf.WriteString(`,"next_cursor":"off` + fmt.Sprint(end) + `"`)
	} else {
		buf.WriteString(`,"next_cursor":null`)
	}
	buf.WriteString(`,"page_bytes":100}`)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

func (f *fakeDatasetService) handleStage(w http.ResponseWriter, r *http.Request) {
	if f.stageEarlyReject != nil {
		// Early rejection: answered before the body streams — the client
		// must tear its feed down instead of streaming into a dead upload.
		w.WriteHeader(f.stageEarlyReject.Status)
		_, _ = w.Write([]byte(f.stageEarlyReject.Body))
		return
	}
	q := r.URL.Query()
	reservation := q.Get("reservation_id")
	// Stream the body WITHOUT holding the service mutex: the client feeds
	// this upload from concurrent rows:scan pages against the same server,
	// so a lock held across the body read would deadlock the pipeline.
	hash := sha256.New()
	var size, records int64
	truncated := false
	var parquetBody []byte
	if q.Get("format") == "parquet" && r.Header.Get("Content-Type") == "application/vnd.apache.parquet" {
		body, err := io.ReadAll(r.Body)
		if err == nil {
			reader, readErr := file.NewParquetReader(bytes.NewReader(body))
			err = readErr
			if err == nil {
				records = reader.NumRows()
				_ = reader.Close()
			}
		}
		truncated = err != nil
		if !truncated {
			parquetBody = body
			size = int64(len(body))
			hash.Write(body)
		}
	} else if !q.Has("format") && r.Header.Get("Content-Type") == "application/x-ndjson" {
		reader := bufio.NewReader(r.Body)
		for {
			line, err := reader.ReadBytes('\n')
			if errors.Is(err, io.EOF) {
				if len(line) > 0 {
					truncated = true // no trailing newline: partial record
				}
				break
			}
			if err != nil {
				truncated = true
				break
			}
			hash.Write(line)
			size += int64(len(line))
			records++
		}
	} else {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	f.mu.Lock()
	f.stageCalls++
	f.mu.Unlock()
	if truncated {
		f.mu.Lock()
		f.stageTruncated[reservation] = true
		f.mu.Unlock()
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"staged_body_truncated"}`))
		return
	}
	f.stageTruncated[reservation] = false
	descriptor := &fakeStagedDescriptor{
		key:         q.Get("idempotency_key"),
		datasetRID:  "ri.foundry.main.dataset." + bareDatasetUUID(r.URL.Path, "/outputs:stage"),
		branch:      q.Get("branch"),
		reservation: reservation,
		buildID:     q.Get("build_id"),
		logicalPath: q.Get("logical_path"),
		size:        size,
		sha:         hex.EncodeToString(hash.Sum(nil)),
		records:     records,
	}
	if existing, ok := f.stageDescriptors[reservation]; ok {
		if existing.size != descriptor.size || existing.sha != descriptor.sha {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"staged_object_conflict","code":"STAGED_OBJECT_CONFLICT"}`))
			return
		}
		descriptor = existing // byte-identical retransmission reuses the seal
	} else {
		f.stageDescriptors[reservation] = descriptor
		f.stageBodies[reservation] = parquetBody
	}
	receipt := map[string]any{
		"staged_object_id": "22222222-2222-2222-2222-222222222222",
		"dataset_rid":      descriptor.datasetRID,
		"branch":           descriptor.branch,
		"reservation_id":   descriptor.reservation,
		"build_id":         descriptor.buildID,
		"logical_path":     descriptor.logicalPath,
		"idempotency_key":  descriptor.key,
		"size_bytes":       descriptor.size,
		"sha256":           descriptor.sha,
		"record_count":     descriptor.records,
		"expires_at":       time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
	}
	if q.Has("format") {
		receipt["format"] = q.Get("format")
	}
	if f.stageReceiptMutator != nil {
		f.stageReceiptMutator(receipt)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(receipt)
}

func (f *fakeDatasetService) handleCommit(w http.ResponseWriter, r *http.Request) {
	f.commitCalls++
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var req struct {
		IdempotencyKey  string `json:"idempotency_key"`
		Branch          string `json:"branch"`
		TransactionType string `json:"transaction_type"`
		ReservationID   string `json:"reservation_id"`
		BuildID         string `json:"build_id"`
		Files           []struct {
			LogicalPath    string `json:"logical_path"`
			Operation      string `json:"operation"`
			StagedObjectID string `json:"staged_object_id"`
			SizeBytes      int64  `json:"size_bytes"`
			SHA256         string `json:"sha256"`
		} `json:"files"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(body))
	if receipt, ok := f.receipts[req.IdempotencyKey]; ok {
		if receipt.fingerprint != fingerprint {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"IDEMPOTENCY_KEY_CONFLICT"}`))
			return
		}
		// Receipt replay: the ORIGINAL response bytes, 200.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(receipt.response)
		return
	}
	// First commit: same-key + binding + digest validation (DVS parity).
	if req.TransactionType != "SNAPSHOT" || len(req.Files) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	file := req.Files[0]
	byKey := map[string]*fakeStagedDescriptor{}
	for _, d := range f.stageDescriptors {
		byKey[d.key] = d
	}
	obj, ok := byKey[req.IdempotencyKey]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"staged object unknown for key"}`))
		return
	}
	if obj.key != req.IdempotencyKey || obj.branch != req.Branch || obj.reservation != req.ReservationID ||
		obj.buildID != req.BuildID || obj.logicalPath != file.LogicalPath ||
		file.Operation != "STAGED_ADD" || obj.consumed ||
		file.SizeBytes != obj.size || file.SHA256 != obj.sha {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"staged binding mismatch"}`))
		return
	}
	obj.consumed = true
	published := uuid.New()
	commitUUID := bareDatasetUUID(r.URL.Path, "/outputs:commit")
	f.commits = append(f.commits, string(body))
	f.commitTxns = append(f.commitTxns, published)
	response, _ := json.Marshal(map[string]any{
		"dataset_id":  commitUUID,
		"dataset_rid": obj.datasetRID,
		"branch":      req.Branch,
		"transaction": map[string]any{
			"id":          published.String(),
			"dataset_id":  commitUUID,
			"branch_name": req.Branch,
			"tx_type":     "SNAPSHOT",
			"status":      "COMMITTED",
		},
	})
	f.receipts[req.IdempotencyKey] = fakeCommitReceipt{fingerprint: fingerprint, response: response}
	if f.failCommitOnceWith != nil {
		status := *f.failCommitOnceWith
		f.failCommitOnceWith = nil
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"error":"OUTCOME_UNKNOWN"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(response)
}

func (f *fakeDatasetService) handleAbort(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	txn := strings.TrimSuffix(parts[len(parts)-1], ":abort")
	f.aborted[txn] = "ABORTED"
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ABORTED"}`))
}

func jsonString(s string) string { b, _ := json.Marshal(s); return string(b) }

// bareDatasetUUID extracts the dataset UUID from a /v1/datasets/<seg><suffix>
// path; <seg> may be a bare UUID or the canonical RID.
func bareDatasetUUID(path, suffix string) string {
	seg := strings.TrimSuffix(strings.TrimPrefix(path, "/v1/datasets/"), suffix)
	return strings.TrimPrefix(seg, "ri.foundry.main.dataset.")
}

// ---------------------------------------------------------------------------
// Fake batched store (in-memory mirror of the postgres state machine).
// ---------------------------------------------------------------------------

func (*fakeBatchedStore) GetPipeline(_ context.Context, id uuid.UUID) (*models.Pipeline, error) {
	return &models.Pipeline{ID: id}, nil
}

type fakeBatchedStore struct {
	mu          sync.Mutex
	configs     map[uuid.UUID]*models.BatchedBuildConfig
	jobs        map[uuid.UUID]string // build -> job state
	buildStates map[uuid.UUID]string
	locks       map[uuid.UUID]bool // build -> lock held
	pending     map[uuid.UUID][]PendingOutputTxn
	finalized   []uuid.UUID
	failed      []uuid.UUID
	released    []uuid.UUID
}

func newFakeBatchedStore() *fakeBatchedStore {
	return &fakeBatchedStore{
		configs:     map[uuid.UUID]*models.BatchedBuildConfig{},
		jobs:        map[uuid.UUID]string{},
		buildStates: map[uuid.UUID]string{},
		locks:       map[uuid.UUID]bool{},
		pending:     map[uuid.UUID][]PendingOutputTxn{},
	}
}

func (s *fakeBatchedStore) GetBatchedConfig(_ context.Context, idOrRID string) (*models.BatchedBuildConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cfg := range s.configs {
		if cfg.BuildID.String() == idOrRID || "ri.foundry.main.build."+cfg.BuildID.String() == idOrRID {
			clone := *cfg
			return &clone, nil
		}
	}
	return nil, nil
}

func (s *fakeBatchedStore) ListPendingTxns(_ context.Context, buildID uuid.UUID) ([]PendingOutputTxn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]PendingOutputTxn(nil), s.pending[buildID]...), nil
}

func (s *fakeBatchedStore) acquire(ctx context.Context, buildID, owner uuid.UUID, leaseTTL time.Duration, from []string, target string) (*models.BatchedBuildConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, ok := s.configs[buildID]
	if !ok {
		return nil, ErrBatchedNotFound
	}
	switch cfg.Stage {
	case models.BatchedStagePreparing:
		return nil, ErrBatchedPreparing
	case models.BatchedStageDone, models.BatchedStageFailed, models.BatchedStageAborted:
		return nil, ErrBatchedTerminal
	}
	if cfg.LeaseExpiresAt != nil && cfg.LeaseExpiresAt.After(time.Now()) {
		return nil, ErrBatchedLeaseHeld
	}
	allowed := false
	for _, stage := range from {
		if cfg.Stage == stage {
			allowed = true
		}
	}
	if !allowed {
		return nil, fmt.Errorf("batched_stage_conflict: %s", cfg.Stage)
	}
	if s.jobs[buildID] == "" || s.jobs[buildID] == "WAITING" {
		s.jobs[buildID] = "RUNNING"
	}
	cfg.Stage = target
	// abort_requested survives the takeover (production semantics).
	ownerCopy := owner
	cfg.LeaseOwner = &ownerCopy
	exp := time.Now().Add(leaseTTL)
	cfg.LeaseExpiresAt = &exp
	clone := *cfg
	return &clone, nil
}

func (s *fakeBatchedStore) AcquireBatchedRun(ctx context.Context, buildID, owner uuid.UUID, leaseTTL time.Duration) (*models.BatchedBuildConfig, error) {
	return s.acquire(ctx, buildID, owner, leaseTTL, []string{models.BatchedStagePending, models.BatchedStageStaging, models.BatchedStageStaged}, models.BatchedStageStaging)
}

func (s *fakeBatchedStore) AcquireBatchedReplay(ctx context.Context, buildID, owner uuid.UUID, leaseTTL time.Duration) (*models.BatchedBuildConfig, error) {
	return s.acquire(ctx, buildID, owner, leaseTTL, []string{models.BatchedStagePublishing, models.BatchedStageOutcomeUnknown}, models.BatchedStagePublishing)
}

func (s *fakeBatchedStore) MarkBatchedStaged(_ context.Context, buildID, owner uuid.UUID, stagedObjectID, sha string, size, records int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.configs[buildID]
	if cfg == nil || cfg.LeaseOwner == nil || *cfg.LeaseOwner != owner || cfg.Stage != models.BatchedStageStaging {
		return ErrBatchedFenceLost
	}
	cfg.Stage = models.BatchedStageStaged
	cfg.StagedObjectID, cfg.StagedSHA256, cfg.StagedSize, cfg.StagedRecords = stagedObjectID, sha, size, records
	return nil
}

func (s *fakeBatchedStore) PersistBatchedCommitIntent(_ context.Context, buildID, owner uuid.UUID, key string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.configs[buildID]
	if cfg == nil || cfg.LeaseOwner == nil || *cfg.LeaseOwner != owner || cfg.Stage != models.BatchedStageStaged {
		return ErrBatchedFenceLost
	}
	if cfg.AbortRequested {
		return ErrBatchedPublishing
	}
	cfg.Stage = models.BatchedStagePublishing
	cfg.CommitIDempotencyKey = key
	cfg.CommitRequestBody = append([]byte(nil), body...)
	return nil
}

func (s *fakeBatchedStore) MarkBatchedOutcomeUnknown(_ context.Context, buildID, owner uuid.UUID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.configs[buildID]
	if cfg == nil || cfg.LeaseOwner == nil || *cfg.LeaseOwner != owner || cfg.Stage != models.BatchedStagePublishing {
		return ErrBatchedFenceLost
	}
	cfg.Stage = models.BatchedStageOutcomeUnknown
	cfg.ErrorMessage = reason
	cfg.LeaseOwner, cfg.LeaseExpiresAt = nil, nil
	return nil
}

func (s *fakeBatchedStore) FinalizeBatchedCommit(_ context.Context, buildID, owner uuid.UUID, responseBody []byte, publishedTxnRID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.configs[buildID]
	if cfg == nil || cfg.LeaseOwner == nil || *cfg.LeaseOwner != owner || (cfg.Stage != models.BatchedStagePublishing && cfg.Stage != models.BatchedStageOutcomeUnknown) {
		return ErrBatchedFenceLost
	}
	cfg.Stage = models.BatchedStageDone
	cfg.CommitResponseBody = append([]byte(nil), responseBody...)
	cfg.PublishedTxnRID = publishedTxnRID
	cfg.LeaseOwner, cfg.LeaseExpiresAt = nil, nil
	s.jobs[buildID] = "COMPLETED"
	s.buildStates[buildID] = "BUILD_COMPLETED"
	s.locks[buildID] = false
	s.pending[buildID] = nil
	s.finalized = append(s.finalized, buildID)
	return nil
}

func (s *fakeBatchedStore) FailBatchedBuild(_ context.Context, buildID, owner uuid.UUID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.configs[buildID]
	if cfg == nil || cfg.LeaseOwner == nil || *cfg.LeaseOwner != owner {
		return ErrBatchedFenceLost
	}
	cfg.Stage = models.BatchedStageFailed
	cfg.ErrorMessage = reason
	cfg.LeaseOwner, cfg.LeaseExpiresAt = nil, nil
	s.jobs[buildID] = "FAILED"
	s.buildStates[buildID] = "BUILD_FAILED"
	s.failed = append(s.failed, buildID)
	return nil
}

func (s *fakeBatchedStore) RenewBatchedLease(_ context.Context, buildID, owner uuid.UUID, leaseTTL time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.configs[buildID]
	if cfg == nil || cfg.LeaseOwner == nil || *cfg.LeaseOwner != owner || (cfg.LeaseExpiresAt != nil && !cfg.LeaseExpiresAt.After(time.Now())) {
		return ErrBatchedFenceLost
	}
	exp := time.Now().Add(leaseTTL)
	cfg.LeaseExpiresAt = &exp
	return nil
}

func (s *fakeBatchedStore) ReleaseBatchedLease(_ context.Context, buildID, owner uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.configs[buildID]
	if cfg == nil || cfg.LeaseOwner == nil || *cfg.LeaseOwner != owner {
		return ErrBatchedFenceLost
	}
	cfg.LeaseOwner, cfg.LeaseExpiresAt = nil, nil
	s.released = append(s.released, buildID)
	return nil
}

func (s *fakeBatchedStore) BeginBatchedAbort(_ context.Context, idOrRID string, takeoverOwner uuid.UUID, leaseTTL time.Duration) (*BatchedAbortDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cfg *models.BatchedBuildConfig
	for _, c := range s.configs {
		if c.BuildID.String() == idOrRID || "ri.foundry.main.build."+c.BuildID.String() == idOrRID {
			cfg = c
			break
		}
	}
	if cfg == nil {
		return nil, ErrBatchedNotFound
	}
	leaseLive := cfg.LeaseExpiresAt != nil && cfg.LeaseExpiresAt.After(time.Now())
	switch cfg.Stage {
	case models.BatchedStagePreparing:
		return &BatchedAbortDecision{Kind: BatchedAbortKindPreparing, Config: cloneBatched(cfg)}, nil
	case models.BatchedStagePublishing, models.BatchedStageOutcomeUnknown:
		return &BatchedAbortDecision{Kind: BatchedAbortKindPublishing, Config: cloneBatched(cfg), Permanent: cfg.Stage == models.BatchedStageOutcomeUnknown, Detail: cfg.ErrorMessage}, nil
	case models.BatchedStageDone:
		return &BatchedAbortDecision{Kind: BatchedAbortKindDone, Config: cloneBatched(cfg)}, nil
	case models.BatchedStageAborted:
		return &BatchedAbortDecision{Kind: BatchedAbortKindAborted, Config: cloneBatched(cfg)}, nil
	case models.BatchedStageFailed:
		if leaseLive {
			return &BatchedAbortDecision{Kind: BatchedAbortKindFailedActive, Config: cloneBatched(cfg)}, nil
		}
		ownerCopy := takeoverOwner
		cfg.LeaseOwner = &ownerCopy
		exp := time.Now().Add(leaseTTL)
		cfg.LeaseExpiresAt = &exp
		return &BatchedAbortDecision{Kind: BatchedAbortKindFailedRetry, Config: cloneBatched(cfg), Owner: takeoverOwner}, nil
	}
	if leaseLive {
		cfg.AbortRequested = true
		return &BatchedAbortDecision{Kind: BatchedAbortKindFlagged, Config: cloneBatched(cfg)}, nil
	}
	ownerCopy := takeoverOwner
	cfg.LeaseOwner = &ownerCopy
	exp := time.Now().Add(leaseTTL)
	cfg.LeaseExpiresAt = &exp
	cfg.AbortRequested = true
	return &BatchedAbortDecision{Kind: BatchedAbortKindTakeover, Config: cloneBatched(cfg), Owner: takeoverOwner}, nil
}

func (s *fakeBatchedStore) CompleteBatchedAbort(_ context.Context, buildID, owner uuid.UUID, keepFailed bool, outputDatasetRID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.configs[buildID]
	if cfg == nil || cfg.LeaseOwner == nil || *cfg.LeaseOwner != owner {
		return ErrBatchedFenceLost
	}
	if keepFailed {
		cfg.Stage = models.BatchedStageFailed
		s.jobs[buildID] = "FAILED"
		s.buildStates[buildID] = "BUILD_FAILED"
	} else {
		cfg.Stage = models.BatchedStageAborted
		s.jobs[buildID] = "ABORTED"
		s.buildStates[buildID] = "BUILD_ABORTED"
	}
	cfg.LeaseOwner, cfg.LeaseExpiresAt = nil, nil
	cfg.AbortRequested = false
	s.locks[buildID] = false
	s.pending[buildID] = nil
	return nil
}

// CreateBuild-side ports (OpenBuild family + recorder + prepare).
func (s *fakeBatchedStore) OpenBuild(context.Context, resolver.ResolveBuildArgs, uuid.UUID) error {
	return nil
}

func (s *fakeBatchedStore) PersistResolvedBuild(_ context.Context, resolved *models.ResolvedBuild) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if resolved.BatchConfig != nil {
		cfg := s.configs[resolved.BuildID]
		if cfg != nil {
			cfg.Stage = models.BatchedStagePending
			// UUID-tolerant match (production parses both spellings).
			configUUID, cerr := batchedDatasetUUID(cfg.OutputDatasetRID)
			for _, opened := range resolved.OpenedTransactions {
				openedUUID, oerr := batchedDatasetUUID(opened.DatasetRID)
				if cerr == nil && oerr == nil && openedUUID == configUUID {
					cfg.ReservationRID = opened.TransactionRID
				}
			}
			s.locks[resolved.BuildID] = true
			s.pending[resolved.BuildID] = []PendingOutputTxn{{
				DatasetRID:     cfg.OutputDatasetRID,
				Branch:         cfg.OutputBranch,
				TransactionRID: cfg.ReservationRID,
			}}
			s.jobs[resolved.BuildID] = "WAITING"
		}
	}
	return nil
}

func (s *fakeBatchedStore) MarkBuildFailed(_ context.Context, buildID uuid.UUID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cfg := s.configs[buildID]; cfg != nil && cfg.Stage == models.BatchedStagePreparing {
		cfg.Stage = models.BatchedStageFailed
		cfg.ErrorMessage = reason
	}
	return nil
}

func (s *fakeBatchedStore) OpenBatchedPrepare(_ context.Context, _ resolver.ResolveBuildArgs, buildID uuid.UUID, cfg *models.BatchedBuildConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clone := *cfg
	clone.BuildID = buildID
	clone.Stage = models.BatchedStagePreparing
	s.configs[buildID] = &clone
	return nil
}

func (s *fakeBatchedStore) RecordPendingTxn(_ context.Context, buildID uuid.UUID, datasetRID, branch, txnRID string) error {
	return nil
}

func (s *fakeBatchedStore) ClearPendingTxn(_ context.Context, buildID uuid.UUID, txnRID string) error {
	return nil
}

func (s *fakeBatchedStore) LoadPlan(context.Context, uuid.UUID) (executor.Plan, error) {
	return executor.Plan{}, nil
}

func cloneBatched(cfg *models.BatchedBuildConfig) *models.BatchedBuildConfig {
	clone := *cfg
	return &clone
}

// ---------------------------------------------------------------------------
// Stub versioning adapter for the CreateBuild side.
// ---------------------------------------------------------------------------

type batchedStubVersioning struct {
	activeBranch   string
	openedForBuild map[string]map[string]any // txn RID -> providence
	openLegacy     int
	schema         json.RawMessage
	abortedTargets []string // dataset|branch|txn RID of precise cancels
}

func newBatchedStubVersioning(active string) *batchedStubVersioning {
	return &batchedStubVersioning{activeBranch: active, openedForBuild: map[string]map[string]any{}, schema: json.RawMessage(`{}`)}
}

func (v *batchedStubVersioning) ListBranches(_ context.Context, _ string) ([]models.BranchSnapshot, error) {
	return []models.BranchSnapshot{{Name: v.activeBranch}, {Name: "main"}}, nil
}

func (v *batchedStubVersioning) OpenTransaction(_ context.Context, _, _ string) (string, error) {
	v.openLegacy++
	return "ri.foundry.main.transaction." + uuid.NewString(), nil
}

func (v *batchedStubVersioning) OpenTransactionForBuild(_ context.Context, _, _ string, buildID uuid.UUID, purpose string) (string, error) {
	txn := "ri.foundry.main.transaction." + uuid.NewString()
	v.openedForBuild[txn] = map[string]any{"build_id": buildID.String(), "purpose": purpose}
	return txn, nil
}

func (v *batchedStubVersioning) ViewSchema(_ context.Context, _RID, _branch string) (json.RawMessage, error) {
	return v.schema, nil
}

func (v *batchedStubVersioning) DatasetActiveBranch(_ context.Context, _ string) (string, error) {
	return v.activeBranch, nil
}

func (v *batchedStubVersioning) AbortTransaction(_ context.Context, datasetRID, branch, txnRID string) error {
	v.abortedTargets = append(v.abortedTargets, datasetRID+"|"+branch+"|"+txnRID)
	return nil
}

// ---------------------------------------------------------------------------
// Grammar matrix.
// ---------------------------------------------------------------------------

func batchedGrammarSpec(t *testing.T, mutate func(p *batchedPayload), inputRID, outputRID string) models.JobSpec {
	t.Helper()
	payload := batchedPayload{
		TransformType: "batched_dataset_copy",
		Input:         batchedPayloadInput{Version: 3, Branch: "main", AllFiles: true},
		Output:        batchedPayloadOutput{},
	}
	if mutate != nil {
		mutate(&payload)
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return models.JobSpec{
		RID: "ri.foundry.main.job_spec.b1", PipelineRID: "ri.foundry.main.pipeline." + uuid.NewString(), BranchName: "main",
		Inputs: []models.InputSpec{{DatasetRID: inputRID}}, OutputDatasetRIDs: []string{outputRID},
		LogicKind: "TRANSFORM", LogicPayload: raw, ContentHash: "hash-1",
	}
}

func TestParseBatchedSpecMatrix(t *testing.T) {
	in := "ri.foundry.main.dataset." + uuid.NewString()
	out := "ri.foundry.main.dataset." + uuid.NewString()
	baseArgs := func() resolver.ResolveBuildArgs {
		return resolver.ResolveBuildArgs{BuildBranch: "main", TriggerKind: "MANUAL"}
	}
	baseBody := func(outRID string) models.CreateBuildRequest {
		return models.CreateBuildRequest{PipelineRID: "ri.foundry.main.pipeline.p", BuildBranch: "main", OutputDatasetRIDs: []string{outRID}}
	}

	t.Run("accepts the canonical shape", func(t *testing.T) {
		spec := batchedGrammarSpec(t, nil, in, out)
		cfg, err := parseBatchedSpec([]models.JobSpec{spec}, baseBody(out), baseArgs(), uuid.New())
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, "main", cfg.InputBranch)
		require.Equal(t, int32(3), cfg.InputVersion)
		require.True(t, cfg.AllFiles)
		require.Equal(t, "part-00000.ndjson", cfg.LogicalPath)
		require.Equal(t, out, cfg.OutputDatasetRID)
		require.Equal(t, in, cfg.InputDatasetRID)
		require.NotEqual(t, cfg.InputDatasetRID, cfg.OutputDatasetRID)
	})

	t.Run("batched family with malformed payload is rejected not re-routed", func(t *testing.T) {
		// transform_type matches the family but input.version is a string:
		// the discriminant-only detection must claim it so the strict parse
		// REJECTS it — a full-struct detection failure would silently fall
		// back to the legacy runtime.
		raw := json.RawMessage(`{"transform_type":"batched_dataset_copy","input":{"version":"three","branch":"main","all_files":true}}`)
		spec := models.JobSpec{RID: "s", LogicKind: "TRANSFORM", OutputDatasetRIDs: []string{out}, Inputs: []models.InputSpec{{DatasetRID: in}}, LogicPayload: raw, ContentHash: "h"}
		require.True(t, detectBatchedFamily([]models.JobSpec{spec}))
		cfg, err := parseBatchedSpec([]models.JobSpec{spec}, baseBody(out), baseArgs(), uuid.New())
		require.Error(t, err)
		require.Nil(t, cfg)
	})

	t.Run("non batched spec returns nil nil", func(t *testing.T) {
		spec := models.JobSpec{RID: "s", LogicKind: "TRANSFORM", OutputDatasetRIDs: []string{out}, Inputs: []models.InputSpec{{DatasetRID: in}}}
		cfg, err := parseBatchedSpec([]models.JobSpec{spec}, baseBody(out), baseArgs(), uuid.New())
		require.NoError(t, err)
		require.Nil(t, cfg)
	})

	rejections := []struct {
		name  string
		mutdb func(p *batchedPayload)
		args  func(resolver.ResolveBuildArgs) resolver.ResolveBuildArgs
		body  func(models.CreateBuildRequest) models.CreateBuildRequest
		specs func(spec models.JobSpec) []models.JobSpec
		spec  func(spec models.JobSpec) models.JobSpec
		input func(spec models.JobSpec) models.JobSpec
	}{
		{name: "multi spec", specs: func(spec models.JobSpec) []models.JobSpec { return []models.JobSpec{spec, spec} }},
		{name: "wrong logic kind", spec: func(spec models.JobSpec) models.JobSpec { spec.LogicKind = "SYNC"; return spec }},
		{name: "scheduled trigger", args: func(a resolver.ResolveBuildArgs) resolver.ResolveBuildArgs { a.TriggerKind = "SCHEDULED"; return a }},
		{name: "force build", args: func(a resolver.ResolveBuildArgs) resolver.ResolveBuildArgs { a.ForceBuild = true; return a }},
		{name: "request outputs", body: func(b models.CreateBuildRequest) models.CreateBuildRequest {
			b.OutputDatasetRIDs = append(b.OutputDatasetRIDs, out)
			return b
		}},
		{name: "input fallback chain", input: func(spec models.JobSpec) models.JobSpec {
			spec.Inputs[0].FallbackChain = []string{"develop"}
			return spec
		}},
		{name: "input view filter", input: func(spec models.JobSpec) models.JobSpec {
			spec.Inputs[0].ViewFilter = []models.ViewFilter{models.ViewFilter(`{}`)}
			return spec
		}},
		{name: "input require fresh", input: func(spec models.JobSpec) models.JobSpec { spec.Inputs[0].RequireFresh = true; return spec }},
		{name: "multiple inputs", input: func(spec models.JobSpec) models.JobSpec {
			spec.Inputs = append(spec.Inputs, models.InputSpec{DatasetRID: "ri.foundry.main.dataset." + uuid.NewString()})
			return spec
		}},
		{name: "input equals output", input: func(spec models.JobSpec) models.JobSpec { spec.Inputs[0].DatasetRID = out; return spec }},
		{name: "missing version", mutdb: func(p *batchedPayload) { p.Input.Version = 0 }},
		{name: "negative version", mutdb: func(p *batchedPayload) { p.Input.Version = -1 }},
		{name: "branch mismatch", mutdb: func(p *batchedPayload) { p.Input.Branch = "develop" }},
		{name: "branch missing", mutdb: func(p *batchedPayload) { p.Input.Branch = "" }},
		{name: "path plus all files", mutdb: func(p *batchedPayload) { p.Input.Path = "a.ndjson"; p.Input.AllFiles = true }},
		{name: "neither path nor all files", mutdb: func(p *batchedPayload) { p.Input.AllFiles = false }},
		{name: "non canonical path", mutdb: func(p *batchedPayload) { p.Input.AllFiles = false; p.Input.Path = "../escape.ndjson" }},
		{name: "blank predicate", mutdb: func(p *batchedPayload) { p.Input.Predicate = "   " }},
		{name: "overlong predicate", mutdb: func(p *batchedPayload) { p.Input.Predicate = strings.Repeat("x", 257) }},
		{name: "logical path suffix", mutdb: func(p *batchedPayload) { p.Output.LogicalPath = "part.csv" }},
		{name: "reserved transactions subtree", mutdb: func(p *batchedPayload) { p.Output.LogicalPath = "transactions/abc/out.ndjson" }},
	}
	for _, tc := range rejections {
		t.Run(tc.name, func(t *testing.T) {
			spec := batchedGrammarSpec(t, tc.mutdb, in, out)
			if tc.spec != nil {
				spec = tc.spec(spec)
			}
			if tc.input != nil {
				spec = tc.input(spec)
			}
			specs := []models.JobSpec{spec}
			if tc.specs != nil {
				specs = tc.specs(spec)
			}
			args := baseArgs()
			if tc.args != nil {
				args = tc.args(args)
			}
			body := baseBody(out)
			if tc.body != nil {
				body = tc.body(body)
			}
			cfg, err := parseBatchedSpec(specs, body, args, uuid.New())
			require.Error(t, err)
			require.Nil(t, cfg)
		})
	}
}

// ---------------------------------------------------------------------------
// Orchestration harness.
// ---------------------------------------------------------------------------

type countingSpecRepo struct {
	mu    sync.Mutex
	specs []models.JobSpec // served in order; last repeated
	calls int
}

func (r *countingSpecRepo) Lookup(_ context.Context, _, _output, _branch string, _ []string) (*models.JobSpec, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if len(r.specs) == 0 {
		return nil, nil
	}
	spec := r.specs[0]
	if r.calls > 1 && len(r.specs) > 1 {
		spec = r.specs[1]
	}
	return &spec, nil
}

type batchedHarness struct {
	t           *testing.T
	dvs         *fakeDatasetService
	store       *fakeBatchedStore
	versioning  *batchedStubVersioning
	specRepo    *countingSpecRepo
	actor       uuid.UUID
	org         uuid.UUID
	inputRID    string
	outputRID   string
	pipelineRID string
	restore     []func()
}

func newBatchedHarness(t *testing.T, records [][]byte) *batchedHarness {
	t.Helper()
	h := &batchedHarness{
		t:           t,
		dvs:         newFakeBatchedDVS(t, records),
		store:       newFakeBatchedStore(),
		versioning:  newBatchedStubVersioning("main"),
		actor:       uuid.New(),
		org:         uuid.New(),
		inputRID:    "ri.foundry.main.dataset." + uuid.NewString(),
		outputRID:   "ri.foundry.main.dataset." + uuid.NewString(),
		pipelineRID: "ri.foundry.main.pipeline." + uuid.NewString(),
	}
	spec := batchedGrammarSpec(t, nil, h.inputRID, h.outputRID)
	h.specRepo = &countingSpecRepo{specs: []models.JobSpec{spec}}
	h.restore = append(h.restore,
		SetPipelineAuthoringRepository(&anyPipelineAuthoringRepo{}),
		SetBuildLifecyclePorts(BuildLifecyclePorts{JobSpecs: h.specRepo, Versioning: h.versioning, Locks: stubLockRepo{}, Builds: h.store}),
		SetExecutionPorts(ExecutionPorts{Plans: h.store, Parallelism: 1}),
		SetBatchedDatasetClient(BatchedClientConfig{BaseURL: h.dvs.server.URL, ScanPageRows: 2, StageTimeout: 5 * time.Second, LeaseTTL: time.Minute}),
	)
	t.Cleanup(func() {
		for _, restore := range h.restore {
			restore()
		}
	})
	return h
}

func (h *batchedHarness) request(method, path, body string, sub uuid.UUID) *http.Request {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r = r.WithContext(authmw.ContextWithClaims(r.Context(), &authmw.Claims{Sub: sub, OrgID: &h.org}))
	r.Header.Set("Authorization", "Bearer test-token-"+sub.String())
	return r
}

func (h *batchedHarness) createBuild() (uuid.UUID, *httptest.ResponseRecorder) {
	h.t.Helper()
	body := `{"pipeline_rid":"` + h.pipelineRID + `","build_branch":"main","output_dataset_rids":["` + h.outputRID + `"]}`
	rr := httptest.NewRecorder()
	CreateBuild(rr, h.request(http.MethodPost, "/api/v1/builds", body, h.actor))
	require.Equal(h.t, http.StatusAccepted, rr.Code, rr.Body.String())
	var envelope struct {
		BuildID            uuid.UUID                  `json:"build_id"`
		OutputTransactions []models.OpenedTransaction `json:"output_transactions"`
	}
	require.NoError(h.t, json.NewDecoder(rr.Body).Decode(&envelope))
	require.Len(h.t, envelope.OutputTransactions, 1)
	return envelope.BuildID, rr
}

func (h *batchedHarness) execute(buildID, sub uuid.UUID) *httptest.ResponseRecorder {
	h.t.Helper()
	body := `{"build_id":"` + buildID.String() + `"}`
	rr := httptest.NewRecorder()
	ExecutePipeline(rr, h.request(http.MethodPost, "/api/v1/execute", body, sub))
	return rr
}

func (h *batchedHarness) abort(idOrRID string, sub uuid.UUID) *httptest.ResponseRecorder {
	h.t.Helper()
	rr := httptest.NewRecorder()
	AbortBuild(rr, h.request(http.MethodPost, "/api/v1/builds/"+idOrRID+"/abort", "", sub))
	return rr
}

func batchedFixture(n int) [][]byte {
	records := make([][]byte, n)
	for i := range records {
		records[i] = []byte(fmt.Sprintf(`{"id":%d,"payload":"row-%d"}`, i, i))
	}
	return records
}

// ---------------------------------------------------------------------------
// Canonical happy path.
// ---------------------------------------------------------------------------

func TestBatchedCanonicalHappyPath(t *testing.T) {
	records := batchedFixture(5)
	h := newBatchedHarness(t, records)
	buildID, _ := h.createBuild()

	// The reservation carries the build-bound marker; the legacy opener
	// was never used.
	require.Len(h.t, h.versioning.openedForBuild, 1)
	for _, providence := range h.versioning.openedForBuild {
		require.Equal(h.t, "wp33-staged-output", providence["purpose"])
		require.Equal(h.t, buildID.String(), providence["build_id"])
	}
	require.Zero(h.t, h.versioning.openLegacy)
	cfg, err := h.store.GetBatchedConfig(context.Background(), buildID.String())
	require.NoError(t, err)
	require.Equal(t, models.BatchedStagePending, cfg.Stage)
	require.NotEmpty(t, cfg.ReservationRID)

	rr := h.execute(buildID, h.actor)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var envelope struct {
		BuildID uuid.UUID             `json:"build_id"`
		State   string                `json:"state"`
		Nodes   map[string]string     `json:"nodes"`
		Batched *batchedExecuteDetail `json:"batched"`
	}
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&envelope))
	require.Equal(t, "BUILD_COMPLETED", envelope.State)
	require.Equal(t, "COMPLETED", envelope.Nodes["ri.foundry.main.job_spec.b1"])
	require.NotNil(t, envelope.Batched)
	require.Equal(t, int64(5), envelope.Batched.Records)

	// One commit, with the SAME stable key the stage descriptor sealed.
	require.Len(t, h.dvs.commits, 1)
	var commitBody struct {
		IdempotencyKey string `json:"idempotency_key"`
		ReservationID  string `json:"reservation_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(h.dvs.commits[0]), &commitBody))
	require.Equal(t, "wp33."+buildID.String(), commitBody.IdempotencyKey)
	require.Len(t, h.dvs.stageDescriptors, 1)
	for _, descriptor := range h.dvs.stageDescriptors {
		require.Equal(t, commitBody.IdempotencyKey, descriptor.key)
		require.True(t, descriptor.consumed)
	}
	reservationUUID := strings.TrimPrefix(cfg.ReservationRID, "ri.foundry.main.transaction.")
	require.Equal(t, reservationUUID, commitBody.ReservationID)

	// The digest over the streamed NDJSON matches the fixture exactly.
	hash := sha256.New()
	for _, record := range records {
		hash.Write(record)
		hash.Write([]byte{'\n'})
	}
	require.Equal(t, hex.EncodeToString(hash.Sum(nil)), envelope.Batched.SHA256)
	var expectedSize int64
	for _, record := range records {
		expectedSize += int64(len(record)) + 1
	}
	require.Equal(t, expectedSize, envelope.Batched.SizeBytes)

	// Published transaction differs from the reservation and lands in the
	// finalize record.
	require.NotEqual(t, cfg.ReservationRID, envelope.Batched.PublishedTxnRID)
	final := h.store.finalized
	require.Len(t, final, 1)
	require.Equal(t, buildID, final[0])
	require.Equal(t, "COMPLETED", h.store.jobs[buildID])
	require.False(t, h.store.locks[buildID])
	require.Empty(t, h.store.pending[buildID])
	require.Len(t, h.dvs.commitTxns, 1)

	// Terminal: re-execute refuses.
	rr = h.execute(buildID, h.actor)
	require.Equal(t, http.StatusConflict, rr.Code)
	require.Contains(t, rr.Body.String(), "batched_build_terminal")
}

// ---------------------------------------------------------------------------
// Bare-UUID JobSpec outputs: persistence compatibility (reservation must
// still bind; input!=output compares by UUID).
// ---------------------------------------------------------------------------

func TestBatchedBareUUIDSpecOutputCompatibility(t *testing.T) {
	h := newBatchedHarness(t, batchedFixture(3))
	// The spec declares BARE UUIDs; the CreateBuild request uses canonical
	// RIDs — every persisted match must go through the parsed UUID.
	bareInput := strings.TrimPrefix(h.inputRID, "ri.foundry.main.dataset.")
	bareOutput := strings.TrimPrefix(h.outputRID, "ri.foundry.main.dataset.")
	h.specRepo.mu.Lock()
	h.specRepo.specs[0].Inputs[0].DatasetRID = bareInput
	h.specRepo.specs[0].OutputDatasetRIDs = []string{bareOutput}
	h.specRepo.mu.Unlock()

	buildID, _ := h.createBuild()
	cfg, err := h.store.GetBatchedConfig(context.Background(), buildID.String())
	require.NoError(t, err)
	require.NotEmpty(t, cfg.ReservationRID, "a bare-UUID spec output still binds its reservation (UUID match, not string)")
	require.Equal(t, h.outputRID, cfg.OutputDatasetRID, "the frozen config stays canonical")
	require.NotEqual(t, cfg.InputDatasetRID, cfg.OutputDatasetRID)

	rr := h.execute(buildID, h.actor)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
}

// ---------------------------------------------------------------------------
// Exact-byte replay (ACK loss) — the核心 recovery protocol.
// ---------------------------------------------------------------------------

func TestBatchedReplayExactBytes(t *testing.T) {
	h := newBatchedHarness(t, batchedFixture(3))
	buildID, _ := h.createBuild()

	// First attempt: the service commits but the answer is a 503.
	failWith := http.StatusServiceUnavailable
	h.dvs.failCommitOnceWith = &failWith
	rr := h.execute(buildID, h.actor)
	require.Equal(t, http.StatusServiceUnavailable, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "batched_publish_outcome_unknown")
	cfg, err := h.store.GetBatchedConfig(context.Background(), buildID.String())
	require.NoError(t, err)
	require.Equal(t, models.BatchedStageOutcomeUnknown, cfg.Stage)
	require.NotEmpty(t, cfg.CommitIDempotencyKey)
	storedBody := append([]byte(nil), cfg.CommitRequestBody...)

	// Second attempt replays the PERSISTED bytes; the receipt answers.
	rr = h.execute(buildID, h.actor)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	cfg, err = h.store.GetBatchedConfig(context.Background(), buildID.String())
	require.NoError(t, err)
	require.Equal(t, models.BatchedStageDone, cfg.Stage)
	require.Equal(t, storedBody, cfg.CommitRequestBody, "replay must not re-marshal: bytes identical")
	// Exactly one real commit happened (the first); the second was a
	// receipt replay answered with the original bytes.
	require.Len(t, h.dvs.commits, 1)
	require.Len(t, h.dvs.receipts, 1)
}

// ---------------------------------------------------------------------------
// Actor binding.
// ---------------------------------------------------------------------------

func TestBatchedActorBinding(t *testing.T) {
	h := newBatchedHarness(t, batchedFixture(2))
	buildID, _ := h.createBuild()

	other := uuid.New()
	rr := h.execute(buildID, other)
	require.Equal(t, http.StatusForbidden, rr.Code)
	require.Contains(t, rr.Body.String(), "batched_build_actor_mismatch")

	rr = h.abort(buildID.String(), other)
	require.Equal(t, http.StatusForbidden, rr.Code)
	require.Contains(t, rr.Body.String(), "batched_build_actor_mismatch")
}

func TestBatchedCreateBuildRequiresActor(t *testing.T) {
	h := newBatchedHarness(t, batchedFixture(2))
	body := `{"pipeline_rid":"` + h.pipelineRID + `","build_branch":"main","output_dataset_rids":["` + h.outputRID + `"]}`
	// No claims: unscoped, no subject — the batched opt-in refuses.
	rr := httptest.NewRecorder()
	CreateBuild(rr, httptest.NewRequest(http.MethodPost, "/api/v1/builds", strings.NewReader(body)))
	require.Equal(t, http.StatusForbidden, rr.Code)
	require.Contains(t, rr.Body.String(), "batched_actor_required")
}

// ---------------------------------------------------------------------------
// Inline rejection + freeze (mapping fork) defense.
// ---------------------------------------------------------------------------

func TestBatchedInlineExecuteRejected(t *testing.T) {
	h := newBatchedHarness(t, batchedFixture(2))
	buildID, _ := h.createBuild()
	body := `{"build_id":"` + buildID.String() + `","nodes":[{"id":"n1"}]}`
	rr := httptest.NewRecorder()
	ExecutePipeline(rr, h.request(http.MethodPost, "/api/v1/execute", body, h.actor))
	require.Equal(t, http.StatusBadRequest, rr.Code)
	require.Contains(t, rr.Body.String(), "batched_inline_not_supported")
}

func TestBatchedFreezeRejectsRepublishDuringResolve(t *testing.T) {
	h := newBatchedHarness(t, batchedFixture(2))
	// The first Lookup (CreateBuild preflight) sees the original spec; the
	// second (ResolveBuild's internal preflight) sees a republished one —
	// the freeze hook must fail the resolution before any reservation.
	mutated := batchedGrammarSpec(t, func(p *batchedPayload) { p.Input.Version = 4 }, h.inputRID, h.outputRID)
	mutated.ContentHash = "hash-2"
	h.specRepo.specs = []models.JobSpec{
		{
			RID: "ri.foundry.main.job_spec.b1", PipelineRID: "ri.foundry.main.pipeline." + uuid.NewString(), BranchName: "main",
			Inputs: []models.InputSpec{{DatasetRID: h.inputRID}}, OutputDatasetRIDs: []string{h.outputRID},
			LogicKind: "TRANSFORM", LogicPayload: mustJSON(t, batchedPayload{
				TransformType: "batched_dataset_copy",
				Input:         batchedPayloadInput{Version: 3, Branch: "main", AllFiles: true},
			}), ContentHash: "hash-1",
		},
		mutated,
	}
	body := `{"pipeline_rid":"` + h.pipelineRID + `","build_branch":"main","output_dataset_rids":["` + h.outputRID + `"]}`
	rr := httptest.NewRecorder()
	CreateBuild(rr, h.request(http.MethodPost, "/api/v1/builds", body, h.actor))
	require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "batched_spec_redefined")
	// Zero reservations, and the marker row is terminal (not PREPARING).
	require.Empty(t, h.versioning.openedForBuild)
	for _, cfg := range h.store.configs {
		require.Equal(t, models.BatchedStageFailed, cfg.Stage)
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return raw
}

// ---------------------------------------------------------------------------
// Abort matrix.
// ---------------------------------------------------------------------------

func TestBatchedAbortMatrix(t *testing.T) {
	t.Run("preparing conflicts", func(t *testing.T) {
		h := newBatchedHarness(t, batchedFixture(1))
		// Simulate the window between OpenBatchedPrepare and resolution.
		buildID := uuid.New()
		require.NoError(t, h.store.OpenBatchedPrepare(context.Background(), resolver.ResolveBuildArgs{}, buildID, &models.BatchedBuildConfig{OutputDatasetRID: h.outputRID, OutputBranch: "main", ActorSubject: h.actor}))
		rr := h.abort(buildID.String(), h.actor)
		require.Equal(t, http.StatusConflict, rr.Code)
		require.Contains(t, rr.Body.String(), "batched_build_preparing")
	})

	t.Run("flagged defers to the in-flight attempt", func(t *testing.T) {
		h := newBatchedHarness(t, batchedFixture(1))
		buildID, _ := h.createBuild()
		owner := uuid.New()
		acquired, err := h.store.AcquireBatchedRun(context.Background(), buildID, owner, time.Minute)
		require.NoError(t, err)
		_ = acquired
		rr := h.abort(buildID.String(), h.actor)
		require.Equal(t, http.StatusOK, rr.Code)
		require.Contains(t, rr.Body.String(), "BUILD_ABORTING")
		cfg, err := h.store.GetBatchedConfig(context.Background(), buildID.String())
		require.NoError(t, err)
		require.True(t, cfg.AbortRequested, "abort recorded for the lease holder")
	})

	t.Run("idle build cancels precisely via takeover", func(t *testing.T) {
		h := newBatchedHarness(t, batchedFixture(1))
		buildID, _ := h.createBuild()
		cfg, err := h.store.GetBatchedConfig(context.Background(), buildID.String())
		require.NoError(t, err)
		rr := h.abort(buildID.String(), h.actor)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		require.Contains(t, rr.Body.String(), "BUILD_ABORTED")
		require.Contains(t, h.versioning.abortedTargets, h.outputRID+"|main|"+cfg.ReservationRID, "exactly this reservation was aborted")
		cfg, err = h.store.GetBatchedConfig(context.Background(), buildID.String())
		require.NoError(t, err)
		require.Equal(t, models.BatchedStageAborted, cfg.Stage)
		require.Empty(t, h.store.pending[buildID])
		require.False(t, h.store.locks[buildID])
	})

	t.Run("publishing intent always conflicts", func(t *testing.T) {
		h := newBatchedHarness(t, batchedFixture(1))
		buildID, _ := h.createBuild()
		owner := uuid.New()
		_, err := h.store.AcquireBatchedRun(context.Background(), buildID, owner, time.Minute)
		require.NoError(t, err)
		require.NoError(t, h.store.MarkBatchedStaged(context.Background(), buildID, owner, "obj", "sha", 1, 1))
		require.NoError(t, h.store.PersistBatchedCommitIntent(context.Background(), buildID, owner, "wp33."+buildID.String(), []byte(`{}`)))
		rr := h.abort(buildID.String(), h.actor)
		require.Equal(t, http.StatusConflict, rr.Code)
		require.Contains(t, rr.Body.String(), "batched_publish_outcome_unknown")
		cfg, err := h.store.GetBatchedConfig(context.Background(), buildID.String())
		require.NoError(t, err)
		require.Empty(t, h.dvs.aborted, "nothing was aborted at the dataset side")
		require.Equal(t, models.BatchedStagePublishing, cfg.Stage)
	})

	t.Run("done conflicts", func(t *testing.T) {
		h := newBatchedHarness(t, batchedFixture(1))
		buildID, _ := h.createBuild()
		rr := h.execute(buildID, h.actor)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		rr = h.abort(buildID.String(), h.actor)
		require.Equal(t, http.StatusConflict, rr.Code)
	})

	t.Run("failed build retries the pending cancel and keeps FAILED", func(t *testing.T) {
		h := newBatchedHarness(t, batchedFixture(1))
		buildID, _ := h.createBuild()
		cfg, err := h.store.GetBatchedConfig(context.Background(), buildID.String())
		require.NoError(t, err)
		// A CreateBuild failure left the config without a reservation but
		// the durable pending reference still holds the real one.
		pendingTxn := "ri.foundry.main.transaction." + uuid.NewString()
		h.store.mu.Lock()
		h.store.configs[buildID].ReservationRID = ""
		h.store.configs[buildID].Stage = models.BatchedStageFailed
		h.store.pending[buildID] = []PendingOutputTxn{{DatasetRID: h.outputRID, Branch: "main", TransactionRID: pendingTxn}}
		h.store.mu.Unlock()
		_ = cfg
		rr := h.abort(buildID.String(), h.actor)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		require.Contains(t, rr.Body.String(), "BUILD_FAILED")
		require.Contains(t, h.versioning.abortedTargets, h.outputRID+"|main|"+pendingTxn,
			"the pending reference — not the empty config field — drove the cancel")
		require.Empty(t, h.store.pending[buildID])
	})
}

// ---------------------------------------------------------------------------
// Cancellation during staging: abort path, never FAILED.
// ---------------------------------------------------------------------------

func TestBatchedStageCancellationRoutesToAbort(t *testing.T) {
	h := newBatchedHarness(t, batchedFixture(4))
	buildID, _ := h.createBuild()

	// The abort flag is recorded (as AbortBuild would) and the in-flight
	// execution is canceled from the scan page handler — the exact
	// sequence a concurrent abort produces.
	var abortFlagDrawn bool
	h.dvs.scanBehavior = func(r *http.Request) (int, []byte) {
		if !abortFlagDrawn {
			abortFlagDrawn = true
			h.store.mu.Lock()
			h.store.configs[buildID].AbortRequested = true
			h.store.mu.Unlock()
			cancelExecution(buildID)
			time.Sleep(50 * time.Millisecond)
		}
		return http.StatusOK, []byte(`{"dataset_id":"` + strings.TrimPrefix(h.inputRID, "ri.foundry.main.dataset.") + `","branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111","rows":[],"row_count":0,"next_cursor":null}`)
	}
	rr := h.execute(buildID, h.actor)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "BUILD_ABORTED")
	cfg, err := h.store.GetBatchedConfig(context.Background(), buildID.String())
	require.NoError(t, err)
	require.Equal(t, models.BatchedStageAborted, cfg.Stage, "canceled staging is an ABORT, not a failure")
	require.Empty(t, h.dvs.commits, "nothing was published")
}

func TestBatchedCallerDisconnectReleasesLeaseForRetry(t *testing.T) {
	h := newBatchedHarness(t, batchedFixture(4))
	buildID, _ := h.createBuild()
	var canceled bool
	h.dvs.scanBehavior = func(r *http.Request) (int, []byte) {
		if !canceled {
			canceled = true
			cancelExecution(buildID) // no abort flag: a bare disconnect
			time.Sleep(50 * time.Millisecond)
		}
		return http.StatusOK, []byte(`{"dataset_id":"` + strings.TrimPrefix(h.inputRID, "ri.foundry.main.dataset.") + `","branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111","rows":[],"row_count":0,"next_cursor":null}`)
	}
	rr := h.execute(buildID, h.actor)
	require.Equal(t, http.StatusServiceUnavailable, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "batched_stage_interrupted")
	cfg, err := h.store.GetBatchedConfig(context.Background(), buildID.String())
	require.NoError(t, err)
	require.Equal(t, models.BatchedStageStaging, cfg.Stage, "no terminal state: retryable")
	require.Nil(t, cfg.LeaseOwner, "lease released for the next attempt")
}

// ---------------------------------------------------------------------------
// Scan page negatives: page validation, truncation, zero publish.
// ---------------------------------------------------------------------------

func TestBatchedScanBindingAndCursorNegatives(t *testing.T) {
	records := batchedFixture(4)
	h := newBatchedHarness(t, records)
	client, err := newBatchedDatasetClient()
	require.NoError(t, err)
	ctx := contextWithCallerBearer(context.Background(), "bearer")
	input := batchedScanInput{DatasetRID: h.inputRID, Branch: "main", Version: 3}

	t.Run("page binding mismatch fails the scan", func(t *testing.T) {
		h.dvs.scanBehavior = func(r *http.Request) (int, []byte) {
			return http.StatusOK, []byte(`{"dataset_id":"` + uuid.NewString() + `","branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111","rows":[],"row_count":0,"next_cursor":null}`)
		}
		_, err := client.ScanStream(ctx, input, func([]byte) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "batched_scan_binding_invalid")
	})

	t.Run("transaction must stay pinned across pages", func(t *testing.T) {
		page := 0
		h.dvs.scanBehavior = func(r *http.Request) (int, []byte) {
			page++
			txn := "11111111-1111-1111-1111-111111111111"
			cursor := `,"next_cursor":"off2"`
			if page > 1 {
				txn = "33333333-3333-3333-3333-333333333333"
				cursor = `,"next_cursor":null`
			}
			return http.StatusOK, []byte(`{"dataset_id":"` + strings.TrimPrefix(h.inputRID, "ri.foundry.main.dataset.") + `","branch":"main","version":3,"transaction_id":"` + txn + `","rows":[{"a":1}],"row_count":1` + cursor + `}`)
		}
		_, err := client.ScanStream(ctx, input, func([]byte) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "batched_scan_binding_invalid")
	})

	t.Run("stalled cursor fails explicitly", func(t *testing.T) {
		h.dvs.scanBehavior = func(r *http.Request) (int, []byte) {
			return http.StatusOK, []byte(`{"dataset_id":"` + strings.TrimPrefix(h.inputRID, "ri.foundry.main.dataset.") + `","branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111","rows":[{"a":1}],"row_count":1,"next_cursor":"off2"}`)
		}
		_, err := client.ScanStream(ctx, input, func([]byte) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "batched_scan_cursor_stalled")
	})

	t.Run("row count mismatch after rows fails the page", func(t *testing.T) {
		h.dvs.scanBehavior = func(r *http.Request) (int, []byte) {
			return http.StatusOK, []byte(`{"dataset_id":"` + strings.TrimPrefix(h.inputRID, "ri.foundry.main.dataset.") + `","branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111","rows":[{"a":1},{"b":2}],"row_count":5,"next_cursor":null}`)
		}
		_, err := client.ScanStream(ctx, input, func([]byte) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "batched_scan_row_count_mismatch")
	})

	t.Run("hard response bound is enforced", func(t *testing.T) {
		restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: h.dvs.server.URL, ScanMaxResponseBytes: 64, StageTimeout: time.Second, LeaseTTL: time.Minute})
		defer restore()
		bounded, err := newBatchedDatasetClient()
		require.NoError(t, err)
		h.dvs.scanBehavior = func(r *http.Request) (int, []byte) {
			return http.StatusOK, []byte(`{"dataset_id":"` + strings.TrimPrefix(h.inputRID, "ri.foundry.main.dataset.") + `","branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111","rows":[],"row_count":0,"next_cursor":null,"pad":"` + strings.Repeat("x", 200) + `"}`)
		}
		_, err = bounded.ScanStream(ctx, input, func([]byte) error { return nil })
		require.Error(t, err)
	})

	t.Run("redirects are refused", func(t *testing.T) {
		redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", "http://evil.invalid/x")
			w.WriteHeader(http.StatusFound)
		}))
		defer redirector.Close()
		restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: redirector.URL, StageTimeout: time.Second, LeaseTTL: time.Minute})
		defer restore()
		refusing, err := newBatchedDatasetClient()
		require.NoError(t, err)
		_, err = refusing.ScanStream(ctx, input, func([]byte) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "302")
	})
}

func TestBatchedPageFailureAfterRowsTruncatesStage(t *testing.T) {
	// B11 control: rows of page 1 stream into the stage request; page 2
	// arrives with a mismatched binding — the writer is closed with the
	// error, the upload truncates, NOTHING seals and nothing publishes.
	h := newBatchedHarness(t, batchedFixture(4))
	buildID, _ := h.createBuild()
	page := 0
	h.dvs.scanBehavior = func(r *http.Request) (int, []byte) {
		page++
		if page == 1 {
			return http.StatusOK, []byte(`{"dataset_id":"` + strings.TrimPrefix(h.inputRID, "ri.foundry.main.dataset.") + `","branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111","rows":[{"a":1},{"a":2}],"row_count":2,"next_cursor":"off2"}`)
		}
		return http.StatusOK, []byte(`{"dataset_id":"` + uuid.NewString() + `","branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111","rows":[],"row_count":0,"next_cursor":null}`)
	}
	rr := h.execute(buildID, h.actor)
	require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "batched_stage_failed")
	require.Empty(t, h.dvs.stageDescriptors, "no descriptor was sealed")
	require.Empty(t, h.dvs.commits, "nothing published")
	cfg, err := h.store.GetBatchedConfig(context.Background(), buildID.String())
	require.NoError(t, err)
	require.Equal(t, models.BatchedStageFailed, cfg.Stage)
}

func TestBatchedStageReceiptBindingMismatchFails(t *testing.T) {
	h := newBatchedHarness(t, batchedFixture(2))
	buildID, _ := h.createBuild()
	h.dvs.stageReceiptMutator = func(receipt map[string]any) {
		receipt["branch"] = "other-branch"
	}
	rr := h.execute(buildID, h.actor)
	require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "batched_stage_receipt_mismatch")
	cfg, err := h.store.GetBatchedConfig(context.Background(), buildID.String())
	require.NoError(t, err)
	require.Equal(t, models.BatchedStageFailed, cfg.Stage)
	require.Empty(t, h.dvs.commits)
}

func TestBatchedTakeoverHonorsPersistedCancel(t *testing.T) {
	// Process death left an expired lease AND a persisted abort request:
	// the takeover completes the exact cancel — it must never re-stage or
	// re-commit.
	h := newBatchedHarness(t, batchedFixture(3))
	buildID, _ := h.createBuild()
	h.store.mu.Lock()
	cfg := h.store.configs[buildID]
	expired := time.Now().Add(-time.Minute)
	cfg.LeaseExpiresAt = &expired
	cfg.AbortRequested = true
	h.store.mu.Unlock()

	rr := h.execute(buildID, h.actor)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "BUILD_ABORTED")
	require.Zero(t, h.dvs.stageCalls, "no stage upload ran")
	require.Empty(t, h.dvs.commits, "nothing was published")
	final, err := h.store.GetBatchedConfig(context.Background(), buildID.String())
	require.NoError(t, err)
	require.Equal(t, models.BatchedStageAborted, final.Stage)
}

func TestBatchedFailedWithLiveLeaseDefersToActiveOwner(t *testing.T) {
	h := newBatchedHarness(t, batchedFixture(1))
	buildID, _ := h.createBuild()
	owner := uuid.New()
	_, err := h.store.AcquireBatchedRun(context.Background(), buildID, owner, time.Minute)
	require.NoError(t, err)
	h.store.mu.Lock()
	h.store.configs[buildID].Stage = models.BatchedStageFailed // failing attempt still cleaning up
	h.store.mu.Unlock()

	rr := h.abort(buildID.String(), h.actor)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "BUILD_FAILED")
	require.Contains(t, rr.Body.String(), "still holds the cleanup lease")
	h.store.mu.Lock()
	stillOwner := h.store.configs[buildID].LeaseOwner != nil && *h.store.configs[buildID].LeaseOwner == owner
	h.store.mu.Unlock()
	require.True(t, stillOwner, "the abort must not hand out or steal the live cleanup fence")
	require.Empty(t, h.versioning.abortedTargets, "no remote cancel was issued by the abort caller")
}

// ---------------------------------------------------------------------------
// Review-round controls: error wire, scan tail bounds, early rejection.
// ---------------------------------------------------------------------------

func TestBatchedErrorEnvelopeParsing(t *testing.T) {
	t.Run("machine code from the code field, description from error", func(t *testing.T) {
		err := batchedErrorFromResponse(http.StatusConflict, []byte(`{"error":"another upload is in flight","code":"STAGED_UPLOAD_IN_FLIGHT"}`))
		require.Equal(t, "STAGED_UPLOAD_IN_FLIGHT", err.Code)
		require.Equal(t, "another upload is in flight", err.Detail)
		require.Contains(t, err.Error(), "batched_dataset_409_STAGED_UPLOAD_IN_FLIGHT")
	})
	t.Run("legacy envelope without a code still parses", func(t *testing.T) {
		err := batchedErrorFromResponse(http.StatusBadRequest, []byte(`{"error":"branch is required"}`))
		require.Empty(t, err.Code)
		require.Equal(t, "branch is required", err.Detail)
	})
	t.Run("plain message fallback", func(t *testing.T) {
		err := batchedErrorFromResponse(http.StatusBadGateway, []byte(`{"message":"upstream down"}`))
		require.Equal(t, "upstream down", err.Detail)
	})
}

func TestBatchedScanTailBounds(t *testing.T) {
	h := newBatchedHarness(t, batchedFixture(1))
	ctx := contextWithCallerBearer(context.Background(), "bearer")
	input := batchedScanInput{DatasetRID: h.inputRID, Branch: "main", Version: 3}
	uuidPart := strings.TrimPrefix(h.inputRID, "ri.foundry.main.dataset.")

	newClient := func(maxBytes int64) batchedDatasetClient {
		restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: h.dvs.server.URL, ScanMaxResponseBytes: maxBytes, StageTimeout: time.Second, LeaseTTL: time.Minute})
		defer restore()
		client, err := newBatchedDatasetClient()
		require.NoError(t, err)
		return client
	}

	t.Run("second JSON value after the page object is rejected", func(t *testing.T) {
		h.dvs.scanBehavior = func(r *http.Request) (int, []byte) {
			return http.StatusOK, []byte(`{"dataset_id":"` + uuidPart + `","branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111","rows":[],"row_count":0,"next_cursor":null} {"evil":1}`)
		}
		_, err := newClient(1<<20).ScanStream(ctx, input, func([]byte) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "trailing content")
	})

	t.Run("syntax-error tail is rejected", func(t *testing.T) {
		h.dvs.scanBehavior = func(r *http.Request) (int, []byte) {
			return http.StatusOK, []byte(`{"dataset_id":"` + uuidPart + `","branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111","rows":[],"row_count":0,"next_cursor":null} truncated-garbage`)
		}
		_, err := newClient(1<<20).ScanStream(ctx, input, func([]byte) error { return nil })
		require.Error(t, err)
	})

	t.Run("whitespace padding past the hard bound is rejected", func(t *testing.T) {
		h.dvs.scanBehavior = func(r *http.Request) (int, []byte) {
			page := `{"dataset_id":"` + uuidPart + `","branch":"main","version":3,"transaction_id":"11111111-1111-1111-1111-111111111111","rows":[],"row_count":0,"next_cursor":null}`
			return http.StatusOK, append([]byte(page), bytes.Repeat([]byte(" "), 4096)...)
		}
		_, err := newClient(2048).ScanStream(ctx, input, func([]byte) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "batched_scan_response_too_large")
	})
}

func TestBatchedStageEarlyRejectionPreservesMachineCode(t *testing.T) {
	h := newBatchedHarness(t, batchedFixture(50))
	buildID, _ := h.createBuild()
	// The service rejects the upload IMMEDIATELY while the client would
	// still be streaming pages: the feed must unwind, the machine code
	// must survive as the primary error, and nothing may seal.
	h.dvs.stageEarlyReject = &struct {
		Status int
		Body   string
	}{Status: http.StatusConflict, Body: `{"error":"another upload is in flight","code":"STAGED_UPLOAD_IN_FLIGHT"}`}
	start := time.Now()
	rr := h.execute(buildID, h.actor)
	elapsed := time.Since(start)
	require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "STAGED_UPLOAD_IN_FLIGHT", "the DVS machine code is the primary verdict")
	require.Less(t, elapsed, 3*time.Second, "an early rejection unwinds the feed immediately")
	require.Empty(t, h.dvs.stageDescriptors)
	cfg, err := h.store.GetBatchedConfig(context.Background(), buildID.String())
	require.NoError(t, err)
	require.Equal(t, models.BatchedStageFailed, cfg.Stage)
}

// ---------------------------------------------------------------------------
// 3C real-fault regression: deterministic unwind on abrupt socket close.
// ---------------------------------------------------------------------------

func TestBatchedStageAbruptSocketCloseUnwinds(t *testing.T) {
	// C5b shape: the proxy accepts the upload, consumes exactly 1MiB, then
	// drops the connection with no response. The client must return
	// promptly with a network error — the historical dual-receiver result
	// handoff deadlocked exactly here (STAGING forever, no HTTP answer).
	abrupt := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.CopyN(io.Discard, r.Body, 1<<20); err != nil {
			return
		}
		panic(http.ErrAbortHandler) // hard connection abort, no response
	}))
	defer abrupt.Close()

	restore := SetBatchedDatasetClient(BatchedClientConfig{
		BaseURL:      abrupt.URL,
		StageTimeout: 10 * time.Second, // generous: the test proves UNWIND, not timeout
		LeaseTTL:     time.Minute,
	})
	defer restore()
	client, err := newBatchedDatasetClient()
	require.NoError(t, err)
	ctx := contextWithCallerBearer(context.Background(), "bearer")

	row := bytes.Repeat([]byte(`{"k":"`), 1) // 4KiB-ish rows built below
	_ = row
	bigRow := []byte(`{"pad":"` + strings.Repeat("x", 4064) + `"}`)
	start := time.Now()
	_, err = client.StageStream(ctx, batchedStageRequest{
		DatasetRID:     "ri.foundry.main.dataset." + uuid.NewString(),
		Branch:         "main",
		ReservationRID: "ri.foundry.main.transaction." + uuid.NewString(),
		BuildID:        uuid.New(),
		LogicalPath:    "part-00000.ndjson",
		IdempotencyKey: "wp33." + uuid.NewString(),
	}, func(_ context.Context, w *bufio.Writer) error {
		// ~6MiB: far past the 1MiB the server consumes — the producer is
		// guaranteed mid-flight when the socket dies.
		for i := 0; i < 1536; i++ {
			if _, err := w.Write(bigRow); err != nil {
				return err
			}
			if err := w.WriteByte('\n'); err != nil {
				return err
			}
		}
		return nil
	})
	elapsed := time.Since(start)
	require.Error(t, err)
	require.Contains(t, err.Error(), "batched_stage_failed")
	require.Less(t, elapsed, 2*time.Second, "abrupt close must unwind in bounded time, not wait for the stage timeout")

	// No goroutine stuck: the producer's single result was consumed before
	// StageStream returned; a short settle must not grow the count.
	time.Sleep(150 * time.Millisecond)
	after := runtime.NumGoroutine()
	time.Sleep(250 * time.Millisecond)
	require.LessOrEqual(t, runtime.NumGoroutine(), after, "the producer goroutine must have exited with the call")
}

func TestBatchedStageProducerParkedCtxClassification(t *testing.T) {
	// The producer has written >0 bytes and is then parked on the producer
	// context (the shape of a blocked scan GET). The verdict must come
	// from the REQUEST result — never a bare context.Canceled that would
	// masquerade as a caller cancel or swallow the machine code.
	parkedRecord := []byte(`{"a":1}` + "\n")
	parkedFeed := func(ctx context.Context, w *bufio.Writer) error {
		if _, err := w.Write(parkedRecord); err != nil {
			return err
		}
		if err := w.Flush(); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	req := batchedStageRequest{
		DatasetRID:     "ri.foundry.main.dataset." + uuid.NewString(),
		Branch:         "main",
		ReservationRID: "ri.foundry.main.transaction." + uuid.NewString(),
		BuildID:        uuid.New(),
		LogicalPath:    "part-00000.ndjson",
		IdempotencyKey: "wp33." + uuid.NewString(),
	}
	ctx := contextWithCallerBearer(context.Background(), "bearer")

	t.Run("early 409 keeps its machine code", func(t *testing.T) {
		rejector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// HTTP/1.1 drains the unread request body BEFORE responding by
			// default — without full duplex the "early" 409 never reaches
			// the parked producer and the fixture deadlocks itself.
			rc := http.NewResponseController(w)
			require.NoError(t, rc.EnableFullDuplex())
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"another upload is in flight","code":"STAGED_UPLOAD_IN_FLIGHT"}`))
			require.NoError(t, rc.Flush())
		}))
		defer rejector.Close()
		restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: rejector.URL, StageTimeout: 10 * time.Second, LeaseTTL: time.Minute})
		defer restore()
		client, err := newBatchedDatasetClient()
		require.NoError(t, err)
		start := time.Now()
		_, err = client.StageStream(ctx, req, parkedFeed)
		require.Error(t, err)
		require.Less(t, time.Since(start), 2*time.Second)
		require.Contains(t, err.Error(), "STAGED_UPLOAD_IN_FLIGHT", "the DVS machine code is the verdict")
		require.NotContains(t, err.Error(), "context canceled", "our teardown must not surface as a caller cancel")
		require.False(t, errors.Is(err, context.Canceled))
	})

	t.Run("socket cut is a network failure, never a caller cancel", func(t *testing.T) {
		abrupt := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Consume exactly the fixed record the parked producer flushed
			// (>0 bytes written) and cut — waiting for MORE body would turn
			// the fixture into the deadlock class it guards against.
			got := make([]byte, len(parkedRecord))
			if _, err := io.ReadFull(r.Body, got); err != nil {
				return
			}
			panic(http.ErrAbortHandler)
		}))
		defer abrupt.Close()
		// A PARKED producer leaves the transport wedged on the unread
		// pipe after the cut — nothing flows and only the global stage
		// bound can settle the attempt. That bound must classify as a
		// deadline (NEVER a caller cancel).
		restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: abrupt.URL, StageTimeout: 2 * time.Second, LeaseTTL: time.Minute})
		defer restore()
		client, err := newBatchedDatasetClient()
		require.NoError(t, err)
		start := time.Now()
		_, err = client.StageStream(ctx, req, parkedFeed)
		require.Error(t, err)
		require.Less(t, time.Since(start), 3*time.Second)
		require.Contains(t, err.Error(), "batched_stage_failed")
		require.False(t, errors.Is(err, context.Canceled), "a socket cut must not be misjudged as a caller cancellation (that would strand STAGING)")
	})

	t.Run("a genuine caller cancel still surfaces as canceled", func(t *testing.T) {
		blocker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Drain the body SYNCHRONOUSLY: only a read (or its failure on
			// client disconnect) makes the HTTP/1 server notice the peer is
			// gone — parking on r.Context().Done() alone would hang
			// httptest.Server.Close forever.
			_, _ = io.Copy(io.Discard, r.Body)
		}))
		defer blocker.Close()
		restore := SetBatchedDatasetClient(BatchedClientConfig{BaseURL: blocker.URL, StageTimeout: 30 * time.Second, LeaseTTL: time.Minute})
		defer restore()
		client, err := newBatchedDatasetClient()
		require.NoError(t, err)
		callerCtx, callerCancel := context.WithCancel(ctx)
		go func() {
			time.Sleep(150 * time.Millisecond)
			callerCancel()
		}()
		start := time.Now()
		_, err = client.StageStream(callerCtx, req, parkedFeed)
		require.Error(t, err)
		require.Less(t, time.Since(start), 3*time.Second)
		require.True(t, errors.Is(err, context.Canceled), "the real caller cancel must remain classifiable for the interrupt path")
	})
}
