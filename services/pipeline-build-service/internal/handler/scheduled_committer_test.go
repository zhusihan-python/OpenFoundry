package handler

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type receiptTestStore struct {
	mu        sync.Mutex
	body      []byte
	completed int
}

func (s *receiptTestStore) AssertDispatchFence(context.Context, uuid.UUID, string, int64) error {
	return nil
}
func (s *receiptTestStore) RecordPendingPublishWithResult(_ context.Context, _ uuid.UUID, _ string, _ int64, _, _, _ string, b []byte, _ json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.body = append([]byte(nil), b...)
	return nil
}
func (s *receiptTestStore) MarkReceiptUnknown(context.Context, uuid.UUID, string, int64, string, string) error {
	return nil
}
func (s *receiptTestStore) CompleteScheduledPublish(context.Context, uuid.UUID, string, int64, string, json.RawMessage, string, string, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completed++
	return nil
}
func TestScheduledReplayUsesOriginalBytesAndBearer(t *testing.T) {
	ds := uuid.New()
	txn := uuid.New()
	calls := 0
	body := []byte(`{"original":"bytes","idempotency_key":"stable"}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		b, _ := io.ReadAll(r.Body)
		if string(b) != string(body) || r.Header.Get("Authorization") != "Bearer caller" {
			t.Error("replay bytes or identity changed")
		}
		if calls == 1 {
			w.WriteHeader(503)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"dataset_id": ds, "transaction": map[string]any{"id": txn, "status": "COMMITTED"}})
	}))
	defer server.Close()
	store := &receiptTestStore{}
	c := &ScheduledOutputCommitter{Store: store, BaseURL: server.URL, EventID: uuid.New()}
	ctx := ContextWithSchedulerBearer(context.Background(), "caller")
	if c.ReplayReceipt(ctx, "stable", "node", ds.String(), body, "") == nil {
		t.Fatal("lost acknowledgement accepted")
	}
	if err := c.ReplayReceipt(ctx, "stable", "node", ds.String(), body, ""); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || store.completed != 1 {
		t.Fatal("replay did not reconcile once")
	}
}

func TestScheduledTwoDatasetsUseIndependentKeys(t *testing.T) {
	keys := map[string]bool{}
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		keys[b["idempotency_key"].(string)] = true
		mu.Unlock()
		parts := strings.Split(r.URL.Path, "/")
		id, _ := datasetUUIDFromRID(parts[3])
		json.NewEncoder(w).Encode(map[string]any{"dataset_id": id, "transaction": map[string]any{"id": uuid.New(), "status": "COMMITTED"}})
	}))
	defer server.Close()
	c := &ScheduledOutputCommitter{Store: &receiptTestStore{}, BaseURL: server.URL, EventID: uuid.New()}
	ctx := ContextWithSchedulerBearer(context.Background(), "caller")
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Commit(ctx, executor.OutputTransaction{DatasetRID: uuid.NewString(), OutputNodeID: "one-job", Branch: "main"}, executor.NodeResult{}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(keys) != 2 {
		t.Fatal("two dataset outputs collided")
	}
}

type successNode struct{}

func (successNode) Run(context.Context, executor.NodeContext) (executor.NodeResult, error) {
	return executor.NodeResult{}, nil
}

type noopScheduledAudit struct{}

func (noopScheduledAudit) Record(context.Context, executor.AuditEvent) error { return nil }

type abortProbe struct{ calls int }

func (p *abortProbe) Abort(context.Context, executor.OutputTransaction) error { p.calls++; return nil }
func (p *abortProbe) AbortTransaction(context.Context, string, string, string) error {
	p.calls++
	return nil
}
func TestScheduledExecutorUnknownPublishNeverCompensates(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	probe := &abortProbe{}
	store := &receiptTestStore{}
	c := &ScheduledOutputCommitter{Store: store, BaseURL: server.URL, EventID: uuid.New()}
	tx := executor.OutputTransaction{DatasetRID: uuid.NewString(), OutputNodeID: "node", Branch: "main"}
	plan := executor.Plan{BuildID: uuid.New(), BuildBranch: "main", Nodes: []executor.Node{{ID: "node", JobID: uuid.New(), Outputs: []executor.OutputTransaction{tx}}}}
	executor.Execute(ContextWithSchedulerBearer(context.Background(), "caller"), plan, successNode{}, &ScheduledTransactionManager{Aborter: probe, Metadata: probe}, c, noopScheduledAudit{})
	if len(store.body) == 0 || probe.calls != 0 {
		t.Fatal("unknown publish was compensated or not durably recorded")
	}
}
