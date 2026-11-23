//go:build integration

package runtime_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	databus "github.com/openfoundry/openfoundry-go/libs/event-bus-data"
	testingx "github.com/openfoundry/openfoundry-go/libs/testing"
	"github.com/openfoundry/openfoundry-go/services/ai-sink/internal/config"
	"github.com/openfoundry/openfoundry-go/services/ai-sink/internal/envelope"
	"github.com/openfoundry/openfoundry-go/services/ai-sink/internal/runtime"
)

type kafkaAIWriter struct {
	mu      sync.Mutex
	batches []map[string][]envelope.AiEventEnvelope
	err     error
}

func (w *kafkaAIWriter) Append(_ context.Context, byTable map[string][]envelope.AiEventEnvelope) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	cp := make(map[string][]envelope.AiEventEnvelope, len(byTable))
	for table, group := range byTable {
		if len(group) == 0 {
			continue
		}
		dup := make([]envelope.AiEventEnvelope, len(group))
		copy(dup, group)
		cp[table] = dup
	}
	w.batches = append(w.batches, cp)
	return nil
}
func (w *kafkaAIWriter) Close() error { return nil }
func (w *kafkaAIWriter) calls() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.batches)
}

func newKafkaSubscriber(t *testing.T, brokers []string, topic, group string) *databus.KafkaSubscriber {
	t.Helper()
	cfg := databus.NewConfig(brokers, databus.InsecureDev("ai-sink-test"))
	sub, err := databus.NewKafkaSubscriber(cfg, group, []string{topic})
	if err != nil {
		t.Fatalf("new kafka subscriber: %v", err)
	}
	return sub
}

// End the single-record run only after the broker acknowledges the commit.
type stopAfterCommit struct {
	*databus.KafkaSubscriber
	stop context.CancelFunc
}

func (s stopAfterCommit) CommitMessages(ctx context.Context, msgs []*databus.DataMessage) error {
	if err := s.KafkaSubscriber.CommitMessages(ctx, msgs); err != nil {
		return err
	}
	s.stop()
	return nil
}

func runAISinkOnce(t *testing.T, brokers []string, topic, group string, w *kafkaAIWriter) error {
	t.Helper()
	sub := newKafkaSubscriber(t, brokers, topic, group)
	defer sub.Close()
	cfg := &config.Config{BatchPolicy: config.BatchPolicy{MaxRecords: 1, MaxWait: 100 * time.Millisecond}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := runtime.Run(ctx, cfg, stopAfterCommit{sub, cancel}, w, runtime.NewMetrics(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("consumer did not complete within deadline")
	}
	return err
}

func TestRuntimeWithRealKafkaValidMalformedAndRetry(t *testing.T) {
	brokers := testingx.KafkaBrokers(t)

	t.Run("valid event writes and commits", func(t *testing.T) {
		topic := "of-ai-sink-valid-" + uuid.NewString()
		group := "of-ai-sink-" + uuid.NewString()
		testingx.CreateKafkaTopic(t, brokers, topic)
		body := mkBytes(t, envelope.KindPrompt, time.Now().Add(-time.Second).UnixMicro())
		testingx.ProduceKafkaMessage(t, brokers, topic, body)

		w := &kafkaAIWriter{}
		if err := runAISinkOnce(t, brokers, topic, group, w); err != nil {
			t.Fatalf("Run valid: %v", err)
		}
		if w.calls() != 1 {
			t.Fatalf("writer calls = %d, want 1", w.calls())
		}
		testingx.RequireKafkaGroupOffset(t, brokers, topic, group, 1)
	})

	t.Run("malformed event commits without writer", func(t *testing.T) {
		topic := "of-ai-sink-malformed-" + uuid.NewString()
		group := "of-ai-sink-" + uuid.NewString()
		testingx.CreateKafkaTopic(t, brokers, topic)
		body := []byte(`{"event_id":`)
		testingx.ProduceKafkaMessage(t, brokers, topic, body)

		w := &kafkaAIWriter{}
		if err := runAISinkOnce(t, brokers, topic, group, w); err != nil {
			t.Fatalf("Run malformed: %v", err)
		}
		if w.calls() != 0 {
			t.Fatalf("malformed event wrote %d batches", w.calls())
		}
		testingx.RequireKafkaGroupOffset(t, brokers, topic, group, 1)
	})

	t.Run("writer error leaves offset uncommitted for retry", func(t *testing.T) {
		topic := "of-ai-sink-retry-" + uuid.NewString()
		group := "of-ai-sink-" + uuid.NewString()
		testingx.CreateKafkaTopic(t, brokers, topic)
		body := mkBytes(t, envelope.KindPrompt, time.Now().Add(-time.Second).UnixMicro())
		testingx.ProduceKafkaMessage(t, brokers, topic, body)

		err := runAISinkOnce(t, brokers, topic, group, &kafkaAIWriter{err: errors.New("writer down")})
		if err == nil || !strings.Contains(err.Error(), "writer down") {
			t.Fatalf("expected writer error, got %v", err)
		}
		testingx.RequireKafkaReplay(t, brokers, topic, group, body)
	})
}
