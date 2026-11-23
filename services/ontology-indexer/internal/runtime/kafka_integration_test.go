//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	testingx "github.com/openfoundry/openfoundry-go/libs/testing"
)

type recordingIndexBackend struct {
	err    error
	calls  int
	topics []string
	events []json.RawMessage
}

func (b *recordingIndexBackend) Handle(_ context.Context, topic string, event json.RawMessage) error {
	b.calls++
	b.topics = append(b.topics, topic)
	b.events = append(b.events, append(json.RawMessage(nil), event...))
	return b.err
}

func newSubscribedKafkaReader(t *testing.T, brokers []string, group string, topics []string) KafkaReader {
	t.Helper()
	r := NewKafkaReader(brokers, group, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Subscribe(ctx, topics); err != nil {
		t.Fatalf("subscribe kafka reader: %v", err)
	}
	return r
}

func TestConsumerWithRealKafkaValidMalformedAndRetry(t *testing.T) {
	brokers := testingx.KafkaBrokers(t)

	t.Run("valid event commits after backend handling", func(t *testing.T) {
		topic := "of-ontology-index-valid-" + uuid.NewString()
		group := "of-ontology-index-" + uuid.NewString()
		testingx.CreateKafkaTopic(t, brokers, topic)
		body := []byte(`{"event_type":"ontology.object.changed.v1","object_id":"obj-1","op":"upsert"}`)
		testingx.ProduceKafkaMessage(t, brokers, topic, body)

		backend := &recordingIndexBackend{}
		consumer := &Consumer{Reader: newSubscribedKafkaReader(t, brokers, group, []string{topic}), Backend: backend}
		t.Cleanup(func() { _ = consumer.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := consumer.RunOnce(ctx); err != nil {
			t.Fatalf("RunOnce valid: %v", err)
		}
		if backend.calls != 1 || len(backend.events) != 1 || backend.topics[0] != topic {
			t.Fatalf("backend calls drift: calls=%d topics=%v events=%d", backend.calls, backend.topics, len(backend.events))
		}
		_ = consumer.Close()
		testingx.RequireKafkaGroupOffset(t, brokers, topic, group, 1)
	})

	t.Run("malformed event commits without backend handling", func(t *testing.T) {
		topic := "of-ontology-index-malformed-" + uuid.NewString()
		group := "of-ontology-index-" + uuid.NewString()
		testingx.CreateKafkaTopic(t, brokers, topic)
		body := []byte(`{"event_type":`)
		testingx.ProduceKafkaMessage(t, brokers, topic, body)

		backend := &recordingIndexBackend{}
		consumer := &Consumer{Reader: newSubscribedKafkaReader(t, brokers, group, []string{topic}), Backend: backend}
		t.Cleanup(func() { _ = consumer.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := consumer.RunOnce(ctx); err != nil {
			t.Fatalf("RunOnce malformed: %v", err)
		}
		if backend.calls != 0 {
			t.Fatalf("malformed event called backend %d times", backend.calls)
		}
		_ = consumer.Close()
		testingx.RequireKafkaGroupOffset(t, brokers, topic, group, 1)
	})

	t.Run("backend error leaves offset uncommitted for retry", func(t *testing.T) {
		topic := "of-ontology-index-retry-" + uuid.NewString()
		group := "of-ontology-index-" + uuid.NewString()
		testingx.CreateKafkaTopic(t, brokers, topic)
		body := []byte(`{"event_type":"ontology.object.changed.v1","object_id":"obj-2","op":"upsert"}`)
		testingx.ProduceKafkaMessage(t, brokers, topic, body)

		consumer := &Consumer{Reader: newSubscribedKafkaReader(t, brokers, group, []string{topic}), Backend: &recordingIndexBackend{err: errors.New("backend down")}}
		t.Cleanup(func() { _ = consumer.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := consumer.RunOnce(ctx)
		if err == nil || !strings.Contains(err.Error(), "backend down") {
			t.Fatalf("expected backend error, got %v", err)
		}
		_ = consumer.Close()
		testingx.RequireKafkaReplay(t, brokers, topic, group, body)
	})
}
