//go:build integration

package subscriber

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"

	testingx "github.com/openfoundry/openfoundry-go/libs/testing"
)

type errorPort struct{ err error }

func (p errorPort) Handle(context.Context, json.RawMessage) error { return p.err }

func newTestKafkaReader(brokers []string, topic, group string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        group,
		Topic:          topic,
		CommitInterval: 0,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        100 * time.Millisecond,
	})
}

func TestConsumerWithRealKafkaValidMalformedAndRetry(t *testing.T) {
	brokers := testingx.KafkaBrokers(t)

	t.Run("valid event commits after port handling", func(t *testing.T) {
		topic := "of-code-review-valid-" + uuid.NewString()
		group := "of-code-review-" + uuid.NewString()
		testingx.CreateKafkaTopic(t, brokers, topic)
		body := []byte(`{"event_type":"dataset.branch.restored.v1","branch_rid":"ri.branch.valid"}`)
		testingx.ProduceKafkaMessage(t, brokers, topic, body)

		port := &recordingPort{}
		consumer := &Consumer{Reader: newTestKafkaReader(brokers, topic, group), Port: port}
		t.Cleanup(func() { _ = consumer.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := consumer.RunOnce(ctx); err != nil {
			t.Fatalf("RunOnce valid: %v", err)
		}
		if port.called != 1 || len(port.events) != 1 {
			t.Fatalf("port calls drift: called=%d events=%d", port.called, len(port.events))
		}
		_ = consumer.Close()
		testingx.RequireKafkaGroupOffset(t, brokers, topic, group, 1)
	})

	t.Run("malformed event commits without port handling", func(t *testing.T) {
		topic := "of-code-review-malformed-" + uuid.NewString()
		group := "of-code-review-" + uuid.NewString()
		testingx.CreateKafkaTopic(t, brokers, topic)
		body := []byte(`{"event_type":`)
		testingx.ProduceKafkaMessage(t, brokers, topic, body)

		port := &recordingPort{}
		consumer := &Consumer{Reader: newTestKafkaReader(brokers, topic, group), Port: port}
		t.Cleanup(func() { _ = consumer.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := consumer.RunOnce(ctx); err != nil {
			t.Fatalf("RunOnce malformed: %v", err)
		}
		if port.called != 0 {
			t.Fatalf("malformed event called port %d times", port.called)
		}
		_ = consumer.Close()
		testingx.RequireKafkaGroupOffset(t, brokers, topic, group, 1)
	})

	t.Run("backend error leaves offset uncommitted for retry", func(t *testing.T) {
		topic := "of-code-review-retry-" + uuid.NewString()
		group := "of-code-review-" + uuid.NewString()
		testingx.CreateKafkaTopic(t, brokers, topic)
		body := []byte(`{"event_type":"dataset.branch.restored.v1","branch_rid":"ri.branch.retry"}`)
		testingx.ProduceKafkaMessage(t, brokers, topic, body)

		consumer := &Consumer{Reader: newTestKafkaReader(brokers, topic, group), Port: errorPort{err: errors.New("backend down")}}
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
