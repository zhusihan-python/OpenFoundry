//go:build integration

package runtime_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	databus "github.com/openfoundry/openfoundry-go/libs/event-bus-data"
	"github.com/openfoundry/openfoundry-go/services/audit-sink/internal/config"
	"github.com/openfoundry/openfoundry-go/services/audit-sink/internal/envelope"
	"github.com/openfoundry/openfoundry-go/services/audit-sink/internal/runtime"
)

type kafkaAuditWriter struct {
	batches [][]envelope.AuditEnvelope
	err     error
}

func (w *kafkaAuditWriter) Append(_ context.Context, batch []envelope.AuditEnvelope) error {
	if w.err != nil {
		return w.err
	}
	cp := make([]envelope.AuditEnvelope, len(batch))
	copy(cp, batch)
	w.batches = append(w.batches, cp)
	return nil
}
func (w *kafkaAuditWriter) Close() error { return nil }
func (w *kafkaAuditWriter) calls() int {
	return len(w.batches)
}

func kafkaBrokers(t *testing.T) []string {
	t.Helper()
	if bootstrap := strings.TrimSpace(os.Getenv("KAFKA_BOOTSTRAP_SERVERS")); bootstrap != "" {
		return strings.Split(bootstrap, ",")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	broker, err := tckafka.Run(ctx, "confluentinc/cp-kafka:7.7.0", tckafka.WithClusterID("test-cluster"))
	if broker != nil {
		t.Cleanup(func() {
			stop, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := broker.Terminate(stop); err != nil {
				t.Errorf("terminate test broker: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatalf("start test broker: %v", err)
	}
	brokers, err := broker.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolve test broker: %v", err)
	}
	return brokers
}

func createKafkaTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	reply, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if topicErr, ok := reply.Errors[topic]; !ok || topicErr != nil {
		t.Fatalf("topic %s creation response: %v", topic, reply.Errors)
	}
	if _, err := kafka.DefaultDialer.LookupLeader(ctx, "tcp", brokers[0], topic, 0); err != nil {
		t.Fatalf("wait for topic leader: %v", err)
	}
}

func produceKafkaMessage(t *testing.T, brokers []string, topic string, value []byte) {
	t.Helper()
	w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, RequiredAcks: kafka.RequireAll, Balancer: &kafka.LeastBytes{}}
	defer w.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(uuid.NewString()), Value: value}); err != nil {
		t.Fatalf("produce kafka message: %v", err)
	}
}

func newKafkaSubscriber(t *testing.T, brokers []string, topic, group string) *databus.KafkaSubscriber {
	t.Helper()
	cfg := databus.NewConfig(brokers, databus.InsecureDev("audit-sink-test"))
	sub, err := databus.NewKafkaSubscriber(cfg, group, []string{topic})
	if err != nil {
		t.Fatalf("new kafka subscriber: %v", err)
	}
	return sub
}

func expectGroupOffset(t *testing.T, brokers []string, topic, group string, want int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	reply, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: group, Topics: map[string][]int{topic: {0}},
	})
	if err != nil {
		t.Fatalf("fetch group offset: %v", err)
	}
	partitions := reply.Topics[topic]
	if reply.Error != nil || len(partitions) != 1 || partitions[0].Partition != 0 || partitions[0].Error != nil {
		t.Fatalf("unexpected group offset response: %+v", reply)
	}
	if got := partitions[0].CommittedOffset; got != want {
		t.Fatalf("group %s partition 0 offset = %d, want %d", group, got, want)
	}
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

func runAuditSinkOnce(t *testing.T, brokers []string, topic, group string, w *kafkaAuditWriter) error {
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
	brokers := kafkaBrokers(t)

	t.Run("valid event writes and commits", func(t *testing.T) {
		topic := "of-audit-sink-valid-" + uuid.NewString()
		group := "of-audit-sink-" + uuid.NewString()
		createKafkaTopic(t, brokers, topic)
		body := mkEnvelopeBytes(t, time.Now().Add(-time.Second).UnixMicro(), "audit.valid")
		produceKafkaMessage(t, brokers, topic, body)

		w := &kafkaAuditWriter{}
		if err := runAuditSinkOnce(t, brokers, topic, group, w); err != nil {
			t.Fatalf("Run valid: %v", err)
		}
		if w.calls() != 1 {
			t.Fatalf("writer calls = %d, want 1", w.calls())
		}
		expectGroupOffset(t, brokers, topic, group, 1)
	})

	t.Run("malformed event commits without writer", func(t *testing.T) {
		topic := "of-audit-sink-malformed-" + uuid.NewString()
		group := "of-audit-sink-" + uuid.NewString()
		createKafkaTopic(t, brokers, topic)
		body := []byte(`{"event_id":`)
		produceKafkaMessage(t, brokers, topic, body)

		w := &kafkaAuditWriter{}
		if err := runAuditSinkOnce(t, brokers, topic, group, w); err != nil {
			t.Fatalf("Run malformed: %v", err)
		}
		if w.calls() != 0 {
			t.Fatalf("malformed event wrote %d batches", w.calls())
		}
		expectGroupOffset(t, brokers, topic, group, 1)
	})

	t.Run("writer error leaves offset uncommitted for retry", func(t *testing.T) {
		topic := "of-audit-sink-retry-" + uuid.NewString()
		group := "of-audit-sink-" + uuid.NewString()
		createKafkaTopic(t, brokers, topic)
		body := mkEnvelopeBytes(t, time.Now().Add(-time.Second).UnixMicro(), "audit.retry")
		produceKafkaMessage(t, brokers, topic, body)

		err := runAuditSinkOnce(t, brokers, topic, group, &kafkaAuditWriter{err: errors.New("writer down")})
		if err == nil || !strings.Contains(err.Error(), "writer down") {
			t.Fatalf("expected writer error, got %v", err)
		}
		expectGroupOffset(t, brokers, topic, group, -1)

		recovered := &kafkaAuditWriter{}
		if err := runAuditSinkOnce(t, brokers, topic, group, recovered); err != nil {
			t.Fatalf("Run recovery: %v", err)
		}
		want, err := envelope.Decode(body)
		if err != nil {
			t.Fatal(err)
		}
		if len(recovered.batches) != 1 || len(recovered.batches[0]) != 1 || recovered.batches[0][0].EventID != want.EventID {
			t.Fatal("recovered consumer did not write the original event")
		}
		expectGroupOffset(t, brokers, topic, group, 1)
	})
}
