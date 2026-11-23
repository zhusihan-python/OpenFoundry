//go:build integration

package testingx

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// KafkaBrokers uses an explicitly configured broker or starts a disposable broker.
func KafkaBrokers(t *testing.T) []string {
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

// CreateKafkaTopic creates a single-partition topic and waits for its leader.
func CreateKafkaTopic(t *testing.T, brokers []string, topic string) {
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

// ProduceKafkaMessage appends a record and waits for broker acknowledgement.
func ProduceKafkaMessage(t *testing.T, brokers []string, topic string, value []byte) {
	t.Helper()
	w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, RequiredAcks: kafka.RequireAll, Balancer: &kafka.LeastBytes{}}
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("close kafka writer: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, kafka.Message{Key: []byte(uuid.NewString()), Value: value}); err != nil {
		t.Fatalf("produce kafka message: %v", err)
	}
}

// RequireKafkaGroupOffset checks the committed next offset for partition zero.
func RequireKafkaGroupOffset(t *testing.T, brokers []string, topic, group string, want int64) {
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

// RequireKafkaReplay verifies a new reader in the same group receives the original first record.
func RequireKafkaReplay(t *testing.T, brokers []string, topic, group string, want []byte) {
	t.Helper()
	RequireKafkaGroupOffset(t, brokers, topic, group, -1)
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, GroupID: group, Topic: topic, MinBytes: 1, MaxBytes: 10e6})
	defer func() {
		if err := r.Close(); err != nil {
			t.Errorf("close kafka replay reader: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	msg, err := r.FetchMessage(ctx)
	if err != nil {
		t.Fatalf("fetch uncommitted record: %v", err)
	}
	if msg.Topic != topic || msg.Partition != 0 || msg.Offset != 0 || !bytes.Equal(msg.Value, want) {
		t.Fatalf("unexpected replay: topic=%s partition=%d offset=%d value=%q", msg.Topic, msg.Partition, msg.Offset, msg.Value)
	}
}
