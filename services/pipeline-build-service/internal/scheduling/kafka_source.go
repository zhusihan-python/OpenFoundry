// Kafka MessageSource for the scheduler consumer: a segmentio reader on
// TopicTriggers whose offsets advance ONLY through explicit Commit calls
// (CommitInterval=0 — no auto-commit; a decision durable in PG is what
// earns the ACK). Tombstones are surfaced as non-nil empty payloads so
// the consumer counts them through the poison/DLQ budget instead of
// treating them as "no message".
package scheduling

import (
	"context"
	"strings"

	"github.com/segmentio/kafka-go"

	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/config"
)

// KafkaSource adapts *kafka.Reader onto the Consumer's MessageSource.
type KafkaSource struct {
	reader *kafka.Reader
}

// NewKafkaSource builds the reader from the frozen config surface. The
// dialer must come from NewDialer (mTLS/SASL semantics live there).
func NewKafkaSource(cfg config.SchedulerConfig, dialer *kafka.Dialer) *KafkaSource {
	brokers := make([]string, 0, 4)
	for _, broker := range strings.Split(cfg.KafkaBootstrap, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        cfg.GroupID,
		Topic:          scheduleevents.TopicTriggers,
		Dialer:         dialer,
		MinBytes:       1,
		MaxBytes:       1 << 20, // trigger envelopes are tiny; oversized = hostile
		CommitInterval: 0,       // explicit CommitMessages only
	})
	return &KafkaSource{reader: reader}
}

// Poll blocks until a message, ctx completion, or reader failure.
func (s *KafkaSource) Poll(ctx context.Context) (raw []byte, topic string, partition int, offset int64, err error) {
	m, err := s.reader.FetchMessage(ctx)
	if err != nil {
		return nil, "", 0, 0, err
	}
	if m.Value == nil {
		// CDC tombstone (delete event): a real consumed message whose
		// payload is undecodable — poison-budget it, never idle-skip.
		return []byte{}, m.Topic, m.Partition, m.Offset, nil
	}
	return m.Value, m.Topic, m.Partition, m.Offset, nil
}

// Commit durably advances the offset — the caller only invokes it after
// the admission/DLQ decision committed.
func (s *KafkaSource) Commit(ctx context.Context, topic string, partition int, offset int64) error {
	return s.reader.CommitMessages(ctx, kafka.Message{Topic: topic, Partition: partition, Offset: offset})
}

// Close releases the reader (partition assignments, connections).
func (s *KafkaSource) Close() error {
	return s.reader.Close()
}
