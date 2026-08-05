package eventbus

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer publishes to Kafka. It satisfies outbox.Publisher, and the outbox relay is the
// only thing in this system that uses it — no service publishes directly, because a direct
// publish is a state change that can be lost.
//
// segmentio/kafka-go rather than confluent-kafka-go: it is pure Go, so the service images
// build from scratch with no librdkafka and no cgo. The trade is a smaller feature surface
// (no transactions, no exactly-once producer), which costs nothing here because the outbox
// already provides the delivery guarantee and the consumers already deduplicate.
type Producer struct {
	writer *kafka.Writer
	log    *slog.Logger
}

// NewProducer builds a producer configured for correctness over throughput.
func NewProducer(brokers []string, log *slog.Logger) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr: kafka.TCP(brokers...),

			// Hash the key to choose the partition. This is what makes aggregate_id the
			// ordering guarantee: all events for one aggregate hash to one partition, and
			// Kafka orders within a partition.
			Balancer: &kafka.Hash{},

			// Wait for every in-sync replica. A leader-only ack would let an event be
			// acknowledged and then lost to a leader election, which would mean the
			// outbox row gets marked published for an event nobody ever received —
			// the one failure mode the outbox exists to make impossible.
			RequiredAcks: kafka.RequireAll,

			// The relay publishes one message at a time in id order and needs to know the
			// result of each before sending the next, so batching is off. Batching would
			// be a throughput win and an ordering hazard.
			BatchSize:    1,
			BatchTimeout: 10 * time.Millisecond,

			// Topics are created by scripts/create_topics.sh from events/topics.yml with
			// deliberate partition counts. Auto-creation would silently produce a
			// one-partition topic with default retention and no record of why.
			AllowAutoTopicCreation: false,

			// gzip, not zstd. The Node services read these topics with kafkajs, which has
			// no zstd decoder — see the rationale in events/topics.yml. The broker
			// recompresses to the topic codec anyway, so this mainly saves compressing
			// twice.
			Compression:  kafka.Gzip,
			WriteTimeout: 10 * time.Second,
			MaxAttempts:  3,
		},
		log: log.With("component", "kafka-producer"),
	}
}

// Publish sends one message. Blocks until the broker acknowledges it or the context
// expires; the relay depends on that, because it must not mark a row published on the
// strength of a message that is still in a buffer somewhere.
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}
	for k, v := range headers {
		msg.Headers = append(msg.Headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("eventbus: publishing to %s: %w", topic, err)
	}
	return nil
}

func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("eventbus: closing producer: %w", err)
	}
	return nil
}
