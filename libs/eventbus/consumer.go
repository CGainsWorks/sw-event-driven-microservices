package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// Handler processes one event.
//
// Returning nil means "done, never show me this again" — the offset is committed.
// Returning an error means the event is retried, and after MaxAttempts it is dead-lettered
// and the offset committed anyway.
//
// A handler MUST be idempotent. It will be called more than once for the same event: the
// outbox publishes at least once, the broker redelivers on rebalance, and a crash between
// handling and offset commit replays everything since the last commit. Deduplication is
// provided by libs/idempotency and is not optional.
type Handler func(ctx context.Context, env *Envelope, headers map[string]string) error

// ErrPermanent marks an error that retrying cannot fix — a malformed payload, an unknown
// event type, a validation failure. Wrapping with it skips the retry loop and goes
// straight to the dead letter topic, because thirty seconds of retries on a message that
// will never parse only delays the rest of the partition.
var ErrPermanent = errors.New("permanent")

// Permanent wraps err so the consumer stops retrying it.
func Permanent(err error) error { return fmt.Errorf("%w: %w", ErrPermanent, err) }

// ConsumerConfig configures one consumer group on one topic.
type ConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string

	// MaxAttempts is how many times one message is handled before it is dead-lettered.
	MaxAttempts int

	// RetryBackoff is the wait after the first failure; it doubles up to RetryBackoffMax.
	RetryBackoff    time.Duration
	RetryBackoffMax time.Duration

	// Validate turns on schema validation of every consumed event.
	//
	// On by default. A consumer that trusts the shape of what it reads is a consumer that
	// crashes at 3am on the day a producer ships a bug, and the stack trace points at the
	// consumer rather than at the producer that caused it. Validating on the way in makes
	// the message land in the DLQ with the producer's name on it instead.
	SkipValidation bool
}

func (c *ConsumerConfig) applyDefaults() {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 200 * time.Millisecond
	}
	if c.RetryBackoffMax <= 0 {
		c.RetryBackoffMax = 5 * time.Second
	}
}

// Consumer reads one topic as part of a consumer group, with retries and dead-lettering.
type Consumer struct {
	reader *kafka.Reader
	dlq    Publisher
	log    *slog.Logger
	cfg    ConsumerConfig
}

// Publisher is the write side the consumer needs for dead-lettering. Declared here as well
// as in libs/outbox so that neither package has to import the other.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
}

func NewConsumer(cfg ConsumerConfig, dlq Publisher, log *slog.Logger) *Consumer {
	cfg.applyDefaults()
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: cfg.Brokers,
			Topic:   cfg.Topic,
			GroupID: cfg.GroupID,

			// Start at the beginning for a group that has never committed. A consumer
			// deployed after a producer must not silently skip everything published
			// before it existed.
			StartOffset: kafka.FirstOffset,

			MinBytes: 1,
			MaxBytes: 10e6,

			// Offsets are committed explicitly after each message is handled or
			// dead-lettered, never automatically. Auto-commit would acknowledge messages
			// that were read but not yet processed, so a crash would lose them.
			CommitInterval: 0,
		}),
		dlq: dlq,
		log: log.With("component", "kafka-consumer", "topic", cfg.Topic, "group", cfg.GroupID),
		cfg: cfg,
	}
}

// Run consumes until the context is cancelled.
func (c *Consumer) Run(ctx context.Context, handle Handler) error {
	c.log.Info("consumer started", "max_attempts", c.cfg.MaxAttempts)
	defer func() {
		if err := c.reader.Close(); err != nil {
			c.log.Error("closing reader", "error", err)
		}
		c.log.Info("consumer stopped")
	}()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// A fetch failure is a broker or network problem. Log and retry; the reader
			// handles reconnection itself.
			c.log.Error("fetching message", "error", err)
			if !sleep(ctx, c.cfg.RetryBackoff) {
				return nil
			}
			continue
		}

		c.process(ctx, msg, handle)

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// The message was handled but the offset was not recorded, so it will be
			// redelivered. Correct, and harmless: the handler is idempotent, which is why
			// that requirement is not negotiable.
			c.log.Error("committing offset, message will be redelivered",
				"error", err, "partition", msg.Partition, "offset", msg.Offset)
		}
	}
}

func (c *Consumer) process(ctx context.Context, msg kafka.Message, handle Handler) {
	headers := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}

	var env Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		c.deadLetter(ctx, msg, headers, 1, fmt.Errorf("payload is not a valid envelope: %w", err))
		return
	}

	if !c.cfg.SkipValidation {
		if err := env.Validate(); err != nil {
			// Straight to the DLQ. Retrying will not make an invalid event valid, and the
			// DLQ record names the producer that emitted it.
			c.deadLetter(ctx, msg, headers, 1, err)
			return
		}
	}

	log := c.log.With(
		"event_id", env.EventID,
		"event_type", env.EventType,
		"correlation_id", env.CorrelationID,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)

	backoff := c.cfg.RetryBackoff
	var lastErr error

	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		err := handle(ctx, &env, headers)
		if err == nil {
			if attempt > 1 {
				log.Info("event handled after retry", "attempts", attempt)
			}
			return
		}
		lastErr = err

		if errors.Is(err, ErrPermanent) {
			log.Warn("permanent handler failure, dead-lettering without retry", "error", err)
			c.deadLetter(ctx, msg, headers, attempt, err)
			return
		}
		if ctx.Err() != nil {
			return
		}
		if attempt < c.cfg.MaxAttempts {
			log.Warn("handler failed, retrying", "attempt", attempt, "error", err, "retry_in", backoff)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, c.cfg.RetryBackoffMax)
		}
	}

	log.Error("handler exhausted retries, dead-lettering", "attempts", c.cfg.MaxAttempts, "error", lastErr)
	c.deadLetter(ctx, msg, headers, c.cfg.MaxAttempts, lastErr)
}

// deadLetter copies the message verbatim to <topic>.dlq with headers describing why.
//
// The body is copied unchanged so that a fixed consumer can replay it without having to
// reconstruct anything. The headers are what a human reads during an incident, and the
// names match events/topics.yml.
func (c *Consumer) deadLetter(ctx context.Context, msg kafka.Message, headers map[string]string, attempts int, cause error) {
	dlqHeaders := make(map[string]string, len(headers)+6)
	for k, v := range headers {
		dlqHeaders[k] = v
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	dlqHeaders["dlq.original_topic"] = msg.Topic
	dlqHeaders["dlq.original_partition"] = strconv.Itoa(msg.Partition)
	dlqHeaders["dlq.original_offset"] = strconv.FormatInt(msg.Offset, 10)
	dlqHeaders["dlq.failure_reason"] = cause.Error()
	dlqHeaders["dlq.attempts"] = strconv.Itoa(attempts)
	dlqHeaders["dlq.last_failed_at"] = now
	if _, ok := dlqHeaders["dlq.first_failed_at"]; !ok {
		dlqHeaders["dlq.first_failed_at"] = now
	}

	topic := msg.Topic + ".dlq"
	if err := c.dlq.Publish(ctx, topic, string(msg.Key), msg.Value, dlqHeaders); err != nil {
		// Nothing left to try. The offset is committed by the caller regardless, because
		// the alternative is this message blocking its partition forever. It is logged at
		// error level with the whole payload so that it exists somewhere.
		c.log.Error("could not dead-letter message; it is being dropped",
			"error", err,
			"dlq_topic", topic,
			"original_offset", msg.Offset,
			"cause", cause,
			"payload", string(msg.Value))
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
