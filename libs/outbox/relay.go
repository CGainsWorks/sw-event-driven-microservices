package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/CGainsWorks/sw-event-driven-microservices/libs/telemetry"
)

// Publisher is what the relay needs from a message broker. Narrow on purpose: the relay is
// tested against an in-memory implementation that can be told to fail on demand, which is
// how the "broker down" chaos case is exercised without stopping a real broker.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
}

// RelayConfig tunes the relay. The defaults are set for a laptop, not for throughput.
type RelayConfig struct {
	// PollInterval is how long to wait when the outbox was empty. When a batch was found
	// the relay loops again immediately, so a burst drains at full speed and an idle
	// system costs one query per interval.
	PollInterval time.Duration

	// BatchSize bounds how many rows one transaction holds locked.
	BatchSize int

	// Shard and Shards partition the outbox across relay replicas by hash of
	// partition_key. Shards = 1 (the default) means one relay handles everything.
	//
	// This is how the relay scales without losing per-aggregate ordering. SKIP LOCKED
	// alone would let two relays publish two events for the same order out of sequence;
	// sharding by partition_key guarantees every event for one aggregate is visible to
	// exactly one relay, so ordering holds within a shard and aggregates never cross one.
	//
	// The trade is that changing the shard count reshuffles the mapping, so it must be
	// done while the outbox is drained. That is a deliberate constraint, not an oversight.
	Shard  int
	Shards int

	// MaxAttempts before a row is abandoned and reported. A row that cannot be published
	// after this many tries is almost always malformed rather than unlucky, and retrying
	// it forever would block every later row for the same partition key.
	MaxAttempts int
}

func (c *RelayConfig) applyDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = 250 * time.Millisecond
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.Shards <= 0 {
		c.Shards = 1
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 10
	}
}

// Relay moves events from the outbox table to Kafka.
type Relay struct {
	pool   *pgxpool.Pool
	pub    Publisher
	log    *slog.Logger
	cfg    RelayConfig
	tracer trace.Tracer
}

func NewRelay(pool *pgxpool.Pool, pub Publisher, log *slog.Logger, cfg RelayConfig) *Relay {
	cfg.applyDefaults()
	return &Relay{
		pool:   pool,
		pub:    pub,
		log:    log.With("component", "outbox-relay"),
		cfg:    cfg,
		tracer: telemetry.Tracer("libs/outbox"),
	}
}

// Run polls until the context is cancelled. Intended to be started in its own goroutine
// alongside the service that owns the database.
func (r *Relay) Run(ctx context.Context) {
	r.log.Info("outbox relay started",
		"poll_interval", r.cfg.PollInterval,
		"batch_size", r.cfg.BatchSize,
		"shard", r.cfg.Shard,
		"shards", r.cfg.Shards)

	// Backoff applies only to broker failures. A healthy but empty outbox waits
	// PollInterval; a broker that is down waits progressively longer, so that a
	// twenty-minute outage does not produce twenty minutes of failed connection attempts
	// at four per second.
	backoff := r.cfg.PollInterval
	const maxBackoff = 30 * time.Second

	for {
		published, err := r.drainOnce(ctx)
		switch {
		case ctx.Err() != nil:
			r.log.Info("outbox relay stopped")
			return
		case err != nil:
			r.log.Error("outbox drain failed, backing off", "error", err, "retry_in", backoff)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, maxBackoff)
		case published > 0:
			// More may be waiting; loop again without pausing.
			backoff = r.cfg.PollInterval
		default:
			backoff = r.cfg.PollInterval
			if !sleep(ctx, r.cfg.PollInterval) {
				return
			}
		}
	}
}

// RunOnce drains a single batch. Exposed for tests, which need a deterministic relay
// rather than one racing a poll interval.
func (r *Relay) RunOnce(ctx context.Context) (int, error) { return r.drainOnce(ctx) }

func (r *Relay) drainOnce(ctx context.Context) (int, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("outbox: beginning relay transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	batch, err := fetchBatch(ctx, tx, r.cfg.Shard, r.cfg.Shards, r.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, nil
	}

	var (
		publishedIDs []int64
		publishErr   error
		failed       *Record
	)

	// Strictly in id order. On the first failure the loop stops rather than skipping
	// ahead: publishing event N+1 for an aggregate whose event N is still stuck would
	// deliver them out of order, which is precisely the guarantee the partition key
	// exists to provide.
	for i := range batch {
		rec := batch[i]

		// LINKED to the enqueuing span, not parented to it.
		//
		// The publish is caused by the request that enqueued the row, but it is not part
		// of that request: parenting would make the request's span appear to last until
		// the relay got round to publishing, which could be minutes and would be a lie
		// about how long the customer waited. A link records the causal relationship
		// without distorting the duration.
		//
		// The headers travelling to Kafka are the ENQUEUING context, so the eventual
		// consumer continues the original trace rather than this relay poll.
		publishCtx, span := r.tracer.Start(ctx, "outbox publish",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithLinks(telemetry.LinkFrom(rec.Headers)...),
			trace.WithAttributes(
				semconv.MessagingSystemKafka,
				semconv.MessagingDestinationName(rec.Topic),
				telemetry.AttrEventID.String(rec.EventID),
			),
		)

		err := r.pub.Publish(publishCtx, rec.Topic, rec.PartitionKey, rec.Payload, rec.Headers)
		if err != nil {
			telemetry.RecordError(span, err)
			span.End()
			publishErr = err
			failed = &batch[i]
			break
		}
		span.End()
		publishedIDs = append(publishedIDs, rec.ID)
	}

	if len(publishedIDs) > 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, publishedIDs); err != nil {
			// The events are already on the broker. Failing to mark them means they will
			// be published again on the next pass — which is exactly the duplicate
			// delivery the consumers are built to absorb. Losing them is not possible.
			return 0, fmt.Errorf("outbox: marking %d rows published: %w", len(publishedIDs), err)
		}
	}

	if failed != nil {
		attempts := failed.Attempts + 1
		if _, err := tx.Exec(ctx,
			`UPDATE outbox SET attempts = $1, last_error = $2 WHERE id = $3`,
			attempts, publishErr.Error(), failed.ID); err != nil {
			return 0, fmt.Errorf("outbox: recording failure for %s: %w", failed.EventID, err)
		}
		if attempts >= r.cfg.MaxAttempts {
			// Loud, and left unpublished. Abandoning it silently would mean a state
			// change nobody was ever told about, which is the one outcome this package
			// exists to prevent. A human has to look.
			r.log.Error("outbox row has exhausted its attempts and is blocking its partition key",
				"event_id", failed.EventID,
				"topic", failed.Topic,
				"partition_key", failed.PartitionKey,
				"attempts", attempts,
				"error", publishErr)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("outbox: committing relay transaction: %w", err)
	}

	if publishErr != nil {
		return len(publishedIDs), fmt.Errorf("outbox: publishing %s: %w", failed.EventID, publishErr)
	}
	return len(publishedIDs), nil
}

// PendingCount reports how many events are waiting. Used by the health endpoint and by the
// chaos tests, which assert that the backlog drains to zero after the broker comes back
// rather than assuming it did.
func (r *Relay) PendingCount(ctx context.Context) (int, error) {
	var n int
	err := r.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("outbox: counting pending: %w", err)
	}
	return n, nil
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

// ErrRelayStopped is returned by helpers that cannot complete because the relay is
// shutting down. Kept distinct from a publish failure so callers do not retry on it.
var ErrRelayStopped = errors.New("outbox: relay stopped")
