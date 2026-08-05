// Package outbox implements the transactional outbox pattern.
//
// The problem it solves: a service that commits a state change and then publishes an event
// has a window in which it can crash between the two, losing the event forever. Publishing
// first and committing second is worse — now the event describes something that never
// happened.
//
// The fix is to write the event into a table in the SAME transaction as the state change.
// The state and the intent to publish commit or fail together; there is no window. A relay
// then reads the table and publishes to Kafka.
//
// The relay can crash after publishing and before marking the row sent, so an event may be
// published more than once. That is accepted rather than fought: it turns an unsolvable
// distributed problem (exactly-once delivery) into a solvable local one (consumer-side
// deduplication, see libs/idempotency).
//
// The system therefore provides at-least-once delivery with exactly-once processing. It
// does not provide exactly-once delivery and nothing in this repository claims it does.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/CGainsWorks/sw-event-driven-microservices/libs/eventbus"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/pgmigrate"
)

// SchemaSQL is the outbox table. Identical in every service, defined once here.
//
// Notes on the shape:
//
//   - id BIGSERIAL is the publication order. Rows are published in id order so that two
//     events written by the same transaction reach Kafka in the order they were enqueued.
//   - event_id is UNIQUE. Enqueuing the same event twice inside one transaction is a bug,
//     and a unique index catches it at the point of the mistake.
//   - published_at NULL means unpublished. A partial index on that condition keeps the
//     relay's query fast when the table has millions of already-published rows in it.
//   - The row is kept after publication rather than deleted, so that "did we ever publish
//     this?" is answerable during an incident. scripts/prune_outbox.sql removes rows
//     older than the topic retention, at which point the answer stops mattering.
const SchemaSQL = `
CREATE TABLE IF NOT EXISTS outbox (
    id             BIGSERIAL   PRIMARY KEY,
    event_id       UUID        NOT NULL UNIQUE,
    topic          TEXT        NOT NULL,
    partition_key  TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    headers        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    attempts       INT         NOT NULL DEFAULT 0,
    last_error     TEXT
);

-- Partial index: the relay only ever asks for unpublished rows, and this keeps that query
-- O(unpublished) rather than O(table).
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (id) WHERE published_at IS NULL;

-- Used by the shard filter when more than one relay runs. See Relay.
CREATE INDEX IF NOT EXISTS outbox_partition_key_idx
    ON outbox (partition_key) WHERE published_at IS NULL;
`

// Migration returns the outbox DDL as a migration for pgmigrate.Apply. Every service that
// publishes events includes this in its migration list.
func Migration() pgmigrate.Migration {
	return pgmigrate.Migration{Name: "0001_outbox", SQL: SchemaSQL}
}

// Execer is the subset of pgx that Enqueue needs. Both pgx.Tx and *pgxpool.Pool satisfy
// it, but callers should always pass a transaction — enqueuing outside the transaction
// that made the state change defeats the entire purpose of this package.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Enqueue writes an event to the outbox inside the caller's transaction.
//
// The envelope is validated against its JSON Schema first. Doing it here means a malformed
// event fails inside the transaction that is about to be rolled back, rather than being
// discovered by the relay after the state change has already committed — at which point
// the service has changed state and cannot tell anyone about it.
func Enqueue(ctx context.Context, tx Execer, env *eventbus.Envelope, headers map[string]string) error {
	if err := env.Validate(); err != nil {
		return err
	}
	topic, err := env.Topic()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: marshalling event %s: %w", env.EventID, err)
	}
	if headers == nil {
		headers = map[string]string{}
	}
	encodedHeaders, err := json.Marshal(headers)
	if err != nil {
		return fmt.Errorf("outbox: marshalling headers for %s: %w", env.EventID, err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (event_id, topic, partition_key, payload, headers)
		VALUES ($1, $2, $3, $4, $5)`,
		env.EventID, topic, env.AggregateID, payload, encodedHeaders,
	)
	if err != nil {
		return fmt.Errorf("outbox: enqueuing event %s (%s): %w", env.EventID, env.EventType, err)
	}
	return nil
}

// Record is one unpublished outbox row, as the relay sees it.
type Record struct {
	ID           int64
	EventID      string
	Topic        string
	PartitionKey string
	Payload      []byte
	Headers      map[string]string
	Attempts     int
}

// fetchBatch reads the next unpublished rows in publication order.
//
// FOR UPDATE SKIP LOCKED lets several relay processes work at once without blocking each
// other. On its own that would break per-aggregate ordering — two relays could publish two
// events for the same order out of sequence — so it is combined with the shard filter in
// Relay, which guarantees that all rows for one partition_key are only ever visible to one
// relay. Ordering is preserved within a shard, and events for one aggregate never span
// shards because the shard is a function of the aggregate id.
func fetchBatch(ctx context.Context, tx pgx.Tx, shard, shards int, limit int) ([]Record, error) {
	const q = `
		SELECT id, event_id, topic, partition_key, payload, headers, attempts
		  FROM outbox
		 WHERE published_at IS NULL
		   AND ($1 = 1 OR abs(hashtext(partition_key)) % $1 = $2)
		 ORDER BY id
		 LIMIT $3
		   FOR UPDATE SKIP LOCKED`

	rows, err := tx.Query(ctx, q, shards, shard, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox: fetching batch: %w", err)
	}
	defer rows.Close()

	var out []Record
	for rows.Next() {
		var r Record
		var rawHeaders []byte
		if err := rows.Scan(&r.ID, &r.EventID, &r.Topic, &r.PartitionKey, &r.Payload, &rawHeaders, &r.Attempts); err != nil {
			return nil, fmt.Errorf("outbox: scanning row: %w", err)
		}
		if len(rawHeaders) > 0 {
			if err := json.Unmarshal(rawHeaders, &r.Headers); err != nil {
				return nil, fmt.Errorf("outbox: decoding headers for %s: %w", r.EventID, err)
			}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
