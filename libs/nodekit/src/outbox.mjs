/**
 * Transactional outbox on MongoDB. Parity with libs/outbox in Go.
 *
 * The problem: a service that commits a state change and then publishes an event can crash
 * between the two and lose the event forever. Publishing first is worse — the event then
 * describes something that never happened.
 *
 * The fix: write the event into a collection in the SAME transaction as the state change.
 * A relay reads the collection and publishes. The relay can crash after publishing and
 * before marking the document sent, so an event may be published more than once — which is
 * accepted rather than fought, because it turns an unsolvable distributed problem
 * (exactly-once delivery) into a solvable local one (consumer-side deduplication).
 *
 * At-least-once delivery with exactly-once processing. Not exactly-once delivery, and
 * nothing here claims it is.
 */

import { assertValid, topicFor } from './envelope.mjs';
import { sleep } from './platform.mjs';
import { injectHeaders, linkFrom, tracer, SpanKind } from './telemetry.mjs';

export const COLLECTION = 'outbox';
const SEQ_COLLECTION = 'outbox_seq';

export async function ensureIndexes(db) {
  const outbox = db.collection(COLLECTION);
  // The relay only ever asks for unpublished documents in sequence order. The partial
  // index keeps that query proportional to the backlog rather than to the collection.
  await outbox.createIndex(
    { seq: 1 },
    { name: 'unpublished_seq', partialFilterExpression: { published_at: null } },
  );
  // Enqueuing the same event twice is a bug, and a unique index catches it at the point of
  // the mistake rather than as a duplicate on a topic days later.
  await outbox.createIndex({ event_id: 1 }, { unique: true });
  await outbox.createIndex({ published_at: 1 });
}

/**
 * Monotonic sequence, allocated inside the caller's transaction.
 *
 * MongoDB has no BIGSERIAL, and ObjectIds are only monotonic within one process — two
 * replicas writing concurrently would interleave by clock, not by causal order. Since
 * publication order is what preserves per-aggregate ordering on the topic, the counter is
 * worth its round trip. It is a single hot document, which is fine at this write rate and
 * would need sharding by partition key if it ever were not.
 */
async function nextSeq(db, session) {
  const doc = await db.collection(SEQ_COLLECTION).findOneAndUpdate(
    { _id: 'outbox' },
    { $inc: { value: 1 } },
    { upsert: true, returnDocument: 'after', session },
  );
  return doc.value;
}

/**
 * Write an event to the outbox inside the caller's transaction.
 *
 * The envelope is validated first. Doing it here means a malformed event fails inside the
 * transaction that is about to be rolled back, rather than being discovered by the relay
 * after the state change has already committed — at which point the service has changed
 * state and cannot tell anyone about it.
 */
export async function enqueue(db, session, envelope, headers = {}) {
  assertValid(envelope);
  const topic = topicFor(envelope.event_type);
  const seq = await nextSeq(db, session);

  // Capture trace context HERE, at enqueue time, not at publish time. The relay publishes
  // this document later — possibly after a restart — and its own context is meaningless to
  // a consumer. Storing the causing request's context means the consumer continues that
  // trace instead of starting a fresh root span. Caller-supplied headers win.
  const traceHeaders = injectHeaders() ?? {};
  const merged = { ...traceHeaders, ...headers };

  await db.collection(COLLECTION).insertOne(
    {
      seq,
      event_id: envelope.event_id,
      topic,
      partition_key: envelope.aggregate_id,
      payload: envelope,
      headers: merged,
      created_at: new Date(),
      published_at: null,
      attempts: 0,
      last_error: null,
    },
    { session },
  );
  return envelope;
}

/**
 * Moves events from the outbox collection to Kafka.
 */
export class Relay {
  /**
   * @param {import('mongodb').Db} db
   * @param {{publish: (topic: string, key: string, value: Buffer|string, headers: object) => Promise<void>}} producer
   */
  constructor(db, producer, log, {
    pollIntervalMs = 250,
    batchSize = 100,
    maxAttempts = 10,
    shard = 0,
    shards = 1,
  } = {}) {
    this.db = db;
    this.producer = producer;
    this.log = log.with({ component: 'outbox-relay' });
    this.pollIntervalMs = pollIntervalMs;
    this.batchSize = batchSize;
    this.maxAttempts = maxAttempts;
    this.shard = shard;
    this.shards = shards;
    this.tracer = tracer('libs/nodekit/outbox');
  }

  /**
   * Poll until the signal aborts.
   *
   * Backoff applies only to broker failures. A healthy but empty outbox waits one poll
   * interval; a broker that is down waits progressively longer, so a twenty-minute outage
   * does not produce twenty minutes of failed connection attempts at four per second.
   */
  async run(signal) {
    this.log.info('outbox relay started', {
      poll_interval_ms: this.pollIntervalMs,
      batch_size: this.batchSize,
      shard: this.shard,
      shards: this.shards,
    });

    let backoff = this.pollIntervalMs;
    const maxBackoff = 30_000;

    while (!signal.aborted) {
      let published = 0;
      let failed = false;
      try {
        published = await this.drainOnce();
      } catch (err) {
        failed = true;
        this.log.error('outbox drain failed, backing off', { error: err, retry_in_ms: backoff });
      }

      if (signal.aborted) break;
      if (failed) {
        await sleep(backoff);
        backoff = Math.min(backoff * 2, maxBackoff);
      } else if (published > 0) {
        backoff = this.pollIntervalMs; // more may be waiting; loop again immediately
      } else {
        backoff = this.pollIntervalMs;
        await sleep(this.pollIntervalMs);
      }
    }
    this.log.info('outbox relay stopped');
  }

  /**
   * Drain a single batch. Exposed so tests get a deterministic relay rather than one
   * racing a poll interval.
   */
  async drainOnce() {
    const query = { published_at: null };
    // Sharding by partition key is how the relay scales without losing per-aggregate
    // ordering: every event for one aggregate is visible to exactly one relay, so ordering
    // holds within a shard and aggregates never cross one.
    if (this.shards > 1) {
      query.$expr = {
        $eq: [{ $mod: [{ $abs: { $toHashedIndexKey: '$partition_key' } }, this.shards] }, this.shard],
      };
    }

    const batch = await this.db
      .collection(COLLECTION)
      .find(query, { sort: { seq: 1 }, limit: this.batchSize })
      .toArray();

    if (batch.length === 0) return 0;

    const publishedIds = [];
    let failure = null;

    // Strictly in sequence order. On the first failure the loop stops rather than skipping
    // ahead: publishing event N+1 for an aggregate whose event N is stuck would deliver
    // them out of order, which is precisely the guarantee the partition key provides.
    for (const doc of batch) {
      // LINKED to the enqueuing span, not parented to it: the publish is caused by that
      // request but is not part of it, and parenting would make the request appear to last
      // until the relay got round to it. The headers sent to Kafka are the ENQUEUING
      // context, so the eventual consumer continues the original trace.
      const span = this.tracer.startSpan(`${doc.topic} publish`, {
        kind: SpanKind.PRODUCER,
        links: linkFrom(doc.headers),
        attributes: {
          'messaging.system': 'kafka',
          'messaging.destination.name': doc.topic,
          'sw_edm.event_id': doc.event_id,
        },
      });
      try {
        await this.producer.publish(
          doc.topic,
          doc.partition_key,
          JSON.stringify(doc.payload),
          doc.headers ?? {},
        );
        publishedIds.push(doc._id);
      } catch (err) {
        span.recordException(err);
        failure = { doc, err };
        break;
      } finally {
        span.end();
      }
    }

    if (publishedIds.length > 0) {
      // These are already on the broker. Failing to mark them means they are published
      // again next pass — the duplicate delivery consumers are built to absorb. Losing
      // them is not possible.
      await this.db
        .collection(COLLECTION)
        .updateMany({ _id: { $in: publishedIds } }, { $set: { published_at: new Date() } });
    }

    if (failure) {
      const attempts = (failure.doc.attempts ?? 0) + 1;
      await this.db
        .collection(COLLECTION)
        .updateOne(
          { _id: failure.doc._id },
          { $set: { attempts, last_error: String(failure.err?.message ?? failure.err) } },
        );

      if (attempts >= this.maxAttempts) {
        // Loud, and left unpublished. Abandoning it silently would mean a state change
        // nobody was ever told about, which is the one outcome this module exists to
        // prevent. A human has to look.
        this.log.error('outbox document has exhausted its attempts and is blocking its partition key', {
          event_id: failure.doc.event_id,
          topic: failure.doc.topic,
          partition_key: failure.doc.partition_key,
          attempts,
          error: failure.err,
        });
      }
      throw new Error(`outbox: publishing ${failure.doc.event_id}: ${failure.err?.message ?? failure.err}`);
    }

    return publishedIds.length;
  }

  /**
   * How many events are waiting. Used by the readiness endpoint and by the chaos tests,
   * which assert the backlog drains after the broker returns rather than assuming it did.
   */
  async pendingCount() {
    return this.db.collection(COLLECTION).countDocuments({ published_at: null });
  }
}
