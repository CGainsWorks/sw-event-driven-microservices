/**
 * Kafka producer and consumer. Parity with libs/eventbus in Go.
 *
 * Nothing in a service publishes directly — publishing goes through the outbox relay,
 * because a direct publish is a state change that can be lost.
 */

import { Kafka, logLevel } from 'kafkajs';

import { assertValid } from './envelope.mjs';
import { sleep } from './platform.mjs';
import {
  extractContext, tracer, context, SpanKind, SpanStatusCode, Attr,
} from './telemetry.mjs';

/**
 * Marks an error that retrying cannot fix — a malformed payload, an unknown event type, a
 * validation failure. Skips the retry loop and goes straight to the dead letter topic,
 * because thirty seconds of retries on a message that will never parse only delays the rest
 * of the partition.
 */
export class PermanentError extends Error {
  constructor(message, cause) {
    super(message);
    this.name = 'PermanentError';
    this.cause = cause;
  }
}

function client(brokers, clientId, log) {
  return new Kafka({
    clientId,
    brokers,
    logLevel: logLevel.WARN,
    logCreator: () => ({ namespace, level, log: entry }) => {
      const msg = entry?.message ?? 'kafka';
      const attrs = { namespace, ...entry };
      delete attrs.message;
      if (level <= logLevel.ERROR) log.error(msg, attrs);
      else log.warn(msg, attrs);
    },
    retry: { initialRetryTime: 300, retries: 8 },
  });
}

export class Producer {
  constructor(brokers, log, clientId = 'sw-edm') {
    this.log = log.with({ component: 'kafka-producer' });
    this.kafka = client(brokers, clientId, this.log);
    this.producer = this.kafka.producer({
      // Broker-side deduplication of producer retries. It does not make delivery
      // exactly-once end to end — the outbox relay can still republish after a crash —
      // but it removes the duplicates the client library itself would create.
      idempotent: true,
      // Required by the idempotent producer, and correct regardless: the relay publishes
      // one message at a time in sequence order and needs each result before sending the
      // next. More in flight would be a throughput win and an ordering hazard.
      maxInFlightRequests: 1,
    });
    this.connected = false;
  }

  async connect() {
    if (!this.connected) {
      await this.producer.connect();
      this.connected = true;
    }
  }

  /**
   * Send one message and wait for the broker to acknowledge it.
   *
   * The relay depends on that wait: it must not mark a document published on the strength
   * of a message still sitting in a buffer.
   */
  async publish(topic, key, value, headers = {}) {
    await this.connect();
    await this.producer.send({
      topic,
      // Every in-sync replica. A leader-only ack would let an event be acknowledged and
      // then lost to a leader election, which would mark the outbox document published
      // for an event nobody ever received — the one failure the outbox exists to prevent.
      acks: -1,
      messages: [{
        key: key ? String(key) : null,
        value: typeof value === 'string' ? value : JSON.stringify(value),
        headers,
      }],
    });
  }

  async disconnect() {
    if (this.connected) {
      await this.producer.disconnect();
      this.connected = false;
    }
  }
}

/**
 * Reads one topic as part of a consumer group, with retries and dead-lettering.
 *
 * The handler MUST be idempotent. It will be called more than once for the same event: the
 * outbox publishes at least once, the broker redelivers on rebalance, and a crash between
 * handling and offset commit replays everything since the last commit. Deduplication comes
 * from idempotency.seen() and is not optional.
 */
export class Consumer {
  constructor({
    brokers,
    topic,
    groupId,
    clientId = 'sw-edm',
    maxAttempts = 3,
    retryBackoffMs = 200,
    retryBackoffMaxMs = 5000,
    validate = true,
  }, dlqProducer, log) {
    this.topic = topic;
    this.groupId = groupId;
    this.maxAttempts = maxAttempts;
    this.retryBackoffMs = retryBackoffMs;
    this.retryBackoffMaxMs = retryBackoffMaxMs;
    this.validate = validate;
    this.dlq = dlqProducer;
    this.log = log.with({ component: 'kafka-consumer', topic, group: groupId });
    this.kafka = client(brokers, clientId, this.log);
    this.consumer = this.kafka.consumer({ groupId });
    this.tracer = tracer('libs/nodekit/kafka');
  }

  async run(handler, signal) {
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: this.topic,
      // A consumer deployed after a producer must not silently skip everything published
      // before it existed.
      fromBeginning: true,
    });

    this.log.info('consumer started', { max_attempts: this.maxAttempts });

    signal.addEventListener('abort', () => {
      this.consumer.disconnect().catch((err) => this.log.error('disconnecting consumer', { error: err }));
    }, { once: true });

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        // Never throws. Retries, dead-lettering and giving up are all handled here, so
        // kafkajs commits the offset and the partition keeps moving. A handler failure
        // must not block every later message for every other aggregate.
        await this.#process(topic, partition, message, handler, signal);
      },
    });

    await new Promise((resolve) => {
      if (signal.aborted) return resolve();
      signal.addEventListener('abort', () => resolve(), { once: true });
    });
    this.log.info('consumer stopped');
  }

  async #process(topic, partition, message, handler, signal) {
    const headers = {};
    for (const [k, v] of Object.entries(message.headers ?? {})) {
      headers[k] = v == null ? '' : v.toString();
    }

    let envelope;
    try {
      envelope = JSON.parse(message.value.toString());
    } catch (err) {
      await this.#deadLetter(topic, partition, message, headers, 1,
        new PermanentError(`payload is not valid JSON: ${err.message}`));
      return;
    }

    if (this.validate) {
      try {
        assertValid(envelope);
      } catch (err) {
        // Straight to the DLQ. Retrying will not make an invalid event valid, and the DLQ
        // record names the producer that emitted it.
        await this.#deadLetter(topic, partition, message, headers, 1, err);
        return;
      }
    }

    const log = this.log.with({
      event_id: envelope.event_id,
      event_type: envelope.event_type,
      correlation_id: envelope.correlation_id,
      partition,
      offset: message.offset,
    });

    // Continue the trace of the request that CAUSED this event, not the poll that fetched
    // it. The headers were written at enqueue time by the outbox. Running the whole retry
    // loop inside this context means any span the handler creates — including the outbox
    // enqueue of the resulting notification event — hangs off the same trace.
    const parentCtx = extractContext(headers);
    await context.with(parentCtx, async () => {
      await this.#handleWithRetries(topic, partition, message, headers, envelope, handler, signal, log);
    });
  }

  async #handleWithRetries(topic, partition, message, headers, envelope, handler, signal, log) {
    const span = this.tracer.startSpan(`${topic} process`, {
      kind: SpanKind.CONSUMER,
      attributes: {
        'messaging.system': 'kafka',
        'messaging.destination.name': topic,
        'messaging.kafka.offset': String(message.offset),
        'messaging.kafka.partition': partition,
        [Attr.EventID]: envelope.event_id,
        [Attr.EventType]: envelope.event_type,
        [Attr.CorrelationID]: envelope.correlation_id,
      },
    });

    // try/finally rather than an end() at every exit: this loop has five ways out, and a
    // span that is not ended is a span that never reaches the collector — a leak that
    // shows up as a mysteriously missing consumer rather than as an error.
    try {
      await this.#retryLoop(topic, partition, message, headers, envelope, handler, signal, log, span);
    } finally {
      span.end();
    }
  }

  async #retryLoop(topic, partition, message, headers, envelope, handler, signal, log, span) {
    let backoff = this.retryBackoffMs;
    let lastErr = null;

    for (let attempt = 1; attempt <= this.maxAttempts; attempt += 1) {
      try {
        await handler(envelope, headers);
        if (attempt > 1) log.info('event handled after retry', { attempts: attempt });
        span.setAttribute('sw_edm.handler_attempts', attempt);
        return;
      } catch (err) {
        lastErr = err;
        if (err instanceof PermanentError) {
          log.warn('permanent handler failure, dead-lettering without retry', { error: err });
          span.recordException(err);
          span.setStatus({ code: SpanStatusCode.ERROR, message: 'dead-lettered' });
          await this.#deadLetter(topic, partition, message, headers, attempt, err);
          return;
        }
        if (signal.aborted) return;
        if (attempt < this.maxAttempts) {
          log.warn('handler failed, retrying', { attempt, error: err, retry_in_ms: backoff });
          await sleep(backoff);
          backoff = Math.min(backoff * 2, this.retryBackoffMaxMs);
        }
      }
    }

    log.error('handler exhausted retries, dead-lettering', { attempts: this.maxAttempts, error: lastErr });
    span.recordException(lastErr);
    span.setStatus({ code: SpanStatusCode.ERROR, message: 'exhausted retries' });
    await this.#deadLetter(topic, partition, message, headers, this.maxAttempts, lastErr);
  }

  /**
   * Copy the message verbatim to <topic>.dlq with headers describing why.
   *
   * The body is unchanged so a fixed consumer can replay it without reconstructing
   * anything. The header names match events/topics.yml.
   */
  async #deadLetter(topic, partition, message, headers, attempts, cause) {
    const now = new Date().toISOString();
    const dlqHeaders = {
      ...headers,
      'dlq.original_topic': topic,
      'dlq.original_partition': String(partition),
      'dlq.original_offset': String(message.offset),
      'dlq.failure_reason': String(cause?.message ?? cause),
      'dlq.attempts': String(attempts),
      'dlq.last_failed_at': now,
    };
    if (!dlqHeaders['dlq.first_failed_at']) dlqHeaders['dlq.first_failed_at'] = now;

    const dlqTopic = `${topic}.dlq`;
    try {
      await this.dlq.publish(dlqTopic, message.key?.toString() ?? null, message.value.toString(), dlqHeaders);
    } catch (err) {
      // Nothing left to try. The offset commits regardless, because the alternative is
      // this message blocking its partition forever. Logged with the whole payload so it
      // exists somewhere.
      this.log.error('could not dead-letter message; it is being dropped', {
        error: err,
        dlq_topic: dlqTopic,
        original_offset: message.offset,
        cause: String(cause?.message ?? cause),
        payload: message.value.toString(),
      });
    }
  }

  async disconnect() {
    await this.consumer.disconnect().catch(() => {});
  }
}
