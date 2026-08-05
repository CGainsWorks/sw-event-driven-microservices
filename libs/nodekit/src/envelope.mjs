/**
 * The event envelope, and validation of it.
 *
 * Parity with libs/eventbus/envelope.go. The two must agree, because they read and write
 * the same topics — and the thing that keeps them agreeing is that both validate against
 * the same JSON Schema files in events/schemas rather than against their own idea of the
 * shape.
 */

import { randomUUID } from 'node:crypto';

import { buildValidators, validateEvent, formatErrors } from '@sw-edm/events';

/**
 * Compiled once per process. Schema compilation is not cheap and the schemas never change
 * at runtime; a service that recompiled per event would spend more time validating than
 * doing work.
 */
let validators = null;
export function schemaValidators() {
  if (!validators) validators = buildValidators();
  return validators;
}

/**
 * Timestamps are always UTC with a trailing Z, at millisecond precision.
 *
 * `toISOString()` already produces exactly that. It is wrapped anyway so there is one
 * place that decides the wire format, matching the Timestamp type in Go — RFC 3339 permits
 * a numeric offset, and once two producers emit the same instant with different offsets,
 * string comparison of timestamps starts returning wrong answers.
 */
export const nowTimestamp = () => new Date().toISOString();
export const toTimestamp = (d) => new Date(d).toISOString();

/**
 * Build an envelope with a fresh event id and the current time.
 *
 * @param {object} spec
 * @param {string} spec.eventType      e.g. "inventory.v1.StockReserved"
 * @param {string} spec.aggregateType  "order" | "payment" | "reservation" | "notification"
 * @param {string} spec.aggregateId    doubles as the Kafka message key
 * @param {string} spec.producer       the owning service
 * @param {string} spec.correlationId  constant for the whole business transaction
 * @param {string} [spec.causationId]  event_id of the event that caused this one
 * @param {object} spec.data           the type-specific payload
 */
export function newEnvelope({
  eventType,
  aggregateType,
  aggregateId,
  producer,
  correlationId,
  causationId,
  data,
}) {
  const envelope = {
    event_id: randomUUID(),
    event_type: eventType,
    event_version: 1,
    occurred_at: nowTimestamp(),
    aggregate_id: aggregateId,
    aggregate_type: aggregateType,
    correlation_id: correlationId,
    producer,
    data,
  };
  // Only set when there is one. An empty string is not an absent field, and the envelope
  // schema constrains causation_id to a UUID pattern that "" does not match.
  if (causationId) envelope.causation_id = causationId;
  return envelope;
}

/**
 * Validate an envelope against the schema for its own event type.
 *
 * Runs on every publish, not only in tests. A schema registry rejects an incompatible
 * *schema*; it does not stop a service publishing a message that fails its own schema.
 * Validating here means a bug that would put a malformed event into a topic — where it
 * sits for seven days and breaks every consumer that reads it — fails at the producer
 * instead, inside the transaction that is about to be rolled back anyway.
 *
 * @throws {SchemaViolation}
 */
export function assertValid(envelope) {
  const { valid, errors } = validateEvent(schemaValidators(), envelope);
  if (!valid) {
    throw new SchemaViolation(envelope, errors);
  }
  return envelope;
}

export class SchemaViolation extends Error {
  constructor(envelope, errors) {
    super(
      `event ${envelope?.event_id} (${envelope?.event_type}) does not match its schema:\n` +
        formatErrors(errors),
    );
    this.name = 'SchemaViolation';
    this.eventId = envelope?.event_id;
    this.eventType = envelope?.event_type;
    this.errors = errors;
  }
}

/**
 * The topic an event belongs on, derived from its type:
 * "inventory.v1.StockReserved" -> "inventory.v1.events".
 *
 * Derived rather than configured. A mapping table is one more thing to keep in step with
 * events/topics.yml, and the derivation is exactly the rule that file already documents.
 */
export function topicFor(eventType) {
  const parts = String(eventType).split('.');
  if (parts.length !== 3) {
    throw new Error(`"${eventType}" is not a <context>.v<major>.<Name> event type`);
  }
  return `${parts[0]}.${parts[1]}.events`;
}
