/**
 * Consumer-driven contracts: what `notifications` needs from each producer.
 *
 * The direction matters and is the whole point of Pact. These tests are written from the
 * CONSUMER's side and state only what the consumer actually reads. Notifications does not
 * care about `total_amount`, `payment_id` or `compensations_applied` — so none of them
 * appear here, and a producer is free to change them without breaking this contract.
 *
 * What it does care about is `event_type`, `data.customer_email` and `data.order_id`, and
 * those are pinned. If a producer ever removed one, provider verification fails with the
 * name of the consumer that needed it, before it reaches a topic.
 *
 * The verification target is `describeNotification`, which is the real function the message
 * handler calls. Targeting a description of the consumer rather than the consumer itself
 * would let the two drift, which is the failure this whole exercise exists to prevent.
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import pkg from '@pact-foundation/pact';
const { MessageConsumerPact, synchronousBodyHandler, Matchers } = pkg;
const { like, term, uuid } = Matchers;

import { describeNotification } from '../../services/notifications/src/core.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
export const PACT_DIR = resolve(HERE, '../pacts');
const LOG_DIR = resolve(HERE, '../logs');

/**
 * The envelope fields the consumer relies on, as matchers rather than fixed values.
 *
 * Matchers, not literals, because a contract that pins `event_id` to one specific UUID says
 * "the producer must always send THIS id", which is nonsense. What the consumer needs is
 * "a UUID is present". Pinning literals is the most common way to write a Pact contract
 * that passes and protects nothing.
 */
const envelope = (eventType, aggregateType, producer) => ({
  event_id: uuid('01000000-0000-4000-8000-000000000001'),
  event_type: eventType,
  event_version: like(1),
  occurred_at: term({
    // The consumer needs UTC with a trailing Z, because it compares and stores these.
    // An offset would parse and then sort wrongly — see libs/eventbus Timestamp.
    matcher: '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?Z$',
    generate: '2026-03-14T09:15:00.000Z',
  }),
  aggregate_id: uuid('3f2b9c14-7e5a-4d3b-9c8e-1a2b3c4d5e6f'),
  aggregate_type: aggregateType,
  correlation_id: uuid('7a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d'),
  producer,
});

function messagePact(provider) {
  return new MessageConsumerPact({
    consumer: 'notifications',
    provider,
    dir: PACT_DIR,
    logLevel: 'warn',
    log: resolve(LOG_DIR, `${provider}-notifications.log`),
  });
}

/**
 * The handler under verification.
 *
 * Returns what the consumer extracted, so a message that parses but yields nothing usable
 * fails rather than silently passing.
 */
const handler = synchronousBodyHandler((message) => {
  const need = describeNotification(message);
  if (!need) throw new Error(`notifications does not act on ${message?.event_type}`);
  if (!need.recipient) throw new Error('no recipient extracted');
  if (!need.orderId) throw new Error('no order id extracted');
  return need;
});

describe('notifications ← orders', () => {
  const pact = messagePact('orders');

  it('needs a confirmed order it can tell the customer about', async () => {
    await pact
      .expectsToReceive('an order that was confirmed')
      .withContent({
        ...envelope('orders.v1.OrderConfirmed', 'order', 'orders'),
        data: {
          order_id: uuid('3f2b9c14-7e5a-4d3b-9c8e-1a2b3c4d5e6f'),
          customer_id: uuid('8c1d4e2a-5b6f-4a7c-8d9e-0f1a2b3c4d5e'),
          // The field that would otherwise force a synchronous call back into orders.
          // Carrying it on the event is deliberate denormalisation, and pinning it here is
          // what stops someone "tidying" it away later.
          customer_email: term({
            matcher: '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$',
            generate: 'ada@example.com',
          }),
        },
      })
      .withMetadata({ contentType: 'application/json' })
      .verify(handler);
  });

  it('needs a cancelled order it can tell the customer about', async () => {
    await pact
      .expectsToReceive('an order that was cancelled')
      .withContent({
        ...envelope('orders.v1.OrderCancelled', 'order', 'orders'),
        data: {
          order_id: uuid('6b3e8d21-9f4c-4b7a-8e2d-5c1a9f7b3e4d'),
          customer_id: uuid('1d5a8c3f-7b2e-4d9a-8f6c-3e5b7d9f1a2c'),
          customer_email: term({
            matcher: '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$',
            generate: 'grace@example.com',
          }),
          // Not read by notifications, and therefore NOT in this contract beyond existing.
          // A contract that pins fields the consumer ignores turns every producer change
          // into a false alarm, and false alarms are how contract tests get disabled.
        },
      })
      .withMetadata({ contentType: 'application/json' })
      .verify(handler);
  });
});

/**
 * payments and inventory produce events notifications consumes but does not act on.
 *
 * The contract is still worth having: it pins the ENVELOPE. Notifications deserialises
 * every message on those topics, validates it, and records its event_id for
 * deduplication — so it genuinely depends on event_id, event_type, correlation_id and the
 * timestamp format being present and well-formed, even when the payload is ignored.
 *
 * The handler for these asserts the message is recognised and correctly NOT actioned, which
 * is a real expectation: if payments started emitting an event type notifications treats as
 * a customer email, that would be a contract break in the other direction.
 */
const ignoredHandler = synchronousBodyHandler((message) => {
  if (!message?.event_id) throw new Error('no event_id to deduplicate on');
  if (!message?.correlation_id) throw new Error('no correlation_id to trace by');
  const need = describeNotification(message);
  if (need !== null) {
    throw new Error(`notifications would act on ${message.event_type}, which it must not`);
  }
  return { actioned: false, dedupKey: message.event_id };
});

describe('notifications ← payments', () => {
  const pact = messagePact('payments');

  it('needs a captured payment to be deduplicable and traceable', async () => {
    await pact
      .expectsToReceive('a payment that was captured')
      .withContent({
        ...envelope('payments.v1.PaymentCaptured', 'payment', 'payments'),
        data: {
          payment_id: uuid('b4e7a1c9-2d8f-4e6b-a3c5-7d9e1f2a3b4c'),
          order_id: uuid('3f2b9c14-7e5a-4d3b-9c8e-1a2b3c4d5e6f'),
        },
      })
      .withMetadata({ contentType: 'application/json' })
      .verify(ignoredHandler);
  });

  it('needs a failed payment to be deduplicable and traceable', async () => {
    await pact
      .expectsToReceive('a payment that failed')
      .withContent({
        ...envelope('payments.v1.PaymentFailed', 'payment', 'payments'),
        data: {
          payment_id: uuid('d6a9c3eb-4f2a-4b8d-a5c7-9f1e3b5d7a9c'),
          order_id: uuid('2e7b4d9a-6c1f-4a8e-b3d5-7f9a1c3e5b7d'),
        },
      })
      .withMetadata({ contentType: 'application/json' })
      .verify(ignoredHandler);
  });
});

describe('notifications ← inventory', () => {
  const pact = messagePact('inventory');

  it('needs a stock reservation to be deduplicable and traceable', async () => {
    await pact
      .expectsToReceive('stock that was reserved')
      .withContent({
        ...envelope('inventory.v1.StockReserved', 'reservation', 'inventory'),
        data: {
          reservation_id: uuid('5d8c3b2a-1f4e-4a9b-8c7d-6e5f4a3b2c1d'),
          order_id: uuid('3f2b9c14-7e5a-4d3b-9c8e-1a2b3c4d5e6f'),
        },
      })
      .withMetadata({ contentType: 'application/json' })
      .verify(ignoredHandler);
  });

  it('needs a refused reservation to be deduplicable and traceable', async () => {
    await pact
      .expectsToReceive('a stock reservation that was refused')
      .withContent({
        ...envelope('inventory.v1.StockReservationFailed', 'reservation', 'inventory'),
        data: {
          reservation_id: uuid('7e2a9c4d-1b8f-4e5a-9c3d-6b2f8a4c1e7d'),
          order_id: uuid('9a4c7e2d-5f8b-4c3a-9e7d-2b6f4a8c1d3e'),
        },
      })
      .withMetadata({ contentType: 'application/json' })
      .verify(ignoredHandler);
  });
});
