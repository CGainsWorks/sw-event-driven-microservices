/**
 * The notifications domain.
 *
 * One invariant: one (source_event_id, channel) produces at most one delivered
 * notification. That is the entire correctness requirement, and it exists because the
 * events this service consumes arrive at least once — a redelivered OrderConfirmed must
 * not email the customer twice.
 *
 * This service calls nobody and nothing waits on it. An order stays confirmed whether or
 * not the customer was told, which is what makes it safe to be the flakiest thing in the
 * system.
 */

import { createHash, randomUUID } from 'node:crypto';

import { newEnvelope, idempotency, outbox } from '@sw-edm/nodekit';

const PRODUCER = 'notifications';

export const NOTIFICATIONS = 'notifications';

export const NotificationState = {
  Pending: 'PENDING',
  Sent: 'SENT',
  Failed: 'FAILED',
};

/**
 * Which events produce a notification, and from which template.
 *
 * Everything else on the subscribed topics is consumed and deliberately ignored. Consuming
 * an event and choosing to do nothing is not the same as not subscribing: it keeps the
 * consumer group's offsets moving, so lag reflects real backlog rather than events this
 * service was never going to act on.
 */
export const TEMPLATES = {
  'orders.v1.OrderConfirmed': 'order_confirmed',
  'orders.v1.OrderCancelled': 'order_cancelled',
};

export const hashRecipient = (address) =>
  createHash('sha256').update(String(address).trim().toLowerCase()).digest('hex');

export async function ensureIndexes(db) {
  await outbox.ensureIndexes(db);
  await idempotency.ensureIndexes(db);

  // THE invariant, enforced by a unique index rather than application code. An application
  // check has a window in which two concurrent redeliveries both see "not sent yet" and
  // both send.
  await db.collection(NOTIFICATIONS).createIndex(
    { source_event_id: 1, channel: 1 },
    { unique: true, name: 'one_notification_per_event_per_channel' },
  );
  await db.collection(NOTIFICATIONS).createIndex({ order_id: 1 });
  await db.collection(NOTIFICATIONS).createIndex({ state: 1, created_at: -1 });
}

/**
 * A delivery channel.
 *
 * The built-in sender writes to the log. It is a stand-in for a real provider and is not
 * presented as anything else: there is no SMTP, no delivery receipt, and "sent" means
 * "handed over", not "read".
 *
 * Behaviour is driven by the recipient address so failure paths are reachable from a test
 * without breaking a container, the same trick the payments PSP simulator uses:
 *
 *   anything@fail.example       always fails
 *   anything@flaky.example      fails once per address, then succeeds
 *   anything else               succeeds
 */
export class LogSender {
  constructor(log) {
    this.log = log.with({ component: 'log-sender' });
    this.flakyAttempts = new Map();
  }

  async send({ channel, recipient, template, orderId }) {
    const domain = String(recipient).split('@')[1] ?? '';

    if (domain === 'fail.example') {
      throw new Error(`simulated permanent delivery failure for ${domain}`);
    }
    if (domain === 'flaky.example') {
      const n = (this.flakyAttempts.get(recipient) ?? 0) + 1;
      this.flakyAttempts.set(recipient, n);
      if (n === 1) throw new Error('simulated transient delivery failure');
    }

    this.log.info('notification delivered', {
      channel,
      template,
      order_id: orderId,
      // The address itself is logged, never published. Kafka topics have a long retention
      // and are not a place for personal data; a log with a short retention is a different
      // risk profile.
      recipient,
    });
    return { providerMessageId: `log-${randomUUID()}` };
  }
}

export class NotificationsService {
  constructor(client, db, sender, log, { maxAttempts = 3 } = {}) {
    this.client = client;
    this.db = db;
    this.sender = sender;
    this.log = log.with({ component: 'notifications-core' });
    this.maxAttempts = maxAttempts;
  }

  async #inTx(fn) {
    const session = this.client.startSession();
    try {
      return await session.withTransaction(async () => fn(session), {
        readConcern: { level: 'snapshot' },
        writeConcern: { w: 'majority' },
      });
    } finally {
      await session.endSession();
    }
  }

  /**
   * Handle one consumed event.
   *
   * Three phases, for the same reason payments splits its provider call:
   *
   *   tx1  mark the event seen and record a PENDING notification, then COMMIT. That commit
   *        is what stops a redelivery arriving mid-send from sending a second copy.
   *   send call the provider, outside any transaction.
   *   tx2  record the outcome and enqueue the resulting event together.
   *
   * Returns a short description of what happened, which the consumer logs.
   */
  async handle(envelope) {
    const template = TEMPLATES[envelope.event_type];

    // Not a notification-producing event. Mark it seen so the offset advances and move on.
    if (!template) {
      await this.#inTx(async (session) => {
        await idempotency.seen(this.db, session, envelope.event_id);
      });
      return { action: 'ignored' };
    }

    const channel = 'EMAIL';
    const recipient = envelope.data?.customer_email;
    if (!recipient) {
      // The producer's schema requires customer_email on both templated events, so this
      // cannot happen without a contract violation that validation should already have
      // caught. Refusing loudly beats sending nowhere.
      throw new Error(`${envelope.event_type} carried no customer_email`);
    }

    let notificationId = null;
    const claimed = await this.#inTx(async (session) => {
      const alreadySeen = await idempotency.seen(this.db, session, envelope.event_id);
      if (alreadySeen) return { skip: true, reason: 'event already processed' };

      // The unique index is the real guard. This read is an optimisation that avoids
      // relying on a duplicate-key error for normal flow control.
      const existing = await this.db.collection(NOTIFICATIONS)
        .findOne({ source_event_id: envelope.event_id, channel }, { session });
      if (existing && existing.state !== NotificationState.Pending) {
        return { skip: true, reason: `already ${existing.state}` };
      }

      notificationId = existing?._id ?? randomUUID();
      await this.db.collection(NOTIFICATIONS).updateOne(
        { _id: notificationId },
        {
          $set: {
            source_event_id: envelope.event_id,
            channel,
            template,
            recipient_hash: hashRecipient(recipient),
            order_id: envelope.data.order_id,
            correlation_id: envelope.correlation_id,
            state: NotificationState.Pending,
            updated_at: new Date(),
          },
          $setOnInsert: { created_at: new Date(), attempts: 0 },
        },
        { upsert: true, session },
      );
      return { skip: false };
    });

    if (claimed.skip) return { action: 'skipped', reason: claimed.reason };

    let attempts = 0;
    let lastError = null;
    let providerMessageId = null;

    // Retries live here rather than in the Kafka consumer, because a delivery retry should
    // not re-run the deduplication and state machinery above. The consumer's own retry is
    // the outer loop for failures of this whole method.
    for (attempts = 1; attempts <= this.maxAttempts; attempts += 1) {
      try {
        const result = await this.sender.send({
          channel,
          recipient,
          template,
          orderId: envelope.data.order_id,
        });
        providerMessageId = result?.providerMessageId ?? null;
        lastError = null;
        break;
      } catch (err) {
        lastError = err;
        if (attempts < this.maxAttempts) await new Promise((r) => setTimeout(r, 100 * attempts));
      }
    }

    const succeeded = lastError === null;
    await this.#inTx(async (session) => {
      const now = new Date();
      await this.db.collection(NOTIFICATIONS).updateOne(
        { _id: notificationId },
        {
          $set: {
            state: succeeded ? NotificationState.Sent : NotificationState.Failed,
            attempts,
            provider_message_id: providerMessageId,
            failure_reason: succeeded ? null : String(lastError?.message ?? lastError),
            [succeeded ? 'sent_at' : 'failed_at']: now,
            updated_at: now,
          },
        },
        { session },
      );

      const base = {
        notification_id: notificationId,
        source_event_id: envelope.event_id,
        channel,
        template,
        // A hash, never the address. This topic keeps 30 days of history and there is no
        // reason for personal data to sit in it; the hash is enough to correlate and to
        // prove a duplicate was avoided.
        recipient_hash: hashRecipient(recipient),
        order_id: envelope.data.order_id,
        attempts: Math.min(attempts, this.maxAttempts),
      };

      await outbox.enqueue(this.db, session, newEnvelope({
        eventType: succeeded ? 'notifications.v1.NotificationSent' : 'notifications.v1.NotificationFailed',
        aggregateType: 'notification',
        aggregateId: notificationId,
        producer: PRODUCER,
        correlationId: envelope.correlation_id,
        causationId: envelope.event_id,
        data: succeeded
          ? { ...base, ...(providerMessageId ? { provider_message_id: providerMessageId } : {}), sent_at: now.toISOString() }
          : { ...base, failure_reason: String(lastError?.message ?? lastError), failed_at: now.toISOString() },
      }));
    });

    if (!succeeded) {
      // Abandoned, and nothing upstream changes as a result. The order stays exactly as it
      // was. This is logged at warn rather than error because it is a known, tolerated
      // outcome rather than a fault in this system.
      this.log.warn('notification abandoned after retries', {
        notification_id: notificationId,
        order_id: envelope.data.order_id,
        correlation_id: envelope.correlation_id,
        attempts,
        error: String(lastError?.message ?? lastError),
      });
    }
    return { action: succeeded ? 'sent' : 'failed', notificationId, attempts };
  }

  /** Notifications for one order, for the integration tests and for support queries. */
  async forOrder(orderId) {
    return this.db.collection(NOTIFICATIONS).find({ order_id: orderId }).sort({ created_at: 1 }).toArray();
  }
}
