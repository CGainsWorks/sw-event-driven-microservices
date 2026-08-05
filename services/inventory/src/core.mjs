/**
 * The inventory domain.
 *
 * One invariant, enforced here and nowhere else:
 *
 *     reserved <= on_hand,  and  available = on_hand - reserved  is never negative.
 *
 * Everything in this file exists to keep that true under concurrency, under retries, and
 * under a caller that dies halfway through. It knows nothing about gRPC — transport lives
 * in grpc.mjs — so it can be tested against a database alone.
 */

import { randomUUID } from 'node:crypto';

import { newEnvelope, idempotency, outbox } from '@sw-edm/nodekit';

const PRODUCER = 'inventory';

export const ReservationState = {
  Held: 'HELD',
  Committed: 'COMMITTED',
  Released: 'RELEASED',
  Expired: 'EXPIRED',
  Failed: 'FAILED',
};

export const Outcome = {
  Reserved: 'RESERVED',
  InsufficientStock: 'INSUFFICIENT_STOCK',
  UnknownSku: 'UNKNOWN_SKU',
  Released: 'RELEASED',
  Committed: 'COMMITTED',
  AlreadyCommitted: 'ALREADY_COMMITTED',
  Expired: 'EXPIRED',
  NotFound: 'NOT_FOUND',
};

export const STOCK = 'stock_items';
export const RESERVATIONS = 'reservations';

export async function ensureIndexes(db) {
  await outbox.ensureIndexes(db);
  await idempotency.ensureIndexes(db);

  // One reservation per idempotency key, forever. A unique index rather than an
  // application check, because an application check has a window in which two concurrent
  // retries both see "no reservation" and both create one.
  await db.collection(RESERVATIONS).createIndex({ idempotency_key: 1 }, { unique: true });
  await db.collection(RESERVATIONS).createIndex({ order_id: 1 });

  // Drives the expiry sweeper: held reservations, oldest expiry first.
  await db.collection(RESERVATIONS).createIndex(
    { expires_at: 1 },
    { name: 'held_expiring', partialFilterExpression: { state: ReservationState.Held } },
  );
}

export class InventoryService {
  /**
   * @param {import('mongodb').MongoClient} client
   * @param {import('mongodb').Db} db
   */
  constructor(client, db, log, { defaultTtlMs = 300_000, maxTtlMs = 900_000 } = {}) {
    this.client = client;
    this.db = db;
    this.log = log.with({ component: 'inventory-core' });
    this.defaultTtlMs = defaultTtlMs;
    this.maxTtlMs = maxTtlMs;
  }

  /**
   * Run fn inside a MongoDB transaction.
   *
   * Snapshot isolation is what makes the conditional stock updates below safe: two
   * concurrent reservations for the last unit cannot both observe it as available and both
   * succeed. One of them hits a write conflict and MongoDB retries or aborts it.
   */
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

  // --- reserve ------------------------------------------------------------

  /**
   * Claim stock for an order.
   *
   * Two paths, and the split is the interesting part:
   *
   *   The happy path runs entirely in one transaction. Each line is decremented with a
   *   CONDITIONAL update — "increase reserved by n, but only if on_hand - reserved >= n" —
   *   so the check and the change are one indivisible operation. There is no moment
   *   between deciding stock is available and taking it.
   *
   *   If any line cannot be satisfied the whole transaction aborts, undoing the lines that
   *   already succeeded. The refusal is then recorded in a SECOND transaction, because the
   *   first one no longer exists to record anything. That second transaction is what makes
   *   a retry return the same refusal rather than re-running the check against stock that
   *   has since changed.
   */
  async reserveStock({ idempotencyKey, orderId, lines, ttlSeconds, correlationId, causationId }) {
    if (!Array.isArray(lines) || lines.length === 0) {
      throw new Error('inventory: a reservation needs at least one line');
    }
    const ttlMs = Math.min(
      ttlSeconds > 0 ? ttlSeconds * 1000 : this.defaultTtlMs,
      this.maxTtlMs,
    );

    let refusal = null;

    const held = await this.#inTx(async (session) => {
      const claimed = await idempotency.claim(
        this.db, session, idempotency.Scope.ReserveStock, idempotencyKey,
        { order_id: orderId, lines },
      );
      if (claimed.status === idempotency.Status.Completed) {
        return { ...claimed.response, deduplicated: true };
      }

      // A resumed claim already has its reservation document; reuse the id so a retry does
      // not produce a second reservation for one request.
      const existing = await this.db.collection(RESERVATIONS)
        .findOne({ idempotency_key: idempotencyKey }, { session });
      const reservationId = existing?._id ?? randomUUID();

      const shortfalls = [];
      for (const line of lines) {
        const res = await this.db.collection(STOCK).updateOne(
          {
            _id: line.sku,
            $expr: { $gte: [{ $subtract: ['$on_hand', '$reserved'] }, line.quantity] },
          },
          { $inc: { reserved: line.quantity }, $set: { updated_at: new Date() } },
          { session },
        );
        if (res.matchedCount === 0) {
          shortfalls.push(line);
        }
      }

      if (shortfalls.length > 0) {
        // Abort. Every increment applied above is undone, which is why the conditional
        // updates can be applied optimistically instead of pre-checking everything first.
        refusal = { reservationId, shortfallLines: shortfalls };
        throw new AbortForRefusal();
      }

      const now = new Date();
      const expiresAt = new Date(now.getTime() + ttlMs);
      await this.db.collection(RESERVATIONS).updateOne(
        { _id: reservationId },
        {
          $set: {
            order_id: orderId,
            idempotency_key: idempotencyKey,
            state: ReservationState.Held,
            lines,
            expires_at: expiresAt,
            updated_at: now,
          },
          $setOnInsert: { created_at: now },
        },
        { upsert: true, session },
      );

      await outbox.enqueue(this.db, session, newEnvelope({
        eventType: 'inventory.v1.StockReserved',
        aggregateType: 'reservation',
        aggregateId: reservationId,
        producer: PRODUCER,
        correlationId,
        causationId,
        data: {
          reservation_id: reservationId,
          order_id: orderId,
          lines,
          idempotency_key: idempotencyKey,
          expires_at: expiresAt.toISOString(),
          reserved_at: now.toISOString(),
        },
      }));

      const result = {
        outcome: Outcome.Reserved,
        reservation_id: reservationId,
        state: ReservationState.Held,
        expires_at: expiresAt.toISOString(),
        shortfalls: [],
      };
      await idempotency.complete(
        this.db, session, idempotency.Scope.ReserveStock, idempotencyKey, result,
      );
      return { ...result, deduplicated: false };
    }).catch((err) => {
      if (err instanceof AbortForRefusal) return null;
      throw err;
    });

    if (held) return held;
    return this.#recordRefusal({ refusal, idempotencyKey, orderId, lines, correlationId, causationId });
  }

  /**
   * Record a reservation that could not be satisfied.
   *
   * A refused reservation is a persisted FAILED document, not the absence of one. A retry
   * with the same key must return the same refusal — re-running the availability check
   * would give a different answer as soon as stock moved, and a caller retrying a
   * request must never get a different outcome for the same request.
   */
  async #recordRefusal({ refusal, idempotencyKey, orderId, lines, correlationId, causationId }) {
    return this.#inTx(async (session) => {
      const claimed = await idempotency.claim(
        this.db, session, idempotency.Scope.ReserveStock, idempotencyKey,
        { order_id: orderId, lines },
      );
      if (claimed.status === idempotency.Status.Completed) {
        return { ...claimed.response, deduplicated: true };
      }

      // Availability as observed now, for every line that was short. Advisory: by the time
      // the caller reads it the figure may already have changed.
      const shortfalls = [];
      let unknownSku = false;
      for (const line of refusal.shortfallLines) {
        const item = await this.db.collection(STOCK).findOne({ _id: line.sku }, { session });
        if (!item) {
          unknownSku = true;
          shortfalls.push({ sku: line.sku, requested: line.quantity, available: 0 });
        } else {
          shortfalls.push({
            sku: line.sku,
            requested: line.quantity,
            available: Math.max(0, item.on_hand - item.reserved),
          });
        }
      }

      const now = new Date();
      await this.db.collection(RESERVATIONS).updateOne(
        { _id: refusal.reservationId },
        {
          $set: {
            order_id: orderId,
            idempotency_key: idempotencyKey,
            state: ReservationState.Failed,
            lines,
            shortfalls,
            updated_at: now,
          },
          $setOnInsert: { created_at: now },
        },
        { upsert: true, session },
      );

      await outbox.enqueue(this.db, session, newEnvelope({
        eventType: 'inventory.v1.StockReservationFailed',
        aggregateType: 'reservation',
        aggregateId: refusal.reservationId,
        producer: PRODUCER,
        correlationId,
        causationId,
        data: {
          reservation_id: refusal.reservationId,
          order_id: orderId,
          lines,
          shortfalls,
          idempotency_key: idempotencyKey,
          failed_at: now.toISOString(),
        },
      }));

      const result = {
        outcome: unknownSku ? Outcome.UnknownSku : Outcome.InsufficientStock,
        reservation_id: refusal.reservationId,
        state: ReservationState.Failed,
        shortfalls,
      };
      await idempotency.complete(
        this.db, session, idempotency.Scope.ReserveStock, idempotencyKey, result,
      );
      this.log.info('reservation refused', {
        order_id: orderId, reservation_id: refusal.reservationId,
        correlation_id: correlationId, shortfalls,
      });
      return { ...result, deduplicated: false };
    });
  }

  // --- release ------------------------------------------------------------

  /**
   * Give a claim back. Compensation for reserveStock.
   *
   * "Already released" and "no such reservation" are both success. A compensation that
   * finds its work already done has succeeded, and treating that as an error would make
   * retrying compensation unsafe — which would in turn make the whole saga unsafe.
   */
  async releaseStock({ idempotencyKey, reservationId, reason, correlationId, causationId }) {
    return this.#inTx(async (session) => {
      const claimed = await idempotency.claim(
        this.db, session, idempotency.Scope.ReleaseStock, idempotencyKey,
        { reservation_id: reservationId },
      );
      if (claimed.status === idempotency.Status.Completed) {
        return { ...claimed.response, deduplicated: true };
      }

      const reservation = await this.db.collection(RESERVATIONS)
        .findOne({ _id: reservationId }, { session });

      let result;
      if (!reservation) {
        result = { outcome: Outcome.NotFound, state: null };
      } else if (reservation.state === ReservationState.Committed) {
        // The units are gone. Releasing is impossible and this is a genuine saga bug that
        // the caller must not paper over.
        result = { outcome: Outcome.AlreadyCommitted, state: reservation.state };
      } else if (reservation.state !== ReservationState.Held) {
        result = { outcome: Outcome.Released, state: reservation.state };
      } else {
        for (const line of reservation.lines) {
          await this.db.collection(STOCK).updateOne(
            { _id: line.sku },
            { $inc: { reserved: -line.quantity }, $set: { updated_at: new Date() } },
            { session },
          );
        }
        const now = new Date();
        await this.db.collection(RESERVATIONS).updateOne(
          { _id: reservationId },
          { $set: { state: ReservationState.Released, released_reason: reason, updated_at: now } },
          { session },
        );

        await outbox.enqueue(this.db, session, newEnvelope({
          eventType: 'inventory.v1.StockReleased',
          aggregateType: 'reservation',
          aggregateId: reservationId,
          producer: PRODUCER,
          correlationId,
          causationId,
          data: {
            reservation_id: reservationId,
            order_id: reservation.order_id,
            lines: reservation.lines,
            reason: reason || 'COMPENSATION',
            released_at: now.toISOString(),
          },
        }));
        result = { outcome: Outcome.Released, state: ReservationState.Released };
      }

      await idempotency.complete(
        this.db, session, idempotency.Scope.ReleaseStock, idempotencyKey, result,
      );
      return { ...result, deduplicated: false };
    });
  }

  // --- commit -------------------------------------------------------------

  /**
   * Turn a claim into a deduction. on_hand and reserved both fall, so availability is
   * unchanged — the units simply stop being ours.
   *
   * An expired reservation is REFUSED rather than committed. The units may since have been
   * sold to someone else, so committing would oversell. Refusing turns a potential oversell
   * into a cancelled order, which is the correct direction for that trade: an order the
   * customer does not get is recoverable, an oversell is not.
   */
  async commitReservation({ idempotencyKey, reservationId, correlationId, causationId }) {
    return this.#inTx(async (session) => {
      const claimed = await idempotency.claim(
        this.db, session, idempotency.Scope.CommitReservation, idempotencyKey,
        { reservation_id: reservationId },
      );
      if (claimed.status === idempotency.Status.Completed) {
        return { ...claimed.response, deduplicated: true };
      }

      const reservation = await this.db.collection(RESERVATIONS)
        .findOne({ _id: reservationId }, { session });

      let result;
      if (!reservation) {
        result = { outcome: Outcome.NotFound, state: null };
      } else if (reservation.state === ReservationState.Committed) {
        result = { outcome: Outcome.Committed, state: reservation.state };
      } else if (reservation.state === ReservationState.Released) {
        result = { outcome: Outcome.Released, state: reservation.state };
      } else if (reservation.state === ReservationState.Expired
        || (reservation.expires_at && reservation.expires_at < new Date())) {
        result = { outcome: Outcome.Expired, state: ReservationState.Expired };
      } else {
        for (const line of reservation.lines) {
          await this.db.collection(STOCK).updateOne(
            { _id: line.sku },
            {
              $inc: { on_hand: -line.quantity, reserved: -line.quantity },
              $set: { updated_at: new Date() },
            },
            { session },
          );
        }
        const now = new Date();
        await this.db.collection(RESERVATIONS).updateOne(
          { _id: reservationId },
          { $set: { state: ReservationState.Committed, committed_at: now, updated_at: now } },
          { session },
        );

        await outbox.enqueue(this.db, session, newEnvelope({
          eventType: 'inventory.v1.StockCommitted',
          aggregateType: 'reservation',
          aggregateId: reservationId,
          producer: PRODUCER,
          correlationId,
          causationId,
          data: {
            reservation_id: reservationId,
            order_id: reservation.order_id,
            lines: reservation.lines,
            committed_at: now.toISOString(),
          },
        }));
        result = { outcome: Outcome.Committed, state: ReservationState.Committed };
      }

      await idempotency.complete(
        this.db, session, idempotency.Scope.CommitReservation, idempotencyKey, result,
      );
      return { ...result, deduplicated: false };
    });
  }

  // --- reads --------------------------------------------------------------

  /**
   * Read a reservation, by id or by the idempotency key that created it.
   *
   * Lookup by key is the one that matters: a caller resolving a timed-out reserveStock
   * never received the reservation id, and asking is the only alternative to guessing.
   */
  async getReservation({ reservationId, idempotencyKey }) {
    const filter = reservationId ? { _id: reservationId } : { idempotency_key: idempotencyKey };
    return this.db.collection(RESERVATIONS).findOne(filter);
  }

  async stockFor(skus) {
    const filter = skus?.length ? { _id: { $in: skus } } : {};
    return this.db.collection(STOCK).find(filter).sort({ _id: 1 }).toArray();
  }

  // --- expiry -------------------------------------------------------------

  /**
   * Reclaim reservations that were never committed or released.
   *
   * A safety net against reservations leaked by a dead saga; without it, stock would
   * slowly drain away and never come back. Its firing during a healthy saga means the TTL
   * is too short, which is why reserved_duration_ms is published — so TTL tuning is driven
   * by measurement rather than guesswork.
   */
  async sweepExpired(limit = 100) {
    const due = await this.db.collection(RESERVATIONS)
      .find({ state: ReservationState.Held, expires_at: { $lt: new Date() } })
      .sort({ expires_at: 1 })
      .limit(limit)
      .toArray();

    let reclaimed = 0;
    for (const reservation of due) {
      await this.#inTx(async (session) => {
        // Re-read under the transaction: another caller may have committed or released it
        // between the scan above and this write.
        const current = await this.db.collection(RESERVATIONS)
          .findOne({ _id: reservation._id, state: ReservationState.Held }, { session });
        if (!current) return;

        for (const line of current.lines) {
          await this.db.collection(STOCK).updateOne(
            { _id: line.sku },
            { $inc: { reserved: -line.quantity }, $set: { updated_at: new Date() } },
            { session },
          );
        }
        const now = new Date();
        await this.db.collection(RESERVATIONS).updateOne(
          { _id: current._id },
          { $set: { state: ReservationState.Expired, expired_at: now, updated_at: now } },
          { session },
        );

        await outbox.enqueue(this.db, session, newEnvelope({
          eventType: 'inventory.v1.ReservationExpired',
          aggregateType: 'reservation',
          aggregateId: current._id,
          producer: PRODUCER,
          correlationId: current.correlation_id ?? randomUUID(),
          data: {
            reservation_id: current._id,
            order_id: current.order_id,
            lines: current.lines,
            expired_at: now.toISOString(),
            reserved_duration_ms: Math.max(0, now.getTime() - new Date(current.created_at).getTime()),
          },
        }));
        reclaimed += 1;
      });
    }

    if (reclaimed > 0) {
      this.log.warn('reclaimed expired reservations', {
        count: reclaimed,
        hint: 'a healthy saga should never leave one; check the reservation TTL against the saga budget',
      });
    }
    return reclaimed;
  }
}

/** Internal signal used to abort a transaction deliberately rather than on an error. */
class AbortForRefusal extends Error {
  constructor() {
    super('inventory: aborting to record a refusal');
    this.name = 'AbortForRefusal';
  }
}
