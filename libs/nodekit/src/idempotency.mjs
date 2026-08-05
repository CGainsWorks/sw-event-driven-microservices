/**
 * MongoDB idempotency, parity with libs/idempotency in Go.
 *
 * Same three shapes, same reasoning:
 *
 *   claim() / complete()  for work that makes an external call. The attempt is recorded
 *                         and committed BEFORE the call, so a crash mid-call leaves
 *                         evidence the request may have gone out.
 *   withIdempotency()     for work that fits in one transaction.
 *   seen()                for deduplicating consumed events by event_id.
 *
 * The record of "this was done" commits in the same transaction as the work. If the two
 * were split, there would be a window where one had committed and the other had not, and
 * that window is exactly the crash that causes a double charge.
 */

import { createHash } from 'node:crypto';

export const COLLECTION = 'idempotency_keys';

/** Scopes, matching the Go constants. A typo must not become a silently separate key space. */
export const Scope = {
  ReserveStock: 'inventory.reserve',
  ReleaseStock: 'inventory.release',
  CommitReservation: 'inventory.commit',
  Event: 'event',
};

export const Status = {
  /** No previous attempt. Do the work. */
  New: 'new',
  /** A previous attempt claimed the key and died. Do the work again; downstream calls are keyed. */
  Resume: 'resume',
  /** An earlier attempt finished. Return its stored response and do nothing else. */
  Completed: 'completed',
};

/**
 * How long an in-progress claim must sit untouched before another attempt may take it over.
 *
 * Comfortably longer than any deadline in this system, so a claim older than this belongs
 * to a process that is not coming back. Resumption is only safe because the work being
 * resumed is itself keyed, so repeating it is a no-op. That assumption is the first thing
 * to check if this number is ever changed.
 */
export const DEFAULT_RESUME_AFTER_MS = 30_000;

export class KeyReuseError extends Error {
  constructor(scope, key) {
    super(`idempotency: key reused with a different request: scope=${scope} key=${key}`);
    this.name = 'KeyReuseError';
    this.scope = scope;
    this.key = key;
  }
}

export class InProgressError extends Error {
  constructor(scope, key) {
    super(`idempotency: an identical request is in progress: scope=${scope} key=${key}`);
    this.name = 'InProgressError';
    this.scope = scope;
    this.key = key;
  }
}

/**
 * Deterministic fingerprint of a request.
 *
 * Keys are sorted so that two structurally identical objects hash the same regardless of
 * property insertion order — JSON.stringify preserves insertion order, which would make
 * the same request hash differently depending on how it was built.
 *
 * Callers must hash only the business-meaningful fields. Including correlation or trace ids
 * would make two genuine retries of one request look like two different requests, which
 * defeats the entire mechanism.
 */
export function hashRequest(request) {
  const canonical = JSON.stringify(request, (_k, v) => {
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      return Object.fromEntries(Object.entries(v).sort(([a], [b]) => (a < b ? -1 : 1)));
    }
    return v;
  });
  return createHash('sha256').update(canonical ?? 'null').digest('hex');
}

const docId = (scope, key) => `${scope}:${key}`;

export async function ensureIndexes(db) {
  // created_at supports the retention sweep; records are kept far longer than any caller
  // will retry, but not forever. See scripts/prune_idempotency.js.
  await db.collection(COLLECTION).createIndex({ created_at: 1 });
  await db.collection(COLLECTION).createIndex({ state: 1, claimed_at: 1 });
}

/**
 * Reserve (scope, key) for this attempt.
 *
 * Implemented as a single upsert with an aggregation-pipeline update, which is what makes
 * it atomic. The pipeline is the equivalent of the SQL version's `ON CONFLICT DO UPDATE
 * ... SET claimed_at = CASE WHEN stale THEN now() ELSE claimed_at END`: it refreshes
 * claimed_at if and only if the existing claim is stale, so taking over a dead attempt and
 * observing a live one are decided in the same indivisible operation.
 *
 * `returnDocument: 'before'` returns null on insert, which is how a fresh claim is told
 * apart from a contended one without a second read.
 *
 * @returns {Promise<{status: string, response: any}>}
 */
export async function claim(db, session, scope, key, request, resumeAfterMs = DEFAULT_RESUME_AFTER_MS) {
  if (!key) throw new Error(`idempotency: empty key for scope ${scope}`);

  const hash = hashRequest(request);
  const now = new Date();
  const staleBefore = new Date(now.getTime() - resumeAfterMs);

  const before = await db.collection(COLLECTION).findOneAndUpdate(
    { _id: docId(scope, key) },
    [
      {
        $set: {
          scope,
          key,
          request_hash: { $ifNull: ['$request_hash', hash] },
          state: { $ifNull: ['$state', 'in_progress'] },
          created_at: { $ifNull: ['$created_at', now] },
          claimed_at: {
            $cond: [
              {
                $and: [
                  { $eq: [{ $ifNull: ['$state', 'in_progress'] }, 'in_progress'] },
                  { $lt: [{ $ifNull: ['$claimed_at', now] }, staleBefore] },
                ],
              },
              now,
              { $ifNull: ['$claimed_at', now] },
            ],
          },
        },
      },
    ],
    { upsert: true, returnDocument: 'before', session },
  );

  if (before === null) return { status: Status.New, response: null };

  if (before.request_hash !== hash) throw new KeyReuseError(scope, key);
  if (before.state === 'completed') {
    return { status: Status.Completed, response: before.response ?? null };
  }

  // Still in progress. The pipeline refreshed claimed_at only if the previous claim was
  // stale, so a stale `before` means this attempt has just taken it over.
  if (before.claimed_at < staleBefore) return { status: Status.Resume, response: null };
  throw new InProgressError(scope, key);
}

/**
 * Record the outcome of a claimed operation.
 *
 * Call it in the transaction that also writes the domain change and the outbox document,
 * so that "it happened", "here is what happened" and "tell everyone it happened" commit
 * together.
 */
export async function complete(db, session, scope, key, response) {
  const res = await db.collection(COLLECTION).updateOne(
    { _id: docId(scope, key) },
    { $set: { state: 'completed', response: response ?? null, completed_at: new Date() } },
    { session },
  );
  if (res.matchedCount === 0) {
    // The claim vanished. The caller is about to commit domain state for an operation
    // with no dedup record, which would let it run twice.
    throw new Error(`idempotency: no claim found for ${scope}/${key}; refusing to complete`);
  }
}

/**
 * Drop a claim so the operation can genuinely be attempted again.
 *
 * Call it when an attempt failed in a way worth retrying. Without it the claim stays in
 * progress, the stored answer is replayed on every retry, and a thirty-second outage turns
 * into a permanently failed request. That bug is subtle and expensive: everything looks
 * like it is retrying, and nothing is.
 *
 * Only TERMINAL outcomes are memoised. "The dependency did not answer" is not an outcome.
 */
export async function release(db, session, scope, key) {
  await db.collection(COLLECTION).deleteOne(
    { _id: docId(scope, key), state: 'in_progress' },
    { session },
  );
}

/**
 * Run fn at most once for (scope, key), entirely inside the caller's transaction.
 *
 * Use it when fn touches nothing outside this database. If fn calls another service, use
 * claim() and complete() instead: a single transaction wrapping an external call cannot
 * record that the call was attempted, because a rollback erases the record while the call
 * has already gone out.
 */
export async function withIdempotency(db, session, scope, key, request, fn) {
  const claimed = await claim(db, session, scope, key, request);
  if (claimed.status === Status.Completed) {
    return { value: claimed.response, deduplicated: true };
  }
  const value = await fn();
  await complete(db, session, scope, key, value);
  return { value, deduplicated: false };
}

/**
 * Record an event_id as processed and report whether it had already been seen.
 *
 * Call it inside the transaction that does the handler's work: if that transaction rolls
 * back, the event is not marked seen and will be retried, which is what makes redelivery
 * safe.
 *
 * @returns {Promise<boolean>} true if this event was already processed
 */
export async function seen(db, session, eventId) {
  const before = await db.collection(COLLECTION).findOneAndUpdate(
    { _id: docId(Scope.Event, eventId) },
    {
      $setOnInsert: {
        scope: Scope.Event,
        key: eventId,
        request_hash: '',
        state: 'completed',
        created_at: new Date(),
        claimed_at: new Date(),
        completed_at: new Date(),
      },
    },
    { upsert: true, returnDocument: 'before', session },
  );
  // null means it was inserted now, so this is the first sighting.
  return before !== null;
}
