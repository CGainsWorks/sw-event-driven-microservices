/**
 * Unit tests for the MongoDB idempotency primitives.
 *
 * The Go side already has these. The Node side did not, and the two implementations are NOT
 * the same shape: Go does insert-then-`SELECT ... FOR UPDATE`, Mongo does a single
 * aggregation-pipeline upsert with `returnDocument: 'before'`. Different mechanisms with
 * the same contract, so "the Go tests pass" says nothing about this one.
 *
 * A real MongoDB is required, because the whole mechanism IS database behaviour — the
 * atomicity of the upsert, and the snapshot isolation that decides what a concurrent
 * duplicate sees. A fake would test the wrapper and skip every part that can be wrong.
 *
 * Skips when SW_EDM_TEST_MONGO_URI is unset, so `npm test` stays hermetic:
 *
 *   SW_EDM_TEST_MONGO_URI='mongodb://localhost:27017/?replicaSet=rs0&directConnection=true' \
 *     node --test libs/nodekit
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';

import { MongoClient } from 'mongodb';

import {
  claim, complete, release, withIdempotency, seen,
  Status, KeyReuseError, InProgressError, ensureIndexes, COLLECTION,
} from './idempotency.mjs';

const URI = process.env.SW_EDM_TEST_MONGO_URI;
const SCOPE = 'test.scope';

let client;
let db;

/** A fresh key per test, so tests never depend on each other or on leftovers. */
const freshKey = () => `test:${randomUUID()}`;

/**
 * Run fn in a transaction.
 *
 * NOTE: MongoDB's withTransaction retries on transient errors, so fn CAN run more than
 * once. That is why the tests below count committed executions with a counter written
 * inside the transaction rather than counting invocations — a retried-and-rolled-back
 * attempt did not happen, and counting it would make a correct implementation look broken.
 */
async function inTx(fn) {
  const session = client.startSession();
  try {
    return await session.withTransaction(async () => fn(session), {
      readConcern: { level: 'snapshot' },
      writeConcern: { w: 'majority' },
    });
  } finally {
    await session.endSession();
  }
}

/** Counts only work that actually committed. */
async function countExecution(session, tag) {
  await db.collection('test_executions').updateOne(
    { _id: tag },
    { $inc: { count: 1 } },
    { upsert: true, session },
  );
}

const executions = async (tag) =>
  (await db.collection('test_executions').findOne({ _id: tag }))?.count ?? 0;

describe('mongodb idempotency', { skip: URI ? false : 'SW_EDM_TEST_MONGO_URI is not set' }, () => {
  before(async () => {
    client = new MongoClient(URI, { serverSelectionTimeoutMS: 5000 });
    await client.connect();
    db = client.db('nodekit_idempotency_test');
    await ensureIndexes(db);
  });

  after(async () => {
    if (db) await db.dropDatabase().catch(() => {});
    if (client) await client.close();
  });

  it('runs the work once and replays the first answer', async () => {
    const key = freshKey();
    const tag = randomUUID();
    const request = { order: 'order-1', amount: 1250 };
    const results = [];

    for (let i = 0; i < 2; i += 1) {
      const out = await inTx(async (session) =>
        withIdempotency(db, session, SCOPE, key, request, async () => {
          await countExecution(session, tag);
          return { outcome: 'AUTHORIZED', id: 'payment-1' };
        }));
      results.push(out);
    }

    assert.equal(await executions(tag), 1, 'the work committed more than once');
    assert.equal(results[0].deduplicated, false);
    assert.equal(results[1].deduplicated, true, 'the second call was not reported as deduplicated');
    // The caller retried because it never learned the answer, so it needs the ANSWER, not
    // an acknowledgement.
    assert.deepEqual(results[1].value, { outcome: 'AUTHORIZED', id: 'payment-1' });
  });

  it('runs the work exactly once under concurrent duplicates', async () => {
    const key = freshKey();
    const tag = randomUUID();
    const request = { order: 'order-2', amount: 999 };
    const attempts = 8;

    const settled = await Promise.allSettled(
      Array.from({ length: attempts }, () =>
        inTx(async (session) =>
          withIdempotency(db, session, SCOPE, key, request, async () => {
            await countExecution(session, tag);
            return { outcome: 'AUTHORIZED', id: 'payment-2' };
          }))),
    );

    // THE assertion. Anything above one is a double charge.
    assert.equal(await executions(tag), 1,
      `the work committed ${await executions(tag)} times across ${attempts} concurrent duplicates`);

    // Every attempt either got the answer or was told to try again. Nothing else is
    // acceptable — an unexpected error here means the race is not handled, it is just rare.
    for (const r of settled) {
      if (r.status === 'fulfilled') {
        assert.deepEqual(r.value.value, { outcome: 'AUTHORIZED', id: 'payment-2' });
      } else {
        assert.ok(
          r.reason instanceof InProgressError || r.reason?.name === 'MongoServerError',
          `unexpected failure: ${r.reason}`,
        );
      }
    }
  });

  it('rejects a key reused with a different request', async () => {
    const key = freshKey();
    const tag = randomUUID();

    await inTx(async (session) =>
      withIdempotency(db, session, SCOPE, key, { order: 'order-3', amount: 100 },
        async () => ({ outcome: 'OK' })));

    await assert.rejects(
      () => inTx(async (session) =>
        withIdempotency(db, session, SCOPE, key, { order: 'order-3', amount: 999 },
          async () => {
            await countExecution(session, tag);
            return { outcome: 'OK' };
          })),
      KeyReuseError,
      'a reused key with a different request was silently accepted, discarding the second request',
    );
    assert.equal(await executions(tag), 0, 'the work ran for a reused key');
  });

  it('leaves no claim behind when the transaction rolls back', async () => {
    const key = freshKey();
    const tag = randomUUID();
    const request = { order: 'order-4', amount: 500 };
    const boom = new Error('the work failed');

    await assert.rejects(
      () => inTx(async (session) =>
        withIdempotency(db, session, SCOPE, key, request, async () => {
          throw boom;
        })),
      /the work failed/,
    );

    // Nothing happened, so the retry must genuinely retry.
    const out = await inTx(async (session) =>
      withIdempotency(db, session, SCOPE, key, request, async () => {
        await countExecution(session, tag);
        return { outcome: 'AUTHORIZED' };
      }));

    assert.equal(await executions(tag), 1, 'the retry did not run; a failed attempt left a claim');
    assert.equal(out.deduplicated, false);
  });

  it('lets release turn a transient failure back into a genuine retry', async () => {
    // The bug this guards against: a retryable failure recorded under the key replays
    // "unavailable" forever, and a thirty-second outage becomes a permanently failed
    // order. From the outside it looks exactly like a working retry loop.
    const key = freshKey();
    const request = { order: 'order-5', amount: 750 };

    await inTx(async (session) => {
      const first = await claim(db, session, SCOPE, key, request);
      assert.equal(first.status, Status.New);
      await release(db, session, SCOPE, key);
    });

    await inTx(async (session) => {
      const second = await claim(db, session, SCOPE, key, request);
      assert.equal(second.status, Status.New,
        'a released claim was still remembered; a transient failure was memoised');
      await complete(db, session, SCOPE, key, { outcome: 'AUTHORIZED' });
    });

    // And a TERMINAL outcome is memoised, which is the other half of the rule.
    await inTx(async (session) => {
      const third = await claim(db, session, SCOPE, key, request);
      assert.equal(third.status, Status.Completed);
      assert.deepEqual(third.response, { outcome: 'AUTHORIZED' });
    });
  });

  it('reports a fresh claim as in progress and a stale one as resumable', async () => {
    const key = freshKey();
    const request = { order: 'order-6', amount: 300 };

    // A claim that commits and is then abandoned: the process died between claiming and
    // completing, which is exactly what Claim/Complete exists to survive.
    await inTx(async (session) => {
      const first = await claim(db, session, SCOPE, key, request, 60_000);
      assert.equal(first.status, Status.New);
    });

    // Still fresh: another attempt must be told to wait, not take over.
    await assert.rejects(
      () => inTx(async (session) => claim(db, session, SCOPE, key, request, 60_000)),
      InProgressError,
      'a live claim was stolen',
    );

    // Stale: it may be resumed. resumeAfter of 1ms rather than waiting out the default.
    await inTx(async (session) => {
      const resumed = await claim(db, session, SCOPE, key, request, 1);
      assert.equal(resumed.status, Status.Resume,
        'a stale claim was not resumable; its key would be blocked forever');
      await complete(db, session, SCOPE, key, { outcome: 'AUTHORIZED' });
    });
  });

  it('detects redelivery of an event', async () => {
    const eventId = randomUUID();
    const sightings = [];
    for (let i = 0; i < 3; i += 1) {
      sightings.push(await inTx(async (session) => seen(db, session, eventId)));
    }
    assert.equal(sightings[0], false, 'the first sighting was reported as already seen');
    assert.deepEqual(sightings.slice(1), [true, true], 'redeliveries were reported as unseen');
  });

  it('does not record an event whose handler transaction rolled back', async () => {
    // If it did, the event would never be retried and the work would be lost — which is
    // the opposite of the failure everything else here guards against.
    const eventId = randomUUID();
    const boom = new Error('handler failed');

    await assert.rejects(
      () => inTx(async (session) => {
        await seen(db, session, eventId);
        throw boom;
      }),
      /handler failed/,
    );

    const already = await inTx(async (session) => seen(db, session, eventId));
    assert.equal(already, false,
      'the event was recorded as processed despite its transaction rolling back');
  });

  it('stores the response under the (scope, key) pair, not the key alone', async () => {
    // Scopes namespace the key space so an inventory reservation key and a payment
    // authorization key cannot collide. Without this, one service completing an operation
    // would silently satisfy another's.
    const key = freshKey();
    const request = { order: 'order-7' };

    await inTx(async (session) =>
      withIdempotency(db, session, 'scope.a', key, request, async () => ({ from: 'a' })));
    const b = await inTx(async (session) =>
      withIdempotency(db, session, 'scope.b', key, request, async () => ({ from: 'b' })));

    assert.equal(b.deduplicated, false, 'a different scope was treated as the same key');
    assert.deepEqual(b.value, { from: 'b' });

    const docs = await db.collection(COLLECTION).find({ key }).toArray();
    assert.equal(docs.length, 2, 'the two scopes did not produce two independent records');
  });
});
