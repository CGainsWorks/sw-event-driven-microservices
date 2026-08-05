# Idempotency strategy

Everything in this system is retried. Commands are retried because a caller's first attempt
timed out; events are redelivered because the outbox publishes at least once and Kafka
redelivers on rebalance; whole sagas are resumed because an orchestrator died. **A handler
that cannot tolerate being run twice is a bug, not a configuration issue.**

Implementations: [`libs/idempotency`](../libs/idempotency/idempotency.go) (Go),
[`libs/nodekit/src/idempotency.mjs`](../libs/nodekit/src/idempotency.mjs) (Node).

---

## 1. Three problems, one mechanism

| Problem | Key | API |
|---|---|---|
| A command arrives twice because the caller retried | caller-chosen `idempotency_key` | `Claim` / `Complete` |
| An event arrives twice from Kafka | `event_id` | `Seen` |
| A command involves an **external call** that must happen at most once | caller-chosen key | `Claim` / `Complete`, split across two transactions |

All three are *"I have seen this key before, here is what happened."* Only the key differs.

```sql
CREATE TABLE idempotency_keys (
    scope         TEXT NOT NULL,   -- namespaces the key space
    key           TEXT NOT NULL,
    request_hash  TEXT NOT NULL,   -- detects key reuse with a different request
    state         TEXT NOT NULL,   -- in_progress | completed
    response      JSONB,           -- what to replay
    claimed_at    TIMESTAMPTZ NOT NULL,
    ...
    PRIMARY KEY (scope, key)
);
```

The MongoDB shape is the same document, keyed `_id = "<scope>:<key>"`.

---

## 2. Four decisions

### It lives in the database, not a cache

The record of *"this was done"* must commit in the same transaction as the work itself. If
deduplication lives in Redis and the work lives in PostgreSQL, there is a window where one
has committed and the other has not — **and that window is exactly the crash that causes a
double charge.** Same database, same transaction, no window.

### The response is stored, not just a flag

Returning "already done" is not enough. The caller retried *because it never learned the
answer*, so it needs the answer, not an acknowledgement. The serialised response is stored
and replayed, which makes the second call indistinguishable from the first as far as the
caller is concerned. That is what idempotent actually means.

### Keys are stable across retries, and that is the whole mechanism

The key for a saga step is `<order_id>:<step_name>:1` — **fixed, not per-attempt.**

A key that varied per attempt would make each retry a new request. A timed-out
`AuthorizePayment` followed by a retry would then charge the customer twice, and every other
guarantee in this document would be worthless.

The same stability is what makes `GetPaymentByIdempotencyKey` able to answer "did my request
arrive?" — see [`sagas.md` §4](sagas.md#4-a-timeout-is-not-an-outcome).

### The claim is an upsert that also locks

```sql
INSERT INTO idempotency_keys (scope, key, request_hash, state)
VALUES ($1, $2, $3, 'in_progress')
ON CONFLICT (scope, key) DO UPDATE SET claimed_at = CASE ... END
RETURNING request_hash, state, response, claimed_at, (xmax = 0) AS inserted
```

`ON CONFLICT DO UPDATE`, not `DO NOTHING`. Two reasons:

1. It always returns a row, so the existing record is read in one round trip.
2. **It takes a row lock.** A concurrent duplicate blocks there until the first transaction
   commits, then reads the true state. `DO NOTHING` neither returns a row nor locks, and
   loses that race.

`(xmax = 0)` distinguishes an insert from an update — zero on a freshly inserted tuple,
non-zero on one `ON CONFLICT` has just locked. MongoDB gets the same behaviour from an
aggregation-pipeline update with `returnDocument: 'before'`, where `null` means "inserted".

---

## 3. Only terminal outcomes are memoised

**This is the subtlest thing here, and getting it wrong is invisible.**

A declined card is terminal: retrying will never change it, so it is recorded and replayed
identically forever. But *"the provider did not answer"* is **not an outcome at all**. If it
is recorded under the key, every retry replays `PSP_UNAVAILABLE`, and a thirty-second
provider outage becomes a permanently failed order.

From the outside, that looks exactly like a working retry loop. The saga retries on
schedule, the logs show attempts, nothing errors — and the order never completes.

So a retryable failure calls `Release`, which drops the claim so the next attempt genuinely
reaches the provider:

| Outcome | Memoised? | Why |
|---|---|---|
| `CARD_DECLINED`, `INSUFFICIENT_FUNDS`, `CARD_EXPIRED`, `FRAUD_SUSPECTED` | **Yes** | Terminal. The retry must get the same answer. |
| `PSP_UNAVAILABLE`, `INTERNAL_ERROR` | **No** — claim released | Nothing was decided. Recording it would make a transient failure permanent. |
| Success | **Yes** | Obviously. |

This was a real bug in this repository, found by the flaky-provider test and fixed in
`services/payments/internal/core/service.go`.
`TestTransientFailureIsRetriedNotMemoised` is the regression test: the simulator fails twice
then succeeds, so a correct implementation reaches `CONFIRMED` and an incorrect one spins
until the test times out.

---

## 4. Two transactions, when there is an external call

A single transaction wrapping a provider call cannot record that the call was attempted: a
crash rolls the record back while the call has already gone out. So the attempt is
committed **before** the call:

```
tx1   Claim the key, write a PENDING payment row, COMMIT
      └─ this commit is the durable evidence the request may have gone out
psp   Call the provider, outside any transaction
tx2   Record the answer + ledger entry + outbox event + Complete, COMMIT
      └─ state and the telling of it cannot diverge
```

Without `tx1`, a retry after a crash cannot tell *"never sent"* from *"sent, answer lost"* —
and those need opposite handling. It is also what lets `GetPaymentByIdempotencyKey` answer a
caller whose call timed out.

The provider call itself is keyed with the same idempotency key, so a real PSP deduplicates
it. **That assumption is what makes the resume path safe**, and it is the first thing to
check if the resume threshold is ever changed.

### Stale claims

A claim whose process died would otherwise block its key forever. A claim untouched for
longer than `DefaultResumeAfter` (30s, comfortably beyond every deadline in the system) may
be taken over, and the takeover happens inside the same atomic upsert.

Honest limitation: **two callers could both resume after the threshold.** It is safe only
because the downstream call is keyed and therefore a no-op the second time. That is a real
assumption, not a proof.

---

## 5. Where it is enforced

Deduplication is defence in depth, not one check:

| Layer | Mechanism |
|---|---|
| Application | `Claim`/`Complete` on `idempotency_keys` |
| Database | `payments.idempotency_key` UNIQUE — one key, one authorization, forever |
| Database | `ledger_entries` UNIQUE `(entry_type, idempotency_key)` — a retried capture cannot write a second ledger row even if every other guard fails |
| Database | `reservations.idempotency_key` UNIQUE |
| Database | `notifications` UNIQUE `(source_event_id, channel)` — the invariant that stops a redelivery emailing twice |

An application check has a window between reading and writing. **A unique index does not.**
Where the cost of being wrong is a double charge, both are used.

---

## 6. Failed attempts are remembered too

A refused reservation persists a `FAILED` reservation with an allocated id. A declined
authorization persists a `FAILED` payment with an allocated id. Neither is the *absence* of
a record.

That matters because otherwise a retry would re-run the availability check against stock
that has since changed, and a caller retrying one request would get a different answer for
the same request. `reservation_id` and `payment_id` are therefore present on failure
responses and on `StockReservationFailed` and `PaymentFailed` events.

---

## 7. Evidence

| Test | Proves |
|---|---|
| `tests/chaos` · duplicate delivery | A real `OrderConfirmed` republished **verbatim 3×** with the same `event_id` produces **exactly one** notification, same id |
| `tests/integration` · `TestNotificationIsNotDuplicated` | Still one notification after the system settles |
| `tests/integration` · `TestTransientFailureIsRetriedNotMemoised` | A transient provider failure is retried, not replayed |
| `tests/chaos` · network partition | No duplicate payment after a 40s partition and recovery |
| `libs/idempotency` · unit tests | **8 concurrent claims on one key run the work exactly once**, plus key reuse, clean retry after a rolled-back attempt, release-then-retry, stale-claim resume, and event redelivery |

Responses carry a `deduplicated` flag purely so tests can prove deduplication *happened*
rather than inferring it from state that would look identical either way.

---

## 8. Limitations

- **`request_hash` is not stable across a change to the request struct's shape.** Adding a
  field changes every hash, so in-flight keys would look like reuse. Acceptable because
  records are pruned well within a deployment cycle — written down so it is not discovered
  during an upgrade.
- **No pruning is implemented.** `idempotency_keys` grows without bound. The index on
  `created_at` exists for the sweep; the sweep does not.
- **Resume is time-based**, with the assumption in §4.
- **The Node implementation has no equivalent unit tests.** The Go side is covered in
  isolation; the Node side is proven only end to end. Its claim is a single
  aggregation-pipeline upsert, which does not have the Go version's insert-then-lock shape
  and has therefore not been checked against the same races.

---

## 9. Two bugs the unit tests found

Written down because both were invisible from the outside and both would have been
expensive.

**The single upsert could not tell a takeover from a fresh claim.** `Claim` was originally
one `INSERT ... ON CONFLICT DO UPDATE` whose `RETURNING` gave back the row *after* the
update. A resumed stale claim sets `claimed_at = now()`, so it looked identical to a claim
someone else had made a moment earlier — and the caller could not distinguish "I took this
over" from "someone else holds it". Replaced with insert-then-`SELECT ... FOR UPDATE`, which
returns the state as it was *before* this attempt touched anything.

**Staleness was decided by comparing two different clocks.** `claimed_at` is written by
PostgreSQL's `now()`; the check compared it against Go's `time.Now()`. Against a database in
a container, the test reported a claim as **"-86ms old"**. Any skew makes a fresh claim look
stale or a stale one look fresh, and the first of those means two callers resuming the same
operation. The comparison now happens in SQL, where both timestamps come from one clock.

A third, smaller one came with it: durations were passed to PostgreSQL as Go duration
strings. `"30s"` parses; `"1ns"` does not, because PostgreSQL has no nanosecond interval
unit. It worked for every default and failed the first time a test used a short one. Now
`make_interval(secs => ...)`.

---

*See also: [`sagas.md`](sagas.md) for how keys are derived per step, and
[`event_catalogue.md`](event_catalogue.md) for delivery semantics.*
