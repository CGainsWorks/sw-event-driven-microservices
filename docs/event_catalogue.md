# Event catalogue

Every event this system publishes, what it means, and the rules that govern changing it.

The machine-readable form is in [`events/`](../events/). This document is the reasoning;
the schemas are the contract. Where the two disagree, the schemas win — and a test in
`events/schemas.test.mjs` fails, because the two are checked against each other.

Run the checks:

```bash
cd events && npm install && npm test
```

> 49 tests, covering schema compilation, 18 golden samples, 15 must-be-rejected cases, and
> agreement between `schemas/`, `topics.yml` and `registry/config.yml`.

---

## 1. The envelope

Every event on every topic has the same outer shape. Infrastructure reads the envelope;
only the owning domain reads `data`.

```json
{
  "event_id":       "01000000-0000-4000-8000-000000000001",
  "event_type":     "orders.v1.OrderPlaced",
  "event_version":  1,
  "occurred_at":    "2026-03-14T09:15:00.000Z",
  "aggregate_id":   "3f2b9c14-7e5a-4d3b-9c8e-1a2b3c4d5e6f",
  "aggregate_type": "order",
  "correlation_id": "7a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d",
  "causation_id":   null,
  "producer":       "orders",
  "data":           { }
}
```

The fields that carry weight:

| Field | Why it exists |
|---|---|
| `event_id` | **The deduplication key.** A redelivery carries the same `event_id`; a new event never reuses one. `libs/idempotency` records it in `processed_events` and drops repeats. Everything the system claims about exactly-once processing rests on this field. |
| `aggregate_id` | Used as the **Kafka message key**. Kafka orders within a partition, so keying by aggregate is what gives per-aggregate ordering. A test asserts `aggregate_id` equals the primary id inside `data` — if they diverge, one aggregate's events split across partitions and silently lose their order. |
| `correlation_id` | Constant across the whole business transaction. Set once when the order is placed, copied onto every subsequent event and gRPC call in every service. Answers *"show me everything that happened because of this order."* |
| `causation_id` | The `event_id` of the event that directly caused this one. `correlation_id` gives you the set; `causation_id` gives you the tree. Absent when the event originated from an external request or a timer. |
| `occurred_at` | When the fact became true, set by the producer at the moment of the state change — **not** when it was published or consumed. Clock skew means this must never be used for ordering. Use partition order. |
| `event_version` | Minor revision within a major version. Consumers must ignore a version higher than they know and read what they recognise. |

### Why not CloudEvents verbatim

CloudEvents 1.0 covers most of this and is the obvious thing to reach for. The field
*semantics* here follow it — `id`, `type`, `time`, `source`, `subject`, `data` all map
directly. The *names* do not: this repository uses snake_case throughout, in JSON events,
in protobuf messages and in database columns, and importing `specversion`/`datacontenttype`
would mean two naming conventions in one codebase for no gain at this scale.

`correlation_id` and `causation_id` have no CloudEvents equivalent and would have to live
in extension attributes regardless.

The honest cost: nothing off-the-shelf that speaks CloudEvents can consume these topics
without a shim. For a system with four first-party services and no external subscribers,
that cost is zero today and would be real the moment a third party needed a feed. If that
happens, the fix is a translating sink, not a rewrite of the producers.

### Trace context is not in the envelope

W3C `traceparent` and `tracestate` travel in **Kafka message headers**, not in the body.
That is where OpenTelemetry's instrumentation looks, so putting them there means
propagation works without custom code on either side. It also keeps trace context out of
the durable record: a replayed event should join a *new* trace, not graft itself onto a
trace that finished weeks ago.

`correlation_id` is the business-level equivalent and *is* durable. The two answer
different questions: `traceparent` reconstructs one execution, `correlation_id` reconstructs
one transaction across all its retries and restarts. See [`tracing.md`](tracing.md).

---

## 2. Delivery semantics

**The system does not claim exactly-once delivery. It claims at-least-once delivery with
exactly-once processing.** That distinction is the whole design.

Producers write events to an `outbox` table in the *same local transaction* as the state
change. A relay publishes from the outbox and marks rows sent. If the relay crashes between
publishing and marking, the event is published twice.

Consumers therefore must tolerate redelivery, and every one of them does: before handling,
the consumer records `event_id` in `processed_events` under a unique constraint. A duplicate
loses the race, sees the constraint violation, and acknowledges without re-running the
handler.

What this buys and what it costs:

| | |
|---|---|
| **No event is ever lost.** | The outbox commits with the state change, so there is no window where the state changed but the event does not exist. |
| **Every event may arrive more than once.** | Consumers must be idempotent. A consumer that is not idempotent is a bug, not a configuration issue. |
| **Ordering holds per aggregate only.** | Two events for the same `aggregate_id` arrive in order. Two events for different aggregates have no guaranteed relative order, and no consumer may assume one. |
| **Ordering does not hold across topics.** | `PaymentCaptured` and `StockCommitted` concern one order but live on different topics. A consumer needing both must handle either arriving first. |

Full mechanism and tests: [`idempotency.md`](idempotency.md).

---

## 3. Topics

| Topic | Key | Owner | Consumers | Retention |
|---|---|---|---|---|
| `orders.v1.events` | `order_id` | orders | notifications | 7 days |
| `payments.v1.events` | `payment_id` | payments | notifications | 7 days |
| `inventory.v1.events` | `reservation_id` | inventory | notifications | 7 days |
| `notifications.v1.events` | `notification_id` | notifications | — | 30 days |

Each has a `.dlq` companion with one partition and 30-day retention.

Several event types share one topic on purpose: they concern the same aggregate, so they
must share a partition to stay ordered. Consumers dispatch on `event_type`. Splitting one
type per topic would give per-type ordering and destroy per-aggregate ordering, which is
the one that matters.

`notifications.v1.events` has no subscribers. It exists so the last stage of the pipeline is
observable rather than a black hole, and it carries no message bodies and no recipient
addresses — only a SHA-256 of the address. A topic with 30-day retention is not a place to
put personal data.

Full topology including partition counts and DLQ headers: [`events/topics.yml`](../events/topics.yml).

---

## 4. The events

### `orders.v1.events`

| Event | Meaning | Emitted when |
|---|---|---|
| **OrderPlaced** | A customer committed to buy. **Not a sale.** Stock is unchecked and no money has moved. Consumers treating this as revenue are wrong. | `POST /orders` succeeds and the order is written |
| **OrderConfirmed** | Every forward step completed: stock deducted, money captured. **This is the sale.** | The saga reaches its terminal success state |
| **OrderCancelled** | The saga failed and **every compensation has already run** — stock released, payment voided or refunded. A consumer seeing this can assume the system holds nothing on the customer's behalf. | The saga finishes compensating, or a customer cancels before the saga starts |

`OrderCancelled` carries `failed_step` and `compensations_applied`, so a support query can
be answered from the event alone without reading the saga log.

### `payments.v1.events`

| Event | Meaning | Reversible by |
|---|---|---|
| **PaymentAuthorized** | Funds held, not taken. Invisible to the customer. Lapses at `expires_at`. | `VoidAuthorization` — leaves no trace |
| **PaymentCaptured** | Money moved. **The point of no cheap return.** | Refund only, which the customer sees |
| **PaymentRefunded** | A capture reversed. Compensation, not rollback: both entries stay in the ledger forever. | Nothing |
| **PaymentFailed** | An attempt did not succeed. Carries `retryable`, derived from `failure_code`. | n/a |

`PaymentFailed` carries a `payment_id` even for a failed authorization, because payments
allocates the id and records the attempt *before* calling the PSP. That is what makes a
retry with the same idempotency key resolve to the same record instead of creating a second
one.

`retryable` is published explicitly rather than left for consumers to derive from
`failure_code`. Deriving it means every consumer keeps its own copy of the mapping, and they
drift. `CARD_DECLINED` is terminal — retrying is pointless and the saga compensates at once.
`PSP_UNAVAILABLE` is not — the saga backs off and tries again.

### `inventory.v1.events`

| Event | Effect on stock |
|---|---|
| **StockReserved** | `reserved` rises. `on_hand` unchanged. Units still physically present. |
| **StockReservationFailed** | Nothing changes. Reports **every** short SKU, not just the first. |
| **StockReleased** | `reserved` falls. A true undo, leaving no trace in the figures. |
| **StockCommitted** | `on_hand` **and** `reserved` both fall. Availability unchanged — the units simply stop being ours. |
| **ReservationExpired** | `reserved` falls. Emitted by the sweeper, not a caller. |

A failed reservation still allocates a `reservation_id` and records a `FAILED` reservation,
for the same reason payments allocates a `payment_id` on failure: a retry with the same
idempotency key must return the same answer rather than re-running the availability check
against stock that has since changed.

`ReservationExpired` is a safety net against reservations leaked by a dead saga. **Seeing it
during a healthy saga means the reservation TTL is too short**, and `reserved_duration_ms`
is published so that TTL tuning is driven by measurement rather than guesswork.

### `notifications.v1.events`

| Event | Meaning |
|---|---|
| **NotificationSent** | A provider accepted the message. Means "handed over", not "read" — there are no delivery receipts here. |
| **NotificationFailed** | Abandoned after the maximum attempts. **Nothing upstream changes.** An order stays confirmed whether or not the customer was told. |

That last point is the whole reason notifications is a separate, terminal service.

---

## 5. Versioning and compatibility

The registry enforces **BACKWARD** compatibility by default: a consumer built against the
new schema can read data written with the old one. That is the direction that matters,
because consumers are deployed before producers and must still understand the messages
already sitting in the topic.

Payment subjects use **BACKWARD_TRANSITIVE** — checked against *every* previous version, not
just the last. Financial topics get replayed from offset 0 during reconciliation, so version
1 must stay readable no matter how many versions have followed.

### What you may change

| Change | Allowed | Then |
|---|---|---|
| Add an optional field | Yes | Bump `event_version`. Consumers ignore what they do not know. |
| Add a member to an enum | Yes, carefully | Consumers must already map unknown members to a safe default. `cancellation_reason` documents this. |
| Relax a constraint (raise a `maxLength`) | Yes | Bump `event_version`. |
| Make an optional field required | **No** | Old messages lack it. New major version. |
| Remove or rename a field | **No** | Rename is delete + add, and the add is required. New major version. |
| Narrow a type or tighten a constraint | **No** | Old messages violate it. New major version. |

### Breaking changes are a new topic, not a new schema

The major version is in the **topic name**, not a header: `orders.v1.events`. An
incompatible change means `orders.v2.events`, and:

1. Producers dual-write to `v1` and `v2` for a migration window.
2. Consumers move to `v2` one at a time, on their own schedule.
3. `v1` is retired once no consumer group has a lag on it.

Old consumers simply never see `v2`. They do not break, they do not need coordinating, and
nobody has to run a flag day. The cost is a migration window in which producers write
everything twice — which is a known, bounded cost, unlike a coordinated cutover.

### Enforcement

Compatibility is checked in CI against a live registry, so an incompatible schema fails a
pull request rather than a production consumer. `events/schemas.test.mjs` additionally
asserts that `schemas/`, `topics.yml` and `registry/config.yml` all agree — the three drift
apart silently otherwise, and the first sign of it is a consumer failing at 3am.

---

## 6. Golden samples

[`events/samples/valid.json`](../events/samples/valid.json) holds 18 events forming five
stories, not 18 disconnected fixtures:

- **A** — happy path, seven events from `OrderPlaced` to `NotificationSent`
- **B** — compensation path: capture succeeds, commit fails, refund and release run in
  reverse, then the cancellation email fails to send and nothing upstream cares
- **C** — reservation refused for want of stock
- **D** — authorization declined
- **E** — a reservation left by a dead saga, reclaimed by the sweeper

Reading `valid.json` top to bottom shows how `correlation_id` ties a transaction together
and how `causation_id` records what caused what. The tests assert that property rather than
trusting it: every event in a story shares one `correlation_id`, every `causation_id` names
a real event in the same story, and no effect precedes its cause.

[`events/samples/invalid.json`](../events/samples/invalid.json) holds 15 events that **must
be rejected**, each with the reason it exists. A schema that accepts everything is
worthless, so each case also declares *where* it is expected to fail — a case that fails for
the wrong reason is not evidence the constraint works.

These samples are reused as fixtures by the Pact contract tests in `contracts/`, so the
events a service is tested against are the same events documented here.

---

*Previous: [`service_boundaries.md`](service_boundaries.md) — why these four services.*
*Next: `sagas.md` — the fulfilment saga and its compensation paths.*
