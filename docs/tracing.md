# Tracing walkthrough

One order produces one trace, spanning four services and two languages.

Everything below is a real capture from the running stack, not an illustration. To
reproduce it:

```bash
docker compose up -d --build
./scripts/run_demo.sh
```

Then open Jaeger at <http://localhost:16686>.

---

## 1. What one order looks like

A single `POST /orders` with a SKU in stock and a card that clears. **20 spans, one
trace**, ordered by start time, with the service that emitted each:

```
orders          POST /orders
orders          saga advance
orders          saga step reserve_stock
orders          sw_edm.inventory.v1.InventoryService/ReserveStock
inventory       InventoryService/ReserveStock
notifications   orders.v1.events process
orders          saga step authorize_payment
orders          sw_edm.payments.v1.PaymentsService/AuthorizePayment
payments        sw_edm.payments.v1.PaymentsService/AuthorizePayment
orders          saga step capture_payment
orders          sw_edm.payments.v1.PaymentsService/CapturePayment
payments        sw_edm.payments.v1.PaymentsService/CapturePayment
orders          saga step commit_reservation
orders          sw_edm.inventory.v1.InventoryService/CommitReservation
inventory       InventoryService/CommitReservation
notifications   inventory.v1.events process
notifications   orders.v1.events process
notifications   inventory.v1.events process
notifications   payments.v1.events process
notifications   payments.v1.events process
```

Three boundaries are crossed and none of them broke the trace:

- **Go → Go** over gRPC (orders → payments)
- **Go → Node** over gRPC (orders → inventory)
- **Go → Kafka → Node** (any service → notifications)

The interleaving is real: `notifications` starts consuming `OrderPlaced` while the saga is
still authorising payment. That is what an event-driven system looks like, and it is
visible here rather than having to be inferred from timestamps in four log streams.

---

## 2. How the context travels

| Boundary | Carrier | Instrumentation |
|---|---|---|
| Inbound HTTP | `traceparent` request header | `otelhttp` |
| gRPC (Go client and server) | `traceparent` in gRPC metadata | `otelgrpc` stats handler |
| gRPC (Node server) | `traceparent` in gRPC metadata | **by hand** — see below |
| Kafka | `traceparent` message header | by hand, via the outbox |
| Saga resumption | `traceparent` in `saga_instances.trace_headers` | by hand |

### The Node gRPC server is instrumented by hand

The Go services get gRPC propagation free from `otelgrpc`. There is no equivalent for
`@grpc/grpc-js` that works under ESM without a loader hook and a `--import` preload, and
that approach fails silently when the preload is missed — no traces, no error, no clue.

So `services/inventory/src/grpc.mjs` extracts `traceparent` from `call.metadata` itself.
Eight lines, and the trace crosses the language boundary. Without it, every span inventory
produced would be a new root and the trace would show a gap exactly where the interesting
work happened.

### The saga's trace context is in the database

The saga is asynchronous: the HTTP span has already ended by the time step 3 runs, and the
work may be resumed hours later by a different process after a restart.

So the trace context captured when the order was placed is **persisted** on
`saga_instances.trace_headers`, and every attempt reads it back. A span whose parent has
already finished is perfectly legal, and the result is the thing worth having: one trace
per order regardless of how many processes and restarts it took.

Holding the context in a channel would lose it at exactly the moment a trace is most
wanted — when something has gone wrong enough to need recovering.

---

## 3. The outbox, and why publishing is in a different trace

The span that **enqueues** an event is not the span that **publishes** it. The enqueue
happens inside a request; the relay publishes later, possibly minutes later, possibly after
a restart.

Two decisions follow:

1. **The headers stored in the outbox row are the enqueuing request's context**, not the
   relay's. The relay publishes them verbatim, so the consumer continues the trace of the
   request that *caused* the event. Without this the trace stops dead at the outbox and
   every consumer starts a fresh root span.

2. **The relay's own publish span is LINKED to the enqueuing span, not parented to it.**
   Parenting would make the request's span appear to last until the relay got round to
   publishing — a lie about how long the customer waited.

The consequence, stated plainly because it surprises people: **`outbox publish` spans are
not in the order's trace.** They form their own short traces, joined to the order's trace
by a link that Jaeger renders as a "References" entry. The consumer span *is* in the
order's trace, because it continues the enqueue context directly.

This is correct OpenTelemetry semantics for asynchronous work, and it is a trade: causality
is preserved, duration is not distorted, and the price is one extra click to see the
publish.

---

## 4. Walking a trace

**Find an order.** Jaeger's search takes tag queries. Every span this system emits that
knows an order id carries it:

```
sw_edm.order_id=8bc97a11-0de3-486c-9a49-70781d3145bc
```

**Find failures.** Business outcomes are attributes, not span errors, so search by outcome
rather than by error status:

```
sw_edm.step_outcome=terminal_failure
sw_edm.step_outcome=unknown
```

`unknown` is the one to watch. It means a step timed out and its outcome could not be
resolved — the saga is neither advancing nor compensating, and it is waiting to ask again.

**Find duplicate work.** `sw_edm.deduplicated=true` marks a response served from an
idempotency record rather than by doing the work again. Seeing it is normal; seeing a lot
of it means something upstream is retrying more than it should.

### Attributes

Every service uses the same keys, so one query works across all four:

| Key | On |
|---|---|
| `sw_edm.order_id` | HTTP, saga, saga steps |
| `sw_edm.correlation_id` | everything |
| `sw_edm.payment_id` / `sw_edm.reservation_id` | steps and RPCs that produced one |
| `sw_edm.saga_step` | saga step spans |
| `sw_edm.step_outcome` | `ok`, `retryable_failure`, `terminal_failure`, `transport_error`, `unknown` |
| `sw_edm.step_detail` | why, when the outcome was not `ok` |
| `sw_edm.idempotency_key` | every keyed call |
| `sw_edm.event_id` / `sw_edm.event_type` | publish and consume spans |
| `sw_edm.deduplicated` | responses replayed from an idempotency record |

---

## 5. A failure, read from the trace alone

A real capture of an order that was refused for want of stock. The `saga step
reserve_stock` span:

```
otel.scope.name        = services/orders/saga
span.kind              = internal
sw_edm.saga_step       = reserve_stock
sw_edm.order_id        = 7ea97067-b896-4746-81ca-72ba98de4812
sw_edm.reservation_id  = fc064c2a-1850-41a8-bde2-fdfc447600fb
sw_edm.idempotency_key = 7ea97067-b896-4746-81ca-72ba98de4812:reserve_stock:1
sw_edm.step_outcome    = terminal_failure
sw_edm.step_detail     = OUTCOME_INSUFFICIENT_STOCK [SKU-TEST-SCARCE requested=5 available=2]
duration               = 22.994 ms
```

That single span answers the whole support question: which step, which SKU, how many were
wanted, how many there were, whether it is worth retrying (`terminal_failure` says no), and
the key to quote if anyone needs to reconcile. No log search, no database access, no
correlating four services by timestamp.

Note the `reservation_id` on a *failed* reservation. Inventory allocates the id and records
a `FAILED` reservation before answering, so a retry returns the same refusal rather than
re-running the check against stock that has since changed — and the trace can point at the
record.

---

## 6. Errors, and what is deliberately not one

**A declined card is not a span error. Neither is an out-of-stock SKU.**

Both are recorded as attributes (`sw_edm.step_outcome=terminal_failure`) on a span with
status `Unset`. Marking them as errors would make an error-rate dashboard measure how many
customers had insufficient funds or wanted more than the shop had — which is a business
metric wearing a reliability metric's clothes, and it would drown the actual faults.

What *is* recorded as a span error:

- a transport failure — the callee could not be reached
- an ambiguous outcome — a call timed out and the result is genuinely unknown
- a handler that exhausted its retries and was dead-lettered
- a schema validation failure

---

## 7. Metrics

Traces answer *what happened to this order*. Metrics answer *is the system doing its job*,
and the two are deliberately different questions with different instruments.

There are **five** instruments, all domain metrics. Go runtime gauges and CPU are available
from a dozen exporters and tell you nothing about whether orders are completing. The rule
applied to each one: it must answer a question somebody would ask during an incident.

| Metric | Answers |
|---|---|
| `sw_edm_orders_placed_total` | How many orders were accepted. Not sales — the denominator. |
| `sw_edm_orders_terminal_total` | Confirmed vs cancelled, **labelled by reason**. A rising share of `INSUFFICIENT_STOCK` is a business problem; a rising share of `PAYMENT_ERROR` is probably ours. |
| `sw_edm_saga_steps_total` | Step attempts by step and outcome. `unknown` appearing at all is worth a look. |
| `sw_edm_saga_step_duration_seconds` | What the reservation TTL must be set against — see the time budget in [`sagas.md`](sagas.md#5-the-time-budget). |
| `sw_edm_saga_stuck_total` | Sagas that could not compensate. Should always be zero. |
| `sw_edm_outbox_pending` | Events written but not published. A backlog is the outbox absorbing an outage; a backlog that stops **falling** is the problem. |

Two decisions worth stating:

**`outbox_pending` is an observable gauge, not a counter the relay updates.** It is a level,
not an event, and registering a callback means the number is read from the database rather
than from a variable that could drift out of step with it.

**There is no separate payments counter.** Payment outcomes already appear on
`saga_steps_total` as `saga_step=authorize_payment`, measured from the caller's side — which
is the side that matters. A second counter inside payments would report almost the same
numbers, disagree at the edges whenever a call was lost in flight, and leave everyone unsure
which to trust.

`classifyStep` produces the outcome word for both the span attribute and the metric label,
so a dashboard filtered on `outcome=unknown` and a trace search for the same thing find the
same events.

## 8. The pipeline

```
                          ┌──► jaeger        (traces)
services ──OTLP/gRPC──► otel-collector
                          └──► :8889 ◄── prometheus ◄── grafana   (metrics)
```

Services never export to Jaeger directly. The collector is the indirection that makes
swapping the backend, adding tail sampling, or fanning out to a second destination a change
to `deploy/otel/collector.yaml` rather than a redeploy of four services in two languages.

The collector also strips attributes that must never leave a service —
`sw_edm.customer_email`, `payment_method_token`, `db.statement` — as a second line of
defence behind the services already not setting them. One rule in one file is auditable;
the same rule repeated in four codebases is not.

---

Prometheus scrapes **one** target, the collector, rather than four services. The services
never learn that Prometheus exists, so replacing it is a change to
`deploy/otel/collector.yaml` and nothing else.

The Grafana dashboard is provisioned from `deploy/grafana/dashboards/fulfilment.json` with
`allowUiUpdates: false` — edits made in the browser are discarded on restart. That is the
point: a dashboard that exists only in one person's browser is not a deliverable.

## 9. Limitations

Stated plainly, because a walkthrough that lists only what works is a demo.

- **Sampling is 100%.** Correct for a system whose purpose is demonstrating failure paths —
  a sampled-out trace is exactly the one you wanted. Wrong for production, which needs a
  parent-based ratio sampler plus tail sampling that keeps all errors.
- **Jaeger stores traces in memory.** They are lost on restart.
- **No database spans in the Node services.** Manual instrumentation was chosen over
  auto-instrumentation because the ESM preload fails silently; the cost is no free MongoDB
  spans. If those become worth having, the auto-instrumentation goes in behind a preload and
  nothing in `libs/nodekit/src/telemetry.mjs` changes.
- **`outbox publish` spans are in linked traces**, not the order's trace. Explained in §3;
  correct, and still a click.
- **No logs pipeline.** Traces and metrics go through the collector; logs are still JSON on
  stdout for whatever collects them.
- **No alerting rules.** The dashboard shows what would be alerted on — stuck sagas,
  unresolved outcomes, a backlog that stops falling — but nothing fires.
- **Prometheus and Jaeger both store in memory**, with a 6-hour retention on Prometheus.
- **Grafana runs with anonymous Admin access.** Correct for a laptop demo where a login
  prompt between a reader and a dashboard is pure friction; indefensible anywhere else.
- **Trace context in `saga_instances` is never pruned.** It is a few hundred bytes per
  order and grows with the table.

---

*Previous: [`event_catalogue.md`](event_catalogue.md) — the events and their schemas.*
