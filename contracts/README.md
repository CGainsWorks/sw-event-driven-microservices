# Contract testing

Consumer-driven contracts with [Pact](https://pact.io). The consumer states what it needs;
the provider proves it supplies it.

```bash
npm test --workspace @sw-edm/contracts     # 1. consumers generate pacts
docker compose up -d --build               # 2. providers need to be running
go test -tags=contract ./tests/contract/...  # 3. providers are verified
```

---

## What is covered

| Pair | Transport | Contract | Provider verified against |
|---|---|---|---|
| orders → notifications | Kafka | message pact | real published events |
| payments → notifications | Kafka | message pact | real published events |
| inventory → notifications | Kafka | message pact | real published events |
| orders → payments | gRPC | protobuf plugin | the live gRPC server |
| orders → inventory | gRPC | protobuf plugin | the live gRPC server |

**Every pair that interacts has a contract**, and every contract is verified against the
real provider rather than a stand-in.

The gRPC contracts need Pact's protobuf plugin:

```bash
pact-plugin-cli install -y https://github.com/pactflow/pact-protobuf-plugin/releases/tag/v-0.8.0
```

---

## 1. The consumer says what it needs — and only that

`consumer/notifications.pact.test.mjs` is written from the consumer's side and pins only
what the consumer actually reads:

- `event_type` and `producer` as **exact values**, because the consumer dispatches on them
- `event_id` and `correlation_id` as **UUID regexes**, because it deduplicates and traces
  by them
- `occurred_at` as a **UTC-with-Z regex**, because it compares and stores it
- `data.customer_email` and `data.order_id` on the two templated events, because it cannot
  send an email without them

`total_amount`, `payment_id`, `compensations_applied` and everything else are **absent**.
Notifications does not read them, so a producer is free to change them without breaking this
contract.

That restraint is the point. A contract that pins fields the consumer ignores turns every
producer change into a false alarm, and false alarms are how contract tests get disabled.

**Matchers, not literals.** Pinning `event_id` to one specific UUID would say "the producer
must always send THIS id", which is nonsense. Writing literals where matchers belong is the
most common way to produce a Pact contract that passes and protects nothing.

### The verification target is real production code

The consumer tests run against `describeNotification()` — the actual function the message
handler calls, exported from `services/notifications/src/core.mjs` for exactly this purpose.

Targeting a description of the consumer rather than the consumer itself would let the two
drift, which is the failure this whole exercise exists to prevent.

---

## 2. The provider is verified against what it really publishes

`tests/contract/provider_test.go` does **not** use `pact-go`, and the reason is a design
decision rather than an obstacle.

Standard provider verification hands Pact a function that *returns* the message the provider
would produce. For a Go producer that means writing a provider-state function — one more
piece of code that can drift from the real producer, which is the drift Pact exists to catch.

So this verifies the real thing:

1. Drive real orders through the running system, exercising confirmed, cancelled and refused
   paths.
2. Read the events the producers **actually published to Kafka**.
3. Check every one of them against the consumer's expectations and matching rules.

A recent run verified **155 real events** across six interactions. If orders stopped putting
`customer_email` on `OrderConfirmed`, this fails naming the consumer that needed it.

The matcher evaluator supports `regex`, `type` and exact equality, and **fails on any
matcher it does not recognise**. A contract check that silently ignores rules it does not
understand is worse than no check at all.

---

## 3. Why the pact files are not committed

`contracts/pacts/` is gitignored. They are generated artefacts, and a stale pact that looks
authoritative is worse than no pact — it would let a provider "pass" against expectations a
consumer stopped having months ago.

The real answer at team scale is a **Pact Broker**, which versions pacts, tracks which
consumer version verified against which provider version, and answers *can-i-deploy*. That
is not set up here: it is a service to run, and this repository is meant to be startable
from one command. The consequence is that provider verification requires the consumer tests
to have been run first, which the Go test says explicitly when the directory is empty.

---

## 4. The gRPC contracts, and what they add over `buf`

The shared `.proto` and `buf breaking` in CI already stop a field being removed or renamed.
So what do the gRPC pacts add?

**Consumer-driven granularity.** Buf can say "this change breaks *someone*". Pact says
"this breaks *orders*, which needs `reservation_id` on a refusal". Only the second lets a
provider drop a field it can prove nobody uses.

Two expectations in `consumer/orders-grpc.pact.test.mjs` are exactly that shape — true of
the system, invisible to the schema:

- **`reservation_id` is populated even when a reservation is REFUSED.** Inventory allocates
  the id and records a `FAILED` reservation before answering, so a retry returns the same
  refusal instead of re-running the availability check against stock that has since changed.
  If inventory stopped doing that, the proto would be unchanged and buf silent.
- **`retryable` is published by payments rather than derived by orders** from the failure
  code. Two copies of that mapping would drift, and drift means either a saga that gives up
  on a transient failure or one that retries a decline forever.

### Request expectations are complete; response expectations are minimal

The plugin rejects any request field the client sends that the interaction does not declare.
That asymmetry is right: on the **request** side an expectation is a *promise* about what
the consumer will send, so completeness is the correct default. On the **response** side it
is a *demand*, so naming only what is read keeps the provider free to change the rest.

### Idempotency and replayed contracts do not compose for free

The genuinely surprising thing here, found by running it.

A Pact interaction pins the idempotency key to a fixed example value, so **every
verification run sends the same key** — and the second run receives the first run's
memoised answer instead of exercising the provider. Worse, whatever the first run produced
is then frozen: the refusal case first verified against plentiful stock, recorded
"reserved" under that key, and replayed it forever regardless of what stock said.

So the provider state resets the key as well as the stock, through
`DELETE /admin/reservations?idempotency_key=…`. An idempotent provider and a replayed
contract interaction need that, and nothing warns you.

Payments needs no state handler at all: its outcomes are driven entirely by the
`payment_method_token` in the request, so each interaction carries its own setup — a
property of the simulated PSP being deterministic, and part of why it was built that way.

<details>
<summary>What this looked like before the plugin was set up</summary>

The two gRPC pairs were not Pact-tested. Pact's protobuf/gRPC support needs the
[protobuf plugin](https://github.com/pactflow/pact-protobuf-plugin), a separate binary
installed through `pact-plugin-cli`. It was not set up here.

What currently protects those pairs:

- **A single shared `.proto`**, compiled into the Go client and server and loaded at runtime
  by the Node server. Neither side can invent a field the other does not know about, because
  there is only one definition.
- **`buf breaking` in CI**, run against the base branch on every pull request. A removed
  field, a changed field number or a narrowed type fails the build.
- **Integration tests** asserting the semantic outcomes — that an out-of-stock request
  returns `INSUFFICIENT_STOCK` and not something else.

What that does **not** give, and Pact would:

- **Consumer-driven granularity.** Buf says "this schema change is breaking to *someone*".
  Pact says "this change breaks *orders*, which needs `reservation_id` on a refusal". Only
  the second lets a provider drop a field it can prove nobody uses.
- **Verification without both services running.** The integration tests need the whole stack;
  a Pact provider verification needs only the provider.
- **`can-i-deploy`.** No mechanical answer to "is it safe to ship payments before orders?"

Adding it means installing the plugin, describing each RPC's request and response as a Pact
interaction, and running verification against a live gRPC server. It is a bounded piece of
work and it is not done.

*— which is what the previous version of this file said. It has since been done, and the
section above describes what it turned out to involve. Kept because the reasoning for what
buf does and does not give you is the same either way.*

</details>

---

## 5. Limitations

- **No Pact Broker**, so no versioning and no `can-i-deploy` — §3.
- **Provider verification needs the stack running.** The message verification is
  build-tagged and outside CI's default path; the gRPC verification skips itself when the
  services are unreachable.
- **The gRPC contracts cover the two RPCs the saga's outcome turns on** — `ReserveStock` and
  `AuthorizePayment`, in both their success and refusal forms. `CapturePayment`,
  `CommitReservation` and the compensating RPCs are not described as pacts; they are covered
  by the shared `.proto`, `buf breaking`, and the compensation-matrix tests.
- **The matcher evaluator implements a subset** of the Pact spec: `regex`, `type` and exact
  equality. That is everything these contracts use, and it refuses anything else rather than
  passing it.
- **Contracts exist only for notifications as consumer.** It is the only cross-service Kafka
  consumer in the system, so this is complete for the message pairs — but it means the whole
  suite has one consumer's perspective in it.
