# Status

Last updated: 2026-10-16

## Complete

- Four services (orders and payments in Go, inventory and notifications in Node) with an
  orchestrated saga, compensation, a transactional outbox and idempotency in both stacks.
- Schema-governed events validated at runtime, Pact contracts for all five interacting
  pairs, OpenTelemetry tracing and metrics, Prometheus and Grafana, kind manifests.
- Protobuf code generated inside the image build, so `make up` and `make demo` work from a
  fresh clone.
- Failure injection (`make chaos`): broker down, payments down, duplicate delivery and a
  network partition. The output of the 2026-08-06 run is committed as
  `docs/chaos-results.json`, and `tests/chaos/summary_test.go` checks the README table
  against it on every push.
- Payment provider: a token-driven simulator by default (`PSP_PROVIDER=simulator`), logged
  at startup. A Stripe adapter of the same shape (`PSP_PROVIDER=stripe`, test-mode keys
  only) is exercised in the CI Go job against stripe-mock `v0.197.0`, plus unit tests of
  its status and error mapping against recorded response shapes.
- Every external dependency runs locally: PostgreSQL, MongoDB, Kafka, the schema registry,
  Jaeger, Prometheus and Grafana as containers; email is a log sink; Kubernetes is kind.
  Nothing in CI uses a paid service.

## In progress

- Nothing.

## Not started

- A logs pipeline (traces and metrics go through the collector, logs are JSON on stdout).
- A Pact Broker and `can-i-deploy`.
- Alerting rules.
- A warehouse fulfilment adapter.

## Limitations

- The chaos scenarios need a live stack and are not rerun in CI; the committed results are
  from one run on 2026-08-06. Rerunning `make chaos` refreshes them.
- No real Stripe account has been used. stripe-mock validates request shape against
  Stripe's OpenAPI spec but returns canned objects, so outcome handling (declines, 3-D
  Secure, webhooks) is tested only against recorded response shapes. The compose stack and
  the saga tests run on the simulator.
- Email is a log sink: "sent" means handed over, not delivered.
- Tracing samples 100% and Jaeger stores in memory: right for a demonstration, wrong for
  production.
