# sw-event-driven-microservices

> **Status: planned. This repository is a brief, not an implementation.**
> Nothing here is built yet. The sections below define what goes in it.

A distributed backend built test-first, focused on the hard parts: exactly-once semantics through idempotency, distributed transactions via sagas with compensation, schema-governed events, contract testing between services and end-to-end tracing.

---

## What this covers

CV entries this repository is the evidence for:

- Event-Driven Microservices Backend

---

## Requirements

### Hardware

- None. Local Kubernetes such as kind or k3d is sufficient

### Software

- Go and Node.js services
- Kafka
- gRPC with protobuf
- PostgreSQL and MongoDB
- Idempotency and saga orchestration
- OpenTelemetry
- Contract testing (Pact)
- Schema registry

### Report and documentation deliverables

- Service boundary rationale
- Event catalogue and schema registry
- Saga sequence diagrams
- Idempotency strategy
- Failure injection test results
- Tracing walkthrough

---

## Inputs

- API and gRPC requests
- Kafka events
- Protobuf and event schemas
- Database migrations
- Contract definitions

## Outputs

- Published domain events
- Persisted state
- Saga compensation actions
- Distributed traces and spans
- Contract test reports
- Service metrics

---

## Intended structure

```
services/
  orders/                 Go
  payments/               Go
  inventory/              Node.js
  notifications/          Node.js
proto/                    gRPC service and message definitions
events/                   event schemas, schema registry config
libs/
  idempotency/
  saga/
  telemetry/
contracts/                Pact contract definitions and verification
deploy/
  k8s/
  kafka/
tests/
  integration/
  chaos/                  failure injection
docs/                     service_boundaries.md, event_catalogue.md,
                          sagas.md, tracing.md
```

Plus the standard baseline: `README.md`, `LICENSE`, `.gitignore`, `.gitattributes`
(Git LFS rules for weights, bags and media), `.github/workflows/ci.yml`, `docs/`,
`scripts/setup.sh`, `scripts/run_demo.sh` and `tests/`.

---

## Build checklist

- [ ] Domain modelling and service boundary rationale
- [ ] Event catalogue and schema definitions
- [ ] Protobuf gRPC contracts
- [ ] First two services communicating over gRPC
- [ ] Kafka event publishing and consumption
- [ ] Idempotency library with deduplication
- [ ] Saga orchestration with compensation paths
- [ ] Contract tests between all service pairs
- [ ] OpenTelemetry tracing across the full request path
- [ ] Failure injection tests: broker down, service down, duplicate delivery
- [ ] Local Kubernetes deployment
- [ ] Tracing walkthrough documentation

---

## Definition of done

This repository is finished when:

- [ ] The whole thing runs from one command on a clean machine.
- [ ] The README opens with a diagram or a GIF, not a wall of text.
- [ ] Every number quoted in the README is reproducible from a script in the repo.
- [ ] Limitations are stated honestly in their own section.
- [ ] A licence is declared.
- [ ] Large binaries live in Releases or LFS, not in the Git history.
- [ ] The repository URL is added to the matching CV bullet point.

---

## Starter prompt

Paste this into your coding agent of choice to begin.

<details>
<summary>Expand</summary>

```
You are helping me build a portfolio project from scratch. Read this brief and then
propose a concrete implementation plan before writing any code.

PROJECT: sw-event-driven-microservices
A distributed backend built test-first, focused on the hard parts: exactly-once semantics through idempotency, distributed transactions via sagas with compensation, schema-governed events, contract testing between services and end-to-end tracing.

TARGET STACK
- Go and Node.js services
- Kafka
- gRPC with protobuf
- PostgreSQL and MongoDB
- Idempotency and saga orchestration
- OpenTelemetry
- Contract testing (Pact)
- Schema registry

HARDWARE CONTEXT
- None. Local Kubernetes such as kind or k3d is sufficient

THE SYSTEM MUST TAKE THESE INPUTS
- API and gRPC requests
- Kafka events
- Protobuf and event schemas
- Database migrations
- Contract definitions

AND PRODUCE THESE OUTPUTS
- Published domain events
- Persisted state
- Saga compensation actions
- Distributed traces and spans
- Contract test reports
- Service metrics

INTENDED REPOSITORY LAYOUT
services/
  orders/                 Go
  payments/               Go
  inventory/              Node.js
  notifications/          Node.js
proto/                    gRPC service and message definitions
events/                   event schemas, schema registry config
libs/
  idempotency/
  saga/
  telemetry/
contracts/                Pact contract definitions and verification
deploy/
  k8s/
  kafka/
tests/
  integration/
  chaos/                  failure injection
docs/                     service_boundaries.md, event_catalogue.md,
                          sagas.md, tracing.md

CONSTRAINTS
- Everything must run from a single command: `docker compose up` or `make demo`.
- No placeholder or stub code presented as working. If something is not implemented, say so.
- Every quantitative claim in the README must be reproducible from a script in this repo.
- Use only public or synthetic data. Record the licence of every dataset used.


START BY
1. Confirming or challenging the proposed repository layout.
2. Listing the specific libraries and versions you intend to pin.
3. Identifying the single riskiest technical unknown and how to de-risk it first.
Then work through the milestones in the README build checklist, one at a time.

```

</details>

---

## Notes

- Do not backdate commits to match CV dates. Publishing older work today is normal and expected.
- Record dataset licences before training on anything. Several common research datasets are
  non-commercial only.
- Flip this repository to public once there is something in it worth showing.
