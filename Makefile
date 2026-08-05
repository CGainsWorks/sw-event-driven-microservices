# sw-event-driven-microservices
#
# `make demo` is the one command the README promises. Everything else here exists so that
# `make demo` never needs a manual step in front of it.
#
# Requires: docker, docker compose, go, node, buf. `make check-tools` verifies them and
# says exactly what is missing rather than failing halfway through a build.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

# Pinned tool versions. Pinned rather than @latest because "it worked yesterday" is not a
# build guarantee, and a codegen plugin that silently moves changes every generated file.
BUF_VERSION                := v1.58.0
PROTOC_GEN_GO_VERSION      := v1.36.10
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1

# Resolved quietly: `make check-tools` is the target that reports a missing Go, and it
# cannot do that if this line has already printed a shell error on every invocation.
GOBIN := $(shell go env GOPATH 2>/dev/null)/bin
export PATH := $(GOBIN):$(PATH)

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# --- tooling ----------------------------------------------------------------

.PHONY: check-tools
check-tools: ## Verify every required tool is installed
	@missing=0; \
	for t in docker go node npm curl jq; do \
		command -v $$t >/dev/null 2>&1 || { echo "MISSING: $$t"; missing=1; }; \
	done; \
	docker compose version >/dev/null 2>&1 || { echo "MISSING: docker compose v2"; missing=1; }; \
	command -v buf >/dev/null 2>&1 || echo "NOTE: buf not found; run 'make tools'"; \
	[ $$missing -eq 0 ] || { echo; echo "Install the missing tools and try again."; exit 1; }; \
	echo "All required tools present."

.PHONY: tools
tools: ## Install pinned code generation tools into GOPATH/bin
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

# --- contracts --------------------------------------------------------------

.PHONY: proto
proto: ## Regenerate Go code from proto/
	cd proto && buf generate

.PHONY: proto-lint
proto-lint: ## Lint the protobuf definitions
	cd proto && buf lint

.PHONY: proto-breaking
proto-breaking: ## Fail if the protos break compatibility with origin/main
	cd proto && buf breaking --against '../.git#branch=origin/main,subdir=proto'

.PHONY: proto-check
proto-check: proto ## Fail if generated code is stale relative to the .proto files
	@if [ -n "$$(git status --porcelain proto/)" ]; then \
		echo "Generated protobuf code is out of date. Run 'make proto' and commit nothing —"; \
		echo "proto/gen is gitignored, so this means your local generation differs from CI."; \
		git status --porcelain proto/; \
		exit 1; \
	fi
	@echo "Generated code matches the .proto files."

.PHONY: events-test
events-test: ## Validate event schemas, samples and topic topology
	cd events && npm ci --no-fund --no-audit && npm test

# --- build and test ---------------------------------------------------------

.PHONY: build
build: proto ## Build all Go services
	go build ./...

.PHONY: test
test: events-test test-go ## Run every unit and contract test

.PHONY: test-go
test-go: proto ## Run Go unit tests
	go test ./... -race -count=1

.PHONY: lint
lint: proto-lint ## Run every linter
	go vet ./...

# --- running ----------------------------------------------------------------

.PHONY: demo
demo: ## Bring the whole system up and run the demo scenario
	./scripts/run_demo.sh

.PHONY: test-integration
test-integration: ## Run integration tests against a running stack
	go test -tags=integration ./tests/integration/... -count=1 -v

.PHONY: contracts
contracts: ## Generate consumer pacts, then verify providers against real published events
	npm test --workspace @sw-edm/contracts
	go test -tags=contract ./tests/contract/... -count=1 -v

.PHONY: chaos
chaos: ## Run failure injection (DESTRUCTIVE: stops containers, severs networks)
	./scripts/run_chaos.sh

# --- kubernetes -------------------------------------------------------------

.PHONY: kind-up
kind-up: ## Create a kind cluster and deploy everything to it
	./scripts/kind_up.sh

.PHONY: kind-down
kind-down: ## Delete the kind cluster
	./scripts/kind_up.sh --down

.PHONY: kind-logs
kind-logs: ## Tail logs from every service in the cluster
	kubectl -n sw-edm logs -l 'app in (orders,payments,inventory,notifications)' \
		--all-containers --prefix -f --tail=50

.PHONY: kind-status
kind-status: ## Show what is running in the cluster
	kubectl -n sw-edm get pods,svc,job -o wide

.PHONY: up
up: ## Start every service and its dependencies
	docker compose up -d --build

.PHONY: down
down: ## Stop everything and remove volumes
	docker compose down -v

.PHONY: logs
logs: ## Tail logs from every service
	docker compose logs -f

.PHONY: clean
clean: ## Remove generated code, build output and containers
	-docker compose down -v --remove-orphans
	rm -rf proto/gen
	go clean -cache -testcache
