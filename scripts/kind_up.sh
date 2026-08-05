#!/usr/bin/env bash
# Bring the whole system up on a local kind cluster.
#
#   ./scripts/kind_up.sh          create the cluster, build, load, deploy, wait
#   ./scripts/kind_up.sh --down   delete the cluster
#
# The same service images docker compose builds are loaded into the cluster, so what runs
# here is what runs there. Nothing is pulled from a registry.

set -euo pipefail

cd "$(dirname "$0")/.."

CLUSTER="${KIND_CLUSTER:-sw-edm}"
NS=sw-edm
K8S=deploy/k8s

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
dim()  { printf '\033[2m%s\033[0m\n' "$*"; }
fail() { printf '\033[31m%s\033[0m\n' "$*" >&2; }

for t in docker kind kubectl; do
    command -v "$t" >/dev/null 2>&1 || { fail "missing required tool: $t"; exit 1; }
done

if [ "${1:-}" = "--down" ]; then
    bold "Deleting cluster $CLUSTER"
    kind delete cluster --name "$CLUSTER"
    exit 0
fi

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    dim "cluster $CLUSTER already exists"
else
    bold "Creating cluster $CLUSTER"
    kind create cluster --config "$K8S/kind-cluster.yaml"
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

bold "Building service images"
# docker compose builds them; kind loads them. Building here rather than with a separate
# `docker build` per service keeps one definition of how an image is built.
docker compose build orders payments inventory notifications

bold "Loading images into the cluster"
for img in sw-edm-orders sw-edm-payments sw-edm-inventory sw-edm-notifications; do
    dim "  $img:latest"
    kind load docker-image "$img:latest" --name "$CLUSTER"
done

bold "Creating the namespace"
kubectl apply -f "$K8S/10-datastores.yaml" >/dev/null

# ConfigMaps FIRST, and built from their single source in the repository.
#
# The topic script and the collector config are the same files docker compose uses. They
# are generated into ConfigMaps rather than pasted into the manifests, because a second
# copy would drift — and the drift would surface as topics with the wrong partition count,
# which looks like nothing at all until ordering breaks.
#
# Creating them before the workloads matters: a pod that mounts a ConfigMap which does not
# exist yet stays in ContainerCreating, and a Job that starts before the real script is in
# place fails on whatever placeholder was there.
bold "Generating configuration from source"
kubectl -n "$NS" create configmap kafka-topics-script \
    --from-file=create_topics.sh=scripts/create_topics.sh \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NS" create configmap otel-collector-config \
    --from-file=config.yaml=deploy/otel/collector.yaml \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null

bold "Applying manifests"
kubectl apply -f "$K8S/10-datastores.yaml"
kubectl apply -f "$K8S/20-kafka.yaml"
kubectl apply -f "$K8S/30-observability.yaml"
# Picks up a collector config that changed since the last run. Without it, editing
# deploy/otel/collector.yaml leaves the running collector on the old config and the change
# appears to have done nothing.
kubectl -n "$NS" rollout restart deployment/otel-collector >/dev/null 2>&1 || true

bold "Waiting for datastores"
kubectl -n "$NS" rollout status statefulset/postgres-orders --timeout=180s
kubectl -n "$NS" rollout status statefulset/postgres-payments --timeout=180s
kubectl -n "$NS" rollout status statefulset/mongo-inventory --timeout=240s
kubectl -n "$NS" rollout status statefulset/mongo-notifications --timeout=240s

bold "Waiting for kafka"
kubectl -n "$NS" rollout status statefulset/kafka --timeout=300s

bold "Creating topics"
# The Job is recreated on every run, because a Job's spec is immutable and a completed one
# from a previous run would otherwise be left in place and never re-run. Only the Job
# document is extracted, so nothing else in the file is disturbed.
kubectl -n "$NS" delete job kafka-topics --ignore-not-found >/dev/null
awk '/^apiVersion: batch\/v1/,0' "$K8S/20-kafka.yaml" | kubectl apply -f - >/dev/null
kubectl -n "$NS" wait --for=condition=complete job/kafka-topics --timeout=300s
kubectl -n "$NS" logs job/kafka-topics | tail -20

bold "Deploying services"
kubectl apply -f "$K8S/40-services.yaml"
for d in payments inventory orders notifications; do
    kubectl -n "$NS" rollout status "deployment/$d" --timeout=300s
done

echo
bold "Up."
kubectl -n "$NS" get pods -o wide
echo
echo "  orders         http://localhost:18080"
echo "  inventory      http://localhost:18082"
echo "  notifications  http://localhost:18083"
echo "  jaeger         http://localhost:16686"
echo
dim "  The NodePorts match the compose host ports, so the integration suite runs unchanged:"
dim "    go test -tags=integration ./tests/integration/... -count=1 -v"
dim "  Tear down with: ./scripts/kind_up.sh --down"
