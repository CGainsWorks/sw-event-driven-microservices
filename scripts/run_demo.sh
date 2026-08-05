#!/usr/bin/env bash
# The one command the README promises.
#
#   make demo
#
# Brings the whole system up, waits for it to be ready, then drives four scenarios through
# the real API and prints what happened. Every figure it prints is read back from a
# service, not asserted by the script — if the system misbehaves, the output says so rather
# than the script claiming success.

set -euo pipefail

ORDERS="${ORDERS_BASE_URL:-http://localhost:18080}"
INVENTORY="${INVENTORY_BASE_URL:-http://localhost:18082}"
NOTIFICATIONS="${NOTIFICATIONS_BASE_URL:-http://localhost:18083}"
JAEGER="${JAEGER_URL:-http://localhost:16686}"

cd "$(dirname "$0")/.."

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
dim()  { printf '\033[2m%s\033[0m\n' "$*"; }
fail() { printf '\033[31m%s\033[0m\n' "$*" >&2; }

need() {
    command -v "$1" >/dev/null 2>&1 || { fail "missing required tool: $1"; exit 1; }
}
need docker
need curl
need jq

bold "Bringing the stack up"
docker compose up -d --build

bold "Waiting for services"
wait_for() {
    local name="$1" url="$2" i=0
    until curl -fsS "$url" >/dev/null 2>&1; do
        i=$((i + 1))
        if [ "$i" -gt 90 ]; then
            fail "$name did not become ready at $url"
            docker compose logs --tail 30 "$name" >&2 || true
            exit 1
        fi
        sleep 2
    done
    dim "  $name ready"
}
wait_for orders        "$ORDERS/health/ready"
wait_for inventory     "$INVENTORY/health/ready"
wait_for notifications "$NOTIFICATIONS/health/ready"

# Deterministic stock, so the demo prints the same numbers on every machine rather than
# depending on whatever previous runs left behind.
bold "Setting stock"
curl -fsS -X PUT "$INVENTORY/admin/stock" \
    -H 'content-type: application/json' \
    -d '{"items":[
          {"sku":"SKU-DEMO-WIDGET","on_hand":100,"reserved":0},
          {"sku":"SKU-DEMO-SCARCE","on_hand":2,"reserved":0}
        ]}' | jq -c .

place() {
    local token="$1" sku="$2" qty="$3"
    curl -fsS -X POST "$ORDERS/orders" \
        -H 'content-type: application/json' \
        -d "$(jq -nc --arg t "$token" --arg s "$sku" --argjson q "$qty" '{
              customer_id: "8c1d4e2a-5b6f-4a7c-8d9e-0f1a2b3c4d5e",
              customer_email: "ada@example.com",
              payment_method_token: $t,
              lines: [{ sku: $s, quantity: $q, unit_amount: { amount_minor: 1250, currency: "GBP" } }]
            }')"
}

await_terminal() {
    local id="$1" i=0 state
    while :; do
        state=$(curl -fsS "$ORDERS/orders/$id" | jq -r .state)
        case "$state" in
            CONFIRMED|CANCELLED) echo "$state"; return 0 ;;
        esac
        i=$((i + 1))
        if [ "$i" -gt 60 ]; then echo "TIMEOUT"; return 1; fi
        sleep 1
    done
}

show() {
    local title="$1" id="$2"
    echo
    bold "$title"
    # Nulls dropped: an order that succeeded has no cancellation reason, and printing
    # "cancellation_reason": null on every happy path is noise that trains the eye to skip
    # the line that matters when it is not null.
    curl -fsS "$ORDERS/orders/$id" \
        | jq -c '{state, cancellation_reason, failed_step, payment_id, reservation_id, correlation_id}
                 | with_entries(select(.value != null))'
    dim "  saga:"
    curl -fsS "$ORDERS/orders/$id/saga" \
        | jq -r '.state as $s | "    saga=\($s)",
                 (.steps[] | "    \(.index) \(.name) \(.state) attempts=\(.attempts)" +
                   (if (.detail // "") != "" then "  \(.detail)" else "" end))'
}

echo
bold "=== 1. Happy path: stock reserved, payment taken, order confirmed ==="
ORDER_OK=$(place pm_ok SKU-DEMO-WIDGET 2 | jq -r .order_id)
dim "  order $ORDER_OK"
await_terminal "$ORDER_OK" >/dev/null
show "Result" "$ORDER_OK"

echo
bold "=== 2. Card declined AFTER stock was reserved: the stock must come back ==="
BEFORE=$(curl -fsS "$INVENTORY/stock?sku=SKU-DEMO-WIDGET" | jq -r '.items[0].available')
ORDER_DECLINE=$(place pm_declined SKU-DEMO-WIDGET 5 | jq -r .order_id)
dim "  order $ORDER_DECLINE"
await_terminal "$ORDER_DECLINE" >/dev/null
show "Result" "$ORDER_DECLINE"
AFTER=$(curl -fsS "$INVENTORY/stock?sku=SKU-DEMO-WIDGET" | jq -r '.items[0].available')
echo "    available before=$BEFORE after=$AFTER  (equal means the compensation ran)"

echo
bold "=== 3. Not enough stock: refused before the card is ever touched ==="
ORDER_SHORT=$(place pm_ok SKU-DEMO-SCARCE 5 | jq -r .order_id)
dim "  order $ORDER_SHORT"
await_terminal "$ORDER_SHORT" >/dev/null
show "Result" "$ORDER_SHORT"
echo "    payment_id is empty above: no authorization was attempted"

echo
bold "=== 4. A flaky provider: two transient failures, then success ==="
ORDER_FLAKY=$(place pm_flaky_2 SKU-DEMO-WIDGET 1 | jq -r .order_id)
dim "  order $ORDER_FLAKY"
await_terminal "$ORDER_FLAKY" >/dev/null
show "Result" "$ORDER_FLAKY"
echo "    authorize_payment attempts > 1 above: a transient failure was retried, not memoised"

echo
bold "=== Stock ==="
curl -fsS "$INVENTORY/stock" | jq -r '.items[] | "  \(.sku)  on_hand=\(.on_hand) reserved=\(.reserved) available=\(.available)"'

echo
bold "=== Notifications ==="
for id in "$ORDER_OK" "$ORDER_DECLINE" "$ORDER_SHORT"; do
    sleep 1
    n=$(curl -fsS "$NOTIFICATIONS/orders/$id/notifications" | jq -c '[.notifications[] | {template, state}]')
    echo "  $id  $n"
done

echo
bold "=== Traces ==="
echo "  $JAEGER"
echo "  Search by tag:  sw_edm.order_id=$ORDER_OK"
dim "  One trace spans orders, inventory, payments and notifications. See docs/tracing.md."

echo
bold "Done. 'make down' stops everything and removes the volumes."
