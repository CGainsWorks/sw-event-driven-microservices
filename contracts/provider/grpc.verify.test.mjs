/**
 * Provider verification for the gRPC pacts.
 *
 * Replays each consumer expectation against the REAL running service over gRPC and checks
 * the real response satisfies it. Requires the stack:
 *
 *   docker compose up -d --build
 *   npm test --workspace @sw-edm/contracts
 *
 * Provider states are honoured by calling inventory's admin endpoint, so "SKU-GIZMO-07 has
 * only 2 in stock" is made true before the interaction runs rather than assumed. Without
 * that the refusal case would pass or fail depending on whatever the last demo left behind.
 *
 * Skips when the services are not reachable, so `npm test` still works on a laptop with
 * nothing running.
 */

import { describe, it, before } from 'node:test';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import pkg from '@pact-foundation/pact';

const { Verifier } = pkg;

const HERE = dirname(fileURLToPath(import.meta.url));
const PACT_DIR = resolve(HERE, '../pacts');

const INVENTORY_HTTP = process.env.INVENTORY_BASE_URL || 'http://localhost:18082';
const INVENTORY_GRPC_PORT = Number(process.env.INVENTORY_GRPC_PORT || 50052);
const PAYMENTS_GRPC_PORT = Number(process.env.PAYMENTS_GRPC_PORT || 50051);

let reachable = false;

async function setStock(sku, onHand) {
  const res = await fetch(`${INVENTORY_HTTP}/admin/stock`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ items: [{ sku, on_hand: onHand, reserved: 0 }] }),
  });
  if (!res.ok) throw new Error(`setting stock for ${sku}: ${res.status}`);
}

async function forgetKey(key) {
  const res = await fetch(
    `${INVENTORY_HTTP}/admin/reservations?idempotency_key=${encodeURIComponent(key)}`,
    { method: 'DELETE' },
  );
  if (!res.ok) throw new Error(`clearing ${key}: ${res.status}`);
}

/**
 * Provider states, made true rather than assumed.
 *
 * Two things have to be reset, and the second is the non-obvious one.
 *
 * STOCK, because a verification that depended on ambient levels would pass or fail
 * according to whatever ran before it.
 *
 * THE IDEMPOTENCY KEY, because a Pact interaction pins it to a fixed example value — so
 * every verification run sends the same key, and the second run gets the FIRST run's
 * memoised answer instead of exercising the provider. That is not a hypothetical: the
 * refusal case below first verified against plentiful stock, recorded "reserved" under the
 * key, and then replayed it on every later run no matter what stock said.
 *
 * An idempotent provider and a replayed contract interaction do not compose without this.
 */
const inventoryStates = {
  'SKU-WIDGET-01 has stock available': async () => {
    await forgetKey('order-1:reserve_stock:1');
    await setStock('SKU-WIDGET-01', 500);
  },
  'SKU-GIZMO-07 has only 2 in stock': async () => {
    await forgetKey('order-2:reserve_stock:1');
    await setStock('SKU-GIZMO-07', 2);
  },
};

before(async () => {
  try {
    const res = await fetch(`${INVENTORY_HTTP}/health/ready`, { signal: AbortSignal.timeout(3000) });
    reachable = res.ok;
  } catch {
    reachable = false;
  }
});

describe('gRPC provider verification', () => {
  it('inventory satisfies the orders contract', { skip: undefined }, async (t) => {
    if (!reachable) return t.skip('the stack is not running; `docker compose up -d` first');

    await new Verifier({
      provider: 'inventory',
      pactUrls: [resolve(PACT_DIR, 'orders-inventory.json')],
      // No providerBaseUrl: this provider speaks gRPC, not HTTP. The transport tells the
      // verifier which port to replay each interaction against, and the protobuf plugin
      // does the encoding.
      transports: [{ protocol: 'grpc', port: INVENTORY_GRPC_PORT }],
      stateHandlers: inventoryStates,
      logLevel: 'warn',
    }).verifyProvider();
  });

  it('payments satisfies the orders contract', async (t) => {
    if (!reachable) return t.skip('the stack is not running; `docker compose up -d` first');

    await new Verifier({
      provider: 'payments',
      pactUrls: [resolve(PACT_DIR, 'orders-payments.json')],
      transports: [{ protocol: 'grpc', port: PAYMENTS_GRPC_PORT }],
      // No state handlers. The payment outcomes are driven entirely by the
      // payment_method_token in the request — pm_ok authorizes, pm_declined declines — so
      // the interaction carries its own setup. That is a property of the simulated PSP
      // being deterministic, and it is why it was built that way.
      logLevel: 'warn',
    }).verifyProvider();
  });
});
