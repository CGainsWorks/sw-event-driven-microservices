/**
 * Consumer-driven gRPC contracts: what `orders` needs from `inventory` and `payments`.
 *
 * These use Pact's protobuf plugin, which speaks real gRPC against a mock server built from
 * the checked-in `.proto`. The client below is an ordinary @grpc/grpc-js client — the same
 * library the service uses — so the interaction is exercised over the wire rather than
 * described.
 *
 * Requires the plugin:
 *
 *   pact-plugin-cli install -y https://github.com/pactflow/pact-protobuf-plugin/releases/tag/v-0.8.0
 *
 * What these pin is the SEMANTIC contract, which the shared `.proto` alone does not give.
 * The proto guarantees the shape of `ReserveStockResponse`; it says nothing about the fact
 * that orders reads `reservation_id` even on a REFUSAL, and would break if inventory
 * stopped populating it. That is the gap `buf breaking` cannot see, because it can only say
 * a change breaks *someone*.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import pkg from '@pact-foundation/pact';

const { PactV4, SpecificationVersion } = pkg;

const HERE = dirname(fileURLToPath(import.meta.url));
const PACT_DIR = resolve(HERE, '../pacts');
const PROTO_ROOT = resolve(HERE, '../../proto');
const PLUGIN_VERSION = '0.8.0';

const INVENTORY_PROTO = resolve(PROTO_ROOT, 'sw_edm/inventory/v1/inventory.proto');
const PAYMENTS_PROTO = resolve(PROTO_ROOT, 'sw_edm/payments/v1/payments.proto');

const UUID = '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}';

function pactFor(provider) {
  return new PactV4({
    consumer: 'orders',
    provider,
    dir: PACT_DIR,
    spec: SpecificationVersion.SPECIFICATION_VERSION_V4,
    logLevel: 'warn',
  });
}

/**
 * A real gRPC client against the plugin's mock server.
 *
 * `keepCase: true` and `enums: String` match how the services load these files, so the
 * contract is exercised through the same deserialisation the production code uses. Loading
 * it differently here would test a client nobody ships.
 */
function client(protoPath, pkgPath, service, port) {
  const definition = protoLoader.loadSync(protoPath, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs: [PROTO_ROOT],
  });
  const loaded = grpc.loadPackageDefinition(definition);
  const ns = pkgPath.split('.').reduce((acc, part) => acc[part], loaded);
  return new ns[service](`127.0.0.1:${port}`, grpc.credentials.createInsecure());
}

const call = (stub, method, request) =>
  new Promise((resolvePromise, reject) => {
    stub[method](request, (err, response) => (err ? reject(err) : resolvePromise(response)));
  });

describe('orders → inventory (gRPC)', () => {
  it('needs a reservation id and a state when stock is reserved', async () => {
    await pactFor('inventory')
      .addSynchronousInteraction('a request to reserve stock that succeeds')
      .given('SKU-WIDGET-01 has stock available')
      .usingPlugin({ plugin: 'protobuf', version: PLUGIN_VERSION })
      .withPluginContents(
        JSON.stringify({
          'pact:proto': INVENTORY_PROTO,
          'pact:proto-service': 'InventoryService/ReserveStock',
          'pact:content-type': 'application/grpc',
          'pact:protobuf-config': { additionalIncludes: [PROTO_ROOT] },

          // What orders SENDS.
          //
          // The REQUEST side must be described in full — the plugin rejects any field the
          // client sends that is not declared here. That is the opposite of the response
          // side, where naming a field is a demand: on the request side it is a promise
          // about what the consumer will send, so completeness is the right default.
          //
          // idempotency_key is the field the whole retry story depends on.
          request: {
            context: { correlation_id: `matching(regex, '${UUID}', '7a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d')` },
            idempotency_key: "matching(type, 'order-1:reserve_stock:1')",
            order_id: `matching(regex, '${UUID}', '3f2b9c14-7e5a-4d3b-9c8e-1a2b3c4d5e6f')`,
            lines: [{ sku: "matching(type, 'SKU-WIDGET-01')", quantity: 'matching(number, 2)' }],
          },

          // What orders READS back. Nothing else from the response is pinned: orders does
          // not look at expires_at or deduplicated, so inventory may change them freely.
          response: {
            outcome: "matching(type, 'OUTCOME_RESERVED')",
            reservation_id: `matching(regex, '${UUID}', '5d8c3b2a-1f4e-4a9b-8c7d-6e5f4a3b2c1d')`,
            state: "matching(type, 'RESERVATION_STATE_HELD')",
          },
        }),
        'application/grpc',
      )
      .startTransport('grpc', '127.0.0.1')
      .executeTest(async (mockServer) => {
        const stub = client(INVENTORY_PROTO, 'sw_edm.inventory.v1', 'InventoryService', mockServer.port);
        const res = await call(stub, 'ReserveStock', {
          context: { correlation_id: '7a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d' },
          idempotency_key: 'order-1:reserve_stock:1',
          order_id: '3f2b9c14-7e5a-4d3b-9c8e-1a2b3c4d5e6f',
          lines: [{ sku: 'SKU-WIDGET-01', quantity: 2 }],
        });

        assert.equal(res.outcome, 'OUTCOME_RESERVED');
        assert.ok(res.reservation_id, 'orders cannot commit or release without a reservation id');
      });
  });

  it('needs a reservation id even when the reservation is REFUSED', async () => {
    // The expectation the shared .proto cannot express and buf cannot check.
    //
    // Inventory allocates the id and records a FAILED reservation before answering, so a
    // retry returns the same refusal rather than re-running the availability check against
    // stock that has since changed. Orders relies on that. If inventory stopped populating
    // it on a refusal the proto would be unchanged, buf would be silent, and this fails.
    await pactFor('inventory')
      .addSynchronousInteraction('a request to reserve stock that is refused')
      .given('SKU-GIZMO-07 has only 2 in stock')
      .usingPlugin({ plugin: 'protobuf', version: PLUGIN_VERSION })
      .withPluginContents(
        JSON.stringify({
          'pact:proto': INVENTORY_PROTO,
          'pact:proto-service': 'InventoryService/ReserveStock',
          'pact:content-type': 'application/grpc',
          'pact:protobuf-config': { additionalIncludes: [PROTO_ROOT] },
          request: {
            context: { correlation_id: `matching(regex, '${UUID}', '3c4d5e6f-7a8b-4c9d-8e0f-1a2b3c4d5e6f')` },
            idempotency_key: "matching(type, 'order-2:reserve_stock:1')",
            order_id: `matching(regex, '${UUID}', '9a4c7e2d-5f8b-4c3a-9e7d-2b6f4a8c1d3e')`,
            lines: [{ sku: "matching(type, 'SKU-GIZMO-07')", quantity: 'matching(number, 5)' }],
          },
          response: {
            outcome: "matching(type, 'OUTCOME_INSUFFICIENT_STOCK')",
            reservation_id: `matching(regex, '${UUID}', '7e2a9c4d-1b8f-4e5a-9c3d-6b2f8a4c1e7d')`,
            state: "matching(type, 'RESERVATION_STATE_FAILED')",
            // Every short SKU, so the caller learns what is wrong in one round trip. Orders
            // puts this straight into the saga's step detail, which is what a support query
            // reads.
            shortfalls: [{
              sku: "matching(type, 'SKU-GIZMO-07')",
              requested: 'matching(number, 5)',
              available: 'matching(number, 2)',
            }],
          },
        }),
        'application/grpc',
      )
      .startTransport('grpc', '127.0.0.1')
      .executeTest(async (mockServer) => {
        const stub = client(INVENTORY_PROTO, 'sw_edm.inventory.v1', 'InventoryService', mockServer.port);
        const res = await call(stub, 'ReserveStock', {
          context: { correlation_id: '3c4d5e6f-7a8b-4c9d-8e0f-1a2b3c4d5e6f' },
          idempotency_key: 'order-2:reserve_stock:1',
          order_id: '9a4c7e2d-5f8b-4c3a-9e7d-2b6f4a8c1d3e',
          lines: [{ sku: 'SKU-GIZMO-07', quantity: 5 }],
        });

        assert.equal(res.outcome, 'OUTCOME_INSUFFICIENT_STOCK');
        assert.ok(res.reservation_id,
          'a refused reservation must still carry its id, or a retry cannot return the same answer');
        assert.equal(res.shortfalls.length, 1);
        assert.equal(res.shortfalls[0].available, 2);
      });
  });
});

describe('orders → payments (gRPC)', () => {
  it('needs a payment id and a state when a payment is authorized', async () => {
    await pactFor('payments')
      .addSynchronousInteraction('a request to authorize a payment that succeeds')
      .usingPlugin({ plugin: 'protobuf', version: PLUGIN_VERSION })
      .withPluginContents(
        JSON.stringify({
          'pact:proto': PAYMENTS_PROTO,
          'pact:proto-service': 'PaymentsService/AuthorizePayment',
          'pact:content-type': 'application/grpc',
          'pact:protobuf-config': { additionalIncludes: [PROTO_ROOT] },
          request: {
            context: { correlation_id: `matching(regex, '${UUID}', '7a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d')` },
            idempotency_key: "matching(type, 'order-1:authorize_payment:1')",
            order_id: `matching(regex, '${UUID}', '3f2b9c14-7e5a-4d3b-9c8e-1a2b3c4d5e6f')`,
            customer_id: `matching(regex, '${UUID}', '8c1d4e2a-5b6f-4a7c-8d9e-0f1a2b3c4d5e')`,
            amount: {
              amount_minor: 'matching(number, 7499)',
              currency: "matching(regex, '[A-Z]{3}', 'GBP')",
            },
            // An opaque handle for a stored instrument. This system never sees, transmits
            // or stores a card number, and pinning the field as an arbitrary string is
            // part of saying so.
            payment_method_token: "matching(type, 'pm_ok')",
          },
          response: {
            outcome: "matching(type, 'OUTCOME_AUTHORIZED')",
            payment_id: `matching(regex, '${UUID}', 'b4e7a1c9-2d8f-4e6b-a3c5-7d9e1f2a3b4c')`,
            state: "matching(type, 'PAYMENT_STATE_AUTHORIZED')",
          },
        }),
        'application/grpc',
      )
      .startTransport('grpc', '127.0.0.1')
      .executeTest(async (mockServer) => {
        const stub = client(PAYMENTS_PROTO, 'sw_edm.payments.v1', 'PaymentsService', mockServer.port);
        const res = await call(stub, 'AuthorizePayment', {
          context: { correlation_id: '7a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d' },
          idempotency_key: 'order-1:authorize_payment:1',
          order_id: '3f2b9c14-7e5a-4d3b-9c8e-1a2b3c4d5e6f',
          customer_id: '8c1d4e2a-5b6f-4a7c-8d9e-0f1a2b3c4d5e',
          amount: { amount_minor: '7499', currency: 'GBP' },
          payment_method_token: 'pm_ok',
        });

        assert.equal(res.outcome, 'OUTCOME_AUTHORIZED');
        assert.ok(res.payment_id, 'orders cannot capture or void without a payment id');
      });
  });

  it('needs a payment id and a retryable flag when a payment is DECLINED', async () => {
    // Two expectations the proto cannot make.
    //
    // payment_id is present on a FAILURE, because payments allocates it and records the
    // attempt before calling the provider — which is what lets a retry resolve to the same
    // record instead of starting a second payment.
    //
    // `retryable` is published by payments rather than derived by orders from the failure
    // code. Two copies of that mapping would drift, and the consequence of drifting is
    // either a saga that gives up on a transient failure or one that retries a decline
    // forever.
    await pactFor('payments')
      .addSynchronousInteraction('a request to authorize a payment that is declined')
      .usingPlugin({ plugin: 'protobuf', version: PLUGIN_VERSION })
      .withPluginContents(
        JSON.stringify({
          'pact:proto': PAYMENTS_PROTO,
          'pact:proto-service': 'PaymentsService/AuthorizePayment',
          'pact:content-type': 'application/grpc',
          'pact:protobuf-config': { additionalIncludes: [PROTO_ROOT] },
          request: {
            context: { correlation_id: `matching(regex, '${UUID}', '4d5e6f7a-8b9c-4d0e-8f1a-2b3c4d5e6f7a')` },
            idempotency_key: "matching(type, 'order-3:authorize_payment:1')",
            order_id: `matching(regex, '${UUID}', '2e7b4d9a-6c1f-4a8e-b3d5-7f9a1c3e5b7d')`,
            customer_id: `matching(regex, '${UUID}', '8c1d4e2a-5b6f-4a7c-8d9e-0f1a2b3c4d5e')`,
            amount: {
              amount_minor: 'matching(number, 3200)',
              currency: "matching(regex, '[A-Z]{3}', 'GBP')",
            },
            payment_method_token: "matching(type, 'pm_declined')",
          },
          response: {
            outcome: "matching(type, 'OUTCOME_FAILED')",
            payment_id: `matching(regex, '${UUID}', 'd6a9c3eb-4f2a-4b8d-a5c7-9f1e3b5d7a9c')`,
            state: "matching(type, 'PAYMENT_STATE_FAILED')",
            failure: {
              code: "matching(type, 'FAILURE_CODE_CARD_DECLINED')",
              retryable: 'matching(boolean, false)',
            },
          },
        }),
        'application/grpc',
      )
      .startTransport('grpc', '127.0.0.1')
      .executeTest(async (mockServer) => {
        const stub = client(PAYMENTS_PROTO, 'sw_edm.payments.v1', 'PaymentsService', mockServer.port);
        const res = await call(stub, 'AuthorizePayment', {
          context: { correlation_id: '4d5e6f7a-8b9c-4d0e-8f1a-2b3c4d5e6f7a' },
          idempotency_key: 'order-3:authorize_payment:1',
          order_id: '2e7b4d9a-6c1f-4a8e-b3d5-7f9a1c3e5b7d',
          customer_id: '8c1d4e2a-5b6f-4a7c-8d9e-0f1a2b3c4d5e',
          amount: { amount_minor: '3200', currency: 'GBP' },
          payment_method_token: 'pm_declined',
        });

        assert.equal(res.outcome, 'OUTCOME_FAILED');
        assert.ok(res.payment_id,
          'a declined payment must still carry its id, or a retry starts a second payment');
        assert.equal(res.failure.retryable, false,
          'orders branches on retryable; deriving it locally would let the two disagree');
      });
  });
});
