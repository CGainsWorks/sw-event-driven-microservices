/**
 * Inventory service: a gRPC server, an outbox relay, an expiry sweeper and a small HTTP
 * surface for health and test fixtures.
 *
 * Inventory never decides business outcomes for anyone else. It answers "can I have this
 * stock" honestly and enforces its own invariant.
 */

import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import grpc from '@grpc/grpc-js';
import {
  Producer, newLogger, mustEnv, env, envInt, envDuration,
  envList, connectMongo, shutdownSignal, untilAborted, sleep, outbox, initTracing,
} from '@sw-edm/nodekit';
import { schemaValidators } from '@sw-edm/nodekit';

import { InventoryService, ensureIndexes, STOCK } from './core.mjs';
import { buildServer } from './grpc.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));

async function main() {
  const log = newLogger('inventory');
  const signal = shutdownSignal(log);

  // Before anything else that might produce a span.
  const tracing = initTracing({
    serviceName: 'inventory',
    version: env('VERSION', 'dev'),
    endpoint: env('OTEL_EXPORTER_OTLP_ENDPOINT', ''),
  }, log);

  const mongoUri = mustEnv('MONGO_URI');
  const grpcAddr = env('GRPC_ADDR', '0.0.0.0:50052');
  const httpPort = envInt('HTTP_PORT', 8080);
  const brokers = envList('KAFKA_BROKERS', ['kafka:9092']);

  // Refuse to start if the embedded schemas do not compile. A service that cannot validate
  // its own events must not publish unvalidated ones, and finding out at startup is far
  // better than finding out on the first order.
  const validators = schemaValidators();
  for (const t of [
    'inventory.v1.StockReserved',
    'inventory.v1.StockReservationFailed',
    'inventory.v1.StockReleased',
    'inventory.v1.StockCommitted',
    'inventory.v1.ReservationExpired',
  ]) {
    validators.validatorFor(t);
  }

  const client = await connectMongo(mongoUri, log);
  const db = client.db(env('MONGO_DB', 'inventory'));
  await ensureIndexes(db);
  await seedIfEmpty(db, log);

  const service = new InventoryService(client, db, log, {
    defaultTtlMs: envDuration('RESERVATION_TTL', 300_000),
    maxTtlMs: envDuration('RESERVATION_TTL_MAX', 900_000),
  });

  const producer = new Producer(brokers, log, 'inventory');
  const relay = new outbox.Relay(db, producer, log, {
    pollIntervalMs: envDuration('OUTBOX_POLL_INTERVAL', 250),
    batchSize: envInt('OUTBOX_BATCH_SIZE', 100),
    shard: envInt('OUTBOX_SHARD', 0),
    shards: envInt('OUTBOX_SHARDS', 1),
  });

  const server = buildServer(service, log);
  await new Promise((resolvePort, reject) => {
    server.bindAsync(grpcAddr, grpc.ServerCredentials.createInsecure(), (err, port) => {
      if (err) return reject(err);
      log.info('grpc server listening', { addr: grpcAddr, port });
      resolvePort(port);
    });
  });

  const http = createServer(httpHandler(service, relay, db, log));
  http.listen(httpPort, () => log.info('http server listening', { port: httpPort }));

  const tasks = [
    relay.run(signal),
    sweeperLoop(service, signal, envDuration('EXPIRY_SWEEP_INTERVAL', 5000), log),
  ];

  await untilAborted(signal);

  // Drain rather than sever. A saga step cut off mid-call becomes an ambiguous outcome
  // that something else has to reconcile, so draining is one fewer reconciliation.
  await new Promise((done) => {
    const forced = setTimeout(() => {
      log.warn('graceful stop timed out, forcing');
      server.forceShutdown();
      done();
    }, 15_000);
    server.tryShutdown(() => {
      clearTimeout(forced);
      done();
    });
  });
  http.close();
  await Promise.allSettled(tasks);
  await producer.disconnect();
  await client.close();
  // Last, so spans produced during shutdown still get flushed.
  await tracing.shutdown();
  log.info('stopped');
}

/**
 * Load the demo catalogue, but only into an empty collection.
 *
 * Guarded so that a restart never resets stock levels a test or a demo has set. Seeding
 * unconditionally would make every failure-injection run start from a different place than
 * it expected.
 */
async function seedIfEmpty(db, log) {
  const count = await db.collection(STOCK).countDocuments({}, { limit: 1 });
  if (count > 0) return;

  const raw = await readFile(resolve(HERE, '../seed.json'), 'utf8');
  const { items } = JSON.parse(raw);
  await db.collection(STOCK).insertMany(
    items.map((i) => ({ _id: i.sku, on_hand: i.on_hand, reserved: 0, updated_at: new Date() })),
  );
  log.info('seeded demo catalogue into an empty collection', {
    skus: items.length,
    note: 'synthetic data, see services/inventory/seed.json',
  });
}

async function sweeperLoop(service, signal, intervalMs, log) {
  log.info('expiry sweeper started', { interval_ms: intervalMs });
  while (!signal.aborted) {
    try {
      await service.sweepExpired();
    } catch (err) {
      log.error('expiry sweep failed', { error: err });
    }
    await sleep(intervalMs);
  }
  log.info('expiry sweeper stopped');
}

function httpHandler(service, relay, db, log) {
  return async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const json = (code, body) => {
      res.writeHead(code, { 'content-type': 'application/json' });
      res.end(JSON.stringify(body));
    };

    try {
      if (req.method === 'GET' && url.pathname === '/health/live') {
        return json(200, { status: 'ok' });
      }

      // Reports the outbox backlog without failing on it. A relay that cannot reach Kafka
      // must not take inventory out of service — the outbox is precisely what makes
      // continuing to serve safe during a broker outage.
      if (req.method === 'GET' && url.pathname === '/health/ready') {
        const pending = await relay.pendingCount();
        return json(200, { status: 'ok', outbox_pending: pending });
      }

      if (req.method === 'GET' && url.pathname === '/stock') {
        const skus = url.searchParams.get('sku')?.split(',').filter(Boolean);
        const items = await service.stockFor(skus);
        return json(200, {
          items: items.map((i) => ({
            sku: i._id,
            on_hand: i.on_hand,
            reserved: i.reserved,
            available: i.on_hand - i.reserved,
          })),
        });
      }

      // Test and demo affordance: set stock levels deterministically.
      //
      // Present so that integration and chaos tests do not depend on seed.json, which
      // would couple every assertion to a fixture nobody expects to be load-bearing. It is
      // not an operational API and would not exist in a real deployment; a warehouse
      // system would own these numbers.
      if (req.method === 'PUT' && url.pathname === '/admin/stock') {
        const body = JSON.parse(await readBody(req));
        const items = Array.isArray(body?.items) ? body.items : [];
        for (const item of items) {
          await db.collection(STOCK).updateOne(
            { _id: item.sku },
            {
              $set: {
                on_hand: Number(item.on_hand),
                ...(item.reserved === undefined ? {} : { reserved: Number(item.reserved) }),
                updated_at: new Date(),
              },
              $setOnInsert: { ...(item.reserved === undefined ? { reserved: 0 } : {}) },
            },
            { upsert: true },
          );
        }
        log.warn('stock levels set through the admin endpoint', { skus: items.map((i) => i.sku) });
        return json(200, { updated: items.length });
      }

      return json(404, { error: 'not_found' });
    } catch (err) {
      log.error('http handler failed', { error: err, path: url.pathname });
      return json(500, { error: 'internal', message: String(err?.message ?? err) });
    }
  };
}

const readBody = (req) => new Promise((resolveBody, reject) => {
  const chunks = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', () => resolveBody(Buffer.concat(chunks).toString('utf8')));
  req.on('error', reject);
});

main().catch((err) => {
  process.stderr.write(`fatal: ${err?.stack ?? err}\n`);
  process.exit(1);
});
