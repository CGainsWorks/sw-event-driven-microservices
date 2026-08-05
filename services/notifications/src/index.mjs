/**
 * Notifications service: three Kafka consumers, an outbox relay and a health endpoint.
 *
 * No gRPC server, because nothing calls it. That absence is the design, not an omission —
 * it is what makes this the one service that can be down for an hour with no correctness
 * consequence, only a latency one.
 */

import { createServer } from 'node:http';

import {
  Producer, Consumer, newLogger, mustEnv, env, envInt, envDuration, envList,
  connectMongo, shutdownSignal, untilAborted, outbox, schemaValidators,
} from '@sw-edm/nodekit';

import { NotificationsService, LogSender, ensureIndexes } from './core.mjs';

/**
 * One consumer group per topic.
 *
 * Separate groups rather than one group across three topics, so that a backlog on payment
 * events cannot delay order confirmations. Lag is then also readable per upstream service,
 * which is the first thing anyone wants during an incident.
 */
const SUBSCRIPTIONS = [
  { topic: 'orders.v1.events', groupId: 'notifications-order-events' },
  { topic: 'payments.v1.events', groupId: 'notifications-payment-events' },
  { topic: 'inventory.v1.events', groupId: 'notifications-inventory-events' },
];

async function main() {
  const log = newLogger('notifications');
  const signal = shutdownSignal(log);

  const mongoUri = mustEnv('MONGO_URI');
  const brokers = envList('KAFKA_BROKERS', ['kafka:9092']);
  const httpPort = envInt('HTTP_PORT', 8080);

  const validators = schemaValidators();
  for (const t of ['notifications.v1.NotificationSent', 'notifications.v1.NotificationFailed']) {
    validators.validatorFor(t);
  }

  const client = await connectMongo(mongoUri, log);
  const db = client.db(env('MONGO_DB', 'notifications'));
  await ensureIndexes(db);

  const producer = new Producer(brokers, log, 'notifications');
  await producer.connect();

  const service = new NotificationsService(client, db, new LogSender(log), log, {
    maxAttempts: envInt('DELIVERY_MAX_ATTEMPTS', 3),
  });

  const relay = new outbox.Relay(db, producer, log, {
    pollIntervalMs: envDuration('OUTBOX_POLL_INTERVAL', 250),
    batchSize: envInt('OUTBOX_BATCH_SIZE', 100),
  });

  const consumers = SUBSCRIPTIONS.map(({ topic, groupId }) => new Consumer(
    {
      brokers,
      topic,
      groupId,
      clientId: 'notifications',
      maxAttempts: envInt('CONSUMER_MAX_ATTEMPTS', 3),
    },
    producer,
    log,
  ));

  const http = createServer(httpHandler(service, relay, log));
  http.listen(httpPort, () => log.info('http server listening', { port: httpPort }));

  const tasks = [
    relay.run(signal),
    ...consumers.map((c, i) => c.run(async (envelope) => {
      const result = await service.handle(envelope);
      if (result.action !== 'ignored') {
        log.info('event handled', {
          event_type: envelope.event_type,
          correlation_id: envelope.correlation_id,
          ...result,
        });
      }
    }, signal).catch((err) => {
      log.error('consumer stopped unexpectedly', { topic: SUBSCRIPTIONS[i].topic, error: err });
    })),
  ];

  log.info('notifications started', { subscriptions: SUBSCRIPTIONS.map((s) => s.topic) });
  await untilAborted(signal);

  await Promise.allSettled(consumers.map((c) => c.disconnect()));
  http.close();
  await Promise.allSettled(tasks);
  await producer.disconnect();
  await client.close();
  log.info('stopped');
}

function httpHandler(service, relay, log) {
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
      if (req.method === 'GET' && url.pathname === '/health/ready') {
        return json(200, { status: 'ok', outbox_pending: await relay.pendingCount() });
      }

      // Exposed so the integration tests can assert that exactly one notification exists
      // per order even after a redelivery — the invariant this service is built around.
      // Returns hashes, never addresses.
      const match = /^\/orders\/([^/]+)\/notifications$/.exec(url.pathname);
      if (req.method === 'GET' && match) {
        const records = await service.forOrder(match[1]);
        return json(200, {
          notifications: records.map((n) => ({
            notification_id: n._id,
            source_event_id: n.source_event_id,
            channel: n.channel,
            template: n.template,
            state: n.state,
            attempts: n.attempts,
            recipient_hash: n.recipient_hash,
            failure_reason: n.failure_reason ?? undefined,
          })),
        });
      }

      return json(404, { error: 'not_found' });
    } catch (err) {
      log.error('http handler failed', { error: err, path: url.pathname });
      return json(500, { error: 'internal', message: String(err?.message ?? err) });
    }
  };
}

main().catch((err) => {
  process.stderr.write(`fatal: ${err?.stack ?? err}\n`);
  process.exit(1);
});
