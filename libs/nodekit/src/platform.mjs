/**
 * Process-level plumbing: configuration from the environment, a structured logger,
 * MongoDB connection with retry, and graceful shutdown.
 *
 * Mirrors libs/platform in Go, field for field where it matters. Two services that log
 * differently are two services whose incidents take longer to diagnose, so the JSON shape
 * here is deliberately the same as slog's: `time`, `level`, `msg`, then attributes.
 */

import { MongoClient } from 'mongodb';

/**
 * Required setting. Exits rather than defaulting — a service that starts with a guessed
 * database URL looks healthy and then fails on the first request, which is a worse failure
 * than not starting at all.
 */
export function mustEnv(key) {
  const v = process.env[key];
  if (!v) {
    process.stderr.write(`fatal: required environment variable ${key} is not set\n`);
    process.exit(1);
  }
  return v;
}

export const env = (key, fallback) => process.env[key] || fallback;

export function envInt(key, fallback) {
  const v = process.env[key];
  if (!v) return fallback;
  const n = Number.parseInt(v, 10);
  return Number.isNaN(n) ? fallback : n;
}

/** Parses a Go-style duration ("250ms", "10s", "5m") so both stacks read the same values. */
export function envDuration(key, fallbackMs) {
  const v = process.env[key];
  if (!v) return fallbackMs;
  const m = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/.exec(v.trim());
  if (!m) return fallbackMs;
  const n = Number.parseFloat(m[1]);
  return { ms: n, s: n * 1000, m: n * 60_000, h: n * 3_600_000 }[m[2]];
}

export const envList = (key, fallback = []) => {
  const v = process.env[key];
  if (!v) return fallback;
  return v.split(',').map((s) => s.trim()).filter(Boolean);
};

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };

/**
 * Structured logger writing JSON to stdout, because these processes run in containers
 * where stdout is the log pipeline and a human-readable format would have to be parsed
 * back out again.
 */
export function newLogger(service, base = {}) {
  const threshold = LEVELS[(process.env.LOG_LEVEL || 'info').toLowerCase()] ?? LEVELS.info;

  const emit = (level, msg, attrs) => {
    if (LEVELS[level] < threshold) return;
    const record = {
      time: new Date().toISOString(),
      level: level.toUpperCase(),
      msg,
      service,
      ...base,
      ...attrs,
    };
    // Errors do not survive JSON.stringify — an Error serialises to {}. Unwrapping them
    // here is the difference between a log line that says what broke and one that says
    // "error": {}.
    for (const [k, v] of Object.entries(record)) {
      if (v instanceof Error) record[k] = v.stack || v.message;
    }
    process.stdout.write(`${JSON.stringify(record)}\n`);
  };

  return {
    debug: (msg, attrs = {}) => emit('debug', msg, attrs),
    info: (msg, attrs = {}) => emit('info', msg, attrs),
    warn: (msg, attrs = {}) => emit('warn', msg, attrs),
    error: (msg, attrs = {}) => emit('error', msg, attrs),
    with: (extra) => newLogger(service, { ...base, ...extra }),
  };
}

/**
 * Connect to MongoDB, waiting for it to accept connections.
 *
 * The wait exists because compose starts everything at once and Mongo takes a few seconds
 * to elect itself primary. Retrying here rather than relying on a healthcheck means the
 * service behaves the same under compose, under Kubernetes, and against a database that
 * happens to be restarting.
 *
 * A REPLICA SET IS REQUIRED, even single-node. MongoDB only offers multi-document
 * transactions on a replica set, and the transactional outbox is worthless without them:
 * the state change and the outbox document must commit together or the whole guarantee
 * collapses back to "publish and hope". compose runs a one-node set for this reason and
 * for no other.
 */
export async function connectMongo(uri, log, { timeoutMs = 60_000 } = {}) {
  const client = new MongoClient(uri, {
    serverSelectionTimeoutMS: 3000,
    retryWrites: true,
  });

  const deadline = Date.now() + timeoutMs;
  let attempt = 0;
  for (;;) {
    attempt += 1;
    try {
      await client.connect();
      await client.db().admin().command({ ping: 1 });

      // Fail loudly and specifically if this is a standalone. The alternative is a
      // service that starts fine and then throws "Transaction numbers are only allowed on
      // a replica set member" on the first order, which points nowhere near the cause.
      const status = await client.db().admin().command({ hello: 1 });
      if (!status.setName) {
        throw new Error(
          'mongodb is running as a standalone; a replica set is required for transactions. ' +
            'Start it with --replSet and run rs.initiate().',
        );
      }

      log.info('connected to mongodb', { attempts: attempt, replica_set: status.setName });
      return client;
    } catch (err) {
      if (Date.now() > deadline) {
        await client.close().catch(() => {});
        throw new Error(`mongodb not ready after ${timeoutMs}ms: ${err.message}`);
      }
      log.warn('waiting for mongodb', { attempt, error: err.message });
      await sleep(1000);
    }
  }
}

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * An AbortSignal that fires on SIGINT or SIGTERM.
 *
 * Every service uses it so that `docker compose down` and a Kubernetes eviction both drain
 * in-flight work instead of severing it. A saga step killed mid-flight becomes an ambiguous
 * outcome someone else has to resolve, so draining properly is one fewer reconciliation
 * rather than mere politeness.
 */
export function shutdownSignal(log) {
  const controller = new AbortController();
  let fired = false;
  for (const sig of ['SIGINT', 'SIGTERM']) {
    process.on(sig, () => {
      if (fired) {
        log.warn('second signal received, exiting immediately', { signal: sig });
        process.exit(1);
      }
      fired = true;
      log.info('shutting down', { signal: sig });
      controller.abort();
    });
  }
  return controller.signal;
}

/** Resolves when the signal aborts. */
export const untilAborted = (signal) =>
  new Promise((resolve) => {
    if (signal.aborted) return resolve();
    signal.addEventListener('abort', () => resolve(), { once: true });
  });
