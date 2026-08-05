/**
 * @sw-edm/nodekit — the Node half of the shared platform.
 *
 * Mirrors libs/eventbus, libs/outbox, libs/idempotency and libs/platform on the Go side.
 * The two halves exist because the brief calls for services in both languages; they are
 * kept honest by both validating against the same JSON Schema files in events/schemas
 * rather than against their own idea of the wire format.
 */

export * as platform from './src/platform.mjs';
export * as idempotency from './src/idempotency.mjs';
export * as outbox from './src/outbox.mjs';

export {
  newEnvelope,
  assertValid,
  topicFor,
  nowTimestamp,
  toTimestamp,
  schemaValidators,
  SchemaViolation,
} from './src/envelope.mjs';

export { Producer, Consumer, PermanentError } from './src/kafka.mjs';

export {
  mustEnv,
  env,
  envInt,
  envDuration,
  envList,
  newLogger,
  connectMongo,
  shutdownSignal,
  untilAborted,
  sleep,
} from './src/platform.mjs';
