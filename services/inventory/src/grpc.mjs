/**
 * gRPC transport for inventory.
 *
 * The .proto files are loaded at RUNTIME with @grpc/proto-loader rather than compiled to
 * JavaScript ahead of time. That means this service is provably serving the contract that
 * is checked into proto/ — it reads the file itself, so there is no generated artifact
 * that could be stale. The cost is no static typing and a little start-up parsing, which
 * is the trade buf.gen.yaml documents.
 *
 * Translation only. No business rules here.
 */

import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';

import { Outcome, ReservationState } from './core.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));

/** Repository proto root. Overridable so the container can put it somewhere else. */
export const PROTO_ROOT = process.env.PROTO_ROOT || resolve(HERE, '../../../proto');

export function loadInventoryProto() {
  const definition = protoLoader.loadSync(
    join(PROTO_ROOT, 'sw_edm/inventory/v1/inventory.proto'),
    {
      // Field names stay exactly as written in the .proto. Without this, proto-loader
      // lowerCamelCases them and every field name in this file would silently differ from
      // the contract it is implementing.
      keepCase: true,
      longs: String,
      // Enums as strings, so a response says OUTCOME_RESERVED rather than 1. A magic
      // number in a log line during an incident is a number somebody has to go and look up.
      enums: String,
      defaults: true,
      oneofs: true,
      includeDirs: [PROTO_ROOT],
    },
  );
  return grpc.loadPackageDefinition(definition).sw_edm.inventory.v1;
}

/** google.protobuf.Timestamp, as proto-loader represents it. */
function toProtoTimestamp(value) {
  if (!value) return null;
  const ms = value instanceof Date ? value.getTime() : new Date(value).getTime();
  return { seconds: String(Math.floor(ms / 1000)), nanos: (ms % 1000) * 1e6 };
}

const RESERVATION_STATE_ENUM = {
  [ReservationState.Held]: 'RESERVATION_STATE_HELD',
  [ReservationState.Committed]: 'RESERVATION_STATE_COMMITTED',
  [ReservationState.Released]: 'RESERVATION_STATE_RELEASED',
  [ReservationState.Expired]: 'RESERVATION_STATE_EXPIRED',
  [ReservationState.Failed]: 'RESERVATION_STATE_FAILED',
};

const toStateEnum = (state) => RESERVATION_STATE_ENUM[state] ?? 'RESERVATION_STATE_UNSPECIFIED';

/**
 * Turn a domain error into a gRPC status.
 *
 * Only genuine faults reach here — business outcomes are values in the response body, not
 * errors, so that a generic retry interceptor cannot retry a question the domain has
 * already answered. The distinction the caller needs is "retrying might work" versus "it
 * will not", so that is what the code carries.
 */
function toStatus(err) {
  if (err?.name === 'InProgressError') {
    return { code: grpc.status.FAILED_PRECONDITION, message: err.message };
  }
  if (err?.name === 'KeyReuseError') {
    return { code: grpc.status.INVALID_ARGUMENT, message: err.message };
  }
  return { code: grpc.status.UNAVAILABLE, message: String(err?.message ?? err) };
}

/**
 * Wrap a handler so that thrown errors become gRPC statuses and every call is logged with
 * its correlation id. Without the wrapper, an unhandled rejection in a handler takes the
 * whole process down.
 */
function handler(log, name, fn) {
  return async (call, callback) => {
    const correlationId = call.request?.context?.correlation_id;
    try {
      const response = await fn(call.request);
      callback(null, response);
    } catch (err) {
      const status = toStatus(err);
      log.error(`${name} failed`, { error: err, correlation_id: correlationId, code: status.code });
      callback(status);
    }
  };
}

function requireKey(request) {
  if (!request?.idempotency_key) {
    const err = new Error('idempotency_key is required');
    err.name = 'KeyReuseError'; // maps to INVALID_ARGUMENT
    throw err;
  }
}

export function buildServer(service, log) {
  const proto = loadInventoryProto();
  const server = new grpc.Server();
  const l = log.with({ component: 'inventory-grpc' });

  server.addService(proto.InventoryService.service, {
    ReserveStock: handler(l, 'ReserveStock', async (request) => {
      requireKey(request);
      const result = await service.reserveStock({
        idempotencyKey: request.idempotency_key,
        orderId: request.order_id,
        lines: (request.lines ?? []).map((x) => ({ sku: x.sku, quantity: Number(x.quantity) })),
        ttlSeconds: Number(request.ttl_seconds ?? 0),
        correlationId: request.context?.correlation_id,
        causationId: request.context?.causation_event_id,
      });

      return {
        outcome: {
          [Outcome.Reserved]: 'OUTCOME_RESERVED',
          [Outcome.InsufficientStock]: 'OUTCOME_INSUFFICIENT_STOCK',
          [Outcome.UnknownSku]: 'OUTCOME_UNKNOWN_SKU',
        }[result.outcome] ?? 'OUTCOME_UNSPECIFIED',
        reservation_id: result.reservation_id ?? '',
        state: toStateEnum(result.state),
        expires_at: toProtoTimestamp(result.expires_at),
        shortfalls: result.shortfalls ?? [],
        deduplicated: Boolean(result.deduplicated),
      };
    }),

    ReleaseStock: handler(l, 'ReleaseStock', async (request) => {
      requireKey(request);
      const result = await service.releaseStock({
        idempotencyKey: request.idempotency_key,
        reservationId: request.reservation_id,
        reason: (request.reason ?? '').replace(/^RELEASE_REASON_/, '') || 'COMPENSATION',
        correlationId: request.context?.correlation_id,
        causationId: request.context?.causation_event_id,
      });
      return {
        outcome: {
          [Outcome.Released]: 'OUTCOME_RELEASED',
          [Outcome.AlreadyCommitted]: 'OUTCOME_ALREADY_COMMITTED',
          [Outcome.NotFound]: 'OUTCOME_NOT_FOUND',
        }[result.outcome] ?? 'OUTCOME_UNSPECIFIED',
        state: toStateEnum(result.state),
        deduplicated: Boolean(result.deduplicated),
      };
    }),

    CommitReservation: handler(l, 'CommitReservation', async (request) => {
      requireKey(request);
      const result = await service.commitReservation({
        idempotencyKey: request.idempotency_key,
        reservationId: request.reservation_id,
        correlationId: request.context?.correlation_id,
        causationId: request.context?.causation_event_id,
      });
      return {
        outcome: {
          [Outcome.Committed]: 'OUTCOME_COMMITTED',
          [Outcome.Expired]: 'OUTCOME_EXPIRED',
          [Outcome.Released]: 'OUTCOME_RELEASED',
          [Outcome.NotFound]: 'OUTCOME_NOT_FOUND',
        }[result.outcome] ?? 'OUTCOME_UNSPECIFIED',
        state: toStateEnum(result.state),
        deduplicated: Boolean(result.deduplicated),
      };
    }),

    // found=false is a real answer, not an error: it means no reservation matches, so the
    // request never arrived and the caller may safely send it again. Reporting NOT_FOUND
    // would make it indistinguishable from "I could not reach inventory", which is exactly
    // the ambiguity this RPC exists to remove.
    GetReservation: handler(l, 'GetReservation', async (request) => {
      const reservation = await service.getReservation({
        reservationId: request.reservation_id || undefined,
        idempotencyKey: request.idempotency_key || undefined,
      });
      if (!reservation) return { found: false };
      return {
        found: true,
        reservation_id: reservation._id,
        order_id: reservation.order_id,
        state: toStateEnum(reservation.state),
        lines: reservation.lines ?? [],
        expires_at: toProtoTimestamp(reservation.expires_at),
        created_at: toProtoTimestamp(reservation.created_at),
      };
    }),
  });

  return server;
}
