/**
 * OpenTelemetry tracing for the Node services. Parity with libs/telemetry in Go.
 *
 * # Why this is manual instrumentation
 *
 * The obvious alternative is @opentelemetry/auto-instrumentations-node, which patches gRPC,
 * HTTP, MongoDB and kafkajs by monkey-patching their modules on require. Under ESM that
 * needs a loader hook and a `--import` preload, and it silently does nothing if the
 * preload is missed — so the failure mode is "no traces, no error, no clue why".
 *
 * Instrumenting by hand is a few dozen lines, works under plain `node src/index.mjs`, and
 * means the spans that exist are the spans somebody chose. The cost is no free database
 * spans; if those become worth having, the auto-instrumentation goes in behind a preload
 * and this file keeps working unchanged.
 *
 * # The outbox wrinkle
 *
 * Same as the Go side: the span that ENQUEUES an event is not the span that PUBLISHES it.
 * Context is captured at enqueue time into the outbox document's headers, and the relay
 * publishes those headers verbatim, so the consumer continues the trace of the request
 * that caused the event rather than the relay poll that happened to pick it up.
 */

import {
  trace, context, propagation, SpanStatusCode, SpanKind, ROOT_CONTEXT,
} from '@opentelemetry/api';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { BatchSpanProcessor, NodeTracerProvider, ParentBasedSampler, TraceIdRatioBasedSampler } from '@opentelemetry/sdk-trace-node';
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions';

export { SpanKind, SpanStatusCode, trace, context };

let provider = null;

/**
 * Set up the tracer provider and the global propagator.
 *
 * Failure to reach the collector is NOT fatal: the exporter retries in the background and
 * spans are dropped until it connects. A service that refuses to start because its
 * telemetry backend is down has made observability an availability dependency, which is
 * the wrong way round.
 *
 * @returns {{shutdown: () => Promise<void>}}
 */
export function initTracing({ serviceName, version = 'dev', endpoint, sampleRatio = 1 }, log) {
  // W3C trace context globally, so the Node services and the Go services read and write
  // the same headers without either knowing about the other.
  propagation.setGlobalPropagator(new W3CTraceContextPropagator());

  if (!endpoint) {
    log?.info('tracing disabled: no OTEL_EXPORTER_OTLP_ENDPOINT set');
    return { shutdown: async () => {} };
  }

  provider = new NodeTracerProvider({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: serviceName,
      [ATTR_SERVICE_VERSION]: version,
    }),
    // ParentBased so a sampling decision made upstream is honoured here. Without it one
    // service could sample a trace in and the next sample it out, producing traces with
    // holes in the middle that look like missing instrumentation.
    sampler: new ParentBasedSampler({ root: new TraceIdRatioBasedSampler(sampleRatio) }),
    spanProcessors: [
      new BatchSpanProcessor(new OTLPTraceExporter({ url: `http://${endpoint}` }), {
        maxQueueSize: 2048,
        scheduledDelayMillis: 2000,
      }),
    ],
  });
  provider.register();

  log?.info('tracing enabled', { endpoint, service: serviceName, sample_ratio: sampleRatio });
  return {
    shutdown: async () => {
      await provider?.shutdown();
    },
  };
}

export const tracer = (name) => trace.getTracer(name);

/**
 * Serialise the active trace context into a header map for a Kafka message or an outbox
 * document.
 *
 * Returns null when there is no active span, so an untraced code path does not write a
 * meaningless traceparent that a consumer would then try to continue.
 */
export function injectHeaders(ctx = context.active()) {
  const span = trace.getSpan(ctx);
  if (!span || !span.spanContext().traceId) return null;
  const carrier = {};
  propagation.inject(ctx, carrier);
  return Object.keys(carrier).length > 0 ? carrier : null;
}

/**
 * Rebuild trace context from headers.
 *
 * The returned context carries the REMOTE span context, so a span started from it becomes
 * a child of the producing span rather than a new root — which is what makes the trace
 * cross the broker.
 */
export function extractContext(headers, base = ROOT_CONTEXT) {
  if (!headers || Object.keys(headers).length === 0) return base;
  return propagation.extract(base, headers);
}

/**
 * Turn stored trace context into a span link.
 *
 * Used by the outbox relay: the publish is CAUSED by the enqueue but is not part of it, so
 * it links rather than nests. Parenting would make the enqueuing request's span appear to
 * last until the relay got round to publishing, which would be a lie about how long the
 * caller waited.
 */
export function linkFrom(headers) {
  const ctx = extractContext(headers);
  const span = trace.getSpan(ctx);
  const sc = span?.spanContext();
  return sc?.traceId ? [{ context: sc }] : [];
}

/**
 * Run fn inside a span, ending it and recording failures automatically.
 *
 * Business outcomes must NOT be passed through as errors. A declined card or an
 * out-of-stock SKU is an answer, and marking those spans as errors would make an
 * error-rate dashboard measure how many customers were unlucky rather than how often the
 * system misbehaved.
 */
export async function withSpan(t, name, options, fn) {
  return t.startActiveSpan(name, options, async (span) => {
    try {
      return await fn(span);
    } catch (err) {
      span.recordException(err);
      span.setStatus({ code: SpanStatusCode.ERROR, message: String(err?.message ?? err) });
      throw err;
    } finally {
      span.end();
    }
  });
}

/** Attribute keys shared with the Go side, so one query works across every service. */
export const Attr = {
  OrderID: 'sw_edm.order_id',
  PaymentID: 'sw_edm.payment_id',
  ReservationID: 'sw_edm.reservation_id',
  CorrelationID: 'sw_edm.correlation_id',
  EventID: 'sw_edm.event_id',
  EventType: 'sw_edm.event_type',
  Outcome: 'sw_edm.step_outcome',
  IdempotencyKey: 'sw_edm.idempotency_key',
  Deduplicated: 'sw_edm.deduplicated',
};
