// Package telemetry wires OpenTelemetry tracing into the Go services.
//
// The goal is one trace per order that spans every process it touched: the HTTP request
// that placed it, each gRPC saga step, the Kafka publish, and the consumer on the other
// side. Anything less is four disconnected traces and a manual join, which is what
// correlation ids were already for.
//
// # The three propagation paths
//
//	HTTP    traceparent in request headers. otelhttp handles both ends.
//	gRPC    traceparent in gRPC metadata. otelgrpc handles both ends.
//	Kafka   traceparent in message headers — but with a wrinkle, below.
//
// # The outbox wrinkle
//
// The span that ENQUEUES an event is not the span that PUBLISHES it. The enqueue happens
// inside a request; the publish happens later, in the relay, possibly after a restart and
// possibly minutes afterwards.
//
// So trace context is captured at enqueue time and stored in the outbox row's headers,
// and the relay publishes those headers verbatim rather than injecting its own. The
// consumer therefore continues the trace of the request that CAUSED the event, not the
// trace of the relay poll that happened to pick it up. Without this the trace stops dead
// at the outbox and every consumer starts a fresh root span.
//
// The relay's own publish span is linked to the causing span rather than parented to it,
// because the relay is not part of that request's critical path and should not inflate
// its duration.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Config for Init. Every field has a workable default so a service can start without a
// collector — tracing must never be the reason an order cannot be placed.
type Config struct {
	ServiceName string
	Version     string

	// Endpoint is the OTLP gRPC collector, e.g. "otel-collector:4317". Empty disables
	// exporting entirely and installs a no-op provider.
	Endpoint string

	// SampleRatio between 0 and 1. Defaults to 1 — every trace.
	//
	// Full sampling is correct here and would not be in production. This system exists to
	// demonstrate failure paths, and a sampled-out trace is exactly the one you wanted
	// when a saga went wrong. A real deployment would use a parent-based ratio sampler
	// with a tail sampler keeping all errors.
	SampleRatio float64
}

// Init sets up the global tracer provider and propagator, and returns a shutdown function.
//
// Failure to reach the collector is NOT fatal and does not block startup: the exporter
// retries in the background and spans are dropped until it connects. A service that
// refuses to start because its telemetry backend is down has made observability a
// availability dependency, which is the wrong way round.
func Init(ctx context.Context, log *slog.Logger, cfg Config) (func(context.Context) error, error) {
	// W3C trace context plus baggage, set globally so every instrumentation library reads
	// and writes the same headers. This is what makes a Go service and a Node service
	// agree without either knowing about the other.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if cfg.Endpoint == "" {
		log.Info("tracing disabled: no OTEL_EXPORTER_OTLP_ENDPOINT set")
		return func(context.Context) error { return nil }, nil
	}
	if cfg.SampleRatio <= 0 {
		cfg.SampleRatio = 1
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithInsecure(),
		// Do not block startup waiting for the collector.
		otlptracegrpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: creating otlp exporter: %w", err)
	}

	// NewSchemaless, not NewWithAttributes(semconv.SchemaURL, ...).
	//
	// resource.Merge refuses to merge two resources with DIFFERENT non-empty schema URLs,
	// and resource.Default() carries whichever version the SDK was built against. Pinning
	// a schema URL here therefore couples this file to the SDK's semconv version and
	// crashes every service on startup the moment they drift — which is exactly what
	// happened: "conflicting Schema URL: 1.43.0 and 1.26.0".
	//
	// A schemaless resource has an empty URL, which merges with anything. The attribute
	// keys are the stable part and they are unchanged; only the version declaration is
	// dropped, and nothing in this system consumes it.
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.Version),
	))
	if err != nil {
		return nil, fmt.Errorf("telemetry: building resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithMaxQueueSize(2048),
			sdktrace.WithBatchTimeout(2*time.Second),
		),
		sdktrace.WithResource(res),
		// ParentBased so that a sampling decision made upstream is honoured downstream.
		// Without it, one service could sample a trace in and the next sample it out,
		// producing traces with holes in the middle that look like missing instrumentation.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)
	otel.SetTracerProvider(tp)

	log.Info("tracing enabled",
		"endpoint", cfg.Endpoint, "service", cfg.ServiceName, "sample_ratio", cfg.SampleRatio)

	return func(shutdownCtx context.Context) error {
		if err := tp.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("telemetry: shutting down tracer provider: %w", err)
		}
		return nil
	}, nil
}

// Tracer returns a named tracer. The name identifies the instrumenting library in the
// trace, which is how a span from libs/outbox is told apart from one in a service.
func Tracer(name string) trace.Tracer { return otel.Tracer(name) }

// InjectHeaders serialises the active trace context into a header map suitable for a Kafka
// message or an outbox row.
//
// Returns nil when there is no active span, so an untraced code path does not write a
// meaningless traceparent that a consumer would then try to continue.
func InjectHeaders(ctx context.Context) map[string]string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return nil
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	return carrier
}

// ExtractContext rebuilds trace context from Kafka message headers.
//
// The returned context carries the REMOTE span context, so a span started from it becomes
// a child of the producing span rather than a new root — which is what makes the trace
// cross the broker.
func ExtractContext(ctx context.Context, headers map[string]string) context.Context {
	if len(headers) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(headers))
}

// LinkFrom turns stored trace context into a span link.
//
// Used by the outbox relay: the publish is CAUSED by the enqueue but is not part of it,
// so it links rather than nests. Parenting it would make the enqueuing request's span
// appear to last until the relay got round to publishing — which could be minutes, and
// would be a lie about how long the customer waited.
func LinkFrom(headers map[string]string) []trace.Link {
	sc := trace.SpanContextFromContext(ExtractContext(context.Background(), headers))
	if !sc.IsValid() {
		return nil
	}
	return []trace.Link{{SpanContext: sc}}
}

// Common attribute keys, so that a query in the tracing backend works across every service
// rather than only against the one that happened to spell the key that way.
const (
	AttrOrderID       = attribute.Key("sw_edm.order_id")
	AttrPaymentID     = attribute.Key("sw_edm.payment_id")
	AttrReservationID = attribute.Key("sw_edm.reservation_id")
	AttrCorrelationID = attribute.Key("sw_edm.correlation_id")
	AttrEventID       = attribute.Key("sw_edm.event_id")
	AttrEventType     = attribute.Key("sw_edm.event_type")
	AttrSagaStep      = attribute.Key("sw_edm.saga_step")
	AttrStepOutcome   = attribute.Key("sw_edm.step_outcome")
	AttrIdempotencyK  = attribute.Key("sw_edm.idempotency_key")
	AttrDeduplicated  = attribute.Key("sw_edm.deduplicated")
)

// RecordError marks a span as failed and attaches the error.
//
// Business outcomes must NOT go through here. A declined card is an answer, not a fault,
// and marking those spans as errors would make an error-rate dashboard measure how many
// customers had insufficient funds rather than how often the system misbehaved.
func RecordError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
