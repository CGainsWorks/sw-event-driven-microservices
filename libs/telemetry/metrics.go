package telemetry

// Metrics.
//
// Deliberately few, and all of them domain metrics rather than process metrics. Go runtime
// gauges and CPU are freely available from a dozen exporters and tell you nothing about
// whether the system is doing its job. What matters here is: are orders completing, are
// sagas compensating more than usual, is the outbox draining, and is anything stuck.
//
// The rule applied to every instrument below: it must answer a question somebody would ask
// during an incident. An instrument that only makes a dashboard look busy is a maintenance
// cost with no payer.

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// InitMetrics sets up the global meter provider and returns a shutdown function.
//
// Like Init, an unreachable collector is not fatal. Metrics are diagnostics; a service that
// refuses to take orders because its metrics backend is down has inverted the relationship.
func InitMetrics(ctx context.Context, log *slog.Logger, cfg Config) (func(context.Context) error, error) {
	if cfg.Endpoint == "" {
		log.Info("metrics disabled: no OTEL_EXPORTER_OTLP_ENDPOINT set")
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(cfg.Endpoint),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: creating otlp metric exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.Version),
	))
	if err != nil {
		return nil, fmt.Errorf("telemetry: building metric resource: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		// 10s, matching the collector's scrape interval closely enough that a counter does
		// not sit unexported for a whole minute during an incident.
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter,
			sdkmetric.WithInterval(10*time.Second))),
	)
	otel.SetMeterProvider(mp)
	log.Info("metrics enabled", "endpoint", cfg.Endpoint, "service", cfg.ServiceName)

	return func(shutdownCtx context.Context) error {
		if err := mp.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("telemetry: shutting down meter provider: %w", err)
		}
		return nil
	}, nil
}

// Metrics is the instrument set shared by the Go services.
//
// Held as a struct rather than package-level variables so a test can build one against a
// no-op provider without touching global state.
type Metrics struct {
	// OrdersPlaced counts every accepted order. The denominator for everything else.
	OrdersPlaced metric.Int64Counter

	// OrdersTerminal counts orders reaching CONFIRMED or CANCELLED, tagged with the
	// outcome and — for cancellations — the reason. This is the one to alert on: a rising
	// share of INSUFFICIENT_STOCK is a business problem, a rising share of PAYMENT_ERROR
	// is probably ours.
	OrdersTerminal metric.Int64Counter

	// SagaSteps counts step attempts by step name and outcome. `unknown` appearing at all
	// is worth a look; `unknown` rising means something is timing out and not resolving.
	SagaSteps metric.Int64Counter

	// SagaStepDuration measures how long each step takes, which is what the reservation
	// TTL has to be set against. See the time budget in docs/sagas.md.
	SagaStepDuration metric.Float64Histogram

	// SagasStuck counts sagas that could not compensate. Should always be zero. Anything
	// else is a human's problem and this is how they find out.
	SagasStuck metric.Int64Counter

	// OutboxPending is observed, not counted: it is a level, not an event. A backlog that
	// is not draining means the broker is unreachable or a message is poisoned.
	OutboxPending metric.Int64ObservableGauge

	// There is deliberately no separate payments counter.
	//
	// Payment outcomes are already visible on SagaSteps, labelled
	// saga_step=authorize_payment with outcome=ok|terminal_failure|retryable_failure|unknown
	// — measured from the caller's side, which is the side that matters. A second counter
	// inside payments would report almost the same numbers, disagree at the edges whenever
	// a call was lost in flight, and leave everyone unsure which to trust.
}

// NewMetrics builds the instrument set.
func NewMetrics(name string) (*Metrics, error) {
	m := otel.Meter(name)
	var (
		err  error
		errs []error
	)
	collect := func(e error) {
		if e != nil {
			errs = append(errs, e)
		}
	}

	out := &Metrics{}

	out.OrdersPlaced, err = m.Int64Counter("sw_edm.orders.placed",
		metric.WithDescription("Orders accepted by the API. Not sales — nothing is reserved or paid yet."))
	collect(err)

	out.OrdersTerminal, err = m.Int64Counter("sw_edm.orders.terminal",
		metric.WithDescription("Orders reaching a terminal state, by outcome and cancellation reason."))
	collect(err)

	out.SagaSteps, err = m.Int64Counter("sw_edm.saga.steps",
		metric.WithDescription("Saga step attempts by step and outcome."))
	collect(err)

	out.SagaStepDuration, err = m.Float64Histogram("sw_edm.saga.step.duration",
		metric.WithDescription("How long a saga step took, including its network call."),
		metric.WithUnit("s"))
	collect(err)

	out.SagasStuck, err = m.Int64Counter("sw_edm.saga.stuck",
		metric.WithDescription("Sagas that could not compensate. Should be zero; needs a human."))
	collect(err)

	if len(errs) > 0 {
		return nil, fmt.Errorf("telemetry: building instruments: %v", errs)
	}
	return out, nil
}

// ObserveOutboxPending registers a callback reporting the current backlog.
//
// An observable gauge rather than a counter the relay updates, because the backlog is a
// level: what matters is its value now, not how many times it changed. Registering a
// callback also means the number comes from the database rather than from a variable that
// could drift out of step with it.
func (mx *Metrics) ObserveOutboxPending(name string, pending func(context.Context) (int, error)) error {
	m := otel.Meter(name)
	gauge, err := m.Int64ObservableGauge("sw_edm.outbox.pending",
		metric.WithDescription("Events written but not yet published. A backlog that is not draining means the broker is unreachable."))
	if err != nil {
		return fmt.Errorf("telemetry: creating outbox gauge: %w", err)
	}
	_, err = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		n, err := pending(ctx)
		if err != nil {
			// Reporting a wrong number is worse than reporting none: a gauge stuck at
			// zero because the query failed reads as "healthy".
			return nil
		}
		o.ObserveInt64(gauge, int64(n))
		return nil
	}, gauge)
	if err != nil {
		return fmt.Errorf("telemetry: registering outbox callback: %w", err)
	}
	mx.OutboxPending = gauge
	return nil
}

// Attribute helpers, so a label is spelled one way across every service. A metric split
// across `outcome` and `result` is a metric nobody can graph.
var (
	AttrOutcome = attribute.Key("outcome")
	AttrStep    = attribute.Key("saga_step")
	AttrReason  = attribute.Key("reason")
	AttrOp      = attribute.Key("operation")
)
