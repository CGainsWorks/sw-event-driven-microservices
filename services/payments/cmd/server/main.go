// Command server runs the payments service: a gRPC server, an outbox relay, and nothing
// else. Payments never initiates — it responds to commands and reports facts.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/CGainsWorks/sw-event-driven-microservices/events"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/eventbus"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/idempotency"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/outbox"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/pgmigrate"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/platform"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/telemetry"
	paymentsv1 "github.com/CGainsWorks/sw-event-driven-microservices/proto/gen/go/sw_edm/payments/v1"
	"github.com/CGainsWorks/sw-event-driven-microservices/services/payments/internal/core"
	"github.com/CGainsWorks/sw-event-driven-microservices/services/payments/internal/grpcapi"
	"github.com/CGainsWorks/sw-event-driven-microservices/services/payments/internal/psp"
)

// version is stamped at build time by services/Dockerfile (-X main.version). It goes into
// the trace resource, so a span can be traced back to a commit — which matters the first
// time two replicas disagree about something.
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := platform.SignalContext()
	defer stop()

	log := platform.NewLogger("payments")

	var (
		grpcAddr    = platform.Env("GRPC_ADDR", ":50051")
		healthAddr  = platform.Env("HEALTH_ADDR", ":8081")
		databaseURL = platform.MustEnv("DATABASE_URL")
		brokers     = platform.EnvList("KAFKA_BROKERS", "kafka:9092")
		authTTL     = platform.EnvDuration("AUTHORIZATION_TTL", 7*24*time.Hour)
	)

	shutdownTracing, err := telemetry.Init(ctx, log, telemetry.Config{
		ServiceName: "payments",
		Version:     version,
		Endpoint:    platform.Env("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
	})
	if err != nil {
		return err
	}
	shutdownMetrics, err := telemetry.InitMetrics(ctx, log, telemetry.Config{
		ServiceName: "payments",
		Version:     version,
		Endpoint:    platform.Env("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
	})
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			log.Error("shutting down tracing", "error", err)
		}
		if err := shutdownMetrics(shutdownCtx); err != nil {
			log.Error("shutting down metrics", "error", err)
		}
	}()

	pool, err := platform.ConnectPostgres(ctx, databaseURL, log, 60*time.Second)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := pgmigrate.Apply(ctx, pool, log,
		core.Migrations(outbox.Migration(), idempotency.Migration())); err != nil {
		return err
	}

	// Refuse to start if the embedded event schemas do not compile. A service that cannot
	// validate its own events must not publish unvalidated ones, and finding out at
	// startup is far better than finding out on the first order.
	if _, err := eventsSelfCheck(); err != nil {
		return err
	}

	producer := eventbus.NewProducer(brokers, log)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("closing producer", "error", err)
		}
	}()

	// The simulator unless PSP_PROVIDER says otherwise; stripe is test mode only.
	provider, providerName, err := psp.FromEnv(os.Getenv)
	if err != nil {
		return err
	}
	log.Info("payment provider", "psp", providerName)

	svc := core.NewService(pool, provider, log, authTTL)
	relay := outbox.NewRelay(pool, producer, log, outbox.RelayConfig{
		PollInterval: platform.EnvDuration("OUTBOX_POLL_INTERVAL", 250*time.Millisecond),
		BatchSize:    platform.EnvInt("OUTBOX_BATCH_SIZE", 100),
		Shard:        platform.EnvInt("OUTBOX_SHARD", 0),
		Shards:       platform.EnvInt("OUTBOX_SHARDS", 1),
	})

	// otelgrpc as a stats handler rather than an interceptor: it is the supported path and
	// it sees stream events an interceptor does not. This is what extracts traceparent
	// from gRPC metadata, so a saga step from orders continues its trace inside payments
	// with no code in either service touching a header.
	// Payments publishes its own outbox backlog. Every service that holds an outbox reports
	// it, so a single dashboard panel covers all of them and a stuck relay is visible
	// wherever it is stuck.
	if m, err := telemetry.NewMetrics("services/payments"); err == nil {
		if err := m.ObserveOutboxPending("services/payments", relay.PendingCount); err != nil {
			log.Error("outbox gauge unavailable", "error", err)
		}
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	paymentsv1.RegisterPaymentsServiceServer(grpcServer, grpcapi.New(svc, log))

	// gRPC health, so that Kubernetes probes and `grpc_health_probe` in the compose
	// healthcheck use the same mechanism rather than two different definitions of ready.
	hs := health.NewServer()
	hs.SetServingStatus("sw_edm.payments.v1.PaymentsService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, hs)

	// Reflection, so that grpcurl works against a running container during development and
	// during a chaos test when there is no time to generate a client.
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("payments: listening on %s: %w", grpcAddr, err)
	}

	httpSrv := &http.Server{
		Addr:              healthAddr,
		Handler:           healthHandler(relay),
		ReadHeaderTimeout: 5 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		log.Info("grpc server listening", "addr", grpcAddr, "authorization_ttl", authTTL)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Error("grpc server stopped", "error", err)
		}
	}()

	go func() {
		defer wg.Done()
		log.Info("health server listening", "addr", healthAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server stopped", "error", err)
		}
	}()

	go func() {
		defer wg.Done()
		relay.Run(ctx)
	}()

	<-ctx.Done()
	log.Info("shutting down")

	// GracefulStop drains in-flight RPCs. A saga step severed mid-call becomes an
	// ambiguous outcome that something else has to reconcile, so draining is one fewer
	// reconciliation rather than mere politeness.
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(15 * time.Second):
		log.Warn("graceful stop timed out, forcing")
		grpcServer.Stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("health server shutdown", "error", err)
	}

	wg.Wait()
	log.Info("stopped")
	return nil
}

// healthHandler exposes liveness and readiness.
//
// /health/ready reports the outbox backlog. It does not fail on a backlog — a relay that
// cannot reach Kafka should keep accepting payments, because the outbox is exactly what
// makes that safe. The number is exposed so the chaos tests can assert the backlog drains
// after the broker returns rather than assuming it did.
func healthHandler(relay *outbox.Relay) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		pending, err := relay.PendingCount(r.Context())
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, `{"status":"degraded","error":%q}`, err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"ok","outbox_pending":%d}`, pending)
	})
	return mux
}

// eventsSelfCheck compiles the schema for every event this service publishes.
//
// Compilation is lazy and cached, so doing it here means the cost is paid at startup and
// any failure surfaces before the first request rather than inside the transaction that
// was about to charge someone.
func eventsSelfCheck() ([]string, error) {
	published := []string{
		"payments.v1.PaymentAuthorized",
		"payments.v1.PaymentCaptured",
		"payments.v1.PaymentRefunded",
		"payments.v1.PaymentFailed",
	}
	for _, t := range published {
		if _, err := events.Validator(t); err != nil {
			return nil, fmt.Errorf("payments: startup schema check failed: %w", err)
		}
	}
	return published, nil
}
