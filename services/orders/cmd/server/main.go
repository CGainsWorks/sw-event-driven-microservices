// Command server runs the orders service: an HTTP API, the saga orchestrator, the saga
// recovery loop and an outbox relay.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/CGainsWorks/sw-event-driven-microservices/events"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/eventbus"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/idempotency"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/outbox"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/pgmigrate"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/platform"
	"github.com/CGainsWorks/sw-event-driven-microservices/services/orders/internal/core"
	"github.com/CGainsWorks/sw-event-driven-microservices/services/orders/internal/httpapi"
	"github.com/CGainsWorks/sw-event-driven-microservices/services/orders/internal/paymentsclient"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := platform.SignalContext()
	defer stop()

	log := platform.NewLogger("orders")

	var (
		httpAddr     = platform.Env("HTTP_ADDR", ":8080")
		databaseURL  = platform.MustEnv("DATABASE_URL")
		brokers      = platform.EnvList("KAFKA_BROKERS", "kafka:9092")
		paymentsGRPC = platform.Env("PAYMENTS_GRPC_ADDR", "payments:50051")
	)

	pool, err := platform.ConnectPostgres(ctx, databaseURL, log, 60*time.Second)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := pgmigrate.Apply(ctx, pool, log,
		core.Migrations(outbox.Migration(), idempotency.Migration())); err != nil {
		return err
	}

	// Refuse to start if the embedded schemas do not compile. A service that cannot
	// validate its own events must not publish unvalidated ones.
	for _, t := range []string{"orders.v1.OrderPlaced", "orders.v1.OrderConfirmed", "orders.v1.OrderCancelled"} {
		if _, err := events.Validator(t); err != nil {
			return fmt.Errorf("orders: startup schema check failed: %w", err)
		}
	}

	payments, err := paymentsclient.Dial(paymentsGRPC, log)
	if err != nil {
		return err
	}
	defer func() {
		if err := payments.Close(); err != nil {
			log.Error("closing payments client", "error", err)
		}
	}()

	producer := eventbus.NewProducer(brokers, log)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("closing producer", "error", err)
		}
	}()

	svc := core.NewService(pool, payments, log, core.Config{
		StepTimeout:   platform.EnvDuration("SAGA_STEP_TIMEOUT", 10*time.Second),
		LeaseDuration: platform.EnvDuration("SAGA_LEASE", 30*time.Second),
		RetryBase:     platform.EnvDuration("SAGA_RETRY_BASE", 2*time.Second),
		RetryMax:      platform.EnvDuration("SAGA_RETRY_MAX", 5*time.Minute),
	})

	relay := outbox.NewRelay(pool, producer, log, outbox.RelayConfig{
		PollInterval: platform.EnvDuration("OUTBOX_POLL_INTERVAL", 250*time.Millisecond),
		BatchSize:    platform.EnvInt("OUTBOX_BATCH_SIZE", 100),
		Shard:        platform.EnvInt("OUTBOX_SHARD", 0),
		Shards:       platform.EnvInt("OUTBOX_SHARDS", 1),
	})

	// The nudge queue advances a saga immediately after its order is placed instead of
	// waiting for the recovery loop's next tick. Buffered and non-blocking: dropping a
	// nudge costs latency, never correctness, because readiness lives in the database.
	nudges := make(chan string, 1024)
	nudge := func(orderID string) {
		select {
		case nudges <- orderID:
		default:
			log.Warn("nudge queue full, saga will be picked up by the recovery loop", "order_id", orderID)
		}
	}

	api := httpapi.New(svc, relay, nudge, log)
	httpSrv := &http.Server{
		Addr:              httpAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	workers := platform.EnvInt("SAGA_WORKERS", 4)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("http server listening", "addr", httpAddr, "payments", paymentsGRPC)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server stopped", "error", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		relay.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		svc.RecoveryLoop(ctx, platform.EnvDuration("SAGA_RECOVERY_INTERVAL", 2*time.Second))
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case orderID := <-nudges:
					if err := svc.Advance(ctx, orderID); err != nil && ctx.Err() == nil {
						// Not fatal. The saga stays in the database with its retry time
						// set, and the recovery loop will come back to it.
						log.Error("advancing saga", "order_id", orderID, "worker", n, "error", err)
					}
				}
			}
		}(i)
	}

	log.Info("orders started", "saga_workers", workers)
	<-ctx.Done()
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "error", err)
	}

	wg.Wait()
	log.Info("stopped")
	return nil
}
