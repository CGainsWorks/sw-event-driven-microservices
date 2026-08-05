// Package platform holds the process-level plumbing every Go service needs: configuration
// from the environment, a structured logger, database connection setup and graceful
// shutdown.
//
// None of it is interesting. It is here so that neither service reimplements it, and so
// that the two behave identically when something goes wrong — a service that logs
// differently from its neighbour is a service whose incidents take longer to diagnose.
package platform

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MustEnv returns an environment variable or exits.
//
// Exits rather than defaulting. A service that starts with a guessed database URL appears
// healthy and then fails on the first request, which is a far worse failure than not
// starting at all.
func MustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "fatal: required environment variable %s is not set\n", key)
		os.Exit(1)
	}
	return v
}

// Env returns an environment variable or a fallback. Only for values that genuinely have a
// safe default — never for endpoints or credentials.
func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// EnvInt reads an integer setting, falling back on anything unparseable.
func EnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

// EnvDuration reads a Go duration string such as "250ms" or "10s".
func EnvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

// EnvList reads a comma-separated list, such as a set of Kafka brokers.
func EnvList(key string, fallback ...string) []string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// NewLogger builds the structured logger every service uses.
//
// JSON to stdout, because these processes run in containers where stdout is the log
// pipeline and a human-readable format would have to be parsed back out again.
func NewLogger(service string) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", service)
}

// ConnectPostgres opens a pool and waits for the database to accept connections.
//
// The wait exists because compose starts everything at once and PostgreSQL takes a few
// seconds to become ready. Retrying here rather than relying on a healthcheck means the
// service behaves the same under compose, under Kubernetes and on a laptop with a database
// that happens to be restarting.
func ConnectPostgres(ctx context.Context, dsn string, log *slog.Logger, timeout time.Duration) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("platform: parsing database url: %w", err)
	}
	cfg.MaxConns = int32(EnvInt("DB_MAX_CONNS", 10))
	cfg.MaxConnLifetime = time.Hour

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("platform: creating pool: %w", err)
	}

	deadline := time.Now().Add(timeout)
	attempt := 0
	for {
		attempt++
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := pool.Ping(pingCtx)
		cancel()
		if err == nil {
			log.Info("connected to postgres", "attempts", attempt)
			return pool, nil
		}
		if time.Now().After(deadline) {
			pool.Close()
			return nil, fmt.Errorf("platform: database not ready after %s: %w", timeout, err)
		}
		log.Warn("waiting for postgres", "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// SignalContext returns a context cancelled on SIGINT or SIGTERM.
//
// Every service uses it so that `docker compose down` and a Kubernetes eviction both drain
// in-flight work instead of severing it. A saga step killed mid-flight becomes an ambiguous
// outcome someone else has to resolve, so draining properly is not politeness — it is one
// fewer reconciliation.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
