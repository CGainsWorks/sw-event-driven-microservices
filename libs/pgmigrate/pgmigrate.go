// Package pgmigrate applies ordered SQL migrations to a PostgreSQL database and records
// which have run.
//
// Migrations are Go values rather than files on disk for one reason: some of the DDL is
// shared. The outbox and processed_events tables are identical in every service, and they
// are defined once, in the package that owns their behaviour, instead of being copied into
// each service's migrations directory where the copies would drift.
//
// This is deliberately not a general migration tool. There is no down-migration, no
// checksum verification and no out-of-order support, because none of those are needed
// here and each would be a thing to get wrong. If this system ever needs them, it needs
// golang-migrate, not an extension of this file.
package pgmigrate

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Migration is one named, ordered schema change. Name must never change once applied to
// any database anywhere — it is the identity recorded in schema_migrations.
type Migration struct {
	Name string
	SQL  string
}

const bootstrapSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    name        TEXT        PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

// Apply runs every migration that has not already been recorded, in the order given.
//
// Each migration runs inside its own transaction together with the insert into
// schema_migrations, so a migration and the record of it having run commit or fail as one.
// A crash halfway through leaves the database at a migration boundary, never between two.
//
// Concurrent callers are safe: an advisory lock serialises them, so several service
// replicas starting at once do not race to create the same table. The losers wait and then
// find every migration already applied.
func Apply(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger, migrations []Migration) error {
	// Arbitrary but fixed. Any service migrating this database must use the same number,
	// which is why it lives here rather than being passed in.
	const advisoryLockID = 0x5711_4544 // "sw-EDM"

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("pgmigrate: acquiring connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockID); err != nil {
		return fmt.Errorf("pgmigrate: taking advisory lock: %w", err)
	}
	defer func() {
		if _, err := conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, advisoryLockID); err != nil {
			log.Error("releasing migration advisory lock", "error", err)
		}
	}()

	if _, err := conn.Exec(ctx, bootstrapSQL); err != nil {
		return fmt.Errorf("pgmigrate: creating schema_migrations: %w", err)
	}

	applied := map[string]bool{}
	rows, err := conn.Query(ctx, `SELECT name FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("pgmigrate: reading applied migrations: %w", err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("pgmigrate: scanning applied migration: %w", err)
		}
		applied[name] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("pgmigrate: reading applied migrations: %w", err)
	}

	for _, m := range migrations {
		if applied[m.Name] {
			continue
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("pgmigrate: beginning %s: %w", m.Name, err)
		}
		if _, err := tx.Exec(ctx, m.SQL); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("pgmigrate: applying %s: %w", m.Name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, m.Name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("pgmigrate: recording %s: %w", m.Name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("pgmigrate: committing %s: %w", m.Name, err)
		}
		log.Info("applied migration", "name", m.Name)
	}
	return nil
}
