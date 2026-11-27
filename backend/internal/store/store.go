// Package store is the data-access layer: a pgx connection pool, transaction
// management, and the sqlc-generated queries in store/sqlcgen.
//
// Hand-written SQL lives in backend/db/queries and backend/db/migrations;
// never edit sqlcgen directly (regenerate with `make sqlc`).
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
)

// NewPool creates a pgx connection pool. It does not connect eagerly, so
// callers decide whether an unreachable database is fatal (admin, migrate)
// or a readiness condition (api).
//
// Neon: application traffic uses the pooled DATABASE_URL (PgBouncer in
// transaction mode, which supports pgx's protocol-level prepared statements).
// Anything needing session state (migrations' advisory lock, LISTEN/NOTIFY,
// SET) must use the direct DATABASE_URL_UNPOOLED. If a pooler ever rejects
// prepared statements, append default_query_exec_mode=exec to the URL.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		// pgx parse errors can echo the connection string; don't wrap them.
		return nil, fmt.Errorf("parse database url: invalid connection settings")
	}
	if _, ok := cfg.ConnConfig.RuntimeParams["application_name"]; !ok {
		cfg.ConnConfig.RuntimeParams["application_name"] = "hookrelay"
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	return pool, nil
}

// Store bundles the pool with the generated queries. Query methods called
// directly on a Store run outside any transaction; use InTx for operations
// that must succeed or fail together.
type Store struct {
	*sqlcgen.Queries
	pool *pgxpool.Pool
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{Queries: sqlcgen.New(pool), pool: pool}
}

// Pool exposes the underlying pool (for health checks and migrations).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// InTx runs fn in a single transaction (READ COMMITTED). The transaction is
// committed if fn returns nil and rolled back if fn returns an error or
// panics (the panic is re-raised).
//
// fn must not perform network I/O other than database calls. In particular,
// never make outbound HTTP requests inside a transaction (CLAUDE.md
// invariant 5).
func (s *Store) InTx(ctx context.Context, fn func(q *sqlcgen.Queries) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return fn(s.WithTx(tx))
	})
}
