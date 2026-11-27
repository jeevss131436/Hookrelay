// Package migrate applies the embedded goose migrations in backend/db/migrations.
package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/jeevss131436/Hookrelay/backend/db"
)

// Migrator runs schema migrations. A Postgres advisory lock is held while
// migrating, so concurrent deploys cannot apply migrations twice.
type Migrator struct {
	provider *goose.Provider
	sqlDB    *sql.DB
}

// New builds a Migrator over pool. Call Close when done.
func New(pool *pgxpool.Pool) (*Migrator, error) {
	fsys, err := fs.Sub(db.Migrations, "migrations")
	if err != nil {
		return nil, fmt.Errorf("open embedded migrations: %w", err)
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return nil, fmt.Errorf("create migration lock: %w", err)
	}
	sqlDB := stdlib.OpenDBFromPool(pool)
	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, fsys, goose.WithSessionLocker(locker))
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("create migration provider: %w", err)
	}
	return &Migrator{provider: provider, sqlDB: sqlDB}, nil
}

// Close releases the database/sql handle. It does not close the pgx pool.
func (m *Migrator) Close() error { return m.sqlDB.Close() }

// Up applies all pending migrations.
func (m *Migrator) Up(ctx context.Context) ([]*goose.MigrationResult, error) {
	return m.provider.Up(ctx)
}

// Down rolls back the most recently applied migration.
func (m *Migrator) Down(ctx context.Context) (*goose.MigrationResult, error) {
	return m.provider.Down(ctx)
}

// DownTo rolls back migrations until the schema is at version (0 = empty).
func (m *Migrator) DownTo(ctx context.Context, version int64) ([]*goose.MigrationResult, error) {
	return m.provider.DownTo(ctx, version)
}

// Status reports every known migration and whether it is applied.
func (m *Migrator) Status(ctx context.Context) ([]*goose.MigrationStatus, error) {
	return m.provider.Status(ctx)
}

// Version returns the current schema version (0 if nothing is applied).
func (m *Migrator) Version(ctx context.Context) (int64, error) {
	return m.provider.GetDBVersion(ctx)
}
