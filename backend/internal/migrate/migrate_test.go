package migrate_test

import (
	"context"
	"io/fs"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"

	"github.com/jeevss131436/Hookrelay/backend/db"
	"github.com/jeevss131436/Hookrelay/backend/internal/migrate"
	"github.com/jeevss131436/Hookrelay/backend/internal/testutil/pgtest"
)

var appTables = []string{"projects", "api_keys", "endpoints", "events", "deliveries", "delivery_attempts"}

// TestEmbeddedMigrations runs without a database: it checks that migrations
// are embedded and every file has both Up and Down sections.
func TestEmbeddedMigrations(t *testing.T) {
	files, err := fs.Glob(db.Migrations, "migrations/*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("no embedded migrations (err %v)", err)
	}
	for _, f := range files {
		b, err := fs.ReadFile(db.Migrations, f)
		if err != nil {
			t.Fatal(err)
		}
		s := string(b)
		if !strings.Contains(s, "-- +goose Up") || !strings.Contains(s, "-- +goose Down") {
			t.Errorf("%s must contain both '-- +goose Up' and '-- +goose Down'", f)
		}
	}
}

func TestUpDownUp(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.NewEmpty(t).Pool
	m, err := migrate.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()

	if v, verr := m.Version(ctx); verr != nil || v != 0 {
		t.Fatalf("fresh db version = %d, %v; want 0", v, verr)
	}

	results, err := m.Up(ctx)
	if err != nil {
		t.Fatalf("up: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("up applied nothing")
	}
	assertSchemaPresent(t, pool, true)

	// Up is idempotent.
	if again, againErr := m.Up(ctx); againErr != nil || len(again) != 0 {
		t.Fatalf("second up = %d results, %v; want 0, nil", len(again), againErr)
	}

	statuses, err := m.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range statuses {
		if s.State != goose.StateApplied {
			t.Errorf("%s state = %s, want applied", s.Source.Path, s.State)
		}
	}

	// Full rollback leaves no application objects behind.
	if _, err := m.DownTo(ctx, 0); err != nil {
		t.Fatalf("down to 0: %v", err)
	}
	if v, err := m.Version(ctx); err != nil || v != 0 {
		t.Fatalf("version after rollback = %d, %v; want 0", v, err)
	}
	assertSchemaPresent(t, pool, false)

	// And the schema can be re-applied cleanly.
	if _, err := m.Up(ctx); err != nil {
		t.Fatalf("re-up: %v", err)
	}
	assertSchemaPresent(t, pool, true)
}

func assertSchemaPresent(t *testing.T, pool *pgxpool.Pool, want bool) {
	t.Helper()
	ctx := context.Background()
	for _, table := range appTables {
		var exists bool
		if err := pool.QueryRow(ctx, "SELECT to_regclass('public.' || $1) IS NOT NULL", table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != want {
			t.Errorf("table %s exists = %v, want %v", table, exists, want)
		}
	}
	var typeExists, fnExists bool
	if err := pool.QueryRow(ctx, "SELECT to_regtype('delivery_status') IS NOT NULL").Scan(&typeExists); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT to_regproc('set_updated_at') IS NOT NULL").Scan(&fnExists); err != nil {
		t.Fatal(err)
	}
	if typeExists != want || fnExists != want {
		t.Errorf("delivery_status type exists = %v, set_updated_at exists = %v, want %v", typeExists, fnExists, want)
	}
}
