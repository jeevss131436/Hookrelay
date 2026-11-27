// Package pgtest provides isolated PostgreSQL databases for integration tests.
//
// Each call creates a brand-new database on the server named by
// TEST_DATABASE_URL and drops it when the test ends, so tests never share
// state and can run in parallel. Tests are skipped when TEST_DATABASE_URL is
// unset. The connecting role needs the CREATEDB privilege (the docker compose
// and CI superusers have it).
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jeevss131436/Hookrelay/backend/internal/migrate"
)

// EnvVar names the server used for integration tests.
const EnvVar = "TEST_DATABASE_URL"

// DB is an isolated test database.
type DB struct {
	Pool *pgxpool.Pool
	URL  string // connection URL for this database (for code that dials itself)
}

// NewMigrated returns a fresh database with all migrations applied.
func NewMigrated(t testing.TB) DB {
	t.Helper()
	db := NewEmpty(t)
	m, err := migrate.New(db.Pool)
	if err != nil {
		t.Fatalf("pgtest: migrator: %v", err)
	}
	defer func() { _ = m.Close() }()
	if _, err := m.Up(context.Background()); err != nil {
		t.Fatalf("pgtest: migrate up: %v", err)
	}
	return db
}

// sameRemoteServer reports whether a and b address the same non-local
// Postgres server. Neon's pooled and direct hostnames differ only by a
// "-pooler" suffix on the first label, so that is normalized away.
func sameRemoteServer(a, b *url.URL) bool {
	norm := func(u *url.URL) string {
		return strings.Replace(strings.ToLower(u.Hostname()), "-pooler.", ".", 1)
	}
	ha, hb := norm(a), norm(b)
	if ha == "" || hb == "" || ha != hb {
		return false
	}
	switch ha {
	case "localhost", "127.0.0.1", "::1":
		return false
	}
	return true
}

// NewEmpty returns a fresh, empty database. It skips the test when
// TEST_DATABASE_URL is unset.
func NewEmpty(t testing.TB) DB {
	t.Helper()
	baseURL := os.Getenv(EnvVar)
	if baseURL == "" {
		t.Skip(EnvVar + " not set; skipping PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var suffix [6]byte
	_, _ = rand.Read(suffix[:])
	name := "hookrelay_test_" + hex.EncodeToString(suffix[:])

	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatalf("pgtest: %s must be a postgres:// URL", EnvVar)
	}
	// Tests CREATE and DROP databases on this server. Refuse to run against
	// the server the application itself uses (e.g. a Neon production branch),
	// unless it is a local development server.
	for _, appVar := range []string{"DATABASE_URL", "DATABASE_URL_UNPOOLED"} {
		if app, parseErr := url.Parse(os.Getenv(appVar)); parseErr == nil && sameRemoteServer(u, app) {
			t.Fatalf("pgtest: %s points at the same server as %s (%s); use a disposable test server or Neon branch",
				EnvVar, appVar, app.Hostname())
		}
	}

	u.Path = "/" + name
	dbURL := u.String()

	admin, err := pgx.Connect(ctx, baseURL)
	if err != nil {
		t.Fatalf("pgtest: connect to %s: %v", EnvVar, err)
	}
	defer func() { _ = admin.Close(context.Background()) }()
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("pgtest: create database: %v", err)
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("pgtest: pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, baseURL)
		if err != nil {
			t.Errorf("pgtest: cleanup connect: %v", err)
			return
		}
		defer func() { _ = conn.Close(context.Background()) }()
		if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("pgtest: drop database %s: %v", name, err)
		}
	})
	return DB{Pool: pool, URL: dbURL}
}
