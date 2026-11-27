package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// TestReadyzIntegration probes real Postgres and Redis. It runs only when
// TEST_DATABASE_URL and TEST_REDIS_URL are set (`make test` sets them from
// .env; CI sets them for its service containers) and is skipped otherwise.
func TestReadyzIntegration(t *testing.T) {
	dbURL, redisURL := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_REDIS_URL")
	if dbURL == "" || redisURL == "" {
		t.Skip("TEST_DATABASE_URL / TEST_REDIS_URL not set; skipping integration test")
	}

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("redis url: %v", err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })

	h := NewRouter(discardLogger(), []Check{PostgresCheck(pool), RedisCheck(rdb)})
	rec := do(t, h, http.MethodGet, "/readyz", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	var body readyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Checks["postgres"] != "ok" || body.Checks["redis"] != "ok" {
		t.Errorf("checks = %v", body.Checks)
	}
}
