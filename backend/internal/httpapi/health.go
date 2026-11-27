package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// readyCheckTimeout bounds each dependency probe so /readyz answers quickly
// even when a dependency hangs.
const readyCheckTimeout = 2 * time.Second

// Check is a named dependency probe used by /readyz.
type Check struct {
	Name string
	Ping func(ctx context.Context) error
}

// PostgresCheck probes the database pool.
func PostgresCheck(pool *pgxpool.Pool) Check {
	return Check{Name: "postgres", Ping: pool.Ping}
}

// RedisCheck probes Redis with PING.
func RedisCheck(client redis.UniversalClient) Check {
	return Check{Name: "redis", Ping: func(ctx context.Context) error {
		return client.Ping(ctx).Err()
	}}
}

type readyResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

// handleHealthz is a liveness probe: the process is up and serving HTTP.
// It deliberately checks no dependencies, so a database outage does not get
// the process restarted.
func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz probes every dependency concurrently. It returns 503 if any
// check fails. Error details are logged, not returned, so connection strings
// and hostnames never leak to callers.
func handleReadyz(log *slog.Logger, checks []Check) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		results := make(map[string]string, len(checks))
		var (
			mu sync.Mutex
			wg sync.WaitGroup
		)
		for _, c := range checks {
			wg.Go(func() {
				ctx, cancel := context.WithTimeout(r.Context(), readyCheckTimeout)
				defer cancel()
				status := "ok"
				if err := c.Ping(ctx); err != nil {
					status = "unavailable"
					log.WarnContext(r.Context(), "readiness check failed",
						slog.String("request_id", RequestIDFromContext(r.Context())),
						slog.String("check", c.Name),
						slog.String("error", err.Error()),
					)
				}
				mu.Lock()
				results[c.Name] = status
				mu.Unlock()
			})
		}
		wg.Wait()

		resp := readyResponse{Status: "ok", Checks: results}
		code := http.StatusOK
		for _, s := range results {
			if s != "ok" {
				resp.Status = "unavailable"
				code = http.StatusServiceUnavailable
				break
			}
		}
		writeJSON(w, code, resp)
	}
}
