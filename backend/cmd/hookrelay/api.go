package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/jeevss131436/Hookrelay/backend/internal/config"
	"github.com/jeevss131436/Hookrelay/backend/internal/endpoints"
	"github.com/jeevss131436/Hookrelay/backend/internal/history"
	"github.com/jeevss131436/Hookrelay/backend/internal/httpapi"
	"github.com/jeevss131436/Hookrelay/backend/internal/ingest"
	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/ssrf"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
)

func runAPI(ctx context.Context, e *cmdEnv) error {
	if len(e.args) > 0 {
		return newUsageError("Usage: hookrelay api\n", "unexpected arguments: %v", e.args)
	}
	cfg, log := e.cfg, e.log

	// Neither client connects eagerly: the API starts even if a dependency is
	// down and reports it through /readyz instead of crash-looping.
	pool, err := store.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("parse redis url: %w", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer func() { _ = rdb.Close() }()

	cipher, err := secrets.NewCipher(cfg.EncryptionKey)
	if err != nil {
		return fmt.Errorf("init secret cipher: %w", err)
	}
	st := store.New(pool)
	// Redis is used only for /readyz here: ingestion writes to PostgreSQL
	// alone (transactional outbox) and keeps working when Redis is down.
	api := httpapi.NewAPI(log, st,
		endpoints.NewService(st, cipher, ssrf.Policy{
			RequireHTTPS: cfg.Env == config.EnvProduction,
			AllowPrivate: cfg.AllowPrivateTargets,
		}),
		ingest.NewService(st),
		history.NewService(st),
	)
	if cfg.AllowPrivateTargets {
		log.Warn("HOOKRELAY_ALLOW_PRIVATE_TARGETS=true: SSRF address checks are disabled (development only)")
	}

	srv := &http.Server{
		Handler: httpapi.NewRouter(log,
			[]httpapi.Check{httpapi.PostgresCheck(pool), httpapi.RedisCheck(rdb)},
			api.Routes()),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		BaseContext:       func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
	}

	// Bind before logging "listening" so a port conflict fails fast.
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	log.Info("api listening", slog.String("addr", ln.Addr().String()))

	return serve(ctx, srv, ln, cfg.ShutdownTimeout, log)
}

// serve runs srv on ln until ctx is cancelled, then shuts down gracefully:
// it stops accepting connections and waits up to shutdownTimeout for
// in-flight requests to finish.
func serve(ctx context.Context, srv *http.Server, ln net.Listener, shutdownTimeout time.Duration, log *slog.Logger) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	log.Info("shutting down", slog.Duration("timeout", shutdownTimeout))
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	log.Info("shutdown complete")
	return nil
}
