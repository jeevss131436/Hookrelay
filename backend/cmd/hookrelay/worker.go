package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/jeevss131436/Hookrelay/backend/internal/delivery"
	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
)

// runWorker runs the Asynq server that executes delivery tasks.
//
// Phase 3 scope: task processing only. The dispatcher (moves due deliveries
// from Postgres into Redis) and the reaper (recovers stuck deliveries) are
// Phase 4; until then tasks are enqueued with `hookrelay admin
// enqueue-delivery`.
func runWorker(ctx context.Context, e *cmdEnv) error {
	if len(e.args) > 0 {
		return newUsageError("Usage: hookrelay worker\n", "unexpected arguments: %v", e.args)
	}
	cfg, log := e.cfg, e.log

	pool, err := store.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	err = pool.Ping(pingCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}

	// Unlike the API, the worker fails fast when Redis is unreachable or
	// rejects its credentials: it has nothing useful to do without it.
	rdb, err := newRedisClient(ctx, cfg.RedisURL)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	cipher, err := secrets.NewCipher(cfg.EncryptionKey)
	if err != nil {
		return fmt.Errorf("init secret cipher: %w", err)
	}
	if cfg.AllowPrivateTargets {
		log.Warn("HOOKRELAY_ALLOW_PRIVATE_TARGETS=true: SSRF address checks are disabled (development only)")
	}
	sender := delivery.NewSender(delivery.SenderOptions{
		Timeout:      cfg.DeliveryHTTPTimeout,
		AllowPrivate: cfg.AllowPrivateTargets,
	})
	defer sender.CloseIdleConnections()
	handler := delivery.NewHandler(store.New(pool), cipher, sender,
		delivery.HandlerOptions{Lease: cfg.DeliveryLease}, log.With(slog.String("component", "delivery")))

	asynqLog := log.With(slog.String("component", "asynq"))
	srv := asynq.NewServerFromRedisClient(rdb, asynq.Config{
		Concurrency: cfg.WorkerConcurrency,
		// Handlers get ShutdownTimeout to finish in-flight HTTP calls on
		// SIGTERM. Anything cut off stays in_flight in Postgres and is
		// recovered by the reaper after its lease (ARCHITECTURE §6.5).
		ShutdownTimeout: cfg.ShutdownTimeout,
		Logger:          asynqLogger{asynqLog},
		ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, t *asynq.Task, err error) {
			asynqLog.ErrorContext(ctx, "delivery task failed; archived without retry (Postgres owns retries)",
				slog.String("type", t.Type()), slog.String("error", err.Error()))
		}),
		// Retries never come from Asynq (tasks use MaxRetry(0)); if one ever
		// did, make it slow rather than hot-looping.
		RetryDelayFunc: func(int, error, *asynq.Task) time.Duration { return time.Minute },
	})
	mux := asynq.NewServeMux()
	mux.Handle(delivery.TypeDeliver, handler)

	if err := srv.Start(mux); err != nil {
		return fmt.Errorf("start asynq server: %w", err)
	}
	log.Info("worker started",
		slog.Int("concurrency", cfg.WorkerConcurrency),
		slog.Duration("http_timeout", cfg.DeliveryHTTPTimeout),
		slog.Duration("task_timeout", cfg.TaskTimeout),
		slog.Duration("lease", cfg.DeliveryLease))

	<-ctx.Done()
	log.Info("shutting down", slog.Duration("timeout", cfg.ShutdownTimeout))
	srv.Shutdown() // stops fetching, waits for in-flight handlers up to ShutdownTimeout
	log.Info("shutdown complete")
	return nil
}

// newRedisClient connects to Redis and verifies connectivity and credentials.
// Errors never include the URL (it contains the password).
func newRedisClient(ctx context.Context, redisURL string) (*redis.Client, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: invalid")
	}
	rdb := redis.NewClient(opts)
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("connect to redis: %w", err)
	}
	return rdb, nil
}

// asynqLogger routes Asynq's internal logs into slog.
type asynqLogger struct{ log *slog.Logger }

func (l asynqLogger) Debug(args ...any) { l.log.Debug(fmt.Sprint(args...)) }
func (l asynqLogger) Info(args ...any)  { l.log.Info(fmt.Sprint(args...)) }
func (l asynqLogger) Warn(args ...any)  { l.log.Warn(fmt.Sprint(args...)) }
func (l asynqLogger) Error(args ...any) { l.log.Error(fmt.Sprint(args...)) }
func (l asynqLogger) Fatal(args ...any) {
	l.log.Error(fmt.Sprint(args...), slog.Bool("fatal", true))
	os.Exit(1)
}
