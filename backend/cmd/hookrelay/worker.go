package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/jeevss131436/Hookrelay/backend/internal/backoff"
	"github.com/jeevss131436/Hookrelay/backend/internal/config"
	"github.com/jeevss131436/Hookrelay/backend/internal/delivery"
	"github.com/jeevss131436/Hookrelay/backend/internal/dispatch"
	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
)

// Process roles (ARCHITECTURE D6). `worker` runs everything a deployment
// needs in one process: the Asynq delivery handlers plus the dispatcher and
// reaper loops. `dispatcher` and `reaper` run a single loop each, for
// deployments that want to scale or isolate them. Any mix of these is safe to
// run with several replicas: the loops use SKIP LOCKED and fenced updates.
type roles struct {
	handlers, dispatcher, reaper bool
}

func runWorker(ctx context.Context, e *cmdEnv) error {
	return runRoles(ctx, e, "worker", roles{handlers: true, dispatcher: true, reaper: true})
}

func runDispatcher(ctx context.Context, e *cmdEnv) error {
	return runRoles(ctx, e, "dispatcher", roles{dispatcher: true})
}

func runReaper(ctx context.Context, e *cmdEnv) error {
	return runRoles(ctx, e, "reaper", roles{reaper: true})
}

func runRoles(ctx context.Context, e *cmdEnv, name string, r roles) error {
	if len(e.args) > 0 {
		return newUsageError(fmt.Sprintf("Usage: hookrelay %s\n", name), "unexpected arguments: %v", e.args)
	}
	cfg, log := e.cfg, e.log

	pool, err := connectPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	st := store.New(pool)
	policy := backoff.Default() // one retry policy for the worker and the reaper

	// The reaper needs only Postgres; everything else needs Redis. Fail fast
	// when Redis is unreachable or rejects the credentials at startup; once
	// running, the dispatcher rides out Redis outages.
	var rdb *redis.Client
	if r.handlers || r.dispatcher {
		rdb, err = newRedisClient(ctx, cfg.RedisURL)
		if err != nil {
			return err
		}
		defer func() { _ = rdb.Close() }()
	}

	var srv *asynq.Server
	if r.handlers {
		srv, err = startHandlers(cfg, log, st, rdb, policy)
		if err != nil {
			return err
		}
	}

	// Loops get their own context so they stop before the handlers drain.
	loopCtx, stopLoops := context.WithCancel(ctx)
	defer stopLoops()
	var wg sync.WaitGroup
	if r.dispatcher {
		d := dispatch.NewDispatcher(st, asynq.NewClientFromRedisClient(rdb), dispatch.DispatcherOptions{
			Interval: cfg.DispatchInterval, BatchSize: cfg.DispatchBatchSize, TaskTimeout: cfg.TaskTimeout,
		}, log.With(slog.String("component", "dispatcher")))
		wg.Go(func() { d.Run(loopCtx) })
	}
	if r.reaper {
		rp := dispatch.NewReaper(st, dispatch.ReaperOptions{
			Interval: cfg.ReaperInterval, BatchSize: cfg.ReaperBatchSize,
			DispatchLease: cfg.DispatchLease, Backoff: policy,
		}, log.With(slog.String("component", "reaper")))
		wg.Go(func() { rp.Run(loopCtx) })
	}
	log.Info(name+" started",
		slog.Bool("handlers", r.handlers), slog.Bool("dispatcher", r.dispatcher), slog.Bool("reaper", r.reaper))

	<-ctx.Done()
	// Graceful shutdown (ARCHITECTURE §6.5): stop producing work first. A
	// dispatch batch in progress finishes (enqueue or release) before its
	// loop returns. Then drain in-flight handlers for up to ShutdownTimeout;
	// anything cut off stays in_flight and the reaper recovers it.
	log.Info("shutting down", slog.Duration("timeout", cfg.ShutdownTimeout))
	stopLoops()
	wg.Wait()
	if srv != nil {
		srv.Shutdown()
	}
	log.Info("shutdown complete")
	return nil
}

func connectPostgres(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := store.NewPool(ctx, url)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return pool, nil
}

// startHandlers starts the Asynq server that executes delivery tasks.
func startHandlers(cfg *config.Config, log *slog.Logger, st *store.Store, rdb *redis.Client, policy backoff.Policy) (*asynq.Server, error) {
	cipher, err := secrets.NewCipher(cfg.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("init secret cipher: %w", err)
	}
	if cfg.AllowPrivateTargets {
		log.Warn("HOOKRELAY_ALLOW_PRIVATE_TARGETS=true: SSRF address checks are disabled (development only)")
	}
	sender := delivery.NewSender(delivery.SenderOptions{
		Timeout:      cfg.DeliveryHTTPTimeout,
		AllowPrivate: cfg.AllowPrivateTargets,
	})
	handler := delivery.NewHandler(st, cipher, sender,
		delivery.HandlerOptions{Lease: cfg.DeliveryLease, Backoff: policy}, log.With(slog.String("component", "delivery")))

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
		return nil, fmt.Errorf("start asynq server: %w", err)
	}
	log.Info("delivery handlers started",
		slog.Int("concurrency", cfg.WorkerConcurrency),
		slog.Duration("http_timeout", cfg.DeliveryHTTPTimeout),
		slog.Duration("task_timeout", cfg.TaskTimeout),
		slog.Duration("lease", cfg.DeliveryLease))
	return srv, nil
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
