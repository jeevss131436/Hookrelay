// Package dispatch runs the two background loops that connect the
// transactional outbox (the deliveries table) to the worker queue
// (docs/ARCHITECTURE.md §3.6):
//
//   - The Dispatcher moves due deliveries from Postgres into Redis/Asynq.
//   - The Reaper recovers deliveries that stopped making progress: queued
//     rows whose task never ran, and in-flight rows whose worker lease expired.
//
// Postgres stays the source of truth. Redis is touched only outside database
// transactions, and every state change is a conditional UPDATE, so several
// dispatchers and reapers can run at once (SKIP LOCKED, fencing).
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jeevss131436/Hookrelay/backend/internal/delivery"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
)

// Enqueuer is the part of *asynq.Client the dispatcher uses.
type Enqueuer interface {
	EnqueueContext(ctx context.Context, task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error)
}

// DispatcherOptions configures a Dispatcher.
type DispatcherOptions struct {
	Interval    time.Duration // poll period when idle (HOOKRELAY_DISPATCH_INTERVAL)
	BatchSize   int           // rows per batch (HOOKRELAY_DISPATCH_BATCH_SIZE)
	TaskTimeout time.Duration // Asynq task timeout (HOOKRELAY_TASK_TIMEOUT)
	// RedisRetryDelay is how long released rows wait before the next dispatch
	// attempt after an enqueue failure. Zero means 5s.
	RedisRetryDelay time.Duration
	// MaxBackoff caps the pause between batches after consecutive errors.
	// Zero means 30s.
	MaxBackoff time.Duration
	// Queue is the Asynq queue; empty means "default". Tests use their own.
	Queue string
}

// batchTimeout bounds one batch. The batch runs on a context detached from
// shutdown so that rows already moved to 'queued' are either enqueued or
// released before the dispatcher exits.
const batchTimeout = 30 * time.Second

// Dispatcher moves due deliveries into the worker queue.
type Dispatcher struct {
	store *store.Store
	queue Enqueuer
	opts  DispatcherOptions
	log   *slog.Logger
}

// NewDispatcher returns a Dispatcher.
func NewDispatcher(st *store.Store, q Enqueuer, o DispatcherOptions, log *slog.Logger) *Dispatcher {
	if o.Interval <= 0 {
		o.Interval = 500 * time.Millisecond
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 500
	}
	if o.TaskTimeout <= 0 {
		o.TaskTimeout = 30 * time.Second
	}
	if o.RedisRetryDelay <= 0 {
		o.RedisRetryDelay = 5 * time.Second
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 30 * time.Second
	}
	return &Dispatcher{store: st, queue: q, opts: o, log: log}
}

// Run dispatches until ctx is cancelled. Errors (database or Redis down) are
// logged and retried with exponential backoff; Run never gives up. A full
// batch is followed immediately by the next one to drain a backlog.
func (d *Dispatcher) Run(ctx context.Context) {
	d.log.Info("dispatcher started", slog.Duration("interval", d.opts.Interval), slog.Int("batch_size", d.opts.BatchSize))
	failures := 0
	for {
		n, err := d.RunOnce(ctx)
		wait := d.opts.Interval
		switch {
		case err != nil:
			failures++
			wait = backoffWait(d.opts.Interval, d.opts.MaxBackoff, failures)
			d.log.Warn("dispatch batch failed; will retry",
				slog.String("error", err.Error()), slog.Int("enqueued", n),
				slog.Int("consecutive_failures", failures), slog.Duration("retry_in", wait))
		case n >= d.opts.BatchSize:
			failures, wait = 0, 0
		default:
			if failures > 0 {
				d.log.Info("dispatcher recovered", slog.Int("after_failures", failures))
			}
			failures = 0
		}
		if !sleep(ctx, wait) {
			d.log.Info("dispatcher stopped")
			return
		}
	}
}

// RunOnce dispatches one batch and returns how many tasks were enqueued.
//
// Step 1 commits the move to 'queued' before step 2 talks to Redis, so no
// Redis call happens inside a transaction. Crash and failure analysis:
//   - Crash after step 1: rows are 'queued' without tasks. The reaper
//     re-dispatches them once the dispatch lease expires.
//   - Redis fails in step 2: the failed row and the rest of the batch are
//     released back to 'scheduled' (retried after RedisRetryDelay). If the
//     release fails too, the reaper again covers them.
//   - Enqueue succeeded but the response was lost (timeout): the row may be
//     released while the task exists. That task is then stale (the next
//     dispatch has a new queued_at), so the worker's claim ignores it.
func (d *Dispatcher) RunOnce(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), batchTimeout)
	defer cancel()

	rows, err := d.claimBatch(ctx)
	if err != nil {
		return 0, err
	}
	return d.enqueueBatch(ctx, rows)
}

// claimBatch is step 1: due rows -> 'queued' (one statement, own transaction).
func (d *Dispatcher) claimBatch(ctx context.Context) ([]sqlcgen.ClaimDueDeliveriesForDispatchRow, error) {
	rows, err := d.store.ClaimDueDeliveriesForDispatch(ctx, clampInt32(d.opts.BatchSize))
	if err != nil {
		return nil, fmt.Errorf("claim due deliveries: %w", err)
	}
	return rows, nil
}

// enqueueBatch is step 2: one Asynq task per row, outside any transaction.
func (d *Dispatcher) enqueueBatch(ctx context.Context, rows []sqlcgen.ClaimDueDeliveriesForDispatchRow) (int, error) {
	var opts []asynq.Option
	if d.opts.Queue != "" {
		opts = append(opts, asynq.Queue(d.opts.Queue))
	}
	for i, r := range rows {
		p := delivery.Payload{ProjectID: r.ProjectID, DeliveryID: r.ID, AttemptCount: r.AttemptCount, QueuedAt: r.QueuedAt}
		task, err := delivery.NewTask(p, d.opts.TaskTimeout)
		if err == nil {
			_, err = d.queue.EnqueueContext(ctx, task, opts...)
		}
		if err == nil || errors.Is(err, asynq.ErrTaskIDConflict) {
			// Conflict: this exact dispatch is already in Redis.
			continue
		}
		// Redis is most likely down: stop and give back this row and the
		// rest of the batch rather than hammering it.
		d.release(ctx, rows[i:])
		return i, fmt.Errorf("enqueue delivery task: %w", err)
	}
	if len(rows) > 0 {
		d.log.Debug("dispatched deliveries", slog.Int("count", len(rows)))
	}
	return len(rows), nil
}

func (d *Dispatcher) release(ctx context.Context, rows []sqlcgen.ClaimDueDeliveriesForDispatchRow) {
	ids := make([]pgtype.UUID, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	// All rows of a batch share queued_at (one statement's now()).
	n, err := d.store.ReleaseQueuedDeliveries(ctx, sqlcgen.ReleaseQueuedDeliveriesParams{
		RetryDelayMs: d.opts.RedisRetryDelay.Milliseconds(),
		Ids:          ids,
		QueuedAt:     rows[0].QueuedAt,
	})
	if err != nil {
		d.log.Error("could not release undispatched deliveries; the reaper will re-dispatch them after the dispatch lease",
			slog.Int("count", len(rows)), slog.String("error", err.Error()))
		return
	}
	d.log.Warn("released undispatched deliveries back to scheduled",
		slog.Int64("count", n), slog.Duration("retry_in", d.opts.RedisRetryDelay))
}

// backoffWait is base * 2^failures, capped.
func backoffWait(base, limit time.Duration, failures int) time.Duration {
	w := base
	for i := 0; i < failures && w < limit; i++ {
		w *= 2
	}
	return min(w, limit)
}

// sleep waits for d or until ctx is done; it reports whether to continue.
func sleep(ctx context.Context, d time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func clampInt32(v int) int32 {
	if v > 1<<31-1 {
		return 1<<31 - 1
	}
	if v < 0 {
		return 0
	}
	return int32(v)
}
