package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jeevss131436/Hookrelay/backend/internal/backoff"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
)

// workerLostMessage is stored on the attempt and as the delivery's last_error.
const workerLostMessage = "worker lost: lease expired before the attempt was recorded"

// ReaperOptions configures a Reaper.
type ReaperOptions struct {
	Interval  time.Duration // HOOKRELAY_REAPER_INTERVAL
	BatchSize int           // HOOKRELAY_REAPER_BATCH_SIZE, per kind of recovery
	// DispatchLease: queued rows older than this are re-dispatched
	// (HOOKRELAY_DISPATCH_LEASE).
	DispatchLease time.Duration
	// Backoff is the delivery retry policy: the same one the worker uses,
	// so a lost attempt is retried exactly like a failed one.
	Backoff backoff.Policy
}

// ReapResult counts what one pass did.
type ReapResult struct {
	Requeued  int64 // stale queued -> scheduled (no attempt consumed)
	Scheduled int   // expired in-flight -> scheduled (attempt recorded as worker_lost)
	Dead      int   // expired in-flight on its last attempt -> dead
}

// Reaper recovers deliveries that stopped making progress.
type Reaper struct {
	store *store.Store
	opts  ReaperOptions
	log   *slog.Logger
}

// NewReaper returns a Reaper.
func NewReaper(st *store.Store, o ReaperOptions, log *slog.Logger) *Reaper {
	if o.Interval <= 0 {
		o.Interval = 30 * time.Second
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 500
	}
	if o.DispatchLease <= 0 {
		o.DispatchLease = 5 * time.Minute
	}
	if o.Backoff.Base() <= 0 {
		o.Backoff = backoff.Default()
	}
	return &Reaper{store: st, opts: o, log: log}
}

// Run reaps every Interval until ctx is cancelled. Errors are logged and the
// next pass retries; Run never gives up.
func (r *Reaper) Run(ctx context.Context) {
	r.log.Info("reaper started", slog.Duration("interval", r.opts.Interval),
		slog.Int("batch_size", r.opts.BatchSize), slog.Duration("dispatch_lease", r.opts.DispatchLease))
	for {
		res, err := r.RunOnce(ctx)
		if err != nil {
			r.log.Warn("reaper pass failed; will retry", slog.String("error", err.Error()))
		}
		if res.Requeued > 0 || res.Scheduled > 0 || res.Dead > 0 {
			r.log.Info("reaper recovered deliveries", slog.Int64("requeued_stale_queued", res.Requeued),
				slog.Int("rescheduled_lost_attempts", res.Scheduled), slog.Int("dead_lost_attempts", res.Dead))
		}
		if !sleep(ctx, r.opts.Interval) {
			r.log.Info("reaper stopped")
			return
		}
	}
}

// RunOnce performs one pass. It is idempotent: a second pass over the same
// state finds nothing to do. Safe to run concurrently with other reapers,
// dispatchers and workers.
func (r *Reaper) RunOnce(ctx context.Context) (ReapResult, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), batchTimeout)
	defer cancel()
	var res ReapResult

	// A. Queued but never claimed within the dispatch lease.
	n, err := r.store.RequeueStaleQueuedDeliveries(ctx, sqlcgen.RequeueStaleQueuedDeliveriesParams{
		StaleAfterMs: r.opts.DispatchLease.Milliseconds(),
		BatchSize:    clampInt32(r.opts.BatchSize),
	})
	if err != nil {
		return res, fmt.Errorf("requeue stale queued deliveries: %w", err)
	}
	res.Requeued = n

	// B. In flight past the worker lease: the worker crashed or hung.
	rows, err := r.store.ListExpiredInFlightDeliveries(ctx, clampInt32(r.opts.BatchSize))
	if err != nil {
		return res, fmt.Errorf("list expired in-flight deliveries: %w", err)
	}
	var firstErr error
	for _, row := range rows {
		status, err := r.recoverLost(ctx, row)
		switch {
		case err != nil:
			r.log.Error("could not recover expired delivery", slog.String("delivery_id", row.ID.String()),
				slog.String("error", err.Error()))
			if firstErr == nil {
				firstErr = err
			}
		case status == sqlcgen.DeliveryStatusDead:
			res.Dead++
		case status == sqlcgen.DeliveryStatusScheduled:
			res.Scheduled++
		}
	}
	return res, firstErr
}

// errNothingToRecover rolls back a recovery whose row moved on meanwhile.
var errNothingToRecover = errors.New("delivery no longer in flight on this attempt")

// recoverLost records the abandoned attempt and applies the normal retry
// policy, in one transaction fenced on status='in_flight', the attempt_count
// read earlier, and an expired lease.
//
// Lock order matches the worker's record transaction (attempt row first, then
// the delivery row), so a worker finishing at the same moment cannot deadlock
// with the reaper: one of them wins and the other's fenced UPDATE matches
// nothing. It returns "" when there was nothing (left) to recover.
func (r *Reaper) recoverLost(ctx context.Context, row sqlcgen.ListExpiredInFlightDeliveriesRow) (sqlcgen.DeliveryStatus, error) {
	var status sqlcgen.DeliveryStatus
	errMsg := workerLostMessage
	err := r.store.InTx(ctx, func(q *sqlcgen.Queries) error {
		if _, err := q.InsertWorkerLostAttempt(ctx, sqlcgen.InsertWorkerLostAttemptParams{
			ErrorMessage: &errMsg, ProjectID: row.ProjectID, ID: row.ID, AttemptCount: row.AttemptCount,
		}); err != nil {
			return fmt.Errorf("record lost attempt: %w", err)
		}
		upd, err := q.MarkDeliveryFailed(ctx, sqlcgen.MarkDeliveryFailedParams{
			Permanent:    false,
			RetryDelayMs: r.opts.Backoff.Delay(int(row.AttemptCount), 0).Milliseconds(),
			LastError:    &errMsg,
			ProjectID:    row.ProjectID,
			ID:           row.ID,
			AttemptCount: row.AttemptCount,
		})
		if store.IsNotFound(err) {
			// The worker recorded its result, or another reaper got here
			// first: undo our attempt row (if any) and leave the delivery.
			return errNothingToRecover
		}
		if err != nil {
			return fmt.Errorf("reschedule lost attempt: %w", err)
		}
		status = upd.Status
		return nil
	})
	if errors.Is(err, errNothingToRecover) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	r.log.Warn("recovered delivery from a lost worker",
		slog.String("delivery_id", row.ID.String()), slog.String("project_id", row.ProjectID.String()),
		slog.Int("attempt", int(row.AttemptCount)), slog.String("status", string(status)))
	return status, nil
}
