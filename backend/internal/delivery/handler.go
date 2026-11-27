package delivery

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/hibiken/asynq"

	"github.com/jeevss131436/Hookrelay/backend/internal/backoff"
	"github.com/jeevss131436/Hookrelay/backend/internal/endpoints"
	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/signing"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
)

// Outcome is what one task did.
type Outcome string

// Outcomes.
const (
	// OutcomeSkipped: the claim matched no row (duplicate or stale task,
	// delivery already handled, or a task for another project). Nothing sent.
	OutcomeSkipped Outcome = "skipped"
	// OutcomeCancelled: the endpoint was disabled or deleted after dispatch.
	OutcomeCancelled Outcome = "cancelled"
	// OutcomeSucceeded: 2xx; the delivery is complete.
	OutcomeSucceeded Outcome = "succeeded"
	// OutcomeScheduled: the attempt failed; a retry is scheduled in Postgres.
	OutcomeScheduled Outcome = "scheduled"
	// OutcomeDead: the attempt failed and no retry follows.
	OutcomeDead Outcome = "dead"
	// OutcomeFencedOut: the request was sent, but the lease had been taken
	// over by a newer attempt, so only the attempt history was written.
	OutcomeFencedOut Outcome = "fenced_out"
)

// recordTimeout bounds the record transaction. It is detached from the task
// context so that a finished HTTP attempt is still recorded when the worker
// is shutting down or the task deadline passes.
const recordTimeout = 10 * time.Second

// HandlerOptions configures a Handler.
type HandlerOptions struct {
	// Lease is how long a claim is honoured (HOOKRELAY_DELIVERY_LEASE).
	Lease time.Duration
	// Backoff schedules retries; the zero value means backoff.Default().
	Backoff backoff.Policy
}

// Handler processes delivery tasks: claim tx, then HTTP with no tx open,
// then record tx (ARCHITECTURE D3, D4).
type Handler struct {
	store   *store.Store
	cipher  *secrets.Cipher
	sender  *Sender
	lease   time.Duration
	backoff backoff.Policy
	log     *slog.Logger
}

// NewHandler returns a Handler.
func NewHandler(st *store.Store, cipher *secrets.Cipher, sender *Sender, o HandlerOptions, log *slog.Logger) *Handler {
	if o.Lease <= 0 {
		o.Lease = 60 * time.Second
	}
	if o.Backoff.Base() <= 0 {
		o.Backoff = backoff.Default()
	}
	return &Handler{store: st, cipher: cipher, sender: sender, lease: o.Lease, backoff: o.Backoff, log: log}
}

// ProcessTask implements asynq.Handler. It returns nil whenever the outcome
// was settled in Postgres (including failed attempts: Postgres schedules the
// retry). It returns an error only when the delivery could not be processed
// at all (e.g. the database is unreachable); with MaxRetry(0) Asynq then
// archives the task and does not retry it, and the delivery's row in
// Postgres is what gets it retried later.
func (h *Handler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	p, err := ParsePayload(t.Payload())
	if err != nil {
		h.log.ErrorContext(ctx, "dropping malformed delivery task", slog.String("error", err.Error()))
		return fmt.Errorf("%w: %w", asynq.SkipRetry, err)
	}
	_, err = h.Deliver(ctx, p)
	return err
}

// Deliver runs one delivery attempt for p and reports what happened.
func (h *Handler) Deliver(ctx context.Context, p Payload) (Outcome, error) {
	log := h.log.With(
		slog.String("delivery_id", p.DeliveryID.String()),
		slog.String("project_id", p.ProjectID.String()),
	)

	// 1. Claim (short tx).
	c, err := h.claim(ctx, p)
	if err != nil {
		log.ErrorContext(ctx, "claim failed", slog.String("error", err.Error()))
		return "", err
	}
	if c.outcome != "" {
		log.InfoContext(ctx, "delivery task not claimed", slog.String("outcome", string(c.outcome)),
			slog.Int("task_attempt_count", int(p.AttemptCount)))
		return c.outcome, nil
	}
	log = log.With(
		slog.String("event_id", c.row.EventID.String()),
		slog.String("endpoint_id", c.row.EndpointID.String()),
		slog.Int("attempt", int(c.row.AttemptCount)),
	)

	// 2. Sign and send (no transaction open).
	secret, err := endpoints.DecryptSecret(h.cipher, p.ProjectID, c.row.EndpointID, c.target.SecretCiphertext)
	if err != nil {
		// Not the receiver's fault and not fixable by another attempt now
		// (wrong HOOKRELAY_ENCRYPTION_KEY?). The row stays in_flight and the
		// reaper (Phase 4) recovers it once the lease expires.
		log.ErrorContext(ctx, "cannot decrypt endpoint signing secret", slog.String("error", err.Error()))
		return "", err
	}
	headers, err := signing.Sign(secret, p.DeliveryID.String(), time.Now(), c.target.Payload)
	if err != nil {
		log.ErrorContext(ctx, "cannot sign delivery", slog.String("error", err.Error()))
		return "", fmt.Errorf("sign delivery: %w", err)
	}
	res := h.sender.Send(ctx, Request{URL: c.target.URL, Headers: headers, Body: c.target.Payload})

	// 3. Record (short tx).
	outcome, next, err := h.record(ctx, p, c.row.AttemptCount, res)
	attrs := []any{
		slog.String("outcome", string(outcome)),
		slog.Int("status_code", res.StatusCode),
		slog.Int64("duration_ms", res.Duration.Milliseconds()),
	}
	if !res.Success() {
		attrs = append(attrs, slog.String("error_kind", res.ErrorKind), slog.String("error", res.ErrorMsg))
	}
	if outcome == OutcomeScheduled {
		attrs = append(attrs, slog.Time("next_attempt_at", next))
	}
	if err != nil {
		log.ErrorContext(ctx, "recording delivery attempt failed", append(attrs, slog.String("record_error", err.Error()))...)
		return "", err
	}
	switch outcome {
	case OutcomeFencedOut:
		log.WarnContext(ctx, "delivery attempt fenced out: a newer attempt owns this delivery", attrs...)
	case OutcomeSucceeded:
		log.InfoContext(ctx, "delivery attempt succeeded", attrs...)
	default:
		log.WarnContext(ctx, "delivery attempt failed", attrs...)
	}
	return outcome, nil
}

type claimed struct {
	outcome Outcome // non-empty: nothing to send
	row     sqlcgen.ClaimDeliveryRow
	target  sqlcgen.GetDeliveryTargetRow
}

func (h *Handler) claim(ctx context.Context, p Payload) (claimed, error) {
	var c claimed
	err := h.store.InTx(ctx, func(q *sqlcgen.Queries) error {
		c = claimed{}
		n, err := q.CancelQueuedDeliveryForInactiveEndpoint(ctx, sqlcgen.CancelQueuedDeliveryForInactiveEndpointParams{
			ProjectID: p.ProjectID, ID: p.DeliveryID, ExpectedAttemptCount: p.AttemptCount,
		})
		if err != nil {
			return fmt.Errorf("cancel for inactive endpoint: %w", err)
		}
		if n > 0 {
			c.outcome = OutcomeCancelled
			return nil
		}
		row, err := q.ClaimDelivery(ctx, sqlcgen.ClaimDeliveryParams{
			LeaseMs: h.lease.Milliseconds(), ProjectID: p.ProjectID, ID: p.DeliveryID,
			ExpectedAttemptCount: p.AttemptCount,
		})
		if store.IsNotFound(err) {
			c.outcome = OutcomeSkipped
			return nil
		}
		if err != nil {
			return fmt.Errorf("claim: %w", err)
		}
		target, err := q.GetDeliveryTarget(ctx, sqlcgen.GetDeliveryTargetParams{ProjectID: p.ProjectID, ID: p.DeliveryID})
		if err != nil {
			return fmt.Errorf("load delivery target: %w", err)
		}
		c.row, c.target = row, target
		return nil
	})
	return c, err
}

// record writes the attempt row and moves the delivery to its next state,
// fenced on status='in_flight' and attempt_count.
func (h *Handler) record(ctx context.Context, p Payload, attempt int32, res Result) (Outcome, time.Time, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordTimeout)
	defer cancel()

	ms := clampInt32(res.Duration.Milliseconds())
	row := sqlcgen.InsertDeliveryAttemptParams{
		ProjectID:     p.ProjectID,
		DeliveryID:    p.DeliveryID,
		AttemptNumber: attempt,
		StartedAt:     res.StartedAt,
		// Derived from the monotonic duration so finished_at >= started_at
		// holds even if the wall clock steps.
		FinishedAt: res.StartedAt.Add(res.Duration),
		DurationMs: ms,
	}
	var statusCode *int32
	if res.StatusCode != 0 {
		sc := clampInt32(int64(res.StatusCode))
		statusCode = &sc
		row.ResponseStatus = statusCode
		row.ResponseBody = &res.Body
	}
	if !res.Success() {
		row.ErrorKind, row.ErrorMessage = &res.ErrorKind, &res.ErrorMsg
	}

	var (
		outcome Outcome
		next    time.Time
	)
	err := h.store.InTx(ctx, func(q *sqlcgen.Queries) error {
		if _, err := q.InsertDeliveryAttempt(ctx, row); err != nil {
			return fmt.Errorf("insert attempt: %w", err)
		}
		if res.Success() {
			n, err := q.MarkDeliverySucceeded(ctx, sqlcgen.MarkDeliverySucceededParams{
				LastStatusCode: statusCode, ProjectID: p.ProjectID, ID: p.DeliveryID, AttemptCount: attempt,
			})
			if err != nil {
				return fmt.Errorf("mark succeeded: %w", err)
			}
			outcome = OutcomeSucceeded
			if n == 0 {
				outcome = OutcomeFencedOut
			}
			return nil
		}
		delay := h.backoff.Delay(int(attempt), res.RetryAfter)
		upd, err := q.MarkDeliveryFailed(ctx, sqlcgen.MarkDeliveryFailedParams{
			Permanent:      res.Permanent,
			RetryDelayMs:   delay.Milliseconds(),
			LastStatusCode: statusCode,
			LastError:      &res.ErrorMsg,
			ProjectID:      p.ProjectID,
			ID:             p.DeliveryID,
			AttemptCount:   attempt,
		})
		if store.IsNotFound(err) {
			outcome = OutcomeFencedOut
			return nil
		}
		if err != nil {
			return fmt.Errorf("mark failed: %w", err)
		}
		outcome, next = OutcomeScheduled, upd.NextAttemptAt
		if upd.Status == sqlcgen.DeliveryStatusDead {
			outcome, next = OutcomeDead, time.Time{}
		}
		return nil
	})
	if err != nil {
		return "", time.Time{}, err
	}
	return outcome, next, nil
}

func clampInt32(v int64) int32 {
	switch {
	case v > math.MaxInt32:
		return math.MaxInt32
	case v < 0:
		return 0
	default:
		return int32(v)
	}
}
