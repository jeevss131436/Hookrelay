package delivery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
)

// ErrNotEnqueueable means the delivery does not exist in the project, or is
// not pending/scheduled (it is already queued, in flight, or finished).
var ErrNotEnqueueable = errors.New("delivery not found or not pending/scheduled")

// EnqueueOne hands a single pending or scheduled delivery to the queue,
// immediately and regardless of next_attempt_at. It backs the
// `hookrelay admin enqueue-delivery` command, for manual testing and
// operations. It is NOT the dispatcher (Phase 4), which does the same for
// due deliveries in batches.
//
// The row is marked queued and committed first, and only then enqueued, so
// no network call happens inside the transaction (invariant 5). If the
// enqueue fails, the row stays queued with no task; the Phase 4 reaper moves
// such rows back to scheduled. A task ID conflict means the same attempt is
// already queued and counts as success.
func EnqueueOne(ctx context.Context, st *store.Store, client *asynq.Client, projectID, deliveryID pgtype.UUID,
	taskTimeout time.Duration, opts ...asynq.Option) (Payload, error) {
	attemptCount, err := st.MarkDeliveryQueued(ctx, sqlcgen.MarkDeliveryQueuedParams{ProjectID: projectID, ID: deliveryID})
	if store.IsNotFound(err) {
		return Payload{}, ErrNotEnqueueable
	}
	if err != nil {
		return Payload{}, fmt.Errorf("mark delivery queued: %w", err)
	}
	p := Payload{ProjectID: projectID, DeliveryID: deliveryID, AttemptCount: attemptCount}
	task, err := NewTask(p, taskTimeout)
	if err != nil {
		return Payload{}, err
	}
	if _, err := client.EnqueueContext(ctx, task, opts...); err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) {
		return Payload{}, fmt.Errorf("enqueue delivery task (row left queued for the reaper): %w", err)
	}
	return p, nil
}
