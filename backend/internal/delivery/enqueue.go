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
// operations. The dispatcher (internal/dispatch) does the same for due
// deliveries in batches.
//
// The row is marked queued and committed first, and only then enqueued, so
// no network call happens inside a transaction (invariant 5). If the enqueue
// fails, the row is released back to scheduled; if even that fails, the
// reaper re-dispatches it once the dispatch lease expires. A task ID conflict
// means this very dispatch is already enqueued and counts as success.
func EnqueueOne(ctx context.Context, st *store.Store, client *asynq.Client, projectID, deliveryID pgtype.UUID,
	taskTimeout time.Duration, opts ...asynq.Option) (Payload, error) {
	row, err := st.MarkDeliveryQueued(ctx, sqlcgen.MarkDeliveryQueuedParams{ProjectID: projectID, ID: deliveryID})
	if store.IsNotFound(err) {
		return Payload{}, ErrNotEnqueueable
	}
	if err != nil {
		return Payload{}, fmt.Errorf("mark delivery queued: %w", err)
	}
	p := Payload{ProjectID: projectID, DeliveryID: deliveryID, AttemptCount: row.AttemptCount, QueuedAt: row.QueuedAt}
	task, err := NewTask(p, taskTimeout)
	if err == nil {
		_, err = client.EnqueueContext(ctx, task, opts...)
	}
	if err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) {
		if _, relErr := st.ReleaseQueuedDeliveries(context.WithoutCancel(ctx), sqlcgen.ReleaseQueuedDeliveriesParams{
			Ids: []pgtype.UUID{deliveryID}, QueuedAt: row.QueuedAt,
		}); relErr != nil {
			return Payload{}, fmt.Errorf("enqueue delivery task: %w (release failed too: %w; the reaper will re-dispatch it)", err, relErr)
		}
		return Payload{}, fmt.Errorf("enqueue delivery task (delivery released back to scheduled): %w", err)
	}
	return p, nil
}
