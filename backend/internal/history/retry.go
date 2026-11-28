package history

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
)

// Reasons a manual retry is refused (NotRetryableError.Reason).
const (
	// ReasonStatus: the delivery is not dead (succeeded, cancelled, or still
	// being worked on: pending, queued, in_flight, scheduled).
	ReasonStatus = "status"
	// ReasonEndpointInactive: the endpoint is disabled or deleted, so the
	// worker would cancel the delivery instead of sending it.
	ReasonEndpointInactive = "endpoint_inactive"
)

// NotRetryableError reports why a delivery cannot be retried manually.
type NotRetryableError struct {
	Reason string
	Status sqlcgen.DeliveryStatus // the delivery's status when refused
}

func (e *NotRetryableError) Error() string {
	if e.Reason == ReasonEndpointInactive {
		return "delivery cannot be retried: its endpoint is disabled or deleted"
	}
	return fmt.Sprintf("delivery cannot be retried: status is %q, only dead deliveries can be retried", e.Status)
}

// Retry makes a dead delivery due again for exactly one more attempt
// (RetryDeadDelivery in db/queries/reads.sql) and returns its new state.
//
// The transition is one conditional UPDATE (WHERE status = 'dead'), so of
// concurrent retries exactly one succeeds and the others get a
// NotRetryableError. Attempt history is untouched and attempt_count is not
// reset, so the attempt fences used by workers and the reaper keep working.
// Nothing is enqueued here: the dispatcher picks the delivery up because it
// is 'pending' with next_attempt_at = now().
//
// Errors: ErrDeliveryNotFound, *NotRetryableError, or an internal error.
func (s *Service) Retry(ctx context.Context, projectID, id pgtype.UUID) (DeliveryDetail, error) {
	var (
		out     DeliveryDetail
		refused bool
	)
	// The read of the new state happens in the same transaction, so the
	// response shows the state the retry produced ('pending'), not whatever a
	// dispatcher did a moment later.
	err := s.store.InTx(ctx, func(q *sqlcgen.Queries) error {
		_, err := q.RetryDeadDelivery(ctx, sqlcgen.RetryDeadDeliveryParams{ProjectID: projectID, ID: id})
		if store.IsNotFound(err) {
			refused = true
			return nil
		}
		if err != nil {
			return fmt.Errorf("retry delivery: %w", err)
		}
		out, err = getDelivery(ctx, q, projectID, id)
		return err
	})
	if err != nil {
		return DeliveryDetail{}, err
	}
	if !refused {
		return out, nil
	}

	// The UPDATE matched nothing; explain why from the current state.
	d, err := s.GetDelivery(ctx, projectID, id)
	if err != nil {
		return DeliveryDetail{}, err // ErrDeliveryNotFound for missing / other project
	}
	switch {
	case d.Status != sqlcgen.DeliveryStatusDead:
		return DeliveryDetail{}, &NotRetryableError{Reason: ReasonStatus, Status: d.Status}
	case !d.EndpointEnabled || d.EndpointDeleted:
		return DeliveryDetail{}, &NotRetryableError{Reason: ReasonEndpointInactive, Status: d.Status}
	default:
		// Dead with an active endpoint now, but not when the UPDATE ran (the
		// delivery died or the endpoint was re-enabled in between).
		return DeliveryDetail{}, ErrRetryRace
	}
}

// ErrRetryRace: the delivery's state changed while the retry was evaluated.
// Sending the retry again will succeed if it is still eligible.
var ErrRetryRace = errors.New("delivery state changed during retry; try again")
