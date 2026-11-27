// Package delivery is the webhook delivery engine: the Asynq task that
// carries a delivery, the handler that claims, sends and records it, and the
// SSRF-guarded HTTP sender (docs/ARCHITECTURE.md §3, D2-D5).
//
// Redis/Asynq is only the hand-off channel. PostgreSQL owns all delivery
// state, including the retry schedule: tasks are enqueued with MaxRetry(0),
// and a failed attempt is rescheduled by writing status='scheduled' and
// next_attempt_at in Postgres, never by Asynq.
package delivery

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgtype"
)

// TypeDeliver is the Asynq task type for one delivery attempt.
const TypeDeliver = "delivery:deliver"

// Payload identifies one dispatch of a delivery. AttemptCount and QueuedAt
// are the row's values when it was moved to 'queued'; the worker's claim only
// succeeds if the row still has both, so every dispatch authorizes at most
// one attempt, and a task left over from an earlier dispatch is a no-op.
type Payload struct {
	ProjectID    pgtype.UUID `json:"project_id"`
	DeliveryID   pgtype.UUID `json:"delivery_id"`
	AttemptCount int32       `json:"attempt_count"`
	QueuedAt     time.Time   `json:"queued_at"`
}

// TaskID is the Asynq task ID, "{delivery_id}:{attempt_count}:{queued_at µs}":
// unique per dispatch. Enqueueing the same dispatch twice is rejected by Asynq
// with asynq.ErrTaskIDConflict, which callers treat as success.
//
// queued_at is part of the ID because Asynq keeps the IDs of archived tasks
// (a task that timed out or whose handler failed). With an ID of only
// "{delivery_id}:{attempt_count}", every later re-dispatch of that attempt
// would conflict with the archived task, count as "already enqueued", and the
// delivery would never run.
func (p Payload) TaskID() string {
	return fmt.Sprintf("%s:%d:%d", p.DeliveryID.String(), p.AttemptCount, p.QueuedAt.UnixMicro())
}

// NewTask builds the task for p. MaxRetry(0) is essential: Postgres, not
// Asynq, owns retries (CLAUDE.md invariant 3). timeout must be the configured
// HOOKRELAY_TASK_TIMEOUT, which config validates to sit between the HTTP
// timeout and the lease. Callers may add asynq.Queue when enqueueing.
func NewTask(p Payload, timeout time.Duration) (*asynq.Task, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal delivery task: %w", err)
	}
	return asynq.NewTask(TypeDeliver, b,
		asynq.MaxRetry(0),
		asynq.Timeout(timeout),
		asynq.TaskID(p.TaskID()),
	), nil
}

// ParsePayload decodes and validates a task payload.
func ParsePayload(b []byte) (Payload, error) {
	var p Payload
	if err := json.Unmarshal(b, &p); err != nil {
		return Payload{}, fmt.Errorf("decode delivery task: %w", err)
	}
	if err := p.validate(); err != nil {
		return Payload{}, err
	}
	return p, nil
}

func (p Payload) validate() error {
	if !p.ProjectID.Valid || !p.DeliveryID.Valid {
		return errors.New("delivery task: project_id and delivery_id are required")
	}
	if p.AttemptCount < 0 {
		return errors.New("delivery task: attempt_count must not be negative")
	}
	if p.QueuedAt.IsZero() {
		return errors.New("delivery task: queued_at is required")
	}
	return nil
}
