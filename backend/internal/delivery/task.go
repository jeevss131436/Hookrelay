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

// Payload identifies the delivery a task is for. AttemptCount is the
// delivery's attempt_count when it was enqueued; the claim only succeeds if
// the row still has that count, so every enqueue authorizes at most one
// attempt.
type Payload struct {
	ProjectID    pgtype.UUID `json:"project_id"`
	DeliveryID   pgtype.UUID `json:"delivery_id"`
	AttemptCount int32       `json:"attempt_count"`
}

// TaskID is the Asynq task ID, "{delivery_id}:{attempt_count}". Enqueueing
// the same delivery twice for the same attempt is rejected by Asynq with
// asynq.ErrTaskIDConflict, which callers treat as success.
func (p Payload) TaskID() string {
	return fmt.Sprintf("%s:%d", p.DeliveryID.String(), p.AttemptCount)
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
	return nil
}
