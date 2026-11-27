// Package ingest accepts events (docs/ARCHITECTURE.md §3.1, decision D1).
//
// The event row and one pending delivery per matching endpoint are written in
// a single PostgreSQL transaction: the deliveries table is the transactional
// outbox. This package deliberately has no Redis dependency. Ingestion keeps
// working when Redis is down, and the dispatcher (Phase 4) later moves pending
// deliveries from Postgres to the queue.
package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jeevss131436/Hookrelay/backend/internal/endpoints"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
	"github.com/jeevss131436/Hookrelay/backend/internal/validation"
)

// Limits (PRODUCT_SPEC §4.1). MaxPayloadBytes mirrors the CHECK on events.payload.
const (
	MaxPayloadBytes        = 256 * 1024
	MaxIdempotencyKeyBytes = 255
)

// ErrIdempotencyConflict means the idempotency key was already used in this
// project for a different event_type or payload.
var ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")

// Request is a validated-at-the-edge ingest request. Payload must be the exact
// JSON bytes received; they are stored, signed and delivered verbatim (D5).
type Request struct {
	EventType      string
	Payload        []byte
	IdempotencyKey *string
}

// Result describes the stored event.
type Result struct {
	EventID           pgtype.UUID
	EventType         string
	IdempotencyKey    *string
	CreatedAt         time.Time
	DeliveriesCreated int64
	// Replayed is true when the idempotency key matched an earlier,
	// identical request; nothing new was written.
	Replayed bool
}

// Service ingests events.
type Service struct {
	store *store.Store
}

// NewService returns a Service.
func NewService(st *store.Store) *Service {
	return &Service{store: st}
}

// Validate checks a request without touching the database.
func Validate(r Request) error {
	var vb validation.Builder
	if !endpoints.EventTypePattern.MatchString(r.EventType) {
		vb.Add("event_type", "is required and must be 1-100 of a-z A-Z 0-9 . _ -")
	}
	switch {
	case len(r.Payload) > MaxPayloadBytes:
		vb.Add("payload", fmt.Sprintf("must be at most %d bytes", MaxPayloadBytes))
	case len(bytes.TrimSpace(r.Payload)) == 0:
		vb.Add("payload", "is required")
	case bytes.TrimSpace(r.Payload)[0] != '{' || !json.Valid(r.Payload):
		vb.Add("payload", "must be a JSON object")
	}
	if r.IdempotencyKey != nil {
		k := *r.IdempotencyKey
		if len(k) == 0 || len(k) > MaxIdempotencyKeyBytes {
			vb.Add("idempotency_key", fmt.Sprintf("must be 1-%d characters", MaxIdempotencyKeyBytes))
		} else {
			for i := 0; i < len(k); i++ {
				if k[i] < 0x21 || k[i] > 0x7e {
					vb.Add("idempotency_key", "must contain only printable ASCII without spaces")
					break
				}
			}
		}
	}
	return vb.Err()
}

// Ingest stores the event and fans out deliveries in one transaction. When it
// returns nil, the transaction has committed, so the caller may report
// success. It performs no network I/O other than the database.
func (s *Service) Ingest(ctx context.Context, projectID pgtype.UUID, r Request) (Result, error) {
	if err := Validate(r); err != nil {
		return Result{}, err
	}
	sum := sha256.Sum256(r.Payload)

	var res Result
	err := s.store.InTx(ctx, func(q *sqlcgen.Queries) error {
		res = Result{}
		ev, err := q.InsertEvent(ctx, sqlcgen.InsertEventParams{
			ProjectID:      projectID,
			EventType:      r.EventType,
			Payload:        r.Payload,
			PayloadSha256:  sum[:],
			IdempotencyKey: r.IdempotencyKey,
		})
		switch {
		case store.IsNotFound(err):
			// ON CONFLICT DO NOTHING: the key was used before. Under READ
			// COMMITTED a concurrent duplicate waits for the first insert to
			// commit, so the existing row is visible here.
			return s.replay(ctx, q, projectID, r, sum[:], &res)
		case err != nil:
			return fmt.Errorf("insert event: %w", err)
		}

		n, err := q.FanOutDeliveries(ctx, sqlcgen.FanOutDeliveriesParams{
			EventID: ev.ID, ProjectID: projectID, EventType: r.EventType,
		})
		if err != nil {
			return fmt.Errorf("fan out deliveries: %w", err)
		}
		res = Result{
			EventID: ev.ID, EventType: ev.EventType, IdempotencyKey: ev.IdempotencyKey,
			CreatedAt: ev.CreatedAt, DeliveriesCreated: n,
		}
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	return res, nil
}

func (s *Service) replay(ctx context.Context, q *sqlcgen.Queries, projectID pgtype.UUID, r Request, sum []byte, res *Result) error {
	ev, err := q.GetEventByIdempotencyKey(ctx, sqlcgen.GetEventByIdempotencyKeyParams{
		ProjectID: projectID, IdempotencyKey: r.IdempotencyKey,
	})
	if err != nil {
		return fmt.Errorf("load idempotent event: %w", err)
	}
	if ev.EventType != r.EventType || !bytes.Equal(ev.PayloadSha256, sum) {
		return ErrIdempotencyConflict
	}
	n, err := q.CountDeliveriesForEvent(ctx, sqlcgen.CountDeliveriesForEventParams{ProjectID: projectID, EventID: ev.ID})
	if err != nil {
		return fmt.Errorf("count deliveries: %w", err)
	}
	*res = Result{
		EventID: ev.ID, EventType: ev.EventType, IdempotencyKey: ev.IdempotencyKey,
		CreatedAt: ev.CreatedAt, DeliveriesCreated: n, Replayed: true,
	}
	return nil
}
