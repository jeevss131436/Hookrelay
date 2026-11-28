// Package history serves the Phase 5 read side of the API: events,
// deliveries and attempts (read-only listings), manual retry of dead
// deliveries, and project statistics.
//
// Every method takes the authenticated project's ID and every query is scoped
// by it, so another project's rows behave exactly like missing ones. Nothing
// here touches Redis: a manual retry only makes the delivery due in
// PostgreSQL, and the dispatcher enqueues it (ARCHITECTURE D1).
package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
)

// Page sizes for every listing.
const (
	DefaultPageSize = 50
	MaxPageSize     = 100
	// MaxEventDeliveries bounds the delivery summary embedded in an event.
	MaxEventDeliveries = 100
)

// Not-found errors: the event or delivery does not exist in the project
// (including when it belongs to another project).
var (
	ErrEventNotFound    = errors.New("event not found")
	ErrDeliveryNotFound = errors.New("delivery not found")
)

// Service implements the read APIs, manual retry and statistics.
type Service struct {
	store *store.Store
}

// NewService returns a Service backed by st.
func NewService(st *store.Store) *Service { return &Service{store: st} }

// Cursor marks a position in the (created_at DESC, id DESC) ordering.
type Cursor struct {
	CreatedAt time.Time
	ID        pgtype.UUID
}

// TimeRange is an optional half-open [After, Before) filter on created_at.
type TimeRange struct {
	After  *time.Time
	Before *time.Time
}

func clampLimit(limit int) int32 {
	if limit <= 0 {
		limit = DefaultPageSize
	}
	return int32(min(limit, MaxPageSize)) //nolint:gosec // bounded by MaxPageSize
}

func cursorArgs(c *Cursor) (*time.Time, pgtype.UUID) {
	if c == nil {
		return nil, pgtype.UUID{}
	}
	t := c.CreatedAt
	return &t, c.ID
}

// ---- events ----

// EventSummary is an event as listed (without its payload).
type EventSummary struct {
	ID             pgtype.UUID
	EventType      string
	IdempotencyKey *string
	PayloadSize    int32
	CreatedAt      time.Time
}

// EventFilter narrows ListEvents. Zero values mean "no filter".
type EventFilter struct {
	EventType *string
	Created   TimeRange
}

// EventPage is one page of events; NextCursor is nil on the last page.
type EventPage struct {
	Events     []EventSummary
	NextCursor *Cursor
}

// ListEvents lists the project's events, newest first.
func (s *Service) ListEvents(ctx context.Context, projectID pgtype.UUID, f EventFilter, after *Cursor, limit int) (EventPage, error) {
	n := clampLimit(limit)
	cAt, cID := cursorArgs(after)
	rows, err := s.store.ListEvents(ctx, sqlcgen.ListEventsParams{
		ProjectID: projectID, EventType: f.EventType,
		CreatedAfter: f.Created.After, CreatedBefore: f.Created.Before,
		CursorCreatedAt: cAt, CursorID: cID, RowLimit: n + 1,
	})
	if err != nil {
		return EventPage{}, fmt.Errorf("list events: %w", err)
	}
	page := EventPage{Events: make([]EventSummary, 0, len(rows))}
	for i, r := range rows {
		if i == int(n) {
			last := page.Events[n-1]
			page.NextCursor = &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
			break
		}
		page.Events = append(page.Events, EventSummary(r))
	}
	return page, nil
}

// EventDelivery summarizes one delivery of an event.
type EventDelivery = sqlcgen.ListEventDeliverySummariesRow

// Event is one event with its payload and per-endpoint delivery summary.
type Event struct {
	ID             pgtype.UUID
	EventType      string
	Payload        []byte // exact stored bytes (valid JSON object)
	IdempotencyKey *string
	CreatedAt      time.Time
	Deliveries     []EventDelivery
	// DeliveriesTruncated is true when the event has more than
	// MaxEventDeliveries deliveries; list them with ListDeliveries.
	DeliveriesTruncated bool
}

// GetEvent returns one event, or ErrEventNotFound.
func (s *Service) GetEvent(ctx context.Context, projectID, id pgtype.UUID) (Event, error) {
	ev, err := s.store.GetEvent(ctx, sqlcgen.GetEventParams{ProjectID: projectID, ID: id})
	if store.IsNotFound(err) {
		return Event{}, ErrEventNotFound
	}
	if err != nil {
		return Event{}, fmt.Errorf("get event: %w", err)
	}
	ds, err := s.store.ListEventDeliverySummaries(ctx, sqlcgen.ListEventDeliverySummariesParams{
		ProjectID: projectID, EventID: id, RowLimit: MaxEventDeliveries + 1,
	})
	if err != nil {
		return Event{}, fmt.Errorf("list event deliveries: %w", err)
	}
	out := Event{
		ID: ev.ID, EventType: ev.EventType, Payload: ev.Payload,
		IdempotencyKey: ev.IdempotencyKey, CreatedAt: ev.CreatedAt, Deliveries: ds,
	}
	if len(ds) > MaxEventDeliveries {
		out.Deliveries, out.DeliveriesTruncated = ds[:MaxEventDeliveries], true
	}
	return out, nil
}

// ---- deliveries ----

// Delivery is a delivery as listed.
type Delivery = sqlcgen.ListDeliveriesRow

// DeliveryFilter narrows ListDeliveries. Zero values mean "no filter".
type DeliveryFilter struct {
	Statuses   []sqlcgen.DeliveryStatus // any of
	EndpointID pgtype.UUID
	EventID    pgtype.UUID
	EventType  *string
	Created    TimeRange
}

// DeliveryPage is one page of deliveries; NextCursor is nil on the last page.
type DeliveryPage struct {
	Deliveries []Delivery
	NextCursor *Cursor
}

// ListDeliveries lists the project's deliveries, newest first.
func (s *Service) ListDeliveries(ctx context.Context, projectID pgtype.UUID, f DeliveryFilter, after *Cursor, limit int) (DeliveryPage, error) {
	n := clampLimit(limit)
	cAt, cID := cursorArgs(after)
	var statuses []string
	if len(f.Statuses) > 0 {
		statuses = make([]string, len(f.Statuses))
		for i, st := range f.Statuses {
			statuses[i] = string(st)
		}
	}
	rows, err := s.store.ListDeliveries(ctx, sqlcgen.ListDeliveriesParams{
		ProjectID: projectID, Statuses: statuses, EndpointID: f.EndpointID, EventID: f.EventID,
		EventType: f.EventType, CreatedAfter: f.Created.After, CreatedBefore: f.Created.Before,
		CursorCreatedAt: cAt, CursorID: cID, RowLimit: n + 1,
	})
	if err != nil {
		return DeliveryPage{}, fmt.Errorf("list deliveries: %w", err)
	}
	page := DeliveryPage{Deliveries: rows}
	if len(rows) > int(n) {
		last := rows[n-1]
		page.Deliveries = rows[:n]
		page.NextCursor = &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	return page, nil
}

// DeliveryDetail is one delivery with its event type and endpoint.
type DeliveryDetail = sqlcgen.GetDeliveryDetailRow

// GetDelivery returns one delivery, or ErrDeliveryNotFound.
func (s *Service) GetDelivery(ctx context.Context, projectID, id pgtype.UUID) (DeliveryDetail, error) {
	return getDelivery(ctx, s.store.Queries, projectID, id)
}

func getDelivery(ctx context.Context, q *sqlcgen.Queries, projectID, id pgtype.UUID) (DeliveryDetail, error) {
	d, err := q.GetDeliveryDetail(ctx, sqlcgen.GetDeliveryDetailParams{ProjectID: projectID, ID: id})
	if store.IsNotFound(err) {
		return DeliveryDetail{}, ErrDeliveryNotFound
	}
	if err != nil {
		return DeliveryDetail{}, fmt.Errorf("get delivery: %w", err)
	}
	return d, nil
}

// Attempt is one recorded delivery attempt.
type Attempt = sqlcgen.ListDeliveryAttemptsPageRow

// AttemptPage is one page of attempts, oldest first. NextAfter is the
// attempt number to continue after, or nil on the last page.
type AttemptPage struct {
	Attempts  []Attempt
	NextAfter *int32
}

// ListAttempts lists a delivery's attempts in attempt order. It returns
// ErrDeliveryNotFound if the delivery does not exist in the project.
func (s *Service) ListAttempts(ctx context.Context, projectID, deliveryID pgtype.UUID, after int32, limit int) (AttemptPage, error) {
	if _, err := s.GetDelivery(ctx, projectID, deliveryID); err != nil {
		return AttemptPage{}, err
	}
	n := clampLimit(limit)
	rows, err := s.store.ListDeliveryAttemptsPage(ctx, sqlcgen.ListDeliveryAttemptsPageParams{
		ProjectID: projectID, DeliveryID: deliveryID, AfterAttempt: after, RowLimit: n + 1,
	})
	if err != nil {
		return AttemptPage{}, fmt.Errorf("list attempts: %w", err)
	}
	page := AttemptPage{Attempts: rows}
	if len(rows) > int(n) {
		page.Attempts = rows[:n]
		next := rows[n-1].AttemptNumber
		page.NextAfter = &next
	}
	return page, nil
}
