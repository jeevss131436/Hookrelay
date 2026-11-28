package history

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
)

// Stats windows.
const (
	DefaultStatsWindow = 24 * time.Hour
	MaxStatsWindow     = 90 * 24 * time.Hour
)

// Stats are project-level statistics over a half-open window [From, To).
// Exact definitions are in docs/API.md ("Statistics"). Rates and latencies
// are nil when their denominator or sample is empty.
type Stats struct {
	From, To time.Time

	Events int64 // events created in the window

	// Deliveries created in the window, by CURRENT status.
	Deliveries sqlcgen.GetDeliveryStatsRow
	// succeeded / (succeeded + dead): outcome of deliveries that finished.
	SuccessRate *float64
	FailureRate *float64

	// Attempts finished in the window.
	Attempts           int64
	AttemptsSucceeded  int64
	AttemptsFailed     int64
	AttemptSuccessRate *float64

	SuccessLatency Latency // successful attempts
	FailureLatency Latency // failed attempts except worker_lost
}

// Latency summarizes attempt durations in milliseconds.
type Latency struct {
	Count         int64
	Avg, P50, P95 *float64
}

func latency(count int64, avg, p50, p95 float64) Latency {
	if count == 0 {
		return Latency{}
	}
	return Latency{Count: count, Avg: &avg, P50: &p50, P95: &p95}
}

func ratio(num, den int64) *float64 {
	if den == 0 {
		return nil
	}
	r := float64(num) / float64(den)
	return &r
}

// Stats computes statistics for [from, to). The three aggregates run in one
// read-only REPEATABLE READ transaction, so they describe one snapshot. All
// aggregation happens in PostgreSQL.
func (s *Service) Stats(ctx context.Context, projectID pgtype.UUID, from, to time.Time) (Stats, error) {
	out := Stats{From: from, To: to}
	var (
		dl sqlcgen.GetDeliveryStatsRow
		at sqlcgen.GetAttemptStatsRow
	)
	err := s.store.InSnapshot(ctx, func(q *sqlcgen.Queries) error {
		var err error
		if out.Events, err = q.GetEventStats(ctx, sqlcgen.GetEventStatsParams{
			ProjectID: projectID, WindowStart: from, WindowEnd: to,
		}); err != nil {
			return fmt.Errorf("event stats: %w", err)
		}
		if dl, err = q.GetDeliveryStats(ctx, sqlcgen.GetDeliveryStatsParams{
			ProjectID: projectID, WindowStart: from, WindowEnd: to,
		}); err != nil {
			return fmt.Errorf("delivery stats: %w", err)
		}
		if at, err = q.GetAttemptStats(ctx, sqlcgen.GetAttemptStatsParams{
			ProjectID: projectID, WindowStart: from, WindowEnd: to,
		}); err != nil {
			return fmt.Errorf("attempt stats: %w", err)
		}
		return nil
	})
	if err != nil {
		return Stats{}, err
	}

	out.Deliveries = dl
	finished := dl.Succeeded + dl.Dead
	out.SuccessRate = ratio(dl.Succeeded, finished)
	out.FailureRate = ratio(dl.Dead, finished)

	out.Attempts, out.AttemptsSucceeded, out.AttemptsFailed = at.Total, at.Succeeded, at.Failed
	out.AttemptSuccessRate = ratio(at.Succeeded, at.Total)
	out.SuccessLatency = latency(at.Succeeded, at.SuccessAvgMs, at.SuccessP50Ms, at.SuccessP95Ms)
	out.FailureLatency = latency(at.FailedMeasured, at.FailedAvgMs, at.FailedP50Ms, at.FailedP95Ms)
	return out, nil
}
