// Package backoff computes the delay before a failed delivery is retried
// (docs/ARCHITECTURE.md §3.2).
//
// The delay is exponential with full jitter:
//
//	delay = max(Base/2, random(0, min(Cap, Base * 2^(attempt-1))))
//
// For 429 and 503 responses with a Retry-After header, the delay is
// max(delay, Retry-After), capped at Cap. Only the delay is computed here: the
// worker adds it to the database clock (now() in SQL, invariant 7) and stores
// the result in deliveries.next_attempt_at, so Postgres owns the schedule
// (ARCHITECTURE D2).
package backoff

import (
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Defaults. With the schema's default max_attempts of 10 there are at most 9
// retries, so the delays' ceilings are 5m, 10m, 20m, 40m, 80m, 160m, 320m,
// 6h, 6h: a worst-case span of about 22.6 hours and an expected span (full
// jitter) of about 11.5 hours, matching PRODUCT_SPEC §4.3 ("up to 10 attempts
// over roughly 24 hours").
const (
	DefaultBase = 5 * time.Minute
	DefaultCap  = 6 * time.Hour
)

// Policy is a retry schedule. The zero value is not usable; use New or
// NewSeeded. A Policy is safe for concurrent use.
type Policy struct {
	base, limit time.Duration

	mu  *sync.Mutex
	rng *rand.Rand // nil = the global, concurrency-safe source
}

// New returns a policy with the given base delay and cap, using a random
// jitter source. Non-positive values fall back to the defaults.
func New(base, limit time.Duration) Policy {
	if base <= 0 {
		base = DefaultBase
	}
	if limit <= 0 {
		limit = DefaultCap
	}
	if limit < base {
		limit = base
	}
	return Policy{base: base, limit: limit}
}

// Default returns New(DefaultBase, DefaultCap).
func Default() Policy { return New(DefaultBase, DefaultCap) }

// NewSeeded is New with a deterministic jitter source, for tests.
func NewSeeded(base, limit time.Duration, seed uint64) Policy {
	p := New(base, limit)
	p.mu = &sync.Mutex{}
	p.rng = rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)) //nolint:gosec // jitter, not security
	return p
}

// Base returns the base delay.
func (p Policy) Base() time.Duration { return p.base }

// Cap returns the maximum delay.
func (p Policy) Cap() time.Duration { return p.limit }

// Ceiling is the upper bound of the jittered delay after the given attempt
// number (1-based): min(Cap, Base * 2^(attempt-1)).
func (p Policy) Ceiling(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.base
	for i := 1; i < attempt; i++ {
		if d >= p.limit/2 {
			return p.limit // doubling would reach the cap (and avoids overflow)
		}
		d *= 2
	}
	return min(d, p.limit)
}

// Delay returns how long to wait before the retry that follows the given
// failed attempt (1-based). retryAfter is the server's Retry-After hint, or 0.
func (p Policy) Delay(attempt int, retryAfter time.Duration) time.Duration {
	ceiling := p.Ceiling(attempt)
	d := time.Duration(p.int64N(int64(ceiling) + 1))
	if floor := p.base / 2; d < floor {
		d = floor
	}
	if retryAfter > d {
		d = retryAfter
	}
	return min(d, p.limit)
}

func (p Policy) int64N(n int64) int64 {
	if p.rng == nil {
		return rand.Int64N(n) //nolint:gosec // jitter, not security
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rng.Int64N(n)
}

// RetryAfter extracts the Retry-After hint from a response that has the given
// status. It is honoured only for 429 and 503, in either the delay-seconds or
// the HTTP-date form. It returns 0 when there is no usable hint.
func RetryAfter(status int, h http.Header, now time.Time) time.Duration {
	if status != http.StatusTooManyRequests && status != http.StatusServiceUnavailable {
		return 0
	}
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs <= 0 {
			return 0
		}
		// Anything beyond a day is capped by Delay anyway; avoid overflow.
		return time.Duration(min(secs, 86400)) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}
