package backoff

import (
	"net/http"
	"testing"
	"time"
)

func TestCeilingGrowsExponentiallyAndCaps(t *testing.T) {
	p := New(30*time.Second, 6*time.Hour)
	want := []time.Duration{
		30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute,
		16 * time.Minute, 32 * time.Minute, 64 * time.Minute, 128 * time.Minute, 256 * time.Minute,
		6 * time.Hour, 6 * time.Hour,
	}
	for i, w := range want {
		if got := p.Ceiling(i + 1); got != w {
			t.Errorf("Ceiling(%d) = %s, want %s", i+1, got, w)
		}
	}
	// Huge attempt numbers must not overflow.
	if got := p.Ceiling(10_000); got != 6*time.Hour {
		t.Errorf("Ceiling(10000) = %s", got)
	}
	if got := p.Ceiling(0); got != 30*time.Second {
		t.Errorf("Ceiling(0) = %s", got)
	}
}

func TestDelayStaysWithinBounds(t *testing.T) {
	p := NewSeeded(30*time.Second, 6*time.Hour, 42)
	for attempt := 1; attempt <= 15; attempt++ {
		lo, hi := p.Base()/2, p.Ceiling(attempt)
		for i := 0; i < 500; i++ {
			d := p.Delay(attempt, 0)
			if d < lo || d > hi {
				t.Fatalf("Delay(%d) = %s, want within [%s, %s]", attempt, d, lo, hi)
			}
		}
	}
}

func TestDelayIsJittered(t *testing.T) {
	p := NewSeeded(time.Minute, time.Hour, 7)
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		seen[p.Delay(5, 0)] = true
	}
	if len(seen) < 40 {
		t.Fatalf("only %d distinct delays out of 50; jitter is not spreading retries", len(seen))
	}
}

func TestSeededPolicyIsDeterministic(t *testing.T) {
	a, b := NewSeeded(time.Minute, time.Hour, 99), NewSeeded(time.Minute, time.Hour, 99)
	for attempt := 1; attempt <= 10; attempt++ {
		if da, db := a.Delay(attempt, 0), b.Delay(attempt, 0); da != db {
			t.Fatalf("attempt %d: %s != %s", attempt, da, db)
		}
	}
}

func TestRetryAfterRaisesDelayButNotPastCap(t *testing.T) {
	p := NewSeeded(time.Second, time.Hour, 1)
	if d := p.Delay(1, 10*time.Minute); d != 10*time.Minute {
		t.Errorf("Delay with Retry-After 10m = %s, want 10m", d)
	}
	if d := p.Delay(1, 48*time.Hour); d != time.Hour {
		t.Errorf("Delay with Retry-After 48h = %s, want cap 1h", d)
	}
	// A smaller hint never shortens the computed delay.
	if d := p.Delay(20, time.Second); d < p.Base()/2 {
		t.Errorf("Delay = %s", d)
	}
}

func TestDefaultScheduleMatchesProductSpecWindow(t *testing.T) {
	p := Default()
	const maxAttempts = 10 // deliveries.max_attempts default
	var worst time.Duration
	for attempt := 1; attempt < maxAttempts; attempt++ {
		worst += p.Ceiling(attempt)
	}
	// PRODUCT_SPEC §4.3: "up to 10 attempts over roughly 24 hours".
	if worst < 20*time.Hour || worst > 26*time.Hour {
		t.Fatalf("worst-case retry span = %s, want roughly 24h", worst)
	}
}

func TestRetryAfterParsing(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	hdr := func(v string) http.Header { return http.Header{"Retry-After": []string{v}} }
	tests := []struct {
		name   string
		status int
		h      http.Header
		want   time.Duration
	}{
		{"seconds on 429", 429, hdr("120"), 2 * time.Minute},
		{"seconds on 503", 503, hdr(" 7 "), 7 * time.Second},
		{"http date", 503, hdr(now.Add(90 * time.Second).Format(http.TimeFormat)), 90 * time.Second},
		{"past date", 429, hdr(now.Add(-time.Hour).Format(http.TimeFormat)), 0},
		{"ignored on 500", 500, hdr("120"), 0},
		{"ignored on 200", 200, hdr("120"), 0},
		{"garbage", 429, hdr("soon"), 0},
		{"negative", 429, hdr("-5"), 0},
		{"huge is bounded", 429, hdr("99999999999999"), 24 * time.Hour},
		{"missing", 429, http.Header{}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RetryAfter(tt.status, tt.h, now); got != tt.want {
				t.Fatalf("RetryAfter = %s, want %s", got, tt.want)
			}
		})
	}
}
