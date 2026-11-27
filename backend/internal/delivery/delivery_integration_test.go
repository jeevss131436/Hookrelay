package delivery

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"

	"github.com/jeevss131436/Hookrelay/backend/internal/backoff"
	"github.com/jeevss131436/Hookrelay/backend/internal/endpoints"
	"github.com/jeevss131436/Hookrelay/backend/internal/ingest"
	"github.com/jeevss131436/Hookrelay/backend/internal/mockreceiver"
	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/signing"
	"github.com/jeevss131436/Hookrelay/backend/internal/ssrf"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
	"github.com/jeevss131436/Hookrelay/backend/internal/testutil/pgtest"
)

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// testPolicy: short, deterministic retry schedule (attempt 1 waits 5-10s).
var testPolicy = backoff.NewSeeded(10*time.Second, time.Hour, 1)

// The payload deliberately is not canonical JSON: it must arrive byte for byte.
var testPayload = []byte("{\"order\":  42,\n \"items\":[ \"a\" ]}")

type env struct {
	t         *testing.T
	ctx       context.Context
	db        pgtest.DB
	st        *store.Store
	cipher    *secrets.Cipher
	projectID pgtype.UUID
}

func newEnv(t *testing.T) *env {
	t.Helper()
	db := pgtest.NewMigrated(t)
	cipher, err := secrets.NewCipher(secrets.GenerateKey())
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, ctx: context.Background(), db: db, st: store.New(db.Pool), cipher: cipher}
	e.projectID = e.newProject()
	return e
}

func (e *env) newProject() pgtype.UUID {
	e.t.Helper()
	p, err := e.st.CreateProject(e.ctx, "delivery test")
	if err != nil {
		e.t.Fatalf("create project: %v", err)
	}
	return p.ID
}

func (e *env) endpointService(policy ssrf.Policy) *endpoints.Service {
	return endpoints.NewService(e.st, e.cipher, policy)
}

// pendingDelivery registers an endpoint for url (dev policy: private targets
// allowed), ingests one event and returns the resulting delivery.
func (e *env) pendingDelivery(url string) (deliveryID, endpointID pgtype.UUID, secret string) {
	e.t.Helper()
	return e.pendingDeliveryWithPolicy(url, ssrf.Policy{AllowPrivate: true})
}

func (e *env) pendingDeliveryWithPolicy(url string, policy ssrf.Policy) (deliveryID, endpointID pgtype.UUID, secret string) {
	e.t.Helper()
	ep, secret, err := e.endpointService(policy).Create(e.ctx, e.projectID, endpoints.CreateParams{URL: url})
	if err != nil {
		e.t.Fatalf("create endpoint: %v", err)
	}
	res, err := ingest.NewService(e.st).Ingest(e.ctx, e.projectID, ingest.Request{EventType: "order.paid", Payload: testPayload})
	if err != nil || res.DeliveriesCreated < 1 {
		e.t.Fatalf("ingest: %+v, %v", res, err)
	}
	if err := e.db.Pool.QueryRow(e.ctx, `SELECT id FROM deliveries WHERE event_id = $1 AND endpoint_id = $2`,
		res.EventID, ep.ID).Scan(&deliveryID); err != nil {
		e.t.Fatalf("find delivery: %v", err)
	}
	return deliveryID, ep.ID, secret
}

// queue does what the Phase 4 dispatcher will do: pending/scheduled -> queued.
func (e *env) queue(id pgtype.UUID) Payload {
	e.t.Helper()
	row, err := e.st.MarkDeliveryQueued(e.ctx, sqlcgen.MarkDeliveryQueuedParams{ProjectID: e.projectID, ID: id})
	if err != nil {
		e.t.Fatalf("mark queued: %v", err)
	}
	return Payload{ProjectID: e.projectID, DeliveryID: id, AttemptCount: row.AttemptCount, QueuedAt: row.QueuedAt}
}

func (e *env) handler(o SenderOptions) *Handler {
	if o.Timeout == 0 {
		o.Timeout = 5 * time.Second
	}
	return NewHandler(e.st, e.cipher, NewSender(o), HandlerOptions{Lease: time.Minute, Backoff: testPolicy}, discardLogger())
}

func (e *env) devHandler() *Handler { return e.handler(SenderOptions{AllowPrivate: true}) }

func (e *env) delivery(id pgtype.UUID) sqlcgen.Delivery {
	e.t.Helper()
	d, err := e.st.GetDelivery(e.ctx, sqlcgen.GetDeliveryParams{ProjectID: e.projectID, ID: id})
	if err != nil {
		e.t.Fatalf("get delivery: %v", err)
	}
	return d
}

func (e *env) attempts(id pgtype.UUID) []sqlcgen.DeliveryAttempt {
	e.t.Helper()
	a, err := e.st.ListDeliveryAttempts(e.ctx, sqlcgen.ListDeliveryAttemptsParams{ProjectID: e.projectID, DeliveryID: id})
	if err != nil {
		e.t.Fatalf("list attempts: %v", err)
	}
	return a
}

func (e *env) dbNow() time.Time {
	e.t.Helper()
	var now time.Time
	if err := e.db.Pool.QueryRow(e.ctx, `SELECT now()`).Scan(&now); err != nil {
		e.t.Fatal(err)
	}
	return now
}

func (e *env) deliver(h *Handler, p Payload) Outcome {
	e.t.Helper()
	out, err := h.Deliver(e.ctx, p)
	if err != nil {
		e.t.Fatalf("Deliver: %v", err)
	}
	return out
}

func receiver(t *testing.T, secret string) (*mockreceiver.Receiver, *httptest.Server) {
	t.Helper()
	rc := mockreceiver.New(secret, nil)
	srv := httptest.NewServer(rc)
	t.Cleanup(srv.Close)
	return rc, srv
}

func TestDeliverSuccess(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	rc, srv := receiver(t, "")
	id, _, secret := e.pendingDelivery(srv.URL + "/hook")
	p := e.queue(id)

	if out := e.deliver(e.devHandler(), p); out != OutcomeSucceeded {
		t.Fatalf("outcome = %s", out)
	}

	d := e.delivery(id)
	if d.Status != sqlcgen.DeliveryStatusSucceeded || d.AttemptCount != 1 || d.CompletedAt == nil ||
		d.LockedUntil != nil || d.LastStatusCode == nil || *d.LastStatusCode != 200 || d.LastError != nil {
		t.Fatalf("delivery = %+v", d)
	}
	att := e.attempts(id)
	if len(att) != 1 {
		t.Fatalf("attempts = %+v", att)
	}
	a := att[0]
	if a.AttemptNumber != 1 || a.ResponseStatus == nil || *a.ResponseStatus != 200 || a.ErrorKind != nil ||
		a.FinishedAt.Before(a.StartedAt) || a.DurationMs < 0 || a.ResponseBody == nil || *a.ResponseBody != "OK" {
		t.Fatalf("attempt = %+v", a)
	}

	reqs := rc.Requests()
	if len(reqs) != 1 {
		t.Fatalf("receiver got %d requests", len(reqs))
	}
	r := reqs[0]
	if string(r.Body) != string(testPayload) {
		t.Errorf("body = %q, want exact stored bytes %q", r.Body, testPayload)
	}
	if r.WebhookID != id.String() {
		t.Errorf("webhook-id = %q, want delivery id %s", r.WebhookID, id.String())
	}
	// The worker decrypted the endpoint's secret and signed with it.
	if err := signing.Verify(secret, r.Header, r.Body, time.Now(), 0); err != nil {
		t.Errorf("signature does not verify with the endpoint secret: %v", err)
	}
}

func TestDeliverHTTP500SchedulesRetry(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	_, srv := receiver(t, "")
	id, _, _ := e.pendingDelivery(srv.URL + "/hook?status=500")
	p := e.queue(id)

	before := e.dbNow()
	if out := e.deliver(e.devHandler(), p); out != OutcomeScheduled {
		t.Fatalf("outcome = %s", out)
	}
	after := e.dbNow()

	d := e.delivery(id)
	if d.Status != sqlcgen.DeliveryStatusScheduled || d.AttemptCount != 1 || d.CompletedAt != nil || d.LockedUntil != nil ||
		d.LastStatusCode == nil || *d.LastStatusCode != 500 || d.LastError == nil || *d.LastError != "HTTP 500" {
		t.Fatalf("delivery = %+v", d)
	}
	// Retry time is computed from the DB clock: now() + delay in [base/2, base].
	lo, hi := before.Add(testPolicy.Base()/2), after.Add(testPolicy.Ceiling(1))
	if d.NextAttemptAt.Before(lo) || d.NextAttemptAt.After(hi) {
		t.Fatalf("next_attempt_at = %s, want within [%s, %s]", d.NextAttemptAt, lo, hi)
	}
	a := e.attempts(id)
	if len(a) != 1 || a[0].ErrorKind == nil || *a[0].ErrorKind != ErrKindHTTPStatus ||
		a[0].ResponseStatus == nil || *a[0].ResponseStatus != 500 {
		t.Fatalf("attempts = %+v", a)
	}
}

func TestDeliverTimeoutSchedulesRetry(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	_, srv := receiver(t, "")
	id, _, _ := e.pendingDelivery(srv.URL + "/hook?latency_ms=3000")
	p := e.queue(id)

	if out := e.deliver(e.handler(SenderOptions{AllowPrivate: true, Timeout: 300 * time.Millisecond}), p); out != OutcomeScheduled {
		t.Fatalf("outcome = %s", out)
	}
	a := e.attempts(id)
	if len(a) != 1 || a[0].ErrorKind == nil || *a[0].ErrorKind != ErrKindTimeout || a[0].ResponseStatus != nil ||
		a[0].ResponseBody != nil {
		t.Fatalf("attempts = %+v", a)
	}
	if d := e.delivery(id); d.Status != sqlcgen.DeliveryStatusScheduled || d.LastStatusCode != nil {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestRetryAfterIsHonoured(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "600")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	id, _, _ := e.pendingDelivery(srv.URL)
	p := e.queue(id)
	before := e.dbNow()
	if out := e.deliver(e.devHandler(), p); out != OutcomeScheduled {
		t.Fatalf("outcome = %s", out)
	}
	if d := e.delivery(id); d.NextAttemptAt.Before(before.Add(600 * time.Second)) {
		t.Fatalf("next_attempt_at = %s, want >= now + 600s (Retry-After)", d.NextAttemptAt)
	}
}

func TestLastAttemptFailureIsDead(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	_, srv := receiver(t, "")
	id, _, _ := e.pendingDelivery(srv.URL + "/hook?status=502")
	if _, err := e.db.Pool.Exec(e.ctx, `UPDATE deliveries SET max_attempts = 2 WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	h := e.devHandler()

	if out := e.deliver(h, e.queue(id)); out != OutcomeScheduled {
		t.Fatalf("attempt 1 outcome = %s", out)
	}
	scheduledAt := e.delivery(id).NextAttemptAt
	if out := e.deliver(h, e.queue(id)); out != OutcomeDead {
		t.Fatalf("attempt 2 outcome = %s", out)
	}
	d := e.delivery(id)
	if d.Status != sqlcgen.DeliveryStatusDead || d.AttemptCount != 2 || d.CompletedAt == nil ||
		!d.NextAttemptAt.Equal(scheduledAt) || *d.LastStatusCode != 502 {
		t.Fatalf("delivery = %+v", d)
	}
	if a := e.attempts(id); len(a) != 2 || a[0].AttemptNumber != 1 || a[1].AttemptNumber != 2 {
		t.Fatalf("attempts = %+v", a)
	}
}

func TestRetryThenSucceed(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var calls atomic.Int32
	var secret string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := signing.Verify(secret, r.Header, body, time.Now(), 0); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if calls.Add(1) <= 3 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(204)
	}))
	t.Cleanup(srv.Close)
	id, _, s := e.pendingDelivery(srv.URL)
	secret = s
	h := e.devHandler()

	want := []Outcome{OutcomeScheduled, OutcomeScheduled, OutcomeScheduled, OutcomeSucceeded}
	for i, w := range want {
		if out := e.deliver(h, e.queue(id)); out != w {
			t.Fatalf("attempt %d outcome = %s, want %s", i+1, out, w)
		}
	}
	d := e.delivery(id)
	if d.Status != sqlcgen.DeliveryStatusSucceeded || d.AttemptCount != 4 || *d.LastStatusCode != 204 || d.LastError != nil {
		t.Fatalf("delivery = %+v", d)
	}
	a := e.attempts(id)
	if len(a) != 4 {
		t.Fatalf("attempts = %d, want 4", len(a))
	}
	for i, at := range a {
		if int(at.AttemptNumber) != i+1 {
			t.Errorf("attempt %d has number %d", i, at.AttemptNumber)
		}
	}
}

// DNS rebinding end to end: the endpoint passed registration with a public
// address, but resolves to loopback at delivery time. Blocking is permanent.
func TestDNSRebindingIsBlockedAndPermanent(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	rc, srv := receiver(t, "")
	url := "http://hooks.example.com:" + port(t, srv) + "/hook"
	r := &fakeResolver{answers: [][]netip.Addr{addrs("93.184.215.14"), addrs("127.0.0.1")}}
	id, _, _ := e.pendingDeliveryWithPolicy(url, ssrf.Policy{Resolver: r})
	p := e.queue(id)

	if out := e.deliver(e.handler(SenderOptions{Resolver: r}), p); out != OutcomeDead {
		t.Fatalf("outcome = %s", out)
	}
	d := e.delivery(id)
	if d.Status != sqlcgen.DeliveryStatusDead || d.AttemptCount != 1 || d.MaxAttempts != 10 || d.LastStatusCode != nil {
		t.Fatalf("delivery = %+v (blocked destination must not be retried)", d)
	}
	a := e.attempts(id)
	if len(a) != 1 || a[0].ErrorKind == nil || *a[0].ErrorKind != ErrKindSSRFBlocked {
		t.Fatalf("attempts = %+v", a)
	}
	if n := len(rc.Requests()); n != 0 {
		t.Fatalf("internal receiver got %d requests", n)
	}
}

func TestDuplicateTasksSendOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	rc, srv := receiver(t, "")
	id, _, _ := e.pendingDelivery(srv.URL + "/hook?latency_ms=200")
	p := e.queue(id)
	h := e.devHandler()

	const workers = 10
	outcomes := make([]Outcome, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			out, err := h.Deliver(e.ctx, p)
			if err != nil {
				t.Errorf("Deliver: %v", err)
			}
			outcomes[i] = out
		}()
	}
	close(start)
	wg.Wait()

	counts := map[Outcome]int{}
	for _, o := range outcomes {
		counts[o]++
	}
	if counts[OutcomeSucceeded] != 1 || counts[OutcomeSkipped] != workers-1 {
		t.Fatalf("outcomes = %v", counts)
	}
	if rc.Count(id.String()) != 1 || rc.Duplicates() != 0 {
		t.Fatalf("receiver saw delivery %d times", rc.Count(id.String()))
	}
	if d := e.delivery(id); d.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d", d.AttemptCount)
	}
	if a := e.attempts(id); len(a) != 1 {
		t.Fatalf("attempt rows = %d", len(a))
	}

	// A task replayed after the delivery finished is also a no-op.
	if out := e.deliver(h, p); out != OutcomeSkipped {
		t.Fatalf("late duplicate outcome = %s", out)
	}
}

// A worker whose lease was taken over (presumed dead, delivery re-dispatched)
// resumes and reports a failure: it must not overwrite the newer success.
func TestStaleWorkerIsFencedOut(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	firstArrived, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(firstArrived)
			<-release
			w.WriteHeader(500) // the stale worker's (late) failure
			return
		}
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)
	id, _, _ := e.pendingDelivery(srv.URL)
	h := e.devHandler()

	// Worker A claims attempt 1 and hangs on the receiver.
	pA := e.queue(id)
	var outA Outcome
	var errA error
	done := make(chan struct{})
	go func() {
		defer close(done)
		outA, errA = h.Deliver(e.ctx, pA)
	}()
	<-firstArrived

	// A's lease "expires": the reaper and dispatcher (Phase 4) re-queue the
	// delivery, and worker B claims attempt 2 and succeeds.
	if _, err := e.db.Pool.Exec(e.ctx,
		`UPDATE deliveries SET status = 'queued', locked_until = NULL WHERE id = $1 AND status = 'in_flight'`, id); err != nil {
		t.Fatal(err)
	}
	pB := Payload{ProjectID: e.projectID, DeliveryID: id, AttemptCount: 1, QueuedAt: pA.QueuedAt}
	if out := e.deliver(h, pB); out != OutcomeSucceeded {
		t.Fatalf("worker B outcome = %s", out)
	}

	close(release)
	<-done
	if errA != nil || outA != OutcomeFencedOut {
		t.Fatalf("worker A = %s, %v; want fenced_out", outA, errA)
	}

	d := e.delivery(id)
	if d.Status != sqlcgen.DeliveryStatusSucceeded || d.AttemptCount != 2 || *d.LastStatusCode != 200 || d.LastError != nil {
		t.Fatalf("delivery = %+v (stale worker overwrote newer state)", d)
	}
	// A's attempt is kept as history; it does not affect the delivery.
	a := e.attempts(id)
	if len(a) != 2 || *a[0].ResponseStatus != 500 || *a[1].ResponseStatus != 200 {
		t.Fatalf("attempts = %+v", a)
	}
}

func TestStaleOrForeignTasksAreSkipped(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	rc, srv := receiver(t, "")
	id, _, _ := e.pendingDelivery(srv.URL)
	p := e.queue(id)
	h := e.devHandler()

	other := e.newProject()
	for name, bad := range map[string]Payload{
		"other project":       {ProjectID: other, DeliveryID: id, AttemptCount: p.AttemptCount, QueuedAt: p.QueuedAt},
		"stale attempt count": {ProjectID: e.projectID, DeliveryID: id, AttemptCount: p.AttemptCount + 3, QueuedAt: p.QueuedAt},
		"earlier dispatch":    {ProjectID: e.projectID, DeliveryID: id, AttemptCount: p.AttemptCount, QueuedAt: p.QueuedAt.Add(-time.Second)},
		"unknown delivery":    {ProjectID: e.projectID, DeliveryID: store.NewUUID(), AttemptCount: 0, QueuedAt: p.QueuedAt},
	} {
		if out := e.deliver(h, bad); out != OutcomeSkipped {
			t.Errorf("%s: outcome = %s", name, out)
		}
	}
	if d := e.delivery(id); d.Status != sqlcgen.DeliveryStatusQueued || d.AttemptCount != 0 {
		t.Fatalf("delivery changed by a foreign/stale task: %+v", d)
	}
	if out := e.deliver(h, p); out != OutcomeSucceeded {
		t.Fatalf("real task outcome = %s", out)
	}
	if len(rc.Requests()) != 1 {
		t.Fatalf("receiver got %d requests", len(rc.Requests()))
	}

	// A pending (not yet queued) delivery cannot be claimed either.
	id2, _, _ := e.pendingDelivery(srv.URL)
	if out := e.deliver(h, Payload{ProjectID: e.projectID, DeliveryID: id2}); out != OutcomeSkipped {
		t.Fatalf("pending delivery outcome = %s", out)
	}
}

func TestEndpointDisabledAfterDispatchCancels(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	rc, srv := receiver(t, "")
	id, epID, _ := e.pendingDelivery(srv.URL)
	p := e.queue(id)

	disabled := false
	if _, err := e.endpointService(ssrf.Policy{AllowPrivate: true}).
		Update(e.ctx, e.projectID, epID, endpoints.UpdateParams{Enabled: &disabled}); err != nil {
		t.Fatal(err)
	}
	if out := e.deliver(e.devHandler(), p); out != OutcomeCancelled {
		t.Fatalf("outcome = %s", out)
	}
	if d := e.delivery(id); d.Status != sqlcgen.DeliveryStatusCancelled || d.AttemptCount != 0 || d.CompletedAt == nil {
		t.Fatalf("delivery = %+v", d)
	}
	if len(rc.Requests()) != 0 || len(e.attempts(id)) != 0 {
		t.Fatal("a cancelled delivery was sent")
	}
}

func TestUndecryptableSecretLeavesDeliveryForReaper(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	rc, srv := receiver(t, "")
	id, _, _ := e.pendingDelivery(srv.URL)
	p := e.queue(id)

	wrongKey, err := secrets.NewCipher(secrets.GenerateKey())
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(e.st, wrongKey, NewSender(SenderOptions{AllowPrivate: true}), HandlerOptions{Backoff: testPolicy}, discardLogger())
	if _, err := h.Deliver(e.ctx, p); err == nil {
		t.Fatal("expected an error with the wrong encryption key")
	}
	d := e.delivery(id)
	if d.Status != sqlcgen.DeliveryStatusInFlight || d.LockedUntil == nil {
		t.Fatalf("delivery = %+v, want in_flight with a lease", d)
	}
	if len(rc.Requests()) != 0 {
		t.Fatal("sent without a valid signature")
	}
}

// ---- Through Redis (Asynq) ----

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	u := os.Getenv("TEST_REDIS_URL")
	if u == "" {
		t.Skip("TEST_REDIS_URL not set; skipping Redis integration test")
	}
	opts, err := redis.ParseURL(u)
	if err != nil {
		t.Fatalf("TEST_REDIS_URL: %v", err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		// Set but unreachable is a failure, not a skip.
		t.Fatalf("redis ping: %v", err)
	}
	return rdb
}

// testQueue is a per-test Asynq queue, deleted afterwards, so tests never see
// each other's tasks or a developer's.
func testQueue(t *testing.T, rdb *redis.Client) string {
	t.Helper()
	q := "hookrelay_test_" + store.NewUUID().String()[:8]
	t.Cleanup(func() {
		_ = asynq.NewInspectorFromRedisClient(rdb).DeleteQueue(q, true)
	})
	return q
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestWorkerDeliversTaskFromRedis(t *testing.T) {
	t.Parallel()
	rdb := testRedis(t)
	e := newEnv(t)
	q := testQueue(t, rdb)
	rc, srv := receiver(t, "")
	id, _, secret := e.pendingDelivery(srv.URL + "/hook")
	client := asynq.NewClientFromRedisClient(rdb)

	p, err := EnqueueOne(e.ctx, e.st, client, e.projectID, id, 30*time.Second, asynq.Queue(q))
	if err != nil {
		t.Fatalf("EnqueueOne: %v", err)
	}
	if d := e.delivery(id); d.Status != sqlcgen.DeliveryStatusQueued || d.QueuedAt == nil {
		t.Fatalf("delivery = %+v", d)
	}

	// The task in Redis carries no Asynq retries and the configured timeout.
	info, err := asynq.NewInspectorFromRedisClient(rdb).GetTaskInfo(q, p.TaskID())
	if err != nil {
		t.Fatalf("GetTaskInfo: %v", err)
	}
	if info.MaxRetry != 0 || info.Timeout != 30*time.Second || info.Type != TypeDeliver {
		t.Fatalf("task info = MaxRetry %d, Timeout %s, Type %s", info.MaxRetry, info.Timeout, info.Type)
	}

	// Duplicate enqueues: the same attempt is rejected by task ID, and the
	// row is no longer enqueueable.
	task, _ := NewTask(p, 30*time.Second)
	if _, err := client.Enqueue(task, asynq.Queue(q)); !errors.Is(err, asynq.ErrTaskIDConflict) {
		t.Fatalf("duplicate enqueue err = %v, want ErrTaskIDConflict", err)
	}
	if _, err := EnqueueOne(e.ctx, e.st, client, e.projectID, id, 30*time.Second, asynq.Queue(q)); !errors.Is(err, ErrNotEnqueueable) {
		t.Fatalf("second EnqueueOne err = %v", err)
	}
	// A duplicate with a different task ID (e.g. a re-dispatch racing the
	// original) gets through Redis, and must still not cause a second send.
	dup := asynq.NewTask(TypeDeliver, task.Payload(), asynq.MaxRetry(0), asynq.TaskID(p.TaskID()+":dup"))
	if _, err := client.Enqueue(dup, asynq.Queue(q)); err != nil {
		t.Fatalf("enqueue dup: %v", err)
	}

	srvA := asynq.NewServerFromRedisClient(rdb, asynq.Config{
		Concurrency: 4, Queues: map[string]int{q: 1}, Logger: asynqTestLogger{}, LogLevel: asynq.ErrorLevel,
		ShutdownTimeout: 5 * time.Second,
	})
	mux := asynq.NewServeMux()
	mux.Handle(TypeDeliver, e.devHandler())
	if err := srvA.Start(mux); err != nil {
		t.Fatalf("start asynq: %v", err)
	}
	t.Cleanup(srvA.Shutdown)

	waitFor(t, 20*time.Second, func() bool { return e.delivery(id).Status == sqlcgen.DeliveryStatusSucceeded })
	// Give the duplicate task time to be processed too.
	insp := asynq.NewInspectorFromRedisClient(rdb)
	waitFor(t, 10*time.Second, func() bool {
		qi, err := insp.GetQueueInfo(q)
		return err == nil && qi.Pending == 0 && qi.Active == 0
	})

	if n := rc.Count(id.String()); n != 1 {
		t.Fatalf("receiver got the delivery %d times, want exactly 1", n)
	}
	r := rc.Requests()[0]
	if err := signing.Verify(secret, r.Header, r.Body, time.Now(), 0); err != nil || string(r.Body) != string(testPayload) {
		t.Fatalf("delivered request invalid: %v", err)
	}
	if d := e.delivery(id); d.AttemptCount != 1 || len(e.attempts(id)) != 1 {
		t.Fatalf("delivery = %+v", d)
	}
}

// A failed attempt is rescheduled in Postgres and the task is NOT retried by
// Asynq (MaxRetry 0): nothing is left in the retry or archive sets.
func TestFailedAttemptIsRetriedByPostgresNotAsynq(t *testing.T) {
	t.Parallel()
	rdb := testRedis(t)
	e := newEnv(t)
	q := testQueue(t, rdb)
	rc, srv := receiver(t, "")
	id, _, _ := e.pendingDelivery(srv.URL + "/hook?status=500")

	client := asynq.NewClientFromRedisClient(rdb)
	if _, err := EnqueueOne(e.ctx, e.st, client, e.projectID, id, 30*time.Second, asynq.Queue(q)); err != nil {
		t.Fatal(err)
	}
	srvA := asynq.NewServerFromRedisClient(rdb, asynq.Config{
		Concurrency: 2, Queues: map[string]int{q: 1}, Logger: asynqTestLogger{}, LogLevel: asynq.ErrorLevel,
	})
	mux := asynq.NewServeMux()
	mux.Handle(TypeDeliver, e.devHandler())
	if err := srvA.Start(mux); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srvA.Shutdown)

	waitFor(t, 20*time.Second, func() bool { return e.delivery(id).Status == sqlcgen.DeliveryStatusScheduled })
	insp := asynq.NewInspectorFromRedisClient(rdb)
	waitFor(t, 10*time.Second, func() bool {
		qi, err := insp.GetQueueInfo(q)
		return err == nil && qi.Pending == 0 && qi.Active == 0
	})
	qi, err := insp.GetQueueInfo(q)
	if err != nil {
		t.Fatal(err)
	}
	if qi.Retry != 0 || qi.Archived != 0 || qi.Scheduled != 0 {
		t.Fatalf("Asynq kept retry state: retry=%d archived=%d scheduled=%d", qi.Retry, qi.Archived, qi.Scheduled)
	}
	if len(rc.Requests()) != 1 {
		t.Fatalf("receiver got %d requests, want 1", len(rc.Requests()))
	}
	if d := e.delivery(id); d.AttemptCount != 1 || !d.NextAttemptAt.After(time.Now().Add(-time.Minute)) {
		t.Fatalf("delivery = %+v", d)
	}
}

type asynqTestLogger struct{}

func (asynqTestLogger) Debug(...any) {}
func (asynqTestLogger) Info(...any)  {}
func (asynqTestLogger) Warn(...any)  {}
func (asynqTestLogger) Error(...any) {}
func (asynqTestLogger) Fatal(...any) {}
