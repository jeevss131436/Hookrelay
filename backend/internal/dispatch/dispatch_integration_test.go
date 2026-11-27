package dispatch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"

	"github.com/jeevss131436/Hookrelay/backend/internal/backoff"
	"github.com/jeevss131436/Hookrelay/backend/internal/delivery"
	"github.com/jeevss131436/Hookrelay/backend/internal/endpoints"
	"github.com/jeevss131436/Hookrelay/backend/internal/ingest"
	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/signing"
	"github.com/jeevss131436/Hookrelay/backend/internal/ssrf"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
	"github.com/jeevss131436/Hookrelay/backend/internal/testutil/pgtest"
)

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// testPolicy: attempt n waits within [5s, min(1h, 10s*2^(n-1))].
var testPolicy = backoff.NewSeeded(10*time.Second, time.Hour, 4)

// ---- environment ----

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
	p, err := e.st.CreateProject(e.ctx, "dispatch test")
	if err != nil {
		t.Fatal(err)
	}
	e.projectID = p.ID
	return e
}

// endpoint registers url (private targets allowed: receivers are local).
func (e *env) endpoint(url string) string {
	e.t.Helper()
	_, secret, err := endpoints.NewService(e.st, e.cipher, ssrf.Policy{AllowPrivate: true}).
		Create(e.ctx, e.projectID, endpoints.CreateParams{URL: url})
	if err != nil {
		e.t.Fatalf("create endpoint: %v", err)
	}
	return secret
}

// ingestN ingests n events and returns the IDs of all deliveries created.
func (e *env) ingestN(n int) []pgtype.UUID {
	e.t.Helper()
	svc := ingest.NewService(e.st)
	var ids []pgtype.UUID
	for i := range n {
		res, err := svc.Ingest(e.ctx, e.projectID, ingest.Request{
			EventType: "order.paid", Payload: fmt.Appendf(nil, `{"n": %d}`, i),
		})
		if err != nil {
			e.t.Fatalf("ingest: %v", err)
		}
		rows, err := e.db.Pool.Query(e.ctx, `SELECT id FROM deliveries WHERE event_id = $1`, res.EventID)
		if err != nil {
			e.t.Fatal(err)
		}
		for rows.Next() {
			var id pgtype.UUID
			if err := rows.Scan(&id); err != nil {
				e.t.Fatal(err)
			}
			ids = append(ids, id)
		}
		rows.Close()
	}
	return ids
}

func (e *env) get(id pgtype.UUID) sqlcgen.Delivery {
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
		e.t.Fatal(err)
	}
	return a
}

func (e *env) exec(sql string, args ...any) {
	e.t.Helper()
	if _, err := e.db.Pool.Exec(e.ctx, sql, args...); err != nil {
		e.t.Fatalf("%s: %v", sql, err)
	}
}

func (e *env) count(sql string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.db.Pool.QueryRow(e.ctx, sql, args...).Scan(&n); err != nil {
		e.t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func (e *env) dbNow() time.Time {
	e.t.Helper()
	var now time.Time
	if err := e.db.Pool.QueryRow(e.ctx, `SELECT now()`).Scan(&now); err != nil {
		e.t.Fatal(err)
	}
	return now
}

// claim does what a worker's claim does, with the given lease (negative =
// already expired), and returns the claimed attempt number.
func (e *env) claim(id pgtype.UUID, leaseMs int64) int32 {
	e.t.Helper()
	d := e.get(id)
	row, err := e.st.ClaimDelivery(e.ctx, sqlcgen.ClaimDeliveryParams{
		LeaseMs: leaseMs, ProjectID: e.projectID, ID: id, ExpectedAttemptCount: d.AttemptCount, QueuedAt: *d.QueuedAt,
	})
	if err != nil {
		e.t.Fatalf("claim: %v", err)
	}
	return row.AttemptCount
}

func (e *env) dispatcher(q Enqueuer, o DispatcherOptions) *Dispatcher {
	return NewDispatcher(e.st, q, o, discardLogger())
}

func (e *env) reaper(o ReaperOptions) *Reaper {
	if o.Backoff.Base() == 0 {
		o.Backoff = testPolicy
	}
	return NewReaper(e.st, o, discardLogger())
}

// ---- fake queue ----

// fakeQueue records tasks like Asynq would (unique task IDs) and can be told
// to fail, simulating a Redis outage.
type fakeQueue struct {
	mu      sync.Mutex
	tasks   map[string]delivery.Payload
	perDlv  map[string]int // successful enqueues per delivery
	down    atomic.Bool
	failAt  int // fail the n-th call (1-based); 0 = never
	calls   int
	failErr error
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{tasks: map[string]delivery.Payload{}, perDlv: map[string]int{}, failErr: errors.New("dial tcp: connection refused")}
}

func (f *fakeQueue) EnqueueContext(_ context.Context, t *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.down.Load() || f.calls == f.failAt {
		return nil, f.failErr
	}
	p, err := delivery.ParsePayload(t.Payload())
	if err != nil {
		return nil, err
	}
	if _, ok := f.tasks[p.TaskID()]; ok {
		return nil, asynq.ErrTaskIDConflict
	}
	f.tasks[p.TaskID()] = p
	f.perDlv[p.DeliveryID.String()]++
	return &asynq.TaskInfo{ID: p.TaskID()}, nil
}

func (f *fakeQueue) len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.tasks)
}

func (f *fakeQueue) payloadFor(id pgtype.UUID) []delivery.Payload {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []delivery.Payload
	for _, p := range f.tasks {
		if p.DeliveryID == id {
			out = append(out, p)
		}
	}
	return out
}

// ======================= Dispatcher =======================

func TestDispatchEnqueuesDueDeliveries(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.endpoint("http://127.0.0.1:1/a")
	ids := e.ingestN(3)
	q := newFakeQueue()
	d := e.dispatcher(q, DispatcherOptions{BatchSize: 10})

	n, err := d.RunOnce(e.ctx)
	if err != nil || n != 3 {
		t.Fatalf("RunOnce = %d, %v; want 3", n, err)
	}
	for _, id := range ids {
		row := e.get(id)
		if row.Status != sqlcgen.DeliveryStatusQueued || row.QueuedAt == nil {
			t.Fatalf("delivery = %+v", row)
		}
		ps := q.payloadFor(id)
		if len(ps) != 1 || ps[0].ProjectID != e.projectID || ps[0].AttemptCount != row.AttemptCount ||
			!ps[0].QueuedAt.Equal(*row.QueuedAt) {
			t.Fatalf("task payloads = %+v for row %+v", ps, row)
		}
	}
	// Nothing is due any more: queued rows are not re-dispatched.
	if n, err := d.RunOnce(e.ctx); err != nil || n != 0 || q.len() != 3 {
		t.Fatalf("second RunOnce = %d, %v (tasks %d)", n, err, q.len())
	}
}

func TestDispatchBatchesRespectBatchSize(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.endpoint("http://127.0.0.1:1/a")
	e.ingestN(7)
	q := newFakeQueue()
	d := e.dispatcher(q, DispatcherOptions{BatchSize: 3})
	for _, want := range []int{3, 3, 1, 0} {
		if n, err := d.RunOnce(e.ctx); err != nil || n != want {
			t.Fatalf("RunOnce = %d, %v; want %d", n, err, want)
		}
	}
}

func TestDispatchDoesNotEnqueueFutureDeliveries(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.endpoint("http://127.0.0.1:1/a")
	ids := e.ingestN(2)
	e.exec(`UPDATE deliveries SET status = 'scheduled', attempt_count = 1, next_attempt_at = now() + interval '1 hour' WHERE id = $1`, ids[0])
	e.exec(`UPDATE deliveries SET next_attempt_at = now() + interval '1 minute' WHERE id = $1`, ids[1])
	q := newFakeQueue()
	if n, err := e.dispatcher(q, DispatcherOptions{}).RunOnce(e.ctx); err != nil || n != 0 {
		t.Fatalf("RunOnce = %d, %v; want 0", n, err)
	}
	if s := e.get(ids[0]).Status; s != sqlcgen.DeliveryStatusScheduled {
		t.Errorf("future scheduled delivery is %s", s)
	}
	if s := e.get(ids[1]).Status; s != sqlcgen.DeliveryStatusPending {
		t.Errorf("future pending delivery is %s", s)
	}
	// Once due, it is dispatched.
	e.exec(`UPDATE deliveries SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, ids[0])
	if n, _ := e.dispatcher(q, DispatcherOptions{}).RunOnce(e.ctx); n != 1 {
		t.Fatalf("due delivery not dispatched (n=%d)", n)
	}
}

// A real Asynq client pointed at an address where no Redis listens.
func TestDispatchWithRedisUnavailableLosesNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.endpoint("http://127.0.0.1:1/a")
	ids := e.ingestN(4)
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = dead.Close() })

	before := e.dbNow()
	n, err := e.dispatcher(asynq.NewClientFromRedisClient(dead), DispatcherOptions{RedisRetryDelay: time.Minute}).RunOnce(e.ctx)
	if err == nil || n != 0 {
		t.Fatalf("RunOnce = %d, %v; want an enqueue error", n, err)
	}
	for _, id := range ids {
		d := e.get(id)
		// Released, not stranded in 'queued'; retried after RedisRetryDelay.
		if d.Status != sqlcgen.DeliveryStatusScheduled || d.AttemptCount != 0 ||
			d.NextAttemptAt.Before(before.Add(59*time.Second)) {
			t.Fatalf("delivery after failed dispatch = %+v", d)
		}
	}
}

func TestDispatchRecoversWhenRedisComesBack(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.endpoint("http://127.0.0.1:1/a")
	ids := e.ingestN(5)
	q := newFakeQueue()
	q.failAt = 3 // Redis dies in the middle of the batch
	d := e.dispatcher(q, DispatcherOptions{RedisRetryDelay: time.Millisecond})

	n, err := d.RunOnce(e.ctx)
	if err == nil || n != 2 {
		t.Fatalf("RunOnce = %d, %v; want 2 enqueued then an error", n, err)
	}
	if got := e.count(`SELECT count(*) FROM deliveries WHERE status = 'queued'`); got != 2 {
		t.Fatalf("queued = %d, want the 2 that reached Redis", got)
	}
	if got := e.count(`SELECT count(*) FROM deliveries WHERE status = 'scheduled'`); got != 3 {
		t.Fatalf("scheduled = %d, want the 3 released", got)
	}

	time.Sleep(20 * time.Millisecond) // RedisRetryDelay passes; Redis is back
	if n, err := d.RunOnce(e.ctx); err != nil || n != 3 {
		t.Fatalf("RunOnce after recovery = %d, %v; want 3", n, err)
	}
	for _, id := range ids {
		if got := q.perDlv[id.String()]; got != 1 {
			t.Errorf("delivery %s enqueued %d times", id.String(), got)
		}
		if s := e.get(id).Status; s != sqlcgen.DeliveryStatusQueued {
			t.Errorf("delivery %s is %s", id.String(), s)
		}
	}
}

// The dispatcher dies after moving rows to 'queued' but before enqueueing.
func TestDispatcherCrashBeforeEnqueueIsRecoveredByReaper(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.endpoint("http://127.0.0.1:1/a")
	ids := e.ingestN(2)
	q := newFakeQueue()
	d := e.dispatcher(q, DispatcherOptions{})

	crashed, err := d.claimBatch(e.ctx) // step 1 commits, step 2 never runs
	if err != nil || len(crashed) != 2 {
		t.Fatalf("claimBatch = %d, %v", len(crashed), err)
	}
	// Stranded: queued, no task, and not due for the dispatcher.
	if n, _ := d.RunOnce(e.ctx); n != 0 || q.len() != 0 {
		t.Fatalf("stranded rows were re-dispatched before the lease expired")
	}
	// A reaper with a long lease leaves them alone...
	if res, rerr := e.reaper(ReaperOptions{DispatchLease: time.Hour}).RunOnce(e.ctx); rerr != nil || res.Requeued != 0 {
		t.Fatalf("reaper(long lease) = %+v, %v", res, rerr)
	}
	// ...one past the lease makes them due again, without using an attempt.
	time.Sleep(5 * time.Millisecond)
	res, err := e.reaper(ReaperOptions{DispatchLease: time.Millisecond}).RunOnce(e.ctx)
	if err != nil || res.Requeued != 2 {
		t.Fatalf("reaper = %+v, %v; want 2 requeued", res, err)
	}
	if n, err := d.RunOnce(e.ctx); err != nil || n != 2 {
		t.Fatalf("re-dispatch = %d, %v", n, err)
	}
	for _, id := range ids {
		row := e.get(id)
		if row.Status != sqlcgen.DeliveryStatusQueued || row.AttemptCount != 0 {
			t.Fatalf("delivery = %+v", row)
		}
	}

	// Had the crashed dispatcher's task reached Redis after all, it would
	// now be stale: a worker processing it claims nothing.
	h := delivery.NewHandler(e.st, e.cipher, delivery.NewSender(delivery.SenderOptions{AllowPrivate: true}),
		delivery.HandlerOptions{Backoff: testPolicy}, discardLogger())
	old := delivery.Payload{ProjectID: crashed[0].ProjectID, DeliveryID: crashed[0].ID,
		AttemptCount: crashed[0].AttemptCount, QueuedAt: crashed[0].QueuedAt}
	if out, err := h.Deliver(e.ctx, old); err != nil || out != delivery.OutcomeSkipped {
		t.Fatalf("task from the crashed dispatch = %s, %v; want skipped", out, err)
	}
}

func TestConcurrentDispatchersEnqueueEachDeliveryOnce(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for i := range 3 {
		e.endpoint(fmt.Sprintf("http://127.0.0.1:1/%d", i))
	}
	ids := e.ingestN(60) // 180 deliveries
	q := newFakeQueue()

	const dispatchers = 6
	var wg sync.WaitGroup
	var total atomic.Int64
	for range dispatchers {
		d := e.dispatcher(q, DispatcherOptions{BatchSize: 7})
		wg.Go(func() {
			for {
				n, err := d.RunOnce(e.ctx)
				if err != nil {
					t.Errorf("RunOnce: %v", err)
					return
				}
				total.Add(int64(n))
				if n == 0 {
					return
				}
			}
		})
	}
	wg.Wait()

	if total.Load() != int64(len(ids)) || q.len() != len(ids) {
		t.Fatalf("enqueued %d (tasks %d), want %d", total.Load(), q.len(), len(ids))
	}
	for _, id := range ids {
		if got := q.perDlv[id.String()]; got != 1 {
			t.Fatalf("delivery %s enqueued %d times", id.String(), got)
		}
	}
	if got := e.count(`SELECT count(*) FROM deliveries WHERE status = 'queued'`); got != len(ids) {
		t.Fatalf("queued rows = %d", got)
	}
}

// ======================= Reaper =======================

func TestReaperRecoversExpiredInFlight(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.endpoint("http://127.0.0.1:1/a")
	ids := e.ingestN(2)
	// Second delivery is on its third attempt, to check the backoff step.
	e.exec(`UPDATE deliveries SET status = 'scheduled', attempt_count = 2 WHERE id = $1`, ids[1])
	if _, err := e.dispatcher(newFakeQueue(), DispatcherOptions{}).RunOnce(e.ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		e.claim(id, -1000) // the worker claimed and died; its lease is over
	}

	before := e.dbNow()
	res, err := e.reaper(ReaperOptions{}).RunOnce(e.ctx)
	after := e.dbNow()
	if err != nil || res.Scheduled != 2 || res.Dead != 0 {
		t.Fatalf("RunOnce = %+v, %v", res, err)
	}
	for i, id := range ids {
		d := e.get(id)
		attempt := int32(i*2 + 1) // 1 and 3
		if d.Status != sqlcgen.DeliveryStatusScheduled || d.AttemptCount != attempt || d.LockedUntil != nil ||
			d.CompletedAt != nil || d.LastError == nil || *d.LastError != workerLostMessage {
			t.Fatalf("delivery = %+v", d)
		}
		// The existing retry policy, applied on the DB clock.
		lo, hi := before.Add(testPolicy.Base()/2), after.Add(testPolicy.Ceiling(int(attempt)))
		if d.NextAttemptAt.Before(lo) || d.NextAttemptAt.After(hi) {
			t.Fatalf("attempt %d: next_attempt_at %s not within [%s, %s]", attempt, d.NextAttemptAt, lo, hi)
		}
		a := e.attempts(id)
		last := a[len(a)-1]
		if len(a) != 1 || last.AttemptNumber != attempt || last.ErrorKind == nil || *last.ErrorKind != "worker_lost" ||
			last.ResponseStatus != nil || last.FinishedAt.Before(last.StartedAt) {
			t.Fatalf("attempts = %+v", a)
		}
	}
}

func TestReaperDoesNotReclaimActiveWorkers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.endpoint("http://127.0.0.1:1/a")
	ids := e.ingestN(1)
	if _, err := e.dispatcher(newFakeQueue(), DispatcherOptions{}).RunOnce(e.ctx); err != nil {
		t.Fatal(err)
	}
	// A generous lease: leases are measured on the DB clock, and a serverless
	// compute resuming from suspend may step its clock forward.
	e.claim(ids[0], time.Hour.Milliseconds())
	res, err := e.reaper(ReaperOptions{DispatchLease: time.Millisecond}).RunOnce(e.ctx)
	if err != nil || res != (ReapResult{}) {
		var state string
		_ = e.db.Pool.QueryRow(e.ctx, `SELECT format('status=%s locked_until=%s now=%s attempt=%s',
			status, locked_until, now(), attempt_count) FROM deliveries WHERE id = $1`, ids[0]).Scan(&state)
		t.Fatalf("RunOnce = %+v, %v; want nothing reaped (%s)", res, err, state)
	}
	if d := e.get(ids[0]); d.Status != sqlcgen.DeliveryStatusInFlight || d.LockedUntil == nil {
		t.Fatalf("active delivery was touched: %+v", d)
	}
	if len(e.attempts(ids[0])) != 0 {
		t.Fatal("attempt recorded for an active worker")
	}
}

func TestReaperMarksExhaustedDeliveryDead(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.endpoint("http://127.0.0.1:1/a")
	ids := e.ingestN(1)
	e.exec(`UPDATE deliveries SET max_attempts = 3, attempt_count = 2, status = 'scheduled' WHERE id = $1`, ids[0])
	if _, err := e.dispatcher(newFakeQueue(), DispatcherOptions{}).RunOnce(e.ctx); err != nil {
		t.Fatal(err)
	}
	e.claim(ids[0], -1)
	res, err := e.reaper(ReaperOptions{}).RunOnce(e.ctx)
	if err != nil || res.Dead != 1 || res.Scheduled != 0 {
		t.Fatalf("RunOnce = %+v, %v", res, err)
	}
	d := e.get(ids[0])
	if d.Status != sqlcgen.DeliveryStatusDead || d.AttemptCount != 3 || d.CompletedAt == nil || d.LockedUntil != nil {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestReaperIsIdempotent(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.endpoint("http://127.0.0.1:1/a")
	ids := e.ingestN(3)
	if _, err := e.dispatcher(newFakeQueue(), DispatcherOptions{}).RunOnce(e.ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		e.claim(id, -1)
	}
	r := e.reaper(ReaperOptions{})
	if res, err := r.RunOnce(e.ctx); err != nil || res.Scheduled != 3 {
		t.Fatalf("first pass = %+v, %v", res, err)
	}
	snapshot := map[string]time.Time{}
	for _, id := range ids {
		snapshot[id.String()] = e.get(id).NextAttemptAt
	}
	for range 3 {
		if res, err := r.RunOnce(e.ctx); err != nil || res != (ReapResult{}) {
			t.Fatalf("repeat pass = %+v, %v; want no-op", res, err)
		}
	}
	for _, id := range ids {
		if d := e.get(id); !d.NextAttemptAt.Equal(snapshot[id.String()]) || d.AttemptCount != 1 {
			t.Fatalf("repeat pass changed %+v", d)
		}
		if n := len(e.attempts(id)); n != 1 {
			t.Fatalf("%d attempt rows after repeated passes", n)
		}
	}
}

func TestConcurrentReapersDoNotCorruptState(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for i := range 2 {
		e.endpoint(fmt.Sprintf("http://127.0.0.1:1/%d", i))
	}
	ids := e.ingestN(20) // 40 deliveries
	if _, err := e.dispatcher(newFakeQueue(), DispatcherOptions{BatchSize: 100}).RunOnce(e.ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		e.claim(id, -1)
	}
	const reapers = 5
	var wg sync.WaitGroup
	var scheduled atomic.Int64
	start := make(chan struct{})
	for range reapers {
		r := e.reaper(ReaperOptions{})
		wg.Go(func() {
			<-start
			res, err := r.RunOnce(e.ctx)
			if err != nil {
				t.Errorf("RunOnce: %v", err)
			}
			scheduled.Add(int64(res.Scheduled))
		})
	}
	close(start)
	wg.Wait()

	if scheduled.Load() != int64(len(ids)) {
		t.Fatalf("recoveries reported = %d, want exactly %d", scheduled.Load(), len(ids))
	}
	if got := e.count(`SELECT count(*) FROM deliveries WHERE status = 'scheduled' AND attempt_count = 1`); got != len(ids) {
		t.Fatalf("scheduled rows = %d", got)
	}
	if got := e.count(`SELECT count(*) FROM delivery_attempts WHERE error_kind = 'worker_lost'`); got != len(ids) {
		t.Fatalf("worker_lost attempts = %d, want one per delivery", got)
	}
}

func TestReaperRequeuesOnlyStaleQueued(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.endpoint("http://127.0.0.1:1/a")
	ids := e.ingestN(1)
	if _, err := e.dispatcher(newFakeQueue(), DispatcherOptions{}).RunOnce(e.ctx); err != nil {
		t.Fatal(err)
	}
	if res, _ := e.reaper(ReaperOptions{DispatchLease: time.Hour}).RunOnce(e.ctx); res.Requeued != 0 {
		t.Fatal("fresh queued delivery was requeued")
	}
	time.Sleep(5 * time.Millisecond)
	if res, _ := e.reaper(ReaperOptions{DispatchLease: time.Millisecond}).RunOnce(e.ctx); res.Requeued != 1 {
		t.Fatalf("stale queued delivery not requeued: %+v", res)
	}
	if d := e.get(ids[0]); d.Status != sqlcgen.DeliveryStatusScheduled || d.AttemptCount != 0 || len(e.attempts(ids[0])) != 0 {
		t.Fatalf("delivery = %+v (a requeue must not consume an attempt)", d)
	}
}

// A worker whose lease expired is recovered by the reaper; when the worker
// finally finishes, it must not overwrite the recovered state.
func TestStaleWorkerCannotOverwriteReaperRecovery(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	arrived, release := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(arrived)
		<-release
		w.WriteHeader(200) // a late success
	}))
	t.Cleanup(srv.Close)
	e.endpoint(srv.URL)
	ids := e.ingestN(1)
	q := newFakeQueue()
	if _, err := e.dispatcher(q, DispatcherOptions{}).RunOnce(e.ctx); err != nil {
		t.Fatal(err)
	}
	p := q.payloadFor(ids[0])[0]

	// Lease of 1 ms: the reaper may take over as soon as the worker is busy.
	h := delivery.NewHandler(e.st, e.cipher, delivery.NewSender(delivery.SenderOptions{AllowPrivate: true}),
		delivery.HandlerOptions{Lease: time.Millisecond, Backoff: testPolicy}, discardLogger())
	var out delivery.Outcome
	done := make(chan struct{})
	go func() {
		defer close(done)
		var err error
		out, err = h.Deliver(e.ctx, p)
		if err != nil {
			t.Errorf("Deliver: %v", err)
		}
	}()
	<-arrived
	time.Sleep(5 * time.Millisecond)
	if res, err := e.reaper(ReaperOptions{}).RunOnce(e.ctx); err != nil || res.Scheduled != 1 {
		t.Fatalf("reaper = %+v, %v", res, err)
	}
	close(release)
	<-done

	if out != delivery.OutcomeFencedOut {
		t.Fatalf("stale worker outcome = %s, want fenced_out", out)
	}
	d := e.get(ids[0])
	if d.Status != sqlcgen.DeliveryStatusScheduled || d.AttemptCount != 1 || d.LastStatusCode != nil {
		t.Fatalf("stale worker overwrote recovered state: %+v", d)
	}
	a := e.attempts(ids[0])
	if len(a) != 1 || *a[0].ErrorKind != "worker_lost" {
		t.Fatalf("attempts = %+v", a)
	}
}

// ======================= Full pipeline (real Redis) =======================

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
		t.Fatalf("redis ping: %v", err) // set but unreachable is a failure
	}
	return rdb
}

func testQueue(t *testing.T, rdb *redis.Client) string {
	t.Helper()
	q := "hookrelay_test_" + store.NewUUID().String()[:8]
	t.Cleanup(func() { _ = asynq.NewInspectorFromRedisClient(rdb).DeleteQueue(q, true) })
	return q
}

type asynqTestLogger struct{}

func (asynqTestLogger) Debug(...any) {}
func (asynqTestLogger) Info(...any)  {}
func (asynqTestLogger) Warn(...any)  {}
func (asynqTestLogger) Error(...any) {}
func (asynqTestLogger) Fatal(...any) {}

// startWorker runs an Asynq server with the real delivery handler.
func startWorker(t *testing.T, rdb *redis.Client, q string, h *delivery.Handler, concurrency int) {
	t.Helper()
	srv := asynq.NewServerFromRedisClient(rdb, asynq.Config{
		Concurrency: concurrency, Queues: map[string]int{q: 1}, Logger: asynqTestLogger{},
		LogLevel: asynq.FatalLevel, ShutdownTimeout: 3 * time.Second,
	})
	mux := asynq.NewServeMux()
	mux.Handle(delivery.TypeDeliver, h)
	if err := srv.Start(mux); err != nil {
		t.Fatalf("start asynq: %v", err)
	}
	t.Cleanup(srv.Shutdown)
}

// runLoop runs fn until the test ends.
func runLoop(t *testing.T, fn func(context.Context)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); fn(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// verifyingReceiver counts deliveries per webhook-id and rejects bad signatures.
type verifyingReceiver struct {
	secret string
	mu     sync.Mutex
	seen   map[string]int
	bad    int
	// gate, if set, decides the response for the n-th request (1-based).
	gate func(n int, w http.ResponseWriter) bool
	n    atomic.Int32
}

func (rc *verifyingReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	n := int(rc.n.Add(1))
	rc.mu.Lock()
	secret := rc.secret
	rc.mu.Unlock()
	if err := signing.Verify(secret, r.Header, body, time.Now(), 0); err != nil {
		rc.mu.Lock()
		rc.bad++
		rc.mu.Unlock()
		w.WriteHeader(401)
		return
	}
	rc.mu.Lock()
	rc.seen[r.Header.Get(signing.HeaderID)]++
	rc.mu.Unlock()
	if rc.gate != nil && rc.gate(n, w) {
		return
	}
	w.WriteHeader(200)
}

func (rc *verifyingReceiver) count(id pgtype.UUID) int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.seen[id.String()]
}

func newReceiver(t *testing.T, e *env, gate func(int, http.ResponseWriter) bool) *verifyingReceiver {
	t.Helper()
	rc := &verifyingReceiver{seen: map[string]int{}, gate: gate}
	srv := httptest.NewServer(rc)
	t.Cleanup(srv.Close)
	secret := e.endpoint(srv.URL + "/hook")
	rc.mu.Lock()
	rc.secret = secret
	rc.mu.Unlock()
	return rc
}

func (rc *verifyingReceiver) badSignatures() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.bad
}

func (e *env) handler(lease time.Duration) *delivery.Handler {
	return delivery.NewHandler(e.st, e.cipher, delivery.NewSender(delivery.SenderOptions{AllowPrivate: true, Timeout: 5 * time.Second}),
		delivery.HandlerOptions{Lease: lease, Backoff: backoff.New(50*time.Millisecond, 200*time.Millisecond)}, discardLogger())
}

func TestPipelineEndToEnd(t *testing.T) {
	t.Parallel()
	rdb := testRedis(t)
	e := newEnv(t)
	q := testQueue(t, rdb)
	rc := newReceiver(t, e, nil)
	ids := e.ingestN(5)

	startWorker(t, rdb, q, e.handler(time.Minute), 4)
	d := e.dispatcher(asynq.NewClientFromRedisClient(rdb), DispatcherOptions{Interval: 50 * time.Millisecond, Queue: q})
	runLoop(t, d.Run)

	waitFor(t, 20*time.Second, "all deliveries to succeed", func() bool {
		return e.count(`SELECT count(*) FROM deliveries WHERE status = 'succeeded'`) == len(ids)
	})
	for _, id := range ids {
		if rc.count(id) != 1 {
			t.Errorf("delivery %s received %d times", id.String(), rc.count(id))
		}
		if a := e.attempts(id); len(a) != 1 || *a[0].ResponseStatus != 200 {
			t.Errorf("attempts = %+v", a)
		}
	}
	if n := rc.badSignatures(); n != 0 {
		t.Fatalf("%d requests failed signature verification", n)
	}
}

// switchQueue forwards to Redis unless "down" is set.
type switchQueue struct {
	real Enqueuer
	down atomic.Bool
}

func (s *switchQueue) EnqueueContext(ctx context.Context, t *asynq.Task, o ...asynq.Option) (*asynq.TaskInfo, error) {
	if s.down.Load() {
		return nil, errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")
	}
	return s.real.EnqueueContext(ctx, t, o...)
}

func TestPipelineRedisOutageAndRecovery(t *testing.T) {
	t.Parallel()
	rdb := testRedis(t)
	e := newEnv(t)
	q := testQueue(t, rdb)
	rc := newReceiver(t, e, nil)

	sq := &switchQueue{real: asynq.NewClientFromRedisClient(rdb)}
	sq.down.Store(true)
	startWorker(t, rdb, q, e.handler(time.Minute), 4)
	d := e.dispatcher(sq, DispatcherOptions{Interval: 20 * time.Millisecond, RedisRetryDelay: 50 * time.Millisecond,
		MaxBackoff: 100 * time.Millisecond, Queue: q})
	runLoop(t, d.Run)

	// Ingest never touches Redis: it succeeds during the outage.
	ids := e.ingestN(6)
	// Failed dispatch rounds release the rows back to 'scheduled' (between
	// a claim and its release they are briefly 'queued').
	waitFor(t, 10*time.Second, "failed dispatches to release rows", func() bool {
		return e.count(`SELECT count(*) FROM deliveries WHERE status = 'scheduled'`) == len(ids)
	})
	time.Sleep(200 * time.Millisecond) // several more failed rounds
	if got := e.count(`SELECT count(*) FROM deliveries
		WHERE status IN ('pending', 'scheduled', 'queued') AND attempt_count = 0`); got != len(ids) {
		t.Fatalf("during the outage only %d of %d deliveries are still waiting in Postgres", got, len(ids))
	}
	if got := e.count(`SELECT count(*) FROM delivery_attempts`); got != 0 {
		t.Fatalf("attempts during outage = %d", got)
	}

	sq.down.Store(false) // Redis is back
	waitFor(t, 20*time.Second, "deliveries after Redis recovery", func() bool {
		return e.count(`SELECT count(*) FROM deliveries WHERE status = 'succeeded'`) == len(ids)
	})
	for _, id := range ids {
		if rc.count(id) != 1 {
			t.Errorf("delivery %s received %d times", id.String(), rc.count(id))
		}
	}
}

// A worker hangs mid-delivery (as if crashed). The reaper recovers the
// delivery after the lease, it is re-dispatched and succeeds; the hung worker
// eventually returns and is fenced out.
func TestPipelineWorkerCrashRecoveredByReaper(t *testing.T) {
	t.Parallel()
	rdb := testRedis(t)
	e := newEnv(t)
	q := testQueue(t, rdb)
	release := make(chan struct{})
	rc := newReceiver(t, e, func(n int, w http.ResponseWriter) bool {
		if n == 1 {
			<-release // first worker never comes back in time
			w.WriteHeader(200)
			return true
		}
		return false
	})
	ids := e.ingestN(1)

	startWorker(t, rdb, q, e.handler(300*time.Millisecond), 2) // short lease
	d := e.dispatcher(asynq.NewClientFromRedisClient(rdb), DispatcherOptions{Interval: 20 * time.Millisecond, Queue: q})
	runLoop(t, d.Run)
	r := e.reaper(ReaperOptions{Interval: 50 * time.Millisecond, Backoff: backoff.New(50*time.Millisecond, 100*time.Millisecond)})
	runLoop(t, r.Run)

	waitFor(t, 20*time.Second, "recovery and redelivery", func() bool {
		return e.get(ids[0]).Status == sqlcgen.DeliveryStatusSucceeded
	})
	close(release)

	dl := e.get(ids[0])
	if dl.AttemptCount != 2 || *dl.LastStatusCode != 200 {
		t.Fatalf("delivery = %+v", dl)
	}
	a := e.attempts(ids[0])
	if len(a) != 2 || *a[0].ErrorKind != "worker_lost" || a[1].ResponseStatus == nil || *a[1].ResponseStatus != 200 {
		t.Fatalf("attempts = %+v", a)
	}
	// The hung worker's late 200 must not change anything.
	time.Sleep(200 * time.Millisecond)
	if d2 := e.get(ids[0]); d2.Status != sqlcgen.DeliveryStatusSucceeded || d2.AttemptCount != 2 || len(e.attempts(ids[0])) != 2 {
		t.Fatalf("stale worker changed state: %+v", d2)
	}
	if rc.count(ids[0]) != 2 || rc.badSignatures() != 0 {
		t.Fatalf("receiver saw %d requests (at-least-once: the hung attempt and the retry)", rc.count(ids[0]))
	}
}

func TestPipelineAlwaysFailingReceiverEndsDead(t *testing.T) {
	t.Parallel()
	rdb := testRedis(t)
	e := newEnv(t)
	q := testQueue(t, rdb)
	rc := newReceiver(t, e, func(_ int, w http.ResponseWriter) bool {
		w.WriteHeader(http.StatusInternalServerError)
		return true
	})
	ids := e.ingestN(1)
	e.exec(`UPDATE deliveries SET max_attempts = 3 WHERE id = $1`, ids[0])

	startWorker(t, rdb, q, e.handler(time.Minute), 2) // backoff 25-200ms
	runLoop(t, e.dispatcher(asynq.NewClientFromRedisClient(rdb), DispatcherOptions{Interval: 20 * time.Millisecond, Queue: q}).Run)

	waitFor(t, 20*time.Second, "delivery to die", func() bool { return e.get(ids[0]).Status == sqlcgen.DeliveryStatusDead })
	d := e.get(ids[0])
	if d.AttemptCount != 3 || d.CompletedAt == nil || *d.LastStatusCode != 500 {
		t.Fatalf("delivery = %+v", d)
	}
	a := e.attempts(ids[0])
	if len(a) != 3 || rc.count(ids[0]) != 3 {
		t.Fatalf("attempts = %d, received = %d; want 3 and 3", len(a), rc.count(ids[0]))
	}
	for i, at := range a {
		if int(at.AttemptNumber) != i+1 || *at.ErrorKind != "http_status" {
			t.Fatalf("attempt %d = %+v", i, at)
		}
	}
}

// Redis loses the queued tasks (flush). The reaper notices the queued rows
// were never claimed and makes them due again; they are delivered.
func TestPipelineRedisFlushedWhileQueuedIsRecovered(t *testing.T) {
	t.Parallel()
	rdb := testRedis(t)
	e := newEnv(t)
	q := testQueue(t, rdb)
	rc := newReceiver(t, e, nil)
	ids := e.ingestN(4)

	client := asynq.NewClientFromRedisClient(rdb)
	if n, err := e.dispatcher(client, DispatcherOptions{Queue: q}).RunOnce(e.ctx); err != nil || n != len(ids) {
		t.Fatalf("dispatch = %d, %v", n, err)
	}
	if err := asynq.NewInspectorFromRedisClient(rdb).DeleteQueue(q, true); err != nil {
		t.Fatalf("flush queue: %v", err)
	}

	startWorker(t, rdb, q, e.handler(time.Minute), 4)
	runLoop(t, e.dispatcher(client, DispatcherOptions{Interval: 20 * time.Millisecond, Queue: q}).Run)
	runLoop(t, e.reaper(ReaperOptions{Interval: 50 * time.Millisecond, DispatchLease: 200 * time.Millisecond}).Run)

	waitFor(t, 20*time.Second, "flushed deliveries to be recovered", func() bool {
		return e.count(`SELECT count(*) FROM deliveries WHERE status = 'succeeded'`) == len(ids)
	})
	for _, id := range ids {
		if rc.count(id) != 1 || e.get(id).AttemptCount != 1 {
			t.Errorf("delivery %s: received %d times, attempt_count %d", id.String(), rc.count(id), e.get(id).AttemptCount)
		}
	}
}

// Two complete worker processes (handlers + dispatcher + reaper each) on the
// same database and queue.
func TestPipelineTwoWorkerProcesses(t *testing.T) {
	t.Parallel()
	rdb := testRedis(t)
	e := newEnv(t)
	q := testQueue(t, rdb)
	rc := newReceiver(t, e, nil)
	ids := e.ingestN(30)

	for range 2 {
		startWorker(t, rdb, q, e.handler(time.Minute), 4)
		runLoop(t, e.dispatcher(asynq.NewClientFromRedisClient(rdb), DispatcherOptions{Interval: 10 * time.Millisecond, BatchSize: 5, Queue: q}).Run)
		runLoop(t, e.reaper(ReaperOptions{Interval: 50 * time.Millisecond}).Run)
	}

	waitFor(t, 30*time.Second, "all deliveries", func() bool {
		return e.count(`SELECT count(*) FROM deliveries WHERE status = 'succeeded'`) == len(ids)
	})
	for _, id := range ids {
		if rc.count(id) != 1 {
			t.Errorf("delivery %s received %d times", id.String(), rc.count(id))
		}
	}
	if got := e.count(`SELECT count(*) FROM delivery_attempts`); got != len(ids) {
		t.Fatalf("attempt rows = %d, want exactly one per delivery", got)
	}
	if got := e.count(`SELECT count(*) FROM deliveries WHERE attempt_count <> 1`); got != 0 {
		t.Fatalf("%d deliveries needed more than one attempt", got)
	}
}
