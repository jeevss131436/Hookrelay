package dispatch

// Manual retry (Phase 5, history.Service.Retry) against the real dispatcher,
// worker handler and reaper. The HTTP-level retry tests live in
// internal/httpapi; these check that a retried delivery flows through the
// unchanged Phase 3/4 pipeline and that fencing still holds.

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/jeevss131436/Hookrelay/backend/internal/delivery"
	"github.com/jeevss131436/Hookrelay/backend/internal/history"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
)

// A delivery dies, is retried by hand and fails again (back to dead after
// exactly one more attempt), then is retried again and succeeds. The
// dispatcher picks up every retry by itself; history is never rewritten.
func TestPipelineManualRetryOfDeadDelivery(t *testing.T) {
	t.Parallel()
	rdb := testRedis(t)
	e := newEnv(t)
	q := testQueue(t, rdb)
	var healthy atomic.Bool
	rc := newReceiver(t, e, func(_ int, w http.ResponseWriter) bool {
		if healthy.Load() {
			return false // 200
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	})
	ids := e.ingestN(1)
	id := ids[0]
	e.exec(`UPDATE deliveries SET max_attempts = 2 WHERE id = $1`, id)

	startWorker(t, rdb, q, e.handler(time.Minute), 2)
	runLoop(t, e.dispatcher(asynq.NewClientFromRedisClient(rdb), DispatcherOptions{Interval: 20 * time.Millisecond, Queue: q}).Run)
	hist := history.NewService(e.st)
	isDead := func() bool { return e.get(id).Status == sqlcgen.DeliveryStatusDead }

	waitFor(t, 20*time.Second, "delivery to die", isDead)
	if d := e.get(id); d.AttemptCount != 2 {
		t.Fatalf("after dying: %+v", d)
	}

	// Retry 1: the receiver still fails. Exactly one more attempt, then dead.
	r1, err := hist.Retry(e.ctx, e.projectID, id)
	if err != nil {
		t.Fatalf("retry 1: %v", err)
	}
	if r1.Status != sqlcgen.DeliveryStatusPending || r1.AttemptCount != 2 || r1.MaxAttempts != 3 || r1.CompletedAt != nil {
		t.Fatalf("retry 1 result = %+v", r1)
	}
	waitFor(t, 20*time.Second, "retried delivery to die again", func() bool {
		d := e.get(id)
		return d.Status == sqlcgen.DeliveryStatusDead && d.AttemptCount == 3
	})
	time.Sleep(150 * time.Millisecond) // no automatic retry may follow
	if d := e.get(id); d.Status != sqlcgen.DeliveryStatusDead || d.AttemptCount != 3 || len(e.attempts(id)) != 3 {
		t.Fatalf("after failed manual attempt: %+v", d)
	}

	// Retry 2: the receiver recovered.
	healthy.Store(true)
	if _, err := hist.Retry(e.ctx, e.projectID, id); err != nil {
		t.Fatalf("retry 2: %v", err)
	}
	waitFor(t, 20*time.Second, "retried delivery to succeed", func() bool {
		return e.get(id).Status == sqlcgen.DeliveryStatusSucceeded
	})

	d := e.get(id)
	if d.AttemptCount != 4 || d.MaxAttempts != 4 || *d.LastStatusCode != 200 || d.CompletedAt == nil {
		t.Fatalf("final delivery = %+v", d)
	}
	a := e.attempts(id)
	if len(a) != 4 {
		t.Fatalf("attempts = %d, want 4 (history preserved)", len(a))
	}
	for i, at := range a {
		if int(at.AttemptNumber) != i+1 {
			t.Fatalf("attempt %d has number %d", i, at.AttemptNumber)
		}
		wantStatus := int32(503)
		if i == 3 {
			wantStatus = 200
		}
		if at.ResponseStatus == nil || *at.ResponseStatus != wantStatus {
			t.Fatalf("attempt %d = %+v", i+1, at)
		}
	}
	// Same webhook-id (the delivery ID) on every attempt, all signed correctly.
	if rc.count(id) != 4 || rc.badSignatures() != 0 {
		t.Fatalf("receiver saw %d requests for the delivery, %d bad signatures", rc.count(id), rc.badSignatures())
	}

	// A succeeded delivery cannot be retried.
	var nr *history.NotRetryableError
	if _, err := hist.Retry(e.ctx, e.projectID, id); !errors.As(err, &nr) {
		t.Fatalf("retry of succeeded delivery: %v", err)
	}
}

// A worker hangs on the final attempt; the reaper declares it lost (dead).
// The operator retries by hand and the new attempt succeeds. Neither the hung
// worker finishing late, nor a replay of the old task, may touch the new
// state.
func TestStaleWorkerCannotOverwriteManualRetry(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	arrived, release := make(chan struct{}), make(chan struct{})
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			close(arrived)
			<-release
			w.WriteHeader(http.StatusOK) // the hung worker's late success
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	e.endpoint(srv.URL)
	id := e.ingestN(1)[0]
	e.exec(`UPDATE deliveries SET max_attempts = 1 WHERE id = $1`, id)

	fq := newFakeQueue()
	if _, err := e.dispatcher(fq, DispatcherOptions{}).RunOnce(e.ctx); err != nil {
		t.Fatal(err)
	}
	oldTask := fq.payloadFor(id)[0]

	// Attempt 1 hangs with a 1 ms lease.
	hung := delivery.NewHandler(e.st, e.cipher, delivery.NewSender(delivery.SenderOptions{AllowPrivate: true}),
		delivery.HandlerOptions{Lease: time.Millisecond, Backoff: testPolicy}, discardLogger())
	var hungOut delivery.Outcome
	done := make(chan struct{})
	go func() {
		defer close(done)
		var err error
		hungOut, err = hung.Deliver(e.ctx, oldTask)
		if err != nil {
			t.Errorf("hung Deliver: %v", err)
		}
	}()
	<-arrived
	time.Sleep(5 * time.Millisecond)
	if res, err := e.reaper(ReaperOptions{}).RunOnce(e.ctx); err != nil || res.Dead != 1 {
		t.Fatalf("reaper = %+v, %v (want the last attempt declared dead)", res, err)
	}

	// Manual retry, then the dispatcher hands it to the queue again.
	if _, err := history.NewService(e.st).Retry(e.ctx, e.projectID, id); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if n, err := e.dispatcher(fq, DispatcherOptions{}).RunOnce(e.ctx); err != nil || n != 1 {
		t.Fatalf("dispatch after retry = %d, %v", n, err)
	}
	tasks := fq.payloadFor(id)
	newTask := tasks[len(tasks)-1]
	if newTask.AttemptCount != 1 || newTask.QueuedAt.Equal(oldTask.QueuedAt) {
		t.Fatalf("new task = %+v, old = %+v", newTask, oldTask)
	}

	// A replay of the old task claims nothing.
	h := e.handler(time.Minute)
	if out, err := h.Deliver(e.ctx, oldTask); err != nil || out != delivery.OutcomeSkipped {
		t.Fatalf("old task replay = %s, %v; want skipped", out, err)
	}
	// The new task delivers attempt 2.
	if out, err := h.Deliver(e.ctx, newTask); err != nil || out != delivery.OutcomeSucceeded {
		t.Fatalf("new task = %s, %v; want succeeded", out, err)
	}

	// The hung worker finally returns: fenced out, nothing changes.
	close(release)
	<-done
	if hungOut != delivery.OutcomeFencedOut {
		t.Fatalf("hung worker outcome = %s, want fenced_out", hungOut)
	}
	d := e.get(id)
	if d.Status != sqlcgen.DeliveryStatusSucceeded || d.AttemptCount != 2 || d.MaxAttempts != 2 {
		t.Fatalf("delivery = %+v", d)
	}
	a := e.attempts(id)
	if len(a) != 2 || a[0].ErrorKind == nil || *a[0].ErrorKind != "worker_lost" ||
		a[1].ErrorKind != nil || *a[1].ResponseStatus != 200 {
		t.Fatalf("attempts = %+v", a)
	}
}
