package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestServeGracefulShutdown checks that cancelling the context lets an
// in-flight request finish, then makes serve return nil.
func TestServeGracefulShutdown(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		time.Sleep(300 * time.Millisecond)
		_, _ = io.WriteString(w, "done")
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- serve(ctx, srv, ln, 5*time.Second, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	}()

	type result struct {
		body string
		err  error
	}
	reqDone := make(chan result, 1)
	go func() {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+ln.Addr().String()+"/slow", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			reqDone <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		reqDone <- result{body: string(b), err: err}
	}()

	<-started
	cancel() // simulate SIGTERM while the request is in flight

	select {
	case r := <-reqDone:
		if r.err != nil || r.body != "done" {
			t.Fatalf("in-flight request was not drained: body=%q err=%v", r.body, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request did not complete")
	}

	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("serve returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after shutdown")
	}

	// New connections are refused after shutdown.
	if conn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second); err == nil {
		_ = conn.Close()
		t.Error("listener still accepting after shutdown")
	}
}
