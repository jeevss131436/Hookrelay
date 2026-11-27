// Command mockreceiver runs a local webhook receiver for testing HookRelay
// deliveries. It verifies signatures when MOCKRECEIVER_SECRET (the endpoint's
// whsec_ secret) is set, logs each webhook-id as JSON (so duplicates can be
// counted), and supports ?status=, ?latency_ms= and ?fail_rate= on the URL.
//
//	go run ./cmd/mockreceiver -addr 127.0.0.1:9000
//
// Register http://127.0.0.1:9000/hook as an endpoint (this needs
// HOOKRELAY_ALLOW_PRIVATE_TARGETS=true in development).
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jeevss131436/Hookrelay/backend/internal/mockreceiver"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "listen address")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With(slog.String("cmd", "mockreceiver"))
	secret := os.Getenv("MOCKRECEIVER_SECRET")
	if secret == "" {
		log.Warn("MOCKRECEIVER_SECRET not set: signatures are NOT verified")
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mockreceiver.New(secret, log),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("mockreceiver listening", slog.String("addr", *addr))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}
