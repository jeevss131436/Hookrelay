// Package httpapi contains the HTTP router, middleware and handlers for the
// HookRelay API process.
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// NewRouter wires middleware and routes. checks are the dependencies probed
// by /readyz.
func NewRouter(log *slog.Logger, checks []Check) http.Handler {
	r := chi.NewRouter()

	// RequestID runs first so every later log line and error carries the ID;
	// Recoverer runs inside Logger so a recovered panic is logged as a 500.
	r.Use(RequestID)
	r.Use(Logger(log))
	r.Use(Recoverer(log))

	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "route not found")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	})

	r.Get("/healthz", handleHealthz)
	r.Get("/readyz", handleReadyz(log, checks))

	return r
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // the client may have gone away; nothing useful to do.
}

// errorBody is the error envelope from ARCHITECTURE.md §5.
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message}})
}
