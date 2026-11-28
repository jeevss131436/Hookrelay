package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/jeevss131436/Hookrelay/backend/internal/endpoints"
	"github.com/jeevss131436/Hookrelay/backend/internal/history"
	"github.com/jeevss131436/Hookrelay/backend/internal/ingest"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/validation"
)

// API serves the authenticated /v1 routes (docs/API.md).
type API struct {
	log       *slog.Logger
	store     *store.Store
	endpoints *endpoints.Service
	ingest    *ingest.Service
	history   *history.Service
}

// NewAPI builds the /v1 API. It has no Redis dependency: ingestion writes only
// to PostgreSQL (transactional outbox), and a manual retry only makes the
// delivery due again for the dispatcher.
func NewAPI(log *slog.Logger, st *store.Store, eps *endpoints.Service, ing *ingest.Service, hist *history.Service) *API {
	return &API{log: log, store: st, endpoints: eps, ingest: ing, history: hist}
}

// Request body limits.
const (
	maxEndpointBodyBytes = 64 << 10
	// The ingest body holds the payload plus a small envelope; the payload
	// itself is limited to ingest.MaxPayloadBytes separately.
	maxEventBodyBytes = ingest.MaxPayloadBytes + 16<<10
)

// Routes returns the /v1 handler. Every route requires an API key.
func (a *API) Routes() http.Handler {
	r := chi.NewRouter()
	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "route not found")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	})

	r.Group(func(r chi.Router) {
		r.Use(a.authenticate)

		r.Post("/events", a.handleCreateEvent)
		r.Get("/events", a.handleListEvents)
		r.Get("/events/{id}", a.handleGetEvent)

		r.Route("/deliveries", func(r chi.Router) {
			r.Get("/", a.handleListDeliveries)
			r.Get("/{id}", a.handleGetDelivery)
			r.Get("/{id}/attempts", a.handleListAttempts)
			r.Post("/{id}/retry", a.handleRetryDelivery)
		})

		r.Get("/stats", a.handleStats)

		r.Route("/endpoints", func(r chi.Router) {
			r.Post("/", a.handleCreateEndpoint)
			r.Get("/", a.handleListEndpoints)
			r.Get("/{id}", a.handleGetEndpoint)
			r.Patch("/{id}", a.handleUpdateEndpoint)
			r.Delete("/{id}", a.handleDeleteEndpoint)
			r.Get("/{id}/secret", a.handleGetEndpointSecret)
		})
	})
	return r
}

// writeServiceError maps service errors to HTTP responses. Unknown errors
// become a generic 500; their details are logged, never returned.
func (a *API) writeServiceError(w http.ResponseWriter, r *http.Request, op string, err error) {
	var (
		ve *validation.Error
		ae *apiError
		nr *history.NotRetryableError
	)
	switch {
	case errors.As(err, &ae):
		writeJSON(w, ae.status, errorBody{Error: errorDetail{Code: ae.code, Message: ae.message, Details: ae.details}})
	case errors.As(err, &ve):
		writeJSON(w, http.StatusUnprocessableEntity, errorBody{Error: errorDetail{
			Code: "validation_failed", Message: "request has invalid fields", Details: ve.Fields,
		}})
	case errors.Is(err, endpoints.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
	case errors.Is(err, endpoints.ErrLimitReached):
		writeError(w, http.StatusConflict, "endpoint_limit_reached",
			"this project already has the maximum number of endpoints")
	case errors.Is(err, history.ErrEventNotFound):
		writeError(w, http.StatusNotFound, "not_found", "event not found")
	case errors.Is(err, history.ErrDeliveryNotFound):
		writeError(w, http.StatusNotFound, "not_found", "delivery not found")
	case errors.As(err, &nr):
		code := "delivery_not_retryable"
		if nr.Reason == history.ReasonEndpointInactive {
			code = "endpoint_inactive"
		}
		writeError(w, http.StatusConflict, code, nr.Error())
	case errors.Is(err, history.ErrRetryRace):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, ingest.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_key_reuse",
			"this Idempotency-Key was already used with a different event_type or payload")
	default:
		a.internalError(w, r, op, err)
	}
}

func (a *API) internalError(w http.ResponseWriter, r *http.Request, op string, err error) {
	a.log.ErrorContext(r.Context(), "request failed",
		slog.String("request_id", RequestIDFromContext(r.Context())),
		slog.String("op", op),
		slog.String("error", err.Error()))
	writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
}
