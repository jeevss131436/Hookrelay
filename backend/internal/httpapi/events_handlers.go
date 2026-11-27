package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/jeevss131436/Hookrelay/backend/internal/ingest"
)

type createEventRequest struct {
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"` // exact bytes as sent (ARCHITECTURE D5)
}

type eventResponse struct {
	ID                string  `json:"id"`
	EventType         string  `json:"event_type"`
	IdempotencyKey    *string `json:"idempotency_key"`
	CreatedAt         string  `json:"created_at"`
	DeliveriesCreated int64   `json:"deliveries_created"`
}

// IdempotencyKeyHeader is the request header carrying the idempotency key.
const IdempotencyKeyHeader = "Idempotency-Key"

// handleCreateEvent ingests an event. The response is written only after the
// ingest transaction has committed:
//   - 202 Accepted: a new event was stored and its deliveries created.
//   - 200 OK + "Idempotent-Replayed: true": the Idempotency-Key matched an
//     identical earlier request; the original event is returned.
func (a *API) handleCreateEvent(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())

	var idemKey *string
	if values, present := r.Header[IdempotencyKeyHeader]; present {
		if len(values) != 1 {
			writeError(w, http.StatusBadRequest, "invalid_header", "send exactly one Idempotency-Key header")
			return
		}
		k := values[0]
		idemKey = &k
	}

	var req createEventRequest
	if aerr := decodeJSON(w, r, maxEventBodyBytes, &req); aerr != nil {
		a.writeServiceError(w, r, "decode", aerr)
		return
	}
	if len(req.Payload) > ingest.MaxPayloadBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large",
			fmt.Sprintf("payload must be at most %d bytes", ingest.MaxPayloadBytes))
		return
	}

	res, err := a.ingest.Ingest(r.Context(), p.ProjectID, ingest.Request{
		EventType: req.EventType, Payload: req.Payload, IdempotencyKey: idemKey,
	})
	if err != nil {
		a.writeServiceError(w, r, "ingest event", err)
		return
	}

	status := http.StatusAccepted
	if res.Replayed {
		status = http.StatusOK
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, status, eventResponse{
		ID: res.EventID.String(), EventType: res.EventType, IdempotencyKey: res.IdempotencyKey,
		CreatedAt: formatTime(res.CreatedAt), DeliveriesCreated: res.DeliveriesCreated,
	})
}
