package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/jeevss131436/Hookrelay/backend/internal/endpoints"
)

type endpointResponse struct {
	ID          string   `json:"id"`
	URL         string   `json:"url"`
	Description string   `json:"description"`
	EventTypes  []string `json:"event_types"`
	Enabled     bool     `json:"enabled"`
	CreatedAt   string   `json:"created_at"`
	UpdatedAt   string   `json:"updated_at"`
}

func toEndpointResponse(e endpoints.Endpoint) endpointResponse {
	eventTypes := e.EventTypes
	if eventTypes == nil {
		eventTypes = []string{}
	}
	return endpointResponse{
		ID: e.ID.String(), URL: e.URL, Description: e.Description, EventTypes: eventTypes,
		Enabled: e.Enabled, CreatedAt: formatTime(e.CreatedAt), UpdatedAt: formatTime(e.UpdatedAt),
	}
}

type createEndpointRequest struct {
	URL         string   `json:"url"`
	Description string   `json:"description"`
	EventTypes  []string `json:"event_types"`
	Enabled     *bool    `json:"enabled"`
}

type createEndpointResponse struct {
	endpointResponse
	// Secret is returned on creation so the receiver can be configured
	// immediately; it can be fetched again from GET /v1/endpoints/{id}/secret.
	Secret string `json:"secret"`
}

func (a *API) handleCreateEndpoint(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	var req createEndpointRequest
	if aerr := decodeJSON(w, r, maxEndpointBodyBytes, &req); aerr != nil {
		a.writeServiceError(w, r, "decode", aerr)
		return
	}
	ep, secret, err := a.endpoints.Create(r.Context(), p.ProjectID, endpoints.CreateParams{
		URL: req.URL, Description: req.Description, EventTypes: req.EventTypes, Enabled: req.Enabled,
	})
	if err != nil {
		a.writeServiceError(w, r, "create endpoint", err)
		return
	}
	w.Header().Set("Location", "/v1/endpoints/"+ep.ID.String())
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusCreated, createEndpointResponse{endpointResponse: toEndpointResponse(ep), Secret: secret})
}

type listEndpointsResponse struct {
	Data       []endpointResponse `json:"data"`
	NextCursor *string            `json:"next_cursor"`
}

func (a *API) handleListEndpoints(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	limit, err := parseLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_parameter", err.Error())
		return
	}
	cursor, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_parameter", "cursor is invalid")
		return
	}
	page, err := a.endpoints.List(r.Context(), p.ProjectID, cursor, limit)
	if err != nil {
		a.writeServiceError(w, r, "list endpoints", err)
		return
	}
	resp := listEndpointsResponse{Data: make([]endpointResponse, len(page.Endpoints)), NextCursor: encodeCursor(page.NextCursor)}
	for i, e := range page.Endpoints {
		resp.Data[i] = toEndpointResponse(e)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *API) handleGetEndpoint(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	id, ok := parseID(chi.URLParam(r, "id"))
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	ep, err := a.endpoints.Get(r.Context(), p.ProjectID, id)
	if err != nil {
		a.writeServiceError(w, r, "get endpoint", err)
		return
	}
	writeJSON(w, http.StatusOK, toEndpointResponse(ep))
}

type updateEndpointRequest struct {
	URL         *string   `json:"url"`
	Description *string   `json:"description"`
	EventTypes  *[]string `json:"event_types"`
	Enabled     *bool     `json:"enabled"`
}

func (a *API) handleUpdateEndpoint(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	id, ok := parseID(chi.URLParam(r, "id"))
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	var req updateEndpointRequest
	if aerr := decodeJSON(w, r, maxEndpointBodyBytes, &req); aerr != nil {
		a.writeServiceError(w, r, "decode", aerr)
		return
	}
	ep, err := a.endpoints.Update(r.Context(), p.ProjectID, id, endpoints.UpdateParams{
		URL: req.URL, Description: req.Description, EventTypes: req.EventTypes, Enabled: req.Enabled,
	})
	if err != nil {
		a.writeServiceError(w, r, "update endpoint", err)
		return
	}
	writeJSON(w, http.StatusOK, toEndpointResponse(ep))
}

func (a *API) handleDeleteEndpoint(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	id, ok := parseID(chi.URLParam(r, "id"))
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	if err := a.endpoints.Delete(r.Context(), p.ProjectID, id); err != nil {
		a.writeServiceError(w, r, "delete endpoint", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) handleGetEndpointSecret(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	id, ok := parseID(chi.URLParam(r, "id"))
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "endpoint not found")
		return
	}
	secret, err := a.endpoints.Secret(r.Context(), p.ProjectID, id)
	if err != nil {
		a.writeServiceError(w, r, "get endpoint secret", err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]string{"secret": secret})
}
