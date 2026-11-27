package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jeevss131436/Hookrelay/backend/internal/endpoints"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
)

// apiError is an error with a fixed HTTP status, code and message that is
// safe to show to clients.
type apiError struct {
	status  int
	code    string
	message string
	details map[string]string
}

func (e *apiError) Error() string { return e.code + ": " + e.message }

func errBadRequest(code, msg string) *apiError {
	return &apiError{status: http.StatusBadRequest, code: code, message: msg}
}

// decodeJSON reads a single JSON object from the request body into dst.
// Unknown fields are rejected so typos (e.g. "event-type") fail loudly.
func decodeJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) *apiError {
	ct := r.Header.Get("Content-Type")
	if mt, _, err := mime.ParseMediaType(ct); err != nil || mt != "application/json" {
		return &apiError{status: http.StatusUnsupportedMediaType, code: "unsupported_media_type",
			message: "Content-Type must be application/json"}
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		var (
			maxErr    *http.MaxBytesError
			syntaxErr *json.SyntaxError
			typeErr   *json.UnmarshalTypeError
		)
		switch {
		case errors.As(err, &maxErr):
			return &apiError{status: http.StatusRequestEntityTooLarge, code: "payload_too_large",
				message: fmt.Sprintf("request body must be at most %d bytes", maxBytes)}
		case errors.Is(err, io.EOF):
			return errBadRequest("invalid_json", "request body is empty")
		case errors.As(err, &syntaxErr), errors.Is(err, io.ErrUnexpectedEOF):
			return errBadRequest("invalid_json", "request body is not valid JSON")
		case errors.As(err, &typeErr):
			field := typeErr.Field
			if field == "" {
				return errBadRequest("invalid_json", "request body must be a JSON object")
			}
			return &apiError{status: http.StatusUnprocessableEntity, code: "validation_failed",
				message: "request has invalid fields",
				details: map[string]string{field: "must be of type " + typeErr.Type.String()}}
		case strings.HasPrefix(err.Error(), "json: unknown field "):
			return errBadRequest("invalid_json", strings.TrimPrefix(err.Error(), "json: "))
		default:
			return errBadRequest("invalid_json", "request body is not valid JSON")
		}
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return &apiError{status: http.StatusRequestEntityTooLarge, code: "payload_too_large",
				message: fmt.Sprintf("request body must be at most %d bytes", maxBytes)}
		}
		return errBadRequest("invalid_json", "request body must contain a single JSON object")
	}
	return nil
}

// parseID parses a path UUID. Malformed IDs cannot exist, so they are
// reported as not found, exactly like IDs of other projects.
func parseID(s string) (pgtype.UUID, bool) {
	id, err := store.ParseUUID(s)
	return id, err == nil
}

// Cursors are opaque to clients: base64url("<RFC3339Nano created_at>|<uuid>").
func encodeCursor(c *endpoints.Cursor) *string {
	if c == nil {
		return nil
	}
	s := base64.RawURLEncoding.EncodeToString([]byte(c.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + c.ID.String()))
	return &s
}

func decodeCursor(s string) (*endpoints.Cursor, error) {
	if s == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, errors.New("invalid cursor")
	}
	ts, idStr, ok := strings.Cut(string(raw), "|")
	if !ok {
		return nil, errors.New("invalid cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return nil, errors.New("invalid cursor")
	}
	id, err := store.ParseUUID(idStr)
	if err != nil {
		return nil, errors.New("invalid cursor")
	}
	return &endpoints.Cursor{CreatedAt: t, ID: id}, nil
}

func parseLimit(s string) (int, error) {
	if s == "" {
		return endpoints.DefaultPageSize, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > endpoints.MaxPageSize {
		return 0, fmt.Errorf("limit must be an integer between 1 and %d", endpoints.MaxPageSize)
	}
	return n, nil
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
