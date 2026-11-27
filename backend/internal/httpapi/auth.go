package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
)

// Principal is the authenticated caller. Every /v1 handler scopes its work to
// ProjectID; there is no way to name another project in a request.
type Principal struct {
	ProjectID pgtype.UUID
	APIKeyID  pgtype.UUID
}

type principalKey struct{}

// principalFrom returns the authenticated principal. It panics if called on a
// route that is not behind authenticate, which is a programming error.
func principalFrom(ctx context.Context) Principal {
	p, ok := ctx.Value(principalKey{}).(Principal)
	if !ok {
		panic("httpapi: handler requires authentication but no principal in context")
	}
	return p
}

// lastUsedResolution throttles last_used_at writes to one per key per minute.
const lastUsedResolution = time.Minute

func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="hookrelay"`)
	writeError(w, http.StatusUnauthorized, "unauthorized", "missing, invalid or revoked API key")
}

// authenticate resolves "Authorization: Bearer hr_..." to a project. The
// presented key is hashed before the database lookup and is never logged or
// echoed. Missing, malformed, unknown and revoked keys all get the same 401.
func (a *API) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") {
			writeUnauthorized(w)
			return
		}
		hash, err := secrets.HashAPIKey(strings.TrimSpace(token))
		if err != nil {
			writeUnauthorized(w)
			return
		}
		key, err := a.store.GetActiveAPIKeyByHash(r.Context(), hash)
		if store.IsNotFound(err) {
			writeUnauthorized(w)
			return
		}
		if err != nil {
			a.internalError(w, r, "authenticate api key", err)
			return
		}

		if key.LastUsedAt == nil || time.Since(*key.LastUsedAt) > lastUsedResolution {
			if err := a.store.TouchAPIKeyLastUsed(r.Context(), key.ID); err != nil {
				a.log.WarnContext(r.Context(), "update api key last_used_at failed",
					slog.String("request_id", RequestIDFromContext(r.Context())), slog.String("error", err.Error()))
			}
		}

		addLogAttrs(r.Context(),
			slog.String("project_id", key.ProjectID.String()),
			slog.String("api_key_id", key.ID.String()))
		ctx := context.WithValue(r.Context(), principalKey{}, Principal{ProjectID: key.ProjectID, APIKeyID: key.ID})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
