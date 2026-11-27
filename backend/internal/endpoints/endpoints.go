// Package endpoints implements endpoint management: validation, SSRF checks,
// encrypted signing secrets, and tenant-scoped persistence.
package endpoints

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/ssrf"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
	"github.com/jeevss131436/Hookrelay/backend/internal/validation"
)

// Limits (docs/ARCHITECTURE.md §4, pitfall 11).
const (
	MaxEndpointsPerProject = 100
	MaxEventTypes          = 50
	MaxDescriptionLen      = 1024
	DefaultPageSize        = 50
	MaxPageSize            = 100
)

// EventTypePattern is the allowed event type format; it mirrors the CHECK on
// events.event_type so a filter can only name types that can actually exist.
var EventTypePattern = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,100}$`)

var (
	// ErrNotFound means the endpoint does not exist in this project (or was
	// deleted). Endpoints of other projects are indistinguishable from
	// missing ones.
	ErrNotFound = errors.New("endpoint not found")
	// ErrLimitReached means the project already has MaxEndpointsPerProject.
	ErrLimitReached = errors.New("endpoint limit reached")
)

// Endpoint is the API-facing view. It never includes the secret.
type Endpoint struct {
	ID          pgtype.UUID
	ProjectID   pgtype.UUID
	URL         string
	Description string
	EventTypes  []string
	Enabled     bool
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// CreateParams are the inputs for Create. EventTypes nil or empty = all.
type CreateParams struct {
	URL         string
	Description string
	EventTypes  []string
	Enabled     *bool // nil = true
}

// UpdateParams are the inputs for Update. Nil fields are left unchanged.
type UpdateParams struct {
	URL         *string
	Description *string
	EventTypes  *[]string // non-nil empty slice = subscribe to all
	Enabled     *bool
}

// Page is one page of a listing. NextCursor is nil on the last page.
type Page struct {
	Endpoints  []Endpoint
	NextCursor *Cursor
}

// Cursor marks a position in the (created_at DESC, id DESC) ordering.
type Cursor struct {
	CreatedAt time.Time
	ID        pgtype.UUID
}

// Service manages endpoints.
type Service struct {
	store  *store.Store
	cipher *secrets.Cipher
	policy ssrf.Policy
}

// NewService returns a Service. policy controls URL/SSRF validation.
func NewService(st *store.Store, cipher *secrets.Cipher, policy ssrf.Policy) *Service {
	return &Service{store: st, cipher: cipher, policy: policy}
}

// secretAAD binds a signing secret's ciphertext to its project and endpoint,
// so a ciphertext copied onto another endpoint row fails to decrypt.
func secretAAD(projectID, endpointID pgtype.UUID) []byte {
	aad := make([]byte, 0, len("hookrelay/endpoint-secret/v1/")+32)
	aad = append(aad, "hookrelay/endpoint-secret/v1/"...)
	aad = append(aad, projectID.Bytes[:]...)
	return append(aad, endpointID.Bytes[:]...)
}

// Create validates the input (including a DNS-based SSRF check, done before
// any transaction), generates and encrypts a signing secret bound to a new
// endpoint ID, and inserts the endpoint. It returns the plaintext secret
// exactly once.
func (s *Service) Create(ctx context.Context, projectID pgtype.UUID, p CreateParams) (Endpoint, string, error) {
	var vb validation.Builder
	url, err := s.policy.ValidateURL(ctx, p.URL)
	if err != nil {
		vb.Add("url", urlProblem(err))
	}
	validateDescription(&vb, p.Description)
	eventTypes := normalizeEventTypes(&vb, p.EventTypes)
	if verr := vb.Err(); verr != nil {
		return Endpoint{}, "", verr
	}
	enabled := true
	if p.Enabled != nil {
		enabled = *p.Enabled
	}

	id := store.NewUUID()
	secret := secrets.GenerateSigningSecret()
	ciphertext := s.cipher.Encrypt([]byte(secret), secretAAD(projectID, id))

	var row sqlcgen.CreateEndpointRow
	err = s.store.InTx(ctx, func(q *sqlcgen.Queries) error {
		if _, lockErr := q.LockProject(ctx, projectID); lockErr != nil {
			return fmt.Errorf("lock project: %w", lockErr)
		}
		n, countErr := q.CountActiveEndpoints(ctx, projectID)
		if countErr != nil {
			return fmt.Errorf("count endpoints: %w", countErr)
		}
		if n >= MaxEndpointsPerProject {
			return ErrLimitReached
		}
		var insertErr error
		row, insertErr = q.CreateEndpoint(ctx, sqlcgen.CreateEndpointParams{
			ID:               id,
			ProjectID:        projectID,
			URL:              url,
			Description:      p.Description,
			EventTypes:       eventTypes,
			SecretCiphertext: ciphertext,
			Enabled:          enabled,
		})
		if insertErr != nil {
			return fmt.Errorf("insert endpoint: %w", insertErr)
		}
		return nil
	})
	if err != nil {
		return Endpoint{}, "", err
	}
	return Endpoint(row), secret, nil
}

// Get returns one endpoint of the project.
func (s *Service) Get(ctx context.Context, projectID, id pgtype.UUID) (Endpoint, error) {
	row, err := s.store.GetEndpoint(ctx, sqlcgen.GetEndpointParams{ProjectID: projectID, ID: id})
	if store.IsNotFound(err) {
		return Endpoint{}, ErrNotFound
	}
	if err != nil {
		return Endpoint{}, fmt.Errorf("get endpoint: %w", err)
	}
	return Endpoint(row), nil
}

// List returns a page of the project's endpoints, newest first.
func (s *Service) List(ctx context.Context, projectID pgtype.UUID, after *Cursor, limit int) (Page, error) {
	if limit <= 0 {
		limit = DefaultPageSize
	}
	limit = min(limit, MaxPageSize)
	params := sqlcgen.ListEndpointsParams{ProjectID: projectID, RowLimit: int32(limit) + 1} //nolint:gosec // limit ≤ MaxPageSize
	if after != nil {
		params.CursorCreatedAt = &after.CreatedAt
		params.CursorID = after.ID
	}
	rows, err := s.store.ListEndpoints(ctx, params)
	if err != nil {
		return Page{}, fmt.Errorf("list endpoints: %w", err)
	}
	page := Page{Endpoints: make([]Endpoint, 0, min(len(rows), limit))}
	for i, r := range rows {
		if i == limit {
			last := page.Endpoints[limit-1]
			page.NextCursor = &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
			break
		}
		page.Endpoints = append(page.Endpoints, Endpoint(r))
	}
	return page, nil
}

// Update changes the given fields. A changed URL is re-validated (SSRF).
// Disabling an endpoint cancels its deliveries that have not been dispatched.
func (s *Service) Update(ctx context.Context, projectID, id pgtype.UUID, p UpdateParams) (Endpoint, error) {
	var vb validation.Builder
	params := sqlcgen.UpdateEndpointParams{ProjectID: projectID, ID: id, Description: p.Description, Enabled: p.Enabled}
	if p.URL != nil {
		url, err := s.policy.ValidateURL(ctx, *p.URL)
		if err != nil {
			vb.Add("url", urlProblem(err))
		}
		params.URL = &url
	}
	if p.Description != nil {
		validateDescription(&vb, *p.Description)
	}
	if p.EventTypes != nil {
		params.EventTypes = normalizeEventTypes(&vb, *p.EventTypes)
	}
	if err := vb.Err(); err != nil {
		return Endpoint{}, err
	}

	var row sqlcgen.UpdateEndpointRow
	err := s.store.InTx(ctx, func(q *sqlcgen.Queries) error {
		var err error
		row, err = q.UpdateEndpoint(ctx, params)
		if store.IsNotFound(err) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("update endpoint: %w", err)
		}
		if p.Enabled != nil && !*p.Enabled {
			if _, err := q.CancelPendingDeliveriesForEndpoint(ctx, sqlcgen.CancelPendingDeliveriesForEndpointParams{
				ProjectID: projectID, EndpointID: id,
			}); err != nil {
				return fmt.Errorf("cancel pending deliveries: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return Endpoint{}, err
	}
	return Endpoint(row), nil
}

// Delete soft-deletes the endpoint (history is kept) and cancels its
// undispatched deliveries, atomically.
func (s *Service) Delete(ctx context.Context, projectID, id pgtype.UUID) error {
	return s.store.InTx(ctx, func(q *sqlcgen.Queries) error {
		n, err := q.SoftDeleteEndpoint(ctx, sqlcgen.SoftDeleteEndpointParams{ProjectID: projectID, ID: id})
		if err != nil {
			return fmt.Errorf("delete endpoint: %w", err)
		}
		if n == 0 {
			return ErrNotFound
		}
		if _, err := q.CancelPendingDeliveriesForEndpoint(ctx, sqlcgen.CancelPendingDeliveriesForEndpointParams{
			ProjectID: projectID, EndpointID: id,
		}); err != nil {
			return fmt.Errorf("cancel pending deliveries: %w", err)
		}
		return nil
	})
}

// Secret decrypts and returns the endpoint's signing secret. Decryption
// verifies that the ciphertext belongs to exactly this project and endpoint.
func (s *Service) Secret(ctx context.Context, projectID, id pgtype.UUID) (string, error) {
	ct, err := s.store.GetEndpointSecret(ctx, sqlcgen.GetEndpointSecretParams{ProjectID: projectID, ID: id})
	if store.IsNotFound(err) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("get endpoint secret: %w", err)
	}
	pt, err := s.cipher.Decrypt(ct, secretAAD(projectID, id))
	if err != nil {
		// Wrong key, tampering, or a ciphertext moved from another row.
		return "", fmt.Errorf("decrypt endpoint secret: %w", err)
	}
	return string(pt), nil
}

func urlProblem(err error) string {
	msg := err.Error()
	if errors.Is(err, ssrf.ErrInvalidURL) {
		msg = msg[len(ssrf.ErrInvalidURL.Error())+2:] // drop the "invalid endpoint url: " prefix
	}
	return msg
}

func validateDescription(vb *validation.Builder, d string) {
	if utf8.RuneCountInString(d) > MaxDescriptionLen {
		vb.Add("description", fmt.Sprintf("must be at most %d characters", MaxDescriptionLen))
	}
}

// normalizeEventTypes validates and de-duplicates (preserving order). It
// always returns a non-nil slice because endpoints.event_types is NOT NULL.
func normalizeEventTypes(vb *validation.Builder, in []string) []string {
	if len(in) > MaxEventTypes {
		vb.Add("event_types", fmt.Sprintf("must contain at most %d entries", MaxEventTypes))
		return []string{}
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, t := range in {
		if !EventTypePattern.MatchString(t) {
			vb.Add("event_types", fmt.Sprintf("%q is not a valid event type (1-100 of a-z A-Z 0-9 . _ -)", t))
			continue
		}
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}
