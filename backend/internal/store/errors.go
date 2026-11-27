package store

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// PostgreSQL SQLSTATE codes we branch on.
const (
	codeUniqueViolation     = "23505"
	codeForeignKeyViolation = "23503"
	codeCheckViolation      = "23514"
)

// IsNotFound reports whether err means a :one query matched no rows.
func IsNotFound(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

// IsUniqueViolation reports whether err is a unique-constraint violation.
// If constraint is non-empty, it must also match the constraint name.
func IsUniqueViolation(err error, constraint string) bool {
	return isPgError(err, codeUniqueViolation, constraint)
}

// IsForeignKeyViolation reports whether err is a foreign-key violation.
// If constraint is non-empty, it must also match the constraint name.
func IsForeignKeyViolation(err error, constraint string) bool {
	return isPgError(err, codeForeignKeyViolation, constraint)
}

// IsCheckViolation reports whether err is a CHECK-constraint violation.
// If constraint is non-empty, it must also match the constraint name.
func IsCheckViolation(err error, constraint string) bool {
	return isPgError(err, codeCheckViolation, constraint)
}

func isPgError(err error, code, constraint string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != code {
		return false
	}
	return constraint == "" || pgErr.ConstraintName == constraint
}

// ParseUUID parses a textual UUID (e.g. from a CLI flag or URL path).
func ParseUUID(s string) (pgtype.UUID, error) {
	var id pgtype.UUID
	if err := id.Scan(s); err != nil {
		return pgtype.UUID{}, fmt.Errorf("invalid UUID %q", s)
	}
	return id, nil
}
