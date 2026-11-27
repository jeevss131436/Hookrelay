package store

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestErrorClassifiers(t *testing.T) {
	unique := fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "23505", ConstraintName: "api_keys_key_hash_key"})
	fk := &pgconn.PgError{Code: "23503", ConstraintName: "deliveries_event_fk"}
	check := &pgconn.PgError{Code: "23514", ConstraintName: "deliveries_attempts_check"}

	if !IsUniqueViolation(unique, "") || !IsUniqueViolation(unique, "api_keys_key_hash_key") {
		t.Error("unique violation not detected through wrapping")
	}
	if IsUniqueViolation(unique, "other_constraint") {
		t.Error("constraint name not checked")
	}
	if IsUniqueViolation(fk, "") || !IsForeignKeyViolation(fk, "deliveries_event_fk") {
		t.Error("foreign key classification wrong")
	}
	if !IsCheckViolation(check, "") || IsCheckViolation(unique, "") {
		t.Error("check classification wrong")
	}
	if IsUniqueViolation(errors.New("plain"), "") || IsUniqueViolation(nil, "") {
		t.Error("non-pg errors must not classify")
	}
	if !IsNotFound(fmt.Errorf("x: %w", pgx.ErrNoRows)) || IsNotFound(errors.New("x")) {
		t.Error("IsNotFound wrong")
	}
}

func TestNewUUID(t *testing.T) {
	a, b := NewUUID(), NewUUID()
	if !a.Valid || a == b {
		t.Fatal("NewUUID must return distinct valid UUIDs")
	}
	s := a.String()
	if len(s) != 36 || s[14] != '4' || !strings.ContainsRune("89ab", rune(s[19])) {
		t.Errorf("not a v4 UUID: %s", s)
	}
	if parsed, err := ParseUUID(s); err != nil || parsed != a {
		t.Errorf("round trip failed: %v", err)
	}
}

func TestParseUUID(t *testing.T) {
	id, err := ParseUUID("6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b")
	if err != nil || !id.Valid || id.String() != "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b" {
		t.Fatalf("ParseUUID = %v, %v", id, err)
	}
	for _, bad := range []string{"", "not-a-uuid", "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4"} {
		if _, err := ParseUUID(bad); err == nil {
			t.Errorf("ParseUUID(%q) succeeded", bad)
		}
	}
}
