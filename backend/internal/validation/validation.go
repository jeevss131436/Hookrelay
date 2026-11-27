// Package validation collects field-level input errors so services can report
// all problems at once and the HTTP layer can render them as a 422.
package validation

import (
	"sort"
	"strings"
)

// Error maps request field names to human-readable problems.
type Error struct {
	Fields map[string]string
}

func (e *Error) Error() string {
	keys := make([]string, 0, len(e.Fields))
	for k := range e.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + ": " + e.Fields[k]
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

// Builder accumulates field errors.
type Builder struct {
	fields map[string]string
}

// Add records a problem for field; the first problem per field wins.
func (b *Builder) Add(field, problem string) {
	if b.fields == nil {
		b.fields = make(map[string]string)
	}
	if _, exists := b.fields[field]; !exists {
		b.fields[field] = problem
	}
}

// Err returns an *Error if any problem was recorded, else nil.
func (b *Builder) Err() error {
	if len(b.fields) == 0 {
		return nil
	}
	return &Error{Fields: b.fields}
}

// Field returns a single-field *Error.
func Field(field, problem string) error {
	return &Error{Fields: map[string]string{field: problem}}
}
