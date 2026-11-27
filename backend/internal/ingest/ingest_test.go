package ingest

import (
	"errors"
	"strings"
	"testing"

	"github.com/jeevss131436/Hookrelay/backend/internal/validation"
)

func ptr(s string) *string { return &s }

func TestValidate(t *testing.T) {
	ok := Request{EventType: "user.created", Payload: []byte(`{"id":1}`)}
	if err := Validate(ok); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	exact := Request{EventType: "a", Payload: []byte(`{"x":"` + strings.Repeat("a", MaxPayloadBytes-8) + `"}`)}
	if len(exact.Payload) != MaxPayloadBytes {
		t.Fatalf("test setup: payload is %d bytes", len(exact.Payload))
	}
	if err := Validate(exact); err != nil {
		t.Fatalf("payload of exactly %d bytes rejected: %v", MaxPayloadBytes, err)
	}

	tests := []struct {
		name  string
		req   Request
		field string
	}{
		{"missing event type", Request{Payload: []byte(`{}`)}, "event_type"},
		{"bad event type", Request{EventType: "user created", Payload: []byte(`{}`)}, "event_type"},
		{"long event type", Request{EventType: strings.Repeat("a", 101), Payload: []byte(`{}`)}, "event_type"},
		{"missing payload", Request{EventType: "a"}, "payload"},
		{"null payload", Request{EventType: "a", Payload: []byte(`null`)}, "payload"},
		{"array payload", Request{EventType: "a", Payload: []byte(`[1]`)}, "payload"},
		{"string payload", Request{EventType: "a", Payload: []byte(`"x"`)}, "payload"},
		{"invalid json payload", Request{EventType: "a", Payload: []byte(`{"a":`)}, "payload"},
		{"too large", Request{EventType: "a", Payload: append(exact.Payload, ' ')}, "payload"},
		{"empty key", Request{EventType: "a", Payload: []byte(`{}`), IdempotencyKey: ptr("")}, "idempotency_key"},
		{"long key", Request{EventType: "a", Payload: []byte(`{}`), IdempotencyKey: ptr(strings.Repeat("k", 256))}, "idempotency_key"},
		{"key with space", Request{EventType: "a", Payload: []byte(`{}`), IdempotencyKey: ptr("order 1")}, "idempotency_key"},
		{"key with unicode", Request{EventType: "a", Payload: []byte(`{}`), IdempotencyKey: ptr("ключ")}, "idempotency_key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.req)
			var ve *validation.Error
			if !errors.As(err, &ve) {
				t.Fatalf("err = %v, want *validation.Error", err)
			}
			if _, ok := ve.Fields[tt.field]; !ok {
				t.Errorf("fields = %v, want %q", ve.Fields, tt.field)
			}
		})
	}
}
