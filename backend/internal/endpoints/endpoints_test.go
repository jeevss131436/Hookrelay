package endpoints

import (
	"bytes"
	"strings"
	"testing"

	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/validation"
)

func TestNormalizeEventTypes(t *testing.T) {
	var vb validation.Builder
	got := normalizeEventTypes(&vb, []string{"user.created", "order.paid", "user.created"})
	if vb.Err() != nil || len(got) != 2 || got[0] != "user.created" || got[1] != "order.paid" {
		t.Fatalf("got %v, %v", got, vb.Err())
	}

	vb = validation.Builder{}
	if got := normalizeEventTypes(&vb, nil); got == nil || len(got) != 0 || vb.Err() != nil {
		t.Errorf("nil input must yield a non-nil empty slice, got %#v", got)
	}

	vb = validation.Builder{}
	normalizeEventTypes(&vb, []string{"ok", "has space"})
	if vb.Err() == nil {
		t.Error("invalid event type accepted")
	}

	vb = validation.Builder{}
	many := make([]string, MaxEventTypes+1)
	for i := range many {
		many[i] = "t" + strings.Repeat("x", i%5)
	}
	normalizeEventTypes(&vb, many)
	if vb.Err() == nil {
		t.Error("too many event types accepted")
	}
}

func TestSecretAADBindsProjectAndEndpoint(t *testing.T) {
	p1, p2, e1, e2 := store.NewUUID(), store.NewUUID(), store.NewUUID(), store.NewUUID()
	a := secretAAD(p1, e1)
	for name, other := range map[string][]byte{
		"other endpoint": secretAAD(p1, e2),
		"other project":  secretAAD(p2, e1),
		"swapped":        secretAAD(e1, p1),
	} {
		if bytes.Equal(a, other) {
			t.Errorf("AAD collides with %s", name)
		}
	}
	if !bytes.Equal(a, secretAAD(p1, e1)) {
		t.Error("AAD is not deterministic")
	}
}
