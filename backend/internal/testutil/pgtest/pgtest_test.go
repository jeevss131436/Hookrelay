package pgtest

import (
	"net/url"
	"testing"
)

func TestSameRemoteServer(t *testing.T) {
	mustParse := func(s string) *url.URL {
		u, err := url.Parse(s)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	tests := []struct {
		a, b string
		want bool
	}{
		// Neon pooled vs direct hostnames for the same branch.
		{"postgresql://u:p@ep-cold-1-pooler.us-east-1.aws.neon.tech/db", "postgresql://u:p@ep-cold-1.us-east-1.aws.neon.tech/other", true},
		{"postgresql://u:p@ep-cold-1.us-east-1.aws.neon.tech/db", "postgresql://u:p@ep-cold-1.us-east-1.aws.neon.tech/db", true},
		// A different Neon branch endpoint is a different server.
		{"postgresql://u:p@ep-dev-2.us-east-1.aws.neon.tech/db", "postgresql://u:p@ep-cold-1.us-east-1.aws.neon.tech/db", false},
		// Local development servers are always allowed.
		{"postgres://u:p@localhost:5432/test", "postgres://u:p@localhost:5432/app", false},
		{"postgres://u:p@127.0.0.1/test", "postgres://u:p@127.0.0.1/app", false},
		// Unset app URL.
		{"postgres://u:p@db.example.com/test", "", false},
	}
	for _, tt := range tests {
		if got := sameRemoteServer(mustParse(tt.a), mustParse(tt.b)); got != tt.want {
			t.Errorf("sameRemoteServer(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}
