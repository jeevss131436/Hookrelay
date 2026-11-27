package ssrf

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"testing"
)

// fakeResolver maps hostnames to addresses; unknown hosts fail to resolve.
type fakeResolver map[string][]string

func (f fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	ips, ok := f[host]
	if !ok {
		return nil, errors.New("no such host")
	}
	out := make([]netip.Addr, len(ips))
	for i, s := range ips {
		out[i] = netip.MustParseAddr(s)
	}
	return out, nil
}

var resolver = fakeResolver{
	"hooks.example.com":    {"93.184.215.14"},
	"dual.example.com":     {"93.184.215.14", "2606:2800:21f:cb07:6820:80da:af6b:8b2c"},
	"rebind.example.com":   {"93.184.215.14", "127.0.0.1"},
	"internal.example.com": {"10.0.0.5"},
	"metadata.example.com": {"169.254.169.254"},
	"v6local.example.com":  {"fd00:ec2::254"},
	"mapped.example.com":   {"::ffff:192.168.1.1"},
	"2130706433":           {"127.0.0.1"}, // decimal IP some resolvers accept
}

func TestIsDisallowed(t *testing.T) {
	disallowed := []string{
		"127.0.0.1", "127.255.255.254", "10.1.2.3", "172.16.0.1", "172.31.255.255", "192.168.0.1",
		"169.254.169.254", "100.64.0.1", "0.0.0.0", "255.255.255.255", "224.0.0.1", "198.18.0.1",
		"::1", "::", "fe80::1", "fc00::1", "fd00:ec2::254", "ff02::1", "::ffff:127.0.0.1", "::ffff:10.0.0.1",
		"64:ff9b::a00:1", "2002:a00:1::",
	}
	allowed := []string{"93.184.215.14", "8.8.8.8", "1.1.1.1", "172.32.0.1", "2606:4700:4700::1111", "::ffff:8.8.8.8"}
	for _, s := range disallowed {
		if !IsDisallowed(netip.MustParseAddr(s)) {
			t.Errorf("%s should be disallowed", s)
		}
	}
	for _, s := range allowed {
		if IsDisallowed(netip.MustParseAddr(s)) {
			t.Errorf("%s should be allowed", s)
		}
	}
	if !IsDisallowed(netip.Addr{}) {
		t.Error("zero Addr should be disallowed")
	}
}

func TestValidateURLAccepts(t *testing.T) {
	p := Policy{RequireHTTPS: true, Resolver: resolver}
	tests := map[string]string{
		"https://hooks.example.com/webhooks":         "https://hooks.example.com/webhooks",
		"HTTPS://Hooks.Example.com/Path?q=1":         "https://hooks.example.com/Path?q=1",
		"https://hooks.example.com:8443/x":           "https://hooks.example.com:8443/x",
		"https://dual.example.com/":                  "https://dual.example.com/",
		"https://93.184.215.14/hook":                 "https://93.184.215.14/hook",
		"https://[2606:4700:4700::1111]/hook":        "https://[2606:4700:4700::1111]/hook",
		"https://hooks.example.com./trailing-dot-ok": "https://hooks.example.com./trailing-dot-ok",
	}
	for in, want := range tests {
		got, err := p.ValidateURL(context.Background(), in)
		if err != nil {
			t.Errorf("ValidateURL(%q) unexpected error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ValidateURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestValidateURLRejects(t *testing.T) {
	p := Policy{RequireHTTPS: true, Resolver: resolver}
	tests := map[string]string{
		"":                                     "required",
		"https://" + strings.Repeat("a", 2100): "at most",
		"http://hooks.example.com/":            "must use https",
		"ftp://hooks.example.com/":             "scheme",
		"javascript:alert(1)":                  "scheme",
		"file:///etc/passwd":                   "scheme",
		"hooks.example.com/no-scheme":          "scheme",
		"https:///no-host":                     "absolute",
		"https://user:pass@hooks.example.com/": "credentials",
		"https://hooks.example.com/#frag":      "fragment",
		"https://hooks.example.com:99999/":     "port",
		"https://hooks .example.com/":          "whitespace",
		"https://127.0.0.1/":                   "private",
		"https://10.0.0.1:8080/":               "private",
		"https://169.254.169.254/latest/meta":  "private",
		"https://[::1]/":                       "private",
		"https://[::ffff:127.0.0.1]/":          "private",
		"https://[fd00:ec2::254]/":             "private",
		"https://0.0.0.0/":                     "private",
		"https://localhost/":                   "public domain",
		"https://LOCALHOST:3000/":              "public domain",
		"https://api.localhost/":               "public domain",
		"https://printer.local/":               "public domain",
		"https://metadata.google.internal/":    "public domain",
		"https://intranet/":                    "public domain",
		"https://2130706433/":                  "public domain",
		"https://internal.example.com/":        "resolves to a private",
		"https://metadata.example.com/":        "resolves to a private",
		"https://v6local.example.com/":         "resolves to a private",
		"https://mapped.example.com/":          "resolves to a private",
		"https://rebind.example.com/":          "resolves to a private",
		"https://nxdomain.example.com/":        "could not be resolved",
	}
	for in, want := range tests {
		_, err := p.ValidateURL(context.Background(), in)
		if err == nil {
			t.Errorf("ValidateURL(%q) accepted, want error containing %q", in, want)
			continue
		}
		if !errors.Is(err, ErrInvalidURL) {
			t.Errorf("ValidateURL(%q) error %v does not wrap ErrInvalidURL", in, err)
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ValidateURL(%q) error %q, want it to contain %q", in, err, want)
		}
	}
}

func TestValidateURLHTTPAllowedOutsideProduction(t *testing.T) {
	p := Policy{RequireHTTPS: false, Resolver: resolver}
	if _, err := p.ValidateURL(context.Background(), "http://hooks.example.com/"); err != nil {
		t.Fatalf("http rejected without RequireHTTPS: %v", err)
	}
	// Address checks still apply over http.
	if _, err := p.ValidateURL(context.Background(), "http://127.0.0.1/"); err == nil {
		t.Fatal("private address accepted over http")
	}
}

func TestValidateURLAllowPrivate(t *testing.T) {
	p := Policy{AllowPrivate: true, Resolver: resolver}
	for _, in := range []string{"http://localhost:9000/hook", "http://127.0.0.1/", "http://10.0.0.5/x"} {
		if _, err := p.ValidateURL(context.Background(), in); err != nil {
			t.Errorf("AllowPrivate rejected %q: %v", in, err)
		}
	}
	// Structural rules still apply.
	if _, err := p.ValidateURL(context.Background(), "http://u:p@localhost/"); err == nil {
		t.Error("credentials accepted with AllowPrivate")
	}
}
