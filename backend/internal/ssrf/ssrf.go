// Package ssrf decides which destinations HookRelay may send webhooks to.
//
// A webhook platform is, by design, a service that makes HTTP requests to
// URLs supplied by users. Without these checks it could be pointed at
// internal infrastructure (cloud metadata services, databases, admin panels).
//
// Two layers use this package (docs/ARCHITECTURE.md §7):
//   - Registration time (Phase 2): ValidateURL rejects endpoint URLs that are
//     malformed, use a forbidden scheme, or resolve to a disallowed address.
//   - Connection time (Phase 3): the delivery dialer re-checks IsDisallowed on
//     the IP actually connected to, which defeats DNS rebinding. Registration
//     checks alone are NOT sufficient.
package ssrf

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

// disallowedPrefixes are networks HookRelay must never deliver to.
var disallowedPrefixes = mustPrefixes(
	"0.0.0.0/8",       // "this network"
	"10.0.0.0/8",      // private
	"100.64.0.0/10",   // carrier-grade NAT
	"127.0.0.0/8",     // loopback
	"169.254.0.0/16",  // link-local, incl. cloud metadata 169.254.169.254
	"172.16.0.0/12",   // private
	"192.0.0.0/24",    // IETF protocol assignments
	"192.0.2.0/24",    // TEST-NET-1
	"192.88.99.0/24",  // 6to4 relay anycast
	"192.168.0.0/16",  // private
	"198.18.0.0/15",   // benchmarking
	"198.51.100.0/24", // TEST-NET-2
	"203.0.113.0/24",  // TEST-NET-3
	"224.0.0.0/4",     // multicast
	"240.0.0.0/4",     // reserved, incl. broadcast
	"::/128",          // unspecified
	"::1/128",         // loopback
	"64:ff9b::/96",    // NAT64 (could reach internal IPv4)
	"64:ff9b:1::/48",  // local-use NAT64
	"100::/64",        // discard-only
	"2001:db8::/32",   // documentation
	"2002::/16",       // 6to4 (embeds IPv4)
	"fc00::/7",        // unique local, incl. AWS IMDS fd00:ec2::254
	"fe80::/10",       // link-local
	"ff00::/8",        // multicast
)

func mustPrefixes(cidrs ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(cidrs))
	for i, c := range cidrs {
		out[i] = netip.MustParsePrefix(c)
	}
	return out
}

// IsDisallowed reports whether ip is an address HookRelay must not connect to.
// IPv4-mapped IPv6 addresses (::ffff:a.b.c.d) are checked as IPv4.
func IsDisallowed(ip netip.Addr) bool {
	if !ip.IsValid() {
		return true
	}
	ip = ip.Unmap()
	for _, p := range disallowedPrefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// Resolver looks up the IP addresses of a host. *net.Resolver satisfies it.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// Policy configures ValidateURL.
type Policy struct {
	// RequireHTTPS rejects http:// URLs (production).
	RequireHTTPS bool
	// AllowPrivate disables all address checks. Local development only;
	// config refuses it in production.
	AllowPrivate bool
	// Resolver resolves hostnames; nil uses net.DefaultResolver.
	Resolver Resolver
	// LookupTimeout bounds DNS resolution; zero means 3s.
	LookupTimeout time.Duration
}

// MaxURLLength mirrors the endpoints.url CHECK constraint.
const MaxURLLength = 2048

// ErrInvalidURL wraps every rejection so callers can map it to a 422.
var ErrInvalidURL = errors.New("invalid endpoint url")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidURL, fmt.Sprintf(format, args...))
}

// blockedHostSuffixes are names that only make sense inside a private network.
var blockedHostSuffixes = []string{".localhost", ".local", ".internal", ".lan", ".home.arpa", ".intranet", ".corp"}

// ValidateURL checks a user-supplied endpoint URL and returns it normalized
// (scheme and host lowercased). It performs a DNS lookup, so call it before
// opening any database transaction.
func (p Policy) ValidateURL(ctx context.Context, raw string) (string, error) {
	if raw == "" {
		return "", invalid("url is required")
	}
	if len(raw) > MaxURLLength {
		return "", invalid("url must be at most %d characters", MaxURLLength)
	}
	if strings.ContainsAny(raw, " \t\r\n\\") {
		return "", invalid("url must not contain whitespace or backslashes")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", invalid("url could not be parsed")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	switch u.Scheme {
	case "https":
	case "http":
		if p.RequireHTTPS {
			return "", invalid("url must use https")
		}
	default:
		return "", invalid("url scheme must be http or https")
	}
	if u.Opaque != "" || u.Host == "" {
		return "", invalid("url must be absolute, like https://example.com/webhooks")
	}
	if u.User != nil {
		return "", invalid("url must not contain credentials")
	}
	if u.Fragment != "" || strings.Contains(raw, "#") {
		return "", invalid("url must not contain a fragment")
	}
	if port := u.Port(); port != "" {
		var n int
		if _, scanErr := fmt.Sscanf(port, "%d", &n); scanErr != nil || n < 1 || n > 65535 {
			return "", invalid("url port is invalid")
		}
	}

	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if host == "" {
		return "", invalid("url host is empty")
	}
	u.Host = strings.ToLower(u.Host)

	if p.AllowPrivate {
		return u.String(), nil
	}

	if ip, parseErr := netip.ParseAddr(host); parseErr == nil {
		if IsDisallowed(ip) {
			return "", invalid("url points to a private or reserved address")
		}
		return u.String(), nil
	}
	if host == "localhost" || !strings.Contains(host, ".") {
		return "", invalid("url host must be a public domain name")
	}
	for _, suffix := range blockedHostSuffixes {
		if strings.HasSuffix(host, suffix) {
			return "", invalid("url host must be a public domain name")
		}
	}

	resolver := p.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	timeout := p.LookupTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	lookupCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	addrs, err := resolver.LookupNetIP(lookupCtx, "ip", host)
	if err != nil || len(addrs) == 0 {
		return "", invalid("url host could not be resolved")
	}
	// Reject if ANY address is disallowed: a mixed answer could still route
	// a delivery to an internal address.
	for _, a := range addrs {
		if IsDisallowed(a) {
			return "", invalid("url host resolves to a private or reserved address")
		}
	}
	return u.String(), nil
}
