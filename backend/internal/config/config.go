// Package config loads and validates HookRelay's runtime configuration from
// environment variables. Loading is strict: unknown HOOKRELAY_* variables,
// malformed values, and violated invariants are all startup errors.
package config

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
)

// Environment names the deployment environment.
type Environment string

// Supported environments.
const (
	EnvDevelopment Environment = "development"
	EnvTest        Environment = "test"
	EnvProduction  Environment = "production"
)

// Config is the fully validated runtime configuration.
type Config struct {
	Env      Environment
	LogLevel slog.Level

	HTTPAddr        string
	ShutdownTimeout time.Duration

	// DatabaseURL serves application traffic. On Neon this is the pooled
	// (PgBouncer, transaction mode) endpoint.
	DatabaseURL string
	// DatabaseURLUnpooled is a direct, session-capable connection for work
	// that needs session state: migrations (goose's advisory lock) and, later,
	// LISTEN/NOTIFY. Read from DATABASE_URL_UNPOOLED (which `neon env pull`
	// writes); defaults to DatabaseURL when unset, e.g. for local Postgres.
	DatabaseURLUnpooled string
	RedisURL            string

	// AllowPrivateTargets disables SSRF address checks so endpoints may point
	// at localhost/private networks. Local development only; rejected in
	// production.
	AllowPrivateTargets bool

	// EncryptionKey encrypts endpoint signing secrets at rest (AES-256-GCM).
	// secrets.Key redacts itself in fmt and slog output.
	EncryptionKey secrets.Key

	// Delivery timing. Not used until Phase 3/4, but validated now so the
	// lease invariant (ARCHITECTURE.md §6.3) can never be misconfigured:
	// DeliveryLease > TaskTimeout > DeliveryHTTPTimeout.
	DeliveryHTTPTimeout time.Duration
	TaskTimeout         time.Duration
	DeliveryLease       time.Duration
}

// knownVars lists every HOOKRELAY_* variable the application reads. Any other
// HOOKRELAY_* variable is rejected so typos fail loudly instead of silently
// falling back to defaults.
var knownVars = map[string]bool{
	"HOOKRELAY_ENV":                   true,
	"HOOKRELAY_LOG_LEVEL":             true,
	"HOOKRELAY_HTTP_ADDR":             true,
	"HOOKRELAY_SHUTDOWN_TIMEOUT":      true,
	"HOOKRELAY_DELIVERY_HTTP_TIMEOUT": true,
	"HOOKRELAY_TASK_TIMEOUT":          true,
	"HOOKRELAY_DELIVERY_LEASE":        true,
	"HOOKRELAY_ENCRYPTION_KEY":        true,
	"HOOKRELAY_ALLOW_PRIVATE_TARGETS": true,
}

// Load builds a Config from the given environment. lookup behaves like
// os.LookupEnv and environ like os.Environ; both are injected for testing.
// All problems are collected and returned together.
func Load(lookup func(string) (string, bool), environ []string) (*Config, error) {
	l := loader{lookup: lookup}

	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(name, "HOOKRELAY_") && !knownVars[name] {
			l.errf("%s: unknown variable (check for typos)", name)
		}
	}

	cfg := &Config{
		Env:                 Environment(l.required("HOOKRELAY_ENV")),
		HTTPAddr:            l.optional("HOOKRELAY_HTTP_ADDR", ":8080"),
		ShutdownTimeout:     l.duration("HOOKRELAY_SHUTDOWN_TIMEOUT", 20*time.Second),
		DatabaseURL:         l.required("DATABASE_URL"),
		RedisURL:            l.required("REDIS_URL"),
		DeliveryHTTPTimeout: l.duration("HOOKRELAY_DELIVERY_HTTP_TIMEOUT", 10*time.Second),
		TaskTimeout:         l.duration("HOOKRELAY_TASK_TIMEOUT", 30*time.Second),
		DeliveryLease:       l.duration("HOOKRELAY_DELIVERY_LEASE", 60*time.Second),
	}

	switch cfg.Env {
	case EnvDevelopment, EnvTest, EnvProduction, "":
	default:
		l.errf("HOOKRELAY_ENV: must be one of development, test, production (got %q)", cfg.Env)
	}

	levelStr := l.optional("HOOKRELAY_LOG_LEVEL", "info")
	if err := cfg.LogLevel.UnmarshalText([]byte(levelStr)); err != nil {
		l.errf("HOOKRELAY_LOG_LEVEL: must be debug, info, warn or error (got %q)", levelStr)
	}

	if _, _, err := net.SplitHostPort(cfg.HTTPAddr); err != nil {
		l.errf("HOOKRELAY_HTTP_ADDR: must be host:port or :port (got %q)", cfg.HTTPAddr)
	}

	if cfg.DatabaseURL != "" {
		l.validateDatabaseURL("DATABASE_URL", cfg.DatabaseURL, cfg.Env)
	}
	cfg.DatabaseURLUnpooled = l.optional("DATABASE_URL_UNPOOLED", cfg.DatabaseURL)
	if cfg.DatabaseURLUnpooled != "" && cfg.DatabaseURLUnpooled != cfg.DatabaseURL {
		l.validateDatabaseURL("DATABASE_URL_UNPOOLED", cfg.DatabaseURLUnpooled, cfg.Env)
	}
	switch v := l.optional("HOOKRELAY_ALLOW_PRIVATE_TARGETS", "false"); v {
	case "false":
	case "true":
		cfg.AllowPrivateTargets = true
		if cfg.Env == EnvProduction {
			l.errf("HOOKRELAY_ALLOW_PRIVATE_TARGETS: must not be true in production (it disables SSRF protection)")
		}
	default:
		l.errf("HOOKRELAY_ALLOW_PRIVATE_TARGETS: must be true or false (got %q)", v)
	}

	if raw := l.required("HOOKRELAY_ENCRYPTION_KEY"); raw != "" {
		key, err := secrets.ParseKey(raw)
		if err != nil {
			l.errf("HOOKRELAY_ENCRYPTION_KEY: %v (generate one with: openssl rand -base64 32)", err)
		}
		cfg.EncryptionKey = key
	}
	if cfg.RedisURL != "" {
		if _, err := redis.ParseURL(cfg.RedisURL); err != nil {
			l.errf("REDIS_URL: invalid: %v", err)
		}
	}

	// Lease invariant: if the lease is shorter than the real work, the reaper
	// re-dispatches deliveries that are still running, causing duplicate sends.
	if cfg.DeliveryHTTPTimeout > 0 && cfg.TaskTimeout > 0 && cfg.TaskTimeout <= cfg.DeliveryHTTPTimeout {
		l.errf("HOOKRELAY_TASK_TIMEOUT (%s) must be greater than HOOKRELAY_DELIVERY_HTTP_TIMEOUT (%s)",
			cfg.TaskTimeout, cfg.DeliveryHTTPTimeout)
	}
	if cfg.TaskTimeout > 0 && cfg.DeliveryLease > 0 && cfg.DeliveryLease <= cfg.TaskTimeout {
		l.errf("HOOKRELAY_DELIVERY_LEASE (%s) must be greater than HOOKRELAY_TASK_TIMEOUT (%s)",
			cfg.DeliveryLease, cfg.TaskTimeout)
	}

	if len(l.errs) > 0 {
		return nil, &Error{Problems: l.errs}
	}
	return cfg, nil
}

// Error reports every configuration problem found, one per line.
type Error struct {
	Problems []error
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("invalid configuration:")
	for _, p := range e.Problems {
		b.WriteString("\n  - ")
		b.WriteString(p.Error())
	}
	return b.String()
}

// Unwrap exposes the individual problems to errors.Is / errors.As.
func (e *Error) Unwrap() []error { return e.Problems }

// validateDatabaseURL checks that the URL parses as a pgx pool config without
// connecting. Credentials are never included in error messages.
func (l *loader) validateDatabaseURL(name, raw string, env Environment) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Host == "" {
		l.errf("%s: must be a postgres:// or postgresql:// URL with a host", name)
		return
	}
	if _, err := pgxpool.ParseConfig(raw); err != nil {
		l.errf("%s: invalid connection settings", name)
		return
	}
	if env == EnvProduction && u.Query().Get("sslmode") == "disable" {
		l.errf("%s: sslmode=disable is not allowed in production", name)
	}
}

type loader struct {
	lookup func(string) (string, bool)
	errs   []error
}

func (l *loader) errf(format string, args ...any) {
	l.errs = append(l.errs, fmt.Errorf(format, args...))
}

func (l *loader) get(name string) (string, bool) {
	v, ok := l.lookup(name)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

func (l *loader) required(name string) string {
	v, ok := l.get(name)
	if !ok {
		l.errf("%s: required but not set", name)
	}
	return v
}

func (l *loader) optional(name, def string) string {
	if v, ok := l.get(name); ok {
		return v
	}
	return def
}

func (l *loader) duration(name string, def time.Duration) time.Duration {
	v, ok := l.get(name)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		l.errf("%s: must be a positive duration like 10s or 1m (got %q)", name, v)
		return 0
	}
	return d
}
