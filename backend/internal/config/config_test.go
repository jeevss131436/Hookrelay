package config

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func validEnv() map[string]string {
	return map[string]string{
		"HOOKRELAY_ENV": "development",
		"DATABASE_URL":  "postgres://user:secret@localhost:5432/hookrelay?sslmode=disable",
		"REDIS_URL":     "redis://localhost:6379/0",
		// Test-only key, not used anywhere real.
		"HOOKRELAY_ENCRYPTION_KEY": testKeyB64,
	}
}

// 32 bytes 0x01..0x20, base64-encoded.
const testKeyB64 = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA="

func load(env map[string]string) (*Config, error) {
	environ := make([]string, 0, len(env))
	for k, v := range env {
		environ = append(environ, k+"="+v)
	}
	return Load(func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	}, environ)
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := load(validEnv())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Env != EnvDevelopment {
		t.Errorf("Env = %q, want development", cfg.Env)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want info", cfg.LogLevel)
	}
	if cfg.ShutdownTimeout != 20*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 20s", cfg.ShutdownTimeout)
	}
	if cfg.EncryptionKey.Base64() != testKeyB64 {
		t.Error("EncryptionKey not loaded")
	}
	if cfg.DeliveryHTTPTimeout != 10*time.Second || cfg.TaskTimeout != 30*time.Second || cfg.DeliveryLease != 60*time.Second {
		t.Errorf("unexpected delivery timings: %v / %v / %v", cfg.DeliveryHTTPTimeout, cfg.TaskTimeout, cfg.DeliveryLease)
	}
	if cfg.WorkerConcurrency != 50 {
		t.Errorf("WorkerConcurrency = %d, want 50", cfg.WorkerConcurrency)
	}
}

func TestLoadOverrides(t *testing.T) {
	env := validEnv()
	env["HOOKRELAY_LOG_LEVEL"] = "debug"
	env["HOOKRELAY_HTTP_ADDR"] = "127.0.0.1:9000"
	env["HOOKRELAY_SHUTDOWN_TIMEOUT"] = "5s"
	env["HOOKRELAY_WORKER_CONCURRENCY"] = "8"
	cfg, err := load(env)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LogLevel != slog.LevelDebug || cfg.HTTPAddr != "127.0.0.1:9000" || cfg.ShutdownTimeout != 5*time.Second ||
		cfg.WorkerConcurrency != 8 {
		t.Errorf("overrides not applied: %+v", cfg)
	}
}

func TestLoadErrors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(map[string]string)
		wantErr string
	}{
		{"missing env", func(e map[string]string) { delete(e, "HOOKRELAY_ENV") }, "HOOKRELAY_ENV: required"},
		{"blank env", func(e map[string]string) { e["HOOKRELAY_ENV"] = "  " }, "HOOKRELAY_ENV: required"},
		{"bad env", func(e map[string]string) { e["HOOKRELAY_ENV"] = "staging" }, "must be one of"},
		{"missing database", func(e map[string]string) { delete(e, "DATABASE_URL") }, "DATABASE_URL: required"},
		{"missing redis", func(e map[string]string) { delete(e, "REDIS_URL") }, "REDIS_URL: required"},
		{"bad allow private", func(e map[string]string) { e["HOOKRELAY_ALLOW_PRIVATE_TARGETS"] = "yes" }, "must be true or false"},
		{
			"allow private in production",
			func(e map[string]string) {
				e["HOOKRELAY_ENV"] = "production"
				e["DATABASE_URL"] = "postgres://u:p@db.example.com/x?sslmode=require"
				e["HOOKRELAY_ALLOW_PRIVATE_TARGETS"] = "true"
			},
			"must not be true in production",
		},
		{"missing encryption key", func(e map[string]string) { delete(e, "HOOKRELAY_ENCRYPTION_KEY") }, "HOOKRELAY_ENCRYPTION_KEY: required"},
		{"short encryption key", func(e map[string]string) { e["HOOKRELAY_ENCRYPTION_KEY"] = "c2hvcnQ=" }, "must decode to 32 bytes"},
		{"non-base64 encryption key", func(e map[string]string) { e["HOOKRELAY_ENCRYPTION_KEY"] = "change-me!" }, "not valid base64"},
		{"bad database scheme", func(e map[string]string) { e["DATABASE_URL"] = "mysql://localhost/db" }, "DATABASE_URL: must be"},
		{"bad redis", func(e map[string]string) { e["REDIS_URL"] = "http://localhost:6379" }, "REDIS_URL: invalid"},
		{"bad log level", func(e map[string]string) { e["HOOKRELAY_LOG_LEVEL"] = "verbose" }, "HOOKRELAY_LOG_LEVEL"},
		{"bad addr", func(e map[string]string) { e["HOOKRELAY_HTTP_ADDR"] = "8080" }, "HOOKRELAY_HTTP_ADDR"},
		{"bad duration", func(e map[string]string) { e["HOOKRELAY_SHUTDOWN_TIMEOUT"] = "soon" }, "HOOKRELAY_SHUTDOWN_TIMEOUT"},
		{"negative duration", func(e map[string]string) { e["HOOKRELAY_SHUTDOWN_TIMEOUT"] = "-1s" }, "HOOKRELAY_SHUTDOWN_TIMEOUT"},
		{"unknown var", func(e map[string]string) { e["HOOKRELAY_HTTP_ADRR"] = ":1" }, "HOOKRELAY_HTTP_ADRR: unknown variable"},
		{"zero concurrency", func(e map[string]string) { e["HOOKRELAY_WORKER_CONCURRENCY"] = "0" }, "HOOKRELAY_WORKER_CONCURRENCY: must be an integer from 1 to 1000"},
		{"huge concurrency", func(e map[string]string) { e["HOOKRELAY_WORKER_CONCURRENCY"] = "5000" }, "HOOKRELAY_WORKER_CONCURRENCY"},
		{"non-numeric concurrency", func(e map[string]string) { e["HOOKRELAY_WORKER_CONCURRENCY"] = "many" }, "HOOKRELAY_WORKER_CONCURRENCY"},
		{
			"task timeout not above http timeout",
			func(e map[string]string) {
				e["HOOKRELAY_DELIVERY_HTTP_TIMEOUT"] = "30s"
				e["HOOKRELAY_TASK_TIMEOUT"] = "30s"
			},
			"HOOKRELAY_TASK_TIMEOUT (30s) must be greater",
		},
		{
			"lease not above task timeout",
			func(e map[string]string) { e["HOOKRELAY_DELIVERY_LEASE"] = "20s" },
			"HOOKRELAY_DELIVERY_LEASE (20s) must be greater",
		},
		{
			"sslmode disable in production",
			func(e map[string]string) { e["HOOKRELAY_ENV"] = "production" },
			"sslmode=disable is not allowed in production",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := validEnv()
			tt.mutate(env)
			_, err := load(env)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := load(map[string]string{})
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"HOOKRELAY_ENV", "DATABASE_URL", "REDIS_URL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}
}

func TestDatabaseURLUnpooled(t *testing.T) {
	cfg, err := load(validEnv())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURLUnpooled != cfg.DatabaseURL {
		t.Errorf("unset DATABASE_URL_UNPOOLED should fall back to DATABASE_URL, got %q", cfg.DatabaseURLUnpooled)
	}

	env := validEnv()
	env["DATABASE_URL"] = "postgresql://u:p@ep-x-pooler.example.neon.tech/neondb?sslmode=require"
	env["DATABASE_URL_UNPOOLED"] = "postgresql://u:p@ep-x.example.neon.tech/neondb?sslmode=require"
	cfg, err = load(env)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURLUnpooled != env["DATABASE_URL_UNPOOLED"] {
		t.Errorf("DatabaseURLUnpooled = %q", cfg.DatabaseURLUnpooled)
	}

	env["DATABASE_URL_UNPOOLED"] = "mysql://nope"
	if _, err := load(env); err == nil || !strings.Contains(err.Error(), "DATABASE_URL_UNPOOLED: must be") {
		t.Errorf("invalid unpooled URL err = %v", err)
	}
}

func TestEncryptionKeyNeverPrinted(t *testing.T) {
	cfg, err := load(validEnv())
	if err != nil {
		t.Fatal(err)
	}
	if s := fmt.Sprintf("%+v", cfg); strings.Contains(s, testKeyB64) {
		t.Errorf("config formatting leaks encryption key: %s", s)
	}

	env := validEnv()
	env["HOOKRELAY_ENCRYPTION_KEY"] = "c2VjcmV0LWJ1dC1zaG9ydA=="
	if _, err := load(env); err == nil || strings.Contains(err.Error(), "c2VjcmV0LWJ1dC1zaG9ydA") {
		t.Errorf("error leaks key material: %v", err)
	}
}

func TestDatabaseURLErrorDoesNotLeakPassword(t *testing.T) {
	env := validEnv()
	env["DATABASE_URL"] = "postgres://user:hunter2@localhost:5432/db?pool_max_conns=notanumber"
	_, err := load(env)
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("error leaks password: %v", err)
	}
}
