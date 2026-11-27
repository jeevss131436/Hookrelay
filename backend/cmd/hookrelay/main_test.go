package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunUsage(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantCode int
		wantOut  string
	}{
		{"no args", nil, 2, "Usage:"},
		{"help", []string{"help"}, 0, "Usage:"},
		{"unknown", []string{"serve"}, 2, `unknown command "serve"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(tt.args, &stdout, &stderr); code != tt.wantCode {
				t.Errorf("exit code = %d, want %d", code, tt.wantCode)
			}
			if out := stdout.String() + stderr.String(); !strings.Contains(out, tt.wantOut) {
				t.Errorf("output %q does not contain %q", out, tt.wantOut)
			}
		})
	}
}

func TestRunFailsFastOnInvalidConfig(t *testing.T) {
	t.Setenv("HOOKRELAY_ENV", "")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("REDIS_URL", "")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"api"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "HOOKRELAY_ENV: required") {
		t.Errorf("stderr = %q", stderr.String())
	}
}

// setTestEnv sets a valid configuration. The database is never contacted
// unless a test points DATABASE_URL at a real server.
func setTestEnv(t *testing.T) {
	t.Helper()
	t.Setenv("HOOKRELAY_ENV", "test")
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost:1/db?sslmode=disable")
	t.Setenv("REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("HOOKRELAY_ENCRYPTION_KEY", "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=") // test-only key
}

func TestWorkerPlaceholderExitsNonZero(t *testing.T) {
	setTestEnv(t)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"worker"}, &stdout, &stderr); code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stdout.String(), "not implemented yet") {
		t.Errorf("stdout = %q", stdout.String())
	}
}

func TestSubcommandUsageErrors(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantMsg string
	}{
		{"api extra args", []string{"api", "now"}, nil, "unexpected arguments"},
		{"migrate missing", []string{"migrate"}, nil, "missing migrate command"},
		{"migrate unknown", []string{"migrate", "sideways"}, nil, `unknown migrate command "sideways"`},
		{"migrate bad flag", []string{"migrate", "up", "--force"}, nil, "flag provided but not defined"},
		{
			"migrate down in production needs --yes",
			[]string{"migrate", "down"},
			map[string]string{"HOOKRELAY_ENV": "production", "DATABASE_URL": "postgres://u:p@localhost:1/db?sslmode=require"},
			"refusing to roll back in production",
		},
		{"admin missing", []string{"admin"}, nil, "missing admin command"},
		{"admin unknown", []string{"admin", "delete-everything"}, nil, `unknown admin command "delete-everything"`},
		{"create-project no name", []string{"admin", "create-project"}, nil, "--name must be 1-100 characters"},
		{"create-project blank name", []string{"admin", "create-project", "--name", "  "}, nil, "--name must be 1-100 characters"},
		{"create-project long name", []string{"admin", "create-project", "--name", strings.Repeat("x", 101)}, nil, "--name must be 1-100 characters"},
		{"create-project stray arg", []string{"admin", "create-project", "--name", "a", "extra"}, nil, "unexpected arguments"},
		{"create-key no project", []string{"admin", "create-key"}, nil, "--project is required"},
		{"create-key bad project", []string{"admin", "create-key", "--project", "nope"}, nil, "invalid UUID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setTestEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			var stdout, stderr bytes.Buffer
			if code := run(tt.args, &stdout, &stderr); code != 2 {
				t.Errorf("exit code = %d, want 2 (stderr %q)", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), tt.wantMsg) || !strings.Contains(stderr.String(), "Usage:") {
				t.Errorf("stderr = %q, want %q and usage", stderr.String(), tt.wantMsg)
			}
		})
	}
}
