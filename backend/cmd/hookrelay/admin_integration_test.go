package main

import (
	"bytes"
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/testutil/pgtest"
)

var (
	keyLine     = regexp.MustCompile(`(?m)^\s+Key:\s+(hr_[0-9A-Za-z]+)$`)
	projectLine = regexp.MustCompile(`(?m)^\s+ID:\s+([0-9a-f-]{36})\n\s+Name:`)
)

func runCLI(t *testing.T, dbURL string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	setTestEnv(t)
	t.Setenv("DATABASE_URL", dbURL)
	t.Setenv("DATABASE_URL_UNPOOLED", dbURL)
	var out, errOut bytes.Buffer
	code = run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestAdminCreateProjectEndToEnd(t *testing.T) {
	db := pgtest.NewMigrated(t)
	ctx := context.Background()

	code, stdout, stderr := runCLI(t, db.URL, "admin", "create-project", "--name", "Acme Inc")
	if code != 0 {
		t.Fatalf("exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}

	km := keyLine.FindStringSubmatch(stdout)
	pm := projectLine.FindStringSubmatch(stdout)
	if km == nil || pm == nil {
		t.Fatalf("could not parse output:\n%s", stdout)
	}
	plaintext, projectID := km[1], pm[1]

	// Shown exactly once, on stdout only; logs (stderr) carry only the prefix.
	if n := strings.Count(stdout, plaintext); n != 1 {
		t.Errorf("plaintext key printed %d times, want 1", n)
	}
	if strings.Contains(stderr, plaintext) {
		t.Errorf("plaintext key leaked into logs: %s", stderr)
	}
	if !strings.Contains(stderr, plaintext[:11]) {
		t.Errorf("logs should mention the key prefix: %s", stderr)
	}

	// The printed key authenticates against the stored hash.
	hash, err := secrets.HashAPIKey(plaintext)
	if err != nil {
		t.Fatalf("printed key is malformed: %v", err)
	}
	st := store.New(db.Pool)
	row, err := st.GetActiveAPIKeyByHash(ctx, hash)
	if err != nil {
		t.Fatalf("printed key does not authenticate: %v", err)
	}
	if row.ProjectID.String() != projectID || row.Name != "default" {
		t.Errorf("key row = %+v, want project %s and name default", row, projectID)
	}
}

func TestAdminCreateKeyEndToEnd(t *testing.T) {
	db := pgtest.NewMigrated(t)

	_, stdout, _ := runCLI(t, db.URL, "admin", "create-project", "--name", "Acme", "--key-name", "first")
	pm := projectLine.FindStringSubmatch(stdout)
	if pm == nil {
		t.Fatalf("could not parse project id:\n%s", stdout)
	}

	code, stdout, stderr := runCLI(t, db.URL, "admin", "create-key", "--project", pm[1], "--name", "ci")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	km := keyLine.FindStringSubmatch(stdout)
	if km == nil {
		t.Fatalf("no key in output:\n%s", stdout)
	}

	st := store.New(db.Pool)
	projectID, _ := store.ParseUUID(pm[1])
	keys, err := st.ListAPIKeys(context.Background(), projectID)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("project has %d keys, want 2", len(keys))
	}
	hash, _ := secrets.HashAPIKey(km[1])
	if row, err := st.GetActiveAPIKeyByHash(context.Background(), hash); err != nil || row.Name != "ci" {
		t.Errorf("new key lookup = %+v, %v", row, err)
	}
}

func TestAdminCreateKeyUnknownProject(t *testing.T) {
	db := pgtest.NewMigrated(t)
	code, stdout, stderr := runCLI(t, db.URL, "admin", "create-key", "--project", "00000000-0000-4000-8000-000000000000")
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(stderr, "project not found") {
		t.Errorf("stderr = %q", stderr)
	}
	if strings.Contains(stdout, "hr_") {
		t.Error("a key was printed for a missing project")
	}
}

func TestCreateProjectIsAtomic(t *testing.T) {
	db := pgtest.NewMigrated(t)
	st := store.New(db.Pool)
	// A key name over 100 chars violates api_keys' CHECK after the project
	// insert succeeds; the transaction must roll the project back too.
	if _, _, err := createProject(context.Background(), st, "Acme", strings.Repeat("k", 101)); err == nil {
		t.Fatal("expected error")
	}
	var n int
	if err := db.Pool.QueryRow(context.Background(), "SELECT count(*) FROM projects").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("projects = %d, want 0 (project must not exist without its key)", n)
	}
}

func TestMigrateCLI(t *testing.T) {
	db := pgtest.NewEmpty(t)

	code, stdout, stderr := runCLI(t, db.URL, "migrate", "up")
	if code != 0 || !strings.Contains(stdout, "Applied  00001_initial_schema.sql") ||
		!strings.Contains(stdout, "Applied  00002_read_api_indexes.sql") || !strings.Contains(stdout, "Schema version: 2") {
		t.Fatalf("up: exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if _, stdout, _ = runCLI(t, db.URL, "migrate", "up"); !strings.Contains(stdout, "No pending migrations.") {
		t.Errorf("second up: %s", stdout)
	}
	if _, stdout, _ = runCLI(t, db.URL, "migrate", "status"); !strings.Contains(stdout, "applied") || !strings.Contains(stdout, "00001_initial_schema.sql") {
		t.Errorf("status: %s", stdout)
	}
	// Each down undoes one migration; every migration must be reversible.
	if code, stdout, _ = runCLI(t, db.URL, "migrate", "down"); code != 0 || !strings.Contains(stdout, "Schema version: 1") {
		t.Errorf("first down: exit %d: %s", code, stdout)
	}
	if code, stdout, _ = runCLI(t, db.URL, "migrate", "down"); code != 0 || !strings.Contains(stdout, "Schema version: 0") {
		t.Errorf("second down: exit %d: %s", code, stdout)
	}
	if _, stdout, _ = runCLI(t, db.URL, "migrate", "version"); !strings.Contains(stdout, "Schema version: 0") {
		t.Errorf("version: %s", stdout)
	}
}
