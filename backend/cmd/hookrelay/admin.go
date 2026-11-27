package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jeevss131436/Hookrelay/backend/internal/secrets"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
	"github.com/jeevss131436/Hookrelay/backend/internal/store/sqlcgen"
)

const adminUsage = `Usage: hookrelay admin <command> [flags]

Commands:
  create-project --name NAME [--key-name NAME]
      Create a project and its first API key (one transaction).
  create-key --project PROJECT_ID [--name NAME]
      Create an additional API key for an existing project.

The plaintext API key is printed once, to stdout. Only its SHA-256 hash is stored.
`

const defaultKeyName = "default"

var errProjectNotFound = errors.New("project not found")

func runAdmin(ctx context.Context, e *cmdEnv) error {
	if len(e.args) == 0 {
		return newUsageError(adminUsage, "missing admin command")
	}
	sub, rest := e.args[0], e.args[1:]

	fs := flag.NewFlagSet("admin "+sub, flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	switch sub {
	case "create-project":
		name := fs.String("name", "", "project name")
		keyName := fs.String("key-name", defaultKeyName, "name of the first API key")
		if err := parseFlags(fs, rest); err != nil {
			return err
		}
		if err := validateName("--name", *name); err != nil {
			return err
		}
		if err := validateName("--key-name", *keyName); err != nil {
			return err
		}
		return withStore(ctx, e, func(st *store.Store) error {
			project, key, err := createProject(ctx, st, *name, *keyName)
			if err != nil {
				return err
			}
			printProject(e.stdout, project)
			printKey(e.stdout, key)
			e.log.Info("project created",
				"project_id", project.ID.String(), "api_key_id", key.row.ID.String(), "api_key", key.key)
			return nil
		})

	case "create-key":
		projectFlag := fs.String("project", "", "project ID")
		keyName := fs.String("name", defaultKeyName, "API key name")
		if err := parseFlags(fs, rest); err != nil {
			return err
		}
		if *projectFlag == "" {
			return newUsageError(adminUsage, "--project is required")
		}
		projectID, err := store.ParseUUID(*projectFlag)
		if err != nil {
			return newUsageError(adminUsage, "--project: %v", err)
		}
		if err := validateName("--name", *keyName); err != nil {
			return err
		}
		return withStore(ctx, e, func(st *store.Store) error {
			key, err := createKey(ctx, st, projectID, *keyName)
			if err != nil {
				return err
			}
			printKey(e.stdout, key)
			e.log.Info("api key created",
				"project_id", projectID.String(), "api_key_id", key.row.ID.String(), "api_key", key.key)
			return nil
		})

	default:
		return newUsageError(adminUsage, "unknown admin command %q", sub)
	}
}

func parseFlags(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		return newUsageError(adminUsage, "%v", err)
	}
	if fs.NArg() > 0 {
		return newUsageError(adminUsage, "unexpected arguments: %v", fs.Args())
	}
	return nil
}

// validateName mirrors the CHECK constraints on projects.name/api_keys.name
// so users get a clear message instead of a constraint error.
func validateName(flagName, v string) error {
	n := utf8.RuneCountInString(v)
	if strings.TrimSpace(v) == "" || n > 100 {
		return newUsageError(adminUsage, "%s must be 1-100 characters", flagName)
	}
	return nil
}

func withStore(ctx context.Context, e *cmdEnv, fn func(*store.Store) error) error {
	pool, err := store.NewPool(ctx, e.cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	return fn(store.New(pool))
}

// createdKey pairs the stored row with the one-time plaintext.
type createdKey struct {
	row sqlcgen.CreateAPIKeyRow
	key secrets.APIKey
}

// createProject creates a project and its first API key atomically, so a
// project never exists without a usable key.
func createProject(ctx context.Context, st *store.Store, name, keyName string) (sqlcgen.Project, createdKey, error) {
	var (
		project sqlcgen.Project
		created createdKey
	)
	err := st.InTx(ctx, func(q *sqlcgen.Queries) error {
		var err error
		project, err = q.CreateProject(ctx, name)
		if err != nil {
			return fmt.Errorf("create project: %w", err)
		}
		created, err = insertKey(ctx, q, project.ID, keyName)
		return err
	})
	if err != nil {
		return sqlcgen.Project{}, createdKey{}, err
	}
	return project, created, nil
}

// createKey adds an API key to an existing project.
func createKey(ctx context.Context, st *store.Store, projectID pgtype.UUID, keyName string) (createdKey, error) {
	if _, err := st.GetProject(ctx, projectID); err != nil {
		if store.IsNotFound(err) {
			return createdKey{}, errProjectNotFound
		}
		return createdKey{}, fmt.Errorf("look up project: %w", err)
	}
	return insertKey(ctx, st.Queries, projectID, keyName)
}

func insertKey(ctx context.Context, q *sqlcgen.Queries, projectID pgtype.UUID, keyName string) (createdKey, error) {
	key := secrets.GenerateAPIKey()
	row, err := q.CreateAPIKey(ctx, sqlcgen.CreateAPIKeyParams{
		ProjectID: projectID,
		Name:      keyName,
		Prefix:    key.Prefix,
		KeyHash:   key.Hash,
	})
	switch {
	case store.IsForeignKeyViolation(err, ""):
		return createdKey{}, errProjectNotFound
	case err != nil:
		return createdKey{}, fmt.Errorf("create api key: %w", err)
	}
	return createdKey{row: row, key: key}, nil
}

func printProject(w io.Writer, p sqlcgen.Project) {
	_, _ = fmt.Fprintf(w, "Created project\n  ID:    %s\n  Name:  %s\n\n", p.ID.String(), p.Name)
}

func printKey(w io.Writer, k createdKey) {
	_, _ = fmt.Fprintf(w, "Created API key %q\n  ID:      %s\n  Project: %s\n  Prefix:  %s\n  Key:     %s\n\n",
		k.row.Name, k.row.ID.String(), k.row.ProjectID.String(), k.row.Prefix, k.key.Plaintext)
	_, _ = fmt.Fprintln(w, "Save this key now. It will not be shown again; only its SHA-256 hash is stored.")
}
