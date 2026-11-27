package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/pressly/goose/v3"

	"github.com/jeevss131436/Hookrelay/backend/internal/config"
	"github.com/jeevss131436/Hookrelay/backend/internal/migrate"
	"github.com/jeevss131436/Hookrelay/backend/internal/store"
)

const migrateUsage = `Usage: hookrelay migrate <command>

Commands:
  up                  Apply all pending migrations
  down [--yes]        Roll back the most recent migration (--yes required in production)
  status              List migrations and whether each is applied
  version             Print the current schema version
`

func runMigrate(ctx context.Context, e *cmdEnv) error {
	if len(e.args) == 0 {
		return newUsageError(migrateUsage, "missing migrate command")
	}
	sub, rest := e.args[0], e.args[1:]

	fs := flag.NewFlagSet("migrate "+sub, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	yes := fs.Bool("yes", false, "confirm a destructive operation in production")
	if err := fs.Parse(rest); err != nil {
		return newUsageError(migrateUsage, "%v", err)
	}
	if fs.NArg() > 0 {
		return newUsageError(migrateUsage, "unexpected arguments: %v", fs.Args())
	}
	switch sub {
	case "up", "down", "status", "version":
	default:
		return newUsageError(migrateUsage, "unknown migrate command %q", sub)
	}
	if sub == "down" && e.cfg.Env == config.EnvProduction && !*yes {
		return newUsageError(migrateUsage, "refusing to roll back in production without --yes")
	}

	// Migrations need a direct (session-mode) connection: goose holds a
	// session-level advisory lock, which a transaction-mode pooler such as
	// Neon's -pooler endpoint cannot guarantee.
	pool, err := store.NewPool(ctx, e.cfg.DatabaseURLUnpooled)
	if err != nil {
		return err
	}
	defer pool.Close()
	m, err := migrate.New(pool)
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	out := e.stdout
	switch sub {
	case "up":
		results, upErr := m.Up(ctx)
		if upErr != nil {
			return fmt.Errorf("migrate up: %w", upErr)
		}
		if len(results) == 0 {
			_, _ = fmt.Fprintln(out, "No pending migrations.")
		}
		for _, r := range results {
			_, _ = fmt.Fprintf(out, "Applied  %s (%s)\n", r.Source.Path, r.Duration.Round(time.Millisecond))
		}
	case "down":
		r, downErr := m.Down(ctx)
		if downErr != nil {
			return fmt.Errorf("migrate down: %w", downErr)
		}
		_, _ = fmt.Fprintf(out, "Rolled back  %s (%s)\n", r.Source.Path, r.Duration.Round(time.Millisecond))
	case "status":
		statuses, statusErr := m.Status(ctx)
		if statusErr != nil {
			return fmt.Errorf("migrate status: %w", statusErr)
		}
		tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "STATE\tMIGRATION\tAPPLIED AT")
		for _, s := range statuses {
			applied := "-"
			if s.State == goose.StateApplied {
				applied = s.AppliedAt.UTC().Format(time.RFC3339)
			}
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\n", s.State, s.Source.Path, applied)
		}
		_ = tw.Flush()
		return nil
	}

	v, err := m.Version(ctx)
	if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	_, _ = fmt.Fprintf(out, "Schema version: %d\n", v)
	return nil
}
