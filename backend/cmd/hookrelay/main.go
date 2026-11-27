// Command hookrelay is the single HookRelay binary. The subcommand selects the
// process role (see docs/ARCHITECTURE.md, decision D6).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"github.com/jeevss131436/Hookrelay/backend/internal/config"
)

const usage = `Usage: hookrelay <command> [args]

Commands:
  api         Run the HTTP API server
  worker      Run the delivery worker: Asynq handlers + dispatcher + reaper
  dispatcher  Run only the dispatcher loop (Postgres -> Redis)
  reaper      Run only the reaper loop (recovers stuck deliveries)
  migrate     Manage database migrations (up, down, status, version)
  admin       Administrative tasks (create-project, create-key, enqueue-delivery)
  help        Show this help

Configuration is read from environment variables; see .env.example.
`

// cmdEnv is what every subcommand receives.
type cmdEnv struct {
	cfg    *config.Config
	log    *slog.Logger
	args   []string  // arguments after the subcommand name
	stdout io.Writer // command output for humans/scripts (not logs)
}

type command struct {
	run func(ctx context.Context, e *cmdEnv) error
	// oneShot commands (admin, migrate) print results to stdout, so their
	// logs go to stderr. Long-running servers log JSON to stdout.
	oneShot bool
}

// usageError makes run print the message plus usage and exit with code 2.
type usageError struct {
	msg   string
	usage string
}

func (e *usageError) Error() string { return e.msg }

func newUsageError(usage, format string, args ...any) error {
	return &usageError{msg: fmt.Sprintf(format, args...), usage: usage}
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run returns the process exit code: 0 success, 1 runtime or config error,
// 2 usage error.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}

	name := args[0]
	var cmd command
	switch name {
	case "help", "-h", "--help":
		_, _ = fmt.Fprint(stdout, usage)
		return 0
	case "api":
		cmd = command{run: runAPI}
	case "worker":
		cmd = command{run: runWorker}
	case "dispatcher":
		cmd = command{run: runDispatcher}
	case "reaper":
		cmd = command{run: runReaper}
	case "migrate":
		cmd = command{run: runMigrate, oneShot: true}
	case "admin":
		cmd = command{run: runAdmin, oneShot: true}
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n\n%s", name, usage)
		return 2
	}

	cfg, err := config.Load(os.LookupEnv, os.Environ())
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "hookrelay %s: %v\n", name, err)
		return 1
	}

	logOut := stdout
	if cmd.oneShot {
		logOut = stderr
	}
	log := slog.New(slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: cfg.LogLevel})).
		With(slog.String("cmd", name), slog.String("env", string(cfg.Env)))
	slog.SetDefault(log)
	redis.SetLogger(redisLogger{log.With(slog.String("component", "redis"))})

	// The first SIGINT/SIGTERM cancels ctx and starts a graceful shutdown.
	// stop() is called as soon as shutdown begins, so a second signal kills
	// the process immediately.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop()
	}()

	err = cmd.run(ctx, &cmdEnv{cfg: cfg, log: log, args: args[1:], stdout: stdout})
	var uerr *usageError
	switch {
	case errors.As(err, &uerr):
		_, _ = fmt.Fprintf(stderr, "hookrelay %s: %s\n\n%s", name, uerr.msg, uerr.usage)
		return 2
	case err != nil:
		log.Error("exited with error", slog.String("error", err.Error()))
		return 1
	}
	log.Info("exited cleanly")
	return 0
}

// redisLogger routes go-redis internal log lines into slog. Without it they
// are printed as plain text and break the JSON log stream.
type redisLogger struct{ log *slog.Logger }

func (l redisLogger) Printf(ctx context.Context, format string, v ...any) {
	l.log.WarnContext(ctx, fmt.Sprintf(format, v...))
}
