// Command cometbft-prune-pacer raises a CometBFT v1 node's indexer retain
// heights in bounded steps so that index pruning never runs out of memory.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/getsentry/sentry-go"
	"github.com/kyoodong/cometbft-prune-pacer/internal/pacer"
)

var version = "dev"

const usage = `usage:
  cometbft-prune-pacer [run] --max-step N --min-retain N [options]
  cometbft-prune-pacer version

run "cometbft-prune-pacer run -h" for all options`

func main() {
	args := os.Args[1:]
	cmd := "run"
	if len(args) > 0 && (args[0] == "run" || args[0] == "version") {
		cmd, args = args[0], args[1:]
	}
	var err error
	switch cmd {
	case "version":
		fmt.Println(version)
	default:
		err = run(args)
	}
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		_, _ = fmt.Fprintln(os.Stderr, err)
		// A config error needs the operator; exit 3 so the unit's
		// RestartPreventExitStatus=3 stops the restart loop. C1 (node not
		// reachable yet) exits 1 and is retried.
		var cf *pacer.CheckFailure
		if errors.As(err, &cf) && cf.ID != "C1" {
			os.Exit(3)
		}
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := pacer.ParseConfig(args)
	if err != nil {
		// Usage only helps with a bad command line, not with a failed check.
		return fmt.Errorf("%w\n%s", err, usage)
	}
	sentryOn, flush, err := pacer.InitSentry("cometbft-prune-pacer@" + version)
	if err != nil {
		return err
	}
	defer flush()
	var handler slog.Handler = slog.NewJSONHandler(os.Stdout, nil)
	if sentryOn {
		handler = pacer.NewSentryHandler(handler)
		defer func() {
			if r := recover(); r != nil {
				sentry.CurrentHub().Recover(r)
				flush()
				panic(r)
			}
		}()
	}
	log := slog.New(handler)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	n, err := pacer.DialNode(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = n.Close() }()

	if err := pacer.Preflight(ctx, cfg, n, log); err != nil {
		var cf *pacer.CheckFailure
		if sentryOn && errors.As(err, &cf) {
			pacer.ReportCheckFailure(cf)
		}
		return err
	}
	p := pacer.NewPacer(cfg, n, log)
	if cfg.Listen != "" {
		p.ServeMetrics(ctx, cfg.Listen)
	}
	log.Info("started", "version", version, "max_step", cfg.MaxStep, "min_retain", cfg.MinRetain,
		"min_mem_avail_mib", cfg.MinMemAvail>>20,
		"dry_run", cfg.DryRun, "metrics", cfg.Listen, "sentry", sentryOn)
	return p.Run(ctx)
}
