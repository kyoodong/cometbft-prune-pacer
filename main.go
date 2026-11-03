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
)

var version = "dev"

const usage = `usage:
  cometbft-prune-pacer [run] --max-step N --min-retain N [options]
  cometbft-prune-pacer install --user USER [run options]
  cometbft-prune-pacer version

run "cometbft-prune-pacer run -h" for all options`

func main() {
	args := os.Args[1:]
	cmd := "run"
	if len(args) > 0 && (args[0] == "run" || args[0] == "install" || args[0] == "version") {
		cmd, args = args[0], args[1:]
	}
	var err error
	switch cmd {
	case "version":
		fmt.Println(version)
	case "install":
		err = install(args)
	default:
		err = run(args)
	}
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	n, err := dialNode(cfg)
	if err != nil {
		return err
	}
	defer n.conn.Close()

	if err := preflight(ctx, cfg, n, log); err != nil {
		return err
	}
	log.Info("started", "version", version, "max_step", cfg.MaxStep, "min_retain", cfg.MinRetain, "dry_run", cfg.DryRun)
	return NewPacer(cfg, n, log).Run(ctx)
}
