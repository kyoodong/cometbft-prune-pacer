package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds every runtime option. Flags win over environment variables.
type Config struct {
	MaxStep     int64         // max blocks a single pruning step may cover (required)
	MinRetain   int64         // blocks that must always be kept (required)
	GRPCAddr    string        // privileged gRPC address ([grpc.privileged] laddr)
	RPCAddr     string        // CometBFT RPC base URL
	MetricsAddr string        // Prometheus endpoint, informational only
	Home        string        // node home; enables config.toml / app.toml checks
	Wait        time.Duration // sleep when the min-retain ceiling is reached
	Poll        time.Duration // witness polling interval
	StepTimeout time.Duration // warn when a step takes longer than this
	MinMemAvail uint64        // bytes; do not start a step below this MemAvailable
	StartHeight int64         // first step base; 0 = auto-detect
	DryRun      bool
}

func parseConfig(args []string) (*Config, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	c := &Config{}
	var minMem string
	fs.Int64Var(&c.MaxStep, "max-step", envInt("PACER_MAX_STEP", 0), "max blocks per pruning step (required, env PACER_MAX_STEP)")
	fs.Int64Var(&c.MinRetain, "min-retain", envInt("PACER_MIN_RETAIN", 0), "blocks to always keep (required, env PACER_MIN_RETAIN)")
	fs.StringVar(&c.GRPCAddr, "grpc-addr", envStr("PACER_GRPC_ADDR", "127.0.0.1:26659"), "privileged gRPC address")
	fs.StringVar(&c.RPCAddr, "rpc-addr", envStr("PACER_RPC_ADDR", "http://127.0.0.1:26657"), "CometBFT RPC URL")
	fs.StringVar(&c.MetricsAddr, "metrics-addr", envStr("PACER_METRICS_ADDR", "http://127.0.0.1:26660/metrics"), "Prometheus URL (informational)")
	fs.StringVar(&c.Home, "home", envStr("PACER_HOME", ""), "node home directory (enables config file checks)")
	fs.DurationVar(&c.Wait, "wait", envDur("PACER_WAIT", 10*time.Minute), "sleep when the min-retain ceiling is reached")
	fs.DurationVar(&c.Poll, "poll", envDur("PACER_POLL", 30*time.Second), "witness polling interval")
	fs.DurationVar(&c.StepTimeout, "step-timeout", envDur("PACER_STEP_TIMEOUT", 2*time.Hour), "warn when a step takes longer than this")
	fs.StringVar(&minMem, "min-mem-avail", envStr("PACER_MIN_MEM_AVAIL", "30GiB"), "minimum MemAvailable to start a step")
	fs.Int64Var(&c.StartHeight, "start-height", envInt("PACER_START_HEIGHT", 0), "first step base height (0 = lowest indexed height)")
	fs.BoolVar(&c.DryRun, "dry-run", envStr("PACER_DRY_RUN", "") == "true", "log decisions without calling Set")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	b, err := parseBytes(minMem)
	if err != nil {
		return nil, fmt.Errorf("--min-mem-avail: %w", err)
	}
	c.MinMemAvail = b
	if c.MaxStep <= 0 {
		return nil, errors.New("--max-step (or PACER_MAX_STEP) is required and must be > 0")
	}
	if c.MinRetain <= 0 {
		return nil, errors.New("--min-retain (or PACER_MIN_RETAIN) is required and must be > 0")
	}
	c.RPCAddr = strings.TrimRight(c.RPCAddr, "/")
	return c, nil
}

func envStr(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

func envInt(k string, def int64) int64 {
	if v, ok := os.LookupEnv(k); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(k); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// parseBytes accepts "30GiB", "512MiB" or a plain byte count.
func parseBytes(s string) (uint64, error) {
	units := []struct {
		suffix string
		mult   uint64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}}
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			n, err := strconv.ParseUint(strings.TrimSuffix(s, u.suffix), 10, 64)
			return n * u.mult, err
		}
	}
	return strconv.ParseUint(s, 10, 64)
}
