package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// f05Step parses the command line exactly like `run` does, then runs one pacer
// step against a node reporting the given MemAvailable. It returns the
// tx-indexer Set history, or the parse error if the flag value was rejected.
func f05Step(t *testing.T, minMem string, avail uint64) ([]int64, error) {
	t.Helper()
	cfg, err := parseConfig([]string{"--max-step", "300", "--min-retain", "1000", "--min-mem-avail", minMem})
	if err != nil {
		return nil, err
	}
	f := newFakeNode(1000, 5000, 0, 0)
	f.memAvail = avail
	p := NewPacer(cfg, f, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.sleep = func(ctx context.Context, _ time.Duration) error { f.tick(); return ctx.Err() }
	if err := p.step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	t.Logf("--min-mem-avail %q -> MinMemAvail=%d bytes; avail=%d GiB; sets=%v", minMem, cfg.MinMemAvail, avail>>30, f.setHistory)
	return f.setHistory, nil
}

// Control: with plenty of memory one step reaches Set, so an empty Set history
// below really means the S3 floor held (the assertion is not vacuous).
func TestF05ControlStepReachesSet(t *testing.T) {
	sets, err := f05Step(t, "30GiB", 64<<30)
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) == 0 {
		t.Fatal("control: expected a Set with 64 GiB available and a 30GiB floor")
	}
}

// F5: a --min-mem-avail value an operator could plausibly write must either be
// rejected at startup or keep the memory floor in force. With 1 GiB available,
// far below the intended 30 GiB, no step may start.
func TestF05MinMemAvailInputsKeepFloor(t *testing.T) {
	for _, in := range []string{
		"30GiB",          // documented form
		"30",             // unit forgotten
		"30G",            // common shorthand
		"30GB",           // SI suffix
		"17179869184GiB", // overflows uint64 when multiplied
	} {
		t.Run(in, func(t *testing.T) {
			sets, err := f05Step(t, in, 1<<30)
			if err != nil {
				t.Logf("rejected at startup: %v", err)
				return
			}
			if len(sets) != 0 {
				t.Fatalf("--min-mem-avail %q accepted, yet a step was Set at 1 GiB MemAvailable: %v", in, sets)
			}
		})
	}
}

// TestF06RejectsNonPositiveDurations: --poll/--wait/--step-timeout drive real
// timers, so a zero or negative value must be refused at parse time.
func TestF06RejectsNonPositiveDurations(t *testing.T) {
	base := []string{"--max-step", "300", "--min-retain", "1000"}
	cases := [][]string{
		{"--poll", "0"}, // flag.Duration takes a bare "0" (time.ParseDuration special case)
		{"--poll", "0s"},
		{"--poll", "-1s"},
		{"--wait", "0"},
		{"--wait", "-5s"},
		{"--step-timeout", "0"},
	}
	for _, c := range cases {
		cfg, err := parseConfig(append(append([]string{}, base...), c...))
		if err == nil {
			t.Errorf("%v accepted: poll=%v wait=%v step-timeout=%v", c, cfg.Poll, cfg.Wait, cfg.StepTimeout)
		}
	}
	// Environment path: a bare "0" also parses; "30" (no unit) is rejected
	// as malformed (see TestBadEnvValueFails).
	t.Setenv("PACER_POLL", "30s")
	if cfg, err := parseConfig(base); err != nil || cfg.Poll != 30*time.Second {
		t.Fatalf("control PACER_POLL=30s: poll=%v err=%v", cfg.Poll, err)
	}
	t.Setenv("PACER_POLL", "0")
	if cfg, err := parseConfig(base); err == nil {
		t.Errorf("PACER_POLL=0 accepted: poll=%v", cfg.Poll)
	}
}

// f06Node counts calls so the real sleepCtx pacing can be measured.
type f06Node struct {
	*fakeNode
	status, txIndexed atomic.Int64
	txErr             error
}

func (n *f06Node) Status(ctx context.Context) (int64, int64, bool, error) {
	n.status.Add(1)
	return n.fakeNode.Status(ctx)
}

func (n *f06Node) TxIndexed(ctx context.Context, h string) (bool, error) {
	n.txIndexed.Add(1)
	if n.txErr != nil {
		return false, n.txErr
	}
	return n.fakeNode.TxIndexed(ctx, h)
}

// f06Pacer builds the pacer from parsed flags and keeps the production sleepCtx.
// ok=false means parseConfig refused the flags (the desired outcome for poll<=0).
func f06Pacer(t *testing.T, poll string) (*Pacer, *f06Node, bool) {
	t.Helper()
	cfg, err := parseConfig([]string{"--max-step", "300", "--min-retain", "1000", "--poll", poll})
	if err != nil {
		t.Logf("--poll %s rejected: %v", poll, err)
		return nil, nil, false
	}
	n := &f06Node{fakeNode: newFakeNode(1000, 5000, 0, 0)}
	return NewPacer(cfg, n, slog.New(slog.NewTextHandler(io.Discard, nil))), n, true
}

const f06Window = 20 * time.Millisecond

// A paused pacer (catching_up) must not hammer the node RPC: over 20ms, at
// most a handful of Status calls for any accepted --poll value.
func TestF06PausedLoopIsPaced(t *testing.T) {
	for _, poll := range []string{"5ms", "0"} { // 5ms = control: harness counts calls
		p, n, ok := f06Pacer(t, poll)
		if !ok {
			continue
		}
		n.catching = true
		ctx, cancel := context.WithTimeout(context.Background(), f06Window)
		_ = p.Run(ctx)
		cancel()
		got := n.status.Load()
		t.Logf("--poll %s, catching_up: Status called %d times in %v", poll, got, f06Window)
		if got == 0 {
			t.Fatalf("--poll %s: Status never called, test is vacuous", poll)
		}
		if got > 10 {
			t.Errorf("--poll %s: busy loop, %d Status calls in %v", poll, got, f06Window)
		}
	}
}

// waitGone retries a failing witness check after sleep(Poll) (F1 fix). While
// the node RPC is down that retry must stay paced too.
func TestF06WitnessErrorRetryIsPaced(t *testing.T) {
	for _, poll := range []string{"5ms", "0"} {
		p, n, ok := f06Pacer(t, poll)
		if !ok {
			continue
		}
		n.txErr = errors.New("dial tcp 127.0.0.1:26657: connection refused")
		ctx, cancel := context.WithTimeout(context.Background(), f06Window)
		err := p.waitGone(ctx, witness{txHashes: []string{"TX1299A"}}, 1300, true)
		cancel()
		got := n.txIndexed.Load()
		t.Logf("--poll %s, node down: TxIndexed called %d times in %v (waitGone err=%v)", poll, got, f06Window, err)
		if got == 0 {
			t.Fatalf("--poll %s: TxIndexed never called, test is vacuous", poll)
		}
		if got > 10 {
			t.Errorf("--poll %s: busy retry, %d TxIndexed calls in %v", poll, got, f06Window)
		}
	}
}

// A malformed PACER_* value must fail startup, not fall back to a default:
// PACER_DRY_RUN=1 used to mean a real run.
func TestBadEnvValueFails(t *testing.T) {
	for k, v := range map[string]string{
		"PACER_DRY_RUN":      "yes",
		"PACER_START_HEIGHT": "1,000,000",
		"PACER_WAIT":         "abc",
		"PACER_MAX_STEP":     "abc",
	} {
		t.Run(k, func(t *testing.T) {
			t.Setenv("PACER_MAX_STEP", "100")
			t.Setenv("PACER_MIN_RETAIN", "100")
			t.Setenv(k, v)
			_, err := parseConfig(nil)
			if err == nil || !strings.Contains(err.Error(), k) {
				t.Fatalf("%s=%q: want an error naming %s, got %v", k, v, k, err)
			}
		})
	}
}

func TestDryRunEnvBool(t *testing.T) {
	for v, want := range map[string]bool{"1": true, "true": true, "0": false, "false": false} {
		t.Setenv("PACER_MAX_STEP", "100")
		t.Setenv("PACER_MIN_RETAIN", "100")
		t.Setenv("PACER_DRY_RUN", v)
		c, err := parseConfig(nil)
		if err != nil || c.DryRun != want {
			t.Fatalf("PACER_DRY_RUN=%s: DryRun=%v err=%v, want %v", v, c != nil && c.DryRun, err, want)
		}
	}
}

// Arguments left after the flags used to be ignored: "--max-step 5 run"
// dropped "run", and a mistyped subcommand stopped flag parsing silently.
func TestLeftoverArgsRejected(t *testing.T) {
	for _, args := range [][]string{
		{"--max-step", "5", "--min-retain", "5", "run"},
		{"rnu", "--max-step", "5", "--min-retain", "5"},
	} {
		if _, err := parseConfig(args); err == nil {
			t.Errorf("%q accepted", args)
		}
	}
}

// Operators copy laddr from config.toml ("tcp://127.0.0.1:26659"); gRPC
// rejected it with "too many colons in address" and the pacer reported C1.
func TestGRPCAddrAcceptsTCPScheme(t *testing.T) {
	c, err := parseConfig([]string{"--max-step", "5", "--min-retain", "5", "--grpc-addr", "tcp://127.0.0.1:26659"})
	if err != nil || c.GRPCAddr != "127.0.0.1:26659" {
		t.Fatalf("GRPCAddr %q err=%v", c.GRPCAddr, err)
	}
}
