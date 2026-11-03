package main

import (
	"context"
	"io"
	"log/slog"
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
