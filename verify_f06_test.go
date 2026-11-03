package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

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
