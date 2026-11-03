package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

// recordingSentry wraps a sentryHandler with a fake clock and records sends.
func recordingSentry(now *time.Time) (*sentryHandler, *[]sent) {
	var got []sent
	h := newSentryHandler(slog.NewTextHandler(io.Discard, nil))
	h.now = func() time.Time { return *now }
	h.send = func(l sentry.Level, m string, f map[string]any) { got = append(got, sent{l, m, f}) }
	return h, &got
}

// F9: run the real preflight with a home whose config exposes the privileged
// gRPC (C1), turns block pruning off (C7) and a node without Prometheus (C5).
// Each distinct startup warning must reach Sentry.
func TestVerifyF09PreflightWarningsEachReachSentry(t *testing.T) {
	home := t.TempDir()
	if err := os.MkdirAll(filepath.Join(home, "config"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfgToml := `[tx_index]
indexer = "kv"
[storage.pruning.data_companion]
enabled = true
[grpc.privileged]
laddr = "tcp://0.0.0.0:26659"
[grpc.privileged.pruning_service]
enabled = true
`
	if err := os.WriteFile(filepath.Join(home, "config", "config.toml"), []byte(cfgToml), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "config", "app.toml"), []byte("min-retain-blocks = 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	now := time.Unix(0, 0)
	h, got := recordingSentry(&now)
	var logs strings.Builder
	h.next = slog.NewTextHandler(&logs, nil)
	f := newFakeNode(1000, 5000, 0, 0) // Metric() returns false -> C5
	cfg := &Config{MaxStep: 300, MinRetain: 1000, Home: home, GRPCAddr: "127.0.0.1:26659"}
	if err := preflight(context.Background(), cfg, f, slog.New(h)); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	// Guard: all three warnings were actually logged.
	if n := strings.Count(logs.String(), "msg=check_warn"); n != 3 {
		t.Fatalf("want 3 check_warn log lines, got %d:\n%s", n, logs.String())
	}
	sentIDs := map[string]bool{}
	for _, s := range *got {
		t.Logf("sent: %s id=%v msg=%v", s.msg, s.fields["id"], s.fields["msg"])
		if s.msg == "check_warn" {
			sentIDs[fmt.Sprint(s.fields["id"])] = true
		}
	}
	if !sentIDs["C1"] {
		t.Errorf("C1 (privileged gRPC exposed) did not reach Sentry")
	}
	for _, id := range []string{"C7", "C5"} {
		if !sentIDs[id] {
			t.Errorf("check_warn %s did not reach Sentry (sent ids: %v)", id, sentIDs)
		}
	}
}

// F9 (2): keepBlocks fails for block and block_results; both must reach Sentry.
type failBlocksNode struct{ *fakeNode }

func (n failBlocksNode) SetRetain(ctx context.Context, r Retain, h int64) error {
	if r == RetainBlock || r == RetainBlockResults {
		return errors.New("height is below the store base")
	}
	return n.fakeNode.SetRetain(ctx, r, h)
}

func TestVerifyF09RetainSetFailedPerRetain(t *testing.T) {
	now := time.Unix(0, 0)
	h, got := recordingSentry(&now)
	f := newFakeNode(1000, 5000, 0, 0)
	p := testPacer(f, 300, 1000)
	p.node = failBlocksNode{f}
	p.log = slog.New(h)
	p.keepBlocks(context.Background(), 4000)

	retains := map[string]bool{}
	for _, s := range *got {
		t.Logf("sent: %s retain=%v err=%v", s.msg, s.fields["retain"], s.fields["err"])
		if s.msg == "retain_set_failed" {
			retains[fmt.Sprint(s.fields["retain"])] = true
		}
	}
	if len(retains) != 2 {
		t.Fatalf("want retain_set_failed for both block retains, got %v", retains)
	}
}

// F9 (noise guard): step_error every 30s for an hour, with an error string
// that carries a changing height, must stay throttled to one report.
func TestVerifyF09StepErrorStaysThrottled(t *testing.T) {
	now := time.Unix(0, 0)
	h, got := recordingSentry(&now)
	log := slog.New(h)
	for i := 0; i < 120; i++ { // 120 x 30s = 1h
		log.Error("step_error", "err", fmt.Sprintf("set tx_indexer=%d: rpc error: connection refused", 1300+300*i))
		now = now.Add(30 * time.Second)
	}
	n := 0
	for _, s := range *got {
		if s.msg == "step_error" {
			n++
		}
	}
	t.Logf("step_error logged 120 times over 1h, sent %d", n)
	if n != 1 {
		t.Fatalf("want 1 step_error report per hour, got %d", n)
	}
}
