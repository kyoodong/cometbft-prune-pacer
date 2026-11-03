package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type sent struct {
	level  sentry.Level
	msg    string
	fields map[string]any
}

// Warn/Error go to Sentry, Info does not, and repeats are throttled.
func TestSentryHandlerForwardsAndThrottles(t *testing.T) {
	var got []sent
	now := time.Unix(0, 0)
	h := newSentryHandler(slog.NewTextHandler(io.Discard, nil))
	h.now = func() time.Time { return now }
	h.send = func(l sentry.Level, m string, f map[string]any) { got = append(got, sent{l, m, f}) }
	log := slog.New(h).With("node", "q2")

	log.Info("step_done", "target", 1)
	log.Warn("step_stuck", "target", 2)
	log.Error("step_error", "err", "boom")
	log.Warn("paused_low_memory", "mem_avail_gib", 3)
	log.Warn("paused_low_memory", "mem_avail_gib", 2) // throttled
	now = now.Add(sentryThrottle + time.Second)
	log.Warn("paused_low_memory", "mem_avail_gib", 1)

	if len(got) != 4 {
		t.Fatalf("want 4 reports, got %d: %+v", len(got), got)
	}
	if got[0].msg != "step_stuck" || got[0].level != sentry.LevelWarning || got[0].fields["node"] != "q2" {
		t.Fatalf("bad first report: %+v", got[0])
	}
	if got[1].level != sentry.LevelError || got[1].fields["err"] != "boom" {
		t.Fatalf("bad error report: %+v", got[1])
	}
}

// A completed step moves the counters and gauges Grafana will chart.
func TestMetricsFollowSteps(t *testing.T) {
	f := newFakeNode(1000, 5000, 1000, 1300) // first step range has no txs
	p := testPacer(f, 300, 1000)
	p.m = newMetrics()
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := p.step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if v := testutil.ToFloat64(p.m.steps); v != 2 {
		t.Fatalf("steps_total %v", v)
	}
	if v := testutil.ToFloat64(p.m.emptySteps); v != 1 {
		t.Fatalf("empty_steps_total %v", v)
	}
	if v := testutil.ToFloat64(p.m.target); v != 1600 {
		t.Fatalf("target_height %v", v)
	}
	if v := testutil.ToFloat64(p.m.backlog); v != 4000-1600 {
		t.Fatalf("backlog_blocks %v", v)
	}
	if v := testutil.ToFloat64(p.m.inProgress); v != 0 {
		t.Fatalf("step_in_progress %v", v)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	m := newMetrics()
	m.observeTarget(10, 30)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.serve(ctx, "127.0.0.1:26671", slog.New(slog.NewTextHandler(io.Discard, nil)))
	var body string
	for i := 0; i < 50; i++ {
		resp, err := http.Get("http://127.0.0.1:26671/metrics")
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			body = string(b)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, want := range []string{"pacer_target_height 10", "pacer_backlog_blocks 20", `pacer_paused{reason="ceiling"} 0`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
}
