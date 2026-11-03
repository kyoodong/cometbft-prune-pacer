package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// f11Scrape reads one unlabeled sample from the real /metrics handler.
func f11Scrape(t *testing.T, m *metrics, name string) float64 {
	rec := httptest.NewRecorder()
	promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	sc := bufio.NewScanner(rec.Body)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), name+" "); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatal(err)
			}
			return f
		}
	}
	t.Fatalf("%s not exposed", name)
	return 0
}

// f11Rate is the counter-reset rule of Prometheus extrapolatedRate
// (promql/functions.go: "if currPoint.F < prevPoint.F { resultFloat += prevPoint.F }"),
// without the edge extrapolation (a <2% factor for a full 1h window of 1m samples).
func f11Rate(v []float64, stepSeconds float64) float64 {
	inc := v[len(v)-1] - v[0]
	for i := 1; i < len(v); i++ {
		if v[i] < v[i-1] {
			inc += v[i-1]
		}
	}
	return inc / (float64(len(v)-1) * stepSeconds)
}

// The README catch-up ETA must stay close to the real ETA across a daemon
// restart in the middle of a step. One scrape per poll, one poll per minute.
func TestF11ReadmeETAAcrossRestart(t *testing.T) {
	f := newFakeNode(1_000_000, 1_100_000, 0, 0)
	f.lag = 3
	var target, backlog, truth, steps []float64
	runUntil := func(n int) {
		p := testPacer(f, 100, 1000)
		p.m = newMetrics()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		p.sleep = func(ctx context.Context, _ time.Duration) error {
			f.tick()
			target = append(target, f11Scrape(t, p.m, "pacer_target_height"))
			backlog = append(backlog, f11Scrape(t, p.m, "pacer_backlog_blocks"))
			steps = append(steps, f11Scrape(t, p.m, "pacer_steps_total"))
			truth = append(truth, float64(min(f.retain[RetainTxIndexer], f.retain[RetainBlockIndexer])))
			if len(target) >= n {
				cancel() // systemctl restart, possibly mid-step
			}
			return ctx.Err()
		}
		if err := p.Run(ctx); err != nil {
			t.Fatal(err)
		}
	}
	const restartAt, evalAt = 122, 200 // 122 = 30 steps of 4 polls + 2: mid-step
	runUntil(restartAt)
	runUntil(evalAt)

	zeros := 0
	for i := restartAt; i < evalAt; i++ {
		if target[i] == 0 {
			zeros++
		}
	}
	ceiling := float64(1_100_000 - 1000)
	var bad []string
	for _, L := range []int{124, 151, 182, 183, 199} {
		// deriv((pacer_target_height > 0)[1h:1m]): least squares over nonzero samples.
		var n, sx, sy, sxx, sxy float64
		for i := L - 59; i <= L; i++ {
			if target[i] > 0 {
				x := float64(i * 60)
				n, sx, sy, sxx, sxy = n+1, sx+x, sy+target[i], sxx+x*x, sxy+x*target[i]
			}
		}
		slope := (n*sxy - sx*sy) / (n*sxx - sx*sx)
		readme := backlog[L] / (slope * 3600)
		trueRate := (truth[L] - truth[L-59]) / (59 * 60)
		trueETA := (ceiling - truth[L]) / (trueRate * 3600)
		t.Logf("t=%dm prescribed ETA %.2f h, true ETA %.2f h", L, readme, trueETA)
		if readme < trueETA/2 || readme > trueETA*2 {
			bad = append(bad, fmt.Sprintf("t=%dm %.3f vs %.1f", L, readme, trueETA))
		}
	}
	t.Logf("zeros after restart: %d", zeros)
	if path := os.Getenv("F11_DUMP"); path != "" {
		join := func(v []float64) string {
			s := make([]string, len(v))
			for i, x := range v {
				s[i] = strconv.FormatFloat(x, 'f', -1, 64)
			}
			return strings.Join(s, " ")
		}
		body := fmt.Sprintf("load 1m\n  pacer_target_height %s\n  pacer_backlog_blocks %s\n  pacer_steps_total %s\n  truth_target_height %s\n",
			join(target), join(backlog), join(steps), join(truth))
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if len(bad) > 0 {
		t.Fatalf("prescribed ETA off by more than 2x: %v", bad)
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
	// A free port, not a fixed one: another process on a fixed port could
	// answer instead and the test would read its body.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	m.serve(ctx, addr, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var body string
	for i := 0; i < 50; i++ {
		resp, err := http.Get("http://" + addr + "/metrics")
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

// While the node catches up the pacer still knows the latest height.
func TestCatchingUpUpdatesLatest(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	f.catching = true
	p := testPacer(f, 300, 1000)
	p.m = newMetrics()
	_ = p.step(context.Background())
	if v := testutil.ToFloat64(p.m.latest); v != 5000 {
		t.Fatalf("latest_height %v while catching up, want 5000", v)
	}
}

// resume() waits for a step Set before the restart. It is not a completed
// step of this process, and the height gauges should show the real state
// while it waits instead of 0.
func TestResumeMetrics(t *testing.T) {
	f := newFakeNode(1300, 5000, 0, 0) // retain 1300 already pruned below
	f.retain[RetainTxIndexer], f.retain[RetainBlockIndexer] = 1300, 1300
	p := testPacer(f, 300, 1000)
	p.m = newMetrics()
	if err := p.resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v := testutil.ToFloat64(p.m.steps); v != 0 {
		t.Errorf("steps_total %v, want 0", v)
	}
	if n := testutil.CollectAndCount(p.m.stepSeconds); n != 1 { // one histogram series
		t.Fatalf("histogram series %d", n)
	}
	if c := histCount(t, p.m); c != 0 {
		t.Errorf("step_duration_seconds count %d, want 0", c)
	}
	for name, g := range map[string]float64{"latest": testutil.ToFloat64(p.m.latest), "target": testutil.ToFloat64(p.m.target), "backlog": testutil.ToFloat64(p.m.backlog)} {
		want := map[string]float64{"latest": 5000, "target": 1300, "backlog": 4000 - 1300}[name]
		if g != want {
			t.Errorf("%s %v, want %v", name, g, want)
		}
	}
}

func histCount(t *testing.T, m *metrics) uint64 {
	t.Helper()
	mfs, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "pacer_step_duration_seconds" {
			return mf.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	t.Fatal("histogram not found")
	return 0
}

type statusErrNode struct{ *fakeNode }

func (statusErrNode) Status(context.Context) (int64, int64, bool, error) {
	return 0, 0, false, errors.New("rpc down")
}

// A pause reason must not stay at 1 once the loop can no longer tell.
func TestPausedClearedOnError(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	f.catching = true
	p := testPacer(f, 300, 1000)
	p.m = newMetrics()
	_ = p.step(context.Background())
	p.node = statusErrNode{f}
	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	p.sleep = func(ctx context.Context, _ time.Duration) error {
		if n++; n > 1 {
			cancel()
		}
		return ctx.Err()
	}
	_ = p.Run(ctx)
	if v := testutil.ToFloat64(p.m.paused.WithLabelValues("catching_up")); v != 0 {
		t.Fatalf("paused{catching_up} %v after errors, want 0", v)
	}
}

// /metrics has no authentication; serving it beyond loopback is worth a warning.
func TestListenNotLoopbackWarns(t *testing.T) {
	var logs strings.Builder
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newMetrics().serve(ctx, "0.0.0.0:0", slog.New(slog.NewTextHandler(&logs, nil)))
	if !strings.Contains(logs.String(), "metrics_listen_not_loopback") {
		t.Fatalf("no warning:\n%s", logs.String())
	}
}
