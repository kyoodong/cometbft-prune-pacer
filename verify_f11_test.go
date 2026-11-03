package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
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
