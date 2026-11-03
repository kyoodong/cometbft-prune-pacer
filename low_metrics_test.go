package main

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

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
