package main

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

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
