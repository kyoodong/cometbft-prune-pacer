package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

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
