package main

import (
	"context"
	"testing"
	"time"
)

// SIGTERM while resume() waits for an in-flight step is a clean shutdown,
// same as during the main loop; it used to return context.Canceled (exit 1).
func TestShutdownDuringResumeIsClean(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	f.retain[RetainTxIndexer], f.retain[RetainBlockIndexer] = 1300, 1300 // Set, not yet pruned
	f.lag = 1 << 30
	f.pending = f.lag
	p := testPacer(f, 300, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	p.sleep = func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() }
	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run after SIGTERM during resume: %v", err)
	}
}
