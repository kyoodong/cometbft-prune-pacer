package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

// F3 (mechanism): C6 must stop startup when the tx indexer retain height is
// far above the lowest indexed height, even if the block indexer retain
// height is low. indexTarget returns min(tx, block), which hides it.
func TestVerifyF03PreflightCatchesLargeTxRetain(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	f.retain[RetainTxIndexer], f.retain[RetainBlockIndexer] = 3000, 1000
	cfg := &Config{MaxStep: 300, MinRetain: 1000}
	err := preflight(context.Background(), cfg, f, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Logf("preflight tx=3000 blk=1000 low=1000 max-step=300: err=%v", err)
	var cf *checkFailure
	if !errors.As(err, &cf) || cf.ID != "C6" {
		t.Fatalf("want C6 failure, got %v", err)
	}
}

// F3 (severity): with tx retain already at 3000 the node's own index pruner
// (state/pruner.go pruneIndexesRoutine: tx Prune first, then block Prune, in
// one goroutine, re-run on every node start) deletes [1,3000) by itself.
// The question is whether running the pacer on top makes any single Prune
// larger than the node alone would. Compare node-only vs node+pacer.
func TestVerifyF03PacerDoesNotWorsenLargeTxRetain(t *testing.T) {
	run := func(withPacer, nodePrunedBeforeStart bool) *cometPruner {
		f := newFakeNode(1000, 5000, 0, 0)
		f.retain[RetainTxIndexer], f.retain[RetainBlockIndexer] = 3000, 1000
		c := &cometPruner{fakeNode: f}
		if nodePrunedBeforeStart {
			c.prune() // the node's pruner already ran its first tick
		}
		if !withPacer {
			for i := 0; i < 50; i++ {
				c.prune()
			}
			return c
		}
		p := testPacer(f, 300, 1000)
		p.node = c
		p.sleep = func(ctx context.Context, _ time.Duration) error { c.prune(); return ctx.Err() }
		ctx := context.Background()
		if err := p.resume(ctx); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 15; i++ {
			if err := p.step(ctx); err != nil {
				t.Fatal(err)
			}
		}
		return c
	}

	for _, pre := range []bool{true, false} {
		node := run(false, pre)
		both := run(true, pre)
		t.Logf("nodePrunedBeforeStart=%v: max tx heights per Prune node-only=%d node+pacer=%d; pacer tx Sets=%v; final retain tx=%d blk=%d",
			pre, node.maxTxHeightsPerPrune, both.maxTxHeightsPerPrune, both.setHistory,
			both.retain[RetainTxIndexer], both.retain[RetainBlockIndexer])
		if both.retain[RetainBlockIndexer] <= 3000 || len(both.setHistory) == 0 {
			t.Fatalf("vacuous: pacer did not step past the large tx retain (blk=%d, tx sets=%v)",
				both.retain[RetainBlockIndexer], both.setHistory)
		}
		if both.maxTxHeightsPerPrune > node.maxTxHeightsPerPrune {
			t.Fatalf("pacer made a single Prune larger: %d > node-only %d",
				both.maxTxHeightsPerPrune, node.maxTxHeightsPerPrune)
		}
		prev := int64(3000)
		for _, h := range both.setHistory {
			if h-prev > 300 {
				t.Fatalf("pacer raised tx retain %d -> %d (> max-step 300)", prev, h)
			}
			prev = h
		}
	}
}
