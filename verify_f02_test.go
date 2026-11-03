package main

import (
	"context"
	"testing"
	"time"
)

// cometPruner models the node-side kv pruning confirmed against CometBFT
// v1.0.1 (state/txindex/kv/kv.go Prune): the lower bound comes from
// LastTxIndexerRetainHeightKey, which is written only on the delete-error
// path, so it stays 1 and one Prune(target) deletes every tx below target.
// Unlike fakeNode.tick it does not assume the tx index and block index start
// at the same height.
type cometPruner struct {
	*fakeNode
	maxTxHeightsPerPrune int64
}

func (c *cometPruner) prune() {
	var n int64
	for h := range c.txs {
		if h < c.retain[RetainTxIndexer] {
			delete(c.txs, h)
			n++
		}
	}
	for h := range c.blocks {
		if h < c.retain[RetainBlockIndexer] {
			delete(c.blocks, h)
		}
	}
	c.maxTxHeightsPerPrune = max(c.maxTxHeightsPerPrune, n)
}

// F2: whatever the first step's base is, the node must never be asked to
// prune more than max-step tx heights at once. Refusing to start (an error,
// no Set) is also correct.
func TestVerifyF02FirstStepBoundedByLowestTx(t *testing.T) {
	cases := []struct {
		name  string
		setup func(f *fakeNode, p *Pacer)
	}{
		{"block index starts at 1500, tx index at 1000", func(f *fakeNode, _ *Pacer) {
			for h := int64(1000); h < 1500; h++ {
				delete(f.blocks, h)
			}
		}},
		{"one-height block-index hole at 2500 hit by the binary search", func(f *fakeNode, _ *Pacer) {
			delete(f.blocks, 2500)
		}},
		{"--start-height=3000 above lowest indexed 1000", func(_ *fakeNode, p *Pacer) {
			p.cfg.StartHeight = 3000
		}},
		{"control: contiguous indexes from 1000, auto start", func(*fakeNode, *Pacer) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeNode(1000, 5000, 0, 0)
			c := &cometPruner{fakeNode: f}
			p := testPacer(f, 300, 1000)
			p.node = c
			p.sleep = func(ctx context.Context, _ time.Duration) error { c.prune(); return ctx.Err() }
			tc.setup(f, p)
			err := p.step(context.Background())
			t.Logf("start=%d setHistory=%v err=%v txHeightsPrunedAtOnce=%d (max-step 300)",
				p.start, f.setHistory, err, c.maxTxHeightsPerPrune)
			if len(f.setHistory) == 0 && err == nil {
				t.Fatal("vacuous: no Set and no error")
			}
			if c.maxTxHeightsPerPrune > 300 {
				t.Fatalf("first Set %v made the node prune %d tx heights at once (> max-step 300)",
					f.setHistory, c.maxTxHeightsPerPrune)
			}
		})
	}
}
