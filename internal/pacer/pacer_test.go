package pacer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// fakeNode simulates the parts of a CometBFT node the pacer touches.
// Pruning is asynchronous: after a Set, entries below the target disappear
// only after `lag` poll sleeps, like the real 10s pruner loop.
type fakeNode struct {
	latest     int64
	catching   bool
	memAvail   uint64
	txs        map[int64][]string // height -> tx hashes still indexed
	blocks     map[int64]bool     // height -> block index entry exists
	retain     map[Retain]int64
	lag        int
	pending    int
	setHistory []int64 // tx indexer targets in order
	maxInMem   int64   // largest number of heights pruned in one go
}

func newFakeNode(low, latest int64, emptyFrom, emptyTo int64) *fakeNode {
	f := &fakeNode{latest: latest, memAvail: 64 << 30, txs: map[int64][]string{}, blocks: map[int64]bool{}, retain: map[Retain]int64{}}
	for h := low; h <= latest; h++ {
		f.blocks[h] = true
		if h < emptyFrom || h >= emptyTo {
			f.txs[h] = []string{fmt.Sprintf("TX%dA", h), fmt.Sprintf("TX%dB", h)}
		}
	}
	return f
}

func (f *fakeNode) Status(context.Context) (int64, int64, bool, error) {
	return f.latest, 0, f.catching, nil
}

func (f *fakeNode) TxHashesAt(_ context.Context, h int64) ([]string, error) { return f.txs[h], nil }

func (f *fakeNode) TxIndexed(_ context.Context, hash string) (bool, error) {
	for _, hs := range f.txs {
		for _, x := range hs {
			if x == hash {
				return true, nil
			}
		}
	}
	return false, nil
}

func (f *fakeNode) BlockIndexed(_ context.Context, h int64) (bool, error) { return f.blocks[h], nil }

func (f *fakeNode) GetRetain(_ context.Context, r Retain) (int64, bool, error) {
	h, ok := f.retain[r]
	return h, ok, nil
}

func (f *fakeNode) SetRetain(_ context.Context, r Retain, h int64) error {
	if cur, ok := f.retain[r]; ok && h < cur {
		return errors.New("cannot lower retain height")
	}
	f.retain[r] = h
	if r == RetainTxIndexer {
		f.setHistory = append(f.setHistory, h)
		f.pending = f.lag
	}
	return nil
}

func (f *fakeNode) MemAvailable() (uint64, error) { return f.memAvail, nil }

func (f *fakeNode) Metric(context.Context, string) (int64, bool) { return 0, false }

// tick runs the simulated pruner once its lag has elapsed.
func (f *fakeNode) tick() {
	if f.pending > 0 {
		f.pending--
		return
	}
	var pruned int64
	for _, r := range []Retain{RetainTxIndexer, RetainBlockIndexer} {
		target := f.retain[r]
		for h := range f.blocks {
			if h < target {
				if r == RetainTxIndexer {
					if _, ok := f.txs[h]; ok {
						pruned++
					}
					delete(f.txs, h)
				} else {
					delete(f.blocks, h)
				}
			}
		}
	}
	f.maxInMem = max(f.maxInMem, pruned)
}

func testPacer(f *fakeNode, maxStep, minRetain int64) *Pacer {
	cfg := &Config{MaxStep: maxStep, MinRetain: minRetain, Poll: time.Second, Wait: time.Minute, StepTimeout: time.Hour, MinMemAvail: 30 << 30}
	p := NewPacer(cfg, f, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.sleep = func(ctx context.Context, _ time.Duration) error { f.tick(); return ctx.Err() }
	return p
}

// Every step stays within MaxStep and never starts before the previous one is done.
func TestStepsAreBoundedAndSequential(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	f.lag = 3
	p := testPacer(f, 300, 1000)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if err := p.step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	prev := int64(1000)
	for _, h := range f.setHistory {
		if h-prev > 300 {
			t.Fatalf("step %d -> %d exceeds max-step", prev, h)
		}
		prev = h
	}
	if f.maxInMem > 300 {
		t.Fatalf("pruner handled %d heights at once", f.maxInMem)
	}
	if last := f.setHistory[len(f.setHistory)-1]; last != 4000 {
		t.Fatalf("expected to stop at ceiling 4000, got %d", last)
	}
}

// A step range with no txs completes without waiting forever.
func TestEmptyRangeDoesNotHang(t *testing.T) {
	f := newFakeNode(1000, 5000, 1000, 1600) // no txs in [1000,1600)
	p := testPacer(f, 300, 1000)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := p.step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.setHistory) != 3 || f.setHistory[2] != 1900 {
		t.Fatalf("got %v", f.setHistory)
	}
}

func TestPausesOnLowMemoryAndCatchingUp(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	p := testPacer(f, 300, 1000)
	f.memAvail = 1 << 30
	_ = p.step(context.Background())
	f.memAvail, f.catching = 64<<30, true
	_ = p.step(context.Background())
	if len(f.setHistory) != 0 {
		t.Fatalf("set while paused: %v", f.setHistory)
	}
}

func TestBlocksKeptAtCeiling(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	p := testPacer(f, 300, 1000)
	_ = p.step(context.Background())
	if f.retain[RetainBlock] != 4000 || f.retain[RetainBlockResults] != 4000 {
		t.Fatalf("block retain %d, results %d", f.retain[RetainBlock], f.retain[RetainBlockResults])
	}
}

func TestLowestIndexed(t *testing.T) {
	f := newFakeNode(1234, 9000, 0, 0)
	got, err := lowestIndexed(context.Background(), f, 9000)
	if err != nil || got != 1234 {
		t.Fatalf("got %d, %v", got, err)
	}
}

// After a daemon restart mid-step, the pacer waits for that step first.
func TestResumeWaitsForRunningStep(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	f.retain[RetainTxIndexer], f.retain[RetainBlockIndexer] = 1300, 1300
	f.lag = 2
	p := testPacer(f, 300, 1000)
	if err := p.resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.txs[1299]; ok {
		t.Fatal("resume returned before the running step finished")
	}
}

// blipNode is a fakeNode whose TxIndexed fails `fail` times (node restarting,
// connection refused). Every tx-indexer Set records whether the previous
// step's range was still indexed at that moment (an S2 violation).
type blipNode struct {
	*fakeNode
	fail       int
	violations []string
}

func (n *blipNode) TxIndexed(ctx context.Context, h string) (bool, error) {
	if n.fail > 0 {
		n.fail--
		return false, errors.New("dial tcp 127.0.0.1:26657: connect: connection refused")
	}
	return n.fakeNode.TxIndexed(ctx, h)
}

func (n *blipNode) SetRetain(ctx context.Context, r Retain, h int64) error {
	if prev, ok := n.retain[RetainTxIndexer]; ok && r == RetainTxIndexer {
		for th := range n.txs {
			if th < prev {
				n.violations = append(n.violations, fmt.Sprintf("Set(%d) while tx at %d < previous target %d still indexed", h, th, prev))
				break
			}
		}
	}
	return n.fakeNode.SetRetain(ctx, r, h)
}

// F1: one transient error while waiting for a step must not let Run issue
// the next Set before that step is proven done (S2), nor make the node
// prune more than max-step heights at once (S1).
func TestVerifyF01TransientErrorInWaitGoneKeepsS2(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	f.lag = 3
	n := &blipNode{fakeNode: f, fail: 1}
	p := testPacer(f, 300, 1000)
	p.node = n
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sleeps := 0
	p.sleep = func(ctx context.Context, _ time.Duration) error {
		f.tick()
		if sleeps++; sleeps >= 40 {
			cancel()
		}
		return ctx.Err()
	}
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("setHistory=%v maxInMem=%d violations=%v", f.setHistory, f.maxInMem, n.violations)
	if len(f.setHistory) < 2 {
		t.Fatalf("vacuous: only %d Set(s) issued", len(f.setHistory))
	}
	if len(n.violations) > 0 {
		t.Errorf("S2 violated: %s", n.violations[0])
	}
	if f.maxInMem > 300 {
		t.Errorf("S1 violated: node pruned %d heights at once (max-step 300)", f.maxInMem)
	}
}

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

// The indexer runs behind the block store; a few unindexed heights at the
// top must not make the lowest-height search fail.
func TestLowestIndexedWithIndexerLag(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	for h := int64(4995); h <= 5000; h++ {
		delete(f.blocks, h) // indexer 6 blocks behind
	}
	low, err := lowestIndexed(context.Background(), f, 5000)
	if err != nil || low != 1000 {
		t.Fatalf("lowestIndexed = %d, %v; want 1000", low, err)
	}
}

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

type baseNode struct {
	*fakeNode
	earliest int64
}

func (n baseNode) Status(ctx context.Context) (int64, int64, bool, error) {
	l, _, c, err := n.fakeNode.Status(ctx)
	return l, n.earliest, c, err
}

// When the block store base is already above the ceiling there is nothing to
// prune; Set would be refused (heightWithinBounds) and warn on every step.
func TestKeepBlocksSkipsBelowStoreBase(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	p := testPacer(f, 300, 1000) // ceiling 4000
	p.node = baseNode{f, 4500}
	var logs strings.Builder
	p.log = slog.New(slog.NewTextHandler(&logs, nil))
	_ = p.step(context.Background())
	if _, set := f.retain[RetainBlock]; set {
		t.Errorf("block retain set to %d below the store base 4500", f.retain[RetainBlock])
	}
}
