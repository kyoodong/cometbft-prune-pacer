package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
func (f *fakeNode) MemAvailable() (uint64, error)                { return f.memAvail, nil }
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

// C6: a retain height far above the lowest indexed height must stop startup.
func TestPreflightRejectsLargeExistingTarget(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	f.retain[RetainTxIndexer] = 3000
	cfg := &Config{MaxStep: 300, MinRetain: 1000}
	err := preflight(context.Background(), cfg, f, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var cf *checkFailure
	if !errors.As(err, &cf) || cf.ID != "C6" {
		t.Fatalf("want C6 failure, got %v", err)
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
