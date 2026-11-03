package main

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

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
