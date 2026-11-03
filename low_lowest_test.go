package main

import (
	"context"
	"testing"
)

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
