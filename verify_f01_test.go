package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

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
