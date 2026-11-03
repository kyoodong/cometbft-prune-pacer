package pacer

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Pacer raises the indexer retain heights one bounded step at a time.
//
// Why: CometBFT v1's kv TxIndex.Prune loads every tx in
// [lastRetain, target) into memory before deleting (Search without
// pagination). A large jump in the retain height therefore OOM-kills the
// node. Keeping each step <= MaxStep blocks, and starting the next step only
// after the previous one is verifiably done, bounds that memory.
type Pacer struct {
	cfg   *Config
	node  Node
	log   *slog.Logger
	m     *metrics // nil = metrics off
	sleep func(ctx context.Context, d time.Duration) error
	now   func() time.Time

	start int64 // lowest indexed height, resolved once
}

// NewPacer returns a pacer with metrics off; see ServeMetrics.
func NewPacer(cfg *Config, n Node, log *slog.Logger) *Pacer {
	return &Pacer{cfg: cfg, node: n, log: log, sleep: sleepCtx, now: time.Now}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// witness is the last thing a step must delete. When the tx witness is gone,
// every tx below it in the same step is gone too, because tx Prune deletes in
// ascending height order and flushes every 1000 entries. Within one height the
// order is unspecified (sort.Sort by height only), so every tx at the witness
// height must be gone, not just one. The block witness is weaker: block Prune
// walks keys in key order, so lower-height event keys may still remain; it
// does not hold the range in memory, so that does not affect the step bound.
type witness struct {
	txHashes    []string // empty = no tx in the step range, nothing to wait for
	blockHeight int64    // 0 = nothing to wait for
}

// Run loops until ctx is cancelled.
func (p *Pacer) Run(ctx context.Context) error {
	if err := p.resume(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	for {
		err := p.step(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			p.m.loopError()
			p.m.setPaused("") // the reason is unknown until the next step gets through
			p.log.Error("step_error", "err", err.Error())
			if err := p.sleep(ctx, p.cfg.Poll); err != nil {
				return nil
			}
		}
	}
}

// resume waits for a step that may still be running from before a restart:
// the retain height is persisted by the node, the witness is not.
func (p *Pacer) resume(ctx context.Context) error {
	cur, ok, err := p.indexTarget(ctx)
	if err != nil || !ok {
		return err
	}
	// Show the real heights while waiting; the gauges start at 0 in a new process.
	if latest, _, _, err := p.node.Status(ctx); err == nil {
		ceiling := latest - p.cfg.MinRetain
		p.m.observeHeights(latest, ceiling)
		p.m.observeTarget(cur, ceiling)
	}
	w, err := p.pickWitness(ctx, max(cur-p.cfg.MaxStep, 1), cur)
	if err != nil {
		return err
	}
	p.log.Info("resume", "target", cur, "tx_witness", len(w.txHashes), "block_witness", w.blockHeight)
	// Not counted as a step: it was Set by an earlier process, and its
	// duration here would be measured from this process's start.
	return p.waitGone(ctx, w, cur, false)
}

// indexTarget returns the lower of the two indexer retain heights.
func (p *Pacer) indexTarget(ctx context.Context) (int64, bool, error) {
	tx, txOK, err := p.node.GetRetain(ctx, RetainTxIndexer)
	if err != nil {
		return 0, false, err
	}
	blk, blkOK, err := p.node.GetRetain(ctx, RetainBlockIndexer)
	if err != nil {
		return 0, false, err
	}
	switch {
	case txOK && blkOK:
		return min(tx, blk), true, nil
	case txOK:
		return tx, true, nil
	case blkOK:
		return blk, true, nil
	}
	return 0, false, nil
}

func (p *Pacer) step(ctx context.Context) error {
	latest, earliest, catching, err := p.node.Status(ctx)
	if err != nil {
		return err
	}
	ceiling := latest - p.cfg.MinRetain
	p.m.observeHeights(latest, ceiling)
	if catching {
		p.m.setPaused("catching_up")
		p.log.Warn("paused_catching_up", "latest", latest)
		return p.sleep(ctx, p.cfg.Poll)
	}
	if avail, err := p.node.MemAvailable(); err == nil {
		p.m.setMemAvail(avail)
		if avail < p.cfg.MinMemAvail {
			p.m.setPaused("low_memory")
			p.log.Warn("paused_low_memory", "mem_avail_gib", avail>>30)
			return p.sleep(ctx, p.cfg.Poll)
		}
	}
	if ceiling <= 0 {
		p.m.setPaused("ceiling")
		p.log.Info("ceiling_reached", "latest", latest, "ceiling", ceiling)
		return p.sleep(ctx, p.cfg.Wait)
	}

	p.keepBlocks(ctx, ceiling, earliest)

	cur, ok, err := p.indexTarget(ctx)
	if err != nil {
		return err
	}
	if !ok {
		if cur, err = p.startHeight(ctx, latest); err != nil {
			return err
		}
	}
	p.m.observeTarget(cur, ceiling)
	next := min(cur+p.cfg.MaxStep, ceiling)
	if next <= cur {
		p.m.setPaused("ceiling")
		p.log.Info("ceiling_reached", "latest", latest, "ceiling", ceiling, "target", cur)
		return p.sleep(ctx, p.cfg.Wait)
	}

	p.m.setPaused("")
	w, err := p.pickWitness(ctx, cur, next)
	if err != nil {
		return err
	}
	p.log.Info("step_set", "latest", latest, "ceiling", ceiling, "prev", cur, "target", next,
		"tx_witness", len(w.txHashes), "block_witness", w.blockHeight, "dry_run", p.cfg.DryRun)
	if p.cfg.DryRun {
		return p.sleep(ctx, p.cfg.Wait)
	}
	for _, r := range []Retain{RetainTxIndexer, RetainBlockIndexer} {
		if err := p.raise(ctx, r, next); err != nil {
			return err
		}
	}
	p.m.observeTarget(next, ceiling)
	return p.waitGone(ctx, w, next, true)
}

// keepBlocks moves the block and block-results retain heights to the
// ceiling. Block pruning of millions of heights at once did not OOM in
// practice, so no stepping is needed here.
func (p *Pacer) keepBlocks(ctx context.Context, ceiling, earliest int64) {
	// The block store base (earliest) is already above the ceiling: nothing to
	// prune, and the node refuses a height below its base.
	if ceiling < earliest {
		return
	}
	for _, r := range []Retain{RetainBlock, RetainBlockResults} {
		if p.cfg.DryRun {
			continue
		}
		if err := p.raise(ctx, r, ceiling); err != nil {
			p.log.Warn("retain_set_failed", "retain", r.String(), "height", ceiling, "err", err.Error())
		}
	}
}

// raise sets a retain height unless it is already at or above h; the node
// refuses to lower a retain height.
func (p *Pacer) raise(ctx context.Context, r Retain, h int64) error {
	cur, ok, err := p.node.GetRetain(ctx, r)
	if err != nil {
		return err
	}
	if ok && cur >= h {
		return nil
	}
	if err := p.node.SetRetain(ctx, r, h); err != nil {
		return fmt.Errorf("set %s=%d: %w", r, h, err)
	}
	return nil
}

// witnessLeft reports whether any tx witness or the block witness is still indexed.
func (p *Pacer) witnessLeft(ctx context.Context, w witness) (txLeft, blkLeft bool, err error) {
	for _, h := range w.txHashes {
		if txLeft, err = p.node.TxIndexed(ctx, h); err != nil || txLeft {
			return txLeft, false, err
		}
	}
	if w.blockHeight > 0 {
		blkLeft, err = p.node.BlockIndexed(ctx, w.blockHeight)
	}
	return false, blkLeft, err
}

// startHeight is the base of the very first step.
func (p *Pacer) startHeight(ctx context.Context, latest int64) (int64, error) {
	if p.start > 0 {
		return p.start, nil
	}
	low := p.cfg.StartHeight
	if low <= 0 {
		var err error
		if low, err = lowestIndexed(ctx, p.node, latest); err != nil {
			return 0, err
		}
	}
	// The node prunes every tx below the target, not just [base, target), so a
	// base above the real start of the tx index would make the first step huge.
	// ponytail: only checks base-1; a gap that is empty exactly there slips through.
	if hs, err := p.node.TxHashesAt(ctx, low-1); err != nil {
		return 0, err
	} else if len(hs) > 0 {
		return 0, fmt.Errorf("tx index has txs at %d, below the first step base %d; pass a lower --start-height", low-1, low)
	}
	p.log.Info("start_height", "base", low)
	p.start = low
	return low, nil
}

// lowestIndexed binary-searches the lowest height with a block-index entry.
// The block indexer writes a height key for every block, so indexed heights
// form one contiguous range ending near latest. The tx index starts at the same
// height (both are written for the same blocks).
func lowestIndexed(ctx context.Context, n Node, latest int64) (int64, error) {
	// The indexer runs behind the block store, so look a little further back
	// for an indexed height to search below.
	hi := int64(0)
	for _, back := range []int64{1, 10, 100, 1000} {
		ok, err := n.BlockIndexed(ctx, latest-back)
		if err != nil {
			return 0, err
		}
		if ok {
			hi = latest - back
			break
		}
	}
	if hi <= 0 {
		return 0, fmt.Errorf("no block index entry in the last 1000 heights below %d: indexer far behind, or [tx_index] indexer is not \"kv\"", latest)
	}
	lo := int64(1)
	for lo < hi {
		mid := lo + (hi-lo)/2
		ok, err := n.BlockIndexed(ctx, mid)
		if err != nil {
			return 0, err
		}
		if ok {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo, nil
}

// pickWitness chooses what must disappear for the step [from, to) to count
// as done: the last indexed tx below `to`, and the block-index entry at to-1.
func (p *Pacer) pickWitness(ctx context.Context, from, to int64) (witness, error) {
	var w witness
	if ok, err := p.node.BlockIndexed(ctx, to-1); err != nil {
		return w, err
	} else if ok {
		w.blockHeight = to - 1
	}
	// ponytail: one tx_search per height, so an empty range (or resume after a
	// finished step) costs up to MaxStep local RPC calls. A range query would
	// be one call but scans the whole tx.height index; go there only if this
	// shows up in the node's RPC latency.
	for h := to - 1; h >= from; h-- {
		hashes, err := p.node.TxHashesAt(ctx, h)
		if err != nil {
			return w, err
		}
		if len(hashes) > 0 {
			w.txHashes = hashes
			break
		}
	}
	if len(w.txHashes) == 0 {
		p.log.Info("step_empty", "from", from, "to", to)
	}
	return w, nil
}

// waitGone polls until both witnesses are deleted. It never gives up: moving
// on early would let the next step merge with leftovers and exceed MaxStep.
// count records the step in steps_total and step_duration_seconds.
func (p *Pacer) waitGone(ctx context.Context, w witness, target int64, count bool) error {
	began := p.now()
	warned := false
	p.m.stepStarted()
	for {
		txLeft, blkLeft, err := p.witnessLeft(ctx, w)
		if err != nil {
			// Keep the witness and retry: returning would let the next step
			// Set a new target before this one is proven done (S2).
			p.log.Warn("witness_check_failed", "target", target, "err", err.Error())
			if err := p.sleep(ctx, p.cfg.Poll); err != nil {
				return err
			}
			continue
		}
		if !txLeft && !blkLeft {
			txBase, _ := p.node.Metric(ctx, "tx_indexer_base_height")
			blkBase, _ := p.node.Metric(ctx, "block_indexer_base_height")
			p.m.stepDone(p.now().Sub(began), len(w.txHashes) == 0, count)
			p.log.Info("step_done", "target", target, "seconds", int(p.now().Sub(began).Seconds()),
				"tx_base", txBase, "block_base", blkBase)
			return nil
		}
		if !warned && p.now().Sub(began) > p.cfg.StepTimeout {
			p.log.Warn("step_stuck", "target", target, "tx_left", txLeft, "block_left", blkLeft,
				"waiting_seconds", int(p.now().Sub(began).Seconds()))
			warned = true
			p.m.stepStuck()
		}
		if err := p.sleep(ctx, p.cfg.Poll); err != nil {
			return err
		}
	}
}
