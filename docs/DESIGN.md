# Design

Status: implemented (v0.1.0), not yet run against a live node.

## Problem

- With `indexer = "kv"`, `data/tx_index.db` grows forever (observed: 1.3 TB, ~20 GB/day).
- `pruning` and `min-retain-blocks` prune blocks and app state, not indexes.
- CometBFT v1 prunes indexes when a data companion raises the indexer retain
  heights through the privileged gRPC `PruningService`.
- `kv.TxIndex.Prune` runs `Search("tx.height < target AND tx.height >= last")`
  without pagination: every tx in the range is loaded into memory first.
  - The search itself scans the whole `tx.height/` key prefix.
  - Observed: one Set covering ~11.7M blocks (~160M txs) took the node from
    13.9 to 122.4 GiB of 125 GiB and got it OOM-killed; systemd restarted it
    and it tried again.
- Retain heights cannot be lowered (`ErrPrunerCannotLowerRetainHeight`).

Code references are CometBFT v1.0.1 (`state/pruner.go`,
`state/txindex/kv/kv.go`, `state/indexer/block/kv/kv.go`); the
`InjectiveLabs/cometbft v1.0.1-inj.9` fork has identical copies of these files.

## Approach

- Raise the indexer retain heights by at most `max-step` blocks per step.
- Start the next step only after proving the previous one finished; otherwise
  leftovers and the new range are pruned together and exceed `max-step`.

## Inputs

See the README table. Required: `--max-step`, `--min-retain`. The gRPC client
is compiled in (`github.com/cometbft/cometbft/api/.../pruning/v1`), so no proto
files or `grpcurl` are needed.

## Startup checks

| ID | Check | Failure |
|---|---|---|
| C1 | privileged gRPC reachable | exit 1 (retried by systemd), hint `[grpc.privileged] laddr` |
| C2 | `PruningService` enabled (not `Unimplemented`) | exit, hint `pruning_service.enabled` |
| C3 | `data_companion.enabled = true` (needs `--home`) | exit — `Pruner.OnStart` only starts `pruneIndexesRoutine` when it is set |
| C4 | `indexer = "kv"` (needs `--home`) | exit |
| C5 | Prometheus reachable | warn |
| C6 | existing indexer retain height ≤ lowest indexed height + `max-step` | exit — the node would prune the whole gap at once |
| C7 | `min-retain-blocks > 0` (needs `--home`) | warn — block pruning is off otherwise |
| C8 | `discard_abci_responses` | info — nothing to prune in block results when true |

Every other failing check exits with code 3: it is a config error, and the
systemd unit's `RestartPreventExitStatus=3` stops the restart loop instead of
re-reporting it to Sentry every `RestartSec`.

## Loop

```
[A] status            catching_up or MemAvailable < min -> sleep poll
    ceiling = latest - min-retain
    block, block-results retain -> ceiling
    current = min(tx indexer retain, block indexer retain)  (unset -> start height)
    next = min(current + max-step, ceiling)                 next <= current -> sleep wait
[B] witnesses for [current, next)
[C] Set tx + block indexer retain = next
[D] poll every --poll until witnesses are gone -> [A]
```

## Step completion: witnesses

Why not the `*_indexer_base_height` metric:
- it only updates when something was pruned (`pruner.go`: `else if numPruned > 0`);
- an empty range returns `0, lastRetainHeight, nil` (`kv.go`), indistinguishable from "still running";
- base is "last pruned tx height + 1", so it never reaches the target if the top heights have no txs;
- it disappears when the node restarts.

Witnesses, picked before the Set, checked with single-key lookups:

| Index | Witness | Done |
|---|---|---|
| tx | all txs at the highest height in `[current, next)` that has any, via `tx_search "tx.height=H"` (prefix seek `tx.height/H/`) | `/tx?hash=` finds none |
| block | `next - 1`, via `block_search "block.height=H"` (`Has(height)`) | count is 0 |

- `Prune` deletes in ascending height order and flushes every 1000 entries, so
  when the top of the range is gone, the rest is gone.
- Order within one height is unspecified (`sort.Sort` by height), hence all txs
  at that height.
- The block indexer writes a height key for every block, with or without events.
- Range with no txs: no tx witness, the step completes on the block witness.
- Witnesses come from the index itself, not the block store, so they work even
  when blocks below the target are already pruned.
- Stuck: after `--step-timeout` the daemon logs `step_stuck` and keeps waiting.
  It never moves on early.

## Start height

- Default: the lowest height with a block-index entry, found by binary search
  over `block_search "block.height=H"` (indexed heights are contiguous up to
  latest). The tx index starts at the same height.
- `--start-height` overrides it.

## Restarts

- Node restart: the target is persisted; the pruner calls `Prune(target)` once
  more. Witness checks are unaffected.
- Daemon restart: no state file. `current` is read back with `Get*RetainHeight`;
  the daemon picks witnesses in `[current - max-step, current)` and waits for
  them before stepping again.

## Safety (principle: never OOM)

| # | Guard |
|---|---|
| S1 | each step ≤ `max-step` blocks |
| S2 | no Set before the previous step is proven done |
| S3 | `MemAvailable` floor |
| S4 | C6 at startup |
| S5 | pause while catching up |
| S6 | target never exceeds `latest - min-retain` |
| S7 | completion = witnesses gone, not metrics |

## Settings that compete with Set

Common rules:
- None of the four retain heights can be lowered.
- Block and block-results Sets must be within `[block store base, latest]`
  (`ErrInvalidHeightValue` otherwise).
- With `data_companion.enabled = false` the node ignores all four.

| Set | Competes with | When it has no effect |
|---|---|---|
| `SetBlockRetainHeight` | the app retain height; the node prunes to `min(app, pruning service)` | `min-retain-blocks = 0`: the app reports 0, nothing is stored, block pruning is off. Otherwise this value can only keep blocks longer than the app would |
| `SetBlockResultsRetainHeight` | nothing | `discard_abci_responses = true` (nothing stored) |
| `SetTxIndexerRetainHeight` | nothing | indexer is not `kv` |
| `SetBlockIndexerRetainHeight` | nothing | same as above |

App retain height (Cosmos SDK v0.50 `GetBlockRetentionHeight`):

```
app = min(latest - evidence.max_age_num_blocks,
          latest - snapshot-interval * snapshot-keep-recent,   (ignored when 0)
          latest - min-retain-blocks)
min-retain-blocks = 0  ->  0, and CometBFT stores nothing (it only stores > 0)
```

Also:
- Blocks still inside the evidence window keep their header and commit when pruned.
- `initial_block_retain_height` / `initial_block_results_retain_height` apply
  once, when the pruning-service value is still 0.
- `index-events` changes what is indexed (size), not pruning.
- `pruning` / `pruning-keep-recent` (app state) are unrelated.

## Observability

- `/metrics` on `--listen` (Prometheus registry of its own, `pacer_*`), meant to
  be scraped by the agent that already scrapes the node.
- Sentry when `SENTRY_DSN` is set: an `slog.Handler` wrapper forwards Warn and
  Error records, throttled to one per hour per key and fingerprinted by the same
  key = event name + check id / retain kind (error text is excluded: it carries
  heights); startup check failures and panics are reported before exit.

## Open questions

- Memory and duration of one 10,000-block step on a large index.
- Cost of the `tx.height/` prefix scan that every `Prune` call performs.
- When pebble returns freed space: the tx indexer is created without a
  compaction option, so space comes back through pebble's own compactions.
