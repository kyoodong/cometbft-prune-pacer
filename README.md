# cometbft-prune-pacer

A daemon that prunes a CometBFT v1 node's **tx and block indexes** in small,
verified steps, so index pruning never runs the node out of memory.

## Why

`indexer = "kv"` keeps every tx forever. `pruning` and `min-retain-blocks` do
not touch `data/tx_index.db`. CometBFT v1 can prune it through the privileged
gRPC `PruningService` (`SetTxIndexerRetainHeight`, `SetBlockIndexerRetainHeight`),
but `kv.TxIndex.Prune` loads **every** tx below the new retain height into
memory before deleting it:

```go
// state/txindex/kv/kv.go (CometBFT v1.0.1)
results, _, err := txi.Search(ctx, query.MustCompile(
    fmt.Sprintf("tx.height < %d AND tx.height >= %d", retainHeight, lastRetainHeight)),
    txindex.Pagination{}) // no pagination
```

On a node with a 1.3 TB index, one `SetTxIndexerRetainHeight` that covered
~11.7M blocks grew the process to 122 GiB and got it OOM-killed. Retain
heights cannot be lowered afterwards.

This daemon raises the retain height by at most `--max-step` blocks at a time
and only takes the next step after it has proven the previous one finished.

## How it works

```
loop
  status: latest, catching_up          catching up or low MemAvailable -> wait
  ceiling = latest - min-retain
  block and block-results retain -> ceiling
  next = min(current + max-step, ceiling)   next <= current -> sleep --wait
  pick witnesses for [current, next)
  SetTxIndexerRetainHeight(next), SetBlockIndexerRetainHeight(next)
  poll until witnesses are gone
```

**Witnesses** decide when a step is done. Prometheus `*_indexer_base_height`
is not used for that: it only moves when something was actually pruned, so an
empty range looks the same as a stuck one, and it resets on node restart.

| Index | Witness | Done when |
|---|---|---|
| tx | all txs at the highest height in `[current, next)` that has txs (`tx_search "tx.height=H"`, a prefix seek) | none of them is found by `/tx?hash=` |
| block | block-index entry at `next - 1` (`block_search "block.height=H"`, a key lookup) | `total_count` is 0 |

`Prune` deletes in ascending height order and flushes every 1000 entries, so
once the witnesses are gone everything below them is gone too. A range with no
txs needs no tx witness.

## Requirements

`config.toml` on the node:

```toml
[grpc.privileged]
laddr = "tcp://127.0.0.1:26659"      # loopback only: anyone who can reach it can prune the node

[grpc.privileged.pruning_service]
enabled = true

[storage.pruning.data_companion]
enabled = true                        # without it the node never prunes its indexes

[tx_index]
indexer = "kv"
```

`app.toml`: `min-retain-blocks > 0`, otherwise the app never reports a block
retain height and block pruning stays off (index pruning still works).

## Usage

```sh
cometbft-prune-pacer run --max-step 10000 --min-retain 1430000 --home /path/to/node/home
```

| Flag | Env | Default | |
|---|---|---|---|
| `--max-step` | `PACER_MAX_STEP` | required | max blocks per step |
| `--min-retain` | `PACER_MIN_RETAIN` | required | blocks always kept |
| `--grpc-addr` | `PACER_GRPC_ADDR` | `127.0.0.1:26659` | privileged gRPC |
| `--rpc-addr` | `PACER_RPC_ADDR` | `http://127.0.0.1:26657` | CometBFT RPC |
| `--metrics-addr` | `PACER_METRICS_ADDR` | `http://127.0.0.1:26660/metrics` | informational only |
| `--home` | `PACER_HOME` | | enables config file checks |
| `--wait` | `PACER_WAIT` | `10m` | sleep when the ceiling is reached |
| `--poll` | `PACER_POLL` | `30s` | witness polling interval |
| `--step-timeout` | `PACER_STEP_TIMEOUT` | `2h` | warn (never skip) when a step is slow |
| `--min-mem-avail` | `PACER_MIN_MEM_AVAIL` | `30GiB` | do not start a step below this |
| `--start-height` | `PACER_START_HEIGHT` | auto | first step base; auto = lowest indexed height |
| `--dry-run` | `PACER_DRY_RUN=true` | false | log decisions only |

## Installation

### 1. Get the binary

Build on any machine with Go 1.23+ and copy it to the node (static binary, no
runtime dependencies):

```sh
git clone https://github.com/kyoodong/cometbft-prune-pacer.git
cd cometbft-prune-pacer
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o bin/cometbft-prune-pacer .
scp bin/cometbft-prune-pacer root@<node>:/tmp/
```

Or build directly on the node:

```sh
go install github.com/kyoodong/cometbft-prune-pacer@latest   # -> $(go env GOPATH)/bin
```

### 2. Prepare the node

1. Set the `config.toml` keys from [Requirements](#requirements) and restart the node.
2. Check the privileged port is bound to loopback only:
   `ss -ltnp '( sport = :26659 )'` should show `127.0.0.1:26659`.
3. Make sure no large indexer retain height was set by hand before. If one
   was, startup check C6 refuses to run; the key has to be removed from
   `data/tx_index.db` while the node is stopped.

### 3. Dry run

Runs every startup check and logs the first step without calling Set:

```sh
/tmp/cometbft-prune-pacer run --dry-run --max-step 10000 --min-retain 1430000 --home /path/to/node/home
```

Expect a `started` line followed by `start_height` and `step_set` with
`"dry_run":true`. A failed check prints its ID and the fix.

### 4. Install the service (as root)

```sh
/tmp/cometbft-prune-pacer install --user <node-user> --max-step 10000 --min-retain 1430000 --home /path/to/node/home
systemctl enable --now cometbft-prune-pacer
journalctl -u cometbft-prune-pacer -f
```

`install` copies the binary to `/usr/local/bin/cometbft-prune-pacer`, writes
`/etc/systemd/system/cometbft-prune-pacer.service` (runs as `--user`, restarts
on failure) and runs `systemctl daemon-reload`. All other flags are passed to
`run` unchanged and validated first.

### 5. Watch the first steps

Each step logs `step_set` then `step_done` with its duration. Watch the node's
memory during the first few steps before leaving it unattended; lower
`--max-step` if a step uses more than you can afford.

### Uninstall

```sh
systemctl disable --now cometbft-prune-pacer
rm /etc/systemd/system/cometbft-prune-pacer.service /usr/local/bin/cometbft-prune-pacer
systemctl daemon-reload
```

Retain heights already set stay on the node and cannot be lowered.

## Startup checks

The daemon exits with a fix hint if any of these fail:

| ID | Check |
|---|---|
| C1 | privileged gRPC reachable |
| C2 | `PruningService` enabled |
| C3 | data companion enabled (`--home`) |
| C4 | `indexer = "kv"` (`--home`) |
| C6 | no indexer retain height already more than `--max-step` above the lowest indexed height (it cannot be lowered and would be pruned in one go) |

Warnings only: C5 Prometheus unreachable, C7 `min-retain-blocks = 0`,
C8 `discard_abci_responses = true`.

## Settings that compete with the retain heights

See [docs/DESIGN.md](docs/DESIGN.md#settings-that-compete-with-set). In short:
the block retain height actually used is `min(app retain, pruning service
retain)`, so this daemon can only keep blocks longer than the app would, never
shorter; with `min-retain-blocks = 0` block pruning is off entirely.

## Build

```sh
go test ./...
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bin/cometbft-prune-pacer-linux-amd64 .
```

Written against CometBFT v1.0.1 sources (and a fork whose pruning files are
identical). Not yet run against a live node. Not for CometBFT v0.38 or earlier,
which have no `PruningService`.
