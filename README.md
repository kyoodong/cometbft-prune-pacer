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
| `--wait` | `PACER_WAIT` | `10m` | sleep when the ceiling is reached; must be > 0 |
| `--poll` | `PACER_POLL` | `30s` | witness polling interval; must be > 0 |
| `--step-timeout` | `PACER_STEP_TIMEOUT` | `2h` | warn (never skip) when a step is slow; must be > 0 |
| `--min-mem-avail` | `PACER_MIN_MEM_AVAIL` | `30GiB` | do not start a step below this; unit required (`GiB`, `MiB`, `KiB`) |
| `--start-height` | `PACER_START_HEIGHT` | auto | first step base; auto = lowest indexed height |
| `--dry-run` | `PACER_DRY_RUN=true` | false | log decisions only |
| `--listen` | `PACER_LISTEN` | `127.0.0.1:26670` | serve this daemon's `/metrics`; empty disables |
| | `SENTRY_DSN` | | enables Sentry |
| | `SENTRY_ENVIRONMENT` | | Sentry environment tag |

## Installation

### 1. Get the binary

On the node, if it has Go 1.23+:

```sh
git clone https://github.com/kyoodong/cometbft-prune-pacer.git
cd cometbft-prune-pacer
make install            # builds and copies to /usr/local/bin (PREFIX=... to change)
```

Or build elsewhere and copy the static binary over:

```sh
make build-linux        # -> bin/cometbft-prune-pacer-linux-amd64
scp bin/cometbft-prune-pacer-linux-amd64 root@<node>:/usr/local/bin/cometbft-prune-pacer
```

Other targets: `make build` (this machine), `make test`, `make clean`. The
version printed by `cometbft-prune-pacer version` comes from `git describe`.

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
cometbft-prune-pacer run --dry-run --max-step 10000 --min-retain 1430000 --home /path/to/node/home
```

Expect a `started` line followed by `start_height` and `step_set` with
`"dry_run":true`. A failed check prints its ID and the fix.

### 4. Install the service (as root)

`/etc/systemd/system/cometbft-prune-pacer.service`:

```ini
[Unit]
Description=CometBFT prune pacer
# Start after the node; the startup checks fail while its gRPC/RPC are down,
# and Restart= retries until they are up.
After=network-online.target injectived.service
Wants=network-online.target

[Service]
# Same user as the node, so it can read <home>/config/*.toml.
User=injective
# Optional secrets (SENTRY_DSN, SENTRY_ENVIRONMENT). "-" = file may be absent.
EnvironmentFile=-/etc/cometbft-prune-pacer.env
Environment=PACER_MAX_STEP=10000
Environment=PACER_MIN_RETAIN=1430000
Environment=PACER_HOME=/path/to/node/home
ExecStart=/usr/local/bin/cometbft-prune-pacer run
Restart=on-failure
RestartSec=60

[Install]
WantedBy=multi-user.target
```

Settings can be given as flags on `ExecStart` or as `PACER_*` variables; flags
win when both are set.

`/etc/cometbft-prune-pacer.env` (optional, `chmod 600`):

```sh
SENTRY_DSN=https://<key>@<org>.ingest.sentry.io/<project>
SENTRY_ENVIRONMENT=production
```

Enable and check:

```sh
systemctl daemon-reload
systemctl enable --now cometbft-prune-pacer
systemctl status cometbft-prune-pacer
journalctl -u cometbft-prune-pacer -f            # JSON lines: started, step_set, step_done ...
curl -s 127.0.0.1:26670/metrics | grep '^pacer_' # own metrics
```

A failed startup check makes the process exit with code 1; systemd retries
every 60 s and the reason is in `journalctl`. Fix the node config, then
`systemctl restart cometbft-prune-pacer`.

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

## Observability

### Metrics (Grafana)

The daemon serves Prometheus metrics on `--listen` (default
`127.0.0.1:26670/metrics`). Point the scraper that already collects the node's
metrics at it, e.g. Grafana Alloy:

```alloy
prometheus.scrape "prune_pacer" {
  targets    = [{"__address__" = "127.0.0.1:26670"}]
  forward_to = [prometheus.remote_write.default.receiver]
}
```

| Metric | Type | Meaning |
|---|---|---|
| `pacer_latest_height` | gauge | latest block height |
| `pacer_ceiling_height` | gauge | `latest - min-retain` |
| `pacer_target_height` | gauge | indexer retain height last set |
| `pacer_backlog_blocks` | gauge | blocks still to prune (`ceiling - target`) |
| `pacer_step_in_progress` | gauge | 1 while waiting for witnesses |
| `pacer_step_stuck` | gauge | 1 after `--step-timeout` |
| `pacer_paused{reason}` | gauge | `catching_up`, `low_memory`, `ceiling` |
| `pacer_mem_available_bytes` | gauge | MemAvailable at the last decision |
| `pacer_steps_total` | counter | completed steps |
| `pacer_empty_steps_total` | counter | steps whose range had no txs |
| `pacer_errors_total` | counter | loop iterations that failed |
| `pacer_step_duration_seconds` | histogram | Set until witnesses gone |

Useful queries: catch-up ETA ≈
`pacer_backlog_blocks / (rate(pacer_target_height[1h]) * 3600)` hours;
alert on `pacer_step_stuck == 1` or `absent(pacer_latest_height)`.

### Sentry

Set `SENTRY_DSN` to send warnings and errors: failed startup checks,
`step_error`, `step_stuck`, `retain_set_failed`, `paused_low_memory`, startup
warnings and panics. Normal progress (`step_set`, `step_done`) stays in the
logs. Each event is reported at most once per hour and grouped by event name.

With the systemd unit from step 4, put secrets in
`/etc/cometbft-prune-pacer.env` (mode 600):

```sh
SENTRY_DSN=https://<key>@<org>.ingest.sentry.io/<project>
SENTRY_ENVIRONMENT=production
```

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
make test
make build-linux        # -> bin/cometbft-prune-pacer-linux-amd64
```

Written against CometBFT v1.0.1 sources (and a fork whose pruning files are
identical). Not yet run against a live node. Not for CometBFT v0.38 or earlier,
which have no `PruningService`.
