# tpc-fleet, replication on, 1024 tenants (2026-10-26)

A capacity check after the Jepsen fixes (`docs/reviews/jepsen-2026-10-24.md`), to see what a
replicating fleet serves with no faults. Code: `a39b1dcb` (the finding 7 quorum fence); the commits
after it up to the run are docs only.

## Setup

```bash
cd deploy/chaos
REPLICATION_ENABLED=true SHARD_FLUSH_INTERVAL_MS=30000 ./chaos.sh up
TPC_NET=container TPC_DRIVER=elixir REPLICATION_ENABLED=true ./chaos.sh tpc-fleet 1024
```

Five nodes on one 12-vCPU colima VM, 1024 tenants (one shard each), 400 TPC-B transactions per
tenant, 1000 accounts per tenant. The node environment was checked (`REPLICATION_ENABLED=true`,
`SHARD_FLUSH_INTERVAL_MS=30000`), and no node logged `clock lags the store` in either run.

## Results

| run | txn/s | errors | shed | p50 | p95 | p99 (CI95) |
|---|---|---|---|---|---|---|
| **fresh VM** (`colima restart` first) | **4,317** | 0 | 0 | 215 ms | 304 ms | **598 ms** (565-631) |
| tired VM (after three Jepsen runs, ~40 min of load) | 4,000 | 0 | 0 | 223 ms | 328 ms | 771 ms (733-827) |
| 2026-10-03 (`c61cbe3`), for comparison | 3,211 | 0 | 0 | — | — | 526 ms |

The fresh-VM run is the number to quote. The tired-VM run is the same code on the same rig: the VM
degradation rule in `AGENTS.md` cost 7% of throughput and 29% on p99 here.

Against 2026-10-03, throughput is up 34% and p99 is up 14%. The 2026-10-03 p99 is a single run with
no interval, so the p99 change is inside the 20% noise band and not shown to be a regression.

## Where the tail comes from

p99 by 10 s window, fresh VM:

```
0s=350  10s=314  20s=321  30s=1167  40s=320  50s=355  60s=1191  70s=343  80s=324  90s=298
```

The two bad windows are 30 s and 60 s: the flush interval. The 10 slowest transactions (1.7-1.8 s)
all sit at 30.8 s or 63.7 s. Between flushes p99 is 300-350 ms. The tired-VM run shows the same
shape (30 s, 60 s and 90 s windows at 1.1-2.0 s). So the p99 tail is mostly the flush (checkpoint +
the replication work it starts), not steady-state commit cost. This is read off the windows, not
proven by an experiment.

Other context from the fresh run: cross-LB `SELECT 1` RTT p50 179 µs; 416,768 quorum commits, all
`ok`; 6,319 catch-up rounds, all `ok` (the tired run had 2 catch-up timeouts); the LB put 186-222
shards on each node (spread 1.19x), and each node's steady-state p99 was 656-731 ms.

## Limits

One VM runs all five nodes, so the absolute txn/s is CPU-bound by one box. Replication traffic
crosses a container network, not a real one. Each configuration is a single run.
