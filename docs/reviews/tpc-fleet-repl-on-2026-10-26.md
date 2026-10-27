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

The two bad windows are 30 s and 60 s; the 10 slowest transactions (1.7-1.8 s) all sit at 30.8 s
or 63.7 s, and between them p99 is 300-350 ms. A first reading blamed the flush. Experiments the
same day showed it is the colima VM's page-cache writeback instead.

### Experiment: what makes the 30 s / 60 s spikes

Each arm is one run on a fresh `colima restart`, everything else as above, 0 `clock lags` lines.

| arm | what changed | spikes at 30 / 60 s (p99 of window) | result |
|---|---|---|---|
| X | `SHARD_FLUSH_INTERVAL_MS=45000` | still at 30 / 60 s (1,673 / 1,181 ms) | the flush is not the cause |
| Y | rig nginx: `keepalive_requests 1000000` at server scope | still at 30 / 60 s (1,129 / 1,161 ms); 0 LB listen overflows | the LB's 1000-request recycle is not the cause |

Both arms sampled `/proc/meminfo` in the VM once a second. Dirty page cache climbs at ~70 MB/s to
7.9 GB, then drops to 0.8 GB at 34-42 s, again at 66-74 s and again at ~100 s after the run starts:
the spikes. The VM has 94 GB of RAM, so `dirty_background_ratio=10` (9.4 GB) is never reached and
nothing is written back until pages hit `dirty_expire_centisecs=3000` (30 s). Then ~7 GB goes to
the virtual disk at once, and every `synchronous=FULL` commit fsync waits behind it. Most of that
data is follower replica writes, which are not fsynced (`:replication_fsync` defaults to off).

Then the same run with `vm.dirty_background_bytes` set in the VM (`sudo sysctl -w`), so writeback
runs continuously above that much dirty data:

| `dirty_background_bytes` | txn/s | p50 | p99 | worst 10 s window | commits that skipped a follower |
|---|---|---|---|---|---|
| default (ratio 10% = 9.4 GB) | 4,317 | 215 ms | 598 ms | 1,191 ms | 17,946 (4.3%) |
| **1 GB** | **4,008** | **233 ms** | **417 ms** | **454 ms** | 21,427 (5.2%) |
| 256 MB | 3,404 | 284 ms | 485 ms | 523 ms | 35,262 (8.5%) |
| 64 MB | 2,273 | 434 ms | 762 ms | 853 ms | 173,740 (42%) |

1 GB removes the spikes (every window 400-454 ms) and cuts p99 by 30% for 7% of throughput. Lower
values write back so constantly that every fsync and follower write slows: throughput falls and p99
rises again. Each row is a single run, so the 1 GB numbers are a direction, not a calibrated optimum.

What this means outside the rig: any Linux host with a lot of RAM and default `vm.dirty_*` settings
batches writeback the same way, and fathom's unfsynced follower writes fill it fast. A deployment
should set `vm.dirty_background_bytes` (around 1 GB was best here) rather than rely on the ratio.

Other context from the fresh run: cross-LB `SELECT 1` RTT p50 179 µs; 416,768 quorum commits, all
`ok`; 6,319 catch-up rounds, all `ok` (the tired run had 2 catch-up timeouts); the LB put 186-222
shards on each node (spread 1.19x), and each node's steady-state p99 was 656-731 ms.

### Follower workers: 8 vs 32 per link (with the 1 GB writeback cap)

A follower worker writes its shards' pushes one at a time. At 8 per link about 26 shards share each
worker, and a commit whose previous push to a follower is still queued skips that follower. Three
alternated runs a side, fresh VM each:

| workers (held fds) | p99 per run | p99 avg | p95 avg | txn/s avg | commits that skipped a follower |
|---|---|---|---|---|---|
| 8 (64) | 424 / 432 / 435 ms | 430 ms | 367 ms | 4,011 | 4.1-6.2% |
| **32 (16)** | **406 / 421 / 407 ms** | **411 ms** | **351 ms** | **4,073** | **0.5-0.6%** |

Every 32-worker run beat every 8-worker run. 32 / 16 became the default in `c46adf3b` (same node fd
budget: 16 × 32 × 4 = 2,048). `./chaos.sh rpo` passed, and a 30-minute all-faults Jepsen run on it
was 16 of 16 valid (`docs/reviews/jepsen-2026-10-24.md`).

### Replication process priority: `normal` vs `all` (32 workers, 1 GB writeback cap)

`REPLICATION_PROCESS_PRIORITY=all` runs the replication processes at high BEAM priority. Three
alternated runs a side, fresh VM each, 0 errors, 0 shed, 0 `clock lags` in every run.

Closed loop (`tpc-fleet 1024`, every tenant sends its next txn when the last returns):

| priority | p99 per run | p99 avg | p50 avg | txn/s avg | replication commit time (mean) | commits that skipped a follower |
|---|---|---|---|---|---|---|
| normal | 408 / 412 / 410 ms | 410 ms | 233 ms | 4,038 | 39.8 ms | 0.3-0.7% |
| all | 399 / 404 / 418 ms | 407 ms | 229 ms | 4,113 | 16.9 ms | 0.0% |

`all` cuts the replication part of a commit by 58%, but p99 does not move: the rig is CPU-bound, and
in a closed loop latency is about tenants / throughput (Little's law), so time saved in one queue
is spent in another. Only the 2% throughput gain shows.

Open loop (`TPC_TAIL_ARMS=open TPC_OPEN_RATE=3000 ./chaos.sh tpc-tail 1024 400 1000 1`, a fixed
3,000 txn/s, about 74% of capacity), to see whether the commit-time cut reaches latency below
saturation:

| priority | p99 per run | p99 avg | p50 avg | replication commit time (build + collect, mean) | commits that skipped a follower |
|---|---|---|---|---|---|
| normal | 200 / 141 / 151 ms | 164 ms | 21 ms | 4.1 ms | 0.3-0.6% |
| all | 136 / 267 / 194 ms | 199 ms | 23 ms | 3.2 ms | 0.0% |

Below saturation replication costs only ~4 ms a commit, so `all` has little to remove, and the runs
overlap. `normal` stays the default. Two things this shows:

- At 3,000 txn/s p50 is 21 ms, against ~230 ms closed loop at capacity: the closed-loop p50 is
  almost all queueing.
- p99 is still 140-270 ms at 74% load, with single 10 s windows up to 418 ms, and it varies run to
  run by 2x. That tail is not replication priority; its cause is not yet known.

## Limits

One VM runs all five nodes, so the absolute txn/s is CPU-bound by one box. Replication traffic
crosses a container network, not a real one. The writeback rows are single runs; the worker and
priority comparisons are three runs a side. Logs: `logs/tpc-fleet-1024-qf*.log`,
`logs/disc-{X,Y,Z,G1,M256}/`, `logs/disc-pr{N,A}-{1,2,3}/`, `logs/disc-ol{N,A}-{1,2,3}/` (local, not
committed).
