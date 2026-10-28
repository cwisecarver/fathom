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

### The open-loop tail is disk writes, and half of them were a fourth follower (2026-10-27)

Same open loop (3,000 txn/s, 1024 tenants), one run per row, fresh VM, 0 errors and 0 `clock
lags` in every run. `tpc_driver.exs` now reports p99 per second with a wall-clock anchor
(`c8439275`), and the VM was sampled every second (`/proc/stat`, `/proc/pressure`, `Dirty`), so a
slow second can be lined up with what the VM was doing.

| arm | p50 | p99 | read |
|---|---|---|---|
| replication off | 6 ms | 13 ms | the floor |
| replication on (as measured above) | 24 ms | 175 ms | |
| replication on, 2,000 txn/s | 9 ms | 92 ms | lower load helps but does not remove it |
| **replication on, `/data/replicas` on tmpfs** | 19 ms | **54 ms** | the follower writes are most of the tail |
| replication on, `REPLICATION_FSYNC=true` | — | collapsed (1,918 txn/s offered 3,000) | followers cannot fsync every push |
| replication on, `dirty_expire_centisecs=200`, `dirty_writeback_centisecs=100` | 32 ms | 220 ms | constant writeback is worse |

With replication on, the worst seconds are the ones where `/proc/pressure/io` full-stall rises, iowait
rises, CPU idle rises and ~400 MB of dirty page cache is written back at once: the primaries'
`synchronous=FULL` fsyncs wait behind follower writeback on the one virtual disk.

Where the bytes come from, counted with bpftrace in the VM (`vfs_write` by file, `block_bio_queue`
by submitting thread), 409,600 transactions:

| what | KB per transaction |
|---|---|
| follower `-wal` writes (4 followers x the primary's WAL) | 66.8 |
| primary `-wal` writes | 16.7 |
| everything else at the file level (`.db`, `-shm`, container logs, Postgres, MinIO) | ~4 |
| **on the disk:** writeback (`kworker`) / ext4 journal (`jbd2`) / fsync-driven (`erts_dios`) | 67.8 / 43.3 / 23.6 = **136.5** |

Every node shipped every shard to all four peers while a commit waits for two. (The trace counted
~2 primary WAL fsyncs per transaction, 841,920 for 409,600. That is a counting artifact: a node's
`/data` sits on the container's overlayfs root, so each `fsync` reaches `vfs_fsync_range` once on
overlay and once on ext4. Counting `sys_enter_fsync` in an exqlite probe gave 100 syscalls for 100
commits.) The exqlite build calls `fsync`, not `fdatasync` (no `HAVE_FDATASYNC`), so every commit
also journals the WAL's inode.

That is not where the journal comes from. Two images built exqlite from source
(`config :exqlite, force_build: true`), identical except `EXQLITE_SYSTEM_CFLAGS=-DHAVE_FDATASYNC=1`
on one; three alternated open-loop runs a side at 3,000 txn/s, three followers, 0 errors, no clock
lags (2026-10-27):

| build | p99 per run | p99 avg | p50 | journal KB/txn | disk KB/txn |
|---|---|---|---|---|---|
| `fsync` | 155 / 130 / 130 ms | 138 ms | 17-18 ms | 38.6 | ~114 |
| `fdatasync` | 118 / 127 / 119 ms | 121 ms | 17-18 ms | 36.2 | ~111.5 |

`fdatasync` removed 2.4 KB/txn of journal (6%) and 2% of disk bytes; writeback and fsync-driven
bytes did not move. The p99 is 12% lower and the ranges just separate (127 vs 130 ms), but that is
under the 20% noise bar and 2% of bytes does not explain it. Not adopted: it would mean compiling
SQLite from source in every build instead of exqlite's precompiled NIF.

Three followers instead of four (a ring: `fathomN` ships to the next three), three alternated runs
a side:

| followers | p99 per run | p99 avg | p50 avg | commit_total mean | io full-stall |
|---|---|---|---|---|---|
| 4 | 151 / 164 / 175 ms | 164 ms | 24 ms | 6.6 ms | 18% |
| **3** | **143 / 125 / 132 ms** | **133 ms** | **19 ms** | **5.0 ms** | **14%** |

Every three-follower run beat every four-follower run on p50 and p99. The rig now runs three
(`deploy/chaos/docker-compose.yml`). It costs one copy: each shard lives on four machines, not
five. A takeover node that is not one of the shard's followers recovers from a peer:
`./chaos.sh rpo` passed on this configuration with exactly that case (holders fathom1, fathom2,
fathom5; fathom2's copy erased; fathom2 pulled the acked write from fathom5 and promoted it), and a
30-minute all-faults Jepsen run was 16 of 16 valid (`docs/reviews/jepsen-2026-10-24.md`).

For a deployment this is the `REPLICATION_FOLLOWERS` list with the default `static` membership.
`REPLICATION_MEMBERSHIP=roster` ships to every node the roster lists, with no cap; capping it
would be a code change, not made here.

### Deferred WAL sync and page size, open loop (2026-10-27)

Same open loop (3,000 txn/s, 1024 tenants, three followers, `SHARD_FLUSH_INTERVAL_MS=30000`), fresh
`colima restart` per run, 0 errors, 0 `clock lags`. The compose file now passes
`REPLICATION_DEFERRED_WAL_SYNC` through (it did not before, so the flag had never run on the rig).
Each run checked the flag live with `WalSync.deferred?()` over rpc on **fathom1 only**; the other
four nodes were not checked.

| arm | p99 per run (excl. first 10 s) | p99 avg | p50 |
|---|---|---|---|
| `REPLICATION_DEFERRED_WAL_SYNC=false` | 140 / 124 / 126 ms | 130 ms | 19-21 ms |
| `REPLICATION_DEFERRED_WAL_SYNC=true` | 116 / 118 / 109 ms | 114 ms | 20-22 ms |

The page-size runs below also ran with the flag OFF and 4096-byte pages, which makes two more
off-arm baselines on the same code (117 / 137 ms). Over the five off runs p99 spans 117-140 ms; the
three on runs span 109-118 ms. The ranges touch (117 off, 118 on), so this is a likely ~10% effect
inside the rig's run-to-run noise, not a demonstrated win. Hot shard
(`./chaos.sh tpcc 1 8 3000 0.02`, 8 threads on one shard, three runs a side): tpmC 4,140 -> 4,351
(+5%), new-order p99 unchanged (~90 ms). Not adopted: the flag stays OFF. It changes the sync mode
of every tenant handle, and the rig keeps its data on the container's overlayfs while the flag is
supported only on ext4/xfs (`WalSync` moduledoc), so a rig Jepsen pass is weak evidence for it. The
`rpo`, crash and Jepsen gates named in `19b02045` have not been run on this flag.

`PRAGMA page_size` on NEW shards (an experiment hook, since reverted), two runs a side, flag OFF.
The header of a tenant file on fathom1 was read to confirm each arm (`16 0` = 4096, `8 0` = 2048,
`4 0` = 1024); a first attempt set the pragma on only one of the two open paths and measured 4096 in
every arm, so its numbers were discarded:

| page size | p99 per run | `du -sk /data` on fathom1, after the run |
|---|---|---|
| 4096 | 117 / 137 ms | 1.26 GB |
| 2048 | 119 / 104 ms | 2.13 GB |
| 1024 | 122 / 128 ms | 1.10 GB |

No difference outside the run-to-run spread. WAL or journal bytes per transaction were NOT measured
in these arms, so this does not show that fewer bytes fail to move p99, only that these page sizes
did not. The `du` column is one node's whole data directory (shards, WALs, temp files) at one
instant; 2048 using more than 4096 is unexplained here and not investigated. Dropped. If revisited:
`session.ex`'s hard-coded `@frame_bytes 4096 + 24` would have to follow the page size, and
`PRAGMA page_size` must not run on existing files. Probed 2026-10-27: on a 4096-byte WAL database,
`PRAGMA page_size=2048` followed by `VACUUM INTO` wrote a 2048-byte output file while the source
stayed 4096. Logs: `logs/ds/` (local, not committed).

### Replicas on a second block device (2026-10-27)

The tmpfs arm above was an upper bound for isolating follower writes; this is the real thing. The
colima VM got a second virtio disk (`limactl disk create replicadisk --size 40G`, attached through
`~/.colima/_lima/_config/override.yaml` `additionalDisks`, ext4 at `/mnt/lima-replicadisk`), and
`deploy/chaos/docker-compose.replicadisk.yml` bind-mounts a directory of it at each node's
`/data/replicas`. `df` inside fathom1 confirmed `/data/replicas` on `/dev/vdb1` in the `disk` arm
and on the overlay root in the `base` arm. Flag off, 4096 pages, open loop 3,000 txn/s, 1024 tenants,
three followers, a fresh `colima restart` per run and the replica directories wiped before each run,
three runs a side alternated, 0 errors, 0 `clock lags`, 2,997-2,999 txn/s achieved.

| replicas on | p99 per run (excl. first 10 s) | p99 avg | p50 |
|---|---|---|---|
| the root disk (`base`) | 122 / 133 / 138 ms | 131 ms | 17.5-18.3 ms |
| a second virtio disk (`disk`) | 45 / 49 / 55 ms | 49 ms | 16.1-17.1 ms |

Every `disk` run beat every `base` run by a wide margin (-62% on the average), well outside the
spread of either arm, and it lands on the tmpfs figure (54 ms). The VM's `/proc/pressure/io`
full-stall avg10 fell from a mean of 1.6-1.7 (max 4.5-4.7) to 1.0-1.1 (max 2.9-3.4), sampled
every second over the whole 330 s window, idle time included. Dirty page cache peaked near the 1 GB
`vm.dirty_background_bytes` cap in both arms, so the global dirty threshold did not couple the two
disks enough to matter here.

Limits: both virtio disks are files on the same host SSD, so this isolates the guest's page cache,
writeback and journal, not host I/O. A deployment whose two devices do not share a queue or a
bandwidth cap (a second local NVMe) is the case this approximates; two EBS volumes on one instance
share its EBS bandwidth and IOPS limit, and that case is unmeasured. One VM, so the absolute numbers
are the rig's. Each arm is three runs and the flag-off, 4096-page baseline is the same one as above.
Replica files were confirmed on the second disk after a short run (140-172 files, 59-73 MB per
node, containers running as `app`).
Not run: replication-off under
the same layout, a second disk for `/data` instead of the replicas, Jepsen on this layout (it moves
bytes between devices and changes no code path, but that is not a measurement).

## Limits

One VM runs all five nodes, so the absolute txn/s is CPU-bound by one box. Replication traffic
crosses a container network, not a real one. The writeback rows are single runs; the worker and
priority comparisons are three runs a side. Logs: `logs/tpc-fleet-1024-qf*.log`,
`logs/disc-{X,Y,Z,G1,M256}/`, `logs/disc-pr{N,A}-{1,2,3}/`, `logs/disc-ol{N,A}-{1,2,3}/`,
`logs/disc-sec*/`, `logs/disc-bpfN-5/`, `logs/disc-f{3,4}-{1,2,3}/`, `logs/chaos-rpo-f3b.log`,
`logs/jepsen-f3.log` (local, not committed). The tmpfs, fsync and writeback arms are single runs.
