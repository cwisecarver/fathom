# Tuning audit, run against the chaos rig (2026-11-11)

A three-panel review (SQLite, BEAM, distributed systems; web research per panel) produced 17
tuning opportunities. Each was run as the measurement or A/B its finding named, and only a measured
win was acted on. The audit itself is a gitignored working file; this is the measured record.

**Method.** Closed-loop `./chaos.sh tpc-fleet 1024` (5 nodes, replication ON, 30 s flush, 0 errors
required), 3 interleaved pairs per A/B, a fresh fleet and `colima restart` before every run, one
variable per arm, flags passed through the rig's `ERL_ZFLAGS` hook or a compose overlay. Image built
from `f6280fd0` (`lib/` unchanged since). Replication is a default feature and is never benchmarked
against replication-off. Raw logs and scripts are in `logs/ds/` (`ab-*-summary.txt`, `iter-*.sh`; not
committed).

**Noise floor.** 15 default-arm runs across five A/Bs: mean 4,328 txn/s, sd 50.9 (1.18%), range
4,219-4,382. The standard error of a 3-vs-3 difference of means is about 42 txn/s, so about 83 txn/s
(1.9%) is the 2-sigma bar. The macOS `corespotlightd` daemon was at about 65% CPU when checked.

## Top finding for production: replicated-WAL disk use (#20), FIXED for quiet shards

Under continuous writes with replication on, each shard's `-wal` reaches **20-47 MB before it wraps** (5-11.5k
frames, 1.25-3x the 4000-frame / 16 MB checkpoint threshold), and every follower keeps a byte-for-byte copy. Keeping
replicas after a primary drops is by design (they are A2's failover and peer-recovery copies); the narrower
finding was what they were made of: **99.6% of the follower footprint was `-wal`** (79 replica shards on fathom1:
1,449.1 MB of `-wal`, 5.9 MB of `.db`; 77 of the 79 WALs over 1 MB, largest 47.4 MB), unchanged 60 s and 180 s
after every primary had dropped (`iter-replsplit.txt`, `iter-walgrowth2.txt`, `iter-shmprobe.txt`). The follower
only absorbed a WAL generation (`absorb_checkpoint`: PASSIVE then TRUNCATE) inside the push-reset path
(`follower.ex`, `absorb_before_reset`), so a shard's final generation sat as an unabsorbed `-wal` on every
follower until that shard was written again. This is what filled the rig's Docker data disk (below).

**The fix** (`aba4830d`, `Follower.quiet_absorb_pass/1`, `REPLICATION_QUIET_ABSORB_MS`, default 2 x `SHARD_IDLE_MS`,
0 = off): a replica no push has touched for the window is absorbed in place under its shard lock, and its row is
replaced by what the files then say (the row a restart builds over an empty WAL). Component-notes entry: "A2
follower: a quiet replica's last WAL generation is absorbed in place".

| Check | Result |
|---|---|
| 128 tenants x 3,000 txns, replica dir by suffix | 1,545 MB `-wal` at +60 s (window 40 s on the rig + the 60 s tick), then **0 `-wal` files and 8.5 MB `.db` at +180 s** (`iter-quietsplit.txt`); run 3,860 txn/s, 0 errors (3,881 before; not an A/B) |
| `RPO_QUIET_WAIT_S=80 ./chaos.sh rpo` (absorb while the primary is alive and unflushed, then kill it) | PASS: keeper `epoch 0, salt1 0, -wal 0 B`; survivor `recovery chose pull from fathom1 at L4/O3@8272`, `PROMOTED a local replica over the stored object`; gate off lost the row, gate on kept it (`iter-quiet-rpo.txt`) |
| standard `./chaos.sh rpo` | PASS (gate off 0, gate on 1) |
| Jepsen Tier 0 (`docs/jepsen-run-plan.md`), window set to 5 s so absorbs happen inside the faults: 1 x 2 min fault-free at 1000/s, 3 x 10 min hard mix (`power-loss,kill,pause,isolate,repl-cut,s3-cut`, 3 concurrent) | all 4 valid (8/8 shards Elle-valid, liveness valid); 110 s after each, 0 non-empty replica WALs on every node that could be read and 0 quiet-absorb warnings (`iter-jepsen-tier0.txt`) |
| `mix precommit` | green, 4,678 tests, dialyzer; 11 new tests, mutation-probed (keep the old row, ignore the mtime, drop the torn/lineage/ordinal guard each fail the tests that name them) |

What the table does not show: (a) the Jepsen runs have no counter for absorbs that fired during the faults (the
metric exists, `follower_quiet_absorb.count`, but was not scraped), so what they prove is that a 5 s window under
those faults loses nothing and leaves no WAL behind, not how many absorbs it took; (b) in the last run fathom2's
container was not found by the post-run check (the nemesis had restarted it; the run itself was valid); (c) a
shard written every few seconds for as long as it is open still holds its 20-47 MB high-water on the primary and
each follower. That part is the designed 2026-10-21 trade, not changed. (d) a primary that is alive but silent past
the window pays one re-ship of its WAL from offset 0 on its next write (unit-tested, not measured on the rig).

## Where the CPU goes (replication ON, saturated)

One run at 4,092-4,365 txn/s, 10 s windows from `/proc` per-thread stats, bpftrace and
`erlang:process_info` reductions on fathom1 (`logs/ds/measure1.txt`, `measure2-true.txt`).

| consumer | CPU |
|---|---|
| each fathom node | 1.47-1.51 cores: normal schedulers 1.10-1.14 (user 0.88-0.93), dirty-IO 0.30 (65% of it kernel), poll 0.04 |
| nginx LB | about 1.0 core |
| load driver | about 1.2 cores |
| dockerd + containerd-shim | 0.72 core, not shown by `docker stats` |
| VM | 55-59% user, 28% system, 6% softirq, 2-6% idle |

BEAM reductions on fathom1 over 10 s (229.3M, 2,540 processes): Bandit handler processes 56.7%,
`Filo.Stream` 17.1%, `Replication.Session` 8.2%, unnamed tasks 6.1%, `Replication.Follower` 4.0%,
`Replication.Shipper` 1.5%. The replication processes are about 14% of reductions; the rest of the
quorum path's cost is kernel network/disk work that reductions do not count. `sched_yield` is the top
syscall by a wide margin: about 34k per second on every scheduler and dirty-IO thread (about 760k/s per
node), i.e. spinning. Minor faults are 1.0-1.9k/s per node, so there is no carrier churn.

## Results

| # | Finding | Outcome |
|---|---|---|
| 1 | Per-thread CPU and syscall attribution | Done; the table above. Surfaced the `sched_yield` spin (#18). |
| 3 | `+Mdai max` | **-2.2%** (4,323.7 -> 4,227.9), all 3 pairs lower. Not adopted. The pre-check also did not reproduce the premise: dirty-thread `driver_alloc` is 54% of calls but only 37k calls/s, and `eheap_alloc` instance 0 saw 1,571 calls. |
| 4 | ThousandIsland per-request timer (`read_timeout: :infinity` bound arm, throwaway image) | **+0.72%** (4,266.9 -> 4,297.6), inside noise. Dropped; no knob or dependency change. |
| 5 | LB access log off | **+11.1%** (4,352.1 -> 4,835.8), 3 of 3 pairs, no overlap; p99 375 -> 359 ms. dockerd/shim CPU explains it. **Rig-only**: it is VM CPU returned to the nodes; a separately hosted LB would only gain its own CPU per request. Opt-in overlay `docker-compose.noaccesslog.yml`; rig default unchanged (series continuity). Production LB logs by default (`deploy/lb/fathom.nginx.conf` has no `access_log`): prefer `buffer=64k flush=5s` or a 2xx filter to `off`. |
| 6 | Shard dir off overlayfs | **Overlayfs is not the cost.** A bind mount on the VM root disk read +3.8%, but that moved to a different block device; the isolating arm (same device as the overlay) read **+0.38%**. The +3.8% is vda vs vdc1 (the Docker data disk and its journal). |
| 8 | LB `worker_processes 4` vs `auto` (12) | +1.5% (4,326.1 -> 4,390.4), 3 of 3 pairs, below the 1.9% bar. Rig-only. The WebSocket arm needs new driver code and a new series. |
| 9 | OTP 29.1.2 vs 28.5 | **No gain: -1.8%** (4,320.7 -> 4,243.6 txn/s), all 3 pairs lower (-10, -138, -83), p99 +6 to +24 ms, 0 errors in all six runs. Run after the rig fault below was fixed, with a runner image that has `libsctp1` (OTP 29 otherwise prints `ESOCK WARNING ... libsctp.so.1` on every `bin/fathom rpc`, which breaks `chaos.sh` parsing). Same commit built twice. Not adopted. |
| 10 | Carrier / page-fault churn | Not a problem (minor faults 1.0-1.9k/s per node). |
| 11 | bridge-netfilter | Premise not reproduced: `br_netfilter` is not loaded during a run. |
| 13 | `-DSQLITE_DEFAULT_PCACHE_INITSZ=4` | **Adopted.** `served_kb_per_shard` 201.6 / 201.8 -> 146.6 / 150.1 (-27%); pooled 404.0 -> 293.0; SQLite share 190.3 -> 134.9 KB. `same_shard_stmt_per_s` -0.4% / -0.7%; open and one-shot latency flat in the second round. Set in both Dockerfiles. |
| 14 | `OMIT_SHARED_CACHE` + `OMIT_DECLTYPE` | Under the 3% gate: served -0.6% / -1.2%, throughput flat. Dropped. `HAVE_FDATASYNC` not tested; it only pays together with a non-growing WAL (#17). |
| 16 | Previous push as a zstd dictionary | Offline, one TPC-B tenant WAL: 2,417 B per commit independent (6.8x), 957 B with the previous commit as a dictionary (17.2x), 529 B with the previous three (31.2x). Passes the finding's gate; **parked**: wire-format change needing the Jepsen/`rpo` gates, CPU cost unmeasured. |
| 18 | `+sbwt none +sbwtdcpu none +sbwtdio none` | **+3.2%** over 6 pairs, all positive (4,292.3 -> 4,430.9; 95% CI about +2.1% to +4.3%). Open-loop 1k/2k/3k txn/s: no latency regression (p99 61.3 -> 52.5, 94.5 -> 88.1, 146.5 -> 145.4 ms; p50 +0.66 ms at 1k, -0.95 ms and -6.0 ms at 2k, 3k; one run per arm per rate). **Rig-only in all likelihood**: five BEAMs spinning on 12 shared vCPUs. `rel/vm.args.eex` unchanged; production on a dedicated host is unmeasured. Use `ERL_ZFLAGS` on the rig. |

Parked, no change made: #7 (S3 first-byte timeout: a policy call on production failure behaviour),
#12 (pinned-cpuset topology is a new benchmark series), #15 (`PREPARE_PERSISTENT`: exqlite exposes no
`db_status` to even measure it), #17 (pre-sized or recycled `-wal`: conflicts with the replication
extent scan-back and needs the A2 gates).

## What this does and does not show

- **Replication is the largest single consumer** on the rig, which is why the replication-ON path, not
  SQLite flags or allocator tuning, is where more throughput would come from. Nothing in the SQLite and
  allocator flags moved the ceiling.
- **Most throughput wins here are rig artifacts** (Docker log collection, spinning schedulers on a
  shared VM, the LB worker count). They raise the benchmark and make it more production-like; they are
  not fathom speedups. The one production-relevant change is #13, a density win.
- **Single host, single VM.** Each A/B used three pairs; the p99 columns are not compared between arms
  because p99 varies by an order of magnitude between 10-second windows inside one run.
- **The rig failed late in the session** for a reason that is now understood (below). All scored A/Bs ran before it, each against its own interleaved default arm, all with 0 errors.
- **One #18 sample is printed in exponent form** (`4.3E+3`): the driver rounds txn/s to 0.1, so that value is
  4300.0 at the driver's precision, not an estimate.

## The late rig fault: the Docker data disk ran out of space

Symptom: from the OTP 29 A/B on, every arm (including the OTP 28 image that was clean an hour earlier)
had 32k-65k failed transactions and 3.1-3.9k txn/s. The driver labels any txn shortfall
`worker_deadline` (`tpc_driver.exs:874`), so that label said nothing about deadlines.

What the instrumented runs showed:

- Every node logged `heartbeat renew failed for <owner>: {:s3_put_status, 507}` and
  `durability flush failed ({:s3_put_status, 507})`, then, after the TTL plus steal margin, `heartbeat not valid
  for > margin+steal_margin - fencing writes` on about 200 shards each and `durability flush FAILED
  (:ownership_unconfirmed)`. The LB showed 2.5M `200`s and no 5xx, because the 503s are inside Hrana results.
- MinIO answers 507 when its drive is full. `/dev/vdc1` (the Docker data disk, which holds MinIO's data
  and every container's writable layer) went from 20.3 GiB free to 0.4 GiB in 85 s during a run
  (`stat -f` on MinIO's root) and the 507s began as it reached zero.
- The fleet itself writes that data: each node's writable layer grew about 58 MB/s (primary WAL plus three
  followers' WAL copies, about 270 MB/s fleet-wide), 3.4 GB per node after 80 s, and the shard WALs were
  not shrinking inside that window. A healthy 1024-tenant `tpc-fleet` run peaks at about **22 GiB** of disk
  (62.1 GiB free at the start, 40.3 GiB minimum during the run).
- My OTP 29 and throwaway-image Docker builds had filled the build cache (46 GB reclaimable), taking the
  free space down to 20 GiB, below that peak. After `docker builder prune -af` (62 GiB free)
  one default run read 4,234.9 txn/s, 0 errors, 0 heartbeat renew failures, 0 fenced shards.

Consequences:

- **The rig needs about 25-30 GiB of free space on the Docker data disk** for a 1024-tenant run, and Docker
  builds eat it silently. Check `docker system df` and remove stale `fathom-chaos:*` images after building (`docker builder prune` is machine-wide and slows every project's next build; use it last).
  `chaos.sh up` now warns when the disk has under 30 GiB free (`RIG_MIN_FREE_GB`, 0 to silence).
- **Fathom's behaviour was the designed one**: a store that rejects the lease-heartbeat PUT for longer than
  the TTL makes every node fence writes (fail closed), and every node depends on the one bucket. A bucket-wide
  failure is a fleet-wide write outage by design (`docs/single-writer.md`; the cross-region report notes the
  bucket as a single dependency). Nothing here shows a heartbeat-path weakness.
- **Why the disk fills, measured on fathom1** (197 primary shards; `logs/ds/iter-walgrowth.txt`, healthy rig,
  62 GiB free, 4,381.8 txn/s, 0 errors). Primary `-wal` files grew linearly from 104 MB (12 s) to 1,106 MB (93 s,
  5.6 MB per shard) and did not shrink at the 30 s flush boundaries; the `.db` files stayed at 1 MB until about
  t+73 s, then grew to 8 MB (a checkpoint backfilled them, late). Received replica files grew 322 MB -> 3,562 MB
  (about 600 shards' replicas). At the run's end the primary WALs went to 0 (idle flush-and-drop folds them),
  but **the replica files stayed at 3,562 MB for the 90 s observed afterwards** (2,601 files).
- **This is the designed trade, not a missing checkpoint** (`replication/session.ex:2487-2494`, corrected after
  review). With replication on, the flush's checkpoint runs in `Session.checkpoint_after_snapshot`: pinned,
  gated on the quorum holding the WAL, and **skipped until the WAL's commit extent reaches
  `flush_checkpoint_min_bytes` (about 4 MB, a quarter of the 4000-frame Session threshold)**; the user decision of
  2026-10-21 is that the replicated flush gains no new checkpoint because each is a WAL-generation reset the
  followers pay for (measured 22-25% txn/s between a 5 s and a 30 s flush). A busy WAL is meant to be bounded by the
  Session's threshold checkpoint (4000 frames = 16 MB) and a quiet one folds at the covered idle close; the longer
  run below shows the first is not true under continuous writes. The data fits: shards
  reach about 4 MB (roughly 290 of the 400 transactions) around t+70 s, which is when the `.db` files start to grow.
  The checkpoint is PASSIVE, which backfills but never truncates the `-wal`; the log only wraps when the next writer
  finds everything backfilled and no reader on an older mark, so under continuous load the bound is set by wrap
  opportunities, not by a missing checkpoint.
- **Longer run: the WAL wraps, but late; its high-water is 1.25-3x the 16 MB threshold.** 128 tenants x 3,000
  transactions (about 12,000 frames per shard, three times the 4000-frame threshold), 3,931 txn/s, 0 errors, 29
  primary shards on fathom1 (`logs/ds/iter-walgrowth2.txt`). The largest primary `-wal` grew linearly from 3.6 MB
  (10 s) to **47.4 MB (103 s)** (the maximum over shards, so a shard that has not wrapped yet dominates it); the
  replica files on the followers reached the same 47.4 MB. Flush checkpoints ran (`ran` 71, `skipped_small` 39) and
  177 covered Session checkpoints were counted on fathom1.
- **Read-only discriminator** (`logs/ds/iter-shmprobe.txt`: the `-shm` header of four hot shards every 10 s; no
  checkpoint was issued): the log **does wrap**. Eight wraps were seen in 100 s, at file sizes of **5,009, 6,009,
  6,009, 8,011 and 9,012 frames (19.7 to 35.4 MB)**; after a wrap `mxFrame` restarts near 0 and the file keeps its
  high-water size. `nBackfill` advances in steps of about 1,000 frames behind `mxFrame` (4,007, 5,009, 6,009,
  7,011, 8,012) and a wrap happens when a checkpoint reaches `mxFrame` at a moment with no older reader. Read
  marks look normal: mark 0 is 0, mark 1 equals the current `mxFrame` (the live reader), marks 2-4 are unused.
  So a shard's WAL is bounded by wrap opportunities at roughly 1.25-3x the intended 4000-frame (16 MB) bound,
  not unbounded; an earlier version of this section said "never wraps", which the probe contradicts.
- **After the run** the primary WALs went to 0 (idle drop) but the follower replica files stayed at 1,449 MB
  for the next 4 minutes, **99.6% of it `-wal`** (`logs/ds/iter-replsplit.txt`; `.db` 5.9 MB, 79 shards; 77 WALs over
  1 MB). Followers keep replicas by design, so the open question is narrower than reclaiming them: the follower
  absorbs a generation only on a reset push, so a shard's last generation is never folded after a clean drop.
- **Sizing:** disk per shard is about `20-47 MB x (1 + followers)` while it is continuously written, not `16 MB x`.
  Caveat: the TPC-B driver keeps each tenant busy with no gap (worst case).
- **Decided and built:** follower-side absorb of a quiet shard's last generation (above). Not done, and not needed for
  it: lowering the checkpoint threshold or a size-triggered primary `RESTART`/`TRUNCATE` (blocks writers, resets the
  generation followers follow, the cost the 2026-10-21 decision avoided) for the 20-47 MB high-water of a hot shard.
