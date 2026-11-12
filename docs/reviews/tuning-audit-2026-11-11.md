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
| 9 | OTP 29.1.2 vs 28.5 | **Not scored.** Two A/Bs, a diagnostic pair and an instrumented diagnostic run all failed the same way in every arm, including the OTP 28 image that was clean an hour earlier (0 errors, 4.2-4.4k txn/s): 32k-56k failed transactions and 3.4-3.9k txn/s. The instrumented run shows why: every node logs `heartbeat not valid for > margin+steal_margin - fencing writes ... Writes 503` and `durability flush FAILED (:ownership_unconfirmed)` on about 200 shards, and the LB logged 2.5M `200`s and no 5xx (the errors are Hrana results). The driver's `worker_deadline` label counts any txn shortfall (`tpc_driver.exs:874`), so it is not a deadline. The S3 lease path is failing under the current rig conditions; images, VM config, disk space, host load, thermals and other compose projects were checked and did not explain it. Cause not isolated; stopped per the repo's stop-after-2 rule. **Open availability item, not closed as an environment problem:** that the same OTP 28 image was clean earlier shows the trigger is environmental; it does not show fathom reacts to it correctly. All five nodes declared their heartbeat invalid at the same time and fenced writes on about 200 shards each, which is either a store slow or failing enough to miss renewals, or a heartbeat-path weakness that a slowdown exposes, and the rig reached it by itself. Discriminating next step: heartbeat-renew PUT latency from `[:fathom, :s3, :op]` against the 10 s cadence and 30 s TTL, plus MinIO and toxiproxy logs from the same window. Whether to spend a run on it is the maintainer's call. |
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
- **The rig degraded late in the session** (see #9: nodes fencing writes because the S3 lease heartbeat
  went invalid). All scored A/Bs were run before it, each against its own interleaved default arm, all
  with 0 errors.
- **One #18 sample is printed in exponent form** (`4.3E+3`): the driver rounds txn/s to 0.1, so that value is
  4300.0 at the driver's precision, not an estimate.
