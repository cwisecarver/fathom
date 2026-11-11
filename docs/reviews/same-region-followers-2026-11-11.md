# Same-region follower links, emulated with toxiproxy (2026-11-11)

What a write costs when a node's followers are in other availability zones of the same region, as
the closest approximation on the rig to Aurora DSQL's single-region case (the comparison
[cross-region-quorum-2026-10-30.md](cross-region-quorum-2026-10-30.md) left open). Code: `f6280fd0`,
filo 0.4.4, image built that day. Scripts: `logs/ds/samereg.sh`, `logs/ds/samereg-probe.sh` (not
committed).

## What same-region latency is

AWS commits to nothing ("single-digit millisecond"). Measured numbers:

| Source | Result |
|---|---|
| [Bits and Cloud, 2023-10](https://www.bitsand.cloud/posts/cross-az-latencies) — `ping` between `t3.micro` in every AZ pair, 28 regions | fastest pairs 0.39-0.47 ms RTT (`us-east-1b`-`c` 0.44, `us-west-2a`-`c` 0.46); slowest `sa-east-1` 2.42 ms; "the vast majority of AZ pairs had sub-millisecond latency". The full per-pair table is collapsed on the page, so **no all-region mean was available**. |
| [AWS Architecture Blog, AZ affinity](https://aws.amazon.com/blogs/architecture/improving-performance-and-reducing-cost-using-availability-zone-affinity/) | "single digit millisecond roundtrip latency between AZs"; intra-AZ p50 about 0.3 ms. Its worked example assumes 1.5 ms cross-AZ p50 and 1 ms minimum; those are the author's assumptions, not measurements. |

So the target is **about 0.5 ms RTT typical, 0.4-2.4 ms across regions and pairs**.

## What toxiproxy can and cannot do

Measured on the rig, keep-alive HTTP to fathom2's health port from fathom1, 399 requests per cell,
time to first byte:

| path | p50 | p99 |
|---|---|---|
| direct | 0.060-0.063 ms | 0.094 ms |
| through toxiproxy, no toxic | 0.107-0.111 ms | 0.139-0.194 ms |
| through toxiproxy, `latency=1` each way | 3.28-3.31 ms | 3.42 ms |

- The proxy hop itself costs about 0.05 ms per request (0.06 direct vs 0.11 through, both over TCP).
  This does not contradict the "about 450 µs" in `docs/a2-quorum-replication.md` (2026-08-08), which
  compares a quorum commit through toxiproxy over real TCP (about 550 µs) with the in-process
  loopback harness (96 µs). That gap includes the socket and TCP path the in-process run skips, not
  only the proxy; the two figures measure different things.
- The latency toxic takes **whole milliseconds** (`latency`, `jitter` are integers). 0.25 ms each
  way cannot be set.
- The smallest toxic, 1 ms each way, produces about **3.2 ms** of added round trip here, not 2. The
  cause is not isolated, but it is not specific to this rig: the 2026-08-08 sweep in
  `docs/a2-quorum-replication.md` (different harness, real TCP) read 26.1 ms for 10 ms each way
  (nominal 20 + 0.56), 73.2 ms for 30 ms (nominal 60) and 131.7 ms for 60 ms (nominal 120), an
  excess of about 5-13 ms in every row. The latency toxic delivers more than its nominal delay in
  both harnesses; the excess is not a constant fraction, so no correction factor is claimed.

Neither point is near 0.5 ms. The two arms below bracket the target from both sides (0.11 ms under
it, 3.3 ms over it). A point at 0.5 ms needs `tc netem` in the containers, not toxiproxy.

## Result

`REPLICATION_ENABLED=true SHARD_FLUSH_INTERVAL_MS=30000 REPL_VIA_TOXI=1 ./chaos.sh up`, then
`TPC_TAIL_ARMS=open TPC_OPEN_RATE=500 ./chaos.sh tpc-tail 256 200 1000 2` (open loop, 500 txn/s, 256
tenants, 5 nodes; TPC_NET=container TPC_DRIVER=elixir). Each arm after `colima restart`; no node logged
`clock lags the store`; toxics read back (0 and 10 on the 5 `repl-*` proxies).

| follower links | run | txn/s | p50 | p99 excl. first 10 s (CI95) | errors | replication commit mean |
|---|---|---|---|---|---|---|
| toxiproxy hop only (~0.11 ms RTT) | 1 | 499.9 | 3.31 ms | 7.1 ms (6.7-8.0) | 0 | 1.13 ms (both runs) |
| toxiproxy hop only | 2 | 499.9 | 3.20 ms | 7.0 ms (6.5-8.0) | 0 | |
| 1 ms each way (~3.3 ms RTT) | 1 | 499.9 | 6.01 ms | 9.2 ms (9.0-9.3) | 0 | 3.83 ms (both runs) |
| 1 ms each way | 2 | 499.9 | 6.03 ms | 9.3 ms (9.2-9.5) | 0 | |

The hop-only arm reproduces the 2026-10-30 control (p50 2.88 ms, p99 5.8-6.2 ms; commit mean
1.07 ms) to within the host's noise. Adding about 3.2 ms of round trip adds +2.7-2.8 ms to p50 and
+2.2-2.3 ms to p99, so a write costs roughly 0.7-0.9 ms per ms of follower round trip at this load
(the 15 ms arm of the 2026-10-30 run read about 1.0).

**Is it one round trip per write?** The commit mean rose 2.70 ms (1.13 to 3.83) and the `collect` stage
0.52 to 3.27 ms, against 2 ms of nominal added delay. The nominal figure is the wrong yardstick: the
probe above measured the 1 ms-each-way toxic at +3.2 ms of real round trip, so +2.7 ms is below one
effective round trip, with no second one needed. The same toxiproxy overshoot is why the slope
below is per measured millisecond, and it is a slope for this toxic and load, not a general one.

**The commit histogram cannot give a p99.** Its buckets are 1/2/5/10/25 ms, and the counts are
cumulative over the whole run including the closed-loop runs and the 30 s flush windows. Share of
commits at or under each bound:

| arm | <=5 ms | <=10 ms | <=25 ms |
|---|---|---|---|
| hop only | 97.4% (103,182 / 105,984) | 98.9% | 99.5% |
| 1 ms each way | 96.6% (102,400 / 105,984) | 98.5% | 99.5% |

Both p99s sit in (10, 25] ms, which says nothing about DSQL's 7.4 ms. Only the means (1.13 and
3.83 ms) and the per-transaction `tpc-tail` p99 above are usable.

[INFERENCE] Interpolating that slope to a 0.5 ms RTT gives p50 about 3.6-3.7 ms and p99 about 7.3-7.4
ms for a whole TPC-B transaction. This is an interpolation between two measured points, not a run.

## Saturated: 1024 tenants, as far as the VM goes

The 500 txn/s runs above are light load. The same two arms on the closed-loop `tpc-fleet` shape
(`TPC_NET=container TPC_DRIVER=elixir REPLICATION_ENABLED=true ./chaos.sh tpc-fleet 1024`, 400
txns per tenant, 5 nodes), one run per arm, each after `colima restart`, image from `f6280fd0`. No
node logged `clock lags the store`; toxics read back as 0 and 10 on the 5 `repl-*` proxies.

| follower links | txn/s | p50 | p95 | p99 (CI95) | errors | replication commit mean |
|---|---|---|---|---|---|---|
| toxiproxy hop only (~0.11 ms RTT) | 4,153 | 226.6 ms | 349 ms | 404 ms (404-405) | 0 | 35.9 ms |
| 1 ms each way (~3.3 ms RTT) | 4,235 | 223.1 ms | 332 ms | 378 ms (377-379) | 0 | 39.4 ms |

At saturation the added round trip shows up in the commit mean (+3.6 ms, about one effective round
trip of 3.2 ms) and nowhere else that is readable. Throughput is the same within a single run's
noise (4,153 vs 4,235 txn/s). The p99 difference (404 vs 378 ms) is one run per arm and is not
interpreted: the "slower" arm reading lower is the sign of noise, not of a gain. The mechanism is the
2026-10-30 one: every transaction already waits about 225 ms for CPU on the one VM, and waiting on a
follower costs no CPU, so it replaces queueing time instead of adding to it. This closed loop
measures capacity, not what a client sees; the open-loop ladder below is the latency question.

## Open-loop ladder, 1024 tenants, 1,000-4,000 txn/s

`TPC_TAIL_ARMS=open TPC_OPEN_RATE=<r> ./chaos.sh tpc-tail 1024 <per_client> 1000 1`, `per_client` set
so each run lasts about 100 s, at r = 1,000 / 2,000 / 3,000 / 4,000 txn/s. Each run on its own fresh
fleet after `colima restart` (the rig's MinIO fills after about two back-to-back 1024-tenant runs,
`chaos.sh:2691`), arms interleaved per rate, same image as above. Zero errors, zero shed, no node
logged `clock lags the store`, toxics read back as 0 and 10. Latency is a whole TPC-B transaction
measured from its intended start. Script `logs/ds/sameladder.sh`.

| target | follower links | achieved | p50 | p99 excl. first 10 s | replication commit mean |
|---|---|---|---|---|---|
| 1,000 | hop only | 999.8 | 4.67 ms | 53.3 ms | 5.89 ms |
| 1,000 | 1 ms each way | 999.8 | 7.49 ms (+2.8) | 53.9 ms | 8.83 ms (+2.9) |
| 2,000 | hop only | 1,999.5 | 9.18 ms | 156.5 ms | 4.80 ms |
| 2,000 | 1 ms each way | 1,999.5 | 11.51 ms (+2.3) | 105.7 ms | 6.87 ms (+2.1) |
| 3,000 | hop only | 2,998.7 | 23.13 ms | 149.6 ms | 6.09 ms |
| 3,000 | 1 ms each way | 2,998.5 | 27.03 ms (+3.9) | 161.6 ms | 8.55 ms (+2.5) |
| 4,000 | hop only | 3,994.9 | 130.23 ms | 447.6 ms | 19.32 ms |
| 4,000 | 1 ms each way | 3,995.9 | 119.75 ms (-10.5) | 359.4 ms | 20.12 ms (+0.8) |

What the ladder supports:

- **The rig sustains 4,000 txn/s open loop** (99.9% of target, no errors) on 1024 tenants, with or
  without the added follower latency.
- **There is a knee between 3,000 and 4,000 txn/s.** Hop-only p50 goes 23 to 130 ms across it and the
  service p99 (excluding queueing before the send) reaches 331 ms. It is the CPU limit of the one
  VM, about 93% of the 4,317 txn/s maximum recorded for this shape, and not a replication effect.
- **Below the knee the added round trip costs about one effective round trip at p50 and in the commit
  mean** (+2.3 to +3.9 ms of p50 and +2.1 to +2.9 ms of commit mean for a measured +3.2 ms RTT).
  Above it the difference disappears into the queue (+0.8 ms commit mean, p50 lower by 10 ms).

What it does not support:

- **No p99 comparison between arms.** Single run per arm per rate, and the p99 is erratic inside one
  run: at 1,000 txn/s the 10-second windows of the hop-only run range from 7.6 to 123.6 ms. At 2,000
  txn/s the arm with the extra latency has the lower p99 (106 vs 157 ms), the wrong sign for a real
  effect. Read p50 and the commit mean; do not read the p99 columns as a result.
- **The 53 ms p99 at 1,000 txn/s is not explained.** The 500 txn/s run on 256 tenants had a 7 ms
  p99. This run has four times the tenants, and the host was busier (1-minute load 6.45 at the
  start, against about 2 earlier, with macOS background daemons). I have not separated the two.
- **Commit mean is not monotonic in rate** (5.9, 4.8, 6.1 ms at 1,000-3,000 txn/s). It averages the
  whole run, flush windows included, so compare arms at one rate and not rates against each other.

## Where the saturated rig's CPU goes

One more `tpc-fleet 1024` run (4,249 txn/s, p50 220 ms, p99 402 ms, no errors) with `docker stats` and
the VM's `top` sampled in steady state (`logs/ds/cpusplit.sh`; two samples inside the run, a third at
its tail, so treat the split as approximate). The VM has 12 vCPUs, the host 18.

| consumer | CPU (cores) |
|---|---|
| 5 fathom nodes | 1.4-1.55 each, about 7.3 together |
| load driver | 1.1-1.2 |
| nginx LB (12 workers) | 1.0-1.1 |
| MinIO, Postgres, toxiproxy | under 0.1 each |
| VM total | 59% user, 28% system, 5-6% softirq, 2% idle; 5-12% iowait |

The VM is out of CPU (about 2% idle), so 4.2k txn/s is a capacity limit of this box. About a third of
its CPU is kernel time (system plus softirq) on top of what the containers report as their own, which
is the container network and storage path, not BEAM or SQLite. Accounted container CPU is about 9.8
cores, roughly 2.3 ms per transaction (7 statements each: 2.87M queries for 410k transactions), and
about 1.7 ms of that is in the fathom nodes. One finding that did not need a profiler: more fleet
throughput on this rig needs more CPU or less CPU per statement; no latency setting moves it.

## Scheduler busy-wait A/B (`+sbwt none +sbwtdcpu none`)

`rel/vm.args.eex` leaves the flags commented out "behind an A/B" on this rig, which runs five BEAMs
of 12 schedulers on 12 vCPUs. Closed-loop `tpc-fleet 1024`, replication on, three runs per side,
interleaved A B A B A B, a fresh fleet and `colima restart` before each. The flags were passed with
`ERL_FLAGS` through a new passthrough in `docker-compose.yml` (empty by default) and confirmed on the
running nodes' command lines (`sbwt none`, `sbwtdcpu none`; absent in the A arm). Image from
`f6280fd0`; zero errors, no clock-lag lines. The 1-minute load was 5.2 at the start (macOS daemons).

| run | default txn/s | busy-wait off txn/s | pair delta |
|---|---|---|---|
| 1 | 4,327.6 | 4,359.3 | +31.7 |
| 2 | 4,375.2 | 4,397.0 | +21.8 |
| 3 | 4,307.1 | 4,384.6 | +77.5 |
| mean | 4,336.6 | 4,380.3 | **+43.7 (+1.0%)** |

All three pairs favour the flags, but the gain is 1.0% against a default-arm spread of 68 txn/s
(1.6%), and the arms overlap (the lowest flag run, 4,359, is below the best default run, 4,375). p99
read 377-383 ms against 383-395 ms. Three pairs cannot separate a 1% effect from noise, and the
2026-10-26 report found the same for a 2% change. **Not enabled in `vm.args.eex`:** it is not a
way past 4k on this rig, and the wake-up-latency risk the file warns about is unmeasured below
saturation. Run the open-loop ladder on it before enabling.

## Microstate accounting, and a scheduler-count A/B

`:msacc` (10 s, steady state of a `tpc-fleet 1024` run at 4,269 txn/s, all five nodes, via
`bin/fathom rpc`; `logs/ds/msacc.sh`) read the same on every node. Normal schedulers (12 per node):
emulator 37%, "other" 25-27%, sleep 17%, port 9-10%, aux 5%, gc 3-4%, check_io 2%. Dirty-IO
schedulers (10): emulator 75-77%, sleep 9-10%. Dirty-CPU and async threads: 100% asleep. Poll
threads: 96% asleep.

**This does not attribute CPU, and should not be read as if it did.** msacc reports wall-clock
thread state, and this VM is oversubscribed: 1-minute load reached 88-97 on 12 vCPUs with about 40
BEAM threads per node on five nodes, while `docker stats` gave each node about 1.5 cores. A thread
"in the emulator" for 37% of a window may be waiting in the OS run queue for most of it. What it does
show: schedulers are not idle (17% asleep), the dirty-IO pool that runs the SQLite NIF is the
busiest thread type, and dirty-CPU schedulers are unused. A useful CPU attribution needs per-thread
CPU time (`perf` or `/proc/<pid>/task/*/stat` in the VM), not msacc.

Because five 12-scheduler BEAMs compete for 12 vCPUs, I tried fewer schedulers: `ERL_FLAGS="+S 3:3"`
against the default, closed-loop `tpc-fleet 1024`, three pairs interleaved, fresh fleet each,
confirmed on the command lines (`logs/ds/sched.sh`):

| run | default txn/s | `+S 3:3` txn/s |
|---|---|---|
| 1 | 4,331.7 | 4,044.8 |
| 2 | 4,316.7 | 4,010.8 |
| 3 | 4,287.8 | 4,098.9 |
| mean | 4,312.1 | 4,051.5 (**-6.0%**) |

Three of three pairs are worse and the arms do not overlap (default spread 44 txn/s, `+S 3:3` 88).
p99 rose from 382-407 ms to 420-438 ms. **Fewer schedulers per node lose throughput here.** I have
no measured reason; one untested reading is that a node whose shards are busy can use cores its
neighbours leave idle only if it has the schedulers to run on. `vm.args.eex` is unchanged.

**Correction to an earlier suggestion.** I proposed batching a transaction's seven statements into
one Hrana request as the largest lever. That changes the workload, not fathom: `tpcb_txn` runs an
interactive `BEGIN IMMEDIATE ... COMMIT`, one request per statement, which is what an unchanged
`django-libsql` client does, and a batched driver's txn/s would not be comparable with any earlier
`tpc-fleet` figure. A batched driver, if wanted, is a separately labelled "pipelined client" shape.
The lever for fathom itself is server CPU per request (about 240 µs per statement on the nodes).

## Against Aurora DSQL

DSQL publishes 7.4 ms p99 single-region COMMIT latency ([paper](https://arxiv.org/pdf/2607.13276),
quoted in the 2026-10-30 report). The interpolated fathom p99 is the same size, but the two are not
the same quantity: fathom's number is a whole TPC-B transaction (several statements plus the commit)
over a fake network, DSQL's is the COMMIT alone on real AWS. Read it as "same order of magnitude, one
round trip to the nearest quorum per write", which is also the shape the cross-region run showed.

## What this does not show

- **No jitter, loss or bandwidth limit, and the client shares the host.** As in 2026-10-30. The p99
  windows of 44-144 ms at 30 s, 60 s and 90 s are the 30 s durability flush, present in both arms.
- **The host was not quiet.** `biomesyncd` and `corespotlightd` held about 65% CPU each when checked just
  before the runs. Both arms ran after that check, so it may or may not have persisted; the absolute numbers are relative, as for every rig result.
- **S3 stayed local.** Nodes in a real region pay S3 latency on cold open, lease and flush.
- **One run pair per arm, one commit mean per arm** (the Prometheus histogram is cumulative). The
  commit p99 only resolves to its 25 ms bucket.
