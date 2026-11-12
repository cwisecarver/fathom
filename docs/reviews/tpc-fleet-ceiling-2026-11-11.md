# What limits `tpc-fleet 1024` on the chaos rig, and what did not move it (2026-11-11)

Why the 1024-tenant, replication-on fleet stops at about 4.2-4.3k txn/s on the rig, and the two
tuning attempts made against that ceiling. Split out of
[same-region-followers-2026-11-11.md](same-region-followers-2026-11-11.md), which found the ceiling
while measuring follower latency.

Common to every run below: the 5-node `deploy/chaos` rig on one 12-vCPU colima VM (host 18 cores),
`REPLICATION_ENABLED=true SHARD_FLUSH_INTERVAL_MS=30000`, closed-loop `TPC_NET=container
TPC_DRIVER=elixir ./chaos.sh tpc-fleet 1024`, image built from `f6280fd0`, a fresh fleet and
`colima restart` before each run, zero errors, no `clock lags the store` lines. Scripts and raw logs
are in `logs/ds/` (not committed): `cpusplit.sh`, `sbwt.sh`, `msacc.sh`, `sched.sh`. The host also
ran macOS background daemons (`corespotlightd` about 65% CPU when checked), so every number is
relative.

**Summary.** The VM is out of CPU. Turning off scheduler busy-wait gained 1.0% (noise); `+S 3:3`
lost 6.0%; `:msacc` cannot attribute CPU on an oversubscribed VM. Neither flag is enabled.

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
`ERL_FLAGS` through a passthrough added to `docker-compose.yml` for these runs (since removed: the rig's existing `ERL_ZFLAGS` hook does the same job and is what to use) and confirmed on the
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

Because five 12-scheduler BEAMs compete for 12 vCPUs, I tried fewer schedulers: `ERL_FLAGS="+S 3:3"` (use `ERL_ZFLAGS` today)
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

**What this does not suggest: batching.** Batching a transaction's seven statements into one Hrana
request would raise txn/s, but it changes the workload, not fathom: `tpcb_txn` runs an interactive
`BEGIN IMMEDIATE ... COMMIT`, one request per statement, which is what an unchanged `django-libsql`
client does, and a batched driver's number would not be comparable with any earlier `tpc-fleet`
figure. A batched driver, if wanted, is a separately labelled "pipelined client" shape. The lever for
fathom itself is server CPU per request (about 240 µs per statement on the nodes).
