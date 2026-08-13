# The single-shard write convoy is SQLite's, and it is a cliff rather than a slope

**2026-08-13.** AGENTS.md has long contrasted the single-shard TPC-C lock convoy against the
fleet case having none. This locates it: **concurrent writers to one shard file do not degrade
gradually — past a threshold, total successful throughput COLLAPSES and keeps falling as you add
writers.** Nothing in fathom causes it; it reproduces with fathom's entire data path removed.

Found by chasing a CI flake (`Fathom.Bench.TpccSweepTest` dying on `SQLITE_BUSY`, fixed in
`446eb14`), which turned out to be the visible tip of this.

## Measured

Raw `Exqlite.Sqlite3` against one file. **No Hrana, no executor, no coordinator, no lease** — N
BEAM processes, each opening its own connection and running `BEGIN IMMEDIATE` → `INSERT` → `COMMIT`
10 times. `journal_mode=WAL`, `synchronous=FULL` (fathom's settings), `busy_timeout=200ms`.

| writers | BUSY | succeeded | p50 acquire | max acquire |
|---:|---:|---:|---:|---:|
| 8 | 0 | 80 | 0 ms | 37 ms |
| 10 | 0 | 100 | 0 ms | 78 ms |
| **12** | **85** | **35** | 1 ms | 270 ms |
| 14 | 115 | 25 | 15 ms | 267 ms |
| 16 | 139 | 21 | 205 ms | 311 ms |
| 24 | 222 | 18 | 306 ms | 620 ms |

Two things in that table matter more than the threshold itself.

**It is congestive collapse, not saturation.** Successful transactions go 100 → 35 → 25 → 21 → 18
as writers increase. Past the cliff, adding writers reduces *total* completed work — the system does
less absolute work with more offered load, which is the defining shape of congestive collapse rather
than of a queue reaching capacity.

**The winners are still instant.** At 12 writers, 71% of attempts fail while `p50 acquire` is 1 ms.
It is not that everyone got slow; it is that most never got in at all. That is the signature of an
unfair retry, and it is exactly what SQLite's busy handling is: **there is no queue.** Every waiter
independently sleeps a delay ladder and re-attempts, so a waiter can lose repeatedly while others
sail through, and it gives up when its own elapsed budget runs out.

## The threshold moves with the timeout budget — do not quote one number

The cliff sits between **10 and 12 writers at a 200 ms timeout**. It is NOT a fixed writer count:
a larger `busy_timeout` absorbs more contention before collapse, because each waiter gets more
ladder attempts before giving up.

At fathom's real setting (`busy_timeout=5000`, `Connection.open/1`), the TPC-C driver is clean at
8 threads (320 txns in ~930 ms, sub-millisecond lock holds) and starves at 16 — where six
resubmissions of a transaction, each waiting the full 5 s, still end in BUSY.

So the honest statement is a range and a mechanism, not a constant:

> On this hardware, one shard supports roughly **8–16 concurrent writers** before write contention
> collapses, and where in that range depends on `busy_timeout`. A longer timeout buys absorption,
> not fairness — it moves the cliff, it does not remove it.

An earlier note in this session said "~8 writers per shard"; that was the 200 ms figure quoted
against fathom's 5000 ms configuration, and is corrected here.

## What it means for fathom

**Nothing is broken and nothing needs fixing in the data path.** The convoy reproduces with every
fathom layer removed, so it is a property of SQLite's single-writer file plus an unfair busy
handler.

It does, however, quantify the architecture's own thesis. Fathom's answer to write load is **more
shards, not more writers per shard** — a tenant is one file with one writer, and throughput scales
by partitioning tenants across nodes. That is why `tpc-fleet` shows no intra-shard convoy at 4096
tenants while a single-shard TPC-C run does: the fleet case never puts 16 writers on one file.

Two practical consequences:

* **A tenant whose own workload needs >8–16 concurrent writers is the wrong shape for one shard**,
  and no amount of tuning changes that. It wants splitting, not a bigger timeout.
* **Retrying `SQLITE_BUSY` is correct below the cliff and useless above it.** Below, a BUSY is one
  unlucky waiter and a resubmit succeeds. Above, every resubmit rejoins the same scrum — which is
  why the TPC-C driver retries a bounded number of times and then fails loudly rather than looping.

## Not established

* **Whether exqlite's custom handler is fairer or less fair than SQLite's default.** `connection.ex`
  says it walks the same delay ladder and only adds cancel/liveness polling, and this probe used it,
  so the collapse is present with it. Whether the stock handler collapses at the same N is untested
  and would need `PRAGMA busy_timeout` — which fathom deliberately avoids (expert review
  2026-07-24 #1: it makes the wait uninterruptible and pins a dirty-IO scheduler thread).
* **Whether `synchronous=FULL` moves the cliff.** Every run here used it. A shorter commit holds the
  write lock for less time, so `NORMAL` would plausibly push the threshold out; not measured.
* **Anything about other hardware.** One machine, one run per point.

## Reproduction

The probe is ~40 lines of `Exqlite.Sqlite3` (N spawned processes, timed `BEGIN IMMEDIATE`, counted
BUSY). Through fathom's own driver, the same collapse is:

```elixir
# clean at 8, starves at 16 — MIX_ENV=test
Fathom.Bench.Tpcc.sweep(max_w: 1, threads: 16, txns: 640, scale: 0.001)
```

**Keep the busy timeout small when probing this.** The first attempt at this measurement used
fathom's 5 s timeout with 24 writers × 40 rounds, where every lost lock costs 5 seconds — it ran for
twenty minutes and produced nothing before being killed. Starvation is a *ratio*; a 200 ms timeout
shows the identical shape in seconds.
