# TPC-C on the Elixir driver — 2026-07-24 (the bug it surfaced: Hrana stream resume)

Adding TPC-C to the Elixir driver (`deploy/chaos/tpc_driver.exs`, over `Filo.Client`) was a
straightforward port of `tpcc_deck.py` — the 9-table schema, the scaled recursive-CTE seed, all five
weighted value-feeding transactions, the W-sweep, tpmC. But it ran **26× slower than the Python
driver with ~20% errors through the LB, while clean direct-to-node**, and chasing that gap uncovered
a real resilience bug in **`Filo.Client`**, not in the port. This is the record of the hunt (dead-ends
included) and the fix.

## Symptom

Same workload, same rig, single warehouse:

| driver | path | tpmC | neworder p99 | errors/100 txns |
|---|---|---:|---:|---:|
| python | LB | 1,604 | 37 ms | 0 |
| elixir (pre-fix) | LB | 70 | 2,658 ms | ~9 |
| elixir (pre-fix) | **direct-to-node** | 2,303 | 31 ms | **0** |

The Elixir port was clearly *correct* — direct-to-node it matched Python. Through the LB it fell apart.
The errors were `STREAM_NOT_FOUND` (HTTP 400) on `BEGIN IMMEDIATE` and `{:transport, :closed}`
mid-transaction; the multi-second tails were the knock-on stalls, not the root.

## The hunt — and the four dead-ends (so nobody re-chases them)

The rig has a lot of moving parts, so the temptation was to blame one. Each was **tested live and
ruled out** — none fixed it:

1. **Rebalancer moving the hot shard.** tpcc concentrates all load on one shard, which blows past
   `REBALANCE_HOT_QPS_FLOOR`, so a mid-run handoff seemed obvious. Disabled it (`rebalancer_enabled
   false`) → **still errored.**
2. **Idle-drop cycling the coordinator.** Raised `shard_idle_ms` to 15 min → **still errored.**
3. **The periodic flush's checkpoint.** Raised `shard_flush_interval_ms` to 1 h → **still errored.**
4. **nginx cross-node retry.** `proxy_next_upstream` retrying a POST onto a different node (streams
   are node-pinned) was a good theory; set it `off` and reloaded → **still errored.**

Server debug logs (all nodes) showed **no** stream/coordinator teardown at all — the coordinator
stayed `active` the whole time. So the *coordinator* wasn't dying; the *Filo stream* was, while its
owner lived.

**The decisive isolation:** drive the node's **direct Hrana port**, bypassing nginx → **0 errors,
tpmC 2,303, p99 31 ms.** Clean. So it was purely a **client ↔ LB** interaction — and one Python's
client didn't trigger.

## Root cause

A Hrana stream is long-lived and **node-pinned**, held by the client across many statements via the
baton. The client↔LB keep-alive connection is *not* long-lived: **nginx recycles it after
`keepalive_requests` (default 1000)**. One tpcc run at 100 txns is ~1,800 statement-requests, so
nginx closes the connection mid-run.

- **Python's `http.client` has `auto_open=True`** — it silently reopens the socket and resends with
  the **same baton**, so the stream *resumes* and the LB recycle is invisible. That is why Python was
  clean at 1,800 requests through the LB.
- **`Filo.Client` did not.** It surfaced the `:closed` as an error; the driver's error path
  reconnected with a **fresh** stream (baton dropped), **abandoning** the open `BEGIN IMMEDIATE` on
  the node. The abandoned stream held the write lock until its 10 s idle timeout; the next txn stalled
  on that lock, and a stall long enough tripped *its* stream's 10 s idle timer → the `STREAM_NOT_FOUND`
  cascade. tpcb never hit this (short txns, load spread one-shard-per-client); tpcc did (one hot shard,
  one stream held across thousands of statements).

## The fix (Filo.Client, `3adbcd9`)

`Filo.Client.pipeline` now retries once on a `:closed` transport error, reopening the connection while
**preserving the baton** — resuming the stream instead of abandoning it, exactly as a real SDK (and
Python's `auto_open`) does:

- Exactly-once is preserved by the baton's **sequence number**: if the request *had* been processed
  before the drop, the resend is rejected (`BATON_REUSED`), never double-applied.
- The transport contract gained a `:closed` sentinel for a dropped connection (retryable);
  `Filo.Client.Transport.Mint` maps Mint's closed/econnreset/epipe errors to it (via `__struct__`
  matches, so it stays compile-safe when `:mint` is absent). Two unit tests cover it (transparent
  resume on one drop; error surfaced after one retry, no loop).

This benefits **every** `Filo.Client` user, not just tpcc — any long-held-stream workload through a
keep-alive-recycling LB needs it.

## Validation — parity restored

| W | threads | python tpmC | elixir tpmC (fixed) | elixir errs |
|---:|---:|---:|---:|---:|
| 1 | 1 | 1,604 | 2,083 | **0** |
| 1 | 4 | 1,765 | 1,830 | **0** |
| 2 | 4 | 1,528 | 1,969 | **0** |

Through the LB, host and container, `chaos.sh` dispatch all clean (containerized in-network:
tpmC 3,390, 0 errs). Elixir now matches (slightly beats) Python — vs 69/64 tpmC with 79 errors before
the fix. The residual ~1 s neworder p99 both drivers show is the **legitimate single-warehouse TPC-C
lock convoy** (contrast tpcb's one-writer-per-tenant, no convoy), not a driver artifact.

## Lessons

- **Direct-vs-LB is the fastest way to split a client/server distributed bug.** Four plausible
  server-side causes (rebalancer, idle-drop, flush, cross-node retry) were all wrong; one bypass test
  localized it in seconds.
- **A stateful stream over a stateless LB needs transparent client-side resume.** LBs recycle
  keep-alives; a client that turns that into a hard error abandons server-side transaction state
  (and its locks). Real SDKs resume by baton — now Filo.Client does too.
- Ruling a hypothesis out **live** (and recording it) is worth as much as confirming the real one.

## TPC-C tenant fleet — the multi-tenant sweep

`tpcc` (the W-sweep) is the *single-shard* convoy: many workers on one warehouse. **`tpcc-fleet`** is
the multi-tenant analog of `tpc-fleet` — each **tenant** is its own shard running a 1-warehouse TPC-C
mix, swept by tenant count, so the heavier value-fed load partitions across the nodes. Live in-network
sweep (per_client=20, scale 0.005):

| tenants | tpmC | txn/s | p99 | errs |
|---:|---:|---:|---:|---:|
| 16 | 20,093 | 719 | 47 ms | 0 |
| 64 | **25,612** | 937 | 152 ms | 0 |
| 128 | 24,512 | 886 | 344 ms | 0 |
| 256 | 22,894 | 830 | 504 ms | 0 |
| 512 | 23,401 | 862 | 529 ms | 0 |
| 1024 | 22,233 | 813 | 654 ms | 0 |
| 2048 | 19,661 | 719 | 1,170 ms | 0 |
| 4096 | 11,831 | 437 | 4,099 ms | 0 |

**8,144 TPC-C tenant shards, ~5M queries, spread 1.12×** across the 3 nodes — **zero errors at every
step, to 4096 concurrent held-stream tenants** (the resume fix holds under the multi-tenant load too).
Aggregate tpmC peaks ~25.6k at 64 tenants and holds ~22–25k through 1024, then declines (19.7k @ 2048,
11.8k @ 4096) as the one 12-vCPU VM saturates — driver + 3 nodes + nginx + the full-TPC-C seed burst
all share it, so p99 climbs to ~4 s while errors stay 0 (contention, not failure). As always, the
absolute is one-box-bound; the even per-node partition is the horizontal "millions" axis. The heavier
value-fed TPC-C mix partitions exactly like TPC-B (contrast `tpc-fleet`'s 1.10–1.15× spreads).

### Single-node baseline (what the partition actually buys on one VM)

Controlled same-session A/B — the same tenant counts driven through the LB (3 nodes) vs direct at one
node (`fathom1:8080`, so it holds *every* tenant):

| tenants | fleet tpmC | fleet errs | 1-node tpmC | 1-node errs | fleet/1-node |
|---:|---:|---:|---:|---:|---:|
| 64 | 25,777 | 0 | 23,715 | 0 | 1.09× |
| 256 | 24,366 | 0 | 22,756 | 0 | 1.07× |
| 1024 | 22,659 | 0 | 20,120 | 0 | 1.13× |
| 4096 | 19,146 | 0 | 12,232 | **8,700** | 1.57× |

At 64–1024 the fleet is only **~1.1×** a single node — both are CPU-bound by the same 12-vCPU VM, and
one BEAM node already uses all cores, so spreading across three node-processes barely moves throughput
(the "one box, not three machines" caveat, made concrete). But at **4096 the single node hits a
per-node wall**: holding all 4,096 active TPC-C tenants (shards + connections + WAL fds + schedulers,
while contending with the driver) it throws **8,700 errors** and falls to 12.2k tpmC, while the
three-node split stays **0 errors** at 19.1k. So on one box the partition's value is **capacity, not
speed**: each node carries only its 1/N of the active tenants, so no single node reaches the
concurrent-tenant ceiling. Raw ~N× throughput still needs N separate machines (CPU here is shared).

## Provenance

- `Filo.Client` transparent stream-resume on a dropped connection — filo `3adbcd9`.
- TPC-C in `deploy/chaos/tpc_driver.exs` (all five value-feeding txns + W-sweep; `--max-w/--threads/
  --scale`); `chaos.sh` `tpc_run` no longer forces python for tpcc — fathom `98a32a3`.
- `tpcc-fleet` mode + `chaos.sh` command (multi-tenant TPC-C sweep) — fathom `793f0da`.
- Reproduce: `TPC_DRIVER=elixir ./chaos.sh tpcc [max_w threads txns scale]` (single-shard convoy)
  or `TPC_NET=container ./chaos.sh tpcc-fleet [tenants_csv per_client scale]` (tenant fleet);
  direct-to-node baseline: `elixir tpc_driver.exs tpcc --lb http://localhost:18081 …`.
