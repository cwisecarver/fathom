# The 512-tenant replication error rate was the rig's flush interval — 2026-08-18

A single-variable A/B, same image, rig restarted between arms. Changing
`SHARD_FLUSH_INTERVAL_MS` from the rig's 5,000 to a realistic 30,000 took the 512-tenant
replication run from **22,599 errors to 4**, and throughput up 49%.

**The measured error rate that made 512 look "degraded" was almost entirely a property of the test
harness, not of fathom.**

## The result

| 512 tenants, replication ON, identical image | flush = 5,000 | flush = 30,000 |
|---|---|---|
| txn/s | 2,241.3 | **3,339.8** (+49%) |
| errors | **22,599** | **4** |
| p50 / p95 | 76 / 247 ms | 118 / 243 ms |
| p99 | 1,176 ms | **585 ms** |

Per-node reject mix over the whole run (two nodes shown):

| reject | flush = 5,000 | flush = 30,000 |
|---|---|---|
| `:stale_wal_gen` | 9,440 / 8,463 | **0** |
| `:overloaded` | 3,651 / 5,353 | **0** |
| `:already_in_flight` | 10,535 / 12,798 | 10,374 / 8,874 — **unchanged** |
| quorum `IMPOSSIBLE` | 4,212 / 4,249 | 91 / 117 |
| node `:erlang.memory()[:binary]` | — | 17-18 MB |

## The mechanism, and it is mechanical

A durability flush **checkpoints** the WAL. A checkpoint starts a new generation with fresh salts,
so `Primary.plan/3` correctly stops computing a delta and returns `{:reset, 0, size}` — **the entire
WAL, from its header**. Flushing every 5 seconds therefore means every shard periodically re-ships
its whole WAL instead of a few pages.

That single fact explains all three columns that moved:

* **`:stale_wal_gen` -> 0.** These are pushes that were in flight when the generation moved. Fewer
  checkpoints, fewer straddles. It went to *zero*, not merely down.
* **`:overloaded` -> 0.** Whole-WAL re-ships are what filled the per-node byte budget, not the tenant
  write rate. The budget was reporting checkpoint churn.
* **`IMPOSSIBLE` 45x lower.** Both of the above cost quorum rejects; remove them and commits stop
  failing.

The 5-second default is not a mistake — the rig's own comment says it exists so failover scenarios
observe durability PUTs inside a scenario's window. It is simply **not a throughput setting**, and
every replication throughput number this rig has produced was taken under it.

## `:already_in_flight` is the top reject AND it is harmless

Worth stating plainly because the previous report guessed the opposite. `:already_in_flight` is
**unchanged** across both arms (~10k per node either way) and the good arm has **4 errors**. Ten
thousand of these rejects per node cost essentially nothing: the quorum absorbs them, because the
follower refusing is one the commit did not need.

So the straggler class is real (`replication_transport_test.exs` now pins it) and it is **not** the
throughput lever. The 2026-08-17 report's suggestion that it was "probably where the next real
availability win is" is **wrong**, and this is the measurement that says so.

## 1024 still produces no result — and it is the DRIVER, provably

The same flush=30,000 configuration run at 1024 tenants again produced no throughput number. This
time the cause was isolated rather than assumed:

* All five nodes **healthy**, each holding ~200 shards (the previous run had one node at **zero**).
* Node binary memory **41-58 MB**.
* **297 and 314 replication sessions per node, zero busy** — nothing on the server owes anyone a reply.
* Server logs **silent for 8 minutes** while the run was still "in progress".
* The driver container at **0.00% CPU** — blocked, not spinning.
* nginx logging **499** (client closed the request before the upstream answered), continuously.
* **And the decisive one: a fresh request through the LB for a brand-new shard answered `HTTP 200`
  in 4.9 ms while all of the above was true.**

A fleet that serves a cold-open in 5 ms is not the thing that is stuck. This is a load-driver
limitation at 1024 concurrent clients under the ~4x per-write latency replication adds, and it
belongs with the driving rules already recorded in `tpc-fleet-highconc-2026-07-23.md` rather than
with fathom's scaling story.

**It is still unproven that fathom handles 1024 replicating tenants.** What is now proven is that
the rig cannot currently ask it the question.

## What this changes

* **512 with replication on is CLEAN** (3,340 txn/s, 4 errors) at a realistic flush interval. The
  previously recorded "512 completes degraded, 4.6% errors" was measuring the harness.
* The **"replication on is a <=256-tenant configuration"** line is superseded: <=512 is clean and
  measured.
* Any future replication throughput run **must** set a realistic `SHARD_FLUSH_INTERVAL_MS`, or it is
  measuring checkpoint churn. `docker-compose.yml` now takes it from the environment for exactly
  this reason.

## An open confound, recorded rather than hidden

The flush=5,000 arm above (22,599 errors) is worse than the 2026-08-17 run at the same setting
(9,480 errors). Those are **different images** — this one carries the `inflight` retention fix
(`3a6c6a3`) — so the two are not comparable and the difference is unattributed. Two candidates: the
fix lets the catch-up loop stop waiting on followers that already answered, so it may now re-ship
sooner; or ordinary rig variance. **Not investigated.** It does not affect the A/B above, which is
same-image on both arms, but it should be resolved before anyone quotes a flush=5,000 number.

## Reproduce

```bash
cd deploy/chaos
./chaos.sh down && ./chaos.sh build
SHARD_FLUSH_INTERVAL_MS=30000 REPLICATION_ENABLED=true ./chaos.sh up
TPC_NET=container TPC_DRIVER=elixir REPLICATION_ENABLED=true ./chaos.sh tpc-fleet 512
```

Confirm the setting actually took before trusting the run — the rig has a default and a silent
fallback is exactly how a one-variable experiment becomes a zero-variable one:

```bash
docker exec fathom-chaos-fathom1-1 /app/bin/fathom rpc \
  'IO.inspect(Application.get_env(:fathom, :shard_flush_interval_ms))'
```
