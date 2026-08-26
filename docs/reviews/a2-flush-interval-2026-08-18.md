# The rig was the bottleneck twice: the flush interval, then the load driver — 2026-08-18

**Two harness defects, one day, both of which had been recorded as fathom limits.** Fixing the
first took 512 from 22,599 errors to 4; fixing the second answered the 1024-tenant question that
had been open since 2026-08-14, at **2,776 txn/s with zero tenants shed**. With a harness that
finally reports honestly, the ceiling is now bracketed: **1024 clean, 2048 collapsed, nodes alive
throughout.**

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

## 1024 ANSWERED: 2,776 txn/s, zero tenants shed

**Resolved the same day.** The hang was the load driver, and with it fixed 1024 replicating tenants
run to completion on this rig:

    1024   409600 txns   2775.9 txn/s   p50 163ms  p95 405ms  p99 1838ms   12042 errs   shed 0

`shed 0` means **every one of the 1024 tenants completed all 400 of its transactions** — the 12,042
errors (2.9%) are per-transaction retries, not abandoned work. All 1024 shards placed across the
five nodes (197/200/186/222/219), node binary memory **30–33 MB**.

So the answer to the question this rig could not previously ask is: **yes, at ~2,776 txn/s.**

### The driver bug

`Task.async_stream(timeout: :infinity)`. Every layer beneath it was already bounded — the Mint
transport recv at 15 s, `establish/3` at 8 attempts, a per-transaction `rescue` — and `:infinity` at
the top undid all of it. ONE wedged tenant out of 1024 hung the entire sweep and produced **no
result at all**, not even a partial one. The accounting for a shed tenant already existed
(`crashed * per_client`); the `:infinity` simply guaranteed it could never fire.

Fixed in two layers, because they catch different things:

* **Graceful** — a worker past `--worker-timeout` (default 300 s) stops issuing new transactions and
  returns the latencies it already collected. A killed task returns *nothing*, so without this a run
  with a few slow tenants would silently discard their samples and report an aggregate describing
  only the fast ones.
* **Backstop** — the `async_stream` timeout, 30 s above that, for a worker blocked *inside* a single
  call, which never reaches the graceful check.

And **`shed_tenants` is now reported as its own column**, because the first completing run reported
17,287 errors and working out that ~43 were killed tenants rather than 17,287 bad transactions took
arithmetic on the ratio. A harness should not make a reader infer that.

### Two runs that were NOT the fleet, and how they were told apart

The shed column paid for itself immediately. A second 1024 run read **304.8 txn/s with 818 tenants
shed** — an apparent catastrophic regression. It was the rig: three heavy runs back to back with no
restart, which is exactly the contamination signature AGENTS.md describes (run 1 healthy, runs 2–3
collapse). A full `down`/`up` and the same command gave 2,775.9 txn/s with **0** shed. Without the
column that run would have looked like a fathom collapse rather than a dirty rig.

## 2048 is past the ceiling, and the cliff is sharp

Run on a freshly restarted rig with a 20-minute per-worker budget, so the driver's own deadline
could not be what was measured:

    2048   819200 txns   257.7 txn/s   p50 124ms  p95 1376ms  p99 4939ms   506196 errs   shed 814

* **257.7 txn/s against 2,775.9 at 1024** — a 10.8x collapse for a 2x load increase.
* **814 of 2048 tenants shed** (40%): they did not finish 400 transactions inside 20 minutes.
  Shed work is **64%** of the error count, which is precisely why it is reported separately —
  read as raw errors this looks like half a million failed transactions rather than 814 tenants
  that ran out of clock.
* Sampled mid-run the fleet was moving **32-38 queries/sec per node**, against ~3,900 at 1024.

**All five nodes survived, healthy, with memory bounded** (707 MB and 183 MB against the 1 GiB
budget). That is the whole point of the byte bound and it is the difference from 2026-08-14, when
**1024 alone** OOM-killed nodes on a 94 GiB VM. Thoroughly overloaded, fathom now refuses writes
instead of dying.

`:overloaded` tracks the saturation cleanly across the sweep — **0 at 512, 8,419/9,294 at 1024,
16,807/12,879 at 2048** — which is the signal to trust when sizing, rather than the raw error count.

### The bracket, and why the shape matters

| tenants | result |
|---|---|
| 256 | clean |
| 512 | clean — 3,340 txn/s, 4 errors |
| 1024 | **clean — 2,776 txn/s, 0 shed** |
| 2048 | collapsed — 258 txn/s, 40% shed, nodes alive |

**On this one box ~1024 is the working limit.** Between 1024 and 2048 is a **cliff, not a slope** —
worth stating because a slope would let an operator run near the edge and read the degradation
coming. This does not. Whatever headroom signal is used, it cannot be throughput.

None of this separates "fathom saturates" from "this box saturates": five nodes, a load driver,
MinIO, Postgres and nginx share 12 vCPUs, and every write ships to four peers. That is exactly the
question `docs/a2-bare-metal-plan.md` exists to answer.

## The flush interval matters at 1024 too — 5.6x throughput, 21x errors

The earlier version of this report said the flush interval's role at 1024 was **confounded**: it was
changed between two runs that both produced no result, so it had only ever been isolated at 512.
Closed with the missing cell — same image, same fixed driver, same 300 s worker budget, both arms on
a freshly restarted rig, only the flush differs:

| 1024 tenants | flush = 5,000 | flush = 30,000 |
|---|---|---|
| txn/s | **500.0** | **2,775.9** (5.6x) |
| errors | 253,081 (62%) | 12,042 (2.9%) |
| shed | **0** | **0** |
| `:overloaded` | **66,441 / 69,270** | 8,419 / 9,294 |
| `:already_in_flight` | 6,173 / 4,204 | 20,079 / 27,959 |
| node binary | 32 / 23 MB | 30 / 33 MB |

Same mechanism as at 512 and even more pronounced: `:overloaded` explodes ~8x and dominates every
other reject, because a checkpoint every 5 s means shards periodically re-ship their WHOLE WAL and
that is what fills the per-node byte budget. Note `:already_in_flight` runs *lower* in the bad arm —
fewer pushes even get attempted, because the budget refuses them first. Memory stays bounded and low
in both arms (23–32 MB), so the budget is working hard and correctly.

### The clean decomposition this finally allows

**`shed 0` in BOTH arms** is the load-bearing detail. It means the driver fix is independent of the
flush setting, and the three contributions separate cleanly:

| change | what it determines |
|---|---|
| the feedback-loop cap + byte budget (`ca5f377`) | **whether the nodes survive at all** |
| the driver's `timeout: :infinity` (`b516ada`) | **whether you get a result at all** |
| `SHARD_FLUSH_INTERVAL_MS` | **whether that result is good** (2,776 txn/s / 2.9%) or bad (500 / 62%) |

All three were required. Only the first was fathom's product code.

## The deferred retry, validated at both scales (2026-08-19)

`Session` now retries a push its OWN shipper refused (`:already_in_flight` / `:overloaded` — neither
ever reached the follower) by RE-ENTERING `handle_call({:commit, ...})`, skipping when a commit has
landed since it was armed. Same-image A/B, `REPLICATION_CATCHUP_MS=0` as the control, fresh rig for
every arm:

| | retry ON | retry OFF |
|---|---|---|
| 512 txn/s | **3,438.7** | 3,420.5 |
| 512 errors | **29** | 68 |
| 1024 txn/s | **2,785.6** | 2,759.3 |
| 1024 errors | **16,848** | 17,790 |
| 1024 p99 | 1,735 ms | 1,849 ms |
| shed (both) | 0 | 0 |

**It costs nothing at either scale and the error count is LOWER in both arms.** Worth stating
because an earlier design — the same feature, planning and shipping in parallel rather than
re-entering the commit path — measured **2,697 txn/s / 2,486 errors at 512**, i.e. -15% throughput
and 35x the errors. The difference is entirely in routing through the existing serialization instead
of alongside it — four designs were tried before that landed, and the three that shipped
alongside the commit path rather than through it all measured worse.

One trap it also settles: 1024's error count varies run to run at saturation (12,042 on 2026-08-18,
16,848 and 17,790 here). **Do not read a single 1024 error count as a regression** — the control arm
had MORE errors than the change being tested. Only a same-session A/B discriminates at this scale.

## What the 1024 numbers still say is saturating

`:already_in_flight` 20,079 / 27,959 and `:overloaded` 8,419 / 9,294 per node. The byte budget IS
firing at 1024 where it was silent at 512, so the replication links are genuinely saturated on this
one VM — the nodes are healthy and bounded (30–33 MB), they simply cannot ship faster. That is the
honest ceiling of a single 12-vCPU box running five nodes, a load driver, MinIO, Postgres and nginx,
and it is the thing three separate machines would answer (`docs/a2-bare-metal-plan.md`).

## Superseded: the earlier reading that 1024 was unanswerable

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

**That was true when written and was resolved the same day** — see the 1024 section above. The rig
could not ask the question; it can now, and the answer is 2,776 txn/s with nothing shed.

## What this changes

* **512 with replication on is CLEAN** (3,340 txn/s, 4 errors) at a realistic flush interval. The
  previously recorded "512 completes degraded, 4.6% errors" was measuring the harness.
* The **"replication on is a <=256-tenant configuration"** line is superseded twice over: 512 is
  clean, and **1024 completes at 2,776 txn/s with 0 tenants shed** once the load driver stops
  hanging on its own `:infinity` timeout.
* Any future replication throughput run **must** set a realistic `SHARD_FLUSH_INTERVAL_MS`, or it is
  measuring checkpoint churn. `docker-compose.yml` now takes it from the environment for exactly
  this reason.

## The confound is CLOSED — flush=5,000 at 512 varies ~9x run to run (2026-08-19)

The section below recorded an unattributed gap: the flush=5,000 arm measured 22,599 errors on
2026-08-18 against 9,480 on 2026-08-17, on different images. Settled by asking a different question
— not "which image", but "how much does this configuration vary at all". Three runs, ONE image,
identical config, fresh rig each:

| run | txn/s | errors |
|---|---|---|
| 1 | 2,766.1 | 6,683 |
| 2 | 2,938.4 | 3,420 |
| 3 | 2,091.8 | **30,774** |

**A 9x spread with nothing changed.** Both historical numbers sit inside it, so there was never an
image difference to attribute — the gap was the regime's own variance.

**Why this configuration in particular is unstable** (consistent with the numbers, not separately
instrumented): flush=5,000 sits right at the saturation boundary. Every checkpoint makes
`Primary.plan/3` re-ship a whole WAL rather than a delta, which sometimes fits inside the per-node
byte budget and sometimes tips it — and once `:overloaded` starts firing it cascades. Run 3's
profile fits that: the WORST error count came with the LOWEST p50 (69.9 ms against 112/114 ms),
which is the shape of many requests failing fast rather than everything running slow.

**The operational reading:** a single error count from a saturated arm is not a measurement. This is
the third time tonight the same trap appeared — the 818-shed run, the 1024 error counts, and now
this — and each time the cheap resolution was to run the control rather than reason about mechanism.
Compare same-session A/B arms, or a spread, never two numbers from different sittings.

## Superseded: the earlier open confound

**Partly closed:** the flush interval's effect at 1024 is no longer confounded — see the 1024 A/B
above (5.6x throughput, 21x errors). What remains open is a different, narrower thing:
the flush=5,000 arm at 512 (22,599 errors) is worse than the 2026-08-17 run at the same setting
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
