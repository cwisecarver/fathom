# Fathom — Benchmark + Commit-Gate Plan

> Status: **B1–B4 shipped; harness complete and gating (2026-06-28).** The migration engine is
> end-to-end (E1–E5), so the system now has a real baseline to measure. The
> measurement core (`Fathom.Bench` + `mix fathom.bench`) and the orchestration
> wrapper (`scripts/benchmark.sh`) are built, and the baseline for commit
> `ddb9ef7` is seeded in `scripts/perf_history.jsonl` (prod, darwin: cold-open
> low-ms, dir-resolve ~180µs, copy ~6–8M rows/s, fan-out ~26 KiB/shard —
> coordinator-only). The gate
> (`scripts/commit_with_bench.sh` + `Fathom.Bench.Gate`) is built and verified
> end-to-end, and `AGENTS.md` now documents it as real (B4). Only optional
> follow-ons remain (true RSS-per-shard for
> fan-out). This plan
> built the harness the way `AGENTS.md` §Benchmarking has specified all along ("the
> discipline to hold and the harness to build").

## Why the gate is multi-metric (and native)

A typical bench-then-commit gate tracks **one** scalar — e.g. TPC-B transactions
per second for a single OLTP write path — because one number is the number that
moves. Fathom's thesis is *millions of small shards*, so cost is dominated by
**per-shard open and fan-out**, not single-query throughput. No one number captures
it: a change can leave cold-open flat while doubling per-shard memory (halving node
density), or speed up the directory read while regressing migration copy. So
fathom's gate is **multi-metric**: it records every hot-path number per run and
**refuses the commit if any one of them regresses ≥20%** — the single-metric ≥20%
rule, generalized.

Second decision: **fathom benches natively on the host, no Docker.** Container
gating earns its keep when the numbers are dominated by macOS F_FULLFSYNC + APFS
write-pressure artifacts (per-transaction fsync workloads). Fathom's gated hot
paths are SQLite file-open, a Postgres directory read, and BEAM memory —
read/CPU/open-bound, not per-row-fsync-bound. The one fsync-touching path is
migration copy, and it issues a **single** end-of-copy checkpoint, not a
per-row fsync, so it is not APFS-dominated the way TPC-B is. We keep it fsync-light
(no extra durability beyond what `Fathom.Migrator.Copy` already does) and lean on
the trial median. If copy throughput ever proves macOS-noisy in practice, the fix
is a `synchronous=OFF` bench option, not a container.

## The four live hot paths (gated metrics)

From `AGENTS.md` §Benchmarking, anchored to real entry points. Each metric is the
**median of `--trials` trials** (median-of-trials to absorb run-to-run noise),
measured prod-compiled against a clean data dir.

| Metric | Entry point | Method | Worse = |
|---|---|---|---|
| `cold_open_p50_us` | `Fathom.Shards.checkout/1` | **Warm/local cold-open.** Pre-seed K shard DBs in Local storage. Cold-open one (no live coordinator, file pulled from local NVMe), `SELECT 1`, then `Fathom.Shards.drain/2` to flush+drop+release so the next iteration is genuinely cold. p50 of the timed `checkout/1` window. This is the node-restart / NVMe-hit path. | higher |
| `cold_open_s3_p50_us` | `Fathom.Shards.checkout/1` (S3 backend) | **Cold-from-S3 cold-open** — the production cold path. Same as above but the backend is `Storage.S3`: each sample seeds a shard object to S3, drops local, then times the checkout (pull from S3) + open + first query. **Opt-in:** `nil` unless `FATHOM_S3_TEST_*` env is set (so the default gate stays S3-free). Against MinIO-on-localhost this is the S3 *protocol* + loopback (~6 ms observed); point the endpoint at S3/R2 in-region for the real TTFB/RTT-bound number. | higher |
| `dir_resolve_p50_us` | `Fathom.Directory.resolve/1` | Warm steady state: pre-resolve once, then time N resolves of the same id. p50. Needs real Postgres. **NOT the request path** — this text used to say "runs on *every* request", which stopped being true when the Recorder landed (expert review 2026-08-01 #41.6). `resolve/1`'s live callers are `Fathom.Tenants` (provision, fork) and `Migrator.ShardMigration` — provisioning and migration, all control-plane. Kept because it still guards that reader; `dir_recorder_flush_rows_per_s` below is the path per-checkout directory work actually takes now. | higher |
| `dir_recorder_flush_rows_per_s` | `Fathom.Directory.Recorder.flush/0` | **The live directory cost** (#41.6). Per-checkout accesses are coalesced in ETS and batch-flushed, so this — not `resolve/1` — is what scales with density: an `:ets.select` walk plus one `:ets.take` per touched shard, then `insert_all` in 1,000-row chunks. At 30k active shards that is ~30k ETS takes and ~30 multi-row upserts **every second** against a `pool_size` of 25. Buffer N shard touches, then time one `flush/0`. Needs real Postgres. | lower |
| `copy_keystone_rows_per_s` | `Fathom.Migrator.Copy.migrate/4` | Seed a source shard with R **`Fathom.Keystone`** rows (every SQLite storage class and affinity, deterministically fuzzed); replay an `ALTER ADD COLUMN` **and a `CREATE INDEX`** as the captured statements (the index scans every row, so the transform does real O(rows) work — an O(1) ALTER alone would leave the metric measuring little more than a page-cache-warm `File.cp`). `R ÷ wall`. The real prod `migrate/4` (one end-of-copy checkpoint, fsync-light). | lower |
| `hrana_rt_us` | `Filo.Client` → `Filo.Plug` → `Fathom.ShardExecutor` | **Per-REQUEST wire cost.** Median µs of a warm-stream `SELECT 1` round trip through a real Hrana listener on loopback. One cell out, no fsync, so it is dominated by fixed cost: framing, stream lookup, routing, `Request.handle`. Loopback **software** cost, not a network RTT (µs link, no bandwidth-delay/TLS/LB hop — the chaos rig measures the real thing). HTTP rather than WebSocket because `mint_web_socket` is dev/test-only and the gate compiles prod. | higher |
| `wire_rows_per_s` | same, over a `Fathom.Keystone` | **Per-CELL wire cost.** Rows/sec for `SELECT * FROM ks_scalars` on a keystone of R rows, so every storage class — **including BLOB** — crosses `Filo.Value`'s encoder on every run. This is the metric that catches an encoder regression; `hrana_rt_us` cannot, because one integer cell does not exercise per-cell cost. | lower |
| `fanout_kb_per_shard` | `Fathom.Shards.checkout/1` × N | Open N shard **coordinators** concurrently (an open-but-idle shard is just the `Fathom.Shard` GenServer — it holds no connection; connections are per-stream and transient). `Δ:erlang.memory(:total) ÷ N`. The node-density number. Deliberately does NOT hold a connection per shard: that would burn ~3 fds each (db + `-wal` + `-shm`) and exhaust the OS fd limit (default 256 on macOS) well before N is interesting. | higher |

**`hrana_rt_us` stopped being a placeholder on 2026-07-31**, and the reason is worth keeping.
Until then it was a recorded `null`, which meant **no gated metric executed a single line of
`Filo`** — `cold_open` and `copy_keystone_rows_per_s` are SQLite and storage, `dir_resolve` is
Postgres, `fanout_kb_per_shard` is BEAM memory. That blind spot let a **200x** regression sit in
row encoding: `Filo.Value.encode_json/1` raised and rescued an exception per BLOB cell (32.84 µs
against 0.16 µs for text), and nothing in the gate could see it. It was found by hand, not by
the harness.

Both wire metrics were verified to **discriminate** before being gated — run once with the fix
reverted, per the "prove the benchmark discriminates" rule: `wire_rows_per_s` fell 60,650 →
37,880 rows/s (**-37.5%**, past the 20% block threshold), while `hrana_rt_us` moved 128 → 119 µs,
i.e. noise. That is precisely why there are two: a `SELECT 1` round trip would **not** have
caught it.

**Known limitation, recorded not hidden:** `fanout_kb_per_shard` measures BEAM-side
memory (`:erlang.memory(:total)`), which excludes SQLite's off-heap page cache.
It tracks coordinator + connection-resource density faithfully; true RSS-per-shard
(which the page cache dominates) is a follow-on once we decide it's worth the
cross-platform noise.

## Where the numbers live

`scripts/perf_history.jsonl` — singular, since fathom is single-topology native
(the name `AGENTS.md` already specifies). One line per run:

```json
{"ts":"2026-06-28T…Z","commit":"e26b655","commit_full":"…","branch":"main",
 "dirty":false,"host":"darwin","mix_env":"prod",
 "cold_open_p50_us":1234.5,"dir_resolve_p50_us":210.0,
 "copy_keystone_rows_per_s":85000.0,"fanout_kb_per_shard":48.0,
 "hrana_rt_us":128.0,"wire_rows_per_s":60650.17,
 "trials":5,"log":"logs/bench-20260628-….log"}
```

**Series break, 2026-07-31 — `copy_rows_per_s` → `copy_keystone_rows_per_s`.** The copy bench
moved from a three-column `(INTEGER, TEXT, INTEGER)` table to `Fathom.Keystone`, which carries
every SQLite storage class and affinity. Rows are far wider, so rows/second dropped by
construction. That is a fixture change, not a code regression, and `AGENTS.md` is explicit that a
harness change ends the comparable series — so the metric was **renamed** rather than quietly
redefined. Entries before this date keep `copy_rows_per_s` and are never compared against entries
after it (a metric absent from the parent is skipped, not treated as flat). Rename again on any
future harness change to this bench.

The `host` **and `mix_env`** fields pin the comparison topology: the
gate compares only same-host, same-env baselines, so a macOS number never gates a
Linux number and a dev run (file-I/O-bound, ~3x slower than prod) can never serve
as the baseline for a prod gate.

**Host-drift caveat (native gate, recorded not hidden):** because fathom gates on
the host rather than in a controlled container, the absolute numbers drift with
machine load (cold-open and copy are file-I/O bound; a busy or thermally-throttled
box was measured ~3x slower in one session). The gate trusts the parent's *stored*
line, so a baseline measured hours earlier under different load can read as a false
regression. Mitigations, in order: the metrics are stable within a single quiet
session (±2% observed); honor the existing "rerun once to rule out noise" rule
(`AGENTS.md`); and when a baseline is stale or the box's load has clearly changed,
re-bench the parent fresh (`COMMIT_BENCH_PARENT=<parent>` or bench a clean parent
checkout) before trusting a BLOCK. A same-run A/B (stash, bench parent, unstash,
bench working tree) would remove drift entirely but doubles bench time; deferred
until drift proves a problem in practice.

### The host-wide bench lock

Benchmarks are only meaningful in isolation, so `mix fathom.bench` takes a host-wide lock
file for the run: it refuses to start if the lock exists (another run in progress, or a
crashed run left it — `rm` it), and creates + removes it otherwise, even on failure. The
create is atomic (`O_EXCL`), so two simultaneous starts can't both win.

The path defaults to `/tmp/fathom_bench.lock` and is overridable with **`FATHOM_BENCH_LOCK`**.
Point several projects at the *same* path to interlock benchmarks across a shared host:

```bash
FATHOM_BENCH_LOCK=/tmp/shared_bench.lock mix fathom.bench
```

That's the reason it's an env var rather than a constant: co-tenant projects can agree on a
shared lock without either repo hardcoding the other's name.

## The harness (this is what B1/B2 build)

Three pieces, split as inner measurement vs. orchestration:

- **`Fathom.Bench`** (`lib/fathom/bench.ex`) — the measurement primitives:
  `cold_open/1`, `dir_resolve/1`, `copy_throughput/1`, `fanout/1`, each returning a
  number. Starts a **minimal subset** of the tree itself (just `Fathom.ShardRegistry`
  + `Fathom.ShardSupervisor`; `Fathom.Repo` only when the directory bench runs) so
  it never binds the Hrana port, never starts Oban, never starts the endpoint. Sets
  its own runtime knobs (clean Local storage dir, `:shard_flush_interval_ms` 0,
  `:directory_touch` false, `:lazy_migrate` false). Shared by the Mix task and the
  test guards — one source of truth for "how we measure."
- **`mix fathom.bench`** (`lib/mix/tasks/fathom.bench.ex`) — drives `Fathom.Bench`
  and emits one **complete** perf-history JSON line (the task owns the schema).
  Flags: `--only m1,m2`, `--trials N`, git-context (`--commit/--branch/--dirty/--host/--log`,
  defaulted so it runs standalone), and `--append PATH` to append the line. Human
  table to stderr, JSON line to stdout.
- **`scripts/benchmark.sh`** (B2) — orchestration: stop stragglers, wipe the bench
  data dir, prod-compile, ensure a migrated `fathom_bench` Postgres DB (the
  directory bench needs real Postgres, not the test sandbox), run the task with the
  git context, tee a log, append `perf_history.jsonl`.

## The gate (B3)

`scripts/commit_with_bench.sh` — the bench-gated commit wrapper:

- **Mechanics:** parent-lookup (most-recent **same-host** line wins), run-bench-
  on-working-tree, BLOCK/WARN/OK verdict, ≥20% refuse (`PERF_REGRESS_BLOCK`),
  `--check-only`, `--skip` / `[skip-bench]` token, the non-TTY WARN abort, and the
  "no baseline for parent → exit 2, bench the parent first" path.
- **Multi-metric:** instead of one throughput delta, compute a **per-metric,
  direction-aware** regression (latency higher-is-worse; throughput lower-is-worse;
  memory higher-is-worse) and **refuse if any gated metric crosses ≥20%**. The
  verdict report prints all deltas so you see which path moved.
- **Not included:** Docker/Colima topology machinery (native single-host gate), and
  the NIF-contract guard (fathom has no NIF yet — a one-line hook comment marks
  where it goes if `fathom_native` ever lands, per `AGENTS.md` §Gates).

Layering: `mix precommit` (compile/format/test) stays the **correctness** gate and
runs first; the bench runs only on a green tree, then commits. One command, full
gate.

## Phases

- **B1 — measurement core (done).** `Fathom.Bench` + `mix fathom.bench` +
  the `@tag :bench` floor/ceiling tests `AGENTS.md` already asks for (e.g.
  `assert cold_open_us < 50_000`), so hot-path changes are guarded even when the
  full gate isn't run. Smoke-runnable without Postgres via
  `--only cold_open,copy,fanout`.
- **B2 — orchestration + baseline (done).** `scripts/benchmark.sh` runs prod-compiled
  against a throwaway `fathom_bench` Postgres DB (its `runtime.exs` needs
  `DATABASE_URL` + `SECRET_KEY_BASE`, set by the script even though no endpoint
  starts), appends to `scripts/perf_history.jsonl`, tees a log. `Fathom.Bench` is
  self-cleaning (wipes its scratch each run), so there's no data dir for the script
  to wipe. Baseline for `2ac53dd` is seeded.
- **B3 — the gate (done).** `scripts/commit_with_bench.sh` implements the
  multi-metric comparison; the decision logic is `Fathom.Bench.Gate.compare/4`
  (pure, unit-tested) driven by `mix fathom.bench.check` (reads the histories,
  exits 0/2/3/4 = ok/no-baseline/warn/block). Snapshots `perf_history.jsonl` before
  benching so the parent baseline and the same-SHA pre-commit line stay distinct.
- **B4 — wire it in (done).** `AGENTS.md` §Benchmarking and §Gates now document the
  harness + gate as real (was "aspirational"), including the host-gating policy.
  Remaining optional: true RSS-per-shard
  for fan-out.

## Decisions (locked 2026-06-28)

- **Multi-metric gate, any ≥20% refuses** (not a single primary metric, not a
  weighted composite — a composite would let a real regression in one path hide
  behind a flat blend).
- **Native host, copy bench fsync-light** (not a Linux container — fathom's gated
  paths aren't per-row-fsync-bound; the single copy checkpoint isn't
  APFS-dominated).
- **Plan persisted here** before building, mirroring `migration-engine-plan.md`.

---

## Appendix — the measured hot paths and the scale harness

Moved out of [`../AGENTS.md`](../AGENTS.md) § Benchmarking on 2026-08-22, verbatim. AGENTS.md
keeps the *rules* (the ≥20% response, the phantom rule, the noise tells, the rig traps); this is
the catalog of what each metric measures and what it has read. Dates are the measurement dates —
treat every absolute as relative to the rig it was taken on, per the same-topology rule.

### Hot paths to watch

- **Hot paths to watch** (fathom's scaling story is *millions of small shards*, so cost is dominated by per-shard open and fan-out, not single-query throughput):
  - **Shard cold-open latency.** Two paths: *warm* — local file present (node restart, or local-NVMe `Storage.Local`) — is `cold_open_p50_us` (~ms, often page-cache-warm); *cold-from-S3* is `cold_open_s3_p50_us` (opt-in). Realism: MinIO-on-localhost measures the S3 *protocol* + loopback (~6 ms), NOT real S3 latency — either point `FATHOM_S3_TEST_ENDPOINT` at real S3/R2, or use `scripts/benchmark_s3_latency.sh`, which puts **toxiproxy** between the bench and MinIO and injects latency/bandwidth (`S3_FAKE_LATENCY_MS`, `S3_FAKE_RATE_KBPS`). Cold-open is optimized to **~1 S3 round-trip**: the lease acquire is an optimistic conditional create (`PUT If-None-Match:*`, read-then-resolve only on 412) and `Fathom.Shard.init` overlaps it with the pull (independent objects, `.lock` vs `.db`). The pull lands in a temp file promoted only after the lease confirms — a lost lease race never leaves a stale local copy, and we still only *serve* after the lease confirms. Expect ~2× one-way latency + a few ms (`scripts/benchmark_s3_sweep.sh`: one-way 10/30/60/100 ms → ~26/77/137/215 ms); bandwidth helps warming throughput, not single-shard cold-open.
  - **Warming throughput** (`warm_s3_shards_per_s`, opt-in S3) — aggregate shards/s a node can pull from S3 at once (startup/failover). Two design facts keep it high: (1) the lease+pull runs in `handle_continue`, not `init`, so concurrent opens don't serialize at the `DynamicSupervisor` (a `:checkout` queues until the open completes; an open failure stops the coordinator with `{:shutdown, _}` — not restarted — and `Fathom.Shard.checkout/1` maps that exit to `{:error, _}`); (2) `Fathom.Shard.Storage.S3` runs a **dedicated, config-driven Finch pool** (`finch_child_spec/0` supervised in `application.ex`; `config :fathom, Fathom.Shard.Storage.S3, pool_size:` default 200 — the measured knee on the localhost rig — plus `pool_count`, default 1, for real-S3 tuning; `req/0` falls back to Req's default pool when ours isn't started). Measured 2026-06-29 (dev build, MinIO+toxiproxy, 30 ms — a relative lever, not a prod-absolute): ~10 → ~800 shards/s across the two fixes; past ~200 conns the localhost MinIO server saturates, so `pool_count` can only show a win on real S3. Sweep with `FATHOM_S3_TEST_POOL_SIZE=N FATHOM_S3_TEST_POOL_COUNT=C S3_BENCH_ONLY=warm_s3 scripts/benchmark_s3_latency.sh --warm-shards M`.
  - **Directory resolve** — Postgres lookup + resolve cache hit/miss; this is on every request.
  - **Migration copy throughput** — rows/sec per shard for the blue/green copy+transform.
  - **Failover RTO — warm standby vs cold (opt-in S3).** `failover_cold_s3_p50_us` (survivor cold-opens with a full S3 pull) vs `failover_warm_s3_p50_us` (shard is in the warm-follower cache; the freshness check is a conditional **304** GET + a local copy), both at `:warm_size_kb`; `mix fathom.bench --only failover_rto` prints the speedup. **The honest delta:** the warm path is *not* purely local — it still pays one S3 round-trip (the 304, no body). The win is the object **body transfer avoided**, so it scales with shard size × bandwidth-delay and is **marginal for a tiny shard on a fat pipe**. Measured 2026-07-01 (dev build, MinIO+toxiproxy, relative): 1 MB / 30 ms one-way / 100 Mbps → **cold ~162 ms, warm ~72 ms (≈2.3×)**; the warm floor is the lease + freshness round-trips, not ~2 ms. (Those numbers predate the steal-touch takeover machinery, which added round-trips to both paths; don't compare new runs against them directly. 2026-07-23 A/B, same rig, no bandwidth cap: the takeover chain is **~1 RTT cheaper** than pre-iteration — failover cold/warm −10–11% at 30 and 60 ms one-way, the #13 confirm-rotation HEAD now read from the touch's own write response — `docs/reviews/s3-latency-ab-2026-07-23.md`.)
  - **Warm-standby density.** `mix fathom.scale --warm-density [--shards N] [--shard-size-mb S]` pre-pulls N shards into the follower cache and reports per-cached-shard disk, BEAM bookkeeping, and warming rate. **Finding:** a warm-cached shard costs its file on disk plus ~0 process/BEAM/fd (no coordinator, no connection), so warm capacity is **disk-bound** (`disk / shard_size`) — orders of magnitude past the ~196 KiB + 3-fd open-shard ceiling. A standby warms far more than it can serve open.
  - **Concurrent shard fan-out** — how many shards a node can hold open at once. **Two ceilings** (measured 2026-06-29): *connection-per-shard* (every shard actively streaming) is **fd-bound** at ~`kern.maxfilesperproc / 3` (~3 fds per WAL connection; ~82k on a 245760-limit box) — fds bind well before memory. *Idle-open coordinators* (connections are per-stream/transient) go far higher, memory-bound at ~26 KiB each. Per-shard cost is connection + coordinator **overhead** (~180–196 KiB), not the page cache — density is overhead-bound, not data-bound.
  - **Hrana round-trip** — once remote shards land.

### The scale harness

- **Scale test (built).** `mix fathom.scale [--shards N] [--shard-size-mb S]` (`Fathom.Scale`) provisions N realistically-sized shards and measures cold-open latency at size + **fan-out node density** (BEAM + RSS per open shard, open throughput). `--ramp [--max N]` opens empty shards until the fd ceiling to find the node-density limit cheaply; `--warm-density` per the bullet above; `--lease-rps` shows per-shard lease renewals collapsed to one node heartbeat (flat regardless of shard count). Raise `ulimit -n` first (~3 fds per live connection). Measured 2026-06-29: 1000 × 4MB → ~196 KiB RSS/shard, warm cold-open ~2.6 ms p50; ramp held 50k linearly → fd ceiling ~82k. (The ramp's per-open slowdown at high N is a one-process test artifact, not fathom — production spreads shards across per-stream processes.)
  - **Hot-spot detectability (`--hotspots`, the Phase-2 §B rebalancing unblock).** `mix fathom.scale --hotspots [--shards N] [--zipf s] [--queries Q] [--workers W] [--stream-len L]` (`Fathom.Scale.hotspots/1`) is the **first reader of `Fathom.ShardLoad`**: it turns on `:shard_load`, drives a Zipf(s)-skewed load over N shards through the real recording path (`Shards.checkout` → `ShardExecutor.execute`), then reads the counters the way a rebalancer would — diff two `snapshot/0`s over a window into per-shard rates. **Load unit is a Hrana stream** (`--stream-len L`): each stream checks out + opens a connection once, bursts L queries on the held connection, then checkins — the real per-stream model. `L=1` (default) is one query per stream (the per-query lower bound, finest detection sampling); raise L to measure realistic throughput, scaling `--queries` so `Q/L` (the stream count) stays ≫ N. Reports the rate distribution (p50/p90/p99/max), two separations (max/median and tail-robust max/p99), **three threshold-family sweeps** (`>Kx median`, `>Kx p99`, and an absolute q/s floor isolating the top-N — each with a Zipf-recall check), a `median_collapsed` flag (shape-based: `>10x-median` flags ≫ `>10x-p99` ⇒ median-relative over-flags), a scale-robust anti-flap signal (top-20-by-rate set overlap across two windows), and whether `ShardLoad.top(20)` recovers the Zipf head. **Detection finding (prod, 10k shards, s=1.1 — synthetic/one-host-relative):** clean (`ShardLoad.top(20)` recall **1.0**, top-20 Jaccard **0.9**, sharp head hot_1 188 q/s → …), but the long cold tail makes **`>Kx-median` over-flag (751/421/216) — key hot-detection on `>Kx-p99` (>20×p99 → 5 shards, recall 1.0) or an absolute q/s floor.** **Throughput finding (prod, 1000 shards, ~20k streams; 2026-07-23):** persistent streams remove the per-query coordinator bottleneck — L=1 **~3.3k q/s** → L=16 **~27k** → L=64 **~54–55k q/s** per node (hottest shard ~600 → ~4.9k → ~9.5k q/s), detection quality flat across all L. So the earlier ~1.2k q/s was the per-query artifact, not a fathom limit. The held-stream numbers are **+13–16% over the pre-2026-07-23-iteration code** (same-day A/B — the per-connection statement cache: at L=64, 63 of 64 queries skip `sqlite3_prepare_v2`), while L=1 is flat — a one-query stream never hits the cache and is per-stream-open-bound, which is the connection-pool question, measure-first pending `hrana_rt_us` (`docs/reviews/hotspots-ab-2026-07-23.md`). **Non-synthetic confirmation — DONE** (`deploy/chaos/chaos.sh hotspots`, 2026-07-06): 17.5k real Hrana requests through the LB over 3 nodes recovered the Zipf head at top-20 recall 0.95, with the hot set spread across nodes (a rebalancer reads `ShardLoad` per node and merges). Enable on a deployed node with `SHARD_LOAD=true` (`config/runtime.exs`).
