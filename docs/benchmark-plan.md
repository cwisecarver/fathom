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
> follow-ons remain (`hrana_rt_us` once remote shards land; true RSS-per-shard for
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
| `dir_resolve_p50_us` | `Fathom.Directory.resolve/1` | Warm steady state (the on-conflict update path that runs on *every* request): pre-resolve once, then time N resolves of the same id. p50. Needs real Postgres. | higher |
| `copy_keystone_rows_per_s` | `Fathom.Migrator.Copy.migrate/4` | Seed a source shard with R **`Fathom.Keystone`** rows (every SQLite storage class and affinity, deterministically fuzzed); replay an `ALTER ADD COLUMN` **and a `CREATE INDEX`** as the captured statements (the index scans every row, so the transform does real O(rows) work — an O(1) ALTER alone would leave the metric measuring little more than a page-cache-warm `File.cp`). `R ÷ wall`. The real prod `migrate/4` (one end-of-copy checkpoint, fsync-light). | lower |
| `fanout_kb_per_shard` | `Fathom.Shards.checkout/1` × N | Open N shard **coordinators** concurrently (an open-but-idle shard is just the `Fathom.Shard` GenServer — it holds no connection; connections are per-stream and transient). `Δ:erlang.memory(:total) ÷ N`. The node-density number. Deliberately does NOT hold a connection per shard: that would burn ~3 fds each (db + `-wal` + `-shm`) and exhaust the OS fd limit (default 256 on macOS) well before N is interesting. | higher |

`hrana_rt_us` is a **placeholder column, recorded `null`** until remote shards land
(`AGENTS.md`: "once remote shards land"). We do not fake it.

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
 "hrana_rt_us":null,"trials":5,"log":"logs/bench-20260628-….log"}
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
  Remaining optional: `hrana_rt_us` once remote shards land; true RSS-per-shard
  for fan-out.

## Decisions (locked 2026-06-28)

- **Multi-metric gate, any ≥20% refuses** (not a single primary metric, not a
  weighted composite — a composite would let a real regression in one path hide
  behind a flat blend).
- **Native host, copy bench fsync-light** (not a Linux container — fathom's gated
  paths aren't per-row-fsync-bound; the single copy checkpoint isn't
  APFS-dominated).
- **Plan persisted here** before building, mirroring `migration-engine-plan.md`.
