# AGENTS.md — Fathom

## Project

Fathom is a multi-tenant sharded data platform built on Phoenix: one SQLite database per shard (eventually millions), served to unchanged libSQL clients (e.g. an unchanged Django app via `django-libsql`) over the network.

**What exists today (the working slice):**

- **Shard data path.** One **connection per Hrana stream**, so transactions are isolated: each stream opens its own `Fathom.Shard.Connection` (an `exqlite` connection to the shard file, WAL + busy_timeout) and closes it when the stream ends. `Fathom.Shard` is the per-shard **coordinator** — one GenServer per active shard — that owns the file lifecycle: on cold start it **pulls** the file from `Fathom.Shard.Storage`; it tracks checked-out connections (monitoring the stream processes); when idle with zero connections checked out it **checkpoints + flushes** the file back to storage, drops the local copy, and stops. So flush never races a write, and the local copy is bottomless-backed. Flushes (the periodic durability flush and the idle flush) are **write-gated by a `dirty` flag** — `ShardExecutor` marks the coordinator dirty on a write, a clean (read-only/idle) shard skips the upload — so durability PUTs track writes, not open-shard count. `Fathom.Shards` is the find-or-start router (`Fathom.ShardRegistry` + `Fathom.ShardSupervisor`, both supervised); `Fathom.Shards.checkout/1` resolves a `shard_id` to its coordinator (starting it on demand) and returns `{:ok, pid, ref, path}`.
- **Shard storage.** `Fathom.Shard.Storage` is a `pull/2` + `flush/2` behaviour, backend chosen by `config :fathom, :shard_storage` — `Fathom.Shard.Storage.Local` (a filesystem object store, the default for dev/test) or `Fathom.Shard.Storage.S3` (Req + `aws_sigv4`, no AWS dep; works with S3 / R2 / Tigris / MinIO). A present local file is authoritative on wake (pull only on cold start), so an un-flushed shard is never clobbered.
- **Network protocol.** `Fathom.ShardExecutor` (a `Filo.Executor`) binds each Hrana stream to a shard. The **Filo** library (separate repo, depended on via `{:filo, path: "../filo"}`) speaks the libSQL Hrana wire protocol — HTTP v1/v2/v3 (+ cursor) and WebSocket hrana1/2/3 — and runs on its own Bandit listener (`config :fathom, :hrana_port`, default 8080; gated by `:hrana_server`, off in test), separate from the web/dashboard endpoint on 4000. The shard is selected from the request's **Host subdomain** (`acme.fathom.example` → `acme`; case-normalized via `Fathom.ShardId.cast` so `ACME`/`acme` are one shard — finding #19), with `?db=` / `x-fathom-shard` as **dev-only** fallbacks gated by `:allow_shard_override` (off in prod — finding #4). When nothing resolves, the fallback is `:default_shard` — **unset in prod ⇒ fail closed** (a 400, not commingling into a shared shard — finding #26); dev/test set it to `"demo"`. **Template capture** (`:template_shard_id`, off in prod by default) records a shard's SQL for fleet-wide replay, so it's a poisoning vector if reachable anonymously: never set a prod `:template_shard_id` without auth on that shard, and never make `:default_shard` equal it — a prod boot guard (`Fathom.Application.check_template_default!`) refuses that config (finding #17). `django-libsql` (WebSocket) and `libsql-experimental`/SDKs (HTTP) both work end to end. The Hrana path carries **no in-app credential** by design: the trust boundary is the network — the port must be reachable only via the LB (firewall/SG/private subnet; pin the interface with `HRANA_BIND_IP`). In-app bearer-token auth (`Phoenix.Token`) is the deferred next step. See `docs/deploy-cluster.md`.
- **Postgres — `Fathom.Repo`.** The orchestration store and web UI backend, in the supervision tree (Phoenix dashboard on port 4000). It also backs the shard **directory / control plane** below; migrations live in `priv/repo/migrations/` (`shards` directory, `shard_migrations`, Oban jobs).
- **Directory / control plane.** `Fathom.Directory` (+ the `Fathom.Directory.Shard` Ecto schema) is the Postgres record of each shard's `schema_version`, lifecycle `status` (`active`/`migrating`/`retired`/`migration_failed`), `last_active_at`, and `retain_until`. It's the source of truth the rollout/migration machinery reads and flips (`resolve`, `cutover`, `retire`, `laggards`). It's decoupled from the data path: per-checkout accesses are **buffered + batch-flushed off the hot path** by `Fathom.Directory.Recorder` (a coalescing ETS buffer; a Postgres outage drops a flush, never a checkout).
- **Migration engine.** `Fathom.Migrator` runs blue/green per-shard schema migration: `Migrator.Capture` records template migrations into fleet versions, `Migrator.Copy` + `Migrator.ShardMigration` do the copy+transform and stamp `user_version`, `Migrator.Release` releases a fleet HEAD, and Oban jobs drive it — `ShardMigrationJob` (unique per shard), `ReconcileJob` (hourly cron sweep so the cold tail converges), `RetirementJob` (drop expired retained versions), `RevertJob` (fleet pointer-flip back). There is **no `Fathom.ShardExec` or `Fathom.Retirement` module** — that work lives in `Migrator.Copy`/`ShardMigration` and `Migrator.RetirementJob`; don't grep for the old names.
- **Per-shard load counters (`Fathom.ShardLoad`) — the Phase-2 rebalancing (B) prerequisite.** A public ETS table of per-shard cumulative counters (checkouts, queries, `rows_read`/`rows_written` = query cost), bumped **lock-free from the executing process** (`:ets.update_counter`, `write_concurrency` — the `Directory.Recorder` pattern, no per-query GenServer hop) on `Fathom.Shards.checkout` and `Fathom.ShardExecutor.execute`; a stopped coordinator's row is dropped in `terminate` (`forget/1`). A control plane reads `top(n, by)` / `snapshot/0` to find hot shards (rates = diff two snapshots, churn-safe). Deliberately **not** a per-shard `Telemetry.Metrics` (a per-shard tag at millions of shards is cardinality death) — the read API is the interface. Gated by `:shard_load`, **off by default** (nothing consumes it until the rebalancer lands, so the hot path doesn't pay for an unread counter).
- **Cross-node single-writer (lease + epoch fence + node heartbeat).** `Fathom.Shard.Storage` carries a per-shard **lock** (`{owner, epoch}`, the monotonic `epoch` is the fencing token) and a per-node **heartbeat** (`Fathom.Shard.Heartbeat` renews one `heartbeat/<node>` object every `ttl/3` — liveness is O(nodes), not O(shards), which is the F1 fix). A shard's owner is live iff its heartbeat is fresh, so `acquire_lease` consults the owner's heartbeat to decide held-vs-steal (and fails closed on a heartbeat read error). Coordinators do **no per-shard renewal**; before a flush they fence via `Heartbeat.valid_for_write?/1` (heartbeat valid-with-margin + no lapse since acquire ⇒ write with no per-shard I/O; on a lapse, re-check the lock via `Storage.check_lease/2` and **self-fence** if superseded so a remapped shard never double-writes). If the heartbeat process is down, coordinators degrade to the legacy per-shard renew fence. This is the only cross-node coordination — via S3, not BEAM.
- **Cluster layer (LB-keyspace-partition).** The L7 load balancer consistent-hashes the `Host` subdomain to one backend node; each node is an independent single-node fathom, and the S3 lease above handles remap safety. `Fathom.HealthPlug` serves `GET /health` (per-node LB probe, `:health_port` default 8081, gated by `:health_server`). `Fathom.Telemetry` runs Telemetry.Metrics over the shard/lease/checkout events + an active-shard poller + a checkout→OpenTelemetry span bridge (traces env-gated on `OTEL_EXPORTER_OTLP_ENDPOINT`, off by default). LB config + chaos rig in `deploy/lb/` + `docs/deploy-cluster.md`; runbook in `docs/runbooks/cluster.md`.
- **Bench + scale harnesses.** `mix fathom.bench` (+ `scripts/benchmark.sh`, `scripts/perf_history.jsonl`, the `scripts/commit_with_bench.sh` regression gate) measures the hot paths; `mix fathom.scale` measures cold-open at real size, fan-out node density (`--ramp`), and (`--lease-rps`) that the lease-renewal storm is gone — per-shard renewals collapse to one node heartbeat (flat regardless of shard count). See Benchmarking.
- **Warm standby (Phase 2, A1 — H1+H2+H3 built).** `Fathom.Shard.WarmFollower` (gated `:warm_follower`, **off by default** and in test) pre-pulls the fleet's recently-active shards this node doesn't own (`Fathom.Directory.active_recent/1`, LRU-capped at `:warm_cache_max`) into a **separate** cache dir, holding the file but **no lease** — it never serves. On failover the coordinator's cold-open **promotes** the warm copy only after a **freshness check**: a warm cache may lag the owner's latest flush, so a stale copy is never served. The check is `Fathom.Shard.Storage.pull_if_changed/3` — a conditional `If-None-Match` GET (304 = promote the cache with no transfer / 200 = re-pull the fresh bytes / 404 = brand-new). The follower records the object's etag in a `<shard>.db.etag` sidecar and **revalidates its whole cached set each poll** so a failover lands on the 304 fast path. A live-dir warm *restart* (this node's own un-flushed writes) still wins untouched — only the separate follower-cache path is validated. **H3 is the measurement:** `mix fathom.bench --only failover_rto` (opt-in S3) times cold-open (full pull) vs warm-open (304-promote) at size, and `mix fathom.scale --warm-density` shows warm density is disk-bound (~0 BEAM/fd per cached shard vs ~196 KiB + 3 fds open). The warm win is the object **body transfer avoided** — ~2.3× at 1 MB / 30 ms / 100 Mbps, and marginal for tiny shards on a fat pipe (both are ~1 S3 RTT). See Benchmarking.

**Planned (see `docs/migration-plan.md`), NOT in the code yet — don't assume these exist:**

- The **Ecto shard path**: `Fathom.ShardRepo` (an `Ecto.Adapters.LibSql` dynamic repo — `name: nil` per pid, `start_shard/2` + `put_dynamic_repo/1`) is defined as a module but has **zero callers**; the live Hrana path uses `Fathom.Shard`/`exqlite` directly. Which becomes the long-term data path is an open decision.
- **Phase 2 remainder**: dynamic rebalancing (B), shard locality/affinity (C), the warm-standby **A1-H3** (failover-RTO bench, warm vs cold), and live WAL streaming (A2, deferred). The warm follower itself (A1-H1/H2) is **built** — see the warm-standby bullet above. A `fathom_native` Rust NIF (no `native/`, no Rustler dep yet).

## Execution style

- **Sequenced directives** ("do X then Y", "review then execute") → execute directly. Don't pause to re-confirm the sequence or ask which item to start with. If genuinely ambiguous, name your default and proceed; stop only if the ambiguity risks irreversible harm.
- **"Go ahead" / "continue" / "proceed"** = continue the *most recently scoped* task. Never authorization to escalate review→implementation or jump phases. Asked for a review → deliver only the review.
- **Locating things:** one targeted Read/Grep/Glob, not a fan-out of speculative `find`/`ls`. If the first lookup fails, widen the query.
- **Don't narrate intentions** ("let me check…", "I'll now…"). State results.

## Build

```bash
mix setup          # deps.get + ecto.setup (Postgres) + assets
mix compile        # build (mix precommit uses --warnings-as-errors)
mix test           # creates+migrates the test Postgres DB, then runs tests
mix test test/fathom_web/controllers/page_controller_test.exs:7  # single test
mix test --failed  # rerun last failures
mix format         # format
mix precommit      # the gate: compile --warnings-as-errors, deps.unlock --unused, format, test
iex -S mix phx.server   # start app (dashboard :4000)
```

- **Shell is zsh.** Backticks and `$(...)` run command substitution even inside double quotes — a backtick or unescaped `$(` in a `git commit -m "..."` body gets executed and silently mangles the message. Don't wrap identifiers in backticks inside `-m`. Unquoted globs (`*`, `?`, `[...]`) and `{a,b}` brace-expand.
- **When `fathom_native` lands:** the NIF builds via Rustler on `mix compile`; release builds need `MIX_ENV=prod` and are slow (set timeouts). Rust tests: `cd native && cargo test`.

## Workflow

Plan mode for non-trivial tasks (3+ steps or an architectural decision). Stop and re-plan when something goes wrong. Use subagents for research/exploration/parallel work — one task each.

**Implementation cycle:** implement → compile → test → (bench if hot path) → `mix precommit` → commit → **push**. Test after every change; fix failures before proceeding. Commit in logical units matching plan phases.

- **ALWAYS `git push` immediately after every commit.** The gate lives at the *commit* — there is no separate gate at push, and push is never something to wait to be asked for. An unpushed commit is unbacked-up work; never batch local commits. (This rule exists because a full night's work was once lost to local-only commits.) Note: this repo currently has **no commits and no remote** — the first commit needs `git remote add` first; confirm the remote with the user before pushing.
- **NEVER commit** with compiler warnings, build errors, or failing tests. `mix precommit` is the gate (see Gates).
- **Never use `sed`/`awk`/`head`/`tail`/`echo` to read or edit files** — use Read (offset/limit), Grep, Edit/Write. Shell text tools are only for things the standard tools genuinely can't do. Piping command output is fine.
- Track plans in `tasks/todo.md`. Record corrections/lessons in `tasks/lessons.md`.

### Stop-after-2-failures rule

If a script or command (test/build/migration/sweep) fails **twice with a similar error**, stop. Print the exact command, the exact error, and a one-paragraph root-cause hypothesis, then wait. Don't loop on infra failures (missing dep, DB-not-created, port conflict, libSQL file-lock) — diagnose them. Same rule for scope blowups: a refactor producing **>50 compile errors** or running **>60 min past estimate** → stop and report. Never paper over flaky tests or build failures with sleeps, retries, or timeout bumps.

## Testing

Complements the framework **Test guidelines** below (`start_supervised!`, no `Process.sleep`/`Process.alive?`, monitor for DOWN) — those still apply. This section is the *discipline*.

- **Add coverage with every feature:** happy path, error cases, edge cases, backward compatibility. **Don't use TDD/red-green unless explicitly asked** — default to implementation + tests together (or test-after for small changes). If you think red-green fits, suggest it and wait.
- **Two stores, two test modes:**
  - **Postgres directory (`Fathom.Repo`)** → `Fathom.DataCase` with the Ecto SQL sandbox (async-safe, auto-rollback per test).
  - **libSQL shards (`Fathom.Shard` / `Fathom.Shards`)** → no Ecto sandbox; a shard is a real SQLite file. Use a unique `shard_id` per test, drive it through `Fathom.Shards`/`Fathom.ShardExecutor` (the registry + supervisor come up with the app), and `File.rm` the file (`System.tmp_dir!/fathom_shards/<id>.db`) in `on_exit`. Never let two tests share a shard file. (If/when the unused `Fathom.ShardRepo` Ecto path is adopted, open each shard with `start_supervised!` + `put_dynamic_repo/1` instead; the SQL sandbox likely does **not** apply to the libSQL adapter — **VERIFY**.) See `test/fathom/shard_executor_test.exs`.
- **Save test output to timestamped logs** so results are readable without rerunning, then prune logs older than a day and read the latest instead of rerunning:
  ```bash
  mix test 2>&1 | tee "logs/test-$(date +%Y%m%d-%H%M%S).log"
  find logs/ -name "test-*.log" -mtime +1 -delete 2>/dev/null
  ls -t logs/test-*.log | head -1 | xargs cat   # read latest
  ```
- **Every bug fix ships with a regression test in the same commit.** It must (1) **reproduce deterministically** — fail pre-fix, pass post-fix; if you can't make it fail without the fix you haven't isolated the bug, keep investigating — and (2) **pin the violated invariant**, not just the reproduction steps. Comment the symptom so future readers know why the test exists. Good targets: concurrency/thread-local races (test at the pure-function level), off-by-one/boundary (test at the boundary), classifier/dispatcher mismatches (test the classification), lifecycle ordering (test the sequence).
- **Fathom-specific must-test invariants** (these are the bugs that bite a sharded multi-tenant system):
  - **Shard isolation.** A query for shard A must *never* resolve to or read shard B's data. Any change to routing — `Fathom.ShardExecutor.shard_from_conn` (request → shard), `Fathom.Shards` resolve, shard-path construction in `Fathom.Shard`, or the planned `Fathom.Directory` — ships with a test proving cross-shard isolation.
  - **Migrations are tested both ways.** Every schema migration ships with a test that runs the forward copy+transform on a seeded `vN-1` shard and validates `vN` (row counts / checksums), **and** a test for the revert pointer-flip back to `vN-1`. See the migration gate.
  - **Cross-version tolerance.** During a rollout the fleet is mixed `vN-1`/`vN`; assert the app reads both (`schema_version`-aware branch, or `vN` superset still usable by old code).
- **Hot-path verification.** When you change a hot path (shard cold-open, directory resolve, migration copy, concurrent shard fan-out), add a microbench-style test that asserts an order-of-magnitude floor/ceiling (e.g. `assert open_us < 50_000`), not an exact latency. Tag it (`@tag :bench`) so it's excluded from the default suite. See Benchmarking.

## Benchmarking

**The harness exists** (`docs/benchmark-plan.md`, built 2026-06-28). `mix fathom.bench` measures the hot paths; `scripts/benchmark.sh` runs it prod-compiled against a throwaway `fathom_bench` DB and appends to `scripts/perf_history.jsonl`; `scripts/commit_with_bench.sh` is the bench-then-commit gate. Hold the discipline: don't invent fake numbers; measure or say "unmeasured."

- **Hot paths to watch** (fathom's scaling story is *millions of small shards*, so cost is dominated by per-shard open and fan-out, not single-query throughput):
  - **Shard cold-open latency** — libSQL file open + pool spin-up + first query. **Two distinct paths, both now in the bench:** *warm* — the local file is present (a node restart, or a pull from local-NVMe `Storage.Local`) — is `cold_open_p50_us` (also `mix fathom.scale`; ~ms, often page-cache-warm); *cold-from-S3* — pull the shard object from the bottomless S3 backend — is `cold_open_s3_p50_us`, **opt-in** (set `FATHOM_S3_TEST_*` env, else `nil`/skipped so the default gate stays S3-free). Realism: against MinIO-on-localhost `cold_open_s3` measures the S3 *protocol* + loopback (~6 ms observed), NOT real S3 latency. Two ways to get representative numbers: point `FATHOM_S3_TEST_ENDPOINT` at S3/R2 in-region (the real thing), or — **without an AWS account** — `scripts/benchmark_s3_latency.sh` puts a **toxiproxy** between the bench and MinIO and injects latency/bandwidth on the real S3 byte stream (`S3_FAKE_LATENCY_MS`, `S3_FAKE_RATE_KBPS`). **Finding + fixes:** the cold-open does sequential S3 round-trips, so it amplifies S3 latency. Two cuts (measured 30 ms one-way: **227 ms → 150 ms → 75 ms, ≈3×**, fencing unchanged): (1) `Storage.S3.acquire_lease` does an **optimistic conditional create** (one `PUT If-None-Match:*`, read-then-resolve only on `412`) — 3 requests → 2; (2) `Fathom.Shard.init` **overlaps the lease acquire with the pull** (independent objects: `.lock` vs `.db`) — 2 → ~1 RTT. The pull writes to a temp file and is promoted only once the lease confirms, so a lost lease race never leaves a stale local copy (a refused start drops the temp; see the `:lease` test). Fence-first holds where it matters: we still only *serve* after the lease confirms. **What to expect** (`scripts/benchmark_s3_sweep.sh`): cold-open is now ~1 round-trip, so it's ~linear in latency — one-way **10 / 30 / 60 / 100 ms → ~26 / 77 / 137 / 215 ms** cold-open (≈2× one-way + a few ms; a real region adds S3's server-side TTFB on top). Same-region/AZ S3 ⇒ tens of ms; cross-region ⇒ ~150–250 ms. Bandwidth (10GbE+) helps *warming throughput* (many shards in parallel), not single-shard cold-open latency, which is gated by S3 first-byte and that round-trip count.
  - **Warming throughput** (`warm_s3_shards_per_s`, opt-in S3) — aggregate shards/s a node can pull from S3 at once (startup/failover). **Finding + fix (2026-06-29):** warming 200×256KB at 30 ms latency was ~10 shards/s — far below the ~50-conn Finch pool ceiling, because `Fathom.Shard.init` did blocking S3 I/O and `DynamicSupervisor.start_child` waits for `init`, so concurrent opens serialized at the supervisor (this also hurt cold-burst tail latency generally). Fixed: `init` returns immediately and the lease+pull run in `handle_continue` (a `:checkout` queues until it completes; an open failure stops the coordinator with `{:shutdown, _}` — not restarted — and `Fathom.Shard.checkout/1` maps that exit to `{:error, _}`). **~10 → ~327 shards/s (≈33×)**; that left Req's shared ~50-conn default pool as the next ceiling. **Pool lever (2026-06-29, DONE):** `Fathom.Shard.Storage.S3` now runs a **dedicated, config-driven Finch pool** (`finch_child_spec/0`, supervised in `application.ex`; size via `config :fathom, Fathom.Shard.Storage.S3, pool_size:`, default 200), and `req/0` routes through it (falling back to Req's default when the pool isn't started, so standalone use still works). Controlled sweep (30 ms injected latency, **dev build, localhost MinIO+toxiproxy — a relative lever, not a prod-absolute**): a 200-shard burst went **164 → 658 shards/s going 50 → 200 conns (~4×)**, then flat at 400 (the burst caps at 200 concurrent checkouts); a 500-shard burst reads **~745–827/s at 200–500 conns** — going past ~200 conns stops helping (200 conns ≈ 827, 500 conns ≈ 745–791), so **200 is the knee** and a sane default. **Count lever (2026-06-29, DONE):** `pool_count` is also config-driven (default 1; total conns = `pool_size * pool_count`), so concurrent checkouts can spread across N `nimble_pool` processes instead of contending on one. Sweep verdict on the localhost rig: count is a **no-op here** — at 200 conns count 1 ≈ count 4 (827 vs 824), at 500 conns count 1/5/10 all land 739–791 (within the ~7% run-to-run noise; the earlier one-shot "569" was just a low sample, re-measured at 745). The 500-conn ceiling is the **single MinIO container saturating server-side**, not the client-side pool checkout contention `count` fixes, so count can't show a win on this rig. Both knobs are exposed for real-S3 production tuning (many front-ends tolerate far more parallelism than one localhost container); warming is no longer client-pool-bound. Sweep with `FATHOM_S3_TEST_POOL_SIZE=N FATHOM_S3_TEST_POOL_COUNT=C S3_BENCH_ONLY=warm_s3 scripts/benchmark_s3_latency.sh --warm-shards M`.
  - **Directory resolve** — Postgres lookup + resolve cache hit/miss; this is on every request.
  - **Migration copy throughput** — rows/sec per shard for the blue/green copy+transform.
  - **Failover RTO — warm standby vs cold (A1-H3, opt-in S3).** `failover_cold_s3_p50_us` (survivor cold-opens the shard with a full S3 pull) vs `failover_warm_s3_p50_us` (the shard is in the warm-follower cache, so the H2 freshness check is a conditional **304** GET + a local copy). Both at `:warm_size_kb`; `mix fathom.bench --only failover_rto` prints the speedup, and `scripts/benchmark_s3_latency.sh` shapes latency/bandwidth. **The honest delta:** the warm path is **not** purely local — H2 must confirm the cache isn't stale, so it still pays ONE S3 round-trip (a 304, no body). The win over cold is the object **body transfer avoided**, so it scales with shard size and bandwidth-delay and is **marginal for a tiny shard on a fat pipe** (both are ~1 S3 RTT). Measured (2026-07-01, dev build, MinIO+toxiproxy, **relative lever not a prod-absolute**): 1 MB shard at 30 ms one-way + 100 Mbps cap → **cold ~162 ms, warm ~72 ms (≈2.3×)**; the ~90 ms saved is the 1 MB body the 304 skips, and the ~72 ms warm floor is the lease + freshness round-trips (which is why warm is *not* the ~2 ms the scoping doc first guessed). Bigger shards / tighter bandwidth widen the gap; same-region tiny shards barely move.
  - **Warm-standby density (A1-H3).** `mix fathom.scale --warm-density [--shards N] [--shard-size-mb S]` pre-pulls N shards into the follower cache and reports per-cached-shard disk (≈ shard size — the ceiling), the follower's per-shard BEAM bookkeeping (its cached-id set), and warming rate. **Finding:** a warm-cached shard costs its file on disk plus ~0 process/BEAM/fd — the follower holds no coordinator, no connection, no fds — so warm capacity is **disk-bound** (`disk / shard_size`), orders of magnitude past the ~196 KiB-BEAM-+-3-fd open-shard ceiling. So a standby warms far more than it can serve open.
  - **Concurrent shard fan-out** — how many shards a node can hold open at once. **Two ceilings** (measured 2026-06-29, see Scale test): *connection-per-shard* (every shard actively streaming) is **fd-bound** at ~`kern.maxfilesperproc / 3` (~82k on a 245760-limit box; ~3 fds per WAL connection), ~15 GB RAM — fds bind well before memory. *Idle-open coordinators* (no held connection; connections are per-stream/transient) is far higher, memory/process-bound (~26 KiB/coordinator). Per-shard cost is exqlite-connection + coordinator **overhead** (~180–196 KiB), not the page cache — density is overhead-bound, not data-bound.
  - **Hrana round-trip** — once remote shards land.
- **Run clean and in prod mode.** Compile in `MIX_ENV=prod` (and the Rust NIF in release, slow) for any bench run; start from a clean DB/data state; don't bench a dev build.
- **Tiered regression response:** **<20%** — ignore (noise). **≥20%** — assume real: rerun once to rule out noise; if confirmed, **revert first**, then reproduce minimally on a branch. Never stack fix attempts on top of a known-regressed commit.
- **Phantom-regression rule.** Declaring an observed regression "phantom" (numbers wrong, not the code) requires (a) the same regression measured by a *second* tool/environment, (b) an explicit explanation of why the original measurement was wrong, and (c) a cooling-off before closing. Don't declare phantom-regressions the same day they're observed — broken instrumentation is not evidence of a fixed regression.
- **The harness (built).** `scripts/benchmark.sh` runs the hot-path benches (`mix fathom.bench`: `cold_open_p50_us`, `cold_open_s3_p50_us` (opt-in, S3 endpoint), `warm_s3_shards_per_s` + `failover_cold_s3_p50_us`/`failover_warm_s3_p50_us` (opt-in, S3), `dir_resolve_p50_us`, `copy_rows_per_s`, `fanout_kb_per_shard`; `hrana_rt_us` is a `null` placeholder until remote shards) and appends one JSON line per run to `scripts/perf_history.jsonl` (commit, branch, dirty, host, metrics). `scripts/commit_with_bench.sh` is the gate: it benches the working tree and **refuses the commit if ANY metric regresses ≥20%** vs the parent's same-host entry (`Fathom.Bench.Gate`; multi-metric, not a single scalar, because fathom's cost is per-shard open + fan-out). It gates natively on the host (the copy bench is fsync-light — one end-of-copy checkpoint, not APFS-dominated like a per-row TPC-B). Hot-path changes also ship with the `@tag :bench` floor/ceiling guards (`test/fathom/bench_test.exs`). Baseline-host discipline: the gate compares same-`host` only.
- **Scale test (built).** `mix fathom.scale [--shards N] [--shard-size-mb S]` (`Fathom.Scale`) provisions N realistically-sized shards and measures cold-open latency at size + **fan-out node density** (BEAM + RSS per open shard, open throughput) — the "how many S-MB shards can one node hold open" number the empty-shard fan-out bench can't give. `mix fathom.scale --ramp [--max N]` opens empty shards until the fd ceiling to find the node-density limit cheaply; `mix fathom.scale --warm-density [--shards N] [--shard-size-mb S]` measures warm-standby density (disk-bound; ~0 BEAM/fd per cached shard — see Failover RTO). Holding N live connections costs ~3 fds each, so raise `ulimit -n` first. Measured 2026-06-29: 1000 × 4MB → ~196 KiB RSS/shard, ~300 MB total, warm cold-open ~2.6 ms p50; ramp held 50k linearly (~182 KiB/shard) → fd-bound ceiling ~82k (see Concurrent shard fan-out). Note: the ramp's per-open slowdown at high N is a single-process test artifact (one process holds all N connections+monitors), not fathom — production spreads shards across per-stream processes.

## Gates

A "gate" is a check that must pass *before* a commit lands — not after.

- **`mix precommit` is the commit gate** (defined in `mix.exs`): `compile --warnings-as-errors`, `deps.unlock --unused`, `format`, `test`. **Never commit if it fails.** Run it when you're done with all changes and fix everything it surfaces.
- **Migration gate.** A schema migration must not ship without: (a) a forward copy+transform test, (b) a revert-flip test, (c) a cross-version-tolerance check. A migration that can't be reverted by pointer-flip within the retention window, or that the running app can't tolerate mid-rollout, is not done.
- **Shard-isolation gate.** Any change to shard routing (`Fathom.ShardExecutor.shard_from_conn`, `Fathom.Shards`, shard-path construction, or the planned `Fathom.Directory` resolve) must have a test proving shard A never resolves to shard B. Treat a cross-tenant leak as a release blocker, not a finding.
- **NIF-contract guard (when `fathom_native` lands).** Elixir is dynamically typed, so a NIF signature change (arity, **return shape**, param types) silently breaks Elixir callers at *runtime* — the Rust compiler can't catch it. After any NIF signature change, run the integration tests that exercise it and `grep -rn "Fathom.Native.<fn>"` for every caller. Don't rely on the unit suite to catch a contract break.
- **Bench-then-commit gate** (built). Any change touching a hot path (shard routing/open, directory resolve, migration copy, the shard coordinator) goes through `scripts/commit_with_bench.sh -m "<msg>"`: it benches the working tree and refuses the commit on a ≥20% regression in any metric vs the parent's entry in `scripts/perf_history.jsonl` (override `PERF_REGRESS_BLOCK`). Pure docs/test/comment-only changes skip it — `git commit` directly with a `[skip-bench]` token, or `--skip`. See Benchmarking and `docs/benchmark-plan.md`.

## Principles

- **Simplicity first.** Minimal code touched. Find root causes, not workarounds. Prefer elegance for non-trivial changes; skip it for obvious fixes.
- **Fix bugs autonomously** — diagnose from logs/errors/tests, then resolve. No hand-holding.
- **Never hand-roll routing, namespace, or SQL string surgery.** Don't build shard paths / namespace names / directory keys by ad-hoc string concatenation scattered across the codebase — route every shard resolution through `Fathom.Shards` (and request → shard through `Fathom.ShardExecutor.shard_from_conn`; eventually `Fathom.Directory`) so isolation and cutover logic live in one place. Don't hand-roll SQL for the libSQL shards either — always bind with parameterized queries (`Fathom.Shard` passes args through to `exqlite`); never interpolate values into SQL (injection and quoting edge cases bite the same way Postgres's do).
- **Multi-tenant safety is non-negotiable.** Every shard query carries its `shard_id` explicitly through `Fathom.Shards`/`Fathom.Shard` — never an implicit/ambient "current shard," and never the Postgres `Fathom.Repo`. (If the `Fathom.ShardRepo` Ecto path is adopted, the equivalent rule is: always `put_dynamic_repo/1` the right shard before a query, never the default repo.) When in doubt about which shard a code path operates on, make it explicit.

## Architecture

### Current data path (built)

```
   libSQL client (django-libsql / ws, libsql-experimental / http)
                          │  Hrana, shard = Host subdomain
                          ▼
   Filo.Plug / Filo.Socket  (the Filo library — Hrana over HTTP + WebSocket)
                          │  Filo.Executor callback
                          ▼
   Fathom.ShardExecutor → Fathom.Shards.checkout/1 (find-or-start → file path)
                          ▼
   Fathom.Shard.Connection (one exqlite conn per stream) → SQLite file
                          ▲ pull on cold start / flush + drop on idle
   Fathom.Shard (coordinator: tracks conns, idle) ── Fathom.Shard.Storage
                                                       (Local | S3 via Req sigv4)
```

Request → shard is still **Host-based** (the shard id comes straight from the
request Host, not a directory lookup). The Postgres directory (`Fathom.Directory`)
now exists and records each access — buffered off the hot path by
`Fathom.Directory.Recorder` — and drives the migration/lifecycle machinery, but it
is not (yet) a routing resolve on the request path. A cross-node lease + epoch
fence (via `Fathom.Shard.Storage`) makes the open single-writer-safe; see the
control-plane / cluster bullets under Project.

### Target design (planned, see `docs/migration-plan.md`)

```
                Postgres  (Fathom.Repo — supervised)
                  directory: shard → namespace, schema_version,
                  live/retired, retain_until, migration_status
                          │  resolve (cached, PubSub-invalidated)
                          ▼
        ┌──────────── per request ────────────┐
        │  resolve shard → shard namespace │
        └──────────────────┬───────────────────┘
                           ▼
   libSQL/Turso shards     one DB per shard · S3 bottomless (planned)
                           ▲
                           │  blue/green migration (docs/migration-plan.md)
        create @vN from template → quiesce → copy+transform → validate
              → flip directory pointer → retire @vN-1 (retain_until)
```

Migration components are **built** (see the Migration engine bullet under Project): `Fathom.Migrator` + `Migrator.Capture`/`Copy`/`ShardMigration`/`Release`, and the Oban jobs `ShardMigrationJob` (unique per shard), `ReconcileJob` (cron sweep), `RetirementJob` (cron drop of expired retained versions), `RevertJob` (fleet pointer-flip). The copy/transform + `user_version` stamp live in `Migrator.Copy`/`ShardMigration` (there is **no** separate `Fathom.ShardExec`); retirement is `Migrator.RetirementJob` (**no** `Fathom.Retirement`). The copy/transform currently goes through `Fathom.Shard`/`exqlite`; whether the long-term data path adopts `Fathom.ShardRepo`/`Ecto.Adapters.LibSql` (defined, unused) is still open. The version stamp lives in **three places**: `_fathom_migrations` in each shard (truth), `PRAGMA user_version` (O(1) gate), `shards.schema_version` in Postgres (laggard queries without opening shards). Still aspirational in the diagram above: a **cached, PubSub-invalidated resolve on the request path** (routing is Host-based today).

---

# Framework guidelines (generated by `phx.new` usage-rules)

This is a web application written using the Phoenix web framework.

## Project guidelines

- Use `mix precommit` alias when you are done with all changes and fix any pending issues
- Use the already included and available `:req` (`Req`) library for HTTP requests, **avoid** `:httpoison`, `:tesla`, and `:httpc`. Req is included by default and is the preferred HTTP client for Phoenix apps

### Phoenix v1.8 guidelines

- **Always** begin your LiveView templates with `<Layouts.app flash={@flash} ...>` which wraps all inner content
- The `MyAppWeb.Layouts` module is aliased in the `my_app_web.ex` file, so you can use it without needing to alias it again
- Anytime you run into errors with no `current_scope` assign:
  - You failed to follow the Authenticated Routes guidelines, or you failed to pass `current_scope` to `<Layouts.app>`
  - **Always** fix the `current_scope` error by moving your routes to the proper `live_session` and ensure you pass `current_scope` as needed
- Phoenix v1.8 moved the `<.flash_group>` component to the `Layouts` module. You are **forbidden** from calling `<.flash_group>` outside of the `layouts.ex` module
- Out of the box, `core_components.ex` imports an `<.icon name="hero-x-mark" class="w-5 h-5"/>` component for for hero icons. **Always** use the `<.icon>` component for icons, **never** use `Heroicons` modules or similar
- **Always** use the imported `<.input>` component for form inputs from `core_components.ex` when available. `<.input>` is imported and using it will save steps and prevent errors
- If you override the default input classes (`<.input class="myclass px-2 py-1 rounded-lg">)`) class with your own values, no default classes are inherited, so your
custom classes must fully style the input

### JS and CSS guidelines

- **Use Tailwind CSS classes and custom CSS rules** to create polished, responsive, and visually stunning interfaces.
- Tailwindcss v4 **no longer needs a tailwind.config.js** and uses a new import syntax in `app.css`:

      @import "tailwindcss" source(none);
      @source "../css";
      @source "../js";
      @source "../../lib/my_app_web";

- **Always use and maintain this import syntax** in the app.css file for projects generated with `phx.new`
- **Never** use `@apply` when writing raw css
- **Always** manually write your own tailwind-based components instead of using daisyUI for a unique, world-class design
- Out of the box **only the app.js and app.css bundles are supported**
  - You cannot reference an external vendor'd script `src` or link `href` in the layouts
  - You must import the vendor deps into app.js and app.css to use them
  - **Never write inline <script>custom js</script> tags within templates**

### UI/UX & design guidelines

- **Produce world-class UI designs** with a focus on usability, aesthetics, and modern design principles
- Implement **subtle micro-interactions** (e.g., button hover effects, and smooth transitions)
- Ensure **clean typography, spacing, and layout balance** for a refined, premium look
- Focus on **delightful details** like hover effects, loading states, and smooth page transitions


<!-- usage-rules-start -->

<!-- phoenix:elixir-start -->
## Elixir guidelines

- Elixir lists **do not support index based access via the access syntax**

  **Never do this (invalid)**:

      i = 0
      mylist = ["blue", "green"]
      mylist[i]

  Instead, **always** use `Enum.at`, pattern matching, or `List` for index based list access, ie:

      i = 0
      mylist = ["blue", "green"]
      Enum.at(mylist, i)

- Elixir variables are immutable, but can be rebound, so for block expressions like `if`, `case`, `cond`, etc
  you *must* bind the result of the expression to a variable if you want to use it and you CANNOT rebind the result inside the expression, ie:

      # INVALID: we are rebinding inside the `if` and the result never gets assigned
      if connected?(socket) do
        socket = assign(socket, :val, val)
      end

      # VALID: we rebind the result of the `if` to a new variable
      socket =
        if connected?(socket) do
          assign(socket, :val, val)
        end

- **Never** nest multiple modules in the same file as it can cause cyclic dependencies and compilation errors
- **Never** use map access syntax (`changeset[:field]`) on structs as they do not implement the Access behaviour by default. For regular structs, you **must** access the fields directly, such as `my_struct.field` or use higher level APIs that are available on the struct if they exist, `Ecto.Changeset.get_field/2` for changesets
- Elixir's standard library has everything necessary for date and time manipulation. Familiarize yourself with the common `Time`, `Date`, `DateTime`, and `Calendar` interfaces by accessing their documentation as necessary. **Never** install additional dependencies unless asked or for date/time parsing (which you can use the `date_time_parser` package)
- Don't use `String.to_atom/1` on user input (memory leak risk)
- Predicate function names should not start with `is_` and should end in a question mark. Names like `is_thing` should be reserved for guards
- Elixir's builtin OTP primitives like `DynamicSupervisor` and `Registry`, require names in the child spec, such as `{DynamicSupervisor, name: MyApp.MyDynamicSup}`, then you can use `DynamicSupervisor.start_child(MyApp.MyDynamicSup, child_spec)`
- Use `Task.async_stream(collection, callback, options)` for concurrent enumeration with back-pressure. The majority of times you will want to pass `timeout: :infinity` as option

## Mix guidelines

- Read the docs and options before using tasks (by using `mix help task_name`)
- To debug test failures, run tests in a specific file with `mix test test/my_test.exs` or run all previously failed tests with `mix test --failed`
- `mix deps.clean --all` is **almost never needed**. **Avoid** using it unless you have good reason

## Test guidelines

- **Always use `start_supervised!/1`** to start processes in tests as it guarantees cleanup between tests
- **Avoid** `Process.sleep/1` and `Process.alive?/1` in tests
  - Instead of sleeping to wait for a process to finish, **always** use `Process.monitor/1` and assert on the DOWN message:

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

   - Instead of sleeping to synchronize before the next call, **always** use `_ = :sys.get_state/1` to ensure the process has handled prior messages
<!-- phoenix:elixir-end -->

<!-- phoenix:phoenix-start -->
## Phoenix guidelines

- Remember Phoenix router `scope` blocks include an optional alias which is prefixed for all routes within the scope. **Always** be mindful of this when creating routes within a scope to avoid duplicate module prefixes.

- You **never** need to create your own `alias` for route definitions! The `scope` provides the alias, ie:

      scope "/admin", AppWeb.Admin do
        pipe_through :browser

        live "/users", UserLive, :index
      end

  the UserLive route would point to the `AppWeb.Admin.UserLive` module

- `Phoenix.View` no longer is needed or included with Phoenix, don't use it
<!-- phoenix:phoenix-end -->

<!-- phoenix:ecto-start -->
## Ecto Guidelines

- **Always** preload Ecto associations in queries when they'll be accessed in templates, ie a message that needs to reference the `message.user.email`
- Remember `import Ecto.Query` and other supporting modules when you write `seeds.exs`
- `Ecto.Schema` fields always use the `:string` type, even for `:text`, columns, ie: `field :name, :string`
- `Ecto.Changeset.validate_number/2` **DOES NOT SUPPORT the `:allow_nil` option**. By default, Ecto validations only run if a change for the given field exists and the change value is not nil, so such as option is never needed
- You **must** use `Ecto.Changeset.get_field(changeset, :field)` to access changeset fields
- Fields which are set programatically, such as `user_id`, must not be listed in `cast` calls or similar for security purposes. Instead they must be explicitly set when creating the struct
- **Always** invoke `mix ecto.gen.migration migration_name_using_underscores` when generating migration files, so the correct timestamp and conventions are applied
<!-- phoenix:ecto-end -->

<!-- phoenix:html-start -->
## Phoenix HTML guidelines

- Phoenix templates **always** use `~H` or .html.heex files (known as HEEx), **never** use `~E`
- **Always** use the imported `Phoenix.Component.form/1` and `Phoenix.Component.inputs_for/1` function to build forms. **Never** use `Phoenix.HTML.form_for` or `Phoenix.HTML.inputs_for` as they are outdated
- When building forms **always** use the already imported `Phoenix.Component.to_form/2` (`assign(socket, form: to_form(...))` and `<.form for={@form} id="msg-form">`), then access those forms in the template via `@form[:field]`
- **Always** add unique DOM IDs to key elements (like forms, buttons, etc) when writing templates, these IDs can later be used in tests (`<.form for={@form} id="product-form">`)
- For "app wide" template imports, you can import/alias into the `my_app_web.ex`'s `html_helpers` block, so they will be available to all LiveViews, LiveComponent's, and all modules that do `use MyAppWeb, :html` (replace "my_app" by the actual app name)

- Elixir supports `if/else` but **does NOT support `if/else if` or `if/elsif`**. **Never use `else if` or `elseif` in Elixir**, **always** use `cond` or `case` for multiple conditionals.

  **Never do this (invalid)**:

      <%= if condition do %>
        ...
      <% else if other_condition %>
        ...
      <% end %>

  Instead **always** do this:

      <%= cond do %>
        <% condition -> %>
          ...
        <% condition2 -> %>
          ...
        <% true -> %>
          ...
      <% end %>

- HEEx require special tag annotation if you want to insert literal curly's like `{` or `}`. If you want to show a textual code snippet on the page in a `<pre>` or `<code>` block you *must* annotate the parent tag with `phx-no-curly-interpolation`:

      <code phx-no-curly-interpolation>
        let obj = {key: "val"}
      </code>

  Within `phx-no-curly-interpolation` annotated tags, you can use `{` and `}` without escaping them, and dynamic Elixir expressions can still be used with `<%= ... %>` syntax

- HEEx class attrs support lists, but you must **always** use list `[...]` syntax. You can use the class list syntax to conditionally add classes, **always do this for multiple class values**:

      <a class={[
        "px-2 text-white",
        @some_flag && "py-5",
        if(@other_condition, do: "border-red-500", else: "border-blue-100"),
        ...
      ]}>Text</a>

  and **always** wrap `if`'s inside `{...}` expressions with parens, like done above (`if(@other_condition, do: "...", else: "...")`)

  and **never** do this, since it's invalid (note the missing `[` and `]`):

      <a class={
        "px-2 text-white",
        @some_flag && "py-5"
      }> ...
      => Raises compile syntax error on invalid HEEx attr syntax

- **Never** use `<% Enum.each %>` or non-for comprehensions for generating template content, instead **always** use `<%= for item <- @collection do %>`
- HEEx HTML comments use `<%!-- comment --%>`. **Always** use the HEEx HTML comment syntax for template comments (`<%!-- comment --%>`)
- HEEx allows interpolation via `{...}` and `<%= ... %>`, but the `<%= %>` **only** works within tag bodies. **Always** use the `{...}` syntax for interpolation within tag attributes, and for interpolation of values within tag bodies. **Always** interpolate block constructs (if, cond, case, for) within tag bodies using `<%= ... %>`.

  **Always** do this:

      <div id={@id}>
        {@my_assign}
        <%= if @some_block_condition do %>
          {@another_assign}
        <% end %>
      </div>

  and **Never** do this – the program will terminate with a syntax error:

      <%!-- THIS IS INVALID NEVER EVER DO THIS --%>
      <div id="<%= @invalid_interpolation %>">
        {if @invalid_block_construct do}
        {end}
      </div>
<!-- phoenix:html-end -->

<!-- phoenix:liveview-start -->
## Phoenix LiveView guidelines

- **Never** use the deprecated `live_redirect` and `live_patch` functions, instead **always** use the `<.link navigate={href}>` and  `<.link patch={href}>` in templates, and `push_navigate` and `push_patch` functions LiveViews
- **Avoid LiveComponent's** unless you have a strong, specific need for them
- LiveViews should be named like `AppWeb.WeatherLive`, with a `Live` suffix. When you go to add LiveView routes to the router, the default `:browser` scope is **already aliased** with the `AppWeb` module, so you can just do `live "/weather", WeatherLive`

### LiveView streams

- **Always** use LiveView streams for collections for assigning regular lists to avoid memory ballooning and runtime termination with the following operations:
  - basic append of N items - `stream(socket, :messages, [new_msg])`
  - resetting stream with new items - `stream(socket, :messages, [new_msg], reset: true)` (e.g. for filtering items)
  - prepend to stream - `stream(socket, :messages, [new_msg], at: -1)`
  - deleting items - `stream_delete(socket, :messages, msg)`

- When using the `stream/3` interfaces in the LiveView, the LiveView template must 1) always set `phx-update="stream"` on the parent element, with a DOM id on the parent element like `id="messages"` and 2) consume the `@streams.stream_name` collection and use the id as the DOM id for each child. For a call like `stream(socket, :messages, [new_msg])` in the LiveView, the template would be:

      <div id="messages" phx-update="stream">
        <div :for={{id, msg} <- @streams.messages} id={id}>
          {msg.text}
        </div>
      </div>

- LiveView streams are *not* enumerable, so you cannot use `Enum.filter/2` or `Enum.reject/2` on them. Instead, if you want to filter, prune, or refresh a list of items on the UI, you **must refetch the data and re-stream the entire stream collection, passing reset: true**:

      def handle_event("filter", %{"filter" => filter}, socket) do
        # re-fetch the messages based on the filter
        messages = list_messages(filter)

        {:noreply,
         socket
         |> assign(:messages_empty?, messages == [])
         # reset the stream with the new messages
         |> stream(:messages, messages, reset: true)}
      end

- LiveView streams *do not support counting or empty states*. If you need to display a count, you must track it using a separate assign. For empty states, you can use Tailwind classes:

      <div id="tasks" phx-update="stream">
        <div class="hidden only:block">No tasks yet</div>
        <div :for={{id, task} <- @stream.tasks} id={id}>
          {task.name}
        </div>
      </div>

  The above only works if the empty state is the only HTML block alongside the stream for-comprehension.

- When updating an assign that should change content inside any streamed item(s), you MUST re-stream the items
  along with the updated assign:

      def handle_event("edit_message", %{"message_id" => message_id}, socket) do
        message = Chat.get_message!(message_id)
        edit_form = to_form(Chat.change_message(message, %{content: message.content}))

        # re-insert message so @editing_message_id toggle logic takes effect for that stream item
        {:noreply,
         socket
         |> stream_insert(:messages, message)
         |> assign(:editing_message_id, String.to_integer(message_id))
         |> assign(:edit_form, edit_form)}
      end

  And in the template:

      <div id="messages" phx-update="stream">
        <div :for={{id, message} <- @streams.messages} id={id} class="flex group">
          {message.username}
          <%= if @editing_message_id == message.id do %>
            <%!-- Edit mode --%>
            <.form for={@edit_form} id="edit-form-#{message.id}" phx-submit="save_edit">
              ...
            </.form>
          <% end %>
        </div>
      </div>

- **Never** use the deprecated `phx-update="append"` or `phx-update="prepend"` for collections

### LiveView JavaScript interop

- Remember anytime you use `phx-hook="MyHook"` and that JS hook manages its own DOM, you **must** also set the `phx-update="ignore"` attribute
- **Always** provide an unique DOM id alongside `phx-hook` otherwise a compiler error will be raised

LiveView hooks come in two flavors, 1) colocated js hooks for "inline" scripts defined inside HEEx,
and 2) external `phx-hook` annotations where JavaScript object literals are defined and passed to the `LiveSocket` constructor.

#### Inline colocated js hooks

**Never** write raw embedded `<script>` tags in heex as they are incompatible with LiveView.
Instead, **always use a colocated js hook script tag (`:type={Phoenix.LiveView.ColocatedHook}`)
when writing scripts inside the template**:

    <input type="text" name="user[phone_number]" id="user-phone-number" phx-hook=".PhoneNumber" />
    <script :type={Phoenix.LiveView.ColocatedHook} name=".PhoneNumber">
      export default {
        mounted() {
          this.el.addEventListener("input", e => {
            let match = this.el.value.replace(/\D/g, "").match(/^(\d{3})(\d{3})(\d{4})$/)
            if(match) {
              this.el.value = `${match[1]}-${match[2]}-${match[3]}`
            }
          })
        }
      }
    </script>

- colocated hooks are automatically integrated into the app.js bundle
- colocated hooks names **MUST ALWAYS** start with a `.` prefix, i.e. `.PhoneNumber`

#### External phx-hook

External JS hooks (`<div id="myhook" phx-hook="MyHook">`) must be placed in `assets/js/` and passed to the
LiveSocket constructor:

    const MyHook = {
      mounted() { ... }
    }
    let liveSocket = new LiveSocket("/live", Socket, {
      hooks: { MyHook }
    });

#### Pushing events between client and server

Use LiveView's `push_event/3` when you need to push events/data to the client for a phx-hook to handle.
**Always** return or rebind the socket on `push_event/3` when pushing events:

    # re-bind socket so we maintain event state to be pushed
    socket = push_event(socket, "my_event", %{...})

    # or return the modified socket directly:
    def handle_event("some_event", _, socket) do
      {:noreply, push_event(socket, "my_event", %{...})}
    end

Pushed events can then be picked up in a JS hook with `this.handleEvent`:

    mounted() {
      this.handleEvent("my_event", data => console.log("from server:", data));
    }

Clients can also push an event to the server and receive a reply with `this.pushEvent`:

    mounted() {
      this.el.addEventListener("click", e => {
        this.pushEvent("my_event", { one: 1 }, reply => console.log("got reply from server:", reply));
      })
    }

Where the server handled it via:

    def handle_event("my_event", %{"one" => 1}, socket) do
      {:reply, %{two: 2}, socket}
    end

### LiveView tests

- `Phoenix.LiveViewTest` module and `LazyHTML` (included) for making your assertions
- Form tests are driven by `Phoenix.LiveViewTest`'s `render_submit/2` and `render_change/2` functions
- Come up with a step-by-step test plan that splits major test cases into small, isolated files. You may start with simpler tests that verify content exists, gradually add interaction tests
- **Always reference the key element IDs you added in the LiveView templates in your tests** for `Phoenix.LiveViewTest` functions like `element/2`, `has_element/2`, selectors, etc
- **Never** tests again raw HTML, **always** use `element/2`, `has_element/2`, and similar: `assert has_element?(view, "#my-form")`
- Instead of relying on testing text content, which can change, favor testing for the presence of key elements
- Focus on testing outcomes rather than implementation details
- Be aware that `Phoenix.Component` functions like `<.form>` might produce different HTML than expected. Test against the output HTML structure, not your mental model of what you expect it to be
- When facing test failures with element selectors, add debug statements to print the actual HTML, but use `LazyHTML` selectors to limit the output, ie:

      html = render(view)
      document = LazyHTML.from_fragment(html)
      matches = LazyHTML.filter(document, "your-complex-selector")
      IO.inspect(matches, label: "Matches")

### Form handling

#### Creating a form from params

If you want to create a form based on `handle_event` params:

    def handle_event("submitted", params, socket) do
      {:noreply, assign(socket, form: to_form(params))}
    end

When you pass a map to `to_form/1`, it assumes said map contains the form params, which are expected to have string keys.

You can also specify a name to nest the params:

    def handle_event("submitted", %{"user" => user_params}, socket) do
      {:noreply, assign(socket, form: to_form(user_params, as: :user))}
    end

#### Creating a form from changesets

When using changesets, the underlying data, form params, and errors are retrieved from it. The `:as` option is automatically computed too. E.g. if you have a user schema:

    defmodule MyApp.Users.User do
      use Ecto.Schema
      ...
    end

And then you create a changeset that you pass to `to_form`:

    %MyApp.Users.User{}
    |> Ecto.Changeset.change()
    |> to_form()

Once the form is submitted, the params will be available under `%{"user" => user_params}`.

In the template, the form form assign can be passed to the `<.form>` function component:

    <.form for={@form} id="todo-form" phx-change="validate" phx-submit="save">
      <.input field={@form[:field]} type="text" />
    </.form>

Always give the form an explicit, unique DOM ID, like `id="todo-form"`.

#### Avoiding form errors

**Always** use a form assigned via `to_form/2` in the LiveView, and the `<.input>` component in the template. In the template **always access forms this**:

    <%!-- ALWAYS do this (valid) --%>
    <.form for={@form} id="my-form">
      <.input field={@form[:field]} type="text" />
    </.form>

And **never** do this:

    <%!-- NEVER do this (invalid) --%>
    <.form for={@changeset} id="my-form">
      <.input field={@changeset[:field]} type="text" />
    </.form>

- You are FORBIDDEN from accessing the changeset in the template as it will cause errors
- **Never** use `<.form let={f} ...>` in the template, instead **always use `<.form for={@form} ...>`**, then drive all form references from the form assign as in `@form[:field]`. The UI should **always** be driven by a `to_form/2` assigned in the LiveView module that is derived from a changeset
<!-- phoenix:liveview-end -->

<!-- usage-rules-end -->