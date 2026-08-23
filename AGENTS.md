# AGENTS.md — Fathom

## Project

Fathom is a multi-tenant sharded data platform built on Phoenix: one SQLite database per shard (eventually millions), served to unchanged libSQL clients (e.g. an unchanged Django app via `django-libsql`) over the network.

**Where the detail lives.** This section is a **map** — what exists, one line each. It is deliberately
short because AGENTS.md is loaded into every session.

- [`docs/README.md`](docs/README.md) — the index: per-subsystem how-it-works stories, design plans, benchmark plans, runbooks, run reports.
- [`docs/component-notes.md`](docs/component-notes.md) — the long-form record for every component below: the finding that motivated it, the fix that was tried and was wrong, the measurement that settled it, the trap that will bite you. **Read the entry before changing a component.**
- [`docs/reviews/`](docs/reviews/) — the full run reports.

### What exists today (the working slice)

| Component | Modules | Deeper |
|---|---|---|
| **Shard data path** — connection per Hrana stream, per-shard coordinator owning the file lifecycle (pull on cold start, checkpoint+flush+drop when idle), write-gated by a `dirty` flag | `Fathom.Shard`, `Fathom.Shard.Connection` (exqlite), `Fathom.Shards` | [data-path.md](docs/data-path.md), [durability.md](docs/durability.md) |
| **Shard storage** — `pull/2` + `flush/2` behaviour; a present local file is authoritative on wake | `Fathom.Shard.Storage{,.Local,.S3}` | — |
| **Network protocol** — Hrana over HTTP v1/v2/v3 + WebSocket, on its own Bandit listener (`:hrana_port`, default 8080) | `Fathom.ShardExecutor` + the **Filo** library (separate repo) | — |
| **Shard selection + admission** — shard = Host subdomain, fail-closed; double-gated novel-shard admission (soft `:max_open_shards` cap + LRU idle-eviction, `NovelLimiter`) | `Fathom.ShardId`, `Fathom.Shards.{NovelLimiter,Lru}` | [admission.md](docs/admission.md) |
| **Auth** — per-shard `Phoenix.Token` as libSQL's `authToken`, on Filo's `:authorize` seam; rotate/revoke, `:ro` scope, issuance ledger, fleet-wide time-scoped revoke | `Fathom.HranaAuth{,.Ledger,.RevokeJob}` | [auth.md](docs/auth.md) |
| **Postgres** — orchestration store + dashboard on :4000 | `Fathom.Repo` | — |
| **Directory / control plane** — per-shard `schema_version`, lifecycle status, `last_active_at`, `last_flushed_at`; buffered **off** the hot path | `Fathom.Directory{,.Recorder,.Shard}` | [directory.md](docs/directory.md) |
| **Migration engine** — blue/green per-shard: capture → copy+transform → stamp → flip; Oban-driven, with cold-tail reconcile, guarded revert, and a `Transform` seam for data migrations | `Fathom.Migrator.*` | [migration.md](docs/migration.md), [django-migrations.md](docs/django-migrations.md) |
| **Tenant lifecycle** — provision, suspend/resume, delete (tombstone + purge + re-mint guard), export, fork; JSON API under `/api` behind admin BasicAuth | `Fathom.Tenants{,.Tombstones,.Suspensions,.DeleteJob}` | [tenant-lifecycle.md](docs/tenant-lifecycle.md) |
| **Per-shard load counters** — lock-free ETS counters read by the rebalancer; `:shard_load`, **off by default** | `Fathom.ShardLoad` | — |
| **Dynamic rebalancing** — detect (per-node reporting) → decide (p99/floor hotness + guards) → execute (warm → flip the LB map → drain the lease). All gates **off by default** | `Fathom.Rebalancer.*` | [rebalancing.md](docs/rebalancing.md), [runbooks/rebalancer.md](docs/runbooks/rebalancer.md) |
| **Cross-node single-writer** — S3 lease `{owner, epoch}` + O(nodes) node heartbeat + etag flush fence. **The only cross-node coordination** — via S3, not BEAM | `Fathom.Shard.{Heartbeat,Storage}` | [single-writer.md](docs/single-writer.md) |
| **Cluster layer** — L7 LB consistent-hashes the Host subdomain to one node; each node is an independent single-node fathom. Health probe, telemetry, OTel bridge, and the `deploy/chaos/` rig | `Fathom.HealthPlug`, `Fathom.Telemetry` | [deploy-cluster.md](docs/deploy-cluster.md), [runbooks/cluster.md](docs/runbooks/cluster.md) |
| **Warm standby** — lease-less read cache of the fleet's hot set; failover promotes after a conditional (304) freshness check. `:warm_follower`, **off by default** | `Fathom.Shard.WarmFollower` | [warm-standby.md](docs/warm-standby.md) |
| **Live WAL replication (A2)** — quorum-replicated WAL frames over A2's own socket protocol + peer-recovery on failover. **Off by default**; ship and receive are **separate** gates | `Fathom.Shard.Replication.*` | [a2-quorum-replication.md](docs/a2-quorum-replication.md) |
| **Scheduled snapshots + GFS retention** — hourly Oban crons, both **off by default**; retention only ever deletes what the scheduler created | `Fathom.Snapshots{,.ScheduleJob,.RetentionJob}` | [durability.md](docs/durability.md) |
| **Restore drill** — verifies the stored object *and* the recovery procedure (fork → cold-open → row-count compare) | `Fathom.RestoreDrillJob` | — |
| **Disk observability + warm-cache back-pressure** — `:disksup` on the existing poller; the warm cache stops warming under a free-space floor | `Fathom.Admin.Measurements` | — |
| **Django UDF compatibility** — a Rust **loadable SQLite extension** supplying the 35 of Django's 54 backend functions SQLite lacks, loaded per connection (enable → load → **disable**) | `native/fathom_udf`, `Fathom.Shard.Extension` | [quickstart-django.md](docs/quickstart-django.md) |
| **Bench + scale harnesses** | `mix fathom.bench`, `mix fathom.scale`, `mix fathom.rpo` | [benchmark-plan.md](docs/benchmark-plan.md) |

**Not in the code — don't assume these exist:** per-shard follower sets, zone-aware placement,
rendezvous/bounded-load hashing (C1), multi-region affinity (C2), a cached PubSub-invalidated
directory resolve on the request path (routing is Host-based today), and a `fathom_native` Rustler
**NIF** (`native/` holds a loadable *extension*, which is not the same thing — twice now the answer
to "we need a NIF" was the extension already there). Also absent: `Fathom.ShardExec`,
`Fathom.Retirement` — old names, don't grep for them.

### Loud warnings

The things that cost a day if you don't know them. Full stories in
[`docs/component-notes.md`](docs/component-notes.md).

- **Every capacity and throughput number in the docs was measured with A2 replication OFF, and they do not hold with it on.** On the chaos rig, replication-on is clean to **~512 tenants** and 1024 runs but saturates the links. Never quote a replication-off number for a replicating fleet.
- **`SHARD_FLUSH_INTERVAL_MS` is not a throughput setting.** A durability flush checkpoints the WAL, which starts a new generation, which makes the next replication push ship the *whole* WAL instead of a delta. The 5 s default exists for failover scenarios; at 512 tenants, 5,000 → 30,000 took errors from 22,599 to **4**. Set it realistically or you are measuring checkpoint churn.
- **`sqlite3_wal_hook` and `wal_autocheckpoint` are the same slot.** A hook that merely observes silently disables checkpointing on every tenant connection and grows the WAL without bound.
- **The replication port is unauthenticated** — `REPLICATION_BIND_IP` is a security control, not a convenience.
- **Extension loading is arbitrary code execution on a multi-tenant engine.** The sequence is enable → load ours → **disable**, on every handle including `:ro`; a failure to re-disable fails the open.
- **A template shard is a fleet-wide poisoning vector.** Never set one in prod without auth on it, and never make `:default_shard` equal it (a boot guard refuses that config).

## Execution style

- **Sequenced directives** ("do X then Y", "review then execute") → execute directly. Don't re-confirm the sequence. If genuinely ambiguous, name your default and proceed; stop only if the ambiguity risks irreversible harm.
- **"Go ahead" / "continue" / "proceed"** = continue the *most recently scoped* task. Never authorization to escalate review→implementation or jump phases. Asked for a review → deliver only the review.
- **A work QUEUE is one task, not N tasks.** A findings list, a migration sweep, a rollout, a batch of files: the "continue" was granted when the queue was accepted. **Never end a turn asking whether to keep going** — "want me to do the next one?" is an abandoned queue, and it reads as completion to whoever left you running. Work it until every item has a terminal status, or a documented stop condition fires (stop-after-2, a >50-error scope blowup, >60 min past estimate).
  - **An item you cannot decide alone gets PARKED, not waited on.** Write down the decision and its options, ship any part that stands independently, move on. One open question must never stall the queue.
  - **Ask the question out loud, then keep working.** State it in prose and carry on; if the user is present the answer arrives with your next tool result. Never block on `AskUserQuestion` inside a queue run — it has no timeout, so "ask and wait a bit" cannot be expressed with it.
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

- **`--warnings-as-errors` enforces placement, not just correctness.** Three that cost a compile round trip each, every session, and are invisible until you build:
  - A `@module_attribute` must be **defined above every use**. Adding one next to the function that reads it fails when that function sits earlier in the file.
  - **Clauses of the same name/arity must be contiguous.** Inserting a new `handle_info/2` clause after an unrelated function splits the group. A private helper defined *between* two clauses splits it too — put the helper after the whole group.
  - A new clause must go **above the catch-all**, not merely near it. `def handle_info({:DOWN, ref, …}, %{renew_task: …})` placed after the generic `{:DOWN, …}` clause is unreachable, and the compiler says so.
  - When scripting a bulk edit with `python3`, a `str.replace` with no count replaces **every** occurrence — a duplicated function definition is the usual result. Pass a count, then `grep -c` to confirm.
- **Shell is zsh — stop re-learning this.** These bite every session; internalize them:
  - Backticks and `$(...)` run command substitution even inside double quotes — a backtick or unescaped `$(` in a `git commit -m "..."` body gets executed and silently mangles the message. Don't wrap identifiers in backticks inside `-m`; use `git commit -F <file>` for any non-trivial message.
  - **zsh parameter-expansion patterns glob.** `${var#pat}` / `${var%pat}` treat `(`, `[`, `]`, `#`, `?`, `*` as pattern metacharacters, so `${m#](}` dies with `bad pattern: ](`. Don't hand-strip brackets/parens with `${...#...}` — use `grep -oE`, or just do the text munging in `python3`.
  - Unquoted globs (`*`, `?`, `[...]`) and `{a,b}` brace-expand — quote them when you mean literals.
  - **Don't assume `sed`/`awk`/`dirname`/`head`/`tail` are on PATH** — several aren't in this sandbox (and AGENTS.md already says use Read/Grep/Edit to read/edit files, never those). For a text pipeline, reach for `grep` or a short `python3 - <<'PY'` heredoc, not `sed`/`awk`.
- **When `fathom_native` lands:** the NIF builds via Rustler on `mix compile`; release builds need `MIX_ENV=prod` and are slow (set timeouts). Rust tests: `cd native && cargo test`.

## Workflow

Plan mode for non-trivial tasks (3+ steps or an architectural decision). Stop and re-plan when something goes wrong. Use subagents for research/exploration/parallel work — one task each. When the parent model is Fable, use your own judgement about which model each subagent should run on (match the model to the subtask's difficulty rather than defaulting to the parent's).

**Implementation cycle:** implement → compile → test → (bench if hot path) → `mix precommit` → commit → **stop**. Pushing is a separate, human-approved step (below). Test after every change; fix failures before proceeding. Commit in logical units matching plan phases.

- **NEVER `git push` without the user's explicit approval.** Not "when it looks ready", not "when the suite is green", not "when the work seems viable" — **ask, and wait for a yes.** Commit locally as much as you like; pushing is the user's call every time. When you believe a batch is worth pushing, say so and stop.
  - **Why:** the repo went **public** on 2026-07-29. `main` is the project's face, anyone can be watching, and history is no longer safely rewritable once others may have cloned it. That makes "should this be visible now?" a judgment about the project's public posture — the user's to make, not something to infer from a test run.
  - **Commits still don't wait.** Local commits are the checkpoint that makes a bad step cheap to undo, so batching *commits* is still wrong; batching the *push* is the point. Don't sit on a large unpushed pile for days without raising it — a full night's work was once lost to local-only commits, so name the risk and ask.
  - Supersedes the earlier "ALWAYS push immediately after every commit" rule (written while the repo was private) and the interim "push when the work is viable" wording, which still left the decision to the agent.
- **NEVER commit** with compiler warnings, build errors, or failing tests. `mix precommit` is the gate (see Gates).
- **Never use `sed`/`awk`/`head`/`tail`/`echo` to read or edit files** — use Read (offset/limit), Grep, Edit/Write. Shell text tools are only for things the standard tools genuinely can't do. Piping command output is fine.
- Track plans in `tasks/todo.md`. Record corrections/lessons in `tasks/lessons.md`.

### Stop-after-2-failures rule

If a script or command (test/build/migration/sweep) fails **twice with a similar error**, stop. Print the exact command, the exact error, and a one-paragraph root-cause hypothesis, then wait. Don't loop on infra failures (missing dep, DB-not-created, port conflict, libSQL file-lock) — diagnose them. Same rule for scope blowups: a refactor producing **>50 compile errors** or running **>60 min past estimate** → stop and report. Never paper over flaky tests or build failures with sleeps, retries, or timeout bumps.

### A review's recommended fix is a hypothesis, not a spec

A finding from `/expert-audit`, `/review`, or any panel has **two separable claims**: *this is broken*
(usually right — panels verify by reading, sometimes by execution) and *fix it this way* (frequently
wrong, because the recommender did not run it). **Verify the mechanism of the fix before building
it**, with the cheapest experiment that would falsify it.

Measured on the 2026-08-01 panel, where 4 of ~31 recommended fixes were wrong in ways the finding
itself was not:

- "Set the SQLite authorizer in `Connection.open/1`" — would have broken **every durability flush**,
  because `VACUUM INTO` is implemented as an internal ATTACH. A 30-second probe caught it.
- "`quick_check` the snapshot temp" — the temp is the post-`VACUUM INTO` file, and VACUUM *rebuilds
  indexes from table content*, so it repairs exactly the corruption class the gate exists to catch.
  The gate would have shipped and never fired.
- "Delete the lock instead of rolling it back" — makes the next `acquire_lease` a fresh create at
  **epoch 1**, a larger backward jump than the bug being fixed.
- "Reduce `prev_load` to the shards that moved" — an unmoved shard still needs its baseline, or the
  next tick reports a huge spurious rate for an *idle* shard.

So: implement the finding, not the prescription. When the prescription turns out wrong, **record why
in the code comment and the progress file** — the next reader needs to know the obvious-looking fix
was tried and is wrong, or they will "simplify" it back.

### An existing test that blocks your fix may be right

When a fix breaks an existing test, decide **which of the two encodes the intended behaviour** before
touching either. Three outcomes, all seen in one session:

1. **The test pinned the defect** (`flush_gate_test` asserted "unbounded by default"; `lb_apply_test`
   asserted a byte-identical re-render after a *failed* reload returns `:ok`). Update the test, and
   say in the test itself that it previously asserted the opposite and why.
2. **The fixture was unrealistic** — it fabricated a state production cannot produce (a shard `.db`
   with no provenance sidecar; a 1-row table whose index page is mostly free space, so the corruption
   fixture corrupted nothing). Make the fixture realistic, and comment what makes it so.
3. **The test was right and the finding was wrong.** `S3StealTouchRollbackTest` caught that deleting
   the lock reintroduces an unfenced takeover — a hazard a prior review had added that rollback to
   prevent. The test saved the fix.

A test that fails because of a deliberate, documented constraint is case 3. Read the comment above it
before assuming case 1.

## Testing

Complements the framework **Test guidelines** below (`start_supervised!`, no `Process.sleep`/`Process.alive?`, monitor for DOWN). This section is the *discipline*.

- **Add coverage with every feature:** happy path, error cases, edge cases, backward compatibility. **Don't use TDD/red-green unless explicitly asked** — default to implementation + tests together (test-after for small changes). If you think red-green fits, suggest it and wait.
- **Two stores, two test modes:**
  - **Postgres directory (`Fathom.Repo`)** → `Fathom.DataCase` with the Ecto SQL sandbox (async-safe, auto-rollback).
  - **libSQL shards (`Fathom.Shard`/`Fathom.Shards`)** → no sandbox; a shard is a real SQLite file. Use a **unique `shard_id` per test**, drive it through `Fathom.Shards`/`Fathom.ShardExecutor`, and `File.rm` the file (`System.tmp_dir!/fathom_shards/<id>.db`) in `on_exit`. Never let two tests share a shard file. See `test/fathom/shard_executor_test.exs`.
- **Save test output to timestamped logs** so results are readable without rerunning:
  ```bash
  mix test 2>&1 | tee "logs/test-$(date +%Y%m%d-%H%M%S).log"
  # NEVER prune test-failures-*.log — a bare `test-*.log` pattern matches it.
  find logs/ -name "test-*.log" ! -name "test-failures-*.log" -mtime +1 -delete 2>/dev/null
  ls -t logs/test-*.log | head -1 | xargs cat   # read latest
  ```
  **A failure log is evidence, not clutter.** The documented cleanup used to be `-name "test-*.log"`, which deleted the one artifact that survives an unreproducible flake — which is why `lb_apply_test:132` is still unattributed on 2026-08-05.
- **On a failure, DON'T re-run first — name it.** A flake you can't name is a flake you can't fix. In order: read the full output you already have → `mix test --failed` (the **next** run overwrites ExUnit's manifest, so do it now) → `mix test --seed <N>` from the run header, since `--seed` fixes test ORDER and is the only way an order-dependent flake reproduces. **Never pipe a possibly-failing suite run through `tail`/`head`** — the failure block is exactly what gets truncated. `Fathom.FailureCaptureFormatter` (in `test/test_helper.exs`) is the backstop: it writes `logs/test-failures-<ts>.log` with the seed, location, and a paste-ready rerun command, and nothing on a green run. Cost of ignoring this: two 2026-07-25 flakes lost their identity permanently despite ~55 later runs.
- **Every bug fix ships with a regression test in the same commit.** It must (1) **reproduce deterministically** — fail pre-fix, pass post-fix; if you can't make it fail without the fix you haven't isolated the bug — and (2) **pin the violated invariant**, not just the reproduction steps. Comment the symptom so future readers know why it exists. Good targets: races (test the pure function), off-by-one (test the boundary), classifier/dispatcher mismatches (test the classification), lifecycle ordering (test the sequence).
- **ALWAYS run the new test against the unfixed code.** Stash the `lib/` change, run, confirm it fails, restore. **Every time, not when it feels uncertain** — a plausible-looking test that passes both ways is the default outcome, not a rare one. In one session this caught four separate tests that were measuring nothing.
- **When a regression test passes pre-fix, suspect the harness before concluding "unreproducible".** In order:
  1. **The test double can't express the bug.** `Storage.Local` identified a lock by `{owner, epoch}` while S3 fences with `If-Match: lock_etag`, so an entire class of stale-lease bugs was *structurally invisible* to `mix test`. **A gap between a double and the real backend's contract silently exempts every bug in that contract** — closing it is worth more than the one fix that exposed it. (`Fathom.Test.FaultyStorage` now knows the real contract.)
  2. **The fixture doesn't create the state.** A corruption fixture that scribbles a *nearly-empty* b-tree page corrupts only free space and `quick_check` still passes. Assert the precondition inside the test: `assert {:error, _} = verify_integrity(path), "the fixture did not actually corrupt anything"`.
  3. **The environment already has the property.** `config/test.exs` sets `heartbeat_server: false`, so a setup block "forcing" legacy mode is a no-op that makes the test look more specific than it is.
- **A coordinator has TWO liveness modes and the suite defaults to the one production does not use.** `acquire_gen` is fixed at open: non-nil ⇒ **heartbeat** (node heartbeat proves liveness), nil ⇒ **legacy** (per-shard renew PUTs). Different fence, renewal and release paths. `heartbeat_server: false` means a test gets LEGACY unless it starts `Fathom.Shard.Heartbeat` itself, while production and the chaos rig run HEARTBEAT — so a heartbeat-only bug is invisible by default. For anything touching lease/fence/flush/drop, **parameterize over both modes** (`for mode <- [:legacy, :heartbeat]`, see `test/fathom/shard_lease_release_test.exs`) and **assert the mode actually took** (`acquire_gen` non-nil/nil) — a scenario that silently ran legacy twice looks like two-mode coverage and is one.
  - Reaching a specific fence verdict needs the right fixture, and the wrong one passes quietly. Killing `Heartbeat` does **not** produce `:skip` — a DOWN heartbeat degrades to the legacy renew fence, which succeeds. Real `:not_valid` is the process ALIVE with `now + margin >= deadline`: publish a past `mono_deadline_ms` at the **same** generation (a different generation routes to `:revalidate`). Assert the intermediate state (`valid_for_write?(gen) == :not_valid`), not just the outcome.
- **A test that races the idle-stop is reproducible on demand: set `:shard_idle_ms` to 1.** The shape is a test that closes its last connection (arming the idle timer) then asserts on the coordinator — `Shards.flush/1` is the usual one. `Fathom.Shard.terminate/2` deliberately settles a pending flush waiter with `{:error, :coordinator_stopped}`, so the coordinator is RIGHT and the assertion is wrong. **The obvious probe misses it:** a `Process.sleep` before the flush PASSES, because by then `Registry.lookup` returns `[]` and `flush/1` takes its `[] -> :ok` branch — which reads as "cannot reproduce" and gets a real race written off. Fix by pinning `:shard_idle_ms` high for that test, not by widening a timeout. A sweep of the other 3 close-then-flush sites found none, so **re-run this probe on a new failure rather than assuming a pattern**.
- **If a test genuinely cannot discriminate, say so in its moduledoc** — plainly, in the file: "these do NOT reproduce the race, and here is what the fix rests on instead." Keep it as an invariant guard, but never let a non-discriminating test read as a regression test; the next person will trust it.
- **Fathom-specific must-test invariants** (the bugs that bite a sharded multi-tenant system):
  - **Shard isolation.** A query for shard A must *never* resolve to or read shard B's data. Any change to routing (`Fathom.ShardExecutor.shard_from_conn`, `Fathom.Shards` resolve, shard-path construction, `Fathom.Directory`) ships with a cross-shard isolation test.
  - **Migrations are tested both ways.** Forward copy+transform on a seeded `vN-1` shard validating `vN` (row counts / checksums), **and** the revert pointer-flip back.
  - **Cross-version tolerance.** During a rollout the fleet is mixed `vN-1`/`vN`; assert the app reads both.
- **Hot-path verification.** When you change a hot path (cold-open, directory resolve, migration copy, fan-out), add a microbench-style test asserting an order-of-magnitude floor/ceiling (`assert open_us < 50_000`), not an exact latency. Tag it `@tag :bench` so it's excluded from the default suite.

## Benchmarking

**The harness exists.** `mix fathom.bench` measures the hot paths; `scripts/benchmark.sh` runs it prod-compiled and appends one JSON line per run (commit, branch, dirty, host, metrics) to `scripts/perf_history.jsonl`; `scripts/commit_with_bench.sh` is the bench-then-commit gate — it benches the working tree and **refuses the commit if ANY metric regresses ≥20%** vs the parent's **same-host** entry (`Fathom.Bench.Gate`). It is multi-metric because fathom's cost is per-shard open + fan-out, not single-query throughput. Hot-path changes also ship `@tag :bench` floor/ceiling guards (`test/fathom/bench_test.exs`). **Hold the discipline: don't invent numbers — measure, or say "unmeasured."**

**What each metric measures, and what it has read** — the hot-path catalog (cold-open, warming throughput, failover RTO, warm density, fan-out) and the `mix fathom.scale` harness including `--hotspots` — is the appendix of [`docs/benchmark-plan.md`](docs/benchmark-plan.md). Two metrics worth knowing about before you trust them: `cold_open_s3_*` on localhost MinIO measures the S3 *protocol*, not real S3 latency (use `scripts/benchmark_s3_latency.sh`, which injects RTT via toxiproxy), and `copy_keystone_rows_per_s` was renamed from `copy_rows_per_s` on 2026-07-31 — the old series is **not** comparable.

- **Run clean and in prod mode.** Compile in `MIX_ENV=prod` (and the Rust NIF in release, slow) for any bench run; start from a clean DB/data state; don't bench a dev build.
- **Tiered regression response:** **<20%** — ignore (noise). **≥20%** — assume real: rerun once to rule out noise; if confirmed, **revert first**, then reproduce minimally on a branch. Never stack fix attempts on top of a known-regressed commit.
- **Phantom-regression rule.** Declaring an observed regression "phantom" (numbers wrong, not the code) requires (a) the same regression measured by a *second* tool/environment, (b) an explicit explanation of why the original measurement was wrong, and (c) a cooling-off before closing. Don't declare phantom-regressions the same day they're observed — broken instrumentation is not evidence of a fixed regression.
- **A suspiciously GOOD number is a broken measurement until proven otherwise** — and it is the easier one to bank by accident, because the gate says OK and nobody looks. Two from one session: a first-draft `flush_p50_us` read **2 µs** for a full `VACUUM INTO` + upload (the bench's minimal tree has no `WriteCounter`, so `bump/1` rescued to `:ok`, the shard read clean, and `flush_now/1` returned having done nothing); and `fanout_kb_per_shard` reported a **−29% "win"** against a tight 3.77–4.07 historical band on a commit that could not plausibly have caused it (a re-bench of the same HEAD returned 3.77). **Check any metric that moves outside its own historical band in EITHER direction, and re-bench HEAD to leave a corrected baseline** — the real damage from an outlier low is the *next* commit being gated against it, where an ordinary reading becomes a false ≥20% regression and blocks clean work.
- **A new bench metric must assert its own preconditions.** The failure mode is not a wrong number, it's a number for work that never happened. Put the guard inside the harness (`unless dirty?(pid), do: raise "flush bench is measuring nothing"`), so the metric fails loudly instead of reporting a spectacular result.
- **Bench-gate baseline workflow.** `commit_with_bench.sh` compares against the parent's **clean-tree** entry, and committing does not leave one (the pre-commit run is recorded `dirty: true`). So each successive gated commit needs: `git stash push -- lib/ test/` → `scripts/benchmark.sh` → `git stash pop` → gate. Skipping it yields `no baseline for parent <sha>`, and the gate then reaches back to an older, possibly-outlying entry. Same trap the `[skip-bench]` note below describes, reached a different way.
- **`./chaos.sh up` does NOT build.** `cmd_up` is `compose up -d --wait`, so it starts whatever `fathom-chaos:latest` already exists — which can be weeks old. A full rig pass against a stale image is the most expensive way to be wrong, because "it passed" reads as validation. **Before trusting any rig result, prove the image contains the change under test**, in this order:
  1. `./chaos.sh build` first, always, when the rig is validating a code change.
  2. Check the date: `docker image inspect fathom-chaos:latest -f '{{.Created}}'` against `git log -1 --format=%cI`.
  3. Best — assert the fix's **own observable** through the LB before running anything else. A rig validating the ATTACH fix should first confirm `ATTACH DATABASE …` is *refused*; if it succeeds, stop, the binary is old. (Learned the hard way on 2026-08-02: `smoke` and `deploy` both passed against a build that predated every fix under test, and the tell was that ATTACH still worked.)
- **Docker is machine-global, and this machine runs more than one fathom stack.** A sibling checkout (`djathom`) keeps its own compose project up for days. Containers, networks and host ports are namespaced by compose project, so there is no cross-talk — but confirm rather than assume with `docker inspect -f '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' <container>`, which names the checkout that launched it. They do share the colima VM's CPU, so **rig timings are contended even when pass/fail is trustworthy** — treat rig latency as relative, per the same-topology rule.
- **`./chaos.sh down` before benching** — the rig's 8 containers compete with `scripts/benchmark.sh` for the same cores, and it does not shut itself down after a verification run. `docker ps -q | wc -l` should be **0**.
- **When two unrelated metrics move TOGETHER in one run, it is the machine, not the code.** The single best tell, and cheaper than any reasoning about mechanism. Measured 2026-08-05 on a change that added `:os_mon`: one gate run blocked on `dir_resolve_p50_us` +46.9% AND `hrana_open_rt_us` +27.6%, the latter the highest value ever recorded. Two more working-tree runs read 357/359 and 130/132 against a parent of 355/128 — i.e. the pair moved together in exactly one run and nowhere else. A code change that genuinely slowed a Postgres resolve *and* a Hrana stream open by different amounts is far less likely than one contended run. `dir_resolve_p50_us` is also visibly **bimodal** (clusters near ~128 and ~185), so its "regression" is often just which mode the baseline landed in.
- **`cold_open_p99_us` is FAT-TAILED and will false-block you ~1 run in 3.** Measured 2026-08-04 on a change that could not possibly touch it: five gate/bench runs of the same working tree read 5053, 5367, 2076, 2209 against parent runs of 2786, 2039, 2211, 2362 — two isolated samples at ~2.4× and the rest indistinguishable. The p50 never moved. **Diagnose it with a same-harness A/B, not with a mechanism argument.** Bench parent and working tree the same way, back to back; if they agree, the block was a tail sample. Three wrong explanations were talked through first — "the rig was up" (it was down for the second block), "the gate uses a different harness" (`commit_with_bench.sh` just calls `benchmark.sh`), and "the code can't reach it" (true here, but that reasoning had already been wrong twice that day). The number settles it; the story does not. Do not reach for `--skip`: re-establishing a clean parent baseline as the last history line and re-running the gate let it pass on its own terms.
- **Never change the shared bench `setup/1` to serve one metric.** Starting `WriteCounter`/`FlushWatermark` there so the flush metric would work changed what `fanout_kb_per_shard` measures (every open shard gains ETS rows) — the gate correctly blocked at **+46.5%**. That is a *harness topology* change, and per the same-topology rule it invalidates the historical series. Scope new dependencies to the metric that needs them.

## Gates

A "gate" is a check that must pass *before* a commit lands — not after.

- **GitHub Actions CI runs again** (2026-07-29). It was off while the repo was private — the
  account is out of Actions minutes for private repos, and a run would fail in ~12 s having
  executed zero steps. Going public restored free minutes. The first real run immediately caught
  a bug the outage had been hiding: the workflow hardcoded a developer's local username as the
  Postgres role, so `config/test.exs` (which resolves `PGUSER || USER || "postgres"`) asked for
  role `runner` on a runner. `PGUSER`/`PGHOST` are now pinned in the job env.
  **CI is the second opinion, not the first** — `mix precommit` is still the gate that has to pass
  before a commit lands. Disable with
  `gh api -X PUT repos/cwisecarver/fathom/actions/permissions -F enabled=false`.
- **`mix precommit` is the commit gate** (defined in `mix.exs`): `compile --warnings-as-errors`, `deps.unlock --unused`, `format`, `dialyzer`, `test`. **Never commit if it fails.** Run it when you're done with all changes and fix everything it surfaces.
- **Typing gate — Dialyzer (added 2026-08-14).** Runs inside `precommit` after `format` and before `test` (deterministic, reuses the fresh beams, ~2 s warm — much cheaper than the suite, so failing fast there is the right order), and in CI across OTP 27/28/29 so a typing difference between VM versions surfaces there rather than on one developer's machine. Manual run is **`MIX_ENV=dev mix dialyzer`**. **The env is the SCOPE, not a detail**: `elixirc_paths/1` compiles `test/support` only in `:test`, so `:dev` analyzes `lib/` alone — which is the plan's stated scope, and which keeps the benchmark drivers' `mint_web_socket` opaque-type cascade (21 findings from one dependency issue, see `mix.exs`) out of the gate. Inside `precommit` it is `cmd env MIX_ENV=dev mix dialyzer`, and the `env` is required — `mix cmd` does not use a shell, so a bare `VAR=value` prefix is taken as the executable and dies with `:enoent`. **First run after `mix deps.get` or a dependency bump pays a partial PLT update (minutes, once); a first-ever build is ~10–20 min.** PLTs live in `priv/plts` (gitignored) so `rm -rf _build` doesn't discard them, and CI caches them keyed on `mix.lock` + OTP version. Suppressions go in `.dialyzer_ignore.exs`, which documents the only two legitimate reasons to be in it and requires a comment per entry; `list_unused_filters: true` fails the run on a filter that stopped matching. **The gate was verified to bite** before being trusted (a deliberately wrong return type on `Shards.migrate_on_touch_mode/0` exits 1 at the dialyzer step without reaching the tests) — see § Typing for the style rules and what it actually caught.
- **Migration gate.** A schema migration must not ship without: (a) a forward copy+transform test, (b) a revert-flip test, (c) a cross-version-tolerance check. A migration that can't be reverted by pointer-flip within the retention window, or that the running app can't tolerate mid-rollout, is not done.
- **Shard-isolation gate.** Any change to shard routing (`Fathom.ShardExecutor.shard_from_conn`, `Fathom.Shards`, shard-path construction, or the planned `Fathom.Directory` resolve) must have a test proving shard A never resolves to shard B. Treat a cross-tenant leak as a release blocker, not a finding.
- **NIF-contract guard (when `fathom_native` lands).** Elixir is dynamically typed, so a NIF signature change (arity, **return shape**, param types) silently breaks Elixir callers at *runtime* — the Rust compiler can't catch it. After any NIF signature change, run the integration tests that exercise it and `grep -rn "Fathom.Native.<fn>"` for every caller. Don't rely on the unit suite to catch a contract break.
- **Bench-then-commit gate** (built). Any change touching a hot path (shard routing/open, directory resolve, migration copy, the shard coordinator) goes through `scripts/commit_with_bench.sh -m "<msg>"`: it benches the working tree and refuses the commit on a ≥20% regression in any metric vs the parent's entry in `scripts/perf_history.jsonl` (override `PERF_REGRESS_BLOCK`). Pure docs/test/comment-only changes skip it — `git commit` directly with a `[skip-bench]` token, or `--skip`. See Benchmarking and `docs/benchmark-plan.md`.

## Typing

Dialyzer-enforced `@spec` coverage. The gate is in § Gates; this is how to write for it.

**The defect it actually finds, over and over.** Fathom had 309 `@spec`s and nothing had ever
verified one. The 2026-08-14 baseline was 115 findings, and the dominant shape was not a wrong
type — it was a **stale** one: someone adds a field or a return case to the code and to every
caller, and never to the declaration. Seven instances, all on paths that matter, all silent:

| stale declaration | what it omitted |
|---|---|
| `Recovery.position` ("the same shape as `FollowerLog.t()`") | `torn` — the field deciding whether a replica may be promoted at all |
| `Storage.lease()` (a CLOSED three-key map) | `lock_etag` — the fencing token release is conditional on |
| `Storage.pull/2`'s `@spec` (its `@callback` was right) | `{:absent, _}` |
| `pull_snapshot/3`'s `@callback` AND `@spec` | `{:absent, _}` |
| `Migrator.status/0` | `review_blocks` — a published control-plane field |
| `Copy.migrate/4` | that statements are `{sql, args}` pairs, not strings |

None broke anything at runtime. Three had a worse consequence than a bad doc: dialyzer concluded
whole paths were **unreachable** — A2 cross-fleet promotion and its mid-flight object re-check read
as dead code because `Recovery.position` lacked one field. **When a type and its callers disagree,
suspect the type**, and prefer aliasing the owning type (`@type position :: FollowerLog.t()`) over
restating its shape, so the drift cannot recur.

**Style rules.**

1. **Skip behaviour callback implementations** — GenServer/LiveView/Plug/Oban/Mix.Task/
   `Filo.Executor`/`Fathom.Shard.Storage` impls. The contract lives once, on the `@callback`.
2. **Spec the client API of GenServers**, not the server callbacks — but know what that buys.
   **A `@spec` on a GenServer client wrapper is DOCUMENTATION ONLY; dialyzer cannot check it.**
   Measured 2026-08-14: `@spec dirty?(pid()) :: :definitely_not_what_it_returns` on
   `Fathom.Shard.dirty?/1` — whose body is one `GenServer.call/3` — passes the gate, because
   `GenServer.call/3` returns `term()` and nothing contradicts it. The discriminating pair is
   `Shards.migrate_on_touch_mode/0`, a pure config read, where the same deliberate break IS caught
   as `invalid_contract`.
   **The rule this generalizes to, and the one worth planning around: a spec is CHECKED only where
   dialyzer can compute a success typing from the body that contradicts it.** Pure functions, data
   transformations and anything whose shape flows between modules are checked. Wrappers over
   `GenServer.call`, `:ets`, dynamic dispatch (`backend().pull(...)`) and NIFs are not. Write them
   anyway for legibility — but do not count them as coverage, and spend effort on the data
   contracts first, since every defect found on 2026-08-14 lived in one.
3. **Reuse owned types**: `Fathom.ShardId.t()`, `Storage.lease()`, `Ecto.Changeset.t()`. Never
   re-inline a shape that has a name — that is how the table above happened.
4. **Ecto schemas get `@type t :: %__MODULE__{}`.** Three were missing it while six specs named it.
5. **A function that always raises is `no_return()`**, not an ignore entry (`mix fathom.token`).
6. **No defensive typing**: no guards added to satisfy a spec, no `term()`/`any()` escape hatches.
   State the bound you can prove — `Migrator.status/0`'s `eta_seconds` is `integer()`, not
   `non_neg_integer()`, because that is what is provable.

**Two things learned the hard way, worth not rediscovering.**

- **Dialyzer uses SUCCESS TYPINGS, not contracts, when analyzing callers.** So a `@spec` on a
  helper — however accurate, including a polymorphic `when result: var` — cannot widen or narrow
  what its callers see. Measured twice on 2026-08-14 (`Bench.with_wire/3`, `HranaClient.await_upgrade/2`).
  An accurate spec on a *public* function still helps: fixing `HranaClient.execute/3` cleared 21
  downstream findings at once.
- **A dependency's `@opaque` type can make a whole subsystem read as dead.** `Mint.WebSocket.t()`
  is opaque, so dialyzer cannot see the `{:ok, conn, t()}` branch of `new/4` and decides the
  handshake never succeeds. Confirm that class in ISOLATION with a probe module that does nothing
  but call the dependency — it separates "our code confuses dialyzer" from "the dependency's
  typings are wrong" in about a minute.

## Principles

- **Simplicity first.** Minimal code touched. Find root causes, not workarounds. Prefer elegance for non-trivial changes; skip it for obvious fixes.
- **Fix bugs autonomously** — diagnose from logs/errors/tests, then resolve. No hand-holding.
- **Never hand-roll routing, namespace, or SQL string surgery.** Don't build shard paths / namespace names / directory keys by ad-hoc string concatenation scattered across the codebase — route every shard resolution through `Fathom.Shards` (and request → shard through `Fathom.ShardExecutor.shard_from_conn`; eventually `Fathom.Directory`) so isolation and cutover logic live in one place. Don't hand-roll SQL for the libSQL shards either — always bind with parameterized queries (`Fathom.Shard` passes args through to `exqlite`); never interpolate values into SQL (injection and quoting edge cases bite the same way Postgres's do).
- **Multi-tenant safety is non-negotiable.** Every shard query carries its `shard_id` explicitly through `Fathom.Shards`/`Fathom.Shard` — never an implicit/ambient "current shard," and never the Postgres `Fathom.Repo`. When in doubt about which shard a code path operates on, make it explicit.

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
control-plane / cluster rows in the § Project map.

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

The migration machinery in this diagram is **built** — see the Migration engine row in the § Project map, and [`docs/migration.md`](docs/migration.md). The version stamp lives in **three places**: `django_migrations` in each shard (Django's own migration ledger — the truth), `PRAGMA user_version` (O(1) gate), `shards.schema_version` in Postgres (laggard queries without opening shards). Still aspirational: the **cached, PubSub-invalidated resolve on the request path** (routing is Host-based today).

---

# Framework guidelines

Trimmed from the full `phx.new` usage-rules on 2026-08-22 to the parts fathom actually uses.
**Fathom's web surface is 23 files of 144** — an admin dashboard on :4000, not a product UI. The
data plane speaks Hrana, not HTML. Sections on LiveView streams (used nowhere), rich form
handling (one file), and "world-class UI design" were dropped as inapplicable. If you add a real
user-facing surface, pull the full rules back from a fresh `phx.new` project.

## Elixir

- Lists **do not support index access** — `mylist[i]` is invalid. Use `Enum.at/2`, pattern matching, or `List`.
- Variables are immutable but rebindable, so **bind the result of a block expression**; you cannot rebind inside it:

      # INVALID — the rebind never escapes the `if`
      if connected?(socket) do socket = assign(socket, :val, val) end
      # VALID
      socket = if connected?(socket), do: assign(socket, :val, val), else: socket

- **Never nest multiple modules in one file** — cyclic dependencies and compile errors.
- **Never use map access syntax on structs** (`changeset[:field]`) — they don't implement Access. Use `struct.field` or `Ecto.Changeset.get_field/2`.
- `if/else` only — Elixir has **no `else if`/`elsif`**. Use `cond` or `case`.
- Don't use `String.to_atom/1` on user input (memory leak / atom exhaustion).
- Predicate names end in `?`, never start with `is_` (reserve `is_` for guards).
- Date/time: the stdlib (`Date`, `Time`, `DateTime`, `Calendar`) has everything. Don't add a dependency.
- `DynamicSupervisor`/`Registry` need `name:` in the child spec.
- `Task.async_stream/3` for concurrent enumeration with back-pressure — but see the fathom-specific warning: `timeout: :infinity` there let one wedged tenant hang a whole 1024-tenant sweep. Give it a real deadline.

## Mix

- Read `mix help <task>` before using a task.
- `mix test path/to/test.exs` for one file; `mix test --failed` to rerun failures.
- `mix deps.clean --all` is almost never needed. Avoid it.

## Phoenix

- Use `:req` (`Req`) for HTTP. **Avoid** `:httpoison`, `:tesla`, `:httpc`.
- A router `scope` block's alias prefixes every route in it — **never** add your own alias for route modules.
- `Phoenix.View` no longer exists. Don't use it.
- Always begin a LiveView template with `<Layouts.app flash={@flash} ...>`. `MyAppWeb.Layouts` is already aliased.
- `<.flash_group>` is **forbidden** outside `layouts.ex`.
- Use the imported `<.icon name="hero-x-mark" class="w-5 h-5"/>` for icons — **never** `Heroicons` modules.
- Use the imported `<.input>` from `core_components.ex` for form inputs. Overriding its `class` inherits **no** defaults.
- Missing `current_scope` assign means routes are in the wrong `live_session` or it wasn't passed to `<Layouts.app>`.
- LiveViews are named `AppWeb.WeatherLive`. Prefer them over `LiveComponent`.
- **Never** `live_redirect`/`live_patch` — use `<.link navigate={...}>` / `<.link patch={...}>`, and `push_navigate`/`push_patch`.

## HEEx

- `~H` or `.html.heex` only — never `~E`.
- **`{...}` inside tag attributes and tag bodies; `<%= ... %>` only for block constructs inside a tag body.** Interpolating into an attribute with `<%= %>` is a syntax error:

      <div id={@id}>
        {@my_assign}
        <%= if @cond do %>{@other}<% end %>
      </div>

- Class attrs take a **list** — always use it for multiple/conditional values, and wrap `if` in parens:

      <a class={["px-2", @flag && "py-5", if(@cond, do: "border-red-500", else: "border-blue-100")]}>

- Comments are `<%!-- ... --%>`. Comprehensions are `<%= for x <- @coll do %>`, never `<% Enum.each %>`.
- To show literal `{`/`}` (a code sample), annotate the parent tag `phx-no-curly-interpolation`.
- Give forms and key elements unique DOM ids — tests select on them.

## LiveView JS interop

Fathom uses exactly one hook (`phx-hook="Chart"` in `admin_overview_live.ex`, defined in
`assets/js/app.js`). Only `app.js` and `app.css` bundles are supported — no external `src`/`href`
in layouts; import vendor deps into the bundles.

- A hook that manages its own DOM **must** also set `phx-update="ignore"`, and needs a unique DOM id.
- **Never** write a raw `<script>` in HEEx. For an inline script use a colocated hook, whose name **must** start with `.`:

      <input id="phone" phx-hook=".PhoneNumber" />
      <script :type={Phoenix.LiveView.ColocatedHook} name=".PhoneNumber">
        export default { mounted() { /* this.el ... */ } }
      </script>

- External hooks live in `assets/js/` and are passed to the `LiveSocket` constructor via `hooks: {MyHook}`.
- Server → client: **rebind or return** the socket from `push_event/3`; the client reads it with `this.handleEvent("name", cb)`.
- Client → server: `this.pushEvent("name", payload, reply => ...)`, handled by `{:reply, %{...}, socket}`.

## Forms

- Always assign a form via `to_form/2` in the LiveView and drive the template from it. **Never** pass a changeset to `<.form for={...}>` or access `@changeset[:field]` in a template — it will error.

      # LiveView
      assign(socket, form: to_form(Chat.change_message(msg)))
      # template
      <.form for={@form} id="msg-form" phx-change="validate" phx-submit="save">
        <.input field={@form[:field]} type="text" />
      </.form>

- `to_form(params)` expects **string** keys; `to_form(params, as: :user)` nests them under `"user"`.
- Use `Phoenix.Component.form/1` and `inputs_for/1` — **never** `Phoenix.HTML.form_for`/`inputs_for`.

## Ecto

- **Always preload** associations a template will touch.
- Schema fields are `:string` even for `:text` columns.
- `validate_number/2` has **no `:allow_nil`** option — validations already skip missing/nil changes.
- Programmatically-set fields (`user_id`) must **never** be in `cast/3` — set them explicitly on the struct.
- Access changeset fields with `Ecto.Changeset.get_field/2`.
- Generate migrations with `mix ecto.gen.migration name_with_underscores` so timestamps are right.
- Remember `import Ecto.Query` in `seeds.exs`.

## CSS

Tailwind v4 — **no `tailwind.config.js`**. Keep this import syntax in `app.css`:

    @import "tailwindcss" source(none);
    @source "../css";
    @source "../js";
    @source "../../lib/fathom_web";

Never use `@apply`. Write your own components rather than pulling in daisyUI.

## Tests (framework-level; the discipline is in § Testing above)

- **Always `start_supervised!/1`** to start processes — it guarantees cleanup.
- **Avoid `Process.sleep/1` and `Process.alive?/1`.** To wait for a process to end, `Process.monitor/1` and `assert_receive {:DOWN, ^ref, :process, ^pid, :normal}`. To sync before the next call, `_ = :sys.get_state(pid)`.
- LiveView: `Phoenix.LiveViewTest` + `LazyHTML`; drive forms with `render_submit/2` / `render_change/2`.
- **Never assert against raw HTML** — use `element/2` / `has_element?/2` against the ids you added. Test outcomes, not your mental model of the markup.
