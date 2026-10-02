# AGENTS.md — Fathom

## Project

Fathom is a multi-tenant sharded data platform built on Phoenix: one SQLite database per shard (eventually millions), served to unchanged libSQL clients (e.g. an unchanged Django app via `django-libsql`) over the network.

**Where the detail lives.** This section is a **map** — what exists, one line each. It is deliberately
short because AGENTS.md is loaded into every session.

- [`docs/README.md`](docs/README.md) — the index: per-subsystem how-it-works stories, design plans, benchmark plans, runbooks, run reports.
- [`docs/component-notes.md`](docs/component-notes.md) — the long-form record for every component below: the finding that motivated it, the fix that was tried and was wrong, the measurement that settled it, the trap that will bite you. **Read the entry before changing a component.**
- [`docs/reviews/`](docs/reviews/) — the measured run reports (benchmarks, chaos runs, density
  sweeps). **Expert-panel audits are NOT here and are never committed** — `/expert-audit` writes
  them to the gitignored `audits/` directory, along with the `.progress.md` log `/iterate` keeps.
  They are working artifacts full of provisional findings; only the fixes they drive belong in the
  repo. See the `/audits/` entry in `.gitignore` for the reasoning.
  - **Cite a review finding by DATE, not a bare `#N`.** Because the reports are gitignored, an
    outside reader can't resolve a reference — and `#N` collides across review series (`#18` appears
    in several unrelated ones), so a bare number is ambiguous even internally. New in-code
    references must carry the review date: `expert review 2026-07-14 #18`, not `review #18`. The
    ~386 legacy bare-`#N` comments already in `lib/` are grandfathered — a mechanical sweep to
    date them is not worth the risk (arch review 2026-09-12 #7); just don't add more.

### What exists today (the working slice)

| Component | Modules | Deeper |
|---|---|---|
| **Shard data path** — connection per Hrana stream, per-shard coordinator owning the file lifecycle (pull on cold start, checkpoint+flush+drop when idle), write-gated by a `dirty` flag | `Fathom.Shard` (the coordinator — the lease/fence/flush/drop state machine, deliberately the irreducible core), `Fathom.Shard.Connection` (exqlite), `Fathom.Shards`, plus the low-coupling pieces extracted 2026-09-13/14: `Fathom.Shard.{Position,Provenance,Integrity,Fork,Materializer,PromoteOnOpen}`. The fence envelope that stays in `Fathom.Shard` (lease validity ↔ flush fence ↔ dirty ↔ drop ↔ heartbeat-lapse) shares one consistency boundary — splitting it reintroduces the split-brain the fence prevents, so leave it whole (rationale: component-notes, and the `refactor(shard): extract …` commits) | [data-path.md](docs/data-path.md), [durability.md](docs/durability.md) |
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
| **Live WAL replication (A2)** — quorum-replicated WAL frames over A2's own socket protocol + peer-recovery on failover. **On by default in prod** (ship + receive + ordinal-wire + frame-auth all default on; off in dev/test), so a prod node without `REPLICATION_FOLLOWERS`+`REPLICATION_BIND_IP`+a frame-auth key **fails to boot**. Ship/receive/wire/auth are **separate** gates each forceable via env; the staged wire-rollout order only matters for a **future** rolling upgrade across a frame-format change (fathom is greenfield) | `Fathom.Shard.Replication.*` | [a2-quorum-replication.md](docs/a2-quorum-replication.md) |
| **Scheduled snapshots + GFS retention** — hourly Oban crons, both **off by default**; retention only ever deletes what the scheduler created | `Fathom.Snapshots{,.ScheduleJob,.RetentionJob}` | [durability.md](docs/durability.md) |
| **Restore drill** — verifies the stored object *and* the recovery procedure (fork → cold-open → row-count compare) | `Fathom.RestoreDrillJob` | — |
| **Disk observability + replica back-pressure** — `:disksup` on the existing poller (`data` and `replica` dirs); an A2 follower refuses a new seed under a free-space floor | `Fathom.Admin.Measurements`, `Fathom.Shard.Replication.Follower` | — |
| **Django UDF compatibility** — a Rust **loadable SQLite extension** supplying the 35 of Django's 54 backend functions SQLite lacks, loaded per connection (enable → load → **disable**) | `native/fathom_udf`, `Fathom.Shard.Extension` | [quickstart-django.md](docs/quickstart-django.md) |
| **Bench + scale harnesses** | `mix fathom.bench`, `mix fathom.scale`, `mix fathom.rpo` | [benchmark-plan.md](docs/benchmark-plan.md) |

**Not in the code — don't assume these exist:** per-shard follower sets, zone-aware placement,
rendezvous/bounded-load hashing (C1), multi-region affinity (C2), a cached PubSub-invalidated
directory resolve on the request path (routing is Host-based today), and a `fathom_native` Rustler
**NIF** (`native/` holds a loadable *extension*, which is not the same thing — twice now the answer
to "we need a NIF" was the extension already there). Also absent: `Fathom.ShardExec`,
`Fathom.Retirement` — old names, don't grep for them. **Warm standby (`Fathom.Shard.WarmFollower`)
was removed 2026-09-14**, superseded by A2 — don't re-add it; if failover RTO ever needs a read
cache, fold it into the A2 follower's read path ([warm-standby.md](docs/warm-standby.md)).

### Loud warnings

The things that cost a day if you don't know them. Full stories in
[`docs/component-notes.md`](docs/component-notes.md).

- **Every capacity and throughput number in the docs was measured with A2 replication OFF, and they do not hold with it on.** Never quote a replication-off number for a replicating fleet — **prod runs replication ON by default** (2026-08-29), so the "measured OFF" numbers are the dev/test/bench figures, not the deployed fleet's. Replication-ON on the chaos rig (2026-10-02, `TPC_NET=container TPC_DRIVER=elixir ./chaos.sh tpc-fleet N`, 0 tenants shed in every arm): **512 tenants 3,201 txn/s / 29 errors; 1024 tenants 2,700 txn/s / 1.1% errors** at a 30 s flush interval. At 1024 the node byte budget (`:overloaded`) is the binding limit.
- **`SHARD_FLUSH_INTERVAL_MS` is not a throughput setting.** Each flush checkpoints the WAL, and every checkpoint costs replication work (the follower absorbs its WAL into its `.db`, and the next push starts a new generation). Same rig, 5 s vs 30 s: 512 tenants 2,387 vs 3,201 txn/s (0.75% vs 0.01% errors); 1024 tenants 1,913 vs 2,700 txn/s (8.2% vs 1.1%). Set it realistically or you are measuring checkpoint churn. (The older "the next push ships the *whole* WAL" mechanism was removed by b184748; until 2026-10-02 a lost-advance bug in `Session` added an `:offset_mismatch` round to most commits after a checkpoint, aff8f45.)
- **Rig runs need a clock check.** A colima clock step makes every node log `clock lags the store` and invalidates the run, roughly 1 in 3 runs even right after `colima restart`. Grep each arm's node logs before reading its numbers.
- **`sqlite3_wal_hook` and `wal_autocheckpoint` are the same slot.** A hook that merely observes silently disables checkpointing on every tenant connection and grows the WAL without bound.
- **The replication port is unauthenticated** — `REPLICATION_BIND_IP` is a security control, not a convenience.
- **Extension loading is arbitrary code execution on a multi-tenant engine.** The sequence is enable → load ours → **disable**, on every handle including `:ro`; a failure to re-disable fails the open.
- **A template shard is a fleet-wide poisoning vector.** Never set one in prod without auth on it, and never make `:default_shard` equal it (a boot guard refuses that config).

## Execution style

These mirror the maintainer's global agent settings; they are restated here because non-Claude
agents (e.g. Codex) read only this file.

- **Sequenced directives** ("do X then Y", "review then execute") — execute them in order without re-confirming. If something is genuinely ambiguous, name your default and proceed; stop only when the ambiguity risks irreversible harm.
- **"Go ahead" / "continue" / "proceed"** continues the *most recently scoped* task. It does not escalate a review into implementation or move to the next phase.
- **A work queue is one task.** A findings list, a migration sweep, a rollout, a batch of files: the go-ahead was given when the queue was accepted, so work every item to a terminal status (or until a stop condition below fires) instead of ending a turn with "want me to do the next one?" — to someone who left the run unattended, that reads as completion.
  - An item you can't decide alone gets **parked**: write down the decision and its options, ship any part that stands independently, move on.
  - Ask the question in prose and keep working; a present user's answer arrives with your next tool result. Avoid `AskUserQuestion` inside a queue run — it has no timeout, so it stalls the whole queue.
- **Locating things:** one targeted Read/Grep/Glob; widen the query if it misses.
- **State results**, not intentions.

## Build

```bash
mix setup          # deps.get + ecto.setup (Postgres) + assets
mix compile        # build (mix precommit uses --warnings-as-errors)
mix test           # creates+migrates the test Postgres DB, then runs tests
mix test test/fathom_web/controllers/page_controller_test.exs:7  # single test
mix test --failed  # rerun last failures
mix format         # format
mix precommit      # the gate: compile --warnings-as-errors, deps.unlock --unused, format, dialyzer, test
iex -S mix phx.server   # start app (dashboard :4000)
```

- **`--warnings-as-errors` enforces placement, not just correctness.** These are invisible until you build, so get them right on the first edit:
  - A `@module_attribute` must be **defined above every use**. Adding one next to the function that reads it fails when that function sits earlier in the file.
  - **Clauses of the same name/arity must be contiguous.** Inserting a new `handle_info/2` clause after an unrelated function splits the group. A private helper defined *between* two clauses splits it too — put the helper after the whole group.
  - A new clause must go **above the catch-all**, not merely near it. `def handle_info({:DOWN, ref, …}, %{renew_task: …})` placed after the generic `{:DOWN, …}` clause is unreachable, and the compiler says so.
  - When scripting a bulk edit with `python3`, a `str.replace` with no count replaces **every** occurrence — a duplicated function definition is the usual result. Pass a count, then `grep -c` to confirm.
- **The shell is zsh.** The traps that recur:
  - Backticks and `$(...)` run command substitution even inside double quotes, so an identifier in backticks inside `git commit -m "..."` gets executed and mangles the message. Use `git commit -F <file>` for any non-trivial message.
  - `${var#pat}` / `${var%pat}` treat `( [ ] # ? *` as pattern metacharacters (`${m#](}` dies with `bad pattern`). Do bracket/paren munging with `grep -oE` or a `python3 - <<'PY'` heredoc instead.
  - Unquoted globs (`*`, `?`, `[...]`) and `{a,b}` expand — quote them when you mean literals.
  - A pipeline's exit status is the last command's, so `mix precommit 2>&1 | tee log` reports `tee`'s 0 even when the gate failed. Redirect to a log and check `$?`, or read the log's final result line.
  - For text pipelines prefer `grep` or a `python3` heredoc over `sed`/`awk`: BSD vs GNU flag differences make them fragile here, and file reads/edits go through Read/Grep/Edit anyway.
- **The Rust extension** (`native/fathom_udf`) builds via the `:fathom_udf` Mix compiler on `mix compile`, and is skipped (not failed) when `cargo` is absent. Rust tests: `cd native/fathom_udf && cargo test`.

## Workflow

Plan before non-trivial work (3+ steps or an architectural decision), and re-plan when something goes wrong. Use subagents for research, exploration and parallel work — one task each, on a model matched to the subtask's difficulty rather than the parent's by default.

**Implementation cycle:** implement → compile → test → (bench if hot path) → `mix precommit` → commit locally → stop. Test after every change and fix failures before moving on. Commit in logical units matching the plan phases, directly on `main` (no feature branches unless the user asks).

- **Pushing requires the user's explicit approval, every time.** The repo has been public since 2026-07-29: `main` is the project's face and its history can't safely be rewritten once cloned, so "should this be visible now?" is the user's call — not something a green suite answers. When a batch looks worth pushing, say so and wait for a yes.
  - Local commits don't wait: they are the checkpoint that makes a bad step cheap to undo. If unpushed work has piled up for days, raise it — a night's work was once lost to local-only commits.
- **Never commit with compiler warnings, build errors, or failing tests**, including ones you didn't introduce. `mix precommit` is the gate (see Gates).
- **Read and edit files with Read (offset/limit), Grep, and Edit/Write**, not `sed`/`awk`/`head`/`tail`/`echo`. Shell text tools are for what those can't do; piping command output is fine.
- Track plans in `tasks/todo.md`; record corrections and lessons in `tasks/lessons.md`.

### Stop-after-2-failures rule

If a script or command (test/build/migration/sweep) fails **twice with a similar error**, stop: print the exact command, the exact error, and a one-paragraph root-cause hypothesis, then wait. Repeated identical failures are usually infrastructure (missing dep, DB not created, port conflict, SQLite file lock), which a third attempt won't fix but a diagnosis will. The same applies to scope blowups — a refactor producing >50 compile errors, or work running >60 min past estimate. Don't paper over a flaky test or build failure with sleeps, retries, or longer timeouts; that hides the evidence.

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
  log="logs/test-$(date +%Y%m%d-%H%M%S).log"; mix test >"$log" 2>&1; echo "exit $?"
  # Keep test-failures-*.log: a bare `test-*.log` prune pattern also matches it.
  find logs/ -name "test-*.log" ! -name "test-failures-*.log" -mtime +1 -delete 2>/dev/null
  ```
  Then Read the newest log. A failure log is evidence: it is often the only record of an unreproducible flake (`lb_apply_test:132` lost its attribution to an over-broad prune).
- **On a failure, name it before re-running.** A flake you can't name is a flake you can't fix. In order: read the full output you already have → `mix test --failed` immediately (the next run overwrites ExUnit's manifest) → `mix test --seed <N>` from the run header, since only the seed reproduces an order-dependent flake. Don't pipe a possibly-failing run through `tail`/`head` — the failure block is what gets truncated. `Fathom.FailureCaptureFormatter` (`test/test_helper.exs`) is the backstop: on failure it writes `logs/test-failures-<ts>.log` with the seed, location, and a rerun command.
- **Every bug fix ships with a regression test in the same commit.** It must (1) **reproduce deterministically** — fail pre-fix, pass post-fix; if you can't make it fail without the fix you haven't isolated the bug — and (2) **pin the violated invariant**, not just the reproduction steps. Comment the symptom so future readers know why it exists. Good targets: races (test the pure function), off-by-one (test the boundary), classifier/dispatcher mismatches (test the classification), lifecycle ordering (test the sequence).
- **Run every new regression test against the unfixed code** — back out the `lib/` change, confirm the test fails, restore. Do it every time, not only when unsure: on this codebase a plausible test passes both ways about a third of the time (one session caught four tests measuring nothing). Back the change out precisely (`git stash push -- lib/<file>` and check `git status`), not with `git checkout`, which discards every other edit in the file.
- **When a regression test passes pre-fix, suspect the harness before concluding "unreproducible".** In order:
  1. **The test double can't express the bug.** `Storage.Local` identified a lock by `{owner, epoch}` while S3 fences with `If-Match: lock_etag`, so an entire class of stale-lease bugs was *structurally invisible* to `mix test`. **A gap between a double and the real backend's contract silently exempts every bug in that contract** — closing it is worth more than the one fix that exposed it. (`Fathom.Test.FaultyStorage` now knows the real contract.)
  2. **The fixture doesn't create the state.** A corruption fixture that scribbles a *nearly-empty* b-tree page corrupts only free space and `quick_check` still passes. Assert the precondition inside the test: `assert {:error, _} = verify_integrity(path), "the fixture did not actually corrupt anything"`.
  3. **The environment already has the property.** `config/test.exs` sets `heartbeat_server: false`, so a setup block "forcing" legacy mode is a no-op that makes the test look more specific than it is.
- **A coordinator has TWO liveness modes and the suite defaults to the one production does not use.** `acquire_gen` is fixed at open: non-nil ⇒ **heartbeat** (node heartbeat proves liveness), nil ⇒ **legacy** (per-shard renew PUTs). Different fence, renewal and release paths. `heartbeat_server: false` means a test gets LEGACY unless it starts `Fathom.Shard.Heartbeat` itself, while production and the chaos rig run HEARTBEAT — so a heartbeat-only bug is invisible by default. For anything touching lease/fence/flush/drop, **parameterize over both modes** (`for mode <- [:legacy, :heartbeat]`, see `test/fathom/shard_lease_release_test.exs`) and **assert the mode actually took** (`acquire_gen` non-nil/nil) — a scenario that silently ran legacy twice looks like two-mode coverage and is one.
  - Reaching a specific fence verdict needs the right fixture, and the wrong one passes quietly. Killing `Heartbeat` does **not** produce `:skip` — a DOWN heartbeat degrades to the legacy renew fence, which succeeds. Real `:not_valid` is the process ALIVE with `now + margin >= deadline`: publish a past `mono_deadline_ms` at the **same** generation (a different generation routes to `:revalidate`). Assert the intermediate state (`valid_for_write?(gen) == :not_valid`), not just the outcome.
- **A test that races the idle-stop reproduces on demand with `:shard_idle_ms` set to 1.** The shape: a test closes its last connection (arming the idle timer), then asserts on the coordinator, usually via `Shards.flush/1`. `Fathom.Shard.terminate/2` deliberately answers a pending flush with `{:error, :coordinator_stopped}`, so the coordinator is right and the assertion is wrong. A `Process.sleep` probe misleadingly passes (by then `flush/1` hits its `[] -> :ok` branch). Fix by pinning `:shard_idle_ms` high in that test, not by widening a timeout.
- **If a test genuinely cannot discriminate, say so in its moduledoc** — plainly, in the file: "these do NOT reproduce the race, and here is what the fix rests on instead." Keep it as an invariant guard, but never let a non-discriminating test read as a regression test; the next person will trust it.
- **Fathom-specific must-test invariants** (the bugs that bite a sharded multi-tenant system):
  - **Shard isolation.** A query for shard A must *never* resolve to or read shard B's data. Any change to routing (`Fathom.ShardExecutor.shard_from_conn`, `Fathom.Shards` resolve, shard-path construction, `Fathom.Directory`) ships with a cross-shard isolation test.
  - **Migrations are tested both ways.** Forward copy+transform on a seeded `vN-1` shard validating `vN` (row counts / checksums), **and** the revert pointer-flip back.
  - **Cross-version tolerance.** During a rollout the fleet is mixed `vN-1`/`vN`; assert the app reads both.
- **Hot-path verification.** When you change a hot path (cold-open, directory resolve, migration copy, fan-out), add a microbench-style test asserting an order-of-magnitude floor/ceiling (`assert open_us < 50_000`), not an exact latency. Tag it `@tag :bench` so it's excluded from the default suite.

## Benchmarking

**The harness exists.** `mix fathom.bench` measures the hot paths; `scripts/benchmark.sh` runs it prod-compiled and appends one JSON line per run (commit, branch, dirty, host, metrics) to `scripts/perf_history.jsonl`; `scripts/commit_with_bench.sh` is the bench-then-commit gate — it benches the working tree and **refuses the commit if ANY metric regresses ≥20%** vs the parent's **same-host** entry (`Fathom.Bench.Gate`). It is multi-metric because fathom's cost is per-shard open + fan-out, not single-query throughput. Hot-path changes also ship `@tag :bench` floor/ceiling guards (`test/fathom/bench_test.exs`). **Hold the discipline: don't invent numbers — measure, or say "unmeasured."**

**What each metric measures, and what it has read** — the hot-path catalog and the `mix fathom.scale` harness including `--hotspots` — is the appendix of [`docs/benchmark-plan.md`](docs/benchmark-plan.md). The measured incidents behind the rules below are in [`docs/agent-field-notes.md`](docs/agent-field-notes.md) § Benchmarking. Two series caveats: `cold_open_s3_*` on localhost MinIO measures the S3 *protocol*, not real S3 latency (use `scripts/benchmark_s3_latency.sh`, which injects RTT via toxiproxy), and `copy_keystone_rows_per_s` (renamed from `copy_rows_per_s` on 2026-07-31) is not comparable with the old series.

- **Run clean and in prod mode**: `MIX_ENV=prod`, a clean DB/data state, `./chaos.sh down` first (`docker ps -q | wc -l` should be 0 — the rig's containers compete for the same cores and don't stop themselves).
- **Regression response:** <20% is noise. ≥20% — rerun once; if it holds, **revert first**, then reproduce minimally. Don't stack fixes on a known-regressed commit.
- **Phantom-regression rule.** Calling a regression "phantom" needs (a) a second tool or environment showing why, (b) an explicit account of why the first measurement was wrong, and (c) a cooling-off rather than same-day closure — unless the confirming measurement is minutes away, in which case run it.
- **Distrust a number outside its own historical band in either direction.** A suspiciously good number is usually a measurement of work that never happened, and banking it is worse than a false block: the *next* commit gets gated against the outlier. Re-bench HEAD to leave a corrected baseline.
- **Some metrics are bimodal or fat-tailed** — `dir_resolve_p50_us` (two modes), `cold_open_p99_us` (false-blocks ~1 run in 3; p50 is stable). `fanout_kb_per_shard` and `fanout_gc_kb_per_shard` are watch-only for this reason; `served_kb_per_shard` is the density metric to believe. A single A/B against a bimodal metric is not evidence however often you repeat one side; diagnose a block with a back-to-back same-harness A/B of parent and working tree, not a mechanism argument, and don't reach for `--skip`.
- **Two unrelated metrics moving together in one run means the machine, not the code.**
- **A new bench metric asserts its own preconditions** inside the harness (`unless dirty?(pid), do: raise "flush bench is measuring nothing"`), so it fails loudly instead of reporting a spectacular result.
- **Don't change the shared bench `setup/1` to serve one metric** — it changes what every other metric measures (a harness-topology change that invalidates the series). Scope new dependencies to the metric that needs them.
- **Bench-gate baseline workflow.** `commit_with_bench.sh` compares against the parent's latest perf_history entry, and a gated commit leaves only a `dirty: true` one. Before each successive gated commit: `git stash push -- lib/ test/` → `scripts/benchmark.sh` → `git stash pop` → gate. Otherwise you get `no baseline for parent <sha>` or a comparison against an older outlier.
- **`./chaos.sh up` does not build** — it starts whatever `fathom-chaos:latest` exists, possibly weeks old, and a pass against a stale image reads as validation. When the rig is validating a change: `./chaos.sh build` first; compare `docker image inspect fathom-chaos:latest -f '{{.Created}}'` with `git log -1 --format=%cI`; best, assert the fix's own observable through the LB before anything else.
- **Docker is machine-global.** A sibling checkout (`djathom`) may keep its own compose project up; projects are namespaced (check with `docker inspect -f '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' <container>`), but they share colima's CPU, so treat rig latency as relative even when pass/fail is trustworthy.

## Gates

A "gate" is a check that must pass *before* a commit lands — not after.

- **`mix precommit` is the commit gate** (defined in `mix.exs`): `compile --warnings-as-errors`, `deps.unlock --unused`, `format`, `dialyzer`, `test`. Run it when your changes are complete and fix everything it surfaces before committing.
- **GitHub Actions CI** is the second opinion, not the first — it runs the same checks across OTP 27/28/29. Disable with `gh api -X PUT repos/cwisecarver/fathom/actions/permissions -F enabled=false`.
- **Dialyzer** runs inside `precommit` as `cmd env MIX_ENV=dev mix dialyzer` (the `env` is required: `mix cmd` has no shell, so a bare `VAR=value` prefix is taken as the executable). Run it manually as `MIX_ENV=dev mix dialyzer` — the env sets the scope: `:dev` analyzes `lib/` only, keeping the bench drivers' `mint_web_socket` opaque-type cascade out. The first run after a dependency change rebuilds part of the PLT (minutes; a first-ever build is ~10–20 min, so set a long timeout). Suppressions go in `.dialyzer_ignore.exs` with a comment per entry; unused filters fail the run. See § Typing.
- **Migration gate.** A schema migration must not ship without: (a) a forward copy+transform test, (b) a revert-flip test, (c) a cross-version-tolerance check. A migration that can't be reverted by pointer-flip within the retention window, or that the running app can't tolerate mid-rollout, is not done.
- **Shard-isolation gate.** Any change to shard routing (`Fathom.ShardExecutor.shard_from_conn`, `Fathom.Shards`, shard-path construction, or the planned `Fathom.Directory` resolve) must have a test proving shard A never resolves to shard B. Treat a cross-tenant leak as a release blocker, not a finding.
- **Bench-then-commit gate** (built). Any change touching a hot path (shard routing/open, directory resolve, migration copy, the shard coordinator) goes through `scripts/commit_with_bench.sh -m "<msg>"`: it benches the working tree and refuses the commit on a ≥20% regression in any metric vs the parent's entry in `scripts/perf_history.jsonl` (override `PERF_REGRESS_BLOCK`). Pure docs/test/comment-only changes skip it — `git commit` directly with a `[skip-bench]` token, or `--skip`. See Benchmarking and `docs/benchmark-plan.md`.

## Typing

Dialyzer-enforced `@spec` coverage. The gate is in § Gates; this is how to write for it.

The defect dialyzer actually finds here is the **stale** type: a field or return case added to the
code and every caller but never to the declaration (seven on the 2026-08-14 baseline, including
`Recovery.position` missing `torn`, which made A2 cross-fleet promotion read as dead code). So when a
type and its callers disagree, suspect the type, and alias the owning type
(`@type position :: FollowerLog.t()`) rather than restating its shape. The full table and the
measurements behind these rules are in [`docs/agent-field-notes.md`](docs/agent-field-notes.md) § Typing.

1. **Skip behaviour callback implementations** (GenServer/LiveView/Plug/Oban/Mix.Task/
   `Filo.Executor`/`Fathom.Shard.Storage` impls); the contract lives once, on the `@callback`.
2. **Spec GenServer client APIs, but don't count them as coverage.** A spec is checked only where
   dialyzer can derive a contradicting success typing from the body. Pure functions and data shapes
   that flow between modules are checked; wrappers over `GenServer.call`, `:ets`, dynamic dispatch
   (`backend().pull(...)`) and NIFs are not. Put effort into the data contracts first.
3. **Reuse owned types** (`Fathom.ShardId.t()`, `Storage.lease()`, `Ecto.Changeset.t()`) instead of
   re-inlining a named shape — re-inlining is how the stale types happened.
4. **Ecto schemas get `@type t :: %__MODULE__{}`.**
5. **A function that always raises is `no_return()`**, not an ignore entry.
6. **No defensive typing** — no guards added to satisfy a spec, no `term()`/`any()` escape hatches.
   State the bound you can prove (`integer()`, not `non_neg_integer()`, if that's what's provable).

Dialyzer analyzes callers using success typings, not contracts, so a spec on a private helper can't
change what its callers see; an accurate spec on a *public* function can. When a dependency's
`@opaque` type makes a subsystem read as dead (`Mint.WebSocket.t()`), confirm it with a probe module
that only calls the dependency.

## Principles

- **Simplicity first.** Touch the minimum code; fix root causes, not symptoms.
- **Fix bugs autonomously** — diagnose from logs, errors and tests, then resolve.
- **Never hand-roll routing, namespace, or SQL string surgery.** Don't build shard paths / namespace names / directory keys by ad-hoc string concatenation scattered across the codebase — route every shard resolution through `Fathom.Shards` (and request → shard through `Fathom.ShardExecutor.shard_from_conn`) so isolation and cutover logic live in one place. Don't hand-roll SQL for the libSQL shards either — always bind with parameterized queries (`Fathom.Shard` passes args through to `exqlite`); never interpolate values into SQL (injection and quoting edge cases bite the same way Postgres's do).
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

### Schema versions and what is still planned

The blue/green migration machinery is built ([`docs/migration.md`](docs/migration.md)). A shard's
schema version lives in **three places**: `django_migrations` in the shard (Django's own ledger —
the truth), `PRAGMA user_version` (the O(1) gate), and `shards.schema_version` in Postgres (laggard
queries without opening shards). Still planned: a cached, PubSub-invalidated directory resolve on
the request path ([`docs/migration-plan.md`](docs/migration-plan.md)); routing is Host-based today.

---

# Framework guidelines

Trimmed from the `phx.new` usage-rules to the parts fathom uses. The web surface is a small admin
dashboard on :4000 (the data plane speaks Hrana, not HTML); if you add a real user-facing surface,
pull the full rules back from a fresh `phx.new` project. Generic Elixir/Mix idioms are omitted —
these are the project conventions and Phoenix 1.8 specifics that differ from older habits.

## Elixir

- One module per file.
- Don't call `String.to_atom/1` on user input (atom exhaustion).
- Predicates end in `?`; reserve `is_` for guards.
- Use the stdlib for date/time; don't add a dependency.
- Give `Task.async_stream/3` a real `timeout:` — `:infinity` once let one wedged tenant hang a whole 1024-tenant sweep.
- Avoid `mix deps.clean --all`; it's almost never the fix.

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

- Preload associations a template will touch.
- Schema fields are `:string` even for `:text` columns.
- `validate_number/2` has no `:allow_nil` option — validations already skip nil changes.
- Keep programmatically-set fields (`user_id`) out of `cast/3`; set them on the struct.
- Generate migrations with `mix ecto.gen.migration name_with_underscores` so timestamps are right.

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
