# Agent field notes — the evidence behind AGENTS.md's rules

AGENTS.md is loaded into every agent session, so it states each rule briefly. This file keeps the
measured incidents that justify those rules — moved verbatim from AGENTS.md when this file was created — so the
reasoning survives without costing every session its tokens. Read the matching section before
arguing with, or relaxing, one of the rules.

## Benchmarking

- **A suspiciously GOOD number is a broken measurement until proven otherwise** — and it is the easier one to bank by accident, because the gate says OK and nobody looks. Two from one session: a first-draft `flush_p50_us` read **2 µs** for a full `VACUUM INTO` + upload (the bench's minimal tree has no `WriteCounter`, so `bump/1` rescued to `:ok`, the shard read clean, and `flush_now/1` returned having done nothing); and `fanout_kb_per_shard` reported a **−29% "win"** against a tight 3.77–4.07 historical band on a commit that could not plausibly have caused it (a re-bench of the same HEAD returned 3.77). **Check any metric that moves outside its own historical band in EITHER direction, and re-bench HEAD to leave a corrected baseline** — the real damage from an outlier low is the *next* commit being gated against it, where an ordinary reading becomes a false ≥20% regression and blocks clean work. (The `fanout_kb_per_shard` half of that example no longer *blocks* — it was demoted to watch-only on 2026-08-26 because its same-tree spread reaches 56%, above any band worth setting; the rule it illustrates is unchanged and still applies to every gating metric. **`fanout_gc_kb_per_shard` followed it to watch-only on 2026-08-28**, and the way it was caught is the part worth carrying: it had *already blocked a clean commit* at "+51%", that block was believed, written into a code comment as a design constraint, and used to park a second finding — before a back-to-back A/B the next day read **+0.9%** for the same line. Three runs of one identical tree read 5.48 / 4.50 / 5.49. **A single A/B against a bimodal metric is not evidence no matter how many times you repeat the B side** — all four original samples landed in one mode. `served_kb_per_shard` (3.4% spread) is now the density metric to believe.)
- **`./chaos.sh up` does not build — assert the fix's own observable.** Best — assert the fix's **own observable** through the LB before running anything else. A rig validating the ATTACH fix should first confirm `ATTACH DATABASE …` is *refused*; if it succeeds, stop, the binary is old. (Learned the hard way on 2026-08-02: `smoke` and `deploy` both passed against a build that predated every fix under test, and the tell was that ATTACH still worked.)
- **When two unrelated metrics move TOGETHER in one run, it is the machine, not the code.** The single best tell, and cheaper than any reasoning about mechanism. Measured 2026-08-05 on a change that added `:os_mon`: one gate run blocked on `dir_resolve_p50_us` +46.9% AND `hrana_open_rt_us` +27.6%, the latter the highest value ever recorded. Two more working-tree runs read 357/359 and 130/132 against a parent of 355/128 — i.e. the pair moved together in exactly one run and nowhere else. A code change that genuinely slowed a Postgres resolve *and* a Hrana stream open by different amounts is far less likely than one contended run. `dir_resolve_p50_us` is also visibly **bimodal** (clusters near ~128 and ~185), so its "regression" is often just which mode the baseline landed in.
- **`cold_open_p99_us` is FAT-TAILED and will false-block you ~1 run in 3.** Measured 2026-08-04 on a change that could not possibly touch it: five gate/bench runs of the same working tree read 5053, 5367, 2076, 2209 against parent runs of 2786, 2039, 2211, 2362 — two isolated samples at ~2.4× and the rest indistinguishable. The p50 never moved. **Diagnose it with a same-harness A/B, not with a mechanism argument.** Bench parent and working tree the same way, back to back; if they agree, the block was a tail sample. Three wrong explanations were talked through first — "the rig was up" (it was down for the second block), "the gate uses a different harness" (`commit_with_bench.sh` just calls `benchmark.sh`), and "the code can't reach it" (true here, but that reasoning had already been wrong twice that day). The number settles it; the story does not. Do not reach for `--skip`: re-establishing a clean parent baseline as the last history line and re-running the gate let it pass on its own terms.
- **Never change the shared bench `setup/1` to serve one metric.** Starting `WriteCounter`/`FlushWatermark` there so the flush metric would work changed what `fanout_kb_per_shard` measures (every open shard gains ETS rows) — the gate correctly blocked at **+46.5%**. That is a *harness topology* change, and per the same-topology rule it invalidates the historical series. Scope new dependencies to the metric that needs them. (`fanout_kb_per_shard` is watch-only since 2026-08-26 and would no longer block that, so the harness-topology rule is now enforced by review rather than by the gate — `fanout_gc_kb_per_shard` and `served_kb_per_shard` are the ones that still bite.)

## Gates — CI history

- **GitHub Actions CI runs again** (2026-07-29). It was off while the repo was private — the
  account is out of Actions minutes for private repos, and a run would fail in ~12 s having
  executed zero steps. Going public restored free minutes. The first real run immediately caught
  a bug the outage had been hiding: the workflow hardcoded a developer's local username as the
  Postgres role, so `config/test.exs` (which resolves `PGUSER || USER || "postgres"`) asked for
  role `runner` on a runner. `PGUSER`/`PGHOST` are now pinned in the job env.

## Gates — Dialyzer details

- **Typing gate — Dialyzer (added 2026-08-14).** Runs inside `precommit` after `format` and before `test` (deterministic, reuses the fresh beams, ~2 s warm — much cheaper than the suite, so failing fast there is the right order), and in CI across OTP 28/29 (27 dropped 2026-10-05) so a typing difference between VM versions surfaces there rather than on one developer's machine. Manual run is **`MIX_ENV=dev mix dialyzer`**. **The env is the SCOPE, not a detail**: `elixirc_paths/1` compiles `test/support` only in `:test`, so `:dev` analyzes `lib/` alone — which is the plan's stated scope, and which keeps the benchmark drivers' `mint_web_socket` opaque-type cascade (21 findings from one dependency issue, see `mix.exs`) out of the gate. Inside `precommit` it is `cmd env MIX_ENV=dev mix dialyzer`, and the `env` is required — `mix cmd` does not use a shell, so a bare `VAR=value` prefix is taken as the executable and dies with `:enoent`. **First run after `mix deps.get` or a dependency bump pays a partial PLT update (minutes, once); a first-ever build is ~10–20 min.** PLTs live in `priv/plts` (gitignored) so `rm -rf _build` doesn't discard them, and CI caches them keyed on `mix.lock` + OTP version. Suppressions go in `.dialyzer_ignore.exs`, which documents the only two legitimate reasons to be in it and requires a comment per entry; `list_unused_filters: true` fails the run on a filter that stopped matching. **The gate was verified to bite** before being trusted (a deliberately wrong return type on `Shards.migrate_on_touch_mode/0` exits 1 at the dialyzer step without reaching the tests) — see § Typing for the style rules and what it actually caught.

## Typing

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
