# Spike plan — per-stream SQLite connection pooling

**Status:** BUILT + measured, but **NOT enabled** — turning it on by default was BLOCKED 2026-09-17
by a validation run. The spike said GO (conditional on a bounded pool); the full pool was implemented
(`Fathom.Shard.HandlePool` + `Connection.reset_for_reuse/2` + coordinator `pool_take`/`checkin/4`/
`close_pool` + executor reuse, all gated on `:connection_pool`, default off) and the win re-confirmed
on the real path (below). But running the WHOLE suite with pooling forced on failed two shard-lifecycle
tests: `ShardPositionSeedTest` (empty-WAL idle-drop must stamp a rankable ordinal, not nil — the exact
invariant A2 promote-on-open ranks on) and `ShardDurabilityTest` (first write restores the full-rate
flush timer). Cause: a pooled handle keeps a SQLite connection OPEN on an otherwise-idle shard, and the
snapshot / position-stamp / flush-timer logic assumes an idle shard has zero open connections. **Before
this can be enabled, the pool must be drained (handles closed) around those lifecycle points, and the
suite re-run forced-on until green.** A TTL-sweep timer and telemetry are also still deferred; all
numbers are min-based on a machine under post-upgrade load.

## The question

Does keeping a shard's SQLite handle alive across Hrana streams (a per-`{shard_id, scope}` pool)
cut the per-stream open latency by enough to justify the memory it costs — **≥20% off
`hrana_open_rt_us` without blowing the `served_kb_per_shard` density ceiling**? This is a one-shot
measurement spike: build the cheapest pool that can answer it, measure, decide, revert.

## Why now (context)

- **The open is dominated by SQLite's `-shm` (WAL-index) setup.** Measured 2026-09-16 (raw
  `Exqlite.Sqlite3`, WAL, min of 400 — trustworthy despite a loaded box): a bare open + first
  statement + close is **~230 µs** with the shard's only connection vs **~54 µs** when a second
  handle keeps `-shm` mapped — **~176 µs (~76%) is the WAL-index creation**. SQLite builds `-shm`
  on the first statement and unlinks it on the last close, and a one-query stream is the shard's
  only connection, so every open recreates it. This is also the flat `L=1` per-stream-open
  throughput ceiling (`docs/benchmark-plan.md`).
- **The prior rejection (expert review 2026-08-26 #11) likely measured a pool that never hit.** Its
  A/B read pooled ≈ unpooled (~22–40 µs apart) — but if `-shm` is ~176 µs of the open, a pool that
  actually kept the handle alive should move it far more. The suspected cause: the cache lived in
  the per-request stream process and missed across the HTTP request→stream hop (see the
  side-channel note in `Fathom.ShardExecutor`). The corrected `hrana_open_rt_us` moduledoc in
  `Fathom.Bench` records this.
- HTTP clients (django-libsql at its default `CONN_MAX_AGE=0`) open a fresh stream **per request**,
  so any saving is per-request, not occasional.

## Hypotheses (each falsifiable, each with its own probe)

- **H1 — the win exists.** A pool that keeps the handle (and `-shm`) alive removes ~120–176 µs from
  the open. Predicted net after reset overhead: **~35% (± ~15) of `hrana_open_rt_us`** (~347 µs
  baseline). *Falsify:* interleaved pooled-vs-unpooled A/B shows < ~20% delta.
- **H2 — the pool actually hits.** The measured delta is real only if the pool serves a live handle
  instead of silently reopening. *Falsify / guard:* an **observable** — the pool must prove the hit,
  not assume it (e.g. assert the shard's `-shm` file stays present across samples, or count
  `Sqlite3.open` calls and assert it does NOT grow per request). If it does not hit, H1's number is
  meaningless; fix the hit before trusting any delta. (This is the exact trap #11 fell into.)
- **H3 — the density cost is acceptable.** A kept-alive handle carries the per-connection page cache
  (the ~2 MiB tail bounded by expert review 2026-07-24 #29). *Falsify:* `served_kb_per_shard` (and
  `served_binary_kb_per_shard`) regress beyond an agreed budget at realistic held-stream counts.

## The throwaway build

Scope it to answer the three hypotheses and nothing more; it must NOT be shippable as-is.

- A per-`{shard_id, scope}` handle cache reached by `Fathom.ShardExecutor.do_open/3`, with the
  **four reset guards a real pool needs, applied on every checkout**: autocommit check, statement-cache
  purge, drop the per-connection process-dictionary keys, re-apply the `@tenant_pragma_allow`
  pragmas. **Separate `:ro` and `:rw` pools** — a `:ro` checkout must NEVER be handed a `:rw` handle
  (this is the same scope-leak class as the process-dict side-channel bug; treat it as security, not
  perf).
- The pool must live somewhere that survives across streams of one connection AND across the
  HTTP request→stream process hop — i.e. NOT the stream process's own dictionary. That placement is
  the crux of whether it hits at all.
- Bake in H2's observable from the start (a counter or an `-shm`-presence assertion the harness can read).

## Measurement protocol

The machine is under sustained post-OS-upgrade indexing (noisy for days), so **do not use the gated
`scripts/benchmark.sh`** (p50/p99 vs history — the noise wrecks it). Use the robust technique the
`-shm` finding already validated:

1. **Interleaved A/B, min of many samples**, pooled vs unpooled, same process, back to back. The min
   filters background noise (proven stable at 230/224/222 across three runs at load 11).
2. Measure through `Fathom.Shard.Connection.open/2` + first query + close (the real path: extension
   load, pragmas, authorizer), not just raw Exqlite.
3. **Assert H2's observable before trusting any latency number** — if the pool did not hit, stop.
4. Measure `served_kb_per_shard` / `served_binary_kb_per_shard` at a realistic count of concurrently
   held pooled handles for H3 (this one is a memory reading, not latency — less noise-sensitive).
5. If a clean window ever appears, confirm with one `scripts/benchmark.sh` run; treat it as
   corroboration, not the primary evidence.

## Decision gates

- **GO** (write the implementation plan) if ALL hold: the pool **provably hits** (H2 observable),
  the open drops **≥20%** (`hrana_open_rt_us`, past the bench gate), and the density regression on
  `served_kb_per_shard` is within an agreed budget.
- **NO-GO / stop** if any of: the pool cannot be made to hit across the request→stream hop; the delta
  is < 20% once it does hit; or the density cost exceeds the budget. Record which, so #11 is not
  re-litigated a third time from scratch.

## Risks & guards

- **Security is the dominant risk, not perf.** A reused handle that leaks scope (`:ro` served a
  `:rw` handle), a stale pragma, a left-open transaction, or the SQLite extension not re-disabled is
  a cross-tenant / privilege hazard. The reset guards and separate `:ro`/`:rw` pools are load-bearing;
  the spike must include a test that a `:ro` checkout can never write, and that a checked-in handle
  carries no transaction/pragma state into the next tenant.
- **Do not ship the throwaway.** If GO, the production pool is a fresh, reviewed implementation with
  its own isolation tests and bench gate.
- **The baseline is noisy.** `hrana_open_rt_us` (347 µs) is one fat-tailed reading; anchor the
  percentage to a min-based A/B on the same box, not to that single number.

## Scope / non-goals

- This spike measures; it does not deliver a production pool.
- No LRU/eviction, TTL, or cross-node concerns here — those belong in the implementation plan if GO.
- Out of scope: changing the `-shm` lifecycle in SQLite itself, or the Filo transport.

## Effort

Time-box to a focused spike (throwaway pool + observable + A/B harness + the two security probes),
then a written decision. If it runs long or the pool can't be made to hit, that is itself the
answer for this pass — stop and report.

## Results (measured 2026-09-17)

Throwaway probes through the real `Fathom.Shard.Connection.open/2`, min of 400 samples (min filters
the load noise). Machine was under sustained post-upgrade indexing, so absolutes are noisy — the
STRUCTURAL results (does it hit; residual size; per-handle order of magnitude) are the trustworthy
part, exact percentages are not.

**H2 — does the pool actually hit? PASS (the decisive result).** The observable held: `-shm` was
absent between unpooled opens and present the whole time the pooled handle was held, and the pooled
arm opened exactly one handle. The pool serves a live handle rather than silently reopening — the
specific thing the 2026-08-26 #11 A/B got wrong.

**H1 — the win. PASS, large.** Unpooled `open + query + close` was ~370–510 µs (the two runs
disagreed — box noise); a warm pooled request was **~23 µs** (reset guards + query) or ~13 µs
(query only). Pooling removes essentially the entire per-open cost — the 9 pragmas, the extension
load, the authorizer, and the `-shm` setup — leaving ~20 µs. Reconciled against `hrana_open_rt_us`
(347 µs, whose non-open baseline `hrana_rt_us` is 131 µs, so the open is ~216 µs): pooling removes
nearly all of that ~216 µs → roughly a **~50–60% cut of `hrana_open_rt_us`**, higher than the
conservative ~35% first estimate. Exact figure pending a clean-box, end-to-end Hrana measurement
(this probe measured `Connection.open` in isolation, not the full transport path).

**H3 — density cost. CONDITIONAL PASS.** RSS per kept-alive handle (NIF/C page-cache memory, so
measured as OS RSS, 50 handles warmed by a full scan): ~150–275 KB, drifting up with the shard's
working set toward SQLite's ~2 MiB `cache_size` cap (numbers non-monotonic = noise; treat as order
of magnitude). This matches the existing `served_kb_per_shard` = 196 KiB. Meaning: a pooled-but-idle
shard costs ~200 KB (the "served" level) instead of ~26 KB (idle coordinator) — a **~7–8× density
hit per held shard**. Node density is memory-bound, so a hold-everything pool is a NO; a pool capped
(LRU) to the hot working set is fine, since cost = (hot set) × ~200 KB.

**Verdict:** GO on H1 and H2; H3 forces the pool to be **bounded/LRU-capped to the hot set**, never
hold-everything. Before an implementation plan: (1) re-measure H1 (exact %) and H3 (exact KB) on a
quiet box; (2) the implementation adds the LRU/eviction this spike left out of scope, plus the
security isolation tests (scope leak, stale state) the throwaway did not carry.

### Re-measured on the REAL implementation (2026-09-17)

Through the shipped path (`ShardExecutor.open` → query → `close`, `:connection_pool` on vs off, min
of 400 — box still under post-upgrade load, so min-based):

| per request (open + query + close) | µs |
|---|---|
| pooling OFF (fresh handle each request) | **392** (p50 467) |
| pooling ON (warm handle reused) | **45** (p50 55) |

**~347 µs saved, ~88% off that path.** Mapped to the end-to-end `hrana_open_rt_us` (~347 µs, whose
non-open `hrana_rt_us` baseline of ~131 µs is Filo transport pooling cannot remove): roughly a
**~50–55% cut of the full per-stream open** — in line with the spike prediction, a touch more
conservative than the throwaway probe because `reset_for_reuse/2` does real work the probe skipped.
H1 and H2 confirmed on real code; H3 (~200 KB/held handle) is bounded by the per-shard cap + the
existing idle-drop. The win justified enabling it — but the forced-on validation run (see Status)
BLOCKED that on the position-ordinal / flush-timer lifecycle interaction, so it stays off. Still-open
follow-ups: the pool-drain-around-lifecycle fix (the enablement blocker), a quiet-box absolute
re-measure, the TTL-sweep timer, and telemetry.

## References

- `reviews/dsv41f.perf.md` — the headline finding this spike acts on.
- `Fathom.Bench` `hrana_open_rt_us` moduledoc — the corrected attribution + the `-shm` numbers.
- expert review 2026-08-26 #11 — the prior rejection this re-tests.
- expert review 2026-07-24 #29 — the ~2 MiB per-connection page-cache bound (the density cost).
- `docs/benchmark-plan.md` — the `L=1` per-stream-open ceiling and the density metrics.
