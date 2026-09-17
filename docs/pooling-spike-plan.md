# Spike plan — per-stream SQLite connection pooling

**Status:** proposed spike (throwaway experiment), not an implementation plan. Decide GO/NO-GO from
measurement, then write a separate implementation plan only if GO.

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

## References

- `reviews/dsv41f.perf.md` — the headline finding this spike acts on.
- `Fathom.Bench` `hrana_open_rt_us` moduledoc — the corrected attribution + the `-shm` numbers.
- expert review 2026-08-26 #11 — the prior rejection this re-tests.
- expert review 2026-07-24 #29 — the ~2 MiB per-connection page-cache bound (the density cost).
- `docs/benchmark-plan.md` — the `L=1` per-stream-open ceiling and the density metrics.
