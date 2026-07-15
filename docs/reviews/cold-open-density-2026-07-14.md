# Cold-open per-open cost at fleet density — the O(N) readdir removed, measured

**2026-07-14, Apple M5 Max (18 cores), macOS 27.0.** The 3-node chaos rig (`fathom1/2/3`
behind nginx + MinIO, all in one colima VM, prod-release nodes) at commit `8d719b9`. This
records the fleet-scale evidence for the cold-open fix in commit `4cd3cd8` (expert review
2026-07-14 #2), which the commit-time localhost bench could not show.

## The claim under test

`Fathom.Shard.handle_continue(:open, …)` used to call `Storage.reap_stale_temps(path, …)` on
**every cold open** to clear a crashed prior open's orphaned temp files. Its body was
`Path.wildcard(base <> ".{dl,snap,tmp,pull}*")`, and `Path.wildcard` **cannot prefix-optimize a
pattern whose filename component holds a `*`** — so it did a full `readdir` of the (flat, fleet-sized)
shard data directory and regex-matched every entry, matching zero temps in the common case. That is
**O(resident shard files)** on the single headline hot path — "millions of small shards, cheap
cold-open" — re-paid on every idle re-open and, worst of all, most expensive exactly at the density
the architecture is sold on.

The fix (`4cd3cd8`): the coordinator clears only its own **deterministic** `.pull` family by direct
name — `Storage.reap_named_temps/2`, O(1), no directory scan (and still the `promote_pull` collision
guard) — and the uniquely-suffixed `.dl`/`.snap`/`.tmp` orphans are swept amortized by
`Fathom.Shard.TempReaper` (one directory scan per interval, off the open path). The claim: **per-open
reap cost drops from O(N) to O(1)** and stops scaling with resident-shard count.

## Why the commit-time bench looked flat

`cold_open_p50_us` (`mix fathom.bench`) runs against a throwaway data dir holding a handful of files,
so the `readdir` was cheap and the metric read flat post-fix (as expected — the fix is not a
regression there). The cost is a **fleet-scale** effect: it only appears when the data directory holds
tens of thousands of shard files. The chaos rig provides exactly that.

## What was measured

1. **`deploy/chaos/chaos.sh density 30000`** minted 30,000 novel shards through the LB across the three
   nodes (each mint is one real Hrana `open → SELECT 1 → close` — a genuine cold-open: create the file
   + acquire the `{owner,epoch}` lease in MinIO on the node nginx hashes the Host subdomain to). Result:

   | node | held (coordinators ≈ `.db` files) | BEAM Δ | KiB/shard | RSS Δ |
   |------|------|------|------|------|
   | fathom1 | 9,557 | 62.1 MB | 6 | 398.6 MB |
   | fathom2 | 9,497 | 63.0 MB | 6 | 380.6 MB |
   | fathom3 | 10,892 | 70.2 MB | 6 | 399.1 MB |

   Partition: 29,946 held across 3 nodes (spread 1.15×), ~6 KiB/shard resident, ~322 shards/s aggregate
   mint. (The mint rate is dominated by the MinIO lease round-trips and the one-process test harness, so
   it is **not** a clean isolation of the readdir — see step 2 for that.) `density` raises `:shard_idle_ms`
   to 900 s **before** minting, and a coordinator freezes its idle timeout at init, so the ~10k `.db`
   files per node stay resident for 15 minutes — the exact condition to measure the per-open reap.

2. **The direct isolation.** With ~10k shard files resident per node, each node was RPC'd to time the
   **old** per-open call (`Storage.reap_stale_temps/2` — the glob `readdir`, still in the codebase because
   `TempReaper` uses it off the hot path) against the **new** per-open call (`Storage.reap_named_temps/2`,
   O(1)) on a real data-dir path, 5 iterations each, min reported:

   | node | resident files | OLD glob `readdir` (per open) | NEW O(1) named reap | speedup |
   |------|------|------|------|------|
   | fathom1 | 9,558  | **2.81 ms** (2813 µs) | 11 µs | **256×** |
   | fathom2 | 9,498  | **3.06 ms** (3058 µs) | 11 µs | **278×** |
   | fathom3 | 10,893 | **3.60 ms** (3596 µs) | 10 µs | **360×** |

   Raw OLD samples (µs): fathom1 `3965/2813/3272/3425/3352`, fathom2 `3743/3058/3846/3478/3166`,
   fathom3 `3862/4045/3639/4077/3596`. Raw NEW samples (µs): fathom1 `46/13/12/13/11`, fathom2
   `48/12/17/11/11`, fathom3 `40/11/11/11/10`.

## Finding

The per-open reap is **O(N) → O(1)**, confirmed directly at fleet density:

- **Old:** ~2.8–3.6 ms per cold open at ~10k shards/node, and visibly **scaling with the file count** —
  fathom3's 10,893 files cost 3.60 ms vs fathom1's 9,558 → 2.81 ms. Linearly extrapolated, the rig's
  proven 105k-held ceiling (`docs/reviews/fleet-density-2026-07-10.md`) would pay **~35 ms of pure
  `readdir` per cold open**, before a single byte is served, re-paid on every idle re-open.
- **New:** ~10–11 µs, **flat** regardless of file count — the O(1) direct-name reap.
- **256–360×** faster at this density; because one path is O(N) and the other O(1), the gap **widens with
  scale**. This is the cold-open cost the fix removed, on the exact density regime the "millions of small
  shards" story depends on.

The `4cd3cd8` bench gate also confirmed the fix is neutral on the measured metrics (`cold_open_p50_us`
−1.7%, `fanout_kb_per_shard` flat at 17.2 KiB once the per-open `garbage_collect` — whose heap-shrink is
load-bearing at density, independent of the removed readdir garbage — was kept).

## Limits / honesty

- The reap timings are the **min of 5** in-process `:timer.tc` calls per node; the first call of each run
  is warmer/colder by OS dircache state, hence the spread in the raw samples. The point is the
  **order-of-magnitude** and the **O(N)-vs-O(1) shape**, not an exact latency.
- This measures the reap **operation** in isolation (the thing the fix changed), not end-to-end
  cold-open latency, which is dominated by the S3/MinIO lease + pull round-trip (see
  `docs/reviews/latency-cost-2026-07-11.md`). The readdir was an **additive** per-open CPU cost on top of
  that RTT; removing it matters most where the RTT is small (local NVMe / `Storage.Local`) and where N is
  large.
- Single-box rig: three prod-release nodes share one colima VM, so absolute mint throughput is
  host-bound. The per-open reap cost is a per-node CPU property and is unaffected by that.

Rig torn down (`chaos.sh down`) after the run; the co-resident other-project MinIO was left untouched.
