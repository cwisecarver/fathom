# served-data re-run — 2026-07-23 (density check after the perf iteration)

**Rig:** `deploy/chaos/chaos.sh served-data` (defaults: 10k shards/node, fleet 30k, 256 KiB
each, held under a live scanning connection) — the same configuration as the 2026-07-10 run
in `fleet-density-2026-07-10.md`. Image built from post-iteration `main`. Purpose: verify
the per-connection statement cache (review 2026-07-23 #1) didn't tax the served-density
footprint.

## Held footprint vs 2026-07-10

| metric | 2026-07-10 | 2026-07-23 | verdict |
|---|---:|---:|---|
| held (fleet) | 30,000 | 30,000 | ✓ |
| RSS/shard | 629–649 (~640 KiB) | **543–544 KiB** | no regression (−15%, cross-day — see below) |
| fds/shard | 3.0 | 3.0 | unchanged |
| scan q/s (fleet) | ~78k | ~17.4k | ⚠ harness artifact — see the A/B below |

- **The memory answer the run was for: the statement cache costs nothing visible at
  density.** RSS/shard measured *lower* than July's ~640 KiB. Treat the −15% as
  cross-day variance plus the iteration's transient-allocation trims at sample time, not
  a claimed win — the load-bearing result is "≤ the documented baseline, fds unchanged."
- fd story unchanged: 3 fds per WAL-active shard, ceiling still `nofile/3`.

## The scan-rate drop is a one-process harness artifact — verified by local A/B

The rig's aggregate scan rate looked alarming (~78k → ~17.4k q/s), and a 1000/node probe
showed it scale-dependent (~12k q/s/node at 1k vs ~5.8k at 10k). Same-day local A/B of the
two shapes (`c6f8603` baseline worktree vs current, prod-compiled, 256×1 KiB table,
aggregate scan):

| shape | c6f8603 | current | delta |
|---|---:|---:|---|
| A: ONE connection, 5,000 repeated scans (the production per-stream shape) | 36,938/s | 38,068/s | **+3.1%** |
| B: 1,000 connections held in ONE process, one scan each (the harness shape) | 24,431/s | 19,933/s | **−18.4%** |

- The statement cache lives in the **owning process's dictionary** — correct and free in
  production, where a process owns ONE connection (the per-stream model; the hotspots A/B
  measured that shape at **+13–16%** node throughput). The served-data harness instead
  holds ALL 10k connections' caches in a single rpc process, so its heap carries 10k live
  cache maps + statement resources and every GC walks them — the same
  one-process-artifact class `fleet-density-2026-07-10.md` already documents for its
  open-rate decay. The rig's remaining gap vs July is that artifact at 10× the local
  probe's connection count, compounded by cross-day VM variance.
- **Follow-up only if a real many-connections-per-process consumer ever appears** (none
  exists: streams hold one; migrator jobs hold one or two): move the cache from the pdict
  to an ETS table keyed by connection, trading a slightly costlier hit for GC decoupling.
