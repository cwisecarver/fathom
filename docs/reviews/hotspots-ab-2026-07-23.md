# hotspots throughput A/B — 2026-07-23 perf iteration (per-query wins)

**Setup:** `mix fathom.scale --hotspots` (prod-compiled, 1000 shards, Zipf s=1.1, ~20k
streams per window at L=16/64; defaults at L=1), same host, back-to-back. A/B between
`c6f8603` (pre-iteration baseline, clean prod-compiled worktree) and `b55e9ae` (current).
The harness drives `Shards.checkout → ShardExecutor.execute` directly — filo's wire layer
is not on this path, so this isolates the fathom-side per-query/per-stream work.

## Node throughput (drive rate, q/s)

| stream-len | c6f8603 | current | delta |
|---:|---:|---:|---|
| L=1 (per-query lower bound) | 3,335–3,410 (3 runs) | 3,239–3,420 (2 runs) | **flat** (overlapping bands, ±3% noise) |
| L=16 | 23,526 | 27,079 | **+15.1%** |
| L=64 | 47,650 | 53,817 / 55,266 (2 runs) | **+13–16%** |

Hottest-shard rate at L=64 rose in proportion (hot_1 ≈ 9,509 q/s current vs the ~9.0k
documented for the old code), so the single-tenant ceiling moved with the aggregate.

## Reading

- **Held-stream throughput is up ~13–16%** — the shape matches the statement cache
  (review 2026-07-23 #1): at L=16, 15 of 16 queries per stream skip `sqlite3_prepare_v2`
  entirely (reset + rebind on the cached statement); at L=64 it's 63 of 64. The
  classify-once / slice-first-downcase / env-read trims (#2/#7/#16/#27) ride the same
  per-query path. This is the workload shape of the flagship client — django-libsql holds
  a WebSocket stream and replays identical parameterized SQL on it.
- **L=1 is flat, as expected**: a one-query stream never hits the statement cache, and
  its cost is dominated by the per-stream work (connection open + coordinator checkout
  round-trip), which the iteration deliberately left mostly untouched (#18 skipped as
  load-bearing; the checkout-path trims #17/#21/#24 are small against an exqlite open).
  Raising the L=1 bound is the connection-pool question — explicitly measure-first
  pending a wired `hrana_rt_us`.
- Detection quality was unchanged across all runs (ShardLoad.top recall 1.0, top-20
  Jaccard 1.0, p99-threshold recalls 1.0) — the counters ride the same gates and the
  rebalancing evidence contract still holds.
- Prior documented numbers (L=1 ~3.0k / L=16 ~24k / L=64 ~51k) were a different session;
  this A/B is same-day, same-host, alternating runs — use it, not the cross-session diff.
