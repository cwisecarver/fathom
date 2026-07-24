# latency-cost — 2026-07-23 re-run (post perf-iteration; the drain RTT fix live)

**Rig:** `deploy/chaos/chaos.sh latency-cost <ms>` — 3 prod-release nodes, MinIO + per-node
toxiproxy, 12 shards/node ~64 KiB each (small by design: the number reflects RTTs, not bulk
transfer). Same harness and topology as `latency-cost-2026-07-11.md`; loopback baselines match
(open ~2.3 ms, flush ~4.3–4.8 ms), so the cross-day comparison is apples-to-apples.
Image built from `a95481e` (post the 2026-07-23 perf iteration).

## Fleet p50 vs the 2026-07-11 sweep

| one-way | cold-open 07-11 | cold-open now | flush/drain 07-11 | flush/drain now | drain saved |
|---:|---:|---:|---:|---:|---|
| 10 ms | ~24 ms | 26.3 ms | ~70 ms | **53.1 ms** | ~17 ms ≈ 0.9 RTT |
| 30 ms | ~77 ms | 70.3 ms | ~215 ms | **143.1 ms** | ~72 ms ≈ 1.2 RTT |
| 60 ms | ~137 ms | 133.1 ms | ~400 ms | **269.9 ms** | ~130 ms ≈ 1.1 RTT |

## Reading

- **The drain path dropped from ~3.5× RTT to ~2.3× RTT** — review 2026-07-23 **#6** live: the
  lease now carries the lock etag from our own PUT responses, so `release_lease` is a single
  conditional DELETE instead of GET-then-DELETE. The saving is ~1 RTT at every injected
  latency (0.9/1.2/1.1), which is the signature of a removed round-trip. The 2026-07-11 doc's
  attribution of that third round-trip to the `.db.etag` sidecar upload was wrong (the sidecar
  is a local file); the review's corrected accounting predicted ~150 ms at 30 ms — measured
  143 ms.
- **Cold-open is unchanged at ~1 RTT (≈2.2–2.4× one-way)** — 26/70/133 ms vs 24/77/137 ms —
  the overlapped acquire∥pull was untouched, and this doubles as a live no-regression check on
  the fleet build.
- The remaining ~2.3-RTT drain is the data PUT (fenced upload) then the conditional DELETE —
  the PUT→DELETE ordering is irreducible (releasing before the flush lands would let a peer
  pull stale bytes), so this is now within ~0.3 RTT of the floor for the current design.
- Not exercised here: the takeover-chain wins (#13 — measured in-process, see
  `s3-latency-ab-2026-07-23.md`: ~1 RTT off failover cold/warm), the warm-restart open (#22),
  and the warm-follower GET elimination (#15 — a steady-state request-count effect, not a
  single-op latency).
