# S3 latency A/B — 2026-07-23 perf iteration (RTT-path wins)

**Setup:** `scripts/benchmark_s3_latency.sh` (MinIO + toxiproxy, injected one-way latency,
jitter 5 ms, no bandwidth cap), `--warm-size-kb 1024`, dev build — a **relative** lever per
the benchmarking rules, not prod-absolute. A/B between `c6f8603` (pre-iteration baseline,
clean worktree) and `4b7c16c` (post-iteration), same rig, back-to-back runs.

## Results (p50, ms)

| metric | one-way | c6f8603 | 4b7c16c | delta |
|---|---|---:|---:|---|
| cold_open_s3 | 30 ms | 76.7 | 79.3 | +2.6 (noise; already ~1 RTT) |
| failover_cold_s3 | 30 ms | 684.9 | 607.0 | **−77.9 (−11.4%) ≈ 1.3 RTT** |
| failover_warm_s3 | 30 ms | 686.0 | 619.4 | **−66.5 (−9.7%) ≈ 1.1 RTT** |
| cold_open_s3 | 60 ms | 138.7 | 140.4 | +1.7 (noise) |
| failover_cold_s3 | 60 ms | 1224.6 | 1097.9 | **−126.7 (−10.3%) ≈ 1.06 RTT** |
| failover_warm_s3 | 60 ms | 1219.4 | 1090.6 | **−128.8 (−10.6%) ≈ 1.07 RTT** |

## Reading

- **The takeover chain lost ~1 RTT, and the saving scales with the injected RTT**
  (≈1.1–1.3× at 30 ms, ≈1.05–1.1× at 60 ms) — the signature of a removed round-trip, not
  noise. Attribution: review 2026-07-23 **#13(b/c)** — the confirm-rotation HEAD is skipped
  when the steal touch's own write response proves the rotation (the bench's seeded object
  carries a single-form etag, so every sample takes the multipart-touch direction whose
  post-etag now comes from the CompleteMultipartUpload body).
- **#13(a) (the per-owner heartbeat memo) is deliberately NOT exercised here** — the bench
  mints a unique synthetic dead owner per sample, so the 1 s memo never hits. Its win is a
  mass-failover effect (N steals from ONE dead owner) — a fleet-rig scenario.
- **Cold open is unchanged** at ~1 RTT + a few ms (76–79 ms at 30 ms; 137–140 ms at 60 ms —
  matching the historical `benchmark_s3_sweep.sh` curve), as designed: none of the iteration
  touched the overlapped acquire∥pull.
- **Warm ≈ cold on this rig** because with no bandwidth cap the 1 MB body transfer the warm
  path avoids is ~free on loopback; the warm win is bandwidth×size, per the failover-RTO
  bullet in AGENTS.md.
- **Not covered by these metrics:** the drain path (#6, 3→2 RTTs — the flush tuning target)
  and the warm-restart open (#22, 3→2 RTTs). The in-process harness has no drain/warm-restart
  S3 timing; the lever is a `deploy/chaos/chaos.sh latency-cost` re-run, where #6 should show
  as flush ~215 → ~150 ms at 30 ms one-way against the 2026-07-11 sweep.
