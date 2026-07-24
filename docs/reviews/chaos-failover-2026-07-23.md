# Kill-failover revalidation — 2026-07-23 (post perf-iteration + LB-502 fix)

`chaos.sh failover fokill` with 30 ms injected S3 latency each way — the same conditions
as the 2026-07-05 reference run. Image from post-iteration `main` (incl. the lease
lock-etag release, the takeover confirm-HEAD removal, the heartbeat memo, the overlapped
fork check, and the fixed LB config).

| | 2026-07-05 | 2026-07-23 |
|---|---:|---:|
| time to first acked write on a survivor | 15.0 s | **14.6 s** |
| flushed pre-kill write survived | yes | **yes** (rows pre=1 → post=2, no loss beyond RPO) |
| new owner | survivor steal | fathom1 → fathom2, clean steal |

- **The loss window still holds**: the durably-flushed pre-kill row survived the silent
  kill, and the first survivor write landed on top of it — the RPO contract (lose at most
  the un-flushed window, never a flushed write) intact through every storage-path change
  in the 2026-07-23 iteration.
- **The failover time is TTL-bounded, as designed** (~14.6 s ≈ heartbeat expiry + steal
  margin, matching 07-05's 15.0 s): the iteration's ~2-RTT takeover savings are real
  (`s3-latency-ab-2026-07-23.md`) but sit inside a window dominated by the deliberate
  liveness wait, so the wall-clock is statistically unchanged — exactly what should
  happen. Shrinking the window itself is the `:shard_lease_ttl_ms`/`steal_margin_ms`
  trade documented in `docs/single-writer.md`, not an RTT problem.
- Survivor opened cold (`:warm_follower` off by default, as in 07-05).
