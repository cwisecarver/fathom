# Fathom — warm standby (REMOVED 2026-09-14)

> **This feature was removed.** The warm-standby follower (Phase-2 A1, `Fathom.Shard.WarmFollower`)
> is gone, superseded by A2 replication. This file is a tombstone; the full how-it-works writeup
> lives in git history (the `refactor`/`docs` commits around 2026-09-14 and everything before).

## What it was

A lease-less, read-only cache of the fleet's recently-active shards, kept on each node so a failover
could promote a cached copy (after a 304 freshness check) instead of doing a cold S3 pull. It cut
failover **RTO**. It never addressed **RPO** — a promoted warm copy was only as fresh as the last
flush it had pulled.

## Why it was removed

A2 replication (`docs/a2-quorum-replication.md`, on by default in prod) closes the node-loss RPO gap
that warm standby could not, and it makes the RTO win largely redundant: the rebalancer's
affinity signal now prefers a handoff target that already holds an **A2 replica**
(`Fathom.Shard.Replication.Follower.replica_shard_ids/1`), and A2 promote-on-open recovers from that
replica without a full cold pull. Maintaining two failover-promotion mechanisms that shared the
`Promote.fresher?` freshness concept but not an implementation was not worth it.

## What the removal touched

- Deleted `Fathom.Shard.WarmFollower` and its supervision child.
- `Fathom.Shard.Materializer` no longer has a warm fast path — `start_pull/2` always cold-pulls.
- The rebalancer affinity signal (`shard_warm_locations`) was **re-sourced** from the A2 replica
  set instead of the warm cache — affinity itself is kept.
- The handoff `warm` pre-warm command was dropped (`drain` is now the only command type). It was
  always best-effort; correctness was the target's cold-open.
- All `WARM_*` env vars, the `warm` disk gauge + back-pressure, the warm telemetry metrics, the
  `FathomWarmCacheDiskPressure` alert, and the `mix fathom.scale --warm-density` /
  `warm_s3`/`failover_warm` bench dimensions are gone.

## If failover RTO ever needs a read cache again

Fold it into the A2 follower's read path rather than reviving a second, separately-fed cache — that
was the intended end state noted when this was retired.
