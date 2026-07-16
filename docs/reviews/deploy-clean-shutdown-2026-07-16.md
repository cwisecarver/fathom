# Clean-shutdown (rolling-deploy) verification — the graceful path loses zero writes

**2026-07-16, Apple M-series, macOS 27.0.** The 3-node chaos rig (`fathom1/2/3` behind nginx +
MinIO, one colima VM, prod-release nodes). This closes the deploy half of expert-review #31: the
code had a real graceful-shutdown story (`trap_exit` → settle → drop-flush within
`:shard_shutdown_ms`, lease release, heartbeat cleanup on clean stop), but it was never *measured*
under a high open-shard count — the herd analog of `failover`. A node-by-node upgrade rests on it,
so it needed a live proof, not just unit coverage.

## What it does

`deploy/chaos/chaos.sh deploy [N node]`:

1. **Hold N shards open AND dirty on one node.** Disable idle-drop and the periodic flush on the
   target (`shard_idle_ms` / `shard_flush_interval_ms` → 1 h), then seed N shards directly on it
   (bypass the LB so they all home there), each with a committed `seq=999`. With both flushes
   disabled, the **only** thing that can persist those writes is the graceful-terminate flush — so
   survival is an unambiguous proof of the terminate path, not an idle/periodic flush that happened
   to fire first.
2. **`docker stop -t 120`** the node — SIGTERM → the BEAM's graceful `init:stop` → the supervision
   tree terminates → each coordinator's `terminate` flushes its shard to S3 and releases its lease.
   `-t 120` so docker never premature-SIGKILLs; the app's own `:shard_shutdown_ms` (60 s) governs.
   Measure the wall time.
3. **Verify every committed row survived** via a *survivor's* cold-open from S3 (`sql_direct` to
   another node). Because clean shutdown **released** the leases, the survivor acquires immediately —
   no TTL wait (contrast a crash, which waits out the lease).

Contrast `chaos.sh failover`, which SIGKILLs the owner (the crash path) and loses committed-but-
unflushed writes — the RPO window. `deploy` is the *voluntary* path a rolling upgrade uses.

## Result

| Open dirty shards | Graceful stop time | Per-shard | Committed writes lost |
|---|---|---|---|
| 200 | 1498 ms | ~7 ms | **0 / 200** |
| 500 | 1697 ms | ~3 ms | **0 / 500** |

- **Zero loss at both sizes.** Every one of the 200 / 500 open dirty shards was flushed to S3 by the
  graceful terminate and re-read intact on a survivor. The clean-shutdown durability guarantee holds
  under a high open-shard count.
- **Sub-linear stop time** (2.5× the shards, +13% wall time) confirms coordinators flush
  **concurrently**, bounded by the S3 client pool (200 connections) and BEAM scheduling — shutdown
  is flush-*throughput* bound, not per-shard-serial. So `:shard_shutdown_ms` (60 s default) has large
  headroom: even ~10k open shards through the default pool is ~50 concurrent waves.

## Caveats (the usual chaos-rig honesty)

- **Loopback MinIO — relative, not prod-absolute.** No injected S3 RTT here. On real S3 the
  concurrent flush scales with `open_shards / pool_size × RTT`; at a 30 ms region RTT, ~10k shards is
  a few seconds of S3 time — still far inside 60 s. Size `Fathom.Shard.Storage.S3` `pool_size` and
  `:shard_shutdown_ms` together for the densest node, and set the orchestrator's kill grace
  (`docker stop -t`, `terminationGracePeriodSeconds`, `TimeoutStopSec`) above `:shard_shutdown_ms` or
  the platform SIGKILLs mid-flush and defeats the guarantee.
- **One process per shard in prod.** The seed loop opens shards serially from one driver; the
  measured *shutdown* is the real concurrent flush, which is what this run is about.

## What this backs

`docs/runbooks/deploy.md` (the node-by-node upgrade procedure + `:shard_shutdown_ms` sizing) cites
these numbers. The property proven: **a graceful restart flushes every open shard before exiting**,
so a rolling deploy is safe node-by-node — the LB reroutes, the survivor cold-opens from S3 and
acquires the released lease immediately, and no committed write is lost.
