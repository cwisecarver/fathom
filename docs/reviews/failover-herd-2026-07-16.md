# Failover herd at density — the whole-node re-home, measured

**2026-07-16, Apple M-series, macOS 27.0.** The 3-node chaos rig (`fathom1/2/3` behind nginx +
MinIO, one colima VM, prod-release nodes). Closes expert-review #39: the chaos rig measured
*single-shard* failover time, never the **herd** — a node death re-homes *all* its tenants onto
survivors at once, and nobody had measured time-to-last-tenant-served, S3-throttling behavior, or the
warm-standby benefit at that scale.

## What it does

`deploy/chaos/chaos.sh failover-herd [shards warm]`:

1. Seed N shards on `fathom1`, each a committed row; wait a durability flush (a kill loses nothing).
2. `warm=on` (default) — wait two follower poll cycles so the survivors pre-warm `fathom1`'s shards.
   `warm=off` — disable the survivors' warm follower and clear their warm cache, forcing every
   re-home to cold-open (full S3 pull).
3. **Silent-kill** `fathom1` and fire **all N requests through the LB concurrently**, each recording
   its own time-to-served from the kill. Report the distribution (p50/p90/p99/max) + served/N.

The LB reroutes `fathom1`'s subdomains to a survivor (passive health + `proxy_next_upstream`); the
survivor cold-opens (or 304-promotes the warm copy) and **steals the lease once `fathom1`'s
heartbeat lapses** — TTL (10s) + steal margin (5s) here, so ~15s is the unavoidable floor for a
silent kill.

## Result (N=300)

| Config | served | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| `warm=on` | **300/300** | 15.05 s | 16.09 s | 16.30 s | 16.32 s |
| `warm=off` | **300/300** | 15.60 s | 16.66 s | 16.87 s | 16.89 s |

- **The herd re-homes cleanly.** All 300 tenants served in both configs — no failures, no S3-throttle
  collapse, no survivor memory blow-up. The survivors absorbed 300 concurrent cold-opens through the
  200-connection Finch pool.
- **The floor is the lease, not the herd.** ~15 s = `SHARD_LEASE_TTL_MS` + steal margin — the
  time a silent-killed owner's shards stay unstealable while its heartbeat lapses. This dominates
  time-to-served for a crash. (A *graceful* stop releases the lease immediately — no floor — see
  `deploy-clean-shutdown-2026-07-16.md`.)
- **The concurrent cold-open spread is small** — ~1.3 s (warm) / ~1.3 s (cold) from p50 to max across
  300 shards. The pool handles the herd without a long tail.
- **Warm saved ~0.5 s here** (p50 15.05 vs 15.60). It's marginal for *tiny* shards because the warm
  win is the object **body transfer avoided**, ~0 for a one-row shard — both paths still pay the
  lease floor + ~1 S3 RTT. The warm benefit **scales with shard size × bandwidth-delay** (the
  single-shard bench: 1 MB / 30 ms / 100 Mbps → cold ~162 ms vs warm ~72 ms).

## Caveats

- Loopback MinIO — relative, not prod-absolute (no injected S3 RTT; run `chaos.sh latency 30` first
  for a real-RTT floor). N=300, not 10k: the density work showed the one-process open-rate decays
  past ~30k/node (a test-driver artifact, not a fathom limit), and the herd shape (clean re-home,
  lease-floored, small pool-bound spread) is what generalizes, not the absolute count.
- The lease floor is tunable: lower `SHARD_LEASE_TTL_MS` for faster crash failover (at the cost of
  more false steals under clock skew / load).

## Sizing guidance (added to `docs/runbooks/cluster.md`)

The herd is safe by construction (single-writer lease, bounded pool). The warm follower is a
**latency** optimization for the failover tail, and it's worth enabling when the cold-pull herd
would move real bytes: roughly when `active_shards_per_node × avg_shard_size / S3_bandwidth`
approaches your failover-RTO budget. Rule of thumb + the `:warm_cache_max` sizing are in the cluster
runbook.
