# Runbook — deploying / upgrading a fathom node

How to roll a new fathom version (or restart a node for config) across the fleet without losing
tenant data. Companion incident runbooks: [`cluster.md`](cluster.md) (lease/ownership),
[`operations.md`](operations.md) (Postgres/disk/heartbeat/LB). Architecture:
[`../deploy-cluster.md`](../deploy-cluster.md).

The whole procedure rests on one property, proven live below: **a graceful (SIGTERM) shutdown
flushes every open shard to S3 before exiting — zero committed-write loss.** A rolling deploy is
therefore node-by-node graceful restarts; the S3 lease makes the reroute safe.

## The clean-shutdown guarantee (measured)

On `SIGTERM` (what `docker stop`, `kill`, systemd, and an orchestrator drain all send), the BEAM runs
a graceful `init:stop`: the supervision tree terminates in reverse start order, each shard
coordinator's `terminate` **flushes its shard to S3** (within its `:shard_shutdown_ms` budget,
default 60 s) and **releases its lease**, and the node heartbeat is cleaned up last. Coordinators
flush **concurrently** (bounded by the S3 client pool), so shutdown time is dominated by flush
throughput, not per-shard cost.

Verified on the chaos rig (`deploy/chaos/chaos.sh deploy <N>`) — N shards held **open and dirty**
(idle-drop + periodic flush disabled, so *only* the terminate path can persist them), then SIGTERM,
then every committed row checked via a survivor's cold-open from S3
([`../reviews/deploy-clean-shutdown-2026-07-16.md`](../reviews/deploy-clean-shutdown-2026-07-16.md)):

| Open dirty shards | Graceful stop time | Per-shard | Committed writes lost |
|---|---|---|---|
| 200 | ~1.5 s | ~7 ms | **0 / 200** |
| 500 | ~1.7 s | ~3 ms | **0 / 500** |

Sub-linear (2.5× the shards, +13% time) confirms the concurrent flush. Contrast `chaos.sh failover`,
which SIGKILLs a node and loses committed-but-unflushed writes (the RPO window) — that is the crash
path, not the deploy path.

> These are loopback-MinIO numbers (relative, not prod-absolute). On real S3 the concurrent flush
> scales with `open_shards / pool_size × RTT`: ~10k shards through the default 200-connection pool is
> ~50 waves; at a 30 ms region RTT that's a few seconds of S3 time — still far inside the 60 s
> `:shard_shutdown_ms`. Size the pool (`Fathom.Shard.Storage.S3` `pool_size`) and the budget together
> for your densest node.

## Sizing `:shard_shutdown_ms`

`:shard_shutdown_ms` (default 60 s) is each coordinator's budget to **settle + drop-flush** on
shutdown; `settle_yield_ms` is capped at a third of it. It must cover the worst-case flush of a
node's open shards. Rule of thumb:

```
shard_shutdown_ms >= (max_open_shards / s3_pool_size) × p99_flush_ms × safety_factor(2–3)
```

With the default 60 s and a 200-connection pool, that covers ~10k open shards at a p99 flush well
into the hundreds of ms. Raise it if you run very dense nodes or slow storage; lower it only if you
accept SIGKILL-ing stragglers (which lose their unflushed writes). **Also raise the orchestrator's
kill grace** (`docker stop -t`, `terminationGracePeriodSeconds`, systemd `TimeoutStopSec`) above
`:shard_shutdown_ms`, or the platform SIGKILLs the node mid-flush and defeats the guarantee.

## Rolling upgrade — node by node

Do one node at a time; never restart a quorum at once (the survivors absorb each node's shards).

1. **Pre-checks.** Fleet green (`/health` per node, `fathom_shards_active` reasonable, no active
   `FathomSelfFenceChurn` / `FathomLeaseStoreDown` alerts). Confirm the new image boots in staging /
   the eval stack.
2. **Bleed traffic off the node (optional but recommended).** Lower its LB weight or mark it down in
   the LB so new streams stop landing on it, and let in-flight streams finish. Passive-health LBs
   also reroute on the restart itself (`proxy_next_upstream`), so this step reduces client-visible
   blips but isn't strictly required — the clean-shutdown flush protects data regardless.
3. **Restart the node** with the new image (graceful SIGTERM → the flush+release above → start the
   new version). Give the platform a stop grace ≥ `:shard_shutdown_ms`.
4. **What tenants on that node experience:** their subdomains reroute to a survivor, which
   cold-opens the shard from S3 and acquires the (already-released) lease **immediately** — no TTL
   wait, because the graceful shutdown released it (contrast a crash, which waits out the lease TTL).
   First request after the reroute pays one cold-open (~1 S3 RTT). Enable the warm-standby follower
   (`WARM_FOLLOWER`) to serve that from a 304 instead of a full pull.
5. **Verify** the node rejoined healthy and is taking its share of shards again (its subdomains
   re-home on the next touch, or immediately if you restored its LB weight). Watch
   `fathom_shard_lease_superseded_count` stays quiet (a spike means churn — pause the rollout).
6. **Proceed to the next node** only after the previous one is healthy and its shards have re-homed.

Roll **backward** the same way — node by node — since there is no BEAM cluster, no shared app state,
and the shard format is unchanged within a release line. If a release includes a Postgres migration,
see below.

## Config changes — restart or not

- **Runtime env in `config/runtime.exs`** (storage, TTLs, caps, auth, rebalancer knobs): read at
  boot, so a change needs a **restart** (the same graceful, one-node-at-a-time procedure). A few
  knobs can be flipped live via `bin/fathom rpc 'Application.put_env(...)'` for an emergency (e.g.
  raising `:max_open_shards`), but treat that as temporary — the next restart reverts to the env.
- **Compile-time config** (`config/prod.exs`, e.g. `WEB_INSECURE_LOCAL`, `force_ssl`): needs a
  **rebuild**, not just a restart.
- **Postgres migrations** shipped with a release: run `bin/fathom eval Fathom.Release.migrate` once
  (from one node / a migrate step) before or as part of the rollout. Directory migrations to date are
  additive/backward-compatible, so a mixed-version fleet tolerates them; keep it that way (an old node
  must run against the new directory schema during the roll).

## Removing a node (drain)

**Status: partially manual (a known gap).** A voluntary drain of a node's *busy* shards to specific
targets is the rebalancer's cross-node command path (`docs/runbooks/rebalancer.md`) and isn't a
one-command "retire this node" yet. Today:

1. Mark the node **down in the LB** and remove it from the upstream / backend set (`LB_BACKENDS`) so
   no subdomain hashes to it.
2. **Gracefully stop it** — the clean-shutdown flush persists and releases every open shard.
3. Its subdomains re-home on survivors on next touch (immediate lease acquire, since it released).
4. Remove it from the fleet. The consistent hash remaps ~1/N subdomains to the remaining nodes.

Because the flush + release happened, there is no data stranded on the removed node — everything is
in S3. The gap is only *automation* (a single drain command), not correctness.

## Compatibility

- **fathom ↔ Filo.** Filo (the Hrana wire server) is currently a **path dependency**
  (`{:filo, path: "../filo"}`) — publishing it is review #9 (pending), so there is no published
  version matrix yet. Build-time truth: **fathom 0.2.0 builds against filo 0.1.0** (the tree checked
  out next to fathom). Pin both repos to a known-good pair in CI until Filo is published; see
  `CHANGELOG.md` for the pair each fathom release was cut against.
- **libSQL clients.** Unchanged `django-libsql` (WebSocket) and `libsql-experimental` / SDKs (HTTP)
  work across a rolling deploy; a reconnect after a node restart re-opens a stream transparently.
