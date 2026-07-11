# Fathom — warm standby (how the built engine works)

> Status: **BUILT** (Phase 2 A1, `Fathom.Shard.WarmFollower`). Gated `:warm_follower`, **off by
> default** (and off in test). This is the "how it actually works" reference; the RTO / warm-density
> numbers live under Benchmarking in `AGENTS.md` (and `mix fathom.bench --only failover_rto` /
> `mix fathom.scale --warm-density`). Live WAL streaming (A2) — keeping a standby *continuously*
> fresh — is **deferred**; this is the pre-pull-the-hot-set approach.

## The problem it solves

A shard's home is one node (the LB consistent-hash). When that node dies, a survivor has to **cold-
open** the shard: acquire the lease and **pull the whole file from S3**. For a shard of any size on
a real network, that body transfer is the dominant cost of the failover — the tenant is down until
the bytes arrive. Warm standby shrinks that window by having survivors **already hold the bytes**
before the failover happens.

## What it is: a pure read cache that never serves

`Fathom.Shard.WarmFollower` (one GenServer per node) pre-pulls the fleet's recently-active shards
that this node **doesn't own** into a **separate** `warm_cache_dir`. It **holds the file but no
lease, and never serves** — it's a read cache, not a second writer. So the standby role is cheap and
safe: no lease means no chance of a double-write, and a warm copy is just bytes on disk waiting for
a failover that may never come.

- **What it warms:** `Fathom.Directory.active_recent/1` (the fleet's recently-active shards),
  LRU-capped at `:warm_cache_max`, refreshed every `:warm_poll_ms`.
- **What it excludes:** the shards this node **owns** — *and* ones it **recently owned**, for
  `:warm_home_retention_ms` (see the warm-home rule below).

## The freshness problem — a cached copy is never served as-is

A warm copy can **lag** the owner's latest flush (the owner wrote after the follower last pulled),
so serving the cache blindly would serve stale data. The rule: **never promote a cached copy without
revalidating it against the store first.**

- Each warm pull goes through **`Fathom.Shard.Storage.pull_if_changed/3`** — a conditional
  `If-None-Match` GET — and records the object's etag in a **`<shard>.db.etag` sidecar**. The three
  outcomes: **304** → the cache is current, promote it (no body transfer); **200** → the object
  changed, re-pull the fresh bytes; **404** → brand-new shard, nothing to promote.
- To keep the etag current, the follower **revalidates its whole cached set every poll** (a cheap
  conditional GET per shard, no body unless something changed), so at failover the shard is almost
  always on the **304 fast path**.

## The failover path — how the coordinator promotes a warm copy

When a survivor cold-opens a shard (`Fathom.Shard`), it distinguishes two "warm" cases:

1. **A live-dir warm *restart*** — the node's **own** local copy is present (it may hold un-flushed
   writes). This wins **untouched**: the coordinator re-adopts the present file rather than pulling
   (adopting the store's older etag would clobber newer local writes). Only the *follower-cache*
   path is revalidated.
2. **A follower-cache promote** — the file is in `warm_cache_dir`. The coordinator runs the
   freshness check (`pull_if_changed`) before serving: 304 promotes the cache, 200 re-pulls.

Either way the shard is only **served after the lease confirms**, and a fresh pull writes to a
**temp file promoted to the real path only once the lease is held** — so a lost lease race never
leaves a stale local copy.

## The warm-home rule — the home node must NOT warm its own shard

A shard's **home** (the LB-hash target that will route back to it) should never spend cache budget
warming its own shard: a failover *toward* the home is exactly the case where it will cold-open
anyway, and warming a shard that routes back to you is pure waste competing for `:warm_cache_max`.
Only **survivors** warm it. The subtle regression this guards against is a home node **re-warming a
shard it just idle-dropped** (lease released, but it's still the hash target) — so the follower
remembers shards it **recently owned** for `:warm_home_retention_ms` and excludes them.

## Density — a standby warms far more than it can serve

A warm-cached shard costs its **file on disk plus ~0 process / BEAM / fd** (no coordinator, no
connection, no lease). So warm capacity is **disk-bound** (`disk / shard_size`) — orders of
magnitude past the open-shard ceiling (~196 KiB + ~3 fds per *served* shard). `mix fathom.scale
--warm-density` measures it. The practical upshot: a node can stand ready to fail over for **far
more** shards than it could ever hold open at once.

## The honest win (and its limit)

The warm path is **not purely local** — it still pays **one S3 round-trip** (the 304 conditional
GET, plus the lease/freshness round-trips). The win is the object **body transfer avoided**, so it
scales with **shard size × bandwidth-delay** and is **marginal for a tiny shard on a fat pipe**
(both paths pay ~1 S3 RTT regardless). Measured (dev build, MinIO + toxiproxy, relative): at
**1 MB / 30 ms one-way / 100 Mbps → cold ~162 ms vs warm ~72 ms (~2.3×)**. The warm floor is those
lease + freshness round-trips, not ~2 ms — so don't oversell it for small shards.

## How it connects

The follower's per-node warm set is also published as a **warm-location signal**
(`Fathom.Rebalancer.WarmLocations` / `shard_warm_locations`), which the **rebalancer** reads to
prefer a handoff **target that already holds the shard warm** (a cheap 304 handoff instead of a full
pull — see `docs/rebalancing.md`). And the freshness/etag machinery is the same lease-fencing
plumbing the crash path and migration flush use — a warm promote is just its read-only direction.

## One-line summary

Survivors pre-pull the fleet's hot shards into a lease-less read cache and keep each copy's etag
current every poll, so a failover promotes a warm copy after a single conditional (304) round-trip
instead of a full-body S3 pull — cheap to hold (disk-bound, ~0 BEAM per shard), never served stale
(revalidated first), and never warmed by the shard's own home.
