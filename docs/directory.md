# Fathom — the directory & the off-hot-path recorder

> Status: **BUILT.** `Fathom.Directory` is the Postgres control plane the migration / rebalancing /
> warm-standby machinery reads; `Fathom.Directory.Recorder` is how per-checkout access is recorded
> into it **without ever touching the shard data hot path**.

## The problem it solves

The directory is the fleet's **source of truth** in Postgres — each shard's `schema_version`,
lifecycle `status`, `last_active_at`, `retain_until`, `cutover_at`. That data drives real decisions
(which shards are behind a migration, which were recently active enough to warm, whether a revert
would discard post-cutover writes). But it must **never be on the shard data path**: recording "shard
X was just accessed" as a synchronous Postgres upsert on **every checkout** would put a Postgres
round-trip in front of every query — latency on the hot path, and a Postgres outage would start
**failing checkouts**. A control plane must not be able to take the data plane down.

## The recorder — coalesce in ETS, batch-flush off the hot path

`Fathom.Directory.Recorder` (a GenServer over a public ETS table) decouples the two:

- **`record/1` is one lock-free `:ets.insert`** — microseconds, no Postgres, no GenServer mailbox.
  The table is a `:set` keyed by `shard_id`, so a shard hit 1000×/s **coalesces to a single buffered
  row** (`{shard_id, last_seen}`), not 1000 rows.
- **A periodic flush** (default 1s) drains the buffer with `:ets.take` (read + delete in one atomic
  op) and **batch-upserts** every touched shard in one chunked `Repo.insert_all`
  (`Directory.record_batch/1`) — one round-trip per interval for the whole node, not one per
  checkout.
- **A Postgres outage drops a *flush*, never a *checkout*.** The `record/1` ETS insert already
  succeeded, so the request never blocked on or failed from Postgres; and a *failed batch*
  **re-buffers** the drained touches (`:ets.insert_new`, so a fresher touch already back in the
  table wins) to retry next cycle. Writes are fire-and-forget from the caller's view; the worst case
  is a slightly stale `last_active_at`, never a dropped request.
- **A second, parallel buffer records durable-flush times** (review #28). Hooked at each successful
  upload, `record_flush/1` coalesces and batch-flushes `last_flushed_at` the same off-hot-path way
  (`Directory.record_flush_batch/1`), so a post-node-loss report survives the node even though the
  node-local flush watermark dies with it — see [durability](durability.md).

This is the same shape as `Fathom.ShardLoad` (lock-free ETS + a periodic drain — the "record off the
hot path" pattern, used for access recency, durable-flush time, and per-shard load).

## What the directory holds, and who reads it

The recorder keeps `last_active_at` fresh; the rest of the directory API is the read/flip surface the
control plane uses — all **without opening shards**:

- **`schema_version`** + **`laggards/2`** — find shards behind the migration HEAD for the reconcile
  sweep ([migration](migration.md)).
- **`last_active_at`** + **`active_recent/1`** — the recently-active set the warm follower pre-pulls
  ([warm-standby](warm-standby.md)); and, versus `cutover_at`, the **write-age** the revert
  force-guard checks (`last_active_at > cutover_at` ⇒ writes since cutover).
- **`status`** (`active` / `migrating` / `retired` / `migration_failed`, plus `deleted` / `suspended`
  from the [tenant lifecycle](tenant-lifecycle.md)) + **`cutover/2`**, **`retire/2`** — the lifecycle
  states: the migration engine flips the first four; `Fathom.Tenants` sets `deleted` / `suspended`.

It's deliberately **decoupled from routing**: the directory is orchestration, not a request-path
resolve (routing is Host-based today — see [admission](admission.md)). That's why a Postgres blip is
survivable: nothing in the serving path waits on it.

## One-line summary

The directory is the Postgres control plane (schema version, status, activity, retention) that the
migration / rebalancing / warm-standby machinery reads without opening shards — fed by a recorder
that turns each checkout into one lock-free ETS insert and batch-flushes off the hot path, so a
Postgres outage costs a stale timestamp, never a request.
