# Runbooks — fathom operations (non-lease incident classes)

The lease/ownership incidents (stuck-unavailable, split-brain, S3 down) live in
[`cluster.md`](cluster.md); rebalancer incidents in [`rebalancer.md`](rebalancer.md); node
upgrade/restart in [`deploy.md`](deploy.md). This file covers the **second-most-likely** outage
classes — Postgres, disk, heartbeat, and LB misconfiguration — plus the shard restore drill.

Read the dependency fail-mode matrix first: fathom deliberately fails **open** for some
dependencies and **closed** for others, and the right on-call response depends on which.

## Dependency fail-mode matrix

What happens to each subsystem when a dependency is unavailable. "Fail closed" = refuses to act
(unavailable but safe); "fail open" = proceeds without the check (available but degraded).

| Dependency down | Data path (read/write) | Durability (flush) | Admission (novel limiter) | Control plane (Oban) | Net posture |
|---|---|---|---|---|---|
| **S3** (lease store + bottomless) | **Fail closed** — no new opens/renews; already-open shards serve until their lease lapses (≤ TTL) | Flushes fail; RPO grows (unflushed age rises) until S3 returns | Existing shards unaffected; novel opens block on S3 | Jobs that touch storage retry/fail | **Closed** — safe, availability degrades |
| **Postgres** (directory / control plane) | **Fail open** — data path keeps serving (no directory resolve on the hot path) | **Fail open** — `Directory.Recorder` **drops** the flush buffer, never a checkout; durability of shard *data* is unaffected (that's S3) | **Weakened** — the directory "is this shard pre-existing?" check **fails open**, so novel-shard rate limiting is effectively relaxed during the outage; the per-node open cap and local-file/running-coordinator checks still hold | **Stalls silently** — migrations, reconcile, rebalancer, retirement, tenant delete/suspend jobs stop; delete/suspend broadcasts don't propagate to other nodes | **Open** — available, control degrades |
| **LB** (one instance) | Its share of subdomains is unroutable until failover/repair | Unaffected | Unaffected | Unaffected | Availability only |
| **LB config** (two LBs / stale hash) | Double-routing: two nodes may be told they own a subdomain | **Safe** — the S3 `{owner,epoch}` lease still guarantees single-writer; the loser self-fences | Unaffected | Unaffected | **Closed on writes** — churn, not corruption |

The through-line: **data corruption is never a failure mode.** The S3 lease arbitrates every
write regardless of what Postgres, the LB, or a second LB believe. Postgres and LB failures cost
*availability* or *control-plane progress*, not tenant data.

---

## Runbook: Postgres (directory / control plane) is down

**Symptom:** the Phoenix dashboard (`:4000`) errors; Oban jobs stop advancing (no migrations,
reconcile, rebalancer moves, retirement, tenant delete/suspend); `[:fathom, :directory, :flush_retry]`
rises. **The Hrana data path keeps serving** — clients see no error, because request→shard is
Host-based and does not resolve through Postgres.

**What is and isn't happening:**
- **Serving is fine.** No directory read is on the hot path. Shard *data* durability rides S3, not
  Postgres.
- **`Directory.Recorder` drops its buffer** (coalesced access records / `last_flushed_at`): a
  Postgres outage loses *access-time bookkeeping*, never a checkout or a shard byte. Expected and safe.
- **Novel-shard rate limiting is weakened.** The limiter's "already exists in the directory?" check
  **fails open** — during the outage an unseen id may not be recognized as pre-existing and the
  directory arm of the gate is relaxed. The per-node `:max_open_shards` cap and the local-file /
  running-coordinator checks still bound novel opens. *Security note:* if the data path is exposed
  to untrusted callers and you rely on `NOVEL_SHARD_RATE` to bound shard minting, a prolonged
  Postgres outage relaxes that specific control — keep the network trust boundary (LB-only
  reachability) as the primary defense, never the limiter alone.
- **Lifecycle broadcasts don't propagate.** Delete/suspend push over Oban's Postgres notifier. A
  *new* delete/suspend issued during the outage won't reach other nodes until Postgres returns;
  already-known tombstones/suspensions stay enforced (they're in per-node ETS). A deleted tenant
  could briefly still be served on a node that missed the broadcast.

**Diagnose:**
1. Confirm it's Postgres (dashboard down, Oban idle) and not a node — the Hrana path should still
   answer.
2. Check `DATABASE_URL` reachability, connection pool exhaustion (`POOL_SIZE`), and Postgres health.

**Respond:**
- Treat as a **control-plane** incident, not a data incident. Do **not** drain nodes or touch shards.
- Restore Postgres. On recovery: Oban resumes; the reconcile sweep converges the cold tail; the
  `last_flushed_at` / access records simply resume (the dropped window is not backfilled — acceptable).
- If the outage was long and the data path is untrusted-reachable, review whether any novel shards
  were minted past your intended `NOVEL_SHARD_RATE` during the weakened window (`Fathom.Directory`
  row-creation timestamps).

---

## Runbook: node disk full (`SHARD_DATA_DIR`)

**Symptom:** on one node, cold-opens fail (pull can't write the temp file), flushes/checkpoints
fail, new-shard opens error; `checkout` outcome `error` on that node only; `df` on
`SHARD_DATA_DIR` (and `WARM_CACHE_DIR` if the warm follower is on) near 100%.

**What is and isn't happening:**
- `SHARD_DATA_DIR` holds the **working copy** of every open shard (+ `-wal`/`-shm`). It grows with
  open-shard count × shard size, not with total tenants — an idle shard's bytes live in S3, not here.
- The **warm follower** (`WARM_FOLLOWER=true`, `WARM_CACHE_DIR`) pre-pulls foreign shards and is a
  large, *elastic* consumer: warm capacity is disk-bound by design. A full disk is often the warm
  cache, not the served set.
- A failed **pull** stops the coordinator with `{:shutdown, _}` (not restarted); the checkout maps
  to `{:error, _}` — the tenant is down on this node, but nothing is corrupted (the temp file is
  promoted only after a complete pull + lease confirm).
- A failed **flush** leaves the write unflushed; the RPO gauge (`oldest_age_ms`) rises. The local
  file remains authoritative on wake, so a subsequent successful flush recovers it — **no data loss
  unless the node is also lost** before the disk is freed.

**Diagnose:**
1. `df` the data dir and the warm cache dir. Which is the consumer?
2. Count open shards on the node (`fathom.shards.active`) vs the measured per-shard footprint
   (`docs/reviews/fleet-density-2026-07-10.md`).

**Respond:**
- **Fastest relief:** if the warm follower is the consumer, lower `WARM_CACHE_MAX` (or set
  `WARM_FOLLOWER=false` on that node and restart) — the warm cache holds no lease and serves
  nothing, so dropping it is non-disruptive.
- **Shed served load:** idle shards flush-and-drop on their own (`SHARD_IDLE_MS`); to force it,
  lower the LB's weight for that node so new streams land elsewhere, then let the idle set drain.
  The soft cap's LRU idle-eviction (`:evict_idle_at_capacity`) also frees idle shards under pressure.
- **Never** delete `SHARD_DATA_DIR` files by hand while a coordinator holds one open — an
  un-flushed working copy is the only fresh copy. Drain first (stop the node cleanly — see
  `deploy.md`), which flushes everything, then the local files are safe to reclaim.
- **Prevent:** size the disk for `max_open_shards × p95 shard size` plus warm-cache headroom; alert
  on disk %; set `SHARD_MAX_PAGE_COUNT` so one runaway tenant can't inflate the working set.

---

## Runbook: heartbeat object lost or deleted

**Symptom:** one node's shards become stealable by survivors even though the node is alive; you see
lease steals / `fathom.shard.lease.acquired` on other nodes for shards this node still holds, and
possibly `[:fathom, :shard, :heartbeat, :lapsed]` on the affected node.

**Background:** liveness is **one object per node** — `heartbeat/<node_key>`, renewed every
`ttl/3` by `Fathom.Shard.Heartbeat` (O(nodes), not O(shards)). A shard's owner is considered live
iff its node's heartbeat is fresh. If that single object is deleted, lost, or its writes start
failing, **every shard homed on that node looks dead** to the rest of the fleet.

**What is and isn't happening:**
- A missing/stale heartbeat makes the node's shards *stealable*, but the **`{owner,epoch}` lease
  still fences writes**: if a survivor steals, the original node **self-fences** on its next flush
  (heartbeat-valid-for-write check fails → re-check lock → superseded → stop without flushing).
  So the worst case is **committed-but-unflushed loss for shards that were mid-write during the
  window** (the standard steal RPO boundary), not corruption.
- If the heartbeat writes are *failing* (not deleted) because the node can't reach S3, this is
  really the **S3 partition** case — see `cluster.md` "lease store down" / the partition runbook.

**Diagnose:**
1. Read `heartbeat/<node_key>` in S3 — present and fresh (`expires_at_ms` in the future)? Missing or
   stale?
2. Is the node actually alive and reaching S3 (can it PUT other objects)? Distinguish "object
   deleted out from under a healthy node" from "node can't write S3."
3. Check for **`node_key` collisions** — two nodes sharing a `NODE_KEY` renew/overwrite the same
   heartbeat and confuse liveness. Each node needs a unique stable `NODE_KEY`.

**Respond:**
- **Object deleted, node healthy:** the node re-creates its heartbeat on the next renew tick (≤
  `ttl/3`); liveness self-heals. If steals already happened, the stolen shards are now correctly
  owned by the survivors and the original self-fenced — let ownership settle; do not fight it.
- **`node_key` collision:** give each node a unique `NODE_KEY` and restart the offenders. This is a
  config bug, not an S3 incident.
- **Bucket lifecycle/expiry misconfig deleting heartbeats:** exclude the `heartbeat/` prefix (and
  `.lock`) from any bucket expiration/lifecycle rule — only stale *shard version* objects
  (`@snap-*`, retired `@vN`) should ever be lifecycle-expired.
- **Prevent:** alert on `[:fathom, :shard, :heartbeat, :lapsed]` (mass self-fence precursor) and on
  bucket delete/lifecycle events touching `heartbeat/`.

---

## Runbook: dual-LB / stale LB config double-routing

**Symptom:** a subdomain flaps between two nodes; sustained `fathom.shard.lease.superseded`
(self-fences) and `fathom.shard.lease.acquired` (steals) for the same shards; clients see brief
per-remap unavailability; data stays correct.

**Background:** the LB consistent-hashes `Host` → one node. If **two** LBs (or a stale + a fresh
config) hash the same subdomain to **different** nodes, both nodes are told they own it. The S3
lease still guarantees single-writer — the loser self-fences — so this is a **churn/availability**
problem, never corruption. This is the operational hazard behind the design's "one authoritative LB
config" rule.

**Diagnose:**
1. Enumerate every LB / ingress in front of fathom. Are there two? A blue/green LB pair mid-cutover?
   A CDN plus an origin LB with different hash configs?
2. Do they use the **same** hash key (`hash $host consistent`) and the **same** backend set/order?
   A different backend order changes the hash ring and remaps a fraction of subdomains.
3. Is one config stale (a removed/replaced node still in one upstream)?

**Respond:**
- Reconcile to **one authoritative** LB config and hash ring. If you must run two LBs (HA pair),
  they must share identical `upstream` blocks and the rebalancer's rendered exception map
  (`LB_MAP_PATH` → the same `exceptions.conf`, distributed to both).
- Until reconciled, expect churn but **no data loss** — the fence holds. Confirm with the isolation
  test (S6) if in doubt.
- **Prevent:** treat the LB config as versioned artifact; a single source renders both HA LBs;
  alert on `fathom.shard.lease.superseded` sustained > 0 fleet-wide (see `cluster.md`).

---

## Runbook: restore a shard from S3 (disaster-recovery drill)

**Symptom / trigger:** a node was lost with un-flushed writes and you need to inspect the last
durable state; or a routine DR drill to prove the bottomless copy is openable; or investigating a
suspected-corrupt shard.

**Background:** a tenant *is* one SQLite file. Its authoritative copy is the live S3 object
(`<prefix><shard>.db`). `mix fathom.shard` (review #14) is the operator toolbox; every flush is
`quick_check`-verified before upload (#41-adjacent), so a stored object is an openable database.

**Do the drill:**
1. **Inspect** (the restore drill: pull + `quick_check` + row counts) —
   `mix fathom.shard inspect <shard_id>`. This is the canonical "is the durable copy good?" check;
   run it periodically per the `inspect` design as a restore drill, not only during an incident.
2. **Pull** a local copy — `mix fathom.shard pull <shard_id> [dest.db]` — then open it with any
   SQLite client for ad-hoc queries / export.
3. **Post-node-loss loss accounting** — `mix fathom.shard loss-report` lists shards active since
   their last durable flush with per-tenant loss windows (backed by the persisted `last_flushed_at`,
   review #28), so you can tell tenants exactly what window, if any, was lost.
4. **Export** (portability / hand to a tenant) — `Fathom.Tenants.export/1` or
   `GET /admin/tenants/:id/export` pulls the durable object and serves it as a download.

**Notes & limits:**
- There is **no point-in-time / historical restore** — each flush overwrites the single live object
  (snapshots/PITR are the not-yet-built #12/#13 scope). Restore means "the last durable state,"
  bounded by the RPO window (flush cadence + any unflushed loss on node death).
- If `inspect`'s `quick_check` reports corruption, do **not** let the node flush over it — a corrupt
  local file is guarded from clobbering the last good S3 copy (the pre-flush integrity check,
  `[:fathom, :shard, :corrupt_flush]`). Pull the S3 copy, verify it, and treat the node's local file
  as suspect.
