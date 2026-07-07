# Phase 2 scoping — availability & capacity beyond the LB-keyspace-partition base

**Status:** scoping (2026-07-01). Picks the highest-value Phase-2 item and locks the
approach before any build. Phase 1 (the cluster phase, S1–S8 + the F1 heartbeat +
the durability dirty-flag) is shipped; this doc scopes what's next.

## The hard constraint

Phase 1 settled the architecture after two review pivots (see the design doc and
`[[fathom-cluster-phase]]`): **LB-keyspace-partition**. The load balancer
consistent-hashes the `Host` subdomain to one node; each node is an independent
single-node fathom; **S3 is the only cross-node coordination** (the lease + epoch
fence + the per-node heartbeat). Explicitly rejected: a BEAM mesh, a ring in BEAM,
`base_url` redirect, and any per-request relay/forward between nodes.

Every Phase-2 option below is judged first on **does it respect that model** — an
option that re-introduces dynamic per-request routing or a BEAM data mesh is
re-opening a closed decision, not building Phase 2.

## Today's baseline (what Phase 2 improves)

- **Failover (node death), RTO.** LB passive-health reroutes the dead node's
  subdomains to survivors; each survivor **cold-opens the shard from S3** (`Storage.pull`)
  and steals the lease (heartbeat-bounded). So per-shard failover latency ≈ the
  cold-open-from-S3 (`cold_open_s3_p50_us`, ~26/77/137/215 ms at 10/30/60/100 ms
  one-way S3 latency), and a mass reroute is a warming fan-out (bounded by the
  dedicated S3 Finch pool, `warm_s3_shards_per_s`).
- **Failover, RPO.** Data loss on a crash = writes since the last flush. Now
  write-gated (the durability dirty-flag): a dirty shard flushes every
  `:shard_flush_interval_ms` (default 30s, tunable), so RPO ≤ that interval for
  actively-written shards.
- **Capacity / hot spots.** Placement is pure ketama hash — a persistently hot shard
  (or a node that hashed several hot shards) can't be moved. `docs/deploy-cluster.md`
  already flags "the Phase-2 weighted/rebalance lever for persistent hot spots."
- **Scale churn.** ketama already remaps only ~1/N of subdomains on a node add/remove.

So the three Phase-2 features each attack a different axis: **A** cuts failover RTO,
**B** relieves capacity hot spots, **C** smooths churn / data gravity.

## A — Warm standby / WAL-follower  (axis: failover RTO, maybe RPO)

Keep recently-active shards **warm on a standby** so a failover skips the
cold-open-from-S3. The fork is how warm:

- **A1 — S3-warm-cache standby (lightweight, recommended).** A standby node
  proactively `Storage.pull`s the recently-active shard set from S3 (the same objects
  the durability flush writes) into a **local cache**, holding the file but **not the
  lease** — it doesn't serve, doesn't fence, just pre-pulls. On failover the standby's
  cold-open becomes a **warm** open (local file present → skip the S3 pull), so RTO
  drops from the S3-pull (~200 ms cross-region) to the warm cold-open (~2 ms). RPO is
  unchanged (the cache is only as fresh as the last S3 flush) — tune it separately via
  the flush interval. **Fits the model exactly:** no cross-node streaming, S3 stays the
  medium, the standby never touches the lease. Reuses the warming infra (concurrent
  pull + the dedicated Finch pool). Builds on: `Fathom.Shard.Storage.pull`, the warming
  pool, `Fathom.Directory.last_active_at` (or a PubSub active-shard feed) for the
  "recently active" set, the existing `warm=true/false` cold-open telemetry.
  - **Open forks:** (1) how the standby learns the active set — poll the Postgres
    directory (`last_active_at`) vs a PubSub broadcast of opens; (2) cache
    size/eviction (LRU by `last_active_at`, capped at the fd/RSS density budget from
    the scale test); (3) re-pull cadence (on each primary durability flush vs a fixed
    poll); (4) does a standby own a keyspace slice (each node is standby for its ring
    neighbor) or is it dedicated.
  - **Effort:** ~3 phases (a `Fathom.Shard.WarmFollower` process that subscribes to the
    active feed + pre-pulls + LRU-evicts; wire failover to prefer the warm copy; a
    failover-RTO bench: warm vs cold, plus a warm-standby density test). **Risk:** low —
    additive, no model change, no correctness impact on the single-writer invariant
    (the standby holds no lease, so it can never double-write).
  - **Status: A1 done (H1+H2+H3).** H1 = `Fathom.Shard.WarmFollower` (directory-poll `active_recent`,
    pre-pull, LRU-evict; gated `:warm_follower`, off by default). **H2 = freshness-validated
    promotion**, the correctness core the "prefer the warm copy" line glossed: a warm cache may
    lag the owner's latest flush, so it is **never served as-is** — the coordinator's cold-open
    validates the cache's etag against storage (`Storage.pull_if_changed/3`, a conditional
    `If-None-Match` GET → 304 promote / 200 re-pull fresh / 404 absent) before promoting it, and
    the follower revalidates its whole cached set each poll (recording an etag sidecar) so a
    failover lands on the 304 fast path. A live-dir warm *restart* (own un-flushed writes) still
    wins untouched — only the separate follower-cache path is validated. **H3 = the measurement**
    (`mix fathom.bench --only failover_rto`, `mix fathom.scale --warm-density`). It corrected this
    doc's optimistic "~2 ms warm": because H2 must freshness-check, the warm path still pays ONE S3
    round-trip (a 304, no body), so the warm win is the object **body transfer avoided** — measured
    ~2.3× (162→72 ms) at 1 MB / 30 ms / 100 Mbps, marginal for tiny shards on a fat pipe. Warm
    density is disk-bound (~0 BEAM/fd per cached shard). See AGENTS.md Benchmarking.
- **A2 — Live WAL streaming (heavyweight, defer).** Ship WAL frames primary→follower
  for near-zero RPO + instant promotion (LiteFS/litestream territory). exqlite exposes
  no WAL-frame stream, so this needs a WAL-shipping layer or adopting libSQL
  replication — and WAL frames crossing nodes is a **cross-node data path**, which
  tensions "S3 is the only coordination." High infra risk, big. Its only win over A1 is
  RPO (per-frame vs per-flush) — and RPO is already tunable via the flush interval. **Not
  worth the risk/model-change now.**

**Verdict:** A1 is the highest value-per-risk Phase-2 item — a real RTO win, additive,
model-consistent, tractable, reusing infra already built.

## B — Dynamic rebalancing  (axis: capacity / hot spots)

Move a persistently hot shard (or hot node) off the overloaded node, overriding the
hash. The fork is where the override lives:

- **B1 — LB override map + control-plane rebalancer.** The LB keeps a small
  per-subdomain **exception table** layered on the ketama hash (nginx `map` / HAProxy
  ACL / Fly): the hot minority is pinned to a chosen node, everything else stays pure
  hash. A control plane detects hot shards from load telemetry, orchestrates a **safe
  handoff** (drain the old node → warm on the new via A1 → flip the override →
  old self-fences via the lease), and reloads the LB. Keeps the LB as authority (just
  an exception table for the hot few); reuses the lease/heartbeat fence for the handoff.
  - **Tension:** it complicates the "pure stateless hash" story with a stateful
    exception table, and needs anti-flap. It's the **biggest** of the three (control
    plane + LB integration + handoff orchestration).
  - **Prerequisite: per-shard load telemetry — BUILT (`Fathom.ShardLoad`, 2026-07-01).**
    The "load telemetry we don't emit yet" is now emitted: a lock-free per-shard ETS
    counter (checkout rate + query cost, `top(n, by)` / `snapshot/0`), gated `:shard_load`
    (off by default until the rebalancer consumes it). So B's data input is in place; what
    remains for B is the LB exception table + the hot-detection policy (with anti-flap) +
    the handoff orchestration.
  - **Evidence harness — BUILT (`mix fathom.scale --hotspots`, 2026-07-06).** The first
    reader of `Fathom.ShardLoad`: it drives a Zipf-skewed query load across N shards
    through the real recording path (`Shards.checkout` → `ShardExecutor.execute`) and reads
    the counters the way a rebalancer would — diff two `snapshot/0`s over a window into
    per-shard rates. It reports the rate distribution (p50/p90/p99/max), the skew ratio, a
    `> K x median` threshold sweep (K=5/10/20 — how many shards each flags and whether it
    catches the true hot set), and cross-window flagged-set stability (Jaccard, the raw
    anti-flap signal). Purpose: **see whether hot spots are detectable and pick the
    threshold + anti-flap policy from real numbers before building the LB override table.**
    Synthetic-relative (one host, seeded skew); the staging run below is the non-synthetic
    confirmation.
  - **Verdict:** high capacity value but premature — it should be justified by real
    hot-spot evidence. The synthetic harness (`--hotspots`) shows the signal is usable and
    lets you tune the threshold/anti-flap knobs; the remaining gate before building B is a
    **staging real-traffic run** (turn on `:shard_load` on a deployed node, read
    `Fathom.ShardLoad.top/2` under a skewed tenant load). B also depends on A1
    (warm-handoff) to move a shard without a cold-open stall. **Do after A, with data.**
- **B2 — In-fathom routing/redirect for rebalanced shards.** Re-opens the rejected
  mailroom / `base_url`-redirect debate (per-request cross-node forwarding). **Don't.**

## C — Shard locality / affinity  (axis: churn / data gravity)

- **C1 — Remap-churn reduction.** ketama already remaps ~1/N; bounded-load or
  rendezvous (HRW) hashing smooths it further. Mostly LB config; the current ~1/N is
  already good → **marginal value.**
- **C2 — Access/region locality.** The data is in S3 (no node gravity), so "locality"
  really means region-affinity (route `acme.*` to the region near acme's users) — a
  multi-region geo-routing feature that **overlaps B** (placement override) and is a
  bigger, separate initiative. The *useful* near-term piece — warming toward where a
  shard is accessed — is just **A1** (access-driven pre-pull).

**Verdict:** standalone, C is the lowest incremental value; its useful bits fold into A
(access-driven warming) and B (placement). Not a standalone Phase-2 item.

## Recommendation

Priority by value-per-risk:

1. **A1 — S3-warm-cache standby.** Build first. Biggest availability win (failover RTO
   ~200 ms → ~2 ms for warmed shards), additive, model-consistent, reuses warming infra,
   zero single-writer risk. ~3 phases.
2. **B1 — dynamic rebalancing (LB override + control plane).** Defer until there's
   hot-spot evidence; it depends on A1 for warm handoff and needs per-shard load
   telemetry first. Biggest effort, partially complicates the pure-hash model.
3. **C — locality/affinity.** Not standalone; fold access-driven warming into A1 and
   region-affinity into a future multi-region initiative.

**Prerequisite that helps B later, cheap now:** start emitting **per-shard load
telemetry** (checkout rate / query cost) so a future rebalancer has data and so we can
*see* whether hot spots are real before building B. This is a small, additive
observability add and could ride along with A1.

## Decisions to lock before building A1

1. **Active-set feed:** poll Postgres `Fathom.Directory.last_active_at` (simple, laggy)
   vs a PubSub broadcast of shard opens (fresh, more plumbing). Lean: poll the directory
   (already the source of truth; A1 doesn't need sub-second freshness).
2. **Standby topology:** dedicated standby node(s) vs every node is warm-standby for its
   ring neighbor (N+1 warmth, no idle standby). Lean: ring-neighbor (no wasted node).
3. **Cache budget + eviction:** cap warmed shards by the fd/RSS density budget (scale
   test: ~196 KiB/shard active), LRU by `last_active_at`.
4. **Failover wiring:** the survivor must *prefer* its warm copy — today `Fathom.Shard`
   already treats a present local file as authoritative on wake (warm cold-open), so A1
   mostly needs to *populate* that local file ahead of time; confirm the warm path skips
   the pull as expected.
