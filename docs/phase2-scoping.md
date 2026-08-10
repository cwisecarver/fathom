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
  `:shard_flush_interval_ms` (default 5s, tunable), so RPO ≤ that interval for
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
  tensions "S3 is the only coordination." High infra risk, big.
  **UPDATE 2026-08-09 — two of those three premises were wrong, and A2 is built on the
  `a2-quorum-replication` branch (not merged here).** exqlite's surface was never the boundary: a
  loadable extension gets a live `sqlite3*`, and `sqlite3_wal_hook` is in the extension pointer
  table — same move as the Django UDFs. No libSQL swap, no forked driver. And it needed no BEAM
  cluster: frames go over A2's own socket protocol, so S3 stays the only cross-node *coordination*.
  What survives is the third premise — a cross-node data path is genuinely new surface, and
  membership is still a static config list. Measured cost: **+225 µs per write (4.04×)** with it
  on, noise with it off. See [a2-quorum-replication](a2-quorum-replication.md). Its only win over A1 is
  RPO (per-frame vs per-flush) — and RPO is already tunable via the flush interval. **Not
  worth the risk/model-change now.**
  **Scoped in full 2026-08-08 → [a2-quorum-replication](a2-quorum-replication.md)**: the
  Waterpark quorum shape (ack after ≥2 follower confirms), why CRDT/OT is the wrong tool here,
  and the **verified blocker** — exqlite 0.37.0 exposes no WAL-frame API at all, so there is no
  seam to ship a frame from. Blocked on a dependency, not on effort.

**Verdict:** A1 is the highest value-per-risk Phase-2 item — a real RTO win, additive,
model-consistent, tractable, reusing infra already built.

## B — Dynamic rebalancing  (axis: capacity / hot spots)

Move a persistently hot shard (or hot node) off the overloaded node, overriding the
hash. The fork is where the override lives:

- **B1 — LB override map + control-plane rebalancer — BUILT (`Fathom.Rebalancer.*`,
  2026-07-06).** The LB keeps a small per-subdomain **exception table** layered on the
  ketama hash (nginx `map $host $fathom_target`): the hot minority is pinned to a chosen
  node, everything else stays `default fathom_hrana` (pure hash). A control plane detects
  hot shards from load telemetry, orchestrates a **safe handoff**, and reloads the LB.
  Keeps the LB as authority (just an exception table for the hot few); reuses the
  lease/heartbeat fence for the handoff. **Proven live on the 3-node chaos rig
  (`chaos.sh rebalance`): `acme` @ 143.9 q/s detected on fathom1 → pinned to fathom2 →
  LB reloaded → source drained → served by fathom2, isolation intact.**
  - **The pieces (all shipped):** per-node **reporter** → `shard_load_samples` Postgres
    (each node diffs two `ShardLoad` snapshots into rates and publishes its hot set —
    there's no BEAM cluster to read ETS across); the **`Policy`** (absolute q/s floor or
    p99-relative — never median; 2-window anti-flap; cooldown; an improvement guard that
    refuses to relocate a lone hotspot; least-loaded target); the **`shard_overrides`**
    exception table + **`LbMap`** nginx renderer + **`LbApply`** (write + reload); a
    **cross-node command channel** (`rebalance_commands` + per-node `CommandPoller`) so
    the orchestrator can warm/drain a shard on a node it can't RPC; and the Oban
    **`RebalanceJob`** (cron singleton via Oban's Postgres peer leadership) + **`HandoffJob`**
    (unique per shard). All gated off by default (`:load_reporter`/`:command_poller`/
    `:rebalancer_enabled`).
  - **The handoff, corrected.** The scoping line above once said "old self-fences via the
    lease" — recon showed that's only the **crash/partition** path (a steal needs the old
    owner's heartbeat *stale*; a healthy node keeps its heartbeat fresh, so the new node's
    `acquire_lease` just gets `{:held}`). So a healthy-node handoff goes through a
    **voluntary drain**: warm the target → **flip the LB first** (new traffic → target,
    which stops the inflow to the source and is what makes draining a *hot* shard finish
    fast) → drain the source (flush + release the lease) → the target acquires the freed
    lease on its next request. The `{owner,epoch}` lease + `Fence.check` still guarantee
    no double-write across every interleaving; the flip-first ordering is for liveness.
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
    per-shard rates. It reports the rate distribution (p50/p90/p99/max), two separation
    measures (max/median and the tail-robust max/p99), **three threshold-family sweeps** —
    `> K x median`, `> K x p99`, and an absolute q/s floor set to isolate the top-N — each
    with a Zipf-recall check, plus a scale-robust anti-flap signal (top-20-by-rate set
    overlap across the two windows) and whether the shipped `ShardLoad.top/2` recovers the
    Zipf head. Purpose: **see whether hot spots are detectable and pick the threshold +
    anti-flap policy from real numbers before building the LB override table.**
  - **Finding (prod build, 10k shards, s=1.1, 150k q/window, 2026-07-06 — synthetic,
    one-host relative).** Hot spots are **cleanly detectable at fleet scale**: `ShardLoad.top(20)`
    recall **1.0**, a sharp Zipf head (hot_1 188 q/s → hot_2 89 → hot_3 57 …), top-20
    anti-flap Jaccard **0.9**. But the threshold **shape matters** — with 8,396 active
    shards the long cold tail pulls the median to ≈0 (p50 0.0, p90 0.1, p99 1.5 q/s), so a
    `> K x median` rule flags **hundreds** (751/421/216 at K=5/10/20). The tail-robust
    forms stay tight: `> 20 x p99` flags **5** shards at recall 1.0; an absolute floor of
    **~32 q/s isolates the top-5** (recall 1.0), ~16 q/s the top-10. **So the rebalancer's
    hot-detection must key on p99-relative or an absolute q/s floor, never `> K x median`;**
    `> K x p99` with a 2-window confirm is a viable anti-flap gate. (`median_collapsed` is
    computed shape-first — `>10x-median` flagging many more shards than `>10x-p99` — so it
    holds at any N or throughput, not from an absolute `median < 1 q/s` test.)
  - **Throughput (prod, 1000 shards, ~20k streams, `--stream-len` sweep).** The load unit
    is a Hrana stream (checkout + open once, burst L queries on the held connection,
    checkin). At `L=1` (one query per stream) the drive is **~3.0k q/s** — the per-query
    lower bound, bottlenecked on the coordinator checkout/checkin round-trip. Persistent
    streams remove that: **L=16 ~24k q/s, L=64 ~51k q/s** per node (hottest shard 530 →
    4.3k → 9.0k q/s), with detection quality flat (recall 0.95–1.0, anti-flap 0.9–1.0) as
    long as `Q/L` (stream count) stays ≫ N. So a node sustains tens of thousands of q/s
    against many shards; the earlier ~1.2k figure was the per-query harness artifact, not a
    fathom limit. (One-host relative; a staging run gives prod-absolute q/s.)
  - **Staging real-traffic run — DONE (chaos rig, 2026-07-06).** The non-synthetic
    confirmation: `SHARD_LOAD=true` on the 3 prod-release nodes (the new `runtime.exs` env),
    then `deploy/chaos/chaos.sh hotspots` drove **17,455 real Hrana requests** through the
    nginx LB (~290 req/s, Zipf s=1.1 over 200 shards), and read `Fathom.ShardLoad.top(20)`
    on each node via `bin/fathom rpc`. Results match the synthetic harness end-to-end:
    (1) the LB consistent-hash **spread the hot set across all three nodes** — hot_1 on
    fathom2, hot_2 on fathom3, hot_4 on fathom1, each shard on exactly one node — so a
    rebalancer reads `ShardLoad` per node and **merges** for the fleet view (the per-node
    view is partial by design); (2) the merged ranking is a clean monotone Zipf head (hot_1
    3767 q → hot_2 1727 → hot_3 1152 → …); (3) **`ShardLoad.top(20)` recovers the head:
    top-5/10 recall 1.0, top-20 recall 0.95** under the real wire protocol + real LB routing
    + real MinIO S3. So the detection signal holds non-synthetically, and the per-node-read/
    merge model is confirmed. (Traffic is per-query over curl, ~290 req/s — enough to prove
    detection; the throughput ceiling is the synthetic `--stream-len` result above.)
  - **Verdict: B1 is BUILT (2026-07-06)** — `Fathom.Rebalancer.*`, proven live on the rig
    (see §B1 above). Detection (reporter → Postgres, p99/absolute policy, 2-window
    anti-flap), the exception table + nginx render + reload, the cross-node command
    channel, and the Oban cron + handoff orchestration all shipped and are gated off by
    default. What remains is **operational, not code**: turn it on in a real deployment
    (`REBALANCER_ENABLED` + `LOAD_REPORTER`/`COMMAND_POLLER` + `SHARD_LOAD`), tune the
    absolute q/s floor to that fleet's rates, and apply the rendered map — either
    `LB_RELOAD_CMD` so `HandoffJob` reloads the LB itself (where the app can reach it) or a
    reloader that watches the map file (the rig's `lb-reloader` sidecar shares nginx's PID
    namespace and HUPs its master on a map change, so the handoff needs no host bridge). An
    **autonomous** handoff was run end to end on the rig 2026-07-07: an engineered node
    imbalance (green 49.6 q/s + blue 25.3 q/s on fathom2) → `Policy.propose` chose
    `green: fathom2 → fathom1` on its own → `RebalanceJob`/`HandoffJob` executed it, the
    sidecar applied the flip, and green moved with data intact. A2 (WAL streaming) and
    Phase-2 **C** (locality) remain the open Phase-2 items.
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
