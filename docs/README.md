# Fathom docs

Fathom is a multi-tenant sharded data platform: one SQLite database per shard (eventually
millions), served to unchanged libSQL clients over the Hrana wire, cold-opened on demand from
S3-backed storage and consistent-hash-partitioned across a fleet.

**The single source of truth for the working agreement (workflow, gates, architecture, the
component map) is [`../AGENTS.md`](../AGENTS.md).** This folder holds the deeper per-subsystem
references, the design plans, the benchmark plans, the operational runbooks, and the run reports.
Each doc says whether it describes **built** behavior or a **plan**.

## How it works — built-engine stories

The "how it actually works" narratives for the major built subsystems. A good learning order is
single-node basics first, then what happens across nodes.

**Single node — how one shard is served and made durable:**

1. **[data-path.md](data-path.md)** — story 0: how a request becomes a served shard —
   connection-per-stream, the coordinator that owns the file and monitors its streams, the dirty
   flag, and the idle checkpoint → flush → drop.
2. **[admission.md](admission.md)** — the front door: fail-closed Host-subdomain routing (never a
   cross-tenant leak) and the double-gated novel-shard admission (soft `max_open_shards` cap + LRU
   idle-eviction, the novel-shard rate limiter, the template-poisoning boot guards).
3. **[durability.md](durability.md)** — how much data can be lost and when: WAL + `synchronous=FULL`
   (a process crash loses nothing) and the write-gated periodic flush (node-loss RPO = the flush
   interval).

**Across nodes — the lease, and everything that rides it:**

4. **[single-writer.md](single-writer.md)** — the distributed foundation: how fathom guarantees *at
   most one node ever writes a shard's file*, across failover / remap / partition, with **no BEAM
   cluster** — the S3 lease `{owner, epoch}`, the etag/epoch flush fence, and the O(nodes) node
   heartbeat.
5. **[warm-standby.md](warm-standby.md)** — how survivors pre-pull the hot set into a lease-less read
   cache and promote a warm copy on failover after a single conditional (304) round-trip instead of
   a full-body pull.
6. **[rebalancing.md](rebalancing.md)** — how a persistently-hot shard is moved off an overloaded
   node: detect (per-node load reporting) → decide (a fleet-singleton policy) → execute (the
   warm → flip-the-LB → drain-the-lease handoff).
7. **[migration.md](migration.md)** — how a schema change rolls across millions of shards:
   blue/green copy-then-flip per shard, cold-tail convergence, a guarded revert, and how the app
   tolerates a mixed vN-1/vN fleet.

**Supporting — security and the control plane:**

8. **[auth.md](auth.md)** — how an unchanged libSQL client authenticates per shard (a
   `Phoenix.Token` as `authToken` on Filo's `:authorize` seam), the `:disabled`/`:required` modes,
   and the network trust boundary when auth is off.
9. **[directory.md](directory.md)** — the Postgres control plane the migration / rebalancing /
   warm-standby readers use, and the off-hot-path recorder (lock-free ETS coalesce + batch-flush) so
   a Postgres outage never fails a checkout.
10. **[tenant-lifecycle.md](tenant-lifecycle.md)** — the tenant control plane: **provision**
    (explicit create + token), **suspend/resume** (administrative offline via a reversible ETS deny
    gate), **delete** (GDPR erasure: synchronous tombstone + fleet-wide re-mint guard + force-stop-
    then-purge, why it can't leave a `.fenced.<ts>` copy behind), and **export** (portability). The
    `/api/tenants` JSON API + admin actions + the operator runbook.

[cluster-architecture.md](cluster-architecture.md) is the **canonical, broader** cluster picture
(the LB-keyspace-partition model + the S3 lease/heartbeat fence) that these zoom into.

## Architecture & deployment

- **[cluster-architecture.md](cluster-architecture.md)** — the cluster design of record:
  LB-keyspace-partition (consistent-hash Host → node) + the S3 lease/epoch/heartbeat fence. *Built.*
- **[deploy-cluster.md](deploy-cluster.md)** — deploying the cluster (LB config, health/observability,
  the two-layer chaos-testing split), plus the phase status (S1–S8). *Built.*

## Design / plans

These predate parts of the working code — read them **with** the built-engine docs above, which say
what actually shipped (some plan assumptions were superseded).

- **[migration-plan.md](migration-plan.md)** — the schema-migration design + decision points.
  *Plan (draft).* The built engine is [migration.md](migration.md).
- **[migration-engine-plan.md](migration-engine-plan.md)** — earlier migration-engine design notes.
  *Plan.*
- **[phase2-scoping.md](phase2-scoping.md)** — Phase 2 scoping: warm standby (A), rebalancing (B),
  locality/affinity (C). *Scoping.*

## Benchmarking

- **[benchmark-plan.md](benchmark-plan.md)** — the hot-path benchmark harness: `mix fathom.bench`,
  the multi-metric regression gate, `mix fathom.scale`, and what each metric means. *Built.*
- **[tpc-benchmark-plan.md](tpc-benchmark-plan.md)** — the wire-true TPC-B + TPC-C benchmark plan
  (loopback WS gate metrics + the recorded-only TPC-C sweep). *Built (rev 7, complete).*

## Runbooks

- **[runbooks/cluster.md](runbooks/cluster.md)** — operating the cluster (lease/ownership incidents:
  stuck-unavailable, split-brain, S3 down) + the metrics/alerts reference.
- **[runbooks/operations.md](runbooks/operations.md)** — the non-lease incident classes:
  Postgres-outage, disk-full, heartbeat-loss, dual-LB double-routing, shard restore drill, and the
  per-dependency fail-open/fail-closed matrix.
- **[runbooks/deploy.md](runbooks/deploy.md)** — rolling a new fathom version node-by-node,
  clean-shutdown flush semantics, config-change vs full-restart, and node removal.
- **[runbooks/rebalancer.md](runbooks/rebalancer.md)** — the staged, gate-by-gate enable path for
  dynamic rebalancing (observe-before-arming dry run + rollback).
- **[runbooks/admin-dashboard.md](runbooks/admin-dashboard.md)** — running the realtime `/admin`
  dashboard, its auth/gates, and the macOS Tailwind re-sign + pending-migration gotchas.

## Reviews & run reports

Point-in-time measurement writeups and expert reviews (dated; relative numbers, single-host unless
noted).

- **[reviews/competitive-oltp-2026-07-10.md](reviews/competitive-oltp-2026-07-10.md)** — fathom vs
  raw SQLite vs Postgres on the same box, both durability modes; the `synchronous=FULL` finding.
- **[reviews/turso-headtohead-2026-07-10.md](reviews/turso-headtohead-2026-07-10.md)** — fathom vs
  the reference libSQL server (`sqld`): per-DB (on par), multi-tenant fan-out (fathom scales out),
  and density (fathom built for millions).
- **[reviews/fleet-density-2026-07-10.md](reviews/fleet-density-2026-07-10.md)** — the multi-node
  fleet density run (`chaos.sh density`): the LB keyspace-partition spreads N shards evenly across
  the nodes at the single-node per-shard cost, so capacity is horizontally additive (the "millions"
  mechanism the single-node density work left as architectural).
- **[reviews/tpc-run-2026-07-10.md](reviews/tpc-run-2026-07-10.md)** — the remote-client TPC run over
  the chaos rig (true cross-LB latency) + the loopback spec-scale TPC-C comparability numbers.
- **[reviews/latency-cost-2026-07-11.md](reviews/latency-cost-2026-07-11.md)** — what an injected S3
  RTT costs the two round-trip-bound paths (`chaos.sh latency-cost`): cold-open ≈ 1 RTT (matches the
  in-process sweep), flush ≈ 3.5 RTT (the drain/release path is *not* overlapped — the tuning target).
- **[reviews/tpc-fleet-2026-07-11.md](reviews/tpc-fleet-2026-07-11.md)** — multi-tenant TPC-B across
  the fleet (`chaos.sh tpc-fleet`): the throughput analog of density — one single-writer shard per
  tenant, load partitions across the nodes (query load follows shards), per-txn p50 stays flat as
  tenants scale (no convoy); absolute txn/s is single-host-bound, horizontal is the real axis.
- **reviews/chaos-run-2026-07-0{5,8,9}.md** — chaos-rig run reports (failover, pause-fence, hotspots,
  rebalance handoff — the live proofs referenced by the built-engine docs).
- **reviews/expert-review-2026-07-0\*.md** — expert-panel review passes (each with a `.progress.md`
  working artifact); the hardening they drove is folded into the code and the docs above.

---

*New a subsystem doc? Match the shape of the built-engine stories (problem → constraint → mechanism
→ safety → the honest limit → one-line summary), ground every claim in the code, and link it from
its `AGENTS.md` bullet.*
