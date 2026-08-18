# Fathom docs

Fathom is a multi-tenant sharded data platform: one SQLite database per shard (eventually
millions), served to unchanged libSQL clients over the Hrana wire, cold-opened on demand from
S3-backed storage and consistent-hash-partitioned across a fleet.

**The single source of truth for the working agreement (workflow, gates, architecture, the
component map) is [`../AGENTS.md`](../AGENTS.md).** This folder holds the deeper per-subsystem
references, the design plans, the benchmark plans, the operational runbooks, and the run reports.
Each doc says whether it describes **built** behavior or a **plan**.

**New here?** The root [`../README.md`](../README.md) has the project overview and a two-path
"Getting started" (Docker eval stack + native dev); [`../CONTRIBUTING.md`](../CONTRIBUTING.md) is the
set-up-and-land-a-change guide for developers.

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
- **[../deploy/compose/README.md](../deploy/compose/README.md)** — the single-node **eval stack**:
  one `docker compose up` (fathom + MinIO + Postgres + nginx, safe defaults) to zero-to-a-served
  tenant in <30 min. *Built.*
- **[configuration.md](configuration.md)** — the env-var reference: every knob fathom reads, its
  default, and its **safety consequence** (kept complete by a drift test). *Built.*
- **[quickstart-django.md](quickstart-django.md)** — pointing an unchanged Django app at a fathom
  tenant over `django-libsql` (connection, write-concurrency, latency, the mixed-window modes;
  multi-tenant routing helper is the pending #16 companion package). *Built.*
- **[django-migrations.md](django-migrations.md)** — the operator schema-migration workflow:
  capture → release → converge (the `/api/migrations/status` deploy gate) → revert, expand-contract
  rules, capture-time safety nets, and `migration_failed` triage. *Built.*

## Design / plans

These predate parts of the working code — read them **with** the built-engine docs above, which say
what actually shipped (some plan assumptions were superseded).

- **[migration-plan.md](migration-plan.md)** — the schema-migration design + decision points.
  *Plan (draft).* The built engine is [migration.md](migration.md).
- **[migration-engine-plan.md](migration-engine-plan.md)** — earlier migration-engine design notes.
  *Plan.*
- **[phase2-scoping.md](phase2-scoping.md)** — Phase 2 scoping: warm standby (A), rebalancing (B),
  locality/affinity (C). *Scoping.*
- **[a2-quorum-replication.md](a2-quorum-replication.md)** — Phase 2 A2 in full: replicate-before-ack
  (the Waterpark quorum shape) as the answer to node-loss RPO, and why CRDT/OT cannot work over
  opaque tenant SQL. **Built and on `main`, off by default** (`REPLICATION_ENABLED` /
  `REPLICATION_LISTEN`). The doc's stated blocker — "exqlite exposes no WAL-frame API" — was
  **disproved 2026-08-09**: a loadable extension gets a live `sqlite3*` and `sqlite3_wal_hook` is in
  the extension pointer table, so exqlite's surface was never the boundary. Current scale limit and
  the fixed OOM: [reviews/a2-feedback-loop-fixed-2026-08-17.md](reviews/a2-feedback-loop-fixed-2026-08-17.md).

## Benchmarking

- **[benchmark-plan.md](benchmark-plan.md)** — the hot-path benchmark harness: `mix fathom.bench`,
  the multi-metric regression gate, `mix fathom.scale`, and what each metric means. *Built.*
- **[tpc-benchmark-plan.md](tpc-benchmark-plan.md)** — the wire-true TPC-B + TPC-C benchmark plan
  (loopback WS gate metrics + the recorded-only TPC-C sweep). *Built (rev 7, complete).*
- **[a2-bare-metal-plan.md](a2-bare-metal-plan.md)** — how to measure A2 replication on three
  SEPARATE machines, and why it is the next thing worth doing: every A2 number to date comes from
  five nodes sharing one VM over loopback, so nothing so far can separate "fathom saturates" from
  "the box saturates". Carries the measured **17.3 KB of WAL per TPC-B commit** that the network
  arithmetic rests on, the wired-vs-Wi-Fi ceiling (Wi-Fi is a shared medium and would bind before
  the CPUs did), and the pass conditions. *Planned, not run.*

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
- **[runbooks/disaster-recovery.md](runbooks/disaster-recovery.md)** — cross-store (Postgres + S3)
  restore coherence: recovering from a directory point-in-time restore without resurrecting deleted
  tenants or revoked tokens.

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
- **[reviews/fleet-rollout-2026-08-04.md](reviews/fleet-rollout-2026-08-04.md)** — fleet
  schema-migration throughput (`chaos.sh rollout`): **~46,000 shards/hour** across 3 nodes against a
  100/hour `:reconcile_batch_size` default, so the knob throttles ~460× below the engine — the
  evidence base review 2026-08-01 #43 asked for, plus the `rate_per_hour`/`eta_seconds` fields
  tracking a real rollout. Also finds a **reproducible stuck-lease bug** (~1 in 300): a coordinator
  lease that outlives its coordinator on a live node is never stealable, so that tenant serves fine
  but can never migrate — **root-caused and fixed**: `release_lease` reported a `412` as success,
  collapsing "someone else's lock" with "still ours at a rotated etag". Heartbeat-mode-only, which
  is why the legacy-only suite never saw it. Three other real lock-leak paths were found and fixed
  en route, none of them the cause.
- **[reviews/a2-shipper-feedback-loop-2026-08-16.md](reviews/a2-shipper-feedback-loop-2026-08-16.md)**
  — why A2 OOM-killed nodes at 1024 tenants: a **positive feedback loop**, not a leak. A push carries
  the WAL since the follower's last ack, so delay makes the next payload bigger, which makes more
  delay. The measurement that settles it — queued messages FLAT while binary held DOUBLED — is also
  why **queue depth is the wrong signal** and why three message-count fixes missed.
- **[reviews/a2-feedback-loop-fixed-2026-08-17.md](reviews/a2-feedback-loop-fixed-2026-08-17.md)** —
  the fix and, just as importantly, what it did **not** buy. The loop is closed (`largest` payload
  pinned at the 1 MiB cap to the byte, node binary 32–922 MB and falling vs 7–18 GiB climbing, nodes
  alive 2 h vs dead in minutes) and a same-rig A/B shows the cap is **free** (12.7% fewer errors at
  512). But **the tenant ceiling did not move**: 256 clean, 512 degraded, 1024 still no result — now
  by throughput collapse rather than by dying. Carries the two open leads (the budget's
  all-or-nothing cliff, and `:already_in_flight` as the dominant reject — pre-existing, not the loop).
- **[reviews/tpc-run-2026-07-10.md](reviews/tpc-run-2026-07-10.md)** — the remote-client TPC run over
  the chaos rig (true cross-LB latency) + the loopback spec-scale TPC-C comparability numbers.
- **[reviews/latency-cost-2026-07-23.md](reviews/latency-cost-2026-07-23.md)** — what an injected S3
  RTT costs the two round-trip-bound paths (`chaos.sh latency-cost`): cold-open ≈ **1 RTT** (26 / 70 /
  133 ms at 10 / 30 / 60 ms one-way — the `Shard.init` acquire∥pull overlap), flush/drain ≈ **2.3×
  RTT**, down from ~3.5× after the drain fix; the remaining PUT→DELETE ordering is irreducible.
  *(Supersedes `latency-cost-2026-07-11.md`, whose ~3.5× number predates the fix and whose
  attribution of the third round-trip to the etag sidecar was wrong — the sidecar is a local file.)*
- **[reviews/tpc-fleet-elixir-driver-2026-07-24.md](reviews/tpc-fleet-elixir-driver-2026-07-24.md)** —
  the current multi-tenant throughput result: **4,096 concurrent tenant shards, 3,414 txn/s, zero
  errors**, driven from one process; 7,168 tenant shards over the sweep, 5.08M queries, consistent-hash
  spread **1.15×**. Load follows the shard partition, so the horizontal axis is the even per-node
  split — absolute txn/s is single-host-bound.
- **[reviews/tpc-fleet-2026-07-23.md](reviews/tpc-fleet-2026-07-23.md)** — the 6→1024-tenant sweep
  behind that result (and an honest null: it reports its own `tpcb_tps` metric as smoke-level only).
  **[reviews/tpc-fleet-highconc-2026-07-23.md](reviews/tpc-fleet-highconc-2026-07-23.md)** finds the
  *driver's* ceilings rather than fathom's; **[reviews/tpc-scaling-2026-07-24.md](reviews/tpc-scaling-2026-07-24.md)**
  charts the whole story; **[reviews/tpcc-elixir-driver-2026-07-24.md](reviews/tpcc-elixir-driver-2026-07-24.md)**
  covers TPC-C (and the Hrana stream-resume bug it surfaced);
  **[reviews/tpcc-4096-shed-root-cause-2026-07-24.md](reviews/tpcc-4096-shed-root-cause-2026-07-24.md)**
  root-causes the single-node 4096 shed to client request timeouts, not a fathom resource.
  ⚠️ **`reviews/tpc-fleet-2026-07-11.md` is superseded — its absolute txn/s is invalid** (~95%
  LB-502 reconnect overhead; see **[reviews/lb-502-fix-2026-07-23.md](reviews/lb-502-fix-2026-07-23.md)**).
- **[reviews/rpo-sweep-2026-07-18.md](reviews/rpo-sweep-2026-07-18.md)** — the measured node-loss
  window that justified the 5 s flush default (452 rows p50 / 800 p99 at 200 writes/s), the evidence
  behind [`durability.md`](durability.md)'s bounded-loss contract.
- **[reviews/served-data-2026-07-23.md](reviews/served-data-2026-07-23.md)** (density re-check with
  real data per shard) and **[reviews/chaos-failover-2026-07-23.md](reviews/chaos-failover-2026-07-23.md)**
  (kill-failover revalidation) — both post perf-iteration and post LB-502 fix.
- **[reviews/s3-latency-ab-2026-07-23.md](reviews/s3-latency-ab-2026-07-23.md)** and
  **[reviews/hotspots-ab-2026-07-23.md](reviews/hotspots-ab-2026-07-23.md)** — the A/B pair for the
  2026-07-23 perf iteration (RTT-path wins; per-query wins).
  **[reviews/vm-args-ab-2026-07-25.md](reviews/vm-args-ab-2026-07-25.md)** measures the BEAM-flag
  follow-ups — and refutes two of them, including one this project had already shipped.
- **reviews/chaos-run-2026-07-0{5,8,9}.md** — chaos-rig run reports (failover, pause-fence, hotspots,
  rebalance handoff — the live proofs referenced by the built-engine docs).
- **reviews/expert-review-2026-07-0\*.md** — expert-panel review passes (each with a `.progress.md`
  working artifact); the hardening they drove is folded into the code and the docs above.

---

*New a subsystem doc? Match the shape of the built-engine stories (problem → constraint → mechanism
→ safety → the honest limit → one-line summary), ground every claim in the code, and link it from
its `AGENTS.md` bullet.*
