# Cluster architecture — decision + status

The durable record of *how* fathom is a multi-node cluster and *why* it took this
shape. Operational detail lives in `deploy-cluster.md`; the on-call runbook in
`runbooks/cluster.md`; the failover-measurement results in
`reviews/chaos-run-2026-07-05.md`. This doc is the decision and its rationale.

## Decision: LB-keyspace-partition + S3 lease/epoch fence

The **load balancer is the placement authority.** It consistent-hashes the request
`Host` subdomain to one backend node (nginx `hash $host consistent`, or HAProxy /
Fly `fly-replay`), so every HTTP request and WebSocket for `acme.*` lands on the same
node, which serves through the **existing single-node path** (`ShardExecutor` →
`Shards.checkout` → `Shard` → exqlite). Each node is an independent single-node
fathom.

The only cross-node coordination is the **S3 lease + epoch fence + node heartbeat**
(`Fathom.Shard.Storage` + `Fathom.Shard.Heartbeat`) — over S3, never BEAM. A per-shard
lock carries `{owner, epoch}` (the monotonic epoch is the fencing token); a per-node
`heartbeat/<owner>` object carries liveness (renewed once per node, O(nodes) not
O(shards) — the F1 fix). On a node death or scale change the LB reroutes the
subdomain, the new node steals the lease once the old owner's heartbeat lapses
(TTL-bounded), and the old node self-fences before its next write.

**There is no ring, no libcluster, no BEAM distribution, no TLS-dist, no
redirect/relay.** Tenant data rides standard client→LB→node TLS; it never crosses a
BEAM mesh.

## Why not the alternatives

Two cross-node designs were shaped and rejected. Both fail on **durable Filo
constraints**, so they're worth recording so they aren't re-proposed:

1. **BEAM-forwarding "mailroom"** (entry node relays streams to a computed owner).
   Hrana HTTP streams are **entry-node-local**: the stream baton is HMAC-signed with a
   key generated **per node boot** (`application.ex` → `Filo.Baton.new_key()`), and
   `Filo.Streams` is a node-local Registry. Forwarding the transport doesn't move the
   stream state, so it fixes the wrong side.

2. **`base_url` redirect** (tell the client to talk to the owner directly). Hrana HTTP
   has **no SQL-free "open"**: the first baton-less POST already carries SQL and must
   return results inline, and `base_url` only governs *subsequent* requests — so the
   entry node still executes the first request (and every single-POST autocommit
   stream) itself. It also can't express "the LB routes to a ring-computed owner"
   because the LB can't see an in-BEAM ring.

LB-keyspace-partition serves both transports (HTTP and WebSocket) with neither
problem, because placement is decided *before* the connection reaches a node.

## Status — cluster phase complete (S1–S8)

All shipped on `main`:

| Step | What |
|---|---|
| S1 | LB mechanism spike (`hash $host consistent`, in-request failover) |
| S2 | nginx LB config + `Fathom.HealthPlug` (`GET /health`) |
| S3 | lease-handoff cross-node tests |
| S4 | crash / commit-ack-lost contract + a checkout-race fix |
| S5 | observability (`Fathom.Telemetry` + OTel span bridge) + runbooks |
| S6 | cross-shard isolation gate + partition / fail-closed tests (release blocker) |
| S7 | directory write moved off the hot path (`Fathom.Directory.Recorder`) |
| S8 | lease-renewal RPS measurement (`mix fathom.scale --lease-rps`) |

Two storms fixed en route: the **F1 lease-renewal storm** (per-shard renewals → one
per-node heartbeat; ~100k PUT/s/node at 1M shards → ~0.1) and the **durability-flush
storm** (a write-gated `dirty` flag, so PUTs track writes not open-shard count).

The **failover-time-and-loss-window measurement** layer that S6 deferred to a real
deployment is now built and exercised: the `deploy/chaos/` Docker rig (3 prod-release
nodes behind nginx + MinIO + per-node toxiproxy), run 2026-07-05 and 2026-07-06 — all
scenarios pass (failover TTL+steal-margin bounded, pause-fence zombie self-fences with
no split-brain, node↔S3 partition fail-closed + recovers, soak zero-loss / zero-leak
through node churn, warm-standby 304-promote observed). See
`reviews/chaos-run-2026-07-05.md`.

## Forward edge — Phase 2

`docs/phase2-scoping.md` scopes it. **A1 (warm standby)** is built —
`Fathom.Shard.WarmFollower` pre-pulls the hot set to survivors and promotes a
freshness-validated (304) copy on failover. Remaining: **B** (dynamic rebalancing,
gated on real hot-spot evidence — `Fathom.ShardLoad` is its built-but-off
prerequisite), **C** (shard locality / affinity), **A2** (active-shard WAL follower,
deferred).

**Data-path engine — decided 2026-07-06: exqlite.** The `Fathom.ShardRepo` /
`Ecto.Adapters.LibSql` path (defined but never wired) was evaluated and removed. The
data plane is a **SQL proxy** — it executes arbitrary opaque SQL from unchanged
external clients and returns raw Hrana result shape (columns/rows/types/`num_changes`/
`last_insert_rowid`), with zero Ecto schemas for shard tables — so Ecto's schema layer
is inert here while adding per-query overhead and a Rust NIF. exqlite (SQLite via
`Exqlite.Sqlite3`) is the right abstraction level and is what the migration copy, bench,
scale, and cluster tests already run on. The one thing that could ever force a change is
a client needing a **libSQL-only** SQL feature — that's an evidence-gated engine swap
behind `Fathom.Shard.Connection` (the thin wrapper is the single swap-point), **not** a
reason to adopt Ecto. Ecto stays where its schema is known: the Postgres control plane
(`Fathom.Repo`).
