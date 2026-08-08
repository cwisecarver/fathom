# A2 — Quorum replication (design, NOT built)

**Status: design only. Blocked on a dependency, not on effort — see [The blocker](#the-blocker).**
Scoped 2026-08-08. Supersedes the one-paragraph deferral in
[phase2-scoping](phase2-scoping.md) §A2 with an actual mechanism, a verified blocker, and the
options for clearing it.

## The gap this closes

A committed write is fsynced to local disk (`PRAGMA synchronous=FULL`,
`lib/fathom/shard/connection.ex:175`), so a **process** crash — BEAM death, OOM kill, redeploy —
loses nothing. `mix fathom.rpo` asserts it.

What is exposed is **node loss**. On failover the survivor cold-opens the shard by pulling the last
flushed object **from S3** ([durability](durability.md), "the survivor cold-opens"). Nothing ever
reads the dead node's disk; there is no re-attach path. So every write since the last flush is gone
— up to `:shard_flush_interval_ms` (300 s in deployment).

Putting `:shard_data_dir` on EBS (see [deploy-cluster](deploy-cluster.md#node-disk-aws)) removes the
*common* case: a reboot re-adopts its own local file and loses nothing. It cannot remove this one,
because the volume is single-attach and AZ-locked and the survivor is elsewhere.

## Why not resolve the conflict instead (CRDT / OT)

Rejected, and worth recording so it is not re-proposed:

- **The data is opaque client SQL.** fathom is a SQL proxy; it does not know that `accounts.balance`
  is money. A CRDT merges because the *data type* carries a merge function. An arbitrary tenant
  schema has none, and fathom cannot infer one.
- **Convergence is not correctness.** CRDTs guarantee replicas reach the *same* state, not a *valid*
  one. Two nodes insert different users with the same email; every merge either violates the
  `UNIQUE` index or silently drops a signup. Same for `FOREIGN KEY`, `CHECK`, and app invariants.
- **Primary keys collide.** Django's `BigAutoField` counts up independently on each side, so both
  assign the same id to different rows and every FK pointing at it becomes ambiguous. Fixing that
  means UUID keys across the tenant's whole schema — an application change, not a fathom one.
- **OT is a worse fit than CRDT.** It transforms operations in a *sequence*; there is no transform
  function for `DELETE ... WHERE <arbitrary predicate>`.

Today's behaviour is the right one and should be kept whatever A2 does: the diverged local file is
**quarantined, not deleted** (`lib/fathom/shard.ex:685`, `:780`), so the writes survive for offline
reconciliation and nothing is silently corrupted.

## The reference architecture

**Project Waterpark** — HCA Healthcare's BEAM platform (Bryan Hunter), live since 2020 across 185
hospitals. The relevant rule, from the Code BEAM talk:

> Before a patient actor commits an event to its event log, **two of its four read-only followers
> must acknowledge receipt of the event.**

- one read-only follower **per data centre** (4 of them), ack at **quorum (2 of 4)**, stragglers
  catch up asynchronously
- on primary crash the replacement asks the followers via the mailroom and adopts **the state of the
  best reader**
- **RAM-based** — no disk, no database. Durability is replication factor, not fsync

The shape is *replicate before ack*, never *merge after diverge*. Note that Waterpark alone loses
data if all replicas die together; fathom keeping S3 underneath would be strictly safer on that
axis — quorum replication **and** a cold object.

## The fathom mapping

The component already exists. `Fathom.Shard.WarmFollower` (A1, built) holds a lease-less copy of
another node's shard and never serves. A2 is the same component with the data path reversed:

| Today (A1) | A2 |
|---|---|
| follower **pulls** from S3, asynchronously | primary **pushes** frames to followers |
| copy may be stale; revalidated by etag | copy is current as of the last acked commit |
| failover pulls the object, loses the tail | failover promotes a follower, loses nothing |
| no effect on ack latency | commit waits for **≥2 follower acks** |

## The blocker

**`exqlite` 0.37.0 exposes no WAL-frame API.** Verified 2026-08-08 against `deps/exqlite/lib/`: the
only WAL-related surface in the entire library is `Exqlite.Pragma.wal_auto_check_point/1`, a
configuration pragma. There is no `sqlite3_wal_hook`, no frame read/apply, no backup API, no session
hook. **There is no seam to ship a frame from.**

This is the same class of gap as the UDF work (expert review #19), where the finding's recommended
`create_function` did not exist either and the fix was a loadable extension via
`enable_load_extension/2`. Establish the seam exists **before** designing against it.

### Options for clearing it

Effort labels are estimates, not measurements.

1. **Write the NIF ourselves** (`fathom_native`, or patch exqlite). Register `sqlite3_wal_hook`,
   read committed frames out of the `-wal` file, apply them on the follower. Most control, most
   work; puts fathom on a forked or extended DB driver. **Large.**
2. **Adopt libSQL's engine for replication.** libSQL already does frame-level replication.
   `Fathom.Shard.Connection` is documented as the single swap-point for exactly this
   (see AGENTS.md). Trades "write it" for "inherit it", but it is an engine swap on the data path
   and needs its own evaluation. **Large, different risk shape.**
3. **SQLite's session extension (changesets).** `sqlite3session` produces a changeset that applies
   to another database with explicit conflict handling — logical replication, built into SQLite.
   Closest thing to a principled merge that actually exists. **Needs verification**: requires
   `SQLITE_ENABLE_SESSION` at compile time and exposes a C API that exqlite does not wrap, so it is
   probably the same blocker wearing a different hat. **Verify before costing.**
4. **Ship SQL statements instead of frames (logical replay).** No NIF needed. But this is precisely
   the migration engine's replay path, which is non-deterministic for `random()` /
   `datetime('now')` and has already produced parameter-binding bugs against real Django. **Cheap
   to start, wrong for durability.** Not recommended.

## The architectural cost

A2 puts tenant **data** on a node-to-node path. Fathom's cluster model is deliberately
LB-keyspace-partition plus an S3 lease, with **S3 as the only cross-node coordination** — no BEAM
cluster, no ring, no mailroom (see [cluster-architecture](cluster-architecture.md)). A quorum ack
needs cluster membership, follower liveness, and a promote protocol. That is a reversal of the
central decision, not an addition to it.

So the honest framing of the decision is **not** "should we add WAL streaming" but **"should fathom
become a BEAM cluster."** Waterpark is the evidence that the clustered answer works at scale in a
domain where losing a write is unacceptable.

## What it would and would not improve

- **Would:** node-loss RPO from ~300 s to ~0. This is the entire benefit.
- **Would NOT: reduce S3 cost.** PUT count is driven by how *often* a dirty shard uploads, not how
  much — ingress bytes are free. Streaming frames *to S3* more often would cost **more** PUTs, not
  fewer. The win is only on the node-to-node path.
- **Would NOT:** remove the need for S3. It stays the cold backstop and the cold-open source.
- **Cost added:** every commit waits for ≥2 follower acks — a network round trip on the write path
  that does not exist today. Must be measured against `hrana_rt_us` before committing to it.

## Decision gate

Do not start until, in order:

1. **A frame seam is proven to exist** — option 1, 2 or 3 demonstrated on a branch, reading and
   applying one committed frame between two processes. Cheapest experiment that kills the premise.
2. **The ack-latency cost is measured** against the current per-request round trip.
3. **The BEAM-cluster reversal is accepted explicitly**, because everything downstream depends on
   it and it is not reversible cheaply.

Until then the position is: EBS covers reboot, S3 covers hardware and AZ loss, the 300 s interval
bounds the exposure between them, and the quarantine keeps a diverged copy recoverable.
