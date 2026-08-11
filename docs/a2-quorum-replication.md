# A2 — Quorum replication (design; not on `main`)

**Status: WORKING on the `a2-quorum-replication` branch, off by default, not merged to `main`.**
Scoped 2026-08-08; the blocker below disproved 2026-08-09; replication proven end to end on the
chaos rig 2026-08-11 (`chaos.sh smoke` passes with `REPLICATION_ENABLED=true` — five tenants, every
write quorum-replicated, cross-shard isolation intact). Supersedes the one-paragraph deferral in
[phase2-scoping](phase2-scoping.md) §A2.

This copy is kept deliberately as the *scoping* document — the mechanism, the cost, and the
decision gates. The branch carries the implementation record, which is several times longer and
does not belong on a `main` that has no A2 code. **Read [The blocker](#the-blocker) with its
correction before quoting anything here**: the sentence "there is no seam to ship a frame from" is
preserved for the record and is false.

## What running it multi-node found

Three defects, none of which any unit test could see, and the first two of which made the feature
non-functional rather than merely imperfect. Recorded here because each one is a *class* of thing
to check when this merges, not just a bug that got fixed.

**1. The receive half was never started.** `Replication.Follower` — the listener that accepts
frames — existed, was well tested, and was referenced outside tests by nothing. `Fleet` supervised
the Registry, the session supervisor and the Shippers: the primary half, all of it. So
`REPLICATION_ENABLED=true` on a real node shipped every commit into a closed port and returned 503
`FILO_NO_QUORUM` for every tenant write, while `configuration.md` told operators to point
`REPLICATION_FOLLOWERS` at a port fathom never opened. Shipping and receiving are now separate
gates (`REPLICATION_ENABLED` / `REPLICATION_LISTEN`) because a rollout must turn listening on
fleet-wide *first*, and one flag cannot express that order.

**2. A recreated WAL read as a rewind, so no replicated write ever succeeded.** `ckpt_seq` counts
checkpoints *within* one WAL file; SQLite deletes the `-wal` when the last connection to a shard
closes — after every Hrana stream on a quiet shard — and the next one starts fresh with new salts
and `ckpt_seq` back at **0**. `Primary.plan/2` had always keyed on the salt and shipped
`{:reset, 0, _}`; `FollowerLog.decide/2` compared only `wal_gen`, saw the same generation, and
demanded its old offset. The primary would send only 0, the follower would accept only its offset,
forever. `salt1` now crosses the wire (protocol `@version 2`).

**3. A tenant's first write always failed.** The seed was started out of band and the triggering
commit failed by design, on the reasoning that a write is the worst place for a multi-megabyte
transfer. Sound, but the consequence — an `OperationalError` on an unchanged Django app's first
INSERT, once per tenant — had never been measured. The commit now waits for the seed, bounded by
its own deadline.

**The common thread is the test environment, not the code.** Every replication test held ONE
connection for its whole run, so the WAL was never recreated; each rig statement is its own stream.
Both gaps are now closed with tests rather than left to the rig. Anything else that only shows up
across a stream boundary is still unguarded, and that is the shape to look for.

## Membership

`REPLICATION_MEMBERSHIP=roster` derives the follower set from addresses nodes publish to
`rebalancer_nodes` (`REPLICATION_ADVERTISE_HOST`), instead of a list every operator maintains on
every node. `static` remains the default and stays the floor: whenever the roster cannot supply
`quorum+1` candidates — fresh fleet, rolling upgrade, Postgres outage — membership falls back to it.

A set below `quorum+1` is **refused** and the previous set stays live. That is not caution, it is
the replacement for a guarantee that stopped holding: `Session.ship_planned/4` derives `n` from the
shipper list on every commit and `Quorum.new/2` raises when `q >= n`, *inside a tenant's write*.
Checking `q < n` once at boot was sound only while the set could never change.

Liveness still never filters the push set — the roster's staleness window is a *candidacy* filter
applied on a timer. A merely-down follower stays a member and costs nothing.

Still not built: per-shard follower sets and zone-aware placement. The RTT sweep makes placement an
82× lever, which makes it operator intent rather than something to infer.

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

> **CORRECTED 2026-08-09 — the conclusion below is wrong, and the paragraph after it says why.**
> The original text is kept verbatim rather than rewritten, because the mistake is the useful part:
> the answer was sitting in this document's own next sentence.

**`exqlite` 0.37.0 exposes no WAL-frame API.** Verified 2026-08-08 against `deps/exqlite/lib/`: the
only WAL-related surface in the entire library is `Exqlite.Pragma.wal_auto_check_point/1`, a
configuration pragma. There is no `sqlite3_wal_hook`, no frame read/apply, no backup API, no session
hook. **There is no seam to ship a frame from.**

This is the same class of gap as the UDF work (expert review #19), where the finding's recommended
`create_function` did not exist either and the fix was a loadable extension via
`enable_load_extension/2`. Establish the seam exists **before** designing against it.

### The seam exists. exqlite never had to expose it

The first sentence of that paragraph named the fix and the conclusion ignored it. exqlite's surface
is irrelevant, because fathom already ships a **loadable extension** (`native/fathom_udf`, loaded
per connection by `Fathom.Shard.Extension`), and an extension is handed a live `sqlite3*`.

- **`sqlite3_wal_hook` is in `sqlite3_api_routines`** — the extension pointer table, not merely the
  normal link surface. So it is reachable from exactly the one place fathom can already reach
  SQLite's C API.
- **rusqlite wraps it safely** as `Connection::wal_hook/1`, behind feature `hooks = []` — no new
  dependencies, a one-line `Cargo.toml` change.

**Proven at runtime, not just on the link surface** (`test/fathom/shard/wal_hook_test.exs`): the
hook fires on a real commit through `Fathom.Shard.Connection`, with the extension loaded the way
production loads it. The test is tagged `:wal_probe` and needs `FATHOM_WAL_PROBE=1` **in the real
OS environment** — `System.put_env` updates the BEAM's table, not C `environ`, so it cannot reach
native code. CI sets it; a local `mix test --include wal_probe` without it fails on a missing
`fathom_wal_commits`, which is a missing env var and not a regression.

**The trap, which nearly shipped as a disk-fill incident.** `sqlite3_wal_hook` and
`wal_autocheckpoint` are **the same slot** — auto-checkpointing *is* a built-in WAL hook, so
registering ours **evicts** it. `Connection.configure_readwrite/3` sets the pragma first and loads
the extension second, so a hook that merely observed would have silently disabled checkpointing on
**every tenant connection** and grown the WAL without bound — surfacing as the disk-fill alert
expert review #36 built, with the diagnostic pointing at storage rather than here. The hook
therefore re-implements what it displaced (PASSIVE checkpoint at the same threshold), and the test
asserts the **negative** — that the WAL still truncates — because a test that only proved the hook
fired would pass either way.

**What the hook gives and does not give.** It is a *notification* ("a commit landed; the WAL holds
N pages"), not frame bytes. The shipper reads the `-wal` from its last shipped offset, which is how
litestream works. That means the design owns WAL-truncation ordering: a checkpoint that runs before
frames ship destroys them, so the rule is *checkpoint only what the write quorum has already
acked*.

The lesson generalizes past this document: **a dependency's public API is not the boundary of what
is reachable.** Twice now — UDFs, then the WAL hook — the blocker was "exqlite does not expose it"
and the answer was "so do not ask exqlite."

### Options for clearing it — resolved, option 1 without its cost

**Option 1 won, and the "Large" estimate was wrong by a lot.** It assumed a NIF or a patched
exqlite; the extension already existed, so registering the hook was a feature flag and a `wal.rs`.
None of options 2–4 were needed and none were pursued. They are kept below as the record of what
was considered, not as open choices.

Effort labels are estimates, not measurements — and option 1's is a demonstration of how far off an
estimate can be when it costs the wrong mechanism.

1. **Write the NIF ourselves** (`fathom_native`, or patch exqlite). Register `sqlite3_wal_hook`,
   read committed frames out of the `-wal` file, apply them on the follower. Most control, most
   work; puts fathom on a forked or extended DB driver. **Large.** — **TAKEN, but via the existing
   loadable extension rather than a NIF or a fork, which is where the estimate went wrong. No
   forked driver, no new dependency.**
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

> **CORRECTED 2026-08-09.** That framing was a false dichotomy. A2 as built ships frames over its
> own socket protocol with **no BEAM distribution** — S3 stays the only cross-node *coordination*
> (lease, fence, cold open), and replication is a separate data path beside it rather than a
> replacement for it. Membership is a static config list, which is precisely the piece a BEAM
> cluster would have supplied and the honest cost of not having one. See
> [Decision gate](#decision-gate--cleared).

## What it would and would not improve

- **Would:** node-loss RPO from ~300 s to ~0. This is the entire benefit.
- **Would NOT: reduce S3 cost.** PUT count is driven by how *often* a dirty shard uploads, not how
  much — ingress bytes are free. Streaming frames *to S3* more often would cost **more** PUTs, not
  fewer. The win is only on the node-to-node path.
- **Would NOT:** remove the need for S3. It stays the cold backstop and the cold-open source.
- **Cost added:** every commit waits for ≥2 follower acks — a network round trip on the write path
  that does not exist today. Must be measured against `hrana_rt_us` before committing to it.
  **Measured 2026-08-09: +225 µs, 4.04× on the write, and ~130 µs of that is fathom's own overhead
  rather than the network.** Off by default, where the cost is one `Application.get_env` per write.
  Numbers and method in [Decision gate](#decision-gate--cleared).

## Decision gate — cleared

Both gates passed on `a2-quorum-replication`. Gate 3 did not apply, for a reason worth recording.

1. **A frame seam is proven to exist.** ✅ `test/fathom/shard/wal_hook_test.exs` — the hook fires
   on a real commit through `Fathom.Shard.Connection`, extension loaded as production loads it.
2. **The ack-latency cost is measured** against the current per-request round trip. ✅
   `test/fathom/shard/replication_cost_test.exs`, N=3 Q=2, loopback, p50 of 200: write only
   **74 µs**, write + replication **299 µs** — **+225 µs, 4.04×**. About **130 µs of that is
   fathom's own overhead**, not the transport (the raw-socket 2-of-4 floor was ~96 µs), i.e. a
   replicated write costs roughly two extra `hrana_rt_us` round trips of *local* work before any
   network. **With replication off the cost is noise**: one `Application.get_env` per write, and
   `hrana_rt_us` moved 127 → 130 µs across the integration commit.
3. **The BEAM-cluster reversal is accepted explicitly.** ❌ **Never arose — the premise was wrong.**
   "The architectural cost" above frames the real question as *"should fathom become a BEAM
   cluster."* It did not have to. A2 ships frames over its own socket protocol
   (`lib/fathom/shard/replication/`: `protocol`, `shipper`, `session`, `quorum`), with **no
   `Node.connect`, no distribution, no libcluster** — so the "reversal of the central decision"
   that section warns about did not happen. S3 remains the lease/fence authority. Membership is
   still a static config list, which is the honest limitation and the reason this is not a cluster
   in disguise.

**Until it is merged to `main`, the `main` position is unchanged**: EBS covers reboot, S3 covers
hardware and AZ loss, the 300 s interval bounds the exposure between them, and the quarantine keeps
a diverged copy recoverable. A2 is off by default on the branch too — `REPLICATION_ENABLED` and,
separately, `REPLICATION_PROMOTE_ON_OPEN`, so frames can ship and earn ordering stamps before
anything changes what a cold open serves.
