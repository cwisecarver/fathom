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

## Replication factor (N) vs ack threshold (Q) — do NOT set Q = N

The single most tempting mistake here is "more confirmations = safer, so confirm to **all** N before
acking." It is the opposite of Waterpark's spec and it is worse on every axis that matters.

`N` = how many followers hold the shard. `Q` = how many must ack before the primary commits. They
are independent knobs and they trade in opposite directions.

| N | Q | Tolerates (writes keep working) | Commit waits for | Notes |
|---|---|---|---|---|
| 2 | 1 | 1 follower down | fastest of 2 | viable on a 3-node cluster |
| 4 | **2** | **2 followers down** | **2nd fastest of 4** | **Waterpark's actual spec** |
| 4 | 3 | 1 follower down | 3rd fastest of 4 | |
| 4 | 4 | **0 followers down** | **slowest of 4** | every write blocks on the worst link |

**Q = N tolerates zero failures.** One slow or dead follower stops every write to that shard — so
`N=4, Q=4` is *less* available than `N=4, Q=2`, despite holding the same four copies. Raising Q
converts spare replicas from redundancy into liabilities.

**Q = N also inherits the worst tail latency.** With Q=2 the primary waits for the 2nd of 4 acks, so
one straggler is invisible. With Q=4 it waits for the max of 4 draws: by order statistics the
*median* commit then lands near a single follower's **p84**, against roughly its **p31** at Q=2.
Every commit pays the worst cross-DC link, on a write path that today has no network hop at all.

**And it buys almost no durability.** At `N=4, Q=2` you must lose the primary *and* both acking
followers simultaneously to lose a write — and fathom, unlike Waterpark, still has the S3 object
underneath. Waterpark is RAM-only with no backstop and still chose 2-of-4; fathom at Q=2 is
therefore **strictly safer than the reference architecture**, not less safe.

**If you want more safety, raise N, not Q.** `N=6, Q=2` is simultaneously more durable *and* more
available than `N=4, Q=4`. That is the whole reason quorum replication exists.

**What this costs at fathom's scale, which Waterpark did not pay.** A Waterpark actor is one
patient's message log; fathom's unit is an entire SQLite database, and the thesis is millions of
them. Follower copies are cheap on the axis that matters — a warm-cached shard is disk-bound with
~0 BEAM/fd overhead (see [warm-standby](warm-standby.md)) — but **write fanout is not**: every
commit ships frames to N nodes. N is therefore a *network* decision, not a storage one, and it
should be configurable per deployment rather than hardcoded to Waterpark's 4.

### Matching Waterpark exactly means FIVE nodes, not four

Easy off-by-one, and it costs half the fault tolerance. Waterpark runs **1 primary + 4 read-only
followers per actor** — "one read-only follower process at each data center", four DCs, and the ack
rule quotes "two of its **four** read-only followers". `N` counts **followers**, so `N = 4` needs
**5 nodes to hold one shard**, on a cluster of 8.

A 4-node cluster is therefore `N = 3, Q = 2`, which tolerates **1** follower loss where Waterpark
tolerates **2**.

| Cluster nodes | N (followers) | Q | Tolerates | Waterpark parity |
|---|---|---|---|---|
| 3 | 2 | 1 | 1 loss | no |
| 4 | 3 | 2 | 1 loss | no — one follower short |
| **5** | **4** | **2** | **2 losses** | **yes** |
| 8 | 4 | 2 | 2 losses | yes (Waterpark's own size) |

**Placement is the actual spec, more than the count.** Waterpark's guarantee is not "4 followers",
it is *one follower per failure domain* — that is what makes a 2-of-4 ack survive losing a whole
data centre. Four followers packed into one AZ satisfy `N = 4` and give **nothing** against AZ loss,
which is precisely the failure S3 was covering before A2. So placement must be a **spread
constraint over failure domains**, not a free choice of any 4 peers, and a deployment that cannot
satisfy the spread should say so loudly at boot rather than silently degrade to co-located replicas.

**Defaults: `N = 4, Q = 2`, minimum cluster size 5**, one follower per failure domain. Fall back to
`N = 2, Q = 1` only on a 3-node cluster, and treat that as a development topology, not a production
one. Boot must validate **`Q < N`** (else the fault tolerance is configured away) and
**`cluster_size ≥ N + 1`** (else the shard cannot place its followers at all).

## The fathom mapping

The component already exists. `Fathom.Shard.WarmFollower` (A1, built) holds a lease-less copy of
another node's shard and never serves. A2 is the same component with the data path reversed:

| Today (A1) | A2 |
|---|---|
| follower **pulls** from S3, asynchronously | primary **pushes** frames to followers |
| copy may be stale; revalidated by etag | copy is current as of the last acked commit |
| failover pulls the object, loses the tail | failover promotes a follower, loses nothing |
| no effect on ack latency | commit waits for **≥2 follower acks** |

## The blocker — and why it is much smaller than it looked

**`exqlite` 0.37.0 exposes no WAL-frame API.** Verified 2026-08-08 against `deps/exqlite/lib/`: the
only WAL-related surface in the entire library is `Exqlite.Pragma.wal_auto_check_point/1`, a
configuration pragma. No hook, no frame read/apply, no backup API.

**That framing was wrong, though — exqlite does not have to expose it.** Same day, same reasoning as
the UDF work (expert review #19): exqlite had no `create_function` either, and the fix was to stop
asking exqlite and use a **loadable extension**. Fathom already ships one (`native/fathom_udf`,
loaded per connection by `Fathom.Shard.Extension`), and an extension receives a live
`rusqlite::Connection` wrapping the host's `sqlite3*`.

Verified against the vendored sources:

- **`sqlite3_wal_hook` is in `sqlite3_api_routines`** — the loadable-extension pointer table, not
  just the normal link surface (`libsqlite3-sys-0.38.1/bindgen-bindings/bindgen_3.34.1_ext.rs:2325`,
  resolved at `:6813` via `if let Some(fun) = (*p_api).wal_hook`). So it is reachable from an
  extension, which is the only place fathom can reach SQLite's C API today.
- **rusqlite wraps it safely** — `Connection::wal_hook(Option<fn(&Wal, c_int) -> Result<()>>)`
  (`rusqlite-0.40.1/src/hooks/mod.rs:398`), behind feature **`hooks = []`** — no additional
  dependencies, a one-line `Cargo.toml` change.
- **`Wal` carries `db` + `db_name` and can `checkpoint`/`checkpoint_v2`**, so the hook can also
  control when the WAL is truncated — which the shipper must, or frames vanish before they ship.

**What the hook gives and does not give.** It is a *notification*: "a commit landed; the WAL now
holds N pages." It does **not** hand over frame bytes. The shipper reads the `-wal` file from its
last shipped offset, which is how litestream works, and is sufficient — but it means the design
must own WAL-truncation ordering, because a checkpoint that runs before frames ship loses them.

**Still unverified — this is gate 1, below.** That the hook actually *fires at runtime* through the
SQLite that exqlite bundles, with the extension loaded the way `Shard.Extension` loads it. The
evidence above is link-surface evidence, not a running callback. Treat a promising-looking seam the
same way AGENTS.md says to treat a suspiciously good benchmark: unproven until it runs.

### The trap: registering the hook DISABLES `wal_autocheckpoint`

`sqlite3_wal_hook` and `wal_autocheckpoint` are **the same slot**. rusqlite says so directly
(`rusqlite-0.40.1/src/hooks/mod.rs:396`): *"the `sqlite3_wal_autocheckpoint()` interface and the
`wal_autocheckpoint` pragma both invoke `sqlite3_wal_hook()` and will overwrite any prior
`sqlite3_wal_hook()` settings."* Auto-checkpointing **is** a built-in WAL hook; registering ours
evicts it.

Both orderings are wrong, and fathom is currently in the worse one:

- **Pragma after hook** → the pragma silently evicts our hook. A2 quietly ships nothing; the shard
  looks replicated and is not.
- **Hook after pragma** → our hook evicts auto-checkpointing. **The WAL grows without bound.**

`Fathom.Shard.Connection.configure_readwrite/3` runs `configure/1` first
(`lib/fathom/shard/connection.ex:79`, which sets `PRAGMA wal_autocheckpoint=4000` at `:196`) and
`load_extension/1` second (`:80`). So a hook registered naïvely from `fathom_udf` lands in the
**second** case — unbounded WAL growth on every tenant connection, presenting as the disk-fill
failure that expert review #36 built `FathomDiskFillingUp` for, with the diagnostic pointing at
storage rather than at this.

That threshold is not a default to be casually displaced: it was deliberately raised from SQLite's
1000 frames to 4000 by expert review 2026-07-24 #4, because an autocheckpoint runs **inline inside
the committing tenant's query**.

**So the A2 hook must take over checkpointing, not merely observe.** This is why
`Wal::checkpoint_v2(CheckpointMode)` is handed to the callback — the design falls out of the
constraint. And it is load-bearing for A2 beyond replacing what it displaced: a checkpoint
**truncates the WAL**, so checkpointing before frames ship destroys exactly the data being
replicated. The hook's real rule is therefore *checkpoint only what has already been acked by the
write quorum*, which makes WAL truncation a **downstream consequence of replication progress**
rather than an independent timer.

Anyone implementing this must also assert the negative: a test that the WAL still gets checkpointed
with the hook installed. The failure is silent, slow, and looks like someone else's bug.

### Options for clearing it

Effort labels are estimates, not measurements.

1. **Register the hook from the existing loadable extension** — `native/fathom_udf` + feature
   `hooks`, then read committed frames out of the `-wal` file and apply them on the follower.
   **This is now the leading option and it is NOT large**: no NIF, no exqlite fork, no new build
   step, and the load/enable/disable security sequence `Shard.Extension` already performs is
   unchanged. Was scoped as "write the NIF ourselves / patch exqlite, **Large**" before the
   extension route was checked — recorded because the obvious-looking answer (fork the driver) is
   the expensive one and someone will propose it again.
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

A2 puts tenant **data** on a node-to-node path. Fathom's cluster model was deliberately
LB-keyspace-partition plus an S3 lease, with **S3 as the only cross-node coordination** — no BEAM
cluster, no ring, no mailroom. A quorum ack needs cluster membership, follower liveness, and a
promote protocol: a reversal of the central decision, not an addition to it.

**That reversal was accepted on 2026-08-08** — see
[cluster-architecture](cluster-architecture.md#amendment-2026-08-08--the-s3-only-rule-is-lifted-for-a2).
The framing was never "should we add WAL streaming" but **"should fathom become a BEAM cluster,"**
and the answer is now yes, because the S3-only rule made node-loss RPO irreducible and Waterpark is
the evidence the clustered answer works at scale where losing a write is unacceptable.

The cost is accepted, not eliminated. It is still the largest single change to fathom's model, and
it is not cheaply reversible — which is why the gates below survive the decision.

**Explicitly still rejected:** the BEAM-forwarding *mailroom* and the `base_url` *redirect*
(cluster-architecture "Why not the alternatives"). Those fail on Filo's entry-node-local stream
batons, not on the S3 rule. A2 replicates **committed data**; it does not move **streams**.

## What it would and would not improve

- **Would:** node-loss RPO from ~300 s to ~0. This is the entire benefit.
- **Would NOT: reduce S3 cost.** PUT count is driven by how *often* a dirty shard uploads, not how
  much — ingress bytes are free. Streaming frames *to S3* more often would cost **more** PUTs, not
  fewer. The win is only on the node-to-node path.
- **Would NOT:** remove the need for S3. It stays the cold backstop and the cold-open source.
- **Cost added:** every commit waits for ≥2 follower acks — a network round trip on the write path
  that does not exist today. Must be measured against `hrana_rt_us` before committing to it.

## Decision gate

~~3. The BEAM-cluster reversal is accepted explicitly.~~ **Accepted 2026-08-08** — the "S3 is the
only cross-node coordination" rule is lifted; see
[cluster-architecture](cluster-architecture.md#amendment-2026-08-08--the-s3-only-rule-is-lifted-for-a2).
Two gates remain, in order:

1. **A frame seam is proven to exist** — reading and applying a single committed frame between two
   processes. Cheapest experiment that kills the premise, and the one that decides which option is
   even available. **Partially cleared 2026-08-08**: the seam exists on the link surface (option 1
   above — `sqlite3_wal_hook` is reachable from the loadable extension fathom already ships). What
   remains is the runtime half — register the hook in `fathom_udf`, drive a tenant commit through
   `Fathom.Shard.Connection`, and assert the callback fired with the expected page count. Until
   that assertion runs, the seam is *plausible*, not proven.
2. **The ack-latency cost is measured** against the current per-request round trip (`hrana_rt_us`),
   because a quorum ack puts a network hop on the write path that does not exist today.

Until both clear, the standing position holds: EBS covers reboot, S3 covers hardware and AZ loss,
the 300 s interval bounds the exposure between them, and the quarantine keeps a diverged copy
recoverable.
