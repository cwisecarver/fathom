# A2 — Quorum replication

**Status: BUILT on branch `a2-quorum-replication`, off by default, not on `main`.** Scoped
2026-08-08; both decision gates cleared the same day; the transport, commit path, seeding and
promotion built 2026-08-08/09. Supersedes the one-paragraph deferral in
[phase2-scoping](phase2-scoping.md) §A2.

The blocker section below is kept as written, because the thing it describes as blocking is exactly
what turned out not to — see the gate-1 note in [Decision gate](#decision-gate). The header used to
read "design only, blocked on a dependency"; it was wrong, and re-reading why is more useful than
deleting it.

## What exists

| Piece | Module | Note |
|---|---|---|
| Wire format | `Replication.Protocol` | Explicit binary; no `binary_to_term` on socket bytes. |
| Accept/reject | `Replication.FollowerLog` | Pure. Every way a follower can corrupt a shard is a branch here. |
| Quorum counting | `Replication.Quorum` | Pure. `Q < N` enforced at construction AND at boot. |
| Transport | `Replication.Shipper` / `Follower` | One socket per follower **node**, not per shard. |
| Commit path | `Replication.Session` | Per-shard serialization point; per-follower offsets. |
| Seeding | `Protocol` + `Follower` | Streamed; a partial seed is never installed. |
| Promotion | `Replication.Promote` | Wired into cold open behind `REPLICATION_PROMOTE_ON_OPEN`. |
| Receive half | `Replication.Follower` | Supervised by `Fleet` behind `REPLICATION_LISTEN`. |
| Membership | `Replication.Membership` | Static list **or** the fleet roster, behind one guarded swap. |

**Not built:** per-shard follower sets, and zone-aware placement. The RTT sweep makes placement an
82× lever, which makes it an operator-intent decision rather than one to infer — see
[Replication factor](#replication-factor-n-vs-ack-threshold-q--do-not-set-q--n). Operator config is
[configuration.md](configuration.md#quorum-replication-phase-2-a2--off-by-default).

### The receive half was missing entirely until 2026-08-10

Worth recording, because it made every other row in that table untrue in practice and nothing said
so. `Replication.Follower` — the listener that accepts frames, applies them and acks — **was never
started outside the test suite.** `Fleet` supervised a Registry, a DynamicSupervisor and the
Shippers: the primary half, all of it.

So `REPLICATION_ENABLED=true` on a real node shipped every commit into a closed port, collected no
acks, and returned 503 `FILO_NO_QUORUM` for every tenant write — while `configuration.md`
instructed operators to point `REPLICATION_FOLLOWERS` at `node_key@host:port` for a port fathom
never opened. It was not on this doc's "Not built" list either, so it read as finished.

Found while starting the membership work, which would otherwise have automated the discovery of
endpoints that cannot answer. Two consequences kept:

- **Shipping and receiving are separate gates** (`REPLICATION_ENABLED` / `REPLICATION_LISTEN`). A
  node can hold others' replicas without replicating its own shards, and a safe rollout turns
  listening on fleet-wide *first*. One flag cannot express that order.
- **The replication port is unauthenticated**, so `REPLICATION_BIND_IP` is a security control
  rather than tuning: unset, `:gen_tcp` binds every interface. The boot line names the interface it
  bound and says so when it is `0.0.0.0`.

Proven multi-node on the chaos rig (`./chaos.sh replication`): all five nodes accept from their
peers, and with shipping on, a write to `acme` on its home node lands as a replica on all four
others. That run also found the rig itself had been broken since 2026-08-08 — five nodes at
`POOL_SIZE=25` exceed Postgres's default `max_connections=100`, and no scenario had run since the
node count was raised.

### Membership: the static list is now a floor, not the only option

`REPLICATION_MEMBERSHIP=roster` derives the follower set from addresses nodes publish to
`rebalancer_nodes` (`REPLICATION_ADVERTISE_HOST`), so adding or replacing a node stops meaning
"edit every other node's config". `static` remains the default.

The guarantee that had to be rebuilt: `Session.ship_planned/4` derives `n` from `Fleet.shippers/0`
on **every commit**, and `Quorum.new/2` raises when `q >= n` — inside a tenant's write. Checking
`q < n` once at boot was sound only while the set could never change. So every swap re-establishes
it: a set below `quorum+1` is **refused** and the previous set stays live, an unreadable roster
keeps the previous set, and the roster falls back to `REPLICATION_FOLLOWERS` whenever it cannot
supply `quorum+1`. Liveness still never filters the push set — the staleness window is a
*candidacy* filter applied on a timer.

A membership change is not free: a newly added follower holds no base copy, so its first push
rejects and triggers a seed **per shard**, over the one socket that also carries every other shard.

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

## What a replicated write actually costs (measured 2026-08-09)

Gate 2 measured the quorum ack on raw sockets; the bench gate measured the hot path with
replication **off**. Neither says what a real fathom write costs with it **on**, because the
composition adds work neither touched: a WAL header read, a `pread` of the delta, a `GenServer`
hop into the per-shard `Session`, and the encode.

`test/fathom/shard/replication_cost_test.exs` (`:bench`) measures the same write both ways.
N=3, Q=2, loopback followers, RAM ack, p50 of 200 samples:

| | p50 |
|---|---|
| write only (replication off) | **74 µs** |
| write + replication | **299 µs** |
| **added by replication** | **225 µs (4.04×)** |

Read alongside the two numbers that bracket it: gate 2's raw-socket 2-of-4 floor was ~96 µs, so
roughly **130 µs of the 225 is fathom's own overhead** rather than the transport. And fathom's whole
per-request round trip (`hrana_rt_us`) is ~127 µs, so a replicated write costs about **two extra
request round trips of local work** before any network is involved.

**With replication OFF the cost is noise** — the gate check is one `Application.get_env` per write,
and the bench gate measured `hrana_rt_us` 127 → 130 µs across the integration commit.

**Deployment cost is this plus one RTT to the 2nd-fastest follower**, which dominates it: same-AZ
~0.1–0.25 ms, cross-AZ ~0.5–1.5 ms. That is why placement, not replica count, is the decision — see
the RTT sweep above.

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

1. ~~A frame seam is proven to exist.~~ **CLEARED 2026-08-08 — the hook fires.**
   `native/fathom_udf/src/wal.rs` registers `sqlite3_wal_hook` from the loadable extension, and
   `test/fathom/shard/wal_hook_test.exs` drives a real commit through `Fathom.Shard.Connection` and
   asserts the callback ran with a non-zero page count. Option 1 is the one that was available, and
   it needed no NIF, no exqlite fork, and no new build step.

   It also produced the constraint above: the hook **takes over checkpointing**, verified to
   discriminate (with the takeover disabled the main database is 4096 bytes after 25 MB of writes —
   every page still in the WAL). Notify-plus-checkpoint-control is therefore the actual shape of the
   seam, and shipping frames still has to read the `-wal` file itself.

   **The receive half is cleared too (same day).** `test/fathom/shard/wal_apply_test.exs` ships two
   successive commits to a follower as **byte-range deltas appended to its `-wal`** and the follower
   reads all three rows. **Incremental WAL shipping works on stock SQLite** — no
   `libsql_wal_insert_frame`, no engine swap, no NIF. The shipper is literally "read `-wal` from the
   last shipped offset, send the bytes, append them on the follower"; nothing rewrites headers or
   recomputes checksums. Verified to discriminate: with the append suppressed the follower sees only
   `[["first"]]`. The follower's main `.db` is asserted byte-identical to the base copy throughout,
   so every row after the first demonstrably arrived as WAL bytes and not through the file copy.

   **So options 2 and 3 below are no longer needed for the seam.** Option 1 covers both directions.

### A follower must not casually open and close its database

Discovered while building the receive test, and it is a design constraint rather than a test
artifact. A **clean close checkpoints**: measured, one open-and-close of the follower moved its
`.db` from **4096 → 8192 bytes** and **deleted** its `-wal`. The next delta then landed on a file
whose lineage no longer matched the offsets it was computed against, and the test failed with a
message confidently blaming stock SQLite — the wrong conclusion, whose action would have been
"adopt libSQL".

Consequences A2 must carry:

- A follower is a **passive recipient of bytes** until promotion. It does not hold an open
  connection to the shard it is following, and any tooling that peeks at a follower's database
  (a health check, an operator running `sqlite3` on it, a restore drill) **desynchronizes it** and
  forces a full re-seed.
- **Promotion** is the first legitimate open, and its first clean close will checkpoint — so the
  promote path must treat that as expected and re-establish offsets rather than assume continuity.
- The follower's WAL and the primary's are a **byte-offset relationship**. Anything that rewrites
  either side independently breaks it, which is the same reason the primary may only checkpoint
  what the quorum has acked.
2. ~~The ack-latency cost is measured.~~ **CLEARED 2026-08-08** — `test/fathom/shard/wal_quorum_bench_test.exs`
   (`:bench`-tagged). Measured on loopback against the prod baseline `hrana_rt_us` **127 µs**:

   | scenario | 2-of-4 | 4-of-4 |
   |---|---|---|
   | all four followers healthy, ack from RAM | **96 µs** | 74 µs |
   | all four healthy, ack after `fdatasync` | **398 µs** | 469 µs |
   | **two of four stragglers (+5 ms each)**, ack from RAM | **185 µs** | **5 994 µs** |

   **RTT sweep — done 2026-08-08, toxiproxy, real TCP.** The loopback floor above plus injected
   one-way latency. (Toxiproxy itself adds ~450 µs of proxy hop: the 0 ms cell reads ~550 µs where
   in-process loopback read 96 µs, so read the RTT-proportional part, not the absolute.)

   **Uniform placement — all four followers at the same latency:**

   | one-way | RTT | 2-of-4 | 4-of-4 |
   |---|---|---|---|
   | 0 ms | 0 | 557 µs | 586 µs |
   | 10 ms | 20 ms | 26.1 ms | 25.5 ms |
   | 30 ms | 60 ms | 73.2 ms | 73.4 ms |
   | 60 ms | 120 ms | 131.7 ms | 131.4 ms |

   **2-of-4 buys nothing here — it tracks 4-of-4 at every latency.** Same finding as the loopback
   straggler result, in its other form: a quorum skips the *slowest* replicas, so when every replica
   is equally slow there is nothing to skip. Spreading four followers evenly across equidistant
   failure domains gets you redundancy and **no latency benefit at all**.

   **Asymmetric placement — two near, two far:**

   | near | far | 2-of-4 | 4-of-4 | cost of `Q=N` |
   |---|---|---|---|---|
   | 0 ms | 0 ms | 520 µs | 549 µs | 1.06× |
   | 0 ms | 30 ms | **1.5 ms** | 72.4 ms | **49×** |
   | 0 ms | 60 ms | **1.6 ms** | 134.0 ms | **82×** |
   | 10 ms | 60 ms | 26.1 ms | 131.0 ms | 5.0× |

   **This is the result that should drive placement.** With two near followers, a commit acks in
   ~1.5 ms while the far pair — 60 ms away — catch up asynchronously, and the shard still holds
   five copies across distant failure domains. Force `Q = N` on the same topology and every commit
   pays 134 ms instead: **82× worse for the same replicas.**

   **The placement rule, which "one follower per failure domain" alone does not give you:** a
   **quorum's worth of followers must be near the primary**, or the quorum has no latency benefit
   and only buys failure tolerance. With `Q = 2` that means **two near followers, not one**.

   **And the tension to resolve deliberately:** near-for-latency pulls against spread-for-failure.
   If both near followers sit in the *primary's own* AZ, one AZ failure takes 3 of the 5 copies and
   leaves exactly `Q` behind with zero slack. The shape that satisfies both is **two followers in a
   nearby but different AZ (~0.5–1.5 ms) and two in another region** — sub-2 ms acks that still
   survive losing any single AZ, including the primary's.

   Three results, in order of how much they should change the design:

   - **`Q = N` is catastrophic under a straggler: 5 994 µs vs 185 µs, a 32× difference.** The
     `Q < N` argument above is now measured, not argued. All-N waits for the slowest replica by
     definition, and one degraded follower stalls every commit on that shard.
   - **A quorum barely notices the stragglers** — 185 µs against 96 µs healthy. Two fast followers
     answer while two slow ones are still working. This is what the redundancy is *for*, and it is
     the entire reason Waterpark acks at 2-of-4.
   - **`fdatasync` on the follower is the expensive choice: 398 µs vs 96 µs**, i.e. ~300 µs added,
     **2.4× fathom's whole current request round trip**. Waterpark acks from RAM and takes its
     durability from replica count instead. Fathom can afford to be stricter — it also has the S3
     object underneath — but this is the cost, and it should be a config knob with the number
     attached rather than an unexamined default.

   **A methodology note worth keeping.** The first version of this measurement compared 2-of-4 and
   4-of-4 with four *identical* healthy followers, and 4-of-4 came out **faster** (74 µs vs 96 µs) —
   which is impossible for an order statistic and tripped the harness's own guard. The cause is not
   a bug but the finding itself: with every replica equally fast the acks land within noise and the
   quorum is unresolvable. **Quorum buys nothing when replicas are uniform and everything when one
   is not**, so any future measurement here must inject a straggler or it is measuring noise.

Until both clear, the standing position holds: EBS covers reboot, S3 covers hardware and AZ loss,
the 300 s interval bounds the exposure between them, and the quarantine keeps a diverged copy
recoverable.
