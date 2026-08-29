# A2 — Quorum replication

**Status: WORKING, ON `main` (merged 2026-08-12). ON BY DEFAULT IN PROD since 2026-08-29** — the
whole feature, not half of it: ship + receive + the ordinal push frame + frame authentication all
default ON in prod (off in dev/test — `config/config.exs`). A prod node now **fails to boot** unless
it is configured for it: `REPLICATION_FOLLOWERS` (else nothing to ship to), `REPLICATION_BIND_IP`
(else the unauthenticated port is on every interface), and a frame-auth key `HRANA_TOKEN_SECRET` /
`REPLICATION_HMAC_SECRET` (else signing has no key). That fail-closed boot is the enforcement of
"replication is core, not optional". Each gate is still separately forceable via env, and the
**staged wire-rollout order** (deploy → sign → require; deploy → then ordinal) now matters only for a
**future rolling upgrade across a frame-format change** — fathom is greenfield, so the first deploy
carries the whole shape to a fleet with no version boundary. Scoped
2026-08-08; both decision gates cleared the same day; the transport, commit path, seeding and
promotion built 2026-08-08/09; **proven end to end multi-node 2026-08-11** — `chaos.sh smoke`
passes with `REPLICATION_ENABLED=true` (five tenants, every write quorum-replicated, cross-shard
isolation intact); **survivor selection built 2026-08-12**, which is what makes the node-loss RPO
claim hold when the LB fails over to a node holding no replica — until then it held only when the
survivor happened to have one, and the rig measured an acked write lost while three peers held it.
Supersedes the one-paragraph deferral in [phase2-scoping](phase2-scoping.md) §A2.

**PROVEN ON THE RIG 2026-08-12** — `chaos.sh rpo`, two runs, and it found a bug that made the whole
thing inert in production (see [The touch erased the stamp](#the-touch-erased-the-stamp-and-that-is-why-nothing-was-ever-promoted)).

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
| Survivor selection | `Replication.Recovery` | Ask peers → choose → pull, behind `REPLICATION_RECOVER_FROM_PEERS`. |
| RPO proof | `chaos.sh rpo` | Kills with NO flush in between; runs both arms, so a run that proves nothing says so. |

**Not built:** per-shard follower sets, and zone-aware placement. The RTT sweep makes placement an
82× lever, which makes it an operator-intent decision rather than one to infer — see
[Replication factor](#replication-factor-n-vs-ack-threshold-q--do-not-set-q--n). Operator config is
[configuration.md](configuration.md#quorum-replication-phase-2-a2--on-by-default-in-prod).

### The RPO claim was conditional until 2026-08-12, and survivor selection is why

Recorded before the mechanism, because the shape of this miss is the useful part: **every component
worked and the guarantee still did not hold.** Frames shipped, quorums acked, promotion promoted.
What was missing was a single edge in the graph — nothing connected *which node holds a current
replica* to *which node the LB fails over to*.

The LB partitions the keyspace by consistent hash on the Host subdomain
([cluster-architecture](cluster-architecture.md)). That function knows about subdomains and node
counts; it knows nothing about replication. So the survivor it picks holds a current replica only by
coincidence — and when it does not, promote-on-open finds nothing local, the shard cold-opens from
S3, and the tail is gone. Measured on the rig 2026-08-11: an acked, quorum-replicated write **lost
while three other nodes held it**. Replication was not broken. Recovery was reading the wrong copy.

The fix is the one Waterpark already describes — *the replacement asks the followers and adopts the
state of the best reader* — and it needs **no BEAM cluster and no mailroom**. That rejection (see
[The architectural cost](#the-architectural-cost)) was about moving *streams*, because Hrana batons
are entry-node-local; this moves *bytes*, over the socket A2 already opened.

Four steps, and only one of them is new:

| step | mechanism |
|---|---|
| **Ask** every peer where its replica sits | `position_query` / `position` — new frames |
| **Choose** the best copy | `Promote.fresher?/2`, unchanged |
| **Pull** it | `seed_begin`/`seed_chunk`/`seed_end`, **in reverse** |
| **Publish** it | `Fathom.Shard`'s promote path, unchanged |

The pulled bytes are installed through `Follower`'s **own seed sink**, so they land in the replica
directory and the ETS row exactly as a pushed seed would. That is what lets the promote path stay
untouched: it cannot tell a pulled replica from one this node had been following all along, so there
is no second provenance story to get wrong.

**Safer than Waterpark's version in the one direction that matters.** Waterpark is RAM-only, so the
best reader is the *only* copy and is adopted whatever it says. Fathom has the stored object
underneath, so a peer is adopted only when it is **provably ahead of that object** — the same
`fresher?` test, with the same rules (`>` not `>=`; an unstamped object is never overridable; every
uncertain answer is `false`). When no peer can prove it, the open degrades to exactly the pre-A2
behaviour. There is no state in which this serves older bytes than leaving it off.

Two things it costs, both deliberate, which is why it is a **separate gate** rather than part of
`REPLICATION_PROMOTE_ON_OPEN`:

- one object-position read on every promote-eligible open (the local-only path reaches the object
  store only when this node actually holds a replica, and is kept bit-for-bit for that reason);
- one round trip to each peer, **concurrent** so it is one round trip and not N, bounded by
  `REPLICATION_RECOVERY_TIMEOUT_MS`. A node already holding the freshest copy short-circuits before
  opening a socket.

**The new frames were added at `@version 2` rather than bumping to 3**, which reverses the reasoning
the salt1 change used. They are purely additive, and the two options are not symmetric: a bump makes
`decode/1` refuse *every* frame from a node one deploy behind, taking the commit path down
fleet-wide for the length of a rolling upgrade. Leaving the version alone means an old peer answers
a query with `{:error, :malformed}` and closes that one short-lived socket, which `Recovery` already
treats as "no offer". Both are loud; only one also breaks replication.

The lineage-carrying seed frame (2026-08-24 #12) was added the same way, for the third time — a new
code at `@version 2`, gated on `REPLICATION_LINEAGE_WIRE` so a node emits it only once an operator
confirms the code is everywhere. Until that flag is on, `fresher?/2` sees `lineage: 0` on every
replica and refuses to rank it, so promotion stays inert — which is what it already was, not a
regression.

### The ordinal, and why promotion is inert until you turn it on (2026-08-26 #2)

`fresher?/2`'s second component was `wal_gen`, and `wal_gen` is SQLite's `ckpt_seq`: it counts
checkpoints **within one WAL file** and restarts at 0 when SQLite deletes and recreates that file —
which it does after every Hrana stream on a quiet shard. Measured here, two consecutive streams both
read `ckpt_seq=0` with salts 977542977 then 978380554. So the ordering could compare positions from
two *unrelated* WALs and promote a replica over an object that is strictly newer, losing the acked
tail silently and indistinguishably from A2 being switched off.

The replacement is a monotone per-lineage **ordinal**, assigned by the shard coordinator
(`Fathom.Shard.wal_ordinal/2`). It answers the same number for the same salt and a higher one for a
new salt, which is what lets the object's stamp and the replicas' positions be on ONE scale: the
coordinator stamps it at flush, `Replication.Session` pushes it, and both read the same answer. It
rides two new type codes — one on the push frame, one on the peer position offer — behind
`REPLICATION_ORDINAL_WIRE`, which **defaults off**, for the same reason every other code did.

**Until you turn it on, promote-on-open and cross-fleet recovery do nothing.** Every replica states
ordinal 0, `fresher?/2` refuses to rank a 0 in either direction, and both paths fall back to the
stored object. That is not a regression to route around: it is the honest replacement for an
ordering that was wrong on a data-loss path. Roll the code out, confirm it is everywhere, then set
`REPLICATION_ORDINAL_WIRE=true`.

Both halves of the offer matter. The push frame is what gives a node's OWN replica an ordinal; the
position offer is what lets a survivor rank someone else's. With only the first, local promotion
works and a survivor that holds nothing still cold-opens the stale object — the exact failover this
subsystem exists for.

**Still conditional on one thing:** the recovering node must have `REPLICATION_LISTEN` on, since the
pull installs through its local replica directory. That matches the documented rollout order, and a
node that cannot hold a replica could not be a useful survivor for anyone else either.

### The touch erased the stamp, and that is why nothing was ever promoted

The bug `chaos.sh rpo` found the first time it ran, and the reason building the scenario was worth
more than the feature it was written to check.

A takeover **touches** the shard object — a server-side self-copy that rotates its etag, which is
what fences the deposed node's `If-Match` flush. S3 requires the `REPLACE` metadata directive for a
self-copy, and REPLACE **drops every user metadata key unless it is sent again**. The touch re-sent
one: the integrity md5 (#12/#17). `x-amz-meta-fathom-pos` — the position stamp — was added later
for A2 and nobody added it to that list.

So **every takeover erased the stamp.** And an unstamped object is deliberately never overridable
(the rule that makes promotion inert rather than dangerous on an un-upgraded fleet). The two
features meet at exactly the wrong moment: the stamp is destroyed by the same operation that
creates the only situation where it is ever read.

Consequences worth being blunt about:

- **promote-on-open has never worked on a real failover**, since it shipped. Not "worked
  sometimes" — the object it compares against is unstamped by the time it looks.
- Nothing failed and nothing logged. The shard recovered to its last flush, which is the pre-A2
  behaviour, which is exactly what a reader would expect to see if A2 were merely disabled.
- The measurement was unambiguous once the scenario printed both inputs at the moment of
  comparison: stamp `%{epoch: 1, wal_gen: 0, offset: 0}` before the kill, `nil` after, with a peer
  sitting at offset 8272.

**The unit suite could not have caught it.** `Storage.Local` and `Fathom.Test.FaultyStorage` keep
their metadata in place across a touch, so "REPLACE drops user metadata" is a property only the
real backend has — the same class of gap AGENTS.md records for the lock-etag contract, where a
double that could not express the bug silently exempted an entire class of them.

The fix is **carry every user metadata key**, not "carry this second one": the failure mode is an
omission from a list, and a list that has been wrong once will be wrong again the next time a key
is added. `S3.carry_meta/1` is public and directly tested for that reason, because the behaviour it
protects cannot be reproduced against either double.

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
- **`REPLICATION_BIND_IP` is a security control** rather than tuning: unset, `:gen_tcp` binds every
  interface. The boot line names the interface it bound and says so when it is `0.0.0.0`. In prod,
  listening on a wildcard now REFUSES TO BOOT.
- **The replication port was unauthenticated until 2026-08-22**, and the posture statement the
  2026-08-20 review asked for is this, in these words: *a reachable replication port was equivalent
  to write access to every shard on the node.* Anyone who could open a TCP connection could seed a
  forged replica for any shard and have it promoted over the tenant's durable object. There was no
  credential in the protocol and no peer allowlist; network reachability was the whole control.

  `REPLICATION_SIGN_FRAMES` + `REPLICATION_HMAC_REQUIRED` close that (`Fathom.Shard.Replication.FrameAuth`).
  **Both default off**, and the rollout is ordered — deploy, then sign fleet-wide, then require —
  for the same reason `REPLICATION_LISTEN` must precede `REPLICATION_ENABLED`. Until you have run
  that rollout, the sentence above still describes your fleet. See
  [`runbooks/replication-frame-auth.md`](runbooks/replication-frame-auth.md).

  **What it does not do**: the signature covers the frame HEADER, not the payload. It stops an
  unauthenticated peer from constructing an acceptable frame at all, which is the threat it is
  sized for. It does not attest the WAL bytes against an on-path attacker who can rewrite an
  authenticated peer's traffic — there is no TLS here. Hashing up to `REPLICATION_MAX_PUSH_BYTES`
  of WAL on the commit path was judged too expensive for that; the trade is recorded in
  `FrameAuth`'s moduledoc and pinned by a test that fails if payload coverage is ever added.

Proven multi-node on the chaos rig (`./chaos.sh replication`): all five nodes accept from their
peers, and with shipping on, a write to `acme` on its home node lands as a replica on all four
others. That run also found the rig itself had been broken since 2026-08-08 — five nodes at
`POOL_SIZE=25` exceed Postgres's default `max_connections=100`, and no scenario had run since the
node count was raised.

### No replicated write succeeded until 2026-08-11, and the wire was why

The second thing running it multi-node found, after the missing listener. Every commit past the
first failed `{:no_quorum, :impossible}` with all four followers answering `offset_mismatch` at the
SAME offset — a deadlock, not a lost frame.

`ckpt_seq` counts checkpoints **within one WAL file**. SQLite deletes the `-wal` when the last
connection to a shard closes — after every Hrana stream on a quiet shard — and the next stream
creates a fresh one with new salts and `ckpt_seq` back at **0**. `Primary.plan/2` had always keyed
on the salt (`when seq != gen or s != salt`) and correctly shipped `{:reset, 0, _}`;
`FollowerLog.decide/2` compared only `wal_gen`, saw the same generation, and demanded its old
offset. The primary would send only 0; the follower would accept only its offset. Forever.

`salt1` now crosses the wire — protocol **`@version 2`**. `decode/1` already refuses a version
mismatch, so a mixed-version fleet fails loudly rather than misparsing a frame.

**A checkpoint does not reproduce it**: `TRUNCATE` bumps `ckpt_seq` too, so both sides agree a
boundary happened. Only a *recreated* WAL diverges, which is why a suite that held one connection
per test could never see it. Both gaps now have tests — one integration test drops all connections
mid-run, one pure-function test asserts a same-generation/different-salt push is a new lineage.

### A tenant's first write used to fail

Third one. The seed was started out of band and the triggering commit failed by design, so that a
write never blocked on a multi-megabyte transfer. Sound reasoning, unmeasured consequence: an
`OperationalError` on an unchanged Django app's first INSERT, once per tenant, forever. The commit
now waits for the seeds it started, bounded by its own deadline — and waits for a **quorum**, not
all N, or the first write would pay for the slowest replica.

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

Note the shape of that sentence — *"the survivor is elsewhere."* Replicating the frames is only half
an answer to it, because the survivor being elsewhere is also why it may hold no replica of its own.
Closing the gap takes `REPLICATION_LINEAGE_WIRE` (without it the comparison is between two different
counters and never fires) **and** `REPLICATION_PROMOTE_ON_OPEN` **and** `REPLICATION_RECOVER_FROM_PEERS`;
see [survivor selection](#the-rpo-claim-was-conditional-until-2026-08-12-and-survivor-selection-is-why).

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
