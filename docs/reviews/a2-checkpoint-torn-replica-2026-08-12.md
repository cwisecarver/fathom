# A checkpoint tears every follower's replica, and nothing marks it unpromotable

**2026-08-12. Found on the chaos rig. `REPLICATION_PROMOTE_ON_OPEN` served an EMPTY database over a
working stored object.** Both gates are off by default, so nothing in production is exposed — but
this is the failover path A2 exists for, and it is the path the RPO claim rests on.

## What was observed

`chaos.sh pause-fence` with `REPLICATION_ENABLED=true REPLICATION_PROMOTE_ON_OPEN=true`:

```
FAIL: survivor could not steal
```

The survivor's write returned `SQLITE_ERROR: no such table: kv`. The same scenario with promote
**off** passes on the same image, so the stored object was fine and promotion is what broke it.

Proof that working data was replaced, read back through the pre-promotion snapshot promotion itself
takes:

| | contents |
|---|---|
| pre-promotion stored object | `kv` present, row `seq=1` — a valid tenant database |
| post-promotion live database | `sqlite_master` **empty** — no tables at all |

The survivor's replica, sampled directly before the steal:

```
%{epoch: 1, wal_gen: 0, salt1: 2962594194, next_offset: 4152}
4096 bytes  /data/replicas/pfp3.db       <- one page: an EMPTY database
4152 bytes  /data/replicas/pfp3.db-wal   <- 32-byte header + exactly ONE frame
```

And the seed that created it, from the same node's log, at a **different salt and a higher offset**:

```
replication seeded pfp3: 4096B db + 8272B wal at gen 0 offset 8272   (salt1 2379222877)
PROMOTED a local replica ... %{... salt1: 709214653, next_offset: 4152}
```

The offset went BACKWARDS and the WAL identity changed, while `wal_gen` stayed `0`.

## Mechanism

1. A follower is seeded with the primary's `.db` **and its current `-wal`** (`FollowerLog.seeded/4`
   — offsets continue from the seed, which is correct).
2. The primary checkpoints. **Every durability flush does**, so this happens every
   `:shard_flush_interval_ms` (default 5 s). Checkpointed pages move into the primary's `.db`, and
   SQLite restarts the WAL **with fresh salts**.
3. The next frame the primary ships carries the new `salt1` at offset 0. `FollowerLog.decide/2`'s
   salt clause routes to `decide_fresh` → `{:reset_then_append, …}` → `Follower.apply_write(…,
   :truncate)`, which **replaces the WAL file and nothing else.**
4. The follower's `.db` is still the **pre-checkpoint** one. Every page the checkpoint moved into
   the primary's `.db` is now in NEITHER of the follower's two files. The replica is torn.
5. Its state still reports a healthy-looking `next_offset` in the new generation, and
   `Promote.fresher?/2` orders on `{epoch, wal_gen, offset}` and **never consults `salt1`**. So the
   torn replica reads as strictly ahead of the stored object and wins.

In this reproduction the seed landed while the schema was still WAL-resident, so the checkpoint
moved the schema itself — and the promoted result had no tables at all.

## Why the existing code looks right

The salt clause is not an oversight; it was added deliberately and its comment explains why. Before
it, a salt change deadlocked replication permanently (`{:no_quorum, :impossible}` at a fixed offset,
forever). That fix was correct **for keeping replication moving** and is the reason `decide/2`
handles the salt at all.

What it did not do is notice that the same event invalidates the follower's `.db`. Truncate-and-
append makes the WAL coherent with the new generation while silently leaving the main file a
generation behind. Replication progresses; the replica stops being a copy of anything.

`wal_gen` exists precisely to mark this seam — `Storage.position`'s typedoc says "`wal_gen`
increases on every checkpoint" — but `ckpt_seq` restarts when SQLite recreates the WAL file, which
is why the salt clause had to be added underneath it. So the one field that would have made
`fresher?` reject this stays equal across the seam.

## Blast radius

Any shard whose follower was seeded before a checkpoint — i.e. effectively every shard older than
one flush interval. Gated off by default, and promotion snapshots the object first so an operator
can recover, but the tenant is served a torn or empty database until someone notices.

This is also why `rpo` and the promote-on-open `soak` both passed: they promote replicas that
happened to be seeded after the last checkpoint. `pause-fence` has a deliberate quiet period
(`sleep 7` to make the schema durable) which places a checkpoint squarely between the seed and the
promotion, and that is what exposes it.

## Fix directions (NOT yet implemented)

Two, and they are not alternatives so much as a floor and the real repair:

**A. Refuse to promote a torn replica.** Mark the replica not-promotable when `decide_fresh` fires,
clear it on a seed, and have `fresher?`/the promote path decline it. Small, obviously correct, and
strictly better than today: it can only decline promotions, never serve worse bytes, and degrades to
opening from the stored object which is always correct. Costs the RPO win until a reseed.

**B. Re-seed on a WAL reset.** The follower's `.db` is stale the moment the salt changes, so the
follower should request a full seed (`.db` + WAL) rather than truncating its WAL. Restores a
coherent replica and keeps the RPO win. Needs protocol/flow work: the follower has to ask, and the
primary has to serve it without stalling the commit path.

A is the safety property and should land regardless. B is what makes A2's guarantee hold across an
ordinary flush.

## Reproduction

```
./chaos.sh build
REPLICATION_ENABLED=true REPLICATION_PROMOTE_ON_OPEN=true ./chaos.sh up
REPLICATION_ENABLED=true REPLICATION_PROMOTE_ON_OPEN=true ./chaos.sh pause-fence pfp1
```

Control (must pass): the same two commands with `REPLICATION_PROMOTE_ON_OPEN` unset.

Note `cmd_pause_fence` discards the survivor write's response body (`>/dev/null`), so the run
reports only "FAIL: survivor could not steal" and the `no such table` cause is invisible. Worth
capturing that body on failure.
