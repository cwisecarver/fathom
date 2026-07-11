# Fathom — cross-node single-writer (storage, lease, fence, heartbeat)

> Status: **BUILT.** How fathom guarantees that **at most one node ever writes a shard's file**,
> across failover, remap, and network partition — coordinating only through the object store, with
> **no BEAM cluster**. This is the mechanism reference; `docs/cluster-architecture.md` is the broader
> cluster picture, and the crash/loss contract is exercised on the rig (`chaos.sh failover` /
> `pause-fence` / `partition`).

## The problem it solves

Every shard is a single-writer SQLite file. The LB partition normally routes each shard to exactly
one home node — but during a **failover** (home died), a **remap** (rebalance moved it), or a
**partition** (a node is frozen/cut off but not dead), two nodes can each believe they own a shard.
If both write and flush, you get split-brain: one node's flush clobbers the other's, silently losing
committed data. So fathom needs a **distributed single-writer guarantee**, and it has to get it
**without nodes talking to each other** — the only shared surface is the object store (S3 / R2 /
Tigris / MinIO, or the Local filesystem backend in dev/test) and Postgres (orchestration).

## The storage layer

`Fathom.Shard.Storage` is a **behaviour** (`config :fathom, :shard_storage`): `Storage.Local` (a
filesystem object store, dev/test default) or `Storage.S3` (Req + `aws_sigv4`, no AWS SDK; works
with S3/R2/Tigris/MinIO). Beyond `pull`/`flush` it carries the coordination primitives below
(`acquire_lease`, `check_lease`, the fenced `flush`, `object_etag`, `pull_if_changed`). One rule sits
underneath everything: **a present local file is authoritative on wake** — a coordinator pulls only
on a genuine cold start, so a node that still has un-flushed writes never clobbers them by re-pulling.

## The three primitives

**1. The lease — `{owner, epoch, expires_at_ms}`, one object per shard.** `owner` is the node that
holds it; **`epoch` is a monotonic fencing token** bumped on every acquire. It's acquired by an
**optimistic conditional create** (`PUT If-None-Match: *` — succeeds only if no lock exists), *never*
an unconditional overwrite (which would silently steal a live owner). On a 412 conflict the acquirer
reads the current lock and resolves held-vs-steal (below). `check_lease/2` is the read-only "is this
still the live lock (owner + epoch)?" fence.

**2. The fence — the flush is conditional, so a stale owner can't clobber a newer one.** A coordinator
flushes only if the stored object still matches the etag it expects (`If-Match`; for a brand-new
shard, `If-None-Match: *` — write only if nothing exists yet). A **412** comes back as
`{:error, :superseded}`: someone else wrote, so this node **self-fences** — it stops **without
flushing**. The epoch orders writers: a shard remapped away has an old epoch, and its flush is
rejected, not applied. "**Never flush over a newer owner.**"

**3. The heartbeat — liveness that's O(nodes), not O(shards).** The naive design renews every
shard's lease on a timer — millions of PUTs (the "F1 storm"). Instead, each **node** renews **one**
`heartbeat/<node>` object every `ttl/3` (`Fathom.Shard.Heartbeat`), and a shard's owner is **live iff
its node's heartbeat is fresh**. So the lease's own `expires_at_ms` isn't the liveness signal — the
owner's heartbeat is. A steal is allowed only if that heartbeat is **missing or expired past
`steal_margin_ms`** (a clock-skew guard). Liveness cost is O(nodes), and it's read lock-free from an
ETS status row `{generation, deadline, margin}`.

## Cold-open — acquire and pull, overlapped

On a cold start `Fathom.Shard.init` **overlaps** the lease acquire with the data pull (independent
objects — the `.lock` and the `.db`), so cold-open is ~1 S3 round-trip, not two serial ones. The
pull lands in a **temp file promoted to the real path only once the lease confirms** — a lost lease
race never leaves a stale local copy, and the shard is only *served* after the lease is held. On an
acquire conflict, the acquirer consults the current owner's **heartbeat**: fresh ⇒ `{:error,
{:held, owner}}` (back off, the owner is alive); missing/expired-past-margin ⇒ **steal** with a
bumped epoch. The coordinator records the heartbeat **generation** at acquire time, which gates its
flush fence.

## Steady-state — the fence with no per-shard I/O

Once it owns a shard, a coordinator does **no per-shard lease renewal** (that's the F1 storm it
avoids). Before a flush it calls `Heartbeat.valid_for_write?(acquire_gen)`:

- **`:ok`** — the node's heartbeat is valid *with margin* **and** hasn't lapsed since this shard was
  acquired ⇒ the heartbeat vouches for liveness, so **write with no per-shard I/O**.
- **`:revalidate`** — the heartbeat is valid now but **lapsed** at some point since acquire (a GC
  pause or partition let it expire, during which another node *could* have stolen the shard) ⇒
  re-check the lock via `Storage.check_lease/2` and **self-fence** if it's been superseded, so a
  remapped shard never double-writes.
- If the **heartbeat process is down** entirely, the coordinator degrades to the **legacy per-shard
  renew fence** (renew the lease on a timer, self-fence on `:superseded`).

The idle flush also re-checks the lease first and self-fences on a lost lease — so a lease lost
between the last check and idle never produces a clobbering write.

## Two directions of the same lease

- **Involuntary steal (crash path).** A survivor cold-opens a dead node's shard, finds its heartbeat
  stale-past-margin, and steals the lease with a bumped epoch. If the dead node was only *frozen*
  (a zombie) and revives, its next flush finds the object superseded (old epoch / etag mismatch) and
  it **self-fences — drops without flushing**. Its un-flushed writes are lost (within RPO); the
  survivor's write stands. This is what `chaos.sh pause-fence` proves (seq=400 survives, the zombie's
  seq=300 is never resurrected).
- **Voluntary drain (rebalance path).** The source node *releases* the lease so a target can acquire
  it — the same primitive in its cooperative direction (see `docs/rebalancing.md`). Because the lease
  blocks double-writes regardless, a healthy node can't be *stolen* from; a move is always a
  voluntary drain.

## Durability / loss contract

Durability is WAL crash-consistency + a **periodic, write-gated flush** to the store (a clean shard
skips the upload, so PUTs track writes, not open-shard count). A crash loses **committed-but-
unflushed** writes — bounded by the flush interval, which *is* the RPO window. `synchronous=FULL`
(now the default) makes each commit locally durable to a node crash; the flushed object is the
cross-node durable record, and the lease guarantees only one writer ever produces it.

## Safety invariant + proof

**At most one node writes a shard's file at any time.** It's enforced at four points: the conditional
lease *acquire*, the etag/epoch-conditional *flush* (a superseded write is rejected, not applied), the
*heartbeat* deciding held-vs-steal past a clock-skew margin, and *self-fencing* on a lapse. The
in-process cluster suite (`test/fathom/cluster/`) pins the safety contract; the rig
(`chaos.sh failover` / `pause-fence` / `partition`) measures the failover window and demonstrates the
zombie self-fence live.

## One-line summary

Each shard has an S3 lease `{owner, epoch}` acquired by conditional-create and fenced on flush by
etag (a superseded write self-fences instead of clobbering), while one per-node heartbeat provides
O(nodes) liveness that decides steal-vs-held — so exactly one node ever writes a shard's file, across
crash, remap, and partition, with no BEAM cluster and coordination only through the object store.
