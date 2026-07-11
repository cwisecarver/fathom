# Fathom — durability & the loss window (how much can I lose, and when?)

> Status: **BUILT.** The answer to "if a node dies, what data survives?" — pulled into one place
> (it's otherwise scattered across [data-path](data-path.md), [single-writer](single-writer.md), and
> the competitive review). The short version: a *process* crash loses nothing; a *node* loss loses at
> most the writes since the last flush (the RPO window), and that window is a knob.

## Two layers, two failure modes

Durability in fathom is two independent layers, each covering a different failure:

| Layer | Mechanism | Survives | Loses |
|---|---|---|---|
| **Local** | WAL + `PRAGMA synchronous=FULL` (fsync the WAL every commit) | a **process / OS crash** on the same node (local disk intact) | nothing — the WAL is durable |
| **Cross-node** | the periodic **flush** of the file to `Fathom.Shard.Storage` (S3/R2/…) | **node / disk loss** | committed-but-**unflushed** writes since the last flush = the **RPO window** |

`synchronous=FULL` is the default (measured ~free for fathom's sharded, wire-bound model —
`docs/reviews/competitive-oltp-2026-07-10.md`), so on a plain restart the coordinator re-adopts the
**present local file** (authoritative, "warm + dirty") and nothing is lost. The RPO window is
strictly about **losing the node's disk**, where only what reached the store survives.

## The flush — write-gated, two triggers

A shard is flushed only if it's **dirty**: `Fathom.Shard.WriteCounter` (bumped lock-free per write
by `ShardExecutor`) shows a write since the last flush. A clean shard **skips the upload**, so
durability PUTs track *writes*, not open-shard count. Two things trigger a flush:

1. **Idle flush.** Zero connections checked out for the idle window ⇒ checkpoint the WAL, flush the
   **whole file**, drop the local copy, stop. Safe to copy whole because there are no writers.
2. **Periodic durability flush.** A busy shard that *never* idles would otherwise accumulate an
   **unbounded** loss window, so every `:shard_flush_interval_ms` the coordinator takes a
   **consistent snapshot** of the live database (via SQLite's online backup — writers stay active)
   and uploads it, **without** dropping the local copy or releasing the lease. This is what **bounds
   the RPO** for hot shards. Set the interval to `0` for idle-only. (It's a **full-file** snapshot
   each interval; incremental WAL streaming — A2 — is future work, and is what would push the RPO
   toward zero for a continuously-hot shard.)

So the RPO of a hot shard ≈ `:shard_flush_interval_ms`; smaller means a tighter window and more
PUTs — the tuning dial.

## Durability never buys split-brain

Every flush **re-checks the lease first** and **self-fences** on a lost lease — a shard that was
remapped or stolen (see [single-writer](single-writer.md)) stops *without flushing*, so it can never
clobber the new owner's newer data. The flush is also etag/epoch-conditional at the store (a `412`
⇒ superseded ⇒ self-fence). Durability and single-writer-safety are the same machinery: the flush
that makes data durable is the same one the fence gates.

## What a failover actually loses

On failover the survivor cold-opens the shard by **pulling the last flushed object** — so it sees
the data as of the source's last flush, and the source's post-flush unflushed writes are gone
(within RPO). If the dead node was only *frozen* and revives, its attempt to flush finds the object
superseded and it self-fences (drops its stale writes). This is exactly what `chaos.sh pause-fence`
demonstrates: the survivor's post-steal write survives, the zombie's unflushed write is never
resurrected. Warm standby ([warm-standby](warm-standby.md)) shrinks the *time* of that failover, not
the loss window — the loss window is set by the flush interval.

## The dials

- **`synchronous`** — `FULL` (default): per-commit fsync, local durability to a process crash.
  `NORMAL` trades that for fewer fsyncs (was the old default; FULL is ~free here, so there's little
  reason to weaken it).
- **`:shard_flush_interval_ms`** — the RPO knob for hot shards. Smaller = tighter loss window, more
  PUTs. `0` = idle-only (unbounded window for a never-idle shard).
- **write-gating is automatic** — clean shards never flush, so tightening the interval costs PUTs
  only on shards actually being written.

## One-line summary

A process crash loses nothing (WAL + `synchronous=FULL`); a node-disk loss loses only the writes
since the last flush — and that window is bounded by a **periodic, lease-fenced, write-gated
snapshot flush** (`:shard_flush_interval_ms`) for busy shards plus an idle flush-and-drop for quiet
ones, with the flushed S3 object as the cross-node durable record the next owner pulls.
