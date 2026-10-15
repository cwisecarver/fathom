# Runbook — a shard's lock object is undecodable

Closes expert review 2026-10-15 #23 (decision D14: no automatic repair on either backend).

## Symptom

One tenant cannot be opened. Checkouts fail with `{:lease_unavailable, :corrupt_lock}`; the node log
carries, on every attempt:

    shard <id>: lock ... is undecodable ... the shard stays CLOSED ... run `mix fathom.lock.repair <id>`

and `fathom.storage.lock_corrupt.count` increments (alert on any occurrence). Other tenants are
unaffected. A held shard (`{:shard_held, ...}`) is a different, healthy condition — see
[cluster.md](cluster.md).

## Why it is not repaired automatically

The lock body carries the fencing **epoch** and the A2 **claim** (the lineage a crashed holder may
have shipped under). An undecodable body has lost both. Deleting it and starting fresh would mint a
new lock at **epoch 1** — a larger backward jump than any fault it cures, and with S3 a takeover
with no steal-touch (AGENTS.md, "delete the lock instead of rolling it back"). `Storage.Local` used to
do exactly that; since 2026-10-15 both backends fail closed identically.

## Diagnose first

The usual cause is a full or failing disk (Local) or a partial/foreign write to the bucket (S3).
Fix that before repairing, or the repair will be corrupted too.

- Local: `ls -l <data_dir>/<shard>.lock` (zero length is the classic full-disk signature); `df -h`.
- S3: `aws s3 cp s3://<bucket>/<prefix><shard>.lock -` and read the bytes. A salvageable `"epoch"`
  is picked up automatically.
- Make sure no node is mid-open for the shard: the task refuses if the lock decodes, but it cannot
  tell a healthy holder from a dead one.

## Repair

    mix fathom.lock.repair <shard_id> [--epoch-floor N] [--claim N]

Run it on any node with the storage config of the fleet (the S3 env, or the Local data dir).

It writes an expired marker owned by the sentinel `-released-`, so the next checkout takes the
ordinary **steal** path (epoch + 1, takeover claim above the marker's). Everything it picks only goes
up:

- **epoch** = highest of the epoch salvaged from the bytes and `--epoch-floor`, else the current unix
  time in seconds, **plus 2**. If you know the last epoch from logs or telemetry, pass it as
  `--epoch-floor`.
- **claim** (replication on only) = one above the object's current lineage. If the dead holder
  crashed several times under replication, its claim may be higher than the object shows: pass
  `--claim` above the highest lineage any follower holds (look at the follower logs for the shard).
  The task refuses with `lineage_unreadable` if the object cannot be read.

It **refuses** (exit 1, nothing written) when the lock decodes (`lock_not_corrupt`), does not exist
(`no_lock` — then there is nothing to repair; the next acquire creates it), or, on S3, if anything
rewrote the lock between its read and write (the write is `If-Match` the corrupt body's etag).

## Verify

1. The task prints `repaired lock for <id>: marker epoch=... claim=...`;
   `fathom.storage.lock_repaired.count` increments.
2. Open the tenant (a normal request). The first open after a repair is a takeover: expect the
   steal-touch (S3) and `took_over`/promote-on-open behaviour for a replicated shard.
3. Confirm `fathom.storage.lock_corrupt.count` has stopped rising for the shard.

If the lock corrupts again, the disk or bucket is the problem, not the lock.
