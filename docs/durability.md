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

So the RPO of a hot shard is `:shard_flush_interval_ms` *plus the flush's own duration* — the
watermark that clears "dirty" is captured when the flush task **starts**, and the interval timer
re-arms only after it **settles**, so the honest worst case is `interval + ~2× snapshot/upload time`
(the interval, plus the in-flight snapshot that didn't quite finish, plus the next one). On a fast
store the flush duration is small and RPO ≈ interval; on a slow/large one, size it in.

## Measured — the loss window tracks the interval

`mix fathom.rpo` (the `Fathom.Rpo` harness) quantifies the two numbers above: it drives a steady
write stream and, at points spread across the flush sawtooth, compares the acked writes against what
a survivor would cold-open — the **last flushed object** (a failover pulls exactly it, so the loss is
measured directly, without killing a node). A sweep at **200 writes/s** (dev, one-host-relative — the
window scales with write rate and interval, not with the host):

| `:shard_flush_interval_ms` | lost rows (p50 / p99) | lost window (p50 / p99) |
|---|---|---|
| 1 000 | 97 / 160 | 0.6 s / 1.0 s |
| 5 000 (default) | 452 / 800 | 2.7 s / 4.8 s |
| 30 000 | 2 647 / 4 800 | 15.9 s / 28.9 s |
| 0 (idle-only) | 2 600 / 4 800 | 16 s / 29 s — run-bounded, **unbounded** for a never-idle shard |

Two things the table pins. The **p99 lost window ≈ the interval** (965 ms / 4.8 s / 28.9 s — a node
that dies just before a flush loses the full interval; the p50 is ~half, mid-sawtooth), confirming the
`RPO ≈ interval` formula empirically. And **lost rows ≈ rate × interval**, so the window is a knob you
set by the interval: tighten `:shard_flush_interval_ms` and the loss shrinks linearly (5 s → ~0.5 k
rows, 1 s → ~0.15 k). **Idle-only (`0`)** loses the whole busy session — only the run length bounded it
here. The **process-crash row is 0** (`synchronous=FULL`: wrote 200, survived 200, with nothing flushed
first — survival came purely from the local WAL), the local-durability layer proven end to end. The
boundary is pinned as deterministic invariants in `test/fathom/rpo_test.exs`; the multi-node /
real-disk complement is `deploy/chaos/chaos.sh soak`.

### Cost — the flush rate the interval buys

The loss window above is the *benefit* of a tighter interval; the *cost* is the flush **rate**.
`mix fathom.rpo --cost` drives a continuously-write-active shard for a window at each interval and
reports the flushes (each a **VACUUM INTO** snapshot + **full-object PUT**) and their per-flush
duration. At **100 writes/s** over a 60 s window (dev, Local storage — the VACUUM + local-copy cost;
real-S3 PUT cost is the `FATHOM_S3_TEST_*` / chaos-rig complement):

| `:shard_flush_interval_ms` | flushes / 60 s | flush µs (p50 / p99) | $ / M writes¹ |
|---|---|---|---|
| 5 000 (default) | 12 | 6.0 ms / 7.7 ms | $0.010 |
| 30 000 | 2 | 5.1 ms / 5.1 ms | $0.0017 |

¹ **Derived, not measured.** The flush *counts* are measured; the dollars are list-price arithmetic
at S3-standard **PUT ~$5.00 / M**, the same convention [deploy-cluster](deploy-cluster.md) uses to
price the other request paths. Byte transfer is free on ingress and a flush overwrites **one** key,
so a tighter interval buys RPO with **request count only** — not storage, not bandwidth. R2/Tigris
differ; re-derive against your store's price sheet.

**Cost per write is set by writes-per-flush, not by write count.** A flush uploads the whole object
no matter how many writes accumulated behind it, so the per-write cost is purely amortization:

    $ per million writes  =  5 / (write_rate_per_s × interval_s)

That reproduces both rows above (100 w/s × 5 s ⇒ $5/500 ⇒ $0.010) and shows where the money
actually is — the **ceiling at one write per interval**, where every write pays a full PUT:
**$5.00 / M writes**, 500× the 100 w/s cell. Per *shard*, a continuously-write-active shard costs
`2.592e6 / interval_s` PUTs per month — **~$2.59/shard/month at 5 s, ~$12.96 at 1 s**. Both are
ceilings that require a write in *every* window, 24/7; a bursty tenant pays for the dirty windows
it actually touches, which is typically orders of magnitude fewer.

**What that does to the cost story vs a provisioned database.** Fathom's saving over
one-Postgres-per-tenant (or a shared instance sized for peak) comes from the **idle tail** — a quiet
tenant costs object storage and nothing else, because the flush is write-gated and a clean shard
never PUTs. The interval does not touch that; it prices only the write-active fraction. Where it
*does* bite is the continuously-hot shard: at 1 s the ~$12.96/month PUT bill lands in the same range
as a small provisioned instance, so **sub-second RPO is the point where per-shard request cost stops
being free** and wants pricing against that tenant's plan. At the 5 s default it is ~$2.59/month at
the ceiling and effectively nothing for bursty tenants.

The flush **rate scales ~1/interval** (12 vs 2 — ~6× for 5 s vs 30 s) at **~constant per-flush cost**
(~5–8 ms on Local, dominated by the VACUUM INTO snapshot, which is also the long-lived reader that
holds back the passive WAL checkpoint). So the default 5 s buys its ~6× smaller loss window (2.7 s vs
15.9 s p50, above) at ~6× the VACUUM/PUT rate — the tradeoff review #20 flagged, now measured on the
benefit **and** cost sides. The 6× PUT rate shares the S3 Finch pool with cold-opens, so before
tightening below 5 s price the real-S3 PUT cost + that contention with `scripts/benchmark_s3_latency.sh`.

**This bound holds only while flushes succeed.** A *persistent* flush failure — an S3 auth change, a
bucket-policy change, a credential expiry — makes the RPO **unbounded**: the shard stays dirty and
retries every interval, the oldest-unflushed-age climbs, and (before review #27) the only trace was
a per-interval `Logger.warning`. The coordinator now counts consecutive failures, emits
`[:fathom, :shard, :flush, :failed]` telemetry, and escalates to `Logger.error` past
`:flush_failure_alert_threshold` (default 3). **Alert on it** — the `FathomFlushFailing` and
`FathomUnflushedAgeHigh` rules in [`deploy/observability/alert-rules.yml`](../deploy/observability/alert-rules.yml)
turn a silent RPO blow-out into a page. Smaller `:shard_flush_interval_ms` means a tighter window
and more PUTs — the tuning dial (it's currently a fleet-global knob; a per-shard override was scoped
out as a tenant-tiering feature).

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

## Snapshots & point-in-time restore (logical-corruption recovery)

The flush window above is the RPO for **node/disk loss**. It does nothing for **logical
corruption** — a bad deploy, a mass `UPDATE`, an accidental `DELETE` — which propagates to the one
durable object within a flush interval and overwrites the last good state. That's the far more
common incident, and Postgres-on-RDS gives it to a Django shop for free (PITR). Fathom's answer
leans on the shape of the data: a tenant **is** one SQLite object, so a snapshot is a server-side
copy of that object under `<shard>@snap-<id>`, and a restore is the copy back — no WAL-replay engine
or copy-on-write page server (expert review 2026-07-14 #12).

- **Create / list / drop:** `mix fathom.snapshot create|list|drop <shard> [id|label]`, or
  `Fathom.Snapshots.create/2` / `list/1` / `drop/2` from a node console. A snapshot captures the
  shard's **last durably-flushed state** (every flush is a complete, checkpointed DB file, so a
  snapshot is always a consistent database); to snapshot the very latest writes, drain the shard or
  let it idle-flush first.
- **Restore:** `Fathom.Snapshots.restore/3` (or `mix fathom.snapshot restore`). It drains any local
  coordinator and then **refuses unless no live node owns the shard** (`Storage.lease_holder/1`), so
  the copy-back can't be clobbered by a writer elsewhere — safe regardless of where it's run.
  Restore is destructive to the current live object; take a fresh snapshot first if you might want
  to undo the undo.
- The same copy-one-object primitive is the kernel of **database forking** (copy to a *new* shard
  id) and preview/staging environments — the differentiator Turso/Neon/PlanetScale market.

Snapshots are backend-uniform (Local + S3), so the whole path is testable without a real object
store (`test/fathom/snapshots_test.exs`).

## Harden the bucket (defense-in-depth beneath snapshots)

One object per shard in one bucket is the durability floor, and S3's 11-nines protects against
*media* failure, not against a bucket deletion, a lifecycle misconfig, a compromised credential with
`s3:DeleteObject`, or a fat-fingered `aws s3 rm --recursive` (expert review 2026-07-14 #13). Harden
the bucket underneath the fathom-managed snapshots above:

- **Versioning on** + a lifecycle policy to expire noncurrent versions — every fenced flush then
  leaves an automatic version trail at zero code cost, a second recovery path beneath the explicit
  snapshots.
- **Object Lock (or MFA-delete)** on the live prefix, and **cross-region/-account replication**, so
  a single bucket-level mistake or compromise can't erase every tenant at once.
- **Least-privilege node credentials:** the data plane needs read/write on `<shard>.db` and the
  `.lock`/`@version`/`@snap-` keys but should not hold broad `DeleteObject` on the live prefix —
  scope deletes to the version/snapshot keys the retirement/retention paths actually remove.

## The dials

- **`synchronous`** — `FULL` (default): per-commit fsync, local durability to a process crash.
  `NORMAL` trades that for fewer fsyncs (was the old default; FULL is ~free here, so there's little
  reason to weaken it).
- **`:shard_flush_interval_ms`** — the RPO knob for hot shards (default **5 s** ⇒ ~5 s node-loss
  window for busy shards). Smaller = tighter loss window, more PUTs. `0` = idle-only (unbounded
  window for a never-idle shard). **Keep the alert thresholds coupled:** `FathomUnflushedAgeHigh`
  (~6×) and `FathomManyDirtyShards` (~12×) in [`alert-rules.yml`](../deploy/observability/alert-rules.yml)
  are sized to this default — change one, retune the others, or a real S3 outage blows the intended
  RPO by that multiple before paging.
- **write-gating is automatic** — clean shards never flush, so tightening the interval costs PUTs
  only on shards actually being written.

## One-line summary

A process crash loses nothing (WAL + `synchronous=FULL`); a node-disk loss loses only the writes
since the last flush — and that window is bounded by a **periodic, lease-fenced, write-gated
snapshot flush** (`:shard_flush_interval_ms`) for busy shards plus an idle flush-and-drop for quiet
ones, with the flushed S3 object as the cross-node durable record the next owner pulls.
