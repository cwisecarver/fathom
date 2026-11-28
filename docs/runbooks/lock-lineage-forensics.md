# Lock and lineage forensics

What to do when a shard opens at a lineage a live (or acked-writing) owner already holds, or a
shard's lock vanishes or regresses under a live owner. This is the Jepsen 2026-11-14 `hard-1` class:
Elle `G2-item-realtime` in one shard, acked appends lost, and a node log line
`not promoting — the replica's lineage N is not below this owner's N`.

**Status: the cause of hard-1 is unattributed.** It was seen once, in the 2026-11-14 hard-mix batch;
about 46 traced hard-mix runs since the tracing went in did not reproduce it. The original failing run's
store (`jepsen/fathom/store/fathom-mixed-hard-power/2026-11-14T23:37:16.369487-05:00`) is the only record
of it: keep it when pruning old stores. This page is how a recurrence explains itself. It is not a
diagnosis.

## What is in place

| Piece | What it gives you |
|---|---|
| **Fail-closed refusal** (`PromoteOnOpen.promote_replica/7`) | A replica fresher than the object whose lineage is not below the owner's lineage now REFUSES the open (retryable 503) and keeps the replica's lineage as a claim, so the retry opens above it. It no longer serves the older object and loses the acked tail. A recurrence therefore shows up as a log line, not as lost writes. **It has never fired on the rig** (no collision was reproduced), so it is covered by unit tests only (`promote_on_open_test.exs`, both liveness modes), not by a rig run. |
| **Always-on collision warning** | Same place: `lineage collision context: lease=%{owner, epoch, claim, took_over, recreated, prev_owner, released_by, touch_pending} lineage=… replica=… object_stamp=…`. No flag needed. |
| **Stale-release identity** (`S3.own_lock?/2`) | A 412'd release resolves to "ours" only for the same owner, epoch AND claim. Closes the older-lease-clobbers-newer-lock shape on the real conditional contract. |
| **`SHARD_ACQUIRE_TRACE`** | `true`: `lock-trace` lines for every acquire branch, release, marker, rollback and the lineage each open chose (`open_lineage`), plus `release_decision` (what a release is about to do, with the object's stamp). `debug`: adds `lock-debug` lines: coordinator timeline (open begin/summary, flush ok, drop, terminate), acquire outcomes, liveness-check inputs, and the store's answer to every lock write and delete. Off by default; the chaos rig sets `debug`. |

## Turning it on

`SHARD_ACQUIRE_TRACE=true|debug` (see `docs/configuration.md`). One line per acquire or release at `true`,
several per open at `debug`. Fine for a chaos rig or a canary; not for a 1024-tenant fleet.

## Tools (`jepsen/fathom/forensics/`)

| Tool | Use |
|---|---|
| `lineage_scan.py <store dir>` | Scans a Jepsen store's `node-logs/` for the invariants below and prints `opens=N flags=N unverified_deletes=N`. |
| `shard_timeline.py <store> <shard> [--trace lock-trace-N.log] [--versions lock-versions-N.jsonl]` | One shard, every source merged in UTC order: node logs, the MinIO `mc admin trace` (port 2100N = fathomN) and the versioned lock history. Unchanged lock polls are collapsed. Rows within a few ms are a tie, not a sequence. |
| `dump_lock_versions.sh <out.jsonl>` | With the rig up and the bucket versioned: every lock version with its body, time, and delete markers. |
| `lineage-hunt.sh` | The campaign harness: versions the bucket, records `mc admin trace` of `*.lock`, scans each run, and dumps the lock history and the store on any append failure, flag, "not promoting" line or unverified delete. `SOAK=1 HARD_RUNS=12` for a soak. |

## What the scanner flags

- `LOCK-VANISHED`: a fresh create for a shard with no traced release before it.
- `LINEAGE-NOT-ABOVE-PRIOR-OPEN`: an open whose lineage is at or below an earlier SERVED open's.
- `CLAIM-DELETED-BEFORE-STAMP`: a lock with claim `c` deleted while the object's stamp was below `c`.
- `unverified_deletes`: such a delete whose stamp could not be read and which is not proven to have missed.

Known benign cases the scanner already skips: an open refused or failed after taking the lease (nothing
served or shipped), a CLEAN unstamped release (that coordinator wrote nothing, so its lineage was never an
ownership), and a delete that failed with a pre-send error (`nxdomain`, `econnrefused`) or whose lock
demonstrably survived. A logged timeout, exit or 5xx is NOT proof the delete missed: it stays unverified.

## When it recurs

1. Keep the run's store and the MinIO lock history before tearing anything down
   (`dump_lock_versions.sh`, `cp -r` the store). `lineage-hunt.sh` does this automatically.
2. Find the shard: the `lineage collision context` line, or the scanner flag.
3. `shard_timeline.py <store> <shard> --trace … --versions …`. Read, in order: who wrote the lock last
   before the collision (`LOCKVER`, with the requesting node from the MinIO rows), what each opener's
   `acquire_result` and `owner_live` said, the `release_decision` of every release (`lineage_stamped`
   against the object stamp), and `open_summary` for the etags behind `lineage_stamped`.
4. Decide which of the known shapes it is: a lock deleted while the object stamp was below its claim;
   a rollback or restored lock body with a lower claim; a claimless marker; or a same-owner older lease.
   Then write the regression test at that site, in both liveness modes.

## Limits

- **The chaos rig cannot exercise the plain-release half of the stale-release fix.** MinIO ignores
  `If-Match` on DELETE (the rig runs with `allow_weak_conditional_delete`), so a stale plain release
  deletes the lock regardless. Real S3 answers 412. See `docs/jepsen-run-plan.md` item 13.
  Since audit 2026-11-22 #7 the release READS the lock first under that flag, so a superseded
  coordinator's clean idle drop no longer deletes the live owner's lock on the rig.
  `PAUSE_FENCE_CLEAN=1 ./chaos.sh pause-fence <shard>` is the arm that exercises it (no dirty write, so
  the unpause-time idle stop takes the CLEAN drop); its verdict is zero bare `release_delete` lines from
  the zombie after the steal. Measured 2026-11-27: the pre-fix build issued one (a stale-etag DELETE 0.4 s
  after the survivor stole, which MinIO honours), the fixed build none.
- With replication off there are no claims, so owner and epoch cannot tell a same-owner reclaim from its
  predecessor.
- The tools read `logs/ds/…` paths by default for run outputs; `logs/` is not committed.
