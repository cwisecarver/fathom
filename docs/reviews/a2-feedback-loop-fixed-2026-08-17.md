# A2's feedback loop is closed; the ceiling is now throughput, not memory — 2026-08-17

The fix for `docs/reviews/a2-shipper-feedback-loop-2026-08-16.md`, and what the rig says about it.

**The memory failure is gone and the mechanism is proven closed. The tenant ceiling did not move.**
1024 still does not complete — but for an entirely different reason, with a different signature, and
with every node alive. Keeping those two claims apart is the whole content of this report: three
previous attempts failed partly by treating "the OOM stopped" and "1024 works" as the same result.

## What changed (`ca5f377`)

1. **`Primary.plan/3` caps the WAL one push may carry** — 1 MiB, `REPLICATION_MAX_PUSH_BYTES`. This
   breaks the feedback: a push carries the delta since the follower's last ACK, so a capped delta
   cannot grow however far behind that follower falls. No wire change was needed —
   `FollowerLog.decide/2` already accepts a contiguous append of any length, `decide_fresh/2` a
   capped reset at offset 0, and `advance/2` already records `offset + len`.
2. **`Session.ship/5` ships in bounded ROUNDS** until the followers are current, so the cap costs
   latency under lag and never durability — the commit still acks only once the quorum holds every
   byte through the commit point.
3. **`Replication.Budget` bounds queued bytes PER NODE** — 1 GiB, `REPLICATION_MAX_QUEUE_BYTES` —
   claimed in the committing process *before* the payload reaches any mailbox. `:replication_max_queue`
   is consulted on DEQUEUE, which a burst outruns.

## The memory result: closed, measured on the axis that was wrong before

Same rig, same command, 5 nodes, replication on. Left column is the failing run of 2026-08-14/16.

| | before | after |
|---|---|---|
| node binary held | 7–18 GiB, peak **45 GB** | **32–922 MB**, and FALLING between samples |
| mean payload, two samples ~40 s apart | 832 KB → **1,593 KB** (climbing) | 732 KB → **790 KB** (flat) |
| largest payload | 1,891 KB → **4,775 KB** | **1024 KB, every sample** |
| node lifetime at 1024 | OOMKilled at ~2 min, then ~6.5 min | **all 5 alive at 2 h** |

**`largest: [1024, 1024, 1024, 1024, 1024] KB` is the finding.** Every one of the five largest
binaries a shipper held was exactly the 1 MiB cap, to the byte — the difference between a bounded
quantity and a runaway one, stated in the units that actually ran away.

Second-most important is the DIRECTION between samples: **216 MB → 105 MB** on one shipper, against
6,893 MB → 15,798 MB before. Memory falls while the mean payload stays flat. Closed loop.

One probe caught a shipper holding 892 MB with **`msgq 0`** — no queued messages at all. That is the
wrong-reading-#2 state the previous report warns about: dequeued binaries not yet collected. A later
probe read 35 MB. Read payload sizes, not queue depth, and never conclude from one sample after a
burst.

## The A/B at 512: the fix costs nothing

Both arms same rig, restarted clean between, `REPLICATION_MAX_PUSH_BYTES=0` disables the cap (the
node was probed to confirm `max_push_bytes = 0` before the run, rather than assumed).

| 512 tenants | cap ON | cap OFF |
|---|---|---|
| txn/s | **2,590.6** | 2,568.5 |
| errors | **9,480** | 10,860 |
| p50 / p95 | 108 / 305 ms | 104 / 293 ms |
| p99 | 882 ms | 736 ms |

Throughput is within noise (+0.9%) and errors are **12.7% LOWER** with the cap. Only p99 is worse,
which is the catch-up loop's tail and is the expected shape. So the ~5% error rate at 512 is
**inherent to 512 on this VM, not a cost of the fix**.

What this A/B does NOT show: peak memory for the cap-off arm. Both arms were probed only after they
drained (34–63 MB either way), so this run cannot say whether 512 would have OOM'd uncapped. The
memory claim rests on the 1024 in-flight sampling above, not on this table.

## Where the boundary actually is

| tenants | before the fix | after |
|---|---|---|
| 256 | 3,689 txn/s, 0 err | **3,418 txn/s, 0 err** — clean, budget never fires |
| 512 | never measured | **2,591 txn/s, 9,480 err (4.6%)** — completes, degraded |
| 1024 | **OOMKilled, no result** | alive 2 h, ~1% of healthy rate, **no result** |

256 is −7.4% against its baseline, inside the 20% noise band and not treated as a regression.

**The clean boundary is still 256.** Replication on remains a ≤256-tenant configuration on this rig.
What changed is the failure mode past it: degradation instead of death.

## 1024: what it looks like now

Killed at ~90 minutes having produced no throughput number.

* **797 of 1024 shards** held fleet-wide; **fathom5 held 0** — its ~227 tenants were stranded, so the
  driver waited forever on clients that would never finish.
* Two samples 76 s apart totalled **+2,150 queries fleet-wide, ~28 q/s** — roughly **1% of the
  healthy rate**, implying ~14 hours to finish.
* fathom5's log ended in a wall of `replication rejected: :overloaded` → `quorum IMPOSSIBLE`, plus
  `shipper connection lost: :timeout`. The `send_timeout` firing means the socket genuinely could
  not drain: the link was saturated and the budget was reporting that, not causing it.
* Probed an hour later, fathom5 was completely idle — **0 MB queued, all four shippers connected,
  15 MB binary, 0 coordinators.** Every tenant that hashed to it had already given up.

At 1024 on this one 12-vCPU VM, replication cannot keep up, and the fix converts that from an OOM
into refused writes plus a latency collapse. That is the trade the change intended, and it is
strictly better than dying — but it is not "1024 works".

## Two leads for the next round, neither acted on here

**1. The budget is all-or-nothing, and at 1024 that cliffs.** `Budget.reserve/2` refuses EVERY push
once the node total is over cap, including small ones for perfectly healthy shards. On fathom5 that
turned a saturated link into a node-wide write outage and then a node with zero tenants. A policy
that sheds the laggiest or largest — or refuses per shard rather than per node — would degrade
instead of cliff. Scope it correctly though: `:overloaded` was **1,673 and 0** on two nodes at 512
and did not dominate anywhere below 1024. This is a 1024-specific cliff, not a general flaw.

**2. `:already_in_flight` is the dominant reject at 512, and it is PRE-EXISTING.** Counts over a
whole run:

| | cap ON | cap OFF |
|---|---|---|
| `:already_in_flight` | 8,781 / 18,226 | 21,442 / 9,175 |
| `:stale_wal_gen` | 14,149 / 5,273 | 0 / 7,323 |
| `:overloaded` | 1,673 / 0 | 0 / 1,003 |

It dominates in BOTH arms, so the catch-up loop did not introduce it — worth stating plainly,
because the loop is the obvious suspect and the numbers clear it. A shipper holds one waiter per
shard and `ship_quorum/4` returns at the Q-th ack, so the follower outside the quorum still has a
push outstanding when the next commit ships. That is where a real availability win probably lives,
and it is unmeasured.

`:stale_wal_gen` is the same story from the other side: a durability flush checkpoints the WAL every
`:shard_flush_interval_ms`, so a straggler's in-flight push can land after the generation moved.

## Reproduce

```bash
cd deploy/chaos
./chaos.sh down && ./chaos.sh build          # `up` does NOT build
REPLICATION_ENABLED=true ./chaos.sh up
TPC_NET=container TPC_DRIVER=elixir REPLICATION_ENABLED=true ./chaos.sh tpc-fleet 256
./bin_holders.sh <node> 3 refs               # name the holder
./bin_sizes.sh <node> <holder>               # twice, ~40 s apart
```

Watch the **MEAN and the LARGEST payload**, not the queue depth — queue depth was flat across a
doubling of memory last time and would have reported success. `largest` at exactly the cap is the
pass condition; above it means some path is not applying the cap.

`REPLICATION_MAX_PUSH_BYTES=0` restores the unbounded delta, which is how the A/B above was run and
how the old OOM is reproduced on purpose.
