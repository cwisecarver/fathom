# A2's 1024-tenant OOM is a feedback loop — 2026-08-16

The 1024-tenant OOM that has blocked replication since 2026-08-14, explained. It is not a leak, not
a rate mismatch, and not the mailbox depth three fixes were aimed at. **Each queued push carries the
WAL accumulated since the last successful send, so delay makes payloads bigger, and bigger payloads
make more delay.**

Nothing here is fixed. This is the diagnosis, the measurement that produced it, and the three wrong
answers that preceded it — the wrong answers are the point, because each was reached by reasoning
from an instrument that could not see what mattered.

## The measurement

Two samples of the same shipper process, forty seconds apart, during a 1024-tenant `tpc-fleet`
run with `REPLICATION_ENABLED=true`:

| | sample 1 | sample 2 (+40 s) |
|---|---|---|
| queued messages | 8,265 | **8,195** |
| binary held | 6,893 MB | **15,798 MB** |
| mean payload | 832 KB | **1,593 KB** |
| largest payload | 1,891 KB | **4,775 KB** |
| payloads ≥ 1 MB | 1,646 | **8,557** |

**The queue did not grow. The messages did.** Memory more than doubled while the message count fell
slightly. That single pair of rows is the whole finding.

## The mechanism

A `Protocol.Push` carries the WAL delta since that shard's last acked position. The shipper is one
process per peer node, holding one socket, carrying every shard.

1. Sends fall behind — the aggregate write rate across ~205 tenants/node exceeds what one socket
   drains.
2. A shard's next push therefore covers a longer interval, so its payload is **larger**.
3. A larger payload takes longer to write to the socket, so the backlog grows in TIME even when it
   is flat in COUNT.
4. Which makes the next payload larger still.

It is positive feedback, which is why the failure is a cliff rather than a slope: 256 tenants is
stable (3,689 txn/s, 0 errors, indefinitely) and 1024 runs away to 45 GB in minutes. There is no
gentle degradation in between to warn anyone.

## Why three fixes missed, and what each got wrong

**1. `fullsweep_after: 0` on the shipper** (`e0fda94`). Correct on its own terms — it took retained
garbage from 7–18 GiB/node to 83–99 MB, and that reduction is real and kept. But it collects
binaries that are GARBAGE, and the ones in a mailbox are LIVE. It moved the floor, not the ceiling.

**2. `:replication_max_queue`, a message-count cap** (`ca09c1e`). Right target, wrong quantity, and
the arithmetic was never done:

    8,192 messages per shipper × ~1 MB × 4 shippers per node ≈ 32 GB permitted

The cap is per shipper and a node runs one per peer. Worse, the measurement above shows a count cap
cannot work **at any value**: the count was stable at ~8,200 while memory doubled. Bounding a count
bounds nothing when each item grows without limit.

**3. Retuning that cap** (1,024 → 8,192). 1,024 throttled a healthy range — 256 tenants legitimately
queues 4,967, and capping there turned 3,505 txn/s / 0 errors into 1,580 / 5,333. 8,192 permits
32 GB. The constraints do not overlap because the variable being capped is the wrong one.

## The instrument was the actual blocker

Every one of those three was reasoned out from `mem_sampler.sh`, which ranks processes by
`Process.info(pid, :memory)` — **a call that excludes an off-heap refc binary's payload and reports
only the small reference.** On a node holding 43 GB of binary it named a 9 MB process as the
largest. Validated against a planted 1.2 GB decoy: the old instrument reported it as **0 MB**.

That blindness produced a specific, seductive wrong turn. The mailbox was the ONE place the
instrument could see a payload (a queued message counts toward the holding process), so the mailbox
is what got blamed — twice, in opposite directions:

- **"The mailbox is the cause."** Right mechanism, no arithmetic.
- **"The mailbox is NOT the cause."** Reached from a surviving node holding 43,005 MB with a queue
  of **1**, which looked decisive. It was not: that queue had already drained, leaving the binaries
  as uncollected garbage. **A queue-depth reading taken after a burst says nothing about what filled
  the memory.**

Two tools now exist, both validated against planted holders rather than hoped at:

- `deploy/chaos/bin_holders.sh` — **who** holds the binaries (`refs`), or who is sitting on
  releasable garbage (`leak`, destructive). Found the 1.2 GB decoy the old tool reported as 0 MB.
- `deploy/chaos/bin_sizes.sh` — **what** they are holding: a size histogram, read from sizes only so
  it never copies a payload. This is the one that produced the table above. `Process.info(pid,
  :messages)` would have copied gigabytes into the calling process and is not safe on a node under
  study.

## A hypothesis killed before it cost a run

Before this, the leading theory was concurrent SEEDING: 1024 new tenants each needing a base copy to
4 peers. It was discarded in one grep of logs already on disk — a seed measured
`8192B db + 8272B wal`, about **16 KB**, and the queued payloads average ~1 MB. Sixteen-kilobyte
seeds cannot average a megabyte.

Worth recording as a habit rather than a result: the evidence that killed it had been sitting in a
log for two days.

## What this implies for a fix

Not attempted here, deliberately — three wrong fixes is enough, and the next one should follow from
the mechanism rather than from another number.

The bound has to be on **bytes, aggregated per node**, because bytes is the quantity that runs away.
But bounding alone only converts an OOM into refused writes, and the deeper property is the
feedback: **a delayed shard's next payload must not be allowed to grow without limit.** Anything
that caps the delta a single push may carry — shipping in fixed-size chunks, or refusing to
accumulate past a threshold and forcing a re-seed — attacks the loop instead of its symptom.

Whatever is tried: measure with `bin_sizes.sh` and watch the MEAN PAYLOAD, not the queue depth. The
queue depth was flat across a doubling of memory and would have reported success.

## Reproduce

```
REPLICATION_ENABLED=true ./chaos.sh up
TPC_NET=container TPC_DRIVER=elixir REPLICATION_ENABLED=true ./chaos.sh tpc-fleet 1024 &
# when a node's binary memory passes ~4 GB:
./bin_holders.sh <node> 1 refs          # name the holder
./bin_sizes.sh  <node> <holder>         # twice, ~40 s apart — watch the MEAN, not the count
```

Status: **open**. Replication on remains a **≤256-tenant** configuration on this rig, a boundary now
measured on both sides.
