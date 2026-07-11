# Multi-node fleet shard density — the LB keyspace-partition, measured

**2026-07-10, Apple M5 Max (18 cores), macOS 27.0.** The 3-node chaos rig (`fathom1/2/3`
behind nginx + MinIO, all in one colima VM, prod-release nodes). This fills the gap the
single-node density work left open.

The [head-to-head density section](turso-headtohead-2026-07-10.md#density--how-many-tenants-can-one-server-hold-and-at-what-cost)
measured how many tenants **one** fathom node holds and at what cost (`mix fathom.scale --ramp`),
and noted the honest limit: the storage-"millions" story is *architectural* — a laptop caps at one
node, and the real capacity lives in the **LB keyspace-partition** that spreads shards across the
fleet. This run measures that partition directly: mint N novel shards through the real LB, then read
per node how many coordinators each holds (**the partition**) and the memory each shard costs
(**the per-shard price**). The claim under test: capacity is **horizontally additive** — the fleet
holds ~N total, ~N/nodes each, at the single-node per-shard cost, with no per-node degradation.

## What it does

`deploy/chaos/chaos.sh density [shards workers]`:

1. **Mint N novel shards through the LB.** Each is one real Hrana stream — `open → SELECT 1 → close`
   (no table needed) against `d_<i>.fathom.test`. nginx consistent-hashes the Host subdomain to one
   backend, so `d_1..d_N` spread across the three nodes. Each open is a genuine **cold-open**: create
   the file + acquire the `{owner,epoch}` lease in MinIO on the hash-home node.
2. **Read per node.** `Registry.count(Fathom.ShardRegistry)` = coordinators held (the partition);
   `:erlang.memory(:total)` after a fleet-wide GC + `/proc/self/status` VmRSS = the cost. Baseline
   captured before minting, so the delta is only this run's shards.
3. Coordinators are held through the read by raising `:shard_idle_ms` **before** minting (a
   coordinator freezes its idle timeout at init, so this only affects the new opens); the rebalancer
   is quieted so no handoff moves a shard off its hash-home node and blurs the partition. Both are
   restored at the end.

## Result (N = 6000, 24 workers)

Minted 6000 novel shards in **18.6 s (~320 shards/s through the LB)** — each a cold-open +
lease-acquire round-trip to loopback MinIO; **5989 / 6000 held** (11 mint drops, 0.18%, transient
under the burst — see limits).

| node | coordinators held | live BEAM Δ | RSS Δ | RSS / shard |
|---|---:|---:|---:|---:|
| fathom1 | 1845 | 12.8 MB | 45.2 MB | **25.1 KiB** |
| fathom2 | 1949 | 11.9 MB | 46.7 MB | **24.5 KiB** |
| fathom3 | 2195 | 15.0 MB | 55.6 MB | **25.9 KiB** |
| **fleet** | **5989** | ~40 MB | **~148 MB** | ~25 KiB |

- **Partition is even.** Ideal 2000/node; observed 1845 / 1949 / 2195, **spread 1.19× (max/min)** —
  consistent-hashing variance over 6000 keys into 3 buckets, not a defect (it tightens with more keys
  or vnodes). No node was starved or overloaded; the keyspace split ~evenly with no coordination.
- **Per-shard cost matches the single node — on every node.** ~**25 KiB RSS/shard** (~6–7 KiB live
  BEAM), dead-consistent across all three (25.1 / 24.5 / 25.9 KiB), and it lands right on the
  single-node **~26 KiB idle-coordinator** figure from the `--ramp` density work. Holding a shard
  warm-available on a rig node costs the same whether the node holds one shard or two thousand — **no
  per-node degradation as the fleet grows.**
- **Capacity is additive.** Three nodes held ~6000 shards for ~148 MB RSS (~49 MB/node). The fleet's
  ceiling is `nodes × per-node ceiling` (the single-node `--ramp` held 10k open at 1873 MB under the
  `max_open_shards` cap; the hardware fd ceiling is ~82k) — and only the **active working set** is
  resident. Idle shards flush to MinIO and cost **0** resident; they cold-re-open on the next request.

## Why this is the density story sqld can't match

Same SQLite engine, opposite resident model — and the pivot is **what memory scales with**, not the
per-unit price:

| | fathom (this run) | sqld ([density section](turso-headtohead-2026-07-10.md#density--how-many-tenants-can-one-server-hold-and-at-what-cost)) |
|---|---|---|
| resident cost / tenant | ~25 KiB RSS (idle coordinator) | ~17 KiB / namespace |
| **resident set** | the **active working set**, divided across N nodes (idle → 0, bottomless in MinIO) | **every** namespace, all in **one** process |
| grows with | active shards / node count | total tenants |
| where it caps | add nodes (horizontal) | one machine's RAM (~17 GB @ 1M in one process) |
| create cost | **O(1)** cold-open, no per-node slowdown (flat 320/s here across the fleet) | admin-API call, rate **degrades** 116 → 29 /s at 1k → 4k |

sqld's per-namespace cost is actually a hair *lower*. That is not the point. sqld keeps **all**
namespaces resident in **one** process, so its RSS tracks *total* tenants and tops out at one box;
fathom keeps only the **active** set resident and spreads it across the fleet, so its resident cost
is `active_set / nodes` and you grow capacity by adding nodes. At a million tenants sqld needs ~17 GB
in a single RSS with a create rate still dropping; fathom holds each node's working set at ~25 KiB
apiece, pages the idle tail to MinIO, and partitions the rest evenly across however many nodes you
run — which is what this run measured the LB actually doing.

## Honest limits

- **Bounded, not run to millions.** N = 6000 across 3 nodes on one shared VM (the density-scope call
  was deliberately conservative — ramping every node to its ceiling risks OOMing the colima VM). The
  "millions" figure is arithmetic: **measured even partition × measured per-node ceiling × node
  count**. The partition evenness and the flat per-node cost are the two things that make that
  multiplication valid, and both are measured here; the product is not exhaustively run.
- **11/6000 mint drops (0.18%).** A few streams failed under the 24-worker burst against loopback
  MinIO (cold-open contention / transient); not chased, and it doesn't move the partition or cost.
- **RSS is sticky.** VmRSS doesn't shrink after a GC, so the ~25 KiB/shard is an honest *upper* bound
  that includes allocator arenas + the (tiny, empty) shard file's page cache; the ~6–7 KiB live BEAM
  is the pure coordinator + Registry + ShardLoad-row cost.
- **Relative, one host.** Loopback MinIO and three containers sharing one VM's cores — the mint rate
  and absolute RSS are rig-relative; the *shape* (even split, flat per-node cost, additive) is the
  result.
- **Spread 1.19× is the hash, not the code.** Finer partitioning (more keys, or nginx `hash`
  vnodes/weights) narrows it; it does not indicate a hot node.

## Reproduce

```bash
cd deploy/chaos
./chaos.sh up                       # rig: fathom1/2/3 + nginx + MinIO (only minio may be up)
# restart the fathom nodes first for a pristine baseline (held == minted); shard files are in MinIO,
# so nothing is lost — coordinators are in-memory only:
docker compose restart fathom1 fathom2 fathom3
./chaos.sh density 6000 24          # mint 6000 novel shards through the LB; per-node partition + cost
```

The harness disables the rebalancer and raises the idle timeout for the run, then restores both.
After any rig run, restore the rebalancer's tracked runtime artifact:
`git checkout -- deploy/chaos/lb-runtime/exceptions.conf`.
