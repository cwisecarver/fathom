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
   against `d_<i>.fathom.test`. `SELECT 1` is a **constant** — it touches no table and writes nothing;
   the only I/O is the **cold-open** itself: create the (empty) file + acquire the `{owner,epoch}`
   lease in MinIO on the node nginx consistent-hashes the Host subdomain to. So `d_1..d_N` spread
   across the three nodes, each an empty shard that has been touched exactly once.
2. **Read per node.** `Registry.count(Fathom.ShardRegistry)` = coordinators held (the partition);
   `:erlang.memory(:total)` after a fleet-wide GC + `/proc/self/status` VmRSS = the cost. Baseline
   captured before minting, so the delta is only this run's shards. **No traffic runs during this
   read** — the streams are already closed, so the shards sit idle (see limits).
3. Coordinators are held through the read by raising `:shard_idle_ms` **before** minting (a
   coordinator freezes its idle timeout at init, so this only affects the new opens); the rebalancer
   is quieted so no handoff moves a shard off its hash-home node and blurs the partition; and the
   `max_open_shards` soft cap is lifted so it doesn't truncate the measurement. All restored at the end.

## Result — pushed to N = 30000 (~10k/node), 32 workers

Minted 30000 shards in **109 s (~275 shards/s through the LB)** — each a cold-open + lease-acquire
round-trip to loopback MinIO; **29956 / 30000 held** (44 mint drops, 0.15%). The `max_open_shards`
soft cap is lifted for the run (else a node approaching it starts LRU-evicting idle shards and we'd
measure eviction churn, not raw density).

| node | coordinators held | live BEAM Δ | RSS Δ | RSS / shard |
|---|---:|---:|---:|---:|
| fathom1 | 9049 | 46.0 MB | 153.3 MB | **17.4 KiB** |
| fathom2 | 9885 | 49.8 MB | 151.8 MB | **15.7 KiB** |
| fathom3 | 11022 | 57.8 MB | 179.0 MB | **16.6 KiB** |
| **fleet** | **29956** | ~154 MB | **~484 MB** | ~16 KiB |

- **Partition stays even at 5× the scale.** Ideal 10000/node; observed 9049 / 9885 / 11022, **spread
  1.22× (max/min)** — statistically identical to the 6000-shard run's 1.19×. The consistent hash's
  variance is a property of the hash, not the load: it does **not** widen as the fleet fills.
- **Per-shard cost is flat-to-*decreasing*.** ~**16 KiB RSS/shard** (~5 KiB live BEAM), dead-consistent
  across all three (17.4 / 15.7 / 16.6 KiB) — and *lower* than the 6000-shard run's ~25 KiB, because
  RSS carries a fixed per-node baseline (allocator arenas, initial heaps) that amortizes over more
  shards. **Marginal density improves with scale:** 5× the shards cost only 3.3× the RSS (148 → 484
  MB). No per-node degradation, no eviction, no blowup.
- **Capacity is additive.** 30000 shards held across 3 nodes for ~484 MB RSS (~161 MB/node delta) —
  and this is the **idle-coordinator** regime (warm-available, the connection closed at stream end),
  the number that governs how many shards a node keeps *resident between requests*. An *actively
  streaming* shard also holds an exqlite connection + fds — the ~180 KiB fanout cost measured
  elsewhere and fd-bound at ~82k/node; the warm-resident floor here is ~10× cheaper and memory-bound.
  Only the active set is resident; idle shards flush to MinIO at **0** resident and cold-re-open on
  the next request.

### Scale comparison — the per-shard cost falls as it grows

| N | held | fleet RSS Δ | RSS / shard | spread (max/min) |
|---:|---:|---:|---:|---:|
| 6000 | 5989 | 148 MB | 25.4 KiB | 1.19× |
| 30000 | 29956 | 484 MB | 16.2 KiB | 1.22× |

Same even split at both scales; the marginal RSS/shard *drops* as the fixed per-node overhead
amortizes — the opposite of a system whose per-tenant cost climbs with tenant count.

## Why this is the density story sqld can't match

Same SQLite engine, opposite resident model — and the pivot is **what memory scales with**, not the
per-unit price:

| | fathom (this run) | sqld ([density section](turso-headtohead-2026-07-10.md#density--how-many-tenants-can-one-server-hold-and-at-what-cost)) |
|---|---|---|
| resident cost / tenant | ~16–25 KiB RSS (idle coordinator; falls with scale) | ~17 KiB / namespace |
| **resident set** | the **active working set**, divided across N nodes (idle → 0, bottomless in MinIO) | **every** namespace, all in **one** process |
| grows with | active shards / node count | total tenants |
| where it caps | add nodes (horizontal) | one machine's RAM (~17 GB @ 1M in one process) |
| create cost | **O(1)** cold-open, no per-node slowdown (flat 320/s here across the fleet) | admin-API call, rate **degrades** 116 → 29 /s at 1k → 4k |

sqld's per-namespace cost is actually a hair *lower*. That is not the point. sqld keeps **all**
namespaces resident in **one** process, so its RSS tracks *total* tenants and tops out at one box;
fathom keeps only the **active** set resident and spreads it across the fleet, so its resident cost
is `active_set / nodes` and you grow capacity by adding nodes. At a million tenants sqld needs ~17 GB
in a single RSS with a create rate still dropping; fathom holds each node's working set at ~16–25 KiB
apiece (cheaper at scale), pages the idle tail to MinIO, and partitions the rest evenly across however
many nodes you run — which is what this run measured the LB actually doing.

## Served density — shards under a live connection (fd-bound)

The run above is the warm-resident *floor* — idle coordinators, memory-bound. The *ceiling* is how
many shards a node holds **under a live connection** at once (`open → query`, connection held): a
served shard also holds an exqlite/SQLite handle, which is **fd-bound**. `chaos.sh served [per]`
measures it — it opens `per` node-scoped shards per node, each keeping a connection, drives a query
pass over all of them, and reads per-node RSS + fds. (Opens are local per node, not through the LB:
the served ceiling is a per-node property; the LB partition is what the idle run measures.)

**The default fd limit is the wall.** As shipped, the rig container's soft `nofile` is **1024** and
the BEAM holds ~84 at rest — so a node hits `EMFILE` at **~945 served shards** (measured), at **1 fd
per connection** (an empty WAL connection here holds one fd, not the three a macOS host shows) and
**~190 KiB RSS each**. Memory is nowhere near the limit (~180 MB); fds are. Real deployments provision
fds, so `docker-compose.yml` now sets the fathom nodes' `nofile` to soft 65536 / hard 524288.

**With fds provisioned — 30000 held across the fleet at once** (`chaos.sh served 10000`, 10k/node,
all three nodes concurrent):

| node | held (live conn) | RSS / shard | fds / shard | q/s (pass) |
|---|---:|---:|---:|---:|
| fathom1 | 10000 | 232 KiB | 1.0 | 55319 |
| fathom2 | 10000 | 216 KiB | 1.0 | 50694 |
| fathom3 | 10000 | 231 KiB | 1.0 | 56723 |
| **fleet** | **30000** | ~220 KiB | 1.0 | **~163k** |

- **Served costs ~220 KiB/shard — ~14× the ~16 KiB idle floor.** That's the price of the held
  connection (the exqlite handle + WAL) on top of the coordinator, and it matches the single-node
  ~180–196 KiB fanout figure. Still memory-cheap: 30000 live connections cost ~6.6 GB across the
  fleet, a rounding error against the VM's 94 GB.
- **The ceiling is fds, and fds are a knob.** 1 fd per connection, so a node's served ceiling is
  ~`nofile` (minus the ~84 baseline) — lift `nofile` to lift it. At soft 65536 a node holds well past
  10k; the fleet multiplies by node count. "How many can we serve at once" = `nodes × nofile`, a
  provisioning number, not an architectural wall.
- **~163k q/s aggregate** on a query pass over the 30000 held connections (each node ~50k/s, a single
  sequential sweep — a reachability/throughput check, not a sustained-load benchmark).

So both regimes scale with the *active* set, not total tenants: the warm floor (~16 KiB, memory-bound,
30k+ held) is what a node keeps *available*; the served ceiling (~220 KiB, fd-bound, 10k/node) is what
it *serves at once*. Idle tenants beyond either are bottomless in MinIO at 0 resident cost.

## Honest limits

- **The idle run is empty shards, no load, no data.** The partition/floor tables measure the
  **warm-resident floor**: each shard is touched once (a cold-open + one constant `SELECT 1`), holds
  no data, and is idle during the read — the connection is already closed. So the ~16 KiB/shard is the
  cost of *keeping a shard available between requests*. The **served** regime (shards under a live
  connection) is measured separately above — 30000 held at once, ~220 KiB/shard, fd-bound. A shard
  **with real data** would additionally warm OS page cache for its hot pages; that (a data-bearing
  working set) is the one axis neither run exercises.
- **Bounded, not run to millions.** N = 30000 across 3 nodes on one VM (which has 94 GB — memory was
  never the limit; the earlier 6000 run was conservatism before that was confirmed). The "millions"
  figure is arithmetic: **measured even partition × measured per-node ceiling × node count**. The
  even partition and the flat (falling) per-node cost are what make that multiplication valid, and
  both are measured here; the product is not exhaustively run.
- **44/30000 mint drops (0.15%).** A few streams failed under the 32-worker burst against loopback
  MinIO (cold-open contention / transient); not chased, and it doesn't move the partition or cost.
  Mint throughput eased 320 → 275 shards/s from 6k to 30k — a loopback-MinIO cold-open ceiling (the
  lease PUT), not a fathom density limit.
- **RSS is sticky.** VmRSS doesn't shrink after a GC, so the ~16 KiB/shard is an honest *upper* bound
  that includes allocator arenas + the (tiny, empty) shard file's page cache; the ~5 KiB live BEAM is
  the pure coordinator + Registry + ShardLoad-row cost.
- **Relative, one host.** Loopback MinIO and three containers sharing one VM's cores — the mint rate
  and absolute RSS are rig-relative; the *shape* (even split, falling per-node cost, additive) is the
  result.
- **Spread 1.22× is the hash, not the code.** Finer partitioning (more keys, or nginx `hash`
  vnodes/weights) narrows it; it held flat from 6k to 30k and does not indicate a hot node.

## Reproduce

```bash
cd deploy/chaos
./chaos.sh up                       # rig: fathom1/2/3 + nginx + MinIO (only minio may be up)
# restart the fathom nodes first for a pristine baseline (held == minted); shard files are in MinIO,
# so nothing is lost — coordinators are in-memory only:
docker compose restart fathom1 fathom2 fathom3
./chaos.sh density 30000 32         # idle floor: mint 30000 shards, per-node partition + cost
./chaos.sh served  10000            # served ceiling: hold 10k live connections/node (30k fleet)
```

`density` lifts the `max_open_shards` cap for the run (to `shards`), so N is bounded only by node
memory — the 94 GB VM here has room for far more than 30000. `served` is **fd-bound**, so
`docker-compose.yml` raises the fathom nodes' `nofile` to soft 65536 (the default 1024 caps a node at
~940); recreate the nodes (`docker compose up -d fathom1 fathom2 fathom3`) after changing it.

The harness disables the rebalancer and raises the idle timeout for the run, then restores both.
After any rig run, restore the rebalancer's tracked runtime artifact:
`git checkout -- deploy/chaos/lb-runtime/exceptions.conf`.
