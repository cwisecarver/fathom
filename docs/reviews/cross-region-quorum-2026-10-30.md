# Cross-region followers, emulated (2026-10-30)

What a write costs when a node's followers are a region away. fathom has nothing that stops nodes
running in several regions (each node is independent and coordinates only through S3), but it had
never been measured with followers at cross-region distance. This emulates that on the rig with
`./chaos.sh repl-latency 15`: 15 ms each way on every follower link, so about 30 ms added to each
quorum round trip. Code: `aeab5b76` (filo 0.4.3).

## Setup

Each arm: `colima restart`, then

```bash
cd deploy/chaos
REPLICATION_ENABLED=true SHARD_FLUSH_INTERVAL_MS=30000 REPL_VIA_TOXI=1 ./chaos.sh up
./chaos.sh repl-latency 15      # the 15 ms arm only
```

`REPL_VIA_TOXI=1` routes every follower link through toxiproxy in BOTH arms, so the 0 ms arm is the
control for the proxy hop itself. The 15 ms arm had 10 latency toxics on the 5 `repl-*` proxies; the
0 ms arm had none. Every node ran filo 0.4.3, and no node logged `clock lags the store`. Scripts:
`logs/ds/xregion.sh`, `logs/ds/xlight.sh` (not committed).

## Light load: what one write costs

Open loop at a fixed 500 txn/s over 256 tenants (`TPC_TAIL_ARMS=open TPC_OPEN_RATE=500 ./chaos.sh
tpc-tail 256 200 1000 2`), two runs per arm. Response time is measured from each transaction's
intended start.

| followers | run | txn/s | p50 | p99 (CI95) | errors |
|---|---|---|---|---|---|
| 0 ms | 1 | 499.9 | 2.88 ms | 5.8 ms (5.7-5.9) | 0 |
| 0 ms | 2 | 499.9 | 2.87 ms | 6.2 ms (6.0-6.4) | 0 |
| ~30 ms RTT | 1 | 499.8 | 33.83 ms | 36.6 ms (36.5-36.7) | 0 |
| ~30 ms RTT | 2 | 499.8 | 33.93 ms | 36.7 ms (36.6-36.7) | 0 |

The replication commit stage (`fathom_replication_commit_duration`, tenant mode) went from a 1.07 ms
mean to 31.78 ms. **A cross-region follower set adds one round trip per write and nothing else**:
p50 +31.0 ms and p99 +30.6 ms for 30 ms of added round trip, with the two runs of each arm within
0.4 ms of each other.

Both arms show p99 windows of 40-80 ms at 30 s, 60 s and 90 s. That is the 30 s durability flush,
present with or without the added latency.

## Saturated: what it costs a busy fleet

The closed-loop 1024-tenant `tpc-fleet` shape from `tpc-fleet-repl-on-2026-10-30.md`, one run per arm:

| followers | txn/s | p50 | p99 (CI95) | errors | replication commit mean |
|---|---|---|---|---|---|
| 0 ms | 4,191 | 227 ms | 387 ms (386-388) | 0 | 35 ms |
| ~30 ms RTT | 4,202 | 225 ms | 389 ms (388-390) | 0 | 62 ms |

The added latency reached the commit path (+27 ms) but not throughput or end-to-end latency. At
saturation every transaction already queues ~225 ms for CPU on the one VM, and waiting on a remote
follower costs no CPU, so it replaces queueing time instead of adding to it. A replicating fleet
does not lose throughput to far-away followers; it loses latency only when it is not saturated.

## What this does not show

- **The network is fake.** Five containers on one VM with toxiproxy adding a fixed delay: no jitter,
  no loss, no bandwidth limit, and the client is on the same host as the nodes. Real cross-region
  links add all of these.
- **The delay is uniform.** Every follower link gets the same 15 ms, so this is "every follower is a
  region away", not a three-region topology with near and far peers. Per-pair delay needs
  per-link proxies or `tc netem`.
- **S3 stayed local.** In a real multi-region deployment, nodes outside the bucket's region pay a
  cross-region round trip on every cold open, lease and flush, and the bucket's region is a single
  dependency: if it is down, no node can renew its lease.
- **Aurora DSQL publishes 7.4 ms p99 single-region and about 30 ms p99 multi-region COMMIT latency**
  ([paper](https://arxiv.org/pdf/2607.13276)), measured on real AWS. The fathom numbers here are a
  whole TPC-B transaction on an emulated network, so they show the same shape (one round trip to the
  nearest other copy per write), not a head-to-head result.
