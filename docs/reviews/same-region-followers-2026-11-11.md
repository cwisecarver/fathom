# Same-region follower links, emulated with toxiproxy (2026-11-11)

What a write costs when a node's followers are in other availability zones of the same region, as
the closest approximation on the rig to Aurora DSQL's single-region case (the comparison
[cross-region-quorum-2026-10-30.md](cross-region-quorum-2026-10-30.md) left open). Code: `f6280fd0`,
filo 0.4.4, image built that day. Scripts: `logs/ds/samereg.sh`, `logs/ds/samereg-probe.sh` (not
committed).

## What same-region latency is

AWS commits to nothing ("single-digit millisecond"). Measured numbers:

| Source | Result |
|---|---|
| [Bits and Cloud, 2023-10](https://www.bitsand.cloud/posts/cross-az-latencies) — `ping` between `t3.micro` in every AZ pair, 28 regions | fastest pairs 0.39-0.47 ms RTT (`us-east-1b`-`c` 0.44, `us-west-2a`-`c` 0.46); slowest `sa-east-1` 2.42 ms; "the vast majority of AZ pairs had sub-millisecond latency". The full per-pair table is collapsed on the page, so **no all-region mean was available**. |
| [AWS Architecture Blog, AZ affinity](https://aws.amazon.com/blogs/architecture/improving-performance-and-reducing-cost-using-availability-zone-affinity/) | "single digit millisecond roundtrip latency between AZs"; intra-AZ p50 about 0.3 ms. Its worked example assumes 1.5 ms cross-AZ p50 and 1 ms minimum; those are the author's assumptions, not measurements. |

So the target is **about 0.5 ms RTT typical, 0.4-2.4 ms across regions and pairs**.

## What toxiproxy can and cannot do

Measured on the rig, keep-alive HTTP to fathom2's health port from fathom1, 399 requests per cell,
time to first byte:

| path | p50 | p99 |
|---|---|---|
| direct | 0.060-0.063 ms | 0.094 ms |
| through toxiproxy, no toxic | 0.107-0.111 ms | 0.139-0.194 ms |
| through toxiproxy, `latency=1` each way | 3.28-3.31 ms | 3.42 ms |

- The proxy hop itself costs about 0.05 ms. The "about 450 µs" in `docs/a2-quorum-replication.md`
  came from an in-process harness; it does not hold on the rig.
- The latency toxic takes **whole milliseconds** (`latency`, `jitter` are integers). 0.25 ms each
  way cannot be set.
- The smallest toxic, 1 ms each way, produces about **3.2 ms** of added round trip here, not 2:
  the extra ~1.2 ms is unexplained (a guess: per-chunk scheduling in the toxic pipeline).

Neither point is near 0.5 ms. The two arms below bracket the target from both sides (0.11 ms under
it, 3.3 ms over it). A point at 0.5 ms needs `tc netem` in the containers, not toxiproxy.

## Result

`REPLICATION_ENABLED=true SHARD_FLUSH_INTERVAL_MS=30000 REPL_VIA_TOXI=1 ./chaos.sh up`, then
`TPC_TAIL_ARMS=open TPC_OPEN_RATE=500 ./chaos.sh tpc-tail 256 200 1000 2` (open loop, 500 txn/s, 256
tenants, 5 nodes; TPC_NET=container TPC_DRIVER=elixir). Each arm after `colima restart`; no node logged
`clock lags the store`; toxics read back (0 and 10 on the 5 `repl-*` proxies).

| follower links | run | txn/s | p50 | p99 excl. first 10 s (CI95) | errors | replication commit mean |
|---|---|---|---|---|---|---|
| toxiproxy hop only (~0.11 ms RTT) | 1 | 499.9 | 3.31 ms | 7.1 ms (6.7-8.0) | 0 | 1.13 ms (both runs) |
| toxiproxy hop only | 2 | 499.9 | 3.20 ms | 7.0 ms (6.5-8.0) | 0 | |
| 1 ms each way (~3.3 ms RTT) | 1 | 499.9 | 6.01 ms | 9.2 ms (9.0-9.3) | 0 | 3.83 ms (both runs) |
| 1 ms each way | 2 | 499.9 | 6.03 ms | 9.3 ms (9.2-9.5) | 0 | |

The hop-only arm reproduces the 2026-10-30 control (p50 2.88 ms, p99 5.8-6.2 ms; commit mean
1.07 ms) to within the host's noise. Adding about 3.2 ms of round trip adds +2.7-2.8 ms to p50 and
+2.2-2.3 ms to p99, so a write costs roughly 0.7-0.9 ms per ms of follower round trip at this load
(the 15 ms arm of the 2026-10-30 run read about 1.0).

[INFERENCE] Interpolating that slope to a 0.5 ms RTT gives p50 about 3.6-3.7 ms and p99 about 7.3-7.4
ms for a whole TPC-B transaction. This is an interpolation between two measured points, not a run.

## Against Aurora DSQL

DSQL publishes 7.4 ms p99 single-region COMMIT latency ([paper](https://arxiv.org/pdf/2607.13276),
quoted in the 2026-10-30 report). The interpolated fathom p99 is the same size, but the two are not
the same quantity: fathom's number is a whole TPC-B transaction (several statements plus the commit)
over a fake network, DSQL's is the COMMIT alone on real AWS. Read it as "same order of magnitude, one
round trip to the nearest quorum per write", which is also the shape the cross-region run showed.

## What this does not show

- **No jitter, loss or bandwidth limit, and the client shares the host.** As in 2026-10-30. The p99
  windows of 44-144 ms at 30 s, 60 s and 90 s are the 30 s durability flush, present in both arms.
- **The host was not quiet.** `biomesyncd` and `corespotlightd` held about 65% CPU each when checked just
  before the runs. Both arms ran after that check, so it may or may not have persisted; the absolute numbers are relative, as for every rig result.
- **S3 stayed local.** Nodes in a real region pay S3 latency on cold open, lease and flush.
- **One run pair per arm, one commit mean per arm** (the Prometheus histogram is cumulative). The
  commit p99 only resolves to its 25 ms bucket.
