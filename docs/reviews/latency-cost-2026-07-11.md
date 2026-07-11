# What an injected S3 RTT costs cold-open and flush — measured on the fleet

**2026-07-11, Apple M5 Max (18 cores), macOS 27.0.** The 3-node chaos rig (`fathom1/2/3`
behind nginx + MinIO + per-node toxiproxy, all in one colima VM — 12 vCPU / 94 GB, prod-release
nodes). This is the **TPC Phase-4 follow-on** the [fleet-density run](fleet-density-2026-07-10.md)
carried as Remaining Work #1.

The rig's existing `chaos.sh latency <ms>` *injects* S3 latency (the toxiproxy knob `failover`,
`pause-fence`, and the density runs lean on so their numbers reflect a real RTT rather than
loopback MinIO). It never *measured* what that latency costs. `chaos.sh latency-cost` does: it times
the two fathom paths whose wall-clock is **S3-round-trip-bound** — a **cold-open** (pull the shard
from MinIO) and a **flush** (upload it back) — with no injected latency (the loopback floor) and
again under an injected one-way RTT, so the delta isolates the S3 cost from local work. It is the
non-synthetic companion to the in-process [`scripts/benchmark_s3_sweep.sh`](../benchmark-plan.md):
same paths, but through the real prod-release coordinator (`Fathom.Shard`) and the real S3 client
(`Fathom.Shard.Storage.S3` → toxiproxy → MinIO), not a bench harness.

## What it does

`deploy/chaos/chaos.sh latency-cost [ms samples rows blob]` (defaults `30 12 64 1024`):

1. **Setup (untimed, no latency).** On each node, seed `samples` node-scoped shards (`rows × blob`
   bytes, ~64 KiB by default), then `Fathom.Shards.drain/2` each — flush to MinIO, drop the local
   copy, release the lease. Now the object lives in MinIO and no local copy exists, so the next
   `checkout` is a **genuine cold pull**. Ids are prefixed with the sanitized node name so the three
   nodes never contend for one lease; the driver runs each measure locally via `bin/fathom rpc`,
   bypassing the LB, because cold-open/flush are **per-node** properties (the LB partition is what
   `chaos.sh density` measures, not this).
2. **Measure (timed).** For each shard: `:timer.tc` around `Fathom.Shards.checkout/1` — the coordinator
   starts, acquires the lease and pulls the `.db` from MinIO, and the call **blocks until the open
   completes**, so the timing captures the whole cold-open. Then a dirty write + `WriteCounter.bump/1`
   (so the flush is write-gated **on** — a clean shard skips the upload, which would time a no-op),
   then `:timer.tc` around `drain/2` — checkpoint, upload the `.db`, release the lease. `drain` also
   re-drops the local copy, so each shard is re-armed for the next round. Report p50 / min / max per
   node over the sample.
3. **Baseline then injected.** Run the measure with latency cleared (loopback floor), inject `ms` each
   way on every node's `s3-fathomN` toxiproxy, run it again, clear latency. Small shards by default so
   the number reflects the **RTT**, not bulk transfer — raise `rows`/`blob` to fold transfer cost in.

## Results — a clean, linear sweep (12 samples/node × 3 nodes)

Baseline is the loopback-MinIO floor (no injected latency); "injected" is the fleet average of the
per-node p50 under `N` ms **each way**. `Δ` is injected − baseline; `×` is Δ against the one-way `N`.

| one-way | cold-open base → inj | cold-open Δ | ≈ RTTs | flush base → inj | flush Δ | ≈ RTTs |
|--------:|---------------------:|------------:|-------:|-----------------:|--------:|-------:|
|  10 ms  | 2.5 → **24.4 ms**    | 21.9 ms (2.2×) | ~1.1 | 4.3 → **70.3 ms**  | 66.1 ms (6.6×) | ~3.3 |
|  30 ms  | 2.4 → **76.6 ms**    | 74.2 ms (2.5×) | ~1.25| 4.1 → **214.8 ms** | 210.7 ms (7.0×) | ~3.5 |
|  60 ms  | 2.4 → **136.9 ms**   | 134.5 ms (2.2×)| ~1.1 | 4.2 → **399.5 ms** | 395.3 ms (6.6×) | ~3.3 |

Per-node spread is tight and even (30 ms run: cold-open p50 75.9 / 76.5 / 77.2 ms across the three
nodes, range 67.6–84.0 ms; flush p50 214.8 / 214.9 / 214.9 ms, range 196.4–231.9 ms) — the three
nodes pay the same S3 cost, as expected (each is an independent single-node fathom against the same
MinIO through its own toxiproxy).

## Findings

- **Cold-open is ~1 RTT, as designed.** It tracks **~2× one-way + a ~2.4 ms local floor** — Δ 21.9 /
  74.2 / 134.5 ms at 10 / 30 / 60 ms one-way. That is the `Fathom.Shard.init` optimization working on
  the real fleet: the lease acquire (`PUT If-None-Match`) and the `.db` pull are **independent objects
  overlapped in `handle_continue`**, so the wall-clock is one round trip, not two serialized ones. The
  ~0.25 RTT over a clean 1.0 is the second half of the two one-way legs plus MinIO service time.
- **It matches the in-process sweep almost exactly.** `scripts/benchmark_s3_sweep.sh` (MinIO +
  toxiproxy, in-process) reported one-way 10 / 30 / 60 ms → ~26 / 77 / 137 ms cold-open; this
  real-fleet run reads **24.4 / 76.6 / 136.9 ms**. Two independent instruments (a bench harness and a
  prod-release node driven over `rpc`) agree within noise — the cold-open cost model is trustworthy.
- **The flush path costs ~3× the round-trips of cold-open (~3.5 RTT), and it is *not* overlapped.**
  Flush Δ is a flat **~6.6–7.0× the one-way** across the sweep. `drain`'s durability path is
  sequential: checkpoint (local) → upload the `.db` → upload the `.db.etag` sidecar → release the
  `{owner,epoch}` lease. Each S3 write is its own round trip, serialized, where cold-open collapsed
  its two objects into one. This is the honest asymmetry: **the read path is tuned to 1 RTT; the
  write/release path is not.** It is a candidate for the same treatment cold-open got — overlap the
  `.db` + etag PUTs, or fold the lease release into the final write — which would roughly halve the
  RTT-bound flush cost. On a real WAN (30 ms one-way) that is ~215 ms → potentially ~120 ms.
- **Both are pure RTT cost, not local work.** The baseline floor (cold-open ~2.4 ms, flush ~4.2 ms) is
  the same regardless of injected latency; everything above it is round trips. So on a fat local pipe
  (same-region S3, ~1 ms) both paths are near-floor, and the numbers here are the tail you pay when a
  node's storage is a region away — the exact case the warm-standby (A1) and the lease-fence design
  care about (a failover cold-open under real S3 latency).

## Why this matters / where it plugs in

- **Failover RTO.** A survivor's cold-open on failover is this cold-open cost. At 30 ms one-way that is
  ~77 ms of S3 before the shard serves — which is why the [warm standby](../warm-standby.md) pre-pulls
  the body and turns the failover open into a 304 revalidation (`failover_warm_s3` in the bench plan).
  This run is the *cold* leg of that comparison, measured on the fleet.
- **Durability cadence.** The periodic write-gated flush (`docs/durability.md`) pays the flush cost
  above on every dirty interval. At a WAN RTT that ~215 ms is real per-flush wall-clock (off the hot
  path — it runs in a Task — but it bounds how tight the flush interval can usefully be, and it is the
  first place to spend an optimization if RPO ever needs shortening).

## Limits (read before citing)

- **Relative, single-host.** toxiproxy injects a *fixed* latency each way; real S3 has variance, connection
  setup, and TLS the loopback path doesn't. The numbers are a clean **RTT-scaling model**, not an
  absolute prod SLA — the value is the *shape* (cold-open ≈ 1 RTT, flush ≈ 3.5 RTT) and the
  cross-check against the in-process sweep, not the milliseconds.
- **Small shards by design.** ~64 KiB isolates the RTT from bulk transfer; toxiproxy here injects
  latency only (no bandwidth cap), so transfer time is negligible and the delta is essentially pure
  round trips. Raise `rows`/`blob` to measure a data-heavy shard where transfer dominates — a
  different question than this run answers.
- **p50 of 12 samples/node.** Enough to pin the median and see the spread is tight; not a tail study.
  The min/max columns show the per-open jitter (mostly MinIO service-time noise), not a p99.
- **`drain` as the flush proxy.** `drain` is the flush-**and**-release path (idle-drop / handoff). The
  steady-state periodic flush uploads without releasing the lease, so it is ~1 RTT cheaper than the
  numbers here — the flush cost above is the upper bound (upload + release), which is the right figure
  for failover/handoff reasoning and a slight overstate for the periodic-flush case.
