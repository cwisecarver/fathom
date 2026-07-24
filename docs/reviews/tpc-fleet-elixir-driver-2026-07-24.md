# tpc-fleet with the Elixir (Filo.Client) driver — 2026-07-24 (1024 tenants from ONE process)

The high-concurrency findings in [`tpc-fleet-highconc-2026-07-23.md`](tpc-fleet-highconc-2026-07-23.md)
were all about the **Python driver's** limits, never fathom's: colima's host forwarder dies
~1k conns (→ drive in-network), raw `http.client` breaks on stale keepalives, and one GIL-bound
process can't drive past ~128 clients (the solo-256 point was reproducibly pathological), so 1024
needed **4 × 256-client processes** for 2,661 tps.

This run swaps the driver for the new Elixir one — `Fathom`'s `tpc_driver.exs` over `Filo.Client`,
one lightweight BEAM process per client — packaged as an in-network container. It clears every one
of those driver ceilings: **6→1024 concurrent tenants from a single container, zero errors at every
step, including the 256 point that broke the solo Python process.**

## Results (in-network, one `fathom-chaos-driver` container, 100 txns/tenant, 200 accounts/tenant)

| tenants | txns | agg txn/s | p50 ms | p95 ms | p99 ms | errs |
|---:|---:|---:|---:|---:|---:|---|
| 16 | 1,600 | 3,077 | 4.8 | 6.0 | 6.6 | 0 |
| 64 | 6,400 | 4,222 | 11.6 | 19.4 | 23.2 | 0 |
| 128 | 12,800 | 3,934 | 26.4 | 39.3 | 44.3 | 0 |
| 256 | 25,600 | 3,992 | 39.4 | 59.0 | 65.7 | 0 |
| 512 | 51,200 | 3,959 | 47.5 | 76.4 | 89.4 | 0 |
| **1024** | **102,400** | **3,807** | **50.0** | **99.8** | **118.6** | **0** |

Cross-LB warm `SELECT 1` RTT p50 ~200 µs (the per-statement network floor). Per-node split at 1024:
491 / 507 / 538 shards (fathom1/2/3), 347k / 359k / 380k queries — 1,536 tenant shards, 1.09M
queries, **spread 1.10×** (the consistent hash evening out with key count).

## What made 1024-from-one-process work

1. **One BEAM process per client, no GIL.** The BEAM holds 1024 lightweight processes each owning a
   Mint socket at ~KB of state — no thread-per-client memory, no GIL serialization. The
   ≤128-clients-per-process rule and the multi-process requirement were both Python artifacts; the
   Elixir driver has neither. The solo-256 point that was reproducibly pathological for one Python
   process (18.3% stale-keepalive churn, 10 s p95 stalls) ran clean here.
2. **In-network container.** Driving from a container on the compose network (LB as `http://lb`)
   sidesteps colima's host port-forwarder, which dies ~1k conns. `TPC_NET=container` shells out to
   `docker compose run --rm driver …`.
3. **Raised container `nofile`.** One held socket per client, so past ~1000 clients the driver hits
   the container's default 1024 fd cap (EMFILE). The `driver` service sets nofile 65536/524288
   (matching the nodes) — without it, the 1024 step would fault at the fd limit, not on anything
   fathom does.
4. **Shared wire codec.** The driver rides `Filo.Client`, which reuses the server's own
   `Filo.Value` / `Filo.StmtResult` codec — no second Hrana implementation to drift. Stale-keepalive
   and transient errors reconnect-and-retry like a real SDK (0 needed here).

## What it means

- **The single-process 1024 barrier was the driver, not fathom.** What the Python driver needed
  4 × 256 processes to reach, one Elixir container does alone, error-free. The driving-validity
  envelope for the Elixir driver is simply "one container, raise its nofile" — no per-process client
  cap, no multi-process orchestration.
- **Throughput plateaus ~3.8–4k tps on the one 12-vCPU VM**, CPU-bound, with the load partitioned
  evenly across all three nodes (spread ≤1.14× across the whole sweep). As always, the absolute
  txn/s is single-host-bound; the horizontal "millions" axis is the even per-node split, not the
  one-box aggregate. Still no intra-shard convoy — each tenant is its own single-writer file.
- **fathom/filo held every contract at every step**: zero server-side failures and zero client
  errors across 200,000 txns, 16→1024 tenants.

## Methodology / caveats

- **Not a controlled A/B vs the Python addendum.** This run used `per_client=100, accounts=200`; the
  2026-07-23 uniform sweep used different parameters (and larger account tables), so the ~3.8–4k tps
  here is **not** a head-to-head throughput win over the Python 4-proc 2,661 tps — it is its own
  datapoint in the same CPU-bound single-VM regime. The claim this run supports is about **driver
  shape and robustness** (one process, zero errors, no client cap), not a tps delta. A parameter-
  matched A/B is the follow-up if a tps comparison is wanted.
- **Relative rig.** One 12-vCPU colima VM hosts all three nodes + LB + MinIO over loopback; absolute
  numbers are relative, per the standing rig caveat.
- Reproduce: `./chaos.sh build-driver` then
  `TPC_NET=container TPC_DRIVER=elixir ./chaos.sh tpc-fleet "16,64,128,256" 100 200` and
  `… tpc-fleet "512,1024" 100 200`.

## Provenance

- `Filo.Client` (filo `39a909f`) — the Hrana client mirroring the server codec.
- `deploy/chaos/tpc_driver.exs` (fathom `7fbd211`) — the thin Elixir driver over `Filo.Client`.
- `chaos.sh` `TPC_DRIVER` / `TPC_NET` wiring (fathom `ae48c73`).
- `Dockerfile.driver` + `driver` compose service (fathom `74e6b22`); container `nofile` raise
  (fathom `25ff39c`).
