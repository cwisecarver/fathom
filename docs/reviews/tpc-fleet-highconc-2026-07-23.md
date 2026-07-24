# tpc-fleet at 512 and 1024 tenants — 2026-07-23 (finding the real ceilings)

Extending the post-LB-502-fix sweep past 256 surfaced three DIFFERENT ceilings, none of
them fathom's. Each was root-caused and either fixed or worked around; the fleet itself
never failed.

## Results

| tenants | driver shape | txns | agg txn/s | p50 | errs | server state |
|---:|---|---:|---:|---:|---|---|
| 512 | 1 host process | 76,800 | 806.0 | 611.8 ms | 0 | clean |
| 1024 | 4 × 256-client containers, in-network | 151,609 (98.7%) | **1,408.8** | ~245 ms/driver | 1,991 (1.3%, transient reconnects) | all nodes healthy, no upstream failures |

## The three ceilings (in discovery order)

1. **colima's host port-forwarder dies at ~1k concurrent connections** — the first 1024
   attempt killed the entire Docker daemon (twice, deterministically); every later connect
   was refused because nothing was listening. Workaround: drive from a container ON the
   rig network (`docker run --network fathom-chaos_default … --lb http://lb:80`), which is
   also how the 30k-shard density runs always worked. The 512 host-path number (806 tps)
   is shaded by forwarder strain and pre-death load; treat in-network numbers as canonical
   at high concurrency.
2. **Raw `http.client` breaks on stale keepalive sockets** — at high thread counts the
   GIL-staggered cadence lets a connection idle past the server-side keepalive closes
   (nginx 75 s client-side / Bandit 60 s), and unlike every real SDK the driver surfaced
   the silent FIN as an uncaught `BrokenPipeError`, killing the worker (680 of 1,024
   died; zero Hrana errors). Fixed in `tpc_driver.py`: stale-socket errors now
   reconnect-and-retry exactly like `HranaError` (with a 3-attempt seed), mirroring real
   SDK behavior.
3. **One Python process can't drive 1024 clients** — even patched, a single-process
   1024-thread run thrashed (13,650 retries, 11 s p99, 246 tps): the GIL turns wide
   concurrency into idle-then-reconnect churn. The valid shape is multiple driver
   processes: 4 × 256 concurrently (the known-good per-process width) drove all 1,024
   tenants at 1,408.8 tps aggregate with 1.3% transient reconnects and a ~245 ms p50 per
   driver.

## What it means

- **The fleet clears ~1.4k tps at 1,024 concurrent single-writer tenants** on the one
  12-vCPU VM, all nodes healthy end-to-end — and therefore the earlier ~1,080-tps
  "CPU plateau" (6→256 sweep) was partly the single driver's GIL, not the fleet. The
  measured plateau is a floor, not the ceiling.
- fathom/filo held every contract at every step tonight: zero server-side failures, the
  1.3% error rate is entirely client keepalive churn (retried, counted, skipped).
- Driver validity envelope going forward: ≤256 clients per driver process; use N
  processes for wider sweeps; always drive in-network past ~512 total connections.
