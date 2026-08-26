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

## Addendum — the full uniform-methodology sweep (2026-07-24, in-network, ≤256 clients/proc)

Rerun of every level with one methodology (in-network driver containers, multi-process
past 256), fresh namespaces per level:

| tenants | procs | txns | agg txn/s | p50 ms | p95 ms | errs |
|---:|---:|---:|---:|---:|---:|---|
| 6 | 1 | 900 | 1,132 | 4.9 | 7.4 | 0 |
| 12 | 1 | 1,800 | 1,143 | 10.2 | 13.8 | 0 |
| 24 | 1 | 3,600 | 1,165 | 19.9 | 28.0 | 0 |
| 64 | 1 | 9,600 | 1,138 | 54.1 | 78.5 | 0 |
| 128 | 1 | 19,200 | 1,114 | 110.3 | 161.9 | 0 |
| 256 | 1 | 31,371 | 69.9 ⚠ | 225.9 | 10,495 ⚠ | 7,029 (18.3%) ⚠ |
| 512 | 2 | 76,800 | 1,625 | 297.4 | 470.6 | 0 |
| 1024 | 4 | 153,008 | **2,661** | 238.6 | 455.5 | 592 (0.4%) |

- **The driver was the plateau all along**: aggregate rises with driver processes —
  ~1,140 (1 proc) → 1,625 (2) → **2,661 tps (4)** — so even four drivers haven't found
  the fleet's ceiling on this VM. Every single-driver number in every tpc-fleet report
  (including the post-LB-fix ~1,080 "CPU plateau") is a *driver* ceiling. The in-network
  single-proc plateau is ~1,140 (the host-forwarder path shaved it to ~1,080).
- **6→128 are textbook**: flat ~1,140 plateau, zero errors, p50 linear in concurrency.
- **⚠ The solo-256 in-network point is reproducibly pathological** (2/2 tonight: 69.9 tps
  with 18.3% stale-keepalive reconnects and 10s p95 stalls; a rerun timed out at 10 min) —
  while the same 256-client width runs clean inside the 512/2-proc and 1024/4-proc levels,
  and 256 solo ran clean on the HOST path earlier the same night (1,026 tps, 0 errs).
  Unattributed, and still open as of 2026-08-26. Suspects: the tighter in-network RTT pushing
  one GIL past its request-loop saturation point (128/proc stays well clear), rig state
  at that sweep position, or a scheduling interaction unique to one saturated python
  process. Until it's understood, the driving rule tightens to **≤128 clients per driver
  process** for canonical numbers.
- 1024's 0.4% transient reconnects (retried/skipped, zero server-side failures) vs 1.3%
  in the first 4×256 run — client keepalive churn, load-dependent, not protocol errors.
