# tpc-fleet with the Elixir (Filo.Client) driver — 2026-07-24 (4096 tenants from ONE process)

The high-concurrency findings in [`tpc-fleet-highconc-2026-07-23.md`](tpc-fleet-highconc-2026-07-23.md)
were all about the **Python driver's** limits, never fathom's: colima's host forwarder dies
~1k conns (→ drive in-network), raw `http.client` breaks on stale keepalives, and one GIL-bound
process can't drive past ~128 clients (the solo-256 point was reproducibly pathological), so 1024
needed **4 × 256-client processes** for 2,661 tps.

This run swaps the driver for the new Elixir one — `Fathom`'s `tpc_driver.exs` over `Filo.Client`,
one lightweight BEAM process per client — packaged as an in-network container. It clears every one
of those driver ceilings: **6→4096 concurrent tenants from a single container, zero errors at every
step**, including the 256 point that broke the solo Python process. (Getting past 1024 took one
driver bugfix — see "The 2048/4096 crash" below; the fleet was never the limit.)

## Results (in-network, one `fathom-chaos-driver` container, 100 txns/tenant, 200 accounts/tenant)

| tenants | txns | agg txn/s | p50 ms | p95 ms | p99 ms | errs |
|---:|---:|---:|---:|---:|---:|---|
| 16 | 1,600 | 3,077 | 4.8 | 6.0 | 6.6 | 0 |
| 64 | 6,400 | 4,222 | 11.6 | 19.4 | 23.2 | 0 |
| 128 | 12,800 | 3,934 | 26.4 | 39.3 | 44.3 | 0 |
| 256 | 25,600 | 3,992 | 39.4 | 59.0 | 65.7 | 0 |
| 512 | 51,200 | 3,959 | 47.5 | 76.4 | 89.4 | 0 |
| 1024 | 102,400 | 3,828 | 53.3 | 104.3 | 127.7 | 0 |
| 2048 | 204,800 | 3,749 | 88.4 | 152.5 | 183.7 | 0 |
| **4096** | **409,600** | **3,414** | **71.4** | **137.5** | **171.8** | **0** |

Cross-LB warm `SELECT 1` RTT p50 ~200 µs (the per-statement network floor). Per-node split over the
1024→4096 legs: 2,233 / 2,366 / 2,569 shards (fathom1/2/3), 1.58M / 1.68M / 1.82M queries — 7,168
tenant shards, **5.08M queries, spread 1.15×** (the consistent hash stays even out to 4096 keys).

## Parameter-matched A/B vs the Python driver

The controlled comparison: **both** drivers, **identical parameters** (`per_client=100, accounts=200`),
both on the host over the same LB, one process each, on **distinct fresh shard namespaces**,
interleaved per level so rig state is matched for each pair. Capped at 256 — the Python driver's
clean single-process envelope.

| clients | python txn/s | python p50 | python errs | elixir txn/s | elixir p50 | elixir errs |
|---:|---:|---:|---:|---:|---:|---:|
| 16 | 1,069 | 14.5 ms | 0 | **1,712** | 8.8 ms | 0 |
| 64 | 1,050 | 58.1 ms | 0 | **2,904** | 18.2 ms | 0 |
| 128 | 117 ⚠ | 114.0 ms | 632 ⚠ | **2,954** | 30.9 ms | 0 |
| 256 | 202 ⚠ | 235.3 ms | 1,213 ⚠ | **3,063** | 65.5 ms | 0 |

- **Elixir wins on throughput even inside Python's clean range**: 1.6× at 16 clients, 2.8× at 64 —
  both error-free. The BEAM drives more txn/s per process than GIL-serialized Python threads, and its
  p50 is 2–3× lower at matched load.
- **A single Python process collapses at ≥128** (the known ≤128-clients/proc ceiling): 117 tps with
  632 transient reconnects at 128, 202 tps / 1,213 at 256 — GIL thrash + stale-keepalive churn. Elixir
  at the same points is 2,954 / 3,063 tps with **zero errors**.
- So the win is **both** dimensions — higher throughput per process *and* robustness. One Elixir
  process scales clean through the range where one Python process falls apart, which is exactly why
  Python needed 4 × 256 processes for 1024 while Elixir needs one for 4096.

(These host single-process numbers run a touch below the in-network container sweep above — same
driver, the host-forwarder path — but the head-to-head shape is identical. Reproduce by running both
`tpcb` modes at matched `--clients`/`--txns`/`--accounts` on distinct `--shard` prefixes.)

## What made it work

1. **One BEAM process per client, no GIL.** The BEAM holds thousands of lightweight processes each
   owning a Mint socket at ~KB of state — no thread-per-client memory, no GIL serialization. The
   ≤128-clients-per-process rule and the multi-process requirement were both Python artifacts; the
   Elixir driver has neither. The solo-256 point that was reproducibly pathological for one Python
   process (18.3% stale-keepalive churn, 10 s p95 stalls) ran clean here.
2. **In-network container.** Driving from a container on the compose network (LB as `http://lb`)
   sidesteps colima's host port-forwarder, which dies ~1k conns. `TPC_NET=container` shells out to
   `docker compose run --rm driver …`.
3. **Raised container `nofile`.** One held socket per client, so at 4096 clients the driver needs far
   more than the default 1024 fds. The `driver` service sets nofile 65536/524288 (matching the
   nodes); without it a multi-thousand-client run would fault at the fd limit.
4. **Connect stagger + retry (the fix that unlocked 2048/4096).** Each worker jitters its initial
   connect over ~N ms and retries connect+seed with backoff — see below.
5. **Shared wire codec.** The driver rides `Filo.Client`, which reuses the server's own
   `Filo.Value` / `Filo.StmtResult` codec — no second Hrana implementation to drift.

## The 2048/4096 crash — a driver bug, root-caused and fixed

The first 2048/4096 attempt returned **blank throughput** while the per-node counters showed the
fleet had processed **2.27M queries, evenly split** — the classic signature of a *driver-side*
crash, not a fleet failure. The sweep swallowed driver stderr (`2>/dev/null`); rerunning the exact
config with stderr visible gave the real exception:

```
** (RuntimeError) tpc exec failed (CREATE TABLE IF NOT EXISTS history ...):
   {:transport, %Mint.HTTPError{reason: :closed}}
   tpc_driver.exs: Tpc.tpcb_seed/2 → Tpc.seed_with_retry/3 → Tpc.worker/2
```

- **It was during *seeding*, not the txn loop, and not an fd limit** (`nofile` is 65536 under
  `compose run`, confirmed). At ≥2048 workers the connect+seed **thundering herd** — every worker
  connecting in lockstep — plus the driver sharing the one 12-vCPU VM with all three nodes + nginx,
  meant a worker got scheduled too slowly *between* its sequential seed statements; the server closed
  the idle-between-statements keepalive, and the next `CREATE TABLE` failed `:closed`.
- **The old driver couldn't cope**: `seed_with_retry` tried only 3 times (all hitting the same herd),
  then **reraised** → the worker task crashed → `mode_tpcb`'s `{:ok, r}`-only `async_stream` map hit
  an `{:exit, _}` it didn't handle → the *whole run* crashed before printing any JSON.
- **Fix** (fathom `f2f02e5`): stagger each worker's initial connect over ~N ms (a real fleet never
  connects in lockstep — the key fix), retry connect+seed together with backoff (8 attempts; the seed
  statements are idempotent so a fresh-connection re-seed is safe), make the worker return an errored
  result instead of raising, and make `mode_tpcb` tolerate crashed workers / empty results.
- **After the fix**: 1024/2048/4096 all complete with **0 errors** (table above). The stagger alone
  removed the closes — it was a thundering-herd bug in the driver, not starvation and not a fathom
  limit.

## What it means

- **Every "ceiling" in this whole line of work has been the driver, not fathom.** Python needed
  4 × 256 processes for 1024; the Elixir driver does 4096 from one container. The two crashes en
  route (the Python solo-256 pathology, the Elixir 2048 herd) were both driver bugs — the fleet
  processed every query at every attempt, zero server-side failures, ~5M queries at the top.
- **Throughput plateaus ~3.4–4k tps on the one 12-vCPU VM**, CPU-bound, load partitioned evenly
  across all three nodes (spread ≤1.15× the whole way to 4096). Absolute txn/s is single-host-bound;
  the horizontal "millions" axis is the even per-node split, not the one-box aggregate. Still no
  intra-shard convoy — each tenant is its own single-writer file.
- **The driving envelope for the Elixir driver**: one in-network container, raised `nofile`, staggered
  connects — no per-process client cap, no multi-process orchestration, out to at least 4096.

## Single-node baseline (light tenants pack on one node)

Controlled same-session A/B — the same tenant counts driven through the LB (3 nodes) vs direct at one
node (`fathom1:8080`, holding *every* tenant), per_client=50:

| tenants | fleet txn/s | fleet errs | 1-node txn/s | 1-node errs | fleet/1-node |
|---:|---:|---:|---:|---:|---:|
| 64 | 4,165 | 0 | 4,038 | 0 | 1.03× |
| 256 | 3,844 | 0 | 3,555 | 0 | 1.08× |
| 1024 | 3,834 | 0 | 3,535 | 0 | 1.08× |
| 4096 | 3,137 | 0 | 2,600 | 0 | 1.21× |

**TPC-B stays clean on one node all the way to 4,096** (0 errors, throughput edge only 1.03→1.21×) —
the exact opposite of the **TPC-C** single-node baseline, where 4,096 tenants on one node broke with
**8,700 errors** (`docs/reviews/tpcc-elixir-driver-2026-07-24.md`). So the per-node wall is a function
of **per-tenant weight**, not tenant count: TPC-B is light (7 statements/txn, tiny seed, quick-released
writes) so one node packs 4,096 fine; TPC-C is heavy (~30 statements/txn, a full 9-table seed, longer-
held write streams) so the same count exhausts one node's connections/fds/scheduler. On one VM the
fleet's *throughput* edge is always modest (CPU-shared); its *capacity* benefit — no node hitting the
per-node ceiling — only bites for heavy tenants. Raw ~N× throughput still needs N machines.

## Methodology / caveats

- **The head-to-head is now controlled** — see the A/B section above: matched params, same host, one
  process each, fresh namespaces. Elixir is 1.6–2.8× faster inside Python's clean range and error-free
  where a single Python process collapses (≥128 clients). Note the in-network *sweep* numbers
  (~3.4–4k tps) are still not directly comparable to the Python 4-proc 2,661 (different params and
  network position); the controlled comparison is the A/B table, not the sweep.
- **Single-host contention is real but not what crashed it.** The driver co-located with the nodes on
  one VM is why the herd could induce closes at all; on separate hosts (as in prod, where the driver
  is not the DB) the margin is wider. The fix makes the driver robust regardless.
- **Relative rig.** One 12-vCPU colima VM hosts all three nodes + LB + MinIO over loopback; absolute
  numbers are relative, per the standing rig caveat.
- Reproduce: `./chaos.sh build-driver` then
  `TPC_NET=container TPC_DRIVER=elixir ./chaos.sh tpc-fleet "16,64,128,256" 100 200`,
  `… "512,1024" 100 200`, and `… "1024,2048,4096" 100 200`.

## Provenance

- `Filo.Client` (filo `39a909f`) — the Hrana client mirroring the server codec.
- `deploy/chaos/tpc_driver.exs` (fathom `7fbd211`) — the thin Elixir driver over `Filo.Client`;
  hardened for 2048/4096 (stagger + retry + crash-proof) in fathom `f2f02e5`.
- `chaos.sh` `TPC_DRIVER` / `TPC_NET` wiring (fathom `ae48c73`).
- `Dockerfile.driver` + `driver` compose service (fathom `74e6b22`); container `nofile` raise
  (fathom `25ff39c`).
