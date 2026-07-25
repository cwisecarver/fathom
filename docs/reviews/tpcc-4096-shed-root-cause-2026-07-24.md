# Single-node TPC-C 4096 shed edge — root cause (2026-07-24)

The `tpcc-fleet` single-node baseline sheds ~11% of connections on ~40% of runs at 4096 tenants
(`0, 0, 0, 8.7k, 9.2k` errors across five runs — `tpcc-elixir-driver-2026-07-24.md`). Prior work
established it is **transaction complexity, not data footprint** (a same-footprint TPC-B stays 0
errors) but left the resource unnamed, guessing "accept backlog? fds? scheduler?" This names it.

**Answer, up front:** the shed is **client request timeouts under whole-VM saturation**, not a fathom
node resource. It is **not** fds, **not** the accept backlog (only a minor secondary symptom), and
**not** a BEAM process/port limit — all three are directly ruled out below. The magnitude's huge,
non-monotonic variance (95 → 35,789 errors across six runs) confirms it is a noisy single-VM
measurement edge, not an architectural wall.

## Method

Every driver catch site used to swallow the reason (`rescue _ -> e + 1`), so a shed run never said
why. Added `ErrTally` to `deploy/chaos/tpc_driver.exs` (fathom `1cd94bd`): a lock-free ETS histogram
that buckets each shed connect/txn by the resource its reason names (timeout / econnrefused / emfile /
system_limit / closed / ...), printed to stderr + the JSON. Then a per-run harness: restart fathom1
for a clean node, snapshot `/proc/net/netstat` accept-queue counters, drive 4096 tenants at
`fathom1:8080` directly in-network (a 4096-wide cold-open storm on one node), sample the beam fd count
and per-container CPU throughout, snapshot the counters after, scan node logs. Run at
`per_client=20, scale=0.005` (the review's parameters).

## Data — six runs at 4096 single-node

| run | errs | ListenOverflows Δ | max_fd (/65536) | dominant reason |
|---:|---:|---:|---:|---|
| 1 (stale driver) | 95 | +565 | 20,333 | — (image predated the tally) |
| 2 | 11,740 | 0 | 19,022 | `timeout=16,534` |
| 3 | 35,789 | 0 | 17,230 | `timeout=21,413` |
| 4 | 5,830 | +135 | 20,781 | `timeout=15,079` |
| 5 | 6,343 | 0 | 21,311 | `timeout=17,587` |
| 6 | 28,148 | +1,315 | 21,839 | `timeout=20,007` |

Every instrumented run: **`timeout` dominates (15k–21k events)**; `ListenOverflows` is 0–1,315,
always an order of magnitude below the timeout count; `max_fd` is always ~17–21k against a 65,536 soft
limit. The secondary "other" buckets are mid-transaction `tpcc exec failed … %filo.error` on
BEGIN/SELECT/UPDATE/INSERT/COMMIT — streams breaking between statements under load.

## What is ruled out (reliable instruments)

- **fds — no.** `max_fd` 17–21k is 3× under the 65,536 soft limit on every run, and node logs show
  **0** `emfile`/`enfile`. A direct count, not an inference.
- **BEAM process/port/atom limits — no.** **0** `system_limit` in fathom1's logs across every severe
  run (processes are `unlimited` in the container). The node never crashed.
- **The S3/MinIO cold-open pool — no.** 0 Finch pool-timeout or pool errors in the logs.
- **The accept backlog — secondary, not primary.** The Hrana listener (`lib/fathom/application.ex:500`)
  uses default ThousandIsland settings (100 acceptors, ~1024-deep backlog under `somaxconn=4096`), and
  it *does* overflow in the most saturated runs (up to 1,315). But `ListenOverflows` is always ≪ the
  timeout count, and the mildest shed (run 1: 95 errors) had the *most* overflow (565) with the fewest
  errors — retries absorbed it. Overflow is a co-symptom of saturation, not the shed's cause.

## What it actually is

The shed is **fixed timeouts crossed by latency under load**:

- **Primary — the client's 15 s per-request timeout** (`Filo.Client`, `client.ex:90`). Driving 4096
  tenants at one node means 4096 simultaneous cold-opens, each a MinIO pull + exqlite WAL open + a
  9-table recursive-CTE seed. Under that storm a request (the seed, or a txn statement) does not get a
  response within 15 s, so the client times out. The tenants whose 8 establish-retries all time out
  become the errors.
- **Secondary — the server's 10 s stream idle_timeout** (`Filo.Stream`, `stream.ex:37`). Under the
  same latency a held stream expires between statements, so the next statement fails on a closed
  stream — the mid-txn `exec failed` buckets.

This is exactly why it is **complexity, not footprint**: the light TPC-B seed at the same row count
never pushes cold-open latency past 15 s, so it stays 0 errors. And why it is **intermittent and
wildly variable**: the shed is a latency-vs-timeout race whose outcome depends on connect-burst
alignment and cumulative rig state (repeated runs pile up MinIO objects, slowing cold-opens — our
magnitudes run higher than the review's original 8.7k because the rig was dirtier).

## What is NOT cleanly measurable on this rig (a caution)

The deeper "CPU vs disk-I/O" split behind the latency is **not** trustworthy here, so this report does
not claim it:

- `docker stats` CPU% is unreliable on this colima setup — run 6 reported fathom1 at **2728%** on a
  **12-vCPU** VM, a physically impossible sampling artifact. A single clean live snapshot showed
  fathom1 ~5.5 cores + driver ~1.9 cores ≈ 8 of 12 in use (CPU headroom), and in-container
  `/proc/loadavg` read ~20 — but loadavg is **VM-wide** (not namespaced), so it counts all three
  nodes + MinIO + the co-located driver, not fathom1.
- Colima caps the VM at 12 vCPUs (the Mac host has more), and the **4096-client driver runs on the
  same 12 vCPUs as the node under test**. So an unknown part of the "saturation" is the rig driving
  itself — the single-node number is contaminated by co-location, independent of the CPU artifact.

The honest statement is: the node is not hitting a hard resource cap (fds/accept/BEAM all ruled out);
it is timing out under aggregate saturation of one shared 12-vCPU VM, and the exact CPU/IO breakdown
is below this rig's measurement floor.

## Production relevance — none at the node level

The LB consistent-hashes tenants across nodes, so no node ever takes a 4096-wide cold-open burst, and
production never co-locates the driver. Every `tpcc-fleet` sweep is 0 errors at every step
(`tpc-fleet-2026-07-23.md`). This edge is the far end of the single-node headroom, reached only by an
artificial worst case on a shared rig VM.

## Recommendations

1. **Truth of record (no code):** document as a single-VM-rig measurement edge — client/server
   timeouts under cold-open-storm saturation when one 12-vCPU VM hosts the node + MinIO + a co-located
   4096-client driver. Not a fathom limit.
2. **For cleaner single-node numbers (optional, rig-side):** isolate the driver onto its own VM, reset
   the MinIO bucket between runs, and/or raise `Filo.Client`'s 15 s timeout for the seed burst. These
   change the measurement, not fathom.
3. **Cheap defense-in-depth (optional, real):** the one-run accept overflow shows the default ~1024
   listener backlog can overflow under a synchronized connect burst; widening it via
   `thousand_island_options: [transport_options: [backlog: 4096], num_acceptors: N]` on the Bandit
   spec (`application.ex:500`) is a small hardening, but it is not the shed's cause.

## Provenance

- Driver err-reason tally: fathom `1cd94bd`. Node-side evidence: `/proc/net/netstat`
  ListenOverflows/ListenDrops deltas, `/proc/1/fd`, `compose logs fathom1`, `docker stats` (unreliable
  — see caution) — captured by a per-run harness on the chaos rig (3 nodes + LB + MinIO on one
  12-vCPU colima VM).
- Reproduce: rebuild the driver image (`chaos.sh build-driver`), then per run: `compose restart
  fathom1`; `compose run --rm driver tpcc-fleet --lb http://fathom1:8080 --domain fathom.test --shard
  tcedgeN --txns 81920 --clients 4096 --scale 0.005`; diff `/proc/net/netstat` before/after and read
  the driver's `err_reasons`.
