# Fathom vs libSQL server (Turso `sqld`) — head-to-head

**2026-07-10, Apple M5 Max (18 cores), macOS 27.0.** The peer that actually matters. libSQL's
server (`sqld` / `libsql-server`) speaks the **same Hrana protocol** fathom does (via Filo), so
`deploy/chaos/tpc_driver.py` drives *both, unchanged* — only the URL differs. That isolates the
one variable: **the server implementation**.

- **fathom** — Elixir / BEAM / Bandit + Filo, S3(MinIO)-backed, multi-tenant (a rig node on its
  direct port, LB bypassed). Hit as one shard.
- **libsql-server** — `sqld` 0.24.33, Rust, local-disk, single-database. Hit as its default DB.

Same engine (SQLite/libSQL), same wire (Hrana v2 HTTP `/v2/pipeline`), same workload, same box →
a fair **per-database** comparison. Run it: `deploy/chaos/turso_headtohead.sh` (fathom rig must be
up; sqld is started fresh each run).

## Result (single DB, single client, steady-state)

| metric | fathom | libsql-server (`sqld`) |
|---|---:|---:|
| RTT p50 (µs) | **418** | 434 |
| RTT p99 (µs) | 682 | **538** |
| TPC-B p50 (µs) | **2808** | 2979 |
| TPC-B p99 (µs) | **3659** | 4121 |
| TPC-B tps (1 client) | **344** | 333 |
| TPC-C tpmC (1 thread) | **2138** | 2017 |
| TPC-C New-Order p50 (µs) | **21306** | 22422 |
| TPC-C Payment p50 (µs) | **4140** | 4591 |

**They are on par** — within a few percent on every metric, fathom marginally ahead on nearly all
of them and sqld ahead on RTT tail. A second manual run agreed (fathom RTT 393 / sqld 446, TPC-B
2724 / 2867, tpmC 1993 / 2018), so the "roughly even, fathom a hair faster on this box" read is
stable, not a single-run fluke.

## What this says

**Fathom's Hrana server holds its own against the reference Rust implementation.** The Elixir /
BEAM / Filo / Bandit path matches (slightly beats, here) `sqld` on the same protocol and workload —
and it does so *while carrying machinery sqld doesn't*: the S3 lease/epoch fence, the per-shard
coordinator, the multi-tenant routing. That overhead is off the per-request hot path (cold-open +
lease happen once; the coordinator is a supervisor, not in the query path), so steady-state serving
is competitive with a bare single-DB server. The per-txn cost on both is dominated by the same
thing — the chatty Hrana round-trips (7 for TPC-B, ~30–60 for New-Order) × the ~0.4 ms wire RTT —
so neither server's language is the bottleneck at this workload; the protocol is.

**The comparison above is deliberately per-DB, which is sqld's home turf, not fathom's.** fathom's
reason to exist — many small isolated single-writer shards, cheap cold-open, fan-out — is layered
*on top* of this per-DB serving, and the earlier findings show that layer is nearly free
(`docs/reviews/competitive-oltp-2026-07-10.md`: durability free, orchestration off the hot path).
So on one DB the honest framing is: **fathom gives you libSQL-server-class per-DB serving, plus the
multi-tenant orchestration, at no measured per-DB cost.** The multi-tenant comparison below is the
one that actually stresses fathom's design point.

## Multi-tenant — fathom shards vs sqld namespaces (the design point)

libSQL server supports many databases via **namespaces** (`--enable-namespaces`), routed by the
*same* Host-subdomain mechanism fathom uses — so the same driver drives both: N tenant DBs, one
writer each, running TPC-B concurrently. Load is **N independent processes** (one per tenant), not
threads — the Python driver is GIL-bound under many threads and would measure the client, not the
servers' fan-out (`turso_multitenant.sh`). Aggregate tps = sum of the per-tenant process tps;
latency = the median per-tenant p50/p99.

| N tenants | fathom agg tps | sqld agg tps | fathom p50 | sqld p50 | fathom p99 | sqld p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 8 | 1324 | 1270 | 6.0 ms | 6.3 ms | 7.6 ms | 8.1 ms |
| 32 | **2993** | 1735 | 10.7 ms | 19.1 ms | 13.7 ms | 23.4 ms |
| 64 | **3654** | 1660 | 17.7 ms | 38.9 ms | 23.0 ms | 51.4 ms |

**At N=8 they're even; as tenants grow, fathom pulls away.** Aggregate throughput *scales* on
fathom (1324 → 2993 → 3654) but *plateaus then declines* on sqld (1270 → 1735 → 1660), and by N=64
fathom's per-tenant latency is roughly **half** sqld's (17.7 vs 38.9 ms p50). This is fathom's
thesis on the board: it fans out across many independent single-writer shards — each its own BEAM
process, spread over the cores by the scheduler — while sqld's namespaces share more server-side
machinery (a shared runtime, and a bounded `--max-active-namespaces` active set beyond which
namespaces churn cold) and hit a contention wall fathom doesn't. **fathom scales *out* with tenant
count where a single-server design scales *up* and stalls.**

This is still only *fan-out throughput at modest N*. The full **density** axis — *millions* of
shards, where fathom cold-opens on demand (`mix fathom.scale` held 50k+ linearly to the fd ceiling)
and a single sqld process cannot keep that many namespaces active — is the larger structural win
and a separate measurement.

## Caveats (so this isn't oversold)

- **Single host, single DB, single client, steady-state (warm).** Relative numbers, ~300–500
  samples per point, one workload. A feel, not a certified benchmark.
- **Durability differs.** The fathom side is the chaos-rig prod-release image (an earlier commit;
  the Hrana serving path is unchanged, but it predates the `synchronous=FULL` switch, so it runs
  WAL/NORMAL) and is S3-backed; sqld runs libSQL's defaults on local disk. Per the durability
  finding, FULL vs NORMAL is within noise for fathom, so this doesn't move the conclusion — but the
  two aren't identically configured for durability.
- **Both containerized under colima** (same port-forward overhead on each side, so symmetric), but
  neither is a bare-metal number.
- **`sqld` is single-database**, so the runner starts it fresh each run (its accumulated state /
  a wedged connection otherwise hangs a later pass — the reason `turso_headtohead.sh` recreates the
  container). fathom stays robust on a fixed shard (idempotent seed + busy_timeout + coordinator).
- **Multi-tenant load is N independent processes** (one per tenant), aggregate tps = the sum of
  per-process tps and latency = the median per-tenant p50/p99 — an approximate aggregate, not a
  single synchronized window. Threads were GIL-bound (measuring the client, not the servers), which
  is why the process model is used. sqld's default `--max-active-namespaces` may contribute to its
  N=64 plateau (churning namespaces cold) — that overlaps the density axis and is not isolated here.

## Reproduce

```bash
cd deploy/chaos
./chaos.sh up                       # fathom rig (3 nodes + LB + MinIO)
./turso_headtohead.sh               # per-DB: fresh sqld, drives both, prints the table
# args: ./turso_headtohead.sh [rtt_samples tpcb_txns tpcc_txns]
./turso_multitenant.sh "8 32 64"    # multi-tenant: fresh sqld w/ namespaces, N shards vs N namespaces
```
