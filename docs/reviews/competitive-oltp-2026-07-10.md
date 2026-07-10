# Fathom vs the field — same-hardware OLTP, and why we switched to `synchronous=FULL`

**2026-07-10, Apple M5 Max (18 cores), macOS 27.0.** Every number here was measured on this one
machine, so the hardware is apples-to-apples even though the architectures differ wildly. This is
a *feel*, not a certified result — relative numbers, quick runs, single host.

The honest framing up front: **fathom is not competing with Postgres or SQLite on single-database
throughput** — it's a multi-tenant proxy that gives you *millions of small isolated single-writer
SQLite databases*, cheap to open and fan out. The comparison below exists to place fathom's
per-DB cost in context and to justify a durability change, not to claim a win on one DB.

The one variable that dominates any TPC-B/pgbench comparison is **fsync policy**, so every system
is shown at both its non-durable and per-commit-durable setting.

## Same-hardware OLTP

| System | point read p50 | TPC-B txn p50 | TPC-B tps (1 writer/client) | write concurrency |
|---|---|---|---|---|
| **raw SQLite**, WAL/NORMAL (no per-commit fsync) | 2 µs | 23 µs | 29,639 | single-writer — no scaling on one DB |
| **raw SQLite**, WAL/FULL (fsync each commit) | 2 µs | 78 µs | 10,628 | single-writer |
| **Postgres 18.4**, `synchronous_commit=off` | 23 µs | 167 µs | 5,994 | MVCC → 13,905 at 8 clients |
| **Postgres 18.4**, `synchronous_commit=on` (durable) | 23 µs | 231 µs | 4,321 | MVCC → 8,935 at 8 clients |
| **fathom** loopback WS, NORMAL | 98 µs (`hrana_rt`) | ~400 µs | `node_tps` 4,167 (16-shard fan-out) | per-shard single-writer; scales by **shards** |
| **fathom** loopback WS, **FULL** | 98 µs | ~450 µs | `node_tps` 4,140 | " |
| **fathom** remote via real LB (chaos rig) | 559 µs (RTT) | ~4 ms | — | " |

Postgres point read = pgbench `select-only`; 8-client Postgres select-only hits 151k tps / 53 µs.

### How to read it

- **Fathom's engine *is* SQLite**, so its per-DB ceiling is SQLite's minus a proxy tax. Raw SQLite
  is untouchable in-process (2 µs reads, 23 µs txns) because there's no IPC at all.
- **The "fathom tax" is a client-server crossing, not a new bottleneck.** Fathom adds ~371 µs/txn
  and takes a read from 2 µs → 98 µs, landing *over the wire* in the same order of magnitude as
  Postgres *over the wire*. Both pay to leave the process. Fathom's Hrana-JSON-over-WebSocket /
  Bandit / BEAM path is ~4× heavier than Postgres's binary libpq on a bare read (98 vs 23 µs) —
  the price of a protocol any unmodified libSQL client speaks. On one DB, **fathom ≈ "SQLite with
  Postgres-like wire latency."** It is not trying to beat Postgres per-DB and does not.
- **The real difference is the concurrency model.** Postgres scales *writes on one DB* via MVCC
  (4.3k → 8.9k tps, 1 → 8 clients). SQLite and fathom cannot — one file, one writer. Fathom scales
  the other way: **millions of independent single-writer shards**, each ~2.6 ms to cold-open and
  ~196 KiB to hold. Postgres falls over well before millions of DBs/schemas; raw SQLite has no
  orchestration at all. That's the axis fathom actually wins on, and it's not on this table (see
  `mix fathom.scale`). The chatty-write convoy *within* a shard (`docs/reviews/tpc-run-2026-07-10.md`)
  is the flip side of the same coin: the answer is more shards, not more threads per shard.

## The durability finding → fathom now runs `synchronous=FULL`

Flipping fathom's SQLite from `NORMAL` to `FULL` (fsync the WAL on every commit) barely moved
anything:

| fathom loopback | NORMAL | **FULL** |
|---|---|---|
| `hrana_rt` (read) | 98 µs | 98 µs |
| `tpcb_node_tps` (16-shard) | 4,167 | 4,140 (−0.6%) |
| TPC-C tpmC (W=1 / W=2, spec scale) | 11,263 / 11,618 | 11,168 / 11,736 (±1%) |
| New-Order p50 | ~3.7 ms | ~3.7 ms |

**Durability is essentially free for fathom** — every delta is within run-to-run noise — while the
same flip costs **raw SQLite 2.8×** (29.6k → 10.6k) and Postgres ~1.4×. Two architectural reasons:

1. **Fathom is wire/executor-bound, not commit-bound.** A single SQLite writer does ~30k txn/s, so
   its throughput is dominated by the commit path and fsync hits it hard. Fathom's per-txn cost is
   dominated by the ~371 µs Hrana/BEAM tax — 7× the ~55 µs fsync — so the engine's durability mode
   is nearly invisible to the aggregate.
2. **Sharding parallelizes fsync.** `node_tps` fans out over 16 shards = 16 independent WAL files
   fsyncing *concurrently* to the NVMe. A single database has one WAL and one fsync at a time.

**Decision:** `Fathom.Shard.Connection.open/1` now sets `PRAGMA synchronous=FULL`. This gives
**per-commit local durability** — a committed write survives a node crash — tightening the local
RPO to zero *below* the S3 flush interval, for a throughput cost the measurements put at noise. A
single-DB engine pays 2–3× for the same guarantee; the many-shards design hands it to us nearly
free. (Cross-node durability is still the S3 lease + flush; this strengthens the *local* story.)

## ClickBench — the wrong axis for fathom

ClickBench is **OLAP**: 43 analytical scans/aggregations over one ~14 GB, 100M-row table. Fathom
is multi-tenant **OLTP** — the opposite workload. Running it on fathom would re-measure
row-oriented SQLite's known OLAP weakness (columnar engines like ClickHouse/DuckDB beat row stores
by 1–3 orders of magnitude there) per shard, plus the wire tax — a foregone conclusion, and a
single-big-table workload is the anti-fathom shard. **Adding ClickBench is low value.** If
per-tenant analytics ever matters, the question is a columnar path per shard (DuckDB attach), not
SQLite-on-ClickBench. (No leaderboard figures cited here — not measured on this box.)

## Turso / libSQL — the peer that actually matters (not run)

libSQL is a SQLite fork + a Hrana server layer, and fathom literally speaks that Hrana protocol
(via Filo). So per-DB the two should land in the same place; the real comparison is the
orchestration/placement/density layer, not single-DB TPS. A head-to-head on a multi-tenant
workload is the competitive benchmark worth building next. Structural reasoning only — not run here.

## Caveats

- **macOS fsync semantics:** SQLite uses `fsync()`, not `F_FULLFSYNC`, by default — on macOS that
  flushes to the OS, not necessarily the drive platter. Both the raw-SQLite and fathom runs used
  the same semantics, so the *relative* comparison holds, but "durable" here is OS-durable. Postgres
  does force platter flushes, so its durable column is a stronger guarantee.
- Single host, relative numbers, quick runs (pgbench 8 s, raw-SQLite 5k txns, fathom loopback
  default samples). A feel, not a certified TPC/pgbench result.
- Fathom's remote/rig numbers are single-host too (loopback client→LB); the realism there is the
  real LB + prod node + S3 storage, not a WAN RTT.

## Reproduce

```bash
# raw SQLite (fathom's engine): TPC-B at NORMAL + FULL, point read — python stdlib sqlite3
# Postgres pgbench (Homebrew postgresql@18):
PGB=/opt/homebrew/Cellar/postgresql@18/*/bin
$PGB/createdb -h localhost pgbench_bench && $PGB/pgbench -i -s 1 pgbench_bench
$PGB/pgbench -T 8 -c 1 -M prepared pgbench_bench                          # durable
PGOPTIONS="-c synchronous_commit=off" $PGB/pgbench -T 8 -c 8 -j 4 -M prepared pgbench_bench
# fathom loopback: MIX_ENV=test mix fathom.wire_bench   /   mix fathom.tpcc --tpcc-scale 1.0
# fathom remote (rig): cd deploy/chaos && ./chaos.sh up && ./chaos.sh tpcb / tpcc
```
