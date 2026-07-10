# Fathom — TPC-B + TPC-C Benchmark Plan

> Status: **Plan only (2026-07-09, rev. 2). Not implemented.** This is the
> implementation-ready design for adding *both* a TPC-B-derived and a TPC-C-derived
> benchmark to fathom, additive to the existing harness (`docs/benchmark-plan.md`,
> `Fathom.Bench`, `mix fathom.bench`, the `Fathom.Bench.Gate` regression gate,
> `Fathom.Scale`). The decision to do both is already made — this plan does not
> re-litigate "which one," and TPC-H stays rejected (an analytical/OLAP workload says
> nothing about fathom's per-tenant OLTP write path). Nothing here changes code; it
> specifies what to build, in what order, and what each number does and does not mean.
>
> **Rev. 2 — every bench crosses the Hrana client wire.** The lead's directive: no TPC
> bench measures the in-process `ShardExecutor` path. Instead:
> - **GATE / CI benches** drive an **in-process loopback Hrana client** against Filo's
>   HTTP listener bound on `127.0.0.1` — so the measurement crosses the *full* wire
>   (JSON framing + `Filo.Value` encode/decode, baton-pinned stream, `Filo.Plug`
>   routing, the `Filo.Executor` callback, `ShardExecutor` → `Connection` → engine)
>   while staying single-host and reproducible enough for the ≥20% commit gate.
> - A separate **PERIODIC chaos-rig run** drives a **real remote libSQL client over the
>   network** for the true cross-network RTT (the realism headline).
>
> Both cross the wire; loopback is gate-viable, the rig is realism. This revision
> reworks the harness, the gated metric definition, `hrana_rt_us`, the drivers, and the
> metrics/gate tables to reflect that.

## Why add TPC at all — the write-path gap

The existing harness (`docs/benchmark-plan.md`) measures fathom's real thesis: cost is
dominated by **per-shard cold-open** and **fan-out density** across *millions of small
shards*, not single-DB throughput. Its four gated metrics (`cold_open_p50_us`,
`dir_resolve_p50_us`, `copy_rows_per_s`, `fanout_kb_per_shard`) plus the opt-in S3 family
cover open/resolve/copy/memory — but every one of them drives **read-only** SQL
(`SELECT 1`, `SELECT count(*)`) or no SQL at all. **Nothing in the harness exercises the
write path**: WAL append + fsync, the `ShardExecutor.wrote?` dirty-flag classification,
the coordinator's idle checkpoint, and the durability flush to `Fathom.Shard.Storage`.
That gap is the entire reason to add TPC — a standard, well-understood OLTP write
workload that touches all of it, on the real client path a libSQL client uses.

### The trap we are designing around: absolute TPS is an fsync/APFS artifact

`docs/benchmark-plan.md` is explicit that fathom benches **natively on macOS with no
container** precisely because its gated paths are open/CPU/read-bound, *not*
per-row-fsync-bound — and it warns that a **TPC-B-style per-transaction-commit workload
is dominated by macOS `F_FULLFSYNC` + APFS write-pressure artifacts**, which is exactly
what would make a raw-TPS gate lie. This plan takes that warning as load-bearing:

- **Only DELTA / no-write metrics gate.** The gated TPC-B number is the **overhead delta**
  between the full loopback-wire per-txn latency and the same txn run direct on the engine
  — run on the *same* shard file so the identical fsync cost **cancels in the subtraction**.
  `hrana_rt_us` gates too, because it round-trips a *read* (`SELECT 1`) that issues **no
  fsync at all**. Everything with an absolute-write-throughput number (TPC-B node TPS, all
  TPC-C latencies) is **recorded, never gated** — the same discipline as the opt-in S3
  metrics, which are measured but excluded from the commit gate.
- Absolute throughput is still worth recording (trend over time, peer comparability) — it
  just never blocks a commit, because a busy or thermally-throttled box moves it ±3× (the
  host-drift caveat in `docs/benchmark-plan.md`) with no code change.

---

## Harness change — the bench must start Filo and drive it over loopback

Today `Fathom.Bench.setup/1` starts only `Fathom.ShardRegistry` + `Fathom.ShardSupervisor`
(+ Local storage) and binds **no Hrana port** — no `Filo.Streams`, no Bandit listener. Every
wire bench needs that infrastructure, so a **loopback-Hrana harness** is shared Phase-1
work, reused by all the wire benches below and the retrofitted `cold_open`.

### Server side — start Filo on `127.0.0.1` (mirrors `Fathom.Application.hrana_listener/0`)

`Fathom.Application` mounts Filo as a Bandit listener with these opts (confirmed in
`lib/fathom/application.ex`): `executor: Fathom.ShardExecutor`,
`streams: Fathom.HranaStreams` (a `Filo.Streams` supervisor), `key: Filo.Baton.new_key()`,
`open_arg: &Fathom.ShardExecutor.shard_from_conn/1`,
`authorize: &Fathom.HranaAuth.authorize/2` — gated by `:hrana_server` (off in test). The
bench harness starts the same two children itself, bound to loopback and an ephemeral port,
with auth disabled:

```elixir
# In the bench setup, additive to today's ShardRegistry + ShardSupervisor:
ensure_started({Filo.Streams, name: Fathom.HranaStreams})

opts = [
  executor: Fathom.ShardExecutor,
  streams: Fathom.HranaStreams,
  key: Filo.Baton.new_key(),
  open_arg: &Fathom.ShardExecutor.shard_from_conn/1
  # authorize omitted -> Filo trusts the connection (loopback, :hrana_auth :disabled)
]

{:ok, _} = Bandit.start_link(plug: {Filo.Plug, opts}, scheme: :http, ip: {127, 0, 0, 1}, port: 0)
# port: 0 -> OS assigns a free port; read it back for the client base URL.
```

Shard selection stays on the production **Host-subdomain** path
(`ShardExecutor.shard_from_conn`): the loopback client sends `Host: <shard>.local` (a
multi-label host with no `:shard_base_domain` set → the first label is the shard, exactly
as `shard_from_host/1` resolves it). No `?db=` override needed, so the bench exercises the
same routing prod does.

### Client side — recommendation: an in-process Req HTTP `/v2/pipeline` client

Filo is a Hrana **server** library only (`Filo` moduledoc: "a Hrana protocol *server*") —
there is no Filo client to reuse. Two loopback-client options:

1. **Req HTTP `POST /v2/pipeline`** (RECOMMENDED for the gate). Pure Elixir, uses the
   already-present `Req` dep (no external toolchain), and drives the exact JSON wire path
   `deploy/chaos/chaos.sh`'s `hrana()` uses — framing/encode, `Filo.Plug` routing, baton,
   the `Executor` callback. Reproducible and single-host, which the ≥20% gate needs.
   Trade-off: it does **not** exercise the WebSocket transport (`Filo.Socket`, the
   `django-libsql` path) or a real client SDK's own encoder.
2. **A real libSQL client** (`libsql-experimental`, the SDKs, or `django-libsql` over WS).
   Higher realism (real client encode + WS), but pulls a non-Elixir dependency into the
   gate and is heavier to run under `mix fathom.bench`.

**Recommendation:** the **gate/CI loopback client is the Req `/v2/pipeline` client**; the
**rig realism run uses a real remote libSQL client over the network** (option 2), which the
chaos rig can already drive. This matches the lead's split exactly: loopback = gate-viable
wire, rig = realism headline.

The loopback client must speak Hrana values on the wire (confirmed in `Filo.Value`):
`integer` is carried as a **string** (`%{"type" => "integer", "value" => "42"}`), `text`
as `%{"type" => "text", "value" => "..."}`, `float` as a number, `blob` as unpadded base64,
`null` as `%{"type" => "null"}`. A parameterized statement therefore encodes as:

```elixir
# %Filo.Stmt{sql: "UPDATE pgbench_accounts SET abalance = abalance + ? WHERE aid = ?", args: [delta, aid]}
# becomes this pipeline request body (args bound as Hrana values — NEVER interpolated into sql):
%{
  "requests" => [
    %{"type" => "execute",
      "stmt" => %{
        "sql" => "UPDATE pgbench_accounts SET abalance = abalance + ? WHERE aid = ?",
        "args" => [Filo.Value.encode(delta), Filo.Value.encode(aid)]
      }},
    %{"type" => "close"}
  ]
}
```

**Persistent stream via baton reuse.** Each pipeline response carries a fresh `baton`; the
client reuses it on the next request to resume the *same* Filo stream (and therefore the
same shard connection), instead of re-opening per request. This matches fathom's real
per-stream model (one connection per Hrana stream, opened at stream start) and amortizes the
stream-open/connection-open cost over a burst — essential for the steady-state delta and
throughput numbers below. A burst omits the `close` until the last request.

---

## TPC-B — two framings

The workload is the **pgbench `tpcb-like` bank transaction**: seven statements per
transaction over `branches`/`tellers`/`accounts`/`history`, "inspired by TPC-B but not
actually TPC-B" (pgbench's own words — [PostgreSQL pgbench docs][pgbench]). The classic
TPC-B branch-row hot-contention story **does not apply to fathom**: each tenant is its own
single-writer SQLite file, so there is no cross-connection write contention on a shared
branch row — worth a code comment so nobody "fixes" a contention problem that can't exist.

### Framing A — wire proxy tax (`tpcb_wire_overhead_us`) — **GATED, loopback wire**

**What it measures.** The per-transaction cost of reaching the engine *through fathom's
full client wire* versus touching the engine directly. Two legs, each run on its own
freshly-seeded identical shard (sequentially, so no connection interference):

1. **Wire leg** — the TPC-B 7-statement transaction driven through the **loopback Hrana
   client** → `Filo.Plug` (`/v2/pipeline`) → baton stream → `Fathom.ShardExecutor` →
   `Fathom.Shard.Connection` → engine (WAL commit + fsync), on a **persistent baton-reused
   stream** so per-stream open is amortized.
2. **Direct leg** — the identical 7 statements run **in-process, direct on the engine**
   (raw `Exqlite.Sqlite3` prepare/bind/step, or `Connection.query/3`) on a held connection
   to the same freshly-seeded shard file (same WAL commit + fsync).

**Metric:** `tpcb_wire_overhead_us = p50(wire_per_txn_us) − p50(direct_per_txn_us)`, µs per
transaction. Positive = the wire+proxy layer got more expensive. (Renamed from rev. 1's
`tpcb_overhead_us`, which subtracted at the executor boundary; the delta now spans the whole
wire.)

**Why the delta is gate-robust on native macOS.** Both legs execute the identical SQL,
committing the same WAL to the same file, so **both pay the same `F_FULLFSYNC` commit cost
per transaction**. The subtraction removes that shared, host-dominated term, leaving only
fathom's per-transaction wire+proxy overhead: HTTP framing + `Filo.Value` encode/decode +
baton verify + `Filo.Plug` routing + the `Executor`/`Connection` path (incl. fathom's
no-statement-cache re-prepare). That residual is CPU + loopback-socket + syscall bound —
**not** fsync-bound — so it is stable enough to gate the same way `cold_open_p50_us` is.

**Honest caveat (recorded in the metric's doc):** the loopback socket + Bandit accept +
HTTP parse add more run-to-run variance than rev. 1's pure in-process delta. It is still
not fsync-dominated (the fsync cancels), and the median-of-trials + the ≥20% band absorb the
residual jitter — but expect a wider noise floor than the file-open metrics, and honor the
"rerun once to rule out noise" rule before trusting a BLOCK.

### `hrana_rt_us` — now measurable (GATED, loopback wire)

Rev. 1 left `hrana_rt_us` a `null` placeholder ("until remote shards land"). The loopback
client makes it real: **the median round-trip latency of a trivial read (`SELECT 1`) through
the loopback Hrana wire on a warm, baton-reused stream** — client encode → `POST
/v2/pipeline` → `Filo.Plug` → `Executor.execute` → `StmtResult` → client decode. Warm
(steady-state) stream, not a cold stream-open, so it is the minimal wire round-trip, not a
cold-open. **`SELECT 1` writes nothing → issues no fsync → very stable → gate-viable.** Add
to `Gate.@metrics` (higher_worse); populated only when the wire bench runs (else `nil` →
skipped). On the chaos rig the same probe over the real network is the true cross-network
RTT (recorded, not gated — the rig is periodic, not in the commit path).

### Framing B — multi-tenant aggregate node TPS (`tpcb_node_tps`) — **RECORDED-ONLY**

**What it measures.** Aggregate transactions/sec a node sustains with **N shards each
running an independent TPC-B write stream concurrently, each through its own loopback Hrana
stream** — fathom's real differentiator, **fan-out under genuine WRITE load**, now measured
through the wire. Unlike the read-only `--hotspots` driver, it exercises WAL growth, the
`wrote?` dirty-flag flips, the coordinator idle checkpoint, and the durability flush on every
shard at once. Reuse the `Fathom.Scale` concurrency shape (`Task.async_stream` + a per-stream
burst), but the per-stream unit is now a **loopback Hrana client** holding a baton-reused
stream and bursting TPC-B transactions, not an in-process `ShardExecutor.execute` loop.
Report aggregate txn/s across the node, plus (recorded for context) per-window write-path
signals from the existing `[:fathom, :shard, ...]` telemetry (flushes, checkpoints,
dirty→clean transitions).

- **Gate/CI:** loopback wire, recorded to `perf_history.jsonl` (trend only, not gated).
- **Realism:** the chaos rig drives N concurrent **remote** libSQL clients over the network
  (`chaos.sh tpcb` shape), recorded to a chaos-run doc.

**Why recorded-only, not gated.** Absolute write throughput is exactly the
`F_FULLFSYNC`/APFS-dominated number `docs/benchmark-plan.md` warns against — a measurement of
*SQLite + the host*, not a fathom regression. Worth recording for trend-watching and peer
comparison; a genuine fan-out-under-write regression (e.g. a flush storm) should be confirmed
against the `[:fathom,:shard]` flush/checkpoint telemetry, not the raw TPS scalar.

**Sizing.** N is a knob (`--tpcb-shards`, default e.g. 64–256 so a laptop run is quick); this
is not a millions-of-shards test (that is `fathom.scale --ramp`), it is "does write fan-out
across a realistic hold-open set stay healthy."

### TPC-B schema + seed (SF = 1 ≈ a small shard)

At pgbench scale factor 1 ([PostgreSQL pgbench docs][pgbench]): `branches` 1 row, `tellers`
10 rows, `accounts` 100,000 rows, `history` 0 rows. SF is a knob (`--tpcb-scale`, default 1)
that multiplies every table except history. SF=1 with standard filler is ~15–20 MB — squarely
a "small shard."

**Seeding is setup, not the measured workload, so it stays in-process** (a direct
`Fathom.Shard.Connection` writing into the storage dir, exactly as
`Fathom.Bench.seed_storage_shard/1` / `Fathom.Scale.provision/2` do today — then the wire
cold-open pulls it). Only the *measured* TPC transactions cross the loopback wire. The `rows`
count is harness-controlled (never user input), so inlining it into the recursive-CTE seed is
safe:

```sql
CREATE TABLE pgbench_branches (bid INTEGER PRIMARY KEY, bbalance INTEGER, filler TEXT);
CREATE TABLE pgbench_tellers  (tid INTEGER PRIMARY KEY, bid INTEGER, tbalance INTEGER, filler TEXT);
CREATE TABLE pgbench_accounts (aid INTEGER PRIMARY KEY, bid INTEGER, abalance INTEGER, filler TEXT);
CREATE TABLE pgbench_history  (tid INTEGER, bid INTEGER, aid INTEGER, delta INTEGER, mtime TEXT, filler TEXT);

WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 100000)
INSERT INTO pgbench_accounts (aid, bid, abalance, filler)
SELECT x, (x - 1) / 100000 + 1, 0, '' FROM c;
```

### TPC-B transaction — parameterized, never interpolated

The 7 statements as `Filo.Stmt` structs with bound `?` placeholders. On the wire they encode
as Hrana `args` (`Filo.Value.encode`, integers as strings); through `Filo.Plug` they decode
back to native terms and bind via `Connection.query/3` → `Exqlite.Sqlite3.bind`. Random
`aid ∈ 1..100000·SF`, `tid ∈ 1..10·SF`, `bid ∈ 1..SF`, `delta ∈ −5000..5000` are drawn in
Elixir and passed as `args` — **values never touch the SQL string** (the project's
multi-tenant-safety principle: bind, never interpolate):

```elixir
alias Filo.Stmt

# BEGIN / COMMIT bracket the txn (sent as their own execute requests in the pipeline).
[
  %Stmt{sql: "UPDATE pgbench_accounts SET abalance = abalance + ? WHERE aid = ?", args: [delta, aid]},
  %Stmt{sql: "SELECT abalance FROM pgbench_accounts WHERE aid = ?",               args: [aid]},
  %Stmt{sql: "UPDATE pgbench_tellers  SET tbalance = tbalance + ? WHERE tid = ?", args: [delta, tid]},
  %Stmt{sql: "UPDATE pgbench_branches SET bbalance = bbalance + ? WHERE bid = ?", args: [delta, bid]},
  %Stmt{sql: "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (?, ?, ?, ?, ?)",
        args: [tid, bid, aid, delta, mtime]}
]
```

Driven over the wire, `wrote?` classifies the four writes dirty and the `SELECT` clean,
exercising the exact durability-classification code a Django write path hits — through the
full protocol, not the executor shortcut.

### Isolation-under-write-load test (`test/`, shard-isolation gate) — through the loopback wire

A concurrent-write isolation test belongs under the **shard-isolation gate**
(`AGENTS.md` §Gates: any change to routing ships with a cross-shard-isolation test; a leak is
a release blocker). Driving it **through the loopback Hrana wire** strengthens it — it now
exercises `shard_from_conn` Host routing end-to-end, the real path a leak would travel.
Design (`test/fathom/tpcb_isolation_test.exs`, tagged so it stays out of the default fast
suite if slow):

- Start the loopback Filo harness. Provision M shards with **unique ids** (real SQLite files,
  `File.rm` in `on_exit`, never share a file), seeded TPC-B (small SF) in-process.
- Drive K TPC-B transactions **per shard concurrently through the loopback client** (each with
  `Host: <shard>.local`), `Task.async_stream`, complete via `Task` join — **no
  `Process.sleep`**.
- Assert two invariants (pin the invariant, not just the repro):
  1. **TPC-B consistency, per shard:** with all balances starting at 0,
     `sum(pgbench_branches.bbalance) == sum(pgbench_tellers.tbalance) ==
     sum(pgbench_accounts.abalance) == sum(pgbench_history.delta)` — read back through the
     wire. Proves every write landed, atomically, on the right shard's file.
  2. **Cross-shard non-contamination:** tag each shard's `history.filler` with its own
     `shard_id`; assert every shard's history has **zero** foreign tags and a row count equal
     to the transactions routed to *that* Host. (Mirrors the chaos rig's `foreign` isolation
     check, under concurrent wire write load.)
- Must fail if `shard_from_conn` / `Fathom.Shards` routing ever leaks A→B. Fixed PRNG seed so
  the transaction stream is deterministic.

---

## TPC-C — comparability / concurrency characterization (RECORDED-ONLY)

**Why TPC-C, framed differently from TPC-B.** The most comparable peer — Turso's Rust-based
libSQL engine — publishes its OLTP concurrency story as TPC-C: **throughput plus p95/p99/max
latency *per transaction type*** over a warehouse × thread matrix. To be comparable we run the
same benchmark and report the same shape. TPC-C is heavier (9 tables, 5 weighted transaction
types) and its latencies are host- and fsync-sensitive, so it is a **comparability /
characterization** benchmark — **never a commit-gate metric**. Both variants cross the wire:

- **Loopback-wire in-process** (`mix fathom.tpcc`, gate host, recorded-only): drives the
  weighted deck through the loopback Hrana client. Good for a local trend line and the
  write-path telemetry; single-host, so **not** a cross-network number.
- **Rig remote client over the network** (`chaos.sh tpcc`, the realism headline): a real
  remote libSQL client through the LB (subdomain → node hash, as prod), yielding the true
  per-txn-type latency + tpmC. Written to `docs/reviews/tpcc-run-<date>.md`.

### Transaction mix (authoritative)

The required mix ([TPC-C spec / Wikipedia][tpcc-wiki]; [Jim Gray Benchmark Handbook ch.
12][graybook]): equal New-Order and Payment counts, and one Delivery, one Order-Status, and
one Stock-Level per ten New-Orders. In the standard weighted deck:

| Transaction | Weight | Shape |
|---|---|---|
| **New-Order** | ~45% | read Item+Stock, write Order+Order-Line+New-Order, update Stock, update District; ~1% forced rollback (invalid item) |
| **Payment** | ~43% | update Warehouse+District+Customer balances, insert History; 60% customer-by-last-name, 40% by id |
| **Order-Status** | ~4% | read-only: latest order + its order lines for a customer |
| **Delivery** | ~4% | batch: for each of 10 districts, oldest New-Order → deliver (update Order, Order-Line, Customer; delete New-Order) |
| **Stock-Level** | ~4% | read-only: count distinct low-stock items for the last 20 orders in a district (join) |

`tpmC` is measured as **New-Order transactions per minute** (New-Order ≈ 44–45% of the deck).
Cite these weights in the code so a reviewer can check the deck.

### Schema (9 tables) and sizing

Per warehouse `W` ([TPC-C spec / Wikipedia][tpcc-wiki]): `warehouse` W, `district` 10·W,
`customer` 30,000·W, `history` ~30,000·W, `new_order` ~9,000·W, `order` (`oorder`) 30,000·W,
`order_line` ~300,000·W, `item` **100,000 (fixed)**, `stock` 100,000·W.

**Sizing honesty:** even at `W=1`, TPC-C is ~100k items + 100k stock + ~300k order-lines —
~100 MB, the **large end** of "small shard." Fine for a *per-tenant comparability* run, but
**not** a fan-out-density workload. Keep **one TPC-C dataset per shard** (a shard = a tenant
DB = one independent W-warehouse set; cross-warehouse New-Order lines stay *within* the shard
— do **not** spread warehouses across shards, which would violate fathom's
single-writer-per-file model). `W` is a knob (`--tpcc-warehouses`, default 1). The
multi-tenant "many small TPC-C at once" angle is already covered by TPC-B Framing B.

### New-Order — parameterized statement profile

All values bound as `?` (SQLite positional) → `Stmt.args` → Hrana `args` on the wire →
`Sqlite3.bind`. `w_id`, `d_id`, `c_id`, `ol_cnt ∈ 5..15`, and per-line `(i_id, supply_w_id,
quantity)` are drawn in Elixir. Inject an invalid `i_id` on ~1% of transactions to trigger the
spec's rollback:

```elixir
[
  %Stmt{sql: "SELECT w_tax FROM warehouse WHERE w_id = ?", args: [w_id]},
  %Stmt{sql: "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?", args: [w_id, d_id]},
  %Stmt{sql: "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = ? AND d_id = ?", args: [w_id, d_id]},
  %Stmt{sql: "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?", args: [w_id, d_id, c_id]},
  %Stmt{sql: "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?)",
        args: [o_id, d_id, w_id, c_id, entry_d, ol_cnt, all_local]},
  %Stmt{sql: "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)", args: [o_id, d_id, w_id]}
  # then, per order line (ol_cnt of them):
  # SELECT i_price, i_name, i_data FROM item WHERE i_id = ?                                   -> [i_id]
  # SELECT s_quantity, s_dist_NN, s_data FROM stock WHERE s_w_id = ? AND s_i_id = ?           -> [supply_w_id, i_id]
  # UPDATE stock SET s_quantity = ?, s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1
  #        WHERE s_w_id = ? AND s_i_id = ?                                                     -> [new_qty, qty, supply_w_id, i_id]
  # INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id,
  #        ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
]
```

### Payment — parameterized statement profile

```elixir
[
  %Stmt{sql: "UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?", args: [amount, w_id]},
  %Stmt{sql: "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?", args: [w_id]},
  %Stmt{sql: "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?", args: [amount, w_id, d_id]},
  %Stmt{sql: "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?", args: [w_id, d_id]},
  # customer selection: 60% by last name (returns a list -> pick the middle row), 40% by id:
  %Stmt{sql: "SELECT c_id, c_first, c_balance, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
        args: [w_id, d_id, c_last]},
  # (by-id branch: SELECT ... WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?)
  %Stmt{sql: "UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
        args: [amount, amount, w_id, d_id, c_id]},
  # (bad-credit customers additionally update c_data)
  %Stmt{sql: "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        args: [c_id, d_id, w_id, d_id, w_id, h_date, amount, h_data]}
]
```

Order-Status, Delivery, and Stock-Level follow the same bound-`?` pattern; only New-Order and
Payment are spelled out here (they are ~88% of the deck and the two required by scope). The
customer-by-last-name select returns multiple rows — see the `Connection.collect`
prerequisite below.

### TPC-C driver — both variants cross the wire

- **Loopback-wire in-process** (`mix fathom.tpcc`): the weighted deck through the loopback
  Hrana client, concurrency matrix `--tpcc-warehouses W` × `--tpcc-threads T` (each thread a
  baton-reused loopback stream). Reports per-txn-type `p50/p95/p99/max` + `tpmC` to
  `scripts/tpc_history.jsonl`. Recorded-only, single-host — a trend line, not a
  cross-network number.
- **Rig remote client** (`chaos.sh tpcc`, shaped like `cmd_hotspots`/`cmd_smoke`): real remote
  Hrana pipeline requests over the network through the LB, args bound as Hrana `args`. The
  peer-comparable headline (per-txn-type latency + tpmC), written to
  `docs/reviews/tpcc-run-<date>.md`.

---

## Retrofit note — the existing gate benches and the wire

The lead's directive extends to the existing request-path gate benches. The split:

- **`cold_open_p50_us` → retrofit to the wire.** Measure cold-open by opening a **fresh
  loopback Hrana stream** to a cold (storage-backed) shard and timing the first query's
  round-trip — the checkout + pull happens inside `Executor.open` on that stream. This is the
  real client cold-open. **Baseline reset required:** a wire-measured cold-open reads higher
  than the stored in-process baseline (it adds the wire constant), so the first wire run would
  read as a regression against the old line. Re-baseline the parent under the new harness
  before the gate is meaningful (the documented stale-baseline procedure in
  `docs/benchmark-plan.md`). Same applies to the opt-in S3 cold-open / failover metrics if
  they are retrofitted. See Open Questions for redefine-in-place vs add-a-new-metric.
- **`dir_resolve_p50_us` → stays in-process (with a note).** `Fathom.Directory.resolve/1` is a
  control-plane function **not currently on the request path** (routing is Host-based today;
  the cached, PubSub-invalidated request-path resolve is still aspirational per `AGENTS.md`).
  There is no client query whose latency isolates a resolve — a wire query would be dominated
  by the shard open, not the resolve. Keep it a direct in-process measurement for now; fold it
  into the wire cold-open path when the request-path resolve lands. (Flagged as an open
  question.)
- **Inherently-internal benches stay in-process — no client query exists:**
  - `copy_rows_per_s` — migration blue/green copy+transform; internal, no client.
  - `fanout_kb_per_shard` — memory per open coordinator; deliberately holds no connection, so
    driving it through the wire (which holds a per-stream connection) would *change what it
    measures* (add ~3 fds/connection), not improve it.
  - `warm_s3_shards_per_s` — background S3 pull throughput; no client query.

---

## Metrics — names, units, direction, gated vs recorded

Follows the repo conventions (`*_p50_us`, `*_per_s`) and the S3-metric opt-in pattern: a
metric absent from a run is `nil`, and `Fathom.Bench.Gate` **auto-skips any metric that is
`nil` in either the parent or the new run** — so an opt-in metric never gates a commit that
didn't run it.

| Metric | Unit | Direction | Gate | Transport | Where recorded | Populated when |
|---|---|---|---|---|---|---|
| `tpcb_wire_overhead_us` | µs / txn | higher_worse | **GATED** | loopback wire (delta vs direct engine) | `perf_history.jsonl` **new col** + `Gate.@metrics` **new line** | only with `--tpcb`; else `nil` |
| `hrana_rt_us` | µs | higher_worse | **GATED** | loopback wire (`SELECT 1` RT) | `perf_history.jsonl` **existing col** (was always `null`) + `Gate.@metrics` **new line** | only with the wire bench; else `nil` |
| `tpcb_node_tps` | txn / s | (lower_worse) | recorded-only | loopback wire (gate host) / remote client (rig) | `perf_history.jsonl` **new col** (like `failover_*`, *not* in `Gate.@metrics`) | only with `--tpcb`; else `nil` |
| `tpcc_tpmc` | New-Order / min | lower_worse | recorded-only | loopback wire (CI) / remote client (rig headline) | `scripts/tpc_history.jsonl` (**separate**) + chaos-run doc | only via the TPC-C tasks |
| `tpcc_<txn>_p50/p95/p99/max_us` | µs | higher_worse | recorded-only | loopback wire (CI) / remote client (rig headline) | `scripts/tpc_history.jsonl` + chaos-run doc | only via the TPC-C tasks |

**Exact `perf_history.jsonl` columns to add** (two new; `hrana_rt_us` already exists and just
starts carrying a value):

```
"tpcb_wire_overhead_us": <float|null>,   // GATED  (nil unless --tpcb)
"tpcb_node_tps":         <float|null>,    // recorded-only (nil unless --tpcb)
"hrana_rt_us":           <float|null>,    // GATED — was always null; now populated by the wire bench
```

Wired in `Mix.Tasks.Fathom.Bench.build_line/3` (add the two new keys; `hrana_rt_us` already
present), `print_table/2` (add rows), and `Fathom.Bench.@all_metrics` / `all/1` (a new `:tpcb`
entry producing `tpcb_wire_overhead_us` + `tpcb_node_tps`, and `hrana_rt_us` produced whenever
the wire harness is up — analogous to how `:failover_rto` produces two `failover_*` columns).

**Exact `Gate.@metrics` lines to add** (two — the delta and the no-fsync round-trip gate):

```elixir
@metrics [
  {:cold_open_p50_us, :higher_worse},
  {:cold_open_s3_p50_us, :higher_worse},
  {:warm_s3_shards_per_s, :lower_worse},
  {:dir_resolve_p50_us, :higher_worse},
  {:copy_rows_per_s, :lower_worse},
  {:fanout_kb_per_shard, :higher_worse},
  {:hrana_rt_us, :higher_worse},          # <-- add (loopback SELECT 1 RT; no fsync)
  {:tpcb_wire_overhead_us, :higher_worse} # <-- add (loopback delta vs direct engine)
]
```

`tpcb_node_tps` is deliberately **not** in `Gate.@metrics` — recorded in the line but never
gates, like `failover_cold_s3_p50_us` today. TPC-C metrics stay **out of
`perf_history.jsonl`** (a per-txn-type × percentile matrix would bloat the gate file) and live
in `scripts/tpc_history.jsonl` + a chaos-run doc.

---

## Gate integration + skip behavior on normal commits

TPC never slows down or blocks an ordinary commit.

- **Normal commit** (`scripts/commit_with_bench.sh -m "..."`) runs `scripts/benchmark.sh`
  **without** `--tpcb` and without the wire harness, so `tpcb_wire_overhead_us`,
  `tpcb_node_tps`, and `hrana_rt_us` all record `nil`. Parents were benched the same way
  (`nil`). `Gate.compare/4` skips any metric `nil` in either run → **none of the wire metrics
  participate** in a normal gate, and the bench stays as fast as today.
- **To gate on the wire metrics:** run `scripts/benchmark.sh --tpcb` (which starts the
  loopback Filo harness and produces `tpcb_wire_overhead_us` + `hrana_rt_us` + `tpcb_node_tps`)
  on **both** the parent (to seed a non-`nil` baseline) and the working tree. Identical to how
  the opt-in S3 metrics gate only when `FATHOM_S3_TEST_*` is set on both runs.
- **`--tpcb`** is a new pass-through flag on `mix fathom.bench` (add to `@switches` /
  `@all_metrics` / `parse_only`) and on `scripts/benchmark.sh` (`"$@"` already forwards it). It
  also implies starting the loopback wire harness (needed for `hrana_rt_us` too).
- **TPC-C** is **not** on `mix fathom.bench` — a separate `mix fathom.tpcc` (loopback,
  recorded-only) and `chaos.sh tpcc` (remote, recorded-only). It never writes
  `perf_history.jsonl`, so it cannot affect the gate.
- The host-wide benchmark lock in `mix fathom.bench` still applies — a TPC-B run holds it like
  any other, so no run measures under another's load. The loopback listener binds `port: 0`
  (OS-assigned), so parallel unrelated processes don't collide on a fixed Hrana port.

---

## Prerequisite (blocks any large-result-set path): fix `Connection.collect`

`Fathom.Shard.Connection.collect/3` accumulates rows with `acc ++ rows`:

```elixir
defp collect(conn, stmt, acc \\ []) do
  case Sqlite3.multi_step(conn, stmt) do
    {:rows, rows} -> collect(conn, stmt, acc ++ rows)   # O(n²): re-copies acc each chunk
    {:done, rows} -> {:ok, acc ++ rows}
    :busy -> {:error, :busy}
    {:error, reason} -> {:error, reason}
  end
end
```

`acc ++ rows` re-copies the whole accumulator on every `multi_step` chunk, so collecting an
`R`-row result is **O(R²)**. The current read-only harness only ever `SELECT count(*)` /
`SELECT 1` (one row), so it never triggers this — but TPC-C's customer-by-last-name select,
Order-Status's order-line read, and any validation `SELECT` in the isolation test **do**
return multi-row sets, and a naive large-result path would blow up quadratically — and now it
runs over the wire, so the blow-up would also inflate the wire round-trip. **Fix before
shipping any TPC bench that reads more than a handful of rows:** accumulate chunks and flatten
once (prepend + reverse, O(R)):

```elixir
defp collect(conn, stmt, acc \\ []) do
  case Sqlite3.multi_step(conn, stmt) do
    {:rows, rows} -> collect(conn, stmt, [rows | acc])
    {:done, rows} -> {:ok, :lists.append(Enum.reverse([rows | acc]))}
    :busy -> {:error, :busy}
    {:error, reason} -> {:error, reason}
  end
end
```

This touches the shard read hot path, so it goes through the bench gate itself
(`scripts/commit_with_bench.sh`) and ships with a regression test: a `@tag :bench`
floor/ceiling asserting an order-of-magnitude ceiling on collecting a large (e.g. 50k-row)
result set — fails on the `++` version, passes on the flattened one (pin the invariant: result
assembly is linear in row count).

---

## Effort estimates + phased rollout (build in this order for the most value first)

| Phase | Deliverable | Est. |
|---|---|---|
| **0 — prereq** | Fix `Connection.collect` (`++` → O(R)) + `@tag :bench` large-result regression test; commit through the bench gate. | ~0.5 day |
| **1 — loopback Hrana harness + wire gate metrics (highest value)** | Start `Filo.Streams` + a Bandit `Filo.Plug` listener on `127.0.0.1:0` in the bench harness; build the Req `/v2/pipeline` loopback client (baton reuse, `Filo.Value` arg encode/decode). Then `hrana_rt_us` (trivial once the client exists) + `tpcb_wire_overhead_us` (Framing A: TPC-B seed + 7-stmt deck, wire-vs-direct delta), `--tpcb` flag, `perf_history` columns, two `Gate.@metrics` lines, `@tag :bench` guards. **Retrofit `cold_open` to the wire** (+ re-baseline). First honest wire write-path numbers and the two new gates. | ~2.5–3 days |
| **2 — TPC-B aggregate + isolation (wire)** | `tpcb_node_tps` (Framing B) recorded-only via concurrent loopback streams; the concurrent-write **isolation-under-write-load** test through the loopback wire, under the shard-isolation gate. | ~2 days |
| **3 — TPC-C loopback wire (in-process)** | `mix fathom.tpcc`: 9-table schema + seed, all 5 weighted txn profiles, warehouse×thread matrix over the loopback client, per-txn-type `p50/p95/p99/max` + `tpmC` → `scripts/tpc_history.jsonl`. Recorded-only. | ~3.5–4.5 days |
| **4 — remote-client realism (rig headline)** | `chaos.sh tpcb` + `chaos.sh tpcc`: real remote libSQL client over the network through the LB (true cross-network RTT + per-txn-type latency + tpmC), written to `docs/reviews/tpc*-run-<date>.md`. | ~2.5–3.5 days |

Phases 0–1 deliver the entire *gated* value (a robust wire write-path delta + a real
`hrana_rt_us`) in ~3 days, including the shared loopback harness every later phase reuses; 2
adds the multi-tenant write story and the isolation guarantee; 3–4 are the heavier
comparability harness and the remote-client realism headline, and can follow independently.

---

## What this does NOT tell us about fathom (kept honest)

- **Loopback crosses the full wire, but it is not a cross-network RTT.** The gate/CI benches
  drive a `127.0.0.1` HTTP client through Bandit + `Filo.Plug` — real framing, encode/decode,
  baton, routing, executor — but with **no network**: loopback has ~µs link latency and no
  bandwidth-delay, congestion, TLS, or LB hop. So `hrana_rt_us` and `tpcb_wire_overhead_us` on
  the gate are the **software** wire cost, not what a client in another region pays. **Only the
  chaos-rig remote-client run gives the true cross-network RTT** — which is exactly why the rig
  numbers are the realism headline and the loopback numbers are the reproducible gate.
- **TPC re-measures SQLite + the host, not fathom.** Both benchmarks are, at bottom, a
  measurement of the SQLite write engine and the machine's `F_FULLFSYNC`/WAL/APFS behavior.
  Absolute TPC-B TPS and all TPC-C latencies move with host load and fsync policy, not fathom
  code — which is why only the *cancelling* overhead delta and the *no-fsync* `SELECT 1`
  round-trip gate, and everything else is recorded-only.
- **It says nothing about fathom's actual differentiators.** Cold-open latency, fan-out density
  across *millions* of shards, cross-node lease/epoch/heartbeat correctness, migration copy
  throughput, warm-standby RTO — fathom's whole reason to exist — are **already** measured by
  `mix fathom.scale` and the gated `fathom.bench` metrics. TPC adds the missing *write-path*
  dimension on a single (or a modest fan-out of) tenant DBs; it is not, and should not be sold
  as, a fathom-scale test.
- **TPC-C comparability is per-tenant.** The peer-comparable TPC-C run characterizes *one tenant
  DB's* OLTP concurrency; even over the rig's remote client it does not stress the LB partition,
  the S3 lease, or the directory across shards. It answers "how does a single fathom shard
  compare to peer X on TPC-C over the wire," not "how does fathom's architecture scale."

---

## Sources

- pgbench `tpcb-like` transaction (7 statements, tables, scale factor 1 = 1/10/100000/0 rows):
  [PostgreSQL documentation — pgbench][pgbench].
- TPC-C schema (9 tables, per-warehouse cardinality), transaction mix (New-Order ~45%, Payment
  ~43%, Order-Status/Delivery/Stock-Level ~4% each; one Delivery/Order-Status/Stock-Level per ten
  New-Orders), New-Order + Payment behavior: [TPC-C — Wikipedia][tpcc-wiki] and the [TPC-C chapter
  of the Benchmark Handbook (Jim Gray)][graybook].
- Hrana wire (loopback client target): `Filo.Plug` routes (`/v2/pipeline` etc.) and `Filo.Value`
  value encoding (integer carried as a string) — the `../filo` source; the in-process HTTP
  pipeline driver pattern is `deploy/chaos/chaos.sh`'s `hrana()`.

[pgbench]: https://www.postgresql.org/docs/current/pgbench.html
[tpcc-wiki]: https://en.wikipedia.org/wiki/TPC-C
[graybook]: https://jimgray.azurewebsites.net/BenchmarkHandbook/chapter12.pdf

---

## Open questions for the lead

1. **Loopback client: Req HTTP `/v2/pipeline` (recommended, dep-free, HTTP-only) vs a real
   libSQL client in-process (adds WS + real client encode, but a non-Elixir dep in the gate)?**
   Plan recommends Req for the gate, a real remote client for the rig. Also: do we want the WS
   transport (`Filo.Socket`, the `django-libsql` path) covered on the gate, or is HTTP-only
   acceptable there and WS left to the rig?
2. **`cold_open` retrofit — redefine in place vs add a new metric.** Redefining
   `cold_open_p50_us` to be wire-measured is a **baseline reset** (it reads higher; the first
   run flags as a regression until the parent is re-benched). Alternative: keep the in-process
   `cold_open_p50_us` and add a new `cold_open_wire_p50_us` alongside. Plan assumes redefine +
   re-baseline per the directive; confirm.
3. **`dir_resolve` on the wire.** Resolve is not on the request path today, so the plan keeps
   `dir_resolve_p50_us` an in-process measurement (a wire query wouldn't isolate it). Accept
   that, or defer the metric until the request-path resolve lands?
4. **`tpcb_wire_overhead_us` direct leg:** raw `Exqlite.Sqlite3` (measures fathom's *whole*
   wire+proxy tax incl. `Connection`'s no-cache re-prepare — recommended) vs `Connection.query/3`
   (folds the re-prepare into the baseline, so the delta is wire+executor only). Plan recommends
   raw-exqlite.
5. **Gate `tpcb_node_tps`?** Plan says no (fsync/APFS-dominated → recorded-only). A loose guard
   (block only on a >50% drop) is possible but would need its own direction entry — flagged, not
   assumed.
6. **TPC-C default `W`:** plan defaults `--tpcc-warehouses 1` (~100 MB). Confirm that per-tenant
   comparability size vs a smaller item/stock-reduced variant (which breaks strict peer
   comparability). And confirm TPC-C metrics live in a **separate** `scripts/tpc_history.jsonl`,
   not new `perf_history.jsonl` columns.
