# Fathom — TPC-B + TPC-C Benchmark Plan

> Status: **Phase 1 BUILT (2026-07-10, rev. 4); Phases 2–4 planned.** This is the
> implementation-ready design for adding *both* a TPC-B-derived and a TPC-C-derived
> benchmark to fathom, additive to the existing harness (`docs/benchmark-plan.md`,
> `Fathom.Bench`, `mix fathom.bench`, the `Fathom.Bench.Gate` regression gate,
> `Fathom.Scale`). The decision to do both is already made — this plan does not
> re-litigate "which one," and TPC-H stays rejected (an analytical/OLAP workload says
> nothing about fathom's per-tenant OLTP write path).
>
> **Rev. 4 — Phase 0 + Phase 1 shipped.** Phase 0: `Connection.collect` is O(R). Phase 1:
> the wire loopback harness + the three gated wire metrics are built and proven —
> `Fathom.Bench.HranaClient` (a Mint.WebSocket client in `test/support`, dev/test only) +
> `Fathom.Bench.Wire` + the **`MIX_ENV=test mix fathom.wire_bench`** task. Metrics (measured
> here, loopback, relative): `hrana_rt_us` ≈ 100–175 µs, `cold_open_wire_p50_us` ≈ 2.5–4 ms
> (vs the in-process `cold_open_p50_us` ≈ 1.6 ms — the delta is the WS software tax),
> `tpcb_wire_overhead_us` ≈ 800 µs/txn (dominated by the 7 chatty round-trips).
>
> **Decision (this session) — where wire metrics are gated.** The WS client is `mint_web_socket`,
> `only: [:dev, :test]` (never ships prod), and the per-commit gate `commit_with_bench.sh` runs
> `MIX_ENV=prod`. So the wire metrics are **NOT** in the prod per-commit gate; they run + gate
> in `mix fathom.wire_bench` (a manual / CI pre-merge step): its `--check` compares each metric
> to the last same-host entry in `scripts/wire_history.jsonl` (gitignored — a committed
> canonical baseline is a follow-on) and blocks a ≥20% regression. "GATED" below means gated in
> *that* wire run, not the prod commit gate.
>
> **Rev. 2 — every bench crosses the Hrana client wire.** No TPC bench measures the
> in-process `ShardExecutor` path. GATE/CI benches drive an in-process loopback Hrana
> client against Filo's listener on `127.0.0.1` (full wire: framing/encode, stream,
> `Filo.Plug`/`Filo.Socket`, the `Executor` callback, `ShardExecutor` → `Connection` →
> engine); a periodic chaos-rig run drives a real remote libSQL client over the network
> for the true cross-network RTT.
>
> **Rev. 3 — lead resolutions folded in:** (1) `cold_open` gains a *parallel*
> `cold_open_wire_p50_us` — the in-process metric is untouched (no re-baseline).
> (2) A `dir_resolve` wire variant is **deferred** (resolve isn't on the request path
> yet). (3) The gate loopback client is an **in-process Elixir Hrana-WebSocket client**
> (`Filo.Socket` — django-libsql's transport), with the HTTP `/v2/pipeline` client kept
> as an optional secondary. (4a) The `tpcb_wire_overhead_us` direct leg is **raw
> `Exqlite.Sqlite3`** (whole fathom tax over bare SQLite). (4b) `tpcb_node_tps` gets a
> **loose gate** (~50%), which requires a small **per-metric-threshold** extension to
> `Fathom.Bench.Gate`. (4c) TPC-C runs a **W = 1..5 warehouse sweep**.

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

- **Prefer DELTA / no-write metrics for gating.** The primary gated TPC-B number is the
  **overhead delta** between the full loopback-wire per-txn latency and the same txn run
  direct on the bare engine — run on the *same* shard file so the identical fsync cost
  **cancels in the subtraction**. `hrana_rt_us` gates too, because it round-trips a *read*
  (`SELECT 1`) that issues **no fsync at all**.
- **One deliberate absolute-throughput exception, loosely gated.** The lead has decided
  `tpcb_node_tps` (aggregate write throughput) **is** gated, but with a **relaxed ~50%
  per-metric threshold** — wide enough to swallow `F_FULLFSYNC`/APFS run-to-run jitter,
  tight enough to catch a catastrophic (≈2×) regression. This requires a per-metric
  threshold in `Fathom.Bench.Gate` (below); it is the one place we knowingly gate a
  host-sensitive absolute number, and we size the threshold for that.
- All TPC-C latencies remain **recorded-only** (peer-comparability / trend, never a gate).

---

## Harness change — the bench must start Filo and drive it over a loopback WebSocket

Today `Fathom.Bench.setup/1` starts only `Fathom.ShardRegistry` + `Fathom.ShardSupervisor`
(+ Local storage) and binds **no Hrana port** — no `Filo.Streams`, no Bandit listener. Every
wire bench needs that infrastructure, so a **loopback-Hrana harness** is shared Phase-1
work, reused by all the wire benches below and the new `cold_open_wire`.

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
# port: 0 -> OS assigns a free port; read it back for the WS/HTTP client base URL.
# The same Bandit/Filo.Plug listener serves BOTH the WebSocket upgrade (-> Filo.Socket)
# and the HTTP /v2/pipeline path, so one listener covers the primary + secondary clients.
```

Shard selection stays on the production **Host-subdomain** path
(`ShardExecutor.shard_from_conn`): the loopback client sends `Host: <shard>.local` (a
multi-label host with no `:shard_base_domain` set → the first label is the shard, exactly
as `shard_from_host/1` resolves it). No `?db=` override needed, so the bench exercises the
same routing prod does.

### Client side — PRIMARY: an in-process Elixir Hrana-WebSocket client

**Why WebSocket is the primary gate transport.** fathom's canonical client, `django-libsql`
(via `libsql-client`), speaks **only** Hrana over WebSocket — it has no HTTP-pipeline path
(`Filo.Socket` moduledoc). Gating on HTTP alone would never exercise `Filo.Socket`, the
handshake, or the persistent-socket stream bookkeeping that the real client uses. So the
gate client opens a real WebSocket to Filo's loopback listener.

**There is no reusable in-process Elixir Hrana-WS client to borrow.** Filo's own WS
integration test (`../filo/test/filo/integration_ws_test.exs`) shells out to a Python
`libsql-client` (`ws_smoke.py`), and its pure-Elixir `socket_test.exs` drives
`Filo.Socket.handle_in/2` **directly** with `{Jason.encode!(msg), [opcode: :text]}` frames —
neither is a network client we can reuse. So we build a small one.

**Recommended WS lib: `Mint.WebSocket`.** `mint` (1.9) is **already in fathom's lock**
(transitive via `Finch` ← `Req`), so `Mint.WebSocket` adds only the thin `mint_web_socket`
hex package — a **new test/bench-only dep**, not a runtime one. (`:gun` is the alternative
but pulls `cowlib` and is less idiomatic; shelling to a real Python/Rust client is the rig's
job, not the gate's.) **Wrap it behind a project-owned module** (e.g.
`Fathom.Bench.HranaClient`) per the "wrap third-party APIs" rule, so the bench code speaks
`open_stream`/`execute`/`close_stream`, not raw Mint frames.

**The Hrana-over-WS protocol the client speaks** (from `Filo.Socket`, JSON encoding,
subprotocol `hrana2`/`hrana3`; no batons — the socket + a client-allocated `stream_id`
identify a stream):

```elixir
# 1. Upgrade WS (subprotocol "hrana3"), then handshake:
send:  %{"type" => "hello", "jwt" => nil}          # auth disabled on the gate; jwt when on
recv:  %{"type" => "hello_ok"}

# 2. Open a stream once (client allocates the stream_id) — the persistent connection:
send:  %{"type" => "request", "request_id" => 1,
         "request" => %{"type" => "open_stream", "stream_id" => 1}}
recv:  %{"type" => "response_ok", "request_id" => 1, "response" => %{"type" => "open_stream"}}

# 3. Execute a parameterized statement — args bound as Hrana values, NEVER interpolated:
#    %Filo.Stmt{sql: "UPDATE pgbench_accounts SET abalance = abalance + ? WHERE aid = ?",
#               args: [delta, aid]}
send:  %{"type" => "request", "request_id" => 2,
         "request" => %{"type" => "execute", "stream_id" => 1,
           "stmt" => %{
             "sql"  => "UPDATE pgbench_accounts SET abalance = abalance + ? WHERE aid = ?",
             "args" => [Filo.Value.encode(delta), Filo.Value.encode(aid)]  # integer -> {"type":"integer","value":"<n>"}
           }}}
recv:  %{"type" => "response_ok", "request_id" => 2, "response" => %{"type" => "execute", "result" => %{...}}}

# 4. Burst all a transaction's statements on the held stream_id, then close at the end:
send:  %{"type" => "request", "request_id" => N,
         "request" => %{"type" => "close_stream", "stream_id" => 1}}
```

**Persistent stream.** Because WS has no baton, the stream is just the open socket plus the
client-allocated `stream_id`: open once, burst many `execute`s (a TPC-B txn = 7 in sequence,
bracketed by `BEGIN`/`COMMIT` executes), `close_stream` at the end. This is fathom's real
per-stream model (one connection per stream for the stream's life) and amortizes stream-open
over the burst — needed for the steady-state delta and throughput numbers below. `Filo.Value`
carries integers as **strings** on the wire (`%{"type" => "integer", "value" => "42"}`), text
as `%{"type" => "text", "value" => "..."}`, floats as numbers, blobs as unpadded base64.

### Client side — SECONDARY (optional): the HTTP `/v2/pipeline` client

Filo serves HTTP too, so a Req `POST /v2/pipeline` client (the shape `deploy/chaos/chaos.sh`'s
`hrana()` uses, dep-free) is a cheap **optional** variant for cross-checking the WS numbers
and for HTTP-only clients (`libsql-experimental`, the SDKs). It uses **batons** (each response
returns a fresh `baton` to reuse on the next request) instead of a `stream_id`. Kept as a
secondary because it was already specified and costs almost nothing; **the gate metric is the
WS number.**

---

## TPC-B — two framings

The workload is the **pgbench `tpcb-like` bank transaction**: seven statements per
transaction over `branches`/`tellers`/`accounts`/`history`, "inspired by TPC-B but not
actually TPC-B" (pgbench's own words — [PostgreSQL pgbench docs][pgbench]). The classic
TPC-B branch-row hot-contention story **does not apply to fathom**: each tenant is its own
single-writer SQLite file, so there is no cross-connection write contention on a shared
branch row — worth a code comment so nobody "fixes" a contention problem that can't exist.

### Framing A — wire proxy tax (`tpcb_wire_overhead_us`) — **GATED, loopback WS wire**

**What it measures.** The per-transaction cost of reaching the engine *through fathom's
full client wire* versus touching bare SQLite directly. Two legs, each on its own
freshly-seeded identical shard (run sequentially, so no connection interference):

1. **Wire leg** — the TPC-B 7-statement transaction driven through the **loopback Hrana-WS
   client** → `Filo.Socket` → baton-less `stream_id` stream → `Fathom.ShardExecutor` →
   `Fathom.Shard.Connection` → engine (WAL commit + fsync), on a **persistent stream** so
   per-stream open is amortized.
2. **Direct leg** — the identical 7 statements run **in-process on the bare engine via raw
   `Exqlite.Sqlite3`** (`prepare` → `bind` → `step`, reusing statements), on a held
   connection to the same freshly-seeded shard file (same WAL commit + fsync). **Not**
   `Fathom.Shard.Connection.query/3` — going straight to `Exqlite.Sqlite3` puts the *whole*
   of fathom's software above the engine on one side of the delta.

**Metric:** `tpcb_wire_overhead_us = p50(wire_per_txn_us) − p50(direct_engine_per_txn_us)`,
µs per transaction. It is therefore the **entire fathom tax over bare SQLite**: the WS wire
(Mint frame encode + socket) + JSON framing + `Filo.Socket` handshake/stream bookkeeping +
`Filo.Plug` upgrade + `ShardExecutor` (`wrote?`, `ShardLoad`, `to_stmt_result`) +
`Connection` (fathom's no-statement-cache re-prepare). Positive = that stack got more
expensive.

**Why the delta is gate-robust on native macOS.** Both legs execute the identical SQL,
committing the same WAL to the same file, so **both pay the same `F_FULLFSYNC` commit cost
per transaction**. The subtraction removes that shared, host-dominated term, leaving only
fathom's per-transaction wire+proxy overhead, which is CPU + loopback-socket + syscall bound
— **not** fsync-bound — so it is stable enough to gate the way `cold_open_p50_us` is.

**Honest caveat (in the metric's doc):** the loopback socket + Bandit accept + WS frame
parse add more run-to-run variance than a pure in-process delta. It is still not
fsync-dominated (the fsync cancels), and the median-of-trials + the ≥20% band absorb the
residual jitter — but expect a wider noise floor than the file-open metrics, and honor the
"rerun once to rule out noise" rule before trusting a BLOCK.

### `hrana_rt_us` — now measurable (GATED, loopback WS wire)

Rev. 1 left `hrana_rt_us` a `null` placeholder. The loopback WS client makes it real: **the
median round-trip latency of a trivial read (`SELECT 1`) through the loopback Hrana-WS wire
on a warm, open stream** — client frame encode → `Filo.Socket` → `Executor.execute` →
`response_ok` → client decode. Warm (steady-state) stream, not a stream-open, so it is the
minimal wire round-trip, not a cold-open. **`SELECT 1` writes nothing → no fsync → very
stable → gate-viable.** Add to `Gate.@metrics` (higher_worse); populated only when the wire
bench runs (else `nil` → skipped). On the chaos rig the same probe over the real network is
the true cross-network RTT (recorded, not gated — the rig is periodic, not in the commit
path).

### Framing B — multi-tenant aggregate node TPS (`tpcb_node_tps`) — **GATED (loose, ~50%)**

**What it measures.** Aggregate transactions/sec a node sustains with **N shards each
running an independent TPC-B write stream concurrently, each through its own loopback WS
stream** — fathom's real differentiator, **fan-out under genuine WRITE load**, through the
wire. Unlike the read-only `--hotspots` driver, it exercises WAL growth, the `wrote?`
dirty-flag flips, the coordinator idle checkpoint, and the durability flush on every shard at
once. Reuse the `Fathom.Scale` concurrency shape (`Task.async_stream` + a per-stream burst),
but the per-stream unit is now a **loopback WS client** holding an open `stream_id` and
bursting TPC-B transactions. Report aggregate txn/s across the node, plus (recorded for
context) per-window write-path signals from the `[:fathom, :shard, ...]` telemetry (flushes,
checkpoints, dirty→clean transitions).

- **Gate/CI:** loopback WS, **gated with a loose ~50% threshold** (see below).
- **Realism:** the chaos rig drives N concurrent **remote** libSQL clients over the network
  (`chaos.sh tpcb` shape), recorded to a chaos-run doc.

**Why the gate is loose (~50%), not the global 20%.** Absolute write throughput is the
`F_FULLFSYNC`/APFS-dominated number `docs/benchmark-plan.md` warns about — it drifts ±3× with
host load, so the standard 20% band would false-positive constantly. A **50% block** means
"ignore ordinary fsync/APFS jitter, but catch a ≈2× throughput collapse" (e.g. a flush storm,
an accidental per-row checkpoint, or a lost `write_concurrency`). Confirm a BLOCK against the
`[:fathom,:shard]` flush/checkpoint telemetry before trusting it (a genuine regression shows
a matching flush-count explosion; pure jitter does not). N is a knob (`--tpcb-shards`, default
e.g. 64–256 so a laptop run is quick); this is not a millions-of-shards test (that is
`fathom.scale --ramp`).

### TPC-B schema + seed (SF = 1 ≈ a small shard)

At pgbench scale factor 1 ([PostgreSQL pgbench docs][pgbench]): `branches` 1 row, `tellers`
10 rows, `accounts` 100,000 rows, `history` 0 rows. SF is a knob (`--tpcb-scale`, default 1)
that multiplies every table except history. SF=1 with standard filler is ~15–20 MB — squarely
a "small shard."

**Seeding is setup, not the measured workload, so it stays in-process** (a direct
`Fathom.Shard.Connection` writing into the storage dir, exactly as
`Fathom.Bench.seed_storage_shard/1` / `Fathom.Scale.provision/2` do today — then the wire
cold-open pulls it). Only the *measured* TPC transactions cross the loopback WS wire. The
`rows` count is harness-controlled (never user input), so inlining it into the recursive-CTE
seed is safe:

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
as Hrana `args` (`Filo.Value.encode`, integers as strings); through `Filo.Socket` they decode
back to native terms and bind via `Connection.query/3` → `Exqlite.Sqlite3.bind`. Random
`aid ∈ 1..100000·SF`, `tid ∈ 1..10·SF`, `bid ∈ 1..SF`, `delta ∈ −5000..5000` are drawn in
Elixir and passed as `args` — **values never touch the SQL string** (the project's
multi-tenant-safety principle: bind, never interpolate):

```elixir
alias Filo.Stmt

# BEGIN / COMMIT bracket the txn (their own execute requests on the held stream_id).
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
full WS protocol, not the executor shortcut.

### Isolation-under-write-load test (`test/`, shard-isolation gate) — through the loopback WS wire

A concurrent-write isolation test belongs under the **shard-isolation gate**
(`AGENTS.md` §Gates: any change to routing ships with a cross-shard-isolation test; a leak is
a release blocker). Driving it **through the loopback WS wire** strengthens it — it exercises
`shard_from_conn` Host routing end-to-end, the real path a leak would travel. Design
(`test/fathom/tpcb_isolation_test.exs`, tagged so it stays out of the default fast suite if
slow):

- Start the loopback Filo harness. Provision M shards with **unique ids** (real SQLite files,
  `File.rm` in `on_exit`, never share a file), seeded TPC-B (small SF) in-process.
- Drive K TPC-B transactions **per shard concurrently through the loopback WS client** (each
  connecting with `Host: <shard>.local`), `Task.async_stream`, complete via `Task` join —
  **no `Process.sleep`**.
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

## TPC-C — comparability / concurrency characterization, W = 1..5 sweep (RECORDED-ONLY)

**Why TPC-C, framed differently from TPC-B.** The most comparable peer — Turso's Rust-based
libSQL engine — publishes its OLTP concurrency story as TPC-C: **throughput plus p95/p99/max
latency *per transaction type*** over a warehouse × thread matrix. To be comparable we run the
same benchmark and report the same shape. TPC-C is heavier (9 tables, 5 weighted transaction
types) and its latencies are host- and fsync-sensitive, so it is a **comparability /
characterization** benchmark — **never a commit-gate metric**. Both variants cross the wire:

- **Loopback-WS in-process** (`mix fathom.tpcc`, gate host, recorded-only): the weighted deck
  through the loopback Hrana-WS client. A local trend line and write-path telemetry;
  single-host, so **not** a cross-network number.
- **Rig remote client over the network** (`chaos.sh tpcc`, the realism headline): a real
  remote libSQL client through the LB (subdomain → node hash, as prod), yielding the true
  per-txn-type latency + tpmC. Written to `docs/reviews/tpcc-run-<date>.md`.

### The warehouse sweep — W = 1, 2, 3, 4, 5

Instead of a single default warehouse count, **the run executes the full 5-transaction
weighted mix at each of W = 1, 2, 3, 4, 5**, reporting `tpcc_tpmc` + the per-txn
`p50/p95/p99/max` **per W value**. The output is a **W-sweep** showing how throughput and
latency scale as one tenant grows from 1→5 warehouses (a within-shard scaling curve, since a
shard = one tenant DB = its whole W-warehouse set). One shard per W value (five shards total
per run), each seeded independently and run in sequence.

**Sizing honesty:** even at `W=1`, TPC-C is ~100k items + 100k stock + ~300k order-lines —
~100 MB, the **large end** of "small shard"; `W=5` is ~5×. Fine for a *per-tenant
comparability* sweep, but **not** a fan-out-density workload. Cross-warehouse New-Order lines
stay *within* the shard — do **not** spread warehouses across shards (that would violate
fathom's single-writer-per-file model). `--tpcc-max-w` (default 5) bounds the sweep.

### Schema (9 tables)

Per warehouse `W` ([TPC-C spec / Wikipedia][tpcc-wiki]): `warehouse` W, `district` 10·W,
`customer` 30,000·W, `history` ~30,000·W, `new_order` ~9,000·W, `order` (`oorder`) 30,000·W,
`order_line` ~300,000·W, `item` **100,000 (fixed, not scaled by W)**, `stock` 100,000·W.

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

### TPC-C driver + `scripts/tpc_history.jsonl` (one row per W)

- **Loopback-WS in-process** (`mix fathom.tpcc`): for each `W ∈ 1..--tpcc-max-w`, seed a shard,
  run the weighted deck through the loopback WS client with `--tpcc-threads T` concurrent
  streams, and append **one JSON row per W** to `scripts/tpc_history.jsonl`:

  ```json
  {"ts":"…","commit":"…","host":"darwin","warehouses":1,"threads":8,
   "tpcc_tpmc":1234.0,
   "tpcc_neworder_p50_us":900,"tpcc_neworder_p95_us":2100,"tpcc_neworder_p99_us":4200,"tpcc_neworder_max_us":9000,
   "tpcc_payment_p50_us":650,"tpcc_payment_p95_us":1500, "…":"…for order_status/delivery/stock_level…"}
  ```

  Recorded-only, single-host — a trend line, not a cross-network number. Five rows per run
  (W=1..5) let the reader plot the scaling curve.
- **Rig remote client** (`chaos.sh tpcc`, shaped like `cmd_hotspots`/`cmd_smoke`): the same
  W-sweep driven by a real remote libSQL client over the network through the LB, args bound as
  Hrana `args`. The peer-comparable headline, written to `docs/reviews/tpcc-run-<date>.md`.

---

## Retrofit note — the existing gate benches and the wire

The lead's directive extends to the existing request-path gate benches. The split:

- **`cold_open` → ADD a parallel `cold_open_wire_p50_us`; the in-process metric is
  UNCHANGED.** Keep `cold_open_p50_us` exactly as it is (no redefinition, **no re-baseline**).
  Add a new **`cold_open_wire_p50_us`**: cold-open a storage-backed shard and run the first
  query by **opening a fresh loopback WS stream** to it — the checkout + pull happens inside
  `Executor.open` on that stream, so this is the real client cold-open including the wire. It
  is a brand-new metric, so its first appearance simply seeds its own baseline (nothing to
  re-baseline). GATED (higher_worse), opt-in with the wire harness (`nil` otherwise → skipped).
  The two coexist: `cold_open_p50_us` isolates the file-open cost; `cold_open_wire_p50_us` adds
  the client-visible wire constant on top.
- **`dir_resolve` → stays in-process; a wire variant is DEFERRED.** `dir_resolve_p50_us`
  remains the direct in-process measurement of `Fathom.Directory.resolve/1`, unchanged. Resolve
  is **not on the request path today** (routing is Host-based; the cached, PubSub-invalidated
  request-path resolve is still aspirational per `AGENTS.md`), so there is no client query that
  isolates it — a wire `dir_resolve` metric is deferred until that request-path resolve
  actually lands. No new metric now.
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
| `tpcb_wire_overhead_us` | µs / txn | higher_worse | **GATED (20%)** | loopback WS (delta vs raw `Exqlite.Sqlite3`) | `perf_history.jsonl` **new col** + `Gate.@metrics` | only with `--tpcb`; else `nil` |
| `hrana_rt_us` | µs | higher_worse | **GATED (20%)** | loopback WS (`SELECT 1` RT) | `perf_history.jsonl` **existing col** (was `null`) + `Gate.@metrics` | only with the wire bench; else `nil` |
| `tpcb_node_tps` | txn / s | lower_worse | **GATED (loose ~50%)** | loopback WS (gate) / remote client (rig) | `perf_history.jsonl` **new col** + `Gate.@metrics` **with a per-metric threshold** | only with `--tpcb`; else `nil` |
| `cold_open_wire_p50_us` | µs | higher_worse | **GATED (20%)** | loopback WS (cold-open + first query) | `perf_history.jsonl` **new col** + `Gate.@metrics` | only with the wire bench; else `nil` |
| `tpcc_tpmc` (per W) | New-Order / min | lower_worse | recorded-only | loopback WS (CI) / remote client (rig headline) | `scripts/tpc_history.jsonl` (**separate**, one row per W) + chaos-run doc | only via the TPC-C tasks |
| `tpcc_<txn>_p50/p95/p99/max_us` (per W) | µs | higher_worse | recorded-only | loopback WS (CI) / remote client (rig headline) | `scripts/tpc_history.jsonl` + chaos-run doc | only via the TPC-C tasks |

**Exact `perf_history.jsonl` columns to add** (three new; `hrana_rt_us` already exists and just
starts carrying a value):

```
"tpcb_wire_overhead_us": <float|null>,   // GATED 20%   (nil unless --tpcb)
"tpcb_node_tps":         <float|null>,    // GATED loose 50% (nil unless --tpcb)
"cold_open_wire_p50_us": <float|null>,    // GATED 20%   (nil unless the wire bench runs)
"hrana_rt_us":           <float|null>,    // GATED 20% — was always null; now populated by the wire bench
```

Wired in `Mix.Tasks.Fathom.Bench.build_line/3` (add the three new keys; `hrana_rt_us` already
present), `print_table/2` (add rows), and `Fathom.Bench.@all_metrics` / `all/1` (a new `:tpcb`
entry producing `tpcb_wire_overhead_us` + `tpcb_node_tps`, plus `hrana_rt_us` and
`cold_open_wire_p50_us` whenever the wire harness is up — analogous to how `:failover_rto`
produces two `failover_*` columns).

### The `Fathom.Bench.Gate` change — per-metric thresholds

Today `@metrics` entries are `{name, direction}` 2-tuples and `compare/4` applies **one**
global `block` to every metric (a single `worst >= block` verdict). The loose `tpcb_node_tps`
gate needs a **per-metric threshold**. Minimal, backward-compatible change:

```elixir
# @metrics entries gain an OPTIONAL third element (the block %); absent -> the global default.
@metrics [
  {:cold_open_p50_us, :higher_worse},
  {:cold_open_s3_p50_us, :higher_worse},
  {:cold_open_wire_p50_us, :higher_worse},   # <-- add (wire cold-open)
  {:warm_s3_shards_per_s, :lower_worse},
  {:dir_resolve_p50_us, :higher_worse},
  {:copy_rows_per_s, :lower_worse},
  {:fanout_kb_per_shard, :higher_worse},
  {:hrana_rt_us, :higher_worse},             # <-- add (loopback SELECT 1 RT; no fsync)
  {:tpcb_wire_overhead_us, :higher_worse},   # <-- add (loopback delta vs raw exqlite)
  {:tpcb_node_tps, :lower_worse, 50}         # <-- add, loose 50% override (fsync/APFS-tolerant)
]
```

`compare/4` changes from a single `worst >= block` verdict to a **per-metric** decision: each
comparable delta blocks if `d.pct >= threshold(metric)` where `threshold(metric)` is the
tuple's third element or the global `block`; the verdict is `:block` if **any** metric crosses
**its own** threshold (`:warn` similarly against a per-metric warn or the global). Normalize a
2-tuple to `{name, dir, nil}` on read so existing entries and the pure-function unit tests are
unaffected. This is a ~15-line change to one pure function + its `format/4` report (show each
metric's effective threshold).

---

## Gate integration + skip behavior on normal commits

TPC never slows down or blocks an ordinary commit.

- **Normal commit** (`scripts/commit_with_bench.sh -m "..."`) runs `scripts/benchmark.sh`
  **without** `--tpcb` and without the wire harness, so `tpcb_wire_overhead_us`,
  `tpcb_node_tps`, `cold_open_wire_p50_us`, and `hrana_rt_us` all record `nil`. Parents were
  benched the same way (`nil`). `Gate.compare/4` skips any metric `nil` in either run → **none
  of the wire metrics participate** in a normal gate, and the bench stays as fast as today
  (`cold_open_p50_us` and the other in-process metrics still gate exactly as now).
- **To gate on the wire metrics:** run `scripts/benchmark.sh --tpcb` (which starts the loopback
  Filo + WS-client harness and produces the four wire metrics) on **both** the parent (to seed a
  non-`nil` baseline) and the working tree. Identical to how the opt-in S3 metrics gate only
  when `FATHOM_S3_TEST_*` is set on both runs. `tpcb_node_tps` gates at its own 50%; the other
  three at the global 20%.
- **`--tpcb`** is a new pass-through flag on `mix fathom.bench` (add to `@switches` /
  `@all_metrics` / `parse_only`) and on `scripts/benchmark.sh` (`"$@"` already forwards it). It
  starts the loopback WS harness (needed for all four wire metrics).
- **TPC-C** is **not** on `mix fathom.bench` — a separate `mix fathom.tpcc` (loopback WS,
  recorded-only, W-sweep) and `chaos.sh tpcc` (remote, recorded-only). It writes
  `scripts/tpc_history.jsonl`, never `perf_history.jsonl`, so it cannot affect the gate.
- The host-wide benchmark lock in `mix fathom.bench` still applies — a TPC-B run holds it like
  any other. The loopback listener binds `port: 0` (OS-assigned), so parallel unrelated
  processes don't collide on a fixed Hrana port.

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
| **0 — prereq ✅ DONE** | Fixed `Connection.collect` (`++` → O(R)) + `@tag :bench` large-result regression test; committed through the bench gate (`23b5b6f`). | ~0.5 day |
| **1 — loopback WS harness + wire gate metrics ✅ DONE** | Built `Fathom.Bench.HranaClient` (`test/support`, `Mint.WebSocket`, dev/test only) — starts `Filo.Streams` + a Bandit `Filo.Plug` listener on `127.0.0.1:0`, drives hello → open_stream → execute → close_stream with `Filo.Value` encode/decode, shard via `Host: <shard>.local`. `Fathom.Bench.Wire` + the **`MIX_ENV=test mix fathom.wire_bench`** task emit `hrana_rt_us`, `cold_open_wire_p50_us`, `tpcb_wire_overhead_us` (Framing A: TPC-B seed + 7-stmt deck, WS-vs-raw-exqlite delta with a reused-prepared baseline). **As-built vs the original row:** gating did NOT touch `Fathom.Bench.Gate`/`perf_history` — the wire metrics live in the gitignored `scripts/wire_history.jsonl` and are gated by the task's `--check` (last-same-host, block ≥20%), because the client dep is dev/test-only and the prod gate runs `MIX_ENV=prod`. The per-metric-threshold `Gate` extension moves to Phase 2 (it's only needed for the loose `tpcb_node_tps` gate). | ~3.5–4 days |
| **2 — TPC-B aggregate + isolation (WS)** | `tpcb_node_tps` (Framing B) gated-loose via concurrent loopback WS streams; the concurrent-write **isolation-under-write-load** test through the loopback WS wire, under the shard-isolation gate. | ~2 days |
| **3 — TPC-C loopback WS, W = 1..5 sweep** | `mix fathom.tpcc`: 9-table schema + seed, all 5 weighted txn profiles, warehouse×thread matrix over the WS client, run at **each W ∈ 1..5**, per-txn-type `p50/p95/p99/max` + `tpmC` **per W** → `scripts/tpc_history.jsonl` (one row per W). Recorded-only. (5× the seed+run work of a single-W run.) | ~4.5–5.5 days |
| **4 — remote-client realism (rig headline)** | `chaos.sh tpcb` + `chaos.sh tpcc`: real remote libSQL client over the network through the LB (true cross-network RTT + the per-txn-type latency + tpmC W-sweep), written to `docs/reviews/tpc*-run-<date>.md`. | ~2.5–3.5 days |

Phases 0–1 deliver the entire *gated* value (a robust wire write-path delta, a real
`hrana_rt_us`, a wire cold-open, and the per-metric-threshold mechanism) in ~4 days, including
the shared loopback WS harness + client every later phase reuses; 2 adds the multi-tenant write
story (loose-gated) and the isolation guarantee; 3–4 are the heavier comparability sweep and the
remote-client realism headline, and can follow independently.

---

## What this does NOT tell us about fathom (kept honest)

- **The WS loopback crosses django-libsql's real transport, but it is not a cross-network
  RTT.** The gate benches open a real WebSocket through Bandit + `Filo.Socket` — the exact
  transport `django-libsql` uses — with real framing, encode/decode, handshake, and stream
  bookkeeping, but over `127.0.0.1`: ~µs link latency, no bandwidth-delay, congestion, TLS, or
  LB hop. So `hrana_rt_us`, `tpcb_wire_overhead_us`, and `cold_open_wire_p50_us` on the gate are
  the **software** wire cost, not what a client in another region pays. **Only the chaos-rig
  remote-client run gives the true cross-network RTT** — which is why the rig numbers are the
  realism headline and the loopback numbers are the reproducible gate.
- **The loose `tpcb_node_tps` gate is a smoke alarm, not a precision instrument.** At a 50%
  threshold it catches a ≈2× throughput collapse, not a real 15% loss — and it can still
  false-positive on a badly loaded box. It is deliberately sized to tolerate the
  `F_FULLFSYNC`/APFS jitter that makes absolute write TPS untrustworthy; confirm any BLOCK
  against the flush/checkpoint telemetry before acting.
- **TPC re-measures SQLite + the host, not fathom.** Both benchmarks are, at bottom, a
  measurement of the SQLite write engine and the machine's `F_FULLFSYNC`/WAL/APFS behavior.
  Absolute TPC-B TPS and all TPC-C latencies move with host load and fsync policy, not fathom
  code — which is why the two clean gates are the *cancelling* overhead delta and the *no-fsync*
  `SELECT 1` round-trip, and the one absolute gate is deliberately loose.
- **It says nothing about fathom's actual differentiators.** Cold-open latency, fan-out density
  across *millions* of shards, cross-node lease/epoch/heartbeat correctness, migration copy
  throughput, warm-standby RTO — fathom's whole reason to exist — are **already** measured by
  `mix fathom.scale` and the gated `fathom.bench` metrics. TPC adds the missing *write-path*
  dimension on a single (or a modest fan-out of) tenant DBs; it is not, and should not be sold
  as, a fathom-scale test.
- **TPC-C comparability is per-tenant.** The W=1..5 sweep characterizes *one tenant DB's* OLTP
  concurrency as it grows; even over the rig's remote client it does not stress the LB
  partition, the S3 lease, or the directory across shards. It answers "how does a single fathom
  shard compare to peer X on TPC-C over the wire, 1→5 warehouses," not "how does fathom's
  architecture scale."

---

## Sources

- pgbench `tpcb-like` transaction (7 statements, tables, scale factor 1 = 1/10/100000/0 rows):
  [PostgreSQL documentation — pgbench][pgbench].
- TPC-C schema (9 tables, per-warehouse cardinality), transaction mix (New-Order ~45%, Payment
  ~43%, Order-Status/Delivery/Stock-Level ~4% each; one Delivery/Order-Status/Stock-Level per ten
  New-Orders), New-Order + Payment behavior: [TPC-C — Wikipedia][tpcc-wiki] and the [TPC-C chapter
  of the Benchmark Handbook (Jim Gray)][graybook].
- Hrana WebSocket wire (loopback client target): `Filo.Socket` — WebSock handler, subprotocol
  `hrana2`/`hrana3`, `hello`/`hello_ok` handshake, `request`/`response_ok` frames with
  `request_id`, `open_stream` (client-allocated `stream_id`)/`execute`/`close_stream`, no
  batons; and `Filo.Value` value encoding (integer carried as a string). The `../filo` source;
  the pure-Elixir WS handler is driven directly in `../filo/test/filo/socket_test.exs`, and the
  real-network WS path is exercised by shelling to Python `libsql-client` in
  `../filo/test/filo/integration_ws_test.exs`. The HTTP `/v2/pipeline` secondary follows
  `deploy/chaos/chaos.sh`'s `hrana()`.

[pgbench]: https://www.postgresql.org/docs/current/pgbench.html
[tpcc-wiki]: https://en.wikipedia.org/wiki/TPC-C
[graybook]: https://jimgray.azurewebsites.net/BenchmarkHandbook/chapter12.pdf

---

## Decisions (resolved by the lead, 2026-07-10)

These were open questions in earlier revisions; they are now settled defaults for the
implementation. They can still be revisited if reality contradicts them (noted per item).

1. **WS client dep — DECIDED: `mint_web_socket`, `only: [:dev, :test]`.** The in-process
   Hrana-WS client builds on `Mint.WebSocket` (Mint is already in the lock via Finch/Req), so
   this adds only the thin `mint_web_socket` package, bench/test-scoped — prod is untouched.
   Rejected: `:gun` (heavier, pulls `cowlib`) and shelling to an external client (defeats the
   in-process, reproducible goal).
2. **WS encoding on the gate — DECIDED: JSON only (`hrana2`/`hrana3`).** The gate client speaks
   JSON (matches `Filo`'s `socket_test.exs`), which is what exercises the wire path meaningfully.
   The `hrana3-protobuf` binary path is deferred to a rig run / follow-on (only relevant if the
   encode cost itself ever becomes a question).
3. **`tpcb_node_tps` loose gate — DECIDED: 50%, via the `{name, direction, threshold}`
   `@metrics` shape.** 50% blocks a ≈2× collapse while tolerating `F_FULLFSYNC`/APFS run-to-run
   jitter. The `Gate` extension normalizes existing 2-tuples to the global 20% default, so no
   other metric changes. (Revisit only if flush/checkpoint telemetry shows the real variance is
   wider than 50%.)
4. **`cold_open_wire_p50_us` gate band — DECIDED: the standard 20%.** It is pull-dominated + a
   small wire constant (like `cold_open`), so it starts on the global band; loosen it via the
   same per-metric mechanism **only if** the added WS variance proves jittery in practice — do
   not pre-loosen.
