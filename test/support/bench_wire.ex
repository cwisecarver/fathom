defmodule Fathom.Bench.Wire do
  @moduledoc """
  The **wire benches** (Phase 1, docs/tpc-benchmark-plan.md): metrics measured through the
  full Hrana WebSocket path — a real `Fathom.Bench.HranaClient` (Mint.WebSocket) → Filo's
  `Filo.Socket` → `Fathom.ShardExecutor` → the shard → back. This is what a real client
  (django-libsql) actually pays, vs the in-process `Fathom.Bench` metrics which stop at
  `ShardExecutor.execute`.

  Test-env only (it uses the dev/test `mint_web_socket` client), so it never runs in the
  prod per-commit gate; it runs via `MIX_ENV=test mix fathom.wire_bench`. The numbers are the
  wire *software* cost (localhost — µs link, no bandwidth-delay/TLS/LB hop), not a
  cross-network RTT (the chaos rig gives that).

  Assumes the shard tree + Local storage are already up — true under `mix test` (the app is
  started) and under the wire-bench task (which runs `app.start` first).
  """

  alias Exqlite.Sqlite3
  alias Fathom.Bench.HranaClient
  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.Shards

  @hrana_rt_samples 200
  @cold_open_wire_samples 30
  @tpcb_txns 500
  @tpcb_accounts 100_000
  @tpcb_tellers 10
  @tpcb_warmup 5

  # Framing B (tpcb_node_tps) fan-out knobs. A smaller account table than SF=1 keeps the
  # in-process seed cheap while still dirtying distinct pages per txn; both are `mix
  # fathom.wire_bench` switches (--tpcb-shards / --tpcb-node-txns).
  @tpcb_node_shards 16
  @tpcb_node_txns 50
  @tpcb_node_accounts 10_000

  @doc """
  `hrana_rt_us` — median µs of a warm-stream `SELECT 1` round-trip over the wire. A read that
  issues no fsync, so it isolates the software wire cost (WS framing + `Filo.Value` decode,
  `Filo.Socket` routing, `Request.handle`, `ShardExecutor.execute`, response encode) with no
  storage noise — stable enough to gate.
  """
  @spec hrana_rt(keyword()) :: float()
  def hrana_rt(opts \\ []) do
    samples = Keyword.get(opts, :hrana_rt_samples, @hrana_rt_samples)

    with_listener(fn port ->
      shard = uniq("wire_rt")

      try do
        {:ok, c} = HranaClient.connect(port, shard)
        # Warm-up (opens the shard + primes the stream) — not timed.
        {:ok, c, _} = HranaClient.execute(c, "SELECT 1")

        {c, us} =
          Enum.reduce(1..samples, {c, []}, fn _, {c, acc} ->
            {t, {:ok, c, _}} = :timer.tc(fn -> HranaClient.execute(c, "SELECT 1") end)
            {c, [t | acc]}
          end)

        HranaClient.close(c)
        p50(us)
      after
        rm_shard(shard)
      end
    end)
  end

  @doc """
  `cold_open_wire_p50_us` — median µs for a client to first-query a **cold** shard over the
  wire: a fresh WS connect whose `open_stream` triggers the cold `Shards.checkout` (pull from
  storage + coordinator start), then the first `execute`. The wire parallel to the in-process
  `Fathom.Bench.cold_open/1`; it adds the WS upgrade/hello + framing a real client pays on a
  new connection. Each sample seeds a fresh shard into storage and drops its local copy, so
  every open is genuinely cold.
  """
  @spec cold_open_wire(keyword()) :: float()
  def cold_open_wire(opts \\ []) do
    samples = Keyword.get(opts, :cold_open_wire_samples, @cold_open_wire_samples)

    with_listener(fn port ->
      # Warm code paths once (module/NIF/WS load), not timed.
      warm = uniq("wire_co_warm")
      seed_cold_shard(warm)
      cold_first_query(port, warm)
      teardown_cold(warm)

      1..samples
      |> Enum.map(fn _ ->
        id = uniq("wire_co")
        seed_cold_shard(id)
        {us, :ok} = :timer.tc(fn -> cold_first_query(port, id) end)
        teardown_cold(id)
        us
      end)
      |> p50()
    end)
  end

  # A fresh connection to a cold shard: connect (upgrade + hello + open_stream → cold checkout)
  # then the first query. Closes the stream; returns :ok.
  defp cold_first_query(port, shard) do
    {:ok, c} = HranaClient.connect(port, shard)
    {:ok, c, _} = HranaClient.execute(c, "SELECT 1")
    HranaClient.close(c)
    :ok
  end

  # Put a shard into storage but leave no local copy or coordinator, so the next open is a
  # genuine cold pull (mirrors Fathom.Bench.seed_storage_shard/1). Uses a `.seed` temp so the
  # coordinator's own `<id>.db` path stays absent.
  defp seed_cold_shard(id) do
    # The .seed is a throwaway local source for the Storage.flush PUT (the coordinator later
    # pulls object `id` to its own data dir), so any writable path works — don't depend on
    # :shard_data_dir, which is nil in test (the coordinator defaults it internally).
    tmp = Path.join(System.tmp_dir!(), "#{id}.seed")
    drop_local(tmp)
    {:ok, conn} = Connection.open(tmp)
    :ok = Connection.exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    :ok = Connection.exec(conn, "INSERT INTO t DEFAULT VALUES")
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok = Storage.flush(id, tmp)
    drop_local(tmp)
  end

  # Stop the coordinator (flush + drop local + release lease) so nothing lingers between
  # samples, then best-effort remove the storage-side seed.
  defp teardown_cold(id) do
    Shards.drain(id, 5_000)
    rm_shard(id)
  end

  defp drop_local(path), do: Enum.each(["", "-wal", "-shm"], &File.rm(path <> &1))

  @doc """
  `tpcb_wire_overhead_us` — the per-TPC-B-transaction tax fathom adds over bare SQLite:
  `p50(wire_per_txn) − p50(direct_per_txn)`. Both legs run the identical pgbench-style
  7-statement bank txn (BEGIN, UPDATE accounts/tellers/branches, SELECT, INSERT history,
  COMMIT) at scale factor 1, on identically-seeded + identically-PRAGMA'd shards (WAL +
  busy_timeout, synchronous FULL) — so the COMMIT fsync cost is the same in both and cancels
  in the subtraction, leaving the wire + framing + `Filo.Socket` + `ShardExecutor` +
  `Connection` cost (incl. Connection's no-cache re-prepare, since the direct leg reuses its
  prepared statements — decision 4a).

  The wire leg sends the 7 statements as 7 separate `execute`s on one stream (a chatty,
  non-batching client — the realistic django-libsql shape), so the delta is dominated by the
  7 wire round-trips per txn. All SQL is parameterized (bound `?`), never interpolated.
  """
  @spec tpcb_wire_overhead(keyword()) :: float()
  def tpcb_wire_overhead(opts \\ []) do
    n = Keyword.get(opts, :tpcb_txns, @tpcb_txns)
    wire_p50 = with_listener(fn port -> tpcb_wire_leg(port, n) end)
    direct_p50 = tpcb_direct_leg(n)
    wire_p50 - direct_p50
  end

  # WIRE leg: seed a shard over the wire, then time N txns sent as 7 executes each.
  defp tpcb_wire_leg(port, n) do
    shard = uniq("tpcb_wire")

    try do
      {:ok, c} = HranaClient.connect(port, shard)
      c = Enum.reduce(tpcb_schema_sql(), c, fn sql, c -> exec_wire(c, sql) end)

      # Warm the path (re-prepare, JIT) before timing.
      c = Enum.reduce(1..@tpcb_warmup, c, fn _, c -> run_tpcb_txn_wire(c, tpcb_rand_args()) end)

      {_c, us} =
        Enum.reduce(1..n, {c, []}, fn _, {c, acc} ->
          args = tpcb_rand_args()
          {t, c} = :timer.tc(fn -> run_tpcb_txn_wire(c, args) end)
          {c, [t | acc]}
        end)

      p50(us)
    after
      Shards.drain(shard, 5_000)
      rm_shard(shard)
    end
  end

  defp exec_wire(c, sql) do
    {:ok, c, _} = HranaClient.execute(c, sql)
    c
  end

  defp run_tpcb_txn_wire(c, {aid, tid, delta, mtime}) do
    c = exec_wire(c, "BEGIN")

    {:ok, c, _} =
      HranaClient.execute(c, "UPDATE accounts SET abalance=abalance+? WHERE aid=?", [delta, aid])

    {:ok, c, _} = HranaClient.execute(c, "SELECT abalance FROM accounts WHERE aid=?", [aid])

    {:ok, c, _} =
      HranaClient.execute(c, "UPDATE tellers SET tbalance=tbalance+? WHERE tid=?", [delta, tid])

    {:ok, c, _} =
      HranaClient.execute(c, "UPDATE branches SET bbalance=bbalance+? WHERE bid=1", [delta])

    {:ok, c, _} =
      HranaClient.execute(
        c,
        "INSERT INTO history (tid,bid,aid,delta,mtime) VALUES (?,1,?,?,?)",
        [tid, aid, delta, mtime]
      )

    exec_wire(c, "COMMIT")
  end

  # DIRECT leg: raw Exqlite on a matching-PRAGMA file; reuse prepared statements so
  # Connection's per-call re-prepare shows up in the delta (not cancelled here).
  defp tpcb_direct_leg(n) do
    path = Path.join(System.tmp_dir!(), "#{uniq("tpcb_direct")}.db")
    drop_local(path)
    {:ok, conn} = Sqlite3.open(path)
    :ok = Sqlite3.execute(conn, "PRAGMA journal_mode=WAL")
    :ok = Sqlite3.execute(conn, "PRAGMA busy_timeout=5000")
    Enum.each(tpcb_schema_sql(), &(:ok = Sqlite3.execute(conn, &1)))

    stmts = %{
      upd_acc: prep(conn, "UPDATE accounts SET abalance=abalance+? WHERE aid=?"),
      sel_acc: prep(conn, "SELECT abalance FROM accounts WHERE aid=?"),
      upd_tel: prep(conn, "UPDATE tellers SET tbalance=tbalance+? WHERE tid=?"),
      upd_bra: prep(conn, "UPDATE branches SET bbalance=bbalance+? WHERE bid=1"),
      ins_hist: prep(conn, "INSERT INTO history (tid,bid,aid,delta,mtime) VALUES (?,1,?,?,?)")
    }

    try do
      Enum.each(1..@tpcb_warmup, fn _ -> run_tpcb_txn_direct(conn, stmts, tpcb_rand_args()) end)

      1..n
      |> Enum.map(fn _ ->
        args = tpcb_rand_args()
        {t, :ok} = :timer.tc(fn -> run_tpcb_txn_direct(conn, stmts, args) end)
        t
      end)
      |> p50()
    after
      Enum.each(stmts, fn {_k, s} -> Sqlite3.release(conn, s) end)
      Sqlite3.close(conn)
      drop_local(path)
    end
  end

  defp run_tpcb_txn_direct(conn, s, {aid, tid, delta, mtime}) do
    :ok = Sqlite3.execute(conn, "BEGIN")
    raw_run(conn, s.upd_acc, [delta, aid])
    raw_run(conn, s.sel_acc, [aid])
    raw_run(conn, s.upd_tel, [delta, tid])
    raw_run(conn, s.upd_bra, [delta])
    raw_run(conn, s.ins_hist, [tid, aid, delta, mtime])
    :ok = Sqlite3.execute(conn, "COMMIT")
    :ok
  end

  defp prep(conn, sql) do
    {:ok, stmt} = Sqlite3.prepare(conn, sql)
    stmt
  end

  # Bind + step-to-done + reset, so the prepared statement is reusable next txn.
  defp raw_run(conn, stmt, args) do
    :ok = Sqlite3.bind(stmt, args)
    drain_steps(conn, stmt)
    :ok = Sqlite3.reset(stmt)
  end

  defp drain_steps(conn, stmt) do
    case Sqlite3.step(conn, stmt) do
      :done -> :ok
      {:row, _} -> drain_steps(conn, stmt)
    end
  end

  # TPC-B schema + SF=1 seed (1 branch, 10 tellers, 100k accounts), as parameterless DDL/seed
  # statements (row counts are harness constants, not user input).
  defp tpcb_schema_sql, do: tpcb_schema_sql(@tpcb_accounts)

  # Parameterized on the account count so the SF=1 wire-overhead leg keeps its frozen 100k
  # baseline while the node-TPS fan-out seeds a smaller table — same schema, cheaper seed.
  defp tpcb_schema_sql(accounts) do
    [
      "CREATE TABLE branches (bid INTEGER PRIMARY KEY, bbalance INTEGER)",
      "CREATE TABLE tellers (tid INTEGER PRIMARY KEY, bid INTEGER, tbalance INTEGER)",
      "CREATE TABLE accounts (aid INTEGER PRIMARY KEY, bid INTEGER, abalance INTEGER)",
      "CREATE TABLE history (tid INTEGER, bid INTEGER, aid INTEGER, delta INTEGER, mtime TEXT)",
      "INSERT INTO branches (bid, bbalance) VALUES (1, 0)",
      seq_insert("tellers (tid, bid, tbalance)", "i, 1, 0", @tpcb_tellers),
      seq_insert("accounts (aid, bid, abalance)", "i, 1, 0", accounts)
    ]
  end

  defp seq_insert(target, select, n) do
    "WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM seq WHERE i < #{n}) " <>
      "INSERT INTO #{target} SELECT #{select} FROM seq"
  end

  defp tpcb_rand_args do
    {:rand.uniform(@tpcb_accounts), :rand.uniform(@tpcb_tellers), :rand.uniform(10_001) - 5001,
     Integer.to_string(System.os_time(:second))}
  end

  @doc """
  `tpcb_node_tps` (Framing B) — aggregate TPC-B write transactions/sec a single node sustains
  with `--tpcb-shards` shards each running an independent TPC-B write stream **concurrently
  through its own loopback WS stream**. Fathom's real differentiator: fan-out under genuine
  WRITE load through the wire — WAL growth, the `wrote?` dirty-flag flips, the coordinator idle
  checkpoint, and the durability flush, on every shard at once. Each shard is seeded in-process
  into storage (setup, untimed); the timed window is the concurrent burst of `--tpcb-node-txns`
  txns/shard, each the same 7-statement pgbench bank txn as `tpcb_wire_overhead`.

  Loose-gated (~50%) in `mix fathom.wire_bench` because absolute write throughput is
  F_FULLFSYNC/APFS-dominated — the band tolerates ordinary fsync jitter but catches a ≈2×
  collapse (a flush storm, a per-row checkpoint, or a lost `write_concurrency`). Confirm any
  BLOCK against the `[:fathom, :shard]` flush/checkpoint telemetry before trusting it.

  There is no cross-connection write contention to model (TPC-B's classic hot branch row): each
  tenant is its own single-writer SQLite file, so the branch UPDATE never contends across shards.
  """
  @spec tpcb_node_tps(keyword()) :: float()
  def tpcb_node_tps(opts \\ []) do
    shards = Keyword.get(opts, :tpcb_shards, @tpcb_node_shards)
    txns_per = Keyword.get(opts, :tpcb_node_txns, @tpcb_node_txns)

    with_listener(fn port ->
      ids = for _ <- 1..shards, do: uniq("tpcb_node")

      # Seed each shard into storage in-process, concurrently — setup, NOT the measured workload.
      ids
      |> Task.async_stream(&seed_tpcb_storage_shard/1,
        max_concurrency: shards,
        timeout: :infinity
      )
      |> Stream.run()

      # Timed window: every shard bursts its txns concurrently, each on its own WS stream. The
      # per-shard cold-open (pull from storage on the first execute) is inside the window but
      # amortized over the burst, so it's negligible at the default txn count.
      {us, :ok} =
        :timer.tc(fn ->
          ids
          |> Task.async_stream(&node_burst(port, &1, txns_per),
            max_concurrency: shards,
            timeout: :infinity
          )
          |> Stream.run()
        end)

      Enum.each(ids, fn id ->
        Shards.drain(id, 5_000)
        rm_shard(id)
      end)

      shards * txns_per / (us / 1_000_000)
    end)
  end

  # One shard's stream: cold-open over the wire, burst `n` TPC-B txns, close.
  defp node_burst(port, id, n) do
    {:ok, c} = HranaClient.connect(port, id)
    c = Enum.reduce(1..n, c, fn _, c -> run_tpcb_txn_wire(c, node_rand_args()) end)
    HranaClient.close(c)
  end

  # Like tpcb_rand_args/0 but draws aid within the smaller node-TPS account table.
  defp node_rand_args do
    {:rand.uniform(@tpcb_node_accounts), :rand.uniform(@tpcb_tellers),
     :rand.uniform(10_001) - 5001, Integer.to_string(System.os_time(:second))}
  end

  # Seed one shard's TPC-B schema + data into storage in-process (a throwaway .seed source
  # flushed to Storage), leaving no local copy — the timed wire open then cold-pulls it. Mirrors
  # seed_cold_shard/1, but with the TPC-B schema at the node-TPS account count.
  defp seed_tpcb_storage_shard(id) do
    tmp = Path.join(System.tmp_dir!(), "#{id}.seed")
    drop_local(tmp)
    {:ok, conn} = Connection.open(tmp)
    Enum.each(tpcb_schema_sql(@tpcb_node_accounts), &(:ok = Connection.exec(conn, &1)))
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)
    :ok = Storage.flush(id, tmp)
    drop_local(tmp)
  end

  # --- shared harness ------------------------------------------------------

  defp with_listener(fun) do
    {:ok, sup, port} = HranaClient.start_listener()

    try do
      fun.(port)
    after
      HranaClient.stop_listener(sup)
    end
  end

  defp uniq(prefix), do: "#{prefix}_#{System.unique_integer([:positive])}"

  defp rm_shard(id) do
    for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
        s <- ["", "-wal", "-shm"] do
      File.rm(Path.join([dir, "#{id}.db"]) <> s)
    end
  end

  # Interpolating p50 (matches the in-process bench's method; kept tiny + local to avoid a
  # cross-module dependency for a one-liner).
  defp p50([]), do: 0.0

  defp p50(samples) do
    sorted = Enum.sort(samples)
    n = length(sorted)
    rank = 0.5 * (n - 1)
    lo = trunc(rank)
    hi = min(lo + 1, n - 1)
    frac = rank - lo
    (Enum.at(sorted, lo) * (1 - frac) + Enum.at(sorted, hi) * frac) / 1.0
  end
end
