defmodule Fathom.TpcbIsolationTest do
  @moduledoc """
  Shard-isolation gate (AGENTS.md §Gates), exercised under concurrent TPC-B **write** load
  through the full loopback Hrana-WS wire — the real path a leak would travel.

  M shards, each with a unique id, are driven concurrently: every shard runs K pgbench-style
  bank transactions on its own WebSocket stream, routed purely by the `Host: <shard>.local`
  subdomain (`Fathom.ShardExecutor.shard_from_conn/1`), exactly as a real client routes. Each
  shard then reads its own state back **through the same wire** and we assert two invariants
  (pin the invariant, not just the repro):

    1. **TPC-B consistency, per shard.** With every balance starting at 0,
       `sum(branches.bbalance) == sum(tellers.tbalance) == sum(accounts.abalance) ==
       sum(history.delta)` — proves every write landed, atomically, on the right shard's file.
    2. **Cross-shard non-contamination.** Each shard tags its `history.filler` with its own id;
       every shard's history must have ZERO foreign-tagged rows and exactly K rows. A routing
       leak A→B shows up as B's file carrying A-tagged rows (foreign > 0) and an over-count
       (total > K). Unique ids per shard make a leak in any direction observable somewhere.

  This is the concurrent-write analogue of the chaos rig's `foreign` isolation check. It runs in
  the default suite (isolation is a release blocker, not an opt-in bench), sized small to stay
  fast. Deterministic per-shard PRNG so a failure reproduces.
  """
  use ExUnit.Case, async: false

  alias Fathom.Bench.HranaClient
  alias Fathom.Shards

  @shards 6
  @txns_per_shard 15
  @accounts 100
  @tellers 10

  # TPC-B schema at a tiny scale (1 branch, 10 tellers, 100 accounts). history carries a
  # `filler` tagged with the writing shard's id — the contamination probe.
  @schema [
    "CREATE TABLE branches (bid INTEGER PRIMARY KEY, bbalance INTEGER)",
    "CREATE TABLE tellers (tid INTEGER PRIMARY KEY, bid INTEGER, tbalance INTEGER)",
    "CREATE TABLE accounts (aid INTEGER PRIMARY KEY, bid INTEGER, abalance INTEGER)",
    "CREATE TABLE history (tid INTEGER, bid INTEGER, aid INTEGER, delta INTEGER, filler TEXT)",
    "INSERT INTO branches (bid, bbalance) VALUES (1, 0)",
    "WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM seq WHERE i < #{@tellers}) " <>
      "INSERT INTO tellers (tid, bid, tbalance) SELECT i, 1, 0 FROM seq",
    "WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM seq WHERE i < #{@accounts}) " <>
      "INSERT INTO accounts (aid, bid, abalance) SELECT i, 1, 0 FROM seq"
  ]

  setup do
    {:ok, sup, port} = HranaClient.start_listener()
    ids = for _ <- 1..@shards, do: "tpcb_iso_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Enum.each(ids, fn id ->
        Shards.drain(id, 5_000)
        rm_shard(id)
      end)

      HranaClient.stop_listener(sup)
    end)

    {:ok, port: port, ids: ids}
  end

  test "concurrent TPC-B write load stays isolated per shard, over the wire", %{
    port: port,
    ids: ids
  } do
    results =
      ids
      |> Enum.with_index()
      |> Task.async_stream(fn {id, idx} -> drive_shard(port, id, idx) end,
        max_concurrency: @shards,
        timeout: :infinity
      )
      |> Enum.map(fn {:ok, r} -> r end)

    for r <- results do
      # Invariant 1: TPC-B consistency — all four running totals agree and equal the deltas this
      # shard applied. A missing/misrouted write breaks the equality.
      assert r.sum_b == r.expected,
             "#{r.id}: sum(bbalance) #{r.sum_b} != applied deltas #{r.expected}"

      assert r.sum_t == r.expected,
             "#{r.id}: sum(tbalance) #{r.sum_t} != applied deltas #{r.expected}"

      assert r.sum_a == r.expected,
             "#{r.id}: sum(abalance) #{r.sum_a} != applied deltas #{r.expected}"

      assert r.sum_h == r.expected,
             "#{r.id}: sum(history.delta) #{r.sum_h} != applied deltas #{r.expected}"

      # Invariant 2: non-contamination — no other shard's writes leaked into this shard's file.
      assert r.foreign == 0,
             "#{r.id}: #{r.foreign} foreign-tagged history rows — cross-shard leak"

      assert r.total == @txns_per_shard,
             "#{r.id}: history has #{r.total} rows, expected #{@txns_per_shard}"
    end
  end

  # One shard: connect (Host-routed), seed, burst K txns, read the invariants back on the same
  # stream, close. Returns the assertion inputs for the parent to check.
  defp drive_shard(port, id, idx) do
    # Deterministic per-shard stream so any failure reproduces.
    :rand.seed(:exsss, {idx + 1, 7919, 104_729})

    {:ok, c} = HranaClient.connect(port, id)
    c = Enum.reduce(@schema, c, fn sql, c -> exec(c, sql) end)

    {c, expected} =
      Enum.reduce(1..@txns_per_shard, {c, 0}, fn _, {c, sum} ->
        aid = :rand.uniform(@accounts)
        tid = :rand.uniform(@tellers)
        delta = :rand.uniform(10_001) - 5001
        {run_txn(c, id, aid, tid, delta), sum + delta}
      end)

    {c, sum_b} = scalar(c, "SELECT COALESCE(SUM(bbalance), 0) FROM branches")
    {c, sum_t} = scalar(c, "SELECT COALESCE(SUM(tbalance), 0) FROM tellers")
    {c, sum_a} = scalar(c, "SELECT COALESCE(SUM(abalance), 0) FROM accounts")
    {c, sum_h} = scalar(c, "SELECT COALESCE(SUM(delta), 0) FROM history")
    {c, foreign} = scalar(c, "SELECT COUNT(*) FROM history WHERE filler <> ?", [id])
    {c, total} = scalar(c, "SELECT COUNT(*) FROM history")
    HranaClient.close(c)

    %{
      id: id,
      expected: expected,
      sum_b: sum_b,
      sum_t: sum_t,
      sum_a: sum_a,
      sum_h: sum_h,
      foreign: foreign,
      total: total
    }
  end

  # One pgbench bank txn, tagging history.filler with the shard id. All values bound, never
  # interpolated. bid is fixed (a single branch), so there is no cross-connection contention on
  # it — each tenant is its own single-writer file.
  defp run_txn(c, shard_id, aid, tid, delta) do
    c
    |> exec("BEGIN")
    |> exec("UPDATE accounts SET abalance = abalance + ? WHERE aid = ?", [delta, aid])
    |> exec("UPDATE tellers SET tbalance = tbalance + ? WHERE tid = ?", [delta, tid])
    |> exec("UPDATE branches SET bbalance = bbalance + ? WHERE bid = 1", [delta])
    |> exec("INSERT INTO history (tid, bid, aid, delta, filler) VALUES (?, 1, ?, ?, ?)", [
      tid,
      aid,
      delta,
      shard_id
    ])
    |> exec("COMMIT")
  end

  defp exec(c, sql, args \\ []) do
    {:ok, c, _} = HranaClient.execute(c, sql, args)
    c
  end

  defp scalar(c, sql, args \\ []) do
    {:ok, c, %{rows: [[v]]}} = HranaClient.execute(c, sql, args)
    {c, v}
  end

  defp rm_shard(id) do
    for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
        s <- ["", "-wal", "-shm"] do
      File.rm(Path.join([dir, "#{id}.db"]) <> s)
    end
  end
end
