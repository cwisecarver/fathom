defmodule Fathom.ShardLoadTest do
  @moduledoc """
  Per-shard load counters (the Phase-2 rebalancing prerequisite): checkout rate +
  query cost per shard, recorded lock-free on the hot path and read by a future
  control plane. Not async — the load table is a shared named ETS table.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, ShardLoad, Shards}
  alias Filo.Stmt

  setup do
    prev = Application.get_env(:fathom, :shard_load)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_load, true)
    ShardLoad.reset()

    on_exit(fn ->
      ShardLoad.reset()
      restore(:shard_load, prev)
      restore(:shard_idle_ms, prev_idle)
    end)

    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)

  defp uniq(p), do: "#{p}_#{System.unique_integer([:positive])}"
  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  defp clean_shard(id) do
    for base <- [Path.join(local_dir(), "#{id}.db"), Path.join(remote_dir(), "#{id}.db")],
        suffix <- ["", "-wal", "-shm"],
        do: File.rm(base <> suffix)

    File.rm(Path.join(remote_dir(), "#{id}.lock"))
  end

  # ── the counter API ──

  test "record_checkout and record_query accumulate per shard" do
    ShardLoad.record_checkout("a")
    ShardLoad.record_checkout("a")
    ShardLoad.record_query("a", 5, 0)
    ShardLoad.record_query("a", 0, 3)

    assert %{shard_id: "a", checkouts: 2, queries: 2, rows_read: 5, rows_written: 3} =
             ShardLoad.get("a")
  end

  # Expert review 2026-10-19 #5: every sample was an `:ets.update_counter` — a WRITE under the key's
  # lock — so the streams of ONE hot shard serialised on it (0.63 us/op alone, 159 us/op at 18
  # workers). A sample now only READS the row and bumps the shard's `:counters` block. Pinned two
  # ways: the stored row is never rewritten by a sample, and racing FIRST touches of a fresh shard
  # converge on one block (a plain `insert` there would drop every loser's counts).
  test "a sample reads the shard's row and never rewrites it (2026-10-19 #5)" do
    ShardLoad.record_query("r", 1, 0)
    [row] = :ets.lookup(ShardLoad, "r")

    for _ <- 1..1_000, do: ShardLoad.record_query("r", 2, 1)
    ShardLoad.record_checkout("r")

    assert :ets.lookup(ShardLoad, "r") == [row], "a sample rewrote the stored row"

    assert %{checkouts: 1, queries: 1_001, rows_read: 2_001, rows_written: 1_000} =
             ShardLoad.get("r")
  end

  # 2,000 fresh shards, every one first-touched by all workers at once, so the create race fires
  # many times per run. Verified to discriminate: with `insert` in place of `insert_new` in
  # `create_block/1` this loses counts (each loser's block replaces the winner's).
  test "racing first touches of fresh shards lose no counts (2026-10-19 #5)" do
    prefix = uniq("race")
    ids = for i <- 1..2_000, do: "#{prefix}_#{i}"
    workers = System.schedulers_online() * 2
    rounds = 3
    parent = self()

    pids =
      for _ <- 1..workers do
        spawn_link(fn ->
          receive do: (:go -> :ok)
          for _ <- 1..rounds, id <- ids, do: ShardLoad.record_query(id, 1, 0)
          send(parent, {:done, self()})
        end)
      end

    Enum.each(pids, &send(&1, :go))
    for pid <- pids, do: assert_receive({:done, ^pid}, 30_000)

    short = Enum.reject(ids, &(ShardLoad.get(&1).queries == workers * rounds))
    assert short == [], "#{length(short)} shard(s) lost counts to the first-touch race"
  end

  test "get is nil for an unrecorded shard" do
    assert ShardLoad.get("never") == nil
  end

  test "snapshot returns every recorded shard" do
    ShardLoad.record_checkout("a")
    ShardLoad.record_checkout("b")

    ids = ShardLoad.snapshot() |> Enum.map(& &1.shard_id) |> Enum.sort()
    assert ids == ["a", "b"]
  end

  test "top orders by the chosen dimension, highest first" do
    ShardLoad.record_query("cold", 1, 0)
    ShardLoad.record_query("hot", 100, 0)
    ShardLoad.record_query("hot", 100, 0)
    ShardLoad.record_query("warm", 10, 0)

    assert ["hot", "warm", "cold"] = ShardLoad.top(3, :rows_read) |> Enum.map(& &1.shard_id)
    # A different dimension can rank differently: `hot` has 2 queries, others 1.
    assert ["hot" | _] = ShardLoad.top(3, :queries) |> Enum.map(& &1.shard_id)
  end

  describe "top/2 is a bounded selection (review dsv41f.perf 2026-09-16 #4)" do
    test "returns exactly the n hottest when n < shard count, matching a full sort" do
      for {id, q} <- [{"s1", 5}, {"s2", 50}, {"s3", 20}, {"s4", 90}, {"s5", 1}, {"s6", 35}],
          _ <- 1..q do
        ShardLoad.record_query(id, 1, 0)
      end

      # Reference: the full sort + take that top/2 replaced with a bounded keep-top scan. Values are
      # distinct so there is no boundary-tie ambiguity between the two shapes.
      expected =
        ShardLoad.snapshot()
        |> Enum.sort_by(& &1.rows_read, :desc)
        |> Enum.take(3)
        |> Enum.map(& &1.shard_id)

      assert expected == ["s4", "s2", "s6"]
      assert ShardLoad.top(3, :rows_read) |> Enum.map(& &1.shard_id) == expected
    end

    test "returns every shard, sorted, when n exceeds the shard count" do
      ShardLoad.record_query("a", 3, 0)
      ShardLoad.record_query("b", 7, 0)

      assert ShardLoad.top(99, :rows_read) |> Enum.map(& &1.shard_id) == ["b", "a"]
    end

    test "ranks each dimension independently" do
      ShardLoad.record_query("reader", 100, 1)
      ShardLoad.record_query("writer", 1, 100)

      assert ["reader" | _] = ShardLoad.top(2, :rows_read) |> Enum.map(& &1.shard_id)
      assert ["writer" | _] = ShardLoad.top(2, :rows_written) |> Enum.map(& &1.shard_id)
    end
  end

  test "forget drops a shard's row" do
    ShardLoad.record_checkout("gone")
    assert ShardLoad.get("gone")

    ShardLoad.forget("gone")
    assert ShardLoad.get("gone") == nil
  end

  test "recording is a no-op when disabled" do
    Application.put_env(:fathom, :shard_load, false)
    ShardLoad.record_checkout("x")
    ShardLoad.record_query("x", 9, 9)

    assert ShardLoad.get("x") == nil
    assert ShardLoad.snapshot() == []
  end

  # ── through the hot path ──

  test "a checkout + queries record load, and a stop forgets it" do
    Application.put_env(:fathom, :shard_idle_ms, 50)
    id = uniq("load")
    on_exit(fn -> clean_shard(id) end)

    {:ok, handle} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(handle, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(handle, stmt("INSERT INTO kv VALUES ('a')"))
    {:ok, _} = ShardExecutor.execute(handle, stmt("SELECT v FROM kv"))

    load = ShardLoad.get(id)
    assert load.checkouts >= 1, "the checkout should be recorded"
    assert load.queries >= 3, "each executed statement counts as query load"
    assert load.rows_written >= 1, "the INSERT is query-cost (rows_written)"
    assert load.rows_read >= 1, "the SELECT is query-cost (rows_read)"

    :ok = ShardExecutor.close(handle)
    :ok = Shards.drain(id)
    assert ShardLoad.get(id) == nil, "a stopped shard's load row is forgotten"
  end

  test "the hot path records nothing when disabled" do
    Application.put_env(:fathom, :shard_load, false)
    Application.put_env(:fathom, :shard_idle_ms, 50)
    id = uniq("loadoff")
    on_exit(fn -> clean_shard(id) end)

    {:ok, handle} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(handle, stmt("CREATE TABLE kv (v TEXT)"))
    :ok = ShardExecutor.close(handle)

    assert ShardLoad.get(id) == nil
    Shards.drain(id)
  end

  defp local_dir, do: Fathom.Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
