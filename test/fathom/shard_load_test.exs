defmodule Fathom.ShardLoadTest do
  @moduledoc """
  Per-shard load counters (the Phase-2 rebalancing prerequisite): checkout rate +
  query cost per shard, recorded lock-free on the hot path and read by a future
  control plane. Not async — the load table is a shared named ETS table.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, ShardLoad, Shards}
  alias Filo.Stmt

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

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
    for base <- [Path.join(@local_dir, "#{id}.db"), Path.join(@remote_dir, "#{id}.db")],
        suffix <- ["", "-wal", "-shm"],
        do: File.rm(base <> suffix)

    File.rm(Path.join(@remote_dir, "#{id}.lock"))
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
end
