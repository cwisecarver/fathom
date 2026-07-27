defmodule Fathom.Shard.StoragePurgeTest do
  @moduledoc """
  Full tenant erasure at the storage layer (expert review 2026-07-14 #15).

  `Storage.purge_shard/1` must delete EVERY object of a shard — live `.db`, `.lock`,
  every `@<version>` copy, every `@snap-<id>` — in one sweep, and it must be
  collision-safe: purging `acme` may NEVER touch `acme2`. That prefix-collision case
  is the shard-isolation gate for this feature (a bare `starts_with(<id>)` match would
  silently erase a different tenant's data), so it is pinned here.
  """
  use ExUnit.Case, async: false

  alias Fathom.{ShardExecutor, Shards, Snapshots}
  alias Fathom.Shard.Storage
  alias Filo.Stmt

  setup do
    n = System.unique_integer([:positive])
    # `sib` has `shard` as a strict prefix (the delimiter after the id is `9`, not
    # `.`/`@`) — the exact collision a bare-prefix match would erase by mistake.
    shard = "pg#{n}"
    sib = "pg#{n}9"

    on_exit(fn ->
      for id <- [shard, sib] do
        Shards.drain(id, 5_000)
        Storage.purge_shard(id)
        rm_local(id)
      end
    end)

    %{shard: shard, sib: sib}
  end

  test "purges every stored object of a shard and leaves a prefix-sibling untouched",
       %{shard: shard, sib: sib} do
    # Seed the shard with a full spread of objects: live, a retained version, a
    # snapshot, and a lease lock.
    write!(shard, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('A')"])
    flush!(shard)
    assert :ok = Storage.retain(shard, 1)
    assert {:ok, _snap} = Snapshots.create(shard)
    assert {:ok, _lease} = Storage.acquire_lease(shard, "owner@test-#{shard}", 60_000)

    # The prefix-sibling gets a live object of its own.
    write!(sib, ["CREATE TABLE t (v TEXT)", "INSERT INTO t VALUES ('B')"])
    flush!(sib)

    # Sanity: everything exists before the purge.
    assert {:ok, etag} = Storage.object_etag(shard)
    assert etag != nil
    assert {:ok, [_ | _]} = Snapshots.list(shard)
    assert File.exists?(remote(shard, "@1.db"))
    assert File.exists?(remote(shard, ".lock"))

    assert :ok = Storage.purge_shard(shard)

    # Every one of the shard's objects is gone.
    assert {:ok, nil} == Storage.object_etag(shard)
    assert {:ok, []} == Snapshots.list(shard)
    refute File.exists?(remote(shard, "@1.db"))
    refute File.exists?(remote(shard, ".lock"))

    # The prefix-sibling is completely untouched — its live object and data survive.
    assert {:ok, sib_etag} = Storage.object_etag(sib)
    assert sib_etag != nil
    assert read_one(sib, "SELECT v FROM t") == [["B"]]
  end

  test "purge is idempotent — a shard with no objects returns :ok", %{shard: shard} do
    assert :ok = Storage.purge_shard(shard)
    assert :ok = Storage.purge_shard(shard)
    assert {:ok, nil} == Storage.object_etag(shard)
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  defp write!(shard, sqls) do
    {:ok, handle} = ShardExecutor.open(shard)
    Enum.each(sqls, fn s -> {:ok, _} = ShardExecutor.execute(handle, stmt(s)) end)
    :ok = ShardExecutor.close(handle)
  end

  defp read_one(shard, sql) do
    {:ok, handle} = ShardExecutor.open(shard)
    {:ok, result} = ShardExecutor.execute(handle, stmt(sql))
    :ok = ShardExecutor.close(handle)
    result.rows
  end

  defp flush!(shard), do: :ok = Shards.drain(shard, 5_000)

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
  defp remote(id, suffix), do: Path.join(remote_dir(), "#{id}#{suffix}")

  defp rm_local(id) do
    for suffix <- ["", "-wal", "-shm"] do
      File.rm(Path.join([Fathom.Shard.data_dir(), "#{id}.db"]) <> suffix)
    end
  end
end
