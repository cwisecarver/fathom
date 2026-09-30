defmodule Fathom.ShardsFlushKeptLocalTest do
  @moduledoc """
  `Shards.flush/1` must not report success over a KEPT local copy (expert review 2026-09-29 #13).

  A drop whose flush fails keeps the local `.db` with its acked-but-unflushed writes and exits
  (`Fathom.Shard.keep_local_release_lease/3`). `Shards.flush/1` answered `:ok` whenever no
  coordinator was registered — "nothing is writing here, so the stored object is current" — which is
  false in exactly that state, and its callers (the GDPR export, `Tenants.fork(flush_source: true)`)
  then read the stale object as the tenant's complete data.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  setup do
    id = "flushkept_#{System.unique_integer([:positive])}"

    prev =
      for key <- [:shard_storage, :storage_fault, :shard_idle_ms],
          do: {key, Application.get_env(:fathom, key)}

    on_exit(fn ->
      for {key, val} <- prev do
        if is_nil(val),
          do: Application.delete_env(:fathom, key),
          else: Application.put_env(:fathom, key, val)
      end

      Shards.stop(id)

      for dir <- [Fathom.Shard.data_dir(), Storage.Local.dir()],
          path <- Path.wildcard(Path.join(dir, "#{id}*")),
          do: File.rm(path)
    end)

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    %{id: id}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp stored_rows(id) do
    tmp = Path.join(System.tmp_dir!(), "#{id}_stored_#{System.unique_integer([:positive])}.db")

    try do
      case Storage.pull(id, tmp) do
        {:ok, _} ->
          {:ok, conn} = Fathom.Shard.Connection.open(tmp)
          {:ok, %{rows: rows}} = Fathom.Shard.Connection.query(conn, "SELECT a FROM t", [])
          Fathom.Shard.Connection.close(conn)
          Enum.map(rows, fn [a] -> a end)

        _ ->
          :no_object
      end
    after
      for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    end
  end

  test "a kept local copy is flushed, not reported as already durable", %{id: id} do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER)"))
    :ok = Shards.flush(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (1)"))
    :ok = ShardExecutor.close(conn)

    # The drop's own flush fails, so the coordinator keeps the local copy and exits.
    Application.put_env(:fathom, :storage_fault, :flush)
    {:ok, pid} = Shards.ensure(id)
    ref = Process.monitor(pid)
    _ = Shards.drain(id, 5_000)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000

    assert File.exists?(Fathom.Shard.db_path(id)),
           "fixture: the failed drop did not keep the local copy"

    assert stored_rows(id) == [], "fixture: the stored object already holds the unflushed row"

    # Storage is back.
    Application.delete_env(:fathom, :storage_fault)

    assert :ok = Shards.flush(id)

    assert stored_rows(id) == [1],
           "Shards.flush/1 reported success while the acked row existed only in the kept local copy"
  end

  test "an unflushable kept copy is an error, never :ok", %{id: id} do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER)"))
    :ok = Shards.flush(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (1)"))
    :ok = ShardExecutor.close(conn)

    Application.put_env(:fathom, :storage_fault, :flush)
    {:ok, pid} = Shards.ensure(id)
    ref = Process.monitor(pid)
    _ = Shards.drain(id, 5_000)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
    assert File.exists?(Fathom.Shard.db_path(id))

    # Storage still refuses flushes: the answer must say so.
    refute Shards.flush(id) == :ok
  end
end
