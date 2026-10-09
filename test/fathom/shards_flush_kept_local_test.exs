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

  # The DRAIN half (expert review 2026-10-08 #4, verified by execution). The coordinator that keeps
  # its local copy still exits `:normal`, and `drain/2` mapped a `:normal` DOWN to `:ok` — "data is
  # durable in storage and the lease is free" — while the stored object lacked the acked row. The
  # migrator, rebalancer handoff and snapshot restore all proceed on that `:ok`. Lease mode does not
  # enter into it: the defect is in how the DOWN is classified, after the coordinator is gone.
  defp kept_local_fixture(id) do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER)"))
    :ok = Shards.flush(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (1)"))
    :ok = ShardExecutor.close(conn)
    Application.put_env(:fathom, :storage_fault, :flush)
    {:ok, pid} = Shards.ensure(id)
    {pid, Process.monitor(pid)}
  end

  test "drain/2 does not report :ok when the stop kept an unflushed local copy", %{id: id} do
    {pid, ref} = kept_local_fixture(id)

    assert {:error, :unflushed_local_copy} = Shards.drain(id, 5_000)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    assert File.exists?(Fathom.Shard.db_path(id)), "fixture: the failed drop kept no local copy"
    assert stored_rows(id) == [], "fixture: the stored object already holds the unflushed row"

    # No coordinator now, but the kept copy is still the only holder of the row: a retried drain
    # must keep saying so rather than read the empty registry as "already cold".
    assert {:error, :unflushed_local_copy} = Shards.drain(id, 5_000)

    # Storage is back: flush/1 makes the copy durable (the migrator's retry path), and only then
    # does the drain report a clean stop.
    Application.delete_env(:fathom, :storage_fault)
    assert :ok = Shards.flush(id)
    assert :ok = Shards.drain(id, 5_000)
    assert stored_rows(id) == [1], "drain/2 reported :ok without the acked row in storage"
    refute File.exists?(Fathom.Shard.db_path(id))
  end

  test "drain_all counts a kept local copy as kept_local, not drained", %{id: id} do
    {pid, ref} = kept_local_fixture(id)
    on_exit(fn -> Fathom.HealthPlug.end_draining() end)

    result = Shards.drain_all(5_000)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    assert result.kept_local >= 1, "drain_all tallied #{inspect(result)}"
    assert File.exists?(Fathom.Shard.db_path(id))
  end
end
