defmodule Fathom.ClusterShardCase do
  @moduledoc """
  Test case for cluster-phase shard tests (S3 lease handoff, S4 crash contract, and the S6
  chaos/isolation gate to come).

  In the LB-keyspace-partition model, "two nodes" coordinate only through shared storage: one
  owner string per node and one shared `.lock`. These tests simulate that — a `put_raw_lock`
  stands in for another node's lease, and the existing `Fathom.Shard` lease + self-fence do the
  real work. See `docs/deploy-cluster.md`.

  Provides per-test: a unique shard id, save/restore of the idle and lease-TTL config, file
  cleanup, and the shard-serving helpers. Not async — shards and lock files are global.
  """
  use ExUnit.CaseTemplate

  import ExUnit.Assertions

  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Shard.Storage
  alias Filo.{Stmt, StmtResult}

  @local_dir Path.join(System.tmp_dir!(), "fathom_shards")
  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  using do
    quote do
      import Fathom.ClusterShardCase
      import ExUnit.CaptureLog
      alias Fathom.{ShardExecutor, Shards}
      alias Fathom.Shard.Storage
      alias Filo.{Stmt, StmtResult}
    end
  end

  setup do
    shard = unique_shard()
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    prev_ttl = Application.get_env(:fathom, :shard_lease_ttl_ms)

    on_exit(fn ->
      put_or_delete(:shard_idle_ms, prev_idle)
      put_or_delete(:shard_lease_ttl_ms, prev_ttl)
    end)

    %{shard: shard}
  end

  @doc """
  A fresh unique shard id whose files are registered for cleanup. `setup` provides one as
  `%{shard: shard}`; tests that need a SECOND (or third) shard — e.g. the isolation gate — call
  this for each.
  """
  def unique_shard do
    shard = "cluster_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for dir <- [@local_dir, @remote_dir],
          suffix <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))
    end)

    shard
  end

  @doc """
  Close a connection and wait for the shard coordinator to idle-stop, so it never lingers
  touching files during on_exit. Set a short `:shard_idle_ms` first.
  """
  def close_and_stop(shard, conn) do
    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)
    :ok = ShardExecutor.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 2_000
  end

  defp put_or_delete(key, nil), do: Application.delete_env(:fathom, key)
  defp put_or_delete(key, value), do: Application.put_env(:fathom, key, value)

  @doc "A Hrana statement."
  def stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  def now_ms, do: System.system_time(:millisecond)
  def local_db(shard), do: Path.join(@local_dir, "#{shard}.db")
  def remote_db(shard), do: Path.join(@remote_dir, "#{shard}.db")
  def lock_file(shard), do: Path.join(@remote_dir, "#{shard}.lock")

  @doc "Write a lock file directly to simulate another node's lease (live or expired)."
  def put_raw_lock(shard, owner, epoch, expires_at_ms) do
    File.mkdir_p!(@remote_dir)

    File.write!(
      lock_file(shard),
      Jason.encode!(%{"owner" => owner, "epoch" => epoch, "expires_at_ms" => expires_at_ms})
    )
  end

  def read_lock_epoch(shard) do
    {:ok, %{epoch: epoch}} = Storage.decode_lease(File.read!(lock_file(shard)))
    epoch
  end

  @doc "Run `SELECT v FROM kv ORDER BY v` and return the values as a list."
  def select_v(conn) do
    {:ok, %StmtResult{rows: rows}} =
      ShardExecutor.execute(conn, stmt("SELECT v FROM kv ORDER BY v"))

    Enum.map(rows, fn [v] -> v end)
  end

  @doc """
  Open the shard, run `fun` with the connection, then close and wait for the coordinator to
  idle-stop (the caller must set a short `:shard_idle_ms`) so it never lingers touching files
  during on_exit. Returns `fun`'s result.
  """
  def serve(shard, fun) do
    {:ok, conn} = ShardExecutor.open(shard)
    result = fun.(conn)
    {:ok, coordinator} = Shards.ensure(shard)
    ref = Process.monitor(coordinator)
    :ok = ShardExecutor.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, :normal}, 2_000
    result
  end
end
