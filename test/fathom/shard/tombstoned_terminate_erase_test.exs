defmodule Fathom.Shard.TombstonedTerminateEraseTest do
  @moduledoc """
  A TOMBSTONED coordinator's stop must ERASE its local files, not keep them (expert review
  2026-10-15 #3, the coordinator half; `Fathom.Tenants.Tombstones` does the on-event/boot sweep).

  `Tenants.purge/1` runs `Shards.stop` + `rm_local` on the node that runs DeleteJob — Oban-routed,
  usually NOT the LB home. On the home node the coordinator stops through one of two paths, and both
  kept the tenant's complete database:

    * the ordinary terminate took `flush_and_drop_unless_tombstoned/2`, which returned `:ok` —
      skipping the flush (right: no resurrection) and the drop (wrong): `<id>.db`, `-wal`, `.etag`
      stayed on disk indefinitely;
    * a flush against the purged object self-fenced (`reconcile_absent` answers `:superseded` for a
      tombstoned id), and `quarantine_fenced!` RENAMED the data to `.fenced.<ts>` — kept 30 days, or
      forever with `quarantine_retention_ms = 0`. Default D1: an erasure may destroy such copies.

  Both liveness modes: the terminate and self-fence paths are lease paths.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "tombterm_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    on_exit(fn ->
      :ets.delete(Fathom.Tenants.Tombstones, shard)
      Shards.drain(shard, 2_000)
      restore(:shard_storage, prev_storage)
      restore(:shard_idle_ms, prev_idle)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          p <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm_rf(p)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  defp local_residue(shard), do: Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db*"))

  # A flushed shard with one more (unflushed) write: dirty, with a local `.db`, `-wal` and `.etag`.
  defp open_written!(shard, mode) do
    set_mode!(mode)
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, pid} = Shards.ensure(shard)
    assert_mode!(pid, mode)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('secret')"))
    assert :ok = Shard.flush_now(pid)
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('secret2')"))
    assert Shard.dirty?(pid)
    {conn, pid}
  end

  defp stop!(shard, pid) do
    ref = Process.monitor(pid)
    :ok = Shards.stop(shard)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      @tag mode: mode
      test "an idle stop of a tombstoned shard erases its local files",
           %{shard: shard, mode: mode} do
        {conn, pid} = open_written!(shard, mode)
        :ok = ShardExecutor.close(conn)
        Fathom.Tenants.Tombstones.put(shard)

        stop!(shard, pid)

        assert local_residue(shard) == [],
               "the erased tenant's database survived on the home node: #{inspect(local_residue(shard))}"
      end

      @tag mode: mode
      test "a busy stop of a tombstoned shard erases its local files",
           %{shard: shard, mode: mode} do
        {conn, pid} = open_written!(shard, mode)
        Fathom.Tenants.Tombstones.put(shard)

        capture_log(fn -> stop!(shard, pid) end)
        _ = ShardExecutor.close(conn)

        assert local_residue(shard) == [],
               "the erased tenant's database survived on the home node: #{inspect(local_residue(shard))}"
      end

      @tag mode: mode
      test "a self-fence against the purged object erases rather than quarantines",
           %{shard: shard, mode: mode} do
        {conn, pid} = open_written!(shard, mode)
        :ok = ShardExecutor.close(conn)

        # The purge: tombstone, then the stored object goes (it is deleted before the lock).
        Fathom.Tenants.Tombstones.put(shard)
        File.rm!(Path.join(Storage.Local.dir(), "#{shard}.db"))
        ref = Process.monitor(pid)

        capture_log(fn ->
          try do
            Shard.flush_now(pid)
          catch
            :exit, _ -> :ok
          end

          assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :lease_lost}}, 5_000
        end)

        assert local_residue(shard) == [],
               "the self-fence kept the erased tenant's data: #{inspect(local_residue(shard))}"
      end
    end
  end
end
