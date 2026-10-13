defmodule Fathom.Shard.FlushObjectAbsentTest do
  @moduledoc """
  A flush that finds the stored object GONE under a live owner escalates and re-creates it — unless
  the tenant is being erased (expert review 2026-10-12 #4).

  ## The defect

  The fenced PUT against a missing object answers `:superseded`; `reconcile_superseded` re-checked
  the lock (still ours), read the object etag as `{:ok, nil}`, and returned `{:reconciled, nil}`.
  That kept the STALE fence etag, so every later flush PUT `If-Match` a gone object and landed in the
  same place, forever; and `{:dirty, _}` skipped `record_flush_failure/2`, so the failure counter
  stayed 0 and the alert never fired. The shard kept acking writes that never became durable.

  ## What is pinned

    * the first such flush is a FAILURE (`{:error, :object_absent}`, counter 1, its own telemetry
      event), and the next flush re-creates the object (create-only) — both liveness modes, mode
      asserted;
    * with the id TOMBSTONED (a purge in progress) the object is NOT re-created: the coordinator
      self-fences instead, because re-creating would resurrect an erased tenant.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "objabs_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    # Not racing the idle-stop: the assertions talk to the coordinator after the last checkin.
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    on_exit(fn ->
      :ets.delete(Fathom.Tenants.Tombstones, shard)
      Shards.drain(shard, 2_000)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)

      if prev_idle,
        do: Application.put_env(:fathom, :shard_idle_ms, prev_idle),
        else: Application.delete_env(:fathom, :shard_idle_ms)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          p <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm_rf(p)
    end)

    %{shard: shard}
  end

  defp set_mode!(:legacy) do
    refute Heartbeat.running?()
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    :ok
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp object(shard), do: Path.join(Storage.Local.dir(), "#{shard}.db")

  defp write!(shard, v) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE IF NOT EXISTS kv (v TEXT)"})
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "INSERT INTO kv VALUES (?)", args: [v]})
    :ok = ShardExecutor.close(conn)
  end

  # A flushed shard whose stored object then vanishes out from under it, with a fresh write on top.
  defp flushed_then_vanished!(shard) do
    write!(shard, "a")
    {:ok, pid} = Shards.ensure(shard)
    assert :ok = Shard.flush_now(pid)
    assert File.exists?(object(shard)), "the fixture must have a stored object to lose"
    assert is_binary(:sys.get_state(pid).etag)

    File.rm!(object(shard))
    write!(shard, "b")
    assert Shard.dirty?(pid)
    pid
  end

  defp attach_absent_handler do
    test_pid = self()
    id = "objabs-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      id,
      [:fathom, :shard, :flush, :object_absent],
      fn _e, _m, meta, _ -> send(test_pid, {:object_absent, meta.shard_id}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "a vanished object fails the flush loudly, then the next flush re-creates it", %{
        shard: shard
      } do
        attach_absent_handler()
        pid = flushed_then_vanished!(shard)
        assert_mode!(pid, unquote(mode))

        log = capture_log(fn -> assert Shard.flush_now(pid) == {:error, :object_absent} end)

        assert log =~ "STORED OBJECT VANISHED"
        assert_receive {:object_absent, ^shard}
        st = :sys.get_state(pid)
        assert st.flush_failures == 1, "the absent object must count as a failed flush"
        assert st.etag == nil, "the next flush must be create-only, not If-Match a gone object"
        refute File.exists?(object(shard))

        # The next flush re-creates the object and the shard is durable again.
        assert :ok = Shard.flush_now(pid)
        assert File.exists?(object(shard))
        assert :sys.get_state(pid).flush_failures == 0
        refute Shard.dirty?(pid)
      end

      test "a TOMBSTONED id is not resurrected: the coordinator self-fences instead", %{
        shard: shard
      } do
        pid = flushed_then_vanished!(shard)
        assert_mode!(pid, unquote(mode))
        Fathom.Tenants.Tombstones.put(shard)
        ref = Process.monitor(pid)

        capture_log(fn ->
          # Answered `{:error, :lease_lost}` just before the stop (or an exit if the stop wins).
          try do
            assert Shard.flush_now(pid) == {:error, :lease_lost}
          catch
            :exit, _ -> :ok
          end

          assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :lease_lost}}, 5_000
        end)

        refute File.exists?(object(shard)), "a purged tenant's object must not be re-created"
      end
    end
  end
end
