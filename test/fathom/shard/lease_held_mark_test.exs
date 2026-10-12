defmodule Fathom.Shard.LeaseHeldMarkTest do
  @moduledoc """
  A coordinator stops advertising `:lease_held` BEFORE it gives the lease up (expert review
  2026-10-12 #20).

  The registry mark is what this node's replication follower reads (`coordinated_here?/2`) to
  refuse wire frames for a shard a coordinator here owns (expert review 2026-10-10 #1). It used to
  be cleared only by the process exiting, so a coordinator that had released the lock — or learned
  it was superseded — kept refusing the NEW owner's pushes and seeds until its terminate finished:
  one of the new owner's followers lost to its quorum.

  What is pinned: at the moment the release call is made (observed from inside the storage call,
  which runs in the coordinator process), the coordinator's registry value is no longer
  `:lease_held`. Pre-fix it still was. Both liveness modes, both release routes (a clean drop's
  plain release and a failed drop flush's claim-keeping release).

  NOT discriminated here: the `lease_lost` / superseded clear sites. Those paths make no storage
  call after learning the lease is gone, so there is no point inside them to observe from; the
  clears there rest on code reading, not on this test.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "leasemark_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)
      Application.delete_env(:fathom, :storage_fault)
      Shards.drain(shard, 2_000)

      if prev_storage,
        do: Application.put_env(:fathom, :shard_storage, prev_storage),
        else: Application.delete_env(:fathom, :shard_storage)

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

  # Record the coordinator's registry value from INSIDE the release (the hook runs in the caller).
  defp observe_release!(shard) do
    test = self()

    Application.put_env(
      :fathom,
      :faulty_before,
      {:release,
       fn
         ^shard -> send(test, {:mark_at_release, Registry.lookup(Fathom.ShardRegistry, shard)})
         _other -> :ok
       end}
    )
  end

  defp stop!(shard, pid) do
    ref = Process.monitor(pid)

    capture_log(fn ->
      Shards.drain(shard, 5_000)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
    end)
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "a clean drop clears the mark before its release", %{shard: shard} do
        {:ok, pid} = Shards.ensure(shard)
        assert_mode!(pid, unquote(mode))
        assert [{^pid, :lease_held}] = Registry.lookup(Fathom.ShardRegistry, shard)

        observe_release!(shard)
        stop!(shard, pid)

        assert_receive {:mark_at_release, [{^pid, value}]}, 1_000
        refute value == :lease_held, "the coordinator still advertised the lease it was releasing"
      end

      test "a failed drop flush clears the mark before its release", %{shard: shard} do
        {:ok, pid} = Shards.ensure(shard)
        assert_mode!(pid, unquote(mode))
        {:ok, conn} = ShardExecutor.open(shard)
        {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE t (v TEXT)", args: []})
        :ok = ShardExecutor.close(conn)
        assert Shard.dirty?(pid)

        Application.put_env(:fathom, :storage_fault, :flush)
        observe_release!(shard)
        log = stop!(shard, pid)
        assert log =~ "keeping local copy", "the drop did not take the failed-flush release path"

        assert_receive {:mark_at_release, [{^pid, value}]}, 1_000
        refute value == :lease_held, "the coordinator still advertised the lease it was releasing"
      end
    end
  end
end
