defmodule Fathom.Shard.OrphanTerminateTest do
  @moduledoc """
  An orphan coordinator's `registry_lost` stop shuts down its in-flight tasks and gives back its
  flush slot (expert review 2026-10-12 #27).

  The orphan clause (fix-review R1-1) deliberately does none of the drop path — the files, lease and
  shard-keyed ETS rows are the replacement's — but it also skipped what IS the orphan's alone: an
  in-flight durability flush task kept its node-global `FlushGate` slot until the 30 s sweep found
  the dead holder, so on a small cap one orphan was a measurable share of the node's flush
  concurrency. The task itself only died with the process's exit signal.

  What is pinned: right after the orphan's DOWN its gate holder row is gone (pre-fix it stayed until
  the sweep) and its flush task is dead. Both liveness modes.

  NOT exercised: closing lent pooled handles (pooling is off in the default suite). Only handles
  whose stream is already dead are closed there — a live stream's lent handle is still in use and
  is closed by that stream's own teardown when it sees the coordinator go down, as on every other
  terminate path.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "orphan_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    on_exit(fn ->
      # The test kills a Registry partition; the one_for_all restart may still be in flight when
      # the NEXT test (any file) registers a coordinator, which then stops as an orphan: CI
      # 2026-10-14 (OTP 29, seed 935568) pre_touch_etag_flush_test and shard_coordinator_open_test.
      Fathom.Test.RegistryKill.settle()
      Application.delete_env(:fathom, :faulty_before)
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

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp registry_partition(pid) do
    {:links, links} = Process.info(pid, :links)

    Enum.find(links, fn p ->
      case Process.info(p, :registered_name) do
        {:registered_name, n} when is_atom(n) ->
          String.starts_with?(Atom.to_string(n), "Elixir.Fathom.ShardRegistry.")

        _ ->
          false
      end
    end)
  end

  for mode <- @modes do
    test "#{mode} mode: an orphan stops its flush task and frees its gate slot", %{shard: shard} do
      set_mode!(unquote(mode))
      {:ok, orphan} = Shards.ensure(shard)
      assert_mode!(orphan, unquote(mode))

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE t (v TEXT)", args: []})
      :ok = ShardExecutor.close(conn)

      # Park the orphan's FIRST flush inside the store call, so the task is in flight (holding the
      # gate slot) when the orphan stops. Later flushes (the replacement's) pass straight through.
      test = self()
      first = :counters.new(1, [])

      Application.put_env(
        :fathom,
        :faulty_before,
        {:flush,
         fn
           ^shard ->
             if :counters.get(first, 1) == 0 do
               :counters.add(first, 1, 1)
               send(test, {:flush_parked, self()})
               receive(do: (:never -> :ok), after: (10_000 -> :ok))
             end

           _ ->
             :ok
         end}
      )

      send(orphan, :durability_flush)
      assert_receive {:flush_parked, task_pid}, 5_000
      task_ref = Process.monitor(task_pid)

      assert :sys.get_state(orphan).flush_slot_held,
             "the fixture needs the flush to hold a gate slot (is the FlushGate cap disabled?)"

      assert [_] = :ets.lookup(Fathom.Shard.FlushGate, {:holder, orphan})

      ref = Process.monitor(orphan)

      capture_log(fn ->
        Process.exit(registry_partition(orphan), :kill)
        send(orphan, :verify_registration)
        assert_receive {:DOWN, ^ref, :process, ^orphan, {:shutdown, :registry_lost}}, 5_000
      end)

      assert_receive {:DOWN, ^task_ref, :process, ^task_pid, _}, 1_000

      assert :ets.lookup(Fathom.Shard.FlushGate, {:holder, orphan}) == [],
             "the orphan's flush slot stayed held until the sweep"
    end
  end
end
