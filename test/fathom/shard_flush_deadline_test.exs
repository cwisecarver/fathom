defmodule Fathom.ShardFlushDeadlineTest do
  @moduledoc """
  Expert review 2026-11-04 #3 (carried 2026-10-29 #25). A flush whose PUT never returns (a peer
  that stops reading mid-body) parked the flush task for the kernel's ~15 min, holding one
  node-wide `FlushGate` slot and the shard's only flush. `FlushGate.sweep/0` frees only DEAD
  holders, so ten stalls refused every flush on the node. The coordinator now kills a flush task
  that outlives `:shard_flush_deadline_ms`, releases the slot, counts a failed flush (the shard
  stays dirty, retried on the backoff), and never marks the shard clean on the timed-out PUT.

  Both liveness modes, asserting the mode took. Not async: shards, app env and the gate are global.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{FlushGate, Heartbeat, Storage}
  alias Filo.Stmt

  @deadline_ms 1_500

  setup do
    shard = "fdeadline_#{System.unique_integer([:positive])}"

    keys = [
      :shard_storage,
      :shard_flush_interval_ms,
      :shard_flush_backoff_ms,
      :shard_idle_ms,
      :shard_flush_deadline_ms,
      :shard_flush_max_concurrency,
      :storage_fault
    ]

    prev = Map.new(keys, &{&1, Application.get_env(:fathom, &1)})

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)
    Application.put_env(:fathom, :shard_flush_backoff_ms, 100)
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    Application.put_env(:fathom, :shard_flush_deadline_ms, @deadline_ms)
    Application.put_env(:fathom, :shard_flush_max_concurrency, 4)

    test_pid = self()
    handler = "fdeadline-#{shard}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush, :deadline],
      fn _e, _m, meta, _ -> send(test_pid, {:deadline, meta.shard_id}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)

      for {k, v} <- prev,
          do:
            if(v,
              do: Application.put_env(:fathom, k, v),
              else: Application.delete_env(:fathom, k)
            )

      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  # Whether `coordinator` holds a main-lane FlushGate slot. Not `FlushGate.in_flight/0`: that
  # counter is node-global, and a lingering coordinator from an earlier test flushing at the same
  # moment moved it (CI OTP 28, 2026-11-06: "the killed flush leaked its FlushGate slot" with no
  # leak). The gate records each holder by pid, so ask about this coordinator only.
  defp holds_slot?(coordinator), do: :ets.member(FlushGate, {:holder, coordinator})

  defp seed!(shard, v) do
    {:ok, conn} = ShardExecutor.open(shard)

    {:ok, _} =
      ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE IF NOT EXISTS kv (v TEXT)", args: []})

    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "INSERT INTO kv VALUES (?)", args: [v]})
    :ok = ShardExecutor.close(conn)
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

  defp assert_mode!(coordinator, :heartbeat),
    do: assert(is_integer(:sys.get_state(coordinator).acquire_gen), "heartbeat mode did not take")

  defp assert_mode!(coordinator, :legacy),
    do: assert(is_nil(:sys.get_state(coordinator).acquire_gen), "legacy mode did not take")

  defp await(coordinator, fun, tries \\ 100) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk(
          "condition never held: #{inspect(Map.take(:sys.get_state(coordinator), [:flush_failures, :flush_task, :flush_timer, :lease_lost, :flush_retry_ms, :etag]))}"
        )

      true ->
        receive do
        after
          50 -> await(coordinator, fun, tries - 1)
        end
    end
  end

  for mode <- [:legacy, :heartbeat], fault <- [:flush_hangs, :flush_lands_then_hangs] do
    describe "#{fault} (#{mode})" do
      setup do: set_mode!(unquote(mode))

      test "the deadline frees the slot, keeps the shard dirty, and a healed store converges",
           %{shard: shard} do
        seed!(shard, "a")
        {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))
        assert Shard.dirty?(coordinator)

        Application.put_env(:fathom, :storage_fault, unquote(fault))

        capture_log(fn ->
          send(coordinator, :durability_flush)
          await(coordinator, fn -> :sys.get_state(coordinator).flush_task != nil end)

          # The stalled PUT holds exactly one gate slot (the premise of the finding).
          assert holds_slot?(coordinator)

          assert_receive {:deadline, ^shard}, 5_000, "the stalled flush was never cut off"
        end)

        state = :sys.get_state(coordinator)
        assert state.flush_task == nil
        refute state.flush_slot_held
        assert state.flush_deadline_timer == nil
        refute holds_slot?(coordinator), "the killed flush leaked its FlushGate slot"
        assert Shard.dirty?(coordinator), "a timed-out PUT must never mark the shard clean"
        assert state.flush_failures >= 1

        # Writes after the (possibly landed) killed PUT, then heal: the stale-etag path must
        # converge to clean rather than trusting the earlier object.
        Application.delete_env(:fathom, :storage_fault)
        seed!(shard, "b")

        await(coordinator, fn ->
          s = :sys.get_state(coordinator)
          clean? = s.flush_failures == 0 and not Shard.dirty?(coordinator)
          # After a PUT that landed, the stale-etag 412 reconciles and the post-kill write waits
          # for the next tick (a full interval, 60 s here): kick it rather than wait it out.
          if not clean?, do: send(coordinator, :durability_flush)
          clean?
        end)

        refute holds_slot?(coordinator)
      end
    end
  end
end
