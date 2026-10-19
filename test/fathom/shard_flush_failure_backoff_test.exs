defmodule Fathom.ShardFlushFailureBackoffTest do
  @moduledoc """
  Expert review 2026-10-19 #2. A transient flush failure (an S3 503, a timeout) and a `:fence_skip`
  (ownership unconfirmed) rescheduled a full jittered `:shard_flush_interval_ms`, so the object-RPO
  window grew from one interval to two while the shard sat dirty. They now retry on the short
  `:shard_flush_backoff_ms`, doubling per consecutive failure and capped at the interval, and the
  counter resets on the first durable flush.

  The interval is pinned to 60 s here, so a retry that arrives within a couple of seconds can only
  have come from the backoff. Attempts are observed through the
  `[:fathom, :shard, :flush, :failed]` telemetry event, which both failure shapes emit. Jitter is 0
  in the test config, so the lower bounds on the gaps are exact; the upper bounds are generous.

  Both liveness modes: the transient PUT failure is mode-agnostic (run for both, asserting the mode
  took); the `:fence_skip` shape needs a heartbeat that is alive but not valid, so it is
  heartbeat-only.

  Not async: shards, app env and the heartbeat are global.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Filo.Stmt

  @backoff_ms 100
  @interval_ms 60_000

  setup do
    shard = "fbackoff_#{System.unique_integer([:positive])}"

    keys = [
      :shard_storage,
      :shard_flush_interval_ms,
      :shard_flush_backoff_ms,
      :shard_idle_ms,
      :storage_fault
    ]

    prev = Map.new(keys, &{&1, Application.get_env(:fathom, &1)})

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_flush_interval_ms, @interval_ms)
    Application.put_env(:fathom, :shard_flush_backoff_ms, @backoff_ms)
    # The seed closes its only connection, which arms the idle timer; keep it from racing us.
    Application.put_env(:fathom, :shard_idle_ms, @interval_ms)

    test_pid = self()
    handler = "fbackoff-#{shard}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush, :failed],
      fn _e, %{consecutive: n}, %{shard_id: id}, _ ->
        if id == shard,
          do: send(test_pid, {:flush_failed, n, System.monotonic_time(:millisecond)})
      end,
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

  defp seed!(shard) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "CREATE TABLE kv (v TEXT)", args: []})
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "INSERT INTO kv VALUES ('x')", args: []})
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

  # Kick the first flush attempt now rather than waiting out the 60 s interval, then collect the
  # next `count - 1` automatic retries. Returns [{consecutive, mono_ms}] in arrival order.
  defp collect_attempts(coordinator, count) do
    capture_log(fn -> send(coordinator, :durability_flush) end)
    first = receive_failure()

    rest =
      for _ <- 2..count//1 do
        receive_failure()
      end

    [first | rest]
  end

  defp receive_failure do
    # 5 s: far below the 60 s interval, far above the 100-800 ms backoffs.
    assert_receive {:flush_failed, n, at},
                   5_000,
                   "no retry within 5 s: the failed flush waited out the full interval"

    {n, at}
  end

  for mode <- [:legacy, :heartbeat] do
    describe "transient PUT failure (#{mode})" do
      setup do: set_mode!(unquote(mode))

      test "retries on the short backoff, doubles per consecutive failure, resets on success",
           %{shard: shard} do
        seed!(shard)
        {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))
        assert Shard.dirty?(coordinator)

        Application.put_env(:fathom, :storage_fault, :flush)

        capture_log(fn ->
          [{1, t1}, {2, t2}, {3, t3}] = collect_attempts(coordinator, 3)

          # Lower bounds are exact (no jitter): backoff * 2^(n-1) after the n-th failure.
          assert t2 - t1 >= @backoff_ms, "first retry came before the backoff elapsed"
          assert t3 - t2 >= 2 * @backoff_ms, "repeated failures did not back off"
        end)

        # Heal the store: the next retry lands, the shard goes clean, and the counter resets.
        Application.delete_env(:fathom, :storage_fault)

        assert_eventually(fn ->
          state = :sys.get_state(coordinator)
          state.flush_failures == 0 and not Shard.dirty?(coordinator)
        end)
      end
    end
  end

  describe "ownership unconfirmed (heartbeat alive, deadline lapsed)" do
    setup do: set_mode!(:heartbeat)

    test "a :fence_skip retries on the backoff, not the interval", %{shard: shard} do
      seed!(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      assert_mode!(coordinator, :heartbeat)

      # Same generation, past deadline: the real `:not_valid`, which makes Fence.check answer :skip.
      state = :sys.get_state(Process.whereis(Heartbeat))

      Heartbeat.publish_status(%{
        state
        | mono_deadline_ms: System.monotonic_time(:millisecond) - 1
      })

      assert Heartbeat.valid_for_write?(:sys.get_state(coordinator).acquire_gen) == :not_valid

      capture_log(fn ->
        [{1, t1}, {2, t2}] = collect_attempts(coordinator, 2)
        assert t2 - t1 >= @backoff_ms
      end)
    end
  end

  # No sleeps: poll the coordinator with a bounded number of mailbox round trips.
  defp assert_eventually(fun, tries \\ 100) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("condition never held after the store healed")

      true ->
        receive do
        after
          50 -> assert_eventually(fun, tries - 1)
        end
    end
  end
end
