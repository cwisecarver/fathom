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

    # The handler must accept EVERY shape of this event. Two emitters in `Fathom.Shard` (the
    # dirty-but-file-GONE drop and `keep_local_release_lease/3`) send `%{count: 1}` with no
    # `:consecutive`, and in a full suite leftover coordinators from earlier tests hit them while this
    # test runs (70-140 per run). The handler used to pattern-match `%{consecutive: n}` in its head,
    # so one such event from ANY shard raised a FunctionClauseError, `:telemetry` detached the
    # handler, and every later attempt went unobserved: "no retry within 5 s" although the
    # coordinator retried on time (full runs, seeds 610028 and 703096). The foreign event below,
    # fired right after attaching, pins that: a strict handler fails all three tests.
    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush, :failed],
      fn _e, measurements, metadata, _ ->
        case {measurements, metadata} do
          {%{consecutive: n}, %{shard_id: ^shard}} ->
            send(test_pid, {:flush_failed, n, System.monotonic_time(:millisecond)})

          _ ->
            :ok
        end
      end,
      nil
    )

    :telemetry.execute([:fathom, :shard, :flush, :failed], %{count: 1}, %{
      shard_id: "#{shard}_other",
      reason: :local_file_missing
    })

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

        assert_eventually(
          fn ->
            state = :sys.get_state(coordinator)
            state.flush_failures == 0 and not Shard.dirty?(coordinator)
          end,
          fn -> heal_diagnosis(coordinator) end
        )
      end
    end
  end

  # FLUSH BACKOFF DEFERRAL (found 2026-10-22, expert review 2026-10-19 #2 follow-up). A backoff retry
  # tick that lands while a lease-touching operation is in flight — the legacy `renew_task`, or the
  # heartbeat-mode lapse revalidation task — is deferred, and `defer_flush/1` re-armed it at the
  # FULL interval (60 s here) whenever no `:flush_now` waiter was parked. Neither the renew result
  # nor the lapse verdict re-sends `:durability_flush`, so the short retry was silently undone for
  # that cycle. The invariant: a deferred transient-failure retry stays on the short backoff.
  #
  # The in-flight operation is a stand-in `%Task{}` installed with `:sys.replace_state/2`, which is
  # exactly the state a real one produces for the guard (it matches only on `%Task{}` / non-nil);
  # a real renew / `check_lease` GET is too short to land a tick inside deterministically. It is
  # removed before healing, and a real one already in flight is left alone (same guard verdict).
  for {mode, field} <- [legacy: :renew_task, heartbeat: :lapse_revalidate_pending] do
    describe "retry tick deferred behind #{field} (#{mode})" do
      setup do: set_mode!(unquote(mode))

      test "keeps the short backoff instead of the full interval", %{shard: shard} do
        field = unquote(field)
        seed!(shard)
        {:ok, coordinator} = Shards.ensure(shard)
        assert_mode!(coordinator, unquote(mode))

        Application.put_env(:fathom, :storage_fault, :flush)
        capture_log(fn -> [{1, _}] = collect_attempts(coordinator, 1) end)

        fake = %Task{ref: make_ref(), pid: nil, owner: coordinator, mfa: {__MODULE__, :fake, 0}}

        :sys.replace_state(coordinator, fn st ->
          if match?(%Task{}, Map.fetch!(st, field)), do: st, else: Map.put(st, field, fake)
        end)

        # The retry tick, landing while the operation is in flight (the armed backoff timer may
        # beat it here; it defers the same way).
        send(coordinator, :durability_flush)
        st = :sys.get_state(coordinator)
        assert match?(%Task{}, Map.fetch!(st, field)), "the in-flight operation was not in place"
        assert st.flush_failures >= 1 and st.flush_waiters == []

        left = is_reference(st.flush_timer) && Process.read_timer(st.flush_timer)

        assert is_integer(left) and left <= 10 * @backoff_ms,
               "deferred retry re-armed at #{inspect(left)} ms, not the #{@backoff_ms} ms backoff " <>
                 "(the full interval is #{@interval_ms} ms)"

        # The operation completes and the store heals: the re-armed retry lands the shard clean.
        :sys.replace_state(coordinator, fn st ->
          if Map.fetch!(st, field) == fake, do: Map.put(st, field, mode_idle(field)), else: st
        end)

        Application.delete_env(:fathom, :storage_fault)

        assert_eventually(
          fn ->
            state = :sys.get_state(coordinator)
            state.flush_failures == 0 and not Shard.dirty?(coordinator)
          end,
          fn -> heal_diagnosis(coordinator) end
        )
      end
    end
  end

  defp mode_idle(:renew_task), do: nil
  defp mode_idle(:lapse_revalidate_pending), do: false

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
  defp assert_eventually(fun, diagnose, tries \\ 100) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("condition never held after the store healed: " <> diagnose.())

      true ->
        receive do
        after
          50 -> assert_eventually(fun, diagnose, tries - 1)
        end
    end
  end

  # This shape failed once in a full run (seed 139252) with three on-time failures and then no heal,
  # and is NOT explained by the handler fix above. The drop at teardown then logged "marked dirty but
  # the local file is GONE", so the file may have vanished mid-test (which reschedules a FULL
  # interval), or the retry may have been deferred behind a renew/flush task. Name it next time.
  defp heal_diagnosis(coordinator) do
    s = :sys.get_state(coordinator)
    left = if is_reference(s.flush_timer), do: Process.read_timer(s.flush_timer)

    inspect(
      %{
        flush_failures: s.flush_failures,
        dirty: Shard.dirty?(coordinator),
        local_file_exists: File.exists?(s.path),
        flush_task: s.flush_task != nil,
        renew_task: s.renew_task != nil,
        flush_timer_left_ms: left,
        flush_timer_slow?: s.flush_timer_slow?
      },
      limit: :infinity
    )
  end
end
