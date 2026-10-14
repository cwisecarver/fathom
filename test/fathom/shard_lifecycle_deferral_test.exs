defmodule Fathom.ShardLifecycleDeferralTest do
  @moduledoc """
  Coordinator lifecycle residuals of expert review 2026-10-13 (#16, #19, #25, #30).

    * #16 — a `checkin/4` cast that lands DURING a terminate clause's slow work (the drop flush) was
      never closed: `close_pool/1` drains the mailbox once, at the top.
    * #19 — terminate gave up on a stuck close chain after its bounded wait and then unlinked the
      files anyway; the periodic flush waited on the chain INLINE for up to 30 s.
    * #25 — a `:flush_now` waiter was not re-kicked when the flush deferred behind a lapse
      revalidation or a legacy renew: it waited a whole flush interval.
    * #30 — the free-space preflight logged at error level on EVERY refusal, and under-counted the
      encoder temp.

  The deferral tests (#25) are mode-agnostic by construction: the branches only inspect
  `lapse_revalidate_pending` / `renew_task` and the waiter list. They run in the suite's default
  (legacy) mode and assert it. The terminate tests (#16, #19) run the legacy drop path (the clauses
  differ only in the fence/renew settle, not in the close/keep-local decision under test).

  Not async: the close hook, free-space reading and storage are global.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Exqlite.Sqlite3
  alias Fathom.Admin.Measurements
  alias Fathom.Shard
  alias Fathom.Shard.WriteFence
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  @env [
    shard_storage: Fathom.Test.FaultyStorage,
    shard_idle_ms: 60_000,
    shard_flush_interval_ms: 600_000,
    shard_terminate_close_wait_ms: 50,
    shard_object_encoding: nil,
    faulty_before: nil,
    storage_fault: nil
  ]

  setup do
    shard = "lifecycle_#{System.unique_integer([:positive])}"
    prev = Map.new(@env, fn {k, _} -> {k, Application.get_env(:fathom, k)} end)

    for {k, v} <- @env do
      if is_nil(v),
        do: Application.delete_env(:fathom, k),
        else: Application.put_env(:fathom, k, v)
    end

    Measurements.clear_data_free_bytes()
    :telemetry_test.attach_event_handlers(self(), [[:fathom, :shard, :flush, :no_space]])

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      Measurements.clear_data_free_bytes()
      _ = Shards.stop(shard)
      WriteFence.forget(shard)

      for dir <- [Shard.data_dir(), Shard.Storage.Local.dir()],
          path <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm(path)
    end)

    %{shard: shard}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}
  defp pid_of({pid, _ref, _conn, _id, _sc, _v, _o}), do: pid
  defp ref_of({_pid, ref, _conn, _id, _sc, _v, _o}), do: ref
  defp conn_of({_pid, _ref, conn, _id, _sc, _v, _o}), do: conn

  defp dirty_shard(shard) do
    {:ok, h} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE t (a INTEGER)"))
    {:ok, _} = ShardExecutor.execute(h, stmt("INSERT INTO t VALUES (1)"))
    h
  end

  defp fake_task, do: %Task{mfa: {__MODULE__, :fake, 0}, owner: self(), pid: nil, ref: make_ref()}

  defp closed?(conn), do: not match?({:ok, _}, Sqlite3.prepare(conn, "SELECT 1"))

  defp eventually(fun, tries \\ 200) do
    cond do
      fun.() ->
        true

      tries == 0 ->
        false

      true ->
        Process.sleep(10)
        eventually(fun, tries - 1)
    end
  end

  # --- #25 ------------------------------------------------------------------------------------

  # Expert review 2026-10-13 #25. With a 10-minute flush interval, a `flush_now` parked behind a
  # lapse revalidation / legacy renew was re-kicked by `schedule_flush` (a full interval) rather than
  # the 250 ms backoff, so `Shards.flush/1` (fork, export, /api) waited out its 60 s timeout.
  for branch <- [:lapse_revalidate_pending, :renew_task] do
    test "a parked flush_now is re-kicked on the short backoff when deferred by #{branch} (#25)",
         %{shard: shard} do
      branch = Function.identity(unquote(branch))
      h = dirty_shard(shard)
      pid = pid_of(h)
      assert :sys.get_state(pid).acquire_gen == nil, "precondition: legacy liveness mode"

      idle = if branch == :lapse_revalidate_pending, do: false, else: nil
      :sys.replace_state(pid, &Map.put(&1, branch, fake_task()))

      flusher = Task.async(fn -> Shards.flush(shard) end)
      assert eventually(fn -> :sys.get_state(pid).flush_waiters != [] end)

      # The :durability_flush kicked by flush_now was queued before this call: it has been handled.
      _ = :sys.get_state(pid)
      assert :sys.get_state(pid).flush_waiters != [], "precondition: the waiter is still parked"

      # The lapse check / renew finishes.
      :sys.replace_state(pid, &Map.put(&1, branch, idle))

      assert {:ok, :ok} == Task.yield(flusher, 3_000),
             "the parked flush_now waited out a whole flush interval (expert review 2026-10-13 #25)"

      :ok = ShardExecutor.close(h)
    end
  end

  # --- #19 ------------------------------------------------------------------------------------

  # Expert review 2026-10-13 #19. The periodic flush awaited the close chain inline (30 s bound),
  # holding the coordinator's mailbox. A running close now just defers the tick.
  test "a periodic flush during a running close defers instead of blocking the mailbox (#19)", %{
    shard: shard
  } do
    h = dirty_shard(shard)
    pid = pid_of(h)
    stuck = spawn(fn -> Process.sleep(:infinity) end)
    :sys.replace_state(pid, &%{&1 | closing: stuck})

    send(pid, :durability_flush)

    t0 = System.monotonic_time(:millisecond)
    state = :sys.get_state(pid, 2_000)
    waited = System.monotonic_time(:millisecond) - t0

    assert waited < 500,
           "the periodic flush blocked the coordinator #{waited} ms waiting on the close chain"

    assert state.flush_timer != nil, "the deferred tick must be rescheduled"
    assert state.flush_task == nil, "nothing may be flushed while a close is checkpointing"

    :sys.replace_state(pid, &%{&1 | closing: nil})
    Process.exit(stuck, :kill)
    :ok = ShardExecutor.close(h)
  end

  # Expert review 2026-10-13 #19. Terminate gave up waiting on a stuck close after its bound and then
  # flushed-and-DROPPED (unlinked) the files the close is still checkpointing into. It now keeps the
  # local copy.
  test "terminate keeps the local copy when the close chain is still running (#19, conns == 0)",
       %{
         shard: shard
       } do
    h = dirty_shard(shard)
    pid = pid_of(h)
    :ok = ShardExecutor.close(h)
    _ = :sys.get_state(pid)

    stuck = spawn(fn -> Process.sleep(:infinity) end)
    :sys.replace_state(pid, &%{&1 | closing: stuck})
    assert File.exists?(Shard.db_path(shard)), "precondition: a local copy"

    pref = Process.monitor(pid)
    _ = :sys.terminate(pid, :shutdown)
    assert_receive {:DOWN, ^pref, :process, ^pid, _}, 15_000

    assert File.exists?(Shard.db_path(shard)),
           "terminate unlinked the shard files under a close that is still running " <>
             "(expert review 2026-10-13 #19)"

    Process.exit(stuck, :kill)
  end

  test "a lost-lease terminate renames, never unlinks, under a running close (#19, lease_lost)",
       %{shard: shard} do
    h = dirty_shard(shard)
    pid = pid_of(h)
    :ok = ShardExecutor.close(h)
    :ok = Shards.flush(shard)
    refute Shard.dirty?(pid), "precondition: clean, so the pre-fix clause would drop_local"

    stuck = spawn(fn -> Process.sleep(:infinity) end)
    :sys.replace_state(pid, &%{&1 | closing: stuck, lease_lost: true})

    pref = Process.monitor(pid)
    _ = :sys.terminate(pid, :shutdown)
    assert_receive {:DOWN, ^pref, :process, ^pid, _}, 15_000

    assert Path.wildcard(Shard.db_path(shard) <> ".fenced.*") != [],
           "a clean copy was unlinked under a running close (expert review 2026-10-13 #19)"

    Process.exit(stuck, :kill)
  end

  # --- #16 ------------------------------------------------------------------------------------

  # Expert review 2026-10-13 #16. In the busy terminate clause the drop flush takes seconds while
  # live streams can still cast `{:checkin, ...}`; nothing read the mailbox again, so that handle was
  # closed only when garbage collected (the 2026-10-12 #9 stale-close hazard).
  test "a checkin cast that lands during the busy terminate's flush is closed (#16)", %{
    shard: shard
  } do
    test_pid = self()
    ha = dirty_shard(shard)
    {:ok, hb} = ShardExecutor.open(shard)
    pid = pid_of(ha)
    conn_b = conn_of(hb)
    ref_b = ref_of(hb)
    refute closed?(conn_b), "precondition: the late handle starts open"

    Application.put_env(:fathom, :faulty_before, {
      :flush,
      fn
        ^shard ->
          send(test_pid, {:in_flush, self()})

          receive do
            :go -> :ok
          after
            10_000 -> :ok
          end

        _other ->
          :ok
      end
    })

    pref = Process.monitor(pid)
    terminator = Task.async(fn -> :sys.terminate(pid, :shutdown) end)
    assert_receive {:in_flush, hook}, 5_000, "precondition: terminate reached its drop flush"

    # The stream's checkin arrives while terminate is inside the flush.
    :ok = Shard.checkin(pid, ref_b, conn_b, :rw)
    send(hook, :go)

    assert_receive {:DOWN, ^pref, :process, ^pid, _}, 10_000
    Task.await(terminator, 10_000)

    assert closed?(conn_b),
           "a handle checked in during the terminate flush was never closed " <>
             "(expert review 2026-10-13 #16)"

    ShardExecutor.close(ha)
  end

  # --- #30 ------------------------------------------------------------------------------------

  defp flush!(pid) do
    send(pid, :durability_flush)
    assert eventually(fn -> :sys.get_state(pid).flush_task == nil end)
  end

  # Expert review 2026-10-13 #30. The refusal logged at error level on every preflight of every dirty
  # shard (once per interval per shard on a full volume), contradicting its own "first occurrence"
  # wording. The telemetry stays per refusal; the log is once per transition into failing.
  test "two refused preflights give one error log and two telemetry events (#30)", %{shard: shard} do
    h = dirty_shard(shard)
    pid = pid_of(h)
    Measurements.put_data_free_bytes(0)

    log =
      capture_log([level: :error], fn ->
        flush!(pid)
        flush!(pid)
      end)

    assert_receive {[:fathom, :shard, :flush, :no_space], _, %{free: 0}, %{shard_id: ^shard}},
                   2_000

    assert_receive {[:fathom, :shard, :flush, :no_space], _, %{free: 0}, %{shard_id: ^shard}},
                   2_000

    assert :sys.get_state(pid).flush_failures == 2

    assert length(String.split(log, "durability flush REFUSED")) - 1 == 1,
           "the refusal was logged on every preflight, not once per transition " <>
             "(expert review 2026-10-13 #30):\n#{log}"

    Measurements.put_data_free_bytes(10 * 1024 * 1024 * 1024)
    flush!(pid)
    :ok = ShardExecutor.close(h)
  end

  # Expert review 2026-10-13 #30. A compressing backend writes a second, encoded temp next to the
  # VACUUM INTO temp, so the space it needs is larger than with encoding off.
  test "the preflight counts the encoder temp when the backend compresses (#30)", %{shard: shard} do
    h = dirty_shard(shard)
    pid = pid_of(h)
    Measurements.put_data_free_bytes(0)

    Application.put_env(:fathom, :shard_object_encoding, :none)
    flush!(pid)
    assert_receive {[:fathom, :shard, :flush, :no_space], _, %{need: need_none}, _}, 2_000

    Application.put_env(:fathom, :shard_object_encoding, :zstd)
    flush!(pid)
    assert_receive {[:fathom, :shard, :flush, :no_space], _, %{need: need_zstd}, _}, 2_000

    assert need_zstd > need_none,
           "the encoder temp was not counted (#{need_zstd} vs #{need_none})"

    :ok = ShardExecutor.close(h)
  end
end
