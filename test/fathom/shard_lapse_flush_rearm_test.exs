defmodule Fathom.ShardLapseFlushRearmTest do
  @moduledoc """
  A heartbeat-lapse revalidation that was skipped because a flush was in flight must be re-armed
  when that flush ends (expert review 2026-11-07 #12, carried from 2026-10-29 #27c).

  The flush task starts at generation G, and its own `Fence.check` (no I/O in heartbeat mode) ran
  BEFORE the lapse. The lapse then bumps the heartbeat to G+1 and arms the jittered
  `:revalidate_lapse`; the timer fires while `flush_task != nil`, the clause skips (mutual exclusion
  with the flush's lease merge — correct, and kept), and the flush completes merging
  `acquire_gen: G`. Nothing re-armed, so `Heartbeat.valid_for_write?(G)` read `:revalidate` for the
  rest of the flush interval and every commit answered 503 `:ownership_unconfirmed` on a shard that
  never lost ownership.

  Both liveness modes, asserted. The lapse path exists only in heartbeat mode (the broadcast needs a
  non-nil `acquire_gen`), so the legacy case pins the other direction: the completion re-arm must
  NOT start a revalidation episode on a legacy coordinator, whose `write_fence_recheck/1` is
  `:not_valid` by construction.

  Not async: the storage, `:faulty_before` hook and Heartbeat are node-global.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard
  alias Fathom.Shard.{Heartbeat, Storage, WriteFence}
  alias Fathom.Test.{Await, Env}
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  setup do
    shard = "lapserearm_#{System.unique_integer([:positive])}"

    Env.put!(:shard_storage, Fathom.Test.FaultyStorage)

    # Nothing idle-stops or periodically flushes under the test: the only flush is the one it sends.
    Env.put!(:shard_idle_ms, 60_000)
    Env.put!(:shard_flush_interval_ms, 600_000)
    Env.put!(:faulty_before, nil)
    Env.put!(:faulty_after_flush, nil)

    on_exit(fn ->
      _ = Shards.stop(shard)
      WriteFence.forget(shard)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          path <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm(path)
    end)

    %{shard: shard}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp dirty_shard(shard) do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (1)"))
    conn
  end

  # Park the next flush for `shard` inside `Storage.flush` and hand the test the parked process.
  # `:faulty_before` parks BEFORE the backend call (so before its pre-PUT ownership recheck);
  # `:faulty_after_flush` parks once the PUT has LANDED, i.e. after that last recheck — the only
  # place a lapse survives into the merge (one that lands earlier aborts the flush with
  # `:ownership_unconfirmed_before_put` and the quick retry's own fence heals it).
  defp park_flush!(shard, where) do
    test_pid = self()

    park = fn
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

    case where do
      :before -> Application.put_env(:fathom, :faulty_before, {:flush, park})
      :after -> Application.put_env(:fathom, :faulty_after_flush, park)
    end
  end

  defp unpark_flush! do
    Application.put_env(:fathom, :faulty_before, nil)
    Application.put_env(:fathom, :faulty_after_flush, nil)
  end

  defp start_heartbeat! do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    hb
  end

  describe "heartbeat mode" do
    test "a lapse revalidation skipped behind an in-flight flush re-arms when the flush ends",
         %{shard: shard} do
      hb = start_heartbeat!()
      conn = dirty_shard(shard)
      {:ok, pid} = Shards.ensure(shard)

      stale = :sys.get_state(pid).acquire_gen
      assert is_integer(stale), "fixture: the coordinator must open in HEARTBEAT mode"
      assert stale == Heartbeat.generation()

      park_flush!(shard, :after)
      send(pid, :durability_flush)
      assert_receive {:in_flush, hook}, 5_000, "fixture: the flush never reached storage"
      assert :sys.get_state(pid).flush_task != nil, "fixture: the flush must be in flight"

      # The lapse lands AFTER the flush's fence: move the REAL heartbeat generation (a past deadline
      # at the same generation would be `:not_valid`, a different verdict), then deliver the timer.
      :sys.replace_state(hb, fn s ->
        Heartbeat.publish_status(%{s | generation: s.generation + 1})
      end)

      assert Heartbeat.valid_for_write?(stale) == :revalidate,
             "fixture: the heartbeat must be valid at a NEW generation (the :revalidate verdict)"

      send(pid, :revalidate_lapse)
      state = :sys.get_state(pid)

      assert state.flush_task != nil
      assert state.lapse_revalidate_pending == false, "fixture: the skip clause must have run"
      assert state.acquire_gen == stale, "fixture: the revalidation must NOT have run"

      send(hook, :go)

      Await.until(
        fn -> :sys.get_state(pid).acquire_gen == Heartbeat.generation() end,
        "the skipped lapse revalidation was never re-armed: acquire_gen stayed at #{stale} while " <>
          "the heartbeat is at #{Heartbeat.generation()}, so every commit reads :revalidate " <>
          "(503 :ownership_unconfirmed) until the next flush (expert review 2026-11-07 #12)",
        6_000
      )

      assert Heartbeat.valid_for_write?(:sys.get_state(pid).acquire_gen) == :ok
      refute Shard.dirty?(pid), "the parked flush must have completed"
      :ok = ShardExecutor.close(conn)
    end

    @tag :capture_log
    test "a failed flush re-arms too: a failing store must not be what keeps the gate stale",
         %{shard: shard} do
      hb = start_heartbeat!()
      conn = dirty_shard(shard)
      {:ok, pid} = Shards.ensure(shard)
      stale = :sys.get_state(pid).acquire_gen
      assert is_integer(stale), "fixture: the coordinator must be in HEARTBEAT mode"

      # The flush parks in storage, then fails with a PERMANENT error (`object_too_large`): its
      # reschedule is a whole flush interval away, so unlike a transient failure's short backoff
      # retry (whose own fence would heal the generation) nothing but the completion re-arm can.
      Env.put!(:storage_fault, :flush_too_large)
      park_flush!(shard, :before)
      send(pid, :durability_flush)
      assert_receive {:in_flush, hook}, 5_000
      assert :sys.get_state(pid).flush_task != nil

      :sys.replace_state(hb, fn s ->
        Heartbeat.publish_status(%{s | generation: s.generation + 1})
      end)

      assert Heartbeat.valid_for_write?(stale) == :revalidate

      send(pid, :revalidate_lapse)
      state = :sys.get_state(pid)
      assert state.lapse_revalidate_pending == false
      assert state.flush_task != nil

      # Nothing may park again.
      unpark_flush!()
      send(hook, :go)

      Await.until(
        fn -> :sys.get_state(pid).flush_failures >= 1 end,
        "fixture: the flush never failed"
      )

      Await.until(
        fn -> :sys.get_state(pid).acquire_gen == Heartbeat.generation() end,
        "a flush that ended in a failure left the skipped lapse revalidation un-armed " <>
          "(expert review 2026-11-07 #12)",
        6_000
      )

      Application.put_env(:fathom, :storage_fault, nil)
      :ok = ShardExecutor.close(conn)
    end
  end

  describe "legacy mode" do
    test "the completion re-arm never starts a revalidation episode on a legacy coordinator",
         %{shard: shard} do
      refute Heartbeat.running?(), "legacy mode needs the heartbeat OFF (config/test.exs default)"
      conn = dirty_shard(shard)
      {:ok, pid} = Shards.ensure(shard)
      assert :sys.get_state(pid).acquire_gen == nil, "fixture: the coordinator must be LEGACY"

      park_flush!(shard, :before)
      send(pid, :durability_flush)
      assert_receive {:in_flush, hook}, 5_000
      assert :sys.get_state(pid).flush_task != nil

      send(pid, :revalidate_lapse)
      assert :sys.get_state(pid).lapse_revalidate_pending == false
      send(hook, :go)

      Await.until(
        fn -> :sys.get_state(pid).flush_task == nil end,
        "fixture: the flush never completed"
      )

      state = :sys.get_state(pid)
      assert state.lapse_revalidate_pending == false
      assert state.acquire_gen == nil
      :ok = ShardExecutor.close(conn)
    end
  end
end
