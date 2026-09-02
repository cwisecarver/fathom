defmodule Fathom.ShardFlushRenewGuardTest do
  @moduledoc """
  Expert review 2026-08-31 #15: in LEGACY mode a durability flush's own fence takes `Fence.legacy/2`
  — a renew PUT that ROTATES the lock etag — so running it concurrently with the periodic
  `:renew_lease` renew task issues two lease-mutating PUTs off the SAME base etag, and a stale
  `lock_etag` on the next flush PUT is a 412 the coordinator reads as a spurious steal.
  `:durability_flush` now defers a tick when a renew task is in flight, mirroring its existing
  flush/lapse guards.

  Legacy mode only — `renew_task` is always nil in heartbeat mode. The in-flight renew is injected
  deterministically with `:sys.replace_state`, not raced.

  The terminate-path settle (`settle_renew_task/1`) closes the same hazard on the stop path: an
  in-flight renew landing after `release_lease` re-writes the lock naming a dead coordinator — a
  leaked lock. That leaked lock is an S3 etag-rotation hazard the Local double cannot express (see
  AGENTS.md and `shard_lease_release_test.exs`), so it is verified structurally (`settle_renew_task`
  is on every terminate clause), not reproduced here.

  Not async: shards + the Registry are node-global; the heartbeat must be OFF for legacy mode.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.Heartbeat
  alias Filo.Stmt

  setup do
    refute Heartbeat.running?(),
           "legacy mode needs the heartbeat OFF; config/test.exs default is heartbeat_server: false"

    shard = "flushrenew_#{System.unique_integer([:positive])}"

    prev = %{
      idle: Application.get_env(:fathom, :shard_idle_ms),
      interval: Application.get_env(:fathom, :shard_flush_interval_ms)
    }

    # High idle + high flush interval so the coordinator neither idle-stops nor fires an automatic
    # durability flush under the assertions — the only flush we want to observe is the one we send.
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    Application.put_env(:fathom, :shard_flush_interval_ms, 600_000)

    on_exit(fn ->
      restore(:shard_idle_ms, prev.idle)
      restore(:shard_flush_interval_ms, prev.interval)
      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Shard.Storage.Local.dir()],
          path <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm(path)
    end)

    %{shard: shard}
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  test "durability_flush defers while a legacy renew is in flight, not stacking a lease PUT", %{
    shard: shard
  } do
    # Open + write + KEEP the connection open, so the shard is DIRTY — without the guard, a
    # durability flush would fire and set flush_task.
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('a')"))

    {:ok, pid} = Shards.ensure(shard)

    assert is_nil(:sys.get_state(pid).acquire_gen),
           "expected LEGACY mode (nil acquire_gen) — a heartbeat started and there is no renew_task"

    assert is_nil(:sys.get_state(pid).flush_task),
           "precondition: no flush in flight before the test injects the renew"

    # Inject an in-flight renew task (never completes) so the guard's condition holds
    # deterministically, without racing a real renew PUT.
    fake = Task.async(fn -> Process.sleep(:infinity) end)
    :sys.replace_state(pid, fn s -> %{s | renew_task: fake} end)

    # Ask for a durability flush. With the guard it DEFERS (a renew is in flight); without it, a
    # dirty shard reaches the flush branch and sets flush_task.
    send(pid, :durability_flush)
    _ = :sys.get_state(pid)

    assert is_nil(:sys.get_state(pid).flush_task),
           "durability_flush started a flush while a legacy renew was in flight — two " <>
             "lease-mutating PUTs off one base etag, a spurious 412 self-steal"

    # Clear the injected task before teardown so terminate's settle_renew_task does not Task.yield a
    # task this test process owns (Task ownership belongs to the creator).
    :sys.replace_state(pid, fn s -> %{s | renew_task: nil} end)
    Task.shutdown(fake, :brutal_kill)
    :ok = ShardExecutor.close(conn)
  end
end
