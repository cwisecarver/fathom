defmodule Fathom.ShardsFlushSelfFenceTest do
  @moduledoc """
  `Shards.flush/1` must not answer `:ok` when the coordinator stops by SELF-FENCE with the call
  still queued (expert review 2026-11-07 #50).

  The coordinator returned `{:stop, {:shutdown, :lease_lost}, …}`; `terminate/2` replies
  `{:error, :lease_lost}` only to waiters already in `flush_waiters`, so a `:flush_now` still in the
  mailbox exits `{{:shutdown, :lease_lost}, _}`. `flush_pid/2` mapped every `{:shutdown, _}` exit
  to `stopped_clean/1`, which answers `:ok` unless a local copy survived — and `quarantine_fenced!`
  had just renamed the `.db` away. After a self-fence the new owner is a REMOTE node that may hold
  unflushed writes, so export, keystone fork and `flush: true` snapshots read a stale object after
  being told it was current.

  A `{:shutdown, _}` exit is "coordinator gone, holder unknown": the answer is what `do_flush`
  gives with NO local coordinator (`kept_local_copy?`, then `RemoteFlush.flush/1`). A clean
  `:normal` / `:shutdown` stop is still `stopped_clean/1` — the drop flushed on its way out.

  ## The fixture

  The queued call must be unprocessed when the stop is decided, so the stop-causing task reply and
  the call are both sitting in a SUSPENDED coordinator's mailbox, in that order: a forged
  `Task` in the lapse-revalidation slot (heartbeat mode) or the renew slot (legacy mode) plus its
  reply `{ref, :superseded}` — the exact messages the real tasks send. Another node's live lock
  and heartbeat make the remote holder real.

  Both liveness modes, mode asserted. Not async (global storage, Heartbeat).
  """
  use Fathom.ClusterShardCase, async: false

  alias Fathom.Shard
  alias Fathom.Shard.Heartbeat
  alias Fathom.Test.Await

  @thief "thiefnode#t1"

  setup do
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :shard_flush_interval_ms, 600_000)

    on_exit(fn ->
      Application.delete_env(:fathom, :shard_flush_interval_ms)
      Storage.clear_heartbeat(@thief)
    end)

    :ok
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?(), "legacy mode needs the heartbeat OFF")

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp fake_task, do: %Task{mfa: {__MODULE__, :fake, 0}, owner: self(), pid: nil, ref: make_ref()}

  # The slot a self-fence verdict arrives through, per mode, and the reply it carries.
  defp stage_stop!(pid, :heartbeat) do
    task = fake_task()
    :sys.replace_state(pid, &%{&1 | lapse_revalidate_pending: task})
    send(pid, {task.ref, :superseded})
  end

  defp stage_stop!(pid, :legacy) do
    task = fake_task()
    :sys.replace_state(pid, &%{&1 | renew_task: task})
    send(pid, {task.ref, {:error, :superseded}})
  end

  for mode <- [:legacy, :heartbeat] do
    test "#{mode}: a flush queued behind a self-fence stop is not answered :ok", %{shard: shard} do
      set_mode!(unquote(mode))
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('acked')"))
      {:ok, pid} = Shards.ensure(shard)
      assert_mode!(pid, unquote(mode))
      assert Shard.dirty?(pid), "fixture: the shard holds acked, unflushed writes"

      # Another node now holds the lease, with a live heartbeat: the remote holder is real.
      put_raw_lock(shard, @thief, 999, now_ms() + 60_000)
      {:ok, _} = Storage.renew_heartbeat(@thief, 60_000)

      ref = Process.monitor(pid)
      :ok = :sys.suspend(pid)
      stage_stop!(pid, unquote(mode))
      flusher = Task.async(fn -> Shards.flush(shard) end)

      Await.until(
        fn ->
          {:messages, messages} = Process.info(pid, :messages)
          Enum.any?(messages, &match?({:"$gen_call", _, :flush_now}, &1))
        end,
        "fixture: the flush call never reached the suspended coordinator's mailbox"
      )

      log =
        capture_log(fn ->
          :ok = :sys.resume(pid)
          assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :lease_lost}}, 10_000
        end)

      assert log =~ "self-fencing" or log =~ "superseded"

      refute File.exists?(Shard.db_path(shard)),
             "fixture: the fence quarantined the local copy, so no local file can answer for it"

      result = Task.await(flusher, 10_000)

      assert result == {:error, :remote_holder_unflushed},
             "Shards.flush/1 answered #{inspect(result)} for a shard whose coordinator " <>
               "self-fenced with the call queued; another node holds the lease and may hold " <>
               "unflushed writes (expert review 2026-11-07 #50)"

      _ = conn
    end
  end
end
