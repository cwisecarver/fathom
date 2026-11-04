defmodule Fathom.ShardFlushDeadlineRaceTest do
  @moduledoc """
  Review of 918bc409 (flush deadline). The task can FINISH between `{:flush_deadline, ref}` being
  queued and the handler running. `Task.shutdown(task, :brutal_kill)` then returns `{:ok, reply}` and
  CONSUMES the reply. By that point the PUT landed and `stamp_sidecar/2` already named the new etag
  and cleared the PUT intents, so discarding the reply leaves the coordinator on the old etag: the
  next flush 412s, `Fork.put_origin` finds the object's md5 in no intent, calls our own landed PUT a
  fork, and quarantines acked writes to `.db.forked` with `lease_lost`.

  The race is built deterministically: the task is held in the `:flush_stamp` hook (after the PUT),
  the coordinator is suspended with the deadline message queued, then the task is released and has
  exited (its reply queued BEHIND the deadline message) before the coordinator resumes.

  Both liveness modes, mode asserted. Not async.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "fdlrace_#{System.unique_integer([:positive])}"
    keys = [:shard_storage, :shard_flush_interval_ms]
    prev = Map.new(keys, &{&1, Application.get_env(:fathom, &1)})
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_flush_interval_ms, 600_000)

    on_exit(fn ->
      Application.delete_env(:fathom, :provenance_write_hook)

      for {k, v} <- prev,
          do:
            if(v,
              do: Application.put_env(:fathom, k, v),
              else: Application.delete_env(:fathom, k)
            )

      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock", ".pos", ".lineage"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(c, :heartbeat), do: assert(is_integer(:sys.get_state(c).acquire_gen))
  defp assert_mode!(c, :legacy), do: assert(is_nil(:sys.get_state(c).acquire_gen))

  for mode <- @modes do
    @tag mode: mode
    test "a flush that finished before its deadline fired is folded, not discarded (#{mode})",
         %{shard: shard, mode: mode} do
      set_mode!(mode)
      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coord} = Shards.ensure(shard)
      assert_mode!(coord, mode)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('first')"))
      :ok = Shards.flush(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('second')"))
      {:ok, e0} = Storage.object_etag(shard)

      test_pid = self()
      path = Shard.db_path(shard)

      Application.put_env(:fathom, :provenance_write_hook, fn
        :flush_stamp, ^path ->
          send(test_pid, {:held, self()})

          receive do
            :go -> :ok
          after
            10_000 -> :ok
          end

        _, _ ->
          :ok
      end)

      send(coord, :durability_flush)
      assert_receive {:held, writer}, 5_000
      %Task{ref: ref} = :sys.get_state(coord).flush_task

      :ok = :sys.suspend(coord)
      send(coord, {:flush_deadline, ref})
      Application.delete_env(:fathom, :provenance_write_hook)
      wref = Process.monitor(writer)
      send(writer, :go)
      assert_receive {:DOWN, ^wref, :process, ^writer, _}, 5_000
      :ok = :sys.resume(coord)

      :ok = Shards.flush(shard)
      {:ok, e1} = Storage.object_etag(shard)
      refute e1 == e0, "fixture: the held flush's PUT did not land"

      state = :sys.get_state(coord)
      assert state.etag == e1, "the finished flush's reply was discarded: stale fence etag"
      refute state.lease_lost
      refute Shard.dirty?(coord)

      forked = Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db.*forked*"))
      assert forked == [], "acked writes were quarantined as a fork: #{inspect(forked)}"
      :ok = ShardExecutor.close(conn)
    end
  end
end
