defmodule Fathom.Shard.FirstWriteStampTest do
  @moduledoc """
  A replicating brand-new shard's first write creates a RANKABLE object at once.

  Found by Jepsen 2026-10-24 (`jepsen/fathom`, `--target direct --faults pause,kill`): a shard
  created by its first request took quorum-acked writes, its owner was SIGKILLed before the first
  periodic flush, and the survivor served it EMPTY while three peers held the writes. With no
  object there is no stamp to beat (`Promote.fresher?(_, nil)` is false, and
  `Recovery.rankable_stamp?(nil)` skips the fleet ask), so nothing ever asks the peers.
  `stamp_on_open/3` (expert review 2026-10-15 #7) closed the same hole for an object with no stamp
  but skipped "no object"; this pins the no-object half.

  A real three-follower fleet at quorum 2, because the write path ships every commit and refuses
  one with nowhere to ship. The periodic interval is pinned at a minute, so the only flush that can
  create the object inside the test is the one the first write asks for. Both liveness modes,
  mode asserted.
  """
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Storage}
  alias Fathom.Shard.Replication.{Fleet, Follower, Recovery, Session}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]
  @env [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :shard_flush_interval_ms
  ]

  setup do
    id = "firstwrite_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "firstwrite_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      Shards.drain(id, 2_000)
      Session.stop(id)

      for {k, v} <- prev,
          do:
            if(is_nil(v),
              do: Application.delete_env(:fathom, k),
              else: Application.put_env(:fathom, k, v)
            )

      File.rm_rf(root)
      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{id}*")), do: File.rm_rf(f)
      Storage.Local.purge_shard(id)
    end)

    Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)
    %{id: id, root: root}
  end

  defp replicating!(root) do
    followers =
      for i <- 1..3 do
        name = :"firstwrite_f#{i}_#{System.unique_integer([:positive])}"

        pid =
          start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, "f#{i}")},
            id: name
          )

        {:ok, port} = Follower.port(pid)
        {name, port}
      end

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)
    followers
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(pid, :heartbeat), do: assert(is_integer(:sys.get_state(pid).acquire_gen))
  defp assert_mode!(pid, :legacy), do: assert(is_nil(:sys.get_state(pid).acquire_gen))

  defp run(conn, sql), do: ShardExecutor.execute(conn, %Stmt{sql: sql, args: []})

  # Let the coordinator handle the write's `:became_dirty` and finish any flush it started.
  defp settle(_pid, 0), do: flunk("the flush never settled")

  defp settle(pid, tries) do
    if :sys.get_state(pid).flush_task == nil do
      :ok
    else
      Process.sleep(10)
      settle(pid, tries - 1)
    end
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      @tag mode: mode
      test "replicating: the first write creates a rankable object without waiting the interval",
           %{id: id, root: root, mode: mode} do
        set_mode!(mode)
        followers = replicating!(root)
        assert {:ok, nil} = Storage.object_head(id), "fixture: the shard must have no object"

        {:ok, conn} = ShardExecutor.open(id)
        {:ok, pid} = Shards.ensure(id)
        assert_mode!(pid, mode)
        for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)

        {:ok, _} = run(conn, "CREATE TABLE kv (v TEXT)")
        {:ok, _} = run(conn, "INSERT INTO kv VALUES ('acked')")
        _ = :sys.get_state(pid)
        settle(pid, 400)

        assert {:ok, %{} = head} = Storage.object_head(id),
               "the first write left NO object: a crash now loses every acked write since the open"

        assert Recovery.rankable_stamp?(head.position),
               "the object is unstamped (#{inspect(head.position)}): replicas holding the acked " <>
                 "writes can never override it"

        :ok = ShardExecutor.close(conn)
      end

      @tag mode: mode
      test "replication off: the first write waits for the interval as before",
           %{id: id, mode: mode} do
        set_mode!(mode)
        Application.put_env(:fathom, :replication_enabled, false)

        {:ok, conn} = ShardExecutor.open(id)
        {:ok, pid} = Shards.ensure(id)
        assert_mode!(pid, mode)

        {:ok, _} = run(conn, "CREATE TABLE kv (v TEXT)")
        _ = :sys.get_state(pid)
        settle(pid, 400)

        assert {:ok, nil} = Storage.object_head(id), "replication off must not PUT early"
        :ok = ShardExecutor.close(conn)
      end
    end
  end
end
