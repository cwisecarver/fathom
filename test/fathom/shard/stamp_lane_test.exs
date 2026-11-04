defmodule Fathom.Shard.StampLaneTest do
  @moduledoc """
  Expert review 2026-11-04 #4. The first-write stamp flush (068ab044) went through the periodic
  `FlushGate` and, refused, backed off by the herd-scaled `open_shards/2000` s (~5 s at 10k shards).
  That re-opens the "acked write, no object yet" window the Jepsen bug-1 fix closed exactly when it
  is most likely to matter (a reopen herd plus a node kill). It now takes a small dedicated gate
  lane and retries on the plain short backoff when that lane is full.

  Falsifier: `shard_flush_max_concurrency: 1` with the one main slot held by someone else (a stalled
  flush). Before the lane, the brand-new shard's object never appeared while the slot was held.

  Not async: the gate, app env and shards are global.
  """
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{FlushGate, Heartbeat, Storage}
  alias Fathom.Shard.Replication.{Fleet, Follower, Recovery, Session}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]
  @env [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :shard_flush_interval_ms,
    :shard_flush_max_concurrency,
    :shard_stamp_flush_max_concurrency
  ]

  describe "FlushGate stamp lane" do
    setup do
      prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})

      on_exit(fn ->
        for {k, v} <- prev,
            do:
              if(is_nil(v),
                do: Application.delete_env(:fathom, k),
                else: Application.put_env(:fathom, k, v)
              )

        FlushGate.reset()
      end)

      FlushGate.reset()
      Application.put_env(:fathom, :shard_flush_max_concurrency, 1)
      Application.put_env(:fathom, :shard_stamp_flush_max_concurrency, 2)
      :ok
    end

    test "is independent of a saturated main lane, and capped on its own" do
      assert :ok = FlushGate.try_acquire()
      assert :full = FlushGate.try_acquire()

      assert :ok = FlushGate.try_acquire_stamp()
      test_pid = self()

      other =
        spawn_link(fn ->
          send(test_pid, {:stamp, FlushGate.try_acquire_stamp()})

          receive do
            :done -> :ok
          end
        end)

      assert_receive {:stamp, :ok}
      assert FlushGate.stamp_in_flight() == 2

      assert :full = FlushGate.try_acquire_stamp()
      assert FlushGate.stamp_in_flight() == 2, "a refused stamp acquire must not consume a slot"
      assert FlushGate.in_flight() == 1, "the stamp lane must not touch the main counter"

      FlushGate.release_stamp()
      assert FlushGate.stamp_in_flight() == 1
      send(other, :done)
    end

    test "is off when the main gate is unbounded, and when configured off" do
      Application.put_env(:fathom, :shard_flush_max_concurrency, 0)
      assert :disabled = FlushGate.try_acquire_stamp()

      Application.put_env(:fathom, :shard_flush_max_concurrency, 1)
      Application.put_env(:fathom, :shard_stamp_flush_max_concurrency, 0)
      assert :disabled = FlushGate.try_acquire_stamp()
    end

    test "sweep reclaims a stamp slot whose holder died" do
      holder =
        spawn(fn ->
          :ok = FlushGate.try_acquire_stamp()
          exit(:kill)
        end)

      ref = Process.monitor(holder)
      assert_receive {:DOWN, ^ref, :process, ^holder, _}
      assert FlushGate.stamp_in_flight() == 1
      assert FlushGate.sweep() == 1
      assert FlushGate.stamp_in_flight() == 0
    end
  end

  describe "first write on a replicating brand-new shard" do
    setup do
      id = "stamplane_#{System.unique_integer([:positive])}"
      root = Path.join(System.tmp_dir!(), "stamplane_#{System.unique_integer([:positive])}")
      File.mkdir_p!(root)
      prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})

      on_exit(fn ->
        FlushGate.release()
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
        FlushGate.reset()
      end)

      FlushGate.reset()
      Application.put_env(:fathom, :shard_flush_interval_ms, 60_000)
      Application.put_env(:fathom, :shard_flush_max_concurrency, 1)
      %{id: id, root: root}
    end

    defp replicating!(root) do
      followers =
        for i <- 1..3 do
          name = :"stamplane_f#{i}_#{System.unique_integer([:positive])}"

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

    defp await_object(id, tries) do
      case Storage.object_head(id) do
        {:ok, %{} = head} ->
          head

        _ when tries == 0 ->
          nil

        _ ->
          receive do
          after
            20 -> await_object(id, tries - 1)
          end
      end
    end

    for mode <- @modes do
      @tag mode: mode
      test "gets its object while the main flush lane is fully held (#{mode})",
           %{id: id, root: root, mode: mode} do
        set_mode!(mode)
        followers = replicating!(root)
        assert {:ok, nil} = Storage.object_head(id), "fixture: the shard must have no object"

        # The one main slot is held by "a stalled flush" (this test process).
        assert :ok = FlushGate.try_acquire()
        assert :full = FlushGate.try_acquire(), "fixture: the main lane must be saturated"

        {:ok, conn} = ShardExecutor.open(id)
        {:ok, pid} = Shards.ensure(id)
        assert_mode!(pid, mode)
        for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)

        {:ok, _} = run(conn, "CREATE TABLE kv (v TEXT)")
        {:ok, _} = run(conn, "INSERT INTO kv VALUES ('acked')")
        _ = :sys.get_state(pid)

        # 250 ms is the plain backoff; at 10k shards the herd-scaled one is ~5 s. The object must
        # appear without the main lane ever freeing.
        head = await_object(id, 150)

        assert head,
               "the first-write stamp flush queued behind the held FlushGate slot: a crash now " <>
                 "loses every acked write"

        assert Recovery.rankable_stamp?(head.position)
        assert FlushGate.stamp_in_flight() == 0, "the stamp slot was not released"
        :ok = ShardExecutor.close(conn)
      end
    end
  end
end
