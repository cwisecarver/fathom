defmodule Fathom.Shard.OpenHeadOverrunTest do
  @moduledoc """
  An open whose head task (post-lease HEAD + fleet ask + peer pull) outlives the first caller's
  `checkout_timeout` (perf review 2026-10-21 #6).

  The review read `await_open_head/1`'s `Task.await(task, :infinity)` as a path to a hard 500: the
  head task can carry HEAD (~20 s) + ask (~4 s) + a peer pull (60 s), past the 75 s checkout budget.
  The probe that settled it is this file. What actually happens:

    * the first caller's `GenServer.call` times out and `ShardExecutor` maps `:timeout` to a
      RETRYABLE 503 (`FILO_UNAVAILABLE`, expert review 2026-10-12 #13) — not a 500;
    * the coordinator keeps going and finishes the open WITH its recovery decision, and the stale
      `:checkout` is dropped by the `{:abandon_checkout, …}` cast, so no phantom connection pins it;
    * the client's retry is served by that already-open coordinator.

  So the `:infinity` is deliberate. Every component of the head task is itself bounded (Req's
  timeouts on the HEAD, the ask's connect/query timeouts, the pull's own deadline). A tighter
  bound on the await would have to KILL a peer pull that is close to finishing — the pull that
  carries quorum-acked writes the stored object lacks — and the retry would start that same pull
  over (a livelock for a large shard), or settle for `:none` and serve the object WITHOUT the
  recovered tail. Both are worse than one retryable 503.

  These tests pin that outcome: if a future change turns the overrun into a 500, strands the
  lease, or leaves a phantom grant, they fail. Both liveness modes, mode asserted.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.{Heartbeat, PromoteOnOpen, Storage}
  alias Fathom.Shard.Replication.{Fleet, Follower, Recovery}
  alias Fathom.{ShardExecutor, Shards}

  @moduletag :capture_log
  @modes [:legacy, :heartbeat]

  @env [
    :replication_promote_on_open,
    :replication_recover_from_peers,
    :replication_enabled,
    :recovery_prefetch_hook,
    :shard_idle_ms,
    :shard_checkout_timeout_ms
  ]

  setup do
    id = "headoverrun_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), "headoverrun_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})
    prev_running = Fleet.running()

    on_exit(fn ->
      Shards.stop(id)
      Fleet.publish(prev_running)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(dir)
      for f <- Path.wildcard(Fathom.Shard.db_path(id) <> "*"), do: File.rm_rf(f)
      Storage.Local.purge_shard(id)
    end)

    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Recovery.init_table()
    start_supervised!({Follower, name: Follower, port: 0, dir: dir})
    %{id: id}
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

  defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

  # Same fixture as `RecoveryPrefetchTest`: a rankable stored object, a replica row on this node's
  # follower, no local copy and no lock — the next open is a cold fresh create that prefetches.
  defp fixture!(id) do
    Application.put_env(:fathom, :replication_enabled, false)
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, coordinator} = Shards.ensure(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER PRIMARY KEY)"))

    for i <- 1..3,
        do: {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (?1)", [i]))

    wal = Fathom.Shard.db_path(id) <> "-wal"
    {:ok, %{salt1: salt}} = Fathom.Shard.Replication.Wal.read(wal)
    assert Fathom.Shard.wal_ordinal(coordinator, salt) > 0
    :ok = Shards.flush(id)
    {:ok, stamp} = Storage.object_position(id)
    assert Recovery.rankable_stamp?(stamp), "fixture: the stamp must be rankable"

    path = Fathom.Shard.db_path(id)
    File.cp!(path, Follower.db_path(Follower, id))
    File.write!(Follower.wal_path(Follower, id), "")
    Follower.seed(Follower, id, 1, stamp.wal_gen, 0, stamp.offset, stamp.epoch)

    :ok = ShardExecutor.close(conn)
    :ok = Shards.stop(id)
    refute File.exists?(path), "fixture: the clean stop must drop the local copy"

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_promote_on_open, true)
    Application.put_env(:fathom, :replication_recover_from_peers, true)
    path
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "a head task that outlives checkout_timeout: retryable 503, then the open completes",
           %{id: id} do
        path = fixture!(id)
        test_pid = self()
        Application.put_env(:fathom, :shard_checkout_timeout_ms, 300)

        # The slow part of the head task (a slow HEAD + a long peer pull), inside the task, after
        # the HEAD and before the marker + ask — 4x the caller's whole checkout budget.
        Application.put_env(:fathom, :recovery_prefetch_hook, fn _p ->
          send(test_pid, {:head_task_slow, self()})
          Process.sleep(1_200)
        end)

        assert {:error, %Filo.Error{status: 503, code: "FILO_UNAVAILABLE"}} =
                 ShardExecutor.open(id),
               "an open-head overrun must reach the client as a RETRYABLE error"

        assert_received {:head_task_slow, head_task}, "fixture: the head task never ran"
        [{coordinator, _}] = Registry.lookup(Fathom.ShardRegistry, id)

        # The open goes on and completes (the head task is awaited, never orphaned).
        ref = Process.monitor(head_task)
        assert_receive {:DOWN, ^ref, :process, ^head_task, _}, 5_000
        state = :sys.get_state(coordinator)

        if unquote(mode) == :heartbeat,
          do: assert(is_integer(state.acquire_gen), "heartbeat mode did not take"),
          else: assert(is_nil(state.acquire_gen), "legacy mode did not take")

        # The timed-out caller's grant was dropped: no phantom connection pins the shard.
        assert state.conns == %{}, "the abandoned checkout left a phantom grant"
        refute File.exists?(PromoteOnOpen.recovering_path(path)), "the marker survived the open"

        # The client's retry is served by the open the first request started.
        Application.delete_env(:fathom, :recovery_prefetch_hook)
        Application.put_env(:fathom, :shard_checkout_timeout_ms, 5_000)
        {:ok, conn} = ShardExecutor.open(id)
        assert [{^coordinator, _}] = Registry.lookup(Fathom.ShardRegistry, id)
        {:ok, %{rows: rows}} = ShardExecutor.execute(conn, stmt("SELECT a FROM t ORDER BY a"))
        assert rows == [[1], [2], [3]]
        :ok = ShardExecutor.close(conn)
      end
    end
  end
end
