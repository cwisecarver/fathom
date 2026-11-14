defmodule Fathom.Shard.FlushRouteTelemetryTest do
  @moduledoc """
  Expert review 2026-10-21 r7 #2: the flush routes prod runs were invisible to telemetry. The
  periodic flush's duration event (`[:fathom, :shard, :flush]`, metric
  `fathom.shard.flush.duration`) carried only `outcome`, so its series moved whenever the
  pinned-live / VACUUM mix moved; and the drop flush (prod's main durability producer, since
  `SHARD_IDLE_MS` < `SHARD_FLUSH_INTERVAL_MS`) emitted a route count but no duration at all.

  Pinned here, in both liveness modes (asserted to have taken):
    * the periodic flush's duration event carries the scan route it took, the same value the
      `[:fathom, :shard, :flush, :route]` counter saw, for each of the two routes;
    * a drop flush emits `[:fathom, :shard, :drop_flush]` with a duration, its drop route and outcome;
    * the route key does not leak into the next flush's event (a flush that never reached a route
      reports `:none`, not the previous flush's route).

  Not async: telemetry handlers and app env are global.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.Admin.Measurements
  alias Fathom.ShardExecutor
  alias Fathom.Shards
  alias Fathom.Test.{Env, ShardFlush}
  alias Fathom.Test.LivenessMode

  @modes [:legacy, :heartbeat]
  @events [
    [:fathom, :shard, :flush],
    [:fathom, :shard, :flush, :route],
    [:fathom, :shard, :drop_flush],
    [:fathom, :shard, :drop_flush, :route]
  ]

  setup do
    id = "flroute_#{System.unique_integer([:positive])}"
    path = Fathom.Shard.db_path(id)
    Env.put!(:shard_idle_ms, 600_000)

    test_pid = self()
    handler = "flroute-#{id}"

    :telemetry.attach_many(
      handler,
      @events,
      fn event, measurements, meta, _ ->
        if meta[:shard_id] == id, do: send(test_pid, {:ev, event, measurements, meta})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)
      Measurements.clear_data_free_bytes()

      Shards.stop(id)
      for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(path <> s)
    end)

    %{id: id}
  end

  defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

  defp flush_event! do
    assert_receive {:ev, [:fathom, :shard, :flush, :route], _, %{route: counted}}, 5_000
    assert_receive {:ev, [:fathom, :shard, :flush], %{duration: d}, meta}, 5_000
    assert is_integer(d) and d >= 0
    assert meta.outcome == :uploaded
    {counted, meta.route}
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: LivenessMode.set!(unquote(mode))

      test "flush and drop-flush duration events carry the route they took", %{id: id} do
        {:ok, c} = Shards.ensure(id)
        LivenessMode.assert!(c, unquote(mode))

        {:ok, conn} = ShardExecutor.open(id)
        {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER)"))
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (1)"))

        # A reader holding a snapshot keeps the pre-pin PASSIVE from folding the WAL, so this
        # flush takes the VACUUM route.
        {:ok, reader} = Fathom.Shard.Connection.open(Fathom.Shard.db_path(id))
        :ok = Fathom.Shard.Connection.exec(reader, "BEGIN")
        {:ok, _} = Fathom.Shard.Connection.query(reader, "SELECT count(*) FROM t", [])
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (2)"))
        ShardFlush.flush!(c)
        assert {:vacuum, :vacuum} = flush_event!()
        _ = Fathom.Shard.Connection.rollback_if_open(reader)
        :ok = Fathom.Shard.Connection.close(reader)

        # No reader now: the PASSIVE folds the WAL and the flush takes the pinned-live route.
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (3)"))
        ShardFlush.flush!(c)
        assert {:pinned_live, :pinned_live} = flush_event!()

        # A flush refused before any route (the free-space preflight) reports :none.
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (5)"))
        Measurements.put_data_free_bytes(0)
        _ = capture_log(fn -> ShardFlush.flush!(c) end)
        Measurements.clear_data_free_bytes()

        assert_receive {:ev, [:fathom, :shard, :flush], _, %{outcome: :error, route: :none}},
                       5_000

        refute_received {:ev, [:fathom, :shard, :flush, :route], _, _}

        # A drop flush: dirty, nothing checked out -> the :checkpoint drop route.
        {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (4)"))
        :ok = ShardExecutor.close(conn)
        ref = Process.monitor(c)
        :ok = Shards.drain(id, 5_000)
        assert_receive {:DOWN, ^ref, :process, ^c, _}, 5_000

        assert_receive {:ev, [:fathom, :shard, :drop_flush, :route], _, %{route: :checkpoint}},
                       5_000

        assert_receive {:ev, [:fathom, :shard, :drop_flush], %{duration: d}, meta}, 5_000
        assert is_integer(d) and d >= 0
        assert meta.route == :checkpoint
        assert meta.outcome == :uploaded
      end
    end
  end
end
