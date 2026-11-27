defmodule Fathom.Shard.FlushSpaceRouteTest do
  @moduledoc """
  Audit 2026-11-22 #17 (Fable decision). `flush_space_need/1` charged the VACUUM INTO temp (db + wal)
  PLUS the codec's encoded copy before the route was chosen, so on a compressing backend it asked for
  about twice the shard. The pinned-live route uploads the live `.db` and writes no snapshot temp, so
  a volume with between one and two shard sizes free refused a flush that would have fit, exactly
  while the disk was under pressure and RPO mattered most.

  The route is not knowable before the preflight (the pin decision is made under the pin), so the
  charge is split: `:upload` (encoded copy + margin) up front, `:snapshot` (the db + wal temp) only on
  the `:fallback` branch that writes it. The page-copy route also writes a shard-sized temp, so it
  stays under `:snapshot`.

  Not async: the cached free-space reading and app env are node-global.
  """
  use ExUnit.Case, async: false

  alias Fathom.Admin.Measurements
  alias Fathom.Shard.Connection
  alias Fathom.{ShardExecutor, Shards}
  alias Fathom.Test.{Env, LivenessMode, ShardFlush}

  @modes [:legacy, :heartbeat]
  @margin 1_000_000

  setup do
    id = "flushspace_#{System.unique_integer([:positive])}"
    path = Fathom.Shard.db_path(id)
    test_pid = self()
    handler = "flushspace-#{id}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush, :route],
      fn _, _, meta, _ -> if meta.shard_id == id, do: send(test_pid, {:route, meta.route}) end,
      nil
    )

    Env.put!(:shard_idle_ms, 600_000)
    Env.put!(:flush_space_margin_bytes, @margin)
    # Prod's encoding: the encoded copy is charged on every route.
    Env.put!(:shard_object_encoding, :zstd)
    Measurements.clear_data_free_bytes()

    on_exit(fn ->
      :telemetry.detach(handler)
      Measurements.clear_data_free_bytes()
      Shards.stop(id)
      for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(path <> s)
    end)

    %{id: id, path: path, wal: path <> "-wal"}
  end

  defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

  defp persistent_stream!(id, rows) do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER, b BLOB)"))

    for i <- rows,
        do:
          {:ok, _} =
            ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (?1, zeroblob(500))", [i]))

    conn
  end

  defp snapshot_bytes(path, wal) do
    Enum.sum(for f <- [path, wal], do: File.stat(f) |> elem(1) |> Map.get(:size, 0))
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: LivenessMode.set!(unquote(mode))

      test "a pinned-live flush is charged the encoded copy and the margin, not a snapshot temp",
           %{id: id, path: path, wal: wal} do
        {:ok, coordinator} = Shards.ensure(id)
        LivenessMode.assert!(coordinator, unquote(mode))
        conn = persistent_stream!(id, 1..50)
        on_exit(fn -> ShardExecutor.close(conn) end)

        snap = snapshot_bytes(path, wal)
        assert snap > 0

        # Room for the encoded copy and the margin, but NOT for a second shard-sized temp.
        Measurements.put_data_free_bytes(snap + @margin + 1)
        ShardFlush.flush!(coordinator)

        assert_received {:route, :pinned_live}

        assert :sys.get_state(coordinator).flush_failures == 0,
               "a flush that fits was refused: the snapshot temp was charged on a route that writes none"

        refute Fathom.Shard.dirty?(coordinator)
      end

      test "the same free space still refuses a flush that takes the VACUUM route",
           %{id: id, path: path, wal: wal} do
        {:ok, coordinator} = Shards.ensure(id)
        LivenessMode.assert!(coordinator, unquote(mode))
        conn = persistent_stream!(id, 1..10)
        on_exit(fn -> ShardExecutor.close(conn) end)

        # A reader on an older snapshot keeps frames un-backfilled, which forces the VACUUM route.
        {:ok, reader} = Connection.open(path)
        :ok = Connection.exec(reader, "BEGIN")
        {:ok, _} = Connection.query(reader, "SELECT count(*) FROM t", [])

        on_exit(fn ->
          _ = Connection.rollback_if_open(reader)
          Connection.close(reader)
        end)

        for i <- 11..40,
            do:
              {:ok, _} =
                ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (?1, zeroblob(500))", [i]))

        snap = snapshot_bytes(path, wal)
        Measurements.put_data_free_bytes(snap + @margin + 1)
        ShardFlush.flush!(coordinator)

        assert :sys.get_state(coordinator).flush_failures == 1,
               "the VACUUM route writes a shard-sized temp and must still be refused here"

        assert Fathom.Shard.dirty?(coordinator)
      end
    end
  end
end
