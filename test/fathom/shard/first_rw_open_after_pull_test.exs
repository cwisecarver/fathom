defmodule Fathom.Shard.FirstRwOpenAfterPullTest do
  @moduledoc """
  MEASUREMENT ARM ONLY for expert review 2026-10-21 r7 #17 (panel 1 #6). It changes nothing.

  `VACUUM INTO` writes a rollback-mode header (bytes 18-19 = `01 01`), so every object the
  periodic/drop VACUUM route stores makes the FIRST `:rw` connection after a cold pull run
  SQLite's rollback -> WAL conversion (`Connection.open/2` runs `PRAGMA journal_mode=WAL` first):
  a DELETE-journal write transaction with about 4 fsyncs, on the first request's critical path.
  The pinned-live route uploads the live `.db`, whose header is already `02 02`. No existing
  cold-open arm sees the difference, because each seeds its object with `Storage.flush(id, live)`,
  i.e. a WAL header.

  This arm stores a real VACUUM-route object through the coordinator's flush, pulls it N times, and
  times the first `:rw` `Connection.open/2` of each pulled copy against the same database with a
  WAL header. Watch-only: it prints both medians and asserts only its own preconditions (the route
  was `:vacuum`, the stored header really is `01 01`, the control's really is `02 02`), so it
  cannot report a spectacular delta for a fixture that never paid the conversion.

  Read it on Linux: macOS `fsync` is not a barrier, so the delta is small there (panel: 134 vs
  199 us). LINUX measured on a quiet VM: 0.68 ms p50 rollback-header vs 0.05 ms WAL-header.

  The fix itself (make the VACUUM route store a WAL header) waits on the user's Q4 answer; when it
  lands, the precondition `stored header == 01 01` flips and this arm is the before/after.

      mix test test/fathom/shard/first_rw_open_after_pull_test.exs --include bench
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.{Connection, Storage}
  alias Fathom.ShardExecutor
  alias Fathom.Shards

  @moduletag :bench
  @trials 30

  setup do
    id = "frwo_#{System.unique_integer([:positive])}"
    path = Fathom.Shard.db_path(id)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    prev_pin = Application.get_env(:fathom, :flush_pinned_live_upload)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    # Force the VACUUM route deterministically (the pinned-live route would store `02 02`).
    Application.put_env(:fathom, :flush_pinned_live_upload, false)

    test_pid = self()
    handler = "frwo-#{id}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush, :route],
      fn _, _, meta, _ -> if meta.shard_id == id, do: send(test_pid, {:route, meta.route}) end,
      nil
    )

    dir = Path.join(System.tmp_dir!(), "frwo_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    on_exit(fn ->
      :telemetry.detach(handler)

      for {k, v} <- [shard_idle_ms: prev_idle, flush_pinned_live_upload: prev_pin] do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      Shards.stop(id)
      for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(path <> s)
      File.rm_rf(dir)
    end)

    %{id: id, dir: dir}
  end

  defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

  defp header_mode(path) do
    {:ok, f} = :file.open(path, [:read, :binary, :raw])
    {:ok, <<w, r>>} = :file.pread(f, 18, 2)
    :ok = :file.close(f)
    {w, r}
  end

  defp time_first_rw_open(path) do
    t0 = System.monotonic_time(:microsecond)
    {:ok, conn} = Connection.open(path)
    us = System.monotonic_time(:microsecond) - t0
    :ok = Connection.close(conn)
    us
  end

  defp median(xs), do: xs |> Enum.sort() |> Enum.at(div(length(xs), 2))

  test "first :rw open after pulling a VACUUM-route object (rollback header) vs a WAL header",
       %{id: id, dir: dir} do
    {:ok, _} = Shards.ensure(id)
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER, b BLOB)"))

    for i <- 1..200,
        do:
          {:ok, _} =
            ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (?1, zeroblob(500))", [i]))

    :ok = ShardExecutor.close(conn)
    :ok = Shards.flush(id)

    assert_received {:route, :vacuum},
                    "precondition: the stored object must be a VACUUM-route one"

    # Rollback-header copies: each pulled fresh, so each first open pays the conversion.
    rollback_us =
      for i <- 1..@trials do
        dst = Path.join(dir, "rb_#{i}.db")
        assert {:ok, _} = Storage.pull(id, dst)

        assert header_mode(dst) == {1, 1},
               "precondition: a VACUUM-route object should carry a rollback header (01 01)"

        us = time_first_rw_open(dst)
        assert header_mode(dst) == {2, 2}, "the first :rw open did not convert the file to WAL"
        us
      end

    # Control: the same database already WAL-headed (a converted copy, the pinned-live shape).
    template = Path.join(dir, "rb_1.db")

    wal_us =
      for i <- 1..@trials do
        dst = Path.join(dir, "wal_#{i}.db")
        File.cp!(template, dst)
        assert header_mode(dst) == {2, 2}, "precondition: control copy must carry a WAL header"
        time_first_rw_open(dst)
      end

    rb = median(rollback_us)
    wal = median(wal_us)

    IO.puts(
      "\n[r7 #17] first_rw_open_after_pull_us p50: rollback-header #{rb} us, " <>
        "WAL-header #{wal} us (delta #{rb - wal} us, #{@trials} trials, #{:os.type() |> elem(0)})"
    )

    # Watch-only: no latency bound (the delta is fsync-cost, i.e. host-dependent). Sanity only.
    assert rb > 0 and wal > 0
  end
end
