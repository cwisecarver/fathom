defmodule Fathom.Shard.FlushPinnedMainFileCheckTest do
  @moduledoc """
  Expert review 2026-10-21 rv6 F3: the pinned-live flush uploads the `.db` itself, but its
  integrity gate ran `quick_check` on the PIN's handle. Once the route accepts a fully backfilled
  (not empty) WAL (2026-10-21 #11), the pin's read mark can sit in the WAL: the pin began while
  frames were still un-backfilled, and a checkpoint finished the backfill under it. That pin reads
  every page the WAL holds from its FRAME, not from the `.db`, so a `.db` page that differs from
  its frame (a torn backfill write, bad media) passed the gate and was uploaded as the durable
  object. Probe (`rv6/flush/probe_qc.py`): the pin's quick_check said ok, the immutable `.db` said
  malformed.

  The fix checks the `.db` bytes alone, through an `immutable=1` handle opened while the pin holds
  them frozen. The invariant pinned here, in BOTH liveness modes (asserted to have taken): a
  pinned-live flush whose `.db` pages are corrupt must refuse to upload, even when every one of
  those pages has an intact copy in the WAL.

  The fixture builds the read-mark-in-WAL shape deterministically: a reader on the empty-table snapshot
  holds back the flush's pre-pin PASSIVE, so the pin begins on an un-backfilled WAL; then, under
  the pin (the `:flush_pin_held_hook` seam), the reader goes away, a PASSIVE completes the
  backfill, and the `.db` pages are overwritten. The fixture asserts the WAL is fully backfilled
  before corrupting, or the route would never have been taken.

  Not async: telemetry handlers and app env are global.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.{Connection, Heartbeat, Storage}
  alias Fathom.Shard.Replication.Wal
  alias Fathom.ShardExecutor
  alias Fathom.Shards

  @modes [:legacy, :heartbeat]

  setup do
    id = "pinmf_#{System.unique_integer([:positive])}"
    path = Fathom.Shard.db_path(id)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)

    # The idle-stop would answer a pending flush with :coordinator_stopped (AGENTS.md testing note).
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    test_pid = self()
    handler = "pinmf-#{id}"

    :telemetry.attach(
      handler,
      [:fathom, :shard, :flush, :route],
      fn _, _, meta, _ -> if meta.shard_id == id, do: send(test_pid, {:route, meta.route}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)
      Application.delete_env(:fathom, :flush_pin_held_hook)

      if prev_idle,
        do: Application.put_env(:fathom, :shard_idle_ms, prev_idle),
        else: Application.delete_env(:fathom, :shard_idle_ms)

      Shards.stop(id)
      for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(path <> s)
    end)

    %{id: id, path: path, wal: path <> "-wal"}
  end

  defp stmt(sql, args \\ []), do: %Filo.Stmt{sql: sql, args: args}

  defp set_mode!(:legacy) do
    refute Heartbeat.running?(),
           "legacy mode needs the heartbeat OFF; config/test.exs default is heartbeat_server: false"

    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
    :ok
  end

  defp assert_mode!(coordinator, :heartbeat),
    do: assert(is_integer(:sys.get_state(coordinator).acquire_gen), "heartbeat mode did not take")

  defp assert_mode!(coordinator, :legacy),
    do: assert(is_nil(:sys.get_state(coordinator).acquire_gen), "legacy mode did not take")

  defp insert!(conn, rows) do
    for i <- rows,
        do:
          {:ok, _} =
            ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES (?1, zeroblob(500))", [i]))
  end

  defp wait(ms) do
    receive do
    after
      ms -> :ok
    end
  end

  defp flush!(coordinator, tries \\ 400) do
    send(coordinator, :durability_flush)
    settle(coordinator, tries)
  end

  defp settle(_c, 0), do: flunk("flush task never settled")

  defp settle(c, tries) do
    if :sys.get_state(c).flush_task == nil do
      :ok
    else
      wait(10)
      settle(c, tries - 1)
    end
  end

  # Overwrite every page past the schema page and the table's root with garbage, as the probe did.
  # Their committed contents all live in WAL frames too, so a reader whose mark is in the WAL never
  # sees the damage.
  defp corrupt_main_file!(path) do
    page_size = 4096
    size = File.stat!(path).size
    pages = div(size, page_size)
    assert pages > 4, "fixture: too few pages to corrupt (#{pages})"
    {:ok, f} = :file.open(path, [:read, :write, :binary, :raw])

    for pg <- 3..pages,
        do: :ok = :file.pwrite(f, (pg - 1) * page_size, :binary.copy(<<0xAA>>, page_size))

    :ok = :file.sync(f)
    :ok = :file.close(f)
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "corrupt .db pages under a WAL-mark pin: the pinned-live flush refuses to upload",
           ctx do
        %{id: id, path: path, wal: wal} = ctx
        {:ok, coordinator} = Shards.ensure(id)
        assert_mode!(coordinator, unquote(mode))

        {:ok, conn} = ShardExecutor.open(id)
        on_exit(fn -> ShardExecutor.close(conn) end)
        {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER, b BLOB)"))

        # A reader on the EMPTY-table snapshot: the flush's pre-pin PASSIVE can backfill only up to
        # its mark, so the pin begins with frames still un-backfilled and takes a mark in the WAL.
        # The snapshot must predate every data page: a reader ignores frames that were already
        # backfilled when it began (SQLite's `minFrame`) and reads those pages from the `.db`, so a
        # later snapshot would let the OLD gate see the corruption on its early pages and pass for
        # the wrong reason (it did, on the first draft of this test, with the reader at 10 rows).
        {:ok, reader} = Connection.open(path)
        :ok = Connection.exec(reader, "BEGIN")
        {:ok, %{rows: [[0]]}} = Connection.query(reader, "SELECT count(*) FROM t", [])
        insert!(conn, 1..200)

        test_pid = self()

        Application.put_env(:fathom, :flush_pin_held_hook, fn sid ->
          if sid == id do
            pinned_unbackfilled = not Wal.fully_backfilled?(wal)
            _ = Connection.rollback_if_open(reader)
            :ok = Connection.close(reader)

            # The backfill finishes UNDER the pin (allowed up to the pin's own mark).
            {:ok, raw} = Connection.open(path)
            {:ok, _} = Connection.query(raw, "PRAGMA wal_checkpoint(PASSIVE)", [])
            :ok = Connection.close(raw)
            backfilled = Wal.fully_backfilled?(wal)

            corrupt_main_file!(path)
            send(test_pid, {:under_pin, pinned_unbackfilled, backfilled})
          end
        end)

        flush!(coordinator)
        Application.delete_env(:fathom, :flush_pin_held_hook)

        assert_received {:under_pin, pinned_unbackfilled, backfilled}
        assert pinned_unbackfilled, "fixture: the pin must begin on an un-backfilled WAL"
        assert backfilled, "fixture: the WAL must be fully backfilled under the pin"
        assert_received {:route, :pinned_live}

        # THE REGRESSION: the pin's quick_check read the intact WAL frames and said ok, and the
        # corrupt `.db` was uploaded as the durable object.
        dst = Path.join(System.tmp_dir!(), "pinmf_obj_#{System.unique_integer([:positive])}.db")
        on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(dst <> s) end)

        assert {:absent, _} = Storage.pull(id, dst),
               "the flush uploaded a .db whose pages are corrupt"

        assert Fathom.Shard.dirty?(coordinator), "a refused flush must keep the shard dirty"
      end
    end
  end
end
