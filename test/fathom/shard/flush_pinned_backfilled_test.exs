defmodule Fathom.Shard.FlushPinnedBackfilledTest do
  @moduledoc """
  Expert review 2026-10-21 #11: the pinned-live flush route (2026-10-20 #28) fired only for an
  absent or zero-length `-wal`. A shard holding a stream open never has one (a finished checkpoint
  leaves the WAL at its length and reuses it), so every periodic flush of such a shard ran a full
  `VACUUM INTO`, even when the WAL was already fully checkpointed.

  The precondition the route actually needs is "no committed frame outside the main file":
  `nBackfill == mxFrame` in the `-shm`, read under the pin (`Wal.fully_backfilled?/1`). With
  replication off, the flush's existing PASSIVE now runs before the pin instead of after the
  VACUUM. With replication on there is NO new checkpoint (user decision 2026-10-21); the route
  applies only when something else already backfilled the whole WAL.

  Invariants pinned here, in BOTH liveness modes (asserted to have taken):

    * a persistent-stream shard with a non-empty WAL takes `:pinned_live`, and its object holds
      exactly the committed rows (the regression: it was `:vacuum`);
    * a commit AND checkpoints landing under the pin cannot move the main file: the object is the
      pin's image, never a torn mix;
    * a reader holding an older snapshot keeps the WAL partly un-backfilled; the route must fall
      back to `:vacuum`, or the upload would miss committed rows (this is the test that fails if the
      backfill guard is weakened);
    * replicated: an un-backfilled WAL gets no extra checkpoint and takes `:vacuum`; an already
      backfilled one takes `:pinned_live`;
    * a concurrent writer + checkpointer across 30 flushes: every object is a consistent prefix.
      This one is an INVARIANT GUARD, NOT a regression test for the guard: with the under-pin
      backfill check replaced by `true` it tore objects ("malformed") in 2 of 3 runs while the
      writer ran nonstop, but a nonstop writer never lets a pin find the WAL backfilled, so the
      route went unexercised; with the bursty writer it needs to exercise the route, the weakened
      guard passed 4 of 4 runs. The deterministic reader test above is what the guard rests on.
      Every third round quiesces the writer, so the route is exercised deterministically rather
      than by luck of the writer's gaps (which never lined up on a slow CI runner).

  Not async: telemetry handlers and app env are global.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.{Connection, Heartbeat, Storage}
  alias Fathom.Shard.Replication.Wal
  alias Fathom.ShardExecutor
  alias Fathom.Shards

  @modes [:legacy, :heartbeat]

  setup do
    id = "pinbf_#{System.unique_integer([:positive])}"
    path = Fathom.Shard.db_path(id)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    prev_repl = Application.get_env(:fathom, :replication_enabled)

    # The idle-stop would answer a pending flush with :coordinator_stopped (AGENTS.md testing note).
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    test_pid = self()
    handler = "pinbf-#{id}"

    :telemetry.attach_many(
      handler,
      [[:fathom, :shard, :flush, :route], [:fathom, :shard, :flush_checkpoint]],
      fn
        [:fathom, :shard, :flush, :route], _, meta, _ ->
          if meta.shard_id == id, do: send(test_pid, {:route, meta.route})

        [:fathom, :shard, :flush_checkpoint], _, meta, _ ->
          send(test_pid, {:flush_checkpoint, meta})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)
      Application.delete_env(:fathom, :flush_after_snapshot_hook)

      if prev_repl == nil,
        do: Application.delete_env(:fathom, :replication_enabled),
        else: Application.put_env(:fathom, :replication_enabled, prev_repl)

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

  # A stream that stays OPEN (the django-libsql WS / CONN_MAX_AGE shape): it is never the last
  # connection, so nothing ever unlinks or truncates the `-wal`.
  defp persistent_stream!(id, rows) do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (a INTEGER, b BLOB)"))
    insert!(conn, rows)
    conn
  end

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

  defp object_rows(id) do
    dst = Path.join(System.tmp_dir!(), "pinbf_obj_#{System.unique_integer([:positive])}.db")
    assert {:ok, _} = Storage.pull(id, dst)
    {:ok, c} = Connection.open(dst)
    {:ok, %{rows: rows}} = Connection.query(c, "SELECT a FROM t ORDER BY a", [])
    {:ok, %{rows: integrity}} = Connection.query(c, "PRAGMA integrity_check", [])
    :ok = Connection.close(c)
    for s <- ["", "-wal", "-shm"], do: File.rm(dst <> s)
    assert integrity == [["ok"]]
    List.flatten(rows)
  end

  defp wal_size(wal), do: File.stat!(wal).size

  # Appends `i` to `t` until told to stop; `{:written, from}` answers the next value (so `i - 1`
  # rows are committed). A failed insert crashes the linked test rather than leaving a gap.
  defp write_loop(raw, i, test_pid) do
    receive do
      :stop ->
        :ok = Connection.close(raw)
        send(test_pid, {:writer_stopped, i - 1})

      {:written, from} ->
        send(from, {:written, i})
        write_loop(raw, i, test_pid)

      # Quiesce BETWEEN commits (this receive only runs between inserts) until `:resume`.
      {:pause, from} ->
        send(from, {:paused, i})

        receive do
          :resume -> write_loop(raw, i, test_pid)
        end
    after
      0 ->
        {:ok, _} = Connection.query(raw, "INSERT INTO t VALUES (?1, zeroblob(500))", [i])

        # Bursts of 10 commits, a checkpoint, then a short quiet gap: a nonstop writer commits
        # between the pre-pin PASSIVE and the pin on nearly every flush, so the run would only
        # ever exercise the fallback. The gaps let some pins land on a fully backfilled WAL while
        # the next burst (and its checkpoint) still lands under the pin.
        if rem(i, 10) == 0 do
          {:ok, _} = Connection.query(raw, "PRAGMA wal_checkpoint(PASSIVE)", [])
          wait(:rand.uniform(4))
        end

        write_loop(raw, i + 1, test_pid)
    end
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do: set_mode!(unquote(mode))

      test "replication off: a persistent stream's non-empty WAL takes :pinned_live", ctx do
        %{id: id, wal: wal} = ctx
        {:ok, coordinator} = Shards.ensure(id)
        assert_mode!(coordinator, unquote(mode))

        conn = persistent_stream!(id, 1..50)
        on_exit(fn -> ShardExecutor.close(conn) end)
        refute Wal.fully_backfilled?(wal), "fixture: the WAL must hold un-backfilled frames"

        flush!(coordinator)

        # THE REGRESSION: this was `:vacuum` on every periodic flush of a shard with a live stream.
        assert_received {:route, :pinned_live}
        refute_received {:route, :vacuum}
        # The flush's PASSIVE ran BEFORE the pin, and it is the only one this flush ran.
        assert_received {:flush_checkpoint, %{replicated: false, result: :ran}}
        refute_received {:flush_checkpoint, _}
        assert object_rows(id) == Enum.to_list(1..50)
        assert wal_size(wal) > 0, "the case this finding is about: the WAL is still non-empty"
      end

      test "a commit and checkpoints under the pin cannot move the main file", ctx do
        %{id: id, path: path, wal: wal} = ctx
        {:ok, coordinator} = Shards.ensure(id)
        assert_mode!(coordinator, unquote(mode))

        conn = persistent_stream!(id, 1..10)
        on_exit(fn -> ShardExecutor.close(conn) end)
        test_pid = self()

        Application.put_env(:fathom, :flush_after_snapshot_hook, fn sid ->
          if sid == id do
            before = File.read!(path)
            # A raw handle (the executor refuses tenant checkpoints).
            {:ok, raw} = Connection.open(path)
            Connection.set_busy_timeout(raw, 0)

            for i <- 1000..1300,
                do:
                  {:ok, _} =
                    Connection.query(raw, "INSERT INTO t VALUES (?1, zeroblob(500))", [i])

            {:ok, passive} = Connection.query(raw, "PRAGMA wal_checkpoint(PASSIVE)", [])
            truncate = Connection.query(raw, "PRAGMA wal_checkpoint(TRUNCATE)", [])
            :ok = Connection.close(raw)
            send(test_pid, {:under_pin, passive, truncate, File.read!(path) == before})
          end
        end)

        flush!(coordinator)
        Application.delete_env(:fathom, :flush_after_snapshot_hook)

        assert_received {:route, :pinned_live}
        assert_received {:under_pin, passive, truncate, main_file_unchanged}
        assert wal_size(wal) > 0

        assert main_file_unchanged,
               "a checkpoint wrote the main file while the pin was held " <>
                 "(passive=#{inspect(passive.rows)} truncate=#{inspect(truncate)})"

        # The object is the pinned image: none of the in-window commits, nothing torn.
        assert object_rows(id) == Enum.to_list(1..10)
      end

      test "a concurrent writer + checkpointer: every flushed object is a consistent prefix",
           ctx do
        %{id: id, path: path} = ctx
        {:ok, coordinator} = Shards.ensure(id)
        assert_mode!(coordinator, unquote(mode))

        conn = persistent_stream!(id, [])
        {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE u (a INTEGER)"))
        on_exit(fn -> ShardExecutor.close(conn) end)

        # One writer appends 1, 2, 3, ... to `t` in checkpointed bursts, for the whole run.
        # Any consistent image of the file is therefore `t = 1..k`; a torn upload (pages from two
        # states) fails integrity_check or shows a gap.
        test_pid = self()

        writer =
          spawn_link(fn ->
            {:ok, raw} = Connection.open(path)
            Connection.set_busy_timeout(raw, 1_000)
            send(test_pid, {:writer_ready, self()})
            write_loop(raw, 1, test_pid)
          end)

        assert_receive {:writer_ready, ^writer}, 5_000

        # Every third round the writer is QUIESCED for the flush, so that round deterministically
        # reaches the pin on a fully backfilled WAL (the flush's own pre-pin PASSIVE folds every
        # frame, and nothing commits after it) and must take `:pinned_live`. The route used to be
        # reached only by luck of the bursty writer's gaps: on a slow 2-core CI runner all 30
        # rounds took `:vacuum` (CI run 39889287951, OTP 29), and the route assertion failed while
        # the invariant this test guards held. The other rounds still flush under a live writer.
        results =
          for round <- 1..30 do
            quiet? = rem(round, 3) == 0
            # Mark the shard dirty through the executor (the flush is write-gated).
            {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO u VALUES (?1)", [round]))

            floor =
              if quiet? do
                send(writer, {:pause, self()})
                assert_receive {:paused, next}, 5_000
                next
              else
                send(writer, {:written, self()})
                assert_receive {:written, next}, 5_000
                next
              end

            flush!(coordinator)
            if quiet?, do: send(writer, :resume)
            rows = object_rows(id)
            k = length(rows)
            assert rows == Enum.to_list(1..k//1), "round #{round}: object is not a prefix"
            assert k >= floor - 1, "round #{round}: object (#{k}) older than the pre-flush state"

            route =
              receive do
                {:route, route} -> route
              after
                0 -> flunk("round #{round}: no flush route reported")
              end

            if quiet?,
              do:
                assert(
                  route == :pinned_live,
                  "round #{round}: a quiesced writer's flush took #{inspect(route)}"
                )

            route
          end

        send(writer, :stop)
        assert_receive {:writer_stopped, total}, 5_000
        assert total > 12
        # The interleaving must actually exercise the new route, not only the VACUUM fallback.
        assert :pinned_live in results, "no round took :pinned_live: #{inspect(results)}"
      end

      test "a reader on an older snapshot keeps frames un-backfilled: falls back to :vacuum",
           ctx do
        %{id: id, path: path, wal: wal} = ctx
        {:ok, coordinator} = Shards.ensure(id)
        assert_mode!(coordinator, unquote(mode))

        conn = persistent_stream!(id, 1..10)
        on_exit(fn -> ShardExecutor.close(conn) end)

        # A long reader whose read mark sits at the 10-row state: a PASSIVE can backfill only up
        # to it, so the frames of rows 11..40 stay in the WAL alone.
        {:ok, reader} = Connection.open(path)
        :ok = Connection.exec(reader, "BEGIN")
        {:ok, %{rows: [[10]]}} = Connection.query(reader, "SELECT count(*) FROM t", [])

        on_exit(fn ->
          _ = Connection.rollback_if_open(reader)
          Connection.close(reader)
        end)

        insert!(conn, 11..40)
        flush!(coordinator)

        assert_received {:route, :vacuum}
        refute_received {:route, :pinned_live}
        refute Wal.fully_backfilled?(wal), "fixture: the reader must hold back the backfill"

        # Uploading the `.db` here would have shipped the 10-row state: rows 11..40 (acked,
        # committed) would be missing from the durable object.
        assert object_rows(id) == Enum.to_list(1..40)
      end

      test "replicated, un-backfilled WAL: no checkpoint is added, route :vacuum", ctx do
        %{id: id, wal: wal} = ctx
        {:ok, coordinator} = Shards.ensure(id)
        assert_mode!(coordinator, unquote(mode))

        conn = persistent_stream!(id, 1..20)
        on_exit(fn -> ShardExecutor.close(conn) end)
        refute Wal.fully_backfilled?(wal)

        Application.put_env(:fathom, :replication_enabled, true)
        flush!(coordinator)
        Application.delete_env(:fathom, :replication_enabled)

        assert_received {:route, :vacuum}
        refute_received {:route, :pinned_live}
        # No replication-off PASSIVE, and the WAL was not backfilled by the flush.
        refute_received {:flush_checkpoint, %{replicated: false}}
        refute Wal.fully_backfilled?(wal), "the replicated flush checkpointed the WAL"
        assert object_rows(id) == Enum.to_list(1..20)
      end

      test "replicated, WAL already fully backfilled: route :pinned_live", ctx do
        %{id: id, path: path, wal: wal} = ctx
        {:ok, coordinator} = Shards.ensure(id)
        assert_mode!(coordinator, unquote(mode))

        conn = persistent_stream!(id, 1..20)
        on_exit(fn -> ShardExecutor.close(conn) end)

        # Stand-in for a Session threshold / covered checkpoint that already folded the WAL.
        {:ok, raw} = Connection.open(path)
        {:ok, %{rows: [[0, n, n]]}} = Connection.query(raw, "PRAGMA wal_checkpoint(PASSIVE)", [])
        :ok = Connection.close(raw)
        assert n > 0 and Wal.fully_backfilled?(wal) and wal_size(wal) > 0

        Application.put_env(:fathom, :replication_enabled, true)
        flush!(coordinator)
        Application.delete_env(:fathom, :replication_enabled)

        assert_received {:route, :pinned_live}
        refute_received {:flush_checkpoint, %{replicated: false}}
        assert object_rows(id) == Enum.to_list(1..20)
      end
    end
  end

  describe "Wal.fully_backfilled?/1" do
    setup do
      path = Path.join(System.tmp_dir!(), "bf_#{System.unique_integer([:positive])}.db")
      on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
      {:ok, w} = Connection.open(path)
      on_exit(fn -> Connection.close(w) end)
      {:ok, _} = Connection.query(w, "PRAGMA wal_autocheckpoint=0", [])
      {:ok, _} = Connection.query(w, "CREATE TABLE t (a INTEGER)", [])
      %{path: path, wal: path <> "-wal", w: w}
    end

    test "tracks nBackfill == mxFrame as SQLite reports it", %{path: path, wal: wal, w: w} do
      refute Wal.fully_backfilled?(wal), "committed frames not yet checkpointed"

      {:ok, %{rows: [[0, n, n]]}} = Connection.query(w, "PRAGMA wal_checkpoint(PASSIVE)", [])
      assert n > 0
      assert Wal.fully_backfilled?(wal)

      # A reader pinned at this state, then more commits: PASSIVE stops at the reader's mark.
      {:ok, _} = Connection.query(w, "INSERT INTO t VALUES (1)", [])
      {:ok, r} = Connection.open(path)
      :ok = Connection.exec(r, "BEGIN")
      {:ok, _} = Connection.query(r, "SELECT count(*) FROM t", [])
      {:ok, _} = Connection.query(w, "INSERT INTO t VALUES (2)", [])
      {:ok, %{rows: [[_, log, done]]}} = Connection.query(w, "PRAGMA wal_checkpoint(PASSIVE)", [])
      assert done < log, "fixture: the reader must hold back the checkpoint"
      refute Wal.fully_backfilled?(wal)

      _ = Connection.rollback_if_open(r)
      :ok = Connection.close(r)
      {:ok, %{rows: [[0, l2, l2]]}} = Connection.query(w, "PRAGMA wal_checkpoint(PASSIVE)", [])
      assert Wal.fully_backfilled?(wal)
    end

    test "no -shm, or not a -wal path, is false", %{path: path} do
      refute Wal.fully_backfilled?(path <> ".nope-wal")
      refute Wal.fully_backfilled?(path)
    end
  end
end
