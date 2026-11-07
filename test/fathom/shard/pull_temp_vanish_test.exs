defmodule Fathom.Shard.PullTempVanishTest do
  @moduledoc """
  Expert review 2026-11-07 #9 (and #28a): a cold open must never install, or fence on, bytes it does
  not have.

  ## The defect

  The pull task renames `<id>.db.pull.dl.N` → `<id>.db.pull` and parks its `{:ok, etag}` reply in the
  coordinator's mailbox while the coordinator sits in `acquire_lease`. `Materializer.promote_pull/2`
  then chose between "install the temp" and "brand-new shard" by whether the temp was ON DISK. A temp
  that had vanished (the `TempReaper` unlinks a `.pull` whose mtime is older than 120 s, however
  slow the acquire; a stat error reads the same) fell into the brand-new-shard branch, which returned
  the object's REAL etag as the flush fence with no bytes installed. The coordinator served an empty
  database and its first write (or the idle drop) flushed it `If-Match: <real etag>` — successfully —
  over the stored object.

  ## What is pinned

    * the open FAILS (`{:pull_failed, :pull_temp_missing}`) and nothing is flushed: the stored object
      is byte-for-byte unchanged and no `.db` exists. The test drives the damage itself when an
      unfixed build opens, so the failure message names the loss rather than a missing error tag;
    * the reaper leaves a `.pull` alone while a coordinator is open for its shard, however old its
      mtime, and still reaps a `.pull` nothing owns;
    * an `{:absent, _}` pull never adopts a `.pull` it did not write (a young temp left by a crashed
      previous incarnation survives the age-gated open-time reap);
    * (#28a) a sidecar that cannot be written aborts the open instead of renaming the pulled `.db`
      beside a stale or absent one.

  Both liveness modes, mode asserted (the bug is before the lease mode matters, but the open path
  differs by it). A private data dir keeps the reaper sweep O(this test's files).
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Heartbeat, Materializer, Provenance, TempReaper}
  alias Fathom.Shard.Storage.Local
  alias Filo.{Stmt, StmtResult}

  @modes [:legacy, :heartbeat]

  @env [
    :shard_storage,
    :shard_data_dir,
    :faulty_before,
    :faulty_after_pull,
    :replication_enabled,
    :replication_promote_on_open,
    :replication_recover_from_peers,
    :shard_idle_ms,
    :shard_flush_interval_ms
  ]

  setup do
    shard = "pulltemp_#{System.unique_integer([:positive])}"
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})
    dir = Path.join(System.tmp_dir!(), "fathom_pulltemp_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    on_exit(fn ->
      Application.delete_env(:fathom, :faulty_before)
      Application.delete_env(:fathom, :faulty_after_pull)
      Shards.drain(shard, 2_000)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(dir)
      Local.purge_shard(shard)
    end)

    Application.put_env(:fathom, :shard_data_dir, dir)
    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)

    # A coordinator that must outlive the whole scenario: pin both timers high (the idle-stop trap).
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :shard_flush_interval_ms, 600_000)
    Application.put_env(:fathom, :replication_enabled, false)
    Application.put_env(:fathom, :replication_promote_on_open, false)
    Application.put_env(:fathom, :replication_recover_from_peers, false)

    %{shard: shard, db: Shard.db_path(shard)}
  end

  defp set_mode!(:legacy), do: refute(Heartbeat.running?(), "legacy mode needs the heartbeat OFF")

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(pid, :heartbeat),
    do: assert(is_integer(:sys.get_state(pid).acquire_gen), "heartbeat mode did not take")

  defp assert_mode!(pid, :legacy),
    do: assert(is_nil(:sys.get_state(pid).acquire_gen), "legacy mode did not take")

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  # A one-row database straight into the "remote" (no lease: a peer's earlier flush).
  defp put_object!(shard, v) do
    tmp = Path.join(System.tmp_dir!(), "pulltemp_seed_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(tmp)
    :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
    :ok = Connection.exec(c, "INSERT INTO kv VALUES ('#{v}')")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    :ok = Local.flush(shard, tmp)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    {:ok, etag} = Local.object_etag(shard)
    etag
  end

  # Park the open inside `acquire_lease` AFTER the pull has answered: the window the `.pull` sits
  # unclaimed in. The hook runs in the coordinator; `{:go, from}` releases it.
  defp open_parked!(shard, e0) do
    test_pid = self()

    Application.put_env(:fathom, :faulty_after_pull, fn
      ^shard, {:ok, etag} -> send(test_pid, {:pulled, etag})
      _, _ -> :ok
    end)

    Application.put_env(
      :fathom,
      :faulty_before,
      {:acquire,
       fn
         ^shard ->
           receive do
             {:go, from} -> send(from, :went)
           after
             10_000 -> flunk("the test never released the acquire")
           end

         _ ->
           :ok
       end}
    )

    {:ok, pid} = Shards.ensure(shard)
    assert_receive {:pulled, ^e0}, 5_000
    pid
  end

  defp release!(pid) do
    send(pid, {:go, self()})
    assert_receive :went, 5_000
  end

  defp tables(shard) do
    {:ok, conn} = ShardExecutor.open(shard)

    {:ok, %StmtResult{rows: rows}} =
      ShardExecutor.execute(conn, stmt("SELECT name FROM sqlite_master WHERE type = 'table'"))

    ShardExecutor.close(conn)
    rows |> List.flatten() |> Enum.sort()
  end

  # A database file holding one table, as raw bytes.
  defp db_bytes!(table) do
    tmp = Path.join(System.tmp_dir!(), "pulltemp_bytes_#{System.unique_integer([:positive])}.db")
    {:ok, c} = Connection.open(tmp)
    :ok = Connection.exec(c, "CREATE TABLE #{table} (v TEXT)")
    :ok = Connection.exec(c, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(c)
    bytes = File.read!(tmp)
    for s <- ["", "-wal", "-shm"], do: File.rm(tmp <> s)
    bytes
  end

  for mode <- @modes do
    describe "#{mode} mode" do
      setup do
        set_mode!(unquote(mode))
        :ok
      end

      test "a pull temp that vanished after the pull replied fails the open; the object is never overwritten",
           %{shard: shard, db: db} do
        e0 = put_object!(shard, "e0")
        temp = Materializer.pull_temp(db)

        pid = open_parked!(shard, e0)
        ref = Process.monitor(pid)
        # (The mode took: `set_mode!/1` asserted the heartbeat's state, and the coordinator cannot
        # be inspected while it is parked in its open.)
        assert File.exists?(temp), "fixture: the completed pull must be parked on disk"

        # What the reaper did (and what a stat error reads as): the temp is gone before promote.
        File.rm!(temp)
        refute File.exists?(temp)
        release!(pid)

        # `get_state` returns only once `handle_continue(:open)` has finished (it runs first), so
        # this says whether the open completed or the coordinator stopped.
        outcome =
          try do
            _ = :sys.get_state(pid)
            :opened
          catch
            :exit, _ -> :stopped
          end

        # An unfixed build opens, serving an empty db fenced on the REAL etag; the first write then
        # flushes over the object. Drive exactly that so the failure message names the loss.
        if outcome == :opened do
          capture_log(fn ->
            {:ok, conn} = ShardExecutor.open(shard)
            {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE junk (x)"))
            ShardExecutor.close(conn)
            Shards.flush(shard)
          end)
        end

        assert Local.object_etag(shard) == {:ok, e0},
               "an EMPTY database fenced on the real etag was flushed over the stored object"

        assert outcome == :stopped, "the open served despite having installed no bytes"

        assert_receive {:DOWN, ^ref, :process, ^pid,
                        {:shutdown, {:pull_failed, :pull_temp_missing}}},
                       5_000

        refute File.exists?(db), "no .db may exist after a refused install"

        # The retry re-pulls and serves the real object (the lease was released).
        Application.delete_env(:fathom, :faulty_before)
        assert "kv" in tables(shard)
      end

      test "the reaper leaves a completed .pull alone while a coordinator is open for its shard",
           %{shard: shard, db: db} do
        e0 = put_object!(shard, "e0")
        temp = Materializer.pull_temp(db)
        start_supervised!(TempReaper)
        _ = TempReaper.sweep()

        pid = open_parked!(shard, e0)
        assert File.exists?(temp), "fixture: the completed pull must be parked on disk"

        # A slow acquire / a forward clock step: the download's mtime is far past the 120 s gate.
        File.touch!(temp, {{2020, 1, 1}, {0, 0, 0}})
        assert File.stat!(temp, time: :posix).mtime < System.system_time(:second) - 3_600

        _ = TempReaper.sweep()

        assert File.exists?(temp),
               "the reaper unlinked a completed pull temp owned by a live coordinator"

        release!(pid)
        assert "kv" in tables(shard), "the open must promote the pull it was holding"
        assert_mode!(pid, unquote(mode))
        assert Local.object_etag(shard) == {:ok, e0}
      end

      test "a stale .pull with NO coordinator is still an orphan and is reaped", %{db: db} do
        start_supervised!(TempReaper)
        _ = TempReaper.sweep()

        orphan = db <> ".pull"
        File.write!(orphan, "left by a coordinator that died")
        File.touch!(orphan, {{2020, 1, 1}, {0, 0, 0}})

        assert TempReaper.sweep() >= 1
        refute File.exists?(orphan), "an unowned stale pull temp must still be reaped"
      end

      test "an {:absent, _} pull never adopts a .pull it did not write",
           %{shard: shard, db: db} do
        # The store holds no object. A YOUNG `.pull` from a crashed previous incarnation survives the
        # age-gated open-time reap; the unfixed `promote_pull/2` installed it as this shard's `.db`.
        Local.purge_shard(shard)
        planted = Materializer.pull_temp(db)
        File.write!(planted, db_bytes!("planted"))

        assert_mode_after_open!(shard, unquote(mode))

        assert tables(shard) == [], "the shard adopted another incarnation's pull temp"
        refute File.exists?(planted)
        assert Provenance.read(db) == :no_object
      end

      test "a sidecar that cannot be written aborts the open instead of installing the pull (#28a)",
           %{shard: shard, db: db} do
        e0 = put_object!(shard, "e0")

        # A directory where the sidecar file goes: the write fails (EISDIR) after the pull succeeded.
        File.mkdir_p!(Provenance.sidecar_path(db))
        assert {:error, _} = Fathom.FS.write(Provenance.sidecar_path(db), "x")

        {result, _log} = with_log(fn -> ShardExecutor.open(shard) end)

        assert {:error, _} = result,
               "the open served a pulled .db whose provenance sidecar was never recorded"

        refute File.exists?(db), "the pulled .db was renamed into place beside no sidecar"
        refute File.exists?(Materializer.pull_temp(db)), "the shard-sized temp must not linger"
        assert Local.object_etag(shard) == {:ok, e0}
      end
    end
  end

  # Start the coordinator and, once its open has run, assert which liveness mode it took.
  defp assert_mode_after_open!(shard, mode) do
    {:ok, pid} = Shards.ensure(shard)
    _ = :sys.get_state(pid)
    assert_mode!(pid, mode)
  end

  test "promote_pull with bytes reported and no temp is a refusal, never a brand-new shard",
       %{db: db} do
    assert {:error, :pull_temp_missing} = Materializer.promote_pull(db, "\"etag-1\"", :bytes)
    refute File.exists?(db)

    assert Provenance.read(db) == :missing,
           "no sidecar may be stamped for bytes that never landed"
  end
end
