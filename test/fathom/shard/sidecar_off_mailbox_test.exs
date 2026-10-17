defmodule Fathom.Shard.SidecarOffMailboxTest do
  @moduledoc """
  The durable provenance-sidecar writes run OFF the coordinator mailbox (expert review 2026-10-17
  #5). Two writes used to run in the coordinator's own `handle_*`: the post-PUT stamp
  (`Provenance.write_durable`, in `apply_flush_verdict/2`) and the first-write fsync
  (`Provenance.make_durable`, in the `:became_dirty` handler). Each is an open + pwrite + fsync,
  measured 7 ms p50 / 10 ms p99, and every checkout/checkin/:DOWN for the shard queued behind it.

  The stamp now runs in the flush task right after `Storage.flush` returns, and the verdict's
  arrival is the proof it is done; the first-write fsync runs in a `sidecar_task` the flush task waits
  for and every terminate clause settles.

  ## What is pinned

    * **The coordinator answers while a sidecar write is in flight** — held there through the
      `:provenance_write_hook` seam (no sleeps). Pre-fix the hook fires INSIDE the coordinator, so
      the checkout times out; the first assertion also names which process runs the write.
    * **The ordering invariant:** once the coordinator has applied a flush verdict its sidecar
      already names the new etag, with no intent left over.
    * **A crash between the PUT and the stamp** still recovers through the recorded PUT intent
      (`Fork` adopts the node's own landed object) — nothing quarantined, nothing lost.

  Parameterized over both liveness modes (AGENTS.md § Testing): `acquire_gen` is fixed at open and
  `config/test.exs` defaults a test to LEGACY, so each scenario asserts the mode took.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Provenance, Storage}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "sidecar_#{System.unique_integer([:positive])}"
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_flush = Application.get_env(:fathom, :shard_flush_interval_ms)

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    # No periodic flush: the test sends `:durability_flush` itself.
    Application.put_env(:fathom, :shard_flush_interval_ms, 600_000)

    on_exit(fn ->
      Application.delete_env(:fathom, :provenance_write_hook)
      Application.delete_env(:fathom, :storage_fault)
      restore(:shard_storage, prev_storage)
      restore(:shard_flush_interval_ms, prev_flush)
      Shards.drain(shard, 2_000)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock", ".pos", ".lineage"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db.*")), do: File.rm(f)
    end)

    %{shard: shard}
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}
  defp db(shard), do: Shard.db_path(shard)

  defp set_mode!(:legacy) do
    refute Heartbeat.running?(), "legacy mode needs the heartbeat OFF"
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

  # Hold the sidecar write for `stage` on `shard`'s sidecar at the seam: report the PROCESS running
  # it, then block until told to go. Writes of other stages / other shards pass straight through.
  defp hold_sidecar_write!(shard, stage) do
    test_pid = self()
    path = db(shard)

    Application.put_env(:fathom, :provenance_write_hook, fn
      ^stage, ^path ->
        send(test_pid, {:sidecar_write, stage, self()})

        receive do
          :go -> :ok
        after
          10_000 -> :ok
        end

      _stage, _path ->
        :ok
    end)
  end

  defp release_hook!, do: Application.delete_env(:fathom, :provenance_write_hook)

  # A seeded, flushed-once shard in `mode`, with a second write pending so the next flush has work.
  defp open_dirty!(shard, mode) do
    set_mode!(mode)
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, coordinator} = Shards.ensure(shard)
    assert_mode!(coordinator, mode)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('first')"))
    # The first-write sidecar fsync is its own (asynchronous) task now; let it finish.
    await_sidecar_task!(coordinator)
    :ok = Shards.flush(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('second')"))
    %{conn: conn, coordinator: coordinator}
  end

  defp await_sidecar_task!(coordinator) do
    case :sys.get_state(coordinator).sidecar_task do
      %Task{pid: pid} ->
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
        # The coordinator folds the reply from its mailbox; sync past it.
        _ = :sys.get_state(coordinator)
        :ok

      nil ->
        :ok
    end
  end

  for mode <- @modes do
    describe "post-PUT stamp (#{mode})" do
      test "runs in the flush task and the coordinator keeps answering meanwhile", %{shard: shard} do
        %{conn: conn, coordinator: coordinator} = open_dirty!(shard, unquote(mode))
        hold_sidecar_write!(shard, :flush_stamp)

        send(coordinator, :durability_flush)
        assert_receive {:sidecar_write, :flush_stamp, writer}, 5_000

        # The invariant under test: the write is NOT in the coordinator's mailbox. Pre-fix the
        # coordinator itself runs it, `writer == coordinator`, and everything below times out.
        refute writer == coordinator, "the post-PUT sidecar stamp still runs in the coordinator"
        assert %Task{pid: ^writer} = :sys.get_state(coordinator).flush_task

        # A checkout + statement while the stamp is held mid-flight.
        probe = Task.async(fn -> ShardExecutor.open(shard) end)
        assert {:ok, {:ok, conn2}} = Task.yield(probe, 3_000) || Task.shutdown(probe)
        assert {:ok, _} = ShardExecutor.execute(conn2, stmt("SELECT count(*) FROM kv"))
        :ok = ShardExecutor.close(conn2)

        release_hook!()
        send(writer, :go)
        :ok = Shards.flush(shard)
        :ok = ShardExecutor.close(conn)
      end

      test "once the verdict is applied the sidecar already names the new etag", %{shard: shard} do
        %{conn: conn, coordinator: coordinator} = open_dirty!(shard, unquote(mode))
        {:ok, before_etag} = Storage.object_etag(shard)

        :ok = Shards.flush(shard)
        {:ok, after_etag} = Storage.object_etag(shard)
        refute after_etag == before_etag, "fixture: the second flush uploaded nothing"

        # The coordinator folded the verdict (its fence etag moved)...
        assert %{etag: ^after_etag} = :sys.get_state(coordinator)
        # ...so the sidecar the next open will read must already say the same, intent resolved.
        assert {:ok, ^after_etag} = Provenance.read(db(shard))
        assert Provenance.read_intents(db(shard)) == []

        :ok = ShardExecutor.close(conn)
      end

      test "a crash between the PUT and the stamp still recovers through the PUT intent",
           %{shard: shard} do
        %{conn: conn, coordinator: coordinator} = open_dirty!(shard, unquote(mode))
        {:ok, e0} = Storage.object_etag(shard)
        hold_sidecar_write!(shard, :flush_stamp)

        send(coordinator, :durability_flush)
        assert_receive {:sidecar_write, :flush_stamp, writer}, 5_000

        # The PUT has landed (the hook runs after Storage.flush returned) and the sidecar has not
        # learned it: the state a node killed in this window leaves behind.
        {:ok, e1} = Storage.object_etag(shard)
        refute e1 == e0, "fixture: the PUT had not landed yet"
        assert {:ok, ^e0} = Provenance.read(db(shard))
        assert is_binary(Provenance.read_intent(db(shard))), "the PUT intent was not recorded"

        ref = Process.monitor(coordinator)
        Process.exit(coordinator, :kill)
        assert_receive {:DOWN, ^ref, :process, ^coordinator, :killed}, 2_000
        wref = Process.monitor(writer)
        assert_receive {:DOWN, ^wref, :process, ^writer, _}, 2_000
        release_hook!()
        _ = :sys.get_state(Fathom.ShardSupervisor)
        ShardExecutor.close(conn)

        log =
          capture_log(fn ->
            {:ok, conn2} = ShardExecutor.open(shard)

            assert {:ok, %Filo.StmtResult{rows: rows}} =
                     ShardExecutor.execute(conn2, stmt("SELECT v FROM kv ORDER BY v"))

            assert List.flatten(rows) |> Enum.map(&to_string/1) == ["first", "second"],
                   "an acked write was lost across the crash"

            :ok = ShardExecutor.close(conn2)
          end)

        assert log =~ "adopted", "the reopen did not adopt the node's own landed PUT"
        assert Path.wildcard(db(shard) <> ".forked.*") == [], "a good copy was quarantined"
        assert {:ok, ^e1} = Provenance.read(db(shard))
      end
    end
  end

  describe "first-write fsync" do
    test "runs in a task: the coordinator answers, and the flush waits behind it", %{
      shard: shard
    } do
      set_mode!(:legacy)
      hold_sidecar_write!(shard, :make_durable)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))

      assert_receive {:sidecar_write, :make_durable, writer}, 5_000
      refute writer == coordinator, "make_durable still runs in the coordinator mailbox"
      assert %Task{pid: ^writer} = :sys.get_state(coordinator).sidecar_task

      # Still answering, and still writable, with the fsync held.
      assert {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('x')"))

      # A flush tick while the fsync is in flight starts (it is not silently dropped) but its task
      # waits for the fsync before touching the sidecar or the object: no PUT while it is held.
      send(coordinator, :durability_flush)
      assert %Task{} = :sys.get_state(coordinator).flush_task
      assert {:ok, nil} = Storage.object_etag(shard), "the flush PUT raced the held sidecar fsync"

      release_hook!()
      send(writer, :go)
      :ok = Shards.flush(shard)
      await_sidecar_task!(coordinator)
      assert %{sidecar_task: nil} = :sys.get_state(coordinator)
      assert {:ok, etag} = Storage.object_etag(shard)
      assert {:ok, ^etag} = Provenance.read(db(shard))
      :ok = ShardExecutor.close(conn)
    end

    test "a drain settles an in-flight first-write fsync before the drop flush", %{shard: shard} do
      set_mode!(:legacy)
      hold_sidecar_write!(shard, :make_durable)

      {:ok, conn} = ShardExecutor.open(shard)
      {:ok, coordinator} = Shards.ensure(shard)
      {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
      assert_receive {:sidecar_write, :make_durable, writer}, 5_000
      :ok = ShardExecutor.close(conn)

      ref = Process.monitor(coordinator)
      drain = Task.async(fn -> Shards.drain(shard, 5_000) end)

      # terminate/2 is waiting on the held task: the coordinator must not be gone yet, and must
      # not have run its drop flush over the sidecar the task is about to write.
      refute_receive {:DOWN, ^ref, :process, ^coordinator, _}, 200

      release_hook!()
      send(writer, :go)
      assert_receive {:DOWN, ^ref, :process, ^coordinator, _}, 10_000
      Task.await(drain, 10_000)

      # The drop flush ran after the settle: the object is current and the shard is cold.
      assert {:ok, _} = Storage.object_etag(shard)
    end
  end
end
