defmodule Fathom.Shard.DropStampUnsyncedTest do
  @moduledoc """
  The DROP flush's post-PUT sidecar stamp is a `pwrite` with NO fsync (expert review 2026-10-17 #13),
  the same trade `SidecarStampUnsyncedTest` proves for the periodic flush (2026-10-17 #32).

  Both drop routes (`checkpoint_then_upload/2`, `snapshot_and_upload/1`) and the 412 retry
  (`retry_drop_upload/1`) PUT through `flush_intent_opts/1`, so the md5 of the bytes about to land is
  fsynced into the sidecar BEFORE the PUT. A stamp lost to an OS crash between the PUT and
  `drop_local` therefore leaves a sidecar that names the landed object by intent, and the reopen
  adopts it — the state the mailbox-gap crash already reaches.

    * **Invariant** — at the instant each drop PUT has landed, the last fsynced sidecar image already
      names that object, and NO fsync of the sidecar follows it during the drop. Pre-fix
      `write_durable/2` fsynced after the PUT, so the second half fails against it. Covered on the
      plain idle drop and on the 412 → `retry_drop_upload/1` path.
    * **Crash-injection** — the coordinator is killed at the stamp (after the PUT landed, before the
      stamp and `drop_local`), the sidecar is set to the last fsynced image (what the disk holds after
      an OS crash), and the shard is reopened: it must ADOPT its own object, quarantine nothing, and
      serve every acked row. Both liveness modes, mode asserted. This half passes against the unfixed
      code too (the fsynced intent existed before #13); it is the safety proof, not the regression.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Heartbeat, Provenance, Storage}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    shard = "dropstamp_#{System.unique_integer([:positive])}"
    prev = Map.new([:shard_storage, :shard_flush_interval_ms, :shard_idle_ms], &{&1, env(&1)})

    Application.put_env(:fathom, :shard_storage, Fathom.Test.FaultyStorage)
    Application.put_env(:fathom, :shard_flush_interval_ms, 600_000)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    on_exit(fn ->
      Application.delete_env(:fathom, :provenance_write_hook)
      Application.delete_env(:fathom, :storage_fault)
      Shards.drain(shard, 2_000)
      for {k, v} <- prev, do: restore(k, v)

      for dir <- [Shard.data_dir(), Storage.Local.dir()],
          suffix <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock", ".pos", ".lineage"],
          do: File.rm(Path.join(dir, shard <> suffix))

      for f <- Path.wildcard(Path.join(Shard.data_dir(), "#{shard}.db.*")), do: File.rm_rf(f)
    end)

    %{shard: shard}
  end

  defp env(k), do: Application.get_env(:fathom, k)
  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, value), do: Application.put_env(:fathom, key, value)

  defp stmt(sql), do: %Stmt{sql: sql, args: []}
  defp db(shard), do: Shard.db_path(shard)

  defp set_mode!(:legacy), do: refute(Heartbeat.running?(), "legacy mode needs the heartbeat OFF")

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(coordinator, :heartbeat),
    do: assert(is_integer(:sys.get_state(coordinator).acquire_gen), "heartbeat mode did not take")

  defp assert_mode!(coordinator, :legacy),
    do: assert(is_nil(:sys.get_state(coordinator).acquire_gen), "legacy mode did not take")

  # `:synced` = an fsynced in-place sidecar image; `:put_landed` = the post-PUT stamp is about to be
  # written (`:flush_stamp` post-fix, `:write_durable` pre-fix — both run after the PUT landed, before
  # the write), with the stored object's etag + md5 at that instant. `on_stamp` may kill the caller.
  defp capture!(shard, on_stamp \\ fn -> :ok end) do
    test_pid = self()
    path = db(shard)

    Application.put_env(:fathom, :provenance_write_hook, fn
      :synced, ^path ->
        send(test_pid, {:synced, File.read!(path <> ".etag")})

      stage, ^path when stage in [:flush_stamp, :write_durable] ->
        {:ok, %{etag: etag, md5: md5}} = Storage.object_head(shard)
        send(test_pid, {:put_landed, etag, md5})
        on_stamp.()

      _stage, _path ->
        :ok
    end)
  end

  defp drain_events(acc \\ []) do
    receive do
      {:synced, _} = e -> drain_events([e | acc])
      {:put_landed, _, _} = e -> drain_events([e | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp image_names?(image, etag, md5) do
    probe =
      Path.join(System.tmp_dir!(), "dropstamp_image_#{System.unique_integer([:positive])}.db")

    File.write!(probe <> ".etag", image)

    try do
      Provenance.read(probe) == {:ok, etag} or md5 in Provenance.read_intents(probe)
    after
      File.rm(probe <> ".etag")
    end
  end

  defp await_sidecar_task!(coordinator) do
    case :sys.get_state(coordinator).sidecar_task do
      %Task{pid: pid} ->
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000

      nil ->
        :ok
    end
  end

  # Open, create + write `rows`, take one periodic flush (so the sidecar is an in-place record with a
  # landed object), then write `tail` rows that only the DROP flush will carry.
  defp open_written!(shard, mode, tail) do
    set_mode!(mode)

    # Short idle: the connection stays checked out until `idle_drop!/2` closes it, so nothing drops
    # before then; closing it is what arms the drop.
    Application.put_env(:fathom, :shard_idle_ms, 50)
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, coordinator} = Shards.ensure(shard)
    assert_mode!(coordinator, mode)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE kv (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('r0')"))
    await_sidecar_task!(coordinator)
    :ok = Shards.flush(shard)

    for v <- tail,
        do: {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES ('#{v}')"))

    %{conn: conn, coordinator: coordinator}
  end

  # The idle drop: last connection closed, idle timer fires, terminate runs flush_then_drop.
  defp idle_drop!(%{conn: conn, coordinator: coordinator}, expect_reason) do
    ref = Process.monitor(coordinator)
    _ = drain_events()
    ShardExecutor.close(conn)
    assert_receive {:DOWN, ^ref, :process, ^coordinator, reason}, 5_000
    assert reason == expect_reason, "drop ended #{inspect(reason)}"
    drain_events()
  end

  defp assert_no_sync_after_put!(events) do
    landed = Enum.count(events, &match?({:put_landed, _, _}, &1))
    assert landed >= 1, "fixture: the drop never landed a PUT"

    Enum.reduce(events, {nil, false}, fn
      {:synced, image}, {_last, after_put?} ->
        refute after_put?, "the sidecar was FSYNCED after a drop PUT landed (one fsync too many)"
        {image, after_put?}

      {:put_landed, etag, md5}, {last, _} ->
        assert last != nil, "a drop PUT landed before any sidecar image was fsynced"
        assert image_names?(last, etag, md5), "a crash at this PUT would quarantine acked writes"
        {last, true}
    end)
  end

  defp served_rows(shard) do
    {:ok, conn} = ShardExecutor.open(shard)

    {:ok, %Filo.StmtResult{rows: rows}} =
      ShardExecutor.execute(conn, stmt("SELECT v FROM kv ORDER BY v"))

    ShardExecutor.close(conn)
    List.flatten(rows)
  end

  for mode <- @modes do
    describe "drop stamp without fsync (#{mode})" do
      test "idle drop: the drop PUT is already named by an fsynced image, and nothing fsyncs after it",
           %{shard: shard} do
        ctx = open_written!(shard, unquote(mode), ["r1"])
        capture!(shard)
        events = idle_drop!(ctx, :normal)
        assert_no_sync_after_put!(events)
        refute File.exists?(db(shard)), "the clean drop did not drop the local copy"
      end

      test "412 retry drop (our own lost-response PUT): same invariant", %{shard: shard} do
        ctx = open_written!(shard, unquote(mode), ["r1"])

        # A periodic PUT that LANDS but reports failure: the fence etag goes stale, so the drop PUT
        # 412s with the lock still ours and takes retry_drop_upload/1.
        Application.put_env(:fathom, :storage_fault, :flush_lands_then_errors)
        capture_log(fn -> assert {:error, _} = Shards.flush(shard) end)
        Application.delete_env(:fathom, :storage_fault)
        {:ok, _} = ShardExecutor.execute(ctx.conn, stmt("INSERT INTO kv VALUES ('r2')"))

        capture!(shard)
        test_pid = self()

        Application.put_env(
          :fathom,
          :faulty_before,
          {:flush, fn s -> if s == shard, do: send(test_pid, :drop_put) end}
        )

        on_exit(fn -> Application.delete_env(:fathom, :faulty_before) end)
        capture_log(fn -> send(self(), {:events, idle_drop!(ctx, :normal)}) end)
        assert_received {:events, events}

        # The 412'd drop PUT, then the re-fenced retry (Logger.info is below the test log level, so
        # the retry is counted at the storage seam rather than grepped).
        assert_received :drop_put
        assert_received :drop_put, "fixture: the retry path never ran"
        refute File.exists?(db(shard)), "the retry did not drop the local copy"
        assert_no_sync_after_put!(events)
      end

      test "OS crash between the drop PUT and drop_local: the reopen adopts its own object",
           %{shard: shard} do
        ctx = open_written!(shard, unquote(mode), ["r1", "r2"])
        capture!(shard, fn -> Process.exit(self(), :kill) end)
        events = idle_drop!(ctx, :killed)
        Application.delete_env(:fathom, :provenance_write_hook)

        image = events |> Enum.filter(&match?({:synced, _}, &1)) |> List.last() |> elem(1)
        {:ok, object_etag} = Storage.object_etag(shard)

        # Precondition: the durable sidecar is BEHIND the object (it never got the stamp), so the
        # reopen has to take the intent-adoption route.
        assert File.exists?(db(shard)), "fixture: the crash must leave the local copy"
        File.write!(db(shard) <> ".etag", image)
        refute Provenance.read(db(shard)) == {:ok, object_etag}, "fixture: the image is not stale"

        log = capture_log(fn -> send(self(), {:rows, served_rows(shard)}) end)
        assert_received {:rows, rows}

        assert log =~ "adopted", "the reopen did not adopt the node's own landed object"
        assert Path.wildcard(db(shard) <> ".forked.*") == [], "a good copy was quarantined"
        assert rows == ["r0", "r1", "r2"], "an acked row was lost across the crash"
        assert {:ok, ^object_etag} = Provenance.read(db(shard))
      end
    end
  end
end
