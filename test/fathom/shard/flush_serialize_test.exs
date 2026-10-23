defmodule Fathom.Shard.FlushSerializeTest do
  @moduledoc """
  Expert review 2026-10-22 r8 #9 (user decision Q8 = A): the periodic flush's VACUUM route takes
  its snapshot with `sqlite3_serialize` (a page copy inside the same read transaction as the
  integrity check) for images up to `:flush_serialize_max_bytes`, and keeps `VACUUM INTO` above the
  cap, for mostly-free files (only VACUUM compacts), and when the integrity gate is off.

  What is pinned:

    * the copy includes committed WAL frames the main file does not hold yet (a stream is held
      open so nothing folds the WAL), in BOTH lease modes, and the object is a valid database;
    * each fallback takes `VACUUM INTO` and still uploads everything;
    * the integrity gate still refuses a corrupt live file, and nothing is uploaded;
    * the gate checks the IMAGE (rv8 F1): `sqlite3_serialize` zero-fills a page it cannot read and
      still succeeds, so a zeroed page in the copy must divert to `VACUUM INTO`, never be uploaded;
    * a serialize error falls back to `VACUUM INTO` instead of failing the flush (rv8 F2).

  The pinned-live route is switched off (`:flush_pinned_live_upload`) so every flush here takes the
  VACUUM route under test. Not async: telemetry handlers, app env, real shard files.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Heartbeat, Storage}
  alias Filo.Stmt

  @env_keys [
    :shard_idle_ms,
    :flush_pinned_live_upload,
    :flush_serialize_max_bytes,
    :verify_flush_integrity,
    :flush_serialize_result_hook
  ]

  setup do
    id = "flushser_#{System.unique_integer([:positive])}"
    path = Shard.db_path(id)
    prev = for k <- @env_keys, do: {k, Application.fetch_env(:fathom, k)}
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :flush_pinned_live_upload, false)

    test_pid = self()
    handler = "flushser-#{id}"

    :telemetry.attach_many(
      handler,
      [[:fathom, :shard, :flush, :snapshot], [:fathom, :shard, :flush, :route]],
      fn
        [:fathom, :shard, :flush, :snapshot], _, %{shard_id: ^id, mode: m}, _ ->
          send(test_pid, {:snapshot, m})

        [:fathom, :shard, :flush, :route], _, %{shard_id: ^id, route: r}, _ ->
          send(test_pid, {:route, r})

        _, _, _, _ ->
          :ok
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)

      for {k, v} <- prev do
        case v do
          {:ok, val} -> Application.put_env(:fathom, k, val)
          :error -> Application.delete_env(:fathom, k)
        end
      end

      Shards.stop(id)
      for f <- Path.wildcard(path <> ".corrupt.*"), do: File.rm(f)
      for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(path <> s)

      for suffix <- [".db", ".db-wal", ".db-shm", ".lock"],
          do: File.rm(Path.join(Storage.Local.dir(), id <> suffix))
    end)

    %{id: id, path: path}
  end

  defp stmt(sql), do: %Stmt{sql: sql, args: []}

  defp set_mode!(:legacy), do: refute(Heartbeat.running?())

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    assert Heartbeat.running?()
  end

  defp assert_mode!(coordinator, :heartbeat),
    do: assert(is_integer(:sys.get_state(coordinator).acquire_gen), "heartbeat mode did not take")

  defp assert_mode!(coordinator, :legacy),
    do: assert(is_nil(:sys.get_state(coordinator).acquire_gen), "legacy mode did not take")

  defp object(id), do: Path.join(Storage.Local.dir(), id <> ".db")

  defp count_rows(file, opener \\ &Connection.open/1) do
    {:ok, c} = opener.(file)

    try do
      {:ok, %{rows: [[n]]}} = Connection.query(c, "SELECT count(*) FROM t", [])
      n
    after
      Connection.close(c)
    end
  end

  # Seed `n` rows through a stream that STAYS OPEN, so no close-time checkpoint folds the WAL: the
  # rows past the CREATE live only in `-wal` frames. Returns the open stream.
  defp seed_in_wal!(id, path, n) do
    {:ok, conn} = ShardExecutor.open(id)

    {:ok, _} =
      ShardExecutor.execute(conn, stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"))

    {:ok, _} =
      ShardExecutor.execute(
        conn,
        stmt(
          "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < #{n}) " <>
            "INSERT INTO t SELECT x, randomblob(200) FROM c"
        )
      )

    assert File.stat!(path <> "-wal").size > 0, "fixture: the rows must be in WAL frames"
    conn
  end

  defp flush!(id) do
    {:ok, coordinator} = Shards.ensure(id)
    send(coordinator, :durability_flush)
    settle(coordinator)
    coordinator
  end

  defp settle(c, tries \\ 400) do
    cond do
      :sys.get_state(c).flush_task == nil -> :ok
      tries == 0 -> flunk("flush task never settled")
      true -> wait_then(10, fn -> settle(c, tries - 1) end)
    end
  end

  defp wait_then(ms, fun) do
    receive do
    after
      ms -> fun.()
    end
  end

  for mode <- [:legacy, :heartbeat] do
    test "[#{mode}] the VACUUM route serializes, WAL frames included, and uploads a valid db", %{
      id: id,
      path: path
    } do
      set_mode!(unquote(mode))
      {:ok, coordinator} = Shards.ensure(id)
      assert_mode!(coordinator, unquote(mode))
      conn = seed_in_wal!(id, path, 2_000)

      # Precondition: the main file alone does NOT hold the rows (they are WAL frames), so an
      # object with all 2,000 proves the copy applied the WAL overlay.
      main_only =
        try do
          count_rows(path, &Connection.open_immutable/1)
        rescue
          MatchError -> :no_table
        end

      assert main_only in [0, :no_table], "fixture: rows already folded (#{inspect(main_only)})"

      flush!(id)
      ShardExecutor.close(conn)

      assert_received {:route, :vacuum}
      assert_received {:snapshot, :serialize}
      refute_received {:snapshot, :vacuum}
      assert count_rows(object(id)) == 2_000
      assert :ok = Shard.verify_integrity(object(id))
    end
  end

  test "an image above :flush_serialize_max_bytes falls back to VACUUM INTO", %{
    id: id,
    path: path
  } do
    Application.put_env(:fathom, :flush_serialize_max_bytes, 4096)
    conn = seed_in_wal!(id, path, 500)

    flush!(id)
    ShardExecutor.close(conn)

    assert_received {:snapshot, :vacuum}
    refute_received {:snapshot, :serialize}
    assert count_rows(object(id)) == 500
  end

  test ":flush_serialize_max_bytes 0 disables the page copy", %{id: id, path: path} do
    Application.put_env(:fathom, :flush_serialize_max_bytes, 0)
    conn = seed_in_wal!(id, path, 100)

    flush!(id)
    ShardExecutor.close(conn)

    assert_received {:snapshot, :vacuum}
    assert count_rows(object(id)) == 100
  end

  test "with the integrity gate off the snapshot stays VACUUM INTO", %{id: id, path: path} do
    Application.put_env(:fathom, :verify_flush_integrity, false)
    conn = seed_in_wal!(id, path, 100)

    flush!(id)
    ShardExecutor.close(conn)

    assert_received {:snapshot, :vacuum}
    refute_received {:snapshot, :serialize}
    assert count_rows(object(id)) == 100
  end

  # Only VACUUM compacts: a mostly-free image must not be page-copied (free pages and all).
  test "a mostly-free file still takes VACUUM INTO and uploads the compacted object", %{
    id: id,
    path: path
  } do
    {:ok, conn} = ShardExecutor.open(id)

    {:ok, _} =
      ShardExecutor.execute(conn, stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"))

    {:ok, _} =
      ShardExecutor.execute(
        conn,
        stmt(
          "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 800) " <>
            "INSERT INTO t SELECT x, randomblob(4096) FROM c"
        )
      )

    {:ok, _} = ShardExecutor.execute(conn, stmt("DELETE FROM t WHERE id % 10 != 0"))

    flush!(id)
    ShardExecutor.close(conn)

    assert_received {:snapshot, :vacuum}
    refute_received {:snapshot, :serialize}
    assert count_rows(object(id)) == 80
    assert File.stat!(object(id)).size < 1_000_000, "the object was not compacted"
    _ = path
  end

  # Expert review 2026-10-22 r8 #25b: `snapshot_and_upload/3` now unlinks only the temp itself. Safe
  # only because neither snapshot producer leaves a companion file; this pins that for both, by
  # tracing every file the flush creates next to the shard while it runs.
  for cap <- [0, 16 * 1024 * 1024] do
    test "the snapshot producer (cap #{cap}) leaves no -wal/-shm/-journal beside its temp", %{
      id: id,
      path: path
    } do
      Application.put_env(:fathom, :flush_serialize_max_bytes, unquote(cap))
      conn = seed_in_wal!(id, path, 300)
      {:ok, coordinator} = Shards.ensure(id)

      # Observe the directory from INSIDE the window: the after-snapshot hook runs between the
      # snapshot and the PUT, while the temp (and anything the producer left beside it) exists.
      test_pid = self()

      Application.put_env(:fathom, :flush_after_snapshot_hook, fn _ ->
        send(test_pid, {:siblings, Path.wildcard(path <> ".snap.*")})
        :ok
      end)

      on_exit(fn -> Application.delete_env(:fathom, :flush_after_snapshot_hook) end)

      send(coordinator, :durability_flush)
      settle(coordinator)
      ShardExecutor.close(conn)

      assert_received {:siblings, files}
      assert [temp] = files, "expected exactly the snapshot temp, got #{inspect(files)}"
      refute String.ends_with?(temp, ["-wal", "-shm", "-journal"])
      assert count_rows(object(id)) == 300
    end
  end

  describe "FLUSH_SERIALIZE_MAX_BYTES" do
    defp runtime_fathom(env) do
      prev = Map.new(env, fn {k, _} -> {k, System.get_env(k)} end)

      Enum.each(env, fn
        {k, nil} -> System.delete_env(k)
        {k, v} -> System.put_env(k, v)
      end)

      try do
        "config/runtime.exs" |> Config.Reader.read!(env: :test) |> Keyword.get(:fathom, [])
      after
        Enum.each(prev, fn
          {k, nil} -> System.delete_env(k)
          {k, v} -> System.put_env(k, v)
        end)
      end
    end

    test "parses a non-negative integer; unset leaves the default; garbage fails the boot" do
      assert runtime_fathom(%{"FLUSH_SERIALIZE_MAX_BYTES" => "0"})[:flush_serialize_max_bytes] ==
               0

      assert runtime_fathom(%{"FLUSH_SERIALIZE_MAX_BYTES" => "1048576"})[
               :flush_serialize_max_bytes
             ] == 1_048_576

      refute Keyword.has_key?(
               runtime_fathom(%{"FLUSH_SERIALIZE_MAX_BYTES" => nil}),
               :flush_serialize_max_bytes
             )

      for bad <- ["-1", "16MiB", ""] do
        assert_raise RuntimeError, ~r/FLUSH_SERIALIZE_MAX_BYTES/, fn ->
          runtime_fathom(%{"FLUSH_SERIALIZE_MAX_BYTES" => bad})
        end
      end
    end
  end

  # Was "refused before any copy": since rv8 F1 the gate runs on the copy, so a corrupt shard pays
  # one capped page copy, fails the image check, and is refused by the fallback's live check.
  test "a corrupt live file is refused and nothing is uploaded", %{id: id, path: path} do
    {:ok, conn} = ShardExecutor.open(id)

    {:ok, _} =
      ShardExecutor.execute(conn, stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)"))

    {:ok, _} =
      ShardExecutor.execute(
        conn,
        stmt(
          "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 500) " <>
            "INSERT INTO t SELECT x, randomblob(200) FROM c"
        )
      )

    ShardExecutor.close(conn)
    # Fold everything into the main file with a trusted connection, then corrupt a data page.
    {:ok, trusted} = Connection.open(path)
    {:ok, _} = Connection.query(trusted, "PRAGMA wal_checkpoint(TRUNCATE)", [])
    {:ok, fd} = :file.open(path, [:read, :write, :binary, :raw])
    :ok = :file.pwrite(fd, 4096, :binary.copy(<<0xEF>>, 4096))
    :ok = :file.close(fd)
    assert {:error, _} = Shard.verify_integrity(path), "fixture: nothing was corrupted"

    {:ok, holder} = ShardExecutor.open(id)
    {:ok, coordinator} = Shards.ensure(id)
    send(coordinator, :durability_flush)
    settle(coordinator)
    ShardExecutor.close(holder)
    :ok = Connection.close(trusted)

    refute_received {:snapshot, _}
    refute File.exists?(object(id)), "a corrupt live file was uploaded"
  end

  # Zero one page of `blob`, the way `sqlite3_serialize` fills a page whose read failed.
  defp zero_page(blob, pgno) do
    <<_::binary-size(16), ps_raw::16, _::binary>> = blob
    ps = if ps_raw == 1, do: 65_536, else: ps_raw
    off = (pgno - 1) * ps
    <<head::binary-size(^off), _::binary-size(^ps), tail::binary>> = blob
    head <> :binary.copy(<<0>>, ps) <> tail
  end

  # rv8 F1 (expert review 2026-10-22 r8 #9). SQLite's `sqlite3_serialize` memsets a page it fails to
  # read (transient EIO, NOMEM) to zero and still returns the buffer. The old route checked the
  # LIVE pages in the transaction and uploaded the image unchecked, so the zeroed page became the
  # authoritative object. The hook stands in for the failed read: it zeroes the table's root page.
  for mode <- [:legacy, :heartbeat] do
    test "[#{mode}] a serialized image with a zeroed page is never uploaded; VACUUM INTO is", %{
      id: id,
      path: path
    } do
      set_mode!(unquote(mode))
      {:ok, coordinator} = Shards.ensure(id)
      assert_mode!(coordinator, unquote(mode))
      conn = seed_in_wal!(id, path, 2_000)
      test_pid = self()

      Application.put_env(:fathom, :flush_serialize_result_hook, fn {:ok, blob} ->
        # Page 2 is `t`'s root (the first CREATE TABLE), an interior page at 2,000 rows.
        bad = zero_page(blob, 2)
        send(test_pid, {:zeroed, bad})
        {:ok, bad}
      end)

      flush!(id)
      ShardExecutor.close(conn)

      # Precondition: the image really was damaged in a way the gate can see.
      assert_received {:zeroed, bad}
      probe = path <> ".probe-zeroed"
      File.write!(probe, bad)
      on_exit(fn -> File.rm(probe) end)
      assert {:error, _} = Shard.verify_integrity(probe), "fixture: the zeroed page is harmless"

      refute_received {:snapshot, :serialize}
      assert_received {:snapshot, :vacuum}
      assert :ok = Shard.verify_integrity(object(id)), "the zero-filled image was uploaded"
      assert count_rows(object(id)) == 2_000
    end
  end

  # rv8 F2: a serialize error (a NULL buffer on NOMEM) used to fail the flush every interval for
  # that shard although `VACUUM INTO` would have worked.
  test "a serialize error falls back to VACUUM INTO and the flush succeeds", %{
    id: id,
    path: path
  } do
    Application.put_env(:fathom, :flush_serialize_result_hook, fn _ -> {:error, :nomem} end)
    conn = seed_in_wal!(id, path, 300)

    coordinator = flush!(id)
    ShardExecutor.close(conn)

    refute_received {:snapshot, :serialize}
    assert_received {:snapshot, :vacuum}
    assert count_rows(object(id)) == 300

    assert :sys.get_state(coordinator).flush_failures == 0,
           "the flush failed instead of falling back"
  end
end
