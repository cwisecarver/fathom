defmodule Fathom.FSBypassTest do
  @moduledoc """
  Expert review 2026-10-21 #4: every non-raw `File.*` call is one message to the node's single
  `file_server_2` process, so shard cold-open/drop and A2 follower file ops serialized on it
  node-wide (measured: 36 requests per cold-open + drop cycle on the Local backend, ~25 of them
  coordinator-side).

  These tests pin that the shard lifecycle's OWN files (`<data>/<id>.db`, `-wal`, `-shm`,
  `.etag`, provenance sidecar, `.retained`, `.recovering`, `.pull` markers) make no
  `file_server_2` requests, plus the `Fathom.FS` semantics that make the swap behaviour-neutral.
  The Local test double's own temp copy (`*.pull.tmp.*`, a `File.cp`) is excluded: it is the
  double, not the coordinator.

  Gotcha pinned here: `File.exists?(path, raw: true)` does NOT bypass the server (Elixir hands
  `{:raw, true}` to `:file`, which only honours the bare atom), so `Fathom.FS` uses `:file`
  directly.
  """
  use ExUnit.Case, async: false

  alias Fathom.{FS, Shards}
  alias Fathom.Shard.Connection

  defp fs_requests_touching(id, fun, dir \\ nil) do
    dir = dir || Path.dirname(Fathom.Shard.db_path(id))
    fs = Process.whereis(:file_server_2)
    :erlang.trace(fs, true, [:receive])

    try do
      fun.()
      # a trace message is delivered before the traced process's next receive; a round trip
      # through the server orders everything we caused ahead of this marker.
      _ = :file.read_file_info("/", [])
      collect([])
    after
      :erlang.trace(fs, false, [:receive])
    end
    |> Enum.filter(fn
      {:"$gen_call", _from, req} when is_tuple(req) and tuple_size(req) > 1 ->
        arg = elem(req, 1)

        is_binary(arg) and String.starts_with?(arg, dir) and String.contains?(arg, id) and
          not String.contains?(arg, ".pull.tmp.")

      _ ->
        false
    end)
  end

  defp collect(acc) do
    receive do
      {:trace, _, :receive, m} -> collect([m | acc])
    after
      200 -> Enum.reverse(acc)
    end
  end

  test "shard cold open, write, and drop make no file_server_2 request on the shard's own files" do
    id = "fsbypass_#{System.unique_integer([:positive])}"

    reqs =
      fs_requests_touching(id, fn ->
        {:ok, pid, ref, path} = Shards.checkout(id)
        {:ok, conn} = Connection.open(path)
        Connection.exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        Connection.exec(conn, "INSERT INTO t (v) VALUES ('x')")
        Connection.close(conn)
        Fathom.Shard.checkin(pid, ref)
        :ok = Shards.drain(id)
      end)

    assert reqs == [],
           "shard lifecycle routed file ops through file_server_2: #{inspect(reqs, limit: 20)}"
  end

  # Expert review 2026-10-21 r7 #4 / #20. Symptom: every failed/held/refused cold open ran
  # `Path.wildcard(<pull>.dl.*)` (a full readdir of the fleet-sized data dir, one file_server_2
  # request per call) and `Storage.ensure_dir` used non-raw `File.dir?`. The guard above filters on
  # the shard id so neither was visible; these count ALL requests under the data dir.
  describe "abandoned pull and ensure_dir" do
    @describetag :tmp_dir

    test "abandon_pull removes every .dl temp with no file_server_2 request (no dir scan)",
         %{tmp_dir: dir} do
      path = Path.join(dir, "victim.db")
      pull = Fathom.Shard.Materializer.pull_temp(path)
      temps = Fathom.Shard.Storage.S3.download_temps(pull)
      assert length(temps) == 3
      for t <- temps, do: File.write!(t, "x")
      File.write!(Path.join(dir, "bystander.db"), "x")

      task = Task.async(fn -> :ok end)
      _ = Task.await(task)

      reqs =
        fs_requests_touching(
          "",
          fn -> :ok = Fathom.Shard.Materializer.abandon_pull(task, path) end,
          dir
        )

      assert reqs == [], "abandon_pull hit file_server_2: #{inspect(reqs, limit: 20)}"
      for t <- temps, do: refute(File.exists?(t))
      assert File.exists?(Path.join(dir, "bystander.db"))
    end

    test "Storage.ensure_dir on an existing dir makes no file_server_2 request", %{tmp_dir: dir} do
      reqs =
        fs_requests_touching("", fn -> :ok = Fathom.Shard.Storage.ensure_dir(dir) end, dir)

      assert reqs == []
      new = Path.join(dir, "a/b")
      assert :ok = Fathom.Shard.Storage.ensure_dir(new)
      assert File.dir?(new)
    end
  end

  describe "Fathom.FS matches File.* semantics" do
    @describetag :tmp_dir

    test "exists?/dir?/regular?", %{tmp_dir: dir} do
      f = Path.join(dir, "f")
      File.write!(f, "x")
      assert FS.exists?(f) and FS.regular?(f) and not FS.dir?(f)
      assert FS.exists?(dir) and FS.dir?(dir) and not FS.regular?(dir)
      refute FS.exists?(Path.join(dir, "nope"))
    end

    test "stat returns File.Stat, honours :time", %{tmp_dir: dir} do
      f = Path.join(dir, "f")
      File.write!(f, "abc")
      assert {:ok, %File.Stat{size: 3} = a} = FS.stat(f)
      assert {:ok, ^a} = File.stat(f)
      assert {:ok, %File.Stat{mtime: m}} = FS.stat(f, time: :posix)
      assert is_integer(m)
      assert {:error, :enoent} = FS.stat(Path.join(dir, "nope"))
    end

    test "rm/rename/read/write", %{tmp_dir: dir} do
      a = Path.join(dir, "a")
      b = Path.join(dir, "b")
      assert :ok = FS.write(a, "hello")
      assert {:ok, "hello"} = FS.read(a)
      assert :ok = FS.rename(a, b)
      assert {:error, :enoent} = FS.read(a)
      assert :ok = FS.rm(b)
      assert {:error, :enoent} = FS.rm(b)
      assert {:error, :enoent} = FS.rename(a, b)
    end

    test "the helper itself makes no file_server_2 request", %{tmp_dir: dir} do
      f = Path.join(dir, "f")

      reqs =
        fs_requests_touching(
          Path.basename(dir),
          fn ->
            FS.write(f, "x")
            FS.exists?(f)
            FS.dir?(dir)
            FS.regular?(f)
            FS.stat(f)
            FS.read(f)
            FS.rename(f, f <> "2")
            FS.rm(f <> "2")
          end,
          dir
        )

      assert reqs == []
    end
  end

  # Expert review 2026-10-22 r8 #7. Symptom: `File.ls/1` / `Path.wildcard` on the fleet-sized data
  # dir built the whole name list inside `file_server_2` (2.9 KB -> 69 MB per 120k entries, never
  # reclaimed, every other non-raw file op stalled ~110 ms). Invariant: a listing makes NO request
  # to that process.
  describe "list_dir_isolated" do
    @describetag :tmp_dir

    test "returns the same names as File.ls, as binaries", %{tmp_dir: dir} do
      for n <- ["a.db", "a.db-wal", "b.db.fenced.1", "sub"],
          do: File.write!(Path.join(dir, n), "")

      assert {:ok, names} = FS.list_dir_isolated(dir)
      assert Enum.sort(names) == Enum.sort(File.ls!(dir))
      assert Enum.all?(names, &is_binary/1)
    end

    test "missing dir is {:error, :enoent}; empty dir is {:ok, []}", %{tmp_dir: dir} do
      assert {:error, :enoent} = FS.list_dir_isolated(Path.join(dir, "nope"))
      assert {:ok, []} = FS.list_dir_isolated(dir)
    end

    test "makes no file_server_2 request", %{tmp_dir: dir} do
      for i <- 1..200, do: File.write!(Path.join(dir, "t#{i}.db"), "")

      reqs = fs_requests_touching("", fn -> {:ok, _} = FS.list_dir_isolated(dir) end, dir)

      assert reqs == [], "list_dir_isolated hit file_server_2: #{inspect(reqs, limit: 5)}"
    end

    test "TempReaper.sweep and Tombstones.sweep_local list without file_server_2", %{tmp_dir: dir} do
      prev = Application.get_env(:fathom, :shard_data_dir)
      Application.put_env(:fathom, :shard_data_dir, dir)

      on_exit(fn ->
        if prev,
          do: Application.put_env(:fathom, :shard_data_dir, prev),
          else: Application.delete_env(:fathom, :shard_data_dir)
      end)

      for i <- 1..50, do: File.write!(Path.join(dir, "ls#{i}.db"), "")
      start_supervised!(Fathom.Shard.TempReaper)

      reqs =
        fs_requests_touching(
          "",
          fn ->
            Fathom.Shard.TempReaper.sweep()
            Fathom.Tenants.Tombstones.sweep_local()
          end,
          dir
        )
        |> Enum.filter(fn {:"$gen_call", _, req} ->
          elem(req, 0) in [:list_dir, :list_dir_all]
        end)

      assert reqs == [], "a sweep listed the data dir through file_server_2: #{inspect(reqs)}"
    end
  end

  describe "SqliteHeader reads raw" do
    @describetag :tmp_dir

    alias Fathom.Shard.SqliteHeader

    test "user_version/free_pages parse a real file and make no file_server_2 request",
         %{tmp_dir: dir} do
      path = Path.join(dir, "hdr.db")
      {:ok, conn} = Connection.open(path)
      Connection.exec(conn, "CREATE TABLE t (v TEXT)")
      Connection.exec(conn, "PRAGMA user_version = 7")
      Connection.close(conn)

      reqs =
        fs_requests_touching(
          "",
          fn ->
            assert {:ok, 7} = SqliteHeader.user_version(path)

            assert {:ok, %{page_size: ps, page_count: pc, freelist: 0}} =
                     SqliteHeader.free_pages(path)

            assert ps > 0 and pc > 0
          end,
          dir
        )

      assert reqs == [], "SqliteHeader hit file_server_2: #{inspect(reqs, limit: 5)}"
    end

    test "error shapes: missing file, empty file, non-sqlite file", %{tmp_dir: dir} do
      assert {:error, :enoent} = SqliteHeader.user_version(Path.join(dir, "nope"))
      assert {:error, :enoent} = SqliteHeader.free_pages(Path.join(dir, "nope"))
      empty = Path.join(dir, "empty")
      File.write!(empty, "")
      assert {:error, {:user_version_unreadable, _}} = SqliteHeader.user_version(empty)
      assert {:error, {:free_pages_unreadable, _}} = SqliteHeader.free_pages(empty)
      junk = Path.join(dir, "junk")
      File.write!(junk, String.duplicate("x", 200))
      assert {:error, {:user_version_unreadable, _}} = SqliteHeader.user_version(junk)
      assert {:error, {:free_pages_unreadable, _}} = SqliteHeader.free_pages(junk)
    end
  end

  # 120k-entry guard (30k open shards x 4 files; expert review 2026-10-22 r8 #7). Measured before
  # the fix: one listing grew file_server_2 2.9 KB -> 69 MB (still 69 MB 2 s later) and a concurrent
  # File.exists? stalled 107 ms; after: both flat. Order-of-magnitude ceilings, not exact figures.
  describe "120k-entry data dir" do
    @describetag :bench
    @describetag :tmp_dir
    @describetag timeout: 300_000

    test "reaper, boot sweep and erase_local keep file_server_2 small and responsive",
         %{tmp_dir: dir} do
      prev = Application.get_env(:fathom, :shard_data_dir)
      Application.put_env(:fathom, :shard_data_dir, dir)

      on_exit(fn ->
        if prev,
          do: Application.put_env(:fathom, :shard_data_dir, prev),
          else: Application.delete_env(:fathom, :shard_data_dir)

        :ets.delete(Fathom.Tenants.Tombstones, "benchvictim")
      end)

      for i <- 1..30_000, suffix <- [".db", ".db-wal", ".db-shm", ".db.etag"] do
        File.write!(Path.join(dir, "bench#{i}#{suffix}"), "")
      end

      File.write!(Path.join(dir, "benchvictim.db"), "x")
      File.write!(Path.join(dir, "benchvictim.db.fenced.1"), "x")
      :ets.insert(Fathom.Tenants.Tombstones, {"benchvictim"})

      fs = Process.whereis(:file_server_2)
      :erlang.garbage_collect(fs)
      {:memory, before} = Process.info(fs, :memory)

      parent = self()

      prober =
        spawn_link(fn ->
          probe = fn probe, max ->
            receive do
              :stop -> send(parent, {:probe_max, max})
            after
              0 ->
                t0 = System.monotonic_time(:microsecond)
                File.exists?(System.tmp_dir!())
                probe.(probe, max(max, System.monotonic_time(:microsecond) - t0))
            end
          end

          probe.(probe, 0)
        end)

      # the reaper's boot sweep (handle_continue) is part of the measured window
      start_supervised!(Fathom.Shard.TempReaper)
      Fathom.Shard.TempReaper.sweep()
      Fathom.Tenants.Tombstones.sweep_local()
      Fathom.Tenants.erase_local("benchvictim")

      send(prober, :stop)
      assert_receive {:probe_max, max_us}, 5_000
      {:memory, aft} = Process.info(fs, :memory)

      refute File.exists?(Path.join(dir, "benchvictim.db"))
      assert aft - before < 1_000_000, "file_server_2 grew #{div(aft - before, 1024)} KiB"
      assert max_us < 5_000, "a concurrent File.exists? stalled #{div(max_us, 1000)} ms"
    end
  end
end
