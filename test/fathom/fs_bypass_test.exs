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
end
