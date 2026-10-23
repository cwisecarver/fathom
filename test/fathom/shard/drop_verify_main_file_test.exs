defmodule Fathom.Shard.DropVerifyMainFileTest do
  @moduledoc """
  Expert review 2026-10-22 r8 #15: the idle-drop `:checkpoint` route verifies an already-empty WAL
  shard with an immutable `quick_check` of the `.db`, instead of opening a full tenant-grade
  connection to TRUNCATE-checkpoint a WAL the last close already removed.

  What is pinned, in BOTH lease modes (the drop path's fence and release differ per mode):

    * empty WAL → `:main_file` check, NO `Connection.open` during the drop, the object is the
      `.db` with every row;
    * a WAL holding frames → the full `:checkpoint` check, and the frames reach the object;
    * the gate still catches what it caught: a corrupt `.db` with an empty WAL is refused and
      quarantined, and the good stored object is not clobbered.

  Not async: telemetry handlers, app env, real shard files and the global registry.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Fathom.Shard.{Connection, Heartbeat, Storage}
  alias Filo.Stmt

  @modes [:legacy, :heartbeat]

  setup do
    id = "dropverify_#{System.unique_integer([:positive])}"
    path = Shard.db_path(id)
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)

    test_pid = self()
    handler = "dropverify-#{id}"

    :telemetry.attach_many(
      handler,
      [[:fathom, :shard, :drop_flush, :verify], [:fathom, :shard, :drop_flush, :route]],
      fn
        [:fathom, :shard, :drop_flush, :verify], _, %{shard_id: ^id, mode: m}, _ ->
          send(test_pid, {:verify, m})

        [:fathom, :shard, :drop_flush, :route], _, %{shard_id: ^id, route: r}, _ ->
          send(test_pid, {:route, r})

        _, _, _, _ ->
          :ok
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)

      if prev_idle,
        do: Application.put_env(:fathom, :shard_idle_ms, prev_idle),
        else: Application.delete_env(:fathom, :shard_idle_ms)

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

  defp write!(id, sqls) do
    {:ok, conn} = ShardExecutor.open(id)
    Enum.each(sqls, fn s -> {:ok, _} = ShardExecutor.execute(conn, stmt(s)) end)
    ShardExecutor.close(conn)
  end

  @seed [
    "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 500) " <>
      "INSERT INTO t SELECT x, 'row_' || x || '_pad_pad_pad_pad_pad' FROM c"
  ]

  defp settle_wal_gone(path, tries \\ 400) do
    cond do
      not File.exists?(path <> "-wal") -> :ok
      tries == 0 -> flunk("the last close never removed the WAL")
      true -> wait_then(10, fn -> settle_wal_gone(path, tries - 1) end)
    end
  end

  defp wait_then(ms, fun) do
    receive do
    after
      ms -> fun.()
    end
  end

  # Count `Connection.open/1,2` calls (all processes: the drop flush runs in a Task) during `fun`.
  defp count_opens(fun) do
    me = self()
    :erlang.trace_pattern({Connection, :open, 1}, true, [])
    :erlang.trace_pattern({Connection, :open, 2}, true, [])
    :erlang.trace(:all, true, [:call, {:tracer, me}])

    try do
      fun.()
    after
      :erlang.trace(:all, false, [:call])
      :erlang.trace_pattern({Connection, :open, 1}, false, [])
      :erlang.trace_pattern({Connection, :open, 2}, false, [])
    end

    drain_opens(0)
  end

  defp drain_opens(n) do
    receive do
      {:trace, _pid, :call, {Connection, :open, _}} -> drain_opens(n + 1)
    after
      100 -> n
    end
  end

  defp object(id), do: Path.join(Storage.Local.dir(), id <> ".db")

  defp rows(file) do
    {:ok, c} = Connection.open(file)
    {:ok, %{rows: [[n]]}} = Connection.query(c, "SELECT count(*) FROM t", [])
    :ok = Connection.close(c)
    n
  end

  defp corrupt_page!(path) do
    assert File.stat!(path).size > 8192, "seeded db too small to corrupt a data page"
    {:ok, fd} = :file.open(path, [:read, :write, :binary, :raw])
    :ok = :file.pwrite(fd, 4096, :binary.copy(<<0xEF>>, 4096))
    :ok = :file.close(fd)
  end

  for mode <- @modes do
    test "[#{mode}] empty WAL: immutable main-file check, no connection open, full object", %{
      id: id,
      path: path
    } do
      set_mode!(unquote(mode))
      {:ok, coordinator} = Shards.ensure(id)
      assert_mode!(coordinator, unquote(mode))
      write!(id, @seed)
      settle_wal_gone(path)
      assert Shard.dirty?(coordinator), "fixture: the drop must have something to upload"

      opens = count_opens(fn -> assert :ok = Shards.stop(id) end)

      assert_received {:route, :checkpoint}
      assert_received {:verify, :main_file}
      assert opens == 0, "the drop opened #{opens} full connection(s) for an empty WAL"
      assert rows(object(id)) == 500
    end

    test "[#{mode}] a WAL holding frames keeps the full checkpoint check", %{id: id, path: path} do
      set_mode!(unquote(mode))
      {:ok, coordinator} = Shards.ensure(id)
      assert_mode!(coordinator, unquote(mode))

      # A trusted connection held across the tenant close keeps it from being the LAST close, so
      # SQLite's close-time checkpoint does not run and the WAL keeps its frames.
      write!(id, ["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"])
      {:ok, holder} = Connection.open(path)
      write!(id, tl(@seed))
      assert File.stat!(path <> "-wal").size > 0, "fixture: the WAL must hold frames"

      # The drop's checkpoint must not be blocked by the idle holder; close it after the drop.
      assert :ok = Shards.stop(id)
      :ok = Connection.close(holder)

      assert_received {:route, :checkpoint}
      assert_received {:verify, :checkpoint}
      assert rows(object(id)) == 500
    end

    test "[#{mode}] a corrupt main file with an empty WAL is still refused and quarantined", %{
      id: id,
      path: path
    } do
      set_mode!(unquote(mode))
      remote = object(id)

      write!(id, @seed)
      :ok = Shards.drain(id, 10_000)
      good = File.read!(remote)

      # Forget the first drop's events: the assertion below is about the corrupt drop.
      receive do
        {:verify, _} -> :ok
      after
        0 -> :ok
      end

      {:ok, coordinator} = Shards.ensure(id)
      assert_mode!(coordinator, unquote(mode))
      write!(id, ["INSERT INTO t (v) VALUES ('more')"])
      settle_wal_gone(path)
      corrupt_page!(path)

      assert {:error, _} = Shard.verify_integrity(path),
             "fixture: the page write did not actually corrupt anything"

      :ok = Shards.drain(id, 10_000)

      assert_received {:verify, :main_file}
      assert File.read!(remote) == good, "a corrupt .db was uploaded over the good object"
      assert Path.wildcard(path <> ".corrupt.*") != [], "the corrupt copy was not quarantined"
    end
  end
end
