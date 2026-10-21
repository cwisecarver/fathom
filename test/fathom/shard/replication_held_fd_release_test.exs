defmodule Fathom.Shard.ReplicationHeldFdReleaseTest do
  @moduledoc """
  A replication `Session` must not hold fds on a shard's UNLINKED `-wal` / `-shm` after the shard's
  last connection closed (expert review 2026-10-20-2027 #2).

  ## The bug

  `Session` keeps a held, inode-revalidated read fd on `<db>-wal` plus one on `<db>-shm`
  (`Wal.read_held/2`). When the shard goes idle its last SQLite connection closes — the pool drain
  grace here — and SQLite checkpoints and unlinks both files. The Session lives as long as the
  coordinator (60 s idle stop in prod) and closed the fds only in `terminate/2`, so for that whole
  minute the node held 2 fds per recently-written shard on deleted inodes: invisible to `du`,
  pinning the WAL's disk, and at ~1k distinct shards/s past a 65,536 nofile limit (`emfile`).

  ## The invariant pinned

  After the last connection closes and the release interval passes with no commit, the BEAM holds
  NO fd on a deleted `-wal`/`-shm` of this shard — and the next write still replicates.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard
  alias Fathom.Shard.Replication.{Fleet, Follower, Session}
  alias Fathom.{ShardExecutor, Shards}
  alias Filo.Stmt

  @keys [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :connection_pool,
    :connection_pool_opts,
    :shard_idle_ms,
    :replication_fd_release_ms,
    :replication_catchup_ms
  ]

  setup do
    id = "repl_fd_release_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replfd_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Map.new(@keys, &{&1, Application.get_env(:fathom, &1)})

    Application.put_env(:fathom, :connection_pool, true)
    Application.put_env(:fathom, :connection_pool_opts, drain_grace_ms: 50)
    # The coordinator (and so the Session) must SURVIVE the idle gap: that is the window the fds
    # leaked through.
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    Application.put_env(:fathom, :replication_fd_release_ms, 100)

    # A catch-up round that happens to run after the unlink stats `:enoent` and releases the fds as
    # a side effect, which made this test pass pre-fix ~1 run in 3. Keep catch-up out of the window
    # so the only thing that can release them is the mechanism under test.
    Application.put_env(:fathom, :replication_catchup_ms, 60_000)

    on_exit(fn ->
      _ = Shards.stop(id)
      Session.stop(id)

      Enum.each(prev, fn
        {k, nil} -> Application.delete_env(:fathom, k)
        {k, v} -> Application.put_env(:fathom, k, v)
      end)

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Shard.db_path(id) <> s)
    end)

    %{id: id, root: root}
  end

  defp enable!(root) do
    followers =
      for i <- 1..3 do
        name = :"fdr_f#{i}_#{System.unique_integer([:positive])}"
        dir = Path.join(root, to_string(name))
        pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
        {:ok, port} = Follower.port(pid)
        {~c"127.0.0.1", port}
      end

    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)
    Application.put_env(:fathom, :replication_followers, followers)
    start_supervised!(Fleet)
  end

  defp exec!(h, sql), do: {:ok, _} = ShardExecutor.execute(h, %Stmt{sql: sql, args: []})
  defp pid_of({pid, _ref, _conn, _id, _sc, _v, _o}), do: pid

  defp eventually(fun, tries \\ 300) do
    cond do
      fun.() -> true
      tries == 0 -> false
      true -> Process.sleep(10) && eventually(fun, tries - 1)
    end
  end

  # fds this OS process holds on DELETED files whose path contains `id`. Linux: /proc's link text
  # carries " (deleted)". Elsewhere: `lsof +L1` (link count < 1, i.e. unlinked).
  defp deleted_fds(id) do
    proc = "/proc/self/fd"

    if File.dir?(proc) do
      for fd <- File.ls!(proc),
          {:ok, target} <- [:file.read_link(Path.join(proc, fd))],
          target = to_string(target),
          String.contains?(target, id) and String.ends_with?(target, "(deleted)"),
          do: target
    else
      lsof = System.find_executable("lsof") || flunk("no /proc and no lsof to count fds with")
      {out, _} = System.cmd(lsof, ["-w", "-n", "-P", "-a", "-p", System.pid(), "+L1"])
      out |> String.split("\n") |> Enum.filter(&String.contains?(&1, id))
    end
  end

  test "the Session releases its WAL and -shm fds once the shard's last connection has closed", %{
    id: id,
    root: root
  } do
    enable!(root)

    {:ok, h} = ShardExecutor.open(id)
    coordinator = pid_of(h)
    exec!(h, "CREATE TABLE t (a INTEGER, b BLOB)")
    for i <- 1..5, do: exec!(h, "INSERT INTO t VALUES (#{i}, randomblob(4000))")
    :ok = ShardExecutor.close(h)

    # The drain grace's close ran and SQLite unlinked the WAL (and -shm with it).
    assert eventually(fn ->
             st = :sys.get_state(coordinator)

             Shard.HandlePool.count(st.pool) == 0 and
               (st.closing == nil or not Process.alive?(st.closing)) and
               not File.exists?(Shard.db_path(id) <> "-wal")
           end),
           "precondition: the idle drain never closed the last connection and unlinked the WAL"

    assert Process.alive?(coordinator),
           "precondition: the coordinator (and Session) survive the gap"

    # Pre-fix: 2 (the deleted -wal and -shm), held until the coordinator's 60 s idle stop.
    assert eventually(fn -> deleted_fds(id) == [] end, 100),
           "fds still held on deleted inodes: #{inspect(deleted_fds(id))}"

    # The release is a starting state, not a broken one: the next write replicates.
    {:ok, h2} = ShardExecutor.open(id)
    assert pid_of(h2) == coordinator
    exec!(h2, "INSERT INTO t VALUES (99, randomblob(10))")
    :ok = ShardExecutor.close(h2)
    # The write above was quorum-acked (Q = 2 of 3), and nobody is left behind.
    assert eventually(fn -> match?(%{behind_followers: 0}, Session.lag_info(id)) end)
  end
end
