defmodule Fathom.Shard.ReplicationWriteAfterFlushTest do
  @moduledoc """
  A commit that finds nothing to ship must not leave the Session holding a CLOSED WAL fd
  (round-4 rig rpo regression 2026-10-20).

  ## The symptom

  The rig's `rpo` arm (replication ON, quorum 2) wrote a base row, flushed it to S3, and the very
  next write was refused `FILO_NO_QUORUM`, every time, with the primary logging

      replication read failed for rpo1: :einval
      shard rpo1 write not replicated to a follower quorum: :einval

  `:einval` is what a raw `:file.pread/3` answers on a CLOSED fd.

  ## The bug

  `Wal.read_held/2` closes the held fd and returns `nil` when the WAL is absent or shorter than a
  header — a flush's `wal_checkpoint(TRUNCATE)` leaves exactly that, a 0-byte `-wal` on the SAME
  inode. `Session.ship/7` then plans `:nothing` and returned the bare atom, so `do_commit/4` (and
  `retry_after_seed/5`) replied with the PRE-ship state, whose `wal_fd` is the handle `read_held/2`
  had just closed. The next commit stats the WAL, sees the same path and inode, and `ensure_fd/3`
  reuses the closed fd: `pread` → `{:error, :einval}` → the commit fails its quorum. On Linux the
  same thing follows an unlink+recreate of `-wal` (SQLite's last-connection close), because ext4
  reuses the freed inode number immediately; on macOS APFS only the truncate shape reaches it,
  which is why an executor-level local run did not reproduce the rig.

  The test drives the truncate shape directly: commit, TRUNCATE checkpoint, a commit that ships
  nothing, then a real write — which must ship.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Fleet
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Session
  alias Fathom.Shards

  @env [
    :replication_enabled,
    :replication_followers,
    :replication_quorum,
    :replication_ordinal_wire
  ]

  setup do
    id = "repl_wraf_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "replwraf_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})

    on_exit(fn ->
      Session.stop(id)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
      for s <- ["", "-wal", "-shm"], do: File.rm(Fathom.Shard.db_path(id) <> s)
    end)

    # The ordinal wire is on in prod; run the rig's wire.
    Application.put_env(:fathom, :replication_ordinal_wire, true)
    %{id: id, root: root}
  end

  defp start_followers!(root, n) do
    for i <- 1..n do
      name = :"wraf_f#{i}_#{System.unique_integer([:positive])}"
      dir = Path.join(root, to_string(name))
      pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
      {:ok, port} = Follower.port(pid)
      {name, port}
    end
  end

  defp enable!(followers) do
    Application.put_env(:fathom, :replication_enabled, true)
    Application.put_env(:fathom, :replication_quorum, 2)

    Application.put_env(
      :fathom,
      :replication_followers,
      for({_n, port} <- followers, do: {~c"127.0.0.1", port})
    )

    start_supervised!(Fleet)
  end

  defp open!(path) do
    {:ok, conn} = Connection.open(path)
    on_exit(fn -> Connection.close(conn) end)
    conn
  end

  test "the first write after a TRUNCATE checkpoint and an empty commit still ships", %{
    id: id,
    root: root
  } do
    followers = start_followers!(root, 3)
    enable!(followers)

    {:ok, coordinator, ref, path} = Shards.checkout(id)
    on_exit(fn -> Fathom.Shard.checkin(coordinator, ref) end)
    wal = path <> "-wal"
    conn = open!(path)
    other = open!(path)

    {:ok, _} = Connection.query(conn, "CREATE TABLE kv (seq INTEGER)", [])
    {:ok, _} = Connection.query(other, "PRAGMA wal_checkpoint(TRUNCATE)", [])
    # Pre-seeded below the real ownership, so the first push is answered with a seed request and
    # the Session ships the `.db` itself (see replication_commit_test's preseed note).
    for {name, _} <- followers, do: Follower.seed(name, id, 0, 0, 0, 0)

    # The base write: shipped, so the Session now HOLDS an fd on this `-wal` inode.
    {:ok, _} = Connection.query(conn, "INSERT INTO kv VALUES (1)", [])
    assert :ok = Session.commit(id, wal, coordinator)
    {:ok, %{inode: inode}} = File.stat(wal)

    # The flush's fold: TRUNCATE leaves a 0-byte WAL on the SAME inode.
    {:ok, %{rows: [[0, _, _]]}} = Connection.query(other, "PRAGMA wal_checkpoint(TRUNCATE)", [])

    assert {:ok, %{size: 0, inode: ^inode}} = File.stat(wal),
           "precondition: the checkpoint did not leave a 0-byte WAL on the same inode"

    # A commit that finds nothing to ship (an empty WAL). `read_held/2` closes the held fd here.
    assert :ok = Session.commit(id, wal, coordinator)

    # The next real write grows the same inode again. It must ship, not die on a closed fd.
    {:ok, _} = Connection.query(conn, "INSERT INTO kv VALUES (999)", [])
    {:ok, %{inode: ^inode}} = File.stat(wal)

    assert :ok = Session.commit(id, wal, coordinator),
           "the first write after an empty commit was refused: the Session kept the fd that " <>
             "Wal.read_held/2 closed on the empty WAL, and pread on it answers :einval " <>
             "(round-4 rig rpo regression 2026-10-20)"
  end
end
