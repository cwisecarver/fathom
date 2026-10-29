defmodule Fathom.Shard.Replication.FollowerRebootTornTest do
  @moduledoc """
  After an OS reboot, a follower holds every replica TORN until it is re-seeded (Jepsen 2026-10-28,
  the first power-loss run, shard 15).

  ## The defect

  Followers ack from RAM (`:replication_fsync` off). A power loss drops frames the follower already
  acked, and the files left behind still describe a coherent, rankable, OLDER position. Recovery
  counted that position as a definite answer toward its `N - Q + 1` floor, whose argument is that
  some answer comes from a follower still holding the acked tail. fathom3 acked a write, lost power,
  came back at an older position, took the shard over, counted its own answer, and opened from the
  stored object while the other acker (paused) still held the write.

  ## What is pinned

    * a changed boot id (a reboot): every replica is torn, so it is unrankable, not offered and not
      counted, and the recorded boot is updated;
    * the same boot id (a process restart, which keeps the page cache): replicas stay trusted, so
      a deploy or a SIGKILL costs nothing;
    * no recorded boot (a first start): trusted, and the boot is recorded;
    * no readable boot id: the check is off;
    * a re-seed after the reboot clears the torn state, and it stays cleared on the next restart.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Follower

  @lineage 7

  setup do
    root = Path.join(System.tmp_dir!(), "reboottorn_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    prev = Application.get_env(:fathom, :replication_boot_id_path)
    boot_file = Path.join(root, "boot_id")
    Application.put_env(:fathom, :replication_boot_id_path, boot_file)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :replication_boot_id_path),
        else: Application.put_env(:fathom, :replication_boot_id_path, prev)

      File.rm_rf(root)
    end)

    src = Path.join(root, "src.db")
    {:ok, conn} = Connection.open(src)
    :ok = Sqlite3.execute(conn, "PRAGMA wal_autocheckpoint=0")
    :ok = Sqlite3.execute(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY)")
    for i <- 1..5, do: :ok = Sqlite3.execute(conn, "INSERT INTO t VALUES (#{i})")
    db = File.read!(src)
    wal = File.read!(src <> "-wal")
    Connection.close(conn)

    %{dir: Path.join(root, "f"), boot_file: boot_file, db: db, wal: wal}
  end

  # Replica files as a previous incarnation left them.
  defp plant!(ctx, id) do
    File.mkdir_p!(ctx.dir)
    File.write!(Path.join(ctx.dir, id <> ".db"), ctx.db)
    File.write!(Path.join(ctx.dir, id <> ".db-wal"), ctx.wal)
    File.write!(Path.join(ctx.dir, id <> ".db.lineage"), Integer.to_string(@lineage))
    id
  end

  defp recorded!(ctx, boot) do
    File.mkdir_p!(ctx.dir)
    File.write!(Path.join(ctx.dir, ".boot_id"), boot)
  end

  defp booted!(ctx, boot), do: File.write!(ctx.boot_file, boot <> "\n")

  defp start!(ctx) do
    name = :"reboottorn_f_#{System.unique_integer([:positive])}"
    pid = start_supervised!({Follower, name: name, port: 0, dir: ctx.dir}, id: name)
    {name, pid}
  end

  # Starts it with the reboot warning captured; returns {started, log}.
  defp start_logged!(ctx) do
    me = self()
    log = capture_log(fn -> send(me, {:started, start!(ctx)}) end)
    assert_received {:started, started}
    {started, log}
  end

  defp ids, do: for(i <- 1..3, do: "rbt_#{i}_#{System.unique_integer([:positive])}")

  test "a reboot holds every replica torn: not ranked, not offered, not counted", ctx do
    ids = Enum.map(ids(), &plant!(ctx, &1))
    recorded!(ctx, "boot-A")
    booted!(ctx, "boot-B")

    {{name, _pid}, log} = start_logged!(ctx)
    assert log =~ "the OS rebooted since"

    for id <- ids do
      assert %{torn: true, lineage: @lineage} = Follower.state_of(name, id),
             "a replica that may have lost acked frames to the reboot was trusted"

      assert Follower.offerable(name, id) == nil
      assert File.exists?(Follower.torn_path(name, id)), "the torn hold is not durable"
    end

    assert File.read!(Path.join(ctx.dir, ".boot_id")) == "boot-B"
  end

  test "a process restart under the same boot keeps every replica trusted", ctx do
    ids = Enum.map(ids(), &plant!(ctx, &1))
    recorded!(ctx, "boot-A")
    booted!(ctx, "boot-A")
    {name, _pid} = start!(ctx)

    for id <- ids, do: assert(%{torn: false} = Follower.state_of(name, id))
  end

  test "a first start with no recorded boot trusts its replicas and records the boot", ctx do
    id = plant!(ctx, "rbt_first_#{System.unique_integer([:positive])}")
    booted!(ctx, "boot-A")
    {name, _pid} = start!(ctx)

    assert %{torn: false} = Follower.state_of(name, id)
    assert File.read!(Path.join(ctx.dir, ".boot_id")) == "boot-A"
  end

  test "with no readable boot id the check is off", ctx do
    id = plant!(ctx, "rbt_noboot_#{System.unique_integer([:positive])}")
    recorded!(ctx, "boot-A")
    {name, _pid} = start!(ctx)

    assert %{torn: false} = Follower.state_of(name, id)
    assert File.read!(Path.join(ctx.dir, ".boot_id")) == "boot-A"
  end

  test "a re-seed after the reboot clears the hold, and a later restart keeps it cleared", ctx do
    id = plant!(ctx, "rbt_reseed_#{System.unique_integer([:positive])}")
    recorded!(ctx, "boot-A")
    booted!(ctx, "boot-B")
    {{name, _pid}, _log} = start_logged!(ctx)
    assert %{torn: true} = Follower.state_of(name, id)

    :ok = Follower.seed(name, id, 1, 0, 0, byte_size(ctx.wal), 0)
    assert %{torn: false} = Follower.state_of(name, id)
    refute File.exists?(Follower.torn_path(name, id))

    stop_supervised!(name)
    {name2, _pid} = start!(ctx)
    assert %{torn: false} = Follower.state_of(name2, id)
  end
end
