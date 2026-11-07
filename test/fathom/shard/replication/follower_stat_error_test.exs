defmodule Fathom.Shard.Replication.FollowerStatErrorTest do
  @moduledoc """
  The follower's `Fathom.FS.exists?/1` callers, audited after expert review 2026-11-07 #21 (batch D
  introduced `Fathom.FS.presence/1` and fixed the destructive callers in shard.ex / retained.ex /
  promote_on_open.ex / shards.ex). `exists?/1` reads EVERY stat error as "no file"; each case below
  is a place in `follower.ex` where that reading licensed a destructive or wrong-data answer:

    * `derive/3`'s `.db` stat: an unstat-able `.db` read as "no `.db`", so a replica that exists
      answered the DEFINITE "I hold nothing" that `Recovery.answered/3` counts toward its `N - Q + 1`
      floor (the #18 failure class, through the `.db` instead of the `-wal`);
    * `derive/3`'s torn-marker stat: an unstat-able marker read as "no marker", so a replica the
      reboot hold (or a failed promotion's fence) had marked torn was recovered UNTORN and rankable;
    * `absorb_before_reset/5`: an unstat-able `.db` (or `-wal`) skipped the absorb, CLEARED `torn`,
      and the reset then overwrote the `-wal` — a replica marked whole and promotable that had lost
      the generation it never absorbed;
    * `forget_if_present/2` (the bulk tombstone erase): an unstat-able replica read as absent and was
      left on disk behind a "nothing to erase" verdict.

  ## The fixture

  A SYMLINK LOOP at one name (`ELOOP` on stat) is a real, portable stat failure other than ENOENT
  that leaves every sibling file behaving normally (a directory the stat cannot search would also
  stop the code under test reading the rest, and pass quietly for the wrong reason). Each test
  asserts the loop really stats as an error and runs a control on the same code path without it.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.{Follower, Protocol, Wal}

  @lineage 3
  @ordinal 5

  setup do
    root = Path.join(System.tmp_dir!(), "follower_stat_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    keys = [:replication_lineage_wire, :replication_ordinal_wire]
    prev = for k <- keys, do: {k, Application.get_env(:fathom, k)}
    for k <- keys, do: Application.put_env(:fathom, k, true)

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
    end)

    %{root: root, id: "statfollow_#{System.unique_integer([:positive])}"}
  end

  defp start!(root, name, tag) do
    start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, to_string(name))},
      id: {name, tag}
    )
  end

  defp restart!(root, name, from, to) do
    stop_supervised!({name, from})
    start!(root, name, to)
  end

  # A real SQLite source with its rows still in the WAL, planted as a seed leaves it.
  defp plant!(root, name, id) do
    src = Path.join(root, "src_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(src)
    :ok = Sqlite3.execute(conn, "PRAGMA wal_autocheckpoint=0")
    :ok = Sqlite3.execute(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY)")
    for i <- 1..5, do: :ok = Sqlite3.execute(conn, "INSERT INTO t VALUES (#{i})")
    File.cp!(src, Follower.db_path(name, id))
    File.cp!(src <> "-wal", Follower.wal_path(name, id))
    Connection.close(conn)

    {:ok, h} = Wal.read(Follower.wal_path(name, id))
    :ok = Follower.seed(name, id, 1, h.ckpt_seq, h.salt1, h.commit_extent, @lineage)
    :ok = Follower.note_ordinal(name, id, @ordinal)
    h
  end

  # Makes `path` stat with ELOOP, and asserts it does (the fixture must not pass by accident).
  defp unstattable!(path) do
    File.rm_rf!(path)
    :ok = File.ln_s(path, path)
    assert {:error, :eloop} = Fathom.FS.presence(path), "fixture: the path must be unstat-able"
  end

  defp heal!(path), do: File.rm!(path)

  test "an unstat-able .db answers torn and uncached, not a definite nothing", ctx do
    %{id: id, root: root} = ctx
    name = :"statfollow_f_#{System.unique_integer([:positive])}"
    _ = start!(root, name, 1)
    plant!(root, name, id)
    db = Follower.db_path(name, id)
    good_db = File.read!(db)
    _ = restart!(root, name, 1, 2)

    # Control: the same restart over the intact replica derives it untorn.
    assert %{torn: false} = Follower.state_of(name, id)
    _ = restart!(root, name, 2, 3)

    unstattable!(db)

    assert %{torn: true, lineage: @lineage} = Follower.state_of(name, id),
           "a replica whose .db could not be stat'ed answered a definite 'I hold nothing'"

    assert :ets.lookup(Follower.table(name), id) == [], "a transient stat error poisoned the row"

    # The failure clears: read again from the files, whole and untorn.
    heal!(db)
    File.write!(db, good_db)
    assert %{torn: false} = Follower.state_of(name, id)
  end

  test "an unstat-able torn marker is torn, and uncached", ctx do
    %{id: id, root: root} = ctx
    name = :"statfollow_f_#{System.unique_integer([:positive])}"
    _ = start!(root, name, 1)
    plant!(root, name, id)
    _ = restart!(root, name, 1, 2)

    marker = Follower.torn_path(name, id)
    unstattable!(marker)

    assert %{torn: true} = Follower.state_of(name, id),
           "a replica whose torn marker could not be stat'ed was recovered untorn and rankable"

    assert :ets.lookup(Follower.table(name), id) == [], "a transient stat error poisoned the row"

    heal!(marker)
    assert %{torn: false} = Follower.state_of(name, id)
  end

  # The reset push a primary sends after a checkpoint: a later generation, offset 0, no statement
  # about the outgoing generation (`prev_extent: 0` reads as complete), so the verdict is `:complete`
  # and the replica is entitled to clear `torn` ONLY by absorbing its old WAL.
  defp reset_push(id, h) do
    %Protocol.Push{
      shard_id: id,
      epoch: 1,
      wal_gen: h.ckpt_seq + 1,
      salt1: h.salt1 + 1,
      offset: 0,
      lineage: @lineage,
      wal_ordinal: @ordinal + 1,
      payload: "next-generation-frames"
    }
  end

  defp reset!(ctx, push) do
    %{name: name, worker: worker, client: client} = ctx
    send(worker, {:frame, push, 10})
    assert_receive {:frame_done, 10}, 5_000
    {:ok, reply} = :gen_tcp.recv(client, 0, 5_000)
    assert {:ok, {:ack, _id, _next}} = Protocol.decode(reply)
    Follower.state_of(name, push.shard_id)
  end

  defp connect!(name) do
    {:ok, l} = :gen_tcp.listen(0, [:binary, packet: 4, active: false])
    {:ok, port} = :inet.port(l)
    {:ok, client} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false])
    {:ok, server} = :gen_tcp.accept(l)
    :gen_tcp.close(l)

    worker = Follower.spawn_worker(server, name, nil, self(), :atomics.new(1, []))
    Process.unlink(worker)
    on_exit(fn -> Process.exit(worker, :kill) end)
    %{name: name, worker: worker, client: client}
  end

  test "a reset over an unstat-able .db stays torn instead of clearing it unabsorbed", ctx do
    %{id: id, root: root} = ctx
    name = :"statfollow_f_#{System.unique_integer([:positive])}"
    _ = start!(root, name, 1)
    conn = connect!(name)

    # Control: an intact replica absorbs its old WAL and the reset makes it whole.
    control = id <> "_ok"
    h = plant!(root, name, control)
    assert %{torn: false} = reset!(conn, reset_push(control, h))

    # The same reset with the `.db` unreadable to stat: the absorb cannot run, so the replica must
    # not be declared whole while its old WAL is overwritten.
    h = plant!(root, name, id)
    unstattable!(Follower.db_path(name, id))

    assert %{torn: true} = reset!(conn, reset_push(id, h)),
           "the absorb was skipped on an indeterminate stat and torn was cleared"
  end

  test "the bulk tombstone erase does not skip a replica it could not stat", ctx do
    %{id: id, root: root} = ctx
    name = :"statfollow_f_#{System.unique_integer([:positive])}"
    _ = start!(root, name, 1)
    plant!(root, name, id)
    _ = restart!(root, name, 1, 2)

    # No row is resident after the restart; the `.db` cannot be stat'ed and there is no `-wal`.
    db = Follower.db_path(name, id)
    File.rm!(Follower.wal_path(name, id))
    unstattable!(db)
    assert :ets.lookup(Follower.table(name), id) == []

    assert :ok = Follower.forget_if_present(name, id)

    assert {:error, :enoent} = File.read_link(db),
           "an unstat-able replica of a deleted tenant read as absent and was left unerased"
  end
end
