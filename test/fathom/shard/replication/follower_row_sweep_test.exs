defmodule Fathom.Shard.Replication.FollowerRowSweepTest do
  @moduledoc """
  Expert review 2026-10-20-2027 #11 (the time suffix separates it from the earlier 2026-10-20
  series): the follower kept a ~300 B ETS row for every shard it ever replicated, forgotten only on
  a promotion or a tombstone — ~200 MB per node at a million fleet shards, 2 GB at ten million.

  The fix sweeps a row untouched for `:replication_row_idle_ms` and re-derives it from the replica's
  files on the next use (`state_of/2`), the way boot recovery does. The invariant these pin: a swept
  replica is INDISTINGUISHABLE from a resident one to every reader — its position over the wire, its
  `torn` flag, its ranking fields — and the next push from its owner is handled as it is after a
  follower restart (one reset round trip, no re-seed). A row the files cannot reproduce is kept.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Wal

  @lineage 3
  @ordinal 5

  setup do
    id = "rowsweep_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "rowsweep_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    keys = [:replication_lineage_wire, :replication_ordinal_wire, :replication_row_idle_ms]
    prev = for k <- keys, do: {k, Application.get_env(:fathom, k)}
    Application.put_env(:fathom, :replication_lineage_wire, true)
    Application.put_env(:fathom, :replication_ordinal_wire, true)
    Application.put_env(:fathom, :replication_row_idle_ms, 60_000)

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
    end)

    name = :"rowsweep_f_#{System.unique_integer([:positive])}"
    pid = start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, "f")})
    {:ok, port} = Follower.port(pid)
    %{id: id, root: root, name: name, port: port}
  end

  # A real SQLite source with `rows` committed rows still in its WAL (kept open: the last close
  # would checkpoint and delete the WAL).
  defp source!(root, rows) do
    src = Path.join(root, "src_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(src)
    :ok = Sqlite3.execute(conn, "PRAGMA wal_autocheckpoint=0")
    :ok = Sqlite3.execute(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY)")
    for i <- 1..rows, do: :ok = Sqlite3.execute(conn, "INSERT INTO t VALUES (#{i})")
    {src, conn}
  end

  # The replica a seed leaves: the source's `.db` + `-wal`, registered at `lineage`/`@ordinal`.
  defp plant!(%{root: root, name: name, id: id}, lineage \\ @lineage) do
    {src, conn} = source!(root, 4)
    File.cp!(src, Follower.db_path(name, id))
    File.cp!(src <> "-wal", Follower.wal_path(name, id))
    :ok = Connection.close(conn)
    {:ok, h} = Wal.read(Follower.wal_path(name, id))
    :ok = Follower.seed(name, id, 1, h.ckpt_seq, h.salt1, h.commit_extent, lineage)
    :ok = Follower.note_ordinal(name, id, @ordinal)
    h
  end

  # Make the row look idle for longer than the sweep window.
  defp age!(name, id),
    do: :ets.insert(Follower.touches(name), {id, System.monotonic_time(:second) - 3_600})

  defp resident?(name, id), do: :ets.lookup(Follower.table(name), id) != []

  defp send_frame!(port, frame) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false], 5_000)

    try do
      :ok = :gen_tcp.send(sock, frame)
      {:ok, bytes} = :gen_tcp.recv(sock, 0, 5_000)
      Protocol.decode(bytes)
    after
      :gen_tcp.close(sock)
    end
  end

  defp position!(port, id) do
    {:ok, {:position, ^id, answer}} = send_frame!(port, Protocol.encode_position_query(id))
    answer
  end

  test "an idle row is swept and re-derived unchanged (but for the epoch) on the next read",
       ctx do
    %{name: name, id: id, port: port} = ctx
    h = plant!(ctx)
    before = Follower.state_of(name, id)
    wire_before = position!(port, id)
    age!(name, id)

    assert Follower.sweep_idle_rows(name) == 1
    refute resident?(name, id), "the idle row was not swept"
    assert :ets.lookup(Follower.touches(name), id) == []

    # Over the wire FIRST (a failover's position query is the reader that must not see "nothing").
    assert position!(port, id) == wire_before
    assert resident?(name, id), "the re-derived row was not re-installed"

    # Every field but the epoch, which is on no disk and comes back 0, exactly as after a restart.
    assert Follower.state_of(name, id) == %{before | epoch: 0}
    assert %{next_offset: off, torn: false, lineage: @lineage, wal_ordinal: @ordinal} = before
    assert off == h.commit_extent
  end

  test "a torn row sweeps into a torn row (the durable marker, never laundered)", ctx do
    %{name: name, id: id} = ctx
    plant!(ctx)
    assert :ok = Follower.fence_for_promotion(name, id, [])
    assert %{torn: true} = Follower.state_of(name, id)
    age!(name, id)

    assert Follower.sweep_idle_rows(name) == 1
    refute resident?(name, id)
    assert %{torn: true} = Follower.state_of(name, id)
    assert Follower.offerable(name, id) == nil
  end

  test "a row the files do not reproduce is kept", ctx do
    %{name: name, id: id} = ctx
    h = plant!(ctx)

    # The row describes a different WAL than the one on disk (as mid-reset): re-deriving would
    # change what it says, so it stays.
    :ok =
      Follower.seed(name, id, 1, h.ckpt_seq, h.salt1 + 1, h.commit_extent, @lineage)

    age!(name, id)
    assert Follower.sweep_idle_rows(name) == 0
    assert resident?(name, id)
    assert %{salt1: salt} = Follower.state_of(name, id)
    assert salt == h.salt1 + 1
  end

  test "an unstated-lineage row is kept: a re-derived epoch 0 would read as a new owner", ctx do
    %{name: name, id: id} = ctx
    plant!(ctx, 0)
    age!(name, id)
    assert Follower.sweep_idle_rows(name) == 0
    assert resident?(name, id)
  end

  test "a recently written row, or one whose lock is held, is not swept", ctx do
    %{name: name, id: id} = ctx
    plant!(ctx)
    assert Follower.sweep_idle_rows(name) == 0
    assert resident?(name, id)

    age!(name, id)
    test = self()

    holder =
      spawn(fn ->
        Follower.with_shard_lock(name, id, 1_000, fn ->
          send(test, :locked)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :locked
    assert Follower.sweep_idle_rows(name) == 0, "swept a row another process holds the lock of"
    assert resident?(name, id)
    send(holder, :release)
  end

  test "a swept replica takes its owner's next reset push without a re-seed", ctx do
    %{name: name, id: id, port: port} = ctx
    h = plant!(ctx)
    age!(name, id)
    assert Follower.sweep_idle_rows(name) == 1

    # The owner checkpointed while the row was swept: a reset for the next generation, same
    # ownership. The re-derived row absorbs the outgoing WAL and appends, as after a restart.
    salt = rem(h.salt1 + 1, 0x1_0000_0000)

    header =
      <<0x377F0682::32, 3_007_000::32, 4096::32, h.ckpt_seq + 1::32, salt::32, 0::32, 0::64>>

    push = %Protocol.Push{
      shard_id: id,
      epoch: 1,
      wal_gen: h.ckpt_seq + 1,
      salt1: salt,
      offset: 0,
      payload: header,
      prev_extent: h.commit_extent,
      wal_ordinal: @ordinal + 1,
      lineage: @lineage
    }

    assert {:ok, {:ack, ^id, 32}} = send_frame!(port, Protocol.encode_push(push))

    assert %{salt1: ^salt, lineage: @lineage, wal_ordinal: ordinal, torn: false} =
             Follower.state_of(name, id)

    assert ordinal == @ordinal + 1
  end
end
