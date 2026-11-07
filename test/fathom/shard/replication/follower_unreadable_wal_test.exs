defmodule Fathom.Shard.Replication.FollowerUnreadableWalTest do
  @moduledoc """
  A replica whose files exist but cannot be READ must not answer a definite "I hold nothing"
  (expert review 2026-11-07 #18).

  `Follower.derive_state/3` returned `nil` whenever `Wal.read/1` errored, and the position-query
  handler maps `nil` to the `have = 0` frame, which `Recovery.answered/3` counts as a DEFINITE
  answer toward the `N - Q + 1` responder floor. So a replica holding the acked tail behind a
  damaged `-wal` header (the zero-filled header an OS crash can leave: a 32-byte-or-longer file that
  is not WAL magic, `{:error, :not_a_wal}`) let the recovering node reach the floor on "nothing"s,
  choose `:none` and open from the stale object. The empty-WAL branch of the same function was
  already fixed for this class (`recovered_state(:empty)` recovers torn, 2026-10-15 #4 follow-up);
  the unreadable-WAL branch was missed.

  Pinned:

    * a deterministic damage (`:not_a_wal`) recovers TORN (silent `:torn` to a query, unrankable,
      never counted), and the torn row is resident like the empty-WAL one;
    * a TRANSIENT read failure (here `-wal` is a directory: `{:error, :eisdir}`, the stand-in for
      EMFILE under fd pressure) answers torn WITHOUT inserting a row, so the replica is read from
      its files again once the failure clears instead of being poisoned until a re-seed;
    * a follower that never held the shard (no `.db`) still answers a definite "nothing", and so
      does one holding a stray `-wal` with no `.db` (nothing a recovery could use).
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Recovery
  alias Fathom.Shard.Replication.Wal

  @lineage 3
  @ordinal 5

  setup do
    id = "unreadwal_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "unreadwal_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    prev =
      for k <- [:replication_lineage_wire, :replication_ordinal_wire],
          do: {k, Application.get_env(:fathom, k)}

    Application.put_env(:fathom, :replication_lineage_wire, true)
    Application.put_env(:fathom, :replication_ordinal_wire, true)

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
    end)

    %{id: id, root: root}
  end

  defp start!(root, name, tag) do
    pid =
      start_supervised!({Follower, name: name, port: 0, dir: Path.join(root, to_string(name))},
        id: {name, tag}
      )

    {:ok, port} = Follower.port(pid)
    port
  end

  defp restart!(root, name, from, to) do
    stop_supervised!({name, from})
    start!(root, name, to)
  end

  # A real SQLite source with its rows still in the WAL, planted as a seed plus one push leaves it.
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

  defp position!(port, id) do
    {:ok, sock} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: 4, active: false], 5_000)

    try do
      :ok = :gen_tcp.send(sock, Protocol.encode_position_query(id))
      {:ok, bytes} = :gen_tcp.recv(sock, 0, 5_000)
      assert {:ok, {:position, ^id, answer}} = Protocol.decode(bytes)
      answer
    after
      :gen_tcp.close(sock)
    end
  end

  test "a replica whose -wal header is not WAL magic answers torn, not a definite nothing", ctx do
    %{id: id, root: root} = ctx
    name = :"unreadwal_f_#{System.unique_integer([:positive])}"
    port = start!(root, name, 1)

    h = plant!(root, name, id)
    assert position!(port, id) |> is_map(), "fixture: the healthy replica must offer a position"

    # The OS crash: the `-wal` survives at full size with a zero-filled header.
    wal = Follower.wal_path(name, id)
    File.write!(wal, :binary.copy(<<0>>, max(h.size, 64)))
    assert {:error, :not_a_wal} = Wal.read(wal), "fixture: the damage must read as not-a-WAL"

    port = restart!(root, name, 1, 2)

    assert position!(port, id) == :torn,
           "a replica that exists but cannot be read answered a definite answer"

    # What the querier does with it: silent, so it counts toward neither floor nor offers.
    assert %{torn: true, lineage: @lineage} = Follower.state_of(name, id)
    assert [{^id, %{torn: true}}] = :ets.lookup(Follower.table(name), id)
    assert Recovery.answered([], Follower.state_of(name, id), previous_owner: nil) == 0
  end

  test "a TRANSIENT read failure answers torn without caching a row", ctx do
    %{id: id, root: root} = ctx
    name = :"unreadwal_f_#{System.unique_integer([:positive])}"
    _ = start!(root, name, 1)

    h = plant!(root, name, id)
    wal = Follower.wal_path(name, id)
    good_wal = File.read!(wal)
    port = restart!(root, name, 1, 2)

    # `-wal` is a directory: `Wal.read/1` stats it (size >= 32), then cannot open it.
    File.rm!(wal)
    File.mkdir_p!(wal)
    assert {:error, reason} = Wal.read(wal)

    assert reason != :not_a_wal,
           "fixture: this must be the transient class, got #{inspect(reason)}"

    assert position!(port, id) == :torn
    assert :ets.lookup(Follower.table(name), id) == [], "a transient error poisoned the row"

    # The failure clears: the replica is read from its files again, whole and untorn.
    File.rm_rf!(wal)
    File.write!(wal, good_wal)

    assert %{torn: false, wal_gen: gen, next_offset: extent} = Follower.state_of(name, id)
    assert gen == h.ckpt_seq and extent == h.commit_extent
    assert %{} = position!(port, id)
  end

  test "a follower that never held the shard, or holds only a stray -wal, still answers nothing",
       ctx do
    %{id: id, root: root} = ctx
    name = :"unreadwal_f_#{System.unique_integer([:positive])}"
    port = start!(root, name, 1)

    assert position!(port, id) == nil

    # A `-wal` with no `.db`: nothing a recovery could build on, so not a replica that "exists".
    File.write!(Follower.wal_path(name, id), :binary.copy(<<0>>, 64))
    port = restart!(root, name, 1, 2)
    assert position!(port, id) == nil
  end
end
