defmodule Fathom.Shard.Replication.FollowerEmptyWalRecoveryTest do
  @moduledoc """
  A follower replica whose `-wal` is EMPTY at restart must not answer a definite "nothing" while
  its `.db` holds acked writes (expert review 2026-10-15 #4 follow-up, found by WP-B while fixing
  #4).

  ## The bug

  `Follower.recover/3` rebuilt a replica only from its WAL header, and `Wal.read/1` answers
  `:empty` for an empty or absent `-wal` — so such a replica was not recovered AT ALL. Its
  position query answered `nil` ("I hold nothing"), which `Recovery.answered/3` counts toward the
  responder floor as definite, and which loses every ranking. But the `.db` can hold the acked
  tail: the follower's absorb (`absorb_before_reset/5`) checkpoints the outgoing generation into the
  `.db` and truncates the WAL, and a BEAM crash before the new generation's bytes land — or an OS
  crash that loses those unsynced bytes, the absorb itself being `synchronous=FULL` — leaves exactly
  `.db` + empty `-wal`. A failover then pulled a STALER peer (or opened the stored object) and the
  writes only this replica held were gone.

  ## The fix

  The absorb records the position the `.db` alone now holds (lineage, the absorbed WAL's ordinal,
  and the extent SQLite actually validated) in `<shard>.db.position`, invalidated before anything
  moves the `.db`. Recovery adopts it for an empty WAL; with no trustworthy record the replica
  recovers TORN (unrankable, silent to a query, re-seeded on its next push) — never "nothing". A
  follower that never held the shard has no `.db` and still answers a definite "nothing".

  The pull path carries the same position: a `.db`-only source offers its recorded offset (it
  offered the empty WAL's 0, which the puller installed below the object), and the puller keeps it.

  These drive the real absorb through a raw reset push, and simulate the crash by emptying the
  `-wal` while the follower is down.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Promote
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Recovery
  alias Fathom.Shard.Replication.Wal

  @lineage 3
  @ordinal 5

  setup do
    id = "emptywal_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "emptywal_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    prev =
      for k <- [:replication_lineage_wire, :replication_ordinal_wire],
          do: {k, Application.get_env(:fathom, k)}

    # The ranking travels only with both wires on (see replication_recovery_test).
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

  # A real SQLite source: `rows` committed rows, all still in the WAL (autocheckpoint off), so a
  # replica built from it holds them only through that WAL — or, after an absorb, in its `.db`.
  defp source!(root, rows) do
    src = Path.join(root, "src_#{System.unique_integer([:positive])}.db")
    {:ok, conn} = Connection.open(src)
    :ok = Sqlite3.execute(conn, "PRAGMA wal_autocheckpoint=0")
    :ok = Sqlite3.execute(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY)")
    for i <- 1..rows, do: :ok = Sqlite3.execute(conn, "INSERT INTO t VALUES (#{i})")
    {src, conn}
  end

  # Copy the source's `.db` + `-wal` into `name`'s directory and register them as a seed leaves
  # them, stated at `@ordinal`.
  defp plant!(name, id, src) do
    File.cp!(src, Follower.db_path(name, id))
    File.cp!(src <> "-wal", Follower.wal_path(name, id))
    {:ok, h} = Wal.read(Follower.wal_path(name, id))
    :ok = Follower.seed(name, id, 1, h.ckpt_seq, h.salt1, h.commit_extent, @lineage)
    :ok = Follower.note_ordinal(name, id, @ordinal)
    h
  end

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

  # The primary checkpointed: a reset push for the NEXT generation, same ownership, stating that
  # the outgoing generation reached `extent` — the follower held all of it, so it absorbs its WAL
  # into its `.db` (the real `absorb_before_reset/5`) before laying down the new header.
  defp reset_push!(port, id, h) do
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
  end

  defp position!(port, id) do
    {:ok, {:position, ^id, answer}} = send_frame!(port, Protocol.encode_position_query(id))
    answer
  end

  defp rows!(path) do
    {:ok, conn} = Connection.open(path)

    try do
      {:ok, %{rows: [[n]]}} = Connection.query(conn, "SELECT count(*) FROM t", [])
      n
    after
      Connection.close(conn)
    end
  end

  defp staged_rows!(root, name, id) do
    temp = Path.join(root, "staged_#{System.unique_integer([:positive])}.db")
    assert :ok = Promote.stage(name, id, temp)
    rows!(temp)
  end

  # The object was flushed a generation earlier: every replica here is ahead of it.
  defp stamp, do: %{epoch: @lineage, wal_ordinal: @ordinal - 1, offset: 0}

  test "an absorbed replica with an EMPTY -wal recovers the position its .db holds, and a " <>
         "failover pulls it over a staler peer",
       %{id: id, root: root} do
    f = :"emptywal_f_#{System.unique_integer([:positive])}"
    g = :"emptywal_g_#{System.unique_integer([:positive])}"
    s = :"emptywal_s_#{System.unique_integer([:positive])}"
    f_port = start!(root, f, 1)
    g_port = start!(root, g, 1)
    start!(root, s, 1)

    # G is a staler replica: it holds rows 1..2 of the generation. F holds all five.
    {src, conn} = source!(root, 2)
    plant!(g, id, src)
    for i <- 3..5, do: :ok = Sqlite3.execute(conn, "INSERT INTO t VALUES (#{i})")
    h = plant!(f, id, src)
    :ok = Connection.close(conn)

    # F absorbs its WAL (all five rows) into its `.db` on the reset...
    reset_push!(f_port, id, h)

    # ...and then loses the new generation's unsynced bytes: `.db` holds everything, `-wal` nothing.
    restart!(root, f, 1, 2)
    File.write!(Follower.wal_path(f, id), "")
    f_port = restart!(root, f, 2, 3)

    # The failover FIRST, so the unfixed code fails on the outcome, not only on F's state: S holds
    # nothing; F and G answer. The acked rows 3..5 are only on F. Unfixed, F answered "nothing" and
    # S pulled G — two rows.
    assert {:ok, installed} =
             Recovery.best_replica(id, stamp(),
               follower: s,
               peers: [{"f", "127.0.0.1", f_port}, {"g", "127.0.0.1", g_port}],
               quorum: 2
             )

    assert installed.next_offset == h.commit_extent,
           "the survivor pulled the staler replica (or under-stated F's): acked writes lost " <>
             "(#{inspect(installed)})"

    recovered = Follower.state_of(f, id)

    assert %{lineage: @lineage, wal_ordinal: @ordinal, torn: false} = recovered,
           "a replica whose acked writes are all in its .db was not recovered: it answers " <>
             "'nothing' to a failover (#{inspect(recovered)})"

    # The extent SQLite validated is the WAL's whole committed extent: nothing over- or under-claimed.
    assert recovered.next_offset == h.commit_extent

    # Over the wire it is a definite, rankable position — and so it counts toward the floor.
    answer = position!(f_port, id)
    assert %{wal_ordinal: @ordinal, next_offset: offset} = answer
    assert offset == h.commit_extent
    assert Recovery.answered([{{"f", ~c"127.0.0.1", 1}, {:answered, answer}}], nil, []) == 2

    # The pulled `.db`-only replica keeps its position across the SURVIVOR's restart too (before
    # staging, which fences the replica torn)...
    restart!(root, s, 1, 2)
    assert %{next_offset: off, wal_ordinal: @ordinal, torn: false} = Follower.state_of(s, id)
    assert off == h.commit_extent

    # ...and promotes to all five rows.
    assert staged_rows!(root, s, id) == 5
  end

  test "promotion serves a .db-only replica: all of its rows, from the .db alone",
       %{id: id, root: root} do
    f = :"emptywal_f_#{System.unique_integer([:positive])}"
    f_port = start!(root, f, 1)
    {src, conn} = source!(root, 5)
    h = plant!(f, id, src)
    :ok = Connection.close(conn)

    reset_push!(f_port, id, h)
    restart!(root, f, 1, 2)
    File.rm!(Follower.wal_path(f, id))
    restart!(root, f, 2, 3)

    # The local path: no peers, and the replica is ahead of the object on its own.
    assert {:ok, %{next_offset: off}} = Recovery.best_replica(id, stamp(), follower: f, peers: [])
    assert off == h.commit_extent
    assert staged_rows!(root, f, id) == 5
  end

  test "an empty -wal with NO position record recovers torn: silent, not counted, not ranked",
       %{id: id, root: root} do
    f = :"emptywal_f_#{System.unique_integer([:positive])}"
    start!(root, f, 1)
    {src, conn} = source!(root, 5)
    plant!(f, id, src)
    :ok = Connection.close(conn)
    # The `.db` plus an empty WAL, with nothing recording what the `.db` holds (a crash between
    # the absorb's checkpoint and its record, or a seed whose WAL was later lost).
    File.write!(Follower.wal_path(f, id), "")
    f_port = restart!(root, f, 1, 2)

    assert %{torn: true} = Follower.state_of(f, id),
           "an empty-WAL replica with unknown contents must be unrankable, not 'nothing'"

    assert position!(f_port, id) == :torn
    assert Recovery.answered([], Follower.state_of(f, id), []) == 0
    refute Promote.fresher?(Follower.state_of(f, id), stamp())
  end

  test "a follower that never held the shard still answers a definite 'nothing'",
       %{id: id, root: root} do
    f = :"emptywal_f_#{System.unique_integer([:positive])}"
    start!(root, f, 1)
    f_port = restart!(root, f, 1, 2)

    assert is_nil(Follower.state_of(f, id))
    assert position!(f_port, id) == nil
    # Definite: counted toward the floor, so fresh followers cause no refusal storm.
    assert Recovery.answered([{{"f", ~c"127.0.0.1", 1}, {:answered, nil}}], nil, []) == 2
  end

  test "a seed install voids the record before replacing the .db", %{id: id, root: root} do
    f = :"emptywal_f_#{System.unique_integer([:positive])}"
    f_port = start!(root, f, 1)
    {src, conn} = source!(root, 5)
    h = plant!(f, id, src)
    :ok = Connection.close(conn)
    reset_push!(f_port, id, h)
    assert {:ok, _} = Follower.recovered_db_position(f, id)

    # A seed whose WAL is empty (the primary had just checkpointed): its `.db` holds an unstated
    # position, so the absorb's record must not survive it.
    db = File.read!(src)

    seeds =
      Follower.begin_seed(f, %{}, %Protocol.SeedBegin{
        shard_id: id,
        epoch: 1,
        wal_gen: 0,
        salt1: 0,
        wal_offset: 0,
        db_size: byte_size(db),
        wal_size: 0,
        lineage: @lineage
      })

    seeds = Follower.write_chunk(seeds, id, :db, 0, db)
    assert {{:ok, 0}, _} = Follower.finish_seed(f, seeds, id)

    assert Follower.recovered_db_position(f, id) == :none
    restart!(root, f, 1, 2)
    assert %{torn: true} = Follower.state_of(f, id)
  end

  test "a record from another lineage is not adopted", %{id: id, root: root} do
    f = :"emptywal_f_#{System.unique_integer([:positive])}"
    f_port = start!(root, f, 1)
    {src, conn} = source!(root, 5)
    h = plant!(f, id, src)
    :ok = Connection.close(conn)
    reset_push!(f_port, id, h)
    restart!(root, f, 1, 2)
    File.write!(Follower.wal_path(f, id), "")
    File.write!(Follower.lineage_path(f, id), Integer.to_string(@lineage + 1))
    restart!(root, f, 2, 3)

    assert %{torn: true} = Follower.state_of(f, id)
  end

  test "with REPLICATION_FSYNC on the record is synced; off, it is not", %{id: id, root: root} do
    prev = Application.get_env(:fathom, :replication_fsync)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :replication_fsync),
        else: Application.put_env(:fathom, :replication_fsync, prev)
    end)

    me = self()
    handler = "emptywal-#{id}"

    :telemetry.attach(
      handler,
      [:fathom, :replication, :follower_state_sync],
      fn _e, _m, %{what: what}, _ -> send(me, {:synced, what}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    for {fsync, tag} <- [{false, :off}, {true, :on}] do
      Application.put_env(:fathom, :replication_fsync, fsync)
      sid = "#{id}_#{tag}"
      f = :"emptywal_f_#{System.unique_integer([:positive])}"
      f_port = start!(root, f, 1)
      {src, conn} = source!(root, 3)
      h = plant!(f, sid, src)
      :ok = Connection.close(conn)
      reset_push!(f_port, sid, h)
      assert {:ok, _} = Follower.recovered_db_position(f, sid)

      if fsync,
        do: assert_received({:synced, :db_position}),
        else: refute_received({:synced, :db_position})
    end
  end

  test "forget/2 removes the record", %{id: id, root: root} do
    f = :"emptywal_f_#{System.unique_integer([:positive])}"
    f_port = start!(root, f, 1)
    {src, conn} = source!(root, 5)
    h = plant!(f, id, src)
    :ok = Connection.close(conn)
    reset_push!(f_port, id, h)
    assert File.exists?(Follower.db_position_path(f, id))
    :ok = Follower.forget(f, id)
    refute File.exists?(Follower.db_position_path(f, id))
  end
end
