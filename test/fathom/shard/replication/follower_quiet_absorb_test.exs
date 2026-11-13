defmodule Fathom.Shard.Replication.FollowerQuietAbsorbTest do
  @moduledoc """
  Tuning audit 2026-11-11 #20. A follower absorbed a WAL generation only on the push that started
  the next one, so a shard whose primary dropped left its last generation on every follower as a
  `-wal` (1,449 MB of `-wal` against 5.9 MB of `.db` over 79 replicas on the chaos rig, unchanged
  minutes after every primary had dropped).

  `Follower.quiet_absorb_pass/1` absorbs a replica nothing has pushed to for the quiet window. The
  invariants these pin:

    * the `.db` ALONE holds what the WAL held (the discriminating assertion: before the pass the
      `.db` is the seed's near-empty file), the WAL is empty, and the row is REPLACED by what a
      restart over an empty WAL would build (epoch 0, salt 0), never kept: the old row names an
      append offset in a WAL that no longer exists;
    * ranking is unchanged: same lineage, same ordinal, same extent, still not torn;
    * a push after it is handled as after a restart: a reset is accepted without a re-seed, and an
      append at the old offset is refused (`:offset_mismatch` at 0) and writes NOTHING, after which
      the primary's full re-ship restores the replica to what it held;
    * it only ever touches a replica that is whole (not torn, lineage and ordinal stated), whose
      lock is free and whose WAL is genuinely quiet.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol
  alias Fathom.Shard.Replication.Wal

  @lineage 3
  @ordinal 5
  @quiet_ms 60_000

  setup do
    id = "quiet_#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), "quiet_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    keys = [
      :replication_lineage_wire,
      :replication_ordinal_wire,
      :replication_row_idle_ms,
      :replication_quiet_absorb_ms
    ]

    prev = for k <- keys, do: {k, Application.get_env(:fathom, k)}
    Application.put_env(:fathom, :replication_lineage_wire, true)
    Application.put_env(:fathom, :replication_ordinal_wire, true)
    Application.put_env(:fathom, :replication_row_idle_ms, 7_200_000)
    Application.put_env(:fathom, :replication_quiet_absorb_ms, @quiet_ms)

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
    end)

    name = :"quiet_f_#{System.unique_integer([:positive])}"
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
  # Returns the WAL header and the WAL's bytes (for the re-ship test).
  defp plant!(%{root: root, name: name, id: id}, lineage \\ @lineage) do
    {src, conn} = source!(root, 4)
    File.cp!(src, Follower.db_path(name, id))
    File.cp!(src <> "-wal", Follower.wal_path(name, id))
    :ok = Connection.close(conn)
    {:ok, h} = Wal.read(Follower.wal_path(name, id))
    :ok = Follower.seed(name, id, 1, h.ckpt_seq, h.salt1, h.commit_extent, lineage)
    :ok = Follower.note_ordinal(name, id, @ordinal)
    {h, File.read!(Follower.wal_path(name, id))}
  end

  # Make the replica look quiet for longer than the window: its touch row AND its WAL's mtime.
  defp age!(name, id) do
    :ets.insert(Follower.touches(name), {id, System.monotonic_time(:second) - 3_600})
    File.touch!(Follower.wal_path(name, id), System.os_time(:second) - 3_600)
  end

  # SQLite deletes a fully checkpointed WAL when its last connection closes, so "emptied" can also
  # read as "gone": both are a WAL that holds nothing.
  defp wal_size(name, id) do
    case File.stat(Follower.wal_path(name, id)) do
      {:ok, %{size: size}} -> size
      {:error, :enoent} -> 0
    end
  end

  defp db_rows(name, id) do
    {:ok, conn} = Sqlite3.open(Follower.db_path(name, id))

    try do
      {:ok, stmt} = Sqlite3.prepare(conn, "SELECT count(*) FROM t")
      {:ok, [[n]]} = Sqlite3.fetch_all(conn, stmt)
      n
    after
      Sqlite3.close(conn)
    end
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

  defp push(id, h, fields) do
    struct!(
      %Protocol.Push{
        shard_id: id,
        epoch: 1,
        wal_gen: h.ckpt_seq,
        salt1: h.salt1,
        offset: 0,
        payload: <<>>,
        prev_extent: h.commit_extent,
        wal_ordinal: @ordinal,
        lineage: @lineage
      },
      fields
    )
  end

  test "a quiet replica is absorbed: the .db alone holds the rows, the row is replaced",
       %{name: name, id: id} = ctx do
    {h, _wal} = plant!(ctx)
    before = Follower.state_of(name, id)
    assert %{torn: false, lineage: @lineage, wal_ordinal: @ordinal, epoch: 1} = before
    assert before.next_offset == h.commit_extent
    assert wal_size(name, id) > 0
    age!(name, id)

    assert Follower.quiet_absorb_pass(name) == 1

    assert wal_size(name, id) == 0, "the WAL was not emptied"
    assert db_rows(name, id) == 4, "the .db alone does not hold what the WAL held"

    after_state = Follower.state_of(name, id)

    # Ranking is what it was: same ownership, ordinal and extent, still whole.
    assert %{torn: false, lineage: @lineage, wal_ordinal: @ordinal} = after_state
    assert after_state.next_offset == before.next_offset

    # And the row can no longer be appended to: it is the empty-WAL row a restart builds.
    assert %{epoch: 0, wal_gen: 0, salt1: 0} = after_state

    # Idempotent: nothing left to absorb.
    assert Follower.quiet_absorb_pass(name) == 0
  end

  test "the position a follower reports over the wire is unchanged by the absorb", ctx do
    %{name: name, id: id, port: port} = ctx
    plant!(ctx)

    {:ok, {:position, ^id, wire_before}} =
      send_frame!(port, Protocol.encode_position_query(id))

    age!(name, id)
    assert Follower.quiet_absorb_pass(name) == 1

    {:ok, {:position, ^id, wire_after}} =
      send_frame!(port, Protocol.encode_position_query(id))

    assert wire_after.lineage == wire_before.lineage
    assert wire_after.wal_ordinal == wire_before.wal_ordinal
    assert wire_after.next_offset == wire_before.next_offset
  end

  test "a replica written inside the window is left alone", %{name: name, id: id} = ctx do
    plant!(ctx)
    before = wal_size(name, id)
    # The row's touch is old, but the WAL was written just now: the mtime decides.
    :ets.insert(Follower.touches(name), {id, System.monotonic_time(:second) - 3_600})

    assert Follower.quiet_absorb_pass(name) == 0
    assert wal_size(name, id) == before
    assert Follower.state_of(name, id).epoch == 1
  end

  test "a torn replica stays torn and keeps its WAL", %{name: name, id: id} = ctx do
    plant!(ctx)
    assert {:ok, _fence} = Follower.fence_for_promotion(name, id, [])
    before = wal_size(name, id)
    age!(name, id)

    assert Follower.quiet_absorb_pass(name) == 0
    assert wal_size(name, id) == before
    assert %{torn: true} = Follower.state_of(name, id)
  end

  test "a replica with no stated lineage is left alone", %{name: name, id: id} = ctx do
    plant!(ctx, 0)
    before = wal_size(name, id)
    age!(name, id)

    assert Follower.quiet_absorb_pass(name) == 0
    assert wal_size(name, id) == before
  end

  test "a replica whose lock is held is skipped this round", %{name: name, id: id} = ctx do
    plant!(ctx)
    before = wal_size(name, id)
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
    assert Follower.quiet_absorb_pass(name) == 0, "absorbed a replica another process holds"
    assert wal_size(name, id) == before
    send(holder, :release)

    assert Follower.quiet_absorb_pass(name) == 1
  end

  test "it is inert with the window at 0 and without the ordinal wire",
       %{name: name, id: id} = ctx do
    plant!(ctx)
    before = wal_size(name, id)
    age!(name, id)

    Application.put_env(:fathom, :replication_quiet_absorb_ms, 0)
    assert Follower.quiet_absorb_pass(name) == 0

    Application.put_env(:fathom, :replication_quiet_absorb_ms, @quiet_ms)
    Application.put_env(:fathom, :replication_ordinal_wire, false)
    assert Follower.quiet_absorb_pass(name) == 0
    assert wal_size(name, id) == before
  end

  test "the owner's next reset push is accepted without a re-seed", ctx do
    %{name: name, id: id, port: port} = ctx
    {h, _wal} = plant!(ctx)
    age!(name, id)
    assert Follower.quiet_absorb_pass(name) == 1

    # The owner reopened and started the next WAL generation, same ownership.
    salt = rem(h.salt1 + 1, 0x1_0000_0000)

    header =
      <<0x377F0682::32, 3_007_000::32, 4096::32, h.ckpt_seq + 1::32, salt::32, 0::32, 0::64>>

    reset =
      push(id, h,
        wal_gen: h.ckpt_seq + 1,
        salt1: salt,
        payload: header,
        wal_ordinal: @ordinal + 1
      )

    assert {:ok, {:ack, ^id, 32}} = send_frame!(port, Protocol.encode_push(reset))

    assert %{salt1: ^salt, lineage: @lineage, torn: false} = Follower.state_of(name, id)
  end

  test "a live primary's append at the old offset is refused and writes nothing; its re-ship restores the replica",
       ctx do
    %{name: name, id: id, port: port} = ctx
    {h, wal_bytes} = plant!(ctx)
    age!(name, id)
    assert Follower.quiet_absorb_pass(name) == 1

    # The primary was alive, only silent: it appends at the offset the follower held before.
    stale = push(id, h, offset: h.commit_extent, payload: :binary.copy(<<7>>, 64))

    assert {:ok, {:reject, ^id, :offset_mismatch, 0}} =
             send_frame!(port, Protocol.encode_push(stale))

    assert wal_size(name, id) == 0, "the refused append wrote into the emptied WAL"

    # It re-ships its WAL from offset 0, and the replica is what it was before the absorb.
    reship = push(id, h, payload: wal_bytes)
    assert {:ok, {:ack, ^id, size}} = send_frame!(port, Protocol.encode_push(reship))
    assert size == byte_size(wal_bytes)
    assert wal_size(name, id) == byte_size(wal_bytes)

    assert %{torn: false, lineage: @lineage, wal_ordinal: @ordinal, salt1: salt} =
             Follower.state_of(name, id)

    assert salt == h.salt1

    # SQLite recovers the full content from the re-shipped WAL on top of the absorbed `.db`.
    assert db_rows(name, id) == 4
  end

  test "the boot pass absorbs a replica that was quiet before any row existed", ctx do
    %{name: name, id: id} = ctx
    plant!(ctx)
    age!(name, id)
    # A restart: no row and no touch for it.
    :ets.delete(Follower.table(name), id)
    :ets.delete(Follower.touches(name), id)

    assert Follower.quiet_absorb_pass(name) == 0, "found a replica with no touch row"
    assert Follower.quiet_absorb_boot_pass(name) == 1
    assert wal_size(name, id) == 0
    assert db_rows(name, id) == 4
    assert %{torn: false, lineage: @lineage, wal_ordinal: @ordinal} = Follower.state_of(name, id)
  end

  test "the boot pass leaves a replica written recently", %{name: name, id: id} = ctx do
    plant!(ctx)
    before = wal_size(name, id)
    :ets.delete(Follower.table(name), id)
    :ets.delete(Follower.touches(name), id)

    assert Follower.quiet_absorb_boot_pass(name) == 0
    assert wal_size(name, id) == before
  end
end
