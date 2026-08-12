defmodule Fathom.Shard.ReplicationPrimaryTest do
  @moduledoc """
  The send-side delta computation — Phase 2 A2. See `docs/a2-quorum-replication.md`.

  Two halves. `Primary.plan/2` is pure and tested as such. `Wal.read/1` is tested against a **real
  SQLite WAL written by `Fathom.Shard.Connection`**, because the entire design rests on an empirical
  claim about SQLite's on-disk format:

  > the WAL header's checkpoint sequence number changes when the WAL is reset

  If that were false, the primary could not distinguish "frames appended" from "the WAL restarted",
  and would splice two unrelated generations together — silently. So it is asserted here rather
  than taken from documentation.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Replication.Primary
  alias Fathom.Shard.Replication.Wal

  setup do
    path = Path.join(System.tmp_dir!(), "replprim_#{System.unique_integer([:positive])}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
    %{path: path, wal: path <> "-wal"}
  end

  describe "Wal.read/1 against a real SQLite WAL" do
    test "an absent or header-less WAL reads as :empty, not as an error", %{wal: wal} do
      assert {:ok, :empty} = Wal.read(wal)
      File.write!(wal, "tiny")
      assert {:ok, :empty} = Wal.read(wal)
    end

    test "a file that is not a WAL is refused rather than shipped", %{wal: wal} do
      File.write!(wal, :binary.copy(<<0>>, 64))
      assert {:error, :not_a_wal} = Wal.read(wal)
    end

    test "THE premise: a checkpoint changes the sequence number", ctx do
      %{path: path, wal: wal} = ctx
      {:ok, conn} = Connection.open(path)
      on_exit(fn -> Connection.close(conn) end)

      {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])

      assert {:ok, before} = Wal.read(wal)
      assert before.size > 0

      # RESTART (not PASSIVE) guarantees the WAL is rewound even with this connection open, which
      # is what makes the assertion below deterministic rather than timing-dependent.
      :ok = Sqlite3.execute(conn, "PRAGMA wal_checkpoint(RESTART)")
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (2)", [])

      assert {:ok, after_ckpt} = Wal.read(wal)

      assert after_ckpt.ckpt_seq != before.ckpt_seq,
             "the WAL checkpoint sequence did NOT change across a reset (#{before.ckpt_seq} -> " <>
               "#{after_ckpt.ckpt_seq}). A2's generation detection rests on this; without it a " <>
               "primary cannot tell an append from a restart and will splice two WALs together."
    end

    test "appending frames grows the size without changing the generation", ctx do
      %{path: path, wal: wal} = ctx
      {:ok, conn} = Connection.open(path)
      on_exit(fn -> Connection.close(conn) end)

      {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])
      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (1)", [])
      assert {:ok, first} = Wal.read(wal)

      {:ok, _} = Connection.query(conn, "INSERT INTO t VALUES (2)", [])
      assert {:ok, second} = Wal.read(wal)

      assert second.ckpt_seq == first.ckpt_seq and second.salt1 == first.salt1
      assert second.size > first.size
    end

    test "read_delta returns exactly the requested range", ctx do
      %{path: path, wal: wal} = ctx
      {:ok, conn} = Connection.open(path)
      on_exit(fn -> Connection.close(conn) end)

      {:ok, _} = Connection.query(conn, "CREATE TABLE t (a)", [])
      {:ok, h} = Wal.read(wal)

      assert {:ok, bin} = Wal.read_delta(wal, 0, h.size)
      assert byte_size(bin) == h.size
      assert bin == File.read!(wal)

      assert {:ok, <<>>} = Wal.read_delta(wal, 0, 0)
      assert {:error, :short_read} = Wal.read_delta(wal, 0, h.size + 1_000)
    end
  end

  describe "Primary.plan/2" do
    defp header(seq, salt, size), do: %{ckpt_seq: seq, salt1: salt, size: size}
    defp state(gen, salt, offset), do: %{wal_gen: gen, salt1: salt, offset: offset}

    test "an empty WAL ships nothing" do
      assert :nothing = Primary.plan(nil, :empty)
      assert :nothing = Primary.plan(state(1, 9, 100), :empty)
    end

    test "a shard never shipped before sends its whole WAL, header first" do
      assert {:reset, 0, 4152} = Primary.plan(nil, header(1, 9, 4152))
    end

    test "an unchanged size ships nothing — a no-op commit costs no round trip" do
      assert :nothing = Primary.plan(state(1, 9, 4152), header(1, 9, 4152))
    end

    test "appended frames ship as the tail range" do
      assert {:append, 4152, 4120} = Primary.plan(state(1, 9, 4152), header(1, 9, 8272))
    end

    test "a new checkpoint sequence resets, even when the size grew" do
      # THE case file-size alone gets wrong. After a reset SQLite reuses the file, so the size can
      # be larger than what we last shipped while the bytes at those offsets are from a different
      # generation entirely.
      assert {:reset, 0, 9000} = Primary.plan(state(1, 9, 4152), header(2, 77, 9000))
    end

    test "a moved salt resets even if the sequence number looks unchanged" do
      assert {:reset, 0, 5000} = Primary.plan(state(1, 9, 4152), header(1, 1234, 5000))
    end

    test "a WAL that shrank without a generation change re-ships everything" do
      # SQLite should not do this. Since the assumption has already proven false, computing a range
      # from it is exactly the wrong move.
      assert {:reset, 0, 100} = Primary.plan(state(1, 9, 4152), header(1, 9, 100))
    end

    test "advance/2 records the generation actually shipped" do
      h = header(3, 55, 9000)
      assert %{wal_gen: 3, salt1: 55, offset: 9000} = Primary.advance(h, {:reset, 0, 9000})
      assert %{wal_gen: 3, salt1: 55, offset: 8272} = Primary.advance(h, {:append, 4152, 4120})
    end
  end
end
