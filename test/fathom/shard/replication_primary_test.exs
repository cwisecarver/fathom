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
    # `commit_extent` defaults to `size` here because these cases predate the distinction and
    # are about plan/3's ORDERING, not about rollback: with no abandoned frames the two are
    # equal, which the "with no rollback" test above pins against a real WAL.
    defp header(seq, salt, size, extent \\ nil),
      do: %{ckpt_seq: seq, salt1: salt, size: size, commit_extent: extent || size}

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

  describe "Primary.plan/3 — the per-push byte cap" do
    # WHY THIS EXISTS: the 1024-tenant OOM is a positive feedback loop, not a leak. A push carries
    # the WAL since the follower's last ACK, so a delayed send makes the next payload bigger, which
    # delays it further. Measured on one shipper 40 s apart, the queued MESSAGE count fell
    # (8,265 -> 8,195) while the binary held DOUBLED (6,893 -> 15,798 MB) and the mean payload went
    # 832 KB -> 1,593 KB. Capping the count cannot touch that; capping the DELTA does.
    # See `docs/reviews/a2-shipper-feedback-loop-2026-08-16.md`.

    test "a delta under the cap is untouched" do
      assert {:append, 4152, 4120} = Primary.plan(state(1, 9, 4152), header(1, 9, 8272), 8192)
    end

    test "a delta exactly at the cap is untouched — the boundary is inclusive" do
      assert {:append, 4152, 4120} = Primary.plan(state(1, 9, 4152), header(1, 9, 8272), 4120)
    end

    test "a delta over the cap is truncated to the cap" do
      assert {:append, 4152, 1024} = Primary.plan(state(1, 9, 4152), header(1, 9, 8272), 1024)
    end

    test "a reset is capped too, and still starts at offset 0" do
      # The offset matters as much as the length: `FollowerLog.decide_fresh/2` accepts a generation
      # change ONLY at offset 0, so a capped reset that moved its start would be refused forever.
      assert {:reset, 0, 1024} = Primary.plan(nil, header(1, 9, 9000), 1024)
      assert {:reset, 0, 1024} = Primary.plan(state(1, 9, 4152), header(2, 77, 9000), 1024)
    end

    test "a capped plan resumes exactly where it stopped, and converges" do
      # THE property the catch-up loop rests on. A truncated ship must leave the follower at a
      # position the NEXT plan continues from — an off-by-one here would either skip bytes (silent
      # divergence) or resend them (a permanent `:offset_mismatch` loop).
      h = header(1, 9, 10_000)
      cap = 4096

      offsets =
        Stream.iterate(state(1, 9, 0), fn s ->
          case Primary.plan(s, h, cap) do
            :nothing -> s
            {_kind, _off, _len} = plan -> Primary.advance(h, plan)
          end
        end)
        |> Enum.take(5)
        |> Enum.map(& &1.offset)

      assert offsets == [0, 4096, 8192, 10_000, 10_000]
      assert :nothing = Primary.plan(state(1, 9, 10_000), h, cap)
    end

    test "no cap is spelled three ways and all mean the same thing" do
      s = state(1, 9, 4152)
      h = header(1, 9, 8272)
      full = {:append, 4152, 4120}

      assert ^full = Primary.plan(s, h, :infinity)
      # 0 is the documented off switch, matching `:replication_max_queue`.
      assert ^full = Primary.plan(s, h, 0)

      # plan/2 must stay exactly plan/3 with no cap, or every existing pure test above is testing a
      # different function from the one the commit path calls.
      assert Primary.plan(s, h) == Primary.plan(s, h, :infinity)
    end

    test "an empty WAL still ships nothing, cap or no cap" do
      assert :nothing = Primary.plan(nil, :empty, 1024)
      assert :nothing = Primary.plan(state(1, 9, 100), :empty, 1024)
    end
  end

  # THE COMMITTED EXTENT IS NOT THE FILE SIZE (expert review 2026-08-20 #5).
  #
  # SQLite does not shorten the WAL on ROLLBACK — it rewinds mxFrame in the wal-index and lets the
  # next transaction overwrite the abandoned slots in place. So the file length is a HIGH-WATER
  # MARK, and a primary keyed on it ships megabytes of abandoned frames once, records that as the
  # follower's position, and then plans :nothing for every subsequent commit while replying :ok to
  # the tenant. Acknowledged writes that no follower holds.
  #
  # This is an empirical claim about SQLite's on-disk behaviour, exactly like the ckpt_seq claim
  # this file already pins, so it is asserted against a real WAL rather than taken on faith.
  describe "the committed extent vs the file's high-water mark" do
    # Big enough to spill past the page cache, which is what makes the abandoned frames hit disk.
    defp rollback_a_big_transaction(conn) do
      :ok = Sqlite3.execute(conn, "BEGIN")

      for i <- 100..40_000 do
        :ok =
          Sqlite3.execute(
            conn,
            "INSERT INTO t VALUES (#{i}, '#{String.duplicate("x", 200)}')"
          )
      end

      :ok = Sqlite3.execute(conn, "ROLLBACK")
    end

    test "a rollback leaves the file grown but the extent where it was", ctx do
      %{path: path, wal: wal} = ctx
      {:ok, conn} = Connection.open(path)
      :ok = Sqlite3.execute(conn, "PRAGMA wal_autocheckpoint=0")
      :ok = Sqlite3.execute(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")
      :ok = Sqlite3.execute(conn, "INSERT INTO t VALUES (1, 'one')")

      assert {:ok, before} = Wal.read(wal)

      assert before.commit_extent == before.size,
             "with no rollback in flight the extent IS the file size; the fixture is not baseline"

      rollback_a_big_transaction(conn)
      assert {:ok, rolled} = Wal.read(wal)

      # The fixture must actually have spilled, or the rest measures nothing.
      assert rolled.size > before.size * 10,
             "the rolled-back transaction never spilled to disk (#{before.size} -> #{rolled.size})"

      assert rolled.commit_extent == before.commit_extent,
             "the extent moved for a transaction that was ROLLED BACK"

      # THE ONE THAT MATTERS. The next real commit does not change the file size, so a primary
      # keyed on size plans :nothing and acks a write no follower received.
      :ok = Sqlite3.execute(conn, "INSERT INTO t VALUES (2, 'two')")
      assert {:ok, after_commit} = Wal.read(wal)

      assert after_commit.size == rolled.size,
             "the file size moved, so this fixture no longer demonstrates the high-water mark"

      assert after_commit.commit_extent > rolled.commit_extent,
             "the committed extent did not advance across a real COMMIT — the tenant would be " <>
               "told the write is quorum-durable while zero bytes shipped"

      # And the plan the primary would actually make is a real append, not :nothing.
      state = %{wal_gen: rolled.ckpt_seq, salt1: rolled.salt1, offset: rolled.commit_extent}

      assert {:append, offset, len} = Primary.plan(state, after_commit, 0)
      assert offset == rolled.commit_extent
      assert len == after_commit.commit_extent - rolled.commit_extent

      :ok = Connection.close(conn)
    end

    test "with no rollback the extent tracks the file exactly", ctx do
      %{path: path, wal: wal} = ctx
      {:ok, conn} = Connection.open(path)
      :ok = Sqlite3.execute(conn, "PRAGMA wal_autocheckpoint=0")
      :ok = Sqlite3.execute(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT)")

      for i <- 1..50 do
        :ok = Sqlite3.execute(conn, "INSERT INTO t VALUES (#{i}, 'row')")
        assert {:ok, h} = Wal.read(wal)

        assert h.commit_extent == h.size,
               "the extent diverged from the file size with no rollback in play — every frame " <>
                 "here ends in a commit, so the last frame IS a commit frame"
      end

      :ok = Connection.close(conn)
    end
  end
end
