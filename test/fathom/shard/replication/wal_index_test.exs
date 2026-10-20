defmodule Fathom.Shard.Replication.WalIndexTest do
  @moduledoc """
  Expert review 2026-10-20 #1 / #17 / #23: the primary's commit extent comes from the `-shm`
  wal-index header (`mxFrame` + salts, SQLite's two-copy + checksum lock-free read), with the frame
  walk only as the fallback.

  ## #1, the safety bug these tests pin

  SQLite's `walWriteOneFrame` writes a frame's 24-byte header and its page in TWO separate writes.
  After an in-place WAL restart the file is already long (a stale tail of the previous
  generation), so a new commit frame's header can sit on disk over a stale slot while its page
  bytes are still the old generation's. The extent walk trusted salts + `nTruncate` from the header
  alone, so it counted that frame as committed and the payload pread shipped the stale page under
  it: a torn frame on the replica, whose checksum chain then breaks there — SQLite on the follower
  silently drops that frame and every later one while the primary counts the follower as holding
  them. `mxFrame` in the wal-index is published only AFTER every frame of the commit is written,
  so the index extent can never include a half-written frame.

  The fixture reproduces the writer's intermediate state exactly: a real SQLite WAL with a stale
  tail, plus one new-generation commit-frame HEADER written past the committed extent over a stale
  slot, page bytes untouched. Pre-fix both the cursor walk and the backward walk report it.

  ## #17, the cost

  Pre-fix, every header read in the post-restart cycle did one 24-byte pread PER NEW FRAME (the
  EOF fast path misses because the EOF frame is stale): panel 3 measured 6.0 / 9.2 / 21.6 preads
  per `read_held` at 1 / 4 / 16-row commits. The index read is one pread regardless.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Replication.Wal

  @hdr 32
  @fh 24

  setup do
    dir = Path.join(System.tmp_dir!(), "walindex_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  defp open_db!(dir) do
    path = Path.join(dir, "c.db")
    {:ok, db} = Exqlite.Sqlite3.open(path)
    on_exit(fn -> Exqlite.Sqlite3.close(db) end)
    exec!(db, "PRAGMA journal_mode=WAL")
    exec!(db, "PRAGMA wal_autocheckpoint=0")

    # Prod's setting (connection.ex): the WAL is NOT truncated at restart, so it keeps a stale tail.
    exec!(db, "PRAGMA journal_size_limit=67108864")
    exec!(db, "CREATE TABLE t (a INTEGER, b BLOB)")
    {db, path <> "-wal"}
  end

  defp exec!(db, sql), do: :ok = Exqlite.Sqlite3.execute(db, sql)

  defp commit_rows!(db, k) do
    exec!(db, "BEGIN")
    for i <- 1..k, do: exec!(db, "INSERT INTO t VALUES (#{i}, randomblob(3500))")
    exec!(db, "COMMIT")
  end

  # A WAL restarted in place over a long old generation: everything past the new generation's
  # extent is stale frames with the old salts.
  defp stale_tail!(db) do
    commit_rows!(db, 300)
    exec!(db, "PRAGMA wal_checkpoint(PASSIVE)")
    exec!(db, "INSERT INTO t VALUES (-1, NULL)")
  end

  defp page_size(wal) do
    {:ok, fd} = :file.open(wal, [:read, :raw, :binary])
    {:ok, <<_::64, page::32, _::binary>>} = :file.pread(fd, 0, 16)
    :file.close(fd)
    page
  end

  # Write ONLY a commit-frame header (this generation's salts, nTruncate != 0) at `at`, leaving the
  # stale page bytes under it — SQLite's state between walWriteOneFrame's two writes.
  defp write_torn_commit_header!(wal, at) do
    {:ok, fd} = :file.open(wal, [:read, :write, :raw, :binary])
    {:ok, <<salts::binary-8>>} = :file.pread(fd, 16, 8)
    {:ok, <<_pgno::32, _trunc::32, old_salts::binary-8, _::binary>>} = :file.pread(fd, at, @fh)
    assert old_salts != salts, "fixture: the slot past the extent is not a stale frame"
    :ok = :file.pwrite(fd, at, <<1::32, 9_999::32, salts::binary, 0::64>>)
    :file.close(fd)
  end

  describe "a half-written commit frame past the extent (expert review 2026-10-20 #1)" do
    test "is NOT counted by the held read (cursor walk pre-fix)", %{dir: dir} do
      {db, wal} = open_db!(dir)
      stale_tail!(db)

      {:ok, before, h} = Wal.read_held(nil, wal)

      assert before.size > before.commit_extent + 10 * (@fh + page_size(wal)),
             "fixture: no stale tail"

      write_torn_commit_header!(wal, before.commit_extent)

      assert {:ok, after_, h} = Wal.read_held(h, wal)

      assert after_.commit_extent == before.commit_extent,
             "the extent advanced over a frame whose header is written but whose page is not " <>
               "(#{before.commit_extent} -> #{after_.commit_extent}): the payload pread would ship " <>
               "the stale page under a new-generation commit header — a torn frame on the replica"

      Wal.close_held(h)
    end

    test "is NOT counted by a fresh held read (backward walk pre-fix)", %{dir: dir} do
      {db, wal} = open_db!(dir)
      stale_tail!(db)
      {:ok, before, h} = Wal.read_held(nil, wal)
      Wal.close_held(h)

      write_torn_commit_header!(wal, before.commit_extent)

      assert {:ok, fresh, h} = Wal.read_held(nil, wal)
      assert fresh.commit_extent == before.commit_extent
      Wal.close_held(h)
    end

    test "is NOT counted by the primary's one-shot read (seed / flush covers-check)", %{dir: dir} do
      {db, wal} = open_db!(dir)
      stale_tail!(db)
      {:ok, before} = Wal.read(wal, index: true)

      write_torn_commit_header!(wal, before.commit_extent)

      assert {:ok, again} = Wal.read(wal, index: true)
      assert again.commit_extent == before.commit_extent
    end

    test "the real commit that finishes the frame IS counted", %{dir: dir} do
      {db, wal} = open_db!(dir)
      stale_tail!(db)
      {:ok, before, h} = Wal.read_held(nil, wal)

      exec!(db, "INSERT INTO t VALUES (-2, NULL)")
      assert {:ok, after_, h} = Wal.read_held(h, wal)
      assert after_.commit_extent > before.commit_extent

      # And it agrees with the walk now that every byte is on disk.
      assert {:ok, ref} = Wal.read(wal)
      assert after_.commit_extent == ref.commit_extent
      Wal.close_held(h)
    end
  end

  describe "an unusable index with the -shm fd held (round-4 durability review 2026-10-20 F2)" do
    # Symptom: the walk fallback trusts a frame header, so with the index transiently unreadable
    # (-shm short during the first opener's rebuild, torn reads) it counted a torn commit header,
    # and the cursor's max() kept that over-count for the rest of the generation. The fixture
    # swaps the held -shm fd for an empty file, which `read_index` reports as :none.
    defp blind_shm!(dir, h) do
      empty = Path.join(dir, "empty-shm")
      File.write!(empty, <<>>)
      {:ok, fd} = :file.open(empty, [:read, :raw, :binary])
      {real, h} = {h.shm, %{h | shm: fd}}
      {real, h}
    end

    test "holds the cursor extent instead of walking onto a torn header", %{dir: dir} do
      {db, wal} = open_db!(dir)
      stale_tail!(db)
      {:ok, before, h} = Wal.read_held(nil, wal)
      write_torn_commit_header!(wal, before.commit_extent)
      {_real, h} = blind_shm!(dir, h)

      assert {:ok, again, h} = Wal.read_held(h, wal)

      assert again.commit_extent == before.commit_extent,
             "the walk fallback counted a torn commit header (#{before.commit_extent} -> " <>
               "#{again.commit_extent})"

      Wal.close_held(h)
    end

    test "after the index is usable again the extent is the index's, not a sticky over-count",
         %{dir: dir} do
      {db, wal} = open_db!(dir)
      stale_tail!(db)
      {:ok, before, h} = Wal.read_held(nil, wal)
      write_torn_commit_header!(wal, before.commit_extent)
      {real, h} = blind_shm!(dir, h)

      # Exhaust the hold budget so the walk runs and over-counts the torn header.
      h =
        Enum.reduce(1..10, h, fn _, h ->
          {:ok, r, h} = Wal.read_held(h, wal)
          send(self(), {:extent, r.commit_extent})
          h
        end)

      assert_received {:extent, last} when last > before.commit_extent

      {:ok, back, h} = Wal.read_held(%{h | shm: real}, wal)
      assert back.commit_extent == before.commit_extent
      Wal.close_held(h)
    end
  end

  describe "agreement with the walk on a real SQLite WAL" do
    test "through appends, a rollback, restarts and a commit over abandoned slots", %{dir: dir} do
      {db, wal} = open_db!(dir)

      check = fn h, label ->
        assert {:ok, held, h} = Wal.read_held(h, wal)
        assert {:ok, ref} = Wal.read(wal)
        assert {:ok, indexed} = Wal.read(wal, index: true)

        assert held.commit_extent == ref.commit_extent,
               "#{label}: held #{held.commit_extent} != walk #{ref.commit_extent}"

        assert indexed == ref,
               "#{label}: read(index: true) #{inspect(indexed)} != #{inspect(ref)}"

        assert {held.ckpt_seq, held.salt1} == {ref.ckpt_seq, ref.salt1}
        h
      end

      exec!(db, "INSERT INTO t VALUES (0, NULL)")
      h = check.(nil, "first read")
      commit_rows!(db, 40)
      h = check.(h, "append")

      exec!(db, "BEGIN")
      for i <- 1..30, do: exec!(db, "INSERT INTO t VALUES (#{i}, randomblob(3900))")
      exec!(db, "ROLLBACK")
      h = check.(h, "rollback (abandoned same-generation tail)")
      exec!(db, "INSERT INTO t VALUES (-1, NULL)")
      h = check.(h, "commit over abandoned slots")

      for round <- 1..3 do
        exec!(db, "PRAGMA wal_checkpoint(PASSIVE)")
        exec!(db, "INSERT INTO t VALUES (-2, NULL)")
        h = check.(h, "restart #{round}")
        commit_rows!(db, 4)
        _ = check.(h, "append after restart #{round}")
      end

      Wal.close_held(h)
    end
  end

  describe "raw file calls (expert review 2026-10-20 #17 / #23)" do
    # Panel 3 measured 6.0 / 9.2 / 21.6 preads per read_held in the post-restart cycle at
    # 1 / 4 / 16-row commits; one stat per read throughout.
    for k <- [1, 4, 16] do
      test "a read_held after a #{k}-row commit in a stale-tail cycle is one stat + one pread",
           %{dir: dir} do
        {db, wal} = open_db!(dir)
        stale_tail!(db)
        k = unquote(k)

        calls =
          traced(fn -> elem(Wal.read_held(nil, wal), 2) end, fn h ->
            Enum.reduce(1..10, h, fn _, h ->
              commit_rows!(db, k)
              {:ok, _hdr, h} = Wal.read_held(h, wal)
              h
            end)
          end)

        preads = Enum.count(calls, &(&1 == :pread))
        stats = Enum.count(calls, &(&1 == :read_file_info))
        assert stats == 10

        assert preads <= 10,
               "#{preads / 10} preads per read_held (pre-fix 6-22): #{inspect(calls)}"
      end
    end

    test "a warm replicated commit's read shape is 5 raw file calls (was 6)", %{dir: dir} do
      {db, wal} = open_db!(dir)
      exec!(db, "INSERT INTO t VALUES (0, NULL)")

      prep = fn ->
        {:ok, _, h} = Wal.read_held(nil, wal)
        exec!(db, "INSERT INTO t VALUES (1, NULL)")
        h
      end

      calls =
        traced(prep, fn h ->
          {:ok, hdr, h} = Wal.read_held(h, wal)
          {:ok, _bin, h} = Wal.read_delta_held(h, wal, @hdr, hdr.commit_extent - @hdr)
          {:ok, _after, h} = Wal.recheck_held(h, wal)
          h
        end)

      assert length(calls) == 5,
             "expected stat, index pread, payload pread, stat, header pread; saw #{inspect(calls)}"
    end

    # The read must happen in a process the test traces (a process cannot trace itself), and that
    # process must own the fds, so `prep` (untraced: opens the handle) and `run` (traced) both run
    # there.
    defp traced(prep, run) do
      test = self()

      reader =
        spawn_link(fn ->
          h = prep.()
          send(test, :ready)

          receive do
            :go -> send(test, {:done, Wal.close_held(run.(h))})
          end
        end)

      assert_receive :ready, 10_000

      for f <- [:pread, :read_file_info, :open],
          do: :erlang.trace_pattern({:file, f, :_}, true, [:local])

      :erlang.trace(reader, true, [:call, {:tracer, self()}])
      send(reader, :go)
      assert_receive {:done, nil}, 10_000
      ref = :erlang.trace_delivered(reader)
      assert_receive {:trace_delivered, _, ^ref}
      collect([])
    end

    defp collect(acc) do
      receive do
        {:trace, _, :call, {:file, f, _}} -> collect([f | acc])
      after
        0 -> Enum.reverse(acc)
      end
    end
  end

  describe "the index header parser and its fallbacks (synthetic files)" do
    # A WAL with two commit frames (page 512) and an index claiming only ONE. The walk says 2 frames;
    # a used index says 1. So `commit_extent` tells which source answered.
    @page 512
    @salt1 0x01020304
    @salt2 0x0A0B0C0D

    defp synth!(dir, index) do
      wal = Path.join(dir, "s.db-wal")

      frame = fn trunc ->
        <<1::32, trunc::32, @salt1::32, @salt2::32, 0::64>> <> :binary.copy(<<7>>, @page)
      end

      File.write!(
        wal,
        <<0x377F0682::32, 3_007_000::32, @page::32, 5::32, @salt1::32, @salt2::32, 0::64>> <>
          frame.(1) <> frame.(2)
      )

      if index, do: File.write!(Path.join(dir, "s.db-shm"), index)
      wal
    end

    defp index_hdr(opts \\ []) do
      body =
        <<Keyword.get(opts, :version, 3_007_000)::32-native, 0::32-native, 9::32-native,
          Keyword.get(opts, :init, 1)::8, 0::8, Keyword.get(opts, :szpage, @page)::16-native,
          Keyword.get(opts, :mx, 1)::32-native, 2::32-native, 0xDEAD::32-native,
          0xBEEF::32-native, Keyword.get(opts, :salt1, @salt1)::32-big, @salt2::32-big>>

      {c1, c2} = Keyword.get_lazy(opts, :cksum, fn -> cksum(body, 0, 0) end)
      body <> <<c1::32-native, c2::32-native>>
    end

    # SQLite's walChecksumBytes with nativeCksum = 1, written independently of the module's copy.
    defp cksum(<<a::32-native, b::32-native, rest::binary>>, s1, s2) do
      s1 = rem(s1 + a + s2, 0x100000000)
      cksum(rest, s1, rem(s2 + b + s1, 0x100000000))
    end

    defp cksum(<<>>, s1, s2), do: {s1, s2}

    defp extent(wal) do
      {:ok, held, h} = Wal.read_held(nil, wal)
      Wal.close_held(h)
      {:ok, plain} = Wal.read(wal, index: true)
      assert plain.commit_extent == held.commit_extent
      held.commit_extent
    end

    @one @hdr + (@fh + @page)
    @two @hdr + 2 * (@fh + @page)

    test "a valid index is used", %{dir: dir} do
      h = index_hdr()
      assert extent(synth!(dir, h <> h <> :binary.copy(<<0>>, 40))) == @one
    end

    test "64 KiB pages are encoded as szPage = 1", %{dir: dir} do
      # Only the decode is under test, so the WAL's own page size is irrelevant to the extent
      # arithmetic here: a mismatch must fall back, a match must not.
      h = index_hdr(szpage: 1)
      assert extent(synth!(dir, h <> h)) == @two, "szPage=1 decodes to 65536, not the WAL's 512"
    end

    for {label, opts_or_fun} <- [
          {"the two copies differ (a write in progress)", :dirty},
          {"the checksum does not match", [cksum: {1, 2}]},
          {"isInit is 0", [init: 0]},
          {"the version is not SQLite's", [version: 3_007_001]},
          {"the salts are another generation's", [salt1: @salt1 + 1]},
          {"the index is too short", :short},
          {"there is no index", :absent}
        ] do
      test "falls back to the walk when #{label}", %{dir: dir} do
        index =
          case unquote(Macro.escape(opts_or_fun)) do
            :dirty -> index_hdr() <> index_hdr(mx: 2)
            :short -> binary_part(index_hdr(), 0, 30)
            :absent -> nil
            opts -> index_hdr(opts) <> index_hdr(opts)
          end

        assert extent(synth!(dir, index)) == @two
      end
    end

    test "an index ahead of the stat is reported, and a garbage one cannot ship bytes", %{
      dir: dir
    } do
      # Legitimately: a commit published between `read_held/2`'s stat and its index read. Its
      # frames are on disk (published ⇒ written), so the extent is right and `size` is raised to
      # match. A garbage index that passed checksum + salts + page would claim frames that do not
      # exist; the payload pread of them is then a short read — the commit fails, nothing ships.
      h = index_hdr(mx: 50)
      wal = synth!(dir, h <> h)
      fifty = @hdr + 50 * (@fh + @page)
      assert {:ok, %{commit_extent: ^fifty, size: ^fifty}, held} = Wal.read_held(nil, wal)
      assert {:error, :short_read, held} = Wal.read_delta_held(held, wal, @hdr, fifty - @hdr)
      Wal.close_held(held)
    end
  end
end
