defmodule Fathom.Shard.Replication.WalHeldFdTest do
  @moduledoc """
  Expert review 2026-08-26 #32 — the held WAL read fd, and the inode check that makes it safe.

  `Wal.read/1` and `read_delta/3` each did their own `open`+`pread`+`close`, and one
  `Session.commit/3` calls them three times, so a replicated commit paid THREE open/close pairs and
  TWO stats on the tenant's synchronous write path, all on dirty-IO schedulers.

  ## The measurement, because the finding says it is a hypothesis

  Its own falsifying experiment: run `replication_cost_test` before and after, and **"if the delta
  is under ~10 µs, the opens were not the cost and the change should be reverted rather than kept
  for tidiness."**

  Measured back to back on the same machine, two samples each:

      baseline        324 µs, 324 µs   added by replication
      held fd         250 µs, 246 µs

  ~76 µs, about 23% of what replication costs a write. A direct microbench of the two file-op
  shapes predicted ~104 µs; the rest of the path (the GenServer hop, encoding, the socket) makes up
  the difference.

  ## Why the inode check is the point, not an implementation detail

  A cached fd survives an unlink+recreate. SQLite deletes and recreates `-wal`, so an fd held
  across that keeps returning the OLD generation's bytes — and `stable?/2` would then compare
  old-against-old and PASS, which is exactly the cross-generation splice `Wal`'s moduledoc exists
  to prevent. Every read revalidates by inode first.

  It is also strictly stronger than what shipped before: nothing detected an unlink+recreate
  between `ship/5`'s header read and the re-check, except the salt happening to differ — which is
  not guaranteed. These tests pin that stronger property, not merely the absence of a regression.
  """
  use ExUnit.Case, async: true

  alias Fathom.Shard.Replication.Wal

  setup do
    path = Path.join(System.tmp_dir!(), "walfd_#{System.unique_integer([:positive])}.wal")
    on_exit(fn -> File.rm(path) end)
    %{path: path}
  end

  # A real WAL header — random bytes would make every read `:not_a_wal` and the tests would pass
  # while proving nothing.
  defp wal_bytes(ckpt_seq, salt1, payload) do
    <<0x377F0682::32, 3_007_000::32, 4096::32, ckpt_seq::32, salt1::32, 0::32, 0::64>> <> payload
  end

  test "the fd is REUSED across reads while the file is the same", %{path: path} do
    File.write!(path, wal_bytes(7, 111, :binary.copy(<<1>>, 512)))

    assert {:ok, %{ckpt_seq: 7}, h1} = Wal.read_held(nil, path)
    assert {:ok, %{ckpt_seq: 7}, h2} = Wal.read_held(h1, path)

    assert h2.fd == h1.fd,
           "the fd was reopened even though the file did not change — the whole saving is not " <>
             "reopening it (~76 us/commit, measured)"

    assert {:ok, _bin, h3} = Wal.read_delta_held(h2, path, 32, 64)
    assert h3.fd == h1.fd, "read_delta_held reopened rather than reusing the held fd"

    Wal.close_held(h3)
  end

  test "an unlink+recreate REOPENS — it never serves the old generation", %{path: path} do
    # THE safety property. Without the inode check the second read returns generation 7 from the
    # deleted inode, `stable?/2` compares old-against-old and passes, and the session ships
    # new-generation bytes stamped with the old generation's identity.
    File.write!(path, wal_bytes(7, 111, :binary.copy(<<1>>, 512)))
    assert {:ok, %{ckpt_seq: 7, salt1: 111}, h1} = Wal.read_held(nil, path)

    # Unlink, then recreate at the same path — what SQLite does when it restarts the log.
    File.rm!(path)
    File.write!(path, wal_bytes(0, 222, :binary.copy(<<2>>, 512)))

    assert File.stat!(path).inode != h1.inode,
           "fixture: the recreate reused the inode, so this proves nothing — the OS recycled it"

    assert {:ok, header, h2} = Wal.read_held(h1, path)

    assert header.ckpt_seq == 0 and header.salt1 == 222,
           "a held fd served the OLD generation after an unlink+recreate (got " <>
             "ckpt_seq=#{header.ckpt_seq}, salt1=#{header.salt1}). stable?/2 would compare " <>
             "old-against-old and pass, and the session would ship new-generation bytes under " <>
             "the old identity — the exact splice this module exists to prevent."

    refute h2.fd == h1.fd, "the header changed but the fd did not — it cannot have been reopened"

    Wal.close_held(h2)
  end

  test "a delta read after a recreate is caught by the trailing header re-read", %{path: path} do
    # Expert review 2026-10-17 #10a. `read_delta_held/4` no longer stats: the handle's inode was
    # pinned by the `read_held/2` just before it, so in the recreate window it reads the OLD
    # generation's bytes through the held fd (consistent with the header the caller holds). What
    # keeps that from shipping is the commit's trailing `read_held/2`, which stats, sees the new
    # inode and reports the NEW generation's header — `Session.stable?/3` then refuses the round.
    # This used to assert the delta read itself reopened ("delta reads revalidate too"); the
    # revalidation moved to the one place that was always the authority.
    File.write!(path, wal_bytes(7, 111, :binary.copy(<<0xAA>>, 512)))
    assert {:ok, before, h1} = Wal.read_held(nil, path)

    File.rm!(path)
    File.write!(path, wal_bytes(0, 222, :binary.copy(<<0xBB>>, 512)))

    assert {:ok, bin, h2} = Wal.read_delta_held(h1, path, 32, 16)
    assert bin == :binary.copy(<<0xAA>>, 16), "the delta read was expected to stay on the held fd"

    assert {:ok, after_, h3} = Wal.read_held(h2, path)
    refute h3.fd == h1.fd, "the trailing re-read did not reopen the recreated file"

    assert {before.salt1, before.ckpt_seq} != {after_.salt1, after_.ckpt_seq},
           "the re-read cannot tell the generations apart, so `stable?/3` would pass a splice"

    Wal.close_held(h3)
  end

  test "a shrunk file is still a short read without a stat", %{path: path} do
    File.write!(path, wal_bytes(7, 111, :binary.copy(<<1>>, 512)))
    assert {:ok, _h, h1} = Wal.read_held(nil, path)

    File.write!(path, wal_bytes(7, 111, <<1, 2, 3>>))

    assert {:error, :short_read, h2} = Wal.read_delta_held(h1, path, 32, 512)
    Wal.close_held(h2)
  end

  test "the warm delta read issues no stat (expert review 2026-10-17 #10a)", %{path: path} do
    File.write!(path, wal_bytes(7, 111, :binary.copy(<<1>>, 512)))
    test = self()

    # The fd must be owned by the process that reads through it, and a process cannot trace itself
    # into its own mailbox, so the read happens in a helper the test traces.
    reader =
      spawn_link(fn ->
        {:ok, _header, h1} = Wal.read_held(nil, path)
        send(test, :ready)

        receive do
          :go -> send(test, {:read, Wal.read_delta_held(h1, path, 32, 16)})
        end

        receive do
          :done -> :ok
        end
      end)

    assert_receive :ready
    :erlang.trace_pattern({:file, :read_file_info, 2}, true, [:local])
    :erlang.trace(reader, true, [:call, {:tracer, self()}])
    send(reader, :go)
    assert_receive {:read, {:ok, _bin, _h}}
    :erlang.trace(reader, false, [:call])
    ref = :erlang.trace_delivered(reader)
    assert_receive {:trace_delivered, _, ^ref}
    send(reader, :done)

    refute_received {:trace, _, :call, {:file, :read_file_info, _}},
                    "the warm delta read stat'd the WAL again"
  end

  describe "recheck_held/2 (expert review 2026-10-19 #17)" do
    test "reports the same identity as read_held/2, reusing the fd", %{path: path} do
      File.write!(path, wal_bytes(7, 111, :binary.copy(<<1>>, 512)))
      assert {:ok, _header, h1} = Wal.read_held(nil, path)
      assert {:ok, %{ckpt_seq: 7, salt1: 111}, h2} = Wal.recheck_held(h1, path)
      assert h2.fd == h1.fd
      Wal.close_held(h2)
    end

    test "an unlink+recreate reopens and reports the NEW file, never the deleted inode",
         %{path: path} do
      # The inode comparison is why the recheck still stats the path. Skipping it would read the
      # old generation off the held fd and let `stable?/3` compare old against old.
      File.write!(path, wal_bytes(7, 111, :binary.copy(<<1>>, 512)))
      assert {:ok, _header, h1} = Wal.read_held(nil, path)

      File.rm!(path)
      File.write!(path, wal_bytes(0, 222, :binary.copy(<<2>>, 512)))
      assert File.stat!(path).inode != h1.inode, "fixture: the OS recycled the inode"

      assert {:ok, %{ckpt_seq: 0, salt1: 222}, h2} = Wal.recheck_held(h1, path)
      refute h2.fd == h1.fd
      Wal.close_held(h2)
    end

    test "absent, too-short and non-WAL files", %{path: path} do
      File.write!(path, wal_bytes(1, 2, <<>>))
      assert {:ok, _header, h} = Wal.read_held(nil, path)
      File.rm!(path)
      assert {:ok, :empty, nil} = Wal.recheck_held(h, path)

      File.write!(path, <<0, 1, 2>>)
      assert {:ok, :empty, nil} = Wal.recheck_held(nil, path)

      File.write!(path, :binary.copy(<<0>>, 64))
      assert {:error, :not_a_wal, nil} = Wal.recheck_held(nil, path)
    end

    # A replicated commit's reads on the primary: header read, payload read, trailing recheck. Was
    # 7 dirty-IO file jobs (stat, header pread, EOF-frame pread, payload pread, stat, header pread,
    # EOF-frame pread); the recheck no longer repeats the commit-extent pread. This pins the count
    # for the warm (fd already held, EOF frame is a commit) case.
    test "a warm commit's read shape issues 6 raw file calls, not 7", %{path: path} do
      # One real commit frame (nTruncate != 0, this generation's salts) so the EOF-frame pread runs.
      frame = <<1::32, 1::32, 111::32, 0::32, 0::64>> <> :binary.copy(<<1>>, 4096)
      File.write!(path, wal_bytes(7, 111, frame))
      test = self()

      reader =
        spawn_link(fn ->
          {:ok, _header, h1} = Wal.read_held(nil, path)
          send(test, :ready)

          receive do
            :go ->
              {:ok, _hdr, h} = Wal.read_held(h1, path)
              {:ok, _bin, h} = Wal.read_delta_held(h, path, 32, 64)
              {:ok, _after, h} = Wal.recheck_held(h, path)
              send(test, {:done, h})
          end

          receive do
            :exit -> :ok
          end
        end)

      assert_receive :ready
      :erlang.trace_pattern({:file, :pread, 3}, true, [:local])
      :erlang.trace_pattern({:file, :read_file_info, 2}, true, [:local])
      :erlang.trace_pattern({:file, :open, 2}, true, [:local])
      :erlang.trace(reader, true, [:call, {:tracer, self()}])
      send(reader, :go)
      assert_receive {:done, _h}
      :erlang.trace(reader, false, [:call])
      ref = :erlang.trace_delivered(reader)
      assert_receive {:trace_delivered, _, ^ref}
      send(reader, :exit)

      calls = collect_calls([])
      assert Enum.count(calls, &(&1 == :open)) == 0, "a warm commit reopened the WAL"
      assert length(calls) == 6, "expected 6 raw file calls per commit, saw #{inspect(calls)}"
    end

    defp collect_calls(acc) do
      receive do
        {:trace, _, :call, {:file, f, _}} -> collect_calls([f | acc])
      after
        0 -> Enum.reverse(acc)
      end
    end
  end

  test "read shapes still match the un-held versions", %{path: path} do
    # The held path must not become a second, subtly different implementation.
    File.write!(path, wal_bytes(3, 55, :binary.copy(<<9>>, 1024)))

    assert {:ok, plain} = Wal.read(path)
    assert {:ok, held, h} = Wal.read_held(nil, path)
    assert plain == held, "read_held/2 disagrees with read/1 on the same file"

    assert {:ok, plain_delta} = Wal.read_delta(path, 32, 100)
    assert {:ok, held_delta, h} = Wal.read_delta_held(h, path, 32, 100)
    assert plain_delta == held_delta

    Wal.close_held(h)
  end

  test "an absent or too-short file is :empty and releases the fd", %{path: path} do
    File.write!(path, wal_bytes(1, 2, <<>>))
    assert {:ok, _header, h} = Wal.read_held(nil, path)

    File.rm!(path)

    assert {:ok, :empty, nil} = Wal.read_held(h, path),
           "an absent WAL must release the held fd, not keep pointing at the deleted inode"

    File.write!(path, <<0, 1, 2>>)
    assert {:ok, :empty, nil} = Wal.read_held(nil, path)
  end

  test "close_held/1 is idempotent and always returns nil" do
    assert Wal.close_held(nil) == nil
  end

  describe "the held extent cursor (expert review 2026-10-01 perf #8)" do
    # These drive a REAL SQLite WAL through the states the cursor has to get right, and compare
    # the held read against `Wal.read/1`, which still walks backward from EOF on a fresh fd and is
    # the reference. A cursor that drifts from it would ship the wrong byte range.
    setup do
      dir = Path.join(System.tmp_dir!(), "walcursor_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      path = Path.join(dir, "c.db")
      {:ok, db} = Exqlite.Sqlite3.open(path)
      on_exit(fn -> Exqlite.Sqlite3.close(db) end)
      :ok = Exqlite.Sqlite3.execute(db, "PRAGMA journal_mode=WAL")
      :ok = Exqlite.Sqlite3.execute(db, "PRAGMA wal_autocheckpoint=0")
      :ok = Exqlite.Sqlite3.execute(db, "CREATE TABLE t (a INTEGER, b BLOB)")
      %{db: db, wal: path <> "-wal"}
    end

    defp exec!(db, sql), do: :ok = Exqlite.Sqlite3.execute(db, sql)

    defp grow!(db, rows) do
      exec!(db, "BEGIN")
      for i <- 1..rows, do: exec!(db, "INSERT INTO t VALUES (#{i}, randomblob(3900))")
      exec!(db, "COMMIT")
    end

    defp agrees!(h, wal, label) do
      assert {:ok, held, h} = Wal.read_held(h, wal)
      assert {:ok, ref} = Wal.read(wal)

      assert held.commit_extent == ref.commit_extent,
             "#{label}: held extent #{held.commit_extent} != reference #{ref.commit_extent}"

      h
    end

    test "agrees with the backward walk through appends, a rollback and a restart", ctx do
      %{db: db, wal: wal} = ctx
      exec!(db, "INSERT INTO t VALUES (0, NULL)")
      h = agrees!(nil, wal, "first read (no cursor)")

      grow!(db, 50)
      h = agrees!(h, wal, "after an append (cursor, same generation)")

      # A rolled-back transaction leaves abandoned frames of THIS generation past the extent.
      exec!(db, "BEGIN")
      for i <- 1..30, do: exec!(db, "INSERT INTO t VALUES (#{i}, randomblob(3900))")
      exec!(db, "ROLLBACK")
      h = agrees!(h, wal, "after a rollback (abandoned tail)")

      exec!(db, "INSERT INTO t VALUES (-1, NULL)")
      h = agrees!(h, wal, "a commit over the abandoned slots")

      # Checkpoint, then write: the WAL restarts in place, so a stale tail of the old generation.
      exec!(db, "PRAGMA wal_checkpoint(PASSIVE)")
      exec!(db, "INSERT INTO t VALUES (-2, NULL)")
      h = agrees!(h, wal, "first read after a restart (cursor from the old generation)")

      exec!(db, "INSERT INTO t VALUES (-3, NULL)")
      h = agrees!(h, wal, "second read in the new generation")

      Wal.close_held(h)
    end

    # THE REGRESSION GUARD. Pre-fix every header read walked back over every stale frame: measured
    # 7.6 ms per read at 4,000 stale frames (the default autocheckpoint size) against ~25 us with
    # the cursor. The bound is ~25x the post-fix figure and well under the pre-fix one, so it
    # discriminates without being a latency assertion.
    @tag :bench
    test "a header read over a 4,000-frame stale tail does not walk it", ctx do
      %{db: db, wal: wal} = ctx
      grow!(db, 4_000)
      exec!(db, "PRAGMA wal_checkpoint(PASSIVE)")
      exec!(db, "INSERT INTO t VALUES (-1, NULL)")

      {:ok, hdr, h} = Wal.read_held(nil, wal)
      assert hdr.size - hdr.commit_extent > 4_000 * 4_000, "fixture: no stale tail to walk"

      n = 50

      {us, h} =
        :timer.tc(fn ->
          Enum.reduce(1..n, h, fn _, acc ->
            {:ok, _, acc} = Wal.read_held(acc, wal)
            acc
          end)
        end)

      Wal.close_held(h)
      per_read = div(us, n)

      assert per_read < 1_000,
             "Wal.read_held took #{per_read} us per read over a 4,000-frame stale tail; it is " <>
               "walking the tail again (pre-fix: ~7,600 us)"
    end
  end
end
