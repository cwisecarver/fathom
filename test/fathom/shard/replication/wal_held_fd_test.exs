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

  test "delta reads revalidate too, not just header reads", %{path: path} do
    # Both call sites hold the same fd, so both need the check. A delta read that skipped it would
    # ship OLD bytes with a header the caller read correctly — worse than the reverse, because
    # nothing downstream would disagree.
    File.write!(path, wal_bytes(7, 111, :binary.copy(<<0xAA>>, 512)))
    assert {:ok, _h, h1} = Wal.read_held(nil, path)

    File.rm!(path)
    File.write!(path, wal_bytes(0, 222, :binary.copy(<<0xBB>>, 512)))

    assert {:ok, bin, h2} = Wal.read_delta_held(h1, path, 32, 16)

    assert bin == :binary.copy(<<0xBB>>, 16),
           "a delta read through a stale fd returned the DELETED file's bytes"

    Wal.close_held(h2)
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
end
