defmodule Fathom.FSDbFilesTest do
  @moduledoc """
  `Fathom.FS.rm_db/1` and `wal_empty?/1` (cleanliness audit 2026-11-13 #34). A dozen call sites used
  to hand-roll the `["", "-wal", "-shm"]` delete loop, with different file modules and different
  orders; `wal_empty?` existed twice. These pin what the shared helpers promise, so a site cannot
  quietly diverge again.
  """
  use ExUnit.Case, async: true

  alias Fathom.FS

  setup do
    dir = Path.join(System.tmp_dir!(), "fs_db_files_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  test "rm_db removes the main file and both companions, and nothing else", %{dir: dir} do
    db = Path.join(dir, "a.db")
    other = Path.join(dir, "b.db")
    sidecar = db <> ".etag"

    for f <- [db, db <> "-wal", db <> "-shm", other, other <> "-wal", sidecar],
        do: File.write!(f, "x")

    assert :ok = FS.rm_db(db)

    refute File.exists?(db)
    refute File.exists?(db <> "-wal")
    refute File.exists?(db <> "-shm")
    # A sibling database and this one's own provenance sidecar are NOT companions.
    assert File.exists?(other)
    assert File.exists?(other <> "-wal")
    assert File.exists?(sidecar)
  end

  test "rm_db is idempotent and tolerates files that were never there", %{dir: dir} do
    db = Path.join(dir, "absent.db")
    assert :ok = FS.rm_db(db)

    File.write!(db <> "-wal", "x")
    assert :ok = FS.rm_db(db)
    refute File.exists?(db <> "-wal")
    assert :ok = FS.rm_db(db)
  end

  test "rm_db accepts a charlist path", %{dir: dir} do
    db = Path.join(dir, "c.db")
    File.write!(db, "x")
    assert :ok = FS.rm_db(String.to_charlist(db))
    refute File.exists?(db)
  end

  test "wal_empty?: absent and zero-length are empty; a header-only WAL is NOT", %{dir: dir} do
    wal = Path.join(dir, "d.db-wal")
    assert FS.wal_empty?(wal)

    File.write!(wal, "")
    assert FS.wal_empty?(wal)

    # A restarted log holding only its 32-byte header: no frame, but the conservative answer is
    # "not empty", because a caller deciding whether folding it away loses anything must not guess.
    File.write!(wal, :binary.copy(<<0>>, 32))
    refute FS.wal_empty?(wal)
  end
end
