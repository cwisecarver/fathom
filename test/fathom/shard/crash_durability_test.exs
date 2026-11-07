defmodule Fathom.Shard.CrashDurabilityTest do
  @moduledoc """
  Expert review 2026-11-07 #27 (no directory fsync after any rename), #83 (a zero-length lock is
  visible between create and write), #85 (no test could observe an fsync).

  Every case drives real code through `Fathom.FS` against a `Fathom.Test.CrashFS`, which models what
  a power loss keeps: file data only after `sync_file`, a rename or link only after `sync_dir`
  (`crash!/1` reverts the undurable entries, `crash!(renames: :keep)` keeps them and still loses
  unsynced data; a durable write must hold under both).

  The "double bites" describe is the part that makes the rest trustworthy. Each durability property
  is shown false when the corresponding fsync is STUBBED to a no-op (`skip:`), so a future edit that
  drops `sync_file` or `sync_dir` from `with_atomic_temp/3` turns the main tests red instead of the
  suite staying green (review #85: "replacing `sync_file` with `:ok` keeps the suite green").
  Without that, these would be traces of what the code asks for, not proofs.

  Which paths do NOT fsync the directory is pinned too (`atomic_copy(dir_sync: false)`,
  `promote_temp/2`, `atomic_create/2`): that is a measured decision (docs/component-notes.md, review
  #27), and a pin keeps it from being "fixed" back onto the cold-open path unmeasured.

  Not async: the Local-backend cases repoint `Storage.Local`'s directory in the application env.
  """
  use ExUnit.Case, async: false

  alias Fathom.FS
  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.Local
  alias Fathom.Test.CrashFS
  alias Fathom.Test.Env

  setup do
    dir = Path.join(System.tmp_dir!(), "crash_dur_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  describe "the premise: a directory CAN be fsynced on this OTP" do
    # The comment this replaces said `:file.open` on a directory is `eisdir` and that the BEAM
    # cannot fsync one. The plain open IS `eisdir`; the `:directory` mode is what works. Pinned so
    # the false premise cannot come back (the fathom OTP floor is 28; the mode is documented in
    # `kernel`'s `file:open/2`).
    test "the :directory open mode opens it and fsync succeeds", %{dir: dir} do
      assert {:error, :eisdir} = :file.open(String.to_charlist(dir), [:read, :raw])

      assert {:ok, fd} = :file.open(String.to_charlist(dir), [:read, :raw, :directory])
      assert :ok = :file.sync(fd)
      assert :ok = :file.close(fd)

      assert :ok = FS.sync_dir(dir)
      assert {:error, :enoent} = FS.sync_dir(Path.join(dir, "missing"))
    end
  end

  describe "a PUBLISH (atomic_write/2, atomic_copy/2) survives a power loss" do
    test "the order is write, fsync file, rename, fsync dir", %{dir: dir} do
      crash = CrashFS.start!(dir)
      dst = Path.join(dir, "obj")

      assert :ok = Storage.atomic_write(dst, "new")

      assert [{:wrote, tmp}, {:sync_file, tmp}, {:rename, tmp, ^dst}, {:sync_dir, ^dir}] =
               CrashFS.trace(crash)

      assert String.starts_with?(tmp, dst <> ".tmp.")
    end

    for mode <- [:revert, :keep] do
      test "atomic_write: whole-new after a crash that follows the return (renames: #{mode})",
           %{dir: dir} do
        crash = CrashFS.start!(dir)
        dst = Path.join(dir, "obj")
        File.write!(dst, "old")

        assert :ok = Storage.atomic_write(dst, "new")
        assert :ok = CrashFS.crash!(crash, renames: unquote(mode))

        assert File.read!(dst) == "new"
      end

      test "atomic_copy: whole-new after a crash that follows the return (renames: #{mode})",
           %{dir: dir} do
        crash = CrashFS.start!(dir)
        src = Path.join(dir, "src")
        dst = Path.join(dir, "obj")
        File.write!(src, "payload")
        File.write!(dst, "old")

        assert :ok = Storage.atomic_copy(src, dst)
        assert :ok = CrashFS.crash!(crash, renames: unquote(mode))

        assert File.read!(dst) == "payload"
      end
    end

    test "creating the parent directory fsyncs each new level's parent", %{dir: dir} do
      crash = CrashFS.start!(dir)
      nested = Path.join([dir, "a", "b"])

      assert :ok = Storage.ensure_dir(nested)

      assert [{:sync_dir, ^dir}, {:sync_dir, parent}] = CrashFS.trace(crash)
      assert parent == Path.join(dir, "a")
      # An existing directory is the steady state: no fsync, one stat.
      assert :ok = Storage.ensure_dir(nested)
      assert length(CrashFS.trace(crash)) == 2
    end
  end

  describe "an INSTALL (promote_temp/2, atomic_copy(dir_sync: false)) fsyncs the file, not the directory" do
    # Measured and deliberate (review #27, docs/component-notes.md): the installed file only matters
    # once a write is acked, SQLite fsyncs the directory when that write creates the -wal, and the
    # source object is untouched, so a lost rename is a re-pull. On APFS the directory fsync costs
    # about as much as the file fsync and took the Local double's cold_open_p50_us +40 %.
    test "promote_temp: fsync file, rename, and NO directory fsync", %{dir: dir} do
      crash = CrashFS.start!(dir)
      tmp = Path.join(dir, "obj.pull")
      dst = Path.join(dir, "obj")
      # Streamed in by the S3 downloader, i.e. not through the seam and not fsynced.
      File.write!(tmp, "pulled")

      assert :ok = Storage.promote_temp(tmp, dst)

      assert [{:wrote, ^tmp}, {:sync_file, ^tmp}, {:rename, ^tmp, ^dst}] = CrashFS.trace(crash)
    end

    for mode <- [:revert, :keep] do
      test "promote_temp: never torn after a crash, whole-old or whole-new (renames: #{mode})",
           %{dir: dir} do
        crash = CrashFS.start!(dir)
        tmp = Path.join(dir, "obj.pull")
        dst = Path.join(dir, "obj")
        File.write!(dst, "old")
        File.write!(tmp, "pulled")

        assert :ok = Storage.promote_temp(tmp, dst)
        assert :ok = CrashFS.crash!(crash, renames: unquote(mode))

        assert File.read!(dst) in ["old", "pulled"]
      end
    end

    test "atomic_copy(dir_sync: false) skips only the directory fsync", %{dir: dir} do
      crash = CrashFS.start!(dir)
      src = Path.join(dir, "src")
      dst = Path.join(dir, "obj")
      File.write!(src, "payload")

      assert :ok = Storage.atomic_copy(src, dst, dir_sync: false)

      assert [{:wrote, tmp}, {:sync_file, tmp}, {:rename, tmp, ^dst}] = CrashFS.trace(crash)
      assert :ok = CrashFS.crash!(crash, renames: :keep)
      assert File.read!(dst) == "payload"
    end
  end

  describe "the double bites: each durability property is false when its fsync is stubbed" do
    test "without the directory fsync a crash reverts the rename (previous object returns)",
         %{dir: dir} do
      crash = CrashFS.start!(dir, skip: [:sync_dir])
      dst = Path.join(dir, "obj")
      File.write!(dst, "old")

      assert :ok = Storage.atomic_write(dst, "new")
      assert :ok = CrashFS.crash!(crash)

      assert File.read!(dst) == "old",
             "the double must revert a rename whose directory was never synced"
    end

    test "without the file fsync a crash leaves the NAME over zero-filled bytes", %{dir: dir} do
      crash = CrashFS.start!(dir, skip: [:sync_file])
      dst = Path.join(dir, "obj")
      File.write!(dst, "old")

      assert :ok = Storage.atomic_write(dst, "new")
      assert :ok = CrashFS.crash!(crash, renames: :keep)

      assert File.read!(dst) == <<0, 0, 0>>,
             "the double must zero-fill data that was never fsynced"
    end

    test "promote_temp without the file fsync adopts zero-filled bytes", %{dir: dir} do
      crash = CrashFS.start!(dir, skip: [:sync_file])
      tmp = Path.join(dir, "obj.pull")
      dst = Path.join(dir, "obj")
      File.write!(tmp, "pulled")

      assert :ok = Storage.promote_temp(tmp, dst)
      assert :ok = CrashFS.crash!(crash, renames: :keep)

      assert File.read!(dst) == <<0, 0, 0, 0, 0, 0>>
    end

    test "another directory's operations are not recorded (scoping)", %{dir: dir} do
      crash = CrashFS.start!(dir)

      other =
        Path.join(System.tmp_dir!(), "crash_dur_other_#{System.unique_integer([:positive])}")

      File.mkdir_p!(other)
      on_exit(fn -> File.rm_rf(other) end)

      assert :ok = Storage.atomic_write(Path.join(other, "obj"), "x")

      assert CrashFS.trace(crash) == []
    end
  end

  describe "Local backend: flush, then drop the local copy, then lose power" do
    setup %{dir: dir} do
      remote = Path.join(dir, "remote")
      File.mkdir_p!(remote)
      Env.put!(Local, dir: remote)
      %{remote: remote, shard: "crash_#{System.unique_integer([:positive])}"}
    end

    # The drop path: `Local.flush` returns `:ok`, the coordinator then unlinks the only other copy
    # (`drop_local`). If the rename is not durable, a power cut brings the PREVIOUS object back and
    # everything since the last flush, reported durable, is gone.
    test "the flushed object survives", ctx do
      %{dir: dir, remote: remote, shard: shard} = ctx
      crash = CrashFS.start!(remote)
      src = Path.join(dir, "local.db")

      File.write!(src, "v1")
      assert :ok = Local.flush(shard, src)
      assert :ok = CrashFS.crash!(crash)
      File.write!(src, "v2-acked-and-flushed")
      assert :ok = Local.flush(shard, src)

      File.rm!(src)
      assert :ok = CrashFS.crash!(crash)

      assert {:ok, _etag} = Local.pull(shard, src)
      assert File.read!(src) == "v2-acked-and-flushed"
    end

    test "with the directory fsync stubbed, the same sequence loses the flush", ctx do
      %{dir: dir, remote: remote, shard: shard} = ctx
      crash = CrashFS.start!(remote)
      src = Path.join(dir, "local.db")

      File.write!(src, "v1")
      assert :ok = Local.flush(shard, src)
      assert :ok = CrashFS.crash!(crash)

      CrashFS.set(crash, skip: [:sync_dir])
      File.write!(src, "v2-acked-and-flushed")
      assert :ok = Local.flush(shard, src)
      File.rm!(src)
      assert :ok = CrashFS.crash!(crash)

      assert {:ok, _etag} = Local.pull(shard, src)
      assert File.read!(src) == "v1", "the double must reproduce the lost flush"
    end

    test "tombstones and token floors are published durably", %{remote: remote} do
      crash = CrashFS.start!(remote)

      assert :ok = Local.put_tombstone("tomb_1")
      assert :ok = CrashFS.crash!(crash)

      assert {:ok, ids} = Local.tombstoned_ids()
      assert "tomb_1" in ids
    end
  end

  describe "Local.create_lock never publishes a lock without its bytes (review #83)" do
    setup %{dir: dir} do
      remote = Path.join(dir, "remote")
      File.mkdir_p!(remote)
      Env.put!(Local, dir: remote)
      %{remote: remote, id: "lk_#{System.unique_integer([:positive])}"}
    end

    test "the lock name appears by link(2) of an already-written temp", %{remote: remote, id: id} do
      crash = CrashFS.start!(remote)
      lock = Path.join(remote, id <> ".lock")

      assert {:ok, lease} = Local.acquire_lease(id, "node_a", 30_000)

      trace = CrashFS.trace(crash)
      assert [{:wrote, tmp}, {:link, tmp, ^lock}] = trace
      assert String.starts_with?(tmp, lock <> ".tmp.")

      refute Enum.any?(trace, &match?({:wrote, ^lock}, &1)),
             "the lock name itself must never be written in place (a zero-length lock between " <>
               "create and write is :corrupt_lock until an operator repairs it)"

      refute File.exists?(tmp), "the temp name is dropped once the lock is linked"
      assert :ok = Local.check_lease(id, lease)
    end

    test "a failure BEFORE the link leaves no lock at all, never an empty one",
         %{remote: remote, id: id} do
      crash =
        CrashFS.start!(remote,
          inject: fn op -> if match?({:link, _, _}, op), do: {:error, :eio}, else: :ok end
        )

      assert {:error, :eio} = Local.acquire_lease(id, "node_a", 30_000)
      assert :ok = CrashFS.crash!(crash)

      refute File.exists?(Path.join(remote, id <> ".lock"))
      assert Path.wildcard(Path.join(remote, id <> ".lock*")) == []

      # ...and the next acquire is a clean first create.
      CrashFS.set(crash, inject: fn _ -> :ok end)
      assert {:ok, _} = Local.acquire_lease(id, "node_a", 30_000)
    end

    test "the create race still has exactly one winner", %{remote: remote, id: id} do
      _crash = CrashFS.start!(remote)

      assert {:ok, _} = Local.acquire_lease(id, "node_a", 30_000)

      assert {:error, {:held, "node_a", _}} = Local.acquire_lease(id, "node_b", 30_000)
      assert Path.wildcard(Path.join(remote, id <> ".lock.tmp.*")) == []
    end

    # PARKED, pinned so it is not mistaken for fixed: `atomic_create/2` does not fsync (it took the
    # Local double's cold_open_p50_us +39 %, docs/component-notes.md review #27/#83). A POWER loss
    # that keeps the link but loses the temp's data therefore still leaves a zero-filled lock. When
    # the fsync is shipped, flip this to `assert :ok = Local.check_lease(id, lease)` and add the
    # `skip: [:sync_file]` bite test.
    test "KNOWN GAP (parked): a power loss after acquire can leave a zero-filled lock",
         %{remote: remote, id: id} do
      crash = CrashFS.start!(remote)

      assert {:ok, lease} = Local.acquire_lease(id, "node_a", 30_000)
      assert :ok = CrashFS.crash!(crash, renames: :keep)

      assert {:error, :corrupt_lock} = Local.check_lease(id, lease)
    end
  end

  describe "Storage.atomic_create/2" do
    test "is exclusive and leaves the first writer's bytes", %{dir: dir} do
      path = Path.join(dir, "x.lock")

      assert :ok = Storage.atomic_create(path, "first")
      assert {:error, :eexist} = Storage.atomic_create(path, "second")

      assert File.read!(path) == "first"
      assert Path.wildcard(path <> ".tmp.*") == []
    end
  end
end
