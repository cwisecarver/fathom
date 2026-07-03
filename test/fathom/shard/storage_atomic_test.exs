defmodule Fathom.Shard.StorageAtomicTest do
  # Findings #24 + #28: shard object writes go through a temp file + atomic rename, so a
  # crash mid-write can't leave a torn "durable" object (#28) and a concurrent reader (a
  # warm-cache promotion racing a rewrite) can't observe a half-old/half-new file (#24).
  # The atomicity is the rename: the destination is only ever swapped in whole, never
  # mutated in place. Tested at the helper level (the backends delegate to it).
  use ExUnit.Case, async: true

  alias Fathom.Shard.Storage

  setup do
    dir =
      Path.join(System.tmp_dir!(), "fathom_atomic_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{dir: dir}
  end

  defp temps(dir), do: Path.wildcard(Path.join(dir, "*.tmp.*"))

  test "atomic_write writes the whole body and leaves no temp residue", %{dir: dir} do
    dst = Path.join(dir, "obj.db")
    assert :ok = Storage.atomic_write(dst, "hello-world")
    assert File.read!(dst) == "hello-world"
    assert temps(dir) == []
  end

  # The atomicity check that fails against a direct, in-place File.write: an fd opened before
  # the write keeps reading the ORIGINAL bytes, because the rename swaps in a new inode rather
  # than truncating and rewriting the one this fd points at. A direct File.write would show the
  # new content (or, mid-write, a torn prefix) through the same fd.
  test "replaces the destination as a whole, not in place", %{dir: dir} do
    dst = Path.join(dir, "obj.db")
    File.write!(dst, "OLD")

    {:ok, fd} = File.open(dst, [:read, :binary])

    assert :ok = Storage.atomic_write(dst, "NEW-CONTENT")

    assert IO.binread(fd, 100) == "OLD"
    File.close(fd)

    assert File.read!(dst) == "NEW-CONTENT"
  end

  test "atomic_copy copies the source bytes with no temp residue", %{dir: dir} do
    src = Path.join(dir, "src.db")
    dst = Path.join(dir, "dst.db")
    File.write!(src, "payload")

    assert :ok = Storage.atomic_copy(src, dst)
    assert File.read!(dst) == "payload"
    assert temps(dir) == []
  end

  test "a failed copy leaves the destination untouched and drops the temp", %{dir: dir} do
    dst = Path.join(dir, "dst.db")
    File.write!(dst, "durable")

    assert {:error, _} = Storage.atomic_copy(Path.join(dir, "missing.db"), dst)

    assert File.read!(dst) == "durable"
    assert temps(dir) == []
  end
end
