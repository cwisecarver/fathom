defmodule Fathom.ShardVersionedStorageTest do
  # Versioned copies (retain / restore / drop_version) for blue/green migration,
  # via the filesystem backend. Not async: the "remote" dir is global config.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage

  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")

  setup do
    shard = "ver_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      File.rm(live(shard))
      for path <- Path.wildcard(Path.join(@remote_dir, "#{shard}@*.db")), do: File.rm(path)
    end)

    %{shard: shard}
  end

  defp live(shard), do: Path.join(@remote_dir, "#{shard}.db")
  defp versioned(shard, v), do: Path.join(@remote_dir, "#{shard}@#{v}.db")

  defp write_remote!(path, content) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, content)
  end

  test "retain copies the live object to a versioned key, leaving live intact", %{shard: shard} do
    write_remote!(live(shard), "v1-data")

    assert :ok = Storage.retain(shard, 1)
    assert File.read!(versioned(shard, 1)) == "v1-data"
    assert File.read!(live(shard)) == "v1-data"
  end

  test "restore copies a versioned key back to live", %{shard: shard} do
    write_remote!(versioned(shard, 2), "v2-data")

    assert :ok = Storage.restore(shard, 2)
    assert File.read!(live(shard)) == "v2-data"
  end

  test "drop_version deletes the versioned key and is idempotent", %{shard: shard} do
    write_remote!(versioned(shard, 3), "x")

    assert :ok = Storage.drop_version(shard, 3)
    refute File.exists?(versioned(shard, 3))
    assert :ok = Storage.drop_version(shard, 3)
  end

  test "retain → overwrite live → restore round-trips (the revert path)", %{shard: shard} do
    write_remote!(live(shard), "old")
    assert :ok = Storage.retain(shard, 1)

    write_remote!(live(shard), "new")
    assert :ok = Storage.restore(shard, 1)

    assert File.read!(live(shard)) == "old"
  end
end
