defmodule Fathom.Shard.Storage.LocalFlushAtomicityTest do
  @moduledoc """
  Expert review 2026-09-18 #22: the Local backend writes object / position / lineage as THREE
  separate files (S3 stamps the position on the same atomic PUT). On the update path a prior `.pos`
  already exists, so a crash after the object copy but before the position write used to leave the
  NEW object under the OLD, lower stamp — an over-claim A2 promotes on (a replica behind the new
  object but ahead of the old stamp reads as fresher). flush now removes the stale stamp BEFORE the
  copy, so the copy window is stamp-less, which `object_position/1` reads as "unknown" (never
  overridable). We drive a copy that FAILS (nonexistent source) after the removal: the stale stamp
  must be gone, not left describing the object.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage

  @pos1 %{epoch: 8, wal_gen: 1, offset: 100}
  @pos2 %{epoch: 9, wal_gen: 2, offset: 200}

  setup do
    shard = "localflush_#{System.unique_integer([:positive])}"
    src = Path.join(System.tmp_dir!(), "#{shard}_src.db")

    on_exit(fn ->
      for f <- Path.wildcard(Path.join(Storage.Local.dir(), "#{shard}*")), do: File.rm_rf(f)
      File.rm(src)
    end)

    %{shard: shard, src: src}
  end

  test "a flush whose copy fails leaves the object UNSTAMPED, not under the stale stamp (#22)", %{
    shard: shard,
    src: src
  } do
    # First flush: object B1 stamped @pos1.
    File.write!(src, "B1")
    {:ok, etag, _} = Storage.Local.flush(shard, src, nil, @pos1, nil)
    assert {:ok, @pos1} = Storage.Local.object_position(shard)

    # Second flush whose source does not exist → atomic_copy fails AFTER the fix has already removed
    # the stale stamp. Pre-fix nothing removed @pos1, so it stayed describing the object.
    missing = Path.join(System.tmp_dir!(), "#{shard}_missing.db")
    refute File.exists?(missing)

    assert {:error, _} = Storage.Local.flush(shard, missing, etag, @pos2, nil)

    assert {:ok, nil} = Storage.Local.object_position(shard),
           "a failed copy must leave the object UNSTAMPED (pre-fix the stale @pos1 remained)"
  end
end
