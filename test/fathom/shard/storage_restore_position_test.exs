defmodule Fathom.Shard.StorageRestorePositionTest do
  @moduledoc """
  Expert review 2026-09-05 #20. On S3 a restore is a PUT that erases the object's user metadata, so
  the restored object is UNSTAMPED (s3.ex: "the POSITION stamp is deliberately NOT carried"). Local's
  flush/2 and drop_live already mirror that, but the RESTORE paths (restore/2, restore/3,
  restore_snapshot/2, restore_snapshot/3, restore_snapshot_from_file/3) did NOT — so
  object_position/1 kept returning the PRE-restore lineage's stamp, and try_promote_local/5 would
  promote a replica of the rolled-back-from lineage over the restored bytes, re-applying the exact
  corruption the operator just recovered from. Every restore must leave the object unstamped.

  Both restore families are covered here (a version restore and a snapshot restore); the three
  fenced /3 variants get the identical clear-on-success in the same commit.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage

  @pos %{epoch: 8, wal_gen: 1, offset: 100}

  setup do
    shard = "posrestore_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for f <- Path.wildcard(Path.join(Storage.Local.dir(), "#{shard}*")), do: File.rm_rf(f)
    end)

    %{shard: shard}
  end

  # Flush `contents` as the live object WITH the position stamp (the 5-arity fenced flush stamps it),
  # then confirm the stamp is present — the precondition a restore must clear.
  defp seed_stamped!(shard, contents) do
    tmp = Path.join(System.tmp_dir!(), "seed_#{shard}_#{System.unique_integer([:positive])}.db")
    File.write!(tmp, contents)

    # expected_etag nil ⇒ create-only for a brand-new shard; on a re-stamp the live etag is passed.
    {:ok, etag} = Storage.object_etag(shard)
    {:ok, new_etag, _} = Storage.flush(shard, tmp, etag, @pos, nil)
    File.rm(tmp)
    assert {:ok, @pos} = Storage.object_position(shard)
    new_etag
  end

  test "restore/2 (version restore) leaves the object unstamped", %{shard: shard} do
    seed_stamped!(shard, "live-bytes")
    :ok = Storage.retain(shard, 1)
    seed_stamped!(shard, "newer-bytes")

    :ok = Storage.restore(shard, 1)

    assert {:ok, nil} = Storage.object_position(shard),
           "a restore replaces the object's bytes, so the pre-restore position stamp must be gone"
  end

  test "restore_snapshot/2 leaves the object unstamped", %{shard: shard} do
    seed_stamped!(shard, "live-bytes")
    :ok = Storage.snapshot(shard, "snap-a")
    seed_stamped!(shard, "newer-bytes")

    :ok = Storage.restore_snapshot(shard, "snap-a")

    assert {:ok, nil} = Storage.object_position(shard),
           "a snapshot restore replaces the bytes, so the pre-restore position stamp must be gone"
  end
end
