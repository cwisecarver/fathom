defmodule Fathom.Shard.Storage.PullIfChangedTest do
  @moduledoc """
  The conditional-pull freshness primitive (`pull_if_changed/3`) on the Local
  backend — the store double for the warm-standby freshness check. A warm cache may
  lag the owner's latest flush, so before serving it the coordinator confirms it
  equals the store's current object; this primitive is that confirmation.

  Local synthesizes an etag as a content hash (changes iff the bytes change), which
  is exactly S3's `If-None-Match` semantics without a live bucket. The S3 backend's
  real conditional GET is covered under `@tag :s3` in `shard_storage_s3_test.exs`.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.Local

  setup do
    # Isolate each test in its own remote dir so objects never collide.
    dir = Path.join(System.tmp_dir!(), "fathom_pic_#{System.unique_integer([:positive])}")
    prev = Application.get_env(:fathom, Local)
    Application.put_env(:fathom, Local, dir: dir)

    on_exit(fn ->
      File.rm_rf!(dir)

      if prev,
        do: Application.put_env(:fathom, Local, prev),
        else: Application.delete_env(:fathom, Local)
    end)

    %{dir: dir, shard: "pic_#{System.unique_integer([:positive])}"}
  end

  # Seed the "remote" object with `content` and return its etag (captured via an
  # unconditional pull, the same way a follower captures it).
  defp seed_object(shard, content) do
    src = Path.join(System.tmp_dir!(), "pic_src_#{System.unique_integer([:positive])}")
    File.write!(src, content)
    :ok = Storage.flush(shard, src)
    File.rm(src)
  end

  defp local_path(shard),
    do: Path.join(System.tmp_dir!(), "pic_dst_#{shard}_#{System.unique_integer([:positive])}")

  test "a nil etag is an unconditional pull that writes and captures the current etag",
       %{shard: shard} do
    seed_object(shard, "v1-bytes")
    dst = local_path(shard)

    assert {:ok, {:written, etag}} = Storage.pull_if_changed(shard, dst, nil)
    assert is_binary(etag)
    assert File.read!(dst) == "v1-bytes"
  end

  test "a matching etag returns :unchanged and writes nothing", %{shard: shard} do
    seed_object(shard, "v1-bytes")
    dst = local_path(shard)
    {:ok, {:written, etag}} = Storage.pull_if_changed(shard, dst, nil)
    File.rm!(dst)

    # Same etag ⇒ the object hasn't changed ⇒ 304-equivalent: no byte written.
    assert {:ok, :unchanged} = Storage.pull_if_changed(shard, dst, etag)
    refute File.exists?(dst), "an :unchanged pull must not write the local file"
  end

  test "a stale etag re-pulls the fresh bytes and returns the new etag", %{shard: shard} do
    seed_object(shard, "v1-bytes")
    dst = local_path(shard)
    {:ok, {:written, etag1}} = Storage.pull_if_changed(shard, dst, nil)

    # The owner flushes a new version: the object's etag moves.
    seed_object(shard, "v2-bytes")

    assert {:ok, {:written, etag2}} = Storage.pull_if_changed(shard, dst, etag1)
    assert etag2 != etag1
    assert File.read!(dst) == "v2-bytes", "a stale etag must re-pull the current bytes"
  end

  test "a missing object returns :absent and writes nothing", %{shard: shard} do
    dst = local_path(shard)

    assert {:ok, :absent} = Storage.pull_if_changed(shard, dst, nil)
    assert {:ok, :absent} = Storage.pull_if_changed(shard, dst, "some-stale-etag")
    refute File.exists?(dst)
  end
end
