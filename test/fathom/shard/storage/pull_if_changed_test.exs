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

  # Expert review 2026-07-24 #25: the Local backend read whole shard files into binaries and hashed
  # them in memory — a fenced flush held TWO shard-sized binaries at once (local + remote) plus two
  # full hash passes, and `pull_if_changed/3` read and hashed the entire object just to conclude
  # nothing had changed (the S3 backend does a bodiless 304). Both now stream.
  #
  # The etag is the emulated If-Match fence, so the streamed digest MUST equal the whole-body one
  # or every fence comparison silently breaks. Chosen larger than the 256 KiB hash chunk so the
  # streaming path genuinely spans multiple chunks.
  test "the streamed etag equals the whole-body digest across chunk boundaries", %{
    dir: dir,
    shard: shard
  } do
    body = :crypto.strong_rand_bytes(700 * 1024)
    src = Path.join(dir, "#{shard}.src")
    File.mkdir_p!(dir)
    File.write!(src, body)

    :ok = Storage.flush(shard, src)

    # Round-trip through the fence: the etag the flush reports must match what a fresh read sees,
    # and must equal a plain in-memory digest of the same bytes.
    expected = Base.encode16(:crypto.hash(:sha256, body), case: :lower)

    dst = Path.join(dir, "#{shard}.dst")
    assert {:ok, {:written, etag}} = Storage.pull_if_changed(shard, dst, "no-match")

    assert etag == expected,
           "the streamed digest diverged from the whole-body digest — every If-Match comparison " <>
             "in the Local fence double would silently break"

    assert File.read!(dst) == body, "the streamed copy must be byte-identical"
    assert {:ok, :unchanged} = Storage.pull_if_changed(shard, dst, etag)
  end
end
