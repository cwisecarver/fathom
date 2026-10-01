defmodule Fathom.Shard.Storage.LocalStreamedEtagTest do
  @moduledoc """
  The Local backend's etag is a streamed SHA-256 of the object, and it is the emulated If-Match
  fence: `flush/3`, `restore/3` and `restore_snapshot_from_file/3` all compare against it. The
  streamed digest MUST equal the whole-body one, or every fence comparison in the Local double
  silently breaks.

  (Moved here 2026-10-01 from `pull_if_changed_test.exs` when `pull_if_changed/3` — the removed
  warm standby's freshness check — was deleted as dead code. This test was about the etag, not the
  conditional pull, so it now reads the etag through `object_etag/1` and the bytes through `pull/2`.)
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage
  alias Fathom.Shard.Storage.Local

  setup do
    dir = Path.join(System.tmp_dir!(), "fathom_etag_#{System.unique_integer([:positive])}")
    prev = Application.get_env(:fathom, Local)
    Application.put_env(:fathom, Local, dir: dir)

    on_exit(fn ->
      File.rm_rf!(dir)

      if prev,
        do: Application.put_env(:fathom, Local, prev),
        else: Application.delete_env(:fathom, Local)
    end)

    %{dir: dir, shard: "etag_#{System.unique_integer([:positive])}"}
  end

  # Chosen larger than the 256 KiB hash chunk so the streaming path genuinely spans chunks.
  test "the streamed etag equals the whole-body digest across chunk boundaries", %{
    dir: dir,
    shard: shard
  } do
    body = :crypto.strong_rand_bytes(700 * 1024)
    src = Path.join(dir, "#{shard}.src")
    File.mkdir_p!(dir)
    File.write!(src, body)

    :ok = Storage.flush(shard, src)
    expected = Base.encode16(:crypto.hash(:sha256, body), case: :lower)

    assert {:ok, ^expected} = Storage.object_etag(shard),
           "the streamed digest diverged from the whole-body digest — every If-Match comparison " <>
             "in the Local fence double would silently break"

    dst = Path.join(dir, "#{shard}.dst")
    assert {:ok, ^expected} = Storage.pull(shard, dst)
    assert File.read!(dst) == body, "the streamed copy must be byte-identical"
  end
end
