defmodule Fathom.Shard.MaterializerPromotePullTest do
  @moduledoc """
  Expert review 2026-10-08 #10: `promote_pull/2` writes the provenance sidecar BEFORE renaming the
  pulled temp into place, which is safe only if a crash between the two leaves a sidecar with no
  `.db`. On a repull the path already holds the earlier, stale pull, so the residue was the stale
  bytes paired with the NEWER etag — read as a provenance match on the next open, served, and
  flushed over the newer object.

  The crash is reproduced with the `:promote_pull_crash_hook` seam, which raises at exactly that
  point — after the sidecar write, before the rename. The invariant pinned: whatever a crash there
  leaves behind, a `.db` is never paired with an etag it was not pulled from.
  """
  # Not async: the crash hook is global application env.
  use ExUnit.Case, async: false

  alias Fathom.Shard.{Materializer, Provenance}

  setup do
    dir = Path.join(System.tmp_dir!(), "promote_pull_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    %{path: Path.join(dir, "shard.db")}
  end

  test "a promote that dies before the rename leaves no stale .db under the newer etag", %{
    path: path
  } do
    # The earlier pull: bytes of the OLD lineage, stamped with the etag they came from.
    File.write!(path, "stale bytes from the first pull")
    :ok = Provenance.write(path, "\"old-etag\"")

    # The repull's freshly pulled temp, and a node that dies between the sidecar and the rename.
    File.write!(Materializer.pull_temp(path), "fresh bytes from the repull")
    Application.put_env(:fathom, :promote_pull_crash_hook, fn _ -> raise "node died here" end)
    on_exit(fn -> Application.delete_env(:fathom, :promote_pull_crash_hook) end)

    assert_raise RuntimeError, "node died here", fn ->
      Materializer.promote_pull(path, "\"new-etag\"")
    end

    refute File.exists?(path) and Provenance.read(path) == {:ok, "\"new-etag\""},
           "the stale .db survived paired with the newer etag — the next open adopts it as current"
  end

  test "a normal promote over an existing stale .db lands the pulled bytes and their etag", %{
    path: path
  } do
    File.write!(path, "stale")
    :ok = Provenance.write(path, "\"old-etag\"")
    File.write!(Materializer.pull_temp(path), "fresh")

    assert {:ok, "\"new-etag\""} = Materializer.promote_pull(path, "\"new-etag\"")
    assert File.read!(path) == "fresh"
    assert Provenance.read(path) == {:ok, "\"new-etag\""}
  end
end
