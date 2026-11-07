defmodule Fathom.ReplicationDirOverlapTest do
  @moduledoc """
  Expert review 2026-11-07 #37. Symptom: nothing refused `REPLICATION_DIR` equal to or inside
  `SHARD_DATA_DIR`, so a replica `<id>.db` and a live `<id>.db` could share a path. Invariant: the
  pure path comparison behind `check_replication_disk!/0` treats equal and nested directories (in
  either direction) as overlapping, on whole path segments, so siblings that merely share a name
  prefix are NOT overlapping (a false refusal would brick a correct deployment).
  """
  use ExUnit.Case, async: true

  alias Fathom.Application, as: App

  test "equal, nested, and containing directories overlap" do
    assert App.dirs_overlap?("/data/fathom", "/data/fathom")
    assert App.dirs_overlap?("/data/fathom/", "/data/fathom")
    assert App.dirs_overlap?("/data/fathom/replicas", "/data/fathom")
    assert App.dirs_overlap?("/data/fathom", "/data/fathom/replicas")
    assert App.dirs_overlap?("/data/fathom/../fathom/x", "/data/fathom/x")
  end

  test "siblings, including ones sharing a name prefix, do not overlap" do
    refute App.dirs_overlap?("/data/fathom_replication", "/data/fathom_shards")
    refute App.dirs_overlap?("/data/fathom", "/data/fathom2")
    refute App.dirs_overlap?("/mnt/replicas", "/mnt/shards")
  end

  test "the default dirs (both under TMPDIR) are siblings, so the default boots" do
    refute App.dirs_overlap?(
             Path.join(System.tmp_dir!(), "fathom_replication"),
             Path.join(System.tmp_dir!(), "fathom_shards")
           )
  end

  test "the root contains everything" do
    assert App.dirs_overlap?("/", "/data/fathom")
  end
end
