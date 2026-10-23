defmodule Fathom.Tenants.RmLocalErasureTest do
  @moduledoc """
  Tenant deletion is an ERASURE path (expert review 2026-10-22 r8 #7, Q12=A). `rm_local/1` used to
  `Path.wildcard(<id>.db*)` the fleet-sized data dir through `file_server_2`; it now lists raw in a
  side process and removes each match raw. Pinned here: every file family of the deleted tenant is
  gone (live db, WAL, SHM, etag, and ALL `.fenced`/`.forked`/`.corrupt` quarantine copies), and no
  other tenant's file is touched, including ids that merely share a prefix with the victim
  (shard isolation).
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard
  alias Fathom.Tenants.Tombstones

  setup do
    id = "rmlocal#{System.unique_integer([:positive])}"
    dir = Shard.data_dir()
    File.mkdir_p!(dir)
    :ets.insert(Tombstones, {id})

    on_exit(fn ->
      :ets.delete(Tombstones, id)
      for f <- Path.wildcard(Path.join(dir, id <> "*")), do: File.rm(f)
    end)

    %{id: id, dir: dir}
  end

  test "erase_local removes every quarantine copy and file family, and nothing of a similar id",
       %{id: id, dir: dir} do
    mine =
      for suffix <- [
            ".db",
            ".db-wal",
            ".db-shm",
            ".db.etag",
            ".db.fenced.100",
            ".db.fenced.200",
            ".db.forked.100",
            ".db.corrupt.100",
            ".db.corrupt.200"
          ],
          do: Path.join(dir, id <> suffix)

    others =
      for name <- [id <> "9.db", id <> "9.db-wal", id <> "-2.db", id <> "_b.db.fenced.1"],
          do: Path.join(dir, name)

    for f <- mine ++ others, do: File.write!(f, "data")

    assert :ok = Fathom.Tenants.erase_local(id)

    for f <- mine, do: refute(File.exists?(f), "erased tenant's #{Path.basename(f)} survived")
    for f <- others, do: assert(File.exists?(f), "erase touched #{Path.basename(f)}")
  end
end
