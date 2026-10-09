defmodule Fathom.Migrator.CopyWalFoldTest do
  @moduledoc """
  Expert review 2026-10-08 #25: the migration copy's single WAL fold must not report success when
  it could not fold.

  Since the fold moved to once per chain (60c0b99) it is the only thing that puts the whole chain
  into the main file before the main file alone is uploaded. `PRAGMA wal_checkpoint(TRUNCATE)`
  "succeeds" even when a reader still holds a snapshot — it returns `busy = 1` and leaves the frames
  past that reader's mark in the WAL — and `verify_migrated`/`verify_ledger` read THROUGH the WAL, so
  nothing downstream noticed.

  The reader here is the realistic culprit: a `Transform` that opens a second connection on the
  shard and leaves a read transaction open.
  """
  # Not async: the transform allowlist is global application env.
  use ExUnit.Case, async: false

  alias Fathom.Migrator.Copy
  alias Fathom.Shard.Connection

  defmodule PinsAReader do
    @behaviour Fathom.Migrator.Transform

    # Opens a second connection on the file being migrated and leaves a read transaction open on it,
    # then shortens the copy connection's busy wait so the fold gives up quickly instead of after 5 s.
    @impl true
    def run(conn, _shard_id) do
      {:ok, %{rows: rows}} = Connection.query(conn, "PRAGMA database_list", [])
      [_seq, "main", path] = Enum.find(rows, &match?([_, "main", _], &1))
      {:ok, reader} = Connection.open(path)
      :ok = Connection.exec(reader, "BEGIN")
      {:ok, _} = Connection.query(reader, "SELECT count(*) FROM sqlite_master", [])
      Process.put(:pinned_reader, reader)
      Connection.set_busy_timeout(conn, 50)
    end
  end

  setup do
    base = Path.join(System.tmp_dir!(), "fathom_fold_#{System.unique_integer([:positive])}")
    source = base <> "-old.db"
    dest = base <> "-new.db"

    prev = Application.get_env(:fathom, :migration_transforms)
    Application.put_env(:fathom, :migration_transforms, [PinsAReader])

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :migration_transforms, prev),
        else: Application.delete_env(:fathom, :migration_transforms)

      for path <- [source, dest], suffix <- ["", "-wal", "-shm"], do: File.rm(path <> suffix)
    end)

    {:ok, conn} = Connection.open(source)
    :ok = Connection.exec(conn, "CREATE TABLE t (a INTEGER)")
    :ok = Connection.exec(conn, "PRAGMA wal_checkpoint(TRUNCATE)")
    Connection.close(conn)

    %{source: source, dest: dest}
  end

  test "a fold blocked by a lingering reader fails the chain instead of passing", ctx do
    chain = [
      {1, [{"INSERT INTO t VALUES (1)", []}], to_string(PinsAReader)},
      {2, [{"INSERT INTO t VALUES (2)", []}]}
    ]

    result = Copy.migrate_chain(ctx.source, ctx.dest, chain, shard_id: "fold_shard")

    case Process.delete(:pinned_reader) do
      nil -> :ok
      reader -> Connection.close(reader)
    end

    assert {:error, {:wal_fold_incomplete, [1 | _]}} = result,
           "the copy reported success with the chain only partly folded into the main file"
  end
end
