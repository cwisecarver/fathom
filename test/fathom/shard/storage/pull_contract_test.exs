defmodule Fathom.Shard.Storage.PullContractTest do
  @moduledoc """
  `pull/2` must never claim bytes were written when none were (expert review 2026-08-01 #24).

  ## Why this is a data-loss bug, not a typing nit

  The S3 backend collapsed its steal-time sentinel to `{:ok, etag}` while writing no local
  file. Every "pull to a temp, then open it" consumer — `Directory.Reconcile`,
  `Snapshots`, `Tenants.export`, `RestoreDrillJob`, `mix fathom.shard` — then handed the
  missing path to `Connection.open/1`, which **creates** it: a valid, `quick_check`-clean,
  completely EMPTY database, indistinguishable from a real pull of a real shard.

  Found on the chaos rig, not by reading. A 180s soak with node churn left three of six
  tenants serving an empty database and failing every write with `no such table: kv`, their
  real rows surviving only in `.forked.*` quarantine files. All three stored objects were
  4096 bytes with the *same* etag — three copies of the same empty database. The panel had
  rated this Low likelihood ("needs a steal of a never-flushed shard") and it was deferred;
  the rig hit it three times in one run.

  The contract is now explicit:

    * `{:ok, etag}`     — bytes ARE at `local_path`.
    * `{:absent, etag}` — nothing was written; `etag` is still the first-flush fence (which is
      what round-2 #7's sentinel needs, so that property is preserved).

  These tests pin the contract at the backend boundary and at the consumer that would
  fabricate data from it.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Storage

  setup do
    dir = Path.join(System.tmp_dir!(), "pullcontract_#{System.unique_integer([:positive])}")
    prev = Application.get_env(:fathom, Fathom.Shard.Storage.Local)
    File.mkdir_p!(dir)
    Application.put_env(:fathom, Fathom.Shard.Storage.Local, dir: dir)

    on_exit(fn ->
      File.rm_rf!(dir)

      if prev,
        do: Application.put_env(:fathom, Fathom.Shard.Storage.Local, prev),
        else: Application.delete_env(:fathom, Fathom.Shard.Storage.Local)
    end)

    %{dir: dir, id: "pc_#{System.unique_integer([:positive])}"}
  end

  describe "the pull/2 contract" do
    test "a shard with no stored object reports :absent, not {:ok, nil}", %{id: id} do
      dst = Path.join(System.tmp_dir!(), "#{id}_none.db")
      on_exit(fn -> File.rm(dst) end)

      assert {:absent, nil} = Storage.pull(id, dst)

      refute File.exists?(dst),
             "pull must not create the destination when there is nothing to pull"
    end

    test "a real object still reports {:ok, etag} AND writes the bytes", %{id: id} do
      src = Path.join(System.tmp_dir!(), "#{id}_src.db")
      dst = Path.join(System.tmp_dir!(), "#{id}_dst.db")
      on_exit(fn -> for f <- [src, dst], s <- ["", "-wal", "-shm"], do: File.rm(f <> s) end)

      {:ok, c} = Connection.open(src)
      :ok = Connection.exec(c, "CREATE TABLE kv (v TEXT)")
      {:ok, _} = Connection.query(c, "INSERT INTO kv VALUES (?)", ["real"])
      {:ok, _} = Connection.query(c, "PRAGMA wal_checkpoint(TRUNCATE)", [])
      :ok = Connection.close(c)
      :ok = Storage.flush(id, src)

      assert {:ok, etag} = Storage.pull(id, dst)
      assert is_binary(etag)
      assert File.exists?(dst), "{:ok, _} promises bytes at the path"

      {:ok, c2} = Connection.open(dst)
      assert {:ok, %{rows: [["real"]]}} = Connection.query(c2, "SELECT v FROM kv", [])
      :ok = Connection.close(c2)
    end

    test "pull_snapshot follows the same contract", %{id: id} do
      dst = Path.join(System.tmp_dir!(), "#{id}_snap.db")
      on_exit(fn -> File.rm(dst) end)

      assert {:absent, nil} = Storage.pull_snapshot(id, "nosuchsnap", dst)
      refute File.exists?(dst)
    end
  end

  describe "the consumers that used to fabricate an empty database" do
    # THE bug: `{:ok, <etag>}` with no file, then `Connection.open/1` on the missing path
    # creates a clean empty database and every check downstream reads it as real.
    test "export refuses a shard with no stored bytes instead of returning an empty db",
         %{id: id} do
      assert {:error, :not_stored} = Fathom.Tenants.export(id)
    end

    # `Reconcile.stored_user_version/1` and the drill's verify are private, so assert the
    # property at the boundary they both depend on: a pull that wrote nothing must be
    # distinguishable from one that wrote bytes, WITHOUT opening the path. Before the fix a
    # sentinel was indistinguishable, and each of those callers opened it — manufacturing a
    # clean empty database with user_version 0.
    test "no consumer can mistake 'nothing written' for a real pull", %{id: id} do
      tmp = Path.join(System.tmp_dir!(), "#{id}_consumer.db")
      on_exit(fn -> File.rm(tmp) end)

      result = Storage.pull(id, tmp)

      refute match?({:ok, _}, result),
             "a pull that wrote no bytes must not report {:ok, _} — that is what let " <>
               "reconcile/export/the drill open a path Connection.open/1 then CREATED"

      assert {:absent, _} = result
      refute File.exists?(tmp)
    end
  end

  describe "a shard coordinator still opens correctly with no stored object" do
    # The fix must not break the ordinary birth of a new tenant: no object is the normal
    # state for one, and it must still open, serve, and take writes.
    test "a brand-new shard opens, serves, and records born-empty provenance" do
      shard = "pcnew_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        Fathom.Shards.drain(shard, 2_000)
        for s <- ["", "-wal", "-shm", ".etag"], do: File.rm(Fathom.Shard.db_path(shard) <> s)
      end)

      {:ok, conn} = Fathom.ShardExecutor.open(shard)

      assert {:ok, _} =
               Fathom.ShardExecutor.execute(conn, %Filo.Stmt{
                 sql: "CREATE TABLE kv (v TEXT)",
                 args: []
               })

      assert {:ok, _} =
               Fathom.ShardExecutor.execute(conn, %Filo.Stmt{
                 sql: "INSERT INTO kv VALUES ('born')",
                 args: []
               })

      assert :ok = Fathom.ShardExecutor.close(conn)

      assert File.read!(Fathom.Shard.db_path(shard) <> ".etag") == "-",
             "a born-empty shard must record the no-object provenance sentinel"
    end
  end
end
