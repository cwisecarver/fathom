defmodule Fathom.Shard.ExtensionTest do
  @moduledoc """
  The security half of expert review 2026-08-01 #19.

  Supplying Django's UDFs requires `sqlite3_enable_load_extension`, which is **arbitrary code
  execution** on a multi-tenant engine: with it left on, one `SELECT load_extension('/tmp/evil.so')`
  from a tenant loads native code into the node, below every guard fathom has — the authorizer, the
  `:ro` scope, the write fence, the single-writer lease. It is a strictly larger hole than the
  ATTACH primitive #1 closed.

  So the tests here are not about Django at all. They assert the door is shut afterwards, from the
  tenant's side, rather than trusting that `Fathom.Shard.Extension.load/1` calls disable at the end
  — the sequence being right is what we *want*; a tenant being refused is what we can *check*.
  """
  use ExUnit.Case, async: false

  alias Exqlite.Sqlite3
  alias Fathom.Shard.Connection
  alias Fathom.Shard.Extension

  setup do
    path = Path.join(System.tmp_dir!(), "fathom_ext_#{System.unique_integer([:positive])}.db")

    on_exit(fn ->
      Application.delete_env(:fathom, :sqlite_extension)
      for s <- ["", "-wal", "-shm"], do: File.rm(path <> s)
    end)

    %{path: path}
  end

  defp open!(path, opts \\ []) do
    {:ok, conn} = Connection.open(path, opts)
    on_exit(fn -> Connection.close(conn) end)
    conn
  end

  describe "the extension-loading capability is closed again" do
    test "a tenant cannot load an extension of its own after open", %{path: path} do
      conn = open!(path, tenant?: true)

      # A path that does not exist: if loading were still ENABLED we would get a dlopen error
      # ("no such file"), which is a very different failure from being refused outright.
      assert {:error, reason} =
               Connection.query(conn, "SELECT load_extension('/tmp/definitely-not-here.so')", [])

      message = inspect(reason)

      assert message =~ "not authorized",
             "extension loading is still enabled on a tenant connection — got: #{message}"

      refute message =~ "no such file",
             "the load was ATTEMPTED, meaning the capability was left open"
    end

    test "the same holds on a non-tenant handle", %{path: path} do
      # fathom's own handles (durability snapshot, migration replay, bench harnesses) do not get
      # the authorizer, so this is the one that would be missed by a fix scoped to tenant?: true.
      conn = open!(path)

      assert {:error, reason} =
               Connection.query(conn, "SELECT load_extension('/tmp/definitely-not-here.so')", [])

      assert inspect(reason) =~ "not authorized"
    end

    test "the same holds on a read-only handle", %{path: path} do
      _ = open!(path)
      conn = open!(path, scope: :ro)

      assert {:error, reason} =
               Connection.query(conn, "SELECT load_extension('/tmp/definitely-not-here.so')", [])

      assert inspect(reason) =~ "not authorized"
    end

    test "a tenant cannot re-enable loading via a pragma", %{path: path} do
      conn = open!(path, tenant?: true)

      # There is no SQL-level pragma for this (the capability is C-API only), but a tenant will
      # try. Assert the negative so a future SQLite that adds one is caught here.
      _ = Connection.query(conn, "PRAGMA enable_load_extension=1", [])

      assert {:error, reason} =
               Connection.query(conn, "SELECT load_extension('/tmp/definitely-not-here.so')", [])

      assert inspect(reason) =~ "not authorized"
    end
  end

  describe "load/1 contract" do
    test "returns :skipped and leaves the connection usable when disabled by config", %{
      path: path
    } do
      Application.put_env(:fathom, :sqlite_extension, false)

      conn = open!(path)

      assert Extension.path() == nil
      refute Extension.available?()

      # The connection still works — it just has no Django UDFs, which is fathom's pre-#19 state.
      assert {:ok, %{rows: [[1]]}} = Connection.query(conn, "SELECT 1", [])

      assert {:error, reason} =
               Connection.query(conn, "SELECT django_date_extract('year','2026-08-05')", [])

      assert inspect(reason) =~ "no such function"
    end

    test "a missing artifact path is an open FAILURE, not a silent degrade", %{path: path} do
      # An operator who sets :sqlite_extension explicitly has asserted the file is there. Falling
      # back to "no Django functions" would turn a typo into a class of queryset that fails in
      # production and nowhere else.
      Application.put_env(:fathom, :sqlite_extension, "/tmp/fathom-no-such-extension.so")

      assert {:error, _reason} = Connection.open(path)
    end

    test "an explicit path is used over the default", %{path: path} do
      default = Extension.default_path()
      Application.put_env(:fathom, :sqlite_extension, default)

      assert Extension.path() == default
      conn = open!(path)

      assert {:ok, %{rows: [[2026]]}} =
               Connection.query(conn, "SELECT django_date_extract('year','2026-08-05')", [])
    end

    test "loading is idempotent across many connections to the same file", %{path: path} do
      # One connection per Hrana stream means this runs constantly. SQLite refuses to re-register
      # a function only in some configurations, so a second load returning an error would break
      # every stream after the first.
      conns = for _ <- 1..10, do: open!(path)

      for conn <- conns do
        assert {:ok, %{rows: [[2026]]}} =
                 Connection.query(conn, "SELECT django_date_extract('year','2026-08-05')", [])
      end
    end

    test "load/1 re-disables even when the load itself fails" do
      # The failure path is the one that matters: an extension that failed to load must not leave
      # the capability enabled behind it. Driven at the Sqlite3 level because Connection.open/1
      # discards the handle on error, and the point is the state of THAT handle.
      {:ok, db} = Sqlite3.open(":memory:")
      on_exit(fn -> Sqlite3.close(db) end)

      Application.put_env(:fathom, :sqlite_extension, "/tmp/fathom-no-such-extension.so")

      assert {:error, _} = Extension.load(db)

      # Now prove the door is shut on the very connection whose load failed.
      {:ok, stmt} = Sqlite3.prepare(db, "SELECT load_extension('/tmp/also-not-here.so')")
      result = Sqlite3.step(db, stmt)
      Sqlite3.release(db, stmt)

      assert {:error, message} = result
      assert message =~ "not authorized", "loading was left ENABLED after a failed load"
    end
  end

  describe "path resolution" do
    test "an unrecognized :sqlite_extension value is ignored rather than crashing the node" do
      Application.put_env(:fathom, :sqlite_extension, :nonsense)
      assert Extension.path() == nil
    end

    test "the default path lives under priv/sqlite_ext" do
      assert Extension.default_path() =~ "sqlite_ext"
    end
  end
end
