defmodule Fathom.Migrator.RowCountsTest do
  @moduledoc """
  Expert review 2026-11-07 **#64**, the second half: a replay must conserve per-table row counts
  where it should (`Fathom.Migrator.RowCounts`). Django rebuilds a table for most
  `AlterField`/`RemoveField` (`CREATE new__x` / `INSERT … SELECT` / `DROP x` / `RENAME`); a copy
  that drops or duplicates rows left `quick_check`, `foreign_key_check` and the ledger green, and
  the tenant silently lost (or doubled) data.

  Pinned: a lossy and a duplicating rebuild are refused and rolled back; a faithful rebuild, a step
  that writes a table by name, a recreated table, a CTE-led statement and DML under a trigger are
  not; a `Transform` that does not declare `row_count_deltas/0` opts out (so no existing data
  migration starts failing), one that declares them has exactly those tables exempt and every other
  table checked; and the check follows `:verify_flush_integrity`.

  Not async: the allowlist and the verification switch are app env.
  """
  use ExUnit.Case, async: false

  alias Fathom.Migrator.Copy
  alias Fathom.Shard.Connection
  alias Fathom.Test.Env

  # Rewrites values in place and removes no rows: the shape most backfills have.
  defmodule NoDeclaration do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform
    @impl true
    def run(conn, _shard_id), do: exec(conn, "DELETE FROM app_other WHERE id <= 2")

    def exec(conn, sql) do
      case Fathom.Shard.Connection.query(conn, sql, []) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defmodule DeclaresMinusTwo do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform
    @impl true
    def run(conn, _shard_id), do: NoDeclaration.exec(conn, "DELETE FROM app_other WHERE id <= 2")
    @impl true
    def row_count_deltas, do: %{"app_other" => -2}
  end

  defmodule DeclaresAny do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform
    @impl true
    def run(conn, _shard_id), do: NoDeclaration.exec(conn, "DELETE FROM app_other WHERE id <= 2")
    @impl true
    def row_count_deltas, do: %{"app_other" => :any}
  end

  # Declares "values only" but loses a row from an UNdeclared table: the case the check is for.
  defmodule DeclaresNothingButDeletes do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform
    @impl true
    def run(conn, _shard_id), do: NoDeclaration.exec(conn, "DELETE FROM app_other WHERE id = 1")
    @impl true
    def row_count_deltas, do: %{}
  end

  defmodule WrongDelta do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform
    @impl true
    def run(conn, _shard_id), do: NoDeclaration.exec(conn, "DELETE FROM app_other WHERE id = 1")
    @impl true
    def row_count_deltas, do: %{"app_other" => -2}
  end

  defmodule BackfillsOnly do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform
    @impl true
    def run(conn, _shard_id),
      do: NoDeclaration.exec(conn, "UPDATE app_thing SET name = upper(name)")

    @impl true
    def row_count_deltas, do: %{}
  end

  setup do
    base = Path.join(System.tmp_dir!(), "fathom_rowcounts_#{System.unique_integer([:positive])}")
    source = base <> "-old.db"
    dest = base <> "-new.db"

    on_exit(fn ->
      for path <- [source, dest], suffix <- ["", "-wal", "-shm"], do: File.rm(path <> suffix)
    end)

    Env.put!(:migration_transforms, [
      NoDeclaration,
      DeclaresMinusTwo,
      DeclaresAny,
      DeclaresNothingButDeletes,
      BackfillsOnly
    ])

    seed!(source)
    %{source: source, dest: dest}
  end

  # app_thing: 3 rows, app_other: 3 rows, plus Django's ledger.
  defp seed!(path) do
    {:ok, conn} = Connection.open(path)

    for sql <- [
          "CREATE TABLE app_thing (id INTEGER PRIMARY KEY, name TEXT)",
          "INSERT INTO app_thing (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')",
          "CREATE TABLE app_other (id INTEGER PRIMARY KEY, note TEXT)",
          "INSERT INTO app_other (id, note) VALUES (1, 'x'), (2, 'y'), (3, 'z')",
          "CREATE TABLE django_migrations (id INTEGER PRIMARY KEY, app TEXT, name TEXT, applied TEXT)",
          "INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0001', 'now')"
        ] do
      :ok = Connection.exec(conn, sql)
    end

    Connection.close(conn)
  end

  defp count!(path, table) do
    {:ok, conn} = Connection.open(path)
    {:ok, %{rows: [[n]]}} = Connection.query(conn, "SELECT count(*) FROM #{table}", [])
    Connection.close(conn)
    n
  end

  defp version!(path) do
    {:ok, conn} = Connection.open(path)
    {:ok, %{rows: [[v]]}} = Connection.query(conn, "PRAGMA user_version", [])
    Connection.close(conn)
    v
  end

  @new ~s|CREATE TABLE "new__app_thing" ("id" integer NOT NULL PRIMARY KEY, "name" text NULL)|
  @drop ~s|DROP TABLE "app_thing"|
  @rename ~s|ALTER TABLE "new__app_thing" RENAME TO "app_thing"|

  defp rebuild(copy_sql), do: [{@new, []}, {copy_sql, []}, {@drop, []}, {@rename, []}]

  @faithful ~s|INSERT INTO "new__app_thing" ("id", "name") SELECT "id", "name" FROM "app_thing"|

  describe "a rebuild that loses or doubles rows" do
    # Pre-fix: `:ok`, and the tenant shipped at v2 with one row fewer.
    test "dropping a row is refused, rolled back, and names the table", ctx do
      statements =
        rebuild(
          ~s|INSERT INTO "new__app_thing" ("id", "name") SELECT "id", "name" FROM "app_thing" WHERE "id" != 2|
        )

      assert {:error,
              {:row_count_mismatch, 2,
               [%{table: "app_thing", before: 3, after: 2, expected_delta: 0}]}} =
               Copy.migrate(ctx.source, ctx.dest, 2, statements)

      # Rolled back: the copy is the old schema with all its rows, still at the old version.
      assert count!(ctx.dest, "app_thing") == 3
      assert version!(ctx.dest) == 0
    end

    test "duplicating rows is refused too", ctx do
      statements =
        rebuild(
          ~s|INSERT INTO "new__app_thing" ("id", "name") SELECT "id", "name" FROM "app_thing" | <>
            ~s|UNION ALL SELECT "id" + 100, "name" FROM "app_thing"|
        )

      assert {:error, {:row_count_mismatch, 2, [%{table: "app_thing", before: 3, after: 6}]}} =
               Copy.migrate(ctx.source, ctx.dest, 2, statements)
    end

    test "a mismatch in the SECOND step of a chain names that step and leaves the copy at the first",
         ctx do
      chain = [
        {2, [{"ALTER TABLE app_other ADD COLUMN extra TEXT", []}]},
        {3,
         rebuild(~s|INSERT INTO "new__app_thing" SELECT "id", "name" FROM "app_thing" LIMIT 1|)}
      ]

      assert {:error, {:row_count_mismatch, 3, _}} =
               Copy.migrate_chain(ctx.source, ctx.dest, chain)

      assert version!(ctx.dest) == 2
      assert count!(ctx.dest, "app_thing") == 3
    end

    # Control, same fixture: the faithful rebuild passes, so the failures above are the loss.
    test "a faithful rebuild passes and every table keeps its rows", ctx do
      assert :ok = Copy.migrate(ctx.source, ctx.dest, 2, rebuild(@faithful))
      assert count!(ctx.dest, "app_thing") == 3
      assert count!(ctx.dest, "app_other") == 3
      assert version!(ctx.dest) == 2
    end

    test "the check follows :verify_flush_integrity", ctx do
      Env.put!(:verify_flush_integrity, false)

      statements =
        rebuild(~s|INSERT INTO "new__app_thing" SELECT "id", "name" FROM "app_thing" LIMIT 1|)

      assert :ok = Copy.migrate(ctx.source, ctx.dest, 2, statements)
    end
  end

  describe "what a step is allowed to change without declaring it" do
    test "a table its own DML names (quoted, schema-qualified, OR IGNORE) may change", ctx do
      statements = [
        {~s|DELETE FROM "app_thing" WHERE id = 3|, []},
        {~s|INSERT OR IGNORE INTO "main"."app_other" (id, note) VALUES (9, 'new')|, []},
        {"UPDATE app_other SET note = 'u'", []}
      ]

      assert :ok = Copy.migrate(ctx.source, ctx.dest, 2, statements)
      assert count!(ctx.dest, "app_thing") == 2
      assert count!(ctx.dest, "app_other") == 4
    end

    test "a table dropped and created again under its name may change", ctx do
      statements = [
        {"DROP TABLE app_other", []},
        {"CREATE TABLE app_other (id INTEGER PRIMARY KEY, note TEXT)", []}
      ]

      assert :ok = Copy.migrate(ctx.source, ctx.dest, 2, statements)
      assert count!(ctx.dest, "app_other") == 0
    end

    test "a CTE-led write cannot be read, so the step is not checked", ctx do
      statements = [{"WITH gone AS (SELECT id FROM app_other) DELETE FROM app_other", []}]
      assert :ok = Copy.migrate(ctx.source, ctx.dest, 2, statements)
      assert count!(ctx.dest, "app_other") == 0
    end

    test "DML in a database with triggers is not checked (a trigger writes unnamed tables)",
         ctx do
      {:ok, conn} = Connection.open(ctx.source)

      :ok =
        Connection.exec(
          conn,
          "CREATE TRIGGER app_thing_gc AFTER DELETE ON app_thing " <>
            "BEGIN DELETE FROM app_other WHERE id = old.id; END"
        )

      Connection.close(conn)

      assert :ok = Copy.migrate(ctx.source, ctx.dest, 2, [{"DELETE FROM app_thing", []}])
      assert count!(ctx.dest, "app_other") == 0
    end

    test "a DDL-only step that touches no rows passes (the common case)", ctx do
      assert :ok =
               Copy.migrate(ctx.source, ctx.dest, 2, [
                 {"ALTER TABLE app_thing ADD COLUMN created_at TEXT", []},
                 {"INSERT INTO django_migrations (app, name, applied) VALUES ('app', '0002', 'now')",
                  []}
               ])
    end
  end

  describe "the Transform seam" do
    defp with_transform(ctx, module) do
      Copy.migrate_chain(ctx.source, ctx.dest, [{2, [], to_string(module)}], shard_id: "rc")
    end

    # An existing data migration (no callback) deletes rows from a table its statements do not
    # name. It must keep replaying: the opt-out is the default.
    test "a transform that does not declare deltas opts its step out", ctx do
      assert :ok = with_transform(ctx, NoDeclaration)
      assert count!(ctx.dest, "app_other") == 1
    end

    test "a transform's declared delta is the expected count, exactly", ctx do
      assert :ok = with_transform(ctx, DeclaresMinusTwo)
      assert count!(ctx.dest, "app_other") == 1
    end

    test "a declared delta that the transform does not make is refused", ctx do
      # Declares -2, removes 1.
      Env.put!(:migration_transforms, [WrongDelta])

      assert {:error,
              {:row_count_mismatch, 2,
               [%{table: "app_other", before: 3, after: 2, expected_delta: -2}]}} =
               with_transform(ctx, WrongDelta)

      assert count!(ctx.dest, "app_other") == 3
    end

    test ":any exempts the table", ctx do
      assert :ok = with_transform(ctx, DeclaresAny)
    end

    # The point of declaring: tables the transform did not list are still checked.
    test "a transform that declares no deltas but loses rows from another table is refused",
         ctx do
      assert {:error, {:row_count_mismatch, 2, [%{table: "app_other", before: 3, after: 2}]}} =
               with_transform(ctx, DeclaresNothingButDeletes)

      assert version!(ctx.dest) == 0
    end

    test "a values-only backfill that declares %{} passes", ctx do
      assert :ok = with_transform(ctx, BackfillsOnly)
      assert count!(ctx.dest, "app_thing") == 3
    end
  end
end
