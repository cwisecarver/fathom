defmodule Fathom.Migrator.TransformTest do
  @moduledoc """
  Expert review 2026-08-01 #26: the migration engine could not roll out a data migration.

  The engine replays the template's SQL verbatim, so a `RunPython` backfill crossed the wire as
  literal INSERT/UPDATE/DELETE carrying **the template's row values**. Capture flagged that shape
  and capped HEAD below the version, leaving an operator two options — approve it (replay the
  template's rows onto every tenant, the exact corruption the flag prevents) or never advance,
  with every later migration stacked behind it. `AddField` + `RunPython` is the most common
  two-step Django migration, so this was certain in month one.

  These cover the third path: a per-shard transform, run inside the same transaction as the DDL.

  Two things get disproportionate attention here, because they are what makes the seam safe rather
  than merely present:

    * **the allowlist**, since a release row is data written by the capture path and resolving an
      arbitrary module name from it would be fleet-wide remote code execution;
    * **transactionality**, since a transform that fails must roll back the DDL with it — a shard
      left with a new column and no backfill is a silent per-tenant corruption that every version
      stamp would agree was fine.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Migrator.Copy
  alias Fathom.Migrator.Transform
  alias Fathom.Shard.Connection

  # --- test transforms -----------------------------------------------------------------------

  defmodule Backfill do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform

    @impl true
    def run(conn, shard_id) do
      # Deliberately derives the value from THIS tenant's rows plus its own id — the thing a
      # replayed template literal cannot do.
      case Connection.query(conn, "UPDATE t SET note = ? || ':' || (SELECT COUNT(*) FROM t)", [
             shard_id
           ]) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defmodule Failing do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform
    @impl true
    def run(_conn, _shard_id), do: {:error, :deliberate}
  end

  defmodule Raising do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform
    @impl true
    def run(_conn, _shard_id), do: raise("boom")
  end

  defmodule BadReturn do
    @moduledoc false
    @behaviour Fathom.Migrator.Transform
    @impl true
    def run(_conn, _shard_id), do: :yep
  end

  defmodule NotRegistered do
    @moduledoc false
    def run(_conn, _shard_id), do: :ok
  end

  @allowed [Backfill, Failing, Raising, BadReturn]

  setup do
    prev = Application.get_env(:fathom, :migration_transforms)
    Application.put_env(:fathom, :migration_transforms, @allowed)

    dir = Path.join(System.tmp_dir!(), "fathom_transform_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :migration_transforms, prev),
        else: Application.delete_env(:fathom, :migration_transforms)

      File.rm_rf(dir)
    end)

    src = Path.join(dir, "src.db")
    {:ok, conn} = Connection.open(src)
    :ok = Connection.exec(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
    {:ok, _} = Connection.query(conn, "INSERT INTO t (id) VALUES (1),(2),(3)", [])
    Connection.close(conn)

    %{dir: dir, src: src, dest: Path.join(dir, "dest.db")}
  end

  defp read(path, sql) do
    {:ok, conn} = Connection.open(path)

    try do
      case Connection.query(conn, sql, []) do
        {:ok, %{rows: [[v] | _]}} -> v
        other -> other
      end
    after
      Connection.close(conn)
    end
  end

  # --- the allowlist -------------------------------------------------------------------------

  describe "resolve/1 — the security boundary" do
    test "resolves a registered module by either spelling" do
      assert {:ok, Backfill} = Transform.resolve(to_string(Backfill))
      assert {:ok, Backfill} = Transform.resolve("Fathom.Migrator.TransformTest.Backfill")
    end

    test "refuses a module that is loaded but NOT registered" do
      # Being loaded is not permission. This is the case that matters: the attacker's payload is a
      # module that already exists in the release.
      assert Code.ensure_loaded?(NotRegistered)
      assert {:error, :not_allowed} = Transform.resolve(to_string(NotRegistered))
    end

    test "refuses arbitrary and dangerous names" do
      for name <- ["System", "Elixir.System", "File", ":os", "Kernel", "", "Enum"] do
        assert {:error, reason} = Transform.resolve(name)
        assert reason in [:not_allowed, :no_transform], name
      end
    end

    test "resolving never creates an atom" do
      # `String.to_atom/1` on a data column is an unbounded atom-table write; `to_existing_atom/1`
      # would still resolve any loaded module. Neither is used, so a never-seen name stays unknown.
      name = "Definitely.Not.A.Module.#{System.unique_integer([:positive])}"
      assert {:error, :not_allowed} = Transform.resolve(name)
      assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    end

    test "valid?/1 requires BOTH registration and a run/2 export" do
      assert Transform.valid?(Backfill)
      refute Transform.valid?(NotRegistered), "not registered"

      # Registered but without run/2 — the "you registered the wrong module" case, which should be
      # an error at attach time rather than an UndefinedFunctionError mid-rollout.
      Application.put_env(:fathom, :migration_transforms, [Enum])
      refute Transform.valid?(Enum)
    end

    test "the allowlist is empty by default" do
      Application.delete_env(:fathom, :migration_transforms)
      assert Transform.allowlist() == []
      assert {:error, :not_allowed} = Transform.resolve(to_string(Backfill))
    end
  end

  # --- execution -----------------------------------------------------------------------------

  describe "migrate_chain/4 with a transform" do
    test "runs after the DDL, in the same transaction", %{src: src, dest: dest} do
      chain = [{7, [{"ALTER TABLE t ADD COLUMN note TEXT", []}], to_string(Backfill)}]

      assert :ok = Copy.migrate_chain(src, dest, chain, shard_id: "acme")

      # The transform saw the column the DDL had just added, and computed from this tenant's rows.
      assert read(dest, "SELECT note FROM t LIMIT 1") == "acme:3"
      assert read(dest, "PRAGMA user_version") == 7
    end

    test "a DDL-only step still works (the overwhelmingly common case)", %{src: src, dest: dest} do
      assert :ok = Copy.migrate_chain(src, dest, [{3, [{"ALTER TABLE t ADD COLUMN x TEXT", []}]}])
      assert read(dest, "PRAGMA user_version") == 3
    end

    test "a failing transform ROLLS BACK the DDL", %{src: src, dest: dest} do
      # The property that matters most. Committing the DDL without the backfill leaves the tenant
      # with a new column full of NULLs while user_version, django_migrations and the directory all
      # agree the migration succeeded — a silent per-tenant corruption.
      chain = [{7, [{"ALTER TABLE t ADD COLUMN note TEXT", []}], to_string(Failing)}]

      assert {:error, {:transform_failed, 7, _, :deliberate}} =
               Copy.migrate_chain(src, dest, chain, shard_id: "acme")

      assert {:error, _} = read(dest, "SELECT note FROM t LIMIT 1")
      assert read(dest, "PRAGMA user_version") == 0, "the version must not be stamped"
    end

    test "a raising transform is an error, not a crash", %{src: src, dest: dest} do
      chain = [{7, [{"ALTER TABLE t ADD COLUMN note TEXT", []}], to_string(Raising)}]

      assert {:error, {:transform_raised, 7, _, msg}} =
               Copy.migrate_chain(src, dest, chain, shard_id: "acme")

      assert msg =~ "boom"
      assert read(dest, "PRAGMA user_version") == 0
    end

    test "a transform returning something unrecognized fails rather than committing", %{
      src: src,
      dest: dest
    } do
      chain = [{7, [{"ALTER TABLE t ADD COLUMN note TEXT", []}], to_string(BadReturn)}]

      assert {:error, {:transform_bad_return, 7, _, :yep}} =
               Copy.migrate_chain(src, dest, chain, shard_id: "acme")

      assert read(dest, "PRAGMA user_version") == 0
    end

    test "an unregistered transform fails BEFORE any file is copied", %{src: src, dest: dest} do
      chain = [{7, [{"ALTER TABLE t ADD COLUMN note TEXT", []}], to_string(NotRegistered)}]

      assert {:error, {:not_allowed, 7, _}} =
               Copy.migrate_chain(src, dest, chain, shard_id: "acme")

      refute File.exists?(dest), "the copy should not have been made"
    end

    test "a transform without a shard_id is refused", %{src: src, dest: dest} do
      # A backfill that does not know its tenant is either wrong or should not be a transform.
      chain = [{7, [{"ALTER TABLE t ADD COLUMN note TEXT", []}], to_string(Backfill)}]

      assert {:error, {:transform_requires_shard_id, [7]}} = Copy.migrate_chain(src, dest, chain)
      refute File.exists?(dest)
    end

    test "a multi-version chain runs each step's own transform", %{src: src, dest: dest} do
      chain = [
        {5, [{"ALTER TABLE t ADD COLUMN a TEXT", []}], nil},
        {6, [{"ALTER TABLE t ADD COLUMN note TEXT", []}], to_string(Backfill)},
        {7, [{"ALTER TABLE t ADD COLUMN b TEXT", []}], nil}
      ]

      assert :ok = Copy.migrate_chain(src, dest, chain, shard_id: "beta")
      assert read(dest, "SELECT note FROM t LIMIT 1") == "beta:3"
      assert read(dest, "PRAGMA user_version") == 7
    end

    test "a mid-chain transform failure leaves the copy at the last good version", %{
      src: src,
      dest: dest
    } do
      chain = [
        {5, [{"ALTER TABLE t ADD COLUMN a TEXT", []}], nil},
        {6, [{"ALTER TABLE t ADD COLUMN note TEXT", []}], to_string(Failing)}
      ]

      assert {:error, {:transform_failed, 6, _, _}} =
               Copy.migrate_chain(src, dest, chain, shard_id: "acme")

      assert read(dest, "PRAGMA user_version") == 5, "step 5 committed, step 6 rolled back"
      assert {:error, _} = read(dest, "SELECT note FROM t LIMIT 1")
    end

    test "the transform computes per tenant, not from a template literal", %{
      src: src,
      dir: dir
    } do
      # The whole point: the same release, run for two tenants, produces different data.
      chain = [{7, [{"ALTER TABLE t ADD COLUMN note TEXT", []}], to_string(Backfill)}]

      a = Path.join(dir, "a.db")
      b = Path.join(dir, "b.db")

      assert :ok = Copy.migrate_chain(src, a, chain, shard_id: "tenant-a")
      assert :ok = Copy.migrate_chain(src, b, chain, shard_id: "tenant-b")

      assert read(a, "SELECT note FROM t LIMIT 1") == "tenant-a:3"
      assert read(b, "SELECT note FROM t LIMIT 1") == "tenant-b:3"
    end
  end
end
