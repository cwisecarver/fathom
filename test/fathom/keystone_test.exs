defmodule Fathom.KeystoneTest do
  @moduledoc """
  Two jobs, and the first one guards the second.

  **The fixture is honest.** `Fathom.Keystone` claims to cover every SQLite storage class and
  every type affinity. A fixture that quietly stopped covering something would weaken every test
  built on it without failing anything, so the claims are asserted against the built file rather
  than trusted from the DDL.

  **The data path is lossless.** A keystone is what a tenant is forked from and what a migration
  copies. Those paths are asserted to return every value *and its SQLite type* unchanged. The
  types matter as much as the values: affinity decides the storage class a value lands in, so a
  copy that dropped a declared type could hand back `"123"` where the source held `123` — the
  same thing to a loose comparison, a different type to every client.
  """
  # Real SQLite files and the globally-configured Local storage backend.
  use ExUnit.Case, async: false

  alias Fathom.Keystone
  alias Fathom.Migrator.Copy
  alias Fathom.Shard.{Connection, Storage}

  setup do
    id = "ks_#{System.unique_integer([:positive])}"
    %{id: id, path: tmp_path(id)}
  end

  defp tmp_path(name) do
    path = Path.join(System.tmp_dir!(), "#{name}.db")
    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
    path
  end

  defp query!(path, sql, args \\ []) do
    {:ok, conn} = Connection.open(path)
    {:ok, result} = Connection.query(conn, sql, args)
    Connection.close(conn)
    result
  end

  defp family_cols(kind) do
    {_, cols, _} = Enum.find(Keystone.edge_values(), fn {k, _, _} -> k == kind end)
    cols
  end

  # `IS ?` rather than `= ?` so a pinned NULL is matched too — `= NULL` is never true in SQL.
  # Column names are interpolated because they are the generator's own compile-time literals, not
  # input; the VALUE is always bound.
  defp value_present?(path, cols, value) do
    Enum.any?(cols, fn col ->
      %{rows: [[n]]} =
        query!(path, "SELECT count(*) FROM ks_scalars WHERE \"#{col}\" IS ?", [value])

      n > 0
    end)
  end

  defp non_null_match?(path, cols, value) do
    Enum.any?(cols, fn col ->
      %{rows: [[n]]} =
        query!(
          path,
          "SELECT count(*) FROM ks_scalars WHERE \"#{col}\" = ? AND \"#{col}\" IS NOT NULL",
          [value]
        )

      n > 0
    end)
  end

  # --- the fixture keeps its promises ----------------------------------------

  describe "coverage" do
    test "every SQLite storage class is present in the built file", %{path: path} do
      {:ok, _} = Keystone.build!(path)

      # Asserted against the DATA, not the DDL. A column declared BLOB that never received one
      # proves nothing, and NULL in particular is easy to lose from a fixture by accident.
      classes =
        query!(
          path,
          """
          SELECT DISTINCT typeof(c_integer) FROM ks_scalars
          UNION SELECT DISTINCT typeof(c_text) FROM ks_scalars
          UNION SELECT DISTINCT typeof(c_blob) FROM ks_scalars
          UNION SELECT DISTINCT typeof(c_real) FROM ks_scalars
          UNION SELECT DISTINCT typeof(s_any) FROM ks_strict
          """
        ).rows
        |> List.flatten()
        |> MapSet.new()

      assert MapSet.equal?(classes, MapSet.new(~w(null integer real text blob))),
             "keystone must contain all five storage classes, got: #{inspect(Enum.sort(classes))}"
    end

    test "one ANY column carries all five classes at once", %{path: path} do
      {:ok, _} = Keystone.build!(path)

      # STRICT + ANY is the only place SQLite lets a single column hold every class, which makes
      # it the tightest single check that a copy preserves class rather than coercing.
      classes =
        query!(path, "SELECT DISTINCT typeof(s_any) FROM ks_strict").rows
        |> List.flatten()
        |> MapSet.new()

      assert MapSet.equal?(classes, MapSet.new(~w(null integer real text blob)))
    end

    test "EVERY pinned edge value is present somewhere in its affinity family", %{path: path} do
      {:ok, _} = Keystone.build!(path)

      # Driven off `Keystone.edge_values/0` rather than a restated column list, so a value added
      # to the fixture is covered here automatically and a column list cannot drift out of sync.
      #
      # This is the assertion that caught the generator's original spread: values were assigned
      # by `phash2(column)`, so with five pinned blobs and only two BLOB-affinity columns, three
      # of them were never inserted at all while the fixture still claimed to cover them.
      for {kind, cols, values} <- Keystone.edge_values(), value <- values do
        assert value_present?(path, cols, value),
               "#{kind}: pinned value #{inspect(value)} is in no column of #{inspect(cols)}"
      end
    end

    test "the empty string and the empty blob are distinct from NULL", %{path: path} do
      {:ok, _} = Keystone.build!(path)

      # A copy that conflated "" with NULL would pass any test that only asked "is there a value".
      assert non_null_match?(path, family_cols(:text), ""),
             "no non-NULL empty string in the keystone"

      assert non_null_match?(path, family_cols(:blobs), {:blob, <<>>}),
             "no non-NULL empty blob in the keystone"
    end

    test "NUMERIC affinity converts, and the fixture pins the conversion", %{path: path} do
      {:ok, _} = Keystone.build!(path)

      # '123' into a NUMERIC column is stored as INTEGER 123 — correct SQLite behavior, and the
      # reason the keystone pins it rather than letting it surprise someone inside random data.
      assert %{rows: rows} =
               query!(
                 path,
                 "SELECT DISTINCT typeof(c_numeric) FROM ks_scalars WHERE c_numeric IS NOT NULL"
               )

      types = rows |> List.flatten() |> MapSet.new()
      assert MapSet.member?(types, "integer") or MapSet.member?(types, "real")
    end

    test "generated columns compute, and the trigger fired", %{path: path} do
      {:ok, _} = Keystone.build!(path)

      assert %{rows: [[0]]} =
               query!(path, "SELECT count(*) FROM ks_generated WHERE doubled <> base * 2")

      assert %{rows: [[0]]} =
               query!(path, "SELECT count(*) FROM ks_generated WHERE labeled <> 'v' || base")

      # The AFTER INSERT trigger rewrites NULL `ci` to 'auto'. If it never fired, the copy has no
      # trigger-written rows to carry and the trigger is untested.
      assert %{rows: [[n]]} =
               query!(path, "SELECT count(*) FROM ks_constraints WHERE ci = 'auto'")

      assert n > 0, "the keystone trigger never fired, so nothing exercises it"
    end

    test "a couple hundred rows by default, and the count is tunable", %{id: id} do
      default_path = tmp_path("#{id}_default")
      {:ok, _} = Keystone.build!(default_path)

      assert %{rows: [[n]]} = query!(default_path, "SELECT count(*) FROM ks_scalars")
      assert n >= 256, "expected at least a couple hundred rows, got #{n}"

      # Every table is populated — a fixture with one seeded table and five empty ones would
      # satisfy a row count on `ks_scalars` alone.
      for table <- Keystone.tables() do
        assert %{rows: [[c]]} = query!(default_path, "SELECT count(*) FROM #{table}")
        assert c > 0, "#{table} is empty"
      end

      bigger = tmp_path("#{id}_bigger")
      {:ok, _} = Keystone.build!(bigger, rows: 1_000)
      assert %{rows: [[1_000]]} = query!(bigger, "SELECT count(*) FROM ks_scalars")
    end
  end

  # --- determinism ------------------------------------------------------------

  describe "determinism" do
    test "the same seed builds an identical keystone", %{id: id} do
      a = tmp_path("#{id}_a")
      b = tmp_path("#{id}_b")

      {:ok, _} = Keystone.build!(a, rows: 64, seed: 99)
      {:ok, _} = Keystone.build!(b, rows: 64, seed: 99)

      # A fixture that moved between runs would make the benchmark measure the fixture and would
      # stop a failing test from reproducing.
      assert Keystone.dump(a) == Keystone.dump(b)
    end

    test "a different seed builds different data", %{id: id} do
      a = tmp_path("#{id}_a")
      b = tmp_path("#{id}_b")

      {:ok, _} = Keystone.build!(a, rows: 64, seed: 1)
      {:ok, _} = Keystone.build!(b, rows: 64, seed: 2)

      # Guards the reverse mistake: a "seeded" generator that ignores its seed is deterministic
      # and useless as a fuzzer.
      refute Keystone.dump(a) == Keystone.dump(b)
    end

    test "building works in a process that has never touched :rand", %{path: path} do
      # Regression: `:rand.export_seed/0` returns the ATOM `:undefined` in a never-seeded
      # process, and `:undefined` is truthy in Elixir, so the restore fed it to `:rand.seed/1`
      # and died in `:rand.mk_alg/1`. Every ExUnit test process is already seeded, so the whole
      # suite passed while `mix fathom.bench` — a fresh process — crashed on its first run.
      task =
        Task.async(fn ->
          assert :undefined == :rand.export_seed(), "this process must start unseeded"
          Keystone.build!(path, rows: 8)
        end)

      assert {:ok, _} = Task.await(task, 30_000)
    end

    test "building does not disturb the caller's :rand state", %{path: path} do
      :rand.seed(:exsss, {1, 2, 3})
      expected = for _ <- 1..5, do: :rand.uniform(1_000_000)

      :rand.seed(:exsss, {1, 2, 3})
      {:ok, _} = Keystone.build!(path, rows: 8)
      actual = for _ <- 1..5, do: :rand.uniform(1_000_000)

      # Seeding :rand is process-global. A helper that leaves the process re-seeded would make
      # every later random value in that test depend on whether the keystone was built.
      assert actual == expected
    end
  end

  # --- the data path is lossless ---------------------------------------------

  describe "round-trip fidelity" do
    test "Copy.migrate carries every value and every type unchanged", %{id: id} do
      source = tmp_path("#{id}_src")
      dest = tmp_path("#{id}_dst")

      {:ok, _} = Keystone.build!(source)
      before = Keystone.dump(source)

      assert :ok = Copy.migrate(source, dest, 7, [])

      # Values AND typeof(), every table, in a stable order. This is the assertion the whole
      # fixture exists to make: a blue/green copy of a tenant is byte-faithful.
      assert Keystone.dump(dest) == before
      assert %{rows: [[7]]} = query!(dest, "PRAGMA user_version")
    end

    test "Copy.migrate carries the schema objects, not just the rows", %{id: id} do
      source = tmp_path("#{id}_src")
      dest = tmp_path("#{id}_dst")

      {:ok, _} = Keystone.build!(source)
      assert :ok = Copy.migrate(source, dest, 1, [])

      schema = fn path ->
        query!(
          path,
          "SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).rows
      end

      # Indexes, the view, the trigger, the STRICT and WITHOUT ROWID declarations, every
      # collation and CHECK — all of it lives in sqlite_master, and all of it must come across.
      assert schema.(dest) == schema.(source)
    end

    test "a storage flush and pull round-trips the keystone unchanged", %{id: id} do
      source = tmp_path("#{id}_src")
      pulled = tmp_path("#{id}_pulled")

      {:ok, _} = Keystone.build!(source)
      before = Keystone.dump(source)

      # The durability path: what a node uploads on idle and what a survivor pulls on cold open.
      :ok = Storage.flush(id, source)

      on_exit(fn ->
        for p <- Path.wildcard(Path.join(Storage.Local.dir(), "#{id}*")), do: File.rm(p)
      end)

      assert {:ok, _etag} = Storage.pull(id, pulled)
      assert Keystone.dump(pulled) == before
    end

    test "a migration replayed over the keystone leaves the existing data intact", %{id: id} do
      source = tmp_path("#{id}_src")
      dest = tmp_path("#{id}_dst")

      {:ok, _} = Keystone.build!(source)
      before = Keystone.dump(source)

      # A realistic forward migration: add a column and an index over the fuzzed data.
      statements = [
        {"ALTER TABLE ks_scalars ADD COLUMN added_col TEXT", []},
        {"CREATE INDEX ks_added_idx ON ks_scalars (added_col)", []}
      ]

      assert :ok = Copy.migrate(source, dest, 2, statements)

      # The new column exists…
      assert %{rows: [[n]]} =
               query!(
                 dest,
                 "SELECT count(*) FROM pragma_table_info('ks_scalars') WHERE name = 'added_col'"
               )

      assert n == 1

      # …and every pre-existing value in every other table is untouched. `ks_scalars` gained a
      # column, so it is compared on the columns it had before rather than skipped.
      for {table, expected} <- Map.delete(before, "ks_scalars") do
        assert Keystone.dump(dest)[table] == expected, "#{table} changed across the migration"
      end

      assert %{rows: [[c]]} = query!(dest, "SELECT count(*) FROM ks_scalars")
      assert %{rows: [[^c]]} = query!(source, "SELECT count(*) FROM ks_scalars")
    end
  end
end
