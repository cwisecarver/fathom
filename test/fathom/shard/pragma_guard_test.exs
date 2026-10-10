defmodule Fathom.Shard.PragmaGuardTest do
  @moduledoc """
  The ENGINE backstop under the tenant PRAGMA gate (expert review 2026-10-08 #1, tier b).

  `Fathom.ShardExecutor`'s PRAGMA allow-list is a hand parser over statement text, and five of its
  defects shipped as live bypasses — the latest (ae5171a) a `;` inside a comment, which let even a
  `:ro` token set the process-global `hard_heap_limit` and fail every co-resident tenant's
  allocations. Each time the engine ran the assignment, because nothing below the parser looked.

  These tests skip the parser on purpose: they send SQL straight to `Connection.query/4` on a tenant
  handle, which is what a sixth parser defect would amount to, and assert SQLite itself refuses an
  assignment the tenant may not make — and that the value it would have changed is unchanged.
  Before the `fathom_udf` authorizer (native/fathom_udf/src/pragma_guard.rs) every refusal below
  was a successful assignment.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection
  alias Fathom.Shard.Extension

  setup do
    assert Extension.available?(),
           "the fathom_udf extension is not built — the engine PRAGMA guard cannot be tested"

    dir =
      Path.join(System.tmp_dir!(), "fathom_pragma_guard_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    path = Path.join(dir, "shard.db")

    # A real shard file with a table, made by a non-tenant (trusted) handle.
    {:ok, conn} = Connection.open(path)
    :ok = Connection.exec(conn, "CREATE TABLE t(a)")
    Connection.close(conn)

    on_exit(fn ->
      Application.delete_env(:fathom, :tenant_pragma_allow)
      Application.delete_env(:fathom, :sqlite_extension)
      File.rm_rf!(dir)
    end)

    %{path: path, dir: dir}
  end

  defp open!(path, opts) do
    {:ok, conn} = Connection.open(path, opts)
    on_exit(fn -> Connection.close(conn) end)
    conn
  end

  defp read!(conn, name) do
    {:ok, value} = Connection.pragma(conn, name)
    value
  end

  defp refused?(result), do: match?({:error, _}, result) and inspect(result) =~ "not authorized"

  describe "a tenant handle, with the text gate bypassed" do
    # Each pair is an assignment that defeats a protective setting, in a spelling the engine sees
    # however the text was written. `hard_heap_limit` takes a huge value so that, if this ever
    # regresses, the process-global limit it leaves behind is harmless to the rest of the suite.
    @protective [
      {"max_page_count", "PRAGMA max_page_count=7"},
      {"max_page_count", "PRAGMA main . max_page_count = 7"},
      {"synchronous", "PRAGMA synchronous=OFF"},
      {"synchronous", "PRAGMA synchronous /*;*/ = OFF"},
      {"synchronous", "EXPLAIN PRAGMA synchronous=OFF"},
      {"synchronous", "PRAGMA \"synchronous\"(0)"},
      {"journal_mode", "PRAGMA journal_mode=DELETE"},
      {"writable_schema", "PRAGMA writable_schema=ON"},
      {"trusted_schema", "PRAGMA trusted_schema=ON"},
      {"cache_size", "PRAGMA cache_size=-2000000"},
      {"temp_store", "PRAGMA temp_store=MEMORY"},
      {"busy_timeout", "PRAGMA busy_timeout=600000"},
      {"hard_heap_limit", "PRAGMA hard_heap_limit=4611686018427387904"}
    ]

    for scope <- [:rw, :ro] do
      test "the engine refuses protective PRAGMA assignments (#{scope})", %{path: path} do
        conn = open!(path, tenant?: true, scope: unquote(scope))

        for {name, sql} <- @protective do
          before = read!(conn, name)
          result = Connection.query(conn, sql, [])

          assert refused?(result),
                 "#{inspect(sql)} was not refused by the engine on a #{unquote(scope)} tenant " <>
                   "handle: #{inspect(result)}"

          assert read!(conn, name) == before, "#{inspect(sql)} changed #{name}"
        end
      end
    end

    test "a :ro handle cannot switch query_only off at the engine", %{path: path} do
      conn = open!(path, tenant?: true, scope: :ro)
      assert refused?(Connection.query(conn, "PRAGMA query_only=OFF", []))
    end

    test "allowed assignments and every read still work", %{path: path} do
      conn = open!(path, tenant?: true, scope: :rw)

      assert {:ok, _} = Connection.query(conn, "PRAGMA foreign_keys=OFF", [])
      assert read!(conn, "foreign_keys") == 0
      assert {:ok, _} = Connection.query(conn, "PRAGMA main.foreign_keys = ON", [])
      assert read!(conn, "foreign_keys") == 1
      assert {:ok, _} = Connection.query(conn, "PRAGMA legacy_alter_table=ON", [])
      assert read!(conn, "legacy_alter_table") == 1
      assert {:ok, _} = Connection.query(conn, "PRAGMA user_version=7", [])
      assert read!(conn, "user_version") == 7

      assert {:ok, %{rows: [[0, "a" | _]]}} = Connection.query(conn, "PRAGMA table_info(t)", [])
      assert {:ok, %{rows: [[_]]}} = Connection.query(conn, "PRAGMA max_page_count", [])
      assert {:ok, %{rows: [[_]]}} = Connection.query(conn, "PRAGMA writable_schema", [])
    end

    # Expert review 2026-10-10 #33b: the text gate (2220284) limits tenant `wal_checkpoint` to
    # PASSIVE, but the engine guard admitted every mode, so a sixth text-parser defect would let a
    # tenant loop TRUNCATE/RESTART/FULL (they wait on other connections and stall co-resident writers).
    for scope <- [:rw, :ro] do
      test "wal_checkpoint is PASSIVE-or-bare only at the engine (#{scope})", %{path: path} do
        conn = open!(path, tenant?: true, scope: unquote(scope))

        for sql <- [
              "PRAGMA wal_checkpoint(TRUNCATE)",
              "PRAGMA wal_checkpoint(RESTART)",
              "PRAGMA wal_checkpoint(FULL)",
              "PRAGMA wal_checkpoint(full)",
              "PRAGMA wal_checkpoint = truncate",
              "PRAGMA main.wal_checkpoint(FULL)",
              "PRAGMA main . wal_checkpoint ( RESTART )",
              "PRAGMA \"wal_checkpoint\"(TRUNCATE)",
              "PRAGMA wal_checkpoint('TRUNCATE')",
              "PRAGMA wal_checkpoint(\"restart\")",
              "PRAGMA wal_checkpoint /*;*/ (TRUNCATE)",
              "EXPLAIN PRAGMA wal_checkpoint(TRUNCATE)"
            ] do
          result = Connection.query(conn, sql, [])
          assert refused?(result), "#{inspect(sql)} was not refused: #{inspect(result)}"
        end

        for sql <- [
              "PRAGMA wal_checkpoint",
              "PRAGMA wal_checkpoint(PASSIVE)",
              "PRAGMA wal_checkpoint(passive)",
              "PRAGMA main.wal_checkpoint(PASSIVE)",
              "PRAGMA wal_checkpoint('Passive')"
            ] do
          assert {:ok, _} = Connection.query(conn, sql, []), sql
        end
      end
    end

    test "ATTACH, DETACH and VACUUM INTO stay refused (the one authorizer slot)", %{
      path: path,
      dir: dir
    } do
      conn = open!(path, tenant?: true, scope: :rw)

      victim = Path.join(dir, "victim.db")
      assert refused?(Connection.query(conn, "ATTACH DATABASE '#{victim}' AS v", []))
      assert {:error, _} = Connection.query(conn, "DETACH DATABASE v", [])

      snap = Path.join(dir, "snap.db")
      assert {:error, reason} = Connection.query(conn, "VACUUM INTO '#{snap}'", [])
      assert inspect(reason) =~ "authorization denied"
      refute File.exists?(snap)
    end

    test "a tenant cannot re-install or widen the guard", %{path: path} do
      conn = open!(path, tenant?: true, scope: :rw)

      assert {:ok, %{rows: [[0]]}} =
               Connection.query(conn, "SELECT fathom_pragma_guard('synchronous', '')", [])

      assert refused?(Connection.query(conn, "PRAGMA synchronous=OFF", []))
      assert read!(conn, "synchronous") == 2
    end

    test "the operator's :tenant_pragma_allow reaches the engine, but not past the deny list",
         %{path: path} do
      Application.put_env(:fathom, :tenant_pragma_allow, ["max_page_count", "writable_schema"])
      conn = open!(path, tenant?: true, scope: :rw)

      assert {:ok, _} = Connection.query(conn, "PRAGMA max_page_count=1000000", [])
      assert read!(conn, "max_page_count") == 1_000_000
      assert refused?(Connection.query(conn, "PRAGMA writable_schema=ON", []))
    end

    test "fathom's own post-open pragmas still run: pooled reuse and the TEMP cap", %{path: path} do
      rw = open!(path, tenant?: true, scope: :rw)
      assert :ok = Connection.reset_for_reuse(rw, :rw)
      assert :ok = Connection.cap_temp(rw)

      ro = open!(path, tenant?: true, scope: :ro)
      assert :ok = Connection.reset_for_reuse(ro, :ro)

      # The TEMP cap's value is pinned to the temp schema: it must not move MAIN's cap.
      {:ok, %{rows: [[temp_cap]]}} = Connection.query(rw, "PRAGMA temp.max_page_count", [])
      before = read!(rw, "max_page_count")
      assert refused?(Connection.query(rw, "PRAGMA max_page_count=#{temp_cap}", []))
      assert read!(rw, "max_page_count") == before
    end

    test "a real Django migrate's statements run on a tenant handle", %{path: path} do
      conn = open!(path, tenant?: true, scope: :rw)

      # Django's recorder creates its ledger in autocommit, outside any captured migration.
      assert {:ok, _} =
               Connection.query(
                 conn,
                 ~s|CREATE TABLE "django_migrations" ("id" integer NOT NULL PRIMARY KEY | <>
                   ~s|AUTOINCREMENT, "app" varchar(255) NOT NULL, "name" varchar(255) NOT NULL, | <>
                   ~s|"applied" datetime NOT NULL)|,
                 []
               )

      versions =
        "test/support/fixtures/django_migrate_capture.json"
        |> File.read!()
        |> Jason.decode!()
        |> Enum.sort_by(& &1["version"])

      # Django's schema editor wraps every migration in these; the capture drops them, so they are
      # replayed around it, plus the introspection reads (with an argument) that `migrate` sends.
      prelude = ["PRAGMA foreign_keys = OFF", "PRAGMA legacy_alter_table = ON", "BEGIN"]
      postlude = ["COMMIT", "PRAGMA legacy_alter_table = OFF", "PRAGMA foreign_keys = ON"]

      for v <- versions do
        args =
          (v["statement_args"] || [])
          |> Enum.map(fn %{"args" => list} -> Enum.map(list, &Filo.Value.decode/1) end)

        steps =
          Enum.map(prelude, &{&1, []}) ++
            Enum.zip(v["statements"], args) ++ Enum.map(postlude, &{&1, []})

        for {sql, bound} <- steps do
          assert {:ok, _} = Connection.query(conn, sql, bound),
                 "Django statement refused on a tenant handle: #{sql}"
        end
      end

      for sql <- [
            ~s|PRAGMA table_info("finance_account")|,
            ~s|PRAGMA index_list("finance_account")|,
            ~s|PRAGMA foreign_key_list("finance_category")|,
            "PRAGMA foreign_key_check"
          ] do
        assert {:ok, _} = Connection.query(conn, sql, []), sql
      end
    end
  end

  describe "handles the guard must never be on" do
    test "a non-tenant handle keeps VACUUM INTO and fathom's own pragmas", %{path: path, dir: dir} do
      conn = open!(path, [])

      assert :ok = Connection.exec(conn, "PRAGMA synchronous=OFF")
      assert read!(conn, "synchronous") == 0

      snap = Path.join(dir, "snap.db")
      assert :ok = Connection.exec(conn, "VACUUM INTO '#{snap}'")
      assert File.exists?(snap)
    end
  end

  describe "without the extension" do
    test "a tenant handle keeps exqlite's ATTACH/DETACH authorizer", %{path: path, dir: dir} do
      Application.put_env(:fathom, :sqlite_extension, false)
      conn = open!(path, tenant?: true, scope: :rw)

      victim = Path.join(dir, "victim.db")
      assert {:error, _} = Connection.query(conn, "ATTACH DATABASE '#{victim}' AS v", [])
      # No engine pragma guard here: the text gate is the only one, as before tier (b).
      assert {:ok, _} = Connection.query(conn, "PRAGMA synchronous=FULL", [])
    end
  end
end
