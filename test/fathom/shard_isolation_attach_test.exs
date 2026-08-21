defmodule Fathom.ShardIsolationAttachTest do
  @moduledoc """
  The cross-tenant isolation gate (expert review 2026-08-01 #1, #7, #8).

  ## The symptom these tests exist for

  Every shard is a sibling file in one flat directory, and the per-stream connection was
  opened with no SQLite authorizer. So one statement from any authenticated stream —

      ATTACH DATABASE '<data_dir>/<victim>.db' AS v

  — gave a tenant full read AND write access to every co-resident tenant's database. The
  victim's path did not even have to be guessed: `PRAGMA database_list` returns the
  attacker's own absolute path, and the victim is its sibling. Two independent expert panels
  found this and both confirmed it by execution: read victim PII, then wrote to the victim.

  It bypassed every other control fathom has — the `:ro` token scope, `:block_tenant_ddl`,
  `Fathom.Shard.WriteFence`, and the single-writer lease (the victim's coordinator holds the
  lease; the attacker's connection never consults it). AGENTS.md calls a cross-tenant leak a
  release blocker, so these are gate tests, not regression trivia.

  `ATTACH` also *creates* its target, which is the second half: it plants a structurally
  valid `.db` for a tenant that has never been opened, and cold-open trusts a present local
  file as a warm restart (finding #2). `VACUUM INTO '<path>'` reaches the same primitive
  without `ATTACH`, and survives a read-only handle, so it is gated too.

  Not async: shards are addressed by a global Registry and back onto real files.
  """
  use ExUnit.Case, async: false

  alias Fathom.{Shard, ShardExecutor, Shards}
  alias Filo.{Error, Stmt, StmtResult}

  setup do
    victim = "iso_victim_#{System.unique_integer([:positive])}"
    attacker = "iso_attacker_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      for id <- [victim, attacker] do
        Shards.drain(id, 2_000)
        rm_shard_files(id)
      end
    end)

    # Seed the victim with data worth stealing, through the normal path, then drain it so the
    # attacker is not merely racing a live coordinator.
    {:ok, v} = ShardExecutor.open(victim)
    {:ok, _} = ShardExecutor.execute(v, stmt("CREATE TABLE secrets (x TEXT)"))
    {:ok, _} = ShardExecutor.execute(v, stmt("INSERT INTO secrets VALUES (?)", ["victim-PII"]))
    :ok = ShardExecutor.close(v)

    %{victim: victim, attacker: attacker, victim_path: db_path(victim)}
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  defp db_path(id), do: Path.join(Shard.data_dir(), "#{id}.db")

  defp rm_shard_files(id) do
    for base <- [db_path(id), Path.join(Fathom.Shard.Storage.Local.dir(), "#{id}.db")],
        suffix <- ["", "-wal", "-shm"] do
      File.rm(base <> suffix)
    end
  end

  describe "ATTACH — the cross-tenant breach" do
    test "a tenant cannot ATTACH a co-resident tenant's shard file", ctx do
      {:ok, a} = ShardExecutor.open(ctx.attacker)

      assert {:error, %Error{}} =
               ShardExecutor.execute(a, stmt("ATTACH DATABASE '#{ctx.victim_path}' AS v"))

      # And therefore cannot read through it.
      assert {:error, %Error{}} = ShardExecutor.execute(a, stmt("SELECT x FROM v.secrets"))

      :ok = ShardExecutor.close(a)
    end

    test "the victim's rows never reach the attacker's stream", ctx do
      {:ok, a} = ShardExecutor.open(ctx.attacker)
      _ = ShardExecutor.execute(a, stmt("ATTACH DATABASE '#{ctx.victim_path}' AS v"))

      # Whatever the error shape, no statement on this stream may return victim data.
      for sql <- [
            "SELECT x FROM v.secrets",
            "SELECT x FROM secrets",
            "SELECT name FROM v.sqlite_master"
          ] do
        case ShardExecutor.execute(a, stmt(sql)) do
          {:error, %Error{}} ->
            :ok

          {:ok, %StmtResult{rows: rows}} ->
            refute Enum.any?(List.flatten(rows), &(&1 == "victim-PII")),
                   "cross-tenant read leaked through #{inspect(sql)}"
        end
      end

      :ok = ShardExecutor.close(a)
    end

    test "a tenant cannot WRITE to a co-resident tenant's shard file", ctx do
      {:ok, a} = ShardExecutor.open(ctx.attacker)
      _ = ShardExecutor.execute(a, stmt("ATTACH DATABASE '#{ctx.victim_path}' AS v"))

      assert {:error, %Error{}} =
               ShardExecutor.execute(a, stmt("INSERT INTO v.secrets VALUES ('PWNED')"))

      :ok = ShardExecutor.close(a)

      # The victim's data is untouched, read back through its own stream.
      {:ok, v} = ShardExecutor.open(ctx.victim)

      assert {:ok, %StmtResult{rows: rows}} =
               ShardExecutor.execute(v, stmt("SELECT x FROM secrets"))

      assert rows == [["victim-PII"]]
      :ok = ShardExecutor.close(v)
    end

    # THIS TEST PASSES FOR A REASON OTHER THAN THE ONE IT NAMES, and that is worth saying out loud
    # (expert review 2026-08-20 #37). It was the ONLY isolation assertion on `execute_sequence/2`,
    # and its green comes from the SQLite AUTHORIZER refusing the ATTACH, not from the statement
    # gate -- which #18 then showed `execute_sequence/2` never called at all. A test whose green
    # depends on a mechanism other than the one it appears to cover is worse than no test: it is
    # exactly why the `sequence` bypass survived a review pass that explicitly hardened this path.
    #
    # Kept, because "ATTACH must be refused on the sequence path" is a real invariant however it is
    # enforced. The GATE's own coverage of that path is the describe block below, whose cases were
    # confirmed to fail against the pre-#18 code.
    test "ATTACH is blocked inside a multi-statement script (the execute_sequence path)", ctx do
      {:ok, a} = ShardExecutor.open(ctx.attacker)

      assert {:error, %Error{}} =
               ShardExecutor.execute_sequence(
                 a,
                 "CREATE TABLE t(a); ATTACH DATABASE '#{ctx.victim_path}' AS v;"
               )

      :ok = ShardExecutor.close(a)
    end

    test "ATTACH cannot be smuggled past the gate by a comment prefix or casing", ctx do
      {:ok, a} = ShardExecutor.open(ctx.attacker)

      for sql <- [
            "/* c */ ATTACH DATABASE '#{ctx.victim_path}' AS v",
            "-- c\nATTACH DATABASE '#{ctx.victim_path}' AS v",
            "attach database '#{ctx.victim_path}' as v",
            "  \n\t ATTACH DATABASE '#{ctx.victim_path}' AS v"
          ] do
        assert {:error, %Error{}} = ShardExecutor.execute(a, stmt(sql)),
               "ATTACH slipped through as #{inspect(sql)}"
      end

      :ok = ShardExecutor.close(a)
    end
  end

  describe "VACUUM — the file-planting / arbitrary-write primitive" do
    test "VACUUM INTO cannot write a file, including one that plants a shard", ctx do
      planted = db_path("iso_planted_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm(planted) end)

      {:ok, a} = ShardExecutor.open(ctx.attacker)

      assert {:error, %Error{}} = ShardExecutor.execute(a, stmt("VACUUM INTO '#{planted}'"))
      refute File.exists?(planted), "VACUUM INTO planted a database file"

      assert {:error, %Error{}} = ShardExecutor.execute(a, stmt("VACUUM"))

      :ok = ShardExecutor.close(a)
    end

    test "a read-only token cannot VACUUM INTO either", ctx do
      # VACUUM INTO survives a read-only SQLite handle, so it needs its own gate.
      planted =
        Path.join(System.tmp_dir!(), "iso_ro_vacuum_#{System.unique_integer([:positive])}.db")

      on_exit(fn -> File.rm(planted) end)

      {:ok, a} = ShardExecutor.open(ctx.attacker, :ro)
      assert {:error, %Error{}} = ShardExecutor.execute(a, stmt("VACUUM INTO '#{planted}'"))
      refute File.exists?(planted)
      :ok = ShardExecutor.close(a)
    end
  end

  # THE `sequence` PATH SKIPPED THE GATE ENTIRELY (expert review 2026-08-20 #18, verified by
  # execution against this project's own exqlite 0.37.0). execute_sequence/2 checked only
  # `ddl?(sql)` — a LEADING-keyword test, so on a multi-statement script it inspected the first
  # statement and nothing else — and never called `blocked_statement/1` at all. Measured:
  #
  #     "SELECT 1; PRAGMA synchronous=OFF; PRAGMA max_page_count=777; CREATE TABLE evil(a);"
  #     #=> :ok — synchronous 0, max_page_count 777, table `evil` created
  #
  # The existing isolation test on this path asserts only that ATTACH is refused inside a script,
  # which the SQLite AUTHORIZER provides, not the gate — so it reads as coverage of a gate it
  # never exercises. Pragmas have no engine backstop, which is why the half with no second layer
  # was exactly the half `sequence` skipped.
  describe "a script is gated statement by statement (#18)" do
    test "a protective pragma hidden after a harmless first statement is refused", ctx do
      {:ok, a} = ShardExecutor.open(ctx.attacker)

      for script <- [
            "SELECT 1; PRAGMA synchronous=OFF",
            "SELECT 1; PRAGMA max_page_count=777",
            "SELECT 1;\nPRAGMA wal_autocheckpoint=0;\nSELECT 2",
            "/* lead */ SELECT 1; PRAGMA journal_mode=DELETE"
          ] do
        assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
                 ShardExecutor.execute_sequence(a, script),
               "#{inspect(script)} was NOT refused — every protective pragma is settable through " <>
                 "executescript()"
      end

      # And the durability setting really did survive.
      assert {:ok, %{rows: [[2]]}} = ShardExecutor.execute(a, stmt("PRAGMA synchronous"))

      :ok = ShardExecutor.close(a)
    end

    test "a semicolon inside a string literal does not hide the statement after it", ctx do
      {:ok, a} = ShardExecutor.open(ctx.attacker)
      {:ok, _} = ShardExecutor.execute(a, stmt("CREATE TABLE s (v TEXT)"))

      assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
               ShardExecutor.execute_sequence(
                 a,
                 "INSERT INTO s VALUES ('a;b'); PRAGMA synchronous=OFF"
               ),
             "a ';' inside a string literal made the scanner miss the statement after it"

      # The doubling escape, which is the one that turns a scanner inside-out for the whole script.
      assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
               ShardExecutor.execute_sequence(
                 a,
                 "INSERT INTO s VALUES ('it''s; fine'); PRAGMA max_page_count=1"
               )

      :ok = ShardExecutor.close(a)
    end

    test "an ordinary multi-statement script still runs", ctx do
      {:ok, a} = ShardExecutor.open(ctx.attacker)

      assert :ok =
               ShardExecutor.execute_sequence(
                 a,
                 "CREATE TABLE ok1 (v TEXT); INSERT INTO ok1 VALUES ('x;y'); INSERT INTO ok1 VALUES ('z')"
               ),
             "the gate over-refused a legitimate script — including one whose data contains a " <>
               "semicolon, which is the cost of getting the splitter wrong in the safe direction"

      assert {:ok, %{rows: [[2]]}} = ShardExecutor.execute(a, stmt("SELECT count(*) FROM ok1"))

      :ok = ShardExecutor.close(a)
    end
  end

  describe "PRAGMA — fathom's own safety mechanisms are not tenant-settable" do
    test "the protective pragmas are refused", ctx do
      {:ok, a} = ShardExecutor.open(ctx.attacker)

      for sql <- [
            "PRAGMA journal_mode=DELETE",
            "PRAGMA synchronous=OFF",
            "PRAGMA max_page_count=999999999",
            "PRAGMA wal_autocheckpoint=0",
            "PRAGMA query_only=OFF",
            "PRAGMA locking_mode=EXCLUSIVE",
            "PRAGMA page_size=65536",
            "PRAGMA temp_store_directory='/tmp'",
            "PRAGMA writable_schema=ON",
            "PRAGMA main.journal_mode=DELETE",
            "/* c */ PRAGMA synchronous=OFF",
            "PRAGMA journal_mode(DELETE)"
          ] do
        assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
                 ShardExecutor.execute(a, stmt(sql)),
               "#{inspect(sql)} was not refused"
      end

      :ok = ShardExecutor.close(a)
    end

    # THE PADDED FORMS (expert review 2026-08-20 #19, verified by execution against this project's
    # own exqlite 0.37.0 with the tenant authorizer set). The gate used to parse a fixed
    # `String.slice(6, 200)` window and read "no `=` and no `(`" as a bare read — so insignificant
    # whitespace between PRAGMA and the name pushed the `=` past the window and the assignment
    # executed. The list above pins every one of these strings UNPADDED, which is why the suite
    # reported full coverage of a gate with a hole in it.
    test "padding cannot push the assignment out of view", ctx do
      {:ok, a} = ShardExecutor.open(ctx.attacker)

      names = [
        "synchronous",
        "max_page_count",
        "wal_autocheckpoint",
        "journal_mode",
        "locking_mode",
        "query_only",
        "writable_schema"
      ]

      # 210 is just past the old 200-char window; 5_000 is well past any window at all. Tabs and
      # newlines count as whitespace to SQLite exactly as spaces do.
      for name <- names, pad <- [210, 5_000], ws <- [" ", "\t", "\n"] do
        sql = "PRAGMA" <> String.duplicate(ws, pad) <> "#{name}=1"

        assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
                 ShardExecutor.execute(a, stmt(sql)),
               "PRAGMA padded with #{pad} #{inspect(ws)} before #{name} was NOT refused — the " <>
                 "protective pragma is settable by any tenant with a :rw token"
      end

      # And a bare READ of the same names is still allowed, padded or not: the allowlist governs
      # SETTING, and over-refusing reads would break Django's introspection.
      for name <- ["table_info", "foreign_key_list", "synchronous"], pad <- [1, 210] do
        refute match?(
                 {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}},
                 ShardExecutor.execute(a, stmt("PRAGMA" <> String.duplicate(" ", pad) <> name))
               ),
               "a bare PRAGMA read of #{name} was refused; the gate governs assignment only"
      end

      :ok = ShardExecutor.close(a)
    end

    test "the connection's durability settings actually survive the attempt", ctx do
      {:ok, a} = ShardExecutor.open(ctx.attacker)

      _ = ShardExecutor.execute(a, stmt("PRAGMA synchronous=OFF"))
      _ = ShardExecutor.execute(a, stmt("PRAGMA journal_mode=DELETE"))

      # 2 == FULL. The point of the gate is the *effect*, not the error.
      assert {:ok, %StmtResult{rows: [[2]]}} =
               ShardExecutor.execute(a, stmt("PRAGMA synchronous"))

      assert {:ok, %StmtResult{rows: [["wal"]]}} =
               ShardExecutor.execute(a, stmt("PRAGMA journal_mode"))

      :ok = ShardExecutor.close(a)
    end

    test "reads and the pragmas a real client needs still work", ctx do
      {:ok, a} = ShardExecutor.open(ctx.attacker)

      assert {:ok, _} = ShardExecutor.execute(a, stmt("CREATE TABLE t (a INTEGER, b TEXT)"))
      # Django's SQLite backend sets these; blocking them would break an unchanged client.
      assert {:ok, _} = ShardExecutor.execute(a, stmt("PRAGMA foreign_keys=ON"))
      assert {:ok, _} = ShardExecutor.execute(a, stmt("PRAGMA legacy_alter_table=ON"))
      assert {:ok, _} = ShardExecutor.execute(a, stmt("PRAGMA defer_foreign_keys=ON"))
      assert {:ok, _} = ShardExecutor.execute(a, stmt("PRAGMA cache_size=-2000"))
      # Read forms are never gated.
      assert {:ok, %StmtResult{}} = ShardExecutor.execute(a, stmt("PRAGMA table_info(t)"))
      assert {:ok, %StmtResult{}} = ShardExecutor.execute(a, stmt("PRAGMA foreign_key_list(t)"))
      assert {:ok, %StmtResult{}} = ShardExecutor.execute(a, stmt("PRAGMA database_list"))

      :ok = ShardExecutor.close(a)
    end

    test "a rw stream may still stamp fathom's own version pragmas — deliberately", ctx do
      # NOT an oversight in the allow-list. Stamping user_version through the data path is a
      # documented capability (review #15 fixed it being lost on an idle drop, and
      # shard_durability_test.exs pins the round trip). The hole finding #7 named was a
      # READ-ONLY credential rewriting it, and that is closed by the readonly handle — see the
      # ":ro scope" describe block, which asserts the same statement is refused there.
      {:ok, a} = ShardExecutor.open(ctx.attacker)

      assert {:ok, _} = ShardExecutor.execute(a, stmt("PRAGMA user_version = 42"))

      assert {:ok, %StmtResult{rows: [[42]]}} =
               ShardExecutor.execute(a, stmt("PRAGMA user_version"))

      :ok = ShardExecutor.close(a)
    end

    test ":tenant_pragma_allow widens the list without a release", ctx do
      prev = Application.get_env(:fathom, :tenant_pragma_allow, [])
      on_exit(fn -> Application.put_env(:fathom, :tenant_pragma_allow, prev) end)

      {:ok, a} = ShardExecutor.open(ctx.attacker)

      assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
               ShardExecutor.execute(a, stmt("PRAGMA secure_delete=ON"))

      Application.put_env(:fathom, :tenant_pragma_allow, ["secure_delete"])
      assert {:ok, _} = ShardExecutor.execute(a, stmt("PRAGMA secure_delete=ON"))

      :ok = ShardExecutor.close(a)
    end
  end

  describe ":ro scope — SQLite enforces it, not a keyword prefix" do
    test "the four verified classifier bypasses are all refused", ctx do
      {:ok, ro} = ShardExecutor.open(ctx.victim, :ro)

      # Reads still work — this is a read-only token, not a broken one.
      assert {:ok, %StmtResult{rows: [["victim-PII"]]}} =
               ShardExecutor.execute(ro, stmt("SELECT x FROM secrets"))

      for sql <- [
            "/* c */ INSERT INTO secrets VALUES ('x')",
            "-- c\nINSERT INTO secrets VALUES ('x')",
            "WITH q AS (SELECT 1) INSERT INTO secrets SELECT 'x' FROM q",
            "WITH q AS (SELECT 1) DELETE FROM secrets",
            "PRAGMA user_version = 99",
            "/* c */ CREATE TABLE evil (a)",
            "INSERT INTO secrets VALUES ('plain')"
          ] do
        assert {:error, %Error{}} = ShardExecutor.execute(ro, stmt(sql)),
               "read-only scope did not refuse #{inspect(sql)}"
      end

      :ok = ShardExecutor.close(ro)

      # Nothing landed.
      {:ok, v} = ShardExecutor.open(ctx.victim)

      assert {:ok, %StmtResult{rows: [["victim-PII"]]}} =
               ShardExecutor.execute(v, stmt("SELECT x FROM secrets"))

      assert {:ok, %StmtResult{rows: [[0]]}} =
               ShardExecutor.execute(v, stmt("PRAGMA user_version"))

      :ok = ShardExecutor.close(v)
    end

    test "a read-only stream cannot ATTACH a neighbour either", ctx do
      {:ok, ro} = ShardExecutor.open(ctx.attacker, :ro)

      assert {:error, %Error{}} =
               ShardExecutor.execute(ro, stmt("ATTACH DATABASE '#{ctx.victim_path}' AS v"))

      :ok = ShardExecutor.close(ro)
    end
  end

  describe ":block_tenant_ddl is not defeated by a comment prefix" do
    setup do
      prev = Application.get_env(:fathom, :block_tenant_ddl, false)
      Application.put_env(:fathom, :block_tenant_ddl, true)
      on_exit(fn -> Application.put_env(:fathom, :block_tenant_ddl, prev) end)
      :ok
    end

    test "a commented CREATE is still classified as DDL", ctx do
      {:ok, a} = ShardExecutor.open(ctx.attacker)

      for sql <- [
            "CREATE TABLE plain (a)",
            "/* c */ CREATE TABLE commented (a)",
            "-- c\nCREATE TABLE dashed (a)",
            "/* a */ /* b */ DROP TABLE whatever"
          ] do
        assert {:error, %Error{code: "FILO_DDL_BLOCKED"}} =
                 ShardExecutor.execute(a, stmt(sql)),
               "#{inspect(sql)} was not classified as DDL"
      end

      :ok = ShardExecutor.close(a)
    end
  end

  describe "fathom's own connections keep the capabilities the authorizer denies tenants" do
    test "an internal Connection can still VACUUM INTO — the durability snapshot depends on it",
         ctx do
      # SQLite implements VACUUM INTO as an internal ATTACH, so denying :attach also denies
      # VACUUM INTO. Fathom.Shard.snapshot/2 IS a VACUUM INTO on a Connection, so setting the
      # authorizer unconditionally in Connection.open/1 would have broken every durability
      # flush. This test pins the boundary: tenant handles restricted, fathom's own not.
      dest = Path.join(System.tmp_dir!(), "iso_snap_#{System.unique_integer([:positive])}.db")
      on_exit(fn -> File.rm(dest) end)

      {:ok, conn} = Fathom.Shard.Connection.open(db_path(ctx.victim))

      assert {:ok, _} =
               Fathom.Shard.Connection.query(conn, "VACUUM INTO '#{dest}'", [])

      assert File.exists?(dest)
      :ok = Fathom.Shard.Connection.close(conn)
    end
  end
end
