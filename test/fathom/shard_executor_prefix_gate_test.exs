defmodule Fathom.ShardExecutorPrefixGateTest do
  @moduledoc """
  Statement-prefix corpus against the executor's authorization gates (expert review 2026-09-05
  #1, #2, #32).

  SQLite's parser skips leading whitespace, both SQL comment forms, and EMPTY statements (a bare
  `;` — `ecmd ::= SEMI` in its grammar) before the statement it actually runs, and it executes a
  statement's PARSE-TIME pragmas even under EXPLAIN. So every protective gate in `ShardExecutor`
  must see through those prefixes, or a tenant re-opens the knobs the allow-list exists to close.

  Four successive parser defects have shipped here (the `String.slice(6, 200)` window in
  2026-08-20 #19, the `main . name` split in 2026-08-24 #1, the leading `;` in 2026-09-05 #1, and
  EXPLAIN's parse-time pragmas in 2026-09-05 #2), each found by an audit rather than a test. This
  is the table-driven corpus that pins them: every protective pragma, wrapped in every prefix
  SQLite parses through, run through all the gate's entry points.

  These rows FAIL against the pre-fix tree — the `;` and EXPLAIN rows returned `{:ok, _}` (verified
  by stashing `lib/` and re-running). The fix routes every head classifier through one
  `strip_lead_noise/1` and makes `blocked_statement/1` transparent to EXPLAIN.
  """
  # Not async: shards are addressed by a global Registry and back onto files.
  use ExUnit.Case, async: false

  alias Fathom.ShardExecutor
  alias Filo.{Error, Stmt}

  setup do
    shard = "test_prefix_#{System.unique_integer([:positive])}"
    # open/1 with auth disabled (test default) yields a :rw TENANT handle (tenant?: true) — the
    # exact shape the panel's probe used.
    {:ok, handle} = ShardExecutor.open(shard)

    on_exit(fn ->
      ShardExecutor.close(handle)
      rm_shard_files(shard)
    end)

    %{shard: shard, handle: handle}
  end

  defp stmt(sql), do: %Stmt{sql: sql}

  defp rm_shard_files(id) do
    local = Path.join([Fathom.Shard.data_dir(), "#{id}.db"])
    remote = Path.join([Fathom.Shard.Storage.Local.dir(), "#{id}.db"])
    for base <- [local, remote], suffix <- ["", "-wal", "-shm"], do: File.rm(base <> suffix)
  end

  # Protective PRAGMA assignments a tenant must never reach. Each is fathom's own safety
  # mechanism: max_page_count is the size cap, synchronous/journal_mode are durability,
  # locking_mode/wal_autocheckpoint are noisy-neighbour levers against the coordinator's flush.
  @blocked_pragmas [
    "PRAGMA max_page_count=777",
    "PRAGMA synchronous=OFF",
    "PRAGMA journal_mode=DELETE",
    "PRAGMA locking_mode=EXCLUSIVE",
    "PRAGMA wal_autocheckpoint=0",
    # The engine-hardening floor (#3): a tenant must not turn these back off.
    "PRAGMA writable_schema=ON",
    "PRAGMA trusted_schema=ON"
  ]

  # Prefixes SQLite's parser sees through (each a literal string prepended to a statement). The
  # key is that fathom's gate must see through them too.
  @prefixes [
    {"leading semicolon", ";"},
    {"double semicolon", ";;"},
    {"semicolon + comment + semicolon", "; /*c*/ ;"},
    {"form-feed then semicolon", "\f;"},
    {"vertical-tab then semicolon", "\v;"},
    {"leading block comment", "/* c */ "},
    {"EXPLAIN", "EXPLAIN "},
    {"EXPLAIN QUERY PLAN", "EXPLAIN QUERY PLAN "},
    {"comment then EXPLAIN", "/* c */ EXPLAIN "},
    {"semicolon then EXPLAIN", ";EXPLAIN "}
  ]

  test "every protective pragma is refused under every prefix SQLite parses through", %{
    handle: h
  } do
    for pragma <- @blocked_pragmas, {label, prefix} <- @prefixes do
      sql = prefix <> pragma

      assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
               ShardExecutor.execute(h, stmt(sql)),
             "#{label} prefix of `#{pragma}` reached the engine (`#{sql}`)"
    end
  end

  test "describe/2 shares the gate, so the same prefixes are refused there too", %{handle: h} do
    for sql <- [
          ";PRAGMA max_page_count=777",
          "EXPLAIN PRAGMA synchronous=OFF",
          "EXPLAIN QUERY PLAN PRAGMA journal_mode=DELETE"
        ] do
      assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
               ShardExecutor.describe(h, sql),
             "describe let `#{sql}` through"
    end
  end

  test "EXPLAIN of a blocked verb is refused (the inner statement is re-gated)", %{handle: h} do
    assert {:error, %Error{code: "FILO_STATEMENT_BLOCKED"}} =
             ShardExecutor.execute(h, stmt("EXPLAIN ATTACH DATABASE '/etc/passwd' AS x"))

    assert {:error, %Error{code: "FILO_STATEMENT_BLOCKED"}} =
             ShardExecutor.execute(h, stmt(";VACUUM INTO '/tmp/leak.db'"))
  end

  test "legitimate reads and allowed pragmas are NOT over-blocked — including under a prefix", %{
    handle: h
  } do
    # EXPLAIN of a real query still plans (EXPLAIN of a non-DML/non-pragma is transparent).
    assert {:ok, _} = ShardExecutor.execute(h, stmt("EXPLAIN SELECT 1"))
    assert {:ok, _} = ShardExecutor.execute(h, stmt("EXPLAIN QUERY PLAN SELECT 1"))
    assert {:ok, _} = ShardExecutor.describe(h, "SELECT 1")

    # A bare pragma read discloses only this connection's own config and is always allowed.
    assert {:ok, _} = ShardExecutor.execute(h, stmt("PRAGMA journal_mode"))

    # An allowed pragma assignment (Django issues foreign_keys) must pass — even with a leading
    # `;`, the gate must classify it as allowed, not blocked. (Whether exqlite executes the
    # `;`-prefixed form is not the gate's concern, so we only assert it is not gate-blocked.)
    refute match?(
             {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}},
             ShardExecutor.execute(h, stmt("PRAGMA foreign_keys=ON"))
           )

    refute match?(
             {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}},
             ShardExecutor.execute(h, stmt(";PRAGMA foreign_keys=ON"))
           )
  end

  test "the hardening deny-list beats a :tenant_pragma_allow override (#3)", %{handle: h} do
    # The deny is checked BEFORE extra_pragma_allow(), so even an operator misconfiguration that
    # named a hardening pragma in :tenant_pragma_allow cannot widen a tenant back into disabling
    # the engine floor. Pre-#3 (deny == [query_only]) this override WOULD allow it.
    prev = Application.get_env(:fathom, :tenant_pragma_allow, [])
    Application.put_env(:fathom, :tenant_pragma_allow, ["writable_schema", "trusted_schema"])
    on_exit(fn -> Application.put_env(:fathom, :tenant_pragma_allow, prev) end)

    for sql <- ["PRAGMA writable_schema=ON", "PRAGMA trusted_schema=ON"] do
      assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
               ShardExecutor.execute(h, stmt(sql)),
             "a :tenant_pragma_allow override let `#{sql}` disable the engine floor"
    end
  end

  test "leading-; and comment-prefixed DDL is refused under :block_tenant_ddl", %{shard: shard} do
    # block_ddl? is captured at OPEN (stream_opts/1), so set it before opening a fresh handle.
    prev = Application.get_env(:fathom, :block_tenant_ddl, false)
    Application.put_env(:fathom, :block_tenant_ddl, true)
    ddl_shard = "test_prefix_ddl_#{System.unique_integer([:positive])}"
    {:ok, h} = ShardExecutor.open(ddl_shard)

    on_exit(fn ->
      ShardExecutor.close(h)
      Application.put_env(:fathom, :block_tenant_ddl, prev)
      rm_shard_files(ddl_shard)
    end)

    for sql <- [
          ";CREATE TABLE semi (x)",
          "/* c */ ;CREATE INDEX i ON semi (x)",
          ";;DROP TABLE IF EXISTS semi"
        ] do
      assert {:error, %Error{code: "FILO_DDL_BLOCKED"}} =
               ShardExecutor.execute(h, stmt(sql)),
             "`#{sql}` bypassed :block_tenant_ddl"
    end

    _ = shard
  end

  # Expert review 2026-09-05 #21: PRAGMA user_version is fathom's own schema-version stamp and the
  # migrator's crash-forward signal. It stays allowed for :rw (a durability-tested capability), but a
  # tenant setting it can forge its convergence state — so under :block_tenant_ddl (the lever that
  # routes schema evolution through the migration engine) the ASSIGNMENT is refused. The bare read
  # stays allowed; the template is exempt.
  test "under :block_tenant_ddl a tenant cannot set PRAGMA user_version, but can still read it",
       %{
         shard: _shard
       } do
    prev = Application.get_env(:fathom, :block_tenant_ddl, false)
    Application.put_env(:fathom, :block_tenant_ddl, true)
    uv_shard = "test_uv_#{System.unique_integer([:positive])}"
    {:ok, h} = ShardExecutor.open(uv_shard)

    on_exit(fn ->
      ShardExecutor.close(h)
      Application.put_env(:fathom, :block_tenant_ddl, prev)
      rm_shard_files(uv_shard)
    end)

    for sql <- [
          "PRAGMA user_version = 5",
          "PRAGMA user_version=5",
          "PRAGMA main.user_version = 5",
          ";PRAGMA user_version = 5",
          "/* c */ PRAGMA user_version(5)"
        ] do
      assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
               ShardExecutor.execute(h, stmt(sql)),
             "`#{sql}` forged the schema-version stamp under :block_tenant_ddl"
    end

    # The bare read still works (it discloses only the current stamp).
    assert {:ok, _} = ShardExecutor.execute(h, stmt("PRAGMA user_version"))
  end

  test "with :block_tenant_ddl OFF, PRAGMA user_version stays settable (the durability capability)",
       %{shard: _shard} do
    prev = Application.get_env(:fathom, :block_tenant_ddl, false)
    Application.put_env(:fathom, :block_tenant_ddl, false)
    uv_shard = "test_uv_open_#{System.unique_integer([:positive])}"
    {:ok, h} = ShardExecutor.open(uv_shard)

    on_exit(fn ->
      ShardExecutor.close(h)
      Application.put_env(:fathom, :block_tenant_ddl, prev)
      rm_shard_files(uv_shard)
    end)

    # Not gate-blocked when DDL is not locked down — shard_durability_test pins this round trip.
    refute match?(
             {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}},
             ShardExecutor.execute(h, stmt("PRAGMA user_version = 5"))
           )
  end
end
