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
    # A leading UTF-8 BOM (U+FEFF): SQLite's tokenizer skips it and RUNS the statement, but
    # `String.trim_leading/1` does NOT strip it (U+FEFF is Unicode Cf, not White_Space), so every
    # gate keying on `strip_lead_noise/1` missed it and the pragma reached the engine. Verified by
    # execution: `<BOM>PRAGMA max_page_count=777` set the cap and `<BOM>CREATE TABLE` ran. Same
    # defect class as the leading-`;` bypass (expert review 2026-09-05 #1), one prefix later.
    {"byte-order mark", "﻿"},
    {"BOM then semicolon", "﻿;"},
    {"semicolon then BOM", ";﻿"},
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

  # Noise INSIDE the statement, at the token boundaries after the pragma name (expert review
  # 2026-10-08 #1). The corpus above only ever prepended noise; this defect sat between the name and
  # the `=`. `pragma_assignment?` looked at the tail's first non-blank byte (a comment there read as
  # "not an assignment") and `argumentish_tail?` cut at the first `;` with a plain split (a `;`
  # inside a comment hid the `= value`), so all of these reached the engine and RAN — verified by
  # execution, including from a `:ro` token.
  @inner_noise [
    {"block comment holding a semicolon", "/*;*/"},
    {"line comment holding a semicolon", "-- ;\n"},
    {"empty block comment", "/**/"},
    {"line comment", "--x\n"},
    {"tab", "\t"},
    {"newline", "\n"}
  ]

  # {pragma, value} pairs whose effect a later bare READ on the same connection can observe, so
  # the assertion is on the engine's state and not only on the gate's verdict.
  @inner_targets [
    {"max_page_count", "777"},
    {"synchronous", "0"},
    {"wal_autocheckpoint", "0"},
    {"cell_size_check", "0"}
  ]

  defp inner_variants(name, value) do
    for {label, n} <- @inner_noise do
      [
        {"#{label} before =", "PRAGMA #{name} #{n}= #{value}"},
        {"#{label} after =", "PRAGMA #{name} =#{n} #{value}"},
        {"#{label} before (", "PRAGMA #{name} #{n}(#{value})"},
        {"#{label} after schema dot", "PRAGMA main.#{n}#{name} #{n}= #{value}"},
        {"#{label} before the name", "PRAGMA #{n}#{name} = #{value}"}
      ]
    end
    |> List.flatten()
  end

  defp read_pragma(h, name) do
    {:ok, %Filo.StmtResult{rows: [[v]]}} = ShardExecutor.execute(h, stmt("PRAGMA #{name}"))
    v
  end

  test "noise inside the statement cannot turn an assignment into a 'bare read'", %{handle: h} do
    for {name, value} <- @inner_targets do
      before = read_pragma(h, name)

      for {label, sql} <- inner_variants(name, value) do
        assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
                 ShardExecutor.execute(h, stmt(sql)),
               "#{label}: `#{inspect(sql)}` reached the engine"

        assert read_pragma(h, name) == before,
               "#{label}: `#{inspect(sql)}` changed PRAGMA #{name}"
      end
    end
  end

  test "the sequence and describe paths share the inner-noise gate", %{handle: h} do
    before = read_pragma(h, "max_page_count")

    for sql <- [
          "PRAGMA max_page_count /*;*/ = 779",
          "SELECT 1; PRAGMA max_page_count -- ;\n = 779"
        ] do
      assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
               ShardExecutor.execute_sequence(h, sql),
             "sequence `#{inspect(sql)}` reached the engine"
    end

    # describe prepares only the first statement, so it gets the single-statement form.
    for sql <- ["PRAGMA max_page_count /*;*/ = 779", "PRAGMA /*;*/ max_page_count = 779"] do
      assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} = ShardExecutor.describe(h, sql),
             "describe `#{inspect(sql)}` was not refused"
    end

    assert read_pragma(h, "max_page_count") == before
  end

  # The worst instance: `hard_heap_limit` is PROCESS-GLOBAL in SQLite, so one tenant's stream set it
  # for every tenant on the node, and it can only ever be lowered. A `:ro` token was enough. The
  # value used here is huge on purpose, so that if this test ever runs against a broken gate it
  # does not starve the rest of the suite of memory — it still fails, on the other tenant's read.
  test "a :ro stream cannot set the process-global heap limit seen by another tenant", %{
    shard: shard
  } do
    other = "test_prefix_other_#{System.unique_integer([:positive])}"
    {:ok, ro} = ShardExecutor.open(shard, {:ro, nil})
    {:ok, oh} = ShardExecutor.open(other)

    on_exit(fn ->
      ShardExecutor.close(ro)
      ShardExecutor.close(oh)
      rm_shard_files(other)
    end)

    before = read_pragma(oh, "hard_heap_limit")

    for sql <- [
          "PRAGMA hard_heap_limit /*;*/ = 4611686018427387904",
          "PRAGMA soft_heap_limit -- ;\n = 4611686018427387904"
        ] do
      assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} = ShardExecutor.execute(ro, stmt(sql)),
             "a :ro stream ran `#{inspect(sql)}`"
    end

    assert read_pragma(oh, "hard_heap_limit") == before
  end

  # Expert review 2026-10-08 #8, measured: every stream is its own connection with its own page
  # cache, and `:shard_cache_size_kb` was only each one's starting value — a tenant raising it per
  # stream took node RSS from 174 MB to 922 MB with six streams. Any scope could. Refused now; the
  # bare read stays allowed.
  test "a tenant stream cannot raise its page cache past the configured ceiling", %{
    shard: shard,
    handle: h
  } do
    {:ok, ro} = ShardExecutor.open(shard, {:ro, nil})
    on_exit(fn -> ShardExecutor.close(ro) end)

    for handle <- [h, ro] do
      before = read_pragma(handle, "cache_size")

      for sql <- ["PRAGMA cache_size = -2000000", "PRAGMA main.cache_size(-2000000)"] do
        assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
                 ShardExecutor.execute(handle, stmt(sql)),
               "`#{sql}` raised a tenant stream's page cache"
      end

      assert read_pragma(handle, "cache_size") == before
    end
  end

  # Expert review 2026-10-08 #9: `temp_store=MEMORY` put a stream's TEMP tables — outside the shard
  # size cap — in node RAM (166 -> 584 MB RSS from four 100 MB inserts under a 50 MB cap).
  test "a tenant cannot move its TEMP storage into node memory", %{handle: h} do
    before = read_pragma(h, "temp_store")

    for sql <- ["PRAGMA temp_store = MEMORY", "PRAGMA temp_store = 2", "PRAGMA temp_store(2)"] do
      assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} = ShardExecutor.execute(h, stmt(sql)),
             "`#{sql}` was accepted"
    end

    assert read_pragma(h, "temp_store") == before
  end

  # Expert review 2026-10-08 #9: the size cap covered `main` only, so a TEMP table grew past it (four
  # 100 MB inserts under a 50 MB cap). The temp schema is now capped lazily, before DDL and scripts —
  # not at open, which cost ~85 KiB per connection and was refused by the served-density gate.
  describe "the TEMP schema under a 1 MiB shard cap" do
    setup do
      prev = Application.get_env(:fathom, :shard_max_bytes)
      Application.put_env(:fathom, :shard_max_bytes, 1024 * 1024)
      shard = "test_tempcap_#{System.unique_integer([:positive])}"
      {:ok, h} = ShardExecutor.open(shard)

      on_exit(fn ->
        ShardExecutor.close(h)

        if prev,
          do: Application.put_env(:fathom, :shard_max_bytes, prev),
          else: Application.delete_env(:fathom, :shard_max_bytes)

        rm_shard_files(shard)
      end)

      %{temp_handle: h}
    end

    @big_insert """
    INSERT INTO big SELECT randomblob(4000) FROM
      (WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1000)
       SELECT i FROM n)
    """

    test "a TEMP table cannot grow past the cap", %{temp_handle: h} do
      {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TEMP TABLE big (x)"))
      assert read_pragma(h, "temp.max_page_count") == 256

      assert {:error, %Error{message: message}} = ShardExecutor.execute(h, stmt(@big_insert)),
             "a ~4 MB TEMP insert succeeded under a 1 MiB shard cap"

      assert message =~ "full"
    end

    test "nor through a script", %{temp_handle: h} do
      assert {:error, _} =
               ShardExecutor.execute_sequence(
                 h,
                 "CREATE TEMPORARY TABLE big (x); " <> @big_insert
               ),
             "a script grew a TEMP table past the 1 MiB cap"
    end
  end

  test "bare reads with trailing comments, and later batched statements, stay reads", %{
    handle: h
  } do
    for sql <- [
          "PRAGMA journal_mode /* c */",
          "PRAGMA journal_mode -- c",
          "PRAGMA journal_mode /* = ( */ ;",
          "PRAGMA synchronous; SELECT 'a=1'"
        ] do
      refute match?(
               {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}},
               ShardExecutor.execute(h, stmt(sql))
             ),
             "`#{inspect(sql)}` is a bare read and was refused"
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
          ";;DROP TABLE IF EXISTS semi",
          # A leading BOM SQLite runs but the gate missed (see @prefixes).
          "﻿CREATE TABLE bom (x)",
          "﻿;CREATE TABLE bomsemi (x)"
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

    # The Hrana `sequence` path shares the gate (expert review 2026-09-29 #30): refuse_script
    # used to check only DDL, so a script carrying the same assignment forged the stamp that
    # execute/2 refuses — and the stamp really moved.
    for sql <- [
          "PRAGMA user_version = 5",
          "SELECT 1; PRAGMA user_version = 5",
          "/* c */ PRAGMA main.user_version(5);"
        ] do
      assert {:error, %Error{code: "FILO_PRAGMA_BLOCKED"}} =
               ShardExecutor.execute_sequence(h, sql),
             "sequence `#{sql}` forged the schema-version stamp under :block_tenant_ddl"
    end

    # The bare read still works (it discloses only the current stamp) — and it is still 0.
    assert {:ok, %Filo.StmtResult{rows: [[0]]}} =
             ShardExecutor.execute(h, stmt("PRAGMA user_version"))
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
