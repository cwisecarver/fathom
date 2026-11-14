defmodule Fathom.ShardExecutorTest do
  # Exercises the Filo.Executor binding over real shard connections. Not async:
  # shards are addressed by a global Registry and back onto files.
  use ExUnit.Case, async: false

  import Plug.Test

  alias Fathom.ShardExecutor
  alias Filo.{Error, Stmt, StmtResult}

  setup do
    shard = "test_exec_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      local = Path.join([Fathom.Shard.data_dir(), "#{shard}.db"])
      remote = Path.join([Fathom.Shard.Storage.Local.dir(), "#{shard}.db"])

      for base <- [local, remote], suffix <- ["", "-wal", "-shm"], do: File.rm(base <> suffix)
    end)

    %{shard: shard}
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  defp restore_env(k, nil), do: Application.delete_env(:fathom, k)
  defp restore_env(k, v), do: Application.put_env(:fathom, k, v)

  defp rm_shard_files(id) do
    for base <- [
          Path.join([Fathom.Shard.data_dir(), "#{id}.db"]),
          Path.join([Fathom.Shard.Storage.Local.dir(), "#{id}.db"])
        ],
        suffix <- ["", "-wal", "-shm"] do
      File.rm(base <> suffix)
    end
  end

  test "open returns a connection and execute runs CRUD, mapping to StmtResult", %{shard: shard} do
    assert {:ok, conn} = ShardExecutor.open(shard)

    assert {:ok, %StmtResult{}} =
             ShardExecutor.execute(conn, stmt("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)"))

    assert {:ok, %StmtResult{affected_row_count: 1, last_insert_rowid: 1}} =
             ShardExecutor.execute(conn, stmt("INSERT INTO kv VALUES (?, ?)", [1, "alice"]))

    assert {:ok,
            %StmtResult{
              cols: ["v"],
              rows: [["alice"]],
              affected_row_count: 0,
              last_insert_rowid: nil
            }} =
             ShardExecutor.execute(conn, stmt("SELECT v FROM kv WHERE k = ?", [1]))

    assert :ok = ShardExecutor.close(conn)
  end

  # Expert review 2026-09-29 #29. A Filo.Socket runs every stream of a WebSocket session in ONE
  # long-lived process, so any per-conn process-dictionary key that close/1 does not delete grows
  # the socket's dictionary by one entry per stream for hours. `:dirty_signalled` was such a key.
  # Invariant: after close/1, the stream leaves no ShardExecutor keys behind in its process.
  test "close/1 leaves no per-conn process-dictionary state behind (WS socket process)",
       %{shard: shard} do
    executor_keys = fn ->
      for {{Fathom.ShardExecutor, _, _} = k, _} <- Process.get(), do: k
    end

    {:ok, c} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(c, stmt("CREATE TABLE t (v INTEGER)"))
    :ok = ShardExecutor.close(c)

    for i <- 1..5 do
      {:ok, c} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(c, stmt("INSERT INTO t VALUES (?)", [i]))
      assert executor_keys.() != [], "the write did not set any per-conn state to leak"
      :ok = ShardExecutor.close(c)
    end

    assert executor_keys.() == []
  end

  test "two streams on the same shard get isolated transactions", %{shard: shard} do
    {:ok, a} = ShardExecutor.open(shard)
    {:ok, b} = ShardExecutor.open(shard)

    {:ok, _} = ShardExecutor.execute(a, stmt("CREATE TABLE t (v TEXT)"))

    # A opens a write transaction and inserts, but does NOT commit.
    {:ok, _} = ShardExecutor.execute(a, stmt("BEGIN"))
    {:ok, _} = ShardExecutor.execute(a, stmt("INSERT INTO t VALUES ('x')"))

    # B is a separate connection: it must not see A's uncommitted row. (On the old
    # shared-connection model B and A were the same transaction and this was ['x'].)
    assert {:ok, %StmtResult{rows: []}} = ShardExecutor.execute(b, stmt("SELECT v FROM t"))

    {:ok, _} = ShardExecutor.execute(a, stmt("COMMIT"))

    # Once A commits, B sees it.
    assert {:ok, %StmtResult{rows: [["x"]]}} = ShardExecutor.execute(b, stmt("SELECT v FROM t"))

    ShardExecutor.close(a)
    ShardExecutor.close(b)
  end

  test "a SQL error maps to a Filo.Error", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)

    assert {:error, %Error{code: "SQLITE_ERROR"}} =
             ShardExecutor.execute(conn, stmt("SELECT * FROM does_not_exist"))

    ShardExecutor.close(conn)
  end

  # Expert review 2026-07-14 #3: constraint violations must carry the specific SQLITE_CONSTRAINT*
  # code (not a flat SQLITE_ERROR), or django-libsql can't map them to Python IntegrityError —
  # so get_or_create's `except IntegrityError` race handling never fires and the request 500s.
  test "a UNIQUE violation surfaces as SQLITE_CONSTRAINT_UNIQUE", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE u (x INTEGER UNIQUE)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO u VALUES (1)"))

    assert {:error, %Error{code: "SQLITE_CONSTRAINT_UNIQUE"}} =
             ShardExecutor.execute(conn, stmt("INSERT INTO u VALUES (1)"))

    ShardExecutor.close(conn)
  end

  test "a FOREIGN KEY violation surfaces as SQLITE_CONSTRAINT_FOREIGNKEY", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE parent (id INTEGER PRIMARY KEY)"))

    {:ok, _} =
      ShardExecutor.execute(
        conn,
        stmt("CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
      )

    # Relies on FK enforcement being on by default (review #2).
    assert {:error, %Error{code: "SQLITE_CONSTRAINT_FOREIGNKEY"}} =
             ShardExecutor.execute(conn, stmt("INSERT INTO child VALUES (1, 999)"))

    ShardExecutor.close(conn)
  end

  # Expert review 2026-07-14 #35: autocommit? must report the connection's REAL transaction state,
  # not a hardcoded true — a libSQL batch's `{:not, :is_autocommit}`-guarded COMMIT/ROLLBACK is
  # skipped when it lies, leaving a dangling transaction holding the WAL write lock. Pre-fix the
  # in-transaction assertion (false) fails.
  test "autocommit? reflects the real transaction state", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    assert ShardExecutor.autocommit?(conn) == true

    {:ok, _} = ShardExecutor.execute(conn, stmt("BEGIN"))
    assert ShardExecutor.autocommit?(conn) == false

    {:ok, _} = ShardExecutor.execute(conn, stmt("COMMIT"))
    assert ShardExecutor.autocommit?(conn) == true

    ShardExecutor.close(conn)
  end

  # Expert review 2026-07-14 #42: an INSERT/UPDATE/DELETE ... RETURNING returns columns AND mutates,
  # so the old "returns columns ⇒ query" test reported affected_row_count 0 / last_insert_rowid nil.
  # Pre-fix these assertions fail.
  test "INSERT ... RETURNING reports affected_row_count and last_insert_rowid", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)

    {:ok, _} =
      ShardExecutor.execute(conn, stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))

    assert {:ok,
            %StmtResult{cols: ["id"], rows: [[1]], affected_row_count: 1, last_insert_rowid: 1}} =
             ShardExecutor.execute(conn, stmt("INSERT INTO t (v) VALUES ('a') RETURNING id"))

    ShardExecutor.close(conn)
  end

  test "UPDATE ... RETURNING reports the affected row count", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)

    {:ok, _} =
      ShardExecutor.execute(conn, stmt("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))

    {:ok, _} =
      ShardExecutor.execute(conn, stmt("INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b')"))

    assert {:ok, %StmtResult{affected_row_count: 2}} =
             ShardExecutor.execute(conn, stmt("UPDATE t SET v = 'x' RETURNING id"))

    ShardExecutor.close(conn)
  end

  # A plain SELECT still reports no affected rows (the read-classification the RETURNING fix preserves).
  test "a SELECT reports affected_row_count 0", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(conn, stmt("INSERT INTO t VALUES ('a')"))

    assert {:ok, %StmtResult{affected_row_count: 0, last_insert_rowid: nil}} =
             ShardExecutor.execute(conn, stmt("SELECT v FROM t"))

    ShardExecutor.close(conn)
  end

  # Expert review 2026-07-14 #7: a direct `manage.py migrate` on a tenant advances its schema while
  # fathom's version stamp stays at 0, so the shard reads as a laggard and the engine replays the
  # captured chain onto it → "already exists" → quarantine. With :block_tenant_ddl on, client DDL on
  # a tenant is refused loudly at the source instead. Pre-fix (no guard) the CREATE succeeds.
  test "with :block_tenant_ddl on, client DDL on a tenant is refused (DML unaffected)", %{
    shard: shard
  } do
    prev = Application.get_env(:fathom, :block_tenant_ddl)
    Application.put_env(:fathom, :block_tenant_ddl, true)
    on_exit(fn -> restore_env(:block_tenant_ddl, prev) end)

    {:ok, conn} = ShardExecutor.open(shard)

    assert {:error, %Error{code: "FILO_DDL_BLOCKED"}} =
             ShardExecutor.execute(conn, stmt("CREATE TABLE t (v TEXT)"))

    # A non-DDL statement on the same tenant is unaffected.
    assert {:ok, %StmtResult{}} = ShardExecutor.execute(conn, stmt("SELECT 1"))

    ShardExecutor.close(conn)
  end

  test "the template shard is exempt from the tenant-DDL guard" do
    prev = Application.get_env(:fathom, :block_tenant_ddl)
    prev_tmpl = Application.get_env(:fathom, :template_shard_id)
    tmpl = "tmpl_ddl_#{System.unique_integer([:positive])}"
    Application.put_env(:fathom, :block_tenant_ddl, true)
    Application.put_env(:fathom, :template_shard_id, tmpl)

    on_exit(fn ->
      Fathom.Shards.drain(tmpl, 5_000)
      restore_env(:block_tenant_ddl, prev)
      restore_env(:template_shard_id, prev_tmpl)
      rm_shard_files(tmpl)
    end)

    {:ok, conn} = ShardExecutor.open(tmpl)
    assert {:ok, %StmtResult{}} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (v TEXT)"))
    ShardExecutor.close(conn)
  end

  test "with the guard off (default), tenant DDL is allowed", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)
    assert {:ok, %StmtResult{}} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (v TEXT)"))
    ShardExecutor.close(conn)
  end

  # Expert review 2026-07-14 #19: "limited dataset per shard" must be ENFORCEABLE — a per-shard
  # size cap (`:shard_max_page_count`) so one runaway tenant can't grow unbounded and inflate every
  # whole-shard cost. A write past the cap fails SQLITE_FULL. Pre-fix (no cap) the inserts all
  # succeed and the loop never errors.
  test "config :shard_max_page_count caps a shard's size (writes past it get SQLITE_FULL)", %{
    shard: shard
  } do
    prev = Application.get_env(:fathom, :shard_max_page_count)
    Application.put_env(:fathom, :shard_max_page_count, 20)
    on_exit(fn -> restore_env(:shard_max_page_count, prev) end)

    {:ok, conn} = ShardExecutor.open(shard)
    {:ok, _} = ShardExecutor.execute(conn, stmt("CREATE TABLE t (v TEXT)"))

    result =
      Enum.reduce_while(1..5_000, :ok, fn _i, _acc ->
        case ShardExecutor.execute(
               conn,
               stmt("INSERT INTO t VALUES (?)", [String.duplicate("x", 400)])
             ) do
          {:ok, _} -> {:cont, :ok}
          {:error, err} -> {:halt, {:error, err}}
        end
      end)

    assert {:error, %Error{code: "SQLITE_FULL"}} = result

    ShardExecutor.close(conn)
  end

  # Finding #25: a statement that *raises* (connection.ex rescues only ArgumentError, so a
  # bad bind / exqlite NIF error / result-mapping bug propagates) must not crash the Hrana
  # stream — the executor boundary converts any raise to a %Filo.Error{}. Non-list args are a
  # deterministic stand-in: they raise FunctionClauseError in the exqlite bind.
  test "a raising statement returns a protocol error and the stream survives", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)

    assert {:error, %Error{}} =
             ShardExecutor.execute(conn, %Stmt{sql: "SELECT ?", args: :not_a_list})

    # The connection wasn't taken down by the raise — a normal query on the same handle works.
    assert {:ok, %StmtResult{}} = ShardExecutor.execute(conn, stmt("SELECT 1"))

    ShardExecutor.close(conn)
  end

  # Expert review 2026-10-13 #23. The rescue returned `Exception.message(e)` to the tenant, and that
  # text names module internals and paths. The tenant now gets a fixed message; the detail stays in
  # the server log. Before the fix the message was the FunctionClauseError text, not this string.
  test "a raising statement gives the tenant a fixed message and logs the detail", %{shard: shard} do
    {:ok, conn} = ShardExecutor.open(shard)

    # The `execute raised` line is rate-limited per NODE (`Fathom.LogLimiter`, 5 a second, expert
    # review 2026-11-07 #69), and other tests' raising statements share that window. Seen failing in
    # a full run (seed 883557, passing in isolation and at that seed on re-run): the window was
    # already spent, the line was suppressed, and `log` was "". The test is about the log's CONTENT,
    # not the limiter, so it starts from a fresh window.
    Fathom.LogLimiter.reset(:execute_raised)

    {result, log} =
      ExUnit.CaptureLog.with_log(fn ->
        ShardExecutor.execute(conn, %Stmt{sql: "SELECT ?", args: :not_a_list})
      end)

    assert {:error, %Error{code: "SQLITE_ERROR", message: "internal error executing statement"}} =
             result

    assert log =~ "execute raised"

    ShardExecutor.close(conn)
  end

  # Expert review 2026-10-13 #23. A Session exit names the data path and a pid. The FILO_NO_QUORUM
  # message used to interpolate `inspect(reason)`, so it carried both. Pre-fix the message contained
  # the "/" of the path, and this test fails on that.
  test "FILO_NO_QUORUM names no path or pid to the tenant; the reason is logged" do
    reason =
      {:session_down,
       {:timeout, {GenServer, :call, [self(), {:commit, "/var/lib/fathom/acme.db-wal"}, 5_000]}}}

    {error, log} =
      ExUnit.CaptureLog.with_log(fn -> ShardExecutor.no_quorum_error("acme", reason) end)

    assert %Error{code: "FILO_NO_QUORUM", status: 503, message: message} = error
    assert message == "write not replicated to a follower quorum"
    refute message =~ "/"
    refute message =~ "#PID"
    assert log =~ "/var/lib/fathom/acme.db-wal"
  end

  test "an invalid shard id fails to open" do
    assert {:error, %Error{code: "FILO_SHARD_OPEN"}} = ShardExecutor.open("bad/../id")
  end

  # Expert review 2026-10-10 #H6. `{:shard_migrating, _}` and `:unavailable` fell into the
  # catch-all: a status-less 500 whose message carried `inspect(reason)` (internal terms) to the
  # tenant. Routine retry-later states are 503; the catch-all is a fixed message, reason logged.
  test "open errors: retryable states are 503, the catch-all leaks nothing" do
    import ExUnit.CaptureLog

    assert %Error{code: "FILO_SHARD_BUSY", status: 503} =
             ShardExecutor.open_error({:shard_migrating, :held_by_worker})

    assert %Error{code: "FILO_UNAVAILABLE", status: 503} = ShardExecutor.open_error(:unavailable)

    log =
      capture_log(fn ->
        assert %Error{code: "FILO_OPEN_FAILED", status: 500, message: "cannot open shard"} =
                 ShardExecutor.open_error({:weird, "/var/secret/path", :node@internal})
      end)

    assert log =~ "/var/secret/path"
  end

  test "shard_from_conn reads the host subdomain, then ?db=, then header, then default" do
    # Host subdomain is primary (how libSQL clients address a database).
    assert ShardExecutor.shard_from_conn(conn(:get, "http://acme.fathom.test/")) == "acme"

    # A bare host / IP falls through to the curl/test fallbacks (override on in test config).
    assert ShardExecutor.shard_from_conn(conn(:get, "http://localhost/?db=beta")) == "beta"

    header_conn =
      conn(:get, "http://localhost/") |> Plug.Conn.put_req_header("x-fathom-shard", "gamma")

    assert ShardExecutor.shard_from_conn(header_conn) == "gamma"

    assert ShardExecutor.shard_from_conn(conn(:get, "http://localhost/")) == "demo"
  end

  # Expert review #13: with no serving-zone anchor, the first label of ANY multi-label
  # Host selected a shard — the primary production tenant-selection path trusted a fully
  # attacker-controlled header. With :shard_base_domain configured, only
  # <shard>.<zone> resolves; foreign zones, nested labels, and zone-suffix smuggling
  # fail closed to the default chain. Cross-tenant selection is release-blocker class
  # (AGENTS.md shard-isolation gate).
  test "with a serving zone configured, only hosts in the zone select shards" do
    prev = Application.get_env(:fathom, :shard_base_domain)
    Application.put_env(:fathom, :shard_base_domain, "fathom.test")

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :shard_base_domain),
        else: Application.put_env(:fathom, :shard_base_domain, prev)
    end)

    resolve = fn url -> ShardExecutor.shard_from_conn(conn(:get, url)) end

    # In-zone hosts resolve (case-insensitively, trailing dot tolerated).
    assert resolve.("http://acme.fathom.test/") == "acme"
    assert resolve.("http://acme.FATHOM.test./") == "acme"

    # A foreign zone with a shard-shaped first label must NOT select that shard.
    assert resolve.("http://victimshard.attacker.com/") == "demo"

    # Nested labels can't smuggle a different shard under the zone suffix.
    assert resolve.("http://victim.attacker.fathom.test/") == "demo"

    # A bare zone host (no shard label) falls through.
    assert resolve.("http://fathom.test/") == "demo"
  end

  # Round-2 #35: `if System.get_env(...)` treats "" as truthy, so a BLANK
  # SHARD_BASE_DOMAIN configured an empty zone — zone_matches? then denied ALL
  # subdomain routing: fail-closed 400s in prod, but cross-tenant COMMINGLING into
  # :default_shard wherever that is set (as it is in dev/test). The invariant: a
  # blank zone is a misconfig treated as unset — every host still routes to its OWN
  # shard, never to the shared default.
  test "a blank serving zone is treated as unset, not as deny-everything" do
    import ExUnit.CaptureLog

    prev = Application.get_env(:fathom, :shard_base_domain)
    Application.put_env(:fathom, :shard_base_domain, "")

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :shard_base_domain),
        else: Application.put_env(:fathom, :shard_base_domain, prev)
    end)

    capture_log(fn ->
      assert ShardExecutor.shard_from_conn(conn(:get, "http://acme.fathom.test/")) == "acme",
             "a blank zone must not commingle tenants into the default shard"
    end)
  end

  # Expert review #36: Plug parses `?db[]=a&db[]=b` into a LIST and `?db[k]=v` into a
  # MAP, and normalize_resolved only had nil/binary heads — a crafted query param raised
  # FunctionClauseError on the resolve path (shard_from_conn is Filo's open_arg with no
  # rescue): a trivial remote crash oracle wherever the override is enabled. The
  # invariant: non-binary trust-boundary values fail closed to the fallback chain.
  test "list/map ?db= params fail closed instead of crashing resolution" do
    assert ShardExecutor.shard_from_conn(conn(:get, "http://localhost/?db[]=a&db[]=b")) ==
             "demo"

    assert ShardExecutor.shard_from_conn(conn(:get, "http://localhost/?db[k]=v")) == "demo"

    # A map-shaped db still lets the header fallback work.
    header_conn =
      conn(:get, "http://localhost/?db[]=x")
      |> Plug.Conn.put_req_header("x-fathom-shard", "gamma")

    assert ShardExecutor.shard_from_conn(header_conn) == "gamma"
  end

  # Finding #4: the ?db= / x-fathom-shard fallbacks are an unauthenticated shard-selection
  # primitive — a caller who reaches a node directly controls the Host, so a bare/IP host forces
  # the fallback and `?db=<victim>` opens any shard. With :allow_shard_override off (the prod
  # default), the override is inert and a hostless request resolves to the default shard, never
  # the attacker-named one. Cross-shard isolation is a release-blocker (AGENTS.md), so pin it.
  test "with the override off, ?db= / x-fathom-shard cannot select a foreign shard" do
    prev = Application.get_env(:fathom, :allow_shard_override)
    Application.put_env(:fathom, :allow_shard_override, false)
    on_exit(fn -> Application.put_env(:fathom, :allow_shard_override, prev) end)

    assert ShardExecutor.shard_from_conn(conn(:get, "http://localhost/?db=victim")) == "demo"

    header_conn =
      conn(:get, "http://localhost/") |> Plug.Conn.put_req_header("x-fathom-shard", "victim")

    assert ShardExecutor.shard_from_conn(header_conn) == "demo"

    # The LB-routed subdomain is unaffected — it's resolved before the (now-disabled) override.
    assert ShardExecutor.shard_from_conn(conn(:get, "http://acme.fathom.test/?db=victim")) ==
             "acme"
  end

  # Expert reviews #32 + #35: shard_from_host is the primary production tenant-selection
  # path, exposed to a fully attacker-controlled header — but only its 3-label happy path
  # was tested. Pin the adversarial edges. #35 specifically: a trailing-dot FQDN
  # ("localhost.") split to ["localhost", ""] and promoted an otherwise-bare hostname to
  # a shard, so the same logical host routed to a named shard WITH the dot and the
  # default shard without it — a tenant silently split by a legal client behavior.
  test "host-subdomain routing edge cases cannot select unintended shards" do
    resolve = fn url -> ShardExecutor.shard_from_conn(conn(:get, url)) end

    # Trailing-dot FQDNs are the same logical host: no bare-host promotion (#35)…
    assert resolve.("http://localhost./") == "demo",
           "a trailing dot must not promote a bare hostname to a shard"

    # …and the multi-label form resolves identically with or without the dot.
    assert resolve.("http://acme.fathom.test./") == "acme"

    # Nested/multi-label subdomains take the FIRST label — a foreign zone suffix cannot
    # smuggle a different label into resolution.
    assert resolve.("http://victim.attacker.fathom.test/") == "victim"

    # Empty labels never resolve to a shard.
    assert resolve.("http://..fathom.test/") == "demo"

    # IPv4 hosts are not shards (fall through to the default).
    assert resolve.("http://127.0.0.1/") == "demo"

    # An explicit port doesn't disturb subdomain extraction (Plug strips it from host).
    assert resolve.("http://acme.fathom.test:8080/") == "acme"

    # Invalid first labels (shard-id validation) never resolve.
    assert resolve.("http://bad_%40label.fathom.test/") == "demo"
  end

  # Finding #19: shard-id case is normalized at the trust boundary, so `ACME` and `acme` name the
  # ONE shard (one file / registry key / S3 key) instead of splitting a tenant (case-sensitive FS)
  # or colliding by accident (case-insensitive FS).
  test "shard-id case is normalized so upper- and lower-case name the same shard", %{shard: shard} do
    # Routing downcases the resolved id.
    assert ShardExecutor.shard_from_conn(conn(:get, "http://ACME.fathom.test/")) == "acme"

    # Case variants resolve to the SAME coordinator — an exact registry-key check, so this holds
    # on a case-sensitive FS (where pre-fix they'd be two coordinators / a split tenant) as well as
    # a case-insensitive one (macOS, where the files alias but the registry keys would still split).
    {:ok, p1, ref1, _} = Fathom.Shards.checkout(String.upcase(shard))
    {:ok, p2, ref2, _} = Fathom.Shards.checkout(shard)

    assert p1 == p2, "case-variant ids must resolve to one coordinator, not split into two"

    Fathom.Shard.checkin(p1, ref1)
    Fathom.Shard.checkin(p2, ref2)
  end

  # Finding #26: with no configured default shard (the prod fail-closed posture), a request that
  # resolves no subdomain/override must be REFUSED, not commingled into a shared "demo" shard.
  test "with no default shard, a hostless request resolves to nil and open/1 refuses with a 400" do
    prev = Application.get_env(:fathom, :default_shard)
    Application.put_env(:fathom, :default_shard, nil)
    on_exit(fn -> Application.put_env(:fathom, :default_shard, prev) end)

    assert ShardExecutor.shard_from_conn(conn(:get, "http://localhost/")) == nil

    assert {:error, %Error{code: "FILO_NO_SHARD", status: 400}} = ShardExecutor.open(nil)

    # A named subdomain still resolves normally — fail-closed only affects the no-shard case.
    assert ShardExecutor.shard_from_conn(conn(:get, "http://acme.fathom.test/")) == "acme"
  end

  # Security review 2026-11-10 #1, through the wire path: Hrana `args` are BOUND by the executor,
  # and SQLite allows `fts3_tokenizer` for a bound parameter even with the engine flag off. Before
  # the extension's authorizer denied the name, the reader answered an 8-byte module address and the
  # setter stored one, on `:rw` and `:ro` alike.
  for scope <- [:rw, :ro] do
    test "fts3_tokenizer with Hrana-bound args is refused (#{scope})", %{shard: shard} do
      {:ok, seed} = ShardExecutor.open(shard)
      {:ok, _} = ShardExecutor.execute(seed, stmt("CREATE TABLE t (v TEXT)"))
      :ok = ShardExecutor.close(seed)

      {:ok, conn} = ShardExecutor.open(shard, unquote(scope))
      # The reader looks up a built-in; the setter uses a FRESH name, never `simple`/`porter`/
      # `unicode61`: if the guard ever regresses, overwriting a built-in tokenizer with address 0x8
      # would take the VM down on the next FTS use of that handle.
      reader = Filo.Value.decode(%{"type" => "text", "value" => "simple"})
      fresh = Filo.Value.decode(%{"type" => "text", "value" => "fathom_probe"})
      blob = Filo.Value.decode(%{"type" => "blob", "base64" => Base.encode64(<<8::64-little>>)})

      for {sql, args} <- [
            {"SELECT fts3_tokenizer(?)", [reader]},
            {"SELECT fts3_tokenizer(?, ?)", [fresh, blob]}
          ] do
        # The ENGINE's refusal, not merely some error (an arg-type mismatch would also be one).
        assert {:error, %Error{message: message}} = ShardExecutor.execute(conn, stmt(sql, args)),
               sql

        assert message =~ "not authorized", sql
      end

      # The handle is still usable afterwards.
      assert {:ok, %StmtResult{}} = ShardExecutor.execute(conn, stmt("SELECT 1"))
      ShardExecutor.close(conn)
    end
  end
end
