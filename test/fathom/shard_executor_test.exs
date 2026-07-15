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
      local = Path.join([System.tmp_dir!(), "fathom_shards", "#{shard}.db"])
      remote = Path.join([System.tmp_dir!(), "fathom_remote_test", "#{shard}.db"])

      for base <- [local, remote], suffix <- ["", "-wal", "-shm"], do: File.rm(base <> suffix)
    end)

    %{shard: shard}
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

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

  test "an invalid shard id fails to open" do
    assert {:error, %Error{code: "FILO_SHARD_OPEN"}} = ShardExecutor.open("bad/../id")
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

  # Finding #17: prod must refuse to boot if the fallback default shard equals the capture template
  # (anonymous default traffic would then drive fleet-wide capture). Prod-gated; dev/test
  # intentionally couple them ("demo"), so the guard is a no-op outside prod.
  test "the boot guard refuses a prod config where default_shard equals template_shard_id" do
    prev_env = Application.get_env(:fathom, :env)
    prev_tpl = Application.get_env(:fathom, :template_shard_id)
    prev_def = Application.get_env(:fathom, :default_shard)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      Application.put_env(:fathom, :template_shard_id, prev_tpl)
      Application.put_env(:fathom, :default_shard, prev_def)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :template_shard_id, "poison")
    Application.put_env(:fathom, :default_shard, "poison")

    assert_raise RuntimeError, ~r/finding #17/, fn ->
      Fathom.Application.check_template_default!()
    end

    # A different default (or the fail-closed nil) boots fine.
    Application.put_env(:fathom, :default_shard, "safe")
    assert Fathom.Application.check_template_default!() == nil
  end

  # Review 2026-07-09 #1: the rebalancer keys its exception table + dead-node reconciler on
  # node_key = an lb_backends key; a NODE_KEY↔lb_backends mismatch makes every pin look dead
  # and unpins the fleet each tick. Prod must refuse to boot with that config.
  test "the boot guard refuses a prod NODE_KEY that isn't an lb_backends key" do
    prev_env = Application.get_env(:fathom, :env)
    prev_backends = Application.get_env(:fathom, :lb_backends)
    prev_key = Application.get_env(:fathom, :node_key)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:lb_backends, prev_backends)
      restore_env(:node_key, prev_key)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :lb_backends, %{"fathom1" => "fathom1:8080"})
    Application.put_env(:fathom, :node_key, "fathom9")

    assert_raise RuntimeError, ~r/not a key of :lb_backends/, fn ->
      Fathom.Application.check_rebalancer_config!()
    end

    # A matching NODE_KEY boots fine.
    Application.put_env(:fathom, :node_key, "fathom1")
    assert Fathom.Application.check_rebalancer_config!() == nil

    # Inert when :lb_backends is unset (no rebalancer fleet), and outside prod.
    Application.delete_env(:fathom, :lb_backends)
    Application.put_env(:fathom, :node_key, "anything")
    assert Fathom.Application.check_rebalancer_config!() == nil

    Application.put_env(:fathom, :env, :test)
    Application.put_env(:fathom, :lb_backends, %{"fathom1" => "x"})
    Application.put_env(:fathom, :node_key, "mismatch")
    assert Fathom.Application.check_rebalancer_config!() == nil
  end

  defp restore_env(k, nil), do: Application.delete_env(:fathom, k)
  defp restore_env(k, v), do: Application.put_env(:fathom, k, v)

  # Expert review #9: template capture replays a shard's SQL fleet-wide, and AGENTS.md forbids a
  # prod template without auth on that shard — but no guard enforced it, so an anonymously
  # reachable template shard (auth :disabled, one Host header away) could poison a fleet version
  # that Migrator.Copy replays onto every tenant. Prod must refuse to boot with that config.
  test "the boot guard refuses a prod template_shard_id with hrana auth disabled" do
    prev_env = Application.get_env(:fathom, :env)
    prev_tpl = Application.get_env(:fathom, :template_shard_id)
    prev_auth = Application.get_env(:fathom, :hrana_auth)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      Application.put_env(:fathom, :template_shard_id, prev_tpl)

      if is_nil(prev_auth),
        do: Application.delete_env(:fathom, :hrana_auth),
        else: Application.put_env(:fathom, :hrana_auth, prev_auth)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :template_shard_id, "template")
    Application.put_env(:fathom, :hrana_auth, :disabled)

    assert_raise RuntimeError, ~r/poisoning vector/, fn ->
      Fathom.Application.check_template_auth!()
    end

    # An unset :hrana_auth defaults to :disabled — still refused.
    Application.delete_env(:fathom, :hrana_auth)

    assert_raise RuntimeError, ~r/poisoning vector/, fn ->
      Fathom.Application.check_template_auth!()
    end

    # Auth required (or any non-:disabled value, which fails closed to required) boots fine.
    Application.put_env(:fathom, :hrana_auth, :required)
    assert Fathom.Application.check_template_auth!() == nil

    # No template in prod boots fine regardless of auth mode.
    Application.put_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :template_shard_id, nil)
    assert Fathom.Application.check_template_auth!() == nil
  end

  # Finding #3: the Hrana port carries no in-app credential, so the listener's bind interface is
  # a network-hardening control (pin to the private interface the LB reaches). Default is all
  # interfaces; prod sets it from HRANA_BIND_IP (config/runtime.exs).
  test "hrana_bind_ip is configurable and defaults to all interfaces" do
    assert Fathom.Application.hrana_bind_ip() == {0, 0, 0, 0}

    prev = Application.get_env(:fathom, :hrana_bind_ip)
    Application.put_env(:fathom, :hrana_bind_ip, {127, 0, 0, 1})
    on_exit(fn -> Application.put_env(:fathom, :hrana_bind_ip, prev) end)

    assert Fathom.Application.hrana_bind_ip() == {127, 0, 0, 1}
  end

  # Expert review 2026-07-14 #3: Local storage's lease is node-local only (:global.trans scoped to
  # [node()]), so running it across a fleet (:lb_backends set) is silent split-brain — two nodes
  # can each hold a shard's lease and both write. Prod must refuse to boot with that combination.
  test "the boot guard refuses prod Local storage with a multi-node fleet" do
    prev_env = Application.get_env(:fathom, :env)
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_backends = Application.get_env(:fathom, :lb_backends)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:shard_storage, prev_storage)
      restore_env(:lb_backends, prev_backends)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :shard_storage, Fathom.Shard.Storage.Local)
    Application.put_env(:fathom, :lb_backends, %{"fathom1" => "fathom1:8080"})

    assert_raise RuntimeError, ~r/no cross-node single-writer guarantee/, fn ->
      Fathom.Application.check_local_storage_fleet!()
    end

    # Single-node Local (no fleet) boots fine.
    Application.delete_env(:fathom, :lb_backends)
    assert Fathom.Application.check_local_storage_fleet!() == nil

    # The S3 backend across a fleet boots fine (it enforces cross-node single-writer).
    Application.put_env(:fathom, :lb_backends, %{"fathom1" => "fathom1:8080"})
    Application.put_env(:fathom, :shard_storage, Fathom.Shard.Storage.S3)
    assert Fathom.Application.check_local_storage_fleet!() == nil
  end

  # Expert review 2026-07-14 #6: without :shard_base_domain, Fathom.ShardExecutor.shard_from_host
  # promotes any attacker-controlled Host first-label to a shard id (cross-tenant selection). Prod
  # must refuse to boot an EXPOSED data plane (a fleet, or the Hrana listener on) with the zone
  # unset/blank, unless ALLOW_UNANCHORED_ROUTING explicitly acks it.
  test "the boot guard refuses a prod exposed data plane with no shard_base_domain" do
    prev_env = Application.get_env(:fathom, :env)
    prev_zone = Application.get_env(:fathom, :shard_base_domain)
    prev_backends = Application.get_env(:fathom, :lb_backends)
    prev_server = Application.get_env(:fathom, :hrana_server)
    prev_ack = Application.get_env(:fathom, :allow_unanchored_routing)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:shard_base_domain, prev_zone)
      restore_env(:lb_backends, prev_backends)
      restore_env(:hrana_server, prev_server)
      restore_env(:allow_unanchored_routing, prev_ack)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.delete_env(:fathom, :shard_base_domain)
    Application.put_env(:fathom, :lb_backends, %{"fathom1" => "fathom1:8080"})
    Application.delete_env(:fathom, :allow_unanchored_routing)

    assert_raise RuntimeError, ~r/SHARD_BASE_DOMAIN/, fn ->
      Fathom.Application.check_shard_base_domain!()
    end

    # A blank/whitespace zone is treated as unset — still refused.
    Application.put_env(:fathom, :shard_base_domain, "   ")

    assert_raise RuntimeError, ~r/SHARD_BASE_DOMAIN/, fn ->
      Fathom.Application.check_shard_base_domain!()
    end

    # An anchored zone boots fine.
    Application.put_env(:fathom, :shard_base_domain, "fathom.example")
    assert Fathom.Application.check_shard_base_domain!() == nil

    # The explicit ack lets an unanchored deploy boot even with the zone unset.
    Application.delete_env(:fathom, :shard_base_domain)
    Application.put_env(:fathom, :allow_unanchored_routing, true)
    assert Fathom.Application.check_shard_base_domain!() == nil

    # With the data plane NOT exposed (no fleet, Hrana server off), the zone may be unset.
    Application.delete_env(:fathom, :allow_unanchored_routing)
    Application.delete_env(:fathom, :lb_backends)
    Application.put_env(:fathom, :hrana_server, false)
    assert Fathom.Application.check_shard_base_domain!() == nil

    # The Hrana listener being enabled ALSO counts as exposed (refused with the zone unset).
    Application.put_env(:fathom, :hrana_server, true)

    assert_raise RuntimeError, ~r/SHARD_BASE_DOMAIN/, fn ->
      Fathom.Application.check_shard_base_domain!()
    end
  end

  # Expert review 2026-07-14 #15: the ?db= / x-fathom-shard override is an unauthenticated
  # shard-selection primitive (finding #4) that must never be on in prod.
  test "the boot guard refuses a prod config with allow_shard_override enabled" do
    prev_env = Application.get_env(:fathom, :env)
    prev_override = Application.get_env(:fathom, :allow_shard_override)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:allow_shard_override, prev_override)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :allow_shard_override, true)

    assert_raise RuntimeError, ~r/allow_shard_override/, fn ->
      Fathom.Application.check_shard_override!()
    end

    # Off (the prod default) boots fine, whether explicit or unset.
    Application.put_env(:fathom, :allow_shard_override, false)
    assert Fathom.Application.check_shard_override!() == nil

    Application.delete_env(:fathom, :allow_shard_override)
    assert Fathom.Application.check_shard_override!() == nil
  end

  # Expert review 2026-07-14 #16: a non-nil :default_shard in prod commingles all unresolved
  # requests into one shared shard instead of failing closed (finding #26). WARN, never raise —
  # a shared default can be an intentional single-tenant choice.
  test "the boot guard warns (never raises) on a prod non-nil default_shard" do
    import ExUnit.CaptureLog

    prev_env = Application.get_env(:fathom, :env)
    prev_default = Application.get_env(:fathom, :default_shard)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:default_shard, prev_default)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :default_shard, "shared")

    log =
      capture_log(fn ->
        assert Fathom.Application.check_default_shard!() == nil
      end)

    assert log =~ "default_shard"
    assert log =~ "commingles"

    # Nil default (the fail-closed posture) emits no warning.
    Application.put_env(:fathom, :default_shard, nil)

    refute capture_log(fn ->
             assert Fathom.Application.check_default_shard!() == nil
           end) =~ "commingles"
  end

  # Expert review 2026-07-14 #18: an enabled, unauthenticated Hrana data plane bound to all
  # interfaces leaves only an external firewall/SG as the tenant-isolation control. WARN, never
  # raise — this is the documented network-trust posture.
  test "the boot guard warns (never raises) on an exposed unauthenticated Hrana plane" do
    import ExUnit.CaptureLog

    prev_env = Application.get_env(:fathom, :env)
    prev_server = Application.get_env(:fathom, :hrana_server)
    prev_auth = Application.get_env(:fathom, :hrana_auth)
    prev_bind = Application.get_env(:fathom, :hrana_bind_ip)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:hrana_server, prev_server)
      restore_env(:hrana_auth, prev_auth)
      restore_env(:hrana_bind_ip, prev_bind)
    end)

    Application.put_env(:fathom, :env, :prod)
    Application.put_env(:fathom, :hrana_server, true)
    Application.put_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :hrana_bind_ip, {0, 0, 0, 0})

    log =
      capture_log(fn ->
        assert Fathom.Application.check_hrana_exposure!() == nil
      end)

    assert log =~ "unauthenticated"
    assert log =~ "all interfaces"

    # The IPv6 wildcard bind is flagged the same way.
    Application.put_env(:fathom, :hrana_bind_ip, {0, 0, 0, 0, 0, 0, 0, 0})
    assert capture_log(fn -> Fathom.Application.check_hrana_exposure!() end) =~ "firewall"

    # Auth required silences it (the data path is authenticated).
    Application.put_env(:fathom, :hrana_auth, :required)

    refute capture_log(fn ->
             assert Fathom.Application.check_hrana_exposure!() == nil
           end) =~ "firewall"

    # A pinned (non-wildcard) bind silences it (network-isolated to the LB interface).
    Application.put_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :hrana_bind_ip, {127, 0, 0, 1})

    refute capture_log(fn ->
             assert Fathom.Application.check_hrana_exposure!() == nil
           end) =~ "firewall"
  end

  # All five 2026-07-14 config guards are prod-only: with env != :prod (the dev/test default),
  # every one is inert regardless of the risky config, so they never fire outside prod.
  test "all 2026-07-14 config guards are inert when env is not prod" do
    import ExUnit.CaptureLog

    prev_env = Application.get_env(:fathom, :env)
    prev_storage = Application.get_env(:fathom, :shard_storage)
    prev_backends = Application.get_env(:fathom, :lb_backends)
    prev_zone = Application.get_env(:fathom, :shard_base_domain)
    prev_server = Application.get_env(:fathom, :hrana_server)
    prev_override = Application.get_env(:fathom, :allow_shard_override)
    prev_default = Application.get_env(:fathom, :default_shard)
    prev_auth = Application.get_env(:fathom, :hrana_auth)
    prev_bind = Application.get_env(:fathom, :hrana_bind_ip)

    on_exit(fn ->
      Application.put_env(:fathom, :env, prev_env)
      restore_env(:shard_storage, prev_storage)
      restore_env(:lb_backends, prev_backends)
      restore_env(:shard_base_domain, prev_zone)
      restore_env(:hrana_server, prev_server)
      restore_env(:allow_shard_override, prev_override)
      restore_env(:default_shard, prev_default)
      restore_env(:hrana_auth, prev_auth)
      restore_env(:hrana_bind_ip, prev_bind)
    end)

    # env :test (not :prod) + every risky config set at once.
    Application.put_env(:fathom, :env, :test)
    Application.put_env(:fathom, :shard_storage, Fathom.Shard.Storage.Local)
    Application.put_env(:fathom, :lb_backends, %{"fathom1" => "fathom1:8080"})
    Application.delete_env(:fathom, :shard_base_domain)
    Application.put_env(:fathom, :hrana_server, true)
    Application.put_env(:fathom, :allow_shard_override, true)
    Application.put_env(:fathom, :default_shard, "shared")
    Application.put_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :hrana_bind_ip, {0, 0, 0, 0})

    # No raise from the RAISE guards…
    assert Fathom.Application.check_local_storage_fleet!() == nil
    assert Fathom.Application.check_shard_base_domain!() == nil
    assert Fathom.Application.check_shard_override!() == nil

    # …and no warning from the WARN guards.
    log =
      capture_log(fn ->
        assert Fathom.Application.check_default_shard!() == nil
        assert Fathom.Application.check_hrana_exposure!() == nil
      end)

    refute log =~ "commingles"
    refute log =~ "firewall"
  end
end
