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
end
