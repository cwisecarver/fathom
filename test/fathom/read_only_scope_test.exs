defmodule Fathom.ReadOnlyScopeTest do
  @moduledoc """
  Read-only token scope (expert review 2026-07-14 #24; carrier hardened by audit 2026-07-18 #3):
  a `ro` token may read, but every write (DML or DDL) is refused with a distinct 403
  `FILO_READONLY`.

  ## Why the real transports (the I3 gap #3 closed)

  The scope used to travel from `authorize` to `open` through a per-process side-channel (a
  process dictionary). That silently failed on the two real paths:

    * **HTTP** — Filo opens the stream in its OWN process (a `Filo.Stream` GenServer), a different
      process than the one `authorize` ran in, so the dict was empty and a `ro` token got full
      write access.
    * **WebSocket** — `authorize` runs once at `hello` and the read CONSUMED the value, so only the
      first stream on a connection was `ro`; the 2nd escalated to `rw`.

  The scope now flows as Filo's authorize context (`Filo.Executor.open/2`). So these tests drive
  the REAL transports — the HTTP pipeline (`Filo.Plug`) and a WS `hello` with two streams — not a
  same-process shortcut, which is exactly where the bug hid.

  DataCase (async: false): flips `:hrana_auth`, uses the directory + Revocations cache and real
  shard files.
  """
  use Fathom.DataCase, async: false

  import Plug.Test
  import Plug.Conn

  alias Fathom.{Directory, HranaAuth, Shards, ShardExecutor}
  alias Filo.Stmt

  @streams __MODULE__.Streams

  setup do
    prev_mode = Application.get_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :hrana_auth, :required)
    shard = "ro_#{System.unique_integer([:positive])}"
    start_supervised!({Filo.Streams, name: @streams})

    on_exit(fn ->
      Application.put_env(:fathom, :hrana_auth, prev_mode)
      Shards.drain(shard, 2_000)

      for dir <- [remote_dir(), Fathom.Shard.data_dir()],
          path <- Path.wildcard(Path.join(dir, "#{shard}*")),
          do: File.rm(path)
    end)

    %{shard: shard}
  end

  defp stmt(sql, args \\ []), do: %Stmt{sql: sql, args: args}

  # Open a stream the way Filo does: authorize returns the token's scope, open rides it (Filo
  # threads it via open/2). No process-dict shortcut — the scope is an explicit argument.
  defp open_stream!(shard, token) do
    assert {:ok, scope} = HranaAuth.authorize(shard, token)
    {:ok, handle} = ShardExecutor.open(shard, scope)
    handle
  end

  # Seed a table on `shard` via a full-access stream, so a later ro write has a target.
  defp seed_table!(shard) do
    {:ok, _} = Directory.resolve(shard)
    {:ok, full} = HranaAuth.token_for(shard)
    h = open_stream!(shard, full)
    {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE t (v TEXT)"))
    {:ok, _} = ShardExecutor.execute(h, stmt("INSERT INTO t VALUES ('x')"))
    :ok = ShardExecutor.close(h)
  end

  # --- the context-less open/1 fails closed under auth (audit 2026-08-31 #9) ---

  # open/1 carries no auth context, so it cannot honour :ro or re-check a revoked token — its
  # token_version is nil, which makes the revocation floor pass unconditionally. Pre-fix it degraded
  # to full :rw next to the wire path; now, with auth required, it is refused. (Setup sets
  # :hrana_auth to :required.)
  test "open/1 with no auth context is refused when auth is required", %{shard: shard} do
    assert {:error, %Filo.Error{status: 401, code: "FILO_NO_AUTH_CONTEXT"}} =
             ShardExecutor.open(shard)
  end

  # Internal callers (migration, rpo, harnesses) keep full access through the explicit :trusted
  # context — the escalation only closes for the context-less arity-1 path.
  test "an explicit :trusted open still gets full :rw even when auth is required", %{shard: shard} do
    assert {:ok, {_pid, _ref, _conn, ^shard, :rw, nil, _opts} = handle} =
             ShardExecutor.open(shard, :trusted)

    :ok = ShardExecutor.close(handle)
  end

  # --- executor-level scope enforcement (scope threaded to open/2, not a same-process stash) ---

  test "a read-only token can SELECT but not write", %{shard: shard} do
    seed_table!(shard)

    {:ok, ro} = HranaAuth.token_for(shard, scope: :ro)
    h = open_stream!(shard, ro)

    assert {:ok, res} = ShardExecutor.execute(h, stmt("SELECT v FROM t"))
    assert res.rows == [["x"]]

    assert {:error, %Filo.Error{status: 403, code: "FILO_READONLY"}} =
             ShardExecutor.execute(h, stmt("INSERT INTO t VALUES ('y')"))

    assert {:error, %Filo.Error{status: 403, code: "FILO_READONLY"}} =
             ShardExecutor.execute(h, stmt("UPDATE t SET v = 'z'"))

    assert {:error, %Filo.Error{status: 403, code: "FILO_READONLY"}} =
             ShardExecutor.execute(h, stmt("CREATE TABLE u (v TEXT)"))

    # The refused writes really didn't land.
    assert {:ok, %{rows: [["x"]]}} = ShardExecutor.execute(h, stmt("SELECT v FROM t"))
    :ok = ShardExecutor.close(h)
  end

  test "a full (rw) token — the default — writes normally", %{shard: shard} do
    {:ok, _} = Directory.resolve(shard)
    {:ok, full} = HranaAuth.token_for(shard)

    h = open_stream!(shard, full)
    assert {:ok, _} = ShardExecutor.execute(h, stmt("CREATE TABLE t (v TEXT)"))
    assert {:ok, _} = ShardExecutor.execute(h, stmt("INSERT INTO t VALUES ('ok')"))
    :ok = ShardExecutor.close(h)
  end

  # --- real transport: the HTTP pipeline (the cross-process path where the bug hid) ---

  describe "over the real HTTP pipeline (Filo.Plug)" do
    setup %{shard: shard} do
      seed_table!(shard)

      opts =
        Filo.Plug.init(
          executor: Fathom.ShardExecutor,
          streams: @streams,
          key: Filo.Baton.new_key(),
          open_arg: &Fathom.ShardExecutor.shard_from_conn/1,
          authorize: &Fathom.HranaAuth.authorize/2
        )

      %{opts: opts}
    end

    # The shard rides the Host subdomain — the production routing path.
    defp pipeline(opts, shard, sql, token) do
      body = %{"baton" => nil, "requests" => [%{"type" => "execute", "stmt" => %{"sql" => sql}}]}

      conn(:post, "http://#{shard}.fathom.test/v3/pipeline", Jason.encode!(body))
      |> put_req_header("content-type", "application/json")
      |> put_req_header("authorization", "Bearer #{token}")
      |> Filo.Plug.call(opts)
    end

    test "a ro token reads but its write is refused FILO_READONLY (pre-#3: silently wrote)",
         %{opts: opts, shard: shard} do
      {:ok, ro} = HranaAuth.token_for(shard, scope: :ro)

      # A read works — proving the ro token opens a stream at all.
      conn = pipeline(opts, shard, "SELECT v FROM t", ro)
      assert conn.status == 200
      assert [%{"type" => "ok"}] = Jason.decode!(conn.resp_body)["results"]

      # A write is refused. The stream opened in its OWN process, so a process-dict scope would be
      # empty there and the write would have run (the audit #3 Critical). It must be refused.
      conn = pipeline(opts, shard, "INSERT INTO t VALUES ('leak')", ro)
      assert conn.status == 200

      assert [%{"type" => "error", "error" => %{"code" => "FILO_READONLY"}}] =
               Jason.decode!(conn.resp_body)["results"]

      # And the refused write really didn't land: still one row.
      conn = pipeline(opts, shard, "SELECT count(*) FROM t", ro)

      assert [%{"type" => "ok", "response" => %{"result" => %{"rows" => [[cell]]}}}] =
               Jason.decode!(conn.resp_body)["results"]

      assert cell == %{"type" => "integer", "value" => "1"}
    end

    test "a full token writes normally over the same pipeline", %{opts: opts, shard: shard} do
      {:ok, full} = HranaAuth.token_for(shard)
      conn = pipeline(opts, shard, "INSERT INTO t VALUES ('ok')", full)

      assert conn.status == 200
      assert [%{"type" => "ok"}] = Jason.decode!(conn.resp_body)["results"]
    end
  end

  # --- real transport: WebSocket, two streams on one hello (the escalation the bug allowed) ---

  test "a ro WS connection refuses writes on BOTH the 1st and 2nd stream", %{shard: shard} do
    seed_table!(shard)
    {:ok, ro} = HranaAuth.token_for(shard, scope: :ro)

    {:ok, state} =
      Filo.Socket.init(
        executor: Fathom.ShardExecutor,
        open_arg: shard,
        authorize: &Fathom.HranaAuth.authorize/2
      )

    # hello carries the ro jwt (django-libsql's path — the token is not an upgrade header).
    {%{"type" => "hello_ok"}, state} = ws(state, %{"type" => "hello", "jwt" => ro})

    # Two streams on the SAME hello-authorized connection. Pre-#3 the read CONSUMED the scope, so
    # the 2nd stream defaulted to rw and could write. Both must refuse now.
    state = assert_ro_write_refused(state, 1)
    _state = assert_ro_write_refused(state, 2)
  end

  # THE SCOPE MUST REST ON THE ENGINE, NOT ON A PRAGMA (expert review 2026-08-24 #3, verified by
  # execution). `Connection.open_handle/2` falls back to a read-write handle guarded only by
  # `PRAGMA query_only=ON` whenever a `mode: :readonly` open fails. Its comment justified that as
  # "the narrow case where a `-wal` needs recovery" — but the far more common trigger is that the
  # FILE DOES NOT EXIST: `Sqlite3.open(missing, mode: :readonly)` returns
  # `{:error, :database_open_failed}` and exqlite only creates a shard file on first connection
  # open. So every brand-new tenant, and every shard dropped clean while storage held nothing,
  # handed a `:ro` token a read-write handle. Measured before the fix:
  #
  #     ShardExecutor.open(id, {:ro, nil}) on a never-opened shard -> scope :ro, query_only = 1
  #     PRAGMA main . query_only = OFF                             -> query_only = 0
  #     CREATE TABLE written_by_ro (x)                             -> :ok
  #
  # Driven at the `Connection` level ON PURPOSE. `ShardExecutor`'s leading-keyword scope check
  # refuses a `:ro` write before SQLite ever sees it, so a test through the executor passes
  # whether or not the handle is genuinely read-only — it cannot discriminate. This asserts the
  # engine's own enforcement, with the defeatable pragma explicitly turned off first.
  test "a :ro handle on a shard with NO local file is still refused by SQLite itself", %{
    shard: shard
  } do
    path = Path.join(Fathom.Shard.data_dir(), "#{shard}.db")

    refute File.exists?(path),
           "the fixture must start with no local shard file — its absence IS the trigger"

    {:ok, conn} = Fathom.Shard.Connection.open(path, tenant?: true, scope: :ro)
    on_exit(fn -> Fathom.Shard.Connection.close(conn) end)

    # Defeat the fallback belt the way a tenant would, then write anyway. On a genuinely
    # read-only handle this changes nothing; on the rw-plus-query_only fallback it was the
    # whole scope.
    _ = Fathom.Shard.Connection.exec(conn, "PRAGMA query_only=OFF")

    assert {:error, _} =
             Fathom.Shard.Connection.query(conn, "CREATE TABLE written_by_ro (x)", [], dml?: true),
           "a :ro handle created a table — the read-only scope rested on PRAGMA query_only, " <>
             "which the tenant can turn off, rather than on SQLite's own enforcement"
  end

  # Opens WS stream `sid`, attempts a write, asserts it is refused FILO_READONLY, returns the state.
  defp assert_ro_write_refused(state, sid) do
    {%{"response" => %{"type" => "open_stream"}}, state} =
      ws(state, req(sid * 10, %{"type" => "open_stream", "stream_id" => sid}))

    {resp, state} =
      ws(
        state,
        req(sid * 10 + 1, %{
          "type" => "execute",
          "stream_id" => sid,
          "stmt" => %{"sql" => "INSERT INTO t VALUES ('leak')"}
        })
      )

    assert resp["type"] == "response_error", "stream #{sid} write should be refused"
    assert resp["error"]["code"] == "FILO_READONLY"
    state
  end

  defp ws(state, msg) do
    {:push, {:text, json}, new_state} =
      Filo.Socket.handle_in({Jason.encode!(msg), [opcode: :text]}, state)

    {Jason.decode!(json), new_state}
  end

  defp req(id, request), do: %{"type" => "request", "request_id" => id, "request" => request}

  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
