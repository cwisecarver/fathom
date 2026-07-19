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

  @remote_dir Path.join(System.tmp_dir!(), "fathom_remote_test")
  @streams __MODULE__.Streams

  setup do
    prev_mode = Application.get_env(:fathom, :hrana_auth, :disabled)
    Application.put_env(:fathom, :hrana_auth, :required)
    shard = "ro_#{System.unique_integer([:positive])}"
    start_supervised!({Filo.Streams, name: @streams})

    on_exit(fn ->
      Application.put_env(:fathom, :hrana_auth, prev_mode)
      Shards.drain(shard, 2_000)

      for dir <- [@remote_dir, Path.join(System.tmp_dir!(), "fathom_shards")],
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
end
