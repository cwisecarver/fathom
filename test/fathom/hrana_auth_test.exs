defmodule Fathom.HranaAuthTest do
  # Pins the in-app Hrana auth contract (the real fix for "isolation is network-only",
  # fable-review #3/#4): with :hrana_auth :required, a stream open must present a
  # Phoenix.Token for the shard it names — a token for shard A must NEVER open shard B
  # (the shard-isolation gate). Not async: flips :hrana_auth / :hrana_token_max_age
  # app env, and the end-to-end tests open real shard files.
  use ExUnit.Case, async: false

  # These tests mint tokens from processes the Ecto sandbox does not own (the real Hrana pipeline
  # spawns its own), so the #37 issuance ledger correctly reports it could not record those mints —
  # once per mint. That warning is right in production and pure noise here, and ExUnit still prints
  # captured logs for a test that FAILS, so this hides the noise without hiding the signal.
  @moduletag :capture_log

  import Plug.Test
  import Plug.Conn

  alias Fathom.HranaAuth

  setup do
    prev_mode = Application.get_env(:fathom, :hrana_auth, :disabled)
    prev_age = Application.get_env(:fathom, :hrana_token_max_age, :infinity)

    on_exit(fn ->
      Application.put_env(:fathom, :hrana_auth, prev_mode)
      Application.put_env(:fathom, :hrana_token_max_age, prev_age)
    end)

    :ok
  end

  defp require_auth!, do: Application.put_env(:fathom, :hrana_auth, :required)

  defp token!(shard, opts \\ []) do
    {:ok, token} = HranaAuth.token_for(shard, opts)
    token
  end

  # --- authorize/2 ---

  test ":disabled (the default) accepts everything, token or not" do
    Application.put_env(:fathom, :hrana_auth, :disabled)
    assert {:ok, {:rw, _}} = HranaAuth.authorize("demo", nil)
    assert {:ok, {:rw, _}} = HranaAuth.authorize("demo", "garbage")
  end

  test ":required accepts a token minted for the same shard" do
    require_auth!()
    assert {:ok, {:rw, _}} = HranaAuth.authorize("acme", token!("acme"))
  end

  test ":required refuses a missing token with 401" do
    require_auth!()

    assert {:error, %Filo.Error{status: 401, code: "AUTH_REQUIRED", message: "missing" <> _}} =
             HranaAuth.authorize("acme", nil)
  end

  test ":required refuses a garbage token with 401" do
    require_auth!()
    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize("acme", "not-a-token")
  end

  test "a token for shard A must never authorize shard B (shard-isolation gate)" do
    require_auth!()
    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize("intruder", token!("acme"))
  end

  test "grant matching is case-canonical: a token minted for ACME authorizes acme (#19)" do
    require_auth!()
    token = token!("ACME")
    assert {:ok, {:rw, _}} = HranaAuth.authorize("acme", token)
    assert {:ok, {:rw, _}} = HranaAuth.authorize("ACME", token)
  end

  test "an expired token is refused when :hrana_token_max_age is set" do
    require_auth!()
    Application.put_env(:fathom, :hrana_token_max_age, 60)
    stale = token!("acme", signed_at: System.system_time(:second) - 3600)

    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize("acme", stale)
    # A fresh token under the same max_age still passes.
    assert {:ok, {:rw, _}} = HranaAuth.authorize("acme", token!("acme"))
  end

  test "a nil shard passes through so open(nil) keeps its fail-closed 400 (#26)" do
    require_auth!()
    # {:ok, :rw} — the scope is moot; open(nil, _) refuses with a 400 regardless.
    assert {:ok, {:rw, _}} = HranaAuth.authorize(nil, nil)
  end

  test "an invalid shard id is refused even with a well-signed token (defense-in-depth)" do
    require_auth!()
    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize("../evil", token!("acme"))
  end

  test "an unrecognized :hrana_auth value fails closed to :required" do
    Application.put_env(:fathom, :hrana_auth, "off")
    assert {:error, %Filo.Error{status: 401}} = HranaAuth.authorize("acme", nil)
  end

  # --- token_for/2 ---

  test "token_for refuses an invalid shard id" do
    assert {:error, :invalid_shard_id} = HranaAuth.token_for("../evil")
    assert {:error, :invalid_shard_id} = HranaAuth.token_for(nil)
  end

  # --- check_config!/0 (boot guard) ---

  test "check_config! passes for both valid modes" do
    Application.put_env(:fathom, :hrana_auth, :disabled)
    assert :ok = HranaAuth.check_config!()
    require_auth!()
    assert :ok = HranaAuth.check_config!()
  end

  test "check_config! refuses a nonsense mode at boot" do
    Application.put_env(:fathom, :hrana_auth, :off)
    assert_raise RuntimeError, ~r/:hrana_auth must be/, fn -> HranaAuth.check_config!() end
  end

  test "check_config! refuses :required without a usable secret" do
    require_auth!()
    prev = Application.get_env(:fathom, FathomWeb.Endpoint)
    on_exit(fn -> Application.put_env(:fathom, FathomWeb.Endpoint, prev) end)

    Application.put_env(:fathom, FathomWeb.Endpoint, Keyword.delete(prev, :secret_key_base))
    assert_raise RuntimeError, ~r/secret_key_base/, fn -> HranaAuth.check_config!() end

    Application.put_env(:fathom, FathomWeb.Endpoint, Keyword.put(prev, :secret_key_base, "shrt"))
    assert_raise RuntimeError, ~r/too short/, fn -> HranaAuth.check_config!() end
  end

  # Round-2 #36: the moduledoc has always promised a boot warning for the
  # secret_key_base fallback, but secret! was a silent || chain — running
  # :required on the fallback couples the data-path credential to web
  # session/CSRF signing, so a routine web SECRET_KEY_BASE rotation invalidates
  # every outstanding Hrana token with no hint why.
  test "check_config! warns when :required runs on the secret_key_base fallback" do
    import ExUnit.CaptureLog

    require_auth!()
    prev = Application.get_env(:fathom, :hrana_token_secret)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :hrana_token_secret),
        else: Application.put_env(:fathom, :hrana_token_secret, prev)
    end)

    Application.delete_env(:fathom, :hrana_token_secret)

    assert capture_log(fn -> assert :ok = HranaAuth.check_config!() end) =~
             "no :hrana_token_secret",
           "the documented fallback boot warning must fire"

    # With the dedicated secret set, no warning.
    Application.put_env(:fathom, :hrana_token_secret, String.duplicate("a", 64))
    refute capture_log(fn -> assert :ok = HranaAuth.check_config!() end) =~ "hrana_token_secret"
  end

  # --- end to end: Filo.Plug -> shard_from_conn -> HranaAuth -> ShardExecutor ---

  describe "through the real Hrana pipeline" do
    @streams __MODULE__.Streams

    setup do
      require_auth!()
      shard = "test_auth_#{System.unique_integer([:positive])}"
      start_supervised!({Filo.Streams, name: @streams})

      on_exit(fn ->
        local = Path.join([Fathom.Shard.data_dir(), "#{shard}.db"])
        remote = Path.join([Fathom.Shard.Storage.Local.dir(), "#{shard}.db"])
        for base <- [local, remote], suffix <- ["", "-wal", "-shm"], do: File.rm(base <> suffix)
      end)

      opts =
        Filo.Plug.init(
          executor: Fathom.ShardExecutor,
          streams: @streams,
          key: Filo.Baton.new_key(),
          open_arg: &Fathom.ShardExecutor.shard_from_conn/1,
          authorize: &Fathom.HranaAuth.authorize/2
        )

      %{opts: opts, shard: shard}
    end

    defp pipeline(opts, shard, headers) do
      body = %{
        "baton" => nil,
        "requests" => [%{"type" => "execute", "stmt" => %{"sql" => "SELECT 1"}}]
      }

      # The shard rides the Host subdomain — the production routing path.
      Enum.reduce(
        headers,
        conn(:post, "http://#{shard}.fathom.test/v3/pipeline", Jason.encode!(body))
        |> put_req_header("content-type", "application/json"),
        fn {k, v}, conn -> put_req_header(conn, k, v) end
      )
      |> Filo.Plug.call(opts)
    end

    test "a request with the shard's token runs SQL", %{opts: opts, shard: shard} do
      conn = pipeline(opts, shard, [{"authorization", "Bearer #{token!(shard)}"}])

      assert conn.status == 200
      assert [%{"type" => "ok"}] = Jason.decode!(conn.resp_body)["results"]
    end

    test "a request with no token is refused 401 before any shard opens",
         %{opts: opts, shard: shard} do
      conn = pipeline(opts, shard, [])

      assert conn.status == 401
      # The refusal happened before the executor: no shard file was created.
      refute File.exists?(Path.join([Fathom.Shard.data_dir(), "#{shard}.db"]))
    end

    test "a token for another shard is refused 401 (isolation gate, end to end)",
         %{opts: opts, shard: shard} do
      conn = pipeline(opts, shard, [{"authorization", "Bearer #{token!("acme")}"}])

      assert conn.status == 401
      refute File.exists?(Path.join([Fathom.Shard.data_dir(), "#{shard}.db"]))
    end

    test "WebSocket hello authenticates via the jwt field (django-libsql's path)",
         %{shard: shard} do
      {:ok, state} =
        Filo.Socket.init(
          executor: Fathom.ShardExecutor,
          open_arg: shard,
          authorize: &Fathom.HranaAuth.authorize/2
        )

      hello = fn jwt ->
        Filo.Socket.handle_in(
          {Jason.encode!(%{"type" => "hello", "jwt" => jwt}), [opcode: :text]},
          state
        )
      end

      assert {:push, {:text, ok_json}, _} = hello.(token!(shard))
      assert %{"type" => "hello_ok"} = Jason.decode!(ok_json)

      assert {:stop, :normal, {1008, _}, [{:text, err_json}], _} = hello.(token!("acme"))
      assert %{"type" => "hello_error"} = Jason.decode!(err_json)
    end
  end

  # REVOCATION MUST REACH AN ALREADY-OPEN CONNECTION (expert review 2026-08-20 #22).
  #
  # `authorize/2` runs exactly ONCE per Hrana WebSocket, at `hello`. Every later `open_stream`
  # reuses the stashed context; `Filo.Socket` has no idle timeout and no maximum connection
  # lifetime; and AGENTS.md records that a django-libsql stream "lives for hours between requests".
  # So `revoke/1` — documented as IMMEDIATE, the compromise-response path — stopped only NEW
  # connections. An attacker holding a stolen token's socket kept reading and writing indefinitely
  # while every dashboard and ledger row said the credential was revoked.
  #
  # These drive the EXECUTOR handle directly rather than the socket, because that is where the
  # stashed context lives and where the re-check had to go. `Fathom.Shards.ensure/1` already
  # re-checks the tombstone and suspension gates on every checkout, so those two lifecycle denies
  # already reached a live connection; this closes the third.
  describe "a revoked token stops working on a connection that is already open (#22)" do
    setup do
      require_auth!()
      shard = "revoke_live_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        Fathom.Shards.drain(shard, 5_000)

        for dir <- [Fathom.Shard.data_dir(), Fathom.Shard.Storage.Local.dir()],
            suffix <- ["", "-wal", "-shm", ".etag"] do
          File.rm(Path.join(dir, "#{shard}.db") <> suffix)
        end
      end)

      %{shard: shard}
    end

    test "reads and writes are refused after a revoke, on the SAME handle", %{shard: shard} do
      {:ok, token} = HranaAuth.token_for(shard)
      {:ok, {scope, version}} = HranaAuth.authorize(shard, token)
      assert is_integer(version), "the token carried no version; the re-check cannot run"

      {:ok, handle} = Fathom.ShardExecutor.open(shard, {scope, version})

      # Working normally before the revoke.
      assert {:ok, _} =
               Fathom.ShardExecutor.execute(handle, %Filo.Stmt{
                 sql: "CREATE TABLE t (v TEXT)",
                 args: []
               })

      assert {:ok, _} =
               Fathom.ShardExecutor.execute(handle, %Filo.Stmt{sql: "SELECT 1", args: []})

      # `revoke/1` bumps the DIRECTORY (Postgres) and then raises this cache floor. This module has
      # no Repo sandbox, and the directory half is already covered by the revoke tests above — so
      # raise the floor directly, which is exactly what revoke/1 does internally and is the only
      # part the executor's re-check consults.
      Fathom.HranaAuth.Revocations.put(shard, version + 1, nil)

      # THE ASSERTION. Same handle, same connection, no re-authorization anywhere.
      assert {:error, %Filo.Error{status: 401}} =
               Fathom.ShardExecutor.execute(handle, %Filo.Stmt{
                 sql: "INSERT INTO t VALUES ('x')",
                 args: []
               }),
             "a WRITE succeeded on a revoked token's open connection"

      # Reads too: unlike the write fence, which is about durability and deliberately lets reads
      # through, a revoked CREDENTIAL must not keep reading a tenant's data either.
      assert {:error, %Filo.Error{status: 401}} =
               Fathom.ShardExecutor.execute(handle, %Filo.Stmt{sql: "SELECT 1", args: []}),
             "a READ succeeded on a revoked token's open connection"

      # And a script is not a loophole.
      assert {:error, %Filo.Error{status: 401}} =
               Fathom.ShardExecutor.execute_sequence(handle, "SELECT 1; SELECT 2")

      :ok = Fathom.ShardExecutor.close(handle)
    end

    test "the floor denies BELOW it, not everything", %{shard: shard} do
      # Opened with explicit versions rather than minted tokens: `token_for/2` reads the DIRECTORY
      # for its version and this module has no Repo sandbox, so a "fresh" mint would carry version
      # 1 and the test would prove the opposite of what it claims. The property under test is the
      # floor COMPARISON, and these are the two sides of it.
      floor = 7
      Fathom.HranaAuth.Revocations.put(shard, floor, nil)

      {:ok, stale} = Fathom.ShardExecutor.open(shard, {:rw, floor - 1})
      {:ok, current} = Fathom.ShardExecutor.open(shard, {:rw, floor})
      {:ok, newer} = Fathom.ShardExecutor.open(shard, {:rw, floor + 1})

      assert {:error, %Filo.Error{status: 401}} =
               Fathom.ShardExecutor.execute(stale, %Filo.Stmt{sql: "SELECT 1", args: []})

      for {label, handle} <- [{"at the floor", current}, {"above the floor", newer}] do
        assert {:ok, _} =
                 Fathom.ShardExecutor.execute(handle, %Filo.Stmt{sql: "SELECT 1", args: []}),
               "a token #{label} was refused — the re-check is reading the floor as " <>
                 "'deny everything' rather than 'deny below the floor', which would take every " <>
                 "tenant down on the first revoke"
      end

      for h <- [stale, current, newer], do: :ok = Fathom.ShardExecutor.close(h)
    end
  end
end
