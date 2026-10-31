defmodule Fathom.Shard.Storage.S3TokenFloorBudgetTest do
  @moduledoc """
  Expert review 2026-10-29 #20. `S3.read_token_floor/1` was a bare `Req.get`, so it inherited the
  shared request defaults: `:safe_transient` retries (a GET) and a 15 s `receive_timeout`. On the
  `hello` path that held a client ~60 s against a stalled store.

  `read_token_floor/2` with `budget_ms:` is ONE attempt whose wire time is capped at the budget. The
  options are per-call: `build_req/1` is shared with lease/flush/heartbeat traffic and untouched, so
  `read_token_floor/1` (the DR sweep) keeps the retrying default. A real Bandit listener, not a
  `req_plug`, because only the Finch adapter honours `receive_timeout`.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Storage.S3

  defmodule Stub do
    @moduledoc false
    @behaviour Plug

    @impl true
    def init(opts), do: opts

    @impl true
    def call(conn, %{test: test, mode: mode}) do
      send(test, {:floor_get, conn.request_path})

      case mode do
        {:status, status, body} ->
          Plug.Conn.send_resp(conn, status, body)

        :stall ->
          receive do
            :never -> conn
          end
      end
    end
  end

  defp start_stub(mode) do
    bandit =
      start_supervised!(
        {Bandit,
         plug: {Stub, %{test: self(), mode: mode}}, port: 0, ip: :loopback, startup_log: false}
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(bandit)
    prev = Application.get_env(:fathom, S3)

    Application.put_env(:fathom, S3,
      bucket: "b",
      region: "us-east-1",
      access_key_id: "k",
      secret_access_key: "s",
      endpoint: "http://127.0.0.1:#{port}",
      path_style: true,
      prefix: "shards/"
    )

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, S3, prev),
        else: Application.delete_env(:fathom, S3)
    end)

    assert is_pid(Process.whereis(S3.finch_name())), "the dedicated pool must be up"
  end

  test "a stalled store is abandoned at the budget after exactly one request" do
    start_stub(:stall)

    {us, result} = :timer.tc(fn -> S3.read_token_floor("shard-a", budget_ms: 150) end)

    assert {:error, _} = result

    assert us < 1_000_000,
           "the budget (150 ms) must bound the read, not Req's 15 s x retries: #{us}"

    assert_received {:floor_get, "/b/shards/tokenfloors/shard-a"}
    refute_received {:floor_get, _}, "a budgeted read must not retry"
  end

  test "a retryable status is NOT retried under a budget" do
    start_stub({:status, 503, "slow down"})

    assert {:error, {:s3_get_status, 503}} = S3.read_token_floor("shard-a", budget_ms: 2_000)

    assert_received {:floor_get, _}

    refute_received {:floor_get, _},
                    "Req's :safe_transient retry (3 more GETs) must be off for the budgeted read"
  end

  test "the budgeted read still parses the floor" do
    start_stub({:status, 200, "7\n"})
    assert {:ok, 7} = S3.read_token_floor("shard-a", budget_ms: 2_000)
  end

  test "a missing object under a budget is nil" do
    start_stub({:status, 404, ""})
    assert {:ok, nil} = S3.read_token_floor("shard-a", budget_ms: 2_000)
  end
end
