defmodule Fathom.Shard.Storage.S3FinchDeprecationTest do
  # Expert review 2026-10-19 #3. `finch: @finch` (an atom pool name) and a top-level
  # `pool_timeout:` are both deprecated in Req 0.7.4, and each made Req `IO.warn` WITH a
  # stacktrace (~118 us CPU) on every S3 request: ~6,000 warnings in one 1024-tenant rig log,
  # ~350 us on a cold open. The warning is emitted by Req's Finch adapter, so only a request
  # that really goes through Finch can show it (a `req_plug` bypasses the adapter and hides it).
  # Invariant: a request through the dedicated pool emits no Req deprecation warning, and still
  # uses that pool.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Fathom.Shard.Storage.S3

  defmodule Stub do
    @behaviour Plug
    @impl true
    def init(opts), do: opts
    @impl true
    def call(conn, _opts), do: Plug.Conn.send_resp(conn, 200, "")
  end

  setup do
    bandit = start_supervised!({Bandit, plug: Stub, port: 0, ip: :loopback, startup_log: false})
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
    :ok
  end

  test "object_head and a budgeted heartbeat renew emit no Req deprecation warning" do
    err =
      capture_io(:stderr, fn ->
        assert {:ok, _} = S3.object_head("shard-a")
        assert {:ok, _, _} = S3.renew_heartbeat("node@h", 30_000, budget_ms: 2_000)
      end)

    refute err =~ "deprecated", "Req deprecation warning on the S3 request path: #{err}"
  end
end
