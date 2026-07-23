defmodule FathomWeb.ThrottleTest do
  @moduledoc """
  Control-plane abuse throttles (expert review #34): the admin BasicAuth brute-force lockout and the
  `/api` per-IP request-rate limit. Both are config-gated (off by default); each test enables its
  limit for its own scope and resets the node-global limiter table. ConnTest dispatches in the test
  process, so all requests share the `{127,0,0,1}` source IP — which is exactly what lets a burst
  trip the per-IP gate. Not async: global config + the shared limiter table.
  """
  use FathomWeb.ConnCase, async: false

  alias Fathom.RateLimiter

  setup do
    RateLimiter.reset()
    prev_admin = Application.get_env(:fathom, :admin_auth_max_failures)
    prev_api = Application.get_env(:fathom, :api_rate_limit)

    on_exit(fn ->
      restore(:admin_auth_max_failures, prev_admin)
      restore(:api_rate_limit, prev_api)
      RateLimiter.reset()
    end)

    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:fathom, key)
  defp restore(key, val), do: Application.put_env(:fathom, key, val)

  defp basic(conn, user, pass),
    do:
      Plug.Conn.put_req_header(
        conn,
        "authorization",
        "Basic " <> Base.encode64("#{user}:#{pass}")
      )

  test "admin BasicAuth locks out a source IP after the failure threshold (429)", %{conn: conn} do
    Application.put_env(:fathom, :admin_auth_max_failures, 3)

    # Three bad-password attempts each get the ordinary 401 challenge ...
    for _ <- 1..3 do
      assert conn |> basic("admin", "wrong") |> get("/admin/metrics") |> Map.get(:status) == 401
    end

    # ... the fourth is locked out (429), refused BEFORE the credential is checked ...
    assert conn |> basic("admin", "wrong") |> get("/admin/metrics") |> Map.get(:status) == 429

    # ... and even the CORRECT credential is refused while the IP is locked out.
    assert conn |> basic("admin", "secret") |> get("/admin/metrics") |> Map.get(:status) == 429,
           "the lockout precedes the credential check, so brute force can't slip a hit through"
  end

  test "a successful admin auth clears the failure count", %{conn: conn} do
    Application.put_env(:fathom, :admin_auth_max_failures, 3)

    # Two failures (below the 3-threshold) ...
    for _ <- 1..2 do
      assert conn |> basic("admin", "wrong") |> get("/admin/metrics") |> Map.get(:status) == 401
    end

    # ... then a success resets the IP's counter ...
    assert conn |> basic("admin", "secret") |> get("/admin/metrics") |> Map.get(:status) == 200

    # ... so two more failures don't reach the threshold — still 401, not 429.
    assert conn |> basic("admin", "wrong") |> get("/admin/metrics") |> Map.get(:status) == 401
    assert conn |> basic("admin", "wrong") |> get("/admin/metrics") |> Map.get(:status) == 401
  end

  test "the lockout transition is audited exactly once (#9), never per-attempt", %{conn: conn} do
    Application.put_env(:fathom, :admin_auth_max_failures, 2)

    # Four bad attempts: #2 crosses the threshold (audited), #3/#4 are already-locked-out 429s.
    for _ <- 1..4, do: conn |> basic("admin", "wrong") |> get("/admin/metrics")

    locked =
      Fathom.Audit.list(limit: 100)
      |> Enum.filter(&(&1.action == "admin_auth_locked_out"))

    assert length(locked) == 1,
           "only the threshold-crossing attempt writes an audit row (a flood must not amplify into audit writes)"

    assert hd(locked).outcome == "blocked"
  end

  test "the /api control plane rate-limits per IP (429 over budget), before auth", %{conn: conn} do
    Application.put_env(:fathom, :api_rate_limit, 3)

    # Three requests pass the rate gate (they 401 for lack of a credential, but still count) ...
    for _ <- 1..3 do
      assert conn |> get("/api/tenants") |> Map.get(:status) == 401
    end

    # ... the fourth is rate-limited (429), refused before auth even runs.
    assert conn |> get("/api/tenants") |> Map.get(:status) == 429
  end

  test "with the throttles unconfigured (default), repeated failures never lock out", %{
    conn: conn
  } do
    # No :admin_auth_max_failures set ⇒ the throttle is inert: every attempt is the ordinary 401.
    for _ <- 1..8 do
      assert conn |> basic("admin", "wrong") |> get("/admin/metrics") |> Map.get(:status) == 401
    end
  end
end
