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

  describe "#35 — the buckets are per CLIENT, not per proxy" do
    # Both throttles are documented as PER-IP and are on by default in prod, but they keyed on
    # conn.remote_ip — the PROXY behind any proxy, and config/prod.exs's
    # `force_ssl: [rewrite_on: [:x_forwarded_proto]]` says a proxy is expected. So every client
    # shared one bucket: ONE attacker's failed logins locked out EVERY operator, and the lockout
    # is checked before credentials are verified, so no valid password gets anyone back in.
    #
    # These drive the real router, so they fail on the pre-fix client_ip/1.
    setup do
      prev = Application.get_env(:fathom, :trusted_proxies)

      on_exit(fn ->
        if prev,
          do: Application.put_env(:fathom, :trusted_proxies, prev),
          else: Application.delete_env(:fathom, :trusted_proxies)
      end)

      # ConnTest dispatches with remote_ip {127,0,0,1}; trust it so the header is honoured.
      Application.put_env(:fathom, :trusted_proxies, ["127.0.0.1"])
      :ok
    end

    defp via_proxy(conn, client),
      do: Plug.Conn.put_req_header(conn, "x-forwarded-for", client)

    test "one client's lockout does not lock out another behind the same proxy", %{conn: conn} do
      Application.put_env(:fathom, :admin_auth_max_failures, 3)

      for _ <- 1..3 do
        assert conn
               |> via_proxy("203.0.113.9")
               |> basic("admin", "wrong")
               |> get("/admin/metrics")
               |> Map.get(:status) == 401
      end

      assert conn
             |> via_proxy("203.0.113.9")
             |> basic("admin", "wrong")
             |> get("/admin/metrics")
             |> Map.get(:status) == 429,
             "the attacker must still be locked out"

      # The operator, a different client through the same proxy, is unaffected. Pre-fix both
      # keyed on {127,0,0,1} and this was a 429 — the operator DoS the finding describes.
      assert conn
             |> via_proxy("198.51.100.20")
             |> basic("admin", "secret")
             |> get("/admin/metrics")
             |> Map.get(:status) != 429,
             "an unrelated operator was locked out by someone else's failures"
    end

    test "the /api limit is per client, not a fleet-wide cap", %{conn: conn} do
      Application.put_env(:fathom, :api_rate_limit, 3)

      for _ <- 1..3 do
        assert conn |> via_proxy("203.0.113.9") |> get("/api/tenants") |> Map.get(:status) == 401
      end

      assert conn |> via_proxy("203.0.113.9") |> get("/api/tenants") |> Map.get(:status) == 429

      # A different client still has its full budget. Pre-fix this was 429: the limit was a
      # global cap that legitimate control-plane traffic collided with, limiting no attacker.
      assert conn |> via_proxy("198.51.100.20") |> get("/api/tenants") |> Map.get(:status) == 401,
             "a second client inherited the first's rate-limit bucket"
    end

    test "a spoofed header from an UNTRUSTED peer cannot escape the bucket", %{conn: conn} do
      # With the proxy list not covering the peer, the header is ignored — otherwise an attacker
      # would simply rotate X-Forwarded-For to get an unlimited number of fresh budgets.
      Application.delete_env(:fathom, :trusted_proxies)
      Application.put_env(:fathom, :api_rate_limit, 3)

      for i <- 1..3 do
        assert conn |> via_proxy("203.0.113.#{i}") |> get("/api/tenants") |> Map.get(:status) ==
                 401
      end

      assert conn |> via_proxy("203.0.113.99") |> get("/api/tenants") |> Map.get(:status) == 429,
             "rotating a spoofed X-Forwarded-For bought a fresh rate-limit budget"
    end
  end

  test "with the throttles unconfigured (default), repeated failures never lock out", %{
    conn: conn
  } do
    # No :admin_auth_max_failures set ⇒ the throttle is inert: every attempt is the ordinary 401.
    for _ <- 1..8 do
      assert conn |> basic("admin", "wrong") |> get("/admin/metrics") |> Map.get(:status) == 401
    end
  end

  describe "#27 — /api mutations are not CSRF-able through the BasicAuth fallback" do
    # /api takes either a Bearer API key (never auto-attached by a browser) or the shared admin
    # BasicAuth mapped to a full `destroy` actor. Browsers cache Basic credentials per ORIGIN and
    # re-send them on cross-site form submissions — SameSite governs cookies, not the HTTP auth
    # cache — and `pipeline :api` has no protect_from_forgery. `plug :accepts, ["json"]` inspects
    # ACCEPT (a browser sends */*), not content-type, so it stopped nothing.
    #
    # An auto-submitted <form method=POST action=".../api/tenants/<victim>/suspend"> therefore
    # needed no token, no body and no attacker credential. suspend takes a tenant offline
    # fleet-wide; token/rotate and restore are equally reachable.

    test "a form-shaped POST with valid Basic credentials is refused", %{conn: conn} do
      # The exact CSRF shape: urlencoded, which is all a cross-origin <form> can send.
      res =
        conn
        |> basic("admin", "secret")
        |> Plug.Conn.put_req_header("content-type", "application/x-www-form-urlencoded")
        |> post("/api/tenants/victim/suspend", "")

      assert res.status == 403,
             "a cross-site form post with cached Basic credentials suspended a tenant"
    end

    test "a POST with NO content-type at all is refused", %{conn: conn} do
      res = conn |> basic("admin", "secret") |> post("/api/tenants/victim/suspend")
      assert res.status == 403
    end

    test "Sec-Fetch-Site: cross-site is refused even when it claims JSON", %{conn: conn} do
      # Defence in depth for browsers that send it. Not a substitute for the content-type rule —
      # a non-browser client simply omits the header — which is why both exist.
      res =
        conn
        |> basic("admin", "secret")
        |> Plug.Conn.put_req_header("content-type", "application/json")
        |> Plug.Conn.put_req_header("sec-fetch-site", "cross-site")
        |> post("/api/tenants/victim/suspend", "{}")

      assert res.status == 403
    end

    test "a legitimate JSON call still works", %{conn: conn} do
      # The guard must not break the actual control plane: 404 (unknown tenant) proves the
      # request reached the controller rather than being refused at the gate.
      res =
        conn
        |> basic("admin", "secret")
        |> Plug.Conn.put_req_header("content-type", "application/json")
        |> post("/api/tenants/victim/suspend", "{}")

      refute res.status == 403, "a legitimate JSON mutation was blocked as CSRF"
    end

    test "GET is unaffected — it changes nothing", %{conn: conn} do
      res = conn |> basic("admin", "secret") |> get("/api/tenants")
      refute res.status == 403
    end

    test "an API-key request is exempt: a browser never auto-attaches a Bearer token", %{
      conn: conn
    } do
      # Exempt on the AUTH SHAPE, not on validity — an invalid key must still 401 rather than
      # 403, or the guard would be masking the auth layer.
      res =
        conn
        |> Plug.Conn.put_req_header("authorization", "Bearer not-a-real-key")
        |> post("/api/tenants/victim/suspend")

      assert res.status == 401
    end
  end
end
