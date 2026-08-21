defmodule FathomWeb.Router do
  use FathomWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {FathomWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  # BasicAuth for the operator surface. Fails closed (503) when no credentials are configured
  # (`:admin_auth` — dev/test set a default; prod reads ADMIN_USER/ADMIN_PASS), so the dashboard
  # and the /metrics scrape are never anonymously reachable.
  pipeline :admin_auth do
    plug :require_admin_auth
  end

  scope "/", FathomWeb do
    pipe_through :browser

    get "/", PageController, :home
  end

  # Admin dashboard (realtime LiveViews). Behind :browser + BasicAuth.
  scope "/admin", FathomWeb do
    pipe_through [:browser, :admin_auth]

    # Tenant data export (#15): a file download, so a plain controller (not a LiveView).
    get "/tenants/:id/export", AdminTenantController, :export

    # The operator behind the BasicAuth, handed to the LiveViews (expert review 2026-08-20 #32).
    # `AdminQueryLive` mints a tenant credential on every execution and those mints land in the
    # issuance ledger, so an unattributed one is both credential sprawl and a poisoned input to
    # the fleet-wide revoke. Mirrors `AdminTenantController.admin_actor/1`, with a distinct
    # `console:` prefix so console mints stay separable from export attribution.
    live_session :admin, session: {__MODULE__, :admin_live_session, []} do
      live "/", AdminOverviewLive, :index
      live "/shards", AdminShardsLive, :index
      live "/directory", AdminDirectoryLive, :index
      live "/migrations", AdminMigrationsLive, :index
      live "/query", AdminQueryLive, :index
    end
  end

  # Prometheus scrape — auth-gated, but no :browser (a scraper isn't an HTML client, so it must
  # not hit the `accepts ["html"]` negotiation).
  scope "/admin", FathomWeb do
    pipe_through :admin_auth

    get "/metrics", MetricsController, :index
  end

  # Tenant provisioning control-plane (#21): JSON create/list/get/delete, behind the same admin
  # BasicAuth. On :4000, separate from the Hrana data port. `:api_rate_limit` (expert review #34)
  # runs BEFORE auth so an unauthenticated flood is throttled too.
  scope "/api", FathomWeb.Api do
    pipe_through [:api, :api_rate_limit, :api_auth, :api_csrf_guard]

    post "/tenants", TenantController, :create
    get "/tenants", TenantController, :index
    get "/tenants/:id", TenantController, :show
    delete "/tenants/:id", TenantController, :delete
    post "/tenants/:id/suspend", TenantController, :suspend
    post "/tenants/:id/resume", TenantController, :resume
    post "/tenants/:id/token", TenantController, :mint_token
    post "/tenants/:id/token/rotate", TenantController, :rotate_token
    delete "/tenants/:id/token", TenantController, :revoke_token
    post "/tenants/:id/fork", TenantController, :fork
    post "/tenants/:id/flush", TenantController, :flush
    post "/tenants/:id/snapshots", TenantController, :create_snapshot
    get "/tenants/:id/snapshots", TenantController, :list_snapshots
    post "/tenants/:id/restore", TenantController, :restore
    delete "/tenants/:id/snapshots/:snapshot_id", TenantController, :drop_snapshot

    # Fleet migration convergence — the deploy gate a Django CI/CD reads (#25).
    get "/migrations/status", MigrationController, :status
  end

  # Auth gate for the /api control plane (expert review #8). Prefers a scoped `Authorization: Bearer`
  # API key (per-identity, revocable, least-privilege); falls back to the legacy shared admin
  # BasicAuth (mapped to a full-`destroy` actor) so existing deployments keep working while they
  # migrate to keys. Either way it assigns `conn.assigns.api_actor` = `%{name, scope}`, which
  # `require_scope` enforces per action and the audit log (#9) attributes the action to.
  defp api_auth(conn, _opts) do
    case bearer_token(conn) do
      {:ok, token} ->
        case Fathom.ApiKeys.authenticate(token) do
          {:ok, actor} ->
            Plug.Conn.assign(conn, :api_actor, actor)

          :error ->
            conn
            |> Plug.Conn.put_resp_content_type("text/plain")
            |> Plug.Conn.send_resp(401, "invalid or revoked API key")
            |> Plug.Conn.halt()
        end

      :none ->
        # No Bearer token — fall back to the legacy shared admin BasicAuth (backward compat).
        conn = require_admin_auth(conn, [])

        if conn.halted,
          do: conn,
          else:
            Plug.Conn.assign(conn, :api_actor, %{name: "admin (basic-auth)", scope: "destroy"})
    end
  end

  # CSRF guard for state-changing `/api` calls authenticated by the BasicAuth fallback
  # (expert review 2026-08-01 #27).
  #
  # `/api` accepts either a Bearer API key — never auto-attached by a browser, so inherently
  # safe — or the shared admin BasicAuth mapped to a full `destroy` actor. Browsers cache HTTP
  # Basic credentials per ORIGIN and re-send them on any later request to that origin, INCLUDING
  # a cross-site form submission; `SameSite` governs cookies and does not apply to the HTTP auth
  # cache. `pipeline :api` has no `protect_from_forgery`, and `plug :accepts, ["json"]` inspects
  # ACCEPT (a browser sends `*/*`), not content-type — so an auto-submitted
  # `<form method=POST action=".../api/tenants/<victim>/suspend">` needed no token, no body, and
  # no attacker credential. `suspend` takes a tenant offline fleet-wide; `token/rotate` and
  # `restore` are equally reachable.
  #
  # The content-type check is the load-bearing one, not a heuristic: an HTML form can only send
  # `application/x-www-form-urlencoded`, `multipart/form-data` or `text/plain`. Sending
  # `application/json` cross-origin requires fetch/XHR, which triggers a CORS preflight this
  # endpoint answers no headers for. So requiring JSON on mutations closes form-driven CSRF
  # outright. `Sec-Fetch-Site` is defence in depth for browsers that send it, and is only
  # consulted when present — it is not a substitute.
  #
  # NOT the review's preferred fix, which was to drop the BasicAuth fallback for mutating actions
  # entirely. `api_auth/2` exists precisely so existing deployments keep working while they
  # migrate to keys, and removing it is a breaking change to the control plane rather than a
  # security fix — this closes the hole while leaving that migration on its own schedule. The
  # cost is one header for BasicAuth clients, documented in docs/configuration.md.
  defp api_csrf_guard(conn, _opts) do
    cond do
      conn.method in ["GET", "HEAD", "OPTIONS"] -> conn
      match?({:ok, _}, bearer_token(conn)) -> conn
      cross_site?(conn) -> reject_csrf(conn, "cross-site request")
      json_content_type?(conn) -> conn
      true -> reject_csrf(conn, "state-changing /api requests must send application/json")
    end
  end

  defp cross_site?(conn) do
    Plug.Conn.get_req_header(conn, "sec-fetch-site") == ["cross-site"]
  end

  defp json_content_type?(conn) do
    case Plug.Conn.get_req_header(conn, "content-type") do
      [ct | _] -> ct |> String.downcase() |> String.starts_with?("application/json")
      [] -> false
    end
  end

  defp reject_csrf(conn, why) do
    :telemetry.execute([:fathom, :api, :csrf_blocked], %{count: 1}, %{})

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(403, ~s({"error":"forbidden","message":"#{why}"}))
    |> Plug.Conn.halt()
  end

  defp bearer_token(conn) do
    case Plug.Conn.get_req_header(conn, "authorization") do
      ["Bearer " <> token | _] -> {:ok, String.trim(token)}
      _ -> :none
    end
  end

  # BasicAuth gate for /admin (and the /api BasicAuth fallback). Order-independent read of the
  # `:admin_auth` keyword; challenges (401) on a bad credential via Plug.BasicAuth, and fails closed
  # (503) when unconfigured. A per-source-IP failed-attempt throttle (expert review #34, config-gated
  # `:admin_auth_max_failures`, off by default) locks out an IP with 429 after too many failures in
  # the window, so the one shared admin password (#8) isn't online-brute-forceable. Baked in here so
  # BOTH the `:admin_auth` pipeline and the `api_auth` BasicAuth fallback are covered.
  @doc false
  def admin_live_session(conn) do
    actor =
      case Plug.BasicAuth.parse_basic_auth(conn) do
        {user, _pass} -> "console:#{user}"
        _ -> "console"
      end

    %{"admin_actor" => actor}
  end

  defp require_admin_auth(conn, _opts) do
    creds = Application.get_env(:fathom, :admin_auth, [])
    user = creds[:username]
    pass = creds[:password]

    cond do
      not (is_binary(user) and is_binary(pass)) ->
        conn
        |> Plug.Conn.put_resp_content_type("text/plain")
        |> Plug.Conn.send_resp(503, "admin dashboard not configured (set ADMIN_USER/ADMIN_PASS)")
        |> Plug.Conn.halt()

      admin_auth_blocked?(conn) ->
        :telemetry.execute([:fathom, :admin_auth, :blocked], %{count: 1}, %{})

        conn
        |> Plug.Conn.put_resp_content_type("text/plain")
        |> Plug.Conn.send_resp(429, "too many failed admin authentication attempts; retry later")
        |> Plug.Conn.halt()

      true ->
        conn
        |> Plug.BasicAuth.basic_auth(username: user, password: pass)
        |> record_admin_auth_result()
    end
  end

  defp admin_auth_blocked?(conn) do
    case admin_fail_limit() do
      nil ->
        false

      limit ->
        Fathom.RateLimiter.count(:admin_auth, client_ip(conn), admin_fail_window()) >= limit
    end
  end

  # After Plug.BasicAuth: a halt with 401 is a failed credential (count it); an unhalted conn is a
  # success (clear the IP's failure count). A 503 (unconfigured) never reaches here.
  defp record_admin_auth_result(conn) do
    case admin_fail_limit() do
      nil ->
        conn

      limit ->
        cond do
          conn.halted and conn.status == 401 ->
            new_count = Fathom.RateLimiter.bump(:admin_auth, client_ip(conn), admin_fail_window())
            :telemetry.execute([:fathom, :admin_auth, :failed], %{count: 1}, %{})

            # Audit ONLY the lockout transition (the attempt that crosses the threshold), never every
            # attempt — a brute-force flood must not amplify into a Postgres audit-write flood. The
            # high-frequency per-attempt signal is the telemetry counter above.
            if new_count == limit do
              Fathom.Audit.log(conn, :admin_auth_locked_out, nil, :blocked, %{failures: new_count})
            end

            conn

          not conn.halted ->
            Fathom.RateLimiter.forget(:admin_auth, client_ip(conn))
            conn

          true ->
            conn
        end
    end
  end

  # Per-source-IP request-rate limit for the /api control plane (expert review #34, config-gated
  # `:api_rate_limit`, off by default) so an authenticated-but-hostile or buggy client can't hammer
  # expensive ops (list/export/fork) unbounded. NovelLimiter protects only novel data-path minting.
  defp api_rate_limit(conn, _opts) do
    case api_rate_limit_config() do
      nil ->
        conn

      limit ->
        case Fathom.RateLimiter.check(:api, client_ip(conn), limit, api_rate_window()) do
          :ok ->
            conn

          :limited ->
            :telemetry.execute([:fathom, :api, :rate_limited], %{count: 1}, %{})

            conn
            |> Plug.Conn.put_resp_content_type("application/json")
            |> Plug.Conn.send_resp(
              429,
              ~s({"error":"rate_limited","message":"control-plane rate limit exceeded"})
            )
            |> Plug.Conn.halt()
        end
    end
  end

  # Throttle config — all off by default (nil); prod enables via runtime.exs (ADMIN_AUTH_MAX_FAILURES
  # / API_RATE_LIMIT). Windows default in code so only the limits need setting.
  defp admin_fail_limit, do: Application.get_env(:fathom, :admin_auth_max_failures)
  defp admin_fail_window, do: Application.get_env(:fathom, :admin_auth_window_ms, 300_000)
  defp api_rate_limit_config, do: Application.get_env(:fathom, :api_rate_limit)
  defp api_rate_window, do: Application.get_env(:fathom, :api_rate_window_ms, 60_000)
  # The throttles above are documented as PER-IP and are on by default in prod. Behind a proxy
  # `conn.remote_ip` is the proxy, so every client shared one bucket and one attacker's failed
  # logins locked out every operator (expert review 2026-08-01 #35). Honours `X-Forwarded-For`
  # only when the peer is a configured trusted proxy — see FathomWeb.ClientIp for why that
  # condition is mandatory rather than a refinement. Unset `:trusted_proxies` keeps today's
  # behaviour exactly.
  #
  # NOT also keyed on the username, which the review suggested. `:admin_auth` is ONE shared
  # credential, so a username adds no partition — it only lets an attacker vary the username field
  # to get a fresh failure budget per made-up name, multiplying the attempts the lockout allows.
  # That weakens the exact brute-force protection the finding is about.
  defp client_ip(conn), do: FathomWeb.ClientIp.resolve(conn)

  # The Hrana (libSQL) endpoint is served by Filo on its own listener (see
  # Fathom.Application.hrana_listener/0), not through this router.

  # Other scopes may use custom stacks.
  # scope "/api", FathomWeb do
  #   pipe_through :api
  # end

  # Enable LiveDashboard and Swoosh mailbox preview in development
  if Application.compile_env(:fathom, :dev_routes) do
    # If you want to use the LiveDashboard in production, you should put
    # it behind authentication and allow only admins to access it.
    # If your application does not have an admins-only section yet,
    # you can use Plug.BasicAuth to set up some basic authentication
    # as long as you are also using SSL (which you should anyway).
    import Phoenix.LiveDashboard.Router

    scope "/dev" do
      pipe_through :browser

      live_dashboard "/dashboard", metrics: FathomWeb.Telemetry
      forward "/mailbox", Plug.Swoosh.MailboxPreview
    end
  end
end
