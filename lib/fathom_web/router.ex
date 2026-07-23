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

    live_session :admin do
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
    pipe_through [:api, :api_rate_limit, :api_auth]

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
  defp client_ip(conn), do: conn.remote_ip

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
