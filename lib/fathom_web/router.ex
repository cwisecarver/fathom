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

    live_session :admin do
      live "/", AdminOverviewLive, :index
      live "/shards", AdminShardsLive, :index
      live "/migrations", AdminMigrationsLive, :index
    end
  end

  # Prometheus scrape — auth-gated, but no :browser (a scraper isn't an HTML client, so it must
  # not hit the `accepts ["html"]` negotiation).
  scope "/admin", FathomWeb do
    pipe_through :admin_auth

    get "/metrics", MetricsController, :index
  end

  # BasicAuth gate for /admin. Order-independent read of the `:admin_auth` keyword; challenges
  # (401) on a bad credential via Plug.BasicAuth, and fails closed (503) when unconfigured.
  defp require_admin_auth(conn, _opts) do
    creds = Application.get_env(:fathom, :admin_auth, [])
    user = creds[:username]
    pass = creds[:password]

    if is_binary(user) and is_binary(pass) do
      Plug.BasicAuth.basic_auth(conn, username: user, password: pass)
    else
      conn
      |> Plug.Conn.put_resp_content_type("text/plain")
      |> Plug.Conn.send_resp(503, "admin dashboard not configured (set ADMIN_USER/ADMIN_PASS)")
      |> Plug.Conn.halt()
    end
  end

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
