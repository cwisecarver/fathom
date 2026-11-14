defmodule FathomWeb.PageController do
  use FathomWeb, :controller

  # `/` has nothing of its own: the stock phx.new "Peace of mind from prototype to production" page
  # that lived here also printed `Application.spec(:phoenix, :vsn)` on a public route (cleanliness
  # audit 2026-11-13 #19). The operator surface is the dashboard, behind its own BasicAuth.
  def home(conn, _params) do
    redirect(conn, to: ~p"/admin")
  end
end
