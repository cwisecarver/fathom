defmodule FathomWeb.PageControllerTest do
  use FathomWeb.ConnCase

  # `/` used to render the stock phx.new landing page (which disclosed the Phoenix version). It
  # redirects to the dashboard, which carries its own BasicAuth.
  test "GET / redirects to the admin dashboard and discloses nothing", %{conn: conn} do
    conn = get(conn, ~p"/")
    assert redirected_to(conn) == "/admin"
    refute conn.resp_body =~ "Phoenix"
  end
end
