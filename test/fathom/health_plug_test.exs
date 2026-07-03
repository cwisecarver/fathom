defmodule Fathom.HealthPlugTest do
  # Pure plug test: no DB, no port bound — calls the plug function directly.
  use ExUnit.Case, async: true

  import Plug.Test
  import Plug.Conn

  defp call(method, path) do
    conn(method, path) |> Fathom.HealthPlug.call(Fathom.HealthPlug.init([]))
  end

  test "GET /health returns 200 ok (LB liveness probe target)" do
    conn = call(:get, "/health")
    assert conn.status == 200
    assert conn.resp_body == "ok"
    assert get_resp_header(conn, "content-type") |> List.first() =~ "text/plain"
  end

  test "an unknown path returns 404 (not a catch-all 200)" do
    conn = call(:get, "/nope")
    assert conn.status == 404
  end

  test "a non-GET to /health returns 404 (liveness is GET-only)" do
    conn = call(:post, "/health")
    assert conn.status == 404
  end
end
