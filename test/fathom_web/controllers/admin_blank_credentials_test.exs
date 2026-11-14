defmodule FathomWeb.AdminBlankCredentialsTest do
  @moduledoc """
  Expert review 2026-10-13 #9: a blank `:admin_auth` username or password is UNSET, not a credential.

  Before the fix the router's "not configured" check was `is_binary/1`, which is true for `""`, so
  `[username: "", password: ""]` passed the gate and `Plug.BasicAuth` accepted `Basic Og==` (":")
  against it. The surface must answer 503 for any blank half, and must still authenticate a real
  credential. Not async: mutates global application env.
  """
  use FathomWeb.ConnCase, async: false

  alias Fathom.Test.Env

  setup do
    Env.delete!(:admin_auth_max_failures)

    :ok
  end

  defp basic(conn, user, pass),
    do:
      Plug.Conn.put_req_header(
        conn,
        "authorization",
        "Basic " <> Base.encode64("#{user}:#{pass}")
      )

  test "blank username and password fail closed (503), even for the empty ':' credential", %{
    conn: conn
  } do
    Env.put!(:admin_auth, username: "", password: "")

    assert conn |> basic("", "") |> get("/admin/metrics") |> Map.get(:status) == 503
  end

  test "a blank username alone fails closed (503), whatever password is sent", %{conn: conn} do
    Env.put!(:admin_auth, username: "", password: "secret")

    assert conn |> basic("", "secret") |> get("/admin/metrics") |> Map.get(:status) == 503
  end

  test "a whitespace-only password fails closed (503)", %{conn: conn} do
    Env.put!(:admin_auth, username: "admin", password: "   ")

    assert conn |> basic("admin", "   ") |> get("/admin/metrics") |> Map.get(:status) == 503
  end

  test "a real credential still authenticates (the fix does not lock out a configured operator)",
       %{conn: conn} do
    Env.put!(:admin_auth, username: "admin", password: "secret")

    assert conn |> basic("admin", "secret") |> get("/admin/metrics") |> Map.get(:status) == 200
  end
end
