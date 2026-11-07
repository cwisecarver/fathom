defmodule Fathom.QueryConsoleEndpointTest do
  @moduledoc """
  The admin query console's connect target follows the Hrana listener's bound interface (expert
  review 2026-11-07 #41).

  `default_endpoint/0` was hard-coded to `http://127.0.0.1:<hrana_port>`, but `HRANA_BIND_IP=10.0.0.7`
  — the documented hardening — binds ONLY that address, so nothing listens on loopback and every
  console run was `{:error, %{code: "TRANSPORT"}}`. The rest of the console suite always passes
  `endpoint:` explicitly, which is why it could not see it.

  A unit test on the URL plus one REAL connection: a listener bound to a non-loopback local address
  must be reachable through the default, which loopback was not.
  """
  use ExUnit.Case, async: false

  alias Fathom.QueryConsole
  alias Fathom.Test.Env

  setup do
    Env.put!(:hrana_port, 8080)

    # `Env.put!/2` registers the restore; a delete after it leaves the key ABSENT for the test (the
    # defaults apply) and puts the original back — set or absent — at exit.
    Env.put!(:query_console_endpoint, nil)
    Env.put!(:hrana_bind_ip, nil)
    Application.delete_env(:fathom, :query_console_endpoint)
    Application.delete_env(:fathom, :hrana_bind_ip)
    :ok
  end

  test "the default wildcard bind is reached on loopback, as before" do
    assert QueryConsole.default_endpoint() == "http://127.0.0.1:8080"

    Application.put_env(:fathom, :hrana_bind_ip, {0, 0, 0, 0})
    assert QueryConsole.default_endpoint() == "http://127.0.0.1:8080"

    Application.put_env(:fathom, :hrana_bind_ip, {0, 0, 0, 0, 0, 0, 0, 0})
    assert QueryConsole.default_endpoint() == "http://[::1]:8080"
  end

  test "a pinned HRANA_BIND_IP is the connect target (pre-fix: 127.0.0.1)" do
    Application.put_env(:fathom, :hrana_bind_ip, {10, 0, 0, 7})
    assert QueryConsole.default_endpoint() == "http://10.0.0.7:8080"

    Application.put_env(:fathom, :hrana_bind_ip, {0, 0, 0, 0, 0, 0, 0, 1})
    assert QueryConsole.default_endpoint() == "http://[::1]:8080"

    Application.put_env(:fathom, :hrana_bind_ip, {0x2001, 0xDB8, 0, 0, 0, 0, 0, 7})
    assert QueryConsole.default_endpoint() == "http://[2001:db8::7]:8080"
  end

  test "the port follows :hrana_port" do
    Env.put!(:hrana_port, 18_080)
    Application.put_env(:fathom, :hrana_bind_ip, {10, 0, 0, 7})
    assert QueryConsole.default_endpoint() == "http://10.0.0.7:18080"
  end

  test "QUERY_CONSOLE_ENDPOINT (:query_console_endpoint) overrides the whole URL" do
    Application.put_env(:fathom, :hrana_bind_ip, {10, 0, 0, 7})
    Application.put_env(:fathom, :query_console_endpoint, "https://hrana.internal:8443")
    assert QueryConsole.default_endpoint() == "https://hrana.internal:8443"
  end

  # The point of the finding, end to end: the loopback default cannot reach a listener that is bound
  # to another local address, and the new default can. A non-loopback local address is needed, so
  # the test skips itself where the host has none (a sandbox with only `lo`).
  test "the default reaches a listener bound to a pinned local address, loopback does not" do
    case local_non_loopback_ip() do
      nil ->
        :ok

      ip ->
        {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, ip: ip, reuseaddr: true])
        {:ok, port} = :inet.port(listen)
        parent = self()

        acceptor =
          spawn_link(fn ->
            case :gen_tcp.accept(listen, 5_000) do
              {:ok, sock} -> send(parent, {:accepted, sock})
              _ -> :ok
            end
          end)

        on_exit(fn ->
          Process.unlink(acceptor)
          Process.exit(acceptor, :kill)
          :gen_tcp.close(listen)
        end)

        Env.put!(:hrana_port, port)
        Application.put_env(:fathom, :hrana_bind_ip, ip)

        # The pre-fix default (loopback) is refused: nothing listens there.
        assert {:error, _} = :gen_tcp.connect({127, 0, 0, 1}, port, [active: false], 1_000)

        # The new default's host is the one that accepts.
        %URI{host: host, port: ^port} = URI.parse(QueryConsole.default_endpoint())
        {:ok, addr} = :inet.parse_address(String.to_charlist(host))
        assert {:ok, sock} = :gen_tcp.connect(addr, port, [active: false], 1_000)
        assert_receive {:accepted, _}, 2_000
        :gen_tcp.close(sock)
    end
  end

  defp local_non_loopback_ip do
    with {:ok, ifs} <- :inet.getifaddrs() do
      for {_name, opts} <- ifs,
          {:addr, {a, _, _, _} = ip} <- opts,
          a != 127,
          tuple_size(ip) == 4,
          :up in Keyword.get(opts, :flags, []),
          do: ip
    end
    |> case do
      [ip | _] -> ip
      _ -> nil
    end
  end
end
