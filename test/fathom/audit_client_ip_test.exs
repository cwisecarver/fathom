defmodule Fathom.AuditClientIpTest do
  @moduledoc """
  Expert review 2026-10-13 #24: an audit row records the resolved CLIENT address, the same one the
  throttles key on, not `conn.remote_ip`.

  Behind a trusted proxy `remote_ip` is the proxy, so every /api and export row named the proxy and
  an investigation could not tell operators apart. Pre-fix, `source_ip_of/1` read `remote_ip`
  directly, and the first test below recorded 10.0.0.5. Not async: it sets global `:trusted_proxies`.
  """
  use Fathom.DataCase, async: false

  import Plug.Conn

  alias Fathom.Audit

  setup do
    prev = Application.get_env(:fathom, :trusted_proxies)
    on_exit(fn -> restore(prev) end)
    :ok
  end

  defp restore(nil), do: Application.delete_env(:fathom, :trusted_proxies)
  defp restore(v), do: Application.put_env(:fathom, :trusted_proxies, v)

  defp conn_from(peer, xff) do
    conn = Plug.Test.conn(:get, "/api/tenants") |> Map.put(:remote_ip, peer)
    if xff, do: put_req_header(conn, "x-forwarded-for", xff), else: conn
  end

  test "behind a trusted proxy the audit row records the forwarded client, not the proxy" do
    Application.put_env(:fathom, :trusted_proxies, ["10.0.0.0/8"])

    :ok = Audit.log(conn_from({10, 0, 0, 5}, "203.0.113.9"), :delete, "acme", "ok")

    assert [%{source_ip: "203.0.113.9"}] = Audit.list(shard_id: "acme")
  end

  test "from an untrusted peer the forwarded header is ignored and remote_ip is recorded" do
    Application.put_env(:fathom, :trusted_proxies, ["10.0.0.0/8"])

    :ok = Audit.log(conn_from({192, 0, 2, 1}, "203.0.113.9"), :delete, "forged", "ok")

    assert [%{source_ip: "192.0.2.1"}] = Audit.list(shard_id: "forged")
  end

  test "with no trusted proxies configured, remote_ip is recorded as before" do
    Application.delete_env(:fathom, :trusted_proxies)

    :ok = Audit.log(conn_from({198, 51, 100, 7}, "203.0.113.9"), :export, "direct", "ok")

    assert [%{source_ip: "198.51.100.7"}] = Audit.list(shard_id: "direct")
  end
end
