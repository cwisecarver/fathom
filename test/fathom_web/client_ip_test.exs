defmodule FathomWeb.ClientIpTest do
  @moduledoc """
  Client-IP resolution for the control-plane throttles (expert review 2026-08-01 #35).

  The admin lockout and the `/api` rate limit are documented as PER-IP and are on by default in
  prod, but keyed on `conn.remote_ip` — the PROXY's address behind any proxy, so every client
  shared one bucket. One attacker's failed BasicAuth attempts then locked out every operator, and
  the lockout is checked before credentials are verified.

  The security-critical half is not "read the header" — it is **when not to**. `X-Forwarded-For`
  is attacker-supplied; honouring it unconditionally would be strictly worse than the bug, so the
  spoofing cases below matter more than the happy path.
  """
  use ExUnit.Case, async: false

  alias FathomWeb.ClientIp

  setup do
    prev = Application.get_env(:fathom, :trusted_proxies)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:fathom, :trusted_proxies, prev),
        else: Application.delete_env(:fathom, :trusted_proxies)
    end)

    :ok
  end

  defp conn(remote_ip, xff \\ []) do
    %Plug.Conn{
      remote_ip: remote_ip,
      req_headers: Enum.map(xff, &{"x-forwarded-for", &1})
    }
  end

  defp trust(list), do: Application.put_env(:fathom, :trusted_proxies, list)

  describe "fails closed" do
    test "no :trusted_proxies configured ⇒ conn.remote_ip, header ignored entirely" do
      Application.delete_env(:fathom, :trusted_proxies)

      assert ClientIp.resolve(conn({10, 0, 0, 1}, ["203.0.113.9"])) == {10, 0, 0, 1}
    end

    test "an UNTRUSTED peer's header is ignored even when trusted proxies exist" do
      # The attacker connects directly and sends a header. Reading it would let them pick their
      # own rate-limit bucket per request, and pin any IP they like into the admin lockout.
      trust(["10.0.0.0/8"])

      assert ClientIp.resolve(conn({198, 51, 100, 7}, ["203.0.113.9"])) == {198, 51, 100, 7}
    end

    test "a trusted peer that sent NO header falls back to the peer" do
      trust(["10.0.0.0/8"])
      assert ClientIp.resolve(conn({10, 0, 0, 1})) == {10, 0, 0, 1}
    end

    test "a chain of only trusted proxies falls back to the peer" do
      trust(["10.0.0.0/8"])
      assert ClientIp.resolve(conn({10, 0, 0, 1}, ["10.0.0.5, 10.0.0.6"])) == {10, 0, 0, 1}
    end
  end

  describe "rightmost-untrusted is the attributable address" do
    test "a spoofed prefix cannot displace the address the proxy actually observed" do
      # THE case that makes leftmost-wins wrong. The attacker at 203.0.113.9 sends
      # `X-Forwarded-For: 1.2.3.4` — a lie — and nginx APPENDS the peer it really saw. Everything
      # left of 203.0.113.9 is attacker-controlled; the rightmost untrusted entry is the first one
      # anybody actually vouched for.
      trust(["10.0.0.0/8"])

      assert ClientIp.resolve(conn({10, 0, 0, 1}, ["1.2.3.4, 203.0.113.9"])) == {203, 0, 113, 9}
    end

    test "trusted hops to the right are skipped" do
      trust(["10.0.0.0/8", "172.16.0.0/12"])

      assert ClientIp.resolve(conn({10, 0, 0, 1}, ["203.0.113.9, 172.16.4.4, 10.0.0.5"])) ==
               {203, 0, 113, 9}
    end

    test "multiple x-forwarded-for headers are one chain, in order" do
      # RFC 7230 §3.2.2 — repeated headers are equivalent to one comma-joined value, and real
      # proxy chains emit both shapes.
      trust(["10.0.0.0/8"])

      assert ClientIp.resolve(conn({10, 0, 0, 1}, ["1.2.3.4", "203.0.113.9, 10.0.0.5"])) ==
               {203, 0, 113, 9}
    end

    test "garbage entries are dropped, not guessed at" do
      trust(["10.0.0.0/8"])

      assert ClientIp.resolve(conn({10, 0, 0, 1}, ["unknown, _hidden, 203.0.113.9"])) ==
               {203, 0, 113, 9}
    end
  end

  describe "address forms" do
    test "a bare address in the trusted list is its own /32" do
      trust(["10.0.0.1"])

      assert ClientIp.resolve(conn({10, 0, 0, 1}, ["203.0.113.9"])) == {203, 0, 113, 9}
      # A sibling address is NOT trusted, so its header is ignored.
      assert ClientIp.resolve(conn({10, 0, 0, 2}, ["203.0.113.9"])) == {10, 0, 0, 2}
    end

    test "an IPv4-mapped IPv6 peer matches its IPv4 trusted entry" do
      # A dual-stack listener reports the peer as ::ffff:10.0.0.1. Without normalising, a
      # 10.0.0.0/8 entry silently fails to match its own proxy and the header goes unread.
      trust(["10.0.0.0/8"])
      mapped = {0, 0, 0, 0, 0, 0xFFFF, 0x0A00, 0x0001}

      assert ClientIp.resolve(conn(mapped, ["203.0.113.9"])) == {203, 0, 113, 9}
    end

    test "IPv6 proxies and clients work" do
      trust(["2001:db8::/32"])

      assert ClientIp.resolve(conn({0x2001, 0xDB8, 0, 0, 0, 0, 0, 1}, ["2001:db9::5"])) ==
               {0x2001, 0xDB9, 0, 0, 0, 0, 0, 5}
    end

    test "an IPv4 entry never matches an IPv6 address, or vice versa" do
      trust(["10.0.0.0/8"])
      v6 = {0x2001, 0xDB8, 0, 0, 0, 0, 0, 1}
      assert ClientIp.resolve(conn(v6, ["203.0.113.9"])) == v6
    end
  end

  describe "config robustness" do
    test "an unparseable entry is ignored, and can only SHRINK the trusted set" do
      # A typo in an ops config must not 500 the control plane, and must not accidentally widen
      # trust. Here the bad entry is dropped and the good one still works.
      trust(["not-an-ip", "10.0.0.0/8", "10.0.0.0/999"])

      assert ClientIp.resolve(conn({10, 0, 0, 1}, ["203.0.113.9"])) == {203, 0, 113, 9}
      assert length(ClientIp.trusted_proxies()) == 1
    end

    test "every entry unparseable ⇒ behaves as unconfigured" do
      trust(["nonsense"])
      assert ClientIp.resolve(conn({10, 0, 0, 1}, ["203.0.113.9"])) == {10, 0, 0, 1}
    end
  end
end
