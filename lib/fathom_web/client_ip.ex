defmodule FathomWeb.ClientIp do
  @moduledoc """
  Resolve the real client IP for the control-plane throttles, honouring `X-Forwarded-For` only
  when the immediate peer is a configured trusted proxy (expert review 2026-08-01 #35).

  ## The bug this fixes

  `docs/deploy-cluster.md` advertises the admin lockout and the `/api` limit as **per-IP**, and
  both are on by default in prod. They keyed on `conn.remote_ip`, which behind any proxy is the
  PROXY's address — so every client shared one bucket and "per-IP" silently became global:

    * **operator lockout DoS** — one attacker's `ADMIN_AUTH_MAX_FAILURES` failed BasicAuth
      attempts lock out *every* operator for the window, and the lockout is checked BEFORE
      credentials are verified, so no valid password gets you back in;
    * the `/api` limit becomes a fleet-wide cap that legitimate control-plane traffic collides
      with, while providing no per-attacker limiting at all.

  The deployment does expect a proxy: `config/prod.exs` sets
  `force_ssl: [rewrite_on: [:x_forwarded_proto]]`, and both shipped nginx configs send
  `X-Forwarded-For $proxy_add_x_forwarded_for`.

  ## Why the trusted-proxy list is mandatory, not a nicety

  `X-Forwarded-For` is a request header: anyone can send one. Trusting it unconditionally is
  strictly WORSE than the bug — an attacker would spoof a different value per request to evade
  their own rate limit entirely, and could pin a chosen IP into the admin lockout to deny a real
  operator. So a forwarded address is honoured only when `conn.remote_ip` is itself trusted.

  **Unset `:trusted_proxies` ⇒ exactly today's behaviour** (`conn.remote_ip`, no header read).
  Failing closed matters because the safe default has to be the one an operator gets by not
  knowing about this.

  ## Rightmost-untrusted, not leftmost

  Each hop APPENDS the peer it saw, so the chain reads `client, proxy1, proxy2, …`. An attacker
  controls only what they send — everything to the LEFT of their own address. The rightmost entry
  that is not a trusted proxy is therefore the first address that is actually attributable, and it
  is what we key on. Taking the leftmost (the naive read of "the client") is attacker-controlled
  and is the classic way this goes wrong.

  ## Config

      config :fathom, :trusted_proxies, ["10.0.0.0/8", "172.16.0.0/12", "192.168.1.7"]

  Entries are plain addresses or CIDR, IPv4 or IPv6. An unparseable entry is ignored rather than
  crashing a request — a typo in an ops config must not take the control plane down — and, since
  ignoring it can only shrink the trusted set, the failure direction is "stop honouring the
  header", never "trust more than asked".
  """

  import Bitwise

  @doc """
  The address to key a throttle on: the forwarded client when the peer is a trusted proxy,
  otherwise `conn.remote_ip`.
  """
  @spec resolve(Plug.Conn.t()) :: :inet.ip_address()
  def resolve(%Plug.Conn{remote_ip: remote_ip} = conn) do
    case trusted_proxies() do
      [] ->
        remote_ip

      trusted ->
        peer = normalize(remote_ip)

        if trusted?(peer, trusted) do
          conn
          |> forwarded_chain()
          |> rightmost_untrusted(trusted)
          |> case do
            nil -> remote_ip
            ip -> ip
          end
        else
          remote_ip
        end
    end
  end

  @doc "Parsed `:trusted_proxies`, as `{network, prefix_bits}` pairs. Unparseable entries dropped."
  @spec trusted_proxies() :: [{:inet.ip_address(), non_neg_integer()}]
  def trusted_proxies do
    :fathom
    |> Application.get_env(:trusted_proxies, [])
    |> List.wrap()
    |> Enum.flat_map(fn entry ->
      case parse_cidr(entry) do
        {:ok, parsed} -> [parsed]
        :error -> []
      end
    end)
  end

  # Every `x-forwarded-for` header, in order, flattened into one list of addresses. Multiple
  # headers are equivalent to one comma-joined header (RFC 7230 §3.2.2), and proxies do emit both
  # shapes. Anything that is not an address `:inet` recognises is dropped: the header is
  # attacker-influenced, so a token we cannot parse is not something to guess at.
  defp forwarded_chain(conn) do
    conn
    |> Plug.Conn.get_req_header("x-forwarded-for")
    |> Enum.flat_map(&String.split(&1, ","))
    |> Enum.flat_map(fn raw ->
      case raw |> String.trim() |> String.to_charlist() |> :inet.parse_address() do
        {:ok, ip} -> [normalize(ip)]
        {:error, _} -> []
      end
    end)
  end

  defp rightmost_untrusted(chain, trusted) do
    chain
    |> Enum.reverse()
    |> Enum.find(fn ip -> not trusted?(ip, trusted) end)
  end

  defp trusted?(ip, trusted), do: Enum.any?(trusted, &in_network?(ip, &1))

  defp in_network?(ip, {net, prefix}) do
    a = bits(ip)
    b = bits(net)

    byte_size(a) == byte_size(b) and prefix <= bit_size(a) and prefix_equal?(a, b, prefix)
  end

  defp prefix_equal?(a, b, prefix) do
    <<x::bitstring-size(^prefix), _::bitstring>> = a
    <<y::bitstring-size(^prefix), _::bitstring>> = b
    x == y
  end

  defp bits({a, b, c, d}), do: <<a, b, c, d>>

  defp bits({a, b, c, d, e, f, g, h}),
    do: <<a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>>

  # An IPv4-mapped IPv6 address (`::ffff:10.0.0.1`) is the SAME host as its IPv4 form, and a
  # dual-stack listener reports peers this way. Without normalising, a `10.0.0.0/8` trusted entry
  # would silently not match its own proxy and the header would go unread — the failure is quiet,
  # which is the worst kind here.
  defp normalize({0, 0, 0, 0, 0, 0xFFFF, x, y}),
    do: {x >>> 8 &&& 0xFF, x &&& 0xFF, y >>> 8 &&& 0xFF, y &&& 0xFF}

  defp normalize(ip), do: ip

  defp parse_cidr(entry) when is_binary(entry) do
    {addr, prefix} =
      case String.split(entry, "/", parts: 2) do
        [addr, len] -> {addr, len}
        [addr] -> {addr, nil}
      end

    with {:ok, ip} <- addr |> String.trim() |> String.to_charlist() |> :inet.parse_address(),
         ip = normalize(ip),
         {:ok, bits} <- prefix_bits(prefix, ip) do
      {:ok, {ip, bits}}
    else
      _ -> :error
    end
  end

  defp parse_cidr(_), do: :error

  # A bare address is its own /32 or /128.
  defp prefix_bits(nil, ip), do: {:ok, bit_size(bits(ip))}

  defp prefix_bits(len, ip) do
    max = bit_size(bits(ip))

    case Integer.parse(String.trim(len)) do
      {n, ""} when n >= 0 and n <= max -> {:ok, n}
      _ -> :error
    end
  end
end
