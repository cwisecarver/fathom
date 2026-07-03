import Config

# config/runtime.exs is executed for all environments, including
# during releases. It is executed after compilation and before the
# system starts, so it is typically used to load production configuration
# and secrets from environment variables or elsewhere. Do not define
# any compile-time configuration in here, as it won't be applied.
# The block below contains prod specific runtime configuration.

# ## Using releases
#
# If you use `mix release`, you need to explicitly enable the server
# by passing the PHX_SERVER=true when you start it:
#
#     PHX_SERVER=true bin/fathom start
#
# Alternatively, you can use `mix phx.gen.release` to generate a `bin/server`
# script that automatically sets the env var above.
if System.get_env("PHX_SERVER") do
  config :fathom, FathomWeb.Endpoint, server: true
end

config :fathom, FathomWeb.Endpoint,
  http: [port: String.to_integer(System.get_env("PORT", "4000"))]

# OpenTelemetry OTLP trace export — enabled only when an endpoint is set, so it stays a no-op
# in dev/test/CI and in any deploy without a collector (config/config.exs defaults to :none).
# The exporter also honors the standard OTEL_EXPORTER_OTLP_* env vars (headers, protocol).
if otlp_endpoint = System.get_env("OTEL_EXPORTER_OTLP_ENDPOINT") do
  config :opentelemetry, traces_exporter: {:opentelemetry_exporter, %{}}
  config :opentelemetry_exporter, otlp_endpoint: otlp_endpoint
end

if config_env() == :prod do
  database_url =
    System.get_env("DATABASE_URL") ||
      raise """
      environment variable DATABASE_URL is missing.
      For example: ecto://USER:PASS@HOST/DATABASE
      """

  maybe_ipv6 = if System.get_env("ECTO_IPV6") in ~w(true 1), do: [:inet6], else: []

  config :fathom, Fathom.Repo,
    # ssl: true,
    url: database_url,
    pool_size: String.to_integer(System.get_env("POOL_SIZE") || "10"),
    # For machines with several cores, consider starting multiple pools of `pool_size`
    # pool_count: 4,
    socket_options: maybe_ipv6

  # The secret key base is used to sign/encrypt cookies and other secrets.
  # A default value is used in config/dev.exs and config/test.exs but you
  # want to use a different value for prod and you most likely don't want
  # to check this value into version control, so we use an environment
  # variable instead.
  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

  host = System.get_env("PHX_HOST") || "example.com"

  config :fathom, :dns_cluster_query, System.get_env("DNS_CLUSTER_QUERY")

  # The Hrana data path carries no in-app credential (see docs/deploy-cluster.md): the trust
  # boundary is the network, so the port must be reachable only via the L7 LB. Pin the listener
  # to the private interface the LB reaches as defense-in-depth alongside the firewall/security
  # group (the primary control). Unset ⇒ bind all interfaces (relies on network isolation alone).
  if bind = System.get_env("HRANA_BIND_IP") do
    case :inet.parse_address(String.to_charlist(bind)) do
      {:ok, ip} -> config :fathom, :hrana_bind_ip, ip
      {:error, _} -> raise "HRANA_BIND_IP is not a valid IP address: #{inspect(bind)}"
    end
  end

  # In-app bearer-token auth on the Hrana data path (Fathom.HranaAuth). Off by default —
  # the network trust boundary above stands alone; set HRANA_AUTH=required to make every
  # stream open present a per-shard token (needed if 8080 is ever reachable beyond the LB,
  # or for revocable per-tenant credentials). Tokens are signed with SECRET_KEY_BASE.
  case System.get_env("HRANA_AUTH", "disabled") do
    "required" -> config :fathom, :hrana_auth, :required
    "disabled" -> config :fathom, :hrana_auth, :disabled
    other -> raise "HRANA_AUTH must be \"required\" or \"disabled\", got: #{inspect(other)}"
  end

  # Optional token expiry in seconds (unset ⇒ tokens don't expire; revoke by rotating
  # SECRET_KEY_BASE).
  if max_age = System.get_env("HRANA_TOKEN_MAX_AGE") do
    config :fathom, :hrana_token_max_age, String.to_integer(max_age)
  end

  # Per-node open-shard cap (finding #14). config.exs sets a conservative finite default; operators
  # tune it to their node's measured fd/RSS density (mix fathom.scale --ramp) here.
  if cap = System.get_env("MAX_OPEN_SHARDS") do
    config :fathom, :max_open_shards, String.to_integer(cap)
  end

  # Novel-shard creation rate limit (finding #14's churn half; see Fathom.Shards.NovelLimiter).
  # Grants/sec for brand-new shard ids only; unset = off. Size to tenant-signup rate with
  # headroom — legitimate novel creation is rare, so single digits/sec is generous.
  if rate = System.get_env("NOVEL_SHARD_RATE") do
    config :fathom, :novel_shard_rate, String.to_integer(rate)
  end

  if burst = System.get_env("NOVEL_SHARD_BURST") do
    config :fathom, :novel_shard_burst, String.to_integer(burst)
  end

  config :fathom, FathomWeb.Endpoint,
    url: [host: host, port: 443, scheme: "https"],
    http: [
      # Enable IPv6 and bind on all interfaces.
      # Set it to  {0, 0, 0, 0, 0, 0, 0, 1} for local network only access.
      # See the documentation on https://hexdocs.pm/bandit/Bandit.html#t:options/0
      # for details about using IPv6 vs IPv4 and loopback vs public addresses.
      ip: {0, 0, 0, 0, 0, 0, 0, 0}
    ],
    secret_key_base: secret_key_base

  # ## SSL Support
  #
  # To get SSL working, you will need to add the `https` key
  # to your endpoint configuration:
  #
  #     config :fathom, FathomWeb.Endpoint,
  #       https: [
  #         ...,
  #         port: 443,
  #         cipher_suite: :strong,
  #         keyfile: System.get_env("SOME_APP_SSL_KEY_PATH"),
  #         certfile: System.get_env("SOME_APP_SSL_CERT_PATH")
  #       ]
  #
  # The `cipher_suite` is set to `:strong` to support only the
  # latest and more secure SSL ciphers. This means old browsers
  # and clients may not be supported. You can set it to
  # `:compatible` for wider support.
  #
  # `:keyfile` and `:certfile` expect an absolute path to the key
  # and cert in disk or a relative path inside priv, for example
  # "priv/ssl/server.key". For all supported SSL configuration
  # options, see https://hexdocs.pm/plug/Plug.SSL.html#configure/1
  #
  # We also recommend setting `force_ssl` in your config/prod.exs,
  # ensuring no data is ever sent via http, always redirecting to https:
  #
  #     config :fathom, FathomWeb.Endpoint,
  #       force_ssl: [hsts: true]
  #
  # Check `Plug.SSL` for all available options in `force_ssl`.

  # ## Configuring the mailer
  #
  # In production you need to configure the mailer to use a different adapter.
  # Here is an example configuration for Mailgun:
  #
  #     config :fathom, Fathom.Mailer,
  #       adapter: Swoosh.Adapters.Mailgun,
  #       api_key: System.get_env("MAILGUN_API_KEY"),
  #       domain: System.get_env("MAILGUN_DOMAIN")
  #
  # Most non-SMTP adapters require an API client. Swoosh supports Req, Hackney,
  # and Finch out-of-the-box. This configuration is typically done at
  # compile-time in your config/prod.exs:
  #
  #     config :swoosh, :api_client, Swoosh.ApiClient.Req
  #
  # See https://hexdocs.pm/swoosh/Swoosh.html#module-installation for details.
end
