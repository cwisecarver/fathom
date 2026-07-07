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

# ---- Shard-plane deploy knobs (env-gated, any config_env) --------------------------------
# These configure the shard data plane for real deployments (a release in a container, the
# deploy/chaos rig). Unset ⇒ the compile-time defaults (Local storage under System.tmp_dir!),
# so dev/test are unaffected.

# Storage backend for shard files. SHARD_STORAGE=s3 selects the S3 backend and reads its
# connection settings; the boot fence probe (Fathom.Application.check_storage_fence!) then
# verifies the store enforces conditional writes before serving a byte.
case System.get_env("SHARD_STORAGE") do
  nil ->
    :ok

  "local" ->
    config :fathom, :shard_storage, Fathom.Shard.Storage.Local

  "s3" ->
    config :fathom, :shard_storage, Fathom.Shard.Storage.S3

    config :fathom, Fathom.Shard.Storage.S3,
      bucket: System.get_env("S3_BUCKET") || raise("SHARD_STORAGE=s3 requires S3_BUCKET"),
      region: System.get_env("S3_REGION", "us-east-1"),
      # Optional override for S3-compatible stores (MinIO, R2, Tigris). Unset ⇒ AWS.
      endpoint: System.get_env("S3_ENDPOINT"),
      # MinIO and R2 need path-style addressing (endpoint/bucket/key).
      path_style: System.get_env("S3_PATH_STYLE") in ~w(true 1),
      prefix: System.get_env("S3_PREFIX", ""),
      access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
      secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
      token: System.get_env("AWS_SESSION_TOKEN")

  other ->
    raise "SHARD_STORAGE must be \"s3\" or \"local\", got: #{inspect(other)}"
end

# Where shard files live locally while a shard is open (default: System.tmp_dir!/fathom_shards).
# Point it at the node's fast local disk in a real deployment.
if dir = System.get_env("SHARD_DATA_DIR") do
  config :fathom, :shard_data_dir, dir
end

# Node heartbeat / lease TTL (ms). Bounds how long a dead node's shards stay unstealable —
# the failover-stall ceiling. Default 30_000.
if ttl = System.get_env("SHARD_LEASE_TTL_MS") do
  config :fathom, :shard_lease_ttl_ms, String.to_integer(ttl)
end

# Periodic durability-flush cadence and idle (flush+drop+stop) threshold, ms.
if ms = System.get_env("SHARD_FLUSH_INTERVAL_MS") do
  config :fathom, :shard_flush_interval_ms, String.to_integer(ms)
end

if ms = System.get_env("SHARD_IDLE_MS") do
  config :fathom, :shard_idle_ms, String.to_integer(ms)
end

# Per-shard load counters (`Fathom.ShardLoad`) — the Phase-2 rebalancing input. Off by
# default so the hot path doesn't pay for an unread counter; a node/deployment opts in
# (the "turn on :shard_load on a deployed node" knob a rebalancer / hot-spot measurement
# needs). Read live via `Application.get_env`, so it also gates the record_* calls.
if System.get_env("SHARD_LOAD") in ~w(true 1) do
  config :fathom, :shard_load, true
end

# --- Phase-2 B1 dynamic rebalancing (all off by default) ---------------------------
# A stable per-node key the LB addresses this node as (the exception table / backend set
# reference it). Default node(); set per node in a fleet (e.g. NODE_KEY=fathom1).
if key = System.get_env("NODE_KEY") do
  config :fathom, :node_key, key
end

# The LB backend set as node_key=address pairs, e.g.
# LB_BACKENDS="fathom1=fathom1:8080,fathom2=fathom2:8080". The policy picks targets from
# the keys; the map renderer emits a pin-upstream per entry.
if spec = System.get_env("LB_BACKENDS") do
  backends =
    spec
    |> String.split(",", trim: true)
    |> Map.new(fn pair ->
      [k, v] = String.split(pair, "=", parts: 2)
      {String.trim(k), String.trim(v)}
    end)

  config :fathom, :lb_backends, backends
end

# Publish this node's hot shards to Postgres (needs SHARD_LOAD too).
if System.get_env("LOAD_REPORTER") in ~w(true 1) do
  config :fathom, :load_reporter, true
end

# Act on handoff warm/drain commands addressed to this node.
if System.get_env("COMMAND_POLLER") in ~w(true 1) do
  config :fathom, :command_poller, true
end

# Run the rebalance control loop (the cron is scheduled everywhere but inert until this).
if System.get_env("REBALANCER_ENABLED") in ~w(true 1) do
  config :fathom, :rebalancer_enabled, true
end

# Where the rebalancer writes the rendered nginx exception map, and how to reload the LB
# (e.g. "nginx -s reload"; unset ⇒ the map is written but reload is applied out of band).
if path = System.get_env("LB_MAP_PATH") do
  config :fathom, :lb_map_path, path
end

if cmd = System.get_env("LB_RELOAD_CMD") do
  config :fathom, :lb_reload_cmd, cmd
end

# Optional config test run against the candidate map BEFORE it's promoted (finding #3): a
# non-zero exit aborts the promotion and keeps the last-good file. `{}` in the command is
# replaced with the candidate temp path (also exported as LB_MAP_CANDIDATE), e.g.
# LB_TEST_CMD="nginx -t -c /etc/nginx/test.conf" where test.conf includes the candidate.
if cmd = System.get_env("LB_TEST_CMD") do
  config :fathom, :lb_test_cmd, cmd
end

# The hot-detection floor (absolute q/s). Unset ⇒ the p99-relative rule.
if floor = System.get_env("REBALANCE_HOT_QPS_FLOOR") do
  config :fathom, :rebalance_hot_qps_floor, String.to_float(floor)
end

# Rebalancer cadence/policy tunables (all have code defaults).
if ms = System.get_env("LOAD_REPORT_INTERVAL_MS") do
  config :fathom, :load_report_interval_ms, String.to_integer(ms)
end

if n = System.get_env("REBALANCE_CONFIRM_WINDOWS") do
  config :fathom, :rebalance_confirm_windows, String.to_integer(n)
end

if ms = System.get_env("REBALANCE_COOLDOWN_MS") do
  config :fathom, :rebalance_cooldown_ms, String.to_integer(ms)
end

# Opt out of the boot-time conditional-write probe (expert review #16) ONLY for rigs where
# storage isn't reachable at boot and the store is known-good. Never in prod.
if System.get_env("VERIFY_STORAGE_FENCE") in ~w(false 0) do
  config :fathom, :verify_storage_fence, false
end

# Warm-standby follower (Phase 2 A1): opt-in per node role.
if System.get_env("WARM_FOLLOWER") in ~w(true 1) do
  config :fathom, :warm_follower, true
end

if dir = System.get_env("WARM_CACHE_DIR") do
  config :fathom, :warm_cache_dir, dir
end

if ms = System.get_env("WARM_POLL_MS") do
  config :fathom, :warm_poll_ms, String.to_integer(ms)
end

# How long after this node last owned a shard the warm follower still treats it as
# "home" and won't re-warm it (a shard routes back to its home, so warming it has no
# failover value). Outlast a routine idle→reopen gap; a real LB remap lapses it.
if ms = System.get_env("WARM_HOME_RETENTION_MS") do
  config :fathom, :warm_home_retention_ms, String.to_integer(ms)
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
  # Anchor Host-subdomain shard routing to the serving zone (expert review #13):
  # with SHARD_BASE_DOMAIN=fathom.example, only <shard>.fathom.example selects a
  # shard; any foreign/misrouted Host fails closed to the default-shard chain.
  # Unset = unanchored (the pre-anchor behavior) — set it in any real deployment.
  # A BLANK env var is a misconfig, not a zone (round-2 #35): `if "" do` is truthy,
  # and an empty configured zone makes zone_matches? deny ALL subdomain routing —
  # fail-closed 400s in prod, but cross-tenant COMMINGLING into :default_shard if
  # that is ever set. Treat blank/whitespace as unset.
  zone = "SHARD_BASE_DOMAIN" |> System.get_env("") |> String.trim()

  if zone != "" do
    config :fathom, :shard_base_domain, zone
  end

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
  # Dedicated token-signing secret (expert review #31), separate from
  # SECRET_KEY_BASE so a data-path secret rotation never touches web sessions/CSRF.
  # Falls back to secret_key_base when unset (backward compatible).
  if token_secret = System.get_env("HRANA_TOKEN_SECRET") do
    config :fathom, :hrana_token_secret, token_secret
  end

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
