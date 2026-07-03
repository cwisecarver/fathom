# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

config :fathom,
  ecto_repos: [Fathom.Repo],
  generators: [timestamp_type: :utc_datetime],
  # The compile-time environment, baked in so runtime code (the #17 template/default boot guard in
  # Fathom.Application) can gate prod-only checks without Mix available at runtime.
  env: config_env()

# Per-node admission cap on concurrently-open shards (finding #14). A novel valid Host id creates a
# coordinator + ~3 fds + local file + S3 lock PUT + Postgres row; unbounded, that runs into the
# emfile cliff and degrades the whole node. A finite default flips unbounded→bounded — conservative
# under a 65536 ulimit (~3 fds/shard) and far below the measured ~50k RSS / ~82k fd ceiling. Operators
# raise it from the measured density (mix fathom.scale --ramp) via MAX_OPEN_SHARDS (config/runtime.exs).
config :fathom, :max_open_shards, 10_000

# In-app bearer-token auth on the Hrana data path (Fathom.HranaAuth). :disabled means the
# trust boundary is the network alone (LB-only reachability — docs/deploy-cluster.md);
# :required makes every stream open present a per-shard Phoenix.Token (mint: mix fathom.token).
# Prod opts in via HRANA_AUTH=required (config/runtime.exs). Any other value fails closed
# to :required, and a boot guard refuses :required without a usable secret_key_base.
config :fathom, :hrana_auth, :disabled

# Shard databases (libSQL/Turso) are opened dynamically per shard via
# Fathom.ShardRepo. It is intentionally NOT listed in :ecto_repos (there is no
# single static shard database) and is not started in the supervision tree.
config :fathom, Fathom.ShardRepo, pool_size: 1

# Oban runs the shard migration rollout (per-shard migration jobs + scheduled
# retirement of retained old versions). The reconcile cron re-runs the sweep so
# the cold tail converges to HEAD and drift self-heals.
config :fathom, Oban,
  repo: Fathom.Repo,
  queues: [migrations: 10, retirement: 5],
  plugins: [{Oban.Plugins.Cron, crontab: [{"0 * * * *", Fathom.Migrator.ReconcileJob}]}]

# Configure the endpoint
config :fathom, FathomWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: FathomWeb.ErrorHTML, json: FathomWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: Fathom.PubSub,
  live_view: [signing_salt: "FBDTxJQI"]

# Configure the mailer
#
# By default it uses the "Local" adapter which stores the emails
# locally. You can see the emails in your browser, at "/dev/mailbox".
#
# For production it's recommended to configure a different adapter
# at the `config/runtime.exs`.
config :fathom, Fathom.Mailer, adapter: Swoosh.Adapters.Local

# Configure esbuild (the version is required)
config :esbuild,
  version: "0.25.4",
  fathom: [
    args:
      ~w(js/app.js --bundle --target=es2022 --outdir=../priv/static/assets/js --external:/fonts/* --external:/images/* --alias:@=.),
    cd: Path.expand("../assets", __DIR__),
    env: %{"NODE_PATH" => [Path.expand("../deps", __DIR__), Mix.Project.build_path()]}
  ]

# Configure tailwind (the version is required)
config :tailwind,
  version: "4.1.12",
  fathom: [
    args: ~w(
      --input=assets/css/app.css
      --output=priv/static/assets/css/app.css
    ),
    cd: Path.expand("..", __DIR__)
  ]

# Configure Elixir's Logger
config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

# OpenTelemetry: traces for cluster operations (cold-open, checkout). OFF by default — no
# exporter. config/runtime.exs turns on the OTLP exporter when OTEL_EXPORTER_OTLP_ENDPOINT is
# set. service.name surfaces in the traces. Metrics stay on Telemetry.Metrics (see Fathom.Telemetry).
config :opentelemetry,
  traces_exporter: :none,
  resource: %{service: %{name: "fathom"}}

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
