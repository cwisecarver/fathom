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

# The RPO write circuit-breaker (expert review #3). When a node is reachable by clients but cut
# from object storage, its coordinators' heartbeats go stale and durability flushes skip — but by
# default nothing stops them ACKing writes, which are quarantined on partition-heal (a loss window
# equal to the partition duration, not the flush interval). With this on, once a shard's heartbeat
# has been not-valid for longer than ttl + steal_margin (provably stealable), the coordinator fences
# writes: ShardExecutor refuses them with 503 FILO_STALE_LEASE while reads keep serving. Default ON
# in prod (a data-loss guard, like the flush fence itself), off in dev/test. See Fathom.Shard.WriteFence.
config :fathom, :fence_writes_when_stealable, config_env() == :prod

# Soften the cap above: at capacity, evict the least-recently-used IDLE shard (flush +
# drop + release its lease) to admit a new open, rather than refusing with a 503. An idle
# shard is bottomless-backed, so eviction costs only a cold re-open if it's touched again;
# a busy shard (checked-out connections) is never evicted. Set false for a hard cap (503 at
# the limit). Only active when :max_open_shards is finite (see Fathom.Shards.Lru).
config :fathom, :evict_idle_at_capacity, true

# The churn half of finding #14: :max_open_shards bounds how many shards a node holds open;
# this bounds how FAST unseen ids can mint new ones. Grants/sec for NOVEL creations only —
# existing-shard cold opens are never limited, and the directory check behind it fails open.
# nil = off (default; the cold path pays one get_env); prod enables via NOVEL_SHARD_RATE
# (+ NOVEL_SHARD_BURST, default max(10, 2 × rate)). See Fathom.Shards.NovelLimiter.
config :fathom, :novel_shard_rate, nil

# Fork-from-template (finding #10): when true, an admitted NOVEL shard is born AT the fleet
# HEAD by copying the retained `template@HEAD` snapshot (produced by
# `mix fathom.snapshot template-head`) into its stored object, instead of empty at v0 — so a
# new tenant's first ORM query finds its schema. Off by default (the safety valve): current
# born-empty behavior is unchanged until an operator opts in with a snapshot ready
# (prod: FORK_FROM_TEMPLATE=true). Any fork failure falls back to born-empty; a checkout is
# never failed for it. See Fathom.Migrator.fork_from_template/1.
config :fathom, :fork_from_template, false

# In-app bearer-token auth on the Hrana data path (Fathom.HranaAuth). :disabled means the
# trust boundary is the network alone (LB-only reachability — docs/deploy-cluster.md);
# :required makes every stream open present a per-shard Phoenix.Token (mint: mix fathom.token).
# Prod opts in via HRANA_AUTH=required (config/runtime.exs). Any other value fails closed
# to :required, and a boot guard refuses :required without a usable secret_key_base.
config :fathom, :hrana_auth, :disabled

# Oban runs the shard migration rollout (per-shard migration jobs + scheduled
# retirement of retained old versions). The reconcile cron re-runs the sweep so
# the cold tail converges to HEAD and drift self-heals.
# The rebalance cron (Phase-2 B1) runs every minute but is inert unless
# `:rebalancer_enabled` is set — Oban peer leadership makes it a fleet singleton.
# The Pruner caps completed/cancelled/discarded job retention (finding #12): the rebalance
# cron inserts+completes a row every minute in every env (~1,440/day) even while inert, so
# without it the oban_jobs table grows unbounded. 7 days keeps a week of migration/rebalance
# history for debugging while staying bounded. It only touches terminal jobs, so the
# live-state uniqueness the migrator/handoff jobs rely on is unaffected.
config :fathom, Oban,
  repo: Fathom.Repo,
  # Every executing job holds a Fathom.Repo connection for its Postgres work, so this sum (21) is a
  # floor on repo pool demand before the endpoint, the pollers, and the near-hot-path
  # HranaAuth.Revocations reads are counted. RAISING THESE MEANS RAISING `POOL_SIZE` in
  # config/runtime.exs, which is sized against them (expert review 2026-07-24 #21).
  queues: [migrations: 10, retirement: 5, rebalance: 3, tenants: 3],
  plugins: [
    {Oban.Plugins.Pruner, max_age: 60 * 60 * 24 * 7},
    # Rescue jobs stranded in `executing` by a node that died mid-run (expert review 2026-07-24
    # #33). Nothing else transitions them: the Pruner only touches terminal states, so the row
    # sits in `executing` forever. Every per-shard worker is `unique` over states INCLUDING
    # `:executing` — ShardMigrationJob, RevertJob, DeleteJob and HandoffJob — so one stranded row
    # means that shard can never have another migration, revert, delete or handoff enqueued: the
    # lazy path, the hourly ReconcileJob and the rollout sweep all dedup against a ghost.
    # HandoffJob is worst, with `period: :infinity` making the block permanent and
    # RebalanceJob.handoff_in_flight?/1 counting `executing`, so the dead-pin reconciler skips
    # that shard forever and it stays pinned to a node that no longer exists. The symptom is
    # "this tenant never migrates / can't be deleted / stays pinned" with no error anywhere.
    #
    # 60 minutes, matching @default_migration_stale_seconds. rescue_after MUST exceed the longest
    # real job runtime, or Lifeline rescues a job that is still running on a live node and you get
    # two concurrent runs. The long pole is the migration copy, which benches at ~7.8M rows/s, so
    # even a 5 GB shard finishes orders of magnitude inside this window.
    #
    # Re-running a rescued job is safe: DeleteJob and ReconcileJob document idempotency
    # explicitly, the migration/revert/handoff jobs are re-entrant by construction (each step
    # tolerates already-done state), and the S3 lease + epoch fence independently prevents a stale
    # actor from double-writing regardless of what Oban does. Against that, the status quo is a
    # permanent wedge.
    {Oban.Plugins.Lifeline, rescue_after: :timer.minutes(60)},
    {Oban.Plugins.Cron,
     crontab: [
       {"0 * * * *", Fathom.Migrator.ReconcileJob},
       {"* * * * *", Fathom.Rebalancer.RebalanceJob},
       # Automated sampled restore drill (#24): a fleet singleton (peer leadership) that verifies the
       # least-recently-verified shards' durable objects daily. INERT unless `:restore_drill_sample`
       # is set — it inserts+completes a no-op job/day until then (bounded by the Pruner).
       {"0 6 * * *", Fathom.RestoreDrillJob}
     ]}
  ]

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

# os_mon is pulled in ONLY for `:disksup`, which is the sole OTP-provided way to read filesystem
# free space (expert review 2026-08-01 #36 — there is no pure-BEAM statvfs). Configure it down to
# just that:
#
#   * memsup/cpu_sup off — we already publish BEAM memory (`fathom.node.memory`) and cpu_sup spawns
#     a port program per node for a number nothing consumes.
#   * `disk_almost_full_threshold: 1.0` disables os_mon's OWN alarm_handler alarms. It alarms per
#     MOUNT, including mounts fathom has nothing to do with (on a dev mac that means every
#     CoreSimulator volume), and fathom emits its own `fathom.node.disk.used_ratio` gauge for the
#     two directories it actually writes to. One signal, scoped correctly, beats two.
#   * a long poll period because `disksup`'s own periodic sweep shells out across every mount;
#     `Fathom.Admin.Measurements.disk/0` reads the paths it needs on the 10 s telemetry poller.
config :os_mon,
  start_memsup: false,
  start_cpu_sup: false,
  start_disksup: true,
  disk_almost_full_threshold: 1.0,
  disk_space_check_interval: 30
