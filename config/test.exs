import Config

# Configure your database
#
# The MIX_TEST_PARTITION environment variable can be used
# to provide built-in test partitioning in CI environment.
# Run `mix help test` for more information.
config :fathom, Fathom.Repo,
  username: "cwisecarver",
  hostname: "localhost",
  database: "fathom_test#{System.get_env("MIX_TEST_PARTITION")}",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: System.schedulers_online() * 2

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :fathom, FathomWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "D7yOOv/4zsQ+P9K2/PPv+avn5sbSc6BexJFlXdMN0momZFdRO2dCXzjclX0toSw9",
  server: false

# Don't bind the Hrana or health listener ports during tests.
config :fathom, hrana_server: false
config :fathom, health_server: false

# Exercise the `?db=` / `x-fathom-shard` fallbacks in shard_from_conn tests (off by default —
# finding #4). The dedicated #4 regression test flips this to false locally to prove the gate.
config :fathom, :allow_shard_override, true

# Fallback shard for the shard_from_conn tests (unset in prod ⇒ fail closed — finding #26; the
# #26 regression test flips this to nil to prove the fail-closed path).
config :fathom, :default_shard, "demo"

# Don't run the node heartbeat in tests (it does periodic storage I/O); the
# heartbeat tests start it themselves with a short TTL.
config :fathom, heartbeat_server: false

# Don't run the background orphan-temp reaper in tests (it does periodic disk I/O
# over the shared shard data dir); the reaper test starts it / drives sweep/0 itself.
config :fathom, temp_reaper: false

# Skip the checkout -> OpenTelemetry span bridge in tests (telemetry events + metrics still fire).
config :fathom, otel_spans: false

# Admin observability layer off in test (default on elsewhere): no in-process Prometheus reporter
# singleton, and Fathom.Admin.FlushWatermark writes no-op. Tests that exercise the metrics/RPO
# paths flip this on for their scope.
config :fathom, :metrics_collector, false

# Admin dashboard BasicAuth creds for the LiveView tests (the fail-closed path test clears them).
config :fathom, :admin_auth, username: "admin", password: "secret"

# Keep the filesystem storage backend's "remote" under a test-specific dir.
# (Tests that exercise idle flush set a short :shard_idle_ms themselves.)
config :fathom, Fathom.Shard.Storage.Local,
  dir: Path.join(System.tmp_dir!(), "fathom_remote_test")

# Don't touch the Postgres directory from the shard data path by default in test
# (the shard/storage/lease tests aren't sandbox-checked-out). The directory
# wiring test flips this on explicitly.
config :fathom, :directory_touch, false

# Effectively disable the Directory.Recorder's periodic flush in test so it never
# races a deterministic assertion; tests drive Fathom.Directory.Recorder.flush/0
# synchronously instead.
config :fathom, :directory_flush_ms, 600_000

# Same for the HeadCache background poll: quiet it so it never fires a Postgres read
# outside a sandbox owner (noise) — tests drive Fathom.Migrator.HeadCache.refresh/0.
config :fathom, :migrator_head_ttl_ms, 3_600_000

# Oban: don't run queues/plugins in test; jobs are inserted and exercised with
# Oban.Testing (perform_job / assert_enqueued).
config :fathom, Oban, testing: :manual

# In test we don't send emails
config :fathom, Fathom.Mailer, adapter: Swoosh.Adapters.Test

# Disable swoosh api client as it is only required for production adapters
config :swoosh, :api_client, false

# Print only warnings and errors during test
config :logger, level: :warning

# Initialize plugs at runtime for faster test compilation
config :phoenix, :plug_init_mode, :runtime

# Enable helpful, but potentially expensive runtime checks
config :phoenix_live_view,
  enable_expensive_runtime_checks: true

# Sort query params output of verified routes for robust url comparisons
config :phoenix,
  sort_verified_routes_query_params: true
