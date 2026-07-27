import Config

# Configure your database
#
# The MIX_TEST_PARTITION environment variable can be used
# to provide built-in test partitioning in CI environment.
# Run `mix help test` for more information.
# The Postgres role defaults to your OS user (PGUSER, else USER) so a fresh checkout runs the
# suite unedited on a normal local Postgres; override PGUSER/PGPASSWORD/PGHOST if needed.
config :fathom, Fathom.Repo,
  username: System.get_env("PGUSER") || System.get_env("USER") || "postgres",
  password: System.get_env("PGPASSWORD"),
  hostname: System.get_env("PGHOST") || "localhost",
  database: "fathom_test#{System.get_env("MIX_TEST_PARTITION")}",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: System.schedulers_online() * 2,
  # DBConnection SHEDS LOAD by design: past `queue_target` it starts dropping queued checkouts
  # (default 50ms target / 1000ms interval). That is correct for a production data plane — a
  # dropped checkout is better than an unbounded queue — but it is never what you want in a test
  # run, where the sandbox deliberately funnels many processes through one shared connection.
  # Under machine load the pool dropped a request after 119ms and a passing test failed with
  # "connection not available and request was dropped from queue" (2026-07-27,
  # Fathom.Rebalancer.HandoffJobTest at load average ~58), which says nothing about the code.
  # Test-env only; production sizing lives in config/runtime.exs (POOL_QUEUE_TARGET_MS).
  queue_target: 5_000,
  queue_interval: 10_000

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

# Deterministic flush timing in tests: no ±jitter on the periodic durability-flush timer
# (expert review #17). The jitter unit test opts back in explicitly to check the range.
config :fathom, :shard_flush_jitter_ratio, 0.0

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

# The lifecycle deny-set singletons (Tombstones/Suspensions) boot-load from Postgres before any
# sandbox checkout, so their app-startup load fails (ownerless) and now fast-retries (#33). Push
# that first retry an hour out so it never spins during the suite; the #33 boot-retry test starts
# its own isolated instances with a short :retry_ms.
config :fathom, :tenant_denylist_retry_ms, 3_600_000

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
