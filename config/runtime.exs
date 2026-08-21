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

# Optional web-endpoint bind IP (any env) — WEB_BIND_IP=0.0.0.0 to reach the dashboard from the LAN.
# Unset ⇒ the per-env default (dev binds loopback). NB: on 0.0.0.0 the /admin dashboard is reachable
# on every interface with the configured BasicAuth creds (dev default admin/admin — set
# ADMIN_USER/ADMIN_PASS before exposing it). The prod block below sets its own bind.
if bind = System.get_env("WEB_BIND_IP") do
  case :inet.parse_address(String.to_charlist(bind)) do
    {:ok, ip} -> config :fathom, FathomWeb.Endpoint, http: [ip: ip]
    {:error, _} -> raise "WEB_BIND_IP is not a valid IP address: #{inspect(bind)}"
  end
end

# OpenTelemetry OTLP trace export — enabled only when an endpoint is set, so it stays a no-op
# in dev/test/CI and in any deploy without a collector (config/config.exs defaults to :none).
# The exporter also honors the standard OTEL_EXPORTER_OTLP_* env vars (headers, protocol).
# The checkout-span bridge (:otel_spans) attaches together with the exporter: without a
# collector its handlers built recording spans on every checkout and exported them to
# nothing (expert review 2026-07-23 #3).
if otlp_endpoint = System.get_env("OTEL_EXPORTER_OTLP_ENDPOINT") do
  config :opentelemetry, traces_exporter: {:opentelemetry_exporter, %{}}
  config :opentelemetry_exporter, otlp_endpoint: otlp_endpoint
  config :fathom, otel_spans: true
end

# ---- Shard-plane deploy knobs (env-gated, any config_env) --------------------------------
# These configure the shard data plane for real deployments (a release in a container, the
# deploy/chaos rig). Unset ⇒ the compile-time defaults (Local storage under System.tmp_dir!),
# so dev/test are unaffected.

# An optional integer env var: unset (or unparseable) ⇒ nil, so the reading module falls back to
# its own default rather than to 0.
env_int = fn name ->
  case System.get_env(name) do
    nil ->
      nil

    "" ->
      nil

    v ->
      case Integer.parse(v) do
        {n, _} when n > 0 -> n
        _ -> nil
      end
  end
end

# Same, but 0 is a VALUE rather than a rejection. For knobs where zero means "disable this bound"
# (the replication byte caps), `env_int` above would silently ignore the one setting an operator
# reaches for in an incident — it returns nil for 0, which leaves the default in place.
env_nonneg_int = fn name ->
  case System.get_env(name) do
    nil ->
      nil

    "" ->
      nil

    v ->
      case Integer.parse(v) do
        {n, _} when n >= 0 -> n
        _ -> nil
      end
  end
end

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
      token: System.get_env("AWS_SESSION_TOKEN"),
      # Finch pool sizing (expert review 2026-07-24 #14). The defaults (200/1) are the measured
      # knee on the LOCALHOST MinIO rig — past ~200 connections that server saturates, so
      # `pool_count` can only show a win against real S3. Without these env vars that
      # localhost-derived number was baked into every release with no way to tune a deployment.
      pool_size: env_int.("S3_POOL_SIZE"),
      pool_count: env_int.("S3_POOL_COUNT"),
      conn_max_idle_time: env_int.("S3_CONN_MAX_IDLE_MS")

  other ->
    raise "SHARD_STORAGE must be \"s3\" or \"local\", got: #{inspect(other)}"
end

# Per-stream Hrana idle timeout (ms). Filo's default is 10s and fathom used to pass nothing, so
# this was unreachable on a deployed release (expert review 2026-07-24 #22). It bounds CLIENT THINK
# TIME inside an open transaction, not the server: a stream holds live transaction state, and
# expiring it mid-transaction discards acked work as an opaque STREAM_NOT_FOUND. Each held stream
# costs one shard checkout and ~3 fds, and :max_checkouts_per_shard caps per-tenant exposure.
# Do NOT set this to a value that never expires.
if ms = env_int.("HRANA_STREAM_IDLE_MS") do
  config :fathom, :hrana_stream_idle_ms, ms
end

# Scheduled point-in-time snapshots (expert review 2026-08-01 #18). Both jobs are fleet singletons
# on the Oban crontab and INERT until sized here.
#
# The live durable object is overwritten every :shard_flush_interval_ms (default 5 s), so without
# these the last-good state for a LOGICAL corruption — the more common incident — is gone within
# seconds. Snapshots are the only answer to "restore tenant acme to 09:00".
if n = env_int.("SNAPSHOT_SCHEDULE_SAMPLE") do
  config :fathom, :snapshot_schedule_sample, n
end

if n = env_int.("SNAPSHOT_RETENTION_SAMPLE") do
  config :fathom, :snapshot_retention_sample, n
end

# Grandfather-father-son policy, e.g. "24h,7d,4w" ⇒ keep the newest snapshot in each of the last
# 24 populated hours, 7 populated days and 4 populated ISO weeks. Only snapshots the scheduler
# created (labelled `auto`) are ever eligible; a manual one is never expired.
if spec = System.get_env("SNAPSHOT_RETENTION") do
  policy =
    spec
    |> String.split(",", trim: true)
    |> Enum.reduce(%{}, fn part, acc ->
      case Integer.parse(String.trim(part)) do
        {n, "h"} -> Map.put(acc, :hourly, n)
        {n, "d"} -> Map.put(acc, :daily, n)
        {n, "w"} -> Map.put(acc, :weekly, n)
        _ -> raise "SNAPSHOT_RETENTION: expected e.g. \"24h,7d,4w\", got #{inspect(part)}"
      end
    end)

  # AN EMPTY POLICY IS "KEEP NOTHING", NOT "NOT CONFIGURED" (expert review 2026-08-20 #13).
  # `String.split("", ",", trim: true)` is `[]`, so the reduce returns its `%{}` accumulator and the
  # raise above — which lives inside the per-part branch — never runs. `RetentionJob.policy/0` then
  # matched `%{} = policy` (any map, including the empty one) and `Retention.plan/3` evaluated
  # `Map.get(policy, granularity, 0)` for all three buckets. Every automatic snapshot of every
  # tenant deleted on the next hourly tick, with a normal success log.
  #
  # `""` is not exotic: `System.get_env/1` returns it for a set-but-empty variable, and
  # `SNAPSHOT_RETENTION: "${SNAPSHOT_RETENTION:-}"` is the exact passthrough shape this repo
  # already uses for a dozen variables in `deploy/chaos/docker-compose.yml`. So the act that
  # triggers it is the act of enabling the feature.
  #
  # A policy that would delete 100% of every shard's snapshots refuses to boot rather than running.
  if policy == %{} or Enum.all?(policy, fn {_k, n} -> n == 0 end) do
    raise "SNAPSHOT_RETENTION=#{inspect(spec)} parses to a policy that keeps NOTHING " <>
            "(#{inspect(policy)}) — every automatic snapshot of every tenant would be deleted on " <>
            "the next hourly tick. Give it a real policy (e.g. \"24h,7d,4w\") or UNSET the " <>
            "variable to leave retention off."
  end

  config :fathom, :snapshot_retention, policy
end

# The Django-compatibility SQLite extension (expert review 2026-08-01 #19). Unset means "load
# priv/sqlite_ext/ if the artifact is there", which is what makes an unchanged Django app work
# without an operator having to discover a flag.
#
#   SQLITE_EXTENSION=false          disable — Django's ~35 UDFs become unavailable and its
#                                   __year / __date / Trunc* / __regex lookups raise
#                                   OperationalError, i.e. fathom's pre-#19 behaviour.
#   SQLITE_EXTENSION=/path/to.so    load that file instead of the bundled one.
#
# Loading requires briefly enabling SQLite's extension mechanism; `Fathom.Shard.Extension` closes
# it again before the connection is used, and a failure to close fails the open. See its moduledoc.
case System.get_env("SQLITE_EXTENSION") do
  nil -> :ok
  "false" -> config :fathom, :sqlite_extension, false
  path -> config :fathom, :sqlite_extension, path
end

# How long an idle stream process waits before hibernating (ms). A WebSocket stream lives for hours
# between requests while holding its heap; hibernation gives that back. Raise it if a workload is
# burst-heavy enough that the hibernate/wake pair costs more than the heap it reclaims.
if ms = env_int.("HRANA_STREAM_HIBERNATE_MS") do
  config :fathom, :hrana_stream_hibernate_ms, ms
end

# How many listen sockets the Hrana listener binds (expert review 2026-07-24 #6). The default of 4
# uses ThousandIsland's `reuseport` multi-queue path so acceptors don't all contend on one kernel
# accept queue. `1` is the documented escape hatch — ThousandIsland FAILS STARTUP if the platform
# refuses `reuseport`, so an operator on an exotic kernel needs a way out. That escape hatch was
# compile-time only until now, i.e. unreachable on a release: a node that boot-looped on this could
# not be recovered without a rebuild. It is also the A/B seam (1 vs 4) for the accept-queue claim.
if n = env_int.("HRANA_LISTEN_SOCKETS") do
  config :fathom, :hrana_listen_sockets, n
end

# Kernel accept-queue depth per listen socket. Pairs with HRANA_LISTEN_SOCKETS: together they are
# the two halves of the accept-path fix, and each wants its own A/B. Silently clamped to the OS
# `net.core.somaxconn`, so raising that is a separate node-provisioning step.
if n = env_int.("HRANA_BACKLOG") do
  config :fathom, :hrana_backlog, n
end

# How often Bandit runs a forced full GC on an HTTP/1 connection process, in requests (expert
# review 2026-07-24 #40). Bandit's default is 5, which suits a connection serving a handful of
# requests; fathom's is LB-pooled and serves thousands. Raising it trades connection-process
# memory (× 30k held connections) for fewer forced sweeps — measure BOTH before moving it:
# `chaos.sh tpc-fleet` for throughput and `chaos.sh served` for RSS/shard.
if n = env_int.("HRANA_GC_EVERY_N") do
  config :fathom, :hrana_gc_every_n, n
end

# Compression for stored shard objects (expert review 2026-07-24 #38). Default `none`.
#
# DECODING IS ALWAYS ON regardless of this setting — a node reads any marked object — so the flag
# rolls forward and back without a flag day and mixed-version nodes interoperate. This only
# controls what this node WRITES.
#
# It does NOT speed up single-shard cold-open: that path is RTT-bound at fathom's shard sizes
# (~1 RTT with the body essentially free). It pays on aggregate-bandwidth-bound work — mass
# warming/failover, steady-state PUT volume for write-hot shards, cross-region, storage cost — and
# it spends CPU, which is the contended resource on a loaded node. Measure `warm_s3_shards_per_s`
# under `S3_FAKE_RATE_KBPS` before enabling it fleet-wide.
#
# Applies to the S3 backend; `Storage.Local` (dev/test) stores raw bytes either way. Not
# `String.to_atom` — an unknown value must not mint an atom or silently mean "none".
case System.get_env("SHARD_OBJECT_ENCODING") do
  nil -> :ok
  "" -> :ok
  "none" -> config(:fathom, :shard_object_encoding, :none)
  "zlib" -> config(:fathom, :shard_object_encoding, :zlib)
  other -> raise ~s(SHARD_OBJECT_ENCODING must be "none" or "zlib", got: #{inspect(other)})
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
#
# The DEPLOYMENT default is 300 s, deliberately 60× looser than the library default
# (`Fathom.Shard.@default_flush_interval_ms`, 5 s). Cost analysis 2026-08-07: PUT count is
# driven by how OFTEN a dirty shard uploads, not by how much it uploads (S3 ingress bytes are
# free), and a shard's writes are sparse enough that 5 s / 30 s / 60 s all cost the SAME —
# each write lands in its own flush window regardless. The interval only starts saving money
# once it approaches the session length, so 300 s is the first setting that actually reduces
# the bill (measured ~3× fewer PUTs; ~$1,200/mo at 1M DAU).
#
# 300 s and NOT 0 (`idle-only`), which costs exactly the same: with the periodic flush disabled
# a CONTINUOUSLY ACTIVE shard never flushes at all, so the busiest tenants get an unbounded
# loss window — the worst durability for the most valuable data. 300 s keeps it bounded.
#
# The library default stays 5 s: this repo is public, and a stranger embedding Fathom should
# get the safe RPO, not this deployment's cost tradeoff. Prod-gated for the same reason the
# test suite must keep the tight default — several tests drive a real flush and would other-
# wise wait 5 minutes for it.
if config_env() == :prod do
  config :fathom,
         :shard_flush_interval_ms,
         String.to_integer(System.get_env("SHARD_FLUSH_INTERVAL_MS") || "300000")
else
  if ms = System.get_env("SHARD_FLUSH_INTERVAL_MS") do
    config :fathom, :shard_flush_interval_ms, String.to_integer(ms)
  end
end

if ms = System.get_env("SHARD_IDLE_MS") do
  config :fathom, :shard_idle_ms, String.to_integer(ms)
end

# Rollout sweep per-run cap (#19): how many laggards the hourly ReconcileJob enqueues per run.
# Default 100; raise it (RECONCILE_BATCH_SIZE) to converge a large cold tail faster.
if n = System.get_env("RECONCILE_BATCH_SIZE") do
  config :fathom, :reconcile_batch_size, String.to_integer(n)
end

# Full restore drill (expert review 2026-08-01 #48): per-run sample size for the drill that
# actually RESTORES — forks each sampled shard to a scratch tenant, compares row counts, drops it.
# Separate from RESTORE_DRILL_SAMPLE and much smaller: a fork is a full object copy, so this costs
# real storage I/O per sample where the read-only drill costs a GET. Unset ⇒ off.
if n = System.get_env("RESTORE_DRILL_FULL_SAMPLE") do
  config :fathom, :restore_drill_full_sample, String.to_integer(n)
end

# Warm-cache DISK back-pressure (expert review 2026-08-01 #36). `:warm_cache_max` bounds the
# standby cache in shard COUNT, which says nothing about bytes — 500 shards is 8 MB or 2 TB — and
# the cache shares a filesystem with the live shard data, so filling it fails every cold-open pull
# AND every VACUUM INTO while writes keep being acked.
if b = System.get_env("WARM_DISK_FREE_FLOOR_BYTES") do
  config :fathom, :warm_disk_free_floor_bytes, String.to_integer(b)
end

if b = System.get_env("WARM_CACHE_MAX_BYTES") do
  config :fathom, :warm_cache_max_bytes, String.to_integer(b)
end

# How long a migration job may keep DEFERRING before it is reported as stalled (2026-08-04).
# A `{:retry, _}` (shard busy / lease held) snoozes, and an Oban snooze raises max_attempts
# alongside attempt — so the job never exhausts, never quarantines, and never reaches `failed`.
# Past this window it logs at [warning], emits [:fathom, :migrator, :migration_stalled], and
# counts in `Migrator.status/0`'s `stalled`. Default 10 minutes.
if ms = System.get_env("MIGRATION_STALL_AFTER_MS") do
  config :fathom, :migration_stall_after_ms, String.to_integer(ms)
end

# Per-shard load counters (`Fathom.ShardLoad`) — the Phase-2 rebalancing input. Off by
# default so the hot path doesn't pay for an unread counter; a node/deployment opts in
# (the "turn on :shard_load on a deployed node" knob a rebalancer / hot-spot measurement
# needs). Read live via `Application.get_env`, so it also gates the record_* calls.
if System.get_env("SHARD_LOAD") in ~w(true 1) do
  config :fathom, :shard_load, true
end

# Migrate-on-touch mode (expert review #40): how a checkout handles a shard behind the fleet HEAD
# after a release. `off` (default) — the hourly reconcile converges the cold tail (stale-schema
# window up to the cron; expand-contract makes serving vN-1 correct). `async` — enqueue the shard's
# migration on touch and serve vN-1 this request (converges next job cycle, no inline block).
# `inline` — block the checkout on the full blue/green migration (no stale window, multi-second
# first-request latency). See docs/quickstart-django.md.
case System.get_env("MIGRATE_ON_TOUCH") do
  nil ->
    :ok

  "off" ->
    config :fathom, :migrate_on_touch, :off

  "async" ->
    config :fathom, :migrate_on_touch, :async

  "inline" ->
    config :fathom, :migrate_on_touch, :inline

  other ->
    raise "MIGRATE_ON_TOUCH must be \"off\", \"async\", or \"inline\", got: #{inspect(other)}"
end

# The reserved capture template (`Fathom.Migrator.Capture`): the one shard Django migrates
# directly, whose transaction SQL is recorded as the next fleet version and replayed onto every
# other shard. This had NO env wiring — it was set only in `config/dev.exs` ("demo"), so a
# release could not reach the migration engine's entry point at all without editing config and
# rebuilding, while `mix fathom.snapshot template-head` errored telling the operator to
# "set TEMPLATE_SHARD_ID" — a knob nothing read.
#
# Top-level (not prod-only) so dev/test can override the compiled default too. The two prod boot
# guards already cover a set template: `Fathom.Application.check_template_default!` refuses
# :default_shard == the template (finding #17 — anonymous default traffic would drive fleet-wide
# capture) and `check_template_auth!` refuses a prod template with :hrana_auth disabled (expert
# review #9 — an anonymously reachable template is a fleet-wide poisoning vector).
#
# Cast rather than trust the string: an id that ShardId rejects (or that normalizes to a
# different id than the operator typed) would silently never match the template branch in
# `Fathom.ShardExecutor`, so capture would stay off with the template *looking* configured.
if template = System.get_env("TEMPLATE_SHARD_ID") do
  case Fathom.ShardId.cast(template) do
    {:ok, id} ->
      config :fathom, :template_shard_id, id

    :error ->
      raise "TEMPLATE_SHARD_ID is not a valid shard id: #{inspect(template)}"
  end
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
#
# TRUST ASSUMPTION (expert review #17): the pin decision trusts q_per_s, a signal a tenant
# fully controls from the data path. Enabling the rebalancer presumes the Hrana trust
# boundary is enforced — either the network boundary (LB-only reachability; the default
# `:hrana_auth` disabled posture — see docs/deploy-cluster.md) or `HRANA_AUTH=required`.
# Without it, any LB-reachable caller could drive a shard they don't own over the hot bar to
# induce a handoff (a brief drain blip on the victim). The move is bounded by confirm_windows,
# cooldown_ms, max_moves (1/tick), and the improvement guard, but the boundary is the
# defense — do not enable REBALANCER_ENABLED on a data path open to untrusted callers.
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

# Hard deadlines for the LB config-test / reload shell commands (review 2026-07-09 #2): a hung
# command is killed at the deadline so it can't hold the fleet advisory lock / a pooled
# connection. Defaults 10s each.
if ms = System.get_env("LB_TEST_TIMEOUT_MS") do
  config :fathom, :lb_test_timeout_ms, String.to_integer(ms)
end

if ms = System.get_env("LB_RELOAD_TIMEOUT_MS") do
  config :fathom, :lb_reload_timeout_ms, String.to_integer(ms)
end

# The hot-detection floor (absolute q/s). Unset ⇒ the p99-relative rule. Accepts an integer
# or float and raises at boot on an unusable value (finding #16) — a mis-set floor is an
# operator footgun that would otherwise silently disable the rebalancer.
if floor = System.get_env("REBALANCE_HOT_QPS_FLOOR") do
  config :fathom, :rebalance_hot_qps_floor, Fathom.Rebalancer.parse_hot_qps_floor!(floor)
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

# Floor on how often ONE cached shard's body may be re-transferred (expert review 2026-07-24 #26).
# Defaults to 10× the poll. This is what bounds the follower's steady-state cost: a continuously
# written tenant flushes faster than the poll, so its flush signal advances every cycle and every
# conditional GET comes back 200-with-a-body plus a local fsync — forever, to save one body
# transfer at a failover that may never happen. Raising this trades failover RTO on write-hot
# shards for ingress and device writes; it never trades correctness, because the coordinator
# revalidates before promoting a cached copy.
if ms = env_int.("WARM_MIN_REPULL_MS") do
  config :fathom, :warm_min_repull_ms, ms
end

# Optional hard cap on warm-refresh ingress, in bytes/second, spent lag-first (oldest-checked
# shard first) so a budget too small for the whole set converges the cache round-robin instead of
# starving its tail. Unset ⇒ no cap, and the budget path costs nothing.
if n = env_int.("WARM_REFRESH_BYTES_PER_S") do
  config :fathom, :warm_refresh_bytes_per_s, n
end

# ---- Quorum replication (Phase 2 A2) ------------------------------------------------------
# Off by default, like every other Phase 2 component. Until this section existed A2 had NO
# runtime gate at all — every knob was reachable only from `Application.put_env` in tests, so the
# feature was unshippable regardless of how finished the code was.
#
# READ docs/a2-quorum-replication.md BEFORE enabling. Turning this on puts a network round trip
# inside every tenant COMMIT, and the measured cost is dominated by WHERE the followers are, not
# how many: with two near and two far, a quorum acks in ~1.6 ms while all-N pays 134 ms on the
# same replicas. Placement is the decision; replica count is not.
if System.get_env("REPLICATION_ENABLED") in ~w(true 1) do
  config :fathom, :replication_enabled, true
end

# The follower set, as `node_key@host:port` pairs, e.g.
# REPLICATION_FOLLOWERS="fathom2@10.0.1.2:9100,fathom3@10.0.2.3:9100".
#
# `node_key` matches `Fathom.Rebalancer.node_key/0` (NODE_KEY) so a follower is identifiable in
# logs, telemetry and `Fleet.health/0` rather than an anonymous address.
#
# ORDER AND LOCALITY ARE A LATENCY DECISION. A quorum skips the SLOWEST replicas, so it buys
# nothing unless a quorum's worth are near: measured 2-of-4 tracked 4-of-4 exactly at every
# latency when all four sat at the same distance. With Q=2 that means TWO near followers, and
# they should be in a nearby but DIFFERENT AZ — two in the primary's own AZ means one AZ failure
# takes three of five copies and leaves exactly Q with no slack.
if spec = System.get_env("REPLICATION_FOLLOWERS") do
  config :fathom, :replication_followers, Fathom.Shard.Replication.Fleet.parse_followers!(spec)
end

# Acks required before a tenant's commit returns. MUST be < the follower count, and
# `Fleet.validate_quorum!/0` refuses to boot otherwise: Q=N tolerates zero follower failures and
# inherits the slowest replica's latency — measured 32× worse with one straggler on loopback and
# 82× worse with two far followers.
if n = env_int.("REPLICATION_QUORUM") do
  config :fathom, :replication_quorum, n
end

# Whether a follower fdatasyncs before acking. Off matches Waterpark, which acks from RAM and
# takes durability from replica count. Measured cost of turning it on: ~398 µs against a ~96 µs
# floor, i.e. ~2.4× fathom's whole current request round trip. Off is never WORSE than today
# either — if every replica holding an un-synced frame dies at once, the shard falls back to its
# S3 object, which is exactly the pre-A2 behaviour.
if System.get_env("REPLICATION_FSYNC") in ~w(true 1) do
  config :fathom, :replication_fsync, true
end

# How long a commit waits for its quorum before failing with 503 FILO_NO_QUORUM. This is a
# ceiling on tenant write latency when followers are unreachable, not a target — the quorum
# reports :impossible and returns early as soon as too few followers remain, so this only binds
# when a follower is silent rather than refusing.
if ms = env_int.("REPLICATION_TIMEOUT_MS") do
  config :fathom, :replication_timeout_ms, ms
end

# THE TWO BOUNDS THAT CLOSE THE 1024-TENANT OOM. See
# `docs/reviews/a2-shipper-feedback-loop-2026-08-16.md` — the failure is a positive feedback loop,
# not a leak: a push carries the WAL since the follower's last ACK, so a delayed send makes the
# next payload bigger, which delays it further. Measured on one shipper 40 s apart, the queued
# MESSAGE count fell while the binary held DOUBLED. That is why neither of these is a message count
# and why retuning `REPLICATION_MAX_QUEUE` cannot reach it.
#
# The most WAL a single push may carry (default 1 MiB). This is the one that breaks the loop: a
# capped delta cannot grow without limit no matter how far behind a follower falls. `Session` ships
# in bounded rounds until the follower is current, so the commit still only acks once the quorum
# holds every byte — capping costs latency under lag, never durability.
if n = env_nonneg_int.("REPLICATION_MAX_PUSH_BYTES") do
  config :fathom, :replication_max_push_bytes, n
end

# Total queued WAL bytes this NODE may hold across all its shippers (default 1 GiB). The safety net
# under the cap above, claimed in the committing process before the payload reaches any mailbox —
# a dequeue-time check cannot bound a mailbox a cast fills faster than the process drains it.
#
# It should never bite in health; if it does, that is a signal to look at the link, not to raise it.
# Shedding load in a clean range is how the message-count cap failed: 1,024 turned a clean
# 256-tenant step from 3,505 txn/s / 0 errors into 1,580 / 5,333.
#
# Either may be set to 0 to disable, matching REPLICATION_MAX_QUEUE. Both zeroed restores the
# pre-fix behaviour, OOM included.
if n = env_nonneg_int.("REPLICATION_MAX_QUEUE_BYTES") do
  config :fathom, :replication_max_queue_bytes, n
end

# How long after a push OUR OWN shipper refused before the session retries it (default 1000 ms).
#
# A push can be refused locally without ever reaching the follower — `:already_in_flight` (that
# shard's single waiter is still held by an earlier push) or `:overloaded` (the node byte budget
# above). Neither is the follower's fault and neither leaves it holding the bytes, and nothing else
# re-sends them: the follower stays behind until the shard's NEXT WRITE. On a busy shard that is one
# round; on a QUIET one it is unbounded — and quiet is exactly when it bites, because the
# fire-and-forget ship path is taken precisely when the other followers are already current, i.e.
# when no further write is coming to fix it. The commit returns :ok, so nothing looks wrong.
#
# The retry RE-ENTERS the normal commit path rather than shipping alongside it, and skips entirely
# when a commit has landed since it was armed (that commit already re-planned for every follower).
# An earlier design that shipped in parallel cost -15% throughput and 35x the errors at 512 tenants.
#
# Set to 0 to disable, restoring the stale-replica window.
if ms = env_nonneg_int.("REPLICATION_CATCHUP_MS") do
  config :fathom, :replication_catchup_ms, ms
end

# Let a cold open serve a local REPLICA when it is provably newer than the stored object — the
# failover half of A2, and the only thing that actually turns node-loss RPO from ~300 s into ~0.
#
# DELIBERATELY SEPARATE FROM REPLICATION_ENABLED. Shipping frames is safe and measurable on its
# own; this changes what a cold open SERVES, on the code path that owns the lease fence and the
# provenance sidecar. Run replication first — every flush then stamps its position, so by the time
# this is flipped the comparison data already exists fleet-wide and can be sanity-checked.
#
# Safe by construction in three ways, all of which fail toward the stored object: the object's
# stamp over-claims (it is read after the snapshot), promotion needs the replica STRICTLY ahead,
# and an object with no stamp is never overridable at all. The last one means this is inert for a
# shard until its next flush after upgrading.
if System.get_env("REPLICATION_PROMOTE_ON_OPEN") in ~w(true 1) do
  config :fathom, :replication_promote_on_open, true
end

# SURVIVOR SELECTION. Requires REPLICATION_PROMOTE_ON_OPEN, and completes it.
#
# Promote-on-open serves a fresher replica when the node taking the shard over happens to hold one.
# The LB fails over by consistent hash on the Host subdomain, which knows nothing about
# replication, so "happens to" is doing real work there: measured on the rig, an acked
# quorum-replicated write was LOST while three other nodes held it. With this on, a cold open asks
# every peer where its replica sits, adopts the best one that is provably ahead of the stored
# object, and pulls it over A2's own socket.
#
# COSTS ON THE COLD-OPEN PATH, which is why it is separate from the gate above: one object-position
# read on every promote-eligible open plus one concurrent round trip to each peer (bounded by
# REPLICATION_RECOVERY_TIMEOUT_MS), and a whole-database transfer when a peer wins. A node that
# already holds the freshest copy short-circuits before touching the network.
#
# Fails toward the stored object in every uncertain case — an unreachable fleet, a peer one deploy
# behind, an unstamped object — so there is no state in which it serves older bytes than leaving it
# off would. It also needs REPLICATION_LISTEN on this node: the pull installs through the local
# follower's replica directory.
if System.get_env("REPLICATION_RECOVER_FROM_PEERS") in ~w(true 1) do
  config :fathom, :replication_recover_from_peers, true
end

# How long a cold open waits for peers to answer "where is your replica?". All peers are asked at
# once, so this is one round trip's budget, not N. Default 2000 ms — deliberately short: a peer
# that cannot answer in that time is one whose replica we would rather not wait to transfer either,
# and the fallback is the stored object.
if ms = env_int.("REPLICATION_RECOVERY_TIMEOUT_MS") do
  config :fathom, :replication_recovery_timeout_ms, ms
end

# Budget for transferring a winning peer's replica. Default 60000 ms. This one is a whole tenant
# database over the network, so it is sized like a seed rather than like a query.
if ms = env_int.("REPLICATION_RECOVERY_PULL_TIMEOUT_MS") do
  config :fathom, :replication_recovery_pull_timeout_ms, ms
end

# Bytes per frame when seeding a follower's base copy. Bounds MEMORY on both sides (a seed is a
# whole database); it does not bound head-of-line blocking, since one socket per follower node
# carries every shard. Default 4 MiB.
if n = env_int.("REPLICATION_SEED_CHUNK_BYTES") do
  config :fathom, :replication_seed_chunk_bytes, n
end

# ---- The RECEIVE half: this node acts as somebody's follower --------------------------------
# SEPARATE GATE FROM REPLICATION_ENABLED, and the reason is a real gap this closes (2026-08-10):
# the `Follower` listener was only ever started by the test suite, so a node with replication on
# shipped every commit to addresses where nothing listened, got no acks, and 503'd
# `FILO_NO_QUORUM` on every tenant write — while REPLICATION_FOLLOWERS below documented a port
# fathom never opened.
#
# ROLLOUT ORDER: turn this on FLEET-WIDE FIRST, confirm every node is listening, and only then
# enable REPLICATION_ENABLED anywhere. A node can host others' replicas without replicating its
# own shards, which is exactly why these are two flags and not one.
if System.get_env("REPLICATION_LISTEN") in ~w(true 1) do
  config :fathom, :replication_listen, true
end

# Port the follower listener binds. Default 9100 — the port this file's REPLICATION_FOLLOWERS
# example has always shown, and clear of :hrana_port 8080 and :health_port 8081.
if n = env_int.("REPLICATION_LISTEN_PORT") do
  config :fathom, :replication_listen_port, n
end

# WHICH INTERFACE THE REPLICATION PORT BINDS. This is a security control, not tuning.
#
# The replication protocol has NO AUTHENTICATION: whoever can reach this port can push WAL frames
# into any shard this node follows. Unset, `:gen_tcp` binds EVERY interface, which on a cloud host
# means the public one. The trust boundary is the network — the same posture as `hrana_auth:
# :disabled` — so pin it to a private address and firewall it, exactly as HRANA_BIND_IP does for
# the data plane. The follower logs which interface it bound at boot; read that line.
if bind = System.get_env("REPLICATION_BIND_IP") do
  case :inet.parse_address(String.to_charlist(bind)) do
    {:ok, ip} -> config :fathom, :replication_bind_ip, ip
    {:error, _} -> raise "REPLICATION_BIND_IP is not a valid IP address: #{inspect(bind)}"
  end
end

# Where the follower set comes from. `static` (default) is the hand-maintained
# REPLICATION_FOLLOWERS list; `roster` derives it from the addresses nodes publish to
# `rebalancer_nodes`, refreshed on a timer, so adding or replacing a node stops meaning "edit every
# other node's config".
#
# THE STATIC LIST REMAINS THE FLOOR in roster mode. Whenever the roster cannot supply
# REPLICATION_QUORUM+1 candidates — a fresh fleet, a rolling upgrade where peers publish no
# address, a Postgres outage — membership falls back to it rather than to nothing. And a set
# smaller than quorum+1 is REFUSED outright, keeping the previous one live: `n` shrinking below `q`
# is how `q >= n` starts raising inside a tenant's commit.
#
# Roster mode needs REPLICATION_ADVERTISE_HOST set on the nodes that should be candidates, and
# LOAD_REPORTER on (the beat that publishes the address rides that tick).
if System.get_env("REPLICATION_MEMBERSHIP") == "roster" do
  config :fathom, :replication_membership, :roster
end

# How often roster membership is recomputed. Deliberately slow: this is a control-plane read, not
# a liveness signal, and every membership CHANGE costs a seed per shard on the added follower.
if ms = env_int.("REPLICATION_MEMBERSHIP_POLL_MS") do
  config :fathom, :replication_membership_poll_ms, ms
end

# The host peers should use to reach THIS node's replication port, published to the fleet roster
# (`rebalancer_nodes.replication_address`) so membership can be derived instead of hand-listed.
#
# EXPLICIT, never guessed. A node cannot reliably know which of its addresses a peer can reach —
# the hostname is often a container id and the first non-loopback interface is often the wrong
# one — and publishing an unreachable endpoint is worse than publishing none, because the roster
# then reports the node present while every shipper fails to connect. Unset ⇒ this node is not a
# membership candidate. Set it to the private DNS name or IP peers dial (the same one you would
# have written into REPLICATION_FOLLOWERS by hand).
if host = System.get_env("REPLICATION_ADVERTISE_HOST") do
  config :fathom, :replication_advertise_host, host
end

# Where a follower keeps the replicas it receives. Defaults under System.tmp_dir!/ like
# SHARD_DATA_DIR and WARM_CACHE_DIR — fine for dev, wrong for a node that is somebody's durability
# guarantee. Point it at real local disk; it holds a full copy of every shard this node follows,
# and `fathom.node.disk` reports it as `dir=replica`.
if dir = System.get_env("REPLICATION_DIR") do
  config :fathom, :replication_dir, dir
end

# ---- Admin dashboard (/admin + /admin/metrics) BasicAuth ----------------------------------
# Credentials for the operator surface. The router's admin_auth plug fails closed (503) when
# unset, so the dashboard/scrape is never anonymously reachable — set both to enable it. Read in
# any env so a dev can override the config/dev.exs default; prod has no default (see the warn).
admin_user = System.get_env("ADMIN_USER")
admin_pass = System.get_env("ADMIN_PASS")

if is_binary(admin_user) and is_binary(admin_pass) do
  config :fathom, :admin_auth, username: admin_user, password: admin_pass
end

if config_env() == :prod and (is_nil(admin_user) or is_nil(admin_pass)) do
  # Warn loudly, but don't take the data plane down over a dashboard credential: the request-time
  # admin_auth plug already fails closed (503), so the surface stays non-public without creds.
  IO.warn(
    "ADMIN_USER/ADMIN_PASS unset in prod: /admin and /admin/metrics will fail closed (503). " <>
      "Set both to enable the operator dashboard.",
    []
  )
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
    # Expert review 2026-07-24 #21. The phx.new default of 10 sat under a demand floor of ~21 from
    # Oban ALONE (`queues: [migrations: 10, retirement: 5, rebalance: 3, tenants: 3]`, config.exs),
    # each executing job holding a connection — and ShardMigrationJob's cutover runs `Oban.insert`
    # inside a transaction, holding one for its full duration. On top of that: the Oban
    # Cron/Pruner/Peer plugins, the endpoint and every admin LiveView, Directory.Recorder,
    # Migrator.HeadCache, HranaAuth.Revocations, Tenants.Tombstones/Suspensions,
    # Rebalancer.Reporter, CommandPoller, and Shard.WarmFollower.
    #
    # Size it as sum(queue concurrency) + web concurrency + headroom for the pollers. RAISING
    # `queues:` MEANS RAISING THIS.
    pool_size: String.to_integer(System.get_env("POOL_SIZE") || "25"),
    # DBConnection's defaults (50/1000) were already in force implicitly; they are stated here
    # because they are load-bearing. Past `queue_interval` of saturation, checkouts are DROPPED
    # rather than queued — and the module that most needs them, HranaAuth.Revocations, sits on the
    # stream-open path, where a queued caller pins its Hrana stream. That is the pressure #5 removed
    # from the common case; this bounds what happens when the pool saturates anyway.
    queue_target: String.to_integer(System.get_env("POOL_QUEUE_TARGET_MS") || "50"),
    queue_interval: String.to_integer(System.get_env("POOL_QUEUE_INTERVAL_MS") || "1000"),
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

  # Explicit ack for a prod deployment that intends UNANCHORED Host-subdomain routing (no
  # SHARD_BASE_DOMAIN). Without the zone anchor, shard_from_host promotes any attacker-controlled
  # Host first-label to a shard id, so Fathom.Application.check_shard_base_domain! refuses to boot
  # an exposed data plane with the zone unset (expert review 2026-07-14 #6) — set this to override.
  if System.get_env("ALLOW_UNANCHORED_ROUTING") in ~w(true 1) do
    config :fathom, :allow_unanchored_routing, true
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

  # Per-shard size cap (expert review 2026-07-14 #19): the max SQLite pages a single tenant's db may
  # grow to (size = pages × page_size, default 4096B; e.g. 262144 ≈ 1 GiB). A write past it fails
  # SQLITE_FULL, so one runaway tenant can't inflate flush/cold-open/standby costs fleet-wide.
  # Enforces the "limited dataset per shard" premise.
  #
  # DEFAULTS to 1_048_576 pages ≈ 4 GiB since expert review 2026-07-24 #37 — ~1 GiB under the 5 GiB
  # S3 single-PUT ceiling. Unset used to mean unlimited, which made the first brake a FLUSH failure:
  # past 5 GiB the shard kept ACKNOWLEDGING writes it could never upload, retried forever, and its
  # RPO went unbounded with no operator remedy. A write-time cap cannot lose data (SQLITE_FULL is
  # never acked); a flush-time one already has. Set `SHARD_MAX_PAGE_COUNT=0` to opt out.
  if pages = System.get_env("SHARD_MAX_PAGE_COUNT") do
    config :fathom, :shard_max_page_count, String.to_integer(pages)
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

  # Control-plane abuse throttles (#34; see Fathom.RateLimiter). ON by default in prod with
  # generous limits — brute-force / hammering protection should not be opt-in — tunable via env,
  # `=0` disables. The Hrana token path is HMAC-verified (not brute-forceable); these guard the
  # one shared admin password (#8) and the expensive /api ops (list/export/fork).
  #
  # Admin BasicAuth: lock out a source IP after this many failed attempts within the window.
  admin_fail_max = String.to_integer(System.get_env("ADMIN_AUTH_MAX_FAILURES", "20"))
  if admin_fail_max > 0, do: config(:fathom, :admin_auth_max_failures, admin_fail_max)

  if window = System.get_env("ADMIN_AUTH_WINDOW_MS") do
    config :fathom, :admin_auth_window_ms, String.to_integer(window)
  end

  # /api per-IP request rate: this many requests per window before 429.
  api_rate = String.to_integer(System.get_env("API_RATE_LIMIT", "120"))
  if api_rate > 0, do: config(:fathom, :api_rate_limit, api_rate)

  if window = System.get_env("API_RATE_WINDOW_MS") do
    config :fathom, :api_rate_window_ms, String.to_integer(window)
  end

  # Which peers may speak for a client via `X-Forwarded-For` (expert review 2026-08-01 #35).
  # Comma-separated addresses or CIDR, IPv4 or IPv6.
  #
  # Both throttles above are documented as PER-IP, but `conn.remote_ip` behind a proxy is the
  # PROXY — so unset, they degrade to ONE shared bucket for the whole fleet: one attacker's failed
  # logins lock out every operator (the lockout is checked before credentials are verified), and
  # the /api limit becomes a global cap that limits no attacker. Set this to the addresses of
  # whatever terminates TLS in front of `:4000` — the same thing `force_ssl: rewrite_on:
  # [:x_forwarded_proto]` in prod.exs already assumes exists.
  #
  # UNSET IS FAIL-CLOSED, not fail-open: the header is ignored entirely and behaviour is exactly
  # what it was. Never set this to a range you do not control — a peer inside it can name any
  # client it likes, which would let an attacker both evade their own limit and pin a real
  # operator into the lockout.
  if proxies = System.get_env("TRUSTED_PROXIES") do
    config :fathom,
           :trusted_proxies,
           proxies |> String.split(",", trim: true) |> Enum.map(&String.trim/1)
  end

  # Wildcard-TLS serving (#35): set when the deployment terminates TLS with a `*.<zone>` wildcard
  # cert, which CANNOT serve an id that isn't a DNS-safe label (an underscore id — RFC 6125). With
  # this on, `Tenants.provision`/`fork` REFUSE such an id (422) instead of handing back an un-servable
  # `libsql://<id>.<zone>` URL; off (default) they provision but return a `warnings` entry.
  if System.get_env("WILDCARD_TLS_SERVING") in ~w(true 1) do
    config :fathom, :wildcard_tls_serving, true
  end

  # Fork-from-template (finding #10): birth admitted NOVEL shards at the fleet HEAD from
  # the retained template@HEAD snapshot (mix fathom.snapshot template-head). Off by
  # default; enable only with a template + snapshot in place. Fork failures fall back to
  # born-empty. See config/config.exs and Fathom.Migrator.fork_from_template/1.
  if System.get_env("FORK_FROM_TEMPLATE") in ~w(true 1) do
    config :fathom, :fork_from_template, true
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
