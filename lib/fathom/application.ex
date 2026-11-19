defmodule Fathom.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    # Expert review #6: this boot's identity. Lease owners are node()#<nonce>, so a
    # lock can never be silently reclaimed (same owner, same epoch) by a DIFFERENT
    # incarnation of the same node name — a replaced pod goes through the steal path
    # (liveness check + epoch bump) like any other contender. Set here, before the
    # tree, so every reader sees one value with no initialization race.
    :persistent_term.put(
      {Fathom, :incarnation},
      Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)
    )

    Fathom.BootChecks.run!()

    # `SHARD_ACQUIRE_TRACE=debug`: let the verbose lock-lifecycle lines through without lowering the
    # node's global level (see `Fathom.Shard.Storage.trace_debug/4`).
    if Application.get_env(:fathom, :acquire_trace) == :debug,
      do: Logger.put_module_level(Fathom.Shard.Storage, :debug)

    # Node-wide SQLite page-cache bound (expert review 2026-10-08 #8). Before the tree, so no tenant
    # connection opens ahead of it; process-global, so this one trusted `:memory:` connection covers
    # every connection the node will ever open. Unset (dev/test default) ⇒ untouched.
    _ = Fathom.Shard.Connection.apply_soft_heap_limit()
    :ok = Fathom.Shard.Connection.create_live_table()

    # Grouped into plane sub-supervisors (each with its own restart budget) rather than
    # one flat list, so a control-plane restart-storm (e.g. Repo) is contained to its
    # subtree instead of counting against the top supervisor's budget and taking the
    # data plane down with it. Start order Infra -> ControlPlane -> DataPlane -> Edge
    # respects every cross-plane dependency (see each group below).
    children = [
      plane(Fathom.Infra.Supervisor, infra_children()),
      plane(Fathom.ControlPlane.Supervisor, control_plane_children()),
      plane(Fathom.DataPlane.Supervisor, data_plane_children()),
      plane(Fathom.Edge.Supervisor, edge_children())
    ]

    # Top is :one_for_one so the planes restart independently — a data-plane restart
    # never disturbs the edge listeners, and vice versa.
    opts =
      [strategy: :one_for_one, name: Fathom.Supervisor] ++ restart_budget(:top)

    # Every boot-time `:telemetry.attach` (reporters, OTel bridge, admin events) has run once the
    # tree is up: freeze the handler table (expert review 2026-10-20 #34).
    with {:ok, _pid} = started <- Supervisor.start_link(children, opts) do
      :ok = Fathom.Telemetry.persist_handlers()
      started
    end
  end

  # Run the BOUNDED drain before the supervision tree comes down (expert review 2026-08-01 #16).
  #
  # `Fathom.Shards.drain_all/1` already caps drain concurrency and gives each coordinator a slice
  # of a total budget — but it was reachable only as an opt-in release pre-stop rpc. A bare
  # SIGTERM (`docker stop`, a crash-adjacent restart, a plane-supervisor restart per #22) skipped
  # it entirely and took the unbounded path: `DynamicSupervisor` terminates every child
  # simultaneously under ONE wall-clock timer, so every open coordinator ran its
  # checkpoint/snapshot + full-object PUT at once, saturating both the dirty-IO scheduler pool
  # and the Finch pool. Whatever had not finished when the shutdown budget expired was killed
  # mid-flush — and because the timer is shared by the whole group, the loss is not random, it is
  # systematically whichever shards the pool served last.
  #
  # `prep_stop/1` runs before the tree is torn down, so this is the same bounded path the rpc
  # takes, now on by default. The rpc remains useful as the EARLIER step: it deregisters from the
  # LB first, so traffic stops arriving before the drain begins.
  @impl true
  def prep_stop(state) do
    if Application.get_env(:fathom, :drain_on_shutdown, true) do
      outcome = Fathom.Shards.drain_all()
      Logger.info("prep_stop: drained shards #{inspect(outcome)}")
    end

    state
  rescue
    # Never let a drain failure prevent shutdown — the supervisor teardown still flushes what it
    # can, and a raise here would skip that too.
    e ->
      Logger.error("prep_stop: drain_all failed (#{Exception.message(e)}); continuing shutdown")
      state
  end

  # A named sub-supervisor grouping one plane's children with its own :one_for_one
  # restart budget.
  defp plane(name, children) do
    opts = [strategy: :one_for_one, name: name] ++ restart_budget(:plane)

    %{
      id: name,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, opts]}
    }
  end

  # Restart budgets for the plane supervisors and the top supervisor (expert review
  # 2026-08-01 #22). Both used to inherit OTP's default 3-in-5s.
  #
  # Finding #16 recognised this hazard one level down and deliberately sized
  # `Fathom.ShardSupervisor` to 100-in-10s, with the comment that a smaller budget "would
  # exceed 3-in-5s and terminate the supervisor, killing every co-resident shard and risking a
  # DataPlane -> top cascade." The plane supervisors were added later and never got the same
  # treatment — so the cascade that comment describes was reachable from ABOVE.
  #
  # `Fathom.DataPlane.Supervisor` holds the Registry and ShardSupervisor alongside a dozen
  # always-on siblings, several of which do real I/O in callbacks and can raise repeatably
  # (`Heartbeat.do_renew/1` and its clear-previous-incarnation handler call `Storage.*` inline;
  # `TempReaper.handle_continue(:sweep)` does a fleet-sized `Path.wildcard` — now rescued
  # (expert review 2026-08-31 #17), so it degrades to a logged skip rather than crash-looping,
  # but the budget still backstops the others).
  # Four crashes of any one of those in five seconds took EVERY OPEN SHARD down at once — the
  # worst possible input to #16's own budget — while the Edge plane kept running, so live Hrana
  # streams held connections to files the terminating coordinators were about to unlink. That
  # inverts the stated architecture, where a wobble in one plane cannot restart another.

  @default_plane_max_restarts 30
  @default_plane_max_seconds 10

  @doc false
  def restart_budget(:plane) do
    [
      max_restarts:
        Application.get_env(:fathom, :plane_max_restarts, @default_plane_max_restarts),
      max_seconds: Application.get_env(:fathom, :plane_max_seconds, @default_plane_max_seconds)
    ]
  end

  @doc false
  def restart_budget(:top) do
    [
      max_restarts:
        Application.get_env(:fathom, :supervisor_max_restarts, @default_plane_max_restarts),
      max_seconds:
        Application.get_env(:fathom, :supervisor_max_seconds, @default_plane_max_seconds)
    ]
  end

  # Shared infrastructure: web telemetry, the orchestration DB, clustering, pub/sub, jobs.
  # Everything else depends on Repo/PubSub, so this plane starts first.
  defp infra_children do
    [
      Fathom.Repo,
      # PARTITIONED like `Fathom.ShardRegistry` (expert review 2026-08-26 #18), and for the same
      # reason that Registry was: it now holds the same one-row-per-coordinator population.
      #
      # `Phoenix.PubSub.Supervisor` defaults `pool_size` to `ceil(schedulers / 4)` — 2-4 on a
      # typical box — while the shard Registry is explicitly given `System.schedulers_online()`
      # because "the default single partition serializes every coordinator
      # registration/unregistration/death-cleanup through one process". Every open coordinator
      # subscribes to ONE heartbeat topic, so the subscribe/unsubscribe path on shard open and
      # close hits the same serialization the Registry was widened to avoid.
      #
      # This does NOT make the broadcast itself cheap — measured at p50 35 ms against 30 000
      # subscribers, which is why the fan-out moved off the heartbeat process in
      # `Heartbeat.mark_lapse/1`. The two are independent halves of #18, and the finding says so.
      {Phoenix.PubSub, name: Fathom.PubSub, pool_size: System.schedulers_online()},
      # Node-local ETS fixed-window counter behind the control-plane throttles (expert review #34):
      # admin BasicAuth brute-force lockout + the /api request-rate limit. No deps (just owns the
      # table + sweeps it), so it comes up early, before the endpoint accepts requests.
      Fathom.RateLimiter,
      {Oban, Application.fetch_env!(:fathom, Oban)}
    ] ++ host_monitor_children()
  end

  # Host-level tail attribution (expert review 2026-10-29 #17): `:erlang.system_monitor` stall
  # counters (default on in prod, `SYSTEM_MONITOR`) and the 1 Hz fsync probe (opt-in,
  # `FSYNC_PROBE`). Both only emit telemetry, and neither belongs to a data-plane restart budget.
  defp host_monitor_children do
    system_monitor =
      if Application.get_env(:fathom, :system_monitor, false),
        do: [Fathom.Host.SystemMonitor],
        else: []

    fsync_probe =
      if Application.get_env(:fathom, :fsync_probe, false),
        do: [Fathom.Host.FsyncProbe],
        else: []

    system_monitor ++ fsync_probe
  end

  # Control plane (the orchestration store's readers/writers). Needs Repo (Infra) up.
  defp control_plane_children do
    [
      # Coalesces per-checkout directory accesses off the data path and batch-flushes
      # them to Postgres.
      #
      # Explicit 30 s shutdown, not `use GenServer`'s default 5 s (expert review 2026-08-01 #38).
      # Planes stop Edge → DataPlane → ControlPlane → Infra, so by the time this terminates every
      # coordinator's terminate-flush has already run and each one called `record_flush/1`. Its
      # buffer therefore holds a `last_flushed_at` row for every shard the node just flushed, and
      # `terminate/2` is the only chance to persist them. A control-plane process holding node-wide
      # durability metadata has to outlive the data plane's flush burst — truncating it makes
      # `loss-report` under-read the loss on exactly the node that just went down. The child's own
      # budget (`:directory_recorder_shutdown_budget_ms`, 25 s) gives up inside this and logs.
      Supervisor.child_spec(Fathom.Directory.Recorder, shutdown: 30_000),
      # TTL-cached fleet HEAD so the lazy-migrate checkout path never runs a
      # per-checkout `max(version)` on Postgres. Cheap when unused (polls only while
      # `:migrate_on_touch` is `:async` or `:inline`).
      Fathom.Migrator.HeadCache,
      # Read-through cache of per-shard Hrana-token revocation versions, so token
      # verification stays off the Postgres hot path (expert review #31).
      Fathom.HranaAuth.Revocations,
      # ETS set of deleted (tombstoned) shard ids — the admission re-mint guard for tenant
      # deletion (expert review #15). Loaded from the directory at boot, pushed fleet-wide on
      # delete over Oban's notifier, refreshed periodically. Off the Postgres hot path.
      Fathom.Tenants.LocalEraser,
      Fathom.Tenants.Tombstones,
      # ETS set of suspended shard ids — the admission deny gate for tenant suspension (expert
      # review #20). Same shape as Tombstones but reversible (resume clears it; the periodic
      # refresh reconciles). Off the Postgres hot path.
      Fathom.Tenants.Suspensions,
      # Captures template-shard Django migrations into fleet versions.
      Fathom.Migrator.Capture,
      # Runs handoff warm/drain commands concurrently off the CommandPoller (finding #8), so
      # a slow drain never head-of-line-blocks a warm the node needs. Unconditional + idle
      # when unused, so the poller (and tests that start_supervised it) can rely on it.
      {Task.Supervisor, name: Fathom.Rebalancer.TaskSupervisor},
      # In-memory ring of recent rebalancer decisions + last LB-apply status for /admin/rebalancer
      # (expert review 2026-10-15 #13). Unconditional: it is an idle ETS table until written.
      Fathom.Rebalancer.DecisionLog
    ] ++ reporter_children() ++ command_poller_children()
  end

  # Per-node load reporter (Phase-2 B1): publishes this node's hot shards to Postgres so
  # the rebalancer can read a merged fleet view. Gated `:load_reporter`, off by default
  # (needs `:shard_load` on to have anything to report). Reads ShardLoad lazily on its
  # timer (one interval in), so starting before the DataPlane's ShardLoad table is fine.
  defp reporter_children do
    if Application.get_env(:fathom, :load_reporter, false),
      do: [Fathom.Rebalancer.Reporter],
      else: []
  end

  # Per-node handoff command executor (Phase-2 B1): acts on warm/drain commands the
  # rebalancer addresses to this node (the cross-node channel there's no BEAM cluster
  # for). Gated `:command_poller`, off by default. Acts on its timer, so starting before
  # the DataPlane is fine.
  defp command_poller_children do
    if Application.get_env(:fathom, :command_poller, false),
      do: [Fathom.Rebalancer.CommandPoller],
      else: []
  end

  # Data plane (serves shards). Kept separate from the control plane so a control-plane
  # wobble can't restart it — that decoupling is the whole architecture's thesis.
  # Public (@doc false) so the ordering invariant below is testable.
  @doc false
  def data_plane_children do
    [
      # Dedicated Finch pool for S3 shard storage. Started unconditionally: idle pools
      # hold no connections, so it's free when the backend is Local or S3 is idle.
      # Sizing it larger than Req's default ~50-conn pool lets warming pull many shards
      # from S3 at once (startup/failover) without bottlenecking on the conn ceiling.
      Fathom.Shard.Storage.S3.finch_child_spec(),
      # Per-owner memo of heartbeat reads for the S3 steal path (review 2026-07-23 #13):
      # a mass failover steals many shards from ONE dead owner, and each steal re-read
      # the same heartbeat object. Tiny public ETS; the backend degrades to per-call
      # reads when it's absent.
      Fathom.Shard.Storage.HeartbeatCache,
      # Rate-limits novel-shard creation (finding #14's churn half). Started
      # unconditionally — idle when `:novel_shard_rate` is unset (the default).
      Fathom.Shards.NovelLimiter,
      # Owns the per-shard load-counter ETS table. Before the shard supervisor so the
      # table is up before any coordinator records or forgets.
      Fathom.ShardLoad,
      # Owns the per-shard query-latency histogram ETS table (the tail-latency companion to
      # ShardLoad; rides its :shard_load gate). Before the shard supervisor, same as ShardLoad.
      Fathom.ShardLatency,
      # Node-local recency index for idle-eviction at capacity. Before the shard
      # supervisor so the table exists before any checkout stamps or terminate forgets.
      Fathom.Shards.Lru,
      # The rate gate's node-local "known shard" set (expert review 2026-10-01 #18). Before the
      # shard supervisor so the first open can record into it.
      Fathom.Shards.KnownShards,
      # The post-drain_for_migration re-open gate (expert review 2026-10-12 #23). Before the shard
      # supervisor so `Shards.ensure/1` can consult it from the first open.
      Fathom.Shards.MigrationGate,
      # Short negative cache for "held by another node" on cold checkouts (expert review 2026-10-14 N4).
      Fathom.Shards.HeldCache,
      # Per-shard cold-admission single-flight tables (expert review 2026-10-21 r7 #6). Before the
      # shard supervisor so the first cold open can lead a flight.
      Fathom.Shards.AdmissionFlight,
      # Owns the per-shard write-counter ETS table (the dirty-flag signal, off the coordinator
      # mailbox — finding #27). Always on (a data-loss invariant), before the shard supervisor.
      Fathom.Shard.WriteCounter,
      # Owns the idle-drop → reopen gap table (expert review 2026-10-01 #10). Observability only;
      # before the shard supervisor so the first idle stop has somewhere to record.
      Fathom.Shard.ReopenGap,
      # Owns the per-shard flush-watermark ETS table (the metrics layer's RPO/dirtiness source).
      # Always-supervised owner so reads never crash; writes are gated by Fathom.Admin.enabled?.
      # Before the shard supervisor so the table is up before any coordinator publishes/forgets.
      Fathom.Admin.FlushWatermark,
      # Bounded in-memory ring of recent lease/quarantine events (expert review 2026-10-15 #21).
      Fathom.Admin.LeaseEvents,
      # Owns the write-fence ETS table (expert review #3): coordinators publish "provably stealable"
      # shards here and ShardExecutor reads it lock-free before each write. Before the shard
      # supervisor so the table is up before any coordinator publishes/forgets.
      Fathom.Shard.WriteFence,
      # Owns the under-quorum commit marks (Jepsen finding 7, 2026-10-25): ShardExecutor marks a
      # shard whose commit failed its quorum and confirms before serving it again.
      Fathom.Shard.Replication.QuorumFence,
      # Owns the node-global schema generation (expert review 2026-08-26 #7): bumped when any
      # connection runs DDL, read by Connection's statement cache to know its cached COLUMN LIST
      # may be stale. Before the shard supervisor so the table is up before any statement prepares.
      Fathom.Shard.SchemaGen,
      # Owns the node-wide concurrent-flush counter (expert review #17): coordinators reserve a slot
      # before spawning a durability-flush task, bounding the flush storm after a mass re-home.
      # Idle unless :shard_flush_max_concurrency is set. Before the shard supervisor so the table is
      # up before any coordinator flushes.
      Fathom.Shard.FlushGate,
      # Scan-only permit (quick_check + VACUUM INTO), SDio-2: keeps a flush wave from holding every
      # dirty-IO thread (expert review 2026-10-20 #19). Fails open if not running.
      Fathom.Shard.ScanGate,
      # Separate permit for the NON-flush whole-shard scans (migration replay + verify, restore drill,
      # tenant export), a quarter of SDio (expert review 2026-10-29 #3). Independent of ScanGate so a
      # rollout can neither starve flushes nor be starved by them. Fails open if not running.
      Fathom.Shard.BackgroundScanGate,
      # Owns the node-wide idle pooled-handle budget (perf review 2026-10-14 #16). Before the shard
      # supervisor so it outlives every coordinator's terminate-time pool close.
      Fathom.Shard.PoolBudget,
      # Serializes retained-copy claims against eviction (expert review 2026-10-20-2027 #22). Always
      # on (a claim needs it whenever a marker exists, even with retention since turned off). Before
      # the shard supervisor so it outlives every coordinator's terminate-time retain.
      Fathom.Shard.Retained
    ] ++
      temp_reaper_children() ++
      heartbeat_children() ++
      [
        # Shard processes: a Registry for find-by-id and a DynamicSupervisor that owns one
        # Fathom.Shard process per active shard. The supervisor's restart budget is sized to
        # the shard fan-out (see shard_supervisor_opts/0), not the OTP default.
        #
        # The heartbeat MUST start before this pair: supervisors stop children in reverse
        # start order, so on a clean shutdown the heartbeat (and its stored liveness object)
        # outlives every coordinator's terminate-flush. Listed after the supervisor, the
        # heartbeat object was deleted first, every open shard on the stopping node became
        # stealable mid-deploy, and the fenced terminate-flushes self-fenced and dropped
        # their writes (expert review #4).
        # Partitioned (review 2026-07-23 #11): the default single partition serializes every
        # coordinator registration/unregistration/death-cleanup through one process + one ETS
        # pair — capping coordinator CHURN (mass cold-open after failover, idle-stop waves at
        # 10k+ coordinators, eviction storms at the soft cap) and widening the dead-pid window
        # Shards.retry_checkout? papers over. Lookups are caller-side ETS either way.
        {Registry,
         keys: :unique, name: Fathom.ShardRegistry, partitions: System.schedulers_online()},
        # Short-lived data-plane probes that must NOT run in the caller's process (expert
        # review 2026-08-01 #34). The at-capacity eviction probe is the first user: it waits on
        # a coordinator that may reply after the wait gave up, and running it inline let that
        # stray reply accumulate in the admitting Hrana stream process, which lives for hours.
        # A task's mailbox dies with the task.
        {Task.Supervisor, name: Fathom.TaskSupervisor},
        {DynamicSupervisor, shard_supervisor_opts()}
      ] ++ Fathom.Shard.Replication.Fleet.children()
  end

  # Default DynamicSupervisor restart intensity is 3 restarts / 5 s — sized for a small
  # static tree. ShardSupervisor instead holds one transient coordinator per active shard
  # (thousands, up to the fd/memory fan-out ceiling), so a few unrelated coordinators
  # crashing in the same window — or one poison shard crash-looping — would exceed 3-in-5s
  # and terminate the supervisor, killing every co-resident shard and risking a
  # DataPlane -> top cascade (finding #16). Size the budget to the fan-out instead; both
  # knobs are config-tunable per deployment.
  @default_shard_supervisor_max_restarts 100
  @default_shard_supervisor_max_seconds 10

  # `max_children` (the admission cap aligned with :max_open_shards) is deliberately not set
  # here — that is the gated unbounded-shard-creation decision (finding #14).
  @doc false
  def shard_supervisor_opts do
    [
      name: Fathom.ShardSupervisor,
      strategy: :one_for_one,
      max_restarts:
        Application.get_env(
          :fathom,
          :shard_supervisor_max_restarts,
          @default_shard_supervisor_max_restarts
        ),
      max_seconds:
        Application.get_env(
          :fathom,
          :shard_supervisor_max_seconds,
          @default_shard_supervisor_max_seconds
        )
    ]
  end

  # Edge (network-facing). Starts last: Fathom.Telemetry polls the shard Registry, and
  # the Hrana listener needs the stream registry + the data plane ready.
  defp edge_children do
    [
      # Cluster-phase observability: metrics over the shard/lease/checkout telemetry,
      # the active-shard poller, and the checkout -> OpenTelemetry span bridge. Starts the
      # in-process Prometheus reporter the metrics collector below scrapes, so it comes first.
      Fathom.Telemetry
    ] ++
      metrics_collector_children() ++
      [
        # Hrana (libSQL) protocol server: a stream registry plus a dedicated listener that
        # serves shards to libSQL clients, on its own port, separate from the dashboard.
        {Filo.Streams, name: Fathom.HranaStreams}
      ] ++ hrana_listeners() ++ health_listeners() ++ [FathomWeb.Endpoint]
  end

  # The admin dashboard's realtime metrics collector: reads this node's live metrics each tick and
  # fans them out over PubSub. Gated with the rest of the admin observability layer
  # (Fathom.Admin.enabled?, off in test). After Fathom.Telemetry so the Prometheus reporter it
  # scrapes is already up.
  #
  # Fathom.Admin.TaskSupervisor is UNCONDITIONAL (idle when unused) — it supervises the collector's
  # slow storage-usage polls (kept off the tick, timed, overlap-guarded — expert review 2026-07-14
  # #22). Always-on so the collector, and tests that start_supervised/1 it directly, can always
  # reach it; the collector itself stays gated.
  defp metrics_collector_children do
    [{Task.Supervisor, name: Fathom.Admin.TaskSupervisor}] ++
      if Fathom.Admin.enabled?(),
        do: [
          Fathom.Admin.EventRing,
          Fathom.Admin.MetricsCollector,
          Fathom.Admin.FleetCollector,
          Fathom.Admin.HotShardsCache,
          Fathom.Admin.Durability
        ],
        else: []
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    FathomWeb.Endpoint.config_change(changed, removed)
    :ok
  end

  # The amortized orphan-temp janitor (expert review 2026-07-14 #2): one directory
  # scan every few minutes for the uniquely-suffixed `.dl/.snap/.tmp` temps a cold
  # open must NOT scan for per-open. Gated `:temp_reaper` (default on; off in test,
  # where it does periodic disk I/O and tests drive Fathom.Shard.TempReaper.sweep/0).
  defp temp_reaper_children do
    if Application.get_env(:fathom, :temp_reaper, true),
      do: [Fathom.Shard.TempReaper],
      else: []
  end

  # The per-node liveness heartbeat (the F1 fix — one PUT/node replaces per-shard
  # lease renewal). Gated like the listeners and off in test (the heartbeat tests
  # start it themselves), since it does periodic storage I/O.
  defp heartbeat_children do
    if Fathom.Shard.Heartbeat.configured?(),
      do: [Fathom.Shard.Heartbeat],
      else: []
  end

  @doc """
  Whether this node serves the Hrana data plane.

  ONE reader for `:hrana_server`, because the default drifted and silently disarmed two prod
  boot guards (expert review 2026-08-01 #6). The listener defaulted the key to `true` while
  `check_shard_base_domain!/0` and `check_hrana_exposure!/0` defaulted it to `false`, and no
  config file sets it outside test — so the most likely prod topology (one node, no
  `LB_BACKENDS`) booted SERVING Hrana while both guards read "not exposed" and passed.

  That is precisely the release blocker the first guard exists for: with `:shard_base_domain`
  unset, `zone_matches?/1` returns true unconditionally and any attacker-controlled `Host`
  first-label selects any shard — and with the shipped `:hrana_auth :disabled` default, no
  credential is needed. Both guards were inert in exactly that configuration.

  `true` is the correct default: it is what the listener does, so the guards now describe the
  node's actual behaviour.
  """
  def hrana_enabled?, do: Application.get_env(:fathom, :hrana_server, true)

  # The Hrana listener binds a real port, so (like the Phoenix endpoint's
  # `server`) it is gated by config and off in test.
  defp hrana_listeners do
    if hrana_enabled?(), do: [hrana_listener()], else: []
  end

  # A Bandit listener running Filo.Plug (Hrana over HTTP + the WebSocket upgrade).
  # The baton signing key is generated per boot, so restarting the node
  # invalidates any in-flight HTTP stream batons (WebSocket streams reconnect).
  defp hrana_listener do
    opts = [
      executor: Fathom.ShardExecutor,
      streams: Fathom.HranaStreams,
      key: Filo.Baton.new_key(),
      open_arg: &Fathom.ShardExecutor.shard_from_conn/1,
      # In-app bearer-token auth (Fathom.HranaAuth). The callback is always wired;
      # whether it checks anything is the runtime :hrana_auth mode (:disabled default,
      # HRANA_AUTH=required in prod), so flipping the mode needs no listener restart.
      authorize: &Fathom.HranaAuth.authorize/2,
      # Expert review 2026-07-24 #22. Filo's default is 10s and fathom passed nothing, so there was
      # no way to change it on a deployed release. This is NOT about hiding the 4096-tenant shed —
      # that is a rig artifact and widening a timeout to mask it would be a regression. It is that
      # 10s of CLIENT THINK TIME should not silently roll back an open transaction: a stream holds
      # live transaction state, and expiring it mid-transaction discards acked work and surfaces as
      # an opaque STREAM_NOT_FOUND. A Django request doing BEGIN; SELECT; <app logic>; UPDATE;
      # COMMIT under load can plausibly exceed 10s between statements.
      #
      # The cost is bounded and explicit: each held stream is one shard checkout plus ~3 fds, and
      # :max_checkouts_per_shard already caps per-tenant exposure. Do NOT set this to :infinity.
      idle_timeout: Application.get_env(:fathom, :hrana_stream_idle_ms, 30_000),
      # A stream is idle-dominant by construction (a django-libsql WebSocket stream lives for hours
      # between requests) while holding the exqlite handle, its statement cache, and a heap grown to
      # the largest result set it ever materialized — a large part of the measured ~220 KiB (empty)
      # / ~640 KiB (with data) per served shard. Reclaim it the same way #9 does for coordinators.
      #
      # max_heap_size is deliberately absent here too: Filo.Executor.close/1 → Shard.checkin runs in
      # terminate/2, which a heap-limit kill skips, leaking a checkout until the coordinator's
      # monitor fires.
      hibernate_after: Application.get_env(:fathom, :hrana_stream_hibernate_ms, 5_000),
      spawn_opt: [fullsweep_after: 10]
    ]

    Supervisor.child_spec(
      {Bandit,
       [
         plug: {Filo.Plug, opts},
         scheme: :http,
         port: hrana_port(),
         ip: hrana_bind_ip(),
         thousand_island_options: hrana_transport_options(),
         # Compression belongs on the LB, not here (expert review 2026-07-24 #16). Bandit defaults
         # `compress: true`, so any client advertising `accept-encoding: gzip` — reqwest (the Rust
         # libSQL SDK), undici/fetch (JS), Python requests/httpx, all by default — made every
         # /v2|v3/pipeline response pay a full open→deflateInit→deflate→deflateEnd→close cycle,
         # init-dominated at kilobyte JSON sizes, for a hop that is usually a datacenter LAN.
         #
         # This cost is absent from every measurement in the repo: Filo.Client sends only
         # `content-type`, so neither chaos driver ever advertises an encoding and the whole
         # tpc-fleet / hotspots corpus measured the UNCOMPRESSED path. Real SDK traffic did not.
         #
         # nginx now does it once, where `gzip_min_length` can skip the small responses Bandit has
         # no knob for. The client still receives `content-encoding: gzip` — wire-transparent.
         http_options: hrana_http_options(),
         http_1_options: hrana_http_1_options(),
         websocket_options: hrana_websocket_options()
       ]},
      id: :fathom_hrana_listener
    )
  end

  @doc false
  # WebSocket listener options (expert review 2026-07-24 #34).
  #
  # `validate_text_frames` defaults to true, and Bandit's own doc says it "can be an expensive
  # operation and one that may be safely skipped in some situations". Every Hrana-over-WS request
  # frame — django-libsql, the PRIMARY production client path — got a full `String.valid?/1` byte
  # walk, immediately followed by `Jason.decode/1` walking the same bytes. One extra full pass per
  # request frame, scaling with statement size (a fat `INSERT … VALUES` batch is tens of KB). This
  # is the inbound mirror of the 2026-07-23 audit's #8, which removed a `String.valid?` pre-scan on
  # the OUTBOUND path for exactly this reason; the inbound one lives in Bandit's config rather than
  # filo's code, so it was never touched.
  #
  # Invalid UTF-8 is still rejected: `Jason.decode/1` cannot parse it and filo closes the
  # connection. The only thing the pre-scan contributed was the close CODE — RFC 6455 §7.4.1 wants
  # 1007 — so this is paired with filo emitting 1007 from its decode-failure path (filo 7796a91+,
  # `Filo.Socket.handle_in/2`). With that pair in place the change is conformance-neutral, not a
  # spec deviation. Do NOT disable this without that filo version.
  def hrana_websocket_options, do: [validate_text_frames: false]

  @doc false
  # HTTP/1-specific listener options (expert review 2026-07-24 #40).
  #
  # `gc_every_n_keepalive_requests` defaults to 5 in Bandit, and Bandit's own docs call the option
  # "currently experimental". That default suits a web app whose connection serves a handful of
  # requests; fathom's HTTP Hrana path is a long-lived LB-pooled connection (nginx `keepalive 512`,
  # `keepalive_requests 100000`) serving thousands of small round-trips, so it forces a full sweep
  # of the connection heap every 5 of them.
  #
  # Left AT Bandit's default and made configurable rather than raised, deliberately. The review's
  # ~5% -of-one-core figure is its own estimate, explicitly not a fathom measurement, and the
  # tradeoff runs the other way on the metric fathom actually sells: fewer forced collections means
  # each connection process holds more garbage between them, multiplied by 30k held connections per
  # node. Raise it only behind an A/B that shows BOTH sides — `chaos.sh tpc-fleet` for the
  # throughput claim and `chaos.sh served` for RSS/shard, which must not move.
  def hrana_http_1_options do
    [gc_every_n_keepalive_requests: Application.get_env(:fathom, :hrana_gc_every_n, 5)]
  end

  # Accept-path sizing for the Hrana listener (expert review 2026-07-24 #6). The listener shipped
  # with NO transport options, so ThousandIsland's defaults applied: all 100 acceptors contending on
  # ONE listen socket with ONE kernel accept queue capped at 1024.
  #
  # That is the structural version of the ListenOverflows the 4096-shed report measured and
  # correctly dismissed as not the primary cause: a 4096-wide connect burst offers ~4096 SYNs to a
  # 1024-slot queue, overflow drops the SYN, the client retransmits after ~1 s, and that feeds
  # straight into the 15 s Filo.Client timeout that IS the shed's cause. Deeper queues do not hide a
  # failure here — they stop discarding connections the node has the CPU and fds to serve (max_fd
  # measured 17–21k against a 65,536 limit).
  #
  # `num_listen_sockets` + `reuseport` is ThousandIsland's documented multi-queue path: N
  # independent listen sockets, each with its own kernel queue, spread by the kernel. Safe here
  # because all of them belong to one BEAM node started together — the classic "SO_REUSEPORT
  # rebalance drops connections across independent processes" hazard needs separate processes.
  #
  # CONFIG-GATED, deliberately: ThousandIsland FAILS STARTUP if `reuseport` is unsupported, so a
  # hardcoded value would be a boot-fail hazard on an exotic kernel. Set :hrana_listen_sockets to 1
  # to fall back to a single socket with no reuseport.
  #
  # Note a backlog above the OS `somaxconn` is silently clamped, so raising net.core.somaxconn is a
  # node-provisioning step, not something this can do for you.
  @doc false
  # Public for the same reason as hrana_transport_options/0: a test boots a real listener with
  # these and asserts an `accept-encoding: gzip` request comes back UNcompressed.
  def hrana_http_options, do: [compress: false]

  @doc false
  # Public only so a test can boot a listener with exactly these options — ThousandIsland fails
  # startup on an unsupported one (notably `reuseport`), which is precisely what needs proving.
  def hrana_transport_options do
    sockets = Application.get_env(:fathom, :hrana_listen_sockets, 4)
    backlog = Application.get_env(:fathom, :hrana_backlog, 4096)

    transport =
      if is_integer(sockets) and sockets > 1,
        do: [backlog: backlog, reuseport: true],
        else: [backlog: backlog]

    [
      num_listen_sockets: max(sockets, 1),
      transport_options: transport,
      # The WebSocket connection process (django-libsql, the PRIMARY prod path) is the
      # ThousandIsland handler itself: Bandit upgrades in place, so `Filo.Socket` runs in the
      # process started with THESE genserver options. The `hibernate_after` / `spawn_opt` in the
      # Filo.Plug opts reach only `Filo.Stream` (HTTP), so before this a WS connection held the
      # heap of the largest result it ever materialized (~951 KiB idle after one 2000-row x 8-col
      # response) for hours between requests (expert review 2026-10-19 #6). Same 5 s policy as the
      # HTTP stream. `fullsweep_after` rides along in `spawn_opt` (GenServer.start_link honours it).
      genserver_options: [
        hibernate_after: Application.get_env(:fathom, :hrana_ws_hibernate_ms, 5_000),
        spawn_opt: [fullsweep_after: 10]
      ]
    ]
  end

  defp hrana_port, do: Application.get_env(:fathom, :hrana_port, 8080)

  # Which interface the Hrana listener binds. The data path carries no in-app credential, so
  # the trust boundary is the network: the port must be reachable only via the L7 LB. Pin this
  # to the private interface in prod (runtime.exs reads HRANA_BIND_IP) as defense-in-depth
  # alongside the firewall/security-group/private-subnet that is the primary control. Defaults
  # to all interfaces to preserve the single-host dev / chaos-rig setup. See docs/deploy-cluster.md.
  @doc false
  def hrana_bind_ip, do: Application.get_env(:fathom, :hrana_bind_ip, {0, 0, 0, 0})

  # The health listener binds a real port (like the Hrana listener and the Phoenix
  # endpoint's `server`), so it is gated by config and off in test.
  defp health_listeners do
    if Application.get_env(:fathom, :health_server, true), do: [health_listener()], else: []
  end

  # A tiny Bandit listener for load-balancer health checks (`GET /health` -> 200). On its
  # own port and plug, separate from Hrana, so it never touches Filo's stream handling.
  # Liveness only (see Fathom.HealthPlug). The LB consistent-hashes the Host subdomain to a
  # node; this is the per-node probe target — see docs/deploy-cluster.md.
  defp health_listener do
    Supervisor.child_spec(
      {Bandit, plug: Fathom.HealthPlug, scheme: :http, port: health_port()},
      id: :fathom_health_listener
    )
  end

  defp health_port, do: Application.get_env(:fathom, :health_port, 8081)
end
