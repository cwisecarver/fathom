defmodule Fathom.Telemetry do
  @moduledoc """
  Cluster-phase observability (S5).

  - Defines `Telemetry.Metrics` over the `:telemetry` events the shard / lease / checkout paths
    emit (cold-open latency, lease churn = the per-node S3 lease RPS, self-fence/split-brain
    signal, active-shard count, checkout outcomes). Wire a reporter (Prometheus / StatsD /
    OTLP) to `metrics/0` to export them — see `docs/runbooks/cluster.md`.
  - Runs a `telemetry_poller` that gauges the active-shard coordinator count on this node.
  - Bridges the `Fathom.Shards.checkout` `:telemetry.span` to an **OpenTelemetry trace span**
    (so a cold checkout shows its cold-open cost in traces). The OTLP exporter is env-gated in
    `config/runtime.exs` — a no-op until an endpoint is set. The bridge attach is gated by
    `:otel_spans` (off in test).

  Metrics stay on `Telemetry.Metrics` because OpenTelemetry's BEAM metrics SDK is still
  experimental; traces use OpenTelemetry.
  """
  use Supervisor

  import Telemetry.Metrics

  @otel_handler "fathom-otel-checkout-span"
  # `get_application_tracer/1` auto-creates the tracer for this app; no registration needed.
  @tracer_id :fathom

  def start_link(arg), do: Supervisor.start_link(__MODULE__, arg, name: __MODULE__)

  @impl true
  def init(_arg) do
    if otel_spans?(), do: attach_otel_span_bridge()

    children =
      [
        {:telemetry_poller,
         measurements: [
           {__MODULE__, :measure_active_shards, []},
           {Fathom.Admin.Measurements, :node_memory, []},
           {Fathom.Admin.Measurements, :durability, []}
         ],
         period: 10_000,
         name: Fathom.ShardPoller}
      ] ++ prometheus_children()

    Supervisor.init(children, strategy: :one_for_one)
  end

  # In-process Prometheus reporter over metrics/0 — the metrics layer the admin dashboard reads
  # (in-process) and any external Prometheus/Grafana scrapes. Gated with the rest of the admin
  # observability layer (Fathom.Admin.enabled?, off in test). Named :fathom_metrics so the
  # collector and the /metrics endpoint target it via TelemetryMetricsPrometheus.Core.scrape/1.
  defp prometheus_children do
    if Fathom.Admin.enabled?() do
      [{TelemetryMetricsPrometheus.Core, metrics: metrics(), name: :fathom_metrics}]
    else
      []
    end
  end

  @doc """
  `Telemetry.Metrics` over the cluster events. Pass this to a reporter
  (`TelemetryMetricsPrometheus`, `TelemetryMetricsStatsd`, a `ConsoleReporter`, ...) to export.
  """
  def metrics do
    [
      distribution("fathom.shard.cold_open.duration",
        event_name: [:fathom, :shard, :cold_open],
        measurement: :duration,
        unit: {:native, :millisecond},
        tags: [:warm],
        reporter_options: [buckets: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000]],
        description: "Shard cold-open latency (tag warm: local file present vs pulled from S3)"
      ),
      counter("fathom.shard.lease.acquired.count",
        description: "Lease acquisitions (steal-on-lapse + first open)"
      ),
      counter("fathom.shard.lease.renewed.count",
        description: "Lease renewals — the per-node S3 lease RPS source (see F1)"
      ),
      counter("fathom.shard.lease.superseded.count",
        description: "Lease supersessions / self-fences — a split-brain / churn signal"
      ),
      counter("fathom.shard.lease.held.count",
        description: "Starts refused against a live foreign lease"
      ),
      counter("fathom.shards.at_capacity.count",
        event_name: [:fathom, :shards, :at_capacity],
        description:
          "New-shard opens refused by per-node admission control (:max_open_shards) — the hot-spot / rebalance signal"
      ),
      counter("fathom.shard.warm.promoted.count",
        event_name: [:fathom, :shard, :warm, :promoted],
        tags: [:result],
        description:
          "Warm-cache promotions at cold-open by result (hit = served warm, stale = re-pulled)"
      ),
      distribution("fathom.shards.checkout.stop.duration",
        event_name: [:fathom, :shards, :checkout, :stop],
        measurement: :duration,
        unit: {:native, :millisecond},
        tags: [:outcome],
        reporter_options: [buckets: [0.5, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000]],
        description: "Shard checkout latency by outcome (ok / held / unavailable / error)"
      ),
      last_value("fathom.shards.active",
        event_name: [:fathom, :shards],
        measurement: :active,
        description: "Active shard coordinators on this node"
      ),
      counter("fathom.audit.event.count",
        event_name: [:fathom, :audit, :event],
        tags: [:action, :outcome],
        description:
          "Control-plane / admin audit events by action + outcome (#9) — delete / restore / export / token ops"
      ),
      summary("fathom.shard.clock_skew.skew_ms",
        event_name: [:fathom, :shard, :clock_skew],
        measurement: :skew_ms,
        unit: :millisecond,
        description:
          "Local clock minus S3's response Date at a steal-liveness check (#13) — watch for skew that would drive wrongful steals"
      ),

      # --- Rebalancer (Phase-2 B1) — operability for the enable runbook -------------------
      # Low-cardinality only: tag by node/outcome, never shard_id (the ShardLoad cardinality
      # rule). shard_id/from_node/to_node ride in event metadata for logs/traces.
      counter("fathom.rebalancer.move.proposed.count",
        event_name: [:fathom, :rebalancer, :move, :proposed],
        description: "Shard moves the policy proposed (handoffs enqueued) — rebalancer activity"
      ),
      counter("fathom.rebalancer.affinity.count",
        event_name: [:fathom, :rebalancer, :affinity],
        tags: [:outcome],
        description:
          "Move target affinity (hit = warm target chosen, miss = cold) — the #C warm-hit rate"
      ),
      counter("fathom.rebalancer.handoff.stop.count",
        event_name: [:fathom, :rebalancer, :handoff, :stop],
        tags: [:outcome],
        description: "Handoff terminal outcome (completed / reverted) — the core health signal"
      ),
      counter("fathom.rebalancer.handoff.retry.count",
        event_name: [:fathom, :rebalancer, :handoff, :retry],
        description:
          "Handoff attempts that retried (flip not live / slow drain) — thrash precursor"
      ),
      counter("fathom.rebalancer.command.stop.count",
        event_name: [:fathom, :rebalancer, :command, :stop],
        tags: [:command, :outcome],
        description:
          "Warm/drain command outcomes (done / failed / cancelled) — drain-failed = thrash"
      ),
      counter("fathom.rebalancer.lb_apply.count",
        event_name: [:fathom, :rebalancer, :lb_apply],
        tags: [:outcome],
        description:
          "LB-map apply outcome (applied / noop / reload_failed / config_test_failed / …) — routing-at-risk"
      ),
      counter("fathom.rebalancer.reconcile.unpinned.count",
        event_name: [:fathom, :rebalancer, :reconcile, :unpinned],
        description: "Pins unpinned because their node went dead (#1b) — dead-node reconcile rate"
      ),
      counter("fathom.rebalancer.reconcile.divergence.count",
        event_name: [:fathom, :rebalancer, :reconcile, :divergence],
        description:
          "A pinned node stopped BEATING but still HOLDS the shard's S3 lease — reporter/data-plane divergence; the pin was kept (fail-safe). Sustained > 0 ⇒ investigate a wedged/off Reporter"
      ),

      # --- Data-path metrics for the admin dashboard (all low-cardinality; per-shard data is the
      # ShardLoad read-API, never a metric tag) ------------------------------------------------
      distribution("fathom.shard.query.duration",
        event_name: [:fathom, :shard, :query],
        measurement: :duration,
        unit: {:native, :millisecond},
        reporter_options: [buckets: [0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000]],
        description: "Per-statement SQLite query latency (node-wide; deliberately un-tagged)"
      ),
      counter("fathom.s3.op.count",
        event_name: [:fathom, :s3, :op],
        tags: [:op],
        description:
          "S3 operations by HTTP method (get/put/head/delete) — cost + rate-limit headroom"
      ),
      sum("fathom.s3.op.bytes",
        event_name: [:fathom, :s3, :op],
        measurement: :bytes,
        tags: [:op],
        unit: :byte,
        description: "Bytes transferred to/from S3 by method"
      ),
      last_value("fathom.node.memory.total",
        event_name: [:fathom, :node, :memory],
        measurement: :total,
        unit: :byte,
        description: "BEAM total memory on this node"
      ),
      last_value("fathom.durability.dirty_shards",
        event_name: [:fathom, :durability, :rpo],
        measurement: :dirty_shards,
        description: "Open shards holding un-flushed writes (live RPO exposure)"
      ),
      last_value("fathom.durability.oldest_age_ms",
        event_name: [:fathom, :durability, :rpo],
        measurement: :oldest_age_ms,
        unit: :millisecond,
        description: "Age of the oldest un-flushed write on this node (live RPO estimate)"
      ),
      last_value("fathom.storage.objects",
        event_name: [:fathom, :storage, :usage],
        measurement: :objects,
        description: "Live shard objects stored (from the storage backend's stored_usage)"
      ),
      last_value("fathom.storage.bytes",
        event_name: [:fathom, :storage, :usage],
        measurement: :bytes,
        unit: :byte,
        description: "Total bytes stored across shard objects"
      ),

      # --- Page-worthy signals that already emit telemetry but weren't exported (review #30) ----
      # Each is referenced by a rule in deploy/observability/alert-rules.yml. Low-cardinality
      # (no shard_id tag — that's the ShardLoad read-API; shard_id rides event metadata for logs).

      # Durability / data-loss precursors.
      counter("fathom.shard.flush.failed.count",
        event_name: [:fathom, :shard, :flush, :failed],
        description:
          "Durability-flush failures (#27) — a persistent rate means flushes aren't landing and the RPO is growing silently (S3 auth / bucket-policy / reachability). The direct flush-failure signal behind the rising oldest-unflushed-age gauge"
      ),
      counter("fathom.shard.corrupt_flush.count",
        event_name: [:fathom, :shard, :corrupt_flush],
        description:
          "Pre-flush PRAGMA quick_check failures — a corrupt local was quarantined, NOT flushed over the good S3 copy (#4). Any occurrence is page-worthy"
      ),
      counter("fathom.shard.fenced_quarantine.count",
        event_name: [:fathom, :shard, :fenced_quarantine],
        description:
          "Self-fences that quarantined un-flushed writes to a .fenced.<ts> file (#5) — acked-but-unflushed data preserved for recovery; sustained > 0 ⇒ ownership churn losing writes"
      ),

      # Capacity / admission — the tenant-visible refusals.
      counter("fathom.shards.novel_rate_limited.count",
        event_name: [:fathom, :shards, :novel_rate_limited],
        description:
          "Novel-shard opens refused by the NovelLimiter (429) — new-tenant minting hit the :novel_shard_rate budget"
      ),
      counter("fathom.shards.evicted.count",
        event_name: [:fathom, :shards, :evicted],
        description:
          "Idle shards LRU-evicted at the soft cap to admit a new open (#14) — node running at :max_open_shards; sustained ⇒ under-provisioned"
      ),

      # Liveness — the mass-self-fence precursor.
      counter("fathom.shard.heartbeat.lapsed.count",
        event_name: [:fathom, :shard, :heartbeat, :lapsed],
        description:
          "A node heartbeat lapsed (renewal didn't land within the TTL margin) — its shards become stealable; sustained ⇒ S3 reachability / mass self-fence risk"
      ),

      # Control plane — a stalled Oban is a silent migration/reconcile/rebalancer/retirement outage.
      counter("fathom.oban.job.exception.count",
        event_name: [:oban, :job, :exception],
        tags: [:queue],
        description:
          "Oban job failures by queue — migrations/reconcile/rebalancer/retirement/tenant-lifecycle progress; sustained ⇒ control-plane stall (often a Postgres incident, see operations.md)"
      )
    ]
  end

  @doc false
  def measure_active_shards do
    :telemetry.execute([:fathom, :shards], %{active: Registry.count(Fathom.ShardRegistry)}, %{})
  end

  # --- OpenTelemetry span bridge: the checkout :telemetry.span -> an OTel trace span ---

  defp otel_spans?, do: Application.get_env(:fathom, :otel_spans, true)

  defp attach_otel_span_bridge do
    # Detach first so a supervisor restart re-attaches cleanly.
    :telemetry.detach(@otel_handler)

    :telemetry.attach_many(
      @otel_handler,
      [
        [:fathom, :shards, :checkout, :start],
        [:fathom, :shards, :checkout, :stop],
        [:fathom, :shards, :checkout, :exception]
      ],
      &__MODULE__.handle_otel_event/4,
      %{}
    )
  end

  @doc false
  def handle_otel_event([:fathom, :shards, :checkout, :start], _measurements, meta, _config) do
    OpentelemetryTelemetry.start_telemetry_span(@tracer_id, "shards.checkout", meta, %{
      kind: :internal
    })
  end

  def handle_otel_event([:fathom, :shards, :checkout, :stop], _measurements, meta, _config) do
    ctx = OpentelemetryTelemetry.set_current_telemetry_span(@tracer_id, meta)

    OpenTelemetry.Span.set_attributes(ctx, %{
      "fathom.shard_id" => meta.shard_id,
      "fathom.checkout.outcome" => to_string(meta.outcome)
    })

    OpentelemetryTelemetry.end_telemetry_span(@tracer_id, meta)
  end

  def handle_otel_event([:fathom, :shards, :checkout, :exception], _measurements, meta, _config) do
    ctx = OpentelemetryTelemetry.set_current_telemetry_span(@tracer_id, meta)
    OpenTelemetry.Span.set_attributes(ctx, %{"fathom.checkout.error" => true})
    OpentelemetryTelemetry.end_telemetry_span(@tracer_id, meta)
  end
end
