defmodule Fathom.Telemetry do
  @moduledoc """
  Cluster-phase observability (S5).

  - Defines `Telemetry.Metrics` over the `:telemetry` events the shard / lease / checkout paths
    emit (cold-open latency, lease churn = the per-node S3 lease RPS, self-fence/split-brain
    signal, active-shard count, checkout outcomes). Wire a reporter (Prometheus / StatsD /
    OTLP) to `metrics/0` to export them — see `docs/runbooks/cluster.md`.
  - Runs a `telemetry_poller` that gauges the active-shard coordinator count on this node.
  - Bridges the `Fathom.Shards.checkout` `:telemetry.span` to an **OpenTelemetry trace span**
    (so a cold checkout shows its cold-open cost in traces). The bridge attach is gated by
    `:otel_spans`, which defaults **off** and is set true by `config/runtime.exs` only when
    `OTEL_EXPORTER_OTLP_ENDPOINT` configures a real exporter — without a collector the
    handlers built full recording spans on every checkout and exported them to nothing
    (expert review 2026-07-23 #3).

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
           {Fathom.Admin.Measurements, :vm_limits, []},
           {Fathom.Admin.Measurements, :durability, []},
           {Fathom.Admin.Measurements, :disk, []},
           {Fathom.Admin.Measurements, :replication, []},
           {Fathom.Admin.Measurements, :flush_gate, []}
         ],
         period: 10_000,
         name: Fathom.ShardPoller}
      ] ++ oban_poller() ++ prometheus_children()

    Supervisor.init(children, strategy: :one_for_one)
  end

  # The Oban control-plane health gauges (expert review #18) are the ONE measurement that queries
  # Postgres, so they run on their own slower poller and only when the observability layer is on
  # (Fathom.Admin.enabled?, off in test) — the fast 10s shard poller stays Postgres-free, and no
  # poller touches the DB in test / when metrics are off. Every node polls the shared oban_jobs
  # table (redundant but robust): if cron leadership wedges, live nodes still surface the growing
  # queue depth + cron staleness.
  defp oban_poller do
    if Fathom.Admin.enabled?() do
      [
        {:telemetry_poller,
         measurements: [{Fathom.Admin.Measurements, :oban_health, []}],
         period: 30_000,
         name: Fathom.ObanPoller}
      ]
    else
      []
    end
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
      # Rare and consequential: a cold open served a local replica INSTEAD of the stored object,
      # because the replica was provably ahead of what the object claimed. That is the A2 RPO win
      # actually happening, and it is also the moment a lineage was overwritten — so it wants to be
      # visible per shard in the log line and countable on the dashboard. Zero on a healthy fleet;
      # a rising rate means failovers, not a problem with this path.
      # A coordinator stopped while streams were still checked out, so the local copy was KEPT
      # rather than unlinked (expert review 2026-08-20 #9). Not an error: it is the shutdown path
      # declining to delete a file its own live streams are still writing through. Worth counting
      # because a RISING rate means coordinators are being stopped under load — a restart storm, a
      # rolling deploy whose Edge plane is not draining first, or force-stops from tenant deletes —
      # and because each one leaves a local file behind for the next warm open to adopt.
      #
      # NO shard_id tag: at a million tenants that label is cardinality death. It rides the event
      # metadata for the log line instead.
      counter("fathom.shard.drop_deferred.count",
        event_name: [:fathom, :shard, :drop_deferred],
        description:
          "Coordinator shutdowns that kept the local shard copy because connections were still " <>
            "checked out, instead of unlinking it under the streams still using it"
      ),
      counter("fathom.shard.replica_promoted.count",
        event_name: [:fathom, :shard, :replica_promoted],
        description:
          "Cold opens that promoted a newer local replica over the stored object (A2). Each one " <>
            "recovered writes the last flush did not have, and snapshotted what it replaced"
      ),
      # NO `source` TAG, though the event carries one. A node_key is bounded by fleet size today
      # and would be a legitimate label — but the number an operator acts on is "did any shard have
      # to reach across the fleet to recover", and splitting it by source only makes the alert
      # threshold depend on which node died. The source rides the event metadata and the log line.
      counter("fathom.replication.recovered_from_peer.count",
        event_name: [:fathom, :replication, :recovered_from_peer],
        description:
          "Cold opens that pulled a fresher replica from a PEER because this node held none (A2 " <>
            "survivor selection). Each one is a failover where the LB picked a node without a " <>
            "replica and the write tail was recovered anyway — the RPO claim doing its job"
      ),
      # The counterpart to the above, and the one that is otherwise invisible: a recovery that got
      # all the way through the transfer and was then abandoned because the stored object had moved.
      # `recovered_from_peer` fires on the PULL, which did happen, so a raced promotion looks like a
      # success there. Tagged by `reason` (a bounded set of atoms — `:object_moved`,
      # `:object_advanced`, `:object_head_unreadable`) and never by shard_id.
      #
      # A steady trickle is healthy contention. A sustained rate means whole databases are crossing
      # the network and being discarded, which is bandwidth spent to recover nothing.
      counter("fathom.replication.promotion_raced.count",
        event_name: [:fathom, :replication, :promotion_raced],
        tags: [:reason],
        description:
          "Replica promotions abandoned after the pull because the stored object changed while " <>
            "we were recovering — a whole-database transfer that recovered nothing"
      ),
      counter("fathom.shards.at_capacity.count",
        event_name: [:fathom, :shards, :at_capacity],
        description:
          "New-shard opens refused by per-node admission control (:max_open_shards) — the hot-spot / rebalance signal"
      ),
      counter("fathom.shards.handoff_wait.count",
        event_name: [:fathom, :shards, :handoff_wait],
        description:
          "Checkouts that queued (held + retried) at a pinned handoff target during the source's drain window instead of erroring (#20) — a per-handoff burst is expected; a sustained rate means handoffs are draining slowly"
      ),
      counter("fathom.shards.crash_wait.count",
        event_name: [:fathom, :shards, :crash_wait],
        description:
          "Checkouts that queued (held + retried) at the TAIL of a crashed owner's lease-TTL window instead of erroring (#21) — a burst per hard node crash is expected. The RTO floor is :shard_lease_ttl_ms + steal_margin; this only converts the last :crash_failover_hold_ms of it to latency"
      ),
      summary("fathom.shards.held_retry.wait_ms",
        event_name: [:fathom, :shards, :held_retry],
        measurement: :wait_ms,
        unit: :millisecond,
        tags: [:aimed],
        description:
          "How long a held checkout slept before retrying, split by whether the wait was AIMED at a known steal instant (#23) or a blind backoff step. aimed=false dominating a crash failover means the backend stopped supplying the instant and the takeover is back to polling — ~8 retries and ~17 S3 requests per shard instead of one or two"
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
      counter("fathom.rebalancer.command.orphaned.count",
        event_name: [:fathom, :rebalancer, :command, :orphaned],
        tags: [:reason],
        description:
          "In-flight command batches recovered after their task died to an exit signal without completing (#19) — the CommandPoller monitor freeing leaked ids; any occurrence means a TaskSupervisor blip mid-handoff"
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
      # Approach-to-limit, not just current usage: both tables are hard cliffs (+P exhaustion makes
      # spawn throw system_limit ⇒ refused checkouts; +Q exhaustion makes accept return
      # system_limit ⇒ the listener stops accepting), and production materializes 3–4 processes and
      # 1 port per served shard. Alert on the ratios (expert review 2026-07-24 #2).
      last_value("fathom.node.processes",
        event_name: [:fathom, :node, :vm_limits],
        measurement: :processes,
        description: "Live BEAM processes on this node"
      ),
      last_value("fathom.node.process_used_ratio",
        event_name: [:fathom, :node, :vm_limits],
        measurement: :process_used_ratio,
        description: "Live processes as a fraction of +P (1.0 ⇒ spawn fails with system_limit)"
      ),
      last_value("fathom.node.ports",
        event_name: [:fathom, :node, :vm_limits],
        measurement: :ports,
        description: "Open BEAM ports (sockets/fds) on this node"
      ),
      last_value("fathom.node.port_used_ratio",
        event_name: [:fathom, :node, :vm_limits],
        measurement: :port_used_ratio,
        description: "Open ports as a fraction of +Q (1.0 ⇒ the listener stops accepting)"
      ),
      sum("fathom.warm_follower.disk_pressure.declined",
        event_name: [:fathom, :warm_follower, :disk_pressure],
        measurement: :declined,
        description:
          "Shards the warm follower REFUSED to warm because the volume is below " <>
            ":warm_disk_free_floor_bytes. Failover readiness is degrading and disk is the cause"
      ),
      last_value("fathom.warm_follower.disk_pressure.held",
        event_name: [:fathom, :warm_follower, :disk_pressure],
        measurement: :held,
        description: "Shards still cached while under disk pressure (retained, not evicted)"
      ),
      # The flush gate had no observability at all (expert review 2026-08-20 #15). A leaked slot is
      # permanent without the sweep, the cap is single digits, and a node whose gate is full simply
      # stops flushing — quietly, because the failure counter only moves for flushes that RAN.
      # `in_flight` at or above `cap` sustained is the alertable condition.
      last_value("fathom.shard.flush_gate.in_flight",
        event_name: [:fathom, :shard, :flush_gate],
        measurement: :in_flight,
        description: "Durability flushes currently in flight on this node"
      ),
      last_value("fathom.shard.flush_gate.cap",
        event_name: [:fathom, :shard, :flush_gate],
        measurement: :cap,
        description: "Node-wide concurrent-flush cap; in_flight pinned here means nothing flushes"
      ),
      last_value("fathom.shard.flush_gate.refusals",
        event_name: [:fathom, :shard, :flush_gate],
        measurement: :refusals,
        description:
          "Monotonic count of shards turned away by the flush gate (#16). in_flight at cap says the gate is BUSY; this says shards were REFUSED and how often. A rising rate against a flat in_flight is the node-wide flush backlog — every refused shard stays dirty and re-probes, so this is also the RPO-at-risk signal that fathom.shard.flush.failed cannot give (that one only fires for a flush that actually ran)"
      ),
      # A follower refused to seed a shard because its replica volume is below the free-space
      # floor (expert review 2026-08-20 #23). Not an error: that shard's RPO stays at its stored
      # object, which is the pre-A2 behaviour. But a RISING rate means the replica store — which
      # grows from other nodes' traffic and has no retention — is squeezing the disk this node's
      # own durability flushes depend on, and that ends in acked writes that can never be made
      # durable. The counterpart of [:fathom, :warm_follower, :disk_pressure].
      # A lock file that existed but decoded to nothing, recreated at acquire (expert review
      # 2026-08-20 #31). Not routine: an undecodable lock means a full or failing disk, and it
      # used to take that tenant permanently offline because `:corrupt_lock` had two producers and
      # no consumer anywhere. ANY occurrence is worth an operator look, so this is a counter
      # rather than a gauge — the shard is in the log line beside it.
      counter("fathom.storage.lock_repaired.count",
        event_name: [:fathom, :storage, :lock_repaired],
        description: "Undecodable lock files recreated at acquire (investigate the volume)"
      ),
      counter("fathom.replication.disk_pressure.count",
        event_name: [:fathom, :replication, :disk_pressure],
        description:
          "Replica seeds refused because the replication volume is below its free-space floor"
      ),
      counter("fathom.shard.flush_gate.reclaimed.count",
        event_name: [:fathom, :shard, :flush_gate, :reclaimed],
        description:
          "Flush-gate slots reclaimed from dead holders. Non-zero means coordinators are being " <>
            "killed mid-flush; left unreclaimed these accumulate until no shard can flush"
      ),
      last_value("fathom.node.disk.free_bytes",
        event_name: [:fathom, :node, :disk],
        measurement: :free_bytes,
        tags: [:dir],
        description:
          "Free bytes on the volume holding this directory (dir=data|warm). A full volume fails " <>
            "every cold-open pull AND every VACUUM INTO, so writes stay acked and never durable"
      ),
      last_value("fathom.node.disk.used_ratio",
        event_name: [:fathom, :node, :disk],
        measurement: :used_ratio,
        tags: [:dir],
        description: "Used fraction of the volume holding this directory (1.0 = full)"
      ),
      last_value("fathom.replication.followers.connected",
        event_name: [:fathom, :replication, :followers],
        measurement: :connected,
        description: "Followers whose replication socket is up right now (A2)"
      ),
      last_value("fathom.replication.followers.configured",
        event_name: [:fathom, :replication, :followers],
        measurement: :configured,
        description: "Followers in :replication_followers (A2)"
      ),
      # The one to alert on. A commit needs `quorum` acks, so slack=0 means every write still
      # succeeds and the next follower loss fails all of them — a state with no other symptom:
      # latency is normal, no error rate moves, and the shard reports healthy until it doesn't.
      last_value("fathom.replication.followers.slack",
        event_name: [:fathom, :replication, :followers],
        measurement: :slack,
        description:
          "Connected followers minus the write quorum. 0 = every write succeeds and one more " <>
            "follower loss fails all of them; negative = writes are already failing FILO_NO_QUORUM"
      ),
      # Membership swaps (A2 Layer 3). Tagged by `source` and `reason`, both bounded sets — never
      # by node_key, which grows with the fleet.
      counter("fathom.replication.membership_changed.count",
        event_name: [:fathom, :replication, :membership_changed],
        measurement: :size,
        tags: [:source],
        description: "Follower-set swaps applied (A2 membership)"
      ),
      last_value("fathom.replication.membership_changed.size",
        event_name: [:fathom, :replication, :membership_changed],
        measurement: :size,
        tags: [:source],
        description: "Followers in the set after the most recent swap (A2 membership)"
      ),
      # THE ONE TO ALERT ON, and it is quiet by construction. A refusal means a computed set was
      # below quorum+1 and the PREVIOUS set is still live — so writes keep succeeding and nothing
      # else moves, while the node is pinned to a membership the fleet has moved on from. Left
      # alone it ends as a set of followers that no longer exist.
      # SATURATION, leading then lagging. Replication does not degrade gracefully as tenant count
      # rises — 512 -> 1024 -> 2048 measured 3,340 -> 2,776 -> 258 txn/s, a 10.8x collapse for the
      # last doubling — and throughput looks healthy right up to the cliff, so it cannot be the
      # headroom signal (`docs/reviews/a2-flush-interval-2026-08-18.md`).
      #
      # `used_ratio` is the LEADING one: how full the per-node byte budget is now, which climbs
      # before anything is refused. `reject.count` by reason is the LAGGING confirmation — by the
      # time `:overloaded` appears the node is already refusing tenant writes. Alert on the ratio,
      # diagnose with the reasons.
      last_value("fathom.replication.budget.used_ratio",
        event_name: [:fathom, :replication, :budget],
        measurement: :used_ratio,
        description:
          "Per-node replication byte budget in use, 0.0-1.0 (A2). THE headroom signal: " <>
            "throughput gives no warning before the saturation cliff, this does."
      ),
      last_value("fathom.replication.budget.used_bytes",
        event_name: [:fathom, :replication, :budget],
        measurement: :used_bytes,
        unit: :byte,
        description: "Replication payload bytes queued across this node's shippers (A2)"
      ),
      last_value("fathom.replication.budget.max_bytes",
        event_name: [:fathom, :replication, :budget],
        measurement: :max_bytes,
        unit: :byte,
        description: "The per-node replication byte budget, 0 when the bound is disabled (A2)"
      ),
      # Tagged by REASON only, never by shard — a per-shard tag at fathom's scale is cardinality
      # death, the same reason `Fathom.ShardLoad` is a read API rather than a metric.
      counter("fathom.replication.reject.count",
        event_name: [:fathom, :replication, :reject],
        tags: [:reason],
        description:
          "Pushes this node's own shipper refused, by reason (A2). `overloaded` = the byte " <>
            "budget; `already_in_flight` = a straggler still holds that shard's waiter, which is " <>
            "routine and absorbed by the quorum."
      ),
      counter("fathom.replication.membership_refused.count",
        event_name: [:fathom, :replication, :membership_refused],
        measurement: :kept,
        tags: [:source, :reason],
        description:
          "Follower-set swaps REFUSED for being below quorum+1. Sustained non-zero means " <>
            "membership is stuck on a stale set while writes still look healthy"
      ),
      last_value("fathom.durability.dirty_shards",
        event_name: [:fathom, :durability, :rpo],
        measurement: :dirty_shards,
        description: "Open shards holding un-flushed writes (live RPO exposure)"
      ),
      # #17: `watermark_rows` under `open_shards` means the RPO answer above is UNDER-reporting
      # (the watermark table lost rows), rather than the fleet being clean. Without these two an
      # emptied table looked exactly like a fully-flushed fleet.
      last_value("fathom.durability.watermark_rows",
        event_name: [:fathom, :durability, :rpo],
        measurement: :watermark_rows,
        description: "Watermark rows the RPO gauge could see (compare to open_shards)"
      ),
      last_value("fathom.durability.open_shards",
        event_name: [:fathom, :durability, :rpo],
        measurement: :open_shards,
        description: "Open coordinators on this node, for the watermark completeness check"
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
      # #5: the drain window each coordinator was given by drain_all/1. A distribution whose
      # minimum is 0 means shards behind a busy one were handed no window at all — hard-cutting
      # their streams instead of letting them finish, which is what the node drain exists to avoid.
      last_value("fathom.shards.drain_all.slice.window_ms",
        event_name: [:fathom, :shards, :drain_all, :slice],
        measurement: :window_ms,
        description:
          "Drain window (ms) handed to one coordinator by drain_all/1 (#5) — a value of 0 means the budget was already spent by shards ahead of it, so its streams are hard-cut rather than drained"
      ),
      # #3: which route a drop-flush took. `:checkpoint` uploads the LIVE database and is only
      # safe with nothing checked out; `:snapshot` goes through VACUUM INTO, which is quiescent by
      # construction. A rising `:checkpoint` rate with busy shards would mean the routing guard
      # regressed. Tagged by route only — a bounded two-value set; shard_id rides metadata.
      counter("fathom.shard.drop_flush.route.count",
        event_name: [:fathom, :shard, :drop_flush, :route],
        tags: [:route],
        description:
          "Drop-flush route taken (#3) — :snapshot when connections are still checked out (VACUUM INTO, quiescent), :checkpoint when the shard is idle (uploads the live file in place). The live-file path is unsafe with live writers because the change guard between the Content-MD5 read and the body read compares second-resolution mtime"
      ),
      counter("fathom.shard.flush.too_large.count",
        event_name: [:fathom, :shard, :flush, :too_large],
        description:
          "Flushes refused because the shard object exceeds the 5 GiB single-PUT ceiling (#37) — UNLIKE every other flush failure this is PERMANENT: retries fail identically forever while the shard keeps acking writes it can never make durable, and snapshot/fork/retain are disabled for it. Any occurrence is page-worthy and needs shard reduction, not waiting. :shard_max_page_count is the write-time brake that prevents it"
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
      last_value("fathom.shard.quarantines",
        event_name: [:fathom, :shard, :quarantines],
        measurement: :count,
        description:
          "Standing count of quarantine files (.db.fenced/.forked/.corrupt) on this node's data dir (#23), polled by the TempReaper — a growing backlog means preserved acked-but-unflushed writes are piling up unrecovered; enumerate with `mix fathom.shard quarantines`"
      ),
      counter("fathom.restore_drill.result.count",
        event_name: [:fathom, :restore_drill, :result],
        tags: [:status],
        description:
          "Automated restore-drill outcomes by status (#24): ok / corrupt / schema_mismatch / sentinel / absent / error. A durable object is fathom's only copy of a cold-tail tenant — corrupt/schema_mismatch/error means it's bad or unreachable and would surface only when the tenant returns; alert on those"
      ),

      # Birth failure — the tenant that exists but has NO SCHEMA.
      #
      # `Fathom.Shards.fork_novel/1` births a novel tenant by forking `template@HEAD`, and falls
      # back to born-empty on ANY fork failure because a checkout must never fail over an object-
      # store blip. A tenant that takes that fallback is BROKEN and cannot self-heal: its first ORM
      # query fails, and the rollout can't rescue it either — `django_migrations` is created by
      # Django's recorder in autocommit, so it belongs to no captured version and replaying v1 onto
      # an empty file dies on `no such table: django_migrations`. That is a deliberate constraint,
      # not a bug (a born-empty tenant is a FAILED BIRTH, and teaching replay to paper over it would
      # make an already-broken tenant look recoverable) — which is exactly why the state has to be
      # visible instead of healed. Only remedy is an operator deleting and re-minting it.
      #
      # 2026-07-31 made the fallback log loudly and emit this event; nothing ever consumed it, so
      # "alertable" stopped one step short of an actual alert. `:reason` is a deliberately BOUNDED
      # atom set (`Shards.fork_failure_reason/1`) — the shard_id rides event metadata for the log
      # line and must never become a tag.
      # Two opens of the same NOVEL id raced and one deferred to the other (expert review
      # 2026-08-20 #35). Deliberately NOT `fork_fallback`, which pages on any occurrence because a
      # born-empty tenant is a silent hard outage — here the winner's fork SUCCEEDED and the tenant
      # has its schema, so routing an ordinary signup race there told an operator to delete a
      # healthy tenant. Worth counting anyway: the loser has already spent a `NovelLimiter` token,
      # so a rising rate means one novel id is repeatedly consuming two.
      counter("fathom.migrator.fork_retry.count",
        event_name: [:fathom, :migrator, :fork_retry],
        description: "Novel-shard opens that deferred their fork to a concurrent forker"
      ),
      counter("fathom.migrator.fork_fallback.count",
        event_name: [:fathom, :migrator, :fork_fallback],
        tags: [:reason],
        description:
          "Novel tenants born EMPTY because fork-from-template failed while :fork_from_template was ON — the tenant is serving with NO schema, every ORM query fails, and the rollout CANNOT heal it. Any occurrence needs an operator to delete and re-mint that tenant; :no_template_snapshot means the `mix fathom.snapshot template-head` prerequisite never ran"
      ),

      # --- The rest of the emitted-but-unexported sweep (2026-08-06) --------------------------
      # `fork_fallback` above was not a one-off. A diff of every `:telemetry.execute` in lib/
      # against this list found 13 events that emitted into the void — no metric, no Prometheus
      # series, no alert, nothing on the dashboard. Several were written with an explicit
      # intention of being alerted on (`check_template_drift/0`'s own docstring says "so a
      # post-revert wedge is alertable"), which is the failure mode: emitting is the cheap half,
      # and nothing fails when the other half is missing.
      #
      # Same cardinality rule throughout: tag by bounded sets (outcome/kind/dry_run) and NEVER by
      # shard_id — it rides event metadata for the log line. That is also why several of these
      # carry no tags at all despite metadata being available.

      # Data divergence — acked writes that are no longer on the lineage. Both need a human.
      counter("fathom.shard.forked.count",
        event_name: [:fathom, :shard, :forked],
        description:
          "Local shard copies found FORKED from the stored lineage and quarantined to a .forked.<ts> file (#2) — a peer advanced the object while this node held divergent writes. The forked writes survive in that file but are NOT in the tenant's database; recovery is manual. Any occurrence is page-worthy"
      ),
      counter("fathom.shard.write_fenced.count",
        event_name: [:fathom, :shard, :write_fenced],
        description:
          "Shards whose WRITES were refused because the node's heartbeat went stale past the steal margin (:fence_writes_when_stealable, ON in prod). Deliberate — it collapses the loss window from the whole partition to ~ttl+margin — but it is a tenant-visible WRITE OUTAGE while it lasts, and reads keep succeeding so nothing else looks wrong"
      ),
      counter("fathom.shard.write_unfenced.count",
        event_name: [:fathom, :shard, :write_unfenced],
        description:
          "Write fences LIFTED — a coordinator reconfirmed ownership after being provably stealable (#13a). The closing half of write_fenced: a sustained gap between the two counts is a partition that opened breakers and has not healed"
      ),

      # Migration wedge — the template diverged from the fleet and no rollout can proceed.
      counter("fathom.migrator.template_drift.count",
        event_name: [:fathom, :migrator, :template_drift],
        description:
          "Template/fleet migration drift detected at a revert — the template still carries a version the fleet pointer-flipped away from, so the next `makemigrations` builds on DDL the fleet does not have. Untagged on purpose: the metadata carries version NUMBERS, which are unbounded as labels"
      ),

      # Token revocation — degrades to a STALE floor, so a revoked token may still be accepted.
      counter("fathom.hrana.revocation.floor_error.count",
        event_name: [:fathom, :hrana, :revocation, :floor_error],
        description:
          "Revocation-floor reads that failed and fell back to the last-known-good (STALE) floor — a revoke issued after that value was cached is not being enforced on this node. Serving stale beats failing closed on every request, but sustained > 0 means the revocation contract is degraded"
      ),
      counter("fathom.hrana.revocation.bulk.count",
        event_name: [:fathom, :hrana, :revocation, :bulk],
        measurement: :revoked,
        tags: [:outcome],
        description:
          "Fleet-wide revocation refreshes by outcome (#37). `degraded` means the refresh raised and the node is running on whatever floors it already had — pair with floor_error"
      ),
      sum("fathom.hrana.revocation.bulk.revoked",
        event_name: [:fathom, :hrana, :revocation, :bulk],
        measurement: :revoked,
        tags: [:outcome],
        description: "Token floors moved by bulk revocation refreshes (#37)"
      ),

      # Snapshots — the job that DELETES backups. `dropped` is the destructive number.
      sum("fathom.snapshots.retention.dropped",
        event_name: [:fathom, :snapshots, :retention],
        measurement: :dropped,
        tags: [:dry_run],
        description:
          "Snapshots DELETED by the GFS retention sweep (#18), tagged by dry_run so a rehearsal is never mistaken for a real sweep. This is the one automatic path that destroys a backup — it only ever touches `auto`-labelled snapshots, and an unexplained jump means the policy or the clock moved"
      ),
      sum("fathom.snapshots.retention.errors",
        event_name: [:fathom, :snapshots, :retention],
        measurement: :errors,
        tags: [:dry_run],
        description:
          "Per-shard failures during the retention sweep (#18) — the sweep continues past them, so without this the only signal is a Logger.warning"
      ),
      sum("fathom.snapshots.retention.kept",
        event_name: [:fathom, :snapshots, :retention],
        measurement: :kept,
        tags: [:dry_run],
        description: "Snapshots retained by the GFS policy (#18) — the denominator for `dropped`"
      ),

      # Control plane / directory — buffered writes that did not land.
      sum("fathom.directory.flush_retry.count",
        event_name: [:fathom, :directory, :flush_retry],
        description:
          "Directory rows RE-BUFFERED after a failed batch flush — the recorder is deliberately off the hot path and never fails a checkout, so a Postgres problem shows up only here. Summed rather than counted: one event carries a whole batch"
      ),

      # Security — the control-plane refusals that aren't auth failures.
      counter("fathom.api.csrf_blocked.count",
        event_name: [:fathom, :api, :csrf_blocked],
        description:
          "Control-plane (/api) requests refused as cross-site (#27) — browsers re-send cached admin BasicAuth on cross-site submissions, so this is the signal an operator with /admin open visited a hostile page"
      ),

      # Tenant lifecycle — irreversible, and cross-node erase relies on a self-fence.
      counter("fathom.tenants.deleted.count",
        event_name: [:fathom, :tenants, :deleted],
        description:
          "Tenant deletions completed (#15) — irreversible erase of every stored object. Also audited to Postgres; this is the RATE view, where a runaway script shows up as a spike"
      ),
      counter("fathom.tenants.purge_while_held.count",
        event_name: [:fathom, :tenants, :purge_while_held],
        description:
          "Purges that deleted a live object while a REMOTE node still held the lease — correct by design (that node self-fences on its next fenced flush rather than resurrecting the object), but it makes a cross-node erase observable instead of silent. Untagged: the owner is another node's key, and the shard_id stays in the log"
      ),

      # Warm standby — the failover-readiness cache's actual throughput (A1).
      counter("fathom.shard.warm.pulled.count",
        event_name: [:fathom, :shard, :warm, :pulled],
        description:
          "Shards pre-pulled into the warm-follower cache — the failover-readiness fill rate. `warm.promoted` already showed the PAYOFF at failover; this is what pays for it"
      ),
      sum("fathom.shard.warm.pulled.bytes",
        event_name: [:fathom, :shard, :warm, :pulled],
        measurement: :bytes,
        description:
          "Bytes pulled into the warm-follower cache — the disk and bandwidth the standby is spending, and the input to :warm_cache_max_bytes / the disk-pressure floor (#36)"
      ),
      counter("fathom.shard.warm.evicted.count",
        event_name: [:fathom, :shard, :warm, :evicted],
        description:
          "Shards LRU-evicted from the warm-follower cache — sustained eviction against a steady fleet means :warm_cache_max is too small and failover readiness is churning rather than accumulating"
      ),

      # Rebalancer — a fail-safe deferral, so silence here can mean 'working' or 'wedged'.
      counter("fathom.rebalancer.reconcile.skipped.count",
        event_name: [:fathom, :rebalancer, :reconcile, :skipped],
        description:
          "Stale pins the reconcile sweep declined to unpin because a handoff is still in its grace window — fail-safe and expected during a move, but a sustained rate means pins are never being reclaimed. Logged at :info only, so this was the sole way to see it"
      ),

      # Rollout / migration engine — progress, and the three ways it wedges.
      counter("fathom.migrator.shard_migrated.count",
        event_name: [:fathom, :migrator, :shard_migrated],
        description:
          "Shards that MOVED to a new version (once per shard that migrated, not once per attempt — the contract `chaos.sh rollout` asserts). Integrated, this is the rollout burndown; differentiated, it is the shards/s the deploy gate's ETA is computed from"
      ),
      counter("fathom.migrator.migration_stalled.count",
        event_name: [:fathom, :migrator, :migration_stalled],
        measurement: :attempt,
        description:
          "Per-shard migrations still snoozing past :migration_stall_after_ms. Retrying forever is correct (busy and lease-held both clear on their own), so this is the visibility half of the 2026-08-04 fix for a job that sat in `scheduled` at attempt 122/127 with an EMPTY errors array, failed: 0, no quarantine, and nothing above [info]. Untagged: the metadata `reason` is a passed-through error term, not a bounded set"
      ),
      counter("fathom.migrator.unbuildable_chain.count",
        event_name: [:fathom, :migrator, :unbuildable_chain],
        description:
          "Shards whose migration was CANCELLED because a version the chain needs is unknown or yanked. Cancelling is correct — the version will never exist again, and quarantining a healthy untouched shard would hide it from `laggards/2`, `shards_at_version/1` and a later fleet revert — but the shard is then permanently non-converging with nothing above [info] saying so, which is what this exists to surface. Non-zero after a `Migrator.yank/1` of a MIDDLE version means the release graph has a hole every shard below it must walk through, and the fix is a new release bridging it, not a retry. Untagged: `target` and `missing` are version numbers, unbounded as labels"
      ),
      counter("fathom.migrator.revert_no_retained_version.count",
        event_name: [:fathom, :migrator, :revert_no_retained_version],
        description:
          "Shards a fleet revert could not reach at all. The chain-jump case that used to dominate this is now HANDLED (2026-08-26): `shards.retained_version` records what the retain actually wrote, and a value below the target is restored and then migrated forward — see fathom.migrator.revert_climb_back. So non-zero here now means the column is NULL (a pre-column row, or RetirementJob dropped the copy past its retention window) or it named a version storage does not have. Still a PARTIAL revert: these shards are on the bad schema while `revert_status/1` reports remaining: 0, because it counts only active rows. Recovery is manual — restore from a snapshot, or roll forward with a fixed release. Untagged: to_version is a version number, unbounded as a label"
      ),
      counter("fathom.migrator.revert_climb_back.count",
        event_name: [:fathom, :migrator, :revert_climb_back],
        description:
          "Shards a fleet revert had to land BELOW its target and then migrate forward to reach it (2026-08-26). A cold-tail shard chain-migrates several versions in one job and retains only the version it came FROM, so a fleet-wide revert target it skipped has no retained copy; rather than leaving it on the bad schema, the revert restores what it does have and enqueues the climb back. Expected to be non-zero during any revert of a release that sat long enough for cold tenants to skip versions. WATCH IT ALONGSIDE `laggards`: each of these shards is briefly BELOW the fleet version, which the app's documented vN-1/vN tolerance does not cover, so a count that stays non-zero after the migration queue drains means the climb is not completing and those tenants are on a schema the app does not expect. Untagged: landed_at and target are version numbers, unbounded as labels"
      ),
      counter("fathom.migrator.inline_migrate_failed.count",
        event_name: [:fathom, :migrator, :inline_migrate_failed],
        description:
          "Inline migrate-on-touch failures — the shard is served at its OLD version and handed to the async rollout rather than failing the checkout. Sustained ⇒ the shard's schema is ahead of its fathom stamp (a direct `manage.py migrate` against a tenant does that) and the rollout will never converge it"
      ),
      counter("fathom.migrator.revert.count",
        event_name: [:fathom, :migrator, :revert],
        description:
          "Per-shard reverts to a previous version — a fleet pointer-flip backwards. Expected in a burst during a deliberate revert; unexpected at any other time. Untagged: from/to are version NUMBERS"
      ),

      # Capture — the template/fleet contract, and the two ways it silently diverges.
      counter("fathom.migrator.migration_gap.count",
        event_name: [:fathom, :migrator, :migration_gap],
        description:
          "Captures whose template `django_migrations` count jumped — migrations ran OUTSIDE capture (the `atomic = False` idiom runs autocommit and is invisible to it), so the fleet is missing DDL that this version and everything above it assume. `mix fathom.check_migrations` is the pre-flight gate; this is the late backstop firing"
      ),
      sum("fathom.migrator.migration_gap.gap",
        event_name: [:fathom, :migrator, :migration_gap],
        measurement: :gap,
        description: "How many template migrations capture never saw — the size of the divergence"
      ),
      counter("fathom.migrator.backwards_migrate.count",
        event_name: [:fathom, :migrator, :backwards_migrate],
        measurement: :count,
        description:
          "Backwards `manage.py migrate` runs detected on the capture template (#6) — the template moved DOWN while the fleet stayed up. Fleet undo is a fathom revert, never a Django backwards migrate; any occurrence means the two are diverging"
      ),
      sum("fathom.migrator.data_migration_captured.count",
        event_name: [:fathom, :migrator, :data_migration_captured],
        description:
          "Template-literal DML statements captured into a version (#26) — the shape that caps HEAD and freezes the rollout until an operator attaches a transform or approves it. Untagged: metadata carries the version NUMBER. Pairs with the review-block panel on /admin/migrations"
      ),
      sum("fathom.migrator.capture_pending_on_shutdown.count",
        event_name: [:fathom, :migrator, :capture_pending_on_shutdown],
        description:
          "Captured-but-unrecorded migrations still buffered when capture shut down — the template has advanced and the fleet has no version for it, which is the same divergence `migration_gap` catches later and more expensively"
      ),

      # Durability — flush LATENCY (the failure counter above only sees flushes that error).
      distribution("fathom.shard.flush.duration",
        event_name: [:fathom, :shard, :flush],
        measurement: :duration,
        unit: {:native, :millisecond},
        tags: [:outcome],
        reporter_options: [buckets: [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000]],
        description:
          "Durability-flush latency by outcome (uploaded / reconciled / skipped / error). Flushes that SUCCEED but crawl grow the RPO exactly like flushes that fail, and nothing measured that — `flush.failed` only counts the ones that error out"
      ),
      counter("fathom.shard.open.failed.count",
        event_name: [:fathom, :shard, :open, :failed],
        description:
          "Coordinator opens that failed (lease lost, pull failed, corrupt object) — the coordinator stops with {:shutdown, _} and the checkout maps it to an error, so this is a tenant-visible open failure. Untagged: the metadata `reason` is a passed-through storage error"
      ),

      # Single-writer plumbing — the two events that prove the fence is working, not just failing.
      counter("fathom.shard.heartbeat.renewed.count",
        event_name: [:fathom, :shard, :heartbeat, :renewed],
        description:
          "Node-heartbeat renewals — liveness is O(nodes), so this is a flat, predictable rate and its ABSENCE is the signal. `heartbeat.lapsed` catches the lapse; this is the denominator that shows renewals stopped rather than the node going quiet"
      ),
      counter("fathom.shard.lease.release_retried.count",
        event_name: [:fathom, :shard, :lease, :release_retried],
        description:
          "Lease releases that hit a 412 and re-read to find the lock STILL OURS at a rotated etag, then finished the delete (the 2026-08-04 stuck-lease fix). Every one of these would previously have leaked a lock that no live node could ever steal — a tenant that serves normally but can NEVER migrate, snoozing forever with failed: 0"
      ),

      # Warm standby — the revalidation sweep's actual shape (A1).
      last_value("fathom.shard.warm.refresh.cached",
        event_name: [:fathom, :shard, :warm, :refresh],
        measurement: :cached,
        description:
          "Shards held in the warm-follower cache after a revalidation sweep — the standing failover-readiness gauge, bounded by :warm_cache_max and the disk-free floor (#36)"
      ),
      sum("fathom.shard.warm.refresh.body_bytes",
        event_name: [:fathom, :shard, :warm, :refresh],
        measurement: :body_bytes,
        description:
          "Bytes re-downloaded during warm revalidation — the sweep is designed to land on the 304 fast path, so a rising number means cached copies are going stale faster than they are being refreshed"
      ),

      # Snapshots — the scheduler half (the retention/deletion half is above).
      sum("fathom.snapshots.scheduled.ok",
        event_name: [:fathom, :snapshots, :scheduled],
        measurement: :ok,
        description:
          "Snapshots taken by the hourly scheduler (#18) — the RPO for LOGICAL corruption is this cadence, not the 5s flush interval, so a stalled scheduler silently lengthens the only window that answers 'restore tenant acme to 09:00'"
      ),
      sum("fathom.snapshots.scheduled.error",
        event_name: [:fathom, :snapshots, :scheduled],
        measurement: :error,
        description:
          "Per-shard snapshot failures in the scheduled sweep (#18). `last_snapshot_at` is stamped only on SUCCESS, so a failing shard stays at the head of the rotation rather than being marked done — this is what says it is retrying, not converging"
      ),

      # Restore drill — the other two result streams (#48); `restore_drill.result` is above.
      counter("fathom.restore_drill.full_result.count",
        event_name: [:fathom, :restore_drill, :full_result],
        tags: [:status],
        description:
          "Full restore-drill outcomes (#48) — this one exercises the recovery PROCEDURE (fork → row-count compare → drop), not just whether the stored bytes read back. `fork_failed` means the recovery path itself is broken; `restored_mismatch` means it produced a database that is not the source"
      ),
      counter("fathom.restore_drill.snapshot_result.count",
        event_name: [:fathom, :restore_drill, :snapshot_result],
        tags: [:status],
        description:
          "Snapshot verification outcomes (#48). Snapshots are storage objects with no directory row, so the shard sampler never saw them — the one class of data a point-in-time recovery reaches for was the one class nothing checked"
      ),
      last_value("fathom.restore_drill.snapshot_coverage.coverage_runs",
        event_name: [:fathom, :restore_drill, :snapshot_coverage],
        measurement: :coverage_runs,
        description:
          "How many drill runs it takes to verify EVERY snapshot of a shard (#24). The drill rotates -- newest snapshot every run plus one older -- instead of re-downloading all ~35 of them every run. This publishes the period so the guarantee is checkable: if it climbs, retention widened and point-in-time coverage got slower without anyone deciding that"
      ),
      last_value("fathom.restore_drill.snapshot_coverage.held",
        event_name: [:fathom, :restore_drill, :snapshot_coverage],
        measurement: :held,
        description:
          "Snapshots a drilled shard holds. Against snapshot_coverage.verified this is the amplification factor the pre-#24 drill paid every run"
      ),
      last_value("fathom.restore_drill.snapshot_coverage.verified",
        event_name: [:fathom, :restore_drill, :snapshot_coverage],
        measurement: :verified,
        description:
          "Snapshots actually verified for a shard this run (1 or 2 after #24; it was `held` before)"
      ),

      # Lifecycle deny-set — the recovery half of the degraded counter above (#33).
      counter("fathom.tenants.denylist.recovered.count",
        event_name: [:fathom, :tenants, :denylist, :recovered],
        tags: [:kind],
        description:
          "A lifecycle deny set (tombstones/suspensions) finished loading after a degraded start (#33) — pairs with denylist.degraded so an alert can clear rather than needing a human to confirm recovery"
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
      counter("fathom.shard.heartbeat.lapse_broadcast.count",
        event_name: [:fathom, :shard, :heartbeat, :lapse_broadcast],
        tags: [:inline],
        description:
          "Lapse notifications actually fanned out to coordinators, tagged by whether the dispatch ran INLINE in the heartbeat process (#18). Before this the path had no observable at all -- broadcast_lapse/1 swallows every failure, so a lapse that notified nobody looked like one that notified everybody. inline=true sustained means the task supervisor is unavailable and the heartbeat is paying a dispatch measured at p50 35ms against 30k subscribers, inside the critical path that ends the lapse"
      ),

      # Control-plane abuse throttles (#34): the admin BasicAuth brute-force lockout + the /api
      # request-rate limit. High-frequency security signals live here (not the Postgres audit log,
      # which would amplify a flood) — a rising floor ⇒ a brute-force / hammering attempt in progress.
      counter("fathom.admin_auth.failed.count",
        event_name: [:fathom, :admin_auth, :failed],
        description:
          "Failed admin-password (BasicAuth) attempts — the brute-force signal for the one shared admin credential (#8/#34)"
      ),
      counter("fathom.admin_auth.blocked.count",
        event_name: [:fathom, :admin_auth, :blocked],
        description:
          "Admin-auth requests refused with 429 because the source IP is locked out (too many failures in the window) (#34)"
      ),
      counter("fathom.api.rate_limited.count",
        event_name: [:fathom, :api, :rate_limited],
        description:
          "Control-plane (/api) requests refused with 429 for exceeding the per-IP rate limit (#34)"
      ),

      # Lifecycle deny-set boot-load health (#33): the tombstone (410) / suspend (403) admission
      # gates load from the directory at boot and fast-retry a failed load (1s→30s) instead of
      # silently waiting the 5-min refresh. This counter fires on every retry the set is still
      # unloaded — a rising floor ⇒ a node's delete/suspend guarantees don't hold (Postgres
      # unreachable from that node). Low cardinality: one series per kind.
      counter("fathom.tenants.denylist.degraded.count",
        event_name: [:fathom, :tenants, :denylist, :degraded],
        tags: [:kind],
        description:
          "A lifecycle admission deny set (tombstones/suspensions) has an unloaded boot state and is retrying — the delete/suspend contract is not enforced on this node until it recovers (#33)"
      ),

      # Control plane — a stalled Oban is a silent migration/reconcile/rebalancer/retirement outage.
      counter("fathom.oban.job.exception.count",
        event_name: [:oban, :job, :exception],
        tags: [:queue],
        description:
          "Oban job failures by queue — migrations/reconcile/rebalancer/retirement/tenant-lifecycle progress; sustained ⇒ control-plane stall (often a Postgres incident, see operations.md)"
      ),

      # Control plane — the OTHER half (#18): the exception counter catches jobs that FAIL; these
      # catch jobs that DON'T RUN (a backlogged/paused queue, a wedged fleet-singleton cron leader).
      # Low cardinality: one series per configured queue / per cron worker.
      last_value("fathom.oban.queue.available",
        event_name: [:fathom, :oban, :queue],
        measurement: :available,
        tags: [:queue],
        description:
          "Available (runnable, not yet picked up) jobs per queue — a rising floor ⇒ the queue isn't draining (paused, or depth outpacing the concurrency limit)"
      ),
      last_value("fathom.oban.queue.retryable",
        event_name: [:fathom, :oban, :queue],
        measurement: :retryable,
        tags: [:queue],
        description:
          "Retryable (failed, awaiting backoff) jobs per queue — a rising floor ⇒ jobs are failing faster than they succeed"
      ),
      last_value("fathom.oban.queue.oldest_age_ms",
        event_name: [:fathom, :oban, :queue],
        measurement: :oldest_age_ms,
        tags: [:queue],
        unit: :millisecond,
        description:
          "Age of the oldest runnable (available) job per queue — high ⇒ the queue is backlogged/stalled and work is not being picked up (#18)"
      ),
      last_value("fathom.oban.cron.age_ms",
        event_name: [:fathom, :oban, :cron],
        measurement: :age_ms,
        tags: [:worker],
        unit: :millisecond,
        description:
          "Seconds since a fleet-singleton cron (reconcile/rebalance) last inserted a job — > 2× its period ⇒ leadership wedged and the cron silently stopped, zero exceptions (#18)"
      )
    ]
  end

  @doc false
  def measure_active_shards do
    :telemetry.execute([:fathom, :shards], %{active: Registry.count(Fathom.ShardRegistry)}, %{})
  end

  # --- OpenTelemetry span bridge: the checkout :telemetry.span -> an OTel trace span ---

  # Default FALSE: the bridge's handlers run inline in the checkout caller (span ctx alloc,
  # active-span ETS insert/delete) and the default `always_on` sampler makes them full
  # recording spans — pure per-checkout cost when no collector consumes them. runtime.exs
  # flips this true alongside the exporter when OTEL_EXPORTER_OTLP_ENDPOINT is set, so the
  # documented "traces are env-gated on the endpoint" posture is now literally true
  # (expert review 2026-07-23 #3). Public (@doc false) so the default is pinned by test.
  @doc false
  def otel_spans?, do: Application.get_env(:fathom, :otel_spans, false)

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
