defmodule Fathom.Admin.Measurements do
  @moduledoc """
  Periodic gauge measurements for the metrics layer, driven by the `Fathom.Telemetry` poller.
  Each emits a `:telemetry` event the Prometheus reporter turns into a gauge (and the dashboard's
  `Fathom.Admin.MetricsCollector` reads the same values). Cheap, node-local, off the hot path.

  Storage footprint (`Fathom.Shard.Storage.stored_usage/0`) is deliberately **not** here — it can
  be a full S3 LIST, so the collector polls it on a slow, separate cadence and caches it.
  """
  import Ecto.Query

  require Logger

  alias Fathom.Admin.FlushWatermark
  alias Fathom.Repo
  alias Fathom.Shard.WriteCounter

  @doc "BEAM memory gauge for this node."
  @spec node_memory() :: :ok
  def node_memory do
    :telemetry.execute([:fathom, :node, :memory], %{total: :erlang.memory(:total)}, %{})
  end

  @doc """
  Process- and port-table headroom for this node.

  Expert review 2026-07-24 #2: neither limit degrades gracefully. Exhausting `+P` makes `spawn`
  throw `system_limit` (a supervisor start failure — i.e. refused checkouts); exhausting `+Q` makes
  `gen_tcp:accept` return `{:error, :system_limit}` and the listener stops accepting. Both are hard
  availability cliffs, and production materializes 3–4 processes and 1 port per served shard, so at
  the demonstrated densities they are reachable. `rel/vm.args.eex` raises both well clear, and these
  gauges make the approach visible so it can be alerted on rather than discovered as an outage.
  """
  @spec vm_limits() :: :ok
  def vm_limits do
    processes = :erlang.system_info(:process_count)
    process_limit = :erlang.system_info(:process_limit)
    ports = :erlang.system_info(:port_count)
    port_limit = :erlang.system_info(:port_limit)

    :telemetry.execute(
      [:fathom, :node, :vm_limits],
      %{
        processes: processes,
        process_limit: process_limit,
        process_used_ratio: safe_ratio(processes, process_limit),
        ports: ports,
        port_limit: port_limit,
        port_used_ratio: safe_ratio(ports, port_limit)
      },
      %{}
    )
  end

  defp safe_ratio(_used, limit) when not is_integer(limit) or limit <= 0, do: 0.0
  defp safe_ratio(used, limit), do: used / limit

  @doc """
  The node-wide concurrent-flush gate: how many slots are in flight, and the cap.

  Added because the gate had NO observability at all (expert review 2026-08-20 #15). A leaked slot
  — a coordinator killed mid-flush — is permanent without `FlushGate.sweep/0`, and the cap is
  single digits, so a handful of leaks makes `try_acquire/0` answer `:full` forever and every dirty
  shard on the node stops flushing. Nothing else surfaces that: `[:fathom, :shard, :flush, :failed]`
  only fires for a flush that actually RAN, so the node goes quiet rather than loud, and the RPO
  grows unbounded behind the silence.

  `in_flight` at or above `cap` for a sustained period is the alertable condition.
  """
  @spec flush_gate() :: :ok
  def flush_gate do
    case Fathom.Shard.FlushGate.cap() do
      cap when is_integer(cap) ->
        :telemetry.execute(
          [:fathom, :shard, :flush_gate],
          %{in_flight: Fathom.Shard.FlushGate.in_flight(), cap: cap},
          %{}
        )

      _ ->
        # No cap configured: the gate is off and the counter is never touched.
        :ok
    end

    :ok
  end

  @doc """
  Local-disk headroom for the directories fathom writes to (expert review 2026-08-01 #36).

  Nothing in the metrics layer read the filesystem before this: `fathom.storage.bytes` is *S3*
  usage, and the warm-follower cache — the one component deliberately sized to fill disk — is
  budgeted in shard **count** (`:warm_cache_max`, default 500), which is 8 MB or 2 TB depending on
  tenant size. `docs/runbooks/operations.md` ranks disk-full a top-four incident and its prevention
  step is "alert on disk %", a signal fathom did not emit.

  **Why it matters more than an ordinary capacity gauge:** when the volume fills, every cold-open
  `pull` fails AND every dirty shard's `VACUUM INTO` fails, so writes keep being **acked** and can
  never be made durable — the RPO contract goes unbounded. The symptoms that surface
  (`fathom.shard.flush.failed` climbing, `fathom.durability.oldest_age_ms` growing) are the same
  ones an S3 credential or reachability problem produces, so without this gauge the diagnostic path
  points away from the actual cause.

  Emits per directory (`data` = `SHARD_DATA_DIR`, `warm` = the warm-standby cache, `replica` =
  `REPLICATION_DIR`), tagged so a fleet dashboard can break them out: `free_bytes`, `total_bytes`,
  `used_ratio` — the last matching the `process_used_ratio` / `port_used_ratio` convention above.

  `replica` joined 2026-08-10 with the follower listener. A node acting as somebody's follower
  stores a **full copy of every shard it follows**, so it is a third disk consumer of exactly the
  kind this gauge exists for — and the one with the least warning, because it grows from other
  nodes' write traffic rather than from anything happening locally. It is only non-nil on a node
  that actually listens; a pure primary reports `data` and `warm` as before.

  Best-effort: a path that does not exist yet (no warm cache configured, first boot before the data
  dir is created) is skipped rather than reported as 0 free, which would read as a full disk.
  """
  @spec disk() :: :ok
  def disk do
    for {label, path} <-
          [
            {"data", Fathom.Shard.data_dir()},
            {"warm", warm_cache_dir()},
            {"replica", replica_dir()}
          ],
        is_binary(path),
        {:ok, %{total_bytes: total, free_bytes: free}} <- [disk_info(path)] do
      :telemetry.execute(
        [:fathom, :node, :disk],
        %{
          free_bytes: free,
          total_bytes: total,
          used_ratio: safe_ratio(total - free, total)
        },
        %{dir: label}
      )
    end

    :ok
  end

  @doc """
  Free/total bytes for the filesystem holding `path`, or `:error` if it cannot be read.

  `:disksup.get_disk_info/1` reports the mount containing the path as
  `[{mount, total_kb, available_kb, capacity_percent}]`. KB there is 1024-byte blocks. Returns
  `:error` rather than raising or guessing: this feeds a gauge and a back-pressure decision, and a
  fabricated number in either is worse than an absent one — a wrong "plenty free" disables the
  brake, a wrong "full" stops warming on a healthy node.
  """
  @spec disk_info(String.t()) ::
          {:ok, %{mount: String.t(), total_bytes: integer(), free_bytes: integer()}} | :error
  def disk_info(path) do
    # Resolve to the nearest EXISTING ancestor first. `SHARD_DATA_DIR` and the warm cache are
    # created lazily — the data dir does not exist until the first shard opens — and `disksup`
    # returns nothing for a path that is not there. Without this the gauge is blind on exactly the
    # node state where disk headroom is most worth knowing: a freshly booted node about to pull its
    # working set. The ancestor sits on the same filesystem, which is the quantity being measured.
    case :disksup.get_disk_info(to_charlist(existing_ancestor(path))) do
      [{mount, total_kb, avail_kb, _pct} | _] when is_integer(total_kb) and total_kb > 0 ->
        # `mount` identifies the FILESYSTEM, not the path — two directories reporting the same
        # mount share a volume, and therefore share a free-space budget. That is the question
        # `Application.check_replication_disk!/0` asks, and comparing sizes cannot answer it: two
        # separate volumes of the same size look identical.
        {:ok,
         %{
           mount: List.to_string(mount),
           total_bytes: total_kb * 1024,
           free_bytes: avail_kb * 1024
         }}

      _ ->
        :error
    end
  rescue
    # os_mon not started (a release that trimmed it, or a test env), or the path is unreadable.
    _ -> :error
  catch
    :exit, _ -> :error
  end

  defp warm_cache_dir do
    if Fathom.Shard.WarmFollower.enabled?(), do: Fathom.Shard.WarmFollower.cache_dir()
  end

  # Gated on `listening?/0` for the same reason `warm_cache_dir/0` is gated on `enabled?/0`: a node
  # that is not somebody's follower writes nothing here, and reporting the default tmp path would
  # publish a `dir=replica` series for a directory that will never grow — which is worse than
  # absent, because an alert on it would be measuring an unrelated filesystem.
  defp replica_dir do
    if Fathom.Shard.Replication.Fleet.listening?(),
      do: Fathom.Shard.Replication.Follower.default_dir()
  end

  # Walk up until something exists. Bounded by construction: each step drops one path component and
  # `Path.dirname/1` is a fixed point at the root, so the recursion terminates there even for a
  # path that shares no ancestor with anything real.
  defp existing_ancestor(path) do
    cond do
      File.exists?(path) -> path
      Path.dirname(path) == path -> path
      true -> existing_ancestor(Path.dirname(path))
    end
  end

  @doc """
  Durability / RPO gauge: how many open shards hold un-flushed writes, and the oldest such
  shard's RPO age (ms). Derives dirtiness from the published flush watermark exactly as
  `Fathom.Shard.unflushed?/1` does — a `WriteCounter` generation mismatch or a write count past
  the flushed watermark — so it never disagrees with the coordinator's own dirty decision.
  """
  @spec durability() :: :ok
  def durability do
    # O(open shards) walk (a FlushWatermark snapshot copy + one counter read per shard):
    # skip it entirely when the observability layer is off — nothing consumes the event,
    # and the walk scales linearly with density (review 2026-07-23 #30). node_memory/0
    # stays ungated (a single :erlang.memory call, effectively free).
    if Fathom.Admin.enabled?(), do: do_durability()
    :ok
  end

  defp do_durability do
    now = System.monotonic_time(:millisecond)
    gen = WriteCounter.generation()

    {dirty, oldest} =
      Enum.reduce(FlushWatermark.snapshot(), {0, 0}, fn
        {id, flushed_through, counter_gen, flushed_at}, {dirty, oldest} ->
          if counter_gen != gen or WriteCounter.count(id) > flushed_through do
            {dirty + 1, max(oldest, now - flushed_at)}
          else
            {dirty, oldest}
          end
      end)

    :telemetry.execute(
      [:fathom, :durability, :rpo],
      %{dirty_shards: dirty, oldest_age_ms: oldest},
      %{}
    )
  end

  @doc """
  Control-plane liveness gauges (expert review #18): the exception counter catches jobs that
  *fail*, but not jobs that *don't run* — a backlogged/paused queue, or a wedged fleet-singleton
  cron (reconcile, rebalance) that silently stops with zero exceptions. Emits, per queue, the
  `available` + `retryable` depth and the oldest runnable job's age; and, per singleton cron, the
  seconds since it last inserted a job (its freshness). A Postgres blip never crashes the poller —
  the gauge just goes stale, itself a signal. Gated onto its own slow poller by `Fathom.Admin`
  (never runs in test / when metrics are off), so this is the one measurement that touches Postgres.
  """
  @spec oban_health() :: :ok
  def oban_health do
    now = DateTime.utc_now()
    emit_queue_gauges(now)
    emit_cron_gauges(now)
    :ok
  rescue
    e ->
      Logger.warning("oban_health measurement skipped (Postgres unreachable?): #{inspect(e)}")
      :ok
  end

  # Per-queue depth (available + retryable) and the oldest RUNNABLE (available) job's age. Emit for
  # every CONFIGURED queue, defaulting to 0, so the gauges are stable even when a queue is empty.
  defp emit_queue_gauges(now) do
    by_queue =
      Repo.all(
        from(j in Oban.Job,
          where: j.state in ["available", "retryable"],
          group_by: [j.queue, j.state],
          select: {j.queue, j.state, count(j.id), min(j.scheduled_at)}
        )
      )
      |> Enum.reduce(%{}, fn {queue, state, count, oldest}, acc ->
        entry = Map.get(acc, queue, %{available: 0, retryable: 0, oldest_available: nil})

        entry =
          case state do
            "available" -> %{entry | available: count, oldest_available: oldest}
            "retryable" -> %{entry | retryable: count}
            _ -> entry
          end

        Map.put(acc, queue, entry)
      end)

    for queue <- configured_queues() do
      e = Map.get(by_queue, queue, %{available: 0, retryable: 0, oldest_available: nil})

      age_ms =
        if e.oldest_available,
          do: max(0, DateTime.diff(now, e.oldest_available, :millisecond)),
          else: 0

      :telemetry.execute(
        [:fathom, :oban, :queue],
        %{available: e.available, retryable: e.retryable, oldest_age_ms: age_ms},
        %{queue: queue}
      )
    end
  end

  # Cron freshness: seconds since each singleton cron last inserted a job. A wedged cron leader
  # stops inserting, so `now - max(inserted_at)` grows and the alert fires — the exact stall the
  # exception counter can't see. Only crons with rows are emitted (a never-run cron has no series;
  # once it runs the gauge appears and tracks). Worker strings come from the Oban config crontab.
  defp emit_cron_gauges(now) do
    case cron_workers() do
      [] ->
        :ok

      workers ->
        Repo.all(
          from(j in Oban.Job,
            where: j.worker in ^workers,
            group_by: j.worker,
            select: {j.worker, max(j.inserted_at)}
          )
        )
        |> Enum.each(fn
          {worker, %DateTime{} = last} ->
            :telemetry.execute(
              [:fathom, :oban, :cron],
              %{age_ms: max(0, DateTime.diff(now, last, :millisecond))},
              %{worker: worker}
            )

          _ ->
            :ok
        end)
    end
  end

  @doc """
  Quorum-replication follower state (Phase 2 A2).

  Emits `[:fathom, :replication, :followers]` with `configured`, `connected`, `quorum` and
  `slack` (`connected - quorum`).

  **`slack` is the number this exists for.** A commit needs `quorum` acks, so a fleet with exactly
  `quorum` followers connected still succeeds on every write — and is one loss away from every
  write failing. That state is invisible from the outside: latency is normal, no error rate moves,
  and the shard reports healthy right up until it does not. The pre-A2 gauges cannot show it
  either, because they measure S3 and disk.

  A separate `[:fathom, :replication, :degraded]` event was written first and removed:
  `TelemetryCoverageTest` caught that nothing exported it, and it was redundant anyway — `slack`
  is a number, and `== 0` / `< 0` are the two alert conditions
  (`deploy/observability/alert-rules.yml`). A second event carrying strictly less information is
  not a signal, it is a thing to keep in sync. Do not re-add it.

  A no-op when replication is off, so a node that never enabled A2 emits nothing rather than a
  permanently-degraded zero.

  Uses `Fleet.connection_status/0`, **not** `Fleet.health/0`: this rides the 10 s poller, which
  this module's own contract keeps Postgres-free (only the 30 s Oban poller may query the DB). The
  roster's liveness view is the dashboard's, not this gauge's.
  """
  @spec replication() :: :ok
  def replication do
    if Fathom.Shard.Replication.Session.enabled?() do
      status = Fathom.Shard.Replication.Fleet.connection_status()
      quorum = Application.get_env(:fathom, :replication_quorum, 2)
      connected = Enum.count(status, fn {_key, up?} -> up? end)
      slack = connected - quorum

      :telemetry.execute(
        [:fathom, :replication, :followers],
        %{configured: length(status), connected: connected, quorum: quorum, slack: slack},
        %{}
      )

      replication_budget()
    end

    :ok
  end

  # THE LEADING SATURATION SIGNAL, and the reason it exists rather than only a reject counter.
  #
  # Replication does not degrade gracefully as tenant count rises — measured on the rig, 512 -> 1024
  # -> 2048 tenants went 3,340 -> 2,776 -> 258 txn/s, i.e. a 10.8x collapse for the last doubling.
  # An operator cannot see that coming from throughput, because throughput looks fine right up to
  # the cliff (`docs/reviews/a2-flush-interval-2026-08-18.md`).
  #
  # `:overloaded` rejects DO track it cleanly — 0 at 512, ~9k at 1024, ~17k at 2048 — but a reject
  # is a LAGGING signal: by the time it fires the node is already refusing tenant writes. This gauge
  # is the same quantity one step earlier: how full the per-node byte budget is RIGHT NOW, which
  # climbs before anything is refused.
  #
  # Node-level, never per shard. A per-shard tag at fathom's stated scale is cardinality death — the
  # same reasoning that keeps `Fathom.ShardLoad` a read API rather than a metric.
  defp replication_budget do
    alias Fathom.Shard.Replication.Budget

    max = Budget.max_bytes()
    used = Budget.queued()

    # `max` of 0 means the bound is disabled, and a ratio against it is meaningless rather than
    # infinite — report the bytes and leave the ratio at 0 so a dashboard shows "no bound" instead
    # of a division error or a fake 100%.
    ratio = if max > 0, do: used / max, else: 0.0

    :telemetry.execute(
      [:fathom, :replication, :budget],
      %{used_bytes: used, max_bytes: max, used_ratio: ratio},
      %{}
    )
  end

  defp configured_queues do
    :fathom
    |> Application.get_env(Oban, [])
    |> Keyword.get(:queues, [])
    |> Enum.map(fn {queue, _limit} -> to_string(queue) end)
  end

  # The worker module strings of every configured cron (Oban stores `worker` without the
  # "Elixir." prefix), so the freshness query matches the rows the Cron plugin inserts.
  defp cron_workers do
    :fathom
    |> Application.get_env(Oban, [])
    |> Keyword.get(:plugins, [])
    |> Enum.find_value([], fn
      {Oban.Plugins.Cron, opts} -> Keyword.get(opts, :crontab, [])
      _ -> nil
    end)
    |> Enum.map(fn entry -> elem(entry, 1) end)
    |> Enum.map(&(&1 |> Atom.to_string() |> String.replace_leading("Elixir.", "")))
    |> Enum.uniq()
  end
end
