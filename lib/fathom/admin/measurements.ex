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
  Durability / RPO gauge: how many open shards hold un-flushed writes, and the oldest such
  shard's RPO age (ms). Derives dirtiness from the published flush watermark exactly as
  `Fathom.Shard.unflushed?/1` does — a `WriteCounter` generation mismatch or a write count past
  the flushed watermark — so it never disagrees with the coordinator's own dirty decision.
  """
  @spec durability() :: :ok
  def durability do
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
