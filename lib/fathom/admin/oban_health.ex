defmodule Fathom.Admin.ObanHealth do
  @moduledoc """
  The two "does the control plane still RUN" signals, computed once and read by both consumers
  (expert review 2026-10-15 #12): per-queue depth + oldest runnable job age, and per-singleton-cron
  freshness. `Fathom.Admin.Measurements.oban_health/0` turns the result into Prometheus gauges; the
  Migrations page renders it. Before this module the computation lived inside the poller, so the
  signals that catch a wedged queue or a dead cron leader reached Prometheus and nowhere else.

  Both reads are bounded: the queue read touches only `available`/`retryable` rows (Oban's
  `state`-leading index), and the cron read is restricted to `meta.cron = true` rows via Oban's GIN
  index on `meta` (expert review 2026-10-01 perf #13 — `worker` has no index, and an unfiltered
  form scanned every completed job of those workers).
  """
  import Ecto.Query, only: [from: 2]

  alias Fathom.Repo

  @type queue_row :: %{
          queue: String.t(),
          available: non_neg_integer(),
          retryable: non_neg_integer(),
          oldest_age_ms: non_neg_integer()
        }
  @type cron_row :: %{worker: String.t(), age_ms: non_neg_integer()}

  @doc """
  `%{queues: [...], crons: [...]}` as of `now`. Queues are every CONFIGURED queue (zeros when
  empty, so a gauge series is stable); crons are only those with rows (a never-run cron has no
  entry). Raises on a Postgres failure — callers decide whether that is fatal.
  """
  @spec snapshot(DateTime.t()) :: %{queues: [queue_row()], crons: [cron_row()]}
  def snapshot(now \\ DateTime.utc_now()) do
    %{queues: queues(now), crons: crons(now)}
  end

  @spec queues(DateTime.t()) :: [queue_row()]
  def queues(now) do
    by_queue =
      from(j in Oban.Job,
        where: j.state in ["available", "retryable"],
        group_by: [j.queue, j.state],
        select: {j.queue, j.state, count(j.id), min(j.scheduled_at)}
      )
      |> Repo.all()
      |> Enum.reduce(%{}, fn {queue, state, count, oldest}, acc ->
        entry = Map.get(acc, queue, %{available: 0, retryable: 0, oldest_available: nil})

        entry =
          case state do
            "available" -> %{entry | available: count, oldest_available: oldest}
            "retryable" -> %{entry | retryable: count}
          end

        Map.put(acc, queue, entry)
      end)

    for queue <- configured_queues() do
      e = Map.get(by_queue, queue, %{available: 0, retryable: 0, oldest_available: nil})

      age_ms =
        if e.oldest_available,
          do: max(0, DateTime.diff(now, e.oldest_available, :millisecond)),
          else: 0

      %{queue: queue, available: e.available, retryable: e.retryable, oldest_age_ms: age_ms}
    end
  end

  # Seconds since each singleton cron last inserted a job. A wedged cron leader stops inserting, so
  # the age grows. CRON INSERTS ONLY: a job enqueued by hand is not evidence the leader is alive.
  @spec crons(DateTime.t()) :: [cron_row()]
  def crons(now) do
    case cron_workers() do
      [] ->
        []

      workers ->
        from(j in Oban.Job,
          where: j.worker in ^workers and fragment("? @> ?", j.meta, ^%{cron: true}),
          group_by: j.worker,
          select: {j.worker, max(j.inserted_at)}
        )
        |> Repo.all()
        |> Enum.flat_map(fn
          {worker, %DateTime{} = last} ->
            [%{worker: worker, age_ms: max(0, DateTime.diff(now, last, :millisecond))}]

          _ ->
            []
        end)
        |> Enum.sort_by(& &1.worker)
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
