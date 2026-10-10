defmodule Fathom.Snapshots.RetentionJob do
  @moduledoc """
  Grandfather-father-son expiry of scheduled snapshots (expert review 2026-08-01 #18).

  Before this, nothing ever dropped a `@snap-<id>` object: `RetirementJob` expires `@<version>`
  objects only, and `Tenants.purge/1` runs on deletion. So enabling `ScheduleJob` without this
  would grow storage without bound — which is why the two are separately gated and why the config
  docs pair them.

  A fleet-singleton Oban cron that each run sweeps up to `:snapshot_retention_sample` shards
  (least-recently-snapshotted first, so the rotation matches the scheduler's) and applies
  `:snapshot_retention` — e.g. `%{hourly: 24, daily: 7, weekly: 4}`.

  The classification lives in `Fathom.Snapshots.Retention` as a pure function; this module is the
  shell that lists, plans, and deletes. See that module for the safety property that matters most:
  **only snapshots the scheduler created (`-auto`) are ever eligible for deletion.**

  ## Gated + off by default

  Inert unless BOTH `:snapshot_retention` (the policy) and `:snapshot_retention_sample` (the per-run
  shard budget) are set. A policy with no sample size would do nothing and look enabled; a sample
  size with no policy would be a delete sweep with no rule, which is the one configuration mistake
  here that destroys data. Requiring both makes neither reachable by half-configuring.
  """
  use Oban.Worker,
    queue: :migrations,
    max_attempts: 1,
    # No overlapping sweeps (expert review 2026-10-10 #14); see ScheduleJob for why
    # `period: :infinity` and why `:completed` is absent.
    unique: [
      period: :infinity,
      states: [:scheduled, :available, :executing, :retryable, :suspended]
    ]

  require Logger

  alias Fathom.Directory
  alias Fathom.Snapshots
  alias Fathom.Snapshots.Retention

  # Shards swept at once: a LIST and a few DELETEs each against the shared store pool, so this
  # stays well under the Finch pool rather than competing with cold-open pulls (as ScheduleJob).
  @default_concurrency 16
  # Per shard: a LIST plus its DELETEs. Generous, but never `:infinity`.
  @sweep_timeout_ms 120_000

  @impl Oban.Worker
  def perform(%Oban.Job{}) do
    with policy when is_map(policy) <- policy(),
         n when is_integer(n) and n > 0 <- sample_size() do
      run(n, policy)
    else
      _ -> :ok
    end

    :ok
  end

  @doc """
  Sweep up to `n` shards under `policy` now, returning what was dropped.

  Public so an operator can rehearse it from a node console. **Pass `dry_run: true` first** — it
  reports exactly what would be deleted without deleting anything, which is the sensible way to
  adopt a retention policy on a fleet whose snapshot history you have not looked at.
  """
  @spec run(pos_integer(), Retention.policy(), keyword()) :: {:ok, map()}
  def run(n, policy, opts \\ []) when is_integer(n) and n > 0 do
    dry_run? = Keyword.get(opts, :dry_run, false)
    now = Keyword.get(opts, :now, DateTime.utc_now())

    # CONCURRENTLY, bounded (expert review 2026-10-01 perf #27, second half). Each shard is a LIST
    # plus, in steady state, about one DELETE, so a serial sweep ran at ~1/(2 RTT) shards per
    # second — the same shape ScheduleJob had before 584ac87, and retention must keep up with what
    # the scheduler now creates. A real per-shard timeout, never `:infinity`; a shard that times
    # out counts as an error and, never having been swept, is picked again next run. Batching the
    # DELETEs (DeleteObjects) was the review's other half and is NOT done: at ~1 expiring snapshot
    # per shard per run there is nothing to batch within a shard.
    results =
      n
      |> Directory.sample_for_retention()
      |> Task.async_stream(
        fn %{shard_id: id} -> {id, sweep_one(id, policy, now, dry_run?)} end,
        max_concurrency: concurrency(),
        timeout: @sweep_timeout_ms,
        on_timeout: :kill_task,
        ordered: false
      )
      |> Enum.map(fn
        {:ok, result} -> result
        {:exit, _reason} -> {nil, %{kept: 0, dropped: 0, ineligible: 0, errors: 1}}
      end)

    # Advance the rotation past every shard swept cleanly (expert review 2026-10-08 #19). One that
    # errored or timed out is not stamped, so it stays at the head and is retried next run. A dry
    # run deletes nothing, so it does not count as a sweep.
    unless dry_run? do
      results
      |> Enum.flat_map(fn
        {id, %{errors: 0}} when is_binary(id) -> [id]
        _ -> []
      end)
      |> Directory.record_retention(now)
    end

    results = Enum.map(results, fn {_id, result} -> result end)

    totals =
      Enum.reduce(results, %{kept: 0, dropped: 0, ineligible: 0, errors: 0}, fn r, acc ->
        %{
          kept: acc.kept + r.kept,
          dropped: acc.dropped + r.dropped,
          ineligible: acc.ineligible + r.ineligible,
          errors: acc.errors + r.errors
        }
      end)

    :telemetry.execute([:fathom, :snapshots, :retention], totals, %{dry_run: dry_run?})

    if totals.errors > 0 do
      Logger.warning("snapshot retention: #{inspect(totals)}")
    end

    {:ok, totals}
  end

  defp sweep_one(shard_id, policy, now, dry_run?) do
    case Snapshots.list(shard_id) do
      {:ok, snapshots} ->
        plan = Retention.plan(Enum.map(snapshots, & &1.id), policy, now)

        errors =
          if dry_run? do
            if plan.drop != [] do
              Logger.info(
                "snapshot retention (dry run) #{shard_id}: would drop #{length(plan.drop)} " <>
                  "of #{length(snapshots)} — #{inspect(Enum.take(plan.drop, 5))}"
              )
            end

            0
          else
            Enum.count(plan.drop, fn id -> drop(shard_id, id) == :error end)
          end

        %{
          kept: length(plan.keep),
          dropped: if(dry_run?, do: 0, else: length(plan.drop) - errors),
          ineligible: length(plan.ineligible),
          errors: errors
        }

      {:error, reason} ->
        Logger.warning("snapshot retention: list failed for #{shard_id}: #{inspect(reason)}")
        %{kept: 0, dropped: 0, ineligible: 0, errors: 1}
    end
  rescue
    e ->
      Logger.warning("snapshot retention raised for #{shard_id}: #{Exception.message(e)}")
      %{kept: 0, dropped: 0, ineligible: 0, errors: 1}
  end

  defp drop(shard_id, snapshot_id) do
    case Snapshots.drop(shard_id, snapshot_id) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "snapshot retention: drop #{shard_id}@#{snapshot_id} failed: #{inspect(reason)}"
        )

        :error
    end
  end

  # `%{}` matches ANY map, including the empty one — which is how an empty SNAPSHOT_RETENTION
  # became a valid "keep zero in every bucket" policy (expert review 2026-08-20 #13). The config
  # layer now refuses that at boot; this is the second line of defence, because `:snapshot_retention`
  # is also settable directly from code and test setup. An all-zero policy is the same thing spelled
  # differently, so it is rejected too: retention deleting everything is never what "configured"
  # should mean.
  defp policy do
    case Application.get_env(:fathom, :snapshot_retention) do
      %{} = policy -> keeps_something(policy)
      policy when is_list(policy) -> policy |> Map.new() |> keeps_something()
      _ -> nil
    end
  end

  defp keeps_something(policy) do
    if map_size(policy) > 0 and Enum.any?(policy, fn {_k, n} -> is_integer(n) and n > 0 end),
      do: policy,
      else: nil
  end

  defp sample_size, do: Application.get_env(:fathom, :snapshot_retention_sample)

  defp concurrency do
    case Application.get_env(:fathom, :snapshot_retention_concurrency, @default_concurrency) do
      n when is_integer(n) and n > 0 -> n
      _ -> @default_concurrency
    end
  end
end
