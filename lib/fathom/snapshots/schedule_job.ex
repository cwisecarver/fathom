defmodule Fathom.Snapshots.ScheduleJob do
  @moduledoc """
  Scheduled point-in-time snapshots (expert review 2026-08-01 #18).

  `Fathom.Snapshots.create/2` was a correct, well-fenced primitive that **nothing ever called on a
  schedule**. Meanwhile the durable live object is overwritten in place every
  `:shard_flush_interval_ms` (default 5 s), so for logical corruption — which `docs/durability.md`
  itself names as the more common incident — the last-good state was gone within seconds and
  "restore tenant acme to 09:00" had no answer. That was the largest gap between fathom and the
  Postgres-on-RDS baseline it replaces.

  A fleet-singleton Oban cron (peer leadership, like `ReconcileJob` and `RestoreDrillJob`) that each
  run snapshots up to `:snapshot_schedule_sample` shards, choosing them by
  `Fathom.Directory.sample_for_snapshot/1`: **active, flushed since their last snapshot,
  least-recently-snapshotted first.**

  ## Why the selection predicate is the design

  Every snapshot is a server-side object COPY, so a scheduler that snapshotted every shard every
  run would cost O(fleet) storage and O(fleet) COPY requests per cadence — at a million shards, the
  feature would cost more than the data it protects. Keying off `last_flushed_at > last_snapshot_at`
  makes the cost track **writes**, not tenants: a cold tenant that has not flushed since its last
  snapshot is skipped entirely, and the rotation means a fleet whose write set exceeds the per-run
  budget still converges rather than starving the tail.

  ## Labelling, and what it protects

  Every snapshot taken here is labelled `auto`. `Fathom.Snapshots.Retention` will only ever expire
  ids carrying that label, so an operator's manual `Snapshots.create("acme", label: "pre-migration")`
  is untouchable by the automatic policy. Automatic creation and automatic deletion are the same
  set, deliberately — see that module.

  ## Gated + off by default

  Inert unless `:snapshot_schedule_sample` is a positive integer. Set it (and the crontab cadence)
  to size coverage against your fleet's write rate and storage budget. `RetentionJob` is separately
  gated: turning snapshots on without retention grows storage without bound, which the config
  documentation says plainly and a boot-time warning repeats.
  """
  use Oban.Worker, queue: :migrations, max_attempts: 1

  require Logger

  alias Fathom.Directory
  alias Fathom.Snapshots
  alias Fathom.Snapshots.Retention

  @impl Oban.Worker
  def perform(%Oban.Job{}) do
    case sample_size() do
      n when is_integer(n) and n > 0 -> run(n)
      _ -> :ok
    end

    :ok
  end

  @doc """
  Snapshot up to `n` shards now and return the outcome frequencies.

  Public so an operator can run it from a node console (`Fathom.Snapshots.ScheduleJob.run(50)`) and
  so tests drive it without Oban.
  """
  @spec run(pos_integer()) :: {:ok, map()}
  def run(n) when is_integer(n) and n > 0 do
    results =
      n
      |> Directory.sample_for_snapshot()
      |> Enum.map(&snapshot_one(&1.shard_id))

    counts = Enum.frequencies(results)

    :telemetry.execute(
      [:fathom, :snapshots, :scheduled],
      %{
        total: length(results),
        ok: Map.get(counts, :ok, 0),
        error: Map.get(counts, :error, 0)
      },
      %{}
    )

    if Map.get(counts, :error, 0) > 0 do
      Logger.warning("scheduled snapshots: #{inspect(counts)}")
    end

    {:ok, counts}
  end

  defp snapshot_one(shard_id) do
    case Snapshots.create(shard_id, label: Retention.auto_label()) do
      {:ok, _snapshot_id} ->
        # Stamped ONLY on success. A failed snapshot must leave the shard at the head of the
        # rotation so the next run retries it — stamping regardless would mark a tenant "backed up"
        # on the strength of a failed copy and then not revisit it for a full cycle.
        Directory.record_snapshot(shard_id)
        :ok

      {:error, reason} ->
        Logger.warning("scheduled snapshot failed for #{shard_id}: #{inspect(reason)}")
        :error
    end
  rescue
    # One tenant's failure must not abandon the rest of the batch.
    e ->
      Logger.warning("scheduled snapshot raised for #{shard_id}: #{Exception.message(e)}")
      :error
  end

  defp sample_size, do: Application.get_env(:fathom, :snapshot_schedule_sample)
end
