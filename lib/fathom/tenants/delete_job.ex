defmodule Fathom.Tenants.DeleteJob do
  @moduledoc """
  Oban worker that physically erases a tenant's shard (expert review 2026-07-14 #15).

  The re-mint guard (directory tombstone + the fleet-wide `Fathom.Tenants.Tombstones`
  broadcast) is set synchronously by `Fathom.Tenants.delete/1` BEFORE this job is
  enqueued, so from the instant a delete starts no new traffic can open the shard. This
  job then does the durable, retryable erase — cancel pending per-shard jobs, drain the
  home coordinator, and purge every stored object — off the request path.

  Unique per `shard_id` so a double-click can't run two concurrent erases. Idempotent:
  every step tolerates already-gone state, so a retry after a partial run finishes the
  job cleanly.
  """
  # Expert review 2026-10-12 #10. Was 5 attempts on Oban's default backoff (about 7 minutes of
  # horizon): an S3 brownout longer than that DISCARDED the job, leaving a tombstoned tenant's data
  # in storage with nothing to re-drive it. 20 attempts on a 30 s doubling backoff capped at 15 min
  # is about 4.5 hours; `Fathom.Migrator.ReconcileJob` re-enqueues past even that.
  @max_attempts 20
  @backoff_base_s 30
  @backoff_cap_s 900

  require Logger

  use Oban.Worker,
    queue: :tenants,
    max_attempts: @max_attempts,
    unique: [
      keys: [:shard_id],
      # `period: :infinity` is LOAD-BEARING, not decoration (expert review 2026-08-24 #23, extended
      # by 2026-08-26 #36). A KEYWORD LIST in `unique:` merges into Oban's @unique_defaults and so
      # inherits `period: 60` — only a bare `unique: true` gets `:infinity`. Without this line the
      # dedup silently expires after SIXTY SECONDS, and a short retry horizon with Oban's
      # backoff put attempt 3 well past that, so a re-issued job during a retry inserts a second
      # concurrent run. A row stranded in `:executing` by a dead node also stops deduping after a
      # minute — the wedge case Lifeline's `rescue_after` exists for, and `config/config.exs`
      # asserts this property of every per-shard worker by name.
      #
      # Safe BECAUSE `:completed` is deliberately absent from the states below: an infinite period
      # blocks re-enqueue only while a job for that shard is genuinely pending, never forever after
      # one finishes.
      period: :infinity,
      states: [:scheduled, :available, :executing, :retryable, :suspended]
    ]

  # Bound the run under Lifeline's `rescue_after` (config.exs, 60 min) so a wedged job ends instead
  # of being rescued while its process is still alive (expert review 2026-10-13 #6). Oban's default
  # is :infinity. Pinned by test/fathom/oban_worker_timeout_test.exs.
  @impl Oban.Worker
  def timeout(_job), do: :timer.minutes(50)

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"shard_id" => shard_id}} = job) do
    case Fathom.Tenants.purge(shard_id) do
      {:error, reason} = err ->
        # The last attempt's error return discards the job: the tenant is tombstoned but its data
        # is still in storage. Make that loud; the reconcile sweep re-drives it.
        if job.attempt >= job.max_attempts do
          Logger.error("tenant #{shard_id}: DeleteJob discarded (#{inspect(reason)})")

          :telemetry.execute([:fathom, :tenants, :delete_discarded], %{count: 1}, %{
            shard_id: shard_id
          })
        end

        err

      other ->
        other
    end
  end

  @impl Oban.Worker
  def backoff(%Oban.Job{attempt: attempt}) do
    min(@backoff_base_s * Integer.pow(2, max(attempt - 1, 0)), @backoff_cap_s)
  end
end
