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
  use Oban.Worker,
    queue: :tenants,
    max_attempts: 5,
    unique: [
      keys: [:shard_id],
      # `period: :infinity` is LOAD-BEARING, not decoration (expert review 2026-08-24 #23, extended
      # by 2026-08-26 #36). A KEYWORD LIST in `unique:` merges into Oban's @unique_defaults and so
      # inherits `period: 60` — only a bare `unique: true` gets `:infinity`. Without this line the
      # dedup silently expires after SIXTY SECONDS, and `max_attempts: 5` with Oban's default
      # backoff puts attempt 3 well past that, so a re-issued job during a retry inserts a second
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

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"shard_id" => shard_id}}) do
    Fathom.Tenants.purge(shard_id)
  end
end
