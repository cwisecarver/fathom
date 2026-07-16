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
      states: [:scheduled, :available, :executing, :retryable, :suspended]
    ]

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"shard_id" => shard_id}}) do
    Fathom.Tenants.purge(shard_id)
  end
end
