defmodule Fathom.HranaAuth.RevokeJob do
  @moduledoc """
  Revokes one shard's outstanding tokens, as the paced unit of
  `Fathom.HranaAuth.revoke_issued_before/2` (expert review 2026-08-01 #37).

  Per-shard rather than one job for the whole sweep, for two reasons: a partial failure retries only
  the shards that failed, and Oban's queue concurrency is the pacing mechanism — a fleet-wide
  credential revocation should not arrive as one synchronous burst of directory writes and
  LISTEN/NOTIFY broadcasts.

  Unique per shard while pending so an operator who runs the same cutoff twice (or a retry that
  overlaps a re-run) does not queue duplicate revokes. The underlying `revoke/1` is idempotent in
  effect — bumping an already-bumped floor is harmless — but a duplicate still costs a broadcast and
  a round of client disconnects.
  """
  use Oban.Worker,
    queue: :tokens,
    max_attempts: 5,
    unique: [
      keys: [:shard_id],
      # Every incomplete state, matching ShardMigrationJob — omitting :suspended leaves a
      # uniqueness hole Oban warns about at compile time.
      states: [:scheduled, :available, :executing, :retryable, :suspended]
    ]

  require Logger

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"shard_id" => shard_id}}) do
    case Fathom.HranaAuth.revoke(shard_id) do
      {:ok, version} ->
        Logger.info("hrana token revoke: #{shard_id} floor -> v#{version} (bulk sweep)")
        :ok

      # A shard that vanished between selection and execution (deleted tenant) is not a failure:
      # its tokens cannot be used against a shard that no longer exists.
      {:error, :invalid_shard_id} ->
        Logger.info("hrana token revoke: #{shard_id} is gone or invalid; nothing to revoke")
        {:cancel, :invalid_shard_id}
    end
  end
end
