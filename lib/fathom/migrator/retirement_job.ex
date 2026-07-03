defmodule Fathom.Migrator.RetirementJob do
  @moduledoc """
  Drops a shard's retained old version once the retention window has passed.
  Scheduled by `Fathom.Migrator.ShardMigrationJob` for `now + retention` after a
  cutover; an S3 lifecycle rule backstops any it misses.
  """
  use Oban.Worker, queue: :retirement, max_attempts: 5

  alias Fathom.Directory
  alias Fathom.Shard.Storage

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"shard_id" => shard_id, "version" => version}}) do
    case Directory.get(shard_id) do
      # The version is LIVE again — a revert restored it after this drop was scheduled
      # (expert review #22). Its retained copy is the fleet's recovery point, not
      # garbage: dropping it would make the next revert 404 mid-sequence. Belt to the
      # RevertJob's cancel_retirement braces (this catches a drop already dequeued, or
      # one the cancel missed).
      {:ok, %{schema_version: ^version}} ->
        {:cancel, :version_live}

      _ ->
        Storage.drop_version(shard_id, version)
    end
  end
end
