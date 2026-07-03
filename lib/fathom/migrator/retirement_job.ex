defmodule Fathom.Migrator.RetirementJob do
  @moduledoc """
  Drops a shard's retained old version once the retention window has passed.
  Scheduled by `Fathom.Migrator.ShardMigrationJob` for `now + retention` after a
  cutover; an S3 lifecycle rule backstops any it misses.
  """
  use Oban.Worker, queue: :retirement, max_attempts: 5

  alias Fathom.Shard.Storage

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"shard_id" => shard_id, "version" => version}}) do
    Storage.drop_version(shard_id, version)
  end
end
