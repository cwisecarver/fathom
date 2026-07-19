defmodule Fathom.Migrator.RetirementJob do
  @moduledoc """
  Drops a shard's retained old version once the retention window has passed.
  Scheduled by `Fathom.Migrator.ShardMigrationJob` for `now + retention` after a
  cutover; an S3 lifecycle rule backstops any it misses.
  """
  use Oban.Worker, queue: :retirement, max_attempts: 5

  import Ecto.Query, only: [from: 2]

  alias Fathom.Directory
  alias Fathom.Shard.Storage

  @retention_seconds 7 * 24 * 60 * 60

  @doc "Retention window (seconds) a retired version's storage object is kept before this job drops it."
  @spec retention_seconds() :: pos_integer()
  def retention_seconds, do: @retention_seconds

  @doc """
  An Oban changeset scheduling `version`'s storage object for deletion after the retention window.
  The migration/revert cutover `Oban.insert`s this INSIDE its Postgres transaction (the retirement
  outbox, expert review 2026-07-18 #5), so a committed cutover always carries a durable retirement
  enqueue — a Postgres blip can never leave a cut-over shard whose old `@version` object is never
  scheduled for deletion (a silent, un-self-healing storage leak).
  """
  @spec schedule_changeset(String.t(), non_neg_integer()) :: Ecto.Changeset.t()
  def schedule_changeset(shard_id, version),
    do: new(%{shard_id: shard_id, version: version}, schedule_in: @retention_seconds)

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"shard_id" => shard_id, "version" => version}}) do
    cond do
      # The version is LIVE again — a revert restored it after this drop was scheduled
      # (expert review #22). Its retained copy is the fleet's recovery point, not
      # garbage: dropping it would make the next revert 404 mid-sequence. Belt to the
      # RevertJob's cancel_retirement braces (this catches a drop already dequeued, or
      # one the cancel missed).
      version_live?(shard_id, version) ->
        {:cancel, :version_live}

      # A revert TO this version is in flight (expert review round-2 #17): the
      # directory still shows from_version until its cutover, so the live-guard above
      # passes — but the retained copy is that revert's restore source. RevertJob's
      # up-front cancel can't reach a retirement that already dequeued (executing is
      # uncancellable), so this side must check. Cancelled, not snoozed: if the revert
      # lands, the version is live (drop would be wrong); if it fails, the copy leaks
      # until the S3 lifecycle backstop — the recoverable direction.
      revert_in_flight?(shard_id, version) ->
        {:cancel, :revert_in_flight}

      true ->
        Storage.drop_version(shard_id, version)
    end
  end

  defp version_live?(shard_id, version) do
    match?({:ok, %{schema_version: ^version}}, Directory.get(shard_id))
  end

  defp revert_in_flight?(shard_id, version) do
    Fathom.Repo.exists?(
      from(j in Oban.Job,
        where: j.worker == "Fathom.Migrator.RevertJob",
        where: j.state in ["scheduled", "available", "executing", "retryable", "suspended"],
        where: fragment("?->>'shard_id' = ?", j.args, ^shard_id),
        where: fragment("(?->>'to_version')::bigint = ?", j.args, ^version)
      )
    )
  end
end
