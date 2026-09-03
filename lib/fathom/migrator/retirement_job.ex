defmodule Fathom.Migrator.RetirementJob do
  @moduledoc """
  Drops a shard's retained old version once the retention window has passed.
  Scheduled by `Fathom.Migrator.ShardMigrationJob` for `now + retention` after a
  cutover; an S3 lifecycle rule backstops any it misses.

  ## No `unique:`, deliberately (expert review 2026-08-26 #36)

  Every other per-shard worker — `ShardMigrationJob`, `RevertJob`, `DeleteJob`, `RevokeJob`,
  `HandoffJob` — declares `unique: [keys: [:shard_id], period: :infinity, states: [...]]`, and
  `test/fathom/oban_config_test.exs` enumerates them to prove the dedup survives Oban's 60 s
  default. This job is absent from that list on purpose, so the omission is not mistaken for the
  same bug those five were fixed for:

    * its work is a `drop_version` — idempotent, and an already-dropped object is a no-op; and
    * it is enqueued from the cutover transaction's outbox, not from a request path or a sweep,
      so there is no "two callers race to enqueue" shape for uniqueness to protect against.

  If it ever gains a second enqueue site, it needs the same `unique:` block AND a line in that
  test's worker list.
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

      # A revert for this shard is in flight (expert review round-2 #17; broadened 2026-08-31 #8):
      # the directory still shows from_version until its cutover, so the live-guard above passes —
      # but the retained copy is that revert's restore source. This matches ANY in-flight RevertJob
      # for the shard, NOT one whose to_version equals `version`: a chain-jumper (a cold-tail shard
      # that walked v5 -> v9 in one job) carries the fleet TARGET as to_version while its restore
      # SOURCE is a LOWER landing version read from retained_version — so a to_version-keyed guard
      # misses exactly the retirement that drops that source and strands the shard on a yanked
      # version above head (it can neither forward-migrate nor revert, and revert_status reports it
      # as landed). Over-matching an unrelated retirement is the SAFE direction: RevertJob's
      # up-front cancel can't reach a retirement that already dequeued (executing is uncancellable),
      # so this side must check. Cancelled, not snoozed: if the revert lands the version is live
      # (drop would be wrong); if it fails, or the retirement was unrelated, the copy leaks until
      # the S3 lifecycle backstop — the recoverable direction.
      revert_in_flight?(shard_id) ->
        {:cancel, :revert_in_flight}

      true ->
        drop_and_forget(shard_id, version)
    end
  end

  # The column has to stop claiming a copy this job just deleted (expert review 2026-08-24 #16b).
  # `shards.retained_version` is what a fleet revert consults to decide whether a shard can reach
  # the requested target, so a stale value is worse than a null one: it sends the revert at
  # `<shard>@<version>` with confidence, and the absent-object path it lands in is the very thing
  # the column exists to let us avoid.
  #
  # ONLY when it still names the version we dropped. A retain that ran between the guards above and
  # this line has already moved it on, and clearing unconditionally would erase a live recovery
  # point's record while the object itself survives — a revert that would have worked is then
  # refused. Storage first: if the drop fails the object is still there and the column is still
  # true, so the order is the one where a partial failure leaves a consistent pair.
  defp drop_and_forget(shard_id, version) do
    with :ok <- Storage.drop_version(shard_id, version) do
      _ = Directory.clear_retained_version(shard_id, version)
      :ok
    end
  end

  defp version_live?(shard_id, version) do
    match?({:ok, %{schema_version: ^version}}, Directory.get(shard_id))
  end

  defp revert_in_flight?(shard_id) do
    Fathom.Repo.exists?(
      from(j in Oban.Job,
        where: j.worker == "Fathom.Migrator.RevertJob",
        where: j.state in ["scheduled", "available", "executing", "retryable", "suspended"],
        where: fragment("?->>'shard_id' = ?", j.args, ^shard_id)
      )
    )
  end
end
