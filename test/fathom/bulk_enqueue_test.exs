defmodule Fathom.BulkEnqueueTest do
  @moduledoc """
  The partial UNIQUE index backing BulkEnqueue.unique/1 against the concurrent-caller race (expert
  review 2026-08-31 #26). The SELECT-then-insert_all is not atomic, so the guarantee is the DB
  index; Oban.insert_all merges on_conflict: :nothing, so a racing duplicate is silently skipped —
  and the index is SCOPED to the three (worker, shard_id)-unique workers so it never touches
  RetirementJob, which keeps multiple retained versions per shard.
  """
  use Fathom.DataCase, async: false
  use Oban.Testing, repo: Fathom.Repo

  import Ecto.Query

  alias Fathom.Migrator.{RetirementJob, ShardMigrationJob}
  alias Fathom.Repo

  defp live_count(worker, shard) do
    from(j in Oban.Job,
      where: j.worker == ^worker and fragment("?->>'shard_id'", j.args) == ^shard
    )
    |> Repo.aggregate(:count)
  end

  test "the unique index skips a racing duplicate the non-atomic SELECT could miss" do
    shard = "bulkuniq_#{System.unique_integer([:positive])}"

    # The race: two callers' SELECTs both saw the shard un-queued, so both insert_all. Oban's
    # insert_all merges on_conflict: :nothing, so the DB index lets only ONE through.
    assert [_job] = Oban.insert_all([ShardMigrationJob.new(%{shard_id: shard, target: 1})])

    assert [] == Oban.insert_all([ShardMigrationJob.new(%{shard_id: shard, target: 1})]),
           "the unique index must skip the racing duplicate insert"

    assert live_count("Fathom.Migrator.ShardMigrationJob", shard) == 1
  end

  # The index is SCOPED to the three (worker, shard_id)-unique BulkEnqueue workers. RetirementJob
  # shares a shard_id across DIFFERENT retained versions and must NOT be deduped by shard_id alone,
  # or retiring a shard's second version would be blocked.
  test "the index does NOT block RetirementJob's multiple versions per shard (scoping)" do
    shard = "bulkret_#{System.unique_integer([:positive])}"

    assert [_] = Oban.insert_all([RetirementJob.schedule_changeset(shard, 1)])

    assert [_] = Oban.insert_all([RetirementJob.schedule_changeset(shard, 2)]),
           "RetirementJob must allow two versions of one shard — the index is scoped out"

    assert live_count("Fathom.Migrator.RetirementJob", shard) == 2
  end
end
