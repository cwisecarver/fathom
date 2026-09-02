defmodule Fathom.BulkEnqueue do
  @moduledoc """
  Chunked, deduplicated bulk `Oban` insert for per-shard jobs.

  Lifted out of `Fathom.Migrator` for expert review 2026-08-26 #31, which observed that
  `RetirementJob`, `RevokeJob` and `DeleteJob` all want it and that the fleet-wide token revoke had
  reimplemented the shape it replaces — `Enum.each` over `Oban.insert/1`, N serialized round trips,
  each its own transaction.

  ## The two things this exists to get right

  **`Oban.insert_all/1` does NOT honour a worker's `unique:` config.** The basic engine applies
  uniqueness only on `insert/1`, so a bulk insert of jobs a worker declares unique will happily
  create duplicates. That is why `unique/1` runs its own explicit in-flight query against
  `@unique_states` before inserting, rather than trusting the worker's declaration. For the token
  revoke this is not cosmetic: a double revoke is not corruption (the version floor bump is
  idempotent) but it costs a second round of live-client disconnects, which is precisely what
  `Fathom.HranaAuth.RevokeJob`'s own moduledoc says it is avoiding.

  The SELECT-then-`insert_all` is not atomic, so it is a fast-path filter, not the guarantee (expert
  review 2026-08-31 #26): two concurrent callers could each see a shard un-queued and both insert.
  The guarantee is a partial UNIQUE index on `(worker, (args->>'shard_id'))` over the live states,
  SCOPED to exactly the three workers this is used with — `ShardMigrationJob`, `RevertJob`,
  `RevokeJob` (migration `*_add_bulk_shard_unique_index_to_oban_jobs`). `Oban.insert_all` merges
  `on_conflict: :nothing`, so a racing duplicate is silently skipped by the DB and the returned count
  stays accurate. The index is deliberately NOT global: `RetirementJob` shares a shard_id across
  different retained VERSIONS, so a global index would block retiring a shard's second version — a
  new caller keyed by `(worker, shard_id)` must be added to that index's predicate, one keyed by
  anything else must not.

  **Postgres caps a statement at 65 535 bind parameters**, and `insert_all` emits one parameter per
  column per ROW — unlike a `WHERE ... IN`, which Ecto compiles to a single array parameter (see
  `Fathom.Directory.requeue_failed/1`, where that distinction was measured and the two were found
  to have been conflated). At ~9 params per job the ceiling is ~7 281 jobs, which
  `scripts/directory_scale.exs` hit for real at 3.1M rows. `@chunk` sits below it.

  ## What it does not do

  It takes `{shard_id, changeset}` pairs rather than bare changesets, because the dedup query has
  to know which shard each job is for and reading it back out of `changeset.args` would couple this
  to every worker's arg shape. Callers already have the id.
  """
  import Ecto.Query

  alias Fathom.Repo
  alias Oban.Job

  # The in-flight states a worker's `unique` config dedups against. Kept here rather than imported
  # from a worker so the dedup and the chunking travel together.
  @unique_states ~w(scheduled available executing retryable suspended)

  # 5 000 jobs x ~9 params = 45 000, under Postgres's 65 535 bind-parameter cap.
  @chunk 5_000

  @doc """
  Inserts one job per `{shard_id, changeset}` pair, skipping any shard that already has a job for
  the same worker in an in-flight state. Returns how many were actually inserted.

  Chunked at #{@chunk}; every chunk re-runs the dedup query, so a job enqueued by someone else
  between chunks is still respected.
  """
  @spec unique([{String.t(), Ecto.Changeset.t()}]) :: non_neg_integer()
  def unique([]), do: 0

  def unique(id_changesets) do
    id_changesets
    |> Enum.chunk_every(@chunk)
    |> Enum.reduce(0, fn chunk, acc -> acc + unique_chunk(chunk) end)
  end

  @doc "The in-flight job states bulk dedup filters against."
  @spec unique_states() :: [String.t()]
  def unique_states, do: @unique_states

  defp unique_chunk(id_changesets) do
    shard_ids = Enum.map(id_changesets, &elem(&1, 0))
    worker = id_changesets |> hd() |> elem(1) |> Ecto.Changeset.get_field(:worker)

    already_queued =
      from(j in Job,
        where:
          j.worker == ^worker and j.state in @unique_states and
            fragment("?->>'shard_id'", j.args) in ^shard_ids,
        select: fragment("?->>'shard_id'", j.args)
      )
      |> Repo.all()
      |> MapSet.new()

    changesets =
      for {shard_id, changeset} <- id_changesets,
          not MapSet.member?(already_queued, shard_id),
          do: changeset

    case changesets do
      [] -> 0
      cs -> cs |> Oban.insert_all() |> length()
    end
  end
end
