defmodule Fathom.Migrator do
  @moduledoc """
  The shard-schema migration control plane.

  This module currently owns the **migration registry**: the record of released
  schema versions and the fleet HEAD. `Fathom.Directory.laggards/2` and
  `count_laggards/1` take that HEAD to find shards still behind it.

  The rollout engine that acts on laggards — `release` driving a sweep,
  per-shard blue/green copy jobs (Oban, unique per shard, retry → quarantine),
  reconcile cron, and revert — builds on this and on `Fathom.Directory`. That
  engine and its blue/green copy mechanism are the next slice (they involve
  versioned shard storage and draining the live `Fathom.Shard` coordinator).
  """
  import Ecto.Query

  alias Fathom.Directory
  alias Fathom.Migrator.{Release, RevertJob, ShardMigrationJob}
  alias Fathom.Repo
  alias Oban.Job

  # The in-flight states a worker's `unique` config dedups against (see ShardMigrationJob /
  # RevertJob). The basic Oban engine's insert_all/1 does NOT honor :unique, so the bulk
  # sweeps below filter candidates against jobs already in these states before inserting.
  @unique_states ~w(scheduled available executing retryable suspended)

  @doc """
  Records a released shard-schema `version` (HEAD becomes its max), carrying the
  captured SQL `statements` the rollout replays per shard.
  """
  @spec release(pos_integer(), String.t(), [String.t()]) ::
          {:ok, Release.t()} | {:error, Ecto.Changeset.t()}
  def release(version, name, statements \\ []) do
    %Release{}
    |> Release.changeset(%{version: version, name: name, statements: statements})
    |> Repo.insert()
  end

  @doc "The captured SQL statements for `version`, or `nil` if it isn't released."
  @spec statements(pos_integer()) :: [String.t()] | nil
  def statements(version) do
    case Repo.get_by(Release, version: version) do
      nil -> nil
      release -> release.statements
    end
  end

  @doc "The fleet HEAD: the highest released version, or 0 if none are released."
  @spec head() :: non_neg_integer()
  def head, do: Repo.aggregate(Release, :max, :version) || 0

  @doc "All released versions, oldest first."
  @spec list() :: [Release.t()]
  def list, do: Repo.all(from r in Release, order_by: [asc: r.version])

  @doc "Enqueues a per-shard migration job to bring `shard_id` to `target`."
  @spec enqueue_migration(String.t(), pos_integer()) ::
          {:ok, Oban.Job.t()} | {:error, Ecto.Changeset.t()}
  def enqueue_migration(shard_id, target) do
    %{shard_id: shard_id, target: target}
    |> ShardMigrationJob.new()
    |> Oban.insert()
  end

  @doc """
  Enqueues a migration job for up to `limit` shards still behind HEAD, hottest
  first (the rollout sweep). Per-shard uniqueness de-dups against the lazy path and
  earlier sweeps. Returns `{:ok, enqueued_count}`.
  """
  @spec rollout(pos_integer()) :: {:ok, non_neg_integer()}
  def rollout(limit \\ 100) do
    case head() do
      0 ->
        {:ok, 0}

      head ->
        count =
          head
          |> Directory.laggards(limit)
          |> Enum.map(
            &{&1.shard_id, ShardMigrationJob.new(%{shard_id: &1.shard_id, target: head})}
          )
          |> enqueue_unique()

        {:ok, count}
    end
  end

  @doc """
  Enqueues a revert job for every active shard at `from_version`, flipping them back
  to `to_version` (a pointer flip restoring the retained copy). Returns
  `{:ok, enqueued_count}`.
  """
  @spec revert(non_neg_integer(), non_neg_integer()) :: {:ok, non_neg_integer()}
  def revert(from_version, to_version) do
    count =
      from_version
      |> Directory.shards_at_version()
      |> Enum.map(&{&1.shard_id, RevertJob.new(%{shard_id: &1.shard_id, to_version: to_version})})
      |> enqueue_unique()

    {:ok, count}
  end

  # Bulk-enqueue a fleet sweep in one round-trip instead of one Oban.insert per shard
  # (a fleet-wide rollout was N serialized inserts). Takes `{shard_id, changeset}` pairs.
  # insert_all/1 skips the workers' `unique` config, so we first drop shards that already
  # have an in-flight job for this worker (preserving per-shard uniqueness against the lazy
  # path, earlier sweeps, and the hourly reconcile), then insert the rest at once. A shard
  # slipping in between the check and the insert only costs a redundant idempotent job.
  defp enqueue_unique([]), do: 0

  defp enqueue_unique(id_changesets) do
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
