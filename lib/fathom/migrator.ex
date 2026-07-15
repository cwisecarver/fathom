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

  @doc """
  The captured SQL statements for `version`, or `nil` if it isn't released — or if it
  was YANKED (expert review #12): a yanked version must never be applied again, so the
  replay machinery sees it as unknown and any straggler job targeting it errors out
  instead of re-applying the bad schema.
  """
  @spec statements(pos_integer()) :: [String.t()] | nil
  def statements(version) do
    case Repo.get_by(Release, version: version) do
      nil -> nil
      %{yanked: true} -> nil
      release -> release.statements
    end
  end

  @doc """
  The fleet HEAD: the highest released, non-yanked version, or 0 if none. Yanked
  releases are excluded (expert review #12) so a revert actually sticks — pre-fix
  `max(version)` never dropped, every reverted shard was immediately a laggard, and
  the hourly reconcile (or lazy migrate, within seconds) re-applied the reverted-from
  version.
  """
  @spec head() :: non_neg_integer()
  def head do
    Repo.aggregate(from(r in Release, where: not r.yanked), :max, :version) || 0
  end

  @doc """
  The next version number a new release may allocate: `max(version) + 1` INCLUDING
  yanked releases (expert review #10). A yanked version is a tombstone, not a free
  slot — `head/0` excludes yanked for rollout targeting, so allocating from
  `head() + 1` after a yank would collide on the unique version index forever,
  permanently wedging capture.
  """
  @spec next_version() :: pos_integer()
  def next_version do
    (Repo.aggregate(Release, :max, :version) || 0) + 1
  end

  @doc """
  Yanks `version`: drops it from HEAD, makes its statements unappliable, cancels any
  pending forward migration jobs targeting it, and refreshes this node's HeadCache
  (other nodes converge within the cache TTL). `Migrator.revert/3` yanks the
  from-version by default; call this directly to pull a bad release before any revert.
  """
  @spec yank(pos_integer()) :: :ok | {:error, :unknown_version}
  def yank(version) do
    case Repo.get_by(Release, version: version) do
      nil ->
        {:error, :unknown_version}

      release ->
        {:ok, _} = release |> Ecto.Changeset.change(yanked: true) |> Repo.update()

        # ALL live states, including executing/suspended (expert review round-2 #22):
        # an executing job already fetched its statements, so it would keep running
        # past the yank, fence, and cut the shard over to the yanked version AFTER
        # revert/3 read shards_at_version — stranding it (schema_version > head means
        # no laggard sweep ever sees it). Cancelling an executing job kills it, and
        # the migration aborts safely: the lease is released in the copy's `after`,
        # and no cutover has happened yet. The ReconcileJob's stranded sweep is the
        # belt for any job that completes in the cancel's race window.
        Oban.cancel_all_jobs(
          from(j in Job,
            where: j.worker == "Fathom.Migrator.ShardMigrationJob",
            where: j.state in @unique_states,
            where: fragment("(?->>'target')::bigint = ?", j.args, ^version)
          )
        )

        refresh_head_cache()
        :ok
    end
  end

  @doc """
  Enqueues reverts for active shards stranded ON a yanked version above HEAD
  (expert review round-2 #22): a migration that completed in the yank's race window
  cut its shard over to the yanked version AFTER the fleet revert read
  `shards_at_version`, and — being above HEAD — no laggard sweep ever converges it.
  Reverts go to the current HEAD (the version the fleet reverted to), non-forced, so
  the per-shard write-age guard still protects post-cutover writes. Shards at a
  yanked version BELOW head are ordinary laggards; the forward rollout handles them.
  Run from `ReconcileJob`; returns `{:ok, enqueued_count}`.
  """
  @spec revert_stranded() :: {:ok, non_neg_integer()}
  def revert_stranded do
    case head() do
      0 ->
        {:ok, 0}

      head ->
        yanked_above =
          Repo.all(from(r in Release, where: r.yanked and r.version > ^head, select: r.version))

        count =
          Enum.reduce(yanked_above, 0, fn version, acc ->
            {:ok, n} = revert(version, head, yank: false)
            acc + n
          end)

        {:ok, count}
    end
  end

  # Best-effort: the cache TTL-refreshes anyway; a down cache must not fail a yank.
  defp refresh_head_cache do
    _ = Fathom.Migrator.HeadCache.refresh()
    :ok
  catch
    :exit, _ -> :ok
  end

  @doc "All released versions, oldest first."
  @spec list() :: [Release.t()]
  def list, do: Repo.all(from r in Release, order_by: [asc: r.version])

  # --- fork-from-template (finding #10): new-tenant bootstrap at HEAD ---

  @default_template_drain_ms 5_000

  @doc """
  Retains a `template@HEAD` snapshot — the fork source for `fork_from_template/1`.
  Run after migrating the template (e.g. via `mix fathom.snapshot template-head`).

  Drains the template's coordinator first so its stored object is flushed +
  current (`{:error, :busy}` if it won't drain — e.g. a `manage.py migrate` session
  is still open; retry once it finishes), then refuses if a LIVE node still holds
  the template's lease (`{:error, {:held, owner}}` — the cross-node guard, same as
  `Fathom.Snapshots.restore/3`), then copies the template's live stored object to
  `<template>@<HEAD>` via the existing `Storage.retain/2`. Never snapshots a
  half-migrated template: an active migrate session holds a connection, so the
  drain refuses. Returns `{:ok, head}`, `{:error, :no_template}` (no
  `:template_shard_id` configured), or `{:error, :no_head}` (no released version).
  """
  @spec retain_template_head(pos_integer()) :: {:ok, pos_integer()} | {:error, term()}
  def retain_template_head(drain_timeout \\ @default_template_drain_ms) do
    with {:ok, template} <- template_shard_id() do
      case head() do
        0 ->
          {:error, :no_head}

        head ->
          case Fathom.Shards.drain(template, drain_timeout) do
            :ok ->
              case Fathom.Shard.Storage.lease_holder(template) do
                :free -> retain_snapshot(template, head)
                {:held, owner} -> {:error, {:held, owner}}
                {:error, reason} -> {:error, reason}
              end

            {:error, :busy} ->
              {:error, :busy}

            {:error, reason} ->
              {:error, {:drain_failed, reason}}
          end
      end
    end
  end

  defp retain_snapshot(template, head) do
    case Fathom.Shard.Storage.retain(template, head) do
      :ok -> {:ok, head}
      # No stored object for the template at all (never flushed): nothing to snapshot.
      {:error, :enoent} -> {:error, :no_template_object}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Births `dst_shard_id` AT the fleet HEAD by copying the retained `template@HEAD`
  snapshot into its live object and stamping the version places (finding #10) — see
  `Fathom.Migrator.ShardMigration.fork/4` for the mechanism. The admission path
  (`Fathom.Shards`, gated by `config :fathom, :fork_from_template`, off by default)
  calls this when minting a novel shard; on ANY non-`{:ok, _}` result the shard is
  simply born empty (today's behavior) — a checkout is never failed for this.

  Returns `{:ok, %{version: head}}`, or `{:error, :no_template_snapshot}` when no
  version is released (HEAD 0), no `:template_shard_id` is configured, or no
  `template@HEAD` snapshot object exists; `{:error, :template_shard}` refuses
  forking the template onto itself; other errors/`{:retry, _}` pass through.
  """
  @spec fork_from_template(String.t()) :: {:ok, map()} | {:retry, term()} | {:error, term()}
  def fork_from_template(dst_shard_id) do
    case template_shard_id() do
      {:ok, template} when template == dst_shard_id ->
        {:error, :template_shard}

      {:ok, template} ->
        case head() do
          0 -> {:error, :no_template_snapshot}
          head -> Fathom.Migrator.ShardMigration.fork(dst_shard_id, template, head)
        end

      {:error, :no_template} ->
        {:error, :no_template_snapshot}
    end
  end

  defp template_shard_id do
    case Fathom.ShardId.cast(Application.get_env(:fathom, :template_shard_id)) do
      {:ok, id} -> {:ok, id}
      :error -> {:error, :no_template}
    end
  end

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

  Pass `force: true` to override the per-shard write-age guard: without it, any shard
  the directory shows active since its cutover refuses the revert (its job cancels)
  rather than silently discarding post-cutover writes — see
  `Fathom.Migrator.ShardMigration.revert/4` (finding #13).

  Yanks `from_version` first (expert review #12) so HEAD drops and the reconcile
  sweep / lazy migrate cannot re-apply the version being reverted away from. Pass
  `yank: false` to keep the release live (rare — e.g. reverting a few canary shards
  while the rollout continues).
  """
  @enqueue_chunk 5_000

  @spec revert(non_neg_integer(), non_neg_integer(), keyword()) :: {:ok, non_neg_integer()}
  def revert(from_version, to_version, opts \\ []) do
    force? = Keyword.get(opts, :force, false)
    if Keyword.get(opts, :yank, true), do: yank(from_version)
    shards = Directory.shards_at_version(from_version)

    # Expert review #23: the per-shard dedup below ignores `force`, so in the intended
    # operator flow — non-force sweep, guard cancels some shards, re-issue with
    # force: true — any shard whose first RevertJob was still in flight (snoozing on
    # :shard_busy / {:held, _}) was silently dropped from the force sweep; the surviving
    # non-force job then hit the guard and cancelled, so the shard was never reverted
    # despite the explicit force. Upgrade in-flight jobs' args instead of skipping
    # them; they count toward the returned total. Round-2 #21 tightened this to the
    # WHOLE operation: upgrading only `force` while a snoozing job targeted a
    # different to_version force-reverted the shard (a destructive discard) to the
    # WRONG version — so the retarget sets to_version too (last operator command wins).
    forced =
      if force?,
        do: retarget_inflight_reverts(Enum.map(shards, & &1.shard_id), to_version),
        else: 0

    enqueued =
      shards
      |> Enum.map(
        &{&1.shard_id,
         RevertJob.new(%{shard_id: &1.shard_id, to_version: to_version, force: force?})}
      )
      |> enqueue_unique()

    {:ok, enqueued + forced}
  end

  @doc """
  Un-quarantines every `migration_failed` shard and re-enqueues its migration to the
  current HEAD (expert review #25) — the operator's "the cause is fixed, converge the
  frozen slice" API. Returns `{:ok, enqueued_count}` (0 when nothing is quarantined
  or no version is released).
  """
  @spec retry_failed() :: {:ok, non_neg_integer()}
  def retry_failed do
    failed = Directory.failed_shards()
    _ = Directory.requeue_failed(Enum.map(failed, & &1.shard_id))

    case {failed, head()} do
      {[], _} ->
        {:ok, 0}

      {_, 0} ->
        {:ok, 0}

      {failed, head} ->
        count =
          failed
          |> Enum.filter(&(&1.schema_version < head))
          |> Enum.map(
            &{&1.shard_id, ShardMigrationJob.new(%{shard_id: &1.shard_id, target: head})}
          )
          |> enqueue_unique()

        {:ok, count}
    end
  end

  @doc """
  Whether a fleet revert away from `from_version` has completed, and what's left:
  `%{remaining, in_flight, failed}` — shards still active at the version, revert jobs
  still in flight for them, and quarantined shards fleet-wide. Before this the only
  way to answer "did the revert land?" was trawling `oban_jobs` for discarded rows
  (expert review #24).
  """
  @spec revert_status(non_neg_integer()) :: %{
          remaining: non_neg_integer(),
          in_flight: non_neg_integer(),
          failed: non_neg_integer()
        }
  def revert_status(from_version) do
    remaining = Directory.shards_at_version(from_version)

    in_flight =
      case Enum.map(remaining, & &1.shard_id) do
        [] ->
          0

        ids ->
          ids
          |> Enum.chunk_every(@enqueue_chunk)
          |> Enum.reduce(0, fn chunk, acc ->
            acc +
              Repo.aggregate(
                from(j in Job,
                  where: j.worker == "Fathom.Migrator.RevertJob",
                  where: j.state in @unique_states,
                  where: fragment("?->>'shard_id'", j.args) in ^chunk
                ),
                :count
              )
          end)
      end

    %{remaining: length(remaining), in_flight: in_flight, failed: Directory.count_failed()}
  end

  # Rewrite in-flight revert jobs to THIS force sweep's operation — force: true AND
  # its to_version (round-2 #21; see the call site). An EXECUTING job's deserialized
  # args can't be changed here, but a guard refusal re-checks its row before going
  # terminal (RevertJob), so the upgrade still lands.
  defp retarget_inflight_reverts(shard_ids, to_version) do
    # type/2 so the patch binds as a jsonb OBJECT — a plain/pre-encoded binding goes
    # over as a jsonb string scalar, and `object || scalar` builds a 2-element array
    # instead of merging.
    patch = %{"force" => true, "to_version" => to_version}

    shard_ids
    |> Enum.chunk_every(@enqueue_chunk)
    |> Enum.reduce(0, fn chunk, acc ->
      {n, _} =
        from(j in Job,
          where: j.worker == "Fathom.Migrator.RevertJob",
          where: j.state in @unique_states,
          where: fragment("?->>'shard_id'", j.args) in ^chunk,
          where:
            fragment("(?->>'force')::boolean IS DISTINCT FROM true", j.args) or
              fragment("(?->>'to_version')::bigint IS DISTINCT FROM ?", j.args, ^to_version),
          update: [set: [args: fragment("? || ?", j.args, type(^patch, :map))]]
        )
        |> Repo.update_all([])

      acc + n
    end)
  end

  # Bulk-enqueue a fleet sweep in batched round-trips instead of one Oban.insert per
  # shard (a fleet-wide rollout was N serialized inserts). Takes `{shard_id, changeset}`
  # pairs. insert_all/1 skips the workers' `unique` config, so we first drop shards that
  # already have an in-flight job for this worker (preserving per-shard uniqueness against
  # the lazy path, earlier sweeps, and the hourly reconcile), then insert the rest. A shard
  # slipping in between the check and the insert only costs a redundant idempotent job.
  #
  # CHUNKED because Postgres's wire protocol caps a statement at 65,535 bind parameters:
  # one unpartitioned Oban.insert_all crashed past ~7,281 jobs (9 params each), which a
  # fleet revert (unbounded — every shard at a version) or a big rollout limit hits at
  # scale (found by scripts/directory_scale.exs at 3.1M directory rows). 5,000 pairs per
  # chunk keeps both statements comfortably under the cap (dedup: 1 param/id; insert:
  # 9 params/job = 45,000).

  defp enqueue_unique([]), do: 0

  defp enqueue_unique(id_changesets) do
    id_changesets
    |> Enum.chunk_every(@enqueue_chunk)
    |> Enum.reduce(0, fn chunk, acc -> acc + enqueue_unique_chunk(chunk) end)
  end

  defp enqueue_unique_chunk(id_changesets) do
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
