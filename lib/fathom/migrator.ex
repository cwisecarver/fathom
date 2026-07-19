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
  require Logger

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
  captured SQL `statements` the rollout replays per shard. `template_migration_count`
  (expert review #32) is the template's django_migrations count at capture time, used by
  `template_drift/0`; `nil` when unknown (a hand-authored release).
  """
  @spec release(pos_integer(), String.t(), [String.t()], non_neg_integer() | nil, boolean()) ::
          {:ok, Release.t()} | {:error, Ecto.Changeset.t()}
  def release(
        version,
        name,
        statements \\ [],
        template_migration_count \\ nil,
        requires_review \\ false
      ) do
    %Release{}
    |> Release.changeset(%{
      version: version,
      name: name,
      statements: statements,
      template_migration_count: template_migration_count,
      requires_review: requires_review
    })
    |> Repo.insert()
  end

  @doc """
  The captured SQL statements for `version`, or `nil` if it isn't released — or if it is
  gated: YANKED (expert review #12) or flagged REQUIRES_REVIEW (expert review 2026-07-18 #10).

  Both are structural gates on the replay path. A yanked version must never be applied again;
  a `requires_review` version (a captured data-migration held as a fleet-corruption risk) must
  not be replayed until an operator clears the flag (`approve_review/1`). `head/0` already
  ceilings the *automated* rollout below the lowest flagged version, but a DIRECT
  `ShardMigration.run/enqueue_migration` takes an explicit target and would otherwise replay the
  flagged DML at/above the floor. Returning `nil` here makes `ShardMigration.statement_chain/2`
  treat the version as unavailable, so that direct path errors (`{:unknown_version, v}`) with the
  shard left untouched — the same fail-closed behavior yanked already gets.
  """
  @spec statements(pos_integer()) :: [String.t()] | nil
  def statements(version) do
    case Repo.get_by(Release, version: version) do
      nil -> nil
      %{yanked: true} -> nil
      %{requires_review: true} -> nil
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
    # Expert review #1: a `requires_review` version (a captured data-migration flagged as a
    # fleet-wide-corruption risk) is a CEILING — the rollout must not advance to it or past it until
    # an operator reviews and clears the flag (`approve_review/1`). So HEAD is the highest non-yanked
    # version strictly below the lowest un-reviewed version. Respects the linear graph (no skipping):
    # rollout proceeds up to just before the first flagged version.
    review_floor =
      Repo.aggregate(
        from(r in Release, where: not r.yanked and r.requires_review),
        :min,
        :version
      )

    query = from(r in Release, where: not r.yanked)
    query = if review_floor, do: from(r in query, where: r.version < ^review_floor), else: query

    Repo.aggregate(query, :max, :version) || 0
  end

  @doc """
  Releases flagged `requires_review` (expert review #1) — captured versions whose buffer contained
  template-literal data migrations, held below HEAD until reviewed. Oldest first.
  """
  @spec pending_review() :: [Release.t()]
  def pending_review do
    Repo.all(
      from(r in Release, where: r.requires_review and not r.yanked, order_by: [asc: r.version])
    )
  end

  @doc """
  Clears the `requires_review` flag on `version` (expert review #1) after an operator has confirmed
  the captured data migration is safe to replay fleet-wide — HEAD then advances (up to the next
  flagged version, if any) and the rollout proceeds. Refreshes this node's HeadCache.
  """
  @spec approve_review(pos_integer()) :: :ok | {:error, :unknown_version}
  def approve_review(version) do
    case Repo.get_by(Release, version: version) do
      nil ->
        {:error, :unknown_version}

      release ->
        {:ok, _} = release |> Ecto.Changeset.change(requires_review: false) |> Repo.update()
        refresh_head_cache()
        :ok
    end
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

        # Expert review #32: a yank leaves the template ahead of the fleet (it still has the yanked
        # migration applied) — warn the operator to backwards-migrate it before the next release.
        check_template_drift()
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

  @doc """
  The template's `django_migrations` count recorded by the most recently captured version, or `nil`
  if nothing is captured yet or the latest release predates the count (expert review #6). This is
  the DURABLE baseline the non-atomic-gap check compares a new capture's pre-transaction count
  against — no in-memory Capture state (the shared-singleton false-alarm the earlier attempt hit).
  """
  @spec last_template_count() :: non_neg_integer() | nil
  def last_template_count do
    Repo.one(
      from(r in Release,
        order_by: [desc: r.version],
        limit: 1,
        select: r.template_migration_count
      )
    )
  end

  @doc """
  The fleet convergence snapshot (expert review #25) — the deploy gate a Django CI/CD reads before
  shipping app code that depends on HEAD. `converged` (laggards == 0) is the "safe to deploy the new
  app version" signal; `failed` surfaces quarantined shards to triage; `pending_review` lists
  versions held below HEAD awaiting operator sign-off (#1/#6). Exposed over HTTP at
  `GET /api/migrations/status`.
  """
  @spec status() :: %{
          head: non_neg_integer(),
          laggards: non_neg_integer(),
          failed: non_neg_integer(),
          converged: boolean(),
          pending_review: [pos_integer()]
        }
  def status do
    head = head()
    laggards = Directory.count_laggards(head)

    %{
      head: head,
      laggards: laggards,
      failed: Directory.count_failed(),
      converged: laggards == 0,
      pending_review: Enum.map(pending_review(), & &1.version)
    }
  end

  @doc """
  Post-revert template drift check (expert review #32). After a fleet revert yanks version vN and
  flips HEAD to vN-1, the **template** shard still has vN's Django migration applied in its
  `django_migrations` and its schema — Django's migration graph is linear, so the next
  `makemigrations` builds on vN, and the next captured version assumes schema the fleet reverted
  away from → fleet-wide replay failure. The operator must backwards-migrate the template first
  (see `docs/migration.md`).

  Detected from stored counts alone (no template I/O, no SQL parsing): the highest **yanked** release
  above HEAD is where a fresh revert leaves the template, so if that version's captured template
  `django_migrations` count exceeds HEAD's, the template is ahead of the live fleet. Returns
  `:aligned`, `{:drift, details}`, or `:unknown` (the relevant releases predate
  `template_migration_count`).
  """
  @spec template_drift() :: :aligned | :unknown | {:drift, map()}
  def template_drift do
    case head() do
      0 ->
        :aligned

      head_version ->
        head_release = Repo.get_by(Release, version: head_version)

        yanked_above =
          Repo.one(
            from(r in Release,
              where: r.yanked and r.version > ^head_version,
              order_by: [desc: r.version],
              limit: 1
            )
          )

        cond do
          is_nil(yanked_above) ->
            :aligned

          is_nil(head_release) or is_nil(head_release.template_migration_count) or
              is_nil(yanked_above.template_migration_count) ->
            :unknown

          yanked_above.template_migration_count > head_release.template_migration_count ->
            {:drift,
             %{
               template_version: yanked_above.version,
               template_migration_count: yanked_above.template_migration_count,
               head_version: head_version,
               head_migration_count: head_release.template_migration_count
             }}

          true ->
            :aligned
        end
    end
  end

  @doc """
  Runs `template_drift/0` and, on `{:drift, _}`, emits `[:fathom, :migrator, :template_drift]`
  telemetry + a loud `Logger.error` so a post-revert wedge is alertable, not discovered at the next
  fleet-wide `makemigrations`. Called once from `yank/1` (the revert moment); also safe to run
  manually. Returns the drift result.
  """
  @spec check_template_drift() :: :aligned | :unknown | {:drift, map()}
  def check_template_drift do
    case template_drift() do
      {:drift, d} = drift ->
        :telemetry.execute([:fathom, :migrator, :template_drift], %{count: 1}, d)

        Logger.error(
          "template migration drift after revert: the template still has v#{d.template_version} " <>
            "applied (django_migrations count #{d.template_migration_count}) but fleet HEAD is " <>
            "v#{d.head_version} (count #{d.head_migration_count}). Backwards-migrate the template " <>
            "(`manage.py migrate <app> <prev>`) before the next makemigrations, or the next captured " <>
            "version assumes reverted-away schema and fails fleet-wide. See docs/migration.md."
        )

        drift

      other ->
        other
    end
  end

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

    # Keyset-stream the shard_ids in pages instead of materializing every shard at the version as a
    # full struct (expert review 2026-07-18 #12 — a fleet revert loaded millions of rows into
    # memory). Process each page independently so a page's jobs commit and start reverting before
    # the next page is fetched (the emergency path wants shards reverting ASAP, not after a
    # full-fleet scan). Per page: upgrade in-flight jobs (force), then enqueue the rest.
    total =
      Directory.stream_ids_at_version(from_version, @enqueue_chunk)
      |> Stream.chunk_every(@enqueue_chunk)
      |> Enum.reduce(0, fn ids, acc ->
        # Expert review #23: the per-shard dedup in enqueue_unique ignores `force`, so in the
        # intended operator flow — non-force sweep, guard cancels some shards, re-issue with
        # force: true — any shard whose first RevertJob was still in flight (snoozing on
        # :shard_busy / {:held, _}) was silently dropped from the force sweep; the surviving
        # non-force job then hit the guard and cancelled, so the shard was never reverted despite
        # the explicit force. Upgrade in-flight jobs' args instead. Round-2 #21 tightened this to
        # the WHOLE operation: upgrading only `force` while a snoozing job targeted a different
        # to_version force-reverted the shard (a destructive discard) to the WRONG version — so the
        # retarget sets to_version too (last operator command wins).
        forced = if force?, do: retarget_inflight_reverts(ids, to_version), else: 0

        enqueued =
          ids
          |> Enum.map(
            &{&1, RevertJob.new(%{shard_id: &1, to_version: to_version, force: force?})}
          )
          |> enqueue_unique()

        acc + enqueued + forced
      end)

    {:ok, total}
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
    # The remaining count is an aggregate (#12) — never materialize the (millions-large) set just to
    # length/1 it. The in-flight count streams the ids in keyset pages and counts revert jobs per
    # chunk, so memory stays bounded regardless of fleet size.
    remaining = Directory.count_at_version(from_version)

    in_flight =
      Directory.stream_ids_at_version(from_version, @enqueue_chunk)
      |> Stream.chunk_every(@enqueue_chunk)
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

    %{remaining: remaining, in_flight: in_flight, failed: Directory.count_failed()}
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
