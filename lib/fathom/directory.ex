defmodule Fathom.Directory do
  @moduledoc """
  The shard **directory** / control plane: the Postgres record of every shard's
  schema version and lifecycle state. This is the source of truth the
  rollout/migration machinery reads and flips, and the resolve hook the lazy
  migration path hangs on.

  It is deliberately decoupled from the data path: `resolve/1` records that a
  shard is in use (registering it on first sight and touching `last_active_at`),
  but a shard keeps serving through `Fathom.Shards` whether or not Postgres is
  reachable. Hot-path callers should use `touch/1`, which is best-effort and never
  raises.

  The migration *engine* (`Fathom.Migrator`, Oban jobs, blue/green copy) and the
  resolve-driven lazy/sweep rollout build on these operations; they are not here
  yet — this module is the directory itself.

  The data path no longer writes here synchronously: per-checkout accesses are
  coalesced and batch-flushed by `Fathom.Directory.Recorder` (see `record_batch/1`),
  so a checkout never blocks on Postgres.
  """
  import Ecto.Query

  alias Fathom.Directory.Shard
  alias Fathom.Repo

  # Postgres bind-parameter ceiling is ~65535; 6 fields/row keeps a chunk well
  # under it and bounds each statement's size.
  @batch_chunk 1_000

  # How long a shard may sit in `migrating` before the reconcile sweep assumes its
  # migration job was lost and reclaims it (see reclaim_stale_migrating/1). Generous —
  # a copy is seconds-to-minutes — and just above the hourly reconcile cadence, so a
  # genuinely in-flight migration is never reclaimed out from under itself.
  @default_migration_stale_seconds 3_600

  @doc """
  Resolves a shard, registering it on first use and recording the access. Returns
  `{:ok, entry}` with the shard's current `schema_version`/`status` (or
  `{:error, changeset}` for an invalid id). This is the hook the lazy migration
  path will use to spot and enqueue laggards.
  """
  @spec resolve(String.t()) :: {:ok, Shard.t()} | {:error, Ecto.Changeset.t()}
  def resolve(shard_id) do
    now = DateTime.utc_now()

    %Shard{}
    |> Shard.changeset(%{
      shard_id: shard_id,
      schema_version: 0,
      status: "active",
      last_active_at: now
    })
    |> Repo.insert(
      # On re-resolve, only bump recency — never reset version/status.
      on_conflict: [set: [last_active_at: now, updated_at: now]],
      conflict_target: :shard_id,
      returning: true
    )
  end

  @doc """
  Batch-upserts buffered shard accesses — the data path's deferred `resolve/1`.
  `entries` is a list of `{shard_id, last_active_at}`; repeated accesses to a shard
  are expected to be coalesced upstream (see `Fathom.Directory.Recorder`). Like
  `resolve/1`, a first sight registers the shard (`schema_version: 0`, `active`)
  and a re-sight only bumps recency — never resets version/status. Returns the
  number of rows written. Raises on a Postgres error; the caller (the recorder)
  treats flushing as best-effort.

  Shard ids reaching here already passed `Fathom.Shards`' id validation at
  checkout, and `insert_all` parameterizes every value, so this is injection-safe
  even though it bypasses changeset validation.
  """
  @spec record_batch([{String.t(), DateTime.t()}]) :: non_neg_integer()
  def record_batch([]), do: 0

  def record_batch(entries) do
    now = DateTime.utc_now()

    entries
    |> Enum.chunk_every(@batch_chunk)
    |> Enum.reduce(0, fn chunk, acc ->
      rows =
        Enum.map(chunk, fn {shard_id, last_active_at} ->
          %{
            shard_id: shard_id,
            schema_version: 0,
            status: "active",
            last_active_at: last_active_at,
            inserted_at: now,
            updated_at: now
          }
        end)

      {count, _} =
        Repo.insert_all(Shard, rows,
          # GREATEST, not a plain replace: touches are coalesced and flushed later, and two
          # nodes serving the same shard across a remap can flush out of order, so an
          # unconditional replace could rewind last_active_at with a stale stamp — corrupting
          # the recency heuristics (warm-follower target set, laggard ordering). Keep the
          # newer of incoming vs stored; updated_at (bookkeeping) always advances.
          on_conflict:
            from(s in Shard,
              update: [
                set: [
                  last_active_at:
                    fragment("GREATEST(EXCLUDED.last_active_at, ?)", s.last_active_at),
                  updated_at: fragment("EXCLUDED.updated_at")
                ]
              ]
            ),
          conflict_target: :shard_id
        )

      acc + count
    end)
  end

  @doc "Reads a shard's directory entry without recording an access."
  @spec get(String.t()) :: {:ok, Shard.t()} | :error
  def get(shard_id) do
    case Repo.get_by(Shard, shard_id: shard_id) do
      nil -> :error
      shard -> {:ok, shard}
    end
  end

  @doc """
  Cuts a shard over to `schema_version` and marks it `active` — the atomic flip a
  completed migration (or revert) performs.

  `cutover_at` and `last_active_at` are stamped with the SAME instant, so
  immediately after a cutover the shard reads as "no activity since cutover" —
  the revert force-guard (`Fathom.Migrator.ShardMigration.revert/4`) detects
  post-cutover activity as strictly `last_active_at > cutover_at` (finding #13).
  """
  @spec cutover(String.t(), non_neg_integer()) ::
          {:ok, Shard.t()} | {:error, :not_found | Ecto.Changeset.t()}
  def cutover(shard_id, schema_version) do
    now = DateTime.utc_now()

    update_shard(shard_id, %{
      schema_version: schema_version,
      status: "active",
      last_active_at: now,
      cutover_at: now,
      migrating_since: nil
    })
  end

  @doc "Marks a shard as mid-migration (the app pauses writes for the copy window)."
  @spec mark_migrating(String.t()) :: {:ok, Shard.t()} | {:error, :not_found | Ecto.Changeset.t()}
  def mark_migrating(shard_id),
    do: update_shard(shard_id, %{status: "migrating", migrating_since: DateTime.utc_now()})

  @doc "Quarantines a shard whose migration exhausted its retries."
  @spec mark_failed(String.t()) :: {:ok, Shard.t()} | {:error, :not_found | Ecto.Changeset.t()}
  def mark_failed(shard_id),
    do: update_shard(shard_id, %{status: "migration_failed", migrating_since: nil})

  @doc """
  Reclaims shards stuck in `migrating` past `stale_after_seconds` back to `active`, and
  returns their ids. A migration whose Oban job is lost never leaves `migrating`, and every
  laggard/reconcile query filters `status == "active"`, so without this it is invisible to
  every sweep forever — its data never converges to HEAD. Flipping it back to `active` makes
  the next rollout re-enqueue and retry it (the migration copy is idempotent). Called from
  the hourly reconcile; a nil `migrating_since` (a pre-`migrating_since`-migration in-flight
  row) is left alone and self-corrects on the next mark_migrating.
  """
  @spec reclaim_stale_migrating(pos_integer() | nil) :: [String.t()]
  def reclaim_stale_migrating(stale_after_seconds \\ nil) do
    seconds =
      stale_after_seconds ||
        Application.get_env(
          :fathom,
          :migration_stale_after_seconds,
          @default_migration_stale_seconds
        )

    now = DateTime.utc_now()
    cutoff = DateTime.add(now, -seconds, :second)

    {_count, ids} =
      Repo.update_all(
        from(s in Shard,
          where:
            s.status == "migrating" and not is_nil(s.migrating_since) and
              s.migrating_since < ^cutoff,
          select: s.shard_id
        ),
        set: [status: "active", migrating_since: nil, updated_at: now]
      )

    ids
  end

  @doc """
  Retires the old shard after cutover, keeping it until `retain_until` so a revert
  is a pointer flip within the window.
  """
  @spec retire(String.t(), DateTime.t()) ::
          {:ok, Shard.t()} | {:error, :not_found | Ecto.Changeset.t()}
  def retire(shard_id, retain_until) do
    update_shard(shard_id, %{status: "retired", retain_until: retain_until})
  end

  @doc """
  The rollout sweep cursor: active shards behind `head_version`, most-recently-used
  first (hot shards migrate first), capped at `limit`.
  """
  @spec laggards(non_neg_integer(), pos_integer()) :: [Shard.t()]
  def laggards(head_version, limit) do
    laggard_query(head_version)
    |> order_by([s], desc: s.last_active_at)
    |> limit(^limit)
    |> Repo.all()
  end

  @doc "How many active shards are still behind `head_version` (the reconcile gauge)."
  @spec count_laggards(non_neg_integer()) :: non_neg_integer()
  def count_laggards(head_version) do
    laggard_query(head_version) |> Repo.aggregate(:count)
  end

  @doc "Active shards currently at `version` — the set a fleet revert flips back."
  @spec shards_at_version(non_neg_integer()) :: [Shard.t()]
  def shards_at_version(version) do
    Repo.all(from s in Shard, where: s.schema_version == ^version and s.status == "active")
  end

  @doc """
  How many shards are quarantined (`migration_failed` — set when a forward migration
  or a revert exhausts its attempts). A gauge next to `count_laggards/1` so quarantine
  growth is observable instead of silently accumulating (expert review #24).
  """
  @spec count_failed() :: non_neg_integer()
  def count_failed do
    Repo.aggregate(from(s in Shard, where: s.status == "migration_failed"), :count)
  end

  @doc """
  The most-recently-active shards, newest first, capped at `limit` — the fleet-wide
  hot set a warm-standby (`Fathom.Shard.WarmFollower`) pre-pulls so a failover skips
  the cold-open from S3.
  """
  @spec active_recent(pos_integer()) :: [Shard.t()]
  def active_recent(limit) do
    from(s in Shard, where: s.status == "active" and not is_nil(s.last_active_at))
    |> order_by([s], desc: s.last_active_at)
    |> limit(^limit)
    |> Repo.all()
  end

  defp laggard_query(head_version) do
    from(s in Shard, where: s.schema_version < ^head_version and s.status == "active")
  end

  defp update_shard(shard_id, attrs) do
    case Repo.get_by(Shard, shard_id: shard_id) do
      nil -> {:error, :not_found}
      shard -> shard |> Shard.changeset(attrs) |> Repo.update()
    end
  end
end
