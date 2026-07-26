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

  @doc """
  Batch-records durable-flush times — the flush counterpart of `record_batch/1` (expert review #28),
  fed off the hot path by `Fathom.Directory.Recorder`. `entries` is `{shard_id, flushed_at}`. Only
  `last_flushed_at` moves (GREATEST, so an out-of-order flush can't rewind it); `last_active_at` is
  left to the access recorder. Returns rows written. Raises on a Postgres error (the recorder treats
  flushing as best-effort).
  """
  @spec record_flush_batch([{String.t(), DateTime.t()}]) :: non_neg_integer()
  def record_flush_batch([]), do: 0

  def record_flush_batch(entries) do
    now = DateTime.utc_now()

    entries
    |> Enum.chunk_every(@batch_chunk)
    |> Enum.reduce(0, fn chunk, acc ->
      rows =
        Enum.map(chunk, fn {shard_id, flushed_at} ->
          %{
            shard_id: shard_id,
            schema_version: 0,
            status: "active",
            last_active_at: flushed_at,
            last_flushed_at: flushed_at,
            inserted_at: now,
            updated_at: now
          }
        end)

      {count, _} =
        Repo.insert_all(Shard, rows,
          on_conflict:
            from(s in Shard,
              update: [
                set: [
                  # GREATEST ignores NULL, so a first flush sets it and a later one advances it;
                  # an out-of-order flush from a cross-remap can't rewind the watermark.
                  last_flushed_at:
                    fragment("GREATEST(EXCLUDED.last_flushed_at, ?)", s.last_flushed_at),
                  updated_at: fragment("EXCLUDED.updated_at")
                ]
              ]
            ),
          conflict_target: :shard_id
        )

      acc + count
    end)
  end

  @doc """
  The post-node-loss loss report (expert review #28): shards that were active since their last
  durable flush — `last_flushed_at` is NULL (never recorded a flush) or `last_active_at >
  last_flushed_at` — i.e. potentially holding writes that didn't reach storage. Most-recently-active
  first, capped at `limit`. Each row is `%{shard_id, last_active_at, last_flushed_at}`; the caller
  bounds the per-tenant loss window as `last_active_at - last_flushed_at` (or "never flushed").
  Excludes `deleted` shards.
  """
  @spec flush_lag_report(pos_integer()) :: [
          %{
            shard_id: String.t(),
            last_active_at: DateTime.t() | nil,
            last_flushed_at: DateTime.t() | nil
          }
        ]
  def flush_lag_report(limit \\ 100) do
    from(s in Shard,
      where:
        s.status != "deleted" and not is_nil(s.last_active_at) and
          (is_nil(s.last_flushed_at) or s.last_active_at > s.last_flushed_at),
      order_by: [desc: s.last_active_at],
      limit: ^limit,
      select: %{
        shard_id: s.shard_id,
        last_active_at: s.last_active_at,
        last_flushed_at: s.last_flushed_at
      }
    )
    |> Repo.all()
  end

  @doc """
  Samples up to `n` **active** shards to restore-drill (expert review #24), least-recently-verified
  first — `last_verified_at ASC NULLS FIRST`, so never-verified shards go before ones drilled long
  ago, and the whole active fleet cycles through verification over time. Returns `shard_id` +
  `schema_version` (the drill cross-checks the object's `user_version` against it).

  There is deliberately **no index** on `last_verified_at` (it would tax the resolve/record hot path
  for a gated, daily, bounded query — see the migration). So this is a sort over active shards; fine
  at typical scale, and the drill's daily cadence absorbs it. An operator running the drill against
  *millions* of active shards can add the index then.
  """
  @spec sample_for_drill(pos_integer()) :: [
          %{shard_id: String.t(), schema_version: non_neg_integer()}
        ]
  def sample_for_drill(n) when is_integer(n) and n > 0 do
    from(s in Shard,
      where: s.status == "active",
      order_by: [asc_nulls_first: s.last_verified_at],
      limit: ^n,
      select: %{shard_id: s.shard_id, schema_version: s.schema_version}
    )
    |> Repo.all()
  end

  @doc """
  Records a restore-drill outcome (#24) on the shard's row: stamps `last_verified_at` (drives the
  sampling weight) and `last_verify_status` (queryable durably). Returns how many rows were updated
  (0 if the shard is gone). Best-effort — never raises the caller.
  """
  @spec record_verification(String.t(), String.t()) :: non_neg_integer()
  def record_verification(shard_id, status) when is_binary(status) do
    now = DateTime.utc_now()

    {count, _} =
      from(s in Shard, where: s.shard_id == ^shard_id)
      |> Repo.update_all(
        set: [last_verified_at: now, last_verify_status: status, updated_at: now]
      )

    count
  rescue
    _ -> 0
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

  @doc "Every directory row — the DR reconcile sweep (#6). Operator tooling, not a hot path."
  @spec all() :: [Shard.t()]
  def all, do: Repo.all(Shard)

  @doc """
  Aligns a shard's `schema_version` to `version` WITHOUT the cutover side effects (no
  `cutover_at`/`last_active_at` stamp) — the DR reconcile aligning the directory to the shard file's
  authoritative `PRAGMA user_version` after a Postgres point-in-time restore rolled it back (#6). The
  file's version lives in storage (not Postgres), so it survives the restore and is the source of truth.
  """
  @spec reconcile_schema_version(String.t(), non_neg_integer()) ::
          {:ok, Shard.t()} | {:error, :not_found | Ecto.Changeset.t()}
  def reconcile_schema_version(shard_id, version),
    do: update_shard(shard_id, %{schema_version: version})

  @doc """
  Raises a shard's `token_version` to at least `floor` (never lowers) — the DR reconcile aligning the
  directory to the durable storage revocation floor after a restore (#6). Returns the row count
  touched (1 = raised, 0 = already at/above `floor`).
  """
  @spec raise_token_version(String.t(), non_neg_integer()) :: non_neg_integer()
  def raise_token_version(shard_id, floor) do
    now = DateTime.utc_now()

    {count, _} =
      Repo.update_all(
        from(s in Shard, where: s.shard_id == ^shard_id and s.token_version < ^floor),
        set: [token_version: floor, updated_at: now]
      )

    count
  end

  @doc """
  Renews a shard's `migrating_since` liveness stamp (expert review 2026-07-18 #11) — bumps it to
  now, but ONLY while the shard is `migrating`. The migration lease renewer calls this on the same
  cadence it renews the S3 lease, so a genuinely-running long copy keeps its stamp fresh (renewer
  alive ⇒ `reclaim_stale_migrating/1` leaves it be), while a migration whose Oban job was lost
  stops renewing (renewer dead ⇒ the stamp goes stale ⇒ it is reclaimed). `mark_migrating` stamps
  it once; without this renewal a >`stale_after` copy was flipped back to `active` while still
  running.

  A no-op for any non-`migrating` status: the lease renewer also runs for a fork (which holds a
  lease but never enters `migrating`), so this must never resurrect an active/cut-over/failed row.
  Returns the number of rows touched (0 or 1) so a caller can tell a no-op from a real renewal.
  """
  @spec touch_migrating(String.t()) :: non_neg_integer()
  def touch_migrating(shard_id) do
    now = DateTime.utc_now()

    {count, _} =
      Repo.update_all(
        from(s in Shard, where: s.shard_id == ^shard_id and s.status == "migrating"),
        set: [migrating_since: now, updated_at: now]
      )

    count
  end

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
  Tombstones a shard — flips its directory row to `deleted` for full tenant erasure (#15).

  The tombstone is the permanent re-mint guard: `Fathom.Tenants.tombstoned?/1` reads
  `deleted` rows into the admission ETS gate so a stray request can never re-create the
  deleted tenant as an empty shard. Registers a fresh `deleted` row if none exists (a novel
  shard the directory never recorded), so the guard holds regardless. Never resurrected —
  `resolve`/`record_batch` on-conflict only bump recency, never status.
  """
  @spec tombstone(String.t()) :: {:ok, Shard.t()} | {:error, Ecto.Changeset.t()}
  def tombstone(shard_id) do
    now = DateTime.utc_now()

    %Shard{}
    |> Shard.changeset(%{
      shard_id: shard_id,
      schema_version: 0,
      status: "deleted",
      last_active_at: now
    })
    |> Repo.insert(
      # An existing row (any status) flips to `deleted`; a novel id inserts one. Only status +
      # bookkeeping move — schema_version/cutover_at/etc. are irrelevant once erased.
      on_conflict: [set: [status: "deleted", updated_at: now]],
      conflict_target: :shard_id,
      returning: true
    )
  end

  @doc "All tombstoned (`deleted`) shard ids — loaded into the admission tombstone gate at boot/refresh (#15)."
  @spec deleted_shard_ids() :: [String.t()]
  def deleted_shard_ids do
    Repo.all(from s in Shard, where: s.status == "deleted", select: s.shard_id)
  end

  @doc """
  Tombstoned ids whose row changed at or after `since` — the incremental half of the tombstone
  refresh (expert review 2026-07-24 #30).

  `deleted_shard_ids/0` returns EVERY id ever deleted, and the periodic refresh ran it on every
  node every 5 minutes, so both the Postgres read and the receiving process's heap scaled with
  cumulative lifetime deletions rather than with anything current.

  Safe as an incremental key because `tombstone/1` always stamps `updated_at`, so a newly-deleted
  row is never missed; and because the in-memory set is append-only with idempotent inserts, a row
  returned twice costs nothing. Callers should still pass an overlap (query slightly before their
  last refresh) so a transaction that started earlier but committed later cannot slip below the
  high-water mark. Served by `shards_deleted_updated_at_index`.
  """
  @spec deleted_shard_ids_since(DateTime.t()) :: [String.t()]
  def deleted_shard_ids_since(%DateTime{} = since) do
    Repo.all(
      from s in Shard,
        where: s.status == "deleted" and s.updated_at >= ^since,
        select: s.shard_id
    )
  end

  @doc """
  Suspends a shard — flips its directory row to `suspended` (administrative offline, #20). A
  suspended tenant is denied at admission (via the `Fathom.Tenants.Suspensions` gate) until
  `resume/1`. Refuses `:not_found`, or `:deleted` (a tombstoned tenant is gone, not suspendable).
  """
  @spec suspend(String.t()) ::
          {:ok, Shard.t()} | {:error, :not_found | :deleted | Ecto.Changeset.t()}
  def suspend(shard_id) do
    with {:ok, %Shard{status: status}} when status != "deleted" <- fetch_for_status(shard_id) do
      update_shard(shard_id, %{status: "suspended"})
    end
  end

  @doc "Resumes a suspended shard back to `active` (#20). Refuses `:not_found` or `:deleted`."
  @spec resume(String.t()) ::
          {:ok, Shard.t()} | {:error, :not_found | :deleted | Ecto.Changeset.t()}
  def resume(shard_id) do
    with {:ok, %Shard{status: status}} when status != "deleted" <- fetch_for_status(shard_id) do
      update_shard(shard_id, %{status: "active"})
    end
  end

  @doc "All `suspended` shard ids — loaded into the admission suspend gate at boot/refresh (#20)."
  @spec suspended_shard_ids() :: [String.t()]
  def suspended_shard_ids do
    Repo.all(from s in Shard, where: s.status == "suspended", select: s.shard_id)
  end

  defp fetch_for_status(shard_id) do
    case get(shard_id) do
      {:ok, %Shard{status: "deleted"}} -> {:error, :deleted}
      {:ok, %Shard{} = shard} -> {:ok, shard}
      :error -> {:error, :not_found}
    end
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

  @doc """
  Active shards currently at `version` — the set a fleet revert flips back.

  Materializes the WHOLE set as full structs, so at fleet scale (millions at a version) this is a
  memory blowup (expert review 2026-07-18 #12). The revert engine now uses `count_at_version/1`
  (aggregate) and `stream_ids_at_version/2` (keyset-paged ids) instead; keep this only for small,
  known-bounded callers (ops/iex).
  """
  @spec shards_at_version(non_neg_integer()) :: [Shard.t()]
  def shards_at_version(version) do
    Repo.all(from s in Shard, where: s.schema_version == ^version and s.status == "active")
  end

  @doc """
  How many active shards are at `version` — the aggregate count (#12) for a revert-status gauge,
  without materializing the (potentially millions-large) set the way `shards_at_version/1` does.
  """
  @spec count_at_version(non_neg_integer()) :: non_neg_integer()
  def count_at_version(version) do
    Repo.aggregate(
      from(s in Shard, where: s.schema_version == ^version and s.status == "active"),
      :count
    )
  end

  @doc """
  Lazily streams the active shard_ids at `version` in keyset-paginated pages of `page_size` (#12),
  so a fleet-wide set (millions) never materializes at once and never holds one long transaction:
  each page is an independent short query ordered by `shard_id`, so the revert engine can enqueue +
  commit per chunk (a job starts after the first page, not after a full scan). Returns a `Stream` of
  shard_id strings. Robust to shards flipping out of the set mid-scan (a concurrently-reverted shard
  is simply skipped — its revert is already in flight, and enqueue is idempotent).
  """
  @spec stream_ids_at_version(non_neg_integer(), pos_integer()) :: Enumerable.t()
  def stream_ids_at_version(version, page_size \\ 5_000) do
    Stream.resource(
      fn -> "" end,
      fn last ->
        ids =
          Repo.all(
            from(s in Shard,
              where: s.schema_version == ^version and s.status == "active" and s.shard_id > ^last,
              order_by: [asc: s.shard_id],
              limit: ^page_size,
              select: s.shard_id
            )
          )

        case ids do
          [] -> {:halt, last}
          _ -> {ids, List.last(ids)}
        end
      end,
      fn _ -> :ok end
    )
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

  @doc "The quarantined (`migration_failed`) shards."
  @spec failed_shards() :: [Shard.t()]
  def failed_shards do
    Repo.all(from s in Shard, where: s.status == "migration_failed")
  end

  @doc "Total shard rows in the directory across all statuses (the fleet's known-shard count)."
  @spec count() :: non_neg_integer()
  def count, do: Repo.aggregate(Shard, :count)

  @doc """
  Shard counts grouped by lifecycle status, as a `%{status => count}` map — the dashboard's
  status breakdown. NB: `status` alone is unindexed (the only status indexes are partial
  `WHERE status='active'`), so this is a sequential group-by — cheap at current scale, revisit
  with a covering index if the directory grows large.
  """
  @spec count_by_status() :: %{optional(String.t()) => non_neg_integer()}
  def count_by_status do
    from(s in Shard, group_by: s.status, select: {s.status, count(s.shard_id)})
    |> Repo.all()
    |> Map.new()
  end

  @doc """
  The shard's current Hrana-token revocation version (expert review #31), or `nil`
  if the shard has no directory row. A token minted at a version below this no
  longer verifies.
  """
  @spec token_version(String.t()) :: pos_integer() | nil
  def token_version(shard_id) do
    Repo.one(from s in Shard, where: s.shard_id == ^shard_id, select: s.token_version)
  end

  @doc """
  The shard's revocation floor + the instant of the last graceful rotate — `{version, bumped_at}`
  (#24). `HranaAuth` caches this and accepts a token at `version - 1` while `bumped_at` is within
  the rotation grace window (a `revoke` sets `bumped_at` to `nil`, so the previous version is
  refused immediately). `{nil, nil}` if the shard has no directory row.
  """
  @spec token_floor_info(String.t()) :: {pos_integer() | nil, DateTime.t() | nil}
  def token_floor_info(shard_id) do
    case Repo.one(
           from s in Shard,
             where: s.shard_id == ^shard_id,
             select: {s.token_version, s.token_version_bumped_at}
         ) do
      nil -> {nil, nil}
      {version, bumped_at} -> {version, bumped_at}
    end
  end

  @doc """
  Every directory row as a keyset-paginated stream, ordered by `shard_id`.

  `all/0` materializes the whole table; at fleet scale that is hundreds of MB of structs in one
  process — an OOM, not a slow query (expert review 2026-07-24 #15). Use this for any sweep that
  walks the directory. Served by `shards_shard_id_index`, so each page is a bounded index read and
  peak memory is `O(page_size)` rather than `O(fleet)`.

  Not a snapshot: rows inserted below the cursor after the walk passes are missed, and a row updated
  mid-walk is seen in whichever state the page read finds it. That is the right trade for the
  janitorial sweeps that use it — each row is reconciled independently, and the next run picks up
  anything missed.
  """
  @spec all_paged(pos_integer()) :: Enumerable.t()
  def all_paged(page_size \\ 5_000) do
    Stream.resource(
      fn -> "" end,
      fn
        :done ->
          {:halt, :done}

        cursor ->
          rows =
            Repo.all(
              from s in Shard,
                where: s.shard_id > ^cursor,
                order_by: [asc: s.shard_id],
                limit: ^page_size
            )

          case rows do
            [] -> {:halt, :done}
            _ when length(rows) < page_size -> {rows, :done}
            _ -> {rows, List.last(rows).shard_id}
          end
      end,
      fn _ -> :ok end
    )
  end

  @doc """
  Every shard whose Hrana token floor has ever been raised, as `{shard_id, version, bumped_at}`.

  `token_version` defaults to 1 and only `revoke/1`, `rotate/1`, and the reconcile sweep's
  `raise_token_version/2` raise it, so this set is normally a tiny fraction of the fleet — one
  bounded query replaces the per-shard TTL read-through that scaled with SHARD COUNT rather than
  with revocation events (expert review 2026-07-24 #5). Served by
  `shards_revoked_token_version_index`.

  Absence from this set is NOT proof a shard is unrevoked — a Postgres PITR can lower
  `token_version`, and only the durable per-shard storage floor catches that. See
  `Fathom.HranaAuth.Revocations`: a shard with no cached entry always takes the full read-through,
  including the storage-floor union.
  """
  @spec revoked_floors() :: [{String.t(), non_neg_integer(), DateTime.t() | nil}]
  def revoked_floors do
    Repo.all(
      from s in Shard,
        where: s.token_version > 1,
        select: {s.shard_id, s.token_version, s.token_version_bumped_at}
    )
  end

  @doc """
  Graceful zero-downtime rotation (#24): raises `token_version` (so a new token mints one higher)
  and stamps `token_version_bumped_at` = now, so `HranaAuth` keeps accepting the PREVIOUS version
  for the rotation grace window — mint-new → deploy → the old auto-hardens out. Returns
  `{:ok, new_version}` or `{:error, changeset}` for an invalid id.
  """
  @spec rotate_token(String.t()) :: {:ok, pos_integer()} | {:error, Ecto.Changeset.t()}
  def rotate_token(shard_id), do: bump_token(shard_id, DateTime.utc_now())

  @doc """
  Revokes every outstanding Hrana token for `shard_id` by bumping its
  `token_version` (expert review #31). Registers the shard first if unknown (so a
  revoke is never lost to a not-yet-recorded shard) — WITHOUT bumping
  `last_active_at` on an existing row (round-2 #32: a revoke is operator action,
  not tenant activity; the resolve/1 it used to call phantom-bumped recency, so
  revoking during an incident made the subsequent revert's write-age guard cancel
  untouched shards). Returns `{:ok, new_version}`, or `{:error, changeset}` for an
  invalid id (previously a MatchError crash).
  """
  @spec bump_token_version(String.t()) :: {:ok, pos_integer()} | {:error, Ecto.Changeset.t()}
  def bump_token_version(shard_id), do: bump_token(shard_id, nil)

  # Raise token_version by one, setting token_version_bumped_at to `bumped_at` (a DateTime for a
  # graceful rotate — grace on; `nil` for a hard revoke — grace off, previous version refused
  # immediately). Registers the shard first if unknown (a revoke/rotate is never lost to a
  # not-yet-recorded shard) WITHOUT bumping last_active_at (round-2 #32: operator action, not
  # tenant activity). Returns the NEW version.
  defp bump_token(shard_id, bumped_at) do
    register =
      %Shard{}
      |> Shard.changeset(%{
        shard_id: shard_id,
        schema_version: 0,
        status: "active",
        last_active_at: DateTime.utc_now()
      })
      |> Repo.insert(on_conflict: :nothing, conflict_target: :shard_id)

    case register do
      {:ok, _} ->
        {1, [version]} =
          Repo.update_all(
            from(s in Shard, where: s.shard_id == ^shard_id, select: s.token_version),
            inc: [token_version: 1],
            set: [token_version_bumped_at: bumped_at]
          )

        {:ok, version}

      {:error, changeset} ->
        {:error, changeset}
    end
  end

  @doc """
  Returns quarantined shards to `active` so the sweeps see them again — the exit path
  `migration_failed` never had (expert review #25): quarantined shards were excluded
  from laggards, reverts, and every sweep forever, so a wave of transient failures
  (an S3 outage burning attempts) froze a slice of the fleet at the old version even
  after the cause was fixed, and un-quarantining took hand-written SQL. Pass a list of
  shard ids to requeue selectively, or `:all`. Returns the number requeued.
  """
  @spec requeue_failed(:all | [String.t()]) :: non_neg_integer()
  def requeue_failed(shard_ids \\ :all)

  def requeue_failed(:all) do
    {n, _} =
      Repo.update_all(
        from(s in Shard, where: s.status == "migration_failed"),
        set: [status: "active", updated_at: DateTime.utc_now()]
      )

    n
  end

  def requeue_failed(shard_ids) when is_list(shard_ids) do
    {n, _} =
      Repo.update_all(
        from(s in Shard, where: s.status == "migration_failed" and s.shard_id in ^shard_ids),
        set: [status: "active", updated_at: DateTime.utc_now()]
      )

    n
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

  @max_page 200
  @default_page 50

  @doc """
  A page of directory rows for the admin browser (expert review 2026-07-14 #22).
  Filters by `:status` (exact) and `:q` (shard_id substring, case-insensitive),
  ordered by `shard_id`, paginated by `:limit` (default #{@default_page}, capped at
  #{@max_page}) / `:offset`.

  Returns `%{rows:, has_more?:, total:, limit:, offset:}`.

  `has_more?` comes free: the query fetches `limit + 1` rows and reports whether the
  extra one existed. That is all a prev/next UI needs.

  `total` is the exact matching count and costs a **second whole-table aggregate**
  (`COUNT(*)`, unfiltered when no filter is set). Pass `count: false` to skip it and get
  `total: nil` — expert review 2026-07-24 #32: `AdminDirectoryLive` re-runs this on every
  keystroke of the filter box, so at a million shards an 8-character tenant name used to
  cost 8 full-table counts on top of 8 scans. It defaults to `true` because
  `FathomWeb.Api.TenantController` publishes `total` in its JSON list response, and
  silently turning that into `null` would be a breaking API change.
  """
  @spec list_page(keyword()) :: %{
          rows: [Shard.t()],
          has_more?: boolean(),
          total: non_neg_integer() | nil,
          limit: pos_integer(),
          offset: non_neg_integer()
        }
  def list_page(opts \\ []) do
    limit = opts |> Keyword.get(:limit, @default_page) |> clamp_page()
    offset = max(Keyword.get(opts, :offset, 0), 0)
    query = admin_filter(opts)

    # limit + 1: the extra row is the has_more? signal, and it is cheaper than any count.
    fetched =
      query
      |> order_by([s], asc: s.shard_id)
      |> limit(^(limit + 1))
      |> offset(^offset)
      |> Repo.all()

    total =
      if Keyword.get(opts, :count, true), do: Repo.aggregate(query, :count, :id)

    %{
      rows: Enum.take(fetched, limit),
      has_more?: length(fetched) > limit,
      total: total,
      limit: limit,
      offset: offset
    }
  end

  defp admin_filter(opts) do
    base = from(s in Shard)

    base =
      case Keyword.get(opts, :status) do
        status when is_binary(status) and status != "" ->
          from(s in base, where: s.status == ^status)

        _ ->
          base
      end

    case Keyword.get(opts, :q) do
      term when is_binary(term) and term != "" ->
        from(s in base, where: ilike(s.shard_id, ^("%" <> term <> "%")))

      _ ->
        base
    end
  end

  defp clamp_page(n) when is_integer(n) and n > 0, do: min(n, @max_page)
  defp clamp_page(_), do: @default_page

  @doc """
  Guarded operator update of a directory row from the admin UI (expert review
  2026-07-14 #22). Only `:status` and `:retain_until` are castable — see
  `Fathom.Directory.Shard.admin_changeset/2`, which is the edit-safety boundary
  (the migration-state-machine fields can't be hand-flipped here). Returns
  `{:error, :not_found}` for an unknown shard.
  """
  @spec admin_update(String.t(), map()) ::
          {:ok, Shard.t()} | {:error, :not_found | Ecto.Changeset.t()}
  def admin_update(shard_id, attrs) do
    case Repo.get_by(Shard, shard_id: shard_id) do
      nil -> {:error, :not_found}
      shard -> shard |> Shard.admin_changeset(attrs) |> Repo.update()
    end
  end

  defp laggard_query(head_version) do
    base = from(s in Shard, where: s.schema_version < ^head_version and s.status == "active")

    # The reserved capture template (config :template_shard_id) is migrated directly by Django, so
    # its directory stamp never advances and it perpetually reads as the most-recent laggard. Left
    # in, the reconcile sweep drains it + replays its OWN captured DDL onto itself → "already exists"
    # → quarantine, and a drain racing an in-flight `manage.py migrate` can drop the capture buffer
    # and fork the fleet from the template (expert review 2026-07-14 #8). Exclude it from every
    # laggard/rollout sweep. nil (prod default) ⇒ no exclusion.
    case template_shard_id() do
      nil -> base
      id -> from(s in base, where: s.shard_id != ^id)
    end
  end

  defp template_shard_id do
    case Fathom.ShardId.cast(Application.get_env(:fathom, :template_shard_id)) do
      {:ok, id} -> id
      _ -> nil
    end
  end

  defp update_shard(shard_id, attrs) do
    case Repo.get_by(Shard, shard_id: shard_id) do
      nil -> {:error, :not_found}
      shard -> shard |> Shard.changeset(attrs) |> Repo.update()
    end
  end
end
