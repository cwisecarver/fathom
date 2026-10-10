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

  # Automatic cool-off requeues allowed per quarantined shard (fix-review R3-2).
  @auto_requeue_max 3

  # Page size for `stream_failed/1` (expert review 2026-08-26 #21). Matches `Fathom.Migrator`'s
  # `@enqueue_chunk` so the sweep requeues and enqueues in step. It bounds the HEAP — how many
  # rows exist at once — not a bind-parameter count; see `requeue_failed/1` for why that premise
  # of the finding was measured and rejected.
  @requeue_chunk 5_000

  # Postgres bind-parameter ceiling is ~65535; 6 fields/row keeps a chunk well
  # under it and bounds each statement's size.
  @batch_chunk 1_000
  # A re-touch within this many seconds of the stored last_active_at is not written (perf review
  # 2026-10-01 #22). See record_batch/1.
  @default_touch_granularity_s 30

  # How long a shard may sit in `migrating` before the reconcile sweep assumes its
  # migration job was lost and reclaims it (see reclaim_stale_migrating/1). Generous —
  # a copy is seconds-to-minutes — and just above the hourly reconcile cadence, so a
  # genuinely in-flight migration is never reclaimed out from under itself.
  @default_migration_stale_seconds 3_600

  # RESERVED PREFIX for the restore drill's throwaway scratch forks (expert review 2026-09-18 #27).
  # `RestoreDrillJob.restore_one/1` forks a shard to `<prefix><int>`, verifies it, and drops it in an
  # `after` — but a node crash (the job is `max_attempts: 1`) between the fork and the drop leaves an
  # ACTIVE scratch directory row stamped at the SOURCE's schema_version (which can be < HEAD). Without
  # exclusion that row is an eternal laggard: `count_laggards/1` counts it so `converged` never turns
  # true, `laggards/2` keeps enqueuing a migration for a throwaway, and later drills re-sample it.
  # Excluded from the rollout + drill sweeps exactly like the capture template. `RestoreDrillJob` owns
  # the naming via `scratch_prefix/0`; the reserved shape is `scratch_id?/1`, enforced by `Tenants`.
  @scratch_prefix "restoredrill"

  @doc "The reserved prefix for restore-drill scratch forks — see `@scratch_prefix` (#27)."
  @spec scratch_prefix() :: String.t()
  def scratch_prefix, do: @scratch_prefix

  # The drill names a scratch fork `<prefix><positive integer>` and nothing else. Only THIS shape is
  # reserved (expert review 2026-09-29 #34) — the prefix alone would also reserve `restoredrill-eu`.
  @scratch_id_pattern "^" <> @scratch_prefix <> "[0-9]+$"

  @doc """
  True for an id of the restore drill's scratch shape, `#{@scratch_prefix}<digits>` — reserved:
  excluded from rollout and drill sweeps, so a real tenant with such an id would never migrate.
  `Fathom.Tenants` refuses it at provision and fork (#34).
  """
  @spec scratch_id?(String.t()) :: boolean()
  def scratch_id?(@scratch_prefix <> rest) when rest != "",
    do: String.match?(rest, ~r/\A[0-9]+\z/)

  def scratch_id?(_), do: false

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

  **A re-touch is written at most every `:directory_touch_granularity_s` (30 s)** — perf
  review 2026-10-01 #22. Every write here is a non-HOT update (`last_active_at` and
  `updated_at` are indexed), so recording every touched shard every second cost, at 30k
  active shards, ~30k row versions/s, **20.2 MiB/s of WAL** and 2.3M dead tuples in two
  minutes; with the guard, ~1k rows/s and 2.4 MiB/s (-88%, Postgres 18, 120 s each). No
  reader needs second precision, but two need exactness at an EDGE, so a touch is always
  written when the stored value is at or before:

    * `cutover_at` — the revert write-age guard refuses on `last_active_at > cutover_at`, and
      cutover stamps both with the same instant, so the first post-cutover touch must land
      or a revert would discard writes it could not see;
    * `last_flushed_at` — the loss-window report calls a shard dirty when
      `last_active_at > last_flushed_at`, so the first touch after a flush must land.

  Inside a dirty period `last_active_at` can therefore trail by up to the granularity
  (the reported loss window is at most that much short); dirty-vs-clean and
  used-since-cutover stay exact.

  Shard ids reaching here already passed `Fathom.Shards`' id validation at
  checkout, and `insert_all` parameterizes every value, so this is injection-safe
  even though it bypasses changeset validation.
  """
  @spec record_batch([{String.t(), DateTime.t()}]) :: non_neg_integer()
  def record_batch([]), do: 0

  def record_batch(entries) do
    now = DateTime.utc_now()

    granularity_s =
      Application.get_env(:fathom, :directory_touch_granularity_s, @default_touch_granularity_s)

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
          # the recency heuristics (laggard ordering, the revert write-age guard). Keep the
          # newer of incoming vs stored; updated_at (bookkeeping) always advances.
          on_conflict:
            from(s in Shard,
              update: [
                set: [
                  last_active_at:
                    fragment("GREATEST(EXCLUDED.last_active_at, ?)", s.last_active_at),
                  updated_at: fragment("EXCLUDED.updated_at")
                ]
              ],
              # The #22 write guard (see @doc). A row the WHERE rejects is not updated at all —
              # no new tuple, no index entries, no WAL — and does not count in the return value.
              where:
                fragment(
                  "EXCLUDED.last_active_at > ? + make_interval(secs => ?)",
                  s.last_active_at,
                  ^granularity_s
                ) or
                  fragment("? <= coalesce(?, '-infinity')", s.last_active_at, s.cutover_at) or
                  fragment("? <= coalesce(?, '-infinity')", s.last_active_at, s.last_flushed_at)
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
    # Never sample a scratch fork (#27): drilling a throwaway is wasted, and a LEAKED one
    # (never-verified) sorts FIRST under `asc_nulls_first`, so it would be re-drilled every run.
    |> exclude_scratch()
    |> Repo.all()
  end

  @doc """
  Active shards that need a scheduled snapshot (#18), least-recently-snapshotted first.

  "Need" means **the durable object has changed since we last snapshotted it** — `last_flushed_at`
  is ahead of `last_snapshot_at`, or the shard has never been snapshotted. That predicate is what
  keeps the cost proportional to writes rather than to fleet size: a million cold tenants that have
  not flushed since their last snapshot cost nothing, which matters because every selected shard is
  a server-side object COPY.

  `last_flushed_at` being NULL means the shard has never flushed — there is no durable object to
  copy — so those are excluded rather than snapshotted into an error.
  """
  @spec sample_for_snapshot(pos_integer()) :: [%{shard_id: String.t()}]
  def sample_for_snapshot(n) when is_integer(n) and n > 0 do
    from(s in Shard,
      where: s.status == "active",
      where: not is_nil(s.last_flushed_at),
      where: is_nil(s.last_snapshot_at) or s.last_flushed_at > s.last_snapshot_at,
      order_by: [asc_nulls_first: s.last_snapshot_attempt_at, asc_nulls_first: s.last_snapshot_at],
      limit: ^n,
      select: %{shard_id: s.shard_id}
    )
    |> Repo.all()
  end

  @doc """
  Stamps `last_snapshot_attempt_at` BEFORE a scheduled snapshot is tried (expert review 2026-10-10
  #14). The rotation orders by it, so a shard whose snapshot fails every time moves to the back
  after one attempt instead of holding a per-run slot forever (`last_snapshot_at` is stamped only
  on success, so keying the rotation on it let poison shards starve the queue). Returns rows
  updated.
  """
  @spec record_snapshot_attempt(String.t(), DateTime.t()) :: non_neg_integer()
  def record_snapshot_attempt(shard_id, at \\ DateTime.utc_now()) do
    {count, _} =
      from(s in Shard, where: s.shard_id == ^shard_id)
      |> Repo.update_all(set: [last_snapshot_attempt_at: at, updated_at: DateTime.utc_now()])

    count
  end

  @doc """
  The shard's `last_flushed_at`, or nil if the shard is gone or has never flushed.

  Read by the snapshot scheduler BEFORE it copies the object, so the snapshot can be credited with
  the watermark it actually reflects rather than wall-clock now() (expert review 2026-08-31 #7).
  """
  @spec last_flushed_at(String.t()) :: DateTime.t() | nil
  def last_flushed_at(shard_id) do
    from(s in Shard, where: s.shard_id == ^shard_id, select: s.last_flushed_at)
    |> Repo.one()
  end

  @doc """
  Of `shard_ids`, those with a directory row whose status is NOT `active` (suspended, deleted,
  migrating, quarantined, retired). A shard with no row is not listed. Used by the rebalancer to
  skip tenants that must not be handed off (expert review 2026-10-10 #27).
  """
  @spec non_active_among([String.t()]) :: [String.t()]
  def non_active_among([]), do: []

  def non_active_among(shard_ids) when is_list(shard_ids) do
    Repo.all(
      from s in Shard,
        where: s.shard_id in ^shard_ids and s.status != "active",
        select: s.shard_id
    )
  end

  @doc """
  Stamps `last_snapshot_at` after a scheduled snapshot (#18). Returns rows updated (0 if gone).

  Stamped only on SUCCESS by the caller: a failed snapshot must leave the shard at the head of the
  rotation so the next run retries it, rather than marking it done and waiting a full cycle.

  `at` is the WATERMARK the snapshot reflects — the shard's `last_flushed_at` read BEFORE the copy —
  not wall-clock now() (expert review 2026-08-31 #7). `Snapshots.create` copies the live stored
  object, i.e. state as of the last flush that preceded the CopyObject. Stamping now() instead
  marked any flush that landed between the copy and this stamp as "already snapshotted" (the
  `sample_for_snapshot/1` predicate is `last_flushed_at > last_snapshot_at`), even though those
  bytes are not in the snapshot — a silent generation loss if that shard then went cold and the
  live object was later lost. `updated_at` stays wall-clock now(): it records when the row changed,
  not what the snapshot covers.
  """
  @spec record_snapshot(String.t(), DateTime.t()) :: non_neg_integer()
  def record_snapshot(shard_id, at \\ DateTime.utc_now()) do
    {count, _} =
      from(s in Shard, where: s.shard_id == ^shard_id)
      |> Repo.update_all(set: [last_snapshot_at: at, updated_at: DateTime.utc_now()])

    count
  end

  @doc """
  Active shards that have been snapshotted at least once, least-recently-*swept* first (#18).

  The retention sweep's rotation, keyed on its OWN stamp, `last_retention_at`, NULLs first (expert
  review 2026-10-08 #19). It used to be keyed off `last_snapshot_at` on the reasoning that a shard
  being snapshotted is the one accumulating snapshots to expire — but that stamp moves only on a
  new snapshot, so the coldest once-snapshotted shards held the head of the order forever, every
  run re-swept them, and the hot shards that actually accumulate snapshots sorted last and were
  never reached once more than `n` shards had any. `record_retention/2` stamps each swept shard, so
  the rotation now visits all of them.
  """
  @spec sample_for_retention(pos_integer()) :: [%{shard_id: String.t()}]
  def sample_for_retention(n) when is_integer(n) and n > 0 do
    from(s in Shard,
      where: s.status == "active",
      where: not is_nil(s.last_snapshot_at),
      order_by: [asc_nulls_first: s.last_retention_at, asc: s.last_snapshot_at],
      limit: ^n,
      select: %{shard_id: s.shard_id}
    )
    |> Repo.all()
  end

  @doc """
  Stamps `last_retention_at` on the shards a retention sweep visited (expert review 2026-10-08 #19),
  moving them to the back of `sample_for_retention/1`'s rotation. The caller passes only shards whose
  sweep completed, so one that errored stays at the head and is retried next run. Returns rows
  updated.
  """
  @spec record_retention([String.t()], DateTime.t()) :: non_neg_integer()
  def record_retention(shard_ids, at \\ DateTime.utc_now())
  def record_retention([], _at), do: 0

  def record_retention(shard_ids, at) when is_list(shard_ids) do
    {count, _} =
      from(s in Shard, where: s.shard_id in ^shard_ids)
      |> Repo.update_all(set: [last_retention_at: at, updated_at: DateTime.utc_now()])

    count
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

  `retained_version` (3-arity) records which version the `Storage.retain/2` immediately before this
  cutover actually wrote (expert review 2026-08-24 #16b). It belongs in THIS update because it is
  the same transaction: the retain and the version stamp either both land or neither does, and a
  `retained_version` that disagreed with what storage holds is worse than none — it points a revert
  at an object that may not exist. The 2-arity form omits it and leaves the column untouched, which
  is what a caller that did not retain anything wants.
  """
  # Spec matches `guarded_update_shard/3`'s real return (expert review 2026-09-18 #26): it uses
  # `Repo.update_all` + a re-read, never a changeset, so `Ecto.Changeset.t()` was impossible; and it
  # returns `:status_conflict` when the row left the `active`/`migrating` set mid-flip, which the old
  # 2-arity spec omitted even though `register_fork/2` calls this form and a shard suspended/deleted
  # mid-fork yields exactly that.
  @spec cutover(String.t(), non_neg_integer()) ::
          {:ok, Shard.t()} | {:error, :not_found | :status_conflict}
  def cutover(shard_id, schema_version) do
    # Only a shard still `active` or `migrating` may cut over — never one suspended or deleted during
    # the copy window (#11).
    guarded_update_shard(shard_id, cutover_attrs(schema_version), ["active", "migrating"])
  end

  # Same real return as the 2-arity form; the `Ecto.Changeset.t()` here was impossible too (#26).
  #
  # `used_since_live: true` is for a CRASH-FORWARD cutover (expert review 2026-10-08 #14): an earlier
  # attempt already made the new version's bytes live and died before this stamp, so clients may
  # have written on them in between — for a whole Oban backoff, or an hour after a crash. Stamping
  # `last_active_at == cutover_at == now` made every one of those writes look pre-cutover, so the
  # revert write-age guard (`last_active_at > cutover_at` ⇒ refuse unless forced) passed and an
  # unforced revert restored the old version over them. When that window started is unknowable
  # here, so it is treated as USED: `last_active_at` lands one microsecond past `cutover_at`, and a
  # revert needs `force`. Over-refusing is the guard's documented safe direction. `cutover_at` stays
  # `now` because the rollout rate counts it.
  #
  # `keep_suspended: true` is for a REVERT (expert review 2026-10-08 #18). Reverting a suspended
  # tenant is deliberately allowed — it is an operator's restore — and the revert's retain and
  # restore ran for one, but this cutover accepted only `active`/`migrating`, so it rolled back after
  # the destructive restore had landed: live at the old version, the directory still at the new one,
  # no retirement scheduled for the backup, and five retries each re-pulling the shard. Now a
  # suspended row takes the cutover too, keeping its `suspended` status (a revert is not a resume).
  @spec cutover(String.t(), non_neg_integer(), non_neg_integer() | nil, keyword()) ::
          {:ok, Shard.t()} | {:error, :not_found | :status_conflict}
  def cutover(shard_id, schema_version, retained_version, opts \\ []) do
    attrs =
      Map.put(
        cutover_attrs(schema_version, Keyword.get(opts, :used_since_live, false)),
        :retained_version,
        retained_version
      )

    case guarded_update_shard(shard_id, attrs, ["active", "migrating"]) do
      {:error, :status_conflict} = conflict ->
        if Keyword.get(opts, :keep_suspended, false),
          do: guarded_update_shard(shard_id, Map.delete(attrs, :status), ["suspended"]),
          else: conflict

      other ->
        other
    end
  end

  defp cutover_attrs(schema_version, used_since_live? \\ false) do
    now = DateTime.utc_now()

    %{
      schema_version: schema_version,
      status: "active",
      last_active_at: if(used_since_live?, do: DateTime.add(now, 1, :microsecond), else: now),
      cutover_at: now,
      migrating_since: nil,
      # A success ends the auto-requeue streak (fix-review R3-2).
      requeue_count: 0,
      # The retain intent is consumed by the cutover that records `retained_version` (#27).
      retaining_version: nil
    }
  end

  @doc """
  Clears `retained_version` — but only if it still names `version` (expert review 2026-08-24 #16b).

  Called by `Fathom.Migrator.RetirementJob` after it drops `<shard>@<version>`, so the column stops
  claiming a copy that no longer exists. The conditional matters: a forward migration that ran
  between the retirement's guards and its drop has already pointed the column at a NEWER retained
  copy, and clearing unconditionally would erase a live recovery point's record while the object
  survives — turning a revert that would have worked into a refusal.

  Returns `:ok` whether or not a row matched; a shard with no row, or one whose column already
  moved on, is not an error, it is the condition doing its job.
  """
  @spec clear_retained_version(String.t(), non_neg_integer()) :: :ok
  def clear_retained_version(shard_id, version) do
    {_count, _} =
      Repo.update_all(
        from(s in Shard,
          where: s.shard_id == ^shard_id,
          where: s.retained_version == ^version
        ),
        set: [retained_version: nil, updated_at: DateTime.utc_now()]
      )

    :ok
  end

  @doc """
  Marks a shard as mid-migration (the app pauses writes for the copy window), recording
  `retaining_version` — the FILE version the migration is about to retain — in the same update
  (expert review 2026-09-29 #27), so a crash between the flush and the cutover leaves a durable
  record of what was retained. `nil` records nothing (the column is cleared at cutover).
  """
  @spec mark_migrating(String.t(), non_neg_integer() | nil) ::
          {:ok, Shard.t()} | {:error, :not_found | :status_conflict | Ecto.Changeset.t()}
  def mark_migrating(shard_id, retaining_version \\ nil),
    # Only an `active` shard may enter `migrating` — never resurrect a suspended/deleted tenant whose
    # snoozing job finally got the drain it was waiting on (#11).
    #
    # …OR ONE ALREADY `migrating` (expert review 2026-10-08 #7). An attempt that marks the row and
    # then dies before its `else` can unmark it — a node kill or deploy, an Oban Lifeline rescue, a
    # raise from a Postgres blip or `File.mkdir_p!` — leaves the row `migrating`. `run/3` lets that
    # through, but this refused it, and since f1cff2a the refusal is `{:not_active, :status_conflict}`
    # → the job CANCELS: the retry, the job's whole purpose after a transient fault, was dropped and
    # the shard sat invisible to every sweep until `reclaim_stale_migrating` plus the next reconcile.
    # The caller holds the shard's lease here, so the previous attempt is gone and re-marking just
    # re-stamps `migrating_since` and the retain intent. Suspended and deleted are still refused.
    do:
      guarded_update_shard(
        shard_id,
        %{
          status: "migrating",
          migrating_since: DateTime.utc_now(),
          retaining_version: retaining_version
        },
        ["active", "migrating"]
      )

  @doc """
  Restores a shard from `migrating` back to `active` after a migration failed — the counterpart to
  `mark_migrating/1`, which had none.

  Without it a failed copy left the row `migrating` forever, and since every laggard/reconcile query
  filters `status == "active"`, the shard was invisible to every sweep until the hourly
  `reclaim_stuck_migrating/1` noticed. It self-healed, just an hour later than it should have.
  Restoring here does not interfere with the failed job's own retry: that job carries an explicit
  target and does not go through a sweep, and `ShardMigrationJob` is unique per shard so a sweep
  that does re-enqueue in the meantime is deduped.

  **Conditional on the row still being `migrating`**, so it can never resurrect a tenant that was
  deleted or suspended during the copy window, nor overwrite a `migration_failed` quarantine written
  after attempts were exhausted. Returns the number of rows touched (1 = restored, 0 = left alone).
  """
  @spec unmark_migrating(String.t()) :: non_neg_integer()
  def unmark_migrating(shard_id) do
    {count, _} =
      from(s in Shard, where: s.shard_id == ^shard_id and s.status == "migrating")
      |> Repo.update_all(
        set: [status: "active", migrating_since: nil, updated_at: DateTime.utc_now()]
      )

    count
  end

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
  @spec mark_failed(String.t()) ::
          {:ok, Shard.t()} | {:error, :not_found | :status_conflict | Ecto.Changeset.t()}
  def mark_failed(shard_id),
    # Only an `active`/`migrating` shard may be quarantined — a suspend/delete that landed mid-copy
    # must not be overwritten with `migration_failed` (which retry_failed/0 would then flip back to
    # `active`, silently lifting the suspension) (#11).
    do:
      guarded_update_shard(
        shard_id,
        %{status: "migration_failed", migrating_since: nil, quarantined_at: DateTime.utc_now()},
        ["active", "migrating"]
      )

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

  Only an `active` row may be suspended (a re-suspend of an already-`suspended` row is an idempotent
  no-op so an operator retry after a partial failure still reaches the broadcast/drain). A
  `migrating` / `migration_failed` / `retired` row is refused with `{:error, :status_conflict}`:
  the old unconditional write overwrote a live migration's status, and `resume/1` then lifted it to
  `active` with the migration still in flight (expert review 2026-10-10 #10).
  """
  @spec suspend(String.t()) ::
          {:ok, Shard.t()}
          | {:error, :not_found | :deleted | :status_conflict | Ecto.Changeset.t()}
  def suspend(shard_id) do
    with {:ok, %Shard{status: status}} when status != "deleted" <- fetch_for_status(shard_id) do
      guarded_update_shard(shard_id, %{status: "suspended"}, ["active", "suspended"])
    end
  end

  @doc """
  Resumes a suspended shard back to `active` (#20). Refuses `:not_found` or `:deleted`, and
  `{:error, :status_conflict}` for a `migrating` / `migration_failed` / `retired` row, which used
  to be flipped to `active` (expert review 2026-10-10 #10). An already-`active` row returns
  `{:ok, row}` untouched, so a retried resume still re-broadcasts (fix-review R3-6).
  """
  @spec resume(String.t()) ::
          {:ok, Shard.t()}
          | {:error, :not_found | :deleted | :status_conflict | Ecto.Changeset.t()}
  def resume(shard_id) do
    case fetch_for_status(shard_id) do
      # Idempotent (fix-review R3-6): an `active` row means an earlier resume already landed, so a
      # retry after a failed broadcast must reach `Tenants.resume`'s re-broadcast rather than
      # getting `:status_conflict` and never re-announcing. Only `suspend` stays strict about source.
      {:ok, %Shard{status: "active"} = shard} -> {:ok, shard}
      {:ok, %Shard{}} -> guarded_update_shard(shard_id, %{status: "active"}, ["suspended"])
      {:error, _} = error -> error
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
    head_version
    |> laggard_query()
    |> without_live_migration_job()
    |> order_by([s], desc: s.last_active_at)
    |> limit(^limit)
    |> Repo.all()
  end

  # Exclude shards that ALREADY have a live ShardMigrationJob from the ROLLOUT SELECTION, not after
  # (expert review 2026-09-05 #13). `laggards/2` returns the `limit` most-recently-active laggards
  # and BulkEnqueue then drops those already queued -- so once >= batch_size hot shards have a job
  # snoozing forever on :shard_busy (they stay `active` at the old version, and being hottest they
  # sort to the top of EVERY sweep), each sweep selected the same batch, deduped all of it, and
  # inserted ZERO jobs: the cold tail behind them was never reached and the deploy gate never turned
  # green. Excluding them in the SELECT lets LIMIT skip past them to the tail; BulkEnqueue's dedup
  # stays as the race backstop.
  #
  # The literal state list and worker match `oban_jobs_worker_shard_id_live_index`'s partial
  # predicate EXACTLY (and Fathom.BulkEnqueue.unique_states/0), so the planner uses that index for
  # the NOT EXISTS. Kept literal rather than parameterized precisely so the partial-index predicate
  # match holds; the states are a fixed constant, never user input. `count_laggards/1` deliberately
  # does NOT get this exclusion -- an in-flight-but-behind shard is still a laggard, and hiding it
  # from the convergence gauge would make `converged` lie (the opposite of #19).
  defp without_live_migration_job(query) do
    from(s in query,
      where:
        fragment(
          "NOT EXISTS (SELECT 1 FROM oban_jobs j WHERE j.worker = 'Fathom.Migrator.ShardMigrationJob' AND j.state IN ('scheduled','available','executing','retryable','suspended') AND j.args->>'shard_id' = ?)",
          s.shard_id
        )
    )
  end

  @doc "How many active shards are still behind `head_version` (the reconcile gauge)."
  @spec count_laggards(non_neg_integer()) :: non_neg_integer()
  def count_laggards(head_version) do
    laggard_query(head_version) |> Repo.aggregate(:count)
  end

  @doc """
  How many shards are mid-migration (`migrating`) and still stamped below `head_version` (expert
  review 2026-10-08 #6).

  `count_laggards/1` is active-only, and a shard is `migrating` for its whole drain → copy → flush
  → cutover — while it still serves the old schema, since that status does not block checkout. So
  when the last laggards were all executing, `converged` read true while those tenants were still
  on vN-1, and the deploy gate shipped code that needs HEAD against them. `Migrator.status/0` folds
  this count into `converged`; `laggards/2` keeps selecting active rows only, so nothing re-enqueues
  a shard already being migrated.
  """
  @spec count_in_flight(non_neg_integer()) :: non_neg_integer()
  def count_in_flight(head_version) do
    behind_query(head_version, "migrating") |> Repo.aggregate(:count)
  end

  @doc """
  How many active shards are stamped ABOVE `head_version` — stranded past HEAD (expert review
  2026-09-05 #19). A shard that completed its migration in the yank race window sits at the yanked
  vN with `schema_version > head`; `count_laggards/1` is strictly `< head`, so it excludes them and
  `converged` reads true while those tenants still serve the reverted-away schema. This is a
  CURRENT directory claim (unlike the point-in-time stamp-drift gauge), so `Migrator.status/0`
  folds it into `converged`.
  """
  @spec count_above_head(non_neg_integer()) :: non_neg_integer()
  def count_above_head(head_version) do
    from(s in Shard, where: s.schema_version > ^head_version and s.status == "active")
    |> Repo.aggregate(:count)
  end

  @doc """
  How many active shards reached `head_version` at or after `since` — the numerator of the fleet
  rollout rate (expert review 2026-08-01 #43).

  **Why Postgres and not the `[:fathom, :migrator, :shard_migrated]` counter the finding proposes:**
  a telemetry counter is node-local and dies with the node, but `Migrator.status/0` is the
  *fleet* deploy gate — a rate assembled from one node's counter under-reports the rollout by
  however many nodes are running, and reads zero after any node restart mid-rollout. `cutover_at`
  is already stamped fleet-wide by `cutover/2` and survives both. The counter still ships, for
  per-node dashboards; this is what the gate reads.

  Scoped to `schema_version == head_version`, which excludes the COMMON revert (a shard reverted to
  vN-1 lands below head, uncounted). **It does NOT exclude every revert (expert review 2026-09-18
  #25):** `Migrator.revert_stranded/0` reverts shards stranded ABOVE head back DOWN to head, and
  `RevertJob`'s `climb_back` sends a chain-jumper FORWARD up to `to_version` (== head after a fleet
  revert) — both land AT head and stamp `cutover_at` via the shared `cutover/2`, so during an
  EMERGENCY REVERT this count (and the `rate_per_hour`/`eta_seconds` it feeds on the deploy gate)
  over-reports, making a reverting fleet read as progressing. `converged` — the primary gate — is
  unaffected. Distinguishing a revert-to-head from a forward cutover precisely needs a PRE-CUTOVER
  version to compare against, i.e. another column on `shards` — the system's hottest write table,
  where an index was deleted (`20260726023618`) specifically to stop paying for one. That cost is not
  justified for a secondary metric that is only wrong during the rare emergency-revert window, so the
  limitation is documented rather than closed; `status/0`'s `above_head` is the signal that a revert
  is in flight and the rate should be read with that in mind.

  **The `last_active_at` predicate is deliberately redundant** — do not "simplify" it away. There
  is no index on `cutover_at` (and `shards` is the system's hottest write table, where
  `20260726023618` deleted an index precisely to stop paying for one), but
  `shards_active_schema_version_last_active_at_index` covers `(schema_version, last_active_at)
  WHERE status = 'active'`. `cutover/2` stamps both columns with the same instant and
  `last_active_at` only ever moves forward (`record_batch/1` merges with GREATEST), so
  `cutover_at >= since` **implies** `last_active_at >= since`. Stating the implied half lets the
  planner range-scan the existing index instead of filtering every shard at head on the heap.
  """
  @spec count_cutovers_since(non_neg_integer(), DateTime.t()) :: non_neg_integer()
  def count_cutovers_since(head_version, %DateTime{} = since) do
    from(s in Shard,
      where:
        s.schema_version == ^head_version and s.status == "active" and
          s.cutover_at >= ^since and s.last_active_at >= ^since
    )
    |> Repo.aggregate(:count)
  end

  @doc """
  How many active shards are at `version` — the aggregate count (#12) for a revert-status gauge,
  without materializing the (potentially millions-large) set — the unbounded `shards_at_version/1`
  this replaced (expert review 2026-07-18 #12) was removed 2026-10-01 as dead code.
  """
  @spec count_at_version(non_neg_integer()) :: non_neg_integer()
  def count_at_version(version) do
    Repo.aggregate(
      from(s in Shard, where: s.schema_version == ^version and s.status == "active"),
      :count
    )
  end

  @doc """
  How many active shards have already reached `version` or gone past it (expert review 2026-08-24
  #14).

  The predicate for "is this version still attachable". `Copy.migrate_chain/4` runs a step's
  transform only when that step is IN the chain, and `statement_chain/2` builds `current+1 …
  target` from the shard's file version — so once a shard's `PRAGMA user_version >= version`, a
  transform attached to `version` can never run for it. A non-zero count here therefore means
  attaching one now would backfill only part of the fleet.

  `>=`, not `>`: a shard sitting exactly AT `version` has already applied that step.

  Excludes the reserved capture template for the same reason `laggards/2` does — it is migrated
  directly by Django and its directory stamp never advances, so it is not evidence about the
  fleet either way.
  """
  @spec count_at_or_above_version(non_neg_integer()) :: non_neg_integer()
  def count_at_or_above_version(version) do
    base = from(s in Shard, where: s.schema_version >= ^version and s.status == "active")

    case template_shard_id() do
      nil -> base
      id -> from(s in base, where: s.shard_id != ^id)
    end
    |> Repo.aggregate(:count)
  end

  @doc """
  Counts shards mid-copy (`status == "migrating"`) whose directory stamp is still BELOW `version`
  (expert review 2026-09-18 #2).

  `count_at_or_above_version/1` filters `status == "active"`, so it is blind to a shard in the copy
  window: `mark_migrating/1` flips the status to `"migrating"` and does NOT touch `schema_version`
  (the stamp only advances at `cutover`), so a shard actively copying toward `>= version` sits at
  `status == "migrating"` with `schema_version < version` — invisible to the at-or-above count. Such
  a shard built its replay chain (which reads the `transform` column) BEFORE `mark_migrating`, so
  attaching a transform to `version` now would let that shard cut over WITHOUT the backfill while a
  shard that starts migrating later gets it — the exact silent fleet split the attach refusal exists
  to block, reached through the TOCTOU window the active-only count does not cover.

  `< version`, not `<=`: a shard whose stamp already sits at or above `version` cannot cross it, and
  the forward engine migrates toward HEAD (`>= version`), so any migrating shard below `version`
  crosses it. Excludes the reserved template for the same reason `count_at_or_above_version/1` does.
  """
  @spec count_migrating_below_version(non_neg_integer()) :: non_neg_integer()
  def count_migrating_below_version(version) do
    base =
      from(s in Shard, where: s.schema_version < ^version and s.status == "migrating")

    case template_shard_id() do
      nil -> base
      id -> from(s in base, where: s.shard_id != ^id)
    end
    |> Repo.aggregate(:count)
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

  @doc """
  Quarantined (`migration_failed`) shards still BELOW `head` — the quarantined slice that is not at
  the fleet's version. Unlike `count_laggards/1` (active only), this is what `Migrator.status/0`
  folds into `converged` (expert review 2026-10-10 #4).
  """
  @spec count_failed_below(non_neg_integer()) :: non_neg_integer()
  def count_failed_below(head) do
    Repo.aggregate(
      from(s in Shard, where: s.status == "migration_failed" and s.schema_version < ^head),
      :count
    )
  end

  @doc """
  Shards whose last restore drill found their stored object's `PRAGMA user_version` disagreeing with
  this row's `schema_version` — the three-place stamp having drifted (expert review 2026-08-24 #25).

  **THE NUMBER THIS SITS NEXT TO IS THE REASON IT EXISTS.** `count_laggards/1` reads
  `schema_version` alone, and `Migrator.status/0` publishes `converged: laggards == 0` from it. A
  shard whose FILE is behind while its ROW says HEAD is therefore counted as converged, and a
  release gate reading that endpoint ships app code against shards that are not at HEAD. This is the
  same table's own record of the opposite being true.

  **A SAMPLE, NOT A CENSUS, AND IT MUST BE READ THAT WAY.** `RestoreDrillJob` is off by default and
  samples the least-recently-verified rows per run, so `0` means "no drift among the shards drilled
  so far", never "no drift". `stamp_drift_checked/0` returns how many rows have ever been drilled,
  which is what makes the zero interpretable — a zero against a checked count of zero says nothing
  at all.

  **POINT-IN-TIME, which is why it does not drive `converged`.** `last_verify_status` records what a
  drill found when it ran; a shard repaired since then still carries the old status until it is
  drilled again. Feeding that into a boolean release gate would block deploys on stale evidence, so
  the count is published beside `converged` for a human (or an explicit gate) to weigh, not folded
  into it.
  """
  @spec count_stamp_drift() :: non_neg_integer()
  def count_stamp_drift do
    # Counts BOTH stamp-drift verdicts, not just the first (expert review 2026-09-18 #10):
    #   "schema_mismatch"  — user_version disagrees with the directory (leg 2 vs leg 3)
    #   "ledger_mismatch"  — django_migrations disagrees with user_version (leg 1 vs leg 2)
    # Both are the three-place version stamp disagreeing; the full restore drill is the only leg that
    # produces "ledger_mismatch", and before it recorded durably (and before this counted it) a DR
    # audit read clean while the ledgers had drifted. `restored_mismatch`/`fork_failed` are recovery-
    # PATH failures, a different class, so they stay out of the STAMP-drift gauge (they remain
    # queryable via `last_verify_status`).
    Repo.aggregate(
      from(s in Shard,
        where:
          s.status == "active" and s.last_verify_status in ["schema_mismatch", "ledger_mismatch"]
      ),
      :count
    )
  end

  @doc """
  How many active shards have ever been restore-drilled — the denominator that makes
  `count_stamp_drift/0` readable (expert review 2026-08-24 #25).

  Without it a `stamp_drift: 0` is ambiguous between "checked, clean" and "never checked", and those
  are opposite facts for anyone deciding whether to ship.
  """
  @spec stamp_drift_checked() :: non_neg_integer()
  def stamp_drift_checked do
    Repo.aggregate(
      from(s in Shard, where: s.status == "active" and not is_nil(s.last_verified_at)),
      :count
    )
  end

  @doc """
  Keyset-streams `{shard_id, schema_version}` for every quarantined (`migration_failed`) shard,
  a page at a time (expert review 2026-08-26 #21).

  REPLACES `failed_shards/0`, which was a bare `Repo.all` with no `select` and no limit — full
  structs for the whole quarantined slice in one heap, and then every id handed to
  `requeue_failed/1` as bind parameters. Postgres caps a statement at **65 535 parameters**, so
  past ~65k quarantined shards the operator's documented recovery API was a hard error, and it
  failed EXACTLY when a large slice is quarantined (a fleet-wide S3 outage burning attempts) —
  i.e. during the incident it exists for.

  Same shape as `stream_ids_at_version/2`: each page is an independent short query ordered by
  `shard_id`, so nothing materializes at once and nothing holds a long transaction. It carries
  `schema_version` alongside the id because the only caller has to compare it against HEAD, and
  fetching two narrow columns in the page beats a second query per shard.

  SAFE TO REQUEUE WHILE ITERATING, which is what the caller does. The keyset advances by
  `shard_id` and the caller only flips rows it has already read, so every flipped row sorts at or
  below `last` and the next page — `status == "migration_failed" AND shard_id > last` — is
  unaffected. A shard that leaves the set by some other route mid-scan is simply skipped.
  """
  @spec stream_failed(pos_integer(), DateTime.t() | nil) :: Enumerable.t()
  def stream_failed(page_size \\ @requeue_chunk, cooled_before \\ nil) do
    Stream.resource(
      fn -> "" end,
      fn last ->
        rows =
          Repo.all(
            from(s in Shard,
              where: s.status == "migration_failed" and s.shard_id > ^last,
              order_by: [asc: s.shard_id],
              limit: ^page_size,
              select: {s.shard_id, s.schema_version}
            )
            |> cooled_before(cooled_before)
          )

        case rows do
          [] -> {:halt, last}
          _ -> {rows, rows |> List.last() |> elem(0)}
        end
      end,
      fn _ -> :ok end
    )
  end

  # Only rows quarantined at or before `cutoff`, for the reconcile job's cool-off requeue (expert
  # review 2026-10-10 #4). nil = no restriction (the operator path).
  #
  # Fix-review R3-2: keyed on `quarantined_at` (stamped only by `mark_failed`), NOT `updated_at` —
  # every touch (the recorder's upsert, resolve, snapshot/retention stamps) bumps `updated_at`, so a
  # hot quarantined shard never cooled off. A NULL `quarantined_at` (a row quarantined before the
  # column existed) counts as cooled. Rows that have used up their automatic requeues
  # (`:migration_auto_requeue_max`, default #{@auto_requeue_max}) are skipped: a deterministic
  # failure must not flap forever. An operator holds a shard quarantined by setting its
  # `requeue_count` at/above that cap (`UPDATE shards SET requeue_count = 1000 WHERE shard_id = …`);
  # `retry_failed/0` resets it.
  defp cooled_before(query, nil), do: query

  defp cooled_before(query, %DateTime{} = cutoff) do
    max = Application.get_env(:fathom, :migration_auto_requeue_max, @auto_requeue_max)

    from(s in query,
      where: is_nil(s.quarantined_at) or s.quarantined_at <= ^cutoff,
      where: s.requeue_count < ^max
    )
  end

  @doc """
  Up to `limit` quarantined shard IDs, for a display that shows a sample rather than the set.

  Exists because the dashboard called `failed_shards/0` — full structs, no `select`, no limit —
  every 5 s per viewer, and then used only the count and the first 40 ids (expert review
  2026-08-26 #30). The unbounded materialization is a real hazard rather than a tidiness one: the
  scenario that produces a large quarantine is a fleet-wide storage outage burning migration
  attempts, so the query is at its most expensive exactly when an operator is watching it.
  """
  @spec failed_shard_ids(pos_integer()) :: [String.t()]
  def failed_shard_ids(limit) when is_integer(limit) and limit > 0 do
    Repo.all(
      from s in Shard,
        where: s.status == "migration_failed",
        order_by: [asc: s.shard_id],
        limit: ^limit,
        select: s.shard_id
    )
  end

  @doc """
  HARD-delete a directory row, leaving no tombstone (expert review 2026-08-01 #48).

  Not for tenants. `Fathom.Tenants.delete/1` is the supported tenant removal and it tombstones on
  purpose: a deleted subdomain must be refused rather than silently re-minted as an empty shard, so
  the row persists and the id joins the public `Tombstones` ETS set that admission consults on every
  checkout.

  This exists for rows that were never a live tenant — the restore drill's scratch forks, and a
  `Fathom.Tenants.provision/1` whose template fork failed (expert review 2026-08-31 #10): nothing
  ever routed to them and no client ever held a token, so there is nothing to tombstone against, and
  a tombstone per case would grow that admission-path set without bound. Rolling the row back also
  keeps the id cleanly re-mintable on a retry, which a tombstone (the resurrection guard) would
  block.

  Returns the number of rows removed (0 if it was already gone).
  """
  @spec hard_delete(String.t()) :: non_neg_integer()
  def hard_delete(shard_id) do
    {count, _} = Repo.delete_all(from(s in Shard, where: s.shard_id == ^shard_id))
    count
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
        set: [status: "active", requeue_count: 0, updated_at: DateTime.utc_now()]
      )

    n
  end

  # DELIBERATELY NOT CHUNKED, and this comment exists so nobody adds it back.
  #
  # Expert review 2026-08-26 #21 asked for chunking here, on the premise that "Ecto expands
  # `in ^ids` to one bind parameter per element" and Postgres caps a statement at 65 535 of them.
  # MEASURED, and that is not what Ecto does: `field in ^list` compiles to `field = ANY($1)` —
  # **one** parameter carrying an array. `Ecto.Adapters.SQL.to_sql/4` on this exact query with
  # 70 000 ids returns `PARAM_COUNT: 1`, `SHAPES: [list: 70000]`.
  #
  # The finding conflated two different mechanisms. `Fathom.Migrator.enqueue_unique/1` genuinely
  # does chunk at 5 000, but it uses Oban's `insert_all`, which emits one parameter per column per
  # ROW — that is the real crash past ~7 281 jobs its comment records. A `WHERE … IN` does not
  # grow that way, so the same cap does not reach this call. Chunking here would add a loop, and a
  # comment claiming a hazard that does not exist, for no benefit.
  #
  # `directory_bind_parameter_test.exs` pins the measurement, so if a future Ecto or Postgrex
  # changed the expansion this stops being safe LOUDLY rather than silently.
  #
  # The real half of #21 — `failed_shards/0` materializing the whole quarantined slice as structs
  # — is fixed by `stream_failed/1` above.
  def requeue_failed(shard_ids) when is_list(shard_ids) do
    {n, _} =
      Repo.update_all(
        from(s in Shard, where: s.status == "migration_failed" and s.shard_id in ^shard_ids),
        set: [status: "active", requeue_count: 0, updated_at: DateTime.utc_now()]
      )

    n
  end

  @doc """
  `requeue_failed/1` for the reconcile job's AUTOMATIC cool-off requeue: same un-quarantine, but it
  counts against the shard's `requeue_count` cap instead of resetting it (fix-review R3-2).
  """
  @spec requeue_cooled_failed([String.t()]) :: non_neg_integer()
  def requeue_cooled_failed(shard_ids) when is_list(shard_ids) do
    {n, _} =
      Repo.update_all(
        from(s in Shard, where: s.status == "migration_failed" and s.shard_id in ^shard_ids),
        set: [status: "active", updated_at: DateTime.utc_now()],
        inc: [requeue_count: 1]
      )

    n
  end

  # active_recent/1 (the fleet-wide hot set the WarmFollower read cache pre-pulled) was removed
  # 2026-09-14 with the WarmFollower retirement — it had no other caller. The partial index it used
  # (shards_active_last_active_at_index) is KEPT; the laggard path relies on the same
  # status='active' + last_active_at shape.

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

  defp laggard_query(head_version), do: behind_query(head_version, "active")

  # Shards stamped below `head_version` in `status`, minus the scratch forks and the template.
  defp behind_query(head_version, status) do
    base =
      from(s in Shard, where: s.schema_version < ^head_version and s.status == ^status)
      |> exclude_scratch()

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

  # Exclude the restore drill's scratch forks (#27) so a leaked one is not an eternal laggard /
  # re-sampled drill target. EXACTLY the drill's own shape, `<prefix><digits>` (expert review
  # 2026-09-29 #34): this was `LIKE 'restoredrill%'`, but nothing refused that prefix for a real
  # tenant, so `restoredrill-eu` provisioned fine and was then silently never migrated while
  # `converged` read true. The exact shape is also refused at every tenant-facing birth path (see
  # `scratch_id?/1`), so the two sets cannot overlap.
  defp exclude_scratch(query) do
    from(s in query, where: fragment("? !~ ?", s.shard_id, ^@scratch_id_pattern))
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

  # Like update_shard/2, but the write LANDS ONLY IF the row's status is still one of `allowed` —
  # an atomic guard (a single conditional update_all ... returning), so a migration in flight across
  # a suspend or delete cannot resurrect the tenant (expert review 2026-09-05 #11). This mirrors
  # unmark_migrating/1, whose comment already states this invariant ("can never resurrect a tenant
  # that was deleted or suspended during the copy window") — the difference is that the three writes
  # that land on the migration SUCCESS and exhausted-failure paths (mark_migrating, cutover,
  # mark_failed) went through the UNguarded update_shard/2 and so happily overwrote `suspended` /
  # `deleted`. `admin_changeset/2` refuses to hand-flip these statuses for the same reason (moving
  # the directory status while leaving the in-memory admission gate unset); the migration engine was
  # doing exactly that flip on every cutover.
  #
  # Returns {:ok, shard} on a match, {:error, :status_conflict} when 0 rows matched (the row moved
  # to a lifecycle state the caller may not flip), {:error, :not_found} when the row is gone. Callers
  # already funnel any {:error, _} into aborting/cancelling the migration, which is the correct
  # response to a tenant that left the active set mid-copy.
  defp guarded_update_shard(shard_id, attrs, allowed) do
    set = attrs |> Map.put(:updated_at, DateTime.utc_now()) |> Map.to_list()

    {count, _} =
      from(s in Shard, where: s.shard_id == ^shard_id and s.status in ^allowed)
      |> Repo.update_all(set: set)

    # shard_id is unique, so count is 0 or 1. On a match re-read the row (the caller's contract is
    # {:ok, Shard.t()}); on a miss the row is either gone or in a status this write may not flip.
    case count do
      1 -> {:ok, Repo.get_by(Shard, shard_id: shard_id)}
      0 -> status_conflict_reason(shard_id)
    end
  end

  defp status_conflict_reason(shard_id) do
    case Repo.get_by(Shard, shard_id: shard_id) do
      nil -> {:error, :not_found}
      _present -> {:error, :status_conflict}
    end
  end
end
