defmodule Fathom.Directory.Shard do
  @moduledoc """
  A row in the Postgres shard **directory** — the control plane's record of a
  shard: which schema version its data sits at, its lifecycle `status`, when it
  was last used, and (once retired) how long to keep it.

  This is metadata *about* a shard, distinct from `Fathom.Shard` (the per-shard
  coordinator process that owns the live SQLite file). The directory is what the
  rollout/migration machinery reads and flips; the data path keeps running off
  `Fathom.Shards` whether or not the directory is reachable.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @statuses ~w(active migrating retired migration_failed deleted suspended)

  # Statuses an operator may hand-set from the admin directory edit (#22): the
  # migration-lifecycle values only. `deleted` (#15) and `suspended` (#20) are EXCLUDED —
  # they're destructive/administrative orchestrations (fleet-wide tombstone / admission gate
  # + coordinator drain), never a hand-flip that would move the directory status while
  # leaving the in-memory gate unset on this and every other node. They go through the
  # dedicated delete / suspend / resume actions.
  #
  # `migrating` is EXCLUDED as a TARGET too (expert review 2026-11-07 #63): it is engine-owned —
  # only `Directory.mark_migrating/2` sets it, together with `migrating_since`, the liveness stamp
  # the stale-reclaim and the migration renewer key on. A hand-set `migrating` row had no
  # `migrating_since`, so `reclaim_stale_migrating/1` skipped it, no sweep selects anything but
  # `active`, and it counted in `in_flight` — `Migrator.status().converged` stayed false until
  # someone flipped it back by hand. It stays a legal SOURCE (`@admin_updatable_from`) so an
  # operator can still flip a stuck `migrating` row back to `active`.
  @admin_editable_statuses ~w(active retired migration_failed)
  @admin_updatable_from ~w(active migrating retired migration_failed)

  @type t :: %__MODULE__{}

  schema "shards" do
    field :shard_id, :string
    field :schema_version, :integer, default: 0
    field :status, :string, default: "active"
    field :last_active_at, :utc_datetime_usec

    # When the shard last flushed durably to storage (expert review #28), recorded off the hot path
    # by `Fathom.Directory.Recorder`. Survives node death (unlike the node-local FlushWatermark ETS),
    # so a post-node-loss report is `last_active_at > last_flushed_at` ⇒ dirty, window = the gap.
    field :last_flushed_at, :utc_datetime_usec
    field :retain_until, :utc_datetime_usec
    # When the shard entered `migrating` — the reconcile sweep times a migration out from
    # this (not updated_at, which traffic bumps). Set on mark_migrating, cleared on cutover /
    # mark_failed.
    field :migrating_since, :utc_datetime_usec
    # When the shard last cut over to its current schema_version — stamped by
    # Directory.cutover with the SAME instant as last_active_at, so "activity since
    # cutover" is exactly last_active_at > cutover_at. The revert force-guard reads this
    # (fable-review #13); nil (pre-column row / never cut over) = unknown write-age.
    field :cutover_at, :utc_datetime_usec

    # WHICH version this shard has a retained copy of (expert review 2026-08-24 #16b) — written by
    # `Directory.cutover/3` in the same transaction that stamps `schema_version`, because that is
    # the transaction the `Storage.retain/2` before it belongs to.
    #
    # NOT derivable from `schema_version - 1`, which is the assumption that made a fleet revert only
    # partially land: a cold-tail shard walks `current+1 … target` in ONE job and retains only the
    # version it came FROM, so v5 → v9 leaves `retained_version = 5` against `schema_version = 9`.
    # NULL means "none, or unknown" — a pre-column row, or a retained copy that `RetirementJob` has
    # since dropped. Every consumer must treat NULL as "cannot revert by pointer flip".
    field :retained_version, :integer
    # The FILE version an in-flight forward migration is retaining, written with `migrating` and
    # BEFORE the flush, cleared at cutover (expert review 2026-09-29 #27). Lets the crash-forward
    # `finalize/2` record what was actually retained instead of guessing from storage, which a
    # revert's leftover `@N` backup can fool. NULL = none in flight, or a pre-column row.
    field :retaining_version, :integer
    # Per-shard Hrana-token revocation counter (expert review #31): a token embeds the
    # version it was minted at; bumping this via Fathom.HranaAuth.revoke/1 invalidates
    # every outstanding token for THIS shard alone (no fleet-wide secret rotation).
    field :token_version, :integer, default: 1
    # When `token_version` was last raised by a graceful rotate (#24) — `HranaAuth` accepts the
    # PREVIOUS version for a grace window after this instant, so a rotate is zero-downtime. NULL
    # (the default, and what a hard `revoke` resets it to) means "no grace": the previous version
    # is refused immediately.
    field :token_version_bumped_at, :utc_datetime_usec
    field :token_version_changed_at, :utc_datetime_usec

    # When the shard's durable object was last integrity-checked by the RestoreDrillJob (#24), and
    # the outcome ("ok" / "corrupt" / "absent" / "sentinel" / "schema_mismatch" / "error"). NULL =
    # never drilled; the drill samples least-recently-verified first (ASC NULLS FIRST).
    field :last_verified_at, :utc_datetime_usec
    field :last_verify_status, :string

    # When `Fathom.Snapshots.ScheduleJob` last took a point-in-time snapshot of this shard (#18).
    # NULL = never. Two jobs read it: the scheduler samples least-recently-snapshotted first
    # (ASC NULLS FIRST) and skips shards whose `last_flushed_at` has not moved since, so a cold
    # tenant is never re-snapshotted for bytes that did not change.
    field :last_snapshot_at, :utc_datetime_usec
    # The retention sweep's rotation key (expert review 2026-10-08 #19).
    field :last_retention_at, :utc_datetime_usec

    # The snapshot scheduler's rotation key, stamped on every ATTEMPT (expert review 2026-10-10 #14).
    field :last_snapshot_attempt_at, :utc_datetime_usec

    # When `PromoteOnOpen` last took a `-promoexp` pre-promotion snapshot of this shard (expert
    # review 2026-10-12 #8). `ReconcileJob` sweeps rows where this is past the expiry (any status)
    # and clears it, so the 14-day expiry no longer depends on the GFS retention job being on.
    field :promotion_snapshot_at, :utc_datetime_usec

    # When `ErasureSweepJob` last found this DELETED row's storage clean (perf review 2026-10-14
    # #15). Drives the decaying re-probe schedule in `Directory.deleted_page/4`; NULL = never.
    field :purge_verified_at, :utc_datetime_usec

    # When `Directory.mark_failed/1` quarantined the shard (fix-review R3-2). Unlike `updated_at`,
    # nothing else touches it, so the reconcile cool-off measures real time in quarantine even for
    # a hot shard. `requeue_count` counts AUTOMATIC cool-off requeues (capped; reset by a cutover or
    # an operator `retry_failed/0`).
    field :quarantined_at, :utc_datetime_usec
    field :requeue_count, :integer, default: 0

    # PROVENANCE of a restore-drill scratch fork (expert review 2026-11-07 #15). The sweeps that
    # exclude scratch rows from rollout/drill sampling and that PURGE leaked ones act on THIS column,
    # never on the shape of the id: `restoredrill<digits>` is a reservation enforced only at
    # `Tenants.provision/2` and `fork/3`, so an organic tenant can carry it, and a name-keyed sweep
    # destroyed such a live tenant (objects, snapshots, row) 24 h after creation.
    field :scratch, :boolean, default: false

    timestamps(type: :utc_datetime_usec)
  end

  @doc "Valid lifecycle statuses for a directory entry."
  @spec statuses() :: [String.t()]
  def statuses, do: @statuses

  @doc "Statuses an operator may hand-set from the admin edit UI (excludes `deleted`, #15)."
  @spec admin_editable_statuses() :: [String.t()]
  def admin_editable_statuses, do: @admin_editable_statuses

  @doc """
  Statuses a row may CURRENTLY be in for an admin edit to apply (a superset of the editable
  targets: a stuck `migrating` row can be flipped back, expert review 2026-11-07 #63).
  """
  @spec admin_updatable_from() :: [String.t()]
  def admin_updatable_from, do: @admin_updatable_from

  @doc """
  Restricted changeset for operator hand-edits from the admin directory UI
  (expert review 2026-07-14 #22): only the fields **safe to flip by hand** —
  `status` and `retain_until` — are castable. The migration-state-machine fields
  (`schema_version`, `cutover_at`, `migrating_since`, `token_version`,
  `last_active_at`) are deliberately excluded, so an admin edit can never corrupt
  them even if they appear in `attrs` (`cast/3` drops unlisted keys). Editing
  `schema_version` by hand, for example, would desync the three-place version
  stamp and mislead the reconcile sweep — that stays a migration-engine
  operation, not a hand-flip.
  """
  @spec admin_changeset(t(), map()) :: Ecto.Changeset.t()
  def admin_changeset(shard, attrs) do
    shard
    |> cast(attrs, [:status, :retain_until])
    |> validate_required([:status])
    |> validate_inclusion(:status, @admin_editable_statuses)
  end

  @doc false
  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(shard, attrs) do
    shard
    |> cast(attrs, [
      :shard_id,
      :schema_version,
      :status,
      :last_active_at,
      :retain_until,
      :migrating_since,
      :cutover_at,
      # Castable here but deliberately NOT in `admin_changeset/2` above: it is a
      # migration-state-machine field like `schema_version`, and a hand-edit that names a version
      # with no object behind it points a revert at nothing.
      :retained_version,
      # Written ONLY by the restore drill's own fork at birth (`Directory.resolve/2`,
      # `scratch: true`). Not in `admin_changeset/2`: an operator hand-edit must not mark a live
      # tenant a purge target.
      :scratch
    ])
    |> validate_required([:shard_id, :schema_version, :status, :last_active_at])
    # Shard ids become SQLite file names and registry keys elsewhere; defer to the single
    # source of truth (Fathom.ShardId) rather than duplicating the pattern (finding #19).
    |> validate_change(:shard_id, fn :shard_id, id ->
      if Fathom.ShardId.valid?(id), do: [], else: [shard_id: "is not a valid shard id"]
    end)
    |> validate_number(:schema_version, greater_than_or_equal_to: 0)
    |> validate_inclusion(:status, @statuses)
    |> unique_constraint(:shard_id)
  end
end
