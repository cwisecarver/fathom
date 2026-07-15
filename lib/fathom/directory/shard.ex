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

  @statuses ~w(active migrating retired migration_failed)

  @type t :: %__MODULE__{}

  schema "shards" do
    field :shard_id, :string
    field :schema_version, :integer, default: 0
    field :status, :string, default: "active"
    field :last_active_at, :utc_datetime_usec
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
    # Per-shard Hrana-token revocation counter (expert review #31): a token embeds the
    # version it was minted at; bumping this via Fathom.HranaAuth.revoke/1 invalidates
    # every outstanding token for THIS shard alone (no fleet-wide secret rotation).
    field :token_version, :integer, default: 1

    timestamps(type: :utc_datetime_usec)
  end

  @doc "Valid lifecycle statuses for a directory entry."
  def statuses, do: @statuses

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
    |> validate_inclusion(:status, @statuses)
  end

  @doc false
  def changeset(shard, attrs) do
    shard
    |> cast(attrs, [
      :shard_id,
      :schema_version,
      :status,
      :last_active_at,
      :retain_until,
      :migrating_since,
      :cutover_at
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
