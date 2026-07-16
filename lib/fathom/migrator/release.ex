defmodule Fathom.Migrator.Release do
  @moduledoc """
  A released shard-schema version in the registry (`shard_migrations`). The fleet
  HEAD is the max released `version`; a shard whose `schema_version` is below HEAD
  is a laggard the rollout will migrate. The data transform for a version lives in
  code (keyed by version), not here — this row just records that the version was
  released.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @type t :: %__MODULE__{}

  schema "shard_migrations" do
    field :version, :integer
    field :name, :string
    field :statements, {:array, :string}, default: []
    # Expert review #12: a yanked release is dead — excluded from HEAD, its statements
    # never applied again. Set by Migrator.yank/1 (a revert yanks by default).
    field :yanked, :boolean, default: false
    # Expert review #32: the template's django_migrations count when this version was captured
    # (Capture already computes it on commit). Lets the post-revert drift check tell whether the
    # template's linear graph was left ahead of the yanked-away fleet, without opening the template
    # or parsing SQL. Nullable — pre-feature releases have none, and the check skips them.
    field :template_migration_count, :integer

    timestamps(type: :utc_datetime_usec, updated_at: false)
  end

  @doc false
  def changeset(release, attrs) do
    release
    |> cast(attrs, [:version, :name, :statements, :template_migration_count])
    |> validate_required([:version, :name])
    |> validate_number(:version, greater_than: 0)
    |> unique_constraint(:version)
  end
end
