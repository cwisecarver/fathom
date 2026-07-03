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

    timestamps(type: :utc_datetime_usec, updated_at: false)
  end

  @doc false
  def changeset(release, attrs) do
    release
    |> cast(attrs, [:version, :name, :statements])
    |> validate_required([:version, :name])
    |> validate_number(:version, greater_than: 0)
    |> unique_constraint(:version)
  end
end
