defmodule Fathom.Rebalancer.Override do
  @moduledoc """
  One row of the LB exception table — a per-subdomain deviation from the pure ketama
  hash. `shard_id`'s subdomain is pinned to `pinned_node` (a `Fathom.Rebalancer.node_key/0`)
  instead of wherever the hash would place it. `Fathom.Rebalancer.LbMap` renders the set of
  these into the nginx `map $host $target` the LB applies; the control plane owns writes.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @type t :: %__MODULE__{}

  schema "shard_overrides" do
    field :shard_id, :string
    field :pinned_node, :string
    field :reason, :string
    field :q_per_s_at_pin, :float
    field :from_node, :string

    timestamps(type: :utc_datetime_usec)
  end

  @doc false
  def changeset(override, attrs) do
    override
    |> cast(attrs, [:shard_id, :pinned_node, :reason, :q_per_s_at_pin, :from_node])
    |> validate_required([:shard_id, :pinned_node])
    |> unique_constraint(:shard_id)
  end
end
