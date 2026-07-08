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
    # Set when a handoff failed + reverted (finding #4): the renderer skips the row (traffic
    # returns to the source) but the row is retained so its fresh updated_at keeps the shard
    # in the Policy cooldown. Cleared (nil) by a later successful pin.
    field :failed_at, :utc_datetime_usec

    timestamps(type: :utc_datetime_usec)
  end

  @doc false
  def changeset(override, attrs) do
    override
    |> cast(attrs, [:shard_id, :pinned_node, :reason, :q_per_s_at_pin, :from_node, :failed_at])
    |> validate_required([:shard_id, :pinned_node])
    |> unique_constraint(:shard_id)
  end
end
