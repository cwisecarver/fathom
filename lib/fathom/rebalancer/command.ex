defmodule Fathom.Rebalancer.Command do
  @moduledoc """
  One cross-node handoff command — an instruction the orchestrator writes for a specific
  node to execute (`warm` a shard into its cache, or `drain` a shard so it releases the
  lease). The target node's `Fathom.Rebalancer.CommandPoller` picks up commands addressed
  to its `node` (a `Fathom.Rebalancer.node_key/0`), runs them, and flips `status` to `done`
  or `failed`. This is how the control plane reaches a node it can't RPC (no BEAM cluster).
  """
  use Ecto.Schema
  import Ecto.Changeset

  @type t :: %__MODULE__{}

  @commands ~w(warm drain)
  # `cancelled` — a drain that's no longer wanted (the handoff reverted before the source
  # poller ran it, finding #7); terminal like done/failed but distinguishes "abandoned".
  @statuses ~w(pending done failed cancelled)

  schema "rebalance_commands" do
    field :shard_id, :string
    field :node, :string
    field :command, :string
    field :status, :string, default: "pending"
    field :detail, :string

    timestamps(type: :utc_datetime_usec)
  end

  @doc false
  def changeset(command, attrs) do
    command
    |> cast(attrs, [:shard_id, :node, :command, :status, :detail])
    |> validate_required([:shard_id, :node, :command])
    |> validate_inclusion(:command, @commands)
    |> validate_inclusion(:status, @statuses)
  end
end
