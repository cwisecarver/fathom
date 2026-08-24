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
  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(command, attrs) do
    command
    |> cast(attrs, [:shard_id, :node, :command, :status, :detail])
    |> validate_required([:shard_id, :node, :command])
    |> validate_inclusion(:command, @commands)
    |> validate_inclusion(:status, @statuses)
    |> validate_shard_id()
  end

  # The isolation gate at the command write boundary (review 2026-07-09 #6): a command's
  # `shard_id` is later handed straight to `Fathom.Shards.drain/2` and
  # `Fathom.Shard.WarmFollower.warm_now/1` (→ `cache_path/1`, a `#{shard_id}.db` file path) by
  # the target node's poller. Enforce `Fathom.ShardId`'s ONE rule (not a duplicated regex) here
  # so a malformed/path-traversal id can never be persisted for a node to execute, rather than
  # trusting the far-upstream request-path validation.
  defp validate_shard_id(changeset) do
    validate_change(changeset, :shard_id, fn :shard_id, id ->
      if Fathom.ShardId.valid?(id), do: [], else: [shard_id: "is not a valid shard id"]
    end)
  end
end
