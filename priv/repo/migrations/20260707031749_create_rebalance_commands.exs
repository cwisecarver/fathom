defmodule Fathom.Repo.Migrations.CreateRebalanceCommands do
  use Ecto.Migration

  # The cross-node command channel for handoffs. There's no BEAM cluster, so the
  # orchestrator (which runs on one node) can't call warm/drain on another node
  # directly. It writes a command here; each node's Fathom.Rebalancer.CommandPoller
  # acts on the commands addressed to its own node_key and marks them done. Postgres
  # is already the orchestration substrate (Oban, the directory), so this is
  # control-plane coordination, not a new data-plane channel.
  def change do
    create table(:rebalance_commands) do
      add :shard_id, :string, null: false
      add :node, :string, null: false
      add :command, :string, null: false
      add :status, :string, null: false, default: "pending"
      add :detail, :string

      timestamps(type: :utc_datetime_usec)
    end

    # The poller's query: pending commands for my node.
    create index(:rebalance_commands, [:node, :status])
  end
end
