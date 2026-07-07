defmodule Fathom.Repo.Migrations.CreateShardOverridesAndNodeKey do
  use Ecto.Migration

  def change do
    # Rebalancer node identity is a stable `node_key` (config :node_key / env NODE_KEY,
    # default node()) — the LB backend key a shard can be pinned to. Rename the sample
    # column from `owner` (which suggested the boot-scoped S3 lease owner) to make the
    # identity the rebalancer keys on explicit.
    rename table(:shard_load_samples), :owner, to: :node_key

    # The LB exception table: the source of truth for per-shard deviations from the pure
    # ketama hash. A row pins `shard_id`'s subdomain to `pinned_node` (a node_key); the
    # LB map renderer turns these into an nginx `map $host $target` layered on the hash.
    create table(:shard_overrides) do
      add :shard_id, :string, null: false
      add :pinned_node, :string, null: false
      add :reason, :string
      # Observability: the shard's rate when it was pinned, and where it came from.
      add :q_per_s_at_pin, :float
      add :from_node, :string

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:shard_overrides, [:shard_id])
    create index(:shard_overrides, [:pinned_node])
  end
end
