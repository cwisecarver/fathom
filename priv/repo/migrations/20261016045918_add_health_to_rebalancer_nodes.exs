defmodule Fathom.Repo.Migrations.AddHealthToRebalancerNodes do
  use Ecto.Migration

  # Per-node health snapshot (dirty shards, oldest unflushed age, open/cap, disk ratio, replication
  # slack, query p99 latency, draining, clock skew) published on the existing reporter beat so the
  # admin Nodes table is a fleet triage table (expert review 2026-10-15 #7).
  #
  # NULLABLE, no default, no backfill, and the safe direction: a node on an older release publishes
  # none, and readers render NULL as "unknown". Rollback drops the column; the beat path only writes
  # `health` when a snapshot is supplied, so an old node's beat never mentions it.
  def change do
    alter table(:rebalancer_nodes) do
      add :health, :map
    end
  end
end
