defmodule Fathom.Repo.Migrations.AddQHistToRebalancerNodes do
  use Ecto.Migration

  # Review 2026-07-09 #4: the fleet hot bar was a count-weighted mean of per-node p99s, which
  # is not the pooled-distribution p99. Each reporter now publishes a compact fixed-bucket
  # histogram of its per-shard q/s; the orchestrator sums them and reads the TRUE pooled p99.
  # A Postgres integer array (bounded to Fathom.Rebalancer.Stats.bucket_count/0 buckets),
  # nullable so a liveness-only beat/1 or a pre-migration row simply doesn't contribute.
  def change do
    alter table(:rebalancer_nodes) do
      add :q_hist, {:array, :integer}
    end
  end
end
