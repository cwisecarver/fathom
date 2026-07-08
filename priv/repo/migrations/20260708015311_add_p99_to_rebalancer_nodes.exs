defmodule Fathom.Repo.Migrations.AddP99ToRebalancerNodes do
  use Ecto.Migration

  # Fleet-relative p99 (finding #2). Each node publishes, alongside its liveness beat, the
  # p99 of its FULL rate distribution (pre-top-N-truncation) and the sample count. The
  # RebalanceJob reads the median across live nodes as the fleet hot bar, instead of the
  # policy recomputing p99 from the truncated top-N head (systematically too high). Nullable:
  # a node that hasn't computed a window yet just has no p99 (ignored by the fleet median).
  def change do
    alter table(:rebalancer_nodes) do
      add :q_p99, :float
      add :sample_count, :integer
    end
  end
end
