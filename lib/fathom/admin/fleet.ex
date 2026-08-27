defmodule Fathom.Admin.Fleet do
  @moduledoc """
  Fleet-wide roll-ups for the admin dashboard, read from the **Postgres control plane** — the only
  cross-node truth (there is no BEAM cluster). These are the slower, directory-scale reads a
  dashboard LiveView loads via `assign_async` (never on the realtime 1 s tick, which is per-node
  and lives in `Fathom.Admin.MetricsCollector`).

  Every read routes through the owning module (`Fathom.Directory`, `Fathom.Migrator`, the
  `Fathom.Rebalancer.*` readers) so directory/lifecycle logic stays in one place.
  """
  import Ecto.Query, only: [from: 2]

  alias Fathom.Directory
  alias Fathom.Migrator
  alias Fathom.Rebalancer.{Commands, LoadSamples, Nodes, Overrides}
  alias Fathom.Repo

  # A node is "alive" if it beat within this window; hot-set load is summed over this window.
  @node_alive_ms 30_000
  # How many quarantined shard ids the dashboard samples. Matches the `Enum.take(_, 40)` the
  # template used to do client-side over an unbounded list.
  @failed_sample 40

  @load_window_ms 60_000

  @doc "The overview page's fleet roll-up: shard counts, rollout state, node roster + load, pins, jobs."
  @spec overview() :: map()
  def overview do
    head = Migrator.head()

    # One group-by, three numbers (expert review 2026-07-24 #13). `count/0`, `count_by_status/0`
    # and `count_failed/0` were three separate passes over `shards` — at 1M rows that is three
    # scans of ~155 MB every refresh, per connected viewer. `status` is NOT NULL with a default,
    # so the group-by's counts sum to the exact total and carry the migration_failed count too.
    by_status = Directory.count_by_status()

    %{
      total_shards: by_status |> Map.values() |> Enum.sum(),
      by_status: by_status,
      head_version: head,
      laggards: Directory.count_laggards(head),
      failed: Map.get(by_status, "migration_failed", 0),
      nodes: node_roster(),
      node_load: LoadSamples.node_load(@load_window_ms),
      pins: Overrides.all(),
      pending_handoffs: Commands.all_pending(),
      oban: oban_counts()
    }
  end

  @doc "The live fleet roster: each node's beat freshness + published p99, tagged alive/stale."
  @spec node_roster() :: [map()]
  def node_roster do
    alive = Nodes.alive(@node_alive_ms)

    Nodes.all()
    |> Enum.map(fn n ->
      %{
        node_key: n.node_key,
        last_seen_at: n.last_seen_at,
        q_p99: n.q_p99,
        sample_count: n.sample_count,
        alive: MapSet.member?(alive, n.node_key)
      }
    end)
    |> Enum.sort_by(& &1.node_key)
  end

  @doc """
  The migrations page: fleet HEAD, release history, rollout laggards, quarantined shards, and the
  legible review blocks holding HEAD down.

  `review_blocks` is the dashboard half of expert review 2026-08-01 #26 — the API half
  (`GET /api/migrations/status`) shipped first because that is what CI reads, but the operator
  watching `laggards` never converge is looking at THIS page, and a bare "Fleet HEAD v1" with no
  explanation is exactly the illegibility the finding is about.

  Derived through `Migrator.pending_review/0` rather than filtered out of `releases` in memory: the
  "held" predicate (`requires_review and not yanked`) lives in the migrator, and a second copy here
  would drift the moment either flag's meaning changes.
  """
  @spec migrations() :: map()
  def migrations do
    head = Migrator.head()

    %{
      head: head,
      # Narrow projections, not full rows (expert review 2026-08-26 #30). This map is rebuilt every
      # 5 s per connected viewer; `Migrator.list/0` carried every captured migration's full DDL and
      # `Directory.failed_shards/0` was an unbounded select of full structs, while the template uses
      # three columns and a count-plus-40-ids respectively. Same shape as the two fixes recorded
      # above in this module.
      releases: Migrator.list_summary(),
      review_blocks: Enum.map(Migrator.pending_review(), &Migrator.review_block/1),
      laggard_count: Directory.count_laggards(head),
      laggards: Directory.laggards(head, 50),
      failed_shard_count: Directory.count_failed(),
      failed_shard_ids: Directory.failed_shard_ids(@failed_sample)
    }
  end

  @doc "The fleet's currently-hot shards (merged from every node's reported load), hottest first, top 50."
  @spec hot_shards() :: [Fathom.Rebalancer.LoadSample.t()]
  def hot_shards do
    @load_window_ms
    |> LoadSamples.latest_per_shard()
    |> Enum.sort_by(& &1.q_per_s, :desc)
    |> Enum.take(50)
  end

  # The job states an operator can act on. `completed` is deliberately EXCLUDED (expert review
  # 2026-07-24 #13): the Pruner retains completed jobs for 7 days, so after a fleet rollout that is
  # millions of rows, and counting them meant a multi-hundred-MB group-by over the whole
  # `oban_jobs` table on every dashboard refresh, per connected viewer. The remaining states are
  # served by Oban's own `state`-leading index. This does change the panel: a "completed" badge no
  # longer appears. That count was "how many jobs ran this week", not a health signal — the
  # actionable states are all still shown.
  @oban_live_states ~w(available scheduled executing retryable discarded cancelled)

  @doc """
  Oban job counts grouped by `{queue, state}` for the background-jobs panel.

  Bounded to the actionable states (`#{inspect(@oban_live_states)}`) — see the note above on why
  `completed` is excluded.
  """
  @spec oban_counts() :: [%{queue: String.t(), state: String.t(), count: non_neg_integer()}]
  def oban_counts do
    from(j in Oban.Job,
      where: j.state in ^@oban_live_states,
      group_by: [j.queue, j.state],
      select: {j.queue, j.state, count(j.id)}
    )
    |> Repo.all()
    |> Enum.map(fn {queue, state, count} -> %{queue: queue, state: state, count: count} end)
    |> Enum.sort_by(&{&1.queue, &1.state})
  rescue
    _ -> []
  end
end
