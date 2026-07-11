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
  @load_window_ms 60_000

  @doc "The overview page's fleet roll-up: shard counts, rollout state, node roster + load, pins, jobs."
  @spec overview() :: map()
  def overview do
    head = Migrator.head()

    %{
      total_shards: Directory.count(),
      by_status: Directory.count_by_status(),
      head_version: head,
      laggards: Directory.count_laggards(head),
      failed: Directory.count_failed(),
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

  @doc "The migrations page: fleet HEAD, release history, rollout laggards, and quarantined shards."
  @spec migrations() :: map()
  def migrations do
    head = Migrator.head()

    %{
      head: head,
      releases: Migrator.list(),
      laggard_count: Directory.count_laggards(head),
      laggards: Directory.laggards(head, 50),
      failed_shards: Directory.failed_shards()
    }
  end

  @doc "The fleet's currently-hot shards (merged from every node's reported load), newest rate wins."
  @spec hot_shards() :: [map()]
  def hot_shards, do: LoadSamples.latest_per_shard(@load_window_ms)

  @doc "Oban job counts grouped by `{queue, state}` — the background-jobs panel (Pruner-bounded)."
  @spec oban_counts() :: [%{queue: String.t(), state: String.t(), count: non_neg_integer()}]
  def oban_counts do
    from(j in Oban.Job, group_by: [j.queue, j.state], select: {j.queue, j.state, count(j.id)})
    |> Repo.all()
    |> Enum.map(fn {queue, state, count} -> %{queue: queue, state: state, count: count} end)
    |> Enum.sort_by(&{&1.queue, &1.state})
  rescue
    _ -> []
  end
end
