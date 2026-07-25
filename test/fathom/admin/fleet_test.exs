defmodule Fathom.Admin.FleetTest do
  @moduledoc "Fleet Postgres roll-ups for the admin dashboard (control-plane reads, Ecto sandbox)."
  use Fathom.DataCase, async: true

  alias Fathom.Admin.Fleet
  alias Fathom.Directory
  alias Fathom.Rebalancer.{Commands, Nodes}

  test "count and count_by_status reflect directory rows by lifecycle status" do
    {:ok, _} = Directory.resolve("acme")
    {:ok, _} = Directory.resolve("globex")
    {:ok, _} = Directory.mark_failed("globex")

    assert Directory.count() == 2
    assert Directory.count_by_status() == %{"active" => 1, "migration_failed" => 1}
  end

  # Expert review 2026-07-24 #13: overview/0 ran count/0, count_by_status/0 and count_failed/0 as
  # three separate passes over `shards` — three scans of ~155 MB at 1M rows, every refresh, per
  # connected viewer. `status` is NOT NULL with a default, so one group-by yields all three exactly.
  test "overview derives the total and the failed count from a single status group-by" do
    {:ok, _} = Directory.resolve("acme")
    {:ok, _} = Directory.resolve("globex")
    {:ok, _} = Directory.resolve("initech")
    {:ok, _} = Directory.mark_failed("globex")
    {:ok, _} = Directory.tombstone("initech")

    overview = Fleet.overview()

    assert overview.total_shards == Directory.count(),
           "the derived total must equal a direct COUNT(*) across every status"

    assert overview.failed == Directory.count_failed(),
           "the derived failed count must equal a direct migration_failed count"

    assert overview.by_status == %{"active" => 1, "migration_failed" => 1, "deleted" => 1}
  end

  # `completed` is excluded so the panel's group-by does not scan the Pruner's 7-day retention of
  # completed jobs — millions of rows after a fleet rollout, counted on every dashboard refresh.
  test "oban_counts covers the actionable states and excludes completed" do
    assert is_list(Fleet.oban_counts())

    states =
      Fleet.oban_counts()
      |> Enum.map(& &1.state)
      |> Enum.uniq()

    refute "completed" in states
  end

  test "Nodes.all + node_roster tag alive nodes and carry the published p99" do
    :ok = Nodes.beat("fathom1", q_p99: 12.0, sample_count: 5)

    assert [%{node_key: "fathom1"}] = Nodes.all()

    assert [%{node_key: "fathom1", alive: true, q_p99: 12.0, sample_count: 5}] =
             Fleet.node_roster()
  end

  test "all_pending returns fleet-wide pending handoff commands" do
    {:ok, _} = Commands.issue("acme", "fathom1", "warm")
    assert [%{shard_id: "acme", node: "fathom1", status: "pending"}] = Commands.all_pending()
  end

  test "overview assembles the fleet roll-up map without raising" do
    {:ok, _} = Directory.resolve("acme")

    o = Fleet.overview()
    assert o.total_shards >= 1
    assert is_map(o.by_status)
    assert is_list(o.nodes)
    assert is_list(o.pins)
    assert is_list(o.pending_handoffs)
    assert is_list(o.oban)
  end

  test "migrations roll-up returns head, releases, laggards and quarantine" do
    m = Fleet.migrations()
    assert Map.has_key?(m, :head)
    assert is_list(m.releases)
    assert is_integer(m.laggard_count)
    assert is_list(m.failed_shards)
  end
end
