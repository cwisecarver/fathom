defmodule Fathom.Rebalancer.WarmLocationsTest do
  @moduledoc "The warm-location signal (affinity-aware target input, Phase 2 C)."
  use Fathom.DataCase, async: true

  alias Fathom.Rebalancer.{WarmLocation, WarmLocations}

  test "publish upserts, retracts, and empties; warm_nodes maps shard → nodes" do
    :ok = WarmLocations.publish("n1", ["a", "b"])
    :ok = WarmLocations.publish("n2", ["a"])

    wn = WarmLocations.warm_nodes(60_000)
    assert wn["a"] == MapSet.new(["n1", "n2"])
    assert wn["b"] == MapSet.new(["n1"])

    # n1 stops warming "b" — its advertisement for "b" is retracted, "a" kept.
    :ok = WarmLocations.publish("n1", ["a"])
    wn = WarmLocations.warm_nodes(60_000)
    assert wn["a"] == MapSet.new(["n1", "n2"])
    refute Map.has_key?(wn, "b")

    # Empty publish drops all of a node's rows.
    :ok = WarmLocations.publish("n2", [])
    assert WarmLocations.warm_nodes(60_000)["a"] == MapSet.new(["n1"])
  end

  test "a re-publish refreshes kept rows so they survive the age-sweep retract (#14)" do
    # Retract is now an age sweep (`updated_at < now`, a constant 2-param query) rather than
    # `shard_id not in ^ids` (an unbounded bind list). It works because the upsert stamps a
    # fresh `now` on every kept row: kept rows survive the sweep, unrefreshed ones are dropped.
    :ok = WarmLocations.publish("n1", ["a", "b"])
    a0 = Repo.get_by!(WarmLocation, node_key: "n1", shard_id: "a").updated_at

    :ok = WarmLocations.publish("n1", ["a", "c"])

    wn = WarmLocations.warm_nodes(60_000)
    assert wn["a"] == MapSet.new(["n1"]), "kept + refreshed"
    assert wn["c"] == MapSet.new(["n1"]), "newly added"
    refute Map.has_key?(wn, "b"), "not refreshed ⇒ age-swept"

    a1 = Repo.get_by!(WarmLocation, node_key: "n1", shard_id: "a").updated_at
    assert DateTime.compare(a1, a0) == :gt, "kept row's updated_at bumped past the sweep cutoff"
  end

  test "publish drops invalid shard_ids before insert_all (#6)" do
    # insert_all bypasses the changeset, and a warm-location row feeds affinity target
    # selection — an invalid id must never persist. Valid ids in the same call still publish.
    :ok = WarmLocations.publish("n1", ["ok_1", "../evil", "a/b", "also_ok"])

    wn = WarmLocations.warm_nodes(60_000)
    assert wn["ok_1"] == MapSet.new(["n1"])
    assert wn["also_ok"] == MapSet.new(["n1"])
    refute Map.has_key?(wn, "../evil")
    refute Map.has_key?(wn, "a/b")

    # An all-invalid list retracts the node's set (nothing valid to advertise).
    :ok = WarmLocations.publish("n1", ["../evil"])
    assert WarmLocations.warm_nodes(60_000) == %{}
  end

  # Expert review 2026-07-24 #27: prune is now NODE-SCOPED (every node used to delete the whole
  # expired set every window, so N-1 of N nodes scanned and then blocked on row locks to delete
  # nothing), with a rare fleet-wide sweep that still reclaims a DEPARTED node's rows — scoping
  # alone would strand those forever. These pin both halves.
  test "warm_nodes ignores stale rows; a node prunes its own" do
    key = Fathom.Rebalancer.node_key()
    :ok = WarmLocations.publish(key, ["a"])

    from(w in WarmLocation, where: w.node_key == ^key)
    |> Repo.update_all(
      set: [updated_at: DateTime.add(DateTime.utc_now(), -120_000, :millisecond)]
    )

    # An unrefreshed row falls out of the read window.
    assert WarmLocations.warm_nodes(60_000) == %{}
    assert WarmLocations.prune(60_000) == 1
    assert WarmLocations.warm_nodes(600_000) == %{}
  end

  test "a departed node's rows are reclaimed by the fleet sweep, not by the per-node prune" do
    prev = Application.get_env(:fathom, :rebalancer_fleet_sweep_odds)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :rebalancer_fleet_sweep_odds),
        else: Application.put_env(:fathom, :rebalancer_fleet_sweep_odds, prev)
    end)

    # A node that is not this one, i.e. one that has departed the fleet.
    :ok = WarmLocations.publish("departed_node", ["a"])

    from(w in WarmLocation, where: w.node_key == "departed_node")
    |> Repo.update_all(
      set: [updated_at: DateTime.add(DateTime.utc_now(), -600_000, :millisecond)]
    )

    # Sweep off: the node-scoped prune must NOT touch another node's rows — that disjointness is
    # the whole point, since it is what removes the cross-node lock contention.
    Application.put_env(:fathom, :rebalancer_fleet_sweep_odds, 0)
    assert WarmLocations.prune(60_000) == 0
    assert WarmLocations.warm_nodes(3_600_000) != %{}, "another node's row must survive"

    # Sweep on: the departed node's rows are reclaimed, so scoping cannot strand them forever.
    Application.put_env(:fathom, :rebalancer_fleet_sweep_odds, 1)
    _ = WarmLocations.prune(60_000)
    assert WarmLocations.warm_nodes(3_600_000) == %{}
  end
end
