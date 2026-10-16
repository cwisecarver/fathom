defmodule Fathom.Rebalancer.PolicyTraceTest do
  @moduledoc """
  `Policy.propose_traced/4` — recording declined candidates for /admin/rebalancer must not change a
  decision (expert review 2026-10-15 #13). `propose/4` is implemented ON the traced path, so the
  identity test below pins that the moves are the same whatever the declines look like, and the
  guard tests pin which guard gets blamed. Pure, no DB.
  """
  use ExUnit.Case, async: true

  alias Fathom.Rebalancer.Policy

  @backends %{"n1" => "n1:8080", "n2" => "n2:8080", "n3" => "n3:8080"}
  @now ~U[2026-07-07 00:00:00.000000Z]

  defp s(node, shard, q, secs),
    do: %{
      node_key: node,
      shard_id: shard,
      q_per_s: q,
      sampled_at: DateTime.add(@now, -secs, :second)
    }

  defp pin(shard, node, secs),
    do: %{shard_id: shard, pinned_node: node, updated_at: DateTime.add(@now, -secs, :second)}

  defp opts(extra), do: Keyword.merge([now: @now, floor: 500.0, confirm_windows: 2], extra)

  defp samples do
    [
      s("n1", "hot_1", 900, 10),
      s("n1", "hot_1", 900, 0),
      s("n1", "hot_2", 800, 10),
      s("n1", "hot_2", 800, 0),
      s("n1", "once", 700, 0),
      s("n2", "warm", 50, 0),
      s("n3", "cool", 10, 0)
    ]
  end

  defp guards(declined), do: Map.new(declined, &{&1.shard_id, &1.guard})

  test "propose/4 returns exactly the moves of propose_traced/4 across configurations" do
    overrides = [pin("hot_2", "n2", 60)]

    for extra <- [
          [],
          [max_moves: 2],
          [max_moves: 0],
          [ineligible: MapSet.new(["hot_1"])],
          [floor: 100.0, confirm_windows: 1, max_moves: 5]
        ] do
      {moves, _declined} = Policy.propose_traced(samples(), overrides, @backends, opts(extra))
      assert moves == Policy.propose(samples(), overrides, @backends, opts(extra))
    end
  end

  test "an unconfirmed spike and a cooling shard are blamed on their guards" do
    overrides = [pin("hot_2", "n2", 60)]

    {[move], declined} = Policy.propose_traced(samples(), overrides, @backends, opts([]))

    assert move.shard_id == "hot_1"
    assert guards(declined)["once"] == :unconfirmed
    assert guards(declined)["hot_2"] == :cooldown
    refute Map.has_key?(guards(declined), "warm"), "a cold shard is not a 'decline'"
  end

  test "an ineligible shard is declined :ineligible" do
    {_, declined} =
      Policy.propose_traced(samples(), [], @backends, opts(ineligible: MapSet.new(["hot_1"])))

    assert guards(declined)["hot_1"] == :ineligible
  end

  test "max_moves 0 declines every candidate with :max_moves" do
    {[], declined} = Policy.propose_traced(samples(), [], @backends, opts(max_moves: 0))
    assert guards(declined)["hot_1"] == :max_moves
    assert guards(declined)["hot_2"] == :max_moves
  end

  test "max_moves reached declines the hot candidates the walk never reached" do
    {[%{shard_id: "hot_1"}], declined} =
      Policy.propose_traced(samples(), [], @backends, opts(max_moves: 1))

    assert guards(declined)["hot_2"] == :max_moves
  end

  test "a uniform fleet declines with :no_improving_target" do
    uniform = [
      s("n1", "a", 600, 10),
      s("n1", "a", 600, 0),
      s("n2", "b", 600, 10),
      s("n2", "b", 600, 0),
      s("n3", "c", 600, 10),
      s("n3", "c", 600, 0)
    ]

    {[], declined} = Policy.propose_traced(uniform, [], @backends, opts([]))
    assert declined != []
    assert Enum.all?(declined, &(&1.guard == :no_improving_target))
  end

  test "a source node that is not an LB backend is declined :not_backend" do
    off = [s("zz", "a", 900, 10), s("zz", "a", 900, 0)]

    assert {[], [%{guard: :not_backend, shard_id: "a"}]} =
             Policy.propose_traced(off, [], @backends, opts([]))
  end
end
