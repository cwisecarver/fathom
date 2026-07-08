defmodule Fathom.Rebalancer.PolicyTest do
  @moduledoc "The rebalance decision logic — pure, no DB."
  use ExUnit.Case, async: true

  alias Fathom.Rebalancer.Policy

  @backends %{"n1" => "n1:8080", "n2" => "n2:8080", "n3" => "n3:8080"}
  @now ~U[2026-07-07 00:00:00.000000Z]

  # A load sample from `node` for `shard` at `q` q/s, `secs` before @now.
  defp s(node, shard, q, secs) do
    %{node_key: node, shard_id: shard, q_per_s: q, sampled_at: DateTime.add(@now, -secs, :second)}
  end

  # An override pinned `secs` before @now (for cooldown).
  defp pin(shard, node, secs) do
    %{shard_id: shard, pinned_node: node, updated_at: DateTime.add(@now, -secs, :second)}
  end

  defp propose(samples, overrides \\ [], opts) do
    Policy.propose(samples, overrides, @backends, Keyword.put(opts, :now, @now))
  end

  test "moves a confirmed hot shard to the least-loaded node" do
    # n1 is overloaded (hot_1 900 + hot_2 100 = 1000); n2 has 50, n3 has 10 (coldest).
    samples = [
      s("n1", "hot_1", 900, 10),
      s("n1", "hot_1", 900, 0),
      s("n1", "hot_2", 100, 0),
      s("n2", "warm", 50, 0),
      s("n3", "cool", 10, 0)
    ]

    assert [move] = propose(samples, floor: 500.0, confirm_windows: 2)
    assert move.shard_id == "hot_1"
    assert move.from_node == "n1"
    assert move.to_node == "n3", "picks the least-loaded backend"
    assert move.q_per_s == 900.0
  end

  test "anti-flap: a one-window spike (< confirm_windows) is not moved" do
    samples = [
      s("n1", "spike", 900, 0),
      s("n2", "warm", 50, 0)
    ]

    assert propose(samples, floor: 500.0, confirm_windows: 2) == []
  end

  test "cooldown: a recently-pinned shard is skipped even if still hot" do
    # n1 carries residual load beyond hot_1, so moving hot_1 genuinely relieves it.
    samples = [
      s("n1", "hot_1", 900, 10),
      s("n1", "hot_1", 900, 0),
      s("n1", "filler", 300, 0),
      s("n3", "cool", 10, 0)
    ]

    # Pinned 60s ago, cooldown 300s -> still cooling.
    overrides = [pin("hot_1", "n1", 60)]

    assert propose(samples, overrides, floor: 500.0, confirm_windows: 2, cooldown_ms: 300_000) ==
             []

    # Pinned 600s ago -> cooldown elapsed, move allowed.
    old = [pin("hot_1", "n1", 600)]
    assert [_move] = propose(samples, old, floor: 500.0, confirm_windows: 2, cooldown_ms: 300_000)
  end

  test "improvement guard: uniform fleet yields no moves (nowhere better to put it)" do
    # Every node ~equally loaded; moving hot_1 anywhere makes the target as hot as n1 is.
    samples = [
      s("n1", "hot_1", 300, 10),
      s("n1", "hot_1", 300, 0),
      s("n2", "b", 300, 0),
      s("n3", "c", 300, 0)
    ]

    assert propose(samples, floor: 100.0, confirm_windows: 2) == []
  end

  test "p99-relative threshold flags a sharp head and skips a uniform fleet" do
    # A long cold tail (100 shards @ ~1 q/s) makes fleet-p99 ~1, so 20x p99 ~20; hot_1
    # @ 500 clears it. The tail sits on n1 too, so n1 is overloaded and the move helps.
    tail = for i <- 1..100, do: s("n1", "cold_#{i}", 1.0, 0)

    hot = [s("n1", "hot_1", 500, 10), s("n1", "hot_1", 500, 0)]
    assert [move] = propose(hot ++ tail, p99_multiple: 20, confirm_windows: 2)
    assert move.shard_id == "hot_1"

    # Same shape but uniform (all ~equal): p99 ~= max, 20x p99 huge, nothing flagged.
    uniform = for i <- 1..50, do: s("n1", "u_#{i}", 100.0, 0)
    assert propose(uniform, p99_multiple: 20, confirm_windows: 1) == []
  end

  test "max_moves caps per tick and spreads across targets (no new hotspot)" do
    # n1 hashed several hot shards (+ filler so it stays overloaded after the first move);
    # n2 and n3 are cold. With max_moves: 2 the two hottest fan out to different nodes.
    samples = [
      s("n1", "hot_a", 800, 10),
      s("n1", "hot_a", 800, 0),
      s("n1", "hot_b", 700, 10),
      s("n1", "hot_b", 700, 0),
      s("n1", "filler", 1000, 0),
      s("n2", "x", 5, 0),
      s("n3", "y", 10, 0)
    ]

    moves = propose(samples, floor: 500.0, confirm_windows: 2, max_moves: 2)
    assert length(moves) == 2
    targets = moves |> Enum.map(& &1.to_node) |> Enum.sort()
    assert targets == ["n2", "n3"], "two moves fan out to distinct targets"

    # Default max_moves: 1 -> only the hottest moves.
    assert [one] = propose(samples, floor: 500.0, confirm_windows: 2)
    assert one.shard_id == "hot_a"
  end

  test "anti-flap counts distinct windows on the candidate's node, not cross-node rows (#9)" do
    # A shard mid-remap is reported hot by both its old (n1) and current (n2) serving node —
    # one hot window each. Counting rows across nodes reaches confirm: 2 spuriously; counting
    # distinct windows on the CURRENT serving node (n2, the latest sample) sees only 1, so
    # the transient isn't moved. Filler keeps n2 overloaded so a target genuinely exists
    # (isolating the anti-flap fix, not the improvement guard).
    samples = [
      s("n1", "remap", 900, 10),
      s("n2", "remap", 900, 0),
      s("n2", "filler", 500, 0),
      s("n3", "cool", 10, 0)
    ]

    assert propose(samples, floor: 500.0, confirm_windows: 2) == []
  end

  test "ignores shards whose current node isn't a known LB backend" do
    samples = [
      s("ghost", "hot_1", 900, 10),
      s("ghost", "hot_1", 900, 0),
      s("n3", "cool", 10, 0)
    ]

    assert propose(samples, floor: 500.0, confirm_windows: 2) == []
  end
end
