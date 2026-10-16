defmodule Fathom.Rebalancer.DecisionLogTest do
  # The log is one node-global named ETS table, so not async.
  use ExUnit.Case, async: false

  alias Fathom.Rebalancer.DecisionLog

  defp decline(i), do: %{shard_id: "s#{i}", node_key: "n1", q_per_s: i * 1.0, guard: :cooldown}

  defp move(i),
    do: %{
      shard_id: "m#{i}",
      from_node: "n1",
      to_node: "n2",
      q_per_s: 1.0,
      threshold: 0.5,
      reason: "r"
    }

  test "a quiet tick refreshes last_tick but stores no ring entry" do
    before = DecisionLog.recent()
    assert :ok = DecisionLog.record_tick([], [])
    assert DecisionLog.recent() == before
    assert %DateTime{} = DecisionLog.last_tick()
  end

  test "ticks are returned newest first and the ring is bounded" do
    for i <- 1..60, do: DecisionLog.record_tick([move(i)], [])
    recent = DecisionLog.recent()
    assert length(recent) == 50
    assert hd(recent).moves == [move(60)]
    assert recent == Enum.sort_by(recent, & &1.seq, :desc)
  end

  test "declines are capped per tick, hottest first, with the total kept" do
    DecisionLog.record_tick([], Enum.map(1..30, &decline/1))
    [t | _] = DecisionLog.recent()
    assert length(t.declined) == 20
    assert hd(t.declined).shard_id == "s30"
    assert t.declined_total == 30
  end

  test "lb_apply status round-trips" do
    DecisionLog.record_lb_apply(:reload_failed, error: "boom", hash: "abc", entries: 3)

    assert %{outcome: :reload_failed, error: "boom", hash: "abc", entries: 3, at: %DateTime{}} =
             DecisionLog.lb_apply_status()
  end
end
