defmodule Fathom.Rebalancer.LoadSamplesTest do
  @moduledoc """
  Reader over `shard_load_samples` — the merged, fleet-wide load view the rebalance
  policy consumes. Async: rows go through the Ecto SQL sandbox (auto-rollback).
  """
  use Fathom.DataCase, async: true

  alias Fathom.Rebalancer.{LoadSample, LoadSamples}
  alias Fathom.Repo

  # Insert a sample from node `node_key` for `shard`, `age_ms` ago at rate `q`.
  defp sample(node_key, shard, q, age_ms) do
    at = DateTime.add(DateTime.utc_now(), -age_ms, :millisecond)

    Repo.insert!(%LoadSample{
      node_key: node_key,
      shard_id: shard,
      q_per_s: q,
      rows_read_per_s: q * 8,
      checkouts_per_s: q / 4,
      window_s: 10.0,
      sampled_at: at
    })
  end

  test "latest_per_shard keeps the newest sample per shard (the current rate + owner)" do
    sample("nodeA#1", "hot_1", 10.0, 30_000)
    sample("nodeA#1", "hot_1", 90.0, 1_000)
    sample("nodeB#1", "hot_2", 40.0, 1_000)

    latest = LoadSamples.latest_per_shard(120_000) |> Map.new(&{&1.shard_id, &1})

    assert map_size(latest) == 2
    assert latest["hot_1"].q_per_s == 90.0, "newest hot_1 sample wins"
    assert latest["hot_1"].node_key == "nodeA#1"
    assert latest["hot_2"].q_per_s == 40.0
  end

  test "node_load sums each owner's shards' current rates (target-selection input)" do
    sample("nodeA#1", "hot_1", 90.0, 1_000)
    sample("nodeA#1", "hot_3", 10.0, 1_000)
    sample("nodeB#1", "hot_2", 40.0, 1_000)

    load = LoadSamples.node_load(120_000)

    assert load["nodeA#1"] == 100.0
    assert load["nodeB#1"] == 40.0
  end

  test "since/prune are time-windowed (anti-flap history bounded, old rows dropped)" do
    sample("nodeA#1", "hot_1", 50.0, 5_000)
    sample("nodeA#1", "hot_1", 60.0, 90_000)

    assert length(LoadSamples.since(10_000)) == 1, "only the 5s-old sample is within 10s"
    assert length(LoadSamples.since(120_000)) == 2

    {deleted, _} = LoadSamples.prune(30_000)
    assert deleted == 1, "the 90s-old sample is pruned"
    assert length(LoadSamples.since(120_000)) == 1
  end
end
