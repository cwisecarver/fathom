defmodule Fathom.ScaleTest do
  @moduledoc """
  Tiny smoke of the scale harness (`Fathom.Scale` / `mix fathom.scale`), tagged
  `:bench` so it's excluded from the default suite. Asserts the sweep runs and
  produces sane shapes at small scale — not absolute performance.

      mix test --include bench test/fathom/scale_test.exs
  """
  use ExUnit.Case, async: false

  @moduletag :bench

  test "scale sweep provisions sized shards, cold-opens, and fans out" do
    result = Fathom.Scale.run(shards: 8, shard_size_mb: 1, cold_open_samples: 3)

    try do
      assert result.shards == 8
      assert result.fanout_opened == 8, "expected all 8 shards to open (fd ceiling?)"
      assert result.shard_size_mb_actual >= 0.8, "shards should be ~1 MB on disk"
      assert result.cold_open_p50_us > 0
      # RSS/shard is a cross-test RSS delta — meaningful only at scale; at N=8 it's GC
      # noise and can go negative when a prior test left the RSS baseline high (same
      # caveat the ramp / warm-density cases note). Assert it's present, not its sign.
      assert is_integer(result.fanout_rss_kb_per_shard)
    after
      Fathom.Scale.cleanup()
    end
  end

  test "warm-density pre-pulls sized shards into the cache: disk-bound, cheap BEAM" do
    result = Fathom.Scale.warm_density(shards: 8, shard_size_mb: 1)

    try do
      assert result.cached == 8, "expected all 8 shards warmed into the cache"
      assert result.warm_disk_kb_per_shard >= 800, "cache holds ~1 MB/shard on disk"
      # The point of warm standby — a cached shard costs far less BEAM than an open
      # coordinator (~196 KiB) — is a *scale* claim: at N=8 the per-shard BEAM delta is
      # pure GC noise (can land at 197 or go negative when a prior test left the heap
      # high), so assert it's present, not its magnitude. `mix fathom.scale --warm-density`
      # measures the real warm-vs-open gap. (Same GC-noise caveat as the fan-out RSS case.)
      assert is_integer(result.warm_beam_kb_per_shard)
      assert result.warm_pull_per_s > 0
    after
      Fathom.Scale.cleanup()
      Application.delete_env(:fathom, :warm_cache_dir)
    end
  end

  test "hotspots: a Zipf drive yields a detectable, stable hot set via ShardLoad" do
    # The harness flips :shard_load on and pins the cap/idle for the run; snapshot
    # those so this test doesn't leak global config into the rest of the suite.
    keys = [:shard_load, :max_open_shards, :shard_idle_ms]
    prev = Map.new(keys, &{&1, Application.get_env(:fathom, &1)})

    result = Fathom.Scale.hotspots(shards: 50, queries: 10_000, zipf: 1.2)

    try do
      assert result.mode == "hotspots"
      assert result.shards == 50
      assert result.active_shards > 0

      # The Zipf head dominates: the hottest shard's rate is many times the median —
      # the separation a rebalancer keys on. Loose bound so RNG never flakes it.
      assert result.skew_ratio > 5.0
      assert result.rate_max_qps > result.rate_p50_qps

      # The hottest shard by weight (hot_1) lands at/near the observed top.
      top_ids = Enum.map(result.top10, & &1.shard_id)
      assert "hot_1" in Enum.take(top_ids, 3)

      # The shipped read API a rebalancer would call (ShardLoad.top/2) recovers most
      # of the true head — proves the counters are usable, not just my diff.
      assert result.shardload_top20_zipf_recall >= 0.7

      # The absolute floor set to isolate the top-5 is a real positive q/s rate and flags
      # at least those 5, mostly the true head — the portable, scale-robust threshold
      # shape (a fixed q/s, not a distribution ratio). (The p99 sweep is a *scale* tool:
      # at this small N the p99 sits on the head, so >Kx-p99 flags nothing — that's why
      # the absolute floor, not p99, is what we assert here.)
      top5 = Enum.find(result.thresholds_absolute, &(&1.top_n == 5))
      assert top5.floor_qps > 0.0
      assert top5.flagged >= 5
      assert top5.zipf_recall >= 0.6

      # All three threshold families are reported (median / p99 / absolute).
      assert length(result.thresholds) == 3
      assert length(result.thresholds_p99) == 3
      assert length(result.thresholds_absolute) == 4

      # At this small N the cold tail doesn't dominate, so the median stays meaningful —
      # median-relative only collapses at fleet scale (the --hotspots finding).
      refute result.median_collapsed

      # Two windows keep most of the top-20 hottest shards — the scale-robust anti-flap
      # signal (a flapping top set would need hysteresis).
      assert result.flap_top20_jaccard >= 0.4
    after
      Fathom.Scale.cleanup()
      Fathom.ShardLoad.reset()

      Enum.each(prev, fn
        {k, nil} -> Application.delete_env(:fathom, k)
        {k, v} -> Application.put_env(:fathom, k, v)
      end)
    end
  end

  test "ramp opens empty shards and reports a ceiling + fd prediction" do
    result = Fathom.Scale.ramp(max: 5, checkpoint: 100)

    try do
      # 5 is far below the fd ceiling, so it reaches --max without failing.
      assert result.ceiling_shards == 5
      assert result.limit =~ "reached --max"
      assert result.maxfilesperproc > 0
      assert result.fd_predicted_ceiling == div(result.maxfilesperproc, 3)
      # rss_per_shard is meaningful only at scale; at 5 shards it's GC noise
      # (can be negative), so just assert the metric is present.
      assert is_integer(result.rss_per_shard_kb)
    after
      Fathom.Scale.cleanup()
    end
  end
end
