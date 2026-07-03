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
      assert result.fanout_rss_kb_per_shard >= 0
    after
      Fathom.Scale.cleanup()
    end
  end

  test "warm-density pre-pulls sized shards into the cache: disk-bound, cheap BEAM" do
    result = Fathom.Scale.warm_density(shards: 8, shard_size_mb: 1)

    try do
      assert result.cached == 8, "expected all 8 shards warmed into the cache"
      assert result.warm_disk_kb_per_shard >= 800, "cache holds ~1 MB/shard on disk"
      # The point of warm standby: a cached shard costs far less than an open
      # coordinator (~196 KiB BEAM + fds). At tiny N the BEAM figure is GC-noisy, so
      # just bound it well under the open-shard reference.
      assert result.warm_beam_kb_per_shard < result.open_shard_beam_kb_ref
      assert result.warm_pull_per_s > 0
    after
      Fathom.Scale.cleanup()
      Application.delete_env(:fathom, :warm_cache_dir)
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
