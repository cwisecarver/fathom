defmodule Mix.Tasks.Fathom.Scale do
  @moduledoc """
  Scale test: provision N shards of ~S MB each and measure cold-open latency at
  real size plus fan-out node density (see `Fathom.Scale`).

      mix fathom.scale [--shards 1000] [--shard-size-mb 4] [--cold-open-samples 100]

  Provisions ~`shards * shard-size-mb` MB in a scratch dir, opens all N shards
  holding a live connection each, then cleans up. Holding N connections costs ~3
  fds each, so raise the limit first:

      ( ulimit -n 65536; mix fathom.scale --shards 1000 --shard-size-mb 4 )

  Two other modes:

      mix fathom.scale --ramp [--max 200000] [--checkpoint 10000]
      mix fathom.scale --lease-rps [--shards 5000] [--lease-ttl-ms 900] [--window-ms 3000]
      mix fathom.scale --hotspots [--shards 1000] [--zipf 1.1] [--queries 50000] [--workers N] [--stream-len 1]

  `--ramp` opens empty shards until the fd ceiling to find the node-density limit
  cheaply. `--lease-rps` measures that the lease-renewal storm is gone: per-shard
  renewals collapse to one node heartbeat (flat regardless of shard count) — see
  `Fathom.Scale.lease_rps/1`. `--hotspots` drives a Zipf-skewed query load and reads
  `Fathom.ShardLoad` to show whether hot shards are detectable + stable — the
  Phase-2 §B rebalancing evidence (see `Fathom.Scale.hotspots/1`).

  Prints a human table to stderr and one JSON line to stdout. Optional `--append PATH`
  records the JSON line. Run prod-compiled (`MIX_ENV=prod`) for representative numbers.
  """
  @shortdoc "Scale test: N shards of S MB — cold-open latency + fan-out node density"

  use Mix.Task

  # Load config + compile without starting the app; Fathom.Scale starts the
  # minimal subsystem it needs itself.
  @requirements ["app.config"]

  @switches [
    shards: :integer,
    shard_size_mb: :integer,
    cold_open_samples: :integer,
    append: :string,
    ramp: :boolean,
    max: :integer,
    checkpoint: :integer,
    lease_rps: :boolean,
    lease_ttl_ms: :integer,
    window_ms: :integer,
    warm_density: :boolean,
    hotspots: :boolean,
    zipf: :float,
    queries: :integer,
    workers: :integer,
    stream_len: :integer
  ]

  @impl true
  def run(argv) do
    {opts, _, _} = OptionParser.parse(argv, strict: @switches)
    Logger.configure(level: :warning)

    {result, printer} =
      cond do
        Keyword.get(opts, :lease_rps, false) ->
          {Fathom.Scale.lease_rps(Keyword.take(opts, [:shards, :lease_ttl_ms, :window_ms])),
           &print_lease_rps/1}

        Keyword.get(opts, :ramp, false) ->
          {Fathom.Scale.ramp(Keyword.take(opts, [:max, :checkpoint])), &print_ramp/1}

        Keyword.get(opts, :warm_density, false) ->
          {Fathom.Scale.warm_density(Keyword.take(opts, [:shards, :shard_size_mb])),
           &print_warm_density/1}

        Keyword.get(opts, :hotspots, false) ->
          {Fathom.Scale.hotspots(
             Keyword.take(opts, [:shards, :zipf, :queries, :workers, :stream_len])
           ), &print_hotspots/1}

        true ->
          {Fathom.Scale.run(Keyword.take(opts, [:shards, :shard_size_mb, :cold_open_samples])),
           &print_table/1}
      end

    try do
      printer.(result)
      json = Jason.encode!(result)
      Mix.shell().info(json)

      case Keyword.get(opts, :append) do
        nil -> :ok
        path -> File.write!(path, json <> "\n", [:append])
      end
    after
      Fathom.Scale.cleanup()
    end
  end

  defp print_lease_rps(r) do
    rows = [
      {"coordinators",
       "#{r.coordinators_started}/#{r.shards_requested} started (#{r.start_rate_per_s}/s)"},
      {"lease ttl / renew", "#{r.lease_ttl_ms} ms / every #{r.renew_interval_ms} ms"},
      {"window", "#{r.window_ms} ms"},
      {"per-shard renew RPS",
       "#{r.per_shard_renew_rps} /s (#{r.per_shard_renewals_observed} observed — the storm, now gone)"},
      {"node heartbeat RPS",
       "#{r.node_heartbeat_rps} /s (#{r.node_heartbeat_renewals_observed} observed — flat regardless of N)"},
      {"prod cadence", "every #{r.prod_renew_interval_s} s (default ttl 30s / 3)"},
      {"old model @this N", "#{r.legacy_projected_rps_per_node} PUT/s/node (per-shard renewal)"},
      {"old model @1M", "#{r.legacy_projected_rps_per_node_at_1m} PUT/s/node (the storm)"},
      {"heartbeat @1M", "#{r.heartbeat_rps_per_node_prod} PUT/s/node (one heartbeat, any N)"}
    ]

    Mix.shell().error(
      "\n=== fathom.scale --lease-rps (heartbeat collapses the lease-renewal storm) ==="
    )

    Enum.each(rows, fn {k, v} -> Mix.shell().error("  #{String.pad_trailing(k, 22)} #{v}") end)
    Mix.shell().error("")
  end

  defp print_hotspots(r) do
    top =
      r.top10
      |> Enum.map(fn t -> "#{t.shard_id}=#{t.q_per_s}/s" end)
      |> Enum.join(", ")

    median_sweep =
      r.thresholds
      |> Enum.map(fn t -> ">#{t.k}x: #{t.flagged} (recall #{t.zipf_recall})" end)
      |> Enum.join("  ")

    p99_sweep =
      r.thresholds_p99
      |> Enum.map(fn t -> ">#{t.k}x: #{t.flagged} (recall #{t.zipf_recall})" end)
      |> Enum.join("  ")

    abs_sweep =
      r.thresholds_absolute
      |> Enum.map(fn t ->
        "top#{t.top_n}=>#{t.floor_qps}/s: #{t.flagged} (recall #{t.zipf_recall})"
      end)
      |> Enum.join("  ")

    collapse =
      if r.median_collapsed,
        do: " — COLLAPSED (median≈0): use p99/absolute, not median",
        else: ""

    rows = [
      {"shards / zipf", "#{r.shards} shards, s=#{r.zipf_s}, ~#{r.queries_per_window} q/window"},
      {"drive rate",
       "#{r.queries_per_s} q/s (streams x #{r.stream_len} q, window #{r.window_a_s}s), #{r.active_shards} active"},
      {"rate p50/p90/p99",
       "#{r.rate_p50_qps} / #{r.rate_p90_qps} / #{r.rate_p99_qps} q/s (max #{r.rate_max_qps})"},
      {"separation",
       "max/median #{r.skew_ratio}x · max/p99 #{r.separation_over_p99}x#{collapse}"},
      {"top-20 recall",
       "diff #{r.top20_zipf_recall} · ShardLoad.top #{r.shardload_top20_zipf_recall}"},
      {"sweep >Kx median", median_sweep},
      {"sweep >Kx p99", p99_sweep},
      {"sweep absolute", abs_sweep},
      {"anti-flap top-20", "Jaccard #{r.flap_top20_jaccard} across the two windows"},
      {"hottest 10", top},
      {"verdict", r.verdict}
    ]

    Mix.shell().error("\n=== fathom.scale --hotspots (Phase-2 §B rebalancing evidence) ===")
    Enum.each(rows, fn {k, v} -> Mix.shell().error("  #{String.pad_trailing(k, 18)} #{v}") end)
    Mix.shell().error("")
  end

  defp print_warm_density(r) do
    rows = [
      {"cached", "#{r.cached}/#{r.shards_requested} shards @ ~#{r.shard_size_mb_actual} MB"},
      {"warm cache disk", "#{r.warm_cache_disk_mb} MB (#{r.warm_disk_kb_per_shard} KiB/shard)"},
      {"warm BEAM/shard", "#{r.warm_beam_kb_per_shard} KiB (follower cached-id set)"},
      {"warm RSS/shard", "#{r.warm_rss_kb_per_shard} KiB (noisy: incl. page cache)"},
      {"warming rate", "#{r.warm_pull_per_s} shards/s"},
      {"open-shard ref",
       "~#{r.open_shard_beam_kb_ref} KiB BEAM + ~#{r.open_shard_fds_ref} fds each — warm is disk-bound, open is BEAM/fd-bound"}
    ]

    Mix.shell().error(
      "\n=== fathom.scale --warm-density (warm cache: disk-bound, ~0 BEAM/fd) ==="
    )

    Enum.each(rows, fn {k, v} -> Mix.shell().error("  #{String.pad_trailing(k, 16)} #{v}") end)
    Mix.shell().error("")
  end

  defp print_ramp(r) do
    rows = [
      {"ceiling", "#{r.ceiling_shards} open shards"},
      {"limit", r.limit},
      {"fd predicted",
       "#{r.fd_predicted_ceiling} (kern.maxfilesperproc #{r.maxfilesperproc} / 3 fds)"},
      {"RSS at ceiling", "#{r.rss_at_ceiling_mb} MB"},
      {"RSS / shard", "#{r.rss_per_shard_kb} KiB (empty — floor; active ~196 KiB at 4MB)"},
      {"BEAM / shard", "#{r.beam_per_shard_kb} KiB"}
    ]

    Mix.shell().error("\n=== fathom.scale --ramp (node-density ceiling) ===")
    Enum.each(rows, fn {k, v} -> Mix.shell().error("  #{String.pad_trailing(k, 16)} #{v}") end)
    Mix.shell().error("")
  end

  defp print_table(r) do
    fd_note =
      if r.fanout_opened < r.shards,
        do: "  (stopped at #{r.fanout_opened}/#{r.shards} — fd ceiling; raise ulimit -n)",
        else: ""

    rows = [
      {"shards x size", "#{r.shards} x ~#{r.shard_size_mb_actual} MB (#{r.disk_mb} MB on disk)"},
      {"provision", "#{r.provision_s} s"},
      {"cold_open p50", "#{us(r.cold_open_p50_us)}  (checkout + open + first query, real size)"},
      {"cold_open p99", "#{us(r.cold_open_p99_us)}"},
      {"fan-out opened", "#{r.fanout_opened}#{fd_note}"},
      {"fan-out BEAM/shard", "#{r.fanout_beam_kb_per_shard} KiB"},
      {"fan-out RSS/shard", "#{r.fanout_rss_kb_per_shard} KiB"},
      {"fan-out total RSS", "#{r.fanout_total_rss_mb} MB"},
      {"fan-out open rate", "#{r.fanout_open_per_s} shards/s"}
    ]

    Mix.shell().error("\n=== fathom.scale ===")
    Enum.each(rows, fn {k, v} -> Mix.shell().error("  #{String.pad_trailing(k, 20)} #{v}") end)
    Mix.shell().error("")
  end

  defp us(n) when is_number(n), do: "#{Float.round(n / 1000, 2)} ms"
  defp us(_), do: "n/a"
end
