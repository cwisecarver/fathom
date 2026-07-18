defmodule Mix.Tasks.Fathom.Rpo do
  @moduledoc """
  Loss-window (RPO) measurement: how much a shard loses on node/disk loss, as a
  function of the flush-interval knob (`:shard_flush_interval_ms`) and the write
  rate — plus a process-crash check that a disk-intact crash loses nothing.

      mix fathom.rpo [--rate 100] [--samples 30] [--intervals 0,5000,30000]

  For each interval it drives a steady write stream and, at points spread across
  the flush sawtooth, compares the acked writes against what a survivor would
  cold-open (the last flushed object). Reports lost rows + lost window (ms) as a
  p50/p90/p99/max distribution — the RPO curve. `--intervals 0` is idle-only (an
  unbounded window for a never-idle shard); the periodic flush bounds it.

  The process-crash line writes N rows with the flush disabled, hard-kills the
  coordinator, re-opens on the same disk, and confirms `synchronous=FULL` loses
  nothing. See `Fathom.Rpo` and `docs/durability.md`.

  Prints a human table to stderr and one JSON line to stdout. Optional
  `--append PATH` records the JSON line. Run prod-compiled (`MIX_ENV=prod`) for
  representative numbers; the multi-node / real-disk complement is
  `deploy/chaos/chaos.sh soak`.
  """
  @shortdoc "Loss-window (RPO) measurement: lost rows/seconds vs the flush interval"

  use Mix.Task

  # Load config + compile without starting the app; Fathom.Rpo starts the minimal
  # subsystem it needs itself (as Fathom.Scale does).
  @requirements ["app.config"]

  @switches [rate: :integer, samples: :integer, intervals: :string, append: :string]

  @impl true
  def run(argv) do
    {opts, _, _} = OptionParser.parse(argv, strict: @switches)
    Logger.configure(level: :warning)

    intervals =
      case Keyword.get(opts, :intervals) do
        nil ->
          nil

        s ->
          s |> String.split(",", trim: true) |> Enum.map(&String.to_integer(String.trim(&1)))
      end

    measure_opts =
      opts
      |> Keyword.take([:rate, :samples])
      |> maybe_put(:intervals, intervals)

    result = Fathom.Rpo.measure(measure_opts)

    try do
      print(result)
      json = Jason.encode!(result)
      Mix.shell().info(json)

      case Keyword.get(opts, :append) do
        nil -> :ok
        path -> File.write!(path, json <> "\n", [:append])
      end
    after
      Fathom.Rpo.cleanup()
    end
  end

  defp maybe_put(kw, _key, nil), do: kw
  defp maybe_put(kw, key, value), do: Keyword.put(kw, key, value)

  defp print(r) do
    Mix.shell().error("\n=== fathom.rpo — loss window (RPO) vs flush interval ===")

    Mix.shell().error(
      "  #{r.rate_per_s} w/s · #{r.samples_per_interval} samples/interval · synchronous=#{r.synchronous}\n"
    )

    pk = r.process_kill

    ok = if pk.lost_rows == 0, do: "✓", else: "✗ LOSS"

    Mix.shell().error(
      "  process crash (disk intact): wrote #{pk.written}, survived #{pk.survived}, lost #{pk.lost_rows}  #{ok}"
    )

    Mix.shell().error("  node loss (survivor cold-opens the last flushed object):\n")

    Mix.shell().error(
      "  #{pad("interval", 14)}#{pad("flushes", 9)}#{pad("lost rows p50/p90/p99/max", 30)}lost window ms p50/p90/p99/max"
    )

    Enum.each(r.intervals, fn row ->
      label = if row.interval_ms == 0, do: "idle-only", else: "#{row.interval_ms} ms"
      lr = row.lost_rows
      lm = row.lost_ms

      Mix.shell().error(
        "  #{pad(label, 14)}#{pad(Integer.to_string(row.distinct_flush_points), 9)}" <>
          "#{pad("#{lr.p50}/#{lr.p90}/#{lr.p99}/#{lr.max}", 30)}#{lm.p50}/#{lm.p90}/#{lm.p99}/#{lm.max}"
      )
    end)

    Mix.shell().error("")
  end

  defp pad(s, n), do: String.pad_trailing(to_string(s), n)
end
