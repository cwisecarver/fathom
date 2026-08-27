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

  ## Cost side (`--cost`, expert review #20)

      mix fathom.rpo --cost [--rate 100] [--window-ms 20000] [--intervals 5000,30000]

  The complement to the RPO benefit: drives a continuously-write-active shard for
  the window at each interval and reports the flush RATE (flushes/s) and per-flush
  VACUUM-INTO+PUT duration (p50/p90/p99/max µs). A tighter interval flushes
  proportionally more often (≈ `window/interval`) at ~constant per-flush cost, so
  5s vs 30s is ~6× the VACUUM/PUT rate for a tighter RPO — the cost that buys the
  loss-window reduction the default mode measures.

  Storage backend: Local by default (prices the VACUUM + local copy only). Set
  `FATHOM_S3_TEST_ENDPOINT` to run against real S3 and price the PUT plus its
  Finch-pool contention with cold-opens. The chosen backend is printed with the
  results, and appears as `storage` in the JSON line — before expert review
  2026-08-26 #38 this text promised S3 while the harness always forced Local.

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

  @switches [
    rate: :integer,
    samples: :integer,
    intervals: :string,
    append: :string,
    cost: :boolean,
    window_ms: :integer
  ]

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

    # --cost measures the COST side (VACUUM/PUT rate + per-flush duration, expert review #20); the
    # default measures the RPO benefit (loss window). Two complementary views of the same knob.
    {result, printer} =
      if Keyword.get(opts, :cost, false) do
        cost_opts =
          opts
          |> Keyword.take([:rate, :window_ms])
          |> maybe_put(:intervals, intervals || [5_000, 30_000])

        {Fathom.Rpo.flush_cost(cost_opts), &print_cost/1}
      else
        measure_opts =
          opts
          |> Keyword.take([:rate, :samples])
          |> maybe_put(:intervals, intervals)

        {Fathom.Rpo.measure(measure_opts), &print/1}
      end

    # Report the backend rather than letting the reader assume (expert review 2026-08-26 #38). An
    # operator who exported FATHOM_S3_TEST_* and silently got a local file copy is the exact
    # failure that finding was, and a number is only as good as knowing what produced it.
    kind = Fathom.Rpo.storage_kind()

    IO.puts(
      :stderr,
      "storage: #{kind}" <>
        if(kind == :local,
          do: "  (VACUUM + local copy only — set FATHOM_S3_TEST_ENDPOINT to price a real PUT)",
          else: "  (real S3 PUT + Finch-pool contention)"
        )
    )

    result = Map.put(result, :storage, kind)

    try do
      printer.(result)
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

  defp print_cost(r) do
    Mix.shell().error("\n=== fathom.rpo --cost — flush cost (VACUUM+PUT) vs flush interval ===")

    Mix.shell().error("  #{r.rate_per_s} w/s sustained · #{r.window_ms} ms window/interval\n")

    Mix.shell().error(
      "  #{pad("interval", 14)}#{pad("flushes", 9)}#{pad("flushes/s", 11)}flush µs (VACUUM+PUT) p50/p90/p99/max"
    )

    Enum.each(r.intervals, fn row ->
      f = row.flush_us

      Mix.shell().error(
        "  #{pad("#{row.interval_ms} ms", 14)}#{pad(Integer.to_string(row.flushes), 9)}" <>
          "#{pad(:erlang.float_to_binary(row.flushes_per_s, decimals: 2), 11)}" <>
          "#{f.p50}/#{f.p90}/#{f.p99}/#{f.max}"
      )
    end)

    Mix.shell().error("")
  end

  defp pad(s, n), do: String.pad_trailing(to_string(s), n)
end
