defmodule Mix.Tasks.Fathom.WireBench do
  @shortdoc "Wire (loopback Hrana WS) benches — MIX_ENV=test only"
  @moduledoc """
  Runs the wire benches (docs/tpc-benchmark-plan.md Phase 1) through the full Hrana WebSocket
  path (a real Mint.WebSocket client → `Filo.Socket` → `ShardExecutor` → shard → back).

  **Test-env only** — the WS client is a dev/test dep that never ships prod, so this is NOT
  part of the prod per-commit gate (`commit_with_bench.sh`). It's the where-wire-metrics-are-
  gated surface: run it manually or as a CI pre-merge step.

      MIX_ENV=test mix fathom.wire_bench
      MIX_ENV=test mix fathom.wire_bench --append --check

  Options:
    * `--append`             append the JSON result line to `scripts/wire_history.jsonl`
    * `--check`              gate: compare each wire metric to the last same-host entry in the
                             history and exit 1 on a >=20% regression
    * `--hrana-rt-samples N` SELECT-1 round-trips to time for `hrana_rt_us` (default 200)
  """
  use Mix.Task

  @history "scripts/wire_history.jsonl"
  @block 20

  # metric => :higher_worse | :lower_worse (mirrors Fathom.Bench.Gate's direction convention).
  @directions %{hrana_rt_us: :higher_worse}

  @impl true
  def run(argv) do
    unless Mix.env() == :test do
      Mix.raise("fathom.wire_bench runs in MIX_ENV=test only (the WS client is a dev/test dep)")
    end

    {opts, _, _} =
      OptionParser.parse(argv,
        strict: [append: :boolean, check: :boolean, hrana_rt_samples: :integer]
      )

    Mix.Task.run("app.start")

    bench_opts = for k <- [:hrana_rt_samples], v = opts[k], do: {k, v}

    metrics = %{hrana_rt_us: Float.round(Fathom.Bench.Wire.hrana_rt(bench_opts), 1)}
    line = Map.merge(meta(), metrics)

    print(metrics, line)

    # Read the baseline BEFORE appending, so the new line is never its own baseline.
    baseline = last_same_host(line.host)
    if opts[:append], do: File.write!(@history, Jason.encode!(line) <> "\n", [:append])
    if opts[:check], do: gate(baseline, metrics)
  end

  defp meta do
    %{
      ts: DateTime.utc_now() |> DateTime.to_iso8601(),
      commit: git(["rev-parse", "--short", "HEAD"], "unknown"),
      commit_full: git(["rev-parse", "HEAD"], "unknown"),
      branch: git(["rev-parse", "--abbrev-ref", "HEAD"], "unknown"),
      dirty: git(["status", "--porcelain"], "") != "",
      host: to_string(:os.type() |> elem(1)),
      mix_env: "test"
    }
  end

  defp print(metrics, line) do
    IO.puts(
      "\n=== fathom.wire_bench (#{line.commit}#{if line.dirty, do: "-dirty", else: ""}, #{line.host}) ==="
    )

    IO.puts(
      "  hrana_rt_us  #{fmt(metrics.hrana_rt_us)}  µs   (warm SELECT 1 round-trip over the WS wire)"
    )

    IO.puts(Jason.encode!(line))
  end

  # The wire-run gate: compare each metric to the last same-host history entry, block >=20%.
  defp gate(nil, _metrics) do
    IO.puts(
      "\n=== gate ===\n  no same-host baseline in #{@history} — record one (--append) then re-run --check"
    )
  end

  defp gate(baseline, metrics) do
    IO.puts("\n=== gate vs last same-host wire run ===")

    regressed =
      Enum.reduce(@directions, false, fn {metric, dir}, acc ->
        new = Map.fetch!(metrics, metric)
        old = baseline[Atom.to_string(metric)] || baseline[metric]
        acc or report(metric, old, new, dir)
      end)

    if regressed do
      IO.puts("  BLOCKED: a wire metric regressed >= #{@block}%.")
      exit({:shutdown, 1})
    else
      IO.puts("  verdict: OK")
    end
  end

  # Returns true if this metric regressed past the block threshold.
  defp report(metric, old, new, _dir) when not is_number(old) or old == 0 do
    IO.puts("  #{metric}: #{fmt(new)} (no comparable baseline)  skipped")
    false
  end

  defp report(metric, old, new, dir) do
    pct = (new - old) / old * 100.0
    # higher_worse: a positive pct is a regression; lower_worse: a negative pct is.
    regressed = if dir == :higher_worse, do: pct >= @block, else: -pct >= @block
    tag = if regressed, do: "REGRESSION", else: "ok"
    IO.puts("  #{metric}: #{fmt(old)} -> #{fmt(new)}   #{Float.round(pct, 2)}%   #{tag}")
    regressed
  end

  defp last_same_host(host) do
    case File.read(@history) do
      {:ok, body} ->
        body
        |> String.split("\n", trim: true)
        |> Enum.map(&Jason.decode!/1)
        |> Enum.filter(&(&1["host"] == host))
        |> List.last()

      {:error, _} ->
        nil
    end
  end

  defp git(args, default) do
    case System.cmd("git", args, stderr_to_stdout: true) do
      {out, 0} -> String.trim(out)
      _ -> default
    end
  end

  defp fmt(n) when is_float(n), do: :erlang.float_to_binary(n, decimals: 1)
  defp fmt(n), do: to_string(n)
end
