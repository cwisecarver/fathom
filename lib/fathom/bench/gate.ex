defmodule Fathom.Bench.Gate do
  @moduledoc """
  The multi-metric regression decision for the bench-then-commit gate.

  Fathom records several hot-path numbers per run (see `Fathom.Bench`), and a
  change can regress one while leaving the others flat — so the gate refuses a
  commit if **any** gated metric regresses by ≥ the block threshold, multi-metric
  rather than a single throughput scalar. The comparison is a pure function so it
  is unit-testable without running a bench.

  Direction is per metric: latency and memory are higher-is-worse; copy throughput
  is lower-is-worse. A metric that is `nil` in either run (e.g. the directory bench
  with no Postgres, or the always-`nil` `hrana_rt_us` placeholder) or whose parent
  value is zero is skipped, never silently treated as flat.
  """

  # Ordered so the report reads top-to-bottom in a stable order.
  @metrics [
    {:cold_open_p50_us, :higher_worse},
    {:cold_open_s3_p50_us, :higher_worse},
    {:warm_s3_shards_per_s, :lower_worse},
    {:dir_resolve_p50_us, :higher_worse},
    {:copy_rows_per_s, :lower_worse},
    {:fanout_kb_per_shard, :higher_worse}
  ]

  @doc "The gated metrics and their regression direction."
  def metrics, do: @metrics

  @doc """
  Compares `parent` and `new` metric maps (string- or atom-keyed, as decoded from
  `perf_history.jsonl`). `block`/`warn` are percent thresholds. Returns

      %{verdict: :ok | :warn | :block | :no_data, worst: float, deltas: [delta]}

  where each `delta` is `%{metric, parent, new, pct, skipped, reason}` and `pct` is
  the regression percent (positive = worse).
  """
  @spec compare(map(), map(), number(), number()) :: map()
  def compare(parent, new, block \\ 20, warn \\ 20) do
    deltas =
      Enum.map(@metrics, fn {metric, dir} ->
        delta(metric, dir, value(parent, metric), value(new, metric))
      end)

    comparable = Enum.reject(deltas, & &1.skipped)
    worst = comparable |> Enum.map(& &1.pct) |> max_or(0.0)

    verdict =
      cond do
        comparable == [] -> :no_data
        worst >= block -> :block
        worst > warn -> :warn
        true -> :ok
      end

    %{verdict: verdict, worst: worst, deltas: deltas}
  end

  @doc "A human-readable multi-line report of a `compare/3` result."
  @spec format(map(), String.t(), number(), number()) :: String.t()
  def format(result, parent_sha, block \\ 20, warn \\ 20) do
    header = "=== bench gate vs parent #{parent_sha} ==="

    rows =
      Enum.map(result.deltas, fn d ->
        name = String.pad_trailing(to_string(d.metric), 20)

        if d.skipped do
          "  #{name} skipped (#{d.reason})"
        else
          "  #{name} #{fmt(d.parent)} -> #{fmt(d.new)}   #{signed(d.pct)}%   #{tag(d.pct, block, warn)}"
        end
      end)

    verdict =
      "  verdict: #{result.verdict |> to_string() |> String.upcase()} " <>
        "(worst #{signed(result.worst)}%, block >=#{block}%, warn >#{warn}%)"

    Enum.join([header | rows] ++ [verdict], "\n")
  end

  # --- internals -----------------------------------------------------------

  defp value(map, metric) do
    Map.get(map, metric) || Map.get(map, Atom.to_string(metric))
  end

  defp delta(metric, _dir, parent, new) when is_nil(parent) or is_nil(new) do
    %{metric: metric, parent: parent, new: new, pct: 0.0, skipped: true, reason: "nil"}
  end

  defp delta(metric, _dir, parent, new) when parent == 0 do
    %{metric: metric, parent: parent, new: new, pct: 0.0, skipped: true, reason: "parent zero"}
  end

  defp delta(metric, dir, parent, new) do
    pct =
      case dir do
        :higher_worse -> (new - parent) / parent * 100
        :lower_worse -> (parent - new) / parent * 100
      end

    %{metric: metric, parent: parent, new: new, pct: Float.round(pct / 1, 2), skipped: false}
  end

  defp max_or([], default), do: default
  defp max_or(list, _default), do: Enum.max(list)

  defp tag(pct, block, _warn) when pct >= block, do: "BLOCK"
  defp tag(pct, _block, warn) when pct > warn, do: "warn"
  defp tag(_pct, _block, _warn), do: "ok"

  defp signed(n) when n >= 0, do: "+#{:erlang.float_to_binary(n / 1, decimals: 2)}"
  defp signed(n), do: :erlang.float_to_binary(n / 1, decimals: 2)

  defp fmt(n) when is_float(n), do: :erlang.float_to_binary(n, decimals: 2)
  defp fmt(n), do: to_string(n)
end
