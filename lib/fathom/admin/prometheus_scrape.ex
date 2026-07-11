defmodule Fathom.Admin.PrometheusScrape do
  @moduledoc """
  A small, tolerant parser for `TelemetryMetricsPrometheus.Core.scrape/1` exposition text — the
  in-process read path the dashboard's `Fathom.Admin.MetricsCollector` uses to read the metrics
  layer's aggregation (counters, gauges, histograms) without a second instrumentation.

  Deliberately matches metric families by **name prefix**, not exact name: the reporter appends a
  type/unit suffix (`_milliseconds`, `_bytes`, `_bucket`/`_count`/`_sum`) to the sanitized dotted
  metric name but never rewrites the core, so `"fathom_shard_query_duration"` matches whatever the
  reporter emits. A family it can't find yields zeros (a blank panel, never a crash).
  """

  @typedoc "One parsed sample: metric name, its label map, and the numeric value (`:infinity` for +Inf)."
  @type sample :: {String.t(), %{optional(String.t()) => String.t()}, number() | :infinity}

  @line ~r/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(.+)$/

  @doc "Parse exposition text into a list of `{name, labels, value}` samples (HELP/TYPE lines skipped)."
  @spec parse(binary()) :: [sample()]
  def parse(text) when is_binary(text) do
    text
    |> String.split("\n", trim: true)
    |> Enum.flat_map(&parse_line/1)
  end

  def parse(_), do: []

  defp parse_line("#" <> _), do: []

  defp parse_line(line) do
    case Regex.run(@line, String.trim(line)) do
      [_, name, labels, value] ->
        case parse_value(value) do
          {:ok, v} -> [{name, parse_labels(labels), v}]
          :error -> []
        end

      _ ->
        []
    end
  end

  defp parse_value("+Inf"), do: {:ok, :infinity}
  defp parse_value("-Inf"), do: {:ok, :neg_infinity}
  defp parse_value("NaN"), do: :error

  defp parse_value(v) do
    case Float.parse(v) do
      {f, _} -> {:ok, f}
      :error -> :error
    end
  end

  defp parse_labels(nil), do: %{}
  defp parse_labels("{}"), do: %{}

  defp parse_labels(braces) do
    braces
    |> String.trim_leading("{")
    |> String.trim_trailing("}")
    |> String.split(",", trim: true)
    |> Map.new(fn pair ->
      [k, v] = String.split(pair, "=", parts: 2)
      {String.trim(k), v |> String.trim() |> String.trim("\"")}
    end)
  end

  @doc """
  The scalar value of the metric family named by `prefix` whose labels are a superset of
  `labels` (empty ⇒ any). Prefers an exact family (`_total`/`_count`/`_sum`/base) but tolerates the
  reporter's unit suffix. Returns `default` (0.0) when absent.
  """
  @spec value([sample()], String.t(), map(), number()) :: number()
  def value(samples, prefix, labels \\ %{}, default \\ 0.0) do
    Enum.find_value(samples, default, fn {name, ls, v} ->
      if String.starts_with?(name, prefix) and not bucketish?(name) and labels_match?(ls, labels) and
           is_number(v),
         do: v,
         else: false
    end)
  end

  @doc "Distinct values of label `key` across samples in the family named by `prefix`."
  @spec label_values([sample()], String.t(), String.t()) :: [String.t()]
  def label_values(samples, prefix, key) do
    samples
    |> Enum.filter(fn {name, ls, _} ->
      String.starts_with?(name, prefix) and Map.has_key?(ls, key)
    end)
    |> Enum.map(fn {_n, ls, _v} -> ls[key] end)
    |> Enum.uniq()
  end

  @doc """
  Cumulative histogram buckets for the family named by `prefix` (the `<prefix>…_bucket` samples),
  as `[{le, cumulative_count}]` sorted by `le` ascending with `:infinity` last. Empty when absent.
  """
  @spec buckets([sample()], String.t()) :: [{number() | :infinity, number()}]
  def buckets(samples, prefix) do
    samples
    |> Enum.filter(fn {name, ls, _} ->
      String.starts_with?(name, prefix) and String.ends_with?(name, "_bucket") and
        Map.has_key?(ls, "le")
    end)
    |> Enum.map(fn {_name, ls, count} -> {le(ls["le"]), count} end)
    |> Enum.sort_by(fn {le, _} -> if le == :infinity, do: :infinity, else: le end, &le_leq/2)
  end

  @doc """
  Linear-interpolated `pct` percentile (0..100) over a **cumulative** histogram `[{le, cum}]` —
  the shape Prometheus emits, and (after subtracting a previous scrape) the shape of a windowed
  diff. Returns `0.0` for an empty/zero histogram; a value in the open `+Inf` bucket returns the
  last finite `le`.
  """
  @spec percentile_cumulative([{number() | :infinity, number()}], number()) :: float()
  def percentile_cumulative([], _pct), do: 0.0

  def percentile_cumulative(cum, pct) do
    total = cum |> List.last() |> elem(1)

    if total <= 0 do
      0.0
    else
      target = pct / 100 * total
      walk_cumulative(cum, target, 0.0, 0.0)
    end
  end

  # Walk buckets in ascending le; interpolate within the crossing bucket between the previous
  # finite edge (prev_le) and this edge. The +Inf bucket has no upper edge ⇒ return prev_le.
  defp walk_cumulative([{le, cum} | rest], target, prev_le, prev_cum) do
    cond do
      cum < target ->
        next_prev_le = if le == :infinity, do: prev_le, else: le
        walk_cumulative(rest, target, next_prev_le, cum)

      le == :infinity ->
        prev_le

      cum == prev_cum ->
        le

      true ->
        frac = (target - prev_cum) / (cum - prev_cum)
        prev_le + frac * (le - prev_le)
    end
  end

  defp walk_cumulative([], _target, prev_le, _prev_cum), do: prev_le

  @doc "Element-wise diff of two cumulative-bucket lists (current − previous), aligned by `le`."
  @spec diff_buckets([{number() | :infinity, number()}], [{number() | :infinity, number()}]) ::
          [{number() | :infinity, number()}]
  def diff_buckets(current, previous) do
    prev = Map.new(previous)
    Enum.map(current, fn {le, cum} -> {le, max(cum - Map.get(prev, le, 0.0), 0.0)} end)
  end

  defp le("+Inf"), do: :infinity

  defp le(s) do
    case Float.parse(s) do
      {f, _} -> f
      :error -> :infinity
    end
  end

  defp le_leq(:infinity, :infinity), do: true
  defp le_leq(:infinity, _b), do: false
  defp le_leq(_a, :infinity), do: true
  defp le_leq(a, b), do: a <= b

  defp bucketish?(name),
    do: String.ends_with?(name, "_bucket") or String.ends_with?(name, "_sum")

  defp labels_match?(_ls, labels) when map_size(labels) == 0, do: true
  defp labels_match?(ls, labels), do: Enum.all?(labels, fn {k, v} -> Map.get(ls, k) == v end)
end
