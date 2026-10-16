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

  @typedoc """
  A parsed scrape indexed by metric name: `%{name => [{labels, value}]}` (expert review 2026-10-15
  #23). `parse/1` output is a flat sample list, so every lookup scanned all of it (thousands of
  lines, about a dozen lookups, once a second per node, growing with every metric added). Build
  the index once per scrape with `index/1`; the lookups below take either shape (a list is indexed
  on the spot, which costs about what the old scan did).
  """
  @type index :: %{optional(String.t()) => [{map(), number() | :infinity}]}

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
  rescue
    # Belt-and-suspenders for the moduledoc's "never a crash / yields zeros" contract: a single
    # malformed line must degrade to a dropped sample, never a MatchError that takes down the
    # collector tick (which would then reset the rate baselines). Expert review 2026-07-14 #23.
    _ -> []
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

  # No `nil` clause: `labels` only ever arrives from the `[_, name, labels, value]` destructuring
  # of `Regex.run/2`, so it is always a binary — a non-participating group would shorten the list
  # and fail that match outright rather than bind nil. `"{}"` is the real empty case.
  defp parse_labels("{}"), do: %{}

  defp parse_labels(braces) do
    braces
    |> String.trim_leading("{")
    |> String.trim_trailing("}")
    |> String.split(",", trim: true)
    # A pair with no "=" (e.g. a fragment left by splitting a label value that itself contained a
    # comma or an escaped quote) is DROPPED, not matched — a naive `[k, v] = split` raised a
    # MatchError that crashed the whole tick. Best-effort/total parse per the moduledoc contract.
    # Expert review 2026-07-14 #23.
    |> Enum.flat_map(fn pair ->
      case String.split(pair, "=", parts: 2) do
        [k, v] -> [{String.trim(k), v |> String.trim() |> String.trim("\"")}]
        _ -> []
      end
    end)
    |> Map.new()
  end

  @doc "Index a parsed scrape by metric name, preserving exposition order within a name."
  @spec index([sample()] | index()) :: index()
  def index(%{} = idx), do: idx

  def index(samples) when is_list(samples) do
    samples
    |> Enum.reduce(%{}, fn {name, ls, v}, acc ->
      Map.update(acc, name, [{ls, v}], &[{ls, v} | &1])
    end)
    |> Map.new(fn {name, rows} -> {name, Enum.reverse(rows)} end)
  end

  @doc """
  The scalar value of the metric family named by `prefix` whose labels are a superset of
  `labels` (empty ⇒ any). Prefers an exact family (`_total`/`_count`/`_sum`/base) but tolerates the
  reporter's unit suffix. Returns `default` (0.0) when absent.

  An exact family name is one map hit; only a prefix that is not itself a family name scans the
  family names (about a hundred), never the samples.
  """
  @spec value([sample()] | index(), String.t(), map(), number() | nil) :: number() | nil
  def value(scrape, prefix, labels \\ %{}, default \\ 0.0) do
    idx = index(scrape)

    Enum.find_value(families(idx, prefix), default, fn name ->
      if bucketish?(name) do
        false
      else
        Enum.find_value(Map.fetch!(idx, name), false, fn {ls, v} ->
          if labels_match?(ls, labels) and is_number(v), do: v, else: false
        end)
      end
    end)
  end

  @doc """
  The sum, over every label set matching `labels`, of the first non-bucket family named by
  `prefix`. For a counter or histogram `_count` that is tagged (e.g. `held_retry` by `aimed`) when
  the caller wants the untagged total.
  """
  @spec sum([sample()] | index(), String.t(), map()) :: number()
  def sum(scrape, prefix, labels \\ %{}) do
    idx = index(scrape)

    case Enum.find(families(idx, prefix), &(not bucketish?(&1))) do
      nil ->
        0.0

      name ->
        idx
        |> Map.fetch!(name)
        |> Enum.reduce(0.0, fn {ls, v}, acc ->
          if labels_match?(ls, labels) and is_number(v), do: acc + v, else: acc
        end)
    end
  end

  # Family names for `prefix`: the exact name first (O(1)), then every name starting with it in
  # name order (so a shorter, more exact name wins deterministically).
  defp families(idx, prefix) do
    rest =
      idx
      |> Map.keys()
      |> Enum.filter(&(String.starts_with?(&1, prefix) and &1 != prefix))
      |> Enum.sort()

    if Map.has_key?(idx, prefix), do: [prefix | rest], else: rest
  end

  @doc "Distinct values of label `key` across samples in the family named by `prefix`."
  @spec label_values([sample()] | index(), String.t(), String.t()) :: [String.t()]
  def label_values(scrape, prefix, key) do
    scrape
    |> index()
    |> Enum.filter(fn {name, _} -> String.starts_with?(name, prefix) end)
    |> Enum.flat_map(fn {_name, rows} ->
      for {ls, _v} <- rows, Map.has_key?(ls, key), do: ls[key]
    end)
    |> Enum.uniq()
  end

  @doc """
  Cumulative histogram buckets for the family named by `prefix` (the `<prefix>…_bucket` samples),
  as `[{le, cumulative_count}]` sorted by `le` ascending with `:infinity` last. Empty when absent.

  `labels` restricts to series whose labels are a superset (e.g. `%{"warm" => "true"}`). Series
  that remain for the same `le` are SUMMED: a tagged histogram scraped without a filter used to
  yield one entry per tag value per `le`, which `percentile_cumulative/2` then walked as if it
  were a single distribution (expert review 2026-10-15 #6).
  """
  @spec buckets([sample()] | index(), String.t(), map()) :: [{number() | :infinity, number()}]
  def buckets(scrape, prefix, labels \\ %{}) do
    scrape
    |> index()
    |> Enum.filter(fn {name, _} ->
      String.starts_with?(name, prefix) and String.ends_with?(name, "_bucket")
    end)
    |> Enum.flat_map(fn {_name, rows} ->
      for {ls, count} <- rows, Map.has_key?(ls, "le"), labels_match?(ls, labels) do
        {le(ls["le"]), count}
      end
    end)
    |> Enum.reduce(%{}, fn {le, c}, acc -> Map.update(acc, le, c, &(&1 + c)) end)
    |> Enum.sort_by(fn {le, _} -> le end, &le_leq/2)
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
