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
  @type index :: %{
          optional(String.t()) => [{map(), number() | :infinity}],
          optional(:names) => [String.t()]
        }

  @line ~r/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(.+)$/

  @doc """
  Parse exposition text into a list of `{name, labels, value}` samples (HELP/TYPE lines skipped).

  Every line first goes through `fast_line/1`, a binary-pattern matcher for the CANONICAL shape
  the in-process reporters print (`name`, optional `{k="v",...}`, ONE space, a numeric token), and
  falls back to the regex/split parser (`slow_line/1`) for anything else. The regex path measured
  ~1.8 µs and ~2.6 KiB of garbage per sample against ~0.6 µs and ~1 KiB here, and this runs on
  every collector tick over several hundred samples (expert review 2026-10-29 #12). The fast path
  never guesses: it accepts a line only when the slow path provably returns the same sample, so
  output is identical by construction (pinned against the old implementation in
  `prometheus_scrape_test.exs`).

  The review's recommended fix was to read FastHistogram/ETS directly and skip the text. Not done:
  `TelemetryMetricsPrometheus.Core` folds its distributions inside `scrape/1` through
  `@moduledoc false` modules (`Aggregator.aggregate/3`, a destructive `:ets.take` the `/metrics`
  scrape shares), so a direct read would re-implement that dependency's internals, and the ~1.4 ms
  of Core's own render per tick would remain either way. Parsing was the other ~55% of the cost.
  """
  @spec parse(binary()) :: [sample()]
  def parse(text) when is_binary(text) do
    text
    |> :binary.split("\n", [:global, :trim_all])
    |> Enum.flat_map(&parse_line/1)
  end

  def parse(_), do: []

  defp parse_line("#" <> _), do: []

  defp parse_line(line) do
    case fast_line(line) do
      {:ok, sample} -> [sample]
      :slow -> slow_line(line)
    end
  end

  defguardp name_start?(c) when c in ?a..?z or c in ?A..?Z or c == ?_ or c == ?:
  defguardp name_char?(c) when name_start?(c) or c in ?0..?9
  defguardp key_char?(c) when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_
  # Label-value bytes the fast path will carry verbatim. Anything else (`,` `\` `}` `{` `=`
  # whitespace, non-ASCII, a quote) is a value the slow parser's split/trim logic may treat
  # specially, so such a line is left to it.
  defguardp val_char?(c)
            when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c in [?_, ?., ?:, ?/, ?+, ?-]

  defguardp num_char?(c) when c in ?0..?9 or c in [?., ?e, ?E, ?+, ?-]

  # `name value` or `name{labels} value`, exactly one space, no leading/trailing whitespace.
  defp fast_line(<<c, rest::binary>> = line) when name_start?(c), do: fast_name(rest, line, 1)
  defp fast_line(_), do: :slow

  defp fast_name(<<c, rest::binary>>, line, n) when name_char?(c),
    do: fast_name(rest, line, n + 1)

  defp fast_name(<<?\s, value::binary>>, line, n),
    do: fast_value(binary_part(line, 0, n), %{}, value)

  defp fast_name(<<?{, ?}, ?\s, value::binary>>, line, n),
    do: fast_value(binary_part(line, 0, n), %{}, value)

  defp fast_name(<<?{, rest::binary>>, line, n) do
    case fast_labels(rest, %{}) do
      {:ok, labels, value} -> fast_value(binary_part(line, 0, n), labels, value)
      :slow -> :slow
    end
  end

  defp fast_name(_, _, _), do: :slow

  # `k="v"` pairs up to `} ` — a later duplicate key wins, as `Map.new/1` does on the slow path.
  defp fast_labels(<<c, _::binary>> = rest, acc) when key_char?(c) do
    {klen, after_key} = key_run(rest, 0)

    with <<?=, ?", after_eq::binary>> <- after_key,
         {vlen, <<?", after_val::binary>>} <- val_run(after_eq, 0) do
      acc = Map.put(acc, binary_part(rest, 0, klen), binary_part(after_eq, 0, vlen))

      case after_val do
        <<?,, more::binary>> -> fast_labels(more, acc)
        <<?}, ?\s, value::binary>> -> {:ok, acc, value}
        _ -> :slow
      end
    else
      _ -> :slow
    end
  end

  defp fast_labels(_, _), do: :slow

  # Length of the leading run of bytes of one class, and the rest after it. One function per class
  # (not a shared predicate fun): this is the per-byte loop of a once-a-second 600-sample parse.
  defp key_run(<<c, rest::binary>>, n) when key_char?(c), do: key_run(rest, n + 1)
  defp key_run(bin, n), do: {n, bin}

  defp val_run(<<c, rest::binary>>, n) when val_char?(c), do: val_run(rest, n + 1)
  defp val_run(bin, n), do: {n, bin}

  defp num_only?(<<c, rest::binary>>) when num_char?(c), do: num_only?(rest)
  defp num_only?(<<>>), do: true
  defp num_only?(_), do: false

  defp fast_value(name, labels, "+Inf"), do: {:ok, {name, labels, :infinity}}

  defp fast_value(name, labels, <<c, _::binary>> = value) when num_char?(c) do
    with true <- num_only?(value),
         {:ok, v} <- parse_value(value) do
      {:ok, {name, labels, v}}
    else
      _ -> :slow
    end
  end

  defp fast_value(_, _, _), do: :slow

  defp slow_line(line) do
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

  @doc """
  Index a parsed scrape by metric name, preserving exposition order within a name. The index also
  carries `:names`, the family names sorted ONCE here, so a prefix lookup (`families/2`, about
  sixty to a hundred a tick) walks a pre-sorted list instead of re-sorting ~200 keys each time
  (expert review 2026-10-17 #15).
  """
  @spec index([sample()] | index()) :: index()
  def index(%{} = idx), do: idx

  def index(samples) when is_list(samples) do
    by_name =
      samples
      |> Enum.reduce(%{}, fn {name, ls, v}, acc ->
        Map.update(acc, name, [{ls, v}], &[{ls, v} | &1])
      end)
      |> Map.new(fn {name, rows} -> {name, Enum.reverse(rows)} end)

    Map.put(by_name, :names, by_name |> Map.keys() |> Enum.sort())
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
    # `:names` is sorted, so every name starting with `prefix` is one contiguous run beginning at
    # the first name >= prefix: skip to it and stop at the first miss (no per-lookup sort).
    rest =
      idx
      |> names()
      |> Enum.drop_while(&(&1 < prefix))
      |> Enum.take_while(&String.starts_with?(&1, prefix))
      |> Enum.reject(&(&1 == prefix))

    if Map.has_key?(idx, prefix), do: [prefix | rest], else: rest
  end

  defp names(%{names: names}), do: names
  defp names(idx), do: idx |> Map.keys() |> Enum.filter(&is_binary/1) |> Enum.sort()

  @doc "Distinct values of label `key` across samples in the family named by `prefix`."
  @spec label_values([sample()] | index(), String.t(), String.t()) :: [String.t()]
  def label_values(scrape, prefix, key) do
    scrape
    |> index()
    |> Enum.filter(fn {name, _} -> is_binary(name) and String.starts_with?(name, prefix) end)
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
      is_binary(name) and String.starts_with?(name, prefix) and String.ends_with?(name, "_bucket")
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
