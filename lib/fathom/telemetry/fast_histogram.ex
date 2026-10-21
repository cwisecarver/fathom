defmodule Fathom.Telemetry.FastHistogram do
  @moduledoc """
  Lock-free Prometheus histograms for the per-query distributions.

  `TelemetryMetricsPrometheus.Core` records a distribution by inserting every sample into one
  `duplicate_bag` under ONE key per metric, then aggregates on scrape. For the two distributions on
  the request path (`fathom.shard.query.duration`, once per statement, and
  `fathom.shards.checkout.stop.duration`, once per stream) every scheduler hits that one hash bucket,
  and `write_concurrency` does not help with a single key. Measured (perf review 2026-10-01 #14,
  `mix fathom.scale --hotspots --shards 1000 --queries 1280000 --stream-len 64`, prod, 18 cores,
  3 runs per arm): **85.5–85.8k q/s with no reporter vs 61.8–62.1k q/s with Core, -28%**. Turning
  the 1 s scrape off left it at ~61k, so the cost is the per-sample insert, not the collector. A
  `:counters` handler measured 85.5–86.5k, i.e. the cost is gone.

  So these two are pre-bucketed here instead: one `:counters` array per label set (a bucket count
  per bound, `+Inf`, and the sum), held in `:persistent_term`, so the hot path is a
  `persistent_term` read plus two lock-free adds. A new label set (a new checkout `outcome`) is
  created once, serialized through this process. `render/1` prints the same exposition text Core
  prints for a distribution (`le` inclusive, labels sorted and escaped, `_sum`/`_count`), so
  `/metrics`, `Fathom.Admin.PrometheusScrape`, the Grafana dashboard and the alert rules read it
  unchanged. Everything else stays on Core; `Fathom.Telemetry.scrape/0` concatenates the two.

  The sum is kept in integer thousandths of the metric's unit (µs for these millisecond metrics):
  `:counters` holds integers, and µs leaves the 64-bit sum centuries of headroom.
  """
  use GenServer

  alias Telemetry.Metrics.Distribution

  # The distributions emitted on the request path. Anything not listed stays on Core.
  @hot [
    [:fathom, :shard, :query, :duration],
    [:fathom, :shards, :checkout, :stop, :duration],
    # One per replicated commit (expert review 2026-10-17 #25): a Core `duplicate_bag` insert per
    # commit otherwise. Same tag (`outcome`), unit and buckets as declared in telemetry.ex.
    [:fathom, :replication, :commit, :duration],
    # Also one-per-commit (+ per round for commit_stage), added by round-1 #10 as Core distributions
    # (expert review 2026-10-17 #4). Tags `outcome`/`stage` are bounded; buckets match telemetry.ex.
    [:fathom, :replication, :commit_total, :duration],
    [:fathom, :replication, :commit_stage, :duration]
  ]
  @sum_scale 1000

  # Every label set these metrics can emit, created at init (expert review 2026-10-20-2027 #26).
  # Creating a series at runtime is a `:persistent_term.put` on an EXISTING key, which forces a
  # node-wide literal GC scan, so a first-seen outcome used to pay it on the request path. Each set
  # below is the closed set the emitter produces (checkout: `checkout_outcome/1` in shards.ex;
  # commit/commit_total: `commit_outcome/1` in session.ex; commit_stage: `stage` x `reset` in
  # session.ex). A label set NOT listed here still works, it just takes the runtime path once.
  # render/1 skips zero-count series, so pre-creating a set does not change the exposition text.
  @label_sets %{
    [:fathom, :shards, :checkout, :stop, :duration] =>
      for(
        o <- [:ok, :held, :unavailable, :at_capacity, :novel_rate_limited, :error],
        do: %{outcome: o}
      ),
    [:fathom, :replication, :commit, :duration] =>
      for(o <- [:ok, :no_quorum, :timeout, :other], do: %{outcome: o}),
    [:fathom, :replication, :commit_total, :duration] =>
      for(o <- [:ok, :no_quorum, :timeout, :other], do: %{outcome: o}),
    [:fathom, :replication, :commit_stage, :duration] =>
      for(s <- [:build, :collect], r <- [true, false], do: %{stage: s, reset: r})
  }

  @doc "True for the metrics this module records instead of `TelemetryMetricsPrometheus.Core`."
  @spec handles?(Telemetry.Metrics.t()) :: boolean()
  def handles?(%Distribution{name: name}), do: name in @hot
  def handles?(_), do: false

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, {name, Keyword.fetch!(opts, :metrics)}, name: name)
  end

  @doc "Prometheus exposition text for every series recorded so far (`\"\"` when not running)."
  @spec render(atom()) :: String.t()
  def render(name \\ __MODULE__) do
    name
    |> configs()
    |> Enum.map(&render_metric/1)
    |> IO.iodata_to_binary()
  end

  @doc false
  # The :telemetry handler. Runs in the emitting process; must never raise (a raising handler is
  # detached by :telemetry, silently ending the metric).
  def handle_event(_event, measurements, metadata, cfg) do
    with true <- keep?(cfg.keep, metadata),
         {:ok, value} <- measure(cfg.measurement, measurements, metadata),
         {:ok, labels} <- labels(cfg.tags, cfg.tag_values, metadata) do
      ref =
        case :persistent_term.get(cfg.key, %{}) do
          %{^labels => ref} -> ref
          _ -> GenServer.call(cfg.server, {:series, cfg.key, labels, cfg.size})
        end

      :counters.add(ref, bucket_index(cfg.buckets, value, 1), 1)
      :counters.add(ref, cfg.size, round(value * @sum_scale))
    end

    :ok
  catch
    _, _ -> :ok
  end

  @impl true
  def init({name, metrics}) do
    Process.flag(:trap_exit, true)

    cfgs =
      for %Distribution{} = m <- metrics, handles?(m) do
        buckets = Keyword.fetch!(m.reporter_options, :buckets)
        key = {__MODULE__, name, m.name}
        # one slot per bound, one for +Inf, one for the sum
        size = length(buckets) + 2

        series =
          for labels <- Map.get(@label_sets, m.name, []),
              into: %{},
              do: {labels, :counters.new(size, [:write_concurrency])}

        :persistent_term.put(key, series)

        cfg = %{
          server: name,
          key: key,
          metric: m,
          buckets: buckets,
          size: size,
          keep: m.keep,
          measurement: m.measurement,
          tags: m.tags,
          tag_values: m.tag_values
        }

        :ok = :telemetry.attach(key, m.event_name, &__MODULE__.handle_event/4, cfg)
        cfg
      end

    :persistent_term.put({__MODULE__, name}, cfgs)
    {:ok, %{name: name, cfgs: cfgs}}
  end

  @impl true
  def handle_call({:series, key, labels, size}, _from, state) do
    series = :persistent_term.get(key, %{})

    case series do
      %{^labels => ref} ->
        {:reply, ref, state}

      _ ->
        ref = :counters.new(size, [:write_concurrency])
        :persistent_term.put(key, Map.put(series, labels, ref))
        {:reply, ref, state}
    end
  end

  @impl true
  def terminate(_reason, %{name: name, cfgs: cfgs}) do
    for cfg <- cfgs do
      :telemetry.detach(cfg.key)
      :persistent_term.erase(cfg.key)
    end

    :persistent_term.erase({__MODULE__, name})
    :ok
  end

  defp configs(name), do: :persistent_term.get({__MODULE__, name}, [])

  defp keep?(nil, _metadata), do: true
  defp keep?(fun, metadata), do: fun.(metadata)

  defp measure(key, measurements, _metadata) when is_atom(key),
    do: number(Map.get(measurements, key))

  defp measure(fun, measurements, _metadata) when is_function(fun, 1),
    do: number(fun.(measurements))

  defp measure(fun, measurements, metadata) when is_function(fun, 2),
    do: number(fun.(measurements, metadata))

  defp number(v) when is_number(v), do: {:ok, v}
  defp number(_), do: :skip

  # Like Core, a sample missing one of its tags is dropped rather than recorded under a partial label set.
  defp labels([], _tag_values, _metadata), do: {:ok, %{}}

  defp labels(tags, tag_values, metadata) do
    labels = Map.take(tag_values.(metadata), tags)
    if map_size(labels) == length(tags), do: {:ok, labels}, else: :skip
  end

  # 1-based slot of the first bound >= value (`le` is inclusive); past the last bound is +Inf.
  defp bucket_index([], _value, i), do: i
  defp bucket_index([b | _], value, i) when value <= b, do: i
  defp bucket_index([_ | rest], value, i), do: bucket_index(rest, value, i + 1)

  # --- exposition, matching TelemetryMetricsPrometheus.Core.Exporter's distribution format ---

  # Zero-count series are pre-created label sets nothing has hit yet; they are left out so the text
  # matches what Core prints (Core only ever shows observed series).
  defp render_metric(cfg) do
    observed =
      for {labels, ref} <- :persistent_term.get(cfg.key, %{}),
          count(ref, cfg) > 0,
          do: {labels, ref}

    case observed do
      [] ->
        []

      observed ->
        name = format_name(cfg.metric.name)

        rows =
          observed
          |> Enum.map(fn {labels, ref} -> {format_labels(labels), ref} end)
          |> Enum.sort()
          |> Enum.map(fn {labels, ref} -> format_series(name, labels, cfg, ref) end)

        [
          "# HELP #{name} #{escape_help(cfg.metric.description)}\n",
          "# TYPE #{name} histogram\n",
          Enum.intersperse(rows, "\n"),
          "\n"
        ]
    end
  end

  # Sum of the bucket slots (1..length(buckets)+1, the last being +Inf): the sample count.
  defp count(ref, cfg), do: Enum.sum(for i <- 1..(cfg.size - 1), do: :counters.get(ref, i))

  defp format_series(name, labels, cfg, ref) do
    {bucket_rows, count} =
      Enum.map_reduce(Enum.with_index(cfg.buckets ++ ["+Inf"], 1), 0, fn {bound, i}, acc ->
        acc = acc + :counters.get(ref, i)
        {~s(#{name}_bucket{#{join_labels(labels, ~s(le="#{bound}"))}} #{acc}), acc}
      end)

    sum = :counters.get(ref, cfg.size) / @sum_scale
    suffix = if labels == "", do: "", else: "{#{labels}}"

    Enum.join(
      bucket_rows ++ ["#{name}_sum#{suffix} #{sum}", "#{name}_count#{suffix} #{count}"],
      "\n"
    )
  end

  defp join_labels("", le), do: le
  defp join_labels(labels, le), do: labels <> "," <> le

  defp format_labels(labels) do
    labels
    |> Enum.map(fn {k, v} -> ~s/#{k}="#{escape(v)}"/ end)
    |> Enum.sort()
    |> Enum.join(",")
  end

  defp format_name(name) do
    name
    |> Enum.join("_")
    |> String.replace(~r/[^a-zA-Z0-9_]/, "")
    |> String.replace(~r/^[^a-zA-Z]+/, "")
  end

  defp escape(value) do
    value
    |> to_string()
    |> String.replace(~S("), ~S(\"))
    |> String.replace(~S(\\), ~S(\\\\))
    |> String.replace(~S(\n), ~S(\\n))
  end

  defp escape_help(value) do
    value
    |> to_string()
    |> String.replace(~S(\\), ~S(\\\\))
    |> String.replace(~S(\n), ~S(\\n))
  end
end
