defmodule Fathom.Telemetry.FastCounter do
  @moduledoc """
  Lock-free Prometheus counters for the two Core counters that fire on hot paths
  (expert review 2026-10-17 #12): `fathom.shard.pool_take.count` (once per pooled stream open, in
  the coordinator) and `fathom.replication.checkpoint.count` (once per covered commit, in a Session
  task).

  `TelemetryMetricsPrometheus.Core` counts with `:ets.update_counter/4` on ONE key per label set, so
  every scheduler emitting the same event serializes on one hash bucket, the same shape
  `Fathom.Telemetry.FastHistogram` removed for the distributions. Probe (2026-10-17, `mix run`,
  18 schedulers hammering one `pool_take` label set): ~0.45 us/event uncontended, ~9 us/event
  amortized contended; a `:counters` write-concurrency array is lock-free. This is the same
  design: one `:counters` cell per label set, held in `:persistent_term`, new label sets created
  once through this process. The `:telemetry.execute` events are unchanged; only the handler moved.

  `render/1` prints the exposition text Core prints for a counter (`# HELP`, `# TYPE ... counter`,
  one sample per label set), so `/metrics` and the dashboard parser read it unchanged.
  `Fathom.Telemetry.scrape/0` concatenates it with Core's and FastHistogram's.
  """
  use GenServer

  alias Telemetry.Metrics.Counter

  # Counters on a per-operation hot path. Anything not listed stays on Core.
  @hot [
    [:fathom, :shard, :pool_take, :count],
    [:fathom, :replication, :checkpoint, :count],
    # One per follower append (expert review 2026-10-19 #16); tag `result` is :hit | :miss.
    [:fathom, :replication, :held_fd, :count]
  ]

  @doc "True for the metrics this module records instead of `TelemetryMetricsPrometheus.Core`."
  @spec handles?(Telemetry.Metrics.t()) :: boolean()
  def handles?(%Counter{name: name}), do: name in @hot
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
  def handle_event(_event, _measurements, metadata, cfg) do
    with true <- keep?(cfg.keep, metadata),
         {:ok, labels} <- labels(cfg.tags, cfg.tag_values, metadata) do
      ref =
        case :persistent_term.get(cfg.key, %{}) do
          %{^labels => ref} -> ref
          _ -> GenServer.call(cfg.server, {:series, cfg.key, labels})
        end

      :counters.add(ref, 1, 1)
    end

    :ok
  catch
    _, _ -> :ok
  end

  @impl true
  def init({name, metrics}) do
    Process.flag(:trap_exit, true)

    cfgs =
      for %Counter{} = m <- metrics, handles?(m) do
        key = {__MODULE__, name, m.name}
        :persistent_term.put(key, %{})

        cfg = %{
          server: name,
          key: key,
          metric: m,
          keep: m.keep,
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
  def handle_call({:series, key, labels}, _from, state) do
    series = :persistent_term.get(key, %{})

    case series do
      %{^labels => ref} ->
        {:reply, ref, state}

      _ ->
        ref = :counters.new(1, [:write_concurrency])
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

  # Like Core, a sample missing one of its tags is dropped rather than recorded under a partial label set.
  defp labels([], _tag_values, _metadata), do: {:ok, %{}}

  defp labels(tags, tag_values, metadata) do
    labels = Map.take(tag_values.(metadata), tags)
    if map_size(labels) == length(tags), do: {:ok, labels}, else: :skip
  end

  defp render_metric(cfg) do
    case :persistent_term.get(cfg.key, %{}) do
      series when map_size(series) == 0 ->
        []

      series ->
        name = format_name(cfg.metric.name)

        rows =
          series
          |> Enum.map(fn {labels, ref} -> {format_labels(labels), ref} end)
          |> Enum.sort()
          |> Enum.map(fn
            {"", ref} -> "#{name} #{:counters.get(ref, 1)}"
            {labels, ref} -> "#{name}{#{labels}} #{:counters.get(ref, 1)}"
          end)

        [
          "# HELP #{name} #{escape_help(cfg.metric.description)}\n",
          "# TYPE #{name} counter\n",
          Enum.intersperse(rows, "\n"),
          "\n"
        ]
    end
  end

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
