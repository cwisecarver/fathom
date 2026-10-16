defmodule Fathom.Telemetry.Protocol do
  @moduledoc """
  Per-protocol breakdown of the Hrana listener (`:hrana_port`) plus the S3 client's error/latency,
  aggregated lock-free in a `:counters` array. Admin-UI observability review 2026-10-15 #24 / #22.

  ## What it listens to (read from the pinned deps, Bandit 1.12.5)

    * `[:bandit, :request, :stop]` — `measurements.duration` (native), `metadata.conn.status`,
      `metadata.plug = {plug_module, opts}`. Counted only when `plug_module == Filo.Plug`: that is
      the metadata that tells the Hrana listener from the `:4000` Phoenix endpoint (whose plug is
      `FathomWeb.Endpoint`). A WebSocket upgrade request ends with status 101 and is NOT an HTTP
      request here — the connection is counted by the websocket events.
    * `[:bandit, :websocket, :start | :stop]` — `metadata.websock`; counted only for `Filo.Socket`
      (LiveView's socket on :4000 is a different websock). `:stop` carries per-connection frame
      counts and `metadata.error` when the connection ended in error.
    * `[:fathom, :s3, :op]` — `status_class` (incl. `"error"` for transport failures) and
      `duration` (native units, stamped by the S3 Req steps).

  ## Cardinality

  Tagged ONLY by protocol (`:http | :ws | :s3`) and status class — never path, host or shard.
  HTTP latency is a fixed-bucket histogram per status class (so p99 is reportable); a WebSocket
  connection's lifetime is hours, not a latency, so `:ws` carries counts only (opens, closes,
  error closes, received frames).

  The handler is two pattern matches plus 1-2 `:counters.add/3` calls; see
  `test/fathom/telemetry/protocol_test.exs` for the measured per-event cost.
  """

  @bounds_ms [1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000]
  # one slot per bound + the +Inf overflow
  @slots length(@bounds_ms) + 1

  @http_classes ["2xx", "4xx", "5xx"]
  @s3_classes ["2xx", "3xx", "4xx", "5xx", "error"]

  # Series layout: {proto, class} -> base offset into the array.
  @series Enum.with_index(
            Enum.map(@http_classes, &{:http, &1}) ++ Enum.map(@s3_classes, &{:s3, &1})
          )
          |> Map.new(fn {k, i} -> {k, i * @slots + 1} end)
  @ws_base map_size(@series) * @slots + 1
  @ws_opens @ws_base
  @ws_closes @ws_base + 1
  @ws_errors @ws_base + 2
  @ws_frames @ws_base + 3
  @size @ws_base + 3

  @key {__MODULE__, :counters}
  @handler_id "fathom-protocol"

  @type stats :: %{
          http: %{rate: %{String.t() => float()}, p99_ms: float()},
          ws: %{
            opens_per_s: float(),
            closes_per_s: float(),
            errors_per_s: float(),
            frames_per_s: float()
          },
          s3: %{errors_per_s: float(), p99_ms: float()}
        }
  @opaque snapshot :: [non_neg_integer()]

  @doc "Attach the handlers (idempotent). Called by the metrics collector on init."
  @spec attach() :: :ok
  def attach do
    case :persistent_term.get(@key, nil) do
      nil -> :persistent_term.put(@key, :counters.new(@size, [:write_concurrency]))
      _ -> :ok
    end

    :telemetry.detach(@handler_id)

    :ok =
      :telemetry.attach_many(
        @handler_id,
        [
          [:bandit, :request, :stop],
          [:bandit, :websocket, :start],
          [:bandit, :websocket, :stop],
          [:fathom, :s3, :op]
        ],
        &__MODULE__.handle_event/4,
        nil
      )
  end

  @spec detach() :: :ok | {:error, :not_found}
  def detach, do: :telemetry.detach(@handler_id)

  @doc false
  # Must never raise (a raising handler is detached by :telemetry).
  def handle_event([:bandit, :request, :stop], m, %{plug: {Filo.Plug, _}} = meta, _) do
    case meta do
      %{conn: %{status: 101}} -> :ok
      %{conn: %{status: s}} when is_integer(s) -> observe(:http, http_class(s), m)
      _ -> observe(:http, "5xx", m)
    end
  catch
    _, _ -> :ok
  end

  def handle_event([:bandit, :websocket, :start], _m, %{websock: Filo.Socket}, _),
    do: add(@ws_opens, 1)

  def handle_event([:bandit, :websocket, :stop], m, %{websock: Filo.Socket} = meta, _) do
    add(@ws_closes, 1)
    if Map.has_key?(meta, :error), do: add(@ws_errors, 1)

    add(
      @ws_frames,
      Map.get(m, :recv_text_frame_count, 0) + Map.get(m, :recv_binary_frame_count, 0)
    )
  catch
    _, _ -> :ok
  end

  def handle_event([:fathom, :s3, :op], m, %{status_class: class}, _) when is_binary(class),
    do: if(Map.has_key?(@series, {:s3, class}), do: observe(:s3, class, m), else: :ok)

  def handle_event(_event, _m, _meta, _), do: :ok

  defp observe(proto, class, m) do
    base = Map.fetch!(@series, {proto, class})

    idx =
      case m do
        %{duration: d} when is_integer(d) ->
          ms = System.convert_time_unit(d, :native, :microsecond) / 1000
          base + bucket(@bounds_ms, ms, 0)

        _ ->
          # no duration measurement: count it in the overflow slot
          base + @slots - 1
      end

    add(idx, 1)
  end

  defp bucket([], _ms, i), do: i
  defp bucket([b | _], ms, i) when ms <= b, do: i
  defp bucket([_ | rest], ms, i), do: bucket(rest, ms, i + 1)

  defp add(idx, n) do
    case :persistent_term.get(@key, nil) do
      nil -> :ok
      ref -> :counters.add(ref, idx, n)
    end
  end

  defp http_class(s) when s in 400..499, do: "4xx"
  defp http_class(s) when s in 500..599, do: "5xx"
  defp http_class(_), do: "2xx"

  @doc """
  Windowed stats since `prev` (a value returned by an earlier call, or `nil` for the first tick,
  which reports zeros rather than a lifetime spike). Returns `{stats, snapshot}`; keep the
  snapshot for the next call.
  """
  @spec window(snapshot() | nil, number()) :: {stats(), snapshot()}
  def window(prev, window_s) do
    cur = read()
    delta = if prev, do: Enum.zip_with(cur, prev, &(&1 - &2)), else: List.duplicate(0, @size)
    {stats(delta, max(window_s, 0.001)), cur}
  end

  defp read do
    case :persistent_term.get(@key, nil) do
      nil -> List.duplicate(0, @size)
      ref -> for i <- 1..@size, do: :counters.get(ref, i)
    end
  end

  defp stats(d, w) do
    v = List.to_tuple(d)
    at = fn i -> elem(v, i - 1) end

    slice = fn proto, classes ->
      for c <- classes, do: for(i <- 0..(@slots - 1), do: at.(@series[{proto, c}] + i))
    end

    http = slice.(:http, @http_classes)
    s3 = slice.(:s3, @s3_classes)

    %{
      http: %{
        rate: for({c, h} <- Enum.zip(@http_classes, http), into: %{}, do: {c, Enum.sum(h) / w}),
        p99_ms: p99(Enum.zip_with(http, &Enum.sum/1))
      },
      ws: %{
        opens_per_s: at.(@ws_opens) / w,
        closes_per_s: at.(@ws_closes) / w,
        errors_per_s: at.(@ws_errors) / w,
        frames_per_s: at.(@ws_frames) / w
      },
      s3: %{
        errors_per_s: (Enum.sum(Enum.at(s3, 4)) + Enum.sum(Enum.at(s3, 3))) / w,
        p99_ms: p99(Enum.zip_with(s3, &Enum.sum/1))
      }
    }
  end

  # Upper bound (ms) of the bucket holding the 99th percentile; 0.0 with no samples. The overflow
  # bucket reports the last finite bound (a floor, not an estimate).
  defp p99(buckets) do
    n = Enum.sum(buckets)

    if n == 0 do
      0.0
    else
      target = Float.ceil(n * 0.99)

      {_, idx} =
        Enum.reduce_while(Enum.with_index(buckets), {0, @slots - 1}, fn {c, i}, {acc, _} ->
          if acc + c >= target, do: {:halt, {acc + c, i}}, else: {:cont, {acc + c, i}}
        end)

      Enum.at(@bounds_ms, min(idx, length(@bounds_ms) - 1)) * 1.0
    end
  end

  @doc false
  @spec reset() :: :ok
  def reset do
    :persistent_term.erase(@key)
    :ok
  end
end
