defmodule FathomWeb.AdminComponents do
  @moduledoc """
  Presentational components for the admin dashboard — dense, near-monochrome, tabular-mono
  numerals, theme-aware via daisyUI base tokens. Panels + KPI tiles + server-rendered SVG
  sparklines + status badges, plus the display formatters the LiveViews use.

  The streaming hero time-series chart is the `Chart` JS hook (assets/js/app.js), not here.
  """
  use FathomWeb, :html

  @doc "A hairline card with an optional title bar + actions slot."
  attr :title, :string, default: nil
  attr :class, :any, default: nil
  slot :actions
  slot :inner_block, required: true

  def panel(assigns) do
    ~H"""
    <section class={["rounded-lg border border-base-300 bg-base-200/40", @class]}>
      <div
        :if={@title || @actions != []}
        class="flex items-center justify-between gap-2 px-4 py-3 border-b border-base-300"
      >
        <h2 class="text-sm font-semibold text-base-content/90">{@title}</h2>
        <div :if={@actions != []} class="flex items-center gap-2">{render_slot(@actions)}</div>
      </div>
      <div class="p-4">{render_slot(@inner_block)}</div>
    </section>
    """
  end

  @doc "A KPI tile: uppercase micro-label, big mono-tabular value + unit, optional sparkline slot."
  attr :label, :string, required: true
  attr :value, :string, required: true
  attr :unit, :string, default: nil
  attr :accent, :string, default: "text-primary"
  slot :inner_block

  def stat_tile(assigns) do
    ~H"""
    <div class="rounded-lg border border-base-300 bg-base-200/40 p-4 transition-colors hover:border-base-content/25">
      <span class="text-[11px] uppercase tracking-wide text-base-content/50">{@label}</span>
      <div class="mt-1 flex items-baseline gap-1">
        <span class="num text-2xl font-medium leading-none">{@value}</span>
        <span :if={@unit} class="text-xs text-base-content/50">{@unit}</span>
      </div>
      <div :if={@inner_block != []} class={["mt-3 -mb-1 h-7", @accent]}>
        {render_slot(@inner_block)}
      </div>
    </div>
    """
  end

  @doc "A full-bleed server-rendered SVG sparkline (polyline + faint area) from a list of numbers."
  attr :values, :list, default: []
  attr :stroke, :string, default: "currentColor"
  attr :height, :integer, default: 28
  attr :width, :integer, default: 120
  attr :class, :any, default: nil

  def sparkline(assigns) do
    {line, area} = spark_points(assigns.values, assigns.width, assigns.height)
    assigns = assign(assigns, line: line, area: area)

    ~H"""
    <svg
      :if={@line}
      viewBox={"0 0 #{@width} #{@height}"}
      preserveAspectRatio="none"
      class={["w-full", @class]}
      style={"height: #{@height}px"}
      aria-hidden="true"
    >
      <polygon points={@area} fill={@stroke} fill-opacity="0.12" stroke="none" />
      <polyline
        points={@line}
        fill="none"
        stroke={@stroke}
        stroke-width="1.5"
        stroke-linejoin="round"
        stroke-linecap="round"
        vector-effect="non-scaling-stroke"
      />
    </svg>
    <div :if={!@line} class="h-7"></div>
    """
  end

  @doc "A small status badge (ok/warn/error/info/neutral)."
  attr :kind, :atom, default: :neutral
  slot :inner_block, required: true

  def badge(assigns) do
    ~H"""
    <span class={[
      "inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[11px] font-medium num",
      badge_class(@kind)
    ]}>
      {render_slot(@inner_block)}
    </span>
    """
  end

  defp badge_class(:ok), do: "bg-success/15 text-success"
  defp badge_class(:warn), do: "bg-warning/15 text-warning"
  defp badge_class(:error), do: "bg-error/15 text-error"
  defp badge_class(:info), do: "bg-info/15 text-info"
  defp badge_class(_), do: "bg-base-content/10 text-base-content/70"

  # ── sparkline geometry ──

  defp spark_points(values, w, h) when is_list(values) and length(values) >= 2 do
    {min, max} = Enum.min_max(values)
    span = if max == min, do: 1.0, else: max - min
    n = length(values)

    pts =
      values
      |> Enum.with_index()
      |> Enum.map(fn {v, i} ->
        x = i / (n - 1) * w
        # 1px vertical padding top/bottom so the line never clips.
        y = h - (v - min) / span * (h - 2) - 1
        "#{fmt(x)},#{fmt(y)}"
      end)

    line = Enum.join(pts, " ")
    area = line <> " #{fmt(w)},#{h} 0,#{h}"
    {line, area}
  end

  defp spark_points(_values, _w, _h), do: {nil, nil}

  defp fmt(n), do: :erlang.float_to_binary(n / 1, decimals: 1)

  # ── display formatters (public — the LiveViews render with these) ──

  @doc "Integer with thousands separators (`12345 → \"12,345\"`)."
  @spec fmt_int(number() | nil) :: String.t()
  def fmt_int(nil), do: "—"

  def fmt_int(n) when is_number(n) do
    n
    |> round()
    |> Integer.to_string()
    |> String.replace(~r/\B(?=(\d{3})+(?!\d))/, ",")
  end

  @doc "A rate to one decimal (`1234.5 → \"1,234.5\"`)."
  @spec fmt_rate(number() | nil) :: String.t()
  def fmt_rate(nil), do: "—"

  def fmt_rate(n) when is_number(n) do
    # Round to one decimal in ONE step (off n*10), so a fraction that rounds up to 10 CARRIES
    # into the whole part instead of printing a bogus ".10": 9.96 → "10.0" (not "9.10"), 0.98 →
    # "1.0" (not "0.10"). fmt_int keeps the thousands grouping on the integer part. Expert
    # review 2026-07-14 #24. frac is abs'd so a fractional digit is always 0-9 (rates are ≥ 0).
    rounded = round(n * 10)
    whole = div(rounded, 10)
    frac = rem(abs(rounded), 10)
    "#{fmt_int(whole)}.#{frac}"
  end

  @doc "Human bytes (`1536 → \"1.5 KiB\"`)."
  @spec fmt_bytes(number() | nil) :: String.t()
  def fmt_bytes(nil), do: "—"
  def fmt_bytes(n) when is_number(n) and n < 1024, do: "#{round(n)} B"

  def fmt_bytes(n) when is_number(n) do
    units = ["KiB", "MiB", "GiB", "TiB", "PiB"]

    {val, unit} =
      Enum.reduce_while(units, {n / 1024, "KiB"}, fn u, {v, _} ->
        if v < 1024, do: {:halt, {v, u}}, else: {:cont, {v / 1024, u}}
      end)

    "#{:erlang.float_to_binary(val, decimals: 1)} #{unit}"
  end

  @doc "A millisecond duration, compact (`77.0 → \"77 ms\"`, `1500 → \"1.5 s\"`)."
  @spec fmt_ms(number() | nil) :: String.t()
  def fmt_ms(nil), do: "—"

  def fmt_ms(ms) when is_number(ms) and ms >= 1000,
    do: "#{:erlang.float_to_binary(ms / 1000, decimals: 1)} s"

  def fmt_ms(ms) when is_number(ms) and ms < 10,
    do: "#{:erlang.float_to_binary(ms / 1, decimals: 1)} ms"

  def fmt_ms(ms) when is_number(ms), do: "#{round(ms)} ms"
end
