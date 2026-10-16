defmodule FathomWeb.AdminComponents.Errors do
  @moduledoc """
  Overview panels for failure signals fed by `Fathom.Admin.MetricsCollector` (admin-UI round 2):
  the Errors panel (query errors by class, error ratio, no-quorum by reason), the Data-plane auth
  panel (Hrana rejections by reason) and the Cluster page's "Events / 5 min" strip.

  Each map arrives as `%{rates: %{key => per_s}, window: %{key => count_in_5m}}`. Like
  `FathomWeb.AdminComponents.NodeHealth`, every component reads defensively (the payload may
  predate a field or be `nil` before the first tick) and carries its threshold colouring in a
  `data-state` attribute so tests assert state without matching markup.
  """
  use FathomWeb, :html

  import FathomWeb.AdminComponents

  @error_classes ~w(timeout too_large busy sql_error no_quorum other)
  @auth_reasons ~w(expired invalid wrong_shard revoked missing other)
  # Security-relevant auth rejections: a token for the wrong tenant, or one deliberately revoked.
  @auth_hot ~w(wrong_shard revoked)

  @event_labels [
    flush_failed: "flush.failed",
    lease_superseded: "lease.superseded",
    fenced_quarantine: "fenced_quarantine",
    corrupt_flush: "corrupt_flush",
    replication_reject: "replication.reject",
    at_capacity: "shards.at_capacity",
    heartbeat_lapsed: "heartbeat.lapsed"
  ]

  @doc "Event keys in display order (the collector's `events_5m` keys)."
  @spec event_keys() :: [atom()]
  def event_keys, do: Keyword.keys(@event_labels)

  @doc """
  Errors panel state: `:error` when a timeout or a no-quorum refusal happened in the window (or is
  happening now), `:warn` for any other error, `:ok` when clean, `:idle` before the first tick.
  """
  @spec errors_state(map() | nil, map() | nil) :: :error | :warn | :ok | :idle
  def errors_state(nil, nil), do: :idle

  def errors_state(errors, no_quorum) do
    cond do
      pos?(errors, "timeout") or any_pos?(no_quorum) -> :error
      any_pos?(errors) -> :warn
      true -> :ok
    end
  end

  defp pos?(group, key) do
    g = group || %{}
    Map.get(Map.get(g, :window, %{}), key, 0) > 0 or Map.get(Map.get(g, :rates, %{}), key, 0) > 0
  end

  defp any_pos?(nil), do: false

  defp any_pos?(group) do
    Enum.any?(Map.get(group, :window, %{}), fn {_k, n} -> n > 0 end) or
      Enum.any?(Map.get(group, :rates, %{}), fn {_k, r} -> r > 0 end)
  end

  # ───────────────────────── Errors panel ─────────────────────────

  attr :metrics, :map, default: nil
  attr :history, :list, default: []

  def errors_panel(assigns) do
    m = assigns.metrics || %{}
    errors = Map.get(m, :errors) || %{}
    nq = Map.get(m, :no_quorum) || %{}

    assigns =
      assign(assigns,
        state: errors_state(Map.get(m, :errors), Map.get(m, :no_quorum)),
        errors: errors,
        classes: classes(errors, @error_classes),
        nq: nq,
        nq_keys: classes(nq, [])
      )

    ~H"""
    <.panel title="Errors">
      <div id="errors-panel" data-state={@state} class="space-y-3">
        <div class="flex items-end justify-between gap-3">
          <div id="errors-rate">
            <div class="text-[11px] uppercase tracking-wide text-base-content/50">Errors / s</div>
            <div class={["num text-2xl font-medium", @state == :error && "text-error"]}>
              {fmt_rate(@errors[:total_rate])}
            </div>
          </div>
          <div id="errors-ratio" class="text-right">
            <div class="text-[11px] uppercase tracking-wide text-base-content/50">Error ratio</div>
            <div class="num text-lg">{fmt_ratio(@errors[:ratio])}</div>
          </div>
          <div id="errors-ratio-spark" class="w-28 shrink-0">
            <.sparkline
              values={Enum.map(@history || [], &Map.get(&1, :err_ratio, 0.0))}
              stroke="currentColor"
              class={if(@state == :error, do: "text-error", else: "text-warning")}
            />
          </div>
        </div>

        <div id="errors-by-class" class="flex flex-wrap items-center gap-1.5">
          <span class="text-[11px] uppercase tracking-wide text-base-content/50">by class</span>
          <span
            :for={c <- @classes}
            id={"errors-class-#{c}"}
            data-state={class_state(c, @errors)}
          >
            <.badge kind={class_kind(c, @errors)}>
              {c} {fmt_rate(rate(@errors, c))} · {win(@errors, c)} in 5m
            </.badge>
          </span>
        </div>

        <div id="errors-no-quorum" class="flex flex-wrap items-center gap-1.5">
          <span class="text-[11px] uppercase tracking-wide text-base-content/50">no-quorum / s</span>
          <span :if={@nq_keys == []} class="text-sm text-base-content/40">none seen</span>
          <span
            :for={r <- @nq_keys}
            id={"errors-nq-#{r}"}
            data-state={if(pos?(@nq, r), do: "error", else: "ok")}
          >
            <.badge kind={if(pos?(@nq, r), do: :error, else: :neutral)}>
              {r} {fmt_rate(rate(@nq, r))} · {win(@nq, r)} in 5m
            </.badge>
          </span>
        </div>
      </div>
    </.panel>
    """
  end

  defp classes(group, fixed) do
    seen = group |> Map.get(:rates, %{}) |> Map.keys()
    Enum.uniq(fixed ++ seen)
  end

  defp rate(group, key), do: group |> Map.get(:rates, %{}) |> Map.get(key, 0.0)
  defp win(group, key), do: group |> Map.get(:window, %{}) |> Map.get(key, 0)

  defp class_state("timeout", g), do: if(pos?(g, "timeout"), do: "error", else: "ok")
  defp class_state(c, g), do: if(pos?(g, c), do: "warn", else: "ok")

  defp class_kind("timeout", g), do: if(pos?(g, "timeout"), do: :error, else: :neutral)
  defp class_kind(c, g), do: if(pos?(g, c), do: :warn, else: :neutral)

  defp fmt_ratio(nil), do: "—"
  defp fmt_ratio(r) when r == 0, do: "0%"
  defp fmt_ratio(r) when is_number(r), do: "#{Float.round(r * 100, 2)}%"

  # ───────────────────────── Data-plane auth ─────────────────────────

  attr :metrics, :map, default: nil

  def auth_panel(assigns) do
    auth = (assigns.metrics && Map.get(assigns.metrics, :auth_rejected)) || %{}

    assigns =
      assign(assigns,
        auth: auth,
        reasons: classes(auth, @auth_reasons),
        total: auth |> Map.get(:rates, %{}) |> Map.values() |> Enum.sum(),
        hot?: Enum.any?(@auth_hot, &pos?(auth, &1))
      )

    ~H"""
    <.panel title="Data-plane auth">
      <div id="auth-panel" data-state={if(@hot?, do: "error", else: "ok")} class="space-y-3">
        <div id="auth-rejected-rate">
          <div class="text-[11px] uppercase tracking-wide text-base-content/50">Rejected / s</div>
          <div class="num text-2xl font-medium">{fmt_rate(@total)}</div>
        </div>
        <div class="flex flex-wrap items-center gap-1.5">
          <span
            :for={r <- @reasons}
            id={"auth-reason-#{r}"}
            data-state={auth_state(r, @auth)}
          >
            <.badge kind={auth_kind(r, @auth)}>
              {r} {fmt_rate(rate(@auth, r))} · {win(@auth, r)} in 5m
            </.badge>
          </span>
        </div>
        <div
          :if={@hot?}
          id="auth-security-note"
          class="text-xs text-error"
        >
          wrong_shard / revoked rejections are security-relevant: a token presented for the wrong tenant, or a revoked one still in use.
        </div>
      </div>
    </.panel>
    """
  end

  defp auth_state(r, g) when r in @auth_hot, do: if(pos?(g, r), do: "error", else: "ok")
  defp auth_state(r, g), do: if(pos?(g, r), do: "warn", else: "ok")

  defp auth_kind(r, g) when r in @auth_hot, do: if(pos?(g, r), do: :error, else: :neutral)
  defp auth_kind(r, g), do: if(pos?(g, r), do: :warn, else: :neutral)

  # ───────────────────────── Events / 5 min (Cluster page) ─────────────────────────

  attr :events, :map, default: nil

  def events_strip(assigns) do
    assigns = assign(assigns, labels: @event_labels, events: assigns.events || %{})

    ~H"""
    <.panel title="Events / 5 min">
      <div id="events-5m" class="flex flex-wrap gap-2">
        <span
          :for={{key, label} <- @labels}
          id={"event-#{key}"}
          data-state={if(Map.get(@events, key, 0) > 0, do: "error", else: "ok")}
        >
          <.badge kind={if(Map.get(@events, key, 0) > 0, do: :error, else: :neutral)}>
            {label} {fmt_int(Map.get(@events, key, 0))}
          </.badge>
        </span>
      </div>
    </.panel>
    """
  end
end
