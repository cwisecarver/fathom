defmodule FathomWeb.AdminAuditLive do
  @moduledoc """
  The audit trail (`Fathom.Audit`, expert review 2026-10-15 #20): control-plane / admin actions,
  newest first, paginated, with an optional shard filter (`/admin/audit?shard=<id>`), plus a 24 h
  tile of credential mints / rotations / revokes. Postgres reads happen on the connected mount only,
  each bounded (`@page_size` rows; the tile is a time-bounded GROUP BY on the `inserted_at` index).
  """
  use FathomWeb, :live_view

  import FathomWeb.AdminComponents

  alias Fathom.Audit

  @page_size 50

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:page_title, "Audit")
      |> assign(:node_key, Fathom.Rebalancer.node_key())
      |> assign(:shard, "")
      |> assign(:offset, 0)
      |> assign(:events, [])
      |> assign(:has_more?, false)
      |> assign(:counts, nil)
      |> assign(:filter_error, nil)
      |> assign(:page_size, @page_size)

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    shard = params |> Map.get("shard", "") |> to_string() |> String.trim()
    offset = parse_offset(params["offset"])

    socket =
      socket
      |> assign(:shard, shard)
      |> assign(:offset, offset)

    {:noreply, if(connected?(socket), do: load(socket), else: socket)}
  end

  @impl true
  def handle_event("filter", %{"f" => %{"shard" => shard}}, socket) do
    {:noreply, push_patch(socket, to: audit_path(String.trim(shard), 0))}
  end

  defp load(socket) do
    case filter_id(socket.assigns.shard) do
      {:ok, id} ->
        rows =
          Audit.list(shard_id: id, limit: @page_size + 1, offset: socket.assigns.offset)

        socket
        |> assign(:filter_error, nil)
        |> assign(:events, Enum.take(rows, @page_size))
        |> assign(:has_more?, length(rows) > @page_size)
        |> assign(:counts, Audit.credential_action_counts())

      :error ->
        socket
        |> assign(:filter_error, "Not a valid shard id")
        |> assign(:events, [])
        |> assign(:has_more?, false)
        |> assign(:counts, Audit.credential_action_counts())
    end
  end

  defp filter_id(""), do: {:ok, nil}
  defp filter_id(raw), do: Fathom.ShardId.cast(raw)

  defp parse_offset(nil), do: 0

  defp parse_offset(raw) when is_binary(raw) do
    case Integer.parse(raw) do
      {n, ""} when n >= 0 -> n
      _ -> 0
    end
  end

  defp parse_offset(_), do: 0

  defp audit_path(shard, offset) do
    params =
      [shard: shard, offset: offset]
      |> Enum.reject(fn {k, v} -> v in ["", 0] and k in [:shard, :offset] end)

    ~p"/admin/audit?#{params}"
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.admin flash={@flash} active={:audit} node_key={@node_key}>
      <div class="space-y-6">
        <div :if={@counts} class="grid grid-cols-3 gap-4" id="audit-tiles">
          <.stat_tile label="Mints (24h)" value={fmt_int(@counts["mint_token"])} />
          <.stat_tile label="Rotations (24h)" value={fmt_int(@counts["rotate_token"])} />
          <.stat_tile label="Revokes (24h)" value={fmt_int(@counts["revoke_token"])} />
        </div>

        <.panel title="Audit events">
          <.form for={%{}} as={:f} id="audit-filter" phx-submit="filter" class="mb-4 flex gap-2">
            <input
              type="text"
              name="f[shard]"
              id="audit-filter-shard"
              value={@shard}
              placeholder="shard id"
              class="input input-sm input-bordered"
            />
            <button type="submit" class="btn btn-sm">Filter</button>
          </.form>
          <div :if={@filter_error} id="audit-filter-error" class="mb-3 text-sm text-error">
            {@filter_error}
          </div>
          <div
            :if={@events == [] and !@filter_error}
            id="audit-empty"
            class="py-6 text-center text-sm text-base-content/40"
          >
            No audit events.
          </div>
          <table :if={@events != []} id="audit-table" class="w-full text-sm">
            <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
              <tr class="border-b border-base-300">
                <th class="py-2 text-left font-medium">When</th>
                <th class="py-2 text-left font-medium">Actor</th>
                <th class="py-2 text-left font-medium">Action</th>
                <th class="py-2 text-left font-medium">Shard</th>
                <th class="py-2 text-left font-medium">Outcome</th>
                <th class="py-2 text-left font-medium">IP</th>
              </tr>
            </thead>
            <tbody>
              <tr :for={e <- @events} id={"audit-row-#{e.id}"} class="border-b border-base-300/50">
                <td class="num py-1.5">{Calendar.strftime(e.inserted_at, "%Y-%m-%d %H:%M:%S")}</td>
                <td class="num py-1.5 text-base-content/70">{e.actor}</td>
                <td class="num py-1.5">{e.action}</td>
                <td class="num py-1.5">
                  <.link
                    :if={e.shard_id && Fathom.ShardId.valid?(e.shard_id)}
                    navigate={~p"/admin/shards/#{e.shard_id}"}
                    class="link"
                  >
                    {e.shard_id}
                  </.link>
                </td>
                <td class="num py-1.5">
                  <.badge kind={if e.outcome == "ok", do: :ok, else: :error}>{e.outcome}</.badge>
                </td>
                <td class="num py-1.5 text-base-content/60">{e.source_ip}</td>
              </tr>
            </tbody>
          </table>
          <div class="mt-4 flex justify-between text-sm">
            <.link
              :if={@offset > 0}
              id="audit-prev"
              patch={audit_path(@shard, max(@offset - @page_size, 0))}
              class="link"
            >
              Newer
            </.link>
            <span :if={@offset == 0}></span>
            <.link
              :if={@has_more?}
              id="audit-next"
              patch={audit_path(@shard, @offset + @page_size)}
              class="link"
            >
              Older
            </.link>
          </div>
        </.panel>
      </div>
    </Layouts.admin>
    """
  end
end
