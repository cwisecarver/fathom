defmodule FathomWeb.AdminDirectoryLive do
  @moduledoc """
  Browse and hand-edit the Postgres shard **directory** (expert review 2026-07-14
  #22): the control-plane `shards` rows an operator needs to inspect and
  occasionally flip during triage.

  Editing is deliberately narrow — only `status` and `retain_until`, through
  `Fathom.Directory.admin_update/2` / `Shard.admin_changeset/2`. The
  migration-state-machine fields (`schema_version`, `cutover_at`, …) are shown but
  **not** hand-editable here: flipping them by hand desyncs the version stamp, so
  that stays a migration-engine operation. Behind the same admin BasicAuth as the
  rest of `/admin`.
  """
  use FathomWeb, :live_view

  import FathomWeb.AdminComponents

  alias Fathom.Directory
  alias Fathom.Directory.Shard

  @page_size 50

  @impl true
  def mount(_params, _session, socket) do
    {:ok,
     socket
     |> assign(:page_title, "Directory")
     |> assign(:node_key, Fathom.Rebalancer.node_key())
     |> assign(:statuses, Shard.statuses())
     |> assign(:editable_statuses, Shard.admin_editable_statuses())
     |> assign(:filter, %{"status" => "", "q" => ""})
     |> assign(:offset, 0)
     |> assign(:editing, nil)
     |> assign(:edit_error, nil)
     |> load()}
  end

  @impl true
  def handle_event("filter", %{"f" => %{"status" => status, "q" => q}}, socket) do
    {:noreply,
     socket
     |> assign(:filter, %{"status" => status, "q" => q})
     |> assign(:offset, 0)
     |> assign(:editing, nil)
     |> load()}
  end

  def handle_event("page", %{"dir" => dir}, socket) do
    %{offset: offset, page: page} = socket.assigns
    step = if dir == "next", do: page.limit, else: -page.limit
    new_offset = offset |> Kernel.+(step) |> clamp_offset(page.total, page.limit)
    {:noreply, socket |> assign(:offset, new_offset) |> assign(:editing, nil) |> load()}
  end

  def handle_event("edit", %{"id" => shard_id}, socket) do
    {:noreply, assign(socket, editing: shard_id, edit_error: nil)}
  end

  def handle_event("cancel_edit", _params, socket) do
    {:noreply, assign(socket, editing: nil, edit_error: nil)}
  end

  # Tenant deletion (#15): tombstone + broadcast + enqueue the erase. Same admin-BasicAuth /
  # live_session gate as every other event here; the `data-confirm` dialog is the destructive
  # guard. Physical erase runs in the background DeleteJob, so we report "scheduled".
  def handle_event("delete_tenant", %{"id" => shard_id}, socket) do
    case Fathom.Tenants.delete(shard_id) do
      {:ok, :scheduled} ->
        {:noreply,
         socket
         |> assign(editing: nil, edit_error: nil)
         |> put_flash(:info, "Deleting #{shard_id} — data erase scheduled")
         |> load()}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Could not delete #{shard_id}: #{inspect(reason)}")}
    end
  end

  # Suspend / resume (#20): dedicated actions (not a status hand-flip) so the fleet-wide
  # admission gate + coordinator drain fire. Same admin gate as every event here.
  def handle_event("suspend_tenant", %{"id" => shard_id}, socket) do
    lifecycle(socket, Fathom.Tenants.suspend(shard_id), shard_id, "Suspended")
  end

  def handle_event("resume_tenant", %{"id" => shard_id}, socket) do
    lifecycle(socket, Fathom.Tenants.resume(shard_id), shard_id, "Resumed")
  end

  def handle_event("save", %{"edit" => %{"status" => status, "retain_until" => retain}}, socket) do
    shard_id = socket.assigns.editing

    case parse_retain(String.trim(retain || "")) do
      {:ok, retain_until} ->
        save(socket, shard_id, %{status: status, retain_until: retain_until})

      :error ->
        {:noreply,
         assign(socket, :edit_error, "retain_until must be an ISO-8601 timestamp or blank")}
    end
  end

  # Shared result-handling for the suspend/resume events (#20).
  defp lifecycle(socket, result, shard_id, verb) do
    case result do
      :ok ->
        {:noreply,
         socket
         |> assign(editing: nil, edit_error: nil)
         |> put_flash(:info, "#{verb} #{shard_id}")
         |> load()}

      {:error, reason} ->
        {:noreply,
         put_flash(
           socket,
           :error,
           "Could not #{String.downcase(verb)} #{shard_id}: #{inspect(reason)}"
         )}
    end
  end

  defp save(socket, shard_id, attrs) do
    case Directory.admin_update(shard_id, attrs) do
      {:ok, _shard} ->
        {:noreply,
         socket
         |> assign(editing: nil, edit_error: nil)
         |> put_flash(:info, "Updated #{shard_id}")
         |> load()}

      {:error, %Ecto.Changeset{} = cs} ->
        {:noreply, assign(socket, :edit_error, changeset_message(cs))}

      {:error, :not_found} ->
        {:noreply,
         socket
         |> assign(editing: nil)
         |> put_flash(:error, "#{shard_id} no longer exists")
         |> load()}
    end
  end

  defp load(socket) do
    %{filter: filter, offset: offset} = socket.assigns

    page =
      Directory.list_page(
        status: filter["status"],
        q: filter["q"],
        limit: @page_size,
        offset: offset
      )

    assign(socket, :page, page)
  end

  defp parse_retain(""), do: {:ok, nil}

  defp parse_retain(str) do
    case DateTime.from_iso8601(str) do
      {:ok, dt, _offset} -> {:ok, dt}
      {:error, _} -> :error
    end
  end

  defp clamp_offset(offset, total, limit) do
    max_offset = if total <= 0, do: 0, else: div(max(total - 1, 0), limit) * limit
    offset |> max(0) |> min(max_offset)
  end

  defp changeset_message(%Ecto.Changeset{} = cs) do
    cs
    |> Ecto.Changeset.traverse_errors(fn {msg, _opts} -> msg end)
    |> Enum.map_join("; ", fn {field, msgs} -> "#{field} #{Enum.join(msgs, ", ")}" end)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.admin flash={@flash} active={:directory} node_key={@node_key}>
      <div class="space-y-6">
        <.panel title="Shard directory">
          <form id="directory-filter" phx-change="filter" class="mb-4 flex flex-wrap items-end gap-3">
            <div>
              <label class="mb-1 block text-xs font-medium text-base-content/60">Status</label>
              <select
                id="directory-status"
                name="f[status]"
                class="rounded-lg border border-base-300 bg-base-100 px-2 py-1 text-sm"
              >
                <option value="" selected={@filter["status"] == ""}>all</option>
                <option :for={s <- @statuses} value={s} selected={@filter["status"] == s}>{s}</option>
              </select>
            </div>
            <div>
              <label class="mb-1 block text-xs font-medium text-base-content/60">Shard id</label>
              <input
                id="directory-q"
                type="text"
                name="f[q]"
                value={@filter["q"]}
                placeholder="substring…"
                phx-debounce="300"
                class="rounded-lg border border-base-300 bg-base-100 px-2 py-1 text-sm"
              />
            </div>
            <div class="text-xs text-base-content/50">
              {@page.total} row(s)
            </div>
          </form>

          <div :if={@page.rows == []} class="py-6 text-center text-sm text-base-content/40">
            No matching shards.
          </div>

          <div :if={@page.rows != []} class="overflow-x-auto">
            <table id="directory-table" class="w-full text-sm">
              <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                <tr class="border-b border-base-300">
                  <th class="py-2 pr-4 text-left font-medium">Shard</th>
                  <th class="py-2 pr-4 text-left font-medium">Status</th>
                  <th class="py-2 pr-4 text-right font-medium">Schema v</th>
                  <th class="py-2 pr-4 text-left font-medium">Last active</th>
                  <th class="py-2 pr-4 text-left font-medium">Retain until</th>
                  <th class="py-2 pr-4 text-right font-medium">Token v</th>
                  <th class="py-2 text-right font-medium"></th>
                </tr>
              </thead>
              <tbody>
                <%= for row <- @page.rows do %>
                  <tr id={"dir-row-#{row.shard_id}"} class="border-b border-base-300/50">
                    <td class="num py-1.5 pr-4 text-base-content/80">{row.shard_id}</td>
                    <td class="py-1.5 pr-4">
                      <span class={status_class(row.status)}>{row.status}</span>
                    </td>
                    <td class="num py-1.5 pr-4 text-right text-base-content/60">
                      {row.schema_version}
                    </td>
                    <td class="num py-1.5 pr-4 text-base-content/60">{fmt_dt(row.last_active_at)}</td>
                    <td class="num py-1.5 pr-4 text-base-content/60">{fmt_dt(row.retain_until)}</td>
                    <td class="num py-1.5 pr-4 text-right text-base-content/60">
                      {row.token_version}
                    </td>
                    <td class="py-1.5 text-right whitespace-nowrap">
                      <%= if row.status == "deleted" do %>
                        <span class="text-xs text-base-content/40">deleted</span>
                      <% else %>
                        <.link
                          id={"dir-export-#{row.shard_id}"}
                          href={~p"/admin/tenants/#{row.shard_id}/export"}
                          class="btn btn-ghost btn-xs"
                        >
                          Export
                        </.link>
                        <%= if row.status == "suspended" do %>
                          <button
                            id={"dir-resume-#{row.shard_id}"}
                            type="button"
                            phx-click="resume_tenant"
                            phx-value-id={row.shard_id}
                            class="btn btn-ghost btn-xs text-success"
                          >
                            Resume
                          </button>
                        <% else %>
                          <button
                            id={"dir-suspend-#{row.shard_id}"}
                            type="button"
                            phx-click="suspend_tenant"
                            phx-value-id={row.shard_id}
                            data-confirm={"Suspend #{row.shard_id}? New connections are refused (403) fleet-wide until you resume it; in-flight transactions finish."}
                            class="btn btn-ghost btn-xs text-warning"
                          >
                            Suspend
                          </button>
                        <% end %>
                        <button
                          id={"dir-edit-#{row.shard_id}"}
                          type="button"
                          phx-click="edit"
                          phx-value-id={row.shard_id}
                          class="btn btn-ghost btn-xs"
                        >
                          Edit
                        </button>
                        <button
                          id={"dir-delete-#{row.shard_id}"}
                          type="button"
                          phx-click="delete_tenant"
                          phx-value-id={row.shard_id}
                          data-confirm={"Permanently delete #{row.shard_id}? This ERASES all its data (every stored copy, snapshot, and version) and cannot be undone."}
                          class="btn btn-ghost btn-xs text-error"
                        >
                          Delete
                        </button>
                      <% end %>
                    </td>
                  </tr>
                  <tr :if={@editing == row.shard_id} id="directory-edit-row" class="bg-base-content/5">
                    <td colspan="7" class="p-3">
                      <form
                        id="directory-edit-form"
                        phx-submit="save"
                        class="flex flex-wrap items-end gap-3"
                      >
                        <div>
                          <label class="mb-1 block text-xs font-medium text-base-content/60">
                            Status
                          </label>
                          <select
                            name="edit[status]"
                            class="rounded-lg border border-base-300 bg-base-100 px-2 py-1 text-sm"
                          >
                            <option
                              :for={s <- @editable_statuses}
                              value={s}
                              selected={row.status == s}
                            >
                              {s}
                            </option>
                          </select>
                        </div>
                        <div>
                          <label class="mb-1 block text-xs font-medium text-base-content/60">
                            Retain until (ISO-8601, blank to clear)
                          </label>
                          <input
                            type="text"
                            name="edit[retain_until]"
                            value={fmt_dt(row.retain_until)}
                            class="w-72 rounded-lg border border-base-300 bg-base-100 px-2 py-1 font-mono text-sm"
                          />
                        </div>
                        <button id="dir-save" type="submit" class="btn btn-primary btn-xs">
                          Save
                        </button>
                        <button type="button" phx-click="cancel_edit" class="btn btn-ghost btn-xs">
                          Cancel
                        </button>
                        <span :if={@edit_error} class="text-xs text-error">{@edit_error}</span>
                      </form>
                    </td>
                  </tr>
                <% end %>
              </tbody>
            </table>
          </div>

          <div class="mt-4 flex items-center justify-between text-xs text-base-content/60">
            <span>
              {@offset + 1}–{min(@offset + @page.limit, @page.total)} of {@page.total}
            </span>
            <div class="flex gap-2">
              <button
                type="button"
                phx-click="page"
                phx-value-dir="prev"
                disabled={@offset == 0}
                class="btn btn-ghost btn-xs"
              >
                Prev
              </button>
              <button
                type="button"
                phx-click="page"
                phx-value-dir="next"
                disabled={@offset + @page.limit >= @page.total}
                class="btn btn-ghost btn-xs"
              >
                Next
              </button>
            </div>
          </div>
        </.panel>
      </div>
    </Layouts.admin>
    """
  end

  defp status_class("active"), do: "num text-success"
  defp status_class("migration_failed"), do: "num text-error"
  defp status_class("migrating"), do: "num text-warning"
  defp status_class("suspended"), do: "num text-warning"
  defp status_class("deleted"), do: "num text-base-content/40"
  defp status_class(_), do: "num text-base-content/60"

  defp fmt_dt(nil), do: ""
  defp fmt_dt(%DateTime{} = dt), do: DateTime.to_iso8601(dt)
end
