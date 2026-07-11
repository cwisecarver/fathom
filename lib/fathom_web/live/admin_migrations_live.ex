defmodule FathomWeb.AdminMigrationsLive do
  @moduledoc """
  Schema-migration rollout state (fleet-wide, from `Fathom.Admin.Fleet`): the fleet HEAD +
  release history, rollout burndown (laggards), quarantined shards, and Oban job counts.
  """
  use FathomWeb, :live_view

  import FathomWeb.AdminComponents

  alias Fathom.Admin.Fleet

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:page_title, "Migrations")
      |> assign(:node_key, Fathom.Rebalancer.node_key())
      |> assign_async([:mig, :oban], fn ->
        {:ok, %{mig: Fleet.migrations(), oban: Fleet.oban_counts()}}
      end)

    {:ok, socket}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.admin flash={@flash} active={:migrations} node_key={@node_key}>
      <.async_result :let={mig} assign={@mig}>
        <:loading>
          <div class="skeleton h-40 rounded-lg"></div>
        </:loading>
        <:failed :let={_}>
          <.panel>
            <span class="text-sm text-error">Migration directory unavailable (Postgres).</span>
          </.panel>
        </:failed>

        <div class="space-y-6">
          <div class="grid grid-cols-2 gap-4 lg:grid-cols-4">
            <.stat_tile label="Fleet HEAD" value={if(mig.head, do: "v#{mig.head}", else: "—")} />
            <.stat_tile label="Laggards" value={fmt_int(mig.laggard_count)} accent="text-warning" />
            <.stat_tile
              label="Quarantined"
              value={fmt_int(length(mig.failed_shards))}
              accent="text-error"
            />
            <.stat_tile label="Releases" value={fmt_int(length(mig.releases))} />
          </div>

          <div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
            <.panel title="Release history">
              <div :if={mig.releases == []} class="py-6 text-center text-sm text-base-content/40">
                No captured releases yet.
              </div>
              <table :if={mig.releases != []} class="w-full text-sm">
                <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                  <tr class="border-b border-base-300">
                    <th class="py-2 text-left font-medium">Version</th>
                    <th class="py-2 text-left font-medium">Name</th>
                    <th class="py-2 text-right font-medium">Status</th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={r <- mig.releases} class="border-b border-base-300/50">
                    <td class="num py-1.5">v{r.version}</td>
                    <td class="py-1.5 text-base-content/70">{r.name}</td>
                    <td class="py-1.5 text-right">
                      <.badge :if={r.version == mig.head} kind={:ok}>HEAD</.badge>
                      <.badge :if={r.yanked} kind={:error}>yanked</.badge>
                    </td>
                  </tr>
                </tbody>
              </table>
            </.panel>

            <.panel title="Rollout burndown">
              <p class="text-sm text-base-content/60">
                <span class="num text-base-content">{fmt_int(mig.laggard_count)}</span>
                active shards are still behind <span class="num">v{mig.head}</span>. The hourly reconcile sweep + lazy migrate converge the cold tail.
              </p>
              <div :if={mig.failed_shards != []} class="mt-4 border-t border-base-300 pt-3">
                <div class="mb-2 text-[11px] uppercase tracking-wide text-base-content/50">
                  Quarantined shards
                </div>
                <div class="flex flex-wrap gap-1.5">
                  <.badge :for={s <- Enum.take(mig.failed_shards, 40)} kind={:error}>
                    {s.shard_id}
                  </.badge>
                </div>
              </div>
            </.panel>
          </div>

          <.panel title="Background jobs">
            <.async_result :let={oban} assign={@oban}>
              <:loading>
                <div class="skeleton h-12 rounded"></div>
              </:loading>
              <:failed :let={_}><span class="text-sm text-error">Oban unavailable.</span></:failed>
              <div :if={oban == []} class="text-sm text-base-content/40">No jobs.</div>
              <div class="flex flex-wrap gap-2">
                <.badge :for={j <- oban} kind={oban_kind(j.state)}>
                  {j.queue}/{j.state} {j.count}
                </.badge>
              </div>
            </.async_result>
          </.panel>
        </div>
      </.async_result>
    </Layouts.admin>
    """
  end

  defp oban_kind("completed"), do: :ok
  defp oban_kind("executing"), do: :info
  defp oban_kind("available"), do: :info
  defp oban_kind("retryable"), do: :warn
  defp oban_kind("discarded"), do: :error
  defp oban_kind(_), do: :neutral
end
