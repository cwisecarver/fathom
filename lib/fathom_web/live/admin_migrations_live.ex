defmodule FathomWeb.AdminMigrationsLive do
  @moduledoc """
  Schema-migration rollout state (fleet-wide, from `Fathom.Admin.Fleet`, refreshed every 5s): the
  fleet HEAD + release history, **why the rollout is held** (review blocks), rollout burndown
  (laggards), quarantined shards, and Oban job counts. All Postgres — loaded off-process and kept
  flicker-free (last-good stays during refresh).

  ## The review-block panel (expert review 2026-08-01 #26, dashboard half)

  A held version caps HEAD, so `laggards` never reaches zero and every later migration stacks
  behind it. The API half of #26 shipped first because a CI deploy gate reads it, but the human
  staring at a burndown that will not move is looking at THIS page — and it showed a cheerful
  "Fleet HEAD v1" with no hint that a version above it was frozen, which is precisely the
  illegibility the finding is about.

  **Deliberately read-only.** The finding asks to surface *why a version is held and what the
  options are*, not to offer the actions. That is the right scope here, and not merely a smaller
  one: of the two options, `attach_transform` cannot be a button at all (it needs a module written,
  deployed, and added to the `:migration_transforms` allowlist), while `approve_review` replays the
  template's literal row values onto every tenant — the fleet-wide corruption the flag exists to
  prevent, and a decision the UI has no way to help an operator make correctly. A panel offering
  only the dangerous half of a two-way decision biases toward it. So the panel prints the exact
  commands and the consequence of each, and the operator runs the one they mean. Contrast
  `AdminDirectoryLive`'s Delete button: that is destructive but per-tenant and unambiguous — the
  operator already knows whether they mean to erase that tenant.
  """
  use FathomWeb, :live_view

  import FathomWeb.AdminComponents

  alias Fathom.Admin.Fleet

  @fleet_refresh_ms 5_000

  @impl true
  def mount(_params, _session, socket) do
    connected = connected?(socket)
    if connected, do: Process.send_after(self(), :refresh_fleet, @fleet_refresh_ms)

    socket =
      socket
      |> assign(:page_title, "Migrations")
      |> assign(:node_key, Fathom.Rebalancer.node_key())
      |> assign(:mig, nil)
      |> assign(:oban, nil)
      |> load_fleet(connected)

    {:ok, socket}
  end

  defp load_fleet(socket, false), do: socket

  defp load_fleet(socket, true) do
    start_async(socket, :fleet, fn -> %{mig: Fleet.migrations(), oban: Fleet.oban_counts()} end)
  end

  @impl true
  def handle_info(:refresh_fleet, socket) do
    Process.send_after(self(), :refresh_fleet, @fleet_refresh_ms)
    {:noreply, load_fleet(socket, true)}
  end

  @impl true
  def handle_async(:fleet, {:ok, %{mig: mig, oban: oban}}, socket) do
    {:noreply, socket |> assign(:mig, mig) |> assign(:oban, oban)}
  end

  # Keep the last-good data if a refresh crashes — a Postgres blip shouldn't blank the page.
  def handle_async(:fleet, {:exit, _reason}, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.admin flash={@flash} active={:migrations} node_key={@node_key}>
      <div :if={@mig == nil} class="skeleton h-40 rounded-lg"></div>

      <div :if={@mig} class="space-y-6">
        <div class="grid grid-cols-2 gap-4 lg:grid-cols-5">
          <.stat_tile label="Fleet HEAD" value={if(@mig.head, do: "v#{@mig.head}", else: "—")} />
          <.stat_tile
            label="Held for review"
            value={fmt_int(length(@mig.review_blocks))}
            accent="text-error"
          />
          <.stat_tile label="Laggards" value={fmt_int(@mig.laggard_count)} accent="text-warning" />
          <.stat_tile
            label="Quarantined"
            value={fmt_int(@mig.failed_shard_count)}
            accent="text-error"
          />
          <.stat_tile label="Releases" value={fmt_int(length(@mig.releases))} />
        </div>

        <div :if={@mig.review_blocks != []} id="review-blocks">
          <.panel title="Rollout held — operator review required" class="border-error/40">
            <p class="text-sm text-base-content/70">
              Fleet HEAD is capped below the lowest held version, so every migration above it is
              stacked behind it and the burndown below will never reach zero until one of the options
              is taken. Nothing here is applied automatically.
            </p>

            <div class="mt-4 space-y-4">
              <div
                :for={b <- @mig.review_blocks}
                class="rounded-lg border border-base-300 bg-base-100/40 p-3"
              >
                <div class="flex flex-wrap items-center gap-2">
                  <.badge kind={:error}>v{b.version}</.badge>
                  <span class="text-sm font-medium text-base-content/90">{b.name}</span>
                  <.badge kind={:warn}>{b.reason}</.badge>
                </div>

                <p class="mt-2 text-sm text-base-content/70">{reason_text(b.reason)}</p>

                <div :if={detail_statements(b) != []} class="mt-3">
                  <div class="mb-1 text-[11px] uppercase tracking-wide text-base-content/50">
                    Flagged statements
                  </div>
                  <div class="rounded bg-base-300/40 p-2">
                    <code
                      :for={s <- detail_statements(b)}
                      class="block break-words text-[11px] leading-relaxed text-base-content/80"
                    >
                      {s}
                    </code>
                  </div>
                </div>

                <div :if={detail_gap(b)} class="mt-3 text-xs text-base-content/60">
                  Template/fleet migration-count gap:
                  <span class="num text-base-content/80">{detail_gap(b)}</span>
                </div>

                <div :if={b.options != []} class="mt-3 border-t border-base-300 pt-3">
                  <div class="mb-2 text-[11px] uppercase tracking-wide text-base-content/50">
                    Options
                  </div>
                  <div class="space-y-3">
                    <div :for={o <- b.options}>
                      <div class="text-xs font-medium text-base-content/90">{o.action}</div>
                      <div class="mt-1 rounded bg-base-300/40 p-2">
                        <code class="block break-words text-[11px] text-base-content/80">
                          {o.how}
                        </code>
                      </div>
                      <p class="mt-1 text-xs text-base-content/60">{o.effect}</p>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </.panel>
        </div>

        <div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
          <.panel title="Release history">
            <div
              :if={@mig.releases == []}
              class="py-6 text-center text-sm text-base-content/40"
            >
              No captured releases yet.
            </div>
            <table :if={@mig.releases != []} class="w-full text-sm">
              <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                <tr class="border-b border-base-300">
                  <th class="py-2 text-left font-medium">Version</th>
                  <th class="py-2 text-left font-medium">Name</th>
                  <th class="py-2 text-right font-medium">Status</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={r <- @mig.releases} class="border-b border-base-300/50">
                  <td class="num py-1.5">v{r.version}</td>
                  <td class="py-1.5 text-base-content/70">{r.name}</td>
                  <td class="py-1.5 text-right">
                    <.badge :if={r.version == @mig.head} kind={:ok}>HEAD</.badge>
                    <.badge :if={r.yanked} kind={:error}>yanked</.badge>
                  </td>
                </tr>
              </tbody>
            </table>
          </.panel>

          <.panel title="Rollout burndown">
            <p class="text-sm text-base-content/60">
              <span class="num text-base-content">{fmt_int(@mig.laggard_count)}</span>
              active shards are still behind <span class="num">v{@mig.head}</span>
              . The hourly reconcile sweep + lazy migrate converge the cold tail.
            </p>
            <div :if={@mig.failed_shard_count > 0} class="mt-4 border-t border-base-300 pt-3">
              <div class="mb-2 text-[11px] uppercase tracking-wide text-base-content/50">
                Quarantined shards
              </div>
              <div class="flex flex-wrap gap-1.5">
                <.badge :for={id <- @mig.failed_shard_ids} kind={:error}>
                  {id}
                </.badge>
              </div>
            </div>
          </.panel>
        </div>

        <.panel title="Background jobs">
          <div :if={@oban == []} class="text-sm text-base-content/40">No jobs.</div>
          <div class="flex flex-wrap gap-2">
            <.badge :for={j <- @oban} kind={oban_kind(j.state)}>{j.queue}/{j.state} {j.count}</.badge>
          </div>
        </.panel>
      </div>
    </Layouts.admin>
    """
  end

  # Plain-English gloss of `Fathom.Migrator.review_block/1`'s `reason` (#26). The reason string
  # itself is shown too — it is what `/api/migrations/status` reports, so an operator reading the
  # dashboard and an engineer reading the JSON are looking at the same token.
  defp reason_text("data_migration"),
    do:
      "The captured version contains DML carrying the TEMPLATE's row values (a RunPython " <>
        "backfill crosses the wire as literal INSERT/UPDATE/DELETE). Replaying it verbatim would " <>
        "write one tenant's data onto every tenant."

  defp reason_text("migration_gap"),
    do:
      "The template's django_migrations count jumped between captures — a migration ran OUTSIDE " <>
        "any tracked transaction (the `atomic = False` idiom runs autocommit and is invisible to " <>
        "capture). The fleet is missing DDL this version and everything above it assume."

  defp reason_text("data_migration_and_gap"),
    do:
      "Both: the fleet is missing DDL that ran outside capture, AND this version carries " <>
        "template-literal data statements. Reconcile the gap first — a transform cannot conjure " <>
        "DDL the fleet never received."

  defp reason_text(other), do: "Held for review (#{other})."

  # `detail` is a plain map with STRING keys (it round-trips through a jsonb column, and
  # `Migrator.inferred_detail/2` matches that shape for releases captured before #26 existed).
  # Both accessors tolerate a missing/blank detail: the reason is recorded best-effort by design,
  # so the block must stay legible when only the reason survived.
  defp detail_statements(%{detail: %{"statements" => s}}) when is_list(s), do: s
  defp detail_statements(_), do: []

  defp detail_gap(%{detail: %{"gap" => g}}) when is_binary(g), do: g
  defp detail_gap(_), do: nil

  defp oban_kind("completed"), do: :ok
  defp oban_kind("executing"), do: :info
  defp oban_kind("available"), do: :info
  defp oban_kind("retryable"), do: :warn
  defp oban_kind("discarded"), do: :error
  defp oban_kind(_), do: :neutral
end
