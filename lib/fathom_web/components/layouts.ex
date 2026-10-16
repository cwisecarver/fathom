defmodule FathomWeb.Layouts do
  @moduledoc """
  This module holds layouts and related functionality
  used by your application.
  """
  use FathomWeb, :html

  # Embed all files in layouts/* within this module.
  # The default root.html.heex file contains the HTML
  # skeleton of your application, namely HTML headers
  # and other static content.
  embed_templates "layouts/*"

  @doc """
  Renders your app layout.

  This function is typically invoked from every template,
  and it often contains your application menu, sidebar,
  or similar.

  ## Examples

      <Layouts.app flash={@flash}>
        <h1>Content</h1>
      </Layouts.app>

  """
  attr :flash, :map, required: true, doc: "the map of flash messages"

  attr :current_scope, :map,
    default: nil,
    doc: "the current [scope](https://hexdocs.pm/phoenix/scopes.html)"

  slot :inner_block, required: true

  def app(assigns) do
    ~H"""
    <header class="navbar px-4 sm:px-6 lg:px-8">
      <div class="flex-1">
        <a href="/" class="flex-1 flex w-fit items-center gap-2">
          <img src={~p"/images/logo.svg"} width="36" />
          <span class="text-sm font-semibold">v{Application.spec(:phoenix, :vsn)}</span>
        </a>
      </div>
      <div class="flex-none">
        <ul class="flex flex-column px-1 space-x-4 items-center">
          <li>
            <a href="https://phoenixframework.org/" class="btn btn-ghost">Website</a>
          </li>
          <li>
            <a href="https://github.com/phoenixframework/phoenix" class="btn btn-ghost">GitHub</a>
          </li>
          <li>
            <.theme_toggle />
          </li>
          <li>
            <a href="https://hexdocs.pm/phoenix/overview.html" class="btn btn-primary">
              Get Started <span aria-hidden="true">&rarr;</span>
            </a>
          </li>
        </ul>
      </div>
    </header>

    <main class="px-4 py-20 sm:px-6 lg:px-8">
      <div class="mx-auto max-w-2xl space-y-4">
        {render_slot(@inner_block)}
      </div>
    </main>

    <.flash_group flash={@flash} />
    """
  end

  @doc """
  The admin dashboard shell: a fixed sidebar nav + sticky top bar wrapping the page content.
  Dark-capable via daisyUI base tokens; dense and near-monochrome. Every admin LiveView begins
  its template with `<Layouts.admin flash={@flash} active={:overview} node_key={@node_key}>`.
  """
  attr :flash, :map, required: true

  attr :active, :atom,
    default: nil,
    doc:
      "the active nav item (:overview | :shards | :directory | :migrations | :durability | :cluster | :rebalancer | :audit | :query)"

  attr :node_key, :string, default: ""

  attr :as_of_ms, :integer,
    default: nil,
    doc:
      "system-time ms of the page's newest data; when set, the top-bar dot becomes a freshness chip"

  attr :stale_after_ms, :integer,
    default: 5_000,
    doc: "age past which the chip turns amber (about 3 metric ticks / 2 fleet intervals)"

  attr :refresh_failed, :boolean,
    default: false,
    doc: "the page's last async refresh exited; the chip turns red with \"refresh failed\""

  slot :actions, doc: "top-bar right-aligned controls (e.g. a time-range picker)"
  slot :inner_block, required: true

  def admin(assigns) do
    ~H"""
    <div class="min-h-screen bg-base-100 text-base-content">
      <div class="flex">
        <aside class="sticky top-0 hidden h-screen w-56 shrink-0 flex-col border-r border-base-300 md:flex">
          <div class="flex h-14 items-center gap-2 border-b border-base-300 px-4">
            <.icon name="hero-cube-transparent" class="size-5 text-primary" />
            <span class="font-semibold tracking-tight">Fathom</span>
            <span class="ml-1 text-[11px] text-base-content/40">admin</span>
          </div>
          <nav class="space-y-0.5 p-2 text-sm">
            <.nav_item
              navigate={~p"/admin"}
              active={@active == :overview}
              icon="hero-squares-2x2"
              label="Overview"
            />
            <.nav_item
              navigate={~p"/admin/shards"}
              active={@active == :shards}
              icon="hero-circle-stack"
              label="Shards"
            />
            <.nav_item
              navigate={~p"/admin/directory"}
              active={@active == :directory}
              icon="hero-table-cells"
              label="Directory"
            />
            <.nav_item
              navigate={~p"/admin/migrations"}
              active={@active == :migrations}
              icon="hero-arrow-path"
              label="Migrations"
            />
            <.nav_item
              navigate={~p"/admin/rebalancer"}
              active={@active == :rebalancer}
              icon="hero-scale"
              label="Rebalancer"
            />
            <.nav_item
              navigate={~p"/admin/durability"}
              active={@active == :durability}
              icon="hero-shield-check"
              label="Durability"
            />
            <.nav_item
              navigate={~p"/admin/cluster"}
              active={@active == :cluster}
              icon="hero-server-stack"
              label="Cluster"
            />
            <.nav_item
              navigate={~p"/admin/query"}
              active={@active == :query}
              icon="hero-command-line"
              label="Query"
            />
            <.nav_item
              navigate={~p"/admin/audit"}
              active={@active == :audit}
              icon="hero-clipboard-document-list"
              label="Audit"
            />
          </nav>
          <div class="mt-auto border-t border-base-300 p-3 text-[11px] text-base-content/50">
            node <span class="num text-base-content/70">{@node_key}</span>
          </div>
        </aside>

        <div class="min-w-0 flex-1">
          <header class="sticky top-0 z-10 flex h-14 items-center gap-4 border-b border-base-300 bg-base-100/90 px-4 backdrop-blur md:px-6">
            <span class="font-semibold md:hidden">Fathom</span>
            <div class="ml-auto flex items-center gap-4">
              {render_slot(@actions)}
              <.freshness_chip
                as_of_ms={@as_of_ms}
                stale_after_ms={@stale_after_ms}
                refresh_failed={@refresh_failed}
              />
              <.theme_toggle />
            </div>
          </header>

          <.flash_group flash={@flash} />

          <main class="mx-auto max-w-[1400px] p-4 md:p-6">
            {render_slot(@inner_block)}
          </main>
        </div>
      </div>
    </div>
    """
  end

  attr :as_of_ms, :integer, default: nil
  attr :stale_after_ms, :integer, required: true
  attr :refresh_failed, :boolean, default: false

  # The shared staleness indicator (expert review 2026-10-15 #5). No `as_of_ms` (a page with no
  # polled data) keeps the plain "live" dot. Otherwise the server renders the state at render time
  # and a colocated hook keeps the age ticking client-side, so a frozen collector (no re-render
  # ever arrives) still goes amber/red without a per-viewer server timer. The server sends the AGE,
  # not the timestamp, so browser/server clock skew cannot fake staleness.
  defp freshness_chip(%{as_of_ms: nil} = assigns) do
    ~H"""
    <span
      id="freshness-chip"
      data-state="live"
      class="flex items-center gap-1.5 text-xs text-base-content/60"
    >
      <span class="live-dot inline-block size-2 rounded-full bg-success"></span> live
    </span>
    """
  end

  defp freshness_chip(assigns) do
    age = max(System.system_time(:millisecond) - assigns.as_of_ms, 0)
    state = freshness_state(age, assigns.stale_after_ms, assigns.refresh_failed)
    assigns = assign(assigns, age: age, state: state)

    ~H"""
    <span
      id="freshness-chip"
      phx-hook=".Freshness"
      data-state={@state}
      data-age-ms={@age}
      data-stale-ms={@stale_after_ms}
      data-failed={to_string(@refresh_failed)}
      class="group flex items-center gap-1.5 text-xs text-base-content/60 data-[state=stale]:text-warning data-[state=failed]:text-error"
    >
      <span class="live-dot inline-block size-2 rounded-full bg-success group-data-[state=stale]:bg-warning group-data-[state=failed]:bg-error"></span>
      <span id="freshness-label">{freshness_label(@state, @age)}</span>
    </span>
    <script :type={Phoenix.LiveView.ColocatedHook} name=".Freshness">
      export default {
        mounted() { this.sync(); this.timer = setInterval(() => this.paint(), 1000) },
        updated() { this.sync() },
        destroyed() { clearInterval(this.timer) },
        sync() { this.base = parseInt(this.el.dataset.ageMs, 10) || 0; this.t0 = performance.now(); this.paint() },
        paint() {
          const age = this.base + (performance.now() - this.t0)
          const d = this.el.dataset
          if (d.failed !== "true") d.state = age > parseInt(d.staleMs, 10) ? "stale" : "fresh"
          const s = Math.floor(age / 1000)
          const t = s < 60 ? s + "s" : s < 3600 ? Math.floor(s / 60) + "m" : Math.floor(s / 3600) + "h"
          const label = this.el.querySelector("#freshness-label")
          if (label) label.textContent = d.state === "failed" ? "refresh failed · " + t + " ago" : "as of " + t + " ago"
        }
      }
    </script>
    """
  end

  @doc """
  The chip's state for data `age_ms` old: `:failed` (last refresh exited) beats `:stale` (older
  than `stale_after_ms`) beats `:fresh`.
  """
  @spec freshness_state(integer(), integer(), boolean()) :: :fresh | :stale | :failed
  def freshness_state(_age_ms, _stale_after_ms, true), do: :failed
  def freshness_state(age_ms, stale_after_ms, false) when age_ms > stale_after_ms, do: :stale
  def freshness_state(_age_ms, _stale_after_ms, false), do: :fresh

  defp freshness_label(state, age_ms) do
    t = age_text(div(age_ms, 1000))
    if state == :failed, do: "refresh failed · #{t} ago", else: "as of #{t} ago"
  end

  defp age_text(s) when s < 60, do: "#{s}s"
  defp age_text(s) when s < 3600, do: "#{div(s, 60)}m"
  defp age_text(s), do: "#{div(s, 3600)}h"

  attr :navigate, :string, required: true
  attr :active, :boolean, default: false
  attr :icon, :string, required: true
  attr :label, :string, required: true

  defp nav_item(assigns) do
    ~H"""
    <.link
      navigate={@navigate}
      class={[
        "flex items-center gap-2.5 rounded-md border-l-2 px-2.5 py-1.5 transition-colors",
        @active && "border-primary bg-base-content/5 text-base-content",
        !@active &&
          "border-transparent text-base-content/60 hover:bg-base-content/5 hover:text-base-content"
      ]}
    >
      <.icon name={@icon} class="size-4" />
      {@label}
    </.link>
    """
  end

  @doc """
  Shows the flash group with standard titles and content.

  ## Examples

      <.flash_group flash={@flash} />
  """
  attr :flash, :map, required: true, doc: "the map of flash messages"
  attr :id, :string, default: "flash-group", doc: "the optional id of flash container"

  def flash_group(assigns) do
    ~H"""
    <div id={@id} aria-live="polite">
      <.flash kind={:info} flash={@flash} />
      <.flash kind={:error} flash={@flash} />

      <.flash
        id="client-error"
        kind={:error}
        title={gettext("We can't find the internet")}
        phx-disconnected={show(".phx-client-error #client-error") |> JS.remove_attribute("hidden")}
        phx-connected={hide("#client-error") |> JS.set_attribute({"hidden", ""})}
        hidden
      >
        {gettext("Attempting to reconnect")}
        <.icon name="hero-arrow-path" class="ml-1 size-3 motion-safe:animate-spin" />
      </.flash>

      <.flash
        id="server-error"
        kind={:error}
        title={gettext("Something went wrong!")}
        phx-disconnected={show(".phx-server-error #server-error") |> JS.remove_attribute("hidden")}
        phx-connected={hide("#server-error") |> JS.set_attribute({"hidden", ""})}
        hidden
      >
        {gettext("Attempting to reconnect")}
        <.icon name="hero-arrow-path" class="ml-1 size-3 motion-safe:animate-spin" />
      </.flash>
    </div>
    """
  end

  @doc """
  Provides dark vs light theme toggle based on themes defined in app.css.

  See <head> in root.html.heex which applies the theme before page load.
  """
  def theme_toggle(assigns) do
    ~H"""
    <div class="card relative flex flex-row items-center border-2 border-base-300 bg-base-300 rounded-full">
      <div class="absolute w-1/3 h-full rounded-full border-1 border-base-200 bg-base-100 brightness-200 left-0 [[data-theme=light]_&]:left-1/3 [[data-theme=dark]_&]:left-2/3 transition-[left]" />

      <button
        class="flex p-2 cursor-pointer w-1/3"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="system"
      >
        <.icon name="hero-computer-desktop-micro" class="size-4 opacity-75 hover:opacity-100" />
      </button>

      <button
        class="flex p-2 cursor-pointer w-1/3"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="light"
      >
        <.icon name="hero-sun-micro" class="size-4 opacity-75 hover:opacity-100" />
      </button>

      <button
        class="flex p-2 cursor-pointer w-1/3"
        phx-click={JS.dispatch("phx:set-theme")}
        data-phx-theme="dark"
      >
        <.icon name="hero-moon-micro" class="size-4 opacity-75 hover:opacity-100" />
      </button>
    </div>
    """
  end
end
