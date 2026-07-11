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
    doc: "the active nav item (:overview | :shards | :migrations)"

  attr :node_key, :string, default: ""
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
              navigate={~p"/admin/migrations"}
              active={@active == :migrations}
              icon="hero-arrow-path"
              label="Migrations"
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
              <span class="flex items-center gap-1.5 text-xs text-base-content/60">
                <span class="live-dot inline-block size-2 rounded-full bg-success"></span> live
              </span>
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
