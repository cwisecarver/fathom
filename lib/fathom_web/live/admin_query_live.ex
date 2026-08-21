defmodule FathomWeb.AdminQueryLive do
  @moduledoc """
  A direct query console (expert review 2026-07-14 #23): type SQL, pick a tenant,
  submit, see what a real client sees.

  The query is sent to Fathom's **own Hrana endpoint over loopback** through
  `Fathom.QueryConsole` — the genuine front door (Host routing → admission → auth →
  `ShardExecutor`), never an internal shard side-door — so the rows, the Hrana
  error `code`, and the round-trip latency shown here are exactly what a libSQL /
  django-libsql client would get. Useful for "is `acme` serving? what does this
  return? what error code does a client actually see?" and as a per-node smoke test.

  Runs **arbitrary SQL, including writes/DDL**, against any tenant, gated only by
  the admin BasicAuth on `/admin` (see `Fathom.QueryConsole` on the blast radius).
  """
  use FathomWeb, :live_view

  import FathomWeb.AdminComponents

  alias Fathom.{QueryConsole, ShardId}

  @impl true
  def mount(_params, session, socket) do
    {:ok,
     socket
     # Who is at the keyboard, for the issuance ledger (expert review 2026-08-20 #32). Put in the
     # session by the router's `admin_live_session/1`; a LiveView cannot read the BasicAuth header
     # itself, and an unattributed mint is what poisons the fleet-wide revoke.
     |> assign(:admin_actor, Map.get(session, "admin_actor", "console"))
     |> assign(:page_title, "Query")
     |> assign(:node_key, Fathom.Rebalancer.node_key())
     |> assign(:form, to_form(%{"shard" => "", "sql" => ""}, as: :q))
     |> assign(:running, false)
     |> assign(:result, nil)
     |> assign(:error, nil)}
  end

  @impl true
  def handle_event("run", %{"q" => %{"shard" => shard, "sql" => sql}}, socket) do
    shard = String.trim(shard)
    sql = String.trim(sql)

    cond do
      shard == "" ->
        {:noreply, assign(socket, error: input_error("enter a shard id"), result: nil)}

      sql == "" ->
        {:noreply, assign(socket, error: input_error("enter a SQL statement"), result: nil)}

      # Validate the shard id before it reaches QueryConsole.host_for/1, which interpolates it into
      # the Host (expert review 2026-07-18 #16). Unvalidated, a dotted id like "acme.other" is
      # reinterpreted by Host-subdomain routing as its FIRST label ("acme") — an admin would run SQL
      # against the WRONG tenant. ShardId.valid? enforces the DNS-label-safe format (no dots/slashes).
      # Validate the shard id before it reaches QueryConsole.host_for/1, which interpolates it into
      # the Host (expert review 2026-07-18 #16). Unvalidated, a dotted id like "acme.other" is
      # reinterpreted by Host-subdomain routing as its FIRST label ("acme") — an admin would run SQL
      # against the WRONG tenant. ShardId.valid? enforces the DNS-label-safe format (no dots/slashes).
      not ShardId.valid?(shard) ->
        {:noreply,
         assign(socket,
           error:
             input_error(
               "invalid shard id — letters, digits, hyphen or underscore only (no dots or slashes)"
             ),
           result: nil
         )}

      true ->
        # Bound OUTSIDE the closure: `start_async/3` runs in a separate process and referencing
        # `socket` there captures the whole struct.
        actor = socket.assigns.admin_actor

        {:noreply,
         socket
         |> assign(:running, true)
         |> assign(:error, nil)
         |> assign(:result, nil)
         |> assign(:form, to_form(%{"shard" => shard, "sql" => sql}, as: :q))
         |> start_async(:query, fn ->
           QueryConsole.run(shard, sql, actor: actor)
         end)}
    end
  end

  @impl true
  def handle_async(:query, {:ok, {:ok, result}}, socket) do
    {:noreply, assign(socket, running: false, result: result, error: nil)}
  end

  def handle_async(:query, {:ok, {:error, error}}, socket) do
    {:noreply, assign(socket, running: false, error: error, result: nil)}
  end

  def handle_async(:query, {:exit, reason}, socket) do
    {:noreply,
     assign(socket,
       running: false,
       result: nil,
       error: %{code: "CONSOLE_CRASH", message: "query task exited: #{inspect(reason)}"}
     )}
  end

  defp input_error(message), do: %{code: "INPUT", message: message}

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.admin flash={@flash} active={:query} node_key={@node_key}>
      <div class="space-y-6">
        <.panel title="Query a tenant (front door)">
          <p class="mb-4 text-sm text-base-content/60">
            Runs SQL against a tenant over the real Hrana client path — Host routing, admission,
            auth, and the shard executor — so you see exactly what a libSQL client would, including
            the error code and latency. <span class="font-medium">Writes and DDL are permitted.</span>
          </p>

          <.form for={@form} id="query-form" phx-submit="run" class="space-y-3">
            <div class="flex gap-3">
              <div class="w-64">
                <.input field={@form[:shard]} id="query-shard" label="Shard" placeholder="acme" />
              </div>
            </div>
            <div>
              <label class="mb-1 block text-sm font-medium text-base-content/70">SQL</label>
              <textarea
                id="query-sql"
                name="q[sql]"
                rows="4"
                class="w-full rounded-lg border border-base-300 bg-base-100 p-2 font-mono text-sm"
                placeholder="SELECT * FROM sqlite_master;"
              >{Phoenix.HTML.Form.input_value(@form, :sql)}</textarea>
            </div>
            <button
              id="query-run"
              type="submit"
              disabled={@running}
              class="btn btn-primary btn-sm"
            >
              {if @running, do: "Running…", else: "Run"}
            </button>
          </.form>
        </.panel>

        <div :if={@error} id="query-error" class="rounded-lg border border-error/40 bg-error/5 p-4">
          <div class="text-sm font-medium text-error">
            {@error.code || "error"}
          </div>
          <div class="num mt-1 text-sm text-base-content/80">{@error.message}</div>
          <div :if={Map.get(@error, :latency_ms)} class="mt-1 text-xs text-base-content/50">
            {fmt_latency(@error.latency_ms)}
          </div>
        </div>

        <.panel :if={@result} title="Result">
          <div id="query-latency" class="mb-3 flex gap-4 text-xs text-base-content/60">
            <span>{fmt_latency(@result.latency_ms)}</span>
            <span :if={@result.affected_row_count > 0}>
              affected rows: <span class="num">{@result.affected_row_count}</span>
            </span>
            <span :if={@result.last_insert_rowid}>
              last insert rowid: <span class="num">{@result.last_insert_rowid}</span>
            </span>
            <span>rows: <span class="num">{@result.row_count}</span></span>
          </div>

          <div :if={@result.truncated} class="mb-2 text-xs text-warning">
            Showing the first {length(@result.rows)} of {@result.row_count} rows.
          </div>

          <div :if={@result.cols == []} class="py-4 text-sm text-base-content/50">
            No rows returned.
          </div>

          <div :if={@result.cols != []} class="overflow-x-auto">
            <table id="query-results" class="w-full text-sm">
              <thead class="text-[11px] uppercase tracking-wide text-base-content/50">
                <tr class="border-b border-base-300">
                  <th :for={c <- @result.cols} class="py-2 pr-4 text-left font-medium">{c}</th>
                </tr>
              </thead>
              <tbody>
                <tr :for={row <- @result.rows} class="border-b border-base-300/50">
                  <td :for={cell <- row} class="num py-1.5 pr-4 text-base-content/80">
                    {fmt_cell(cell)}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </.panel>
      </div>
    </Layouts.admin>
    """
  end

  defp fmt_latency(nil), do: ""

  defp fmt_latency(ms) when is_number(ms),
    do: "#{:erlang.float_to_binary(ms / 1, decimals: 1)} ms"

  defp fmt_cell(nil), do: "NULL"
  defp fmt_cell({:blob, b64}), do: "<blob #{byte_size(b64)}b b64>"
  defp fmt_cell(v) when is_binary(v), do: v
  defp fmt_cell(v), do: to_string(v)
end
