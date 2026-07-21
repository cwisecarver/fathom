defmodule Fathom.Audit do
  @moduledoc """
  Append-only audit trail for control-plane / admin actions (expert review #9).

  Every mutating, high-blast-radius operation — delete (irreversible erase), restore (overwrites
  live), fork, export (downloads a tenant's whole database), suspend/resume, and token
  mint/rotate/revoke — records **who** (the actor from #8's API key or the admin credential), **what**
  (action), **which tenant** (shard_id), **from where** (source IP), the **outcome**, and safe detail.
  Without this there is no forensic answer to "who deleted / exported tenant X?" — a compliance and
  incident-response blind spot for a platform holding many tenants' data.

  `log/5` reads the actor + source IP from a `conn`; `record/6` is the raw insert for callers with no
  conn. Both emit `[:fathom, :audit, :event]` telemetry. **Best-effort**: a failed audit insert (a
  Postgres blip) is logged, never raised — the audited action has already happened, and losing an
  action's record must not turn into losing the action. `list/1` reads recent events (admin/ops).
  """
  import Ecto.Query
  require Logger

  alias Fathom.Audit.Event
  alias Fathom.Repo

  @doc "Record an action performed via `conn` (actor + source IP extracted from it)."
  @spec log(Plug.Conn.t(), atom() | String.t(), String.t() | nil, atom() | String.t(), map()) ::
          :ok
  def log(conn, action, shard_id, outcome, detail \\ %{}) do
    record(actor_of(conn), action, shard_id, source_ip_of(conn), outcome, detail)
  end

  @doc "Raw audit insert. Best-effort — logs and swallows any DB error so it never breaks the action."
  @spec record(
          String.t(),
          atom() | String.t(),
          String.t() | nil,
          String.t() | nil,
          atom() | String.t(),
          map()
        ) :: :ok
  def record(actor, action, shard_id, source_ip, outcome, detail \\ %{}) do
    action = to_string(action)
    outcome = to_string(outcome)

    :telemetry.execute([:fathom, :audit, :event], %{count: 1}, %{
      action: action,
      actor: actor,
      outcome: outcome,
      shard_id: shard_id
    })

    attrs = %{
      actor: actor,
      action: action,
      shard_id: shard_id,
      source_ip: source_ip,
      outcome: outcome,
      detail: detail
    }

    case %Event{} |> Event.changeset(attrs) |> Repo.insert() do
      {:ok, _} -> :ok
      {:error, changeset} -> Logger.error("audit insert failed: #{inspect(changeset.errors)}")
    end

    :ok
  rescue
    e ->
      Logger.error("audit insert crashed: #{Exception.message(e)}")
      :ok
  catch
    :exit, reason ->
      Logger.error("audit insert exited: #{inspect(reason)}")
      :ok
  end

  @doc "Recent audit events, newest first (`:limit`, default 100; `:shard_id` to filter)."
  @spec list(keyword()) :: [Event.t()]
  def list(opts \\ []) do
    limit = Keyword.get(opts, :limit, 100)

    Event
    |> maybe_filter_shard(Keyword.get(opts, :shard_id))
    |> order_by(desc: :inserted_at)
    |> limit(^limit)
    |> Repo.all()
  end

  defp maybe_filter_shard(query, nil), do: query
  defp maybe_filter_shard(query, shard_id), do: where(query, [e], e.shard_id == ^shard_id)

  defp actor_of(conn) do
    case conn.assigns[:api_actor] do
      %{name: name} when is_binary(name) -> name
      _ -> Map.get(conn.assigns, :audit_actor, "unknown")
    end
  end

  defp source_ip_of(%{remote_ip: nil}), do: nil

  defp source_ip_of(%{remote_ip: ip}) do
    case :inet.ntoa(ip) do
      {:error, _} -> nil
      addr -> to_string(addr)
    end
  end

  defp source_ip_of(_), do: nil
end
