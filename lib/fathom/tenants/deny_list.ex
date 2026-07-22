defmodule Fathom.Tenants.DenyList do
  @moduledoc """
  Shared boot-load retry + degraded-signal helpers for the two node-local admission deny
  sets (`Fathom.Tenants.Tombstones`, `Fathom.Tenants.Suspensions`).

  Both load their set from the Postgres directory at boot and best-effort-rescue a failure
  to an empty ETS table. Expert review #33: a load that FAILS during a Postgres wobble
  coincident with a GenServer restart was treated identically to a successful one — the
  process then waited the FULL refresh interval (5 min) before its next attempt. That leaves
  the deny set empty for up to 5 minutes on that node, a window where a deleted tenant
  re-mints an empty shard (breaking the 410 contract) and a suspended tenant serves normally
  (breaking the 403 contract).

  These helpers give a failed boot load a short-backoff retry (default 1s → 5s → cap 30s)
  and a `[:fathom, :tenants, :denylist, :degraded]` telemetry signal on every attempt the
  set is still unloaded, plus a paired `[:fathom, :tenants, :denylist, :recovered]` when a
  retry finally succeeds — so the exposure window is seconds and alertable rather than a
  silent 5-minute gap. The backoff base/cap are config-tunable (`:tenant_denylist_retry_ms`
  / `:tenant_denylist_retry_cap_ms`) so tests can converge in milliseconds and the test env
  can quiet the app-singleton's (sandbox-ownerless) boot-load failure.
  """

  @default_retry_ms 1_000
  @default_cap_ms 30_000

  @doc "First fast-retry delay after a failed boot load (config `:tenant_denylist_retry_ms`, default 1s)."
  @spec initial_retry_ms() :: pos_integer()
  def initial_retry_ms,
    do: Application.get_env(:fathom, :tenant_denylist_retry_ms, @default_retry_ms)

  @doc "Next backoff delay: 5× the current, capped (config `:tenant_denylist_retry_cap_ms`, default 30s)."
  @spec next_retry_ms(pos_integer()) :: pos_integer()
  def next_retry_ms(current) do
    cap = Application.get_env(:fathom, :tenant_denylist_retry_cap_ms, @default_cap_ms)
    min(current * 5, cap)
  end

  @doc "Emit the degraded signal for `kind` (`:tombstones` | `:suspensions`) with the failure `reason`."
  @spec degraded(atom(), term()) :: :ok
  def degraded(kind, reason) do
    :telemetry.execute(
      [:fathom, :tenants, :denylist, :degraded],
      %{count: 1},
      %{kind: kind, reason: reason}
    )
  end

  @doc "Emit the recovered signal for `kind` when a retry lands the set (pairs with `degraded/2`)."
  @spec recovered(atom()) :: :ok
  def recovered(kind) do
    :telemetry.execute(
      [:fathom, :tenants, :denylist, :recovered],
      %{count: 1},
      %{kind: kind}
    )
  end
end
