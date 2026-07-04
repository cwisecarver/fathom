defmodule Fathom.HranaAuth.Revocations do
  @moduledoc """
  A node-local, TTL-refreshed cache of per-shard Hrana-token revocation versions
  (expert review #31), keeping token verification off the Postgres hot path.

  `Fathom.HranaAuth.verify/2` needs a shard's current `token_version` (the
  revocation floor) on every stream open. Reading Postgres per open would couple
  the data path to the control plane — the exact cost `Fathom.Directory.Recorder`
  exists to avoid — and a Postgres outage would then 401 all authenticated traffic.

  So each version is cached in ETS for `:hrana_revocation_ttl_ms` (default 30s):

    * a cache **hit** answers with no Postgres round-trip;
    * a **miss** reads through to `Fathom.Directory.token_version/1` and caches it;
    * a read-through that **errors** (Postgres blip) returns `0` — the "no
      revocations known" floor — so a **validly-signed** token still opens. This is
      the deliberate fail-open posture: the signature is still required (a leaked
      credential is the pre-existing risk, unchanged), and revocation is
      eventually-consistent, converging within the TTL once Postgres recovers —
      the same best-effort contract the whole directory has.

  `bump/1` is called by `Fathom.HranaAuth.revoke/1` after it bumps the directory
  version, so the revoking node sees the new floor immediately; other nodes
  converge within the TTL.
  """
  use GenServer

  require Logger

  alias Fathom.Directory

  @table __MODULE__
  @default_ttl_ms 30_000

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  The cached revocation floor for `shard_id`. A hit returns instantly; a miss reads
  through to the directory and caches it. A read-through ERROR (Postgres outage)
  serves the last-known-good cached floor even past its TTL — stale-but-safe, never
  weaker than what this node already knew (expert review round-2 #25: collapsing to
  `0` silently re-activated every revoked credential fleet-wide after one TTL,
  coupling an availability failure to a security-control bypass). With no prior
  value at all, `:hrana_revocation_on_error` decides: `:fail_open` (default — the
  signature stays the enforced control) returns `0`; `:fail_closed` returns
  `:unavailable` and the token is refused. Every error path emits
  `[:fathom, :hrana, :revocation, :floor_error]` so a floor-read outage is visible
  as a security-control-down, not just a log line.
  """
  @spec floor(String.t()) :: non_neg_integer() | :unavailable
  def floor(shard_id) do
    now = System.monotonic_time(:millisecond)

    case lookup(shard_id, now) do
      {:hit, version} -> version
      :miss -> read_through(shard_id, now)
    end
  end

  @doc "Caches a freshly-bumped version for `shard_id` (called on the revoking node)."
  @spec put(String.t(), non_neg_integer()) :: :ok
  def put(shard_id, version) do
    insert(shard_id, version, System.monotonic_time(:millisecond))
    :ok
  end

  @impl true
  def init(opts) do
    # public read_concurrency: verify/2 runs in the stream process and reads directly.
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])

    # Fleet-wide revocation push (expert review round-2 #24): a revoked token kept
    # working on every OTHER node for up to a full TTL, because only the revoking
    # node's cache was bumped. Fathom has no BEAM cluster, so Phoenix.PubSub cannot
    # cross nodes — the one shared channel is Postgres, and Oban's LISTEN/NOTIFY
    # notifier is already running on it. Best-effort: without it (Oban down, bench
    # harness), the TTL remains the convergence backstop.
    listen_for_revocations()
    {:ok, %{ttl_ms: Keyword.get(opts, :ttl_ms, ttl_ms())}}
  end

  @impl true
  def handle_info(
        {:notification, :fathom_revocations, %{"shard_id" => shard_id, "version" => version}},
        state
      )
      when is_binary(shard_id) and is_integer(version) do
    put_max(shard_id, version)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp lookup(shard_id, now) do
    case :ets.lookup(@table, shard_id) do
      [{^shard_id, version, expires_at}] when now < expires_at -> {:hit, version}
      _ -> :miss
    end
  rescue
    ArgumentError -> :miss
  end

  defp read_through(shard_id, now) do
    version = Directory.token_version(shard_id) || 0
    insert(shard_id, version, now)
    version
  rescue
    e ->
      Logger.warning("revocation floor read failed for #{shard_id}: #{Exception.message(e)}")
      read_error_fallback(shard_id)
  catch
    :exit, reason ->
      Logger.warning("revocation floor read failed for #{shard_id}: #{inspect(reason)}")
      read_error_fallback(shard_id)
  end

  # See floor/1 (round-2 #25). Expired entries are never deleted, only overwritten,
  # so the last-known-good floor is still in the table to serve stale.
  defp read_error_fallback(shard_id) do
    :telemetry.execute([:fathom, :hrana, :revocation, :floor_error], %{count: 1}, %{
      shard_id: shard_id
    })

    case stale_floor(shard_id) do
      {:ok, version} ->
        version

      :none ->
        case Application.get_env(:fathom, :hrana_revocation_on_error, :fail_open) do
          :fail_closed -> :unavailable
          _ -> 0
        end
    end
  end

  defp stale_floor(shard_id) do
    case :ets.lookup(@table, shard_id) do
      [{^shard_id, version, _expires_at}] -> {:ok, version}
      [] -> :none
    end
  rescue
    ArgumentError -> :none
  end

  defp insert(shard_id, version, now) do
    :ets.insert(@table, {shard_id, version, now + ttl_ms()})
  rescue
    ArgumentError -> :ok
  end

  # A late/duplicate notification must never LOWER the floor below what this node
  # already knows (versions only rise; even an expired entry's version is a valid
  # lower bound).
  defp put_max(shard_id, version) do
    current =
      case :ets.lookup(@table, shard_id) do
        [{^shard_id, v, _expires_at}] -> v
        [] -> 0
      end

    insert(shard_id, max(version, current), System.monotonic_time(:millisecond))
  rescue
    ArgumentError -> :ok
  end

  defp listen_for_revocations do
    :ok = Oban.Notifier.listen(Oban, [:fathom_revocations])
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp ttl_ms, do: Application.get_env(:fathom, :hrana_revocation_ttl_ms, @default_ttl_ms)
end
