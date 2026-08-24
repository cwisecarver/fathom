defmodule Fathom.HranaAuth.Revocations do
  @moduledoc """
  A node-local, TTL-refreshed cache of per-shard Hrana-token revocation state (expert review #31,
  extended for graceful rotation #24), keeping token verification off the Postgres hot path.

  Each entry is `{floor, bumped_at}`: the revocation **floor** (a token minted below it is refused)
  and the instant of the last graceful **rotate** (`nil` after a hard revoke). `HranaAuth.verify/2`
  accepts a token at `floor - 1` while `bumped_at` is within the rotation grace window, so a rotate
  is zero-downtime (mint-new → deploy → the old auto-hardens out) while a revoke is immediate.

  `Fathom.HranaAuth.verify/2` needs this on every stream open. Reading Postgres per open would
  couple the data path to the control plane — the exact cost `Fathom.Directory.Recorder` exists to
  avoid — and a Postgres outage would then 401 all authenticated traffic. So it's cached for
  `:hrana_revocation_ttl_ms` (default 30s):

    * a cache **hit** answers with no Postgres round-trip;
    * a **miss** reads through to `Fathom.Directory.token_floor_info/1` and caches it;
    * a read-through that **errors** (Postgres blip) serves the last-known-good cached value even
      past its TTL — stale-but-safe, never weaker than what this node already knew (round-2 #25).
      With no prior value at all, `:hrana_revocation_on_error` decides: `:fail_open` (default)
      returns floor `0`; `:fail_closed` returns `:unavailable` and the token is refused.

  `put/2` (revoke) and `put/3` (rotate) are called on the acting node so it sees the change
  immediately; other nodes converge within the TTL (or instantly via the notifier push).

  ## Bulk refresh (expert review 2026-07-24 #5)

  Per-shard TTL expiry made the read rate scale with **shard count** rather than with revocation
  events — `distinct_shards_per_node / ttl`, i.e. thousands of Postgres point-reads per second at
  fleet scale, each taking a repo connection from a stream process. Instead, one bounded query per
  node per interval (`Fathom.Directory.revoked_floors/0`, served by a partial index; `token_version`
  defaults to 1, so the set is normally a tiny fraction of the fleet) applies every revoked floor
  through `put_max/3` and then writes a freshness marker **last**.

  A lapsed entry is served while that marker is fresh. **The marker extends freshness for entries
  that already exist — never for an absent one.** That asymmetry is load-bearing: absence from the
  bulk set proves only what the *directory* says, and a Postgres PITR can lower `token_version`, so
  a genuinely-revoked shard can read as unrevoked there. Only the durable per-shard storage floor
  catches that, and only the cold-miss `read_through` consults it. Answering a miss from bulk
  completeness would be an **auth bypass**, not merely a staleness widening — so a miss always takes
  the full read-through, exactly as before.

  If the bulk query fails the marker is simply not advanced, entries expire on their own TTLs, and
  the module degrades to precisely its previous per-shard behaviour (including the stale-serve
  fallback). Fail-safe by construction: no path here can lower a floor or answer a miss.
  """
  use GenServer

  require Logger

  alias Fathom.Directory

  @table __MODULE__
  @default_ttl_ms 30_000
  @bulk_marker :__bulk_ok__

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  The cached `{floor, bumped_at}` for `shard_id` (`bumped_at` is a `DateTime` after a graceful
  rotate, else `nil`), or `:unavailable` when a floor-read outage has no last-known-good value and
  the on-error posture is `:fail_closed`. See the module doc.
  """
  @spec floor_info(String.t()) :: {non_neg_integer(), DateTime.t() | nil} | :unavailable
  def floor_info(shard_id) do
    now = System.monotonic_time(:millisecond)

    case lookup(shard_id, now) do
      {:hit, info} -> info
      :miss -> read_through(shard_id, now)
    end
  end

  @doc "The cached revocation floor for `shard_id` (the integer only; see `floor_info/1`)."
  @spec floor(String.t()) :: non_neg_integer() | :unavailable
  def floor(shard_id) do
    case floor_info(shard_id) do
      :unavailable -> :unavailable
      {version, _bumped_at} -> version
    end
  end

  @doc """
  The cached floor for `shard_id`, WITHOUT the Postgres read-through — `:unknown` on a miss.

  For the per-statement revocation re-check (expert review 2026-08-20 #22), which runs on the hot
  path and must never reach the database. `floor_info/1` falls through to `Directory` on a cold or
  TTL-lapsed entry; that is right at `hello`, where one read per CONNECTION is nothing, and wrong
  per statement.

  A miss answers `:unknown` and the caller ALLOWS. That is deliberate: `authorize/2` already did
  the authoritative check when the connection opened and warmed this entry, and the bulk refresh
  keeps it current, so a miss means "no opinion", not "revoked". Failing closed here would refuse
  live tenant traffic during a Postgres blip — strictly worse than the status quo this fix
  improves on, which was never re-checking at all.
  """
  @spec cached_floor(String.t()) :: {non_neg_integer(), DateTime.t() | nil} | :unknown
  def cached_floor(shard_id) do
    case lookup(shard_id, System.monotonic_time(:millisecond)) do
      {:hit, info} -> info
      :miss -> :unknown
    end
  rescue
    ArgumentError -> :unknown
  end

  @doc "Caches a freshly-revoked floor for `shard_id` (no rotation grace — the previous version dies now)."
  @spec put(String.t(), non_neg_integer()) :: :ok
  def put(shard_id, version), do: put(shard_id, version, nil)

  @doc "Caches a freshly-rotated floor + its bump instant for `shard_id` (grace on for the previous version)."
  @spec put(String.t(), non_neg_integer(), DateTime.t() | nil) :: :ok
  def put(shard_id, version, bumped_at) do
    insert(shard_id, version, bumped_at, System.monotonic_time(:millisecond))
    :ok
  end

  @impl true
  def init(_opts) do
    # public read_concurrency: verify/2 runs in the stream process and reads directly.
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])

    # Fleet-wide push (round-2 #24): without it a change lands on every OTHER node only after a
    # full TTL. Fathom has no BEAM cluster, so the one shared channel is Postgres — Oban's
    # LISTEN/NOTIFY. Best-effort: without it (Oban down, bench harness) the TTL is the backstop.
    listen_for_revocations()

    send(self(), :bulk_refresh)
    {:ok, %{}}
  end

  @impl true
  # One bounded query per node per interval replaces the per-shard TTL read-through, whose rate
  # scaled with SHARD COUNT rather than with revocation events (expert review 2026-07-24 #5). Rows
  # go through `put_max/3`, which is already monotonic and grace-preserving, and the freshness
  # marker is written LAST so it can never vouch for data that has not been applied.
  #
  # On failure the marker is simply not advanced: entries expire on their own TTLs and the module
  # degrades to exactly its previous per-shard behaviour, including the stale-serve fallback.
  # Fail-safe by construction — no path here can lower a floor or answer a miss.
  def handle_info(:bulk_refresh, state) do
    try do
      rows = Directory.revoked_floors()

      Enum.each(rows, fn {shard_id, version, bumped_at} ->
        put_max(shard_id, version, bumped_at)
      end)

      :ets.insert(@table, {@bulk_marker, System.monotonic_time(:millisecond)})

      :telemetry.execute([:fathom, :hrana, :revocation, :bulk], %{revoked: length(rows)}, %{
        outcome: :ok
      })
    rescue
      e ->
        Logger.warning("revocation bulk refresh failed: #{Exception.message(e)}")
        bulk_degraded()
    catch
      :exit, reason ->
        Logger.warning("revocation bulk refresh failed: #{inspect(reason)}")
        bulk_degraded()
    end

    Process.send_after(self(), :bulk_refresh, ttl_ms())
    {:noreply, state}
  end

  def handle_info(
        {:notification, :fathom_revocations,
         %{"shard_id" => shard_id, "version" => version} = msg},
        state
      )
      when is_binary(shard_id) and is_integer(version) do
    put_max(shard_id, version, decode_bumped_at(Map.get(msg, "bumped_at")))
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # An entry is fresh if its own TTL is unexpired OR the last bulk refresh is recent (expert review
  # 2026-07-24 #5). The marker extends freshness for ENTRIES THAT ALREADY EXIST — never for an
  # absent one, which still misses and takes the full `read_through/2` including the storage union.
  #
  # That asymmetry is the whole safety argument, not an optimization detail: absence from the bulk
  # set proves only what the DIRECTORY says, and a Postgres PITR can lower `token_version` so a
  # genuinely-revoked shard reads as unrevoked there. Only the durable per-shard storage floor
  # catches that, and only `read_through/2` consults it. Answering a miss from bulk completeness
  # would be an AUTH BYPASS, not merely a staleness widening.
  defp lookup(shard_id, now) do
    case :ets.lookup(@table, shard_id) do
      [{^shard_id, version, bumped_at, expires_at}] when now < expires_at ->
        {:hit, {version, bumped_at}}

      [{^shard_id, version, bumped_at, _expires_at}] ->
        # TTL lapsed, but a recent bulk refresh already reconciled every revoked shard against the
        # directory, so this entry is as current as a per-shard re-read would make it.
        if bulk_fresh?(now), do: {:hit, {version, bumped_at}}, else: :miss

      _ ->
        :miss
    end
  rescue
    ArgumentError -> :miss
  end

  defp read_through(shard_id, now) do
    {dir_version, dir_bumped} = Directory.token_floor_info(shard_id)
    dir_version = dir_version || 0

    # Monotonic + DR backstop (expert review #6). A Postgres point-in-time restore can LOWER the
    # directory's token_version, un-revoking tokens. So never drop below what this node already
    # cached (protects a running node across a restore at zero storage cost — the floor only ever
    # rises legitimately); and on a genuine COLD miss (empty cache — a node booting after a restore)
    # union the durable storage floor so the revocation survives. The storage read is cold-miss-only,
    # so it never touches the TTL-refresh hot path.
    {version, bumped_at} =
      case stale_floor(shard_id) do
        {:ok, {cached, cached_bumped}} when cached >= dir_version -> {cached, cached_bumped}
        {:ok, _} -> {dir_version, dir_bumped}
        :none -> {max(dir_version, storage_floor(shard_id)), dir_bumped}
      end

    insert(shard_id, version, bumped_at, now)
    {version, bumped_at}
  rescue
    e ->
      Logger.warning("revocation floor read failed for #{shard_id}: #{Exception.message(e)}")
      read_error_fallback(shard_id)
  catch
    :exit, reason ->
      Logger.warning("revocation floor read failed for #{shard_id}: #{inspect(reason)}")
      read_error_fallback(shard_id)
  end

  # See floor_info/1 (round-2 #25). Expired entries are never deleted, only overwritten, so the
  # last-known-good value is still in the table to serve stale.
  defp read_error_fallback(shard_id) do
    :telemetry.execute([:fathom, :hrana, :revocation, :floor_error], %{count: 1}, %{
      shard_id: shard_id
    })

    case stale_floor(shard_id) do
      {:ok, info} ->
        info

      :none ->
        case Application.get_env(:fathom, :hrana_revocation_on_error, :fail_open) do
          :fail_closed -> :unavailable
          _ -> {0, nil}
        end
    end
  end

  # The durable token-revocation floor from storage (the #6 DR backstop), read only on a cold miss.
  # Best-effort: any error → 0, so a storage blip just falls back to the directory floor and never
  # locks out valid traffic (the running-node monotonic guard already covers the common restore case).
  defp storage_floor(shard_id) do
    case Fathom.Shard.Storage.read_token_floor(shard_id) do
      {:ok, v} when is_integer(v) -> v
      _ -> 0
    end
  rescue
    _ -> 0
  catch
    :exit, _ -> 0
  end

  defp stale_floor(shard_id) do
    case :ets.lookup(@table, shard_id) do
      [{^shard_id, version, bumped_at, _expires_at}] -> {:ok, {version, bumped_at}}
      [] -> :none
    end
  rescue
    ArgumentError -> :none
  end

  defp insert(shard_id, version, bumped_at, now) do
    :ets.insert(@table, {shard_id, version, bumped_at, now + ttl_ms()})
  rescue
    ArgumentError -> :ok
  end

  # A late/duplicate notification must never LOWER the floor below what this node already knows
  # (versions only rise). When the version rises, adopt the incoming bump instant; a stale
  # same-version notification keeps whatever grace this node already had.
  defp put_max(shard_id, version, bumped_at) do
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(@table, shard_id) do
      [{^shard_id, current, current_bumped, _expires}] when current >= version ->
        insert(shard_id, current, current_bumped, now)

      _ ->
        insert(shard_id, version, bumped_at, now)
    end
  rescue
    ArgumentError -> :ok
  end

  defp decode_bumped_at(nil), do: nil

  defp decode_bumped_at(iso) when is_binary(iso) do
    case DateTime.from_iso8601(iso) do
      {:ok, dt, _offset} -> dt
      _ -> nil
    end
  end

  defp decode_bumped_at(_), do: nil

  defp listen_for_revocations do
    :ok = Oban.Notifier.listen(Oban, [:fathom_revocations])
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp bulk_degraded do
    :telemetry.execute([:fathom, :hrana, :revocation, :bulk], %{revoked: 0}, %{outcome: :degraded})
  end

  # The bulk-refresh freshness marker. An atom key, so it can never collide with a shard id (always
  # a binary) and `lookup/2` can never mistake it for an entry.
  defp bulk_fresh?(now) do
    case :ets.lookup(@table, @bulk_marker) do
      [{@bulk_marker, at}] -> now < at + ttl_ms()
      _ -> false
    end
  rescue
    ArgumentError -> false
  end

  defp ttl_ms, do: Application.get_env(:fathom, :hrana_revocation_ttl_ms, @default_ttl_ms)
end
