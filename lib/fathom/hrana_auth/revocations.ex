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
  node per interval applies the revoked floors through `put_max/3` and then writes a freshness
  marker **last**.

  The interval's read is INCREMENTAL (expert review 2026-10-13 #10): only rows whose
  `token_version_changed_at` (Postgres clock) is at or after the last successful refresh's cursor
  minus an overlap, via `Directory.revoked_floors_since/1`. The previous shape re-read every shard
  ever rotated or revoked on every node every interval, which after a fleet-wide
  `revoke_issued_before` is the whole fleet. A FULL keyset-paged reload
  (`Directory.revoked_floors_page/2`) runs at boot and then daily, to cover rows that predate the
  cursor column and anything an incremental read missed. It runs in a monitored process so this
  GenServer keeps serving notifications; its pages are applied HERE, so `put_max/3` keeps a single
  writer. **The marker is written only once a full reload has completed**, and the cursor advances
  only on a successful read, so the marker still never vouches for data that was not applied.

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
  @max_entries 200_000
  @full_page 5_000
  @overlap_ms 60_000
  @default_full_reload_ms 86_400_000

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
      {:hit, info} ->
        info

      :miss ->
        # A `lookup` miss is TWO different situations, and collapsing both to :unknown (which
        # version_current?/2 ALLOWS) is a revocation bypass (expert review 2026-09-05 #10). A
        # present-but-expired entry that the bulk marker cannot vouch for still PROVES this node
        # already knew the credential was revoked to `version`, and a floor only rises — so serving
        # it is stale-but-safe, never weaker than what this node knew. Dropping it re-admitted a
        # revoked token on every OPEN connection whenever the bulk marker was stale: for a few ms
        # every cycle in steady state (entry TTL and marker freshness lapse together), and
        # UNBOUNDEDLY during a bulk-refresh stall (a control-plane outage — exactly when a
        # compromise-response revoke_issued_before sweep runs, the 2026-08-20 #22 hole reopened).
        # `:unknown` must mean "no entry AT ALL", where authorize/2's hello-time read_through was the
        # last word. `lookup/2` itself is left unchanged so floor_info/1 (the authoritative hello
        # path) still reads through on a genuine miss.
        case stale_floor(shard_id) do
          {:ok, info} -> info
          :none -> :unknown
        end
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
    # Verify-result cache for `HranaAuth` (expert review 2026-10-17 #17); this process owns it.
    :ets.new(Fathom.HranaAuth.verify_cache_table(), [
      :set,
      :public,
      :named_table,
      read_concurrency: true
    ])

    # Fleet-wide push (round-2 #24): without it a change lands on every OTHER node only after a
    # full TTL. Fathom has no BEAM cluster, so the one shared channel is Postgres — Oban's
    # LISTEN/NOTIFY. Best-effort: without it (Oban down, bench harness) the TTL is the backstop.
    listen_for_revocations()

    send(self(), :bulk_refresh)

    # `since`: the incremental cursor (Postgres clock minus overlap), nil until a full reload has
    # completed. `synced`: a full reload has completed, so incremental reads (and the marker) are
    # valid. `full`: the in-flight full reload, if any. `last_full`: when the last one finished.
    {:ok, %{since: nil, synced: false, full: nil, last_full: nil}}
  end

  @impl true
  # Incremental by default, full at boot and daily (expert review 2026-10-13 #10). Rows go through
  # `put_max/3`, which is already monotonic and grace-preserving, and the freshness marker is written
  # LAST so it can never vouch for data that has not been applied.
  #
  # On failure the marker is simply not advanced and the cursor is not moved: entries expire on their
  # own TTLs and the module degrades to exactly its previous per-shard behaviour, including the
  # stale-serve fallback. Fail-safe by construction — no path here can lower a floor or answer a
  # miss.
  def handle_info(:bulk_refresh, state) do
    Process.send_after(self(), :bulk_refresh, ttl_ms())
    {:noreply, refresh(state)}
  end

  # A page of the full reload, applied here so `put_max/3` keeps a single writer.
  def handle_info({:full_page, ref, rows}, %{full: %{ref: ref} = full} = state) do
    apply_rows(rows)
    {:noreply, %{state | full: %{full | count: full.count + length(rows)}}}
  end

  def handle_info({:full_done, ref, :ok}, %{full: %{ref: ref} = full} = state) do
    Process.demonitor(full.mon, [:flush])
    mark_fresh()
    bulk_ok(full.count)
    # `full.cursor` was taken BEFORE the full read began, so an incremental that ran meanwhile (it
    # only runs once synced) already holds an equal-or-later one: never move it backward.
    since = state.since || full.cursor
    {:noreply, %{state | full: nil, synced: true, since: since, last_full: now_ms()}}
  end

  def handle_info({:full_done, ref, {:error, message}}, %{full: %{ref: ref} = full} = state) do
    Process.demonitor(full.mon, [:flush])
    Logger.warning("revocation full reload failed: #{message}")
    bulk_degraded()
    {:noreply, %{state | full: nil}}
  end

  def handle_info({:DOWN, mon, :process, _pid, reason}, %{full: %{mon: mon}} = state) do
    Logger.warning("revocation full reload died: #{inspect(reason)}")
    bulk_degraded()
    {:noreply, %{state | full: nil}}
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
    # On a genuine COLD miss (no cached value) the directory floor and the durable storage floor are
    # INDEPENDENT reads, so they run concurrently (perf review 2026-10-14 #23): wall time is
    # max(PG, S3) instead of PG + S3. Both are still always consulted, so the union is unchanged.
    # A warm-but-lapsed entry never reads storage, as before.
    stale = stale_floor(shard_id)
    storage_task = if stale == :none, do: Task.async(fn -> storage_floor(shard_id) end)

    try do
      {dir_version, dir_bumped} = Directory.token_floor_info(shard_id)
      dir_version = dir_version || 0

      # Monotonic + DR backstop (expert review #6). A Postgres point-in-time restore can LOWER the
      # directory's token_version, un-revoking tokens. So never drop below what this node already
      # cached (protects a running node across a restore at zero storage cost — the floor only ever
      # rises legitimately); and on a cold miss union the durable storage floor so the revocation
      # survives.
      {version, bumped_at} =
        case stale do
          {:ok, {cached, cached_bumped}} when cached >= dir_version -> {cached, cached_bumped}
          {:ok, _} -> {dir_version, dir_bumped}
          :none -> {max(dir_version, Task.await(storage_task, :infinity)), dir_bumped}
        end

      insert(shard_id, version, bumped_at, now)
      {version, bumped_at}
    after
      # A directory error must not leave the storage task's reply in the caller's mailbox.
      if storage_task, do: Task.shutdown(storage_task, :brutal_kill)
    end
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
        # The directory (Postgres) is unreadable and this node has no cached floor. Under
        # `:fail_open` the durable storage floor is an INDEPENDENT source (mirrored on every
        # revoke), so consult it before taking the blind posture (expert review 2026-10-10 #7): a
        # PG blip on a cold shard (a failover/rebalance target) used to serve floor 0 and re-accept
        # every revoked token even though S3 knew the floor. A positive storage floor is used as-is
        # (not cached, like every error-path answer).
        #
        # Under `:fail_closed` the storage floor is NOT served (expert review 2026-10-10 fix-review
        # R4-1): it is best-effort and may LAG the directory, so answering from it would admit
        # tokens a revoke the directory already knows about invalidated -- the opposite of what an
        # operator choosing fail_closed asked for. The answer stays `:unavailable`.
        case Application.get_env(:fathom, :hrana_revocation_on_error, :fail_open) do
          :fail_closed ->
            :unavailable

          _ ->
            case storage_floor_result(shard_id) do
              {:ok, v} when is_integer(v) and v > 0 -> {v, nil}
              _ -> {0, nil}
            end
        end
    end
  end

  # The durable token-revocation floor from storage (the #6 DR backstop), read only on a cold miss.
  # Best-effort: any error → 0, so a storage blip just falls back to the directory floor and never
  # locks out valid traffic (the running-node monotonic guard already covers the common restore case).
  defp storage_floor(shard_id) do
    case storage_floor_result(shard_id) do
      {:ok, v} when is_integer(v) -> v
      _ -> 0
    end
  end

  defp storage_floor_result(shard_id) do
    Fathom.Shard.Storage.read_token_floor(shard_id)
  rescue
    _ -> {:error, :storage_floor_read_failed}
  catch
    :exit, _ -> {:error, :storage_floor_read_failed}
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
    evict_unrevoked_at_cap()
    :ets.insert(@table, {shard_id, version, bumped_at, now + ttl_ms()})
  rescue
    ArgumentError -> :ok
  end

  # Bound the table (perf review 2026-10-14 #23): entries were never evicted, so it grew by one row
  # per shard ever authenticated. At the cap, drop the floor-0 (never revoked) entries in one pass —
  # a re-read costs one cold-miss read-through, exactly as for a never-seen shard. Revoked entries
  # (floor > 0) are kept: they are the bulk-loaded set and the stale-serve source, and are bounded
  # by revocations, not by shard count. The 2-tuple freshness marker never matches the 4-tuple
  # pattern. Same shape as `Fathom.Shards.KnownShards`.
  defp evict_unrevoked_at_cap do
    if :ets.info(@table, :size) >= max_entries() do
      :ets.match_delete(@table, {:_, 0, :_, :_})
    end
  end

  @doc false
  @spec max_entries() :: pos_integer()
  def max_entries, do: Application.get_env(:fathom, :hrana_revocation_max_entries, @max_entries)

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

  # One refresh tick, public so a test can drive it with a hand-built state.
  @doc false
  @spec refresh(map()) :: map()
  def refresh(%{full: nil} = state) do
    if not state.synced or full_due?(state), do: start_full(state), else: incremental(state)
  end

  # A full reload is in flight. Incrementals continue once synced (a long full must not stall
  # freshness); before the first full completes there is nothing valid to be incremental against.
  def refresh(%{synced: true} = state), do: incremental(state)
  def refresh(state), do: state

  defp full_due?(%{last_full: nil}), do: true
  defp full_due?(%{last_full: at}), do: now_ms() - at >= full_reload_ms()

  defp incremental(%{since: since} = state) do
    # The cursor is taken BEFORE the read, on the Postgres clock the rows are stamped from, minus an
    # overlap so a transaction that began earlier but committed later cannot slip below it.
    next_since = cursor_now()
    rows = Directory.revoked_floors_since(since)
    apply_rows(rows)
    mark_fresh()
    bulk_ok(length(rows))
    %{state | since: next_since}
  rescue
    e ->
      Logger.warning("revocation bulk refresh failed: #{Exception.message(e)}")
      bulk_degraded()
      state
  catch
    :exit, reason ->
      Logger.warning("revocation bulk refresh failed: #{inspect(reason)}")
      bulk_degraded()
      state
  end

  defp start_full(state) do
    parent = self()
    ref = make_ref()

    # The cursor is taken before the first page is read, for the same reason as in `incremental/1`.
    cursor = safe_cursor_now()

    {pid, mon} =
      spawn_monitor(fn ->
        # Ecto's sandbox (tests) resolves the owner through `$callers`.
        Process.put(:"$callers", [parent])
        send(parent, {:full_done, ref, full_reload(parent, ref, "")})
      end)

    %{state | full: %{ref: ref, mon: mon, pid: pid, cursor: cursor, count: 0}}
  end

  # Pages are sent to the GenServer, not applied here.
  defp full_reload(parent, ref, after_id) do
    case Directory.revoked_floors_page(after_id, @full_page) do
      [] ->
        :ok

      rows ->
        send(parent, {:full_page, ref, rows})
        {last_id, _, _} = List.last(rows)
        if length(rows) < @full_page, do: :ok, else: full_reload(parent, ref, last_id)
    end
  rescue
    e -> {:error, Exception.message(e)}
  catch
    :exit, reason -> {:error, inspect(reason)}
  end

  defp apply_rows(rows) do
    Enum.each(rows, fn {shard_id, version, bumped_at} -> put_max(shard_id, version, bumped_at) end)
  end

  defp mark_fresh, do: :ets.insert(@table, {@bulk_marker, now_ms()})

  defp bulk_ok(n) do
    :telemetry.execute([:fathom, :hrana, :revocation, :bulk], %{revoked: n}, %{outcome: :ok})
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp full_reload_ms,
    do: Application.get_env(:fathom, :hrana_revocation_full_reload_ms, @default_full_reload_ms)

  defp cursor_now, do: DateTime.add(Directory.db_now(), -@overlap_ms, :millisecond)

  # The full reload's cursor must not fail the reload: fall back to the node clock (the overlap and
  # the next successful incremental re-seed it).
  defp safe_cursor_now do
    cursor_now()
  rescue
    _ -> DateTime.add(DateTime.utc_now(), -@overlap_ms, :millisecond)
  catch
    :exit, _ -> DateTime.add(DateTime.utc_now(), -@overlap_ms, :millisecond)
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
