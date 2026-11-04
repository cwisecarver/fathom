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

  A COLD miss (no cached value at all) also unions the durable per-shard STORAGE floor, the PITR
  backstop, read concurrently with the directory. That read sits on the `hello` critical path, so
  it is bounded and shared (expert review 2026-10-29 #20):

    * ONE attempt, no retries, capped at `:hrana_storage_floor_budget_ms` (default 3 s;
      `HRANA_STORAGE_FLOOR_BUDGET_MS`). It used to be an unbounded wait over Req's default retried
      GET, ~60 s against a stalled store.
    * If it cannot be read in budget (timeout or error) the same posture applies as for a directory
      error: `:fail_closed` returns `:unavailable`; `:fail_open` serves the directory floor it
      already holds, UNCACHED (never replaced by a cached directory-only answer), without a second
      GET. On the success path the storage floor is always unioned in.
    * A failed read starts a per-shard cool-down (`:hrana_storage_floor_backoff_ms`, default 30 s)
      in which that shard's cold hellos skip the read and take the same posture at once, so an
      outage costs one budget per shard per cool-down, not one per hello. The first hello after
      it reads storage again.
    * N concurrent hellos for one cold shard share one storage read and one directory read
      (per-shard single-flight; a crashed claimant hands over to a waiter).

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
  # Single-flight state for cold-miss read-throughs (expert review 2026-10-29 #20): the claim per
  # shard (`{shard_id, claimant_pid}`), and the followers waiting on it (`{shard_id, ref, pid}`).
  # Owned by this GenServer like `@table`, but only ever written by the callers.
  @flights Module.concat(__MODULE__, Flights)
  @followers Module.concat(__MODULE__, Followers)
  @flight_attempts 3
  # The durable storage floor is read as ONE attempt of at most this many ms (no retries). Sized from
  # the heartbeat's per-attempt budget, `renew_ms / 3` of a 30 s TTL ≈ 3.3 s: the same "give up on
  # one S3 attempt well inside the cadence that needs the answer" reasoning, here the client's
  # hello. Override with `:hrana_storage_floor_budget_ms` (`HRANA_STORAGE_FLOOR_BUDGET_MS`).
  @default_floor_budget_ms 3_000
  # A follower's own bound on waiting for a claimant: the directory read (Repo `timeout`, 15 s by
  # default) plus the storage budget, with slack. The claimant normally answers far sooner.
  @follow_grace_ms 20_000
  # After a storage-floor read fails, cold hellos for that shard skip the read for this long (see
  # `fetch/3`). Matches the entry TTL: the cost of an outage is one budget per shard per TTL, as it
  # was before the read was bounded. Override with `:hrana_storage_floor_backoff_ms`.
  @storage_backoff Module.concat(__MODULE__, StorageBackoff)
  @default_storage_backoff_ms 30_000
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

    # Cold-miss single-flight (expert review 2026-10-29 #20). Keyed per shard, so no cross-shard
    # contention; `write_concurrency` because every cold miss writes.
    :ets.new(@flights, [:set, :public, :named_table, write_concurrency: true])
    :ets.new(@followers, [:bag, :public, :named_table, write_concurrency: true])
    :ets.new(@storage_backoff, [:set, :public, :named_table, read_concurrency: true])

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

  @impl true
  def handle_cast(:evict, state) do
    # A burst of casts at the cap collapses: once a batch has brought the table under the cap the
    # rest are no-ops.
    if :ets.info(@table, :size) >= max_entries(), do: evict_unrevoked()
    {:noreply, state}
  end

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

  # A warm-but-lapsed entry re-reads only the directory (never storage, as before) and is not
  # single-flighted: it needs the bulk marker stale too, and one point-read each is the old cost.
  # A genuine COLD miss is the expensive one (directory read + a storage GET, on the `hello`
  # critical path), so N concurrent hellos for one cold shard share ONE of each (expert review
  # 2026-10-29 #20).
  defp read_through(shard_id, now) do
    case stale_floor(shard_id) do
      :none -> single_flight(shard_id, now, @flight_attempts)
      stale -> fetch(shard_id, now, stale)
    end
  end

  # SINGLE-FLIGHT, per shard, with no process in the middle. `@flights` is `{shard_id, claimant}`
  # (`insert_new` is the claim); `@followers` is a bag of `{shard_id, ref, pid}` waiting on it. The
  # claimant runs `fetch/3` in its own (stream) process and then sends every follower the result.
  # Different shards touch different keys, so nothing here serializes across shards.
  #
  # Crash-safety, because the claimant is a stream/socket process that dies when its client
  # disconnects: followers MONITOR the claimant, and a DOWN clears the (never-released) claim and
  # retries, so one of them becomes the next claimant. A follower also bounds its own wait
  # (`follow_timeout_ms/0`) and, on exhausting its retries or timing out, reads for itself — the
  # worst case is the old unshared behaviour, never a wedge.
  defp single_flight(shard_id, now, attempts_left) do
    case claim(shard_id) do
      :claimed -> lead(shard_id, now)
      {:held_by, claimant} when attempts_left > 0 -> follow(shard_id, claimant, attempts_left)
      # Retries spent, or the tables are not up (a bench harness without this GenServer).
      _ -> fetch(shard_id, now, :none)
    end
  end

  defp claim(shard_id) do
    if :ets.insert_new(@flights, {shard_id, self()}) do
      :claimed
    else
      case :ets.lookup(@flights, shard_id) do
        [{^shard_id, claimant}] -> {:held_by, claimant}
        # Released between the failed insert and the lookup: take another turn.
        [] -> claim(shard_id)
      end
    end
  rescue
    ArgumentError -> :no_table
  end

  # A claimant that RAISES must not keep the claim. `fetch/3` maps every expected failure to an
  # answer, but anything it does not (a bad `:hrana_storage_floor_budget_ms` set at runtime is an
  # ArithmeticError before the reads start) used to skip `release/2`. If the caller caught the
  # raise and lived on, nothing ever cleared the claim: followers watch for the claimant's DOWN,
  # which never came, so every cold hello for the shard waited out `follow_timeout_ms/0` (~23 s)
  # before reading for itself (review of 9a97ec71, 2026-10-31). Now the claim is dropped, the
  # parked followers are told to retry, and the raise continues to the caller unchanged.
  defp lead(shard_id, now) do
    result = fetch(shard_id, now, :none)
    release(shard_id, result)
    result
  catch
    kind, reason ->
      abandon(shard_id)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  # Release the claim BEFORE draining the followers. A follower registers and THEN re-checks the
  # claim: seeing it still held proves its registration precedes this drain (so it will be served);
  # seeing it gone, it withdraws, and `withdraw/2` tells it whether we already took it. A vanished
  # table (restarted owner) is not worth failing a hello over.
  #
  # The followers drained here may include ones that registered behind the NEXT claimant (this one
  # deleted its claim, another claimed, a follower registered under it, and then this drain took
  # it). Left as is on purpose: the answer it gets is a just-read floor for the same shard, as fresh
  # as the next claimant's, and the next claimant finds no one to send to. Keying followers by
  # claimant would change nothing a caller can see (review of 9a97ec71, 2026-10-31).
  defp release(shard_id, result) do
    :ets.delete_object(@flights, {shard_id, self()})
    for {_shard, ref, pid} <- :ets.take(@followers, shard_id), do: send(pid, {ref, result})
    :ok
  rescue
    ArgumentError -> :ok
  end

  # `release/2` for a claimant with no answer: the same ordering, but each follower is told to go
  # again (`await_lead/5` retries, exactly as on the claimant's DOWN).
  defp abandon(shard_id) do
    :ets.delete_object(@flights, {shard_id, self()})
    for {_shard, ref, pid} <- :ets.take(@followers, shard_id), do: send(pid, {:abandoned, ref})
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp follow(shard_id, claimant, attempts_left) do
    ref = make_ref()
    mon = Process.monitor(claimant)
    :ets.insert(@followers, {shard_id, ref, self()})

    held? = :ets.lookup(@flights, shard_id) == [{shard_id, claimant}]

    if held? or not withdraw(shard_id, ref) do
      await_lead(shard_id, claimant, ref, mon, attempts_left)
    else
      Process.demonitor(mon, [:flush])
      retry_follow(shard_id, attempts_left)
    end
  rescue
    ArgumentError -> fetch(shard_id, System.monotonic_time(:millisecond), :none)
  end

  # `true` if our registration was still there (the claimant never drained it, and now will not);
  # `false` if the claimant already took it, so a result is on its way.
  defp withdraw(shard_id, ref) do
    :ets.select_delete(@followers, [{{shard_id, ref, self()}, [], [true]}]) == 1
  end

  defp await_lead(shard_id, claimant, ref, mon, attempts_left) do
    receive do
      {^ref, result} ->
        Process.demonitor(mon, [:flush])
        result

      # The claimant raised and gave its claim up (`abandon/1`): go again, as on its DOWN.
      {:abandoned, ^ref} ->
        Process.demonitor(mon, [:flush])
        retry_follow(shard_id, attempts_left)

      {:DOWN, ^mon, :process, _pid, _reason} ->
        # The claimant died without releasing (killed, or its client went away mid-read). Clear its
        # claim so the next claim can succeed, drop our registration, and go again.
        :ets.delete_object(@flights, {shard_id, claimant})
        withdraw(shard_id, ref)
        retry_follow(shard_id, attempts_left)
    after
      follow_timeout_ms() ->
        Process.demonitor(mon, [:flush])

        # Taken but not yet delivered: absorb it so no stray message lands in the caller's mailbox.
        if not withdraw(shard_id, ref) do
          receive do
            {^ref, _result} -> :ok
            {:abandoned, ^ref} -> :ok
          after
            1_000 -> :ok
          end
        end

        fetch(shard_id, System.monotonic_time(:millisecond), :none)
    end
  end

  # After a lead finished (or died), its answer is in the cache when it succeeded; otherwise this
  # caller claims and reads. `attempts_left` bounds the loop so a pathological claim churn degrades
  # to an unshared read rather than spinning.
  defp retry_follow(shard_id, attempts_left) do
    now = System.monotonic_time(:millisecond)

    case lookup(shard_id, now) do
      {:hit, info} -> info
      :miss -> single_flight(shard_id, now, attempts_left - 1)
    end
  end

  # One read-through: on a COLD miss (`stale == :none`) the directory floor and the durable storage
  # floor are INDEPENDENT reads, so they run concurrently (perf review 2026-10-14 #23): wall time is
  # max(PG, S3) instead of PG + S3. Both are still always consulted, so the union is unchanged.
  #
  # The storage read is BOUNDED (expert review 2026-10-29 #20): one attempt, no retries, capped at
  # `:hrana_storage_floor_budget_ms` of wire time (`Storage.read_token_floor/2`) and, as a second
  # wall, abandoned by `Task.yield` at the same instant. It used to be `Task.await(…, :infinity)`
  # over Req's default GET (3 retries, 15 s receive / 5 s pool per attempt): a stalled store held a
  # hello ~60 s, before checkout and before the coordinator's own pull budget even started. The
  # deadline is taken at task start, so the directory read and the storage read still overlap and
  # the worst case is max(directory, budget), not their sum.
  #
  # Never raises: every failure maps to `read_error_fallback/3` or `storage_unreadable/2`.
  #
  # A read that comes back `:unreadable` starts a per-shard cool-down (`:hrana_storage_floor_backoff_ms`,
  # default 30 s). During it a cold hello for that shard does not try storage at all: it takes the
  # unreadable posture at once (`:fail_open` serves the directory floor, uncached; `:fail_closed`
  # answers `:unavailable`). Without it, the uncached fail-open answer sent EVERY hello for a cold
  # shard back to storage, so an S3 outage added the full budget (3 s) to every connection open
  # for as long as it lasted; single-flight only merged hellos that arrived together (review of
  # 9a97ec71, 2026-10-31). The cool-down never serves a floor of its own and is not the entry
  # cache: what is served during it is exactly what the failed read served.
  defp fetch(shard_id, now, stale) do
    budget = floor_budget_ms()
    deadline = System.monotonic_time(:millisecond) + budget
    read_storage? = stale == :none and not storage_backing_off?(shard_id)
    storage_task = if read_storage?, do: Task.async(fn -> storage_floor(shard_id, budget) end)
    dir = read_directory(shard_id)

    storage =
      cond do
        stale != :none -> :skipped
        is_nil(storage_task) -> :unreadable
        # Nothing will consult the answer: a stale value is served as-is, and `:fail_closed` never
        # serves the storage floor (see `read_error_fallback/3`). Don't wait out a budget for it.
        dir == :error and fail_closed?() -> discard(storage_task)
        true -> storage_task |> await_storage(deadline) |> note_storage(shard_id)
      end

    resolve(shard_id, now, stale, dir, storage)
  end

  defp note_storage(:unreadable, shard_id) do
    back_off_storage(shard_id)
    :unreadable
  end

  defp note_storage(ok, shard_id) do
    clear_storage_backoff(shard_id)
    ok
  end

  defp read_directory(shard_id) do
    {dir_version, dir_bumped} = Directory.token_floor_info(shard_id)
    {:ok, {dir_version || 0, dir_bumped}}
  rescue
    e ->
      Logger.warning("revocation floor read failed for #{shard_id}: #{Exception.message(e)}")
      :error
  catch
    :exit, reason ->
      Logger.warning("revocation floor read failed for #{shard_id}: #{inspect(reason)}")
      :error
  end

  # `{:ok, floor}` (0 when the store holds none) or `:unreadable` — a timeout, an error status, a
  # transport error and a dead task are all the same answer to the caller: the PITR backstop could
  # not be consulted.
  defp await_storage(task, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    case Task.yield(task, remaining) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:ok, floor}} when is_integer(floor) -> {:ok, floor}
      {:ok, {:ok, nil}} -> {:ok, 0}
      _ -> :unreadable
    end
  end

  defp discard(task) do
    Task.shutdown(task, :brutal_kill)
    :skipped
  end

  # A directory floor in hand.
  defp resolve(shard_id, now, stale, {:ok, {dir_version, dir_bumped}}, storage) do
    # Monotonic + DR backstop (expert review #6). A Postgres point-in-time restore can LOWER the
    # directory's token_version, un-revoking tokens. So never drop below what this node already
    # cached (protects a running node across a restore at zero storage cost — the floor only ever
    # rises legitimately); and on a cold miss union the durable storage floor so the revocation
    # survives. The storage floor is never dropped on the SUCCESS path.
    case {stale, storage} do
      {{:ok, {cached, cached_bumped}}, _} when cached >= dir_version ->
        cache(shard_id, cached, cached_bumped, now)

      {{:ok, _}, _} ->
        cache(shard_id, dir_version, dir_bumped, now)

      {:none, {:ok, storage_floor}} ->
        cache(shard_id, max(dir_version, storage_floor), dir_bumped, now)

      {:none, :unreadable} ->
        storage_unreadable(shard_id, {dir_version, dir_bumped})
    end
  end

  defp resolve(shard_id, _now, stale, :error, storage),
    do: read_error_fallback(shard_id, stale, storage)

  defp cache(shard_id, version, bumped_at, now) do
    insert(shard_id, version, bumped_at, now)
    {version, bumped_at}
  end

  # The directory answered but the durable storage floor could not be read in budget, on a cold
  # miss. The same posture as a directory read error (`read_error_fallback/3`): `:fail_closed`
  # refuses (`:unavailable`) — the operator asked for no acceptance without full floor knowledge —
  # and `:fail_open` serves the directory floor it already holds. That answer is NOT cached: it
  # would stand in for the storage floor for a whole TTL (longer under a fresh bulk marker) and
  # turn a brownout into a window in which a PITR-lowered floor is believed. The next hello for the
  # shard reads storage again (one bounded attempt, shared by concurrent hellos). Never a second
  # GET inside this call.
  #
  # A fast storage ERROR is classed with the timeout, not served as floor 0 as it was before: with
  # `receive_timeout` equal to the wall, a stalled store surfaces as a transport error as often as
  # a `Task.yield` timeout, and the two cannot be told apart by the caller.
  defp storage_unreadable(shard_id, dir_info) do
    floor_error(shard_id)
    Logger.warning("revocation storage floor unreadable for #{shard_id}")
    if fail_closed?(), do: :unavailable, else: dir_info
  end

  # See floor_info/1 (round-2 #25). Expired entries are never deleted, only overwritten, so the
  # last-known-good value is still in the table to serve stale.
  defp read_error_fallback(shard_id, stale, storage) do
    floor_error(shard_id)

    case stale do
      {:ok, info} ->
        info

      :none ->
        # The directory (Postgres) is unreadable and this node has no cached floor. Under
        # `:fail_open` the durable storage floor is an INDEPENDENT source (mirrored on every
        # revoke), so use it before taking the blind posture (expert review 2026-10-10 #7): a
        # PG blip on a cold shard (a failover/rebalance target) used to serve floor 0 and re-accept
        # every revoked token even though S3 knew the floor. A positive storage floor is used as-is
        # (not cached, like every error-path answer). It is the answer of THIS call's one bounded
        # read (`fetch/3` started it before the directory read failed): no second GET, so a
        # storage stall cannot be paid twice (expert review 2026-10-29 #20).
        #
        # Under `:fail_closed` the storage floor is NOT served (expert review 2026-10-10 fix-review
        # R4-1): it is best-effort and may LAG the directory, so answering from it would admit
        # tokens a revoke the directory already knows about invalidated -- the opposite of what an
        # operator choosing fail_closed asked for. The answer stays `:unavailable`.
        if fail_closed?() do
          :unavailable
        else
          case storage do
            {:ok, floor} when floor > 0 -> {floor, nil}
            _ -> {0, nil}
          end
        end
    end
  end

  defp floor_error(shard_id) do
    :telemetry.execute([:fathom, :hrana, :revocation, :floor_error], %{count: 1}, %{
      shard_id: shard_id
    })
  end

  defp fail_closed?,
    do: Application.get_env(:fathom, :hrana_revocation_on_error, :fail_open) == :fail_closed

  # The durable token-revocation floor from storage (the #6 DR backstop), read only on a cold miss,
  # as ONE attempt of at most `budget` ms of wire time (see `fetch/3`).
  defp storage_floor(shard_id, budget) do
    Fathom.Shard.Storage.read_token_floor(shard_id, budget_ms: budget)
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
    request_eviction_at_cap()
    :ets.insert(@table, {shard_id, version, bumped_at, now + ttl_ms()})
  rescue
    ArgumentError -> :ok
  end

  # Bound the table (perf review 2026-10-14 #23; expert review 2026-11-04 #2). The caller (a hello
  # or the GenServer itself) does only an O(1) size check and, at the cap, a cast; the eviction runs
  # in this GenServer (`evict_unrevoked/0`). It used to be an inline `match_delete` in the caller's
  # process that targeted floor 0 while real rows carry floor 1 (`token_version` defaults to 1), so
  # it deleted nothing, the table grew without bound, and every insert at the cap paid a full-table
  # scan under the write lock. Not just "change the pattern to 1": a one-shot wipe is a node-wide
  # wave of cold storage-floor reads, hence a bounded batch.
  defp request_eviction_at_cap do
    if :ets.info(@table, :size) >= max_entries(), do: GenServer.cast(__MODULE__, :evict)
  end

  @doc """
  Evicts one bounded batch (about 1% of the cap) of UNREVOKED entries (floor `<= 1`: the default
  `token_version` is 1, and 0 is "directory has no row"), and returns how many it deleted.

  Never touches a floor above 1: a revoked floor is the stale-serve source and the revocation
  property itself. Each delete re-checks the floor atomically (`select_delete` keyed on the shard),
  so a row raised between the select and the delete survives. The 2-tuple freshness marker never
  matches the 4-tuple pattern. Runs in the GenServer, never in a hello.
  """
  @spec evict_unrevoked() :: non_neg_integer()
  def evict_unrevoked do
    batch = max(div(max_entries(), 100), 1)
    spec = [{{:"$1", :"$2", :_, :_}, [{:is_binary, :"$1"}, {:"=<", :"$2", 1}], [:"$1"]}]

    case :ets.select(@table, spec, batch) do
      {keys, _continuation} ->
        Enum.reduce(keys, 0, fn key, acc ->
          acc + :ets.select_delete(@table, [{{key, :"$1", :_, :_}, [{:"=<", :"$1", 1}], [true]}])
        end)

      :"$end_of_table" ->
        0
    end
  rescue
    ArgumentError -> 0
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

  defp floor_budget_ms,
    do: Application.get_env(:fathom, :hrana_storage_floor_budget_ms, @default_floor_budget_ms)

  defp follow_timeout_ms, do: floor_budget_ms() + @follow_grace_ms

  defp storage_backoff_ms,
    do: Application.get_env(:fathom, :hrana_storage_floor_backoff_ms, @default_storage_backoff_ms)

  # Per-shard cool-down after an unreadable storage floor (see `fetch/3`). Stores the instant until
  # which the read is skipped. The table is capped like `@table`: at the cap it is simply cleared,
  # which costs at most one more bounded read per shard.
  defp storage_backing_off?(shard_id) do
    case :ets.lookup(@storage_backoff, shard_id) do
      [{^shard_id, until}] -> now_ms() < until
      [] -> false
    end
  rescue
    ArgumentError -> false
  end

  defp back_off_storage(shard_id) do
    if :ets.info(@storage_backoff, :size) >= max_entries(),
      do: :ets.delete_all_objects(@storage_backoff)

    :ets.insert(@storage_backoff, {shard_id, now_ms() + storage_backoff_ms()})
  rescue
    ArgumentError -> :ok
  end

  defp clear_storage_backoff(shard_id) do
    :ets.delete(@storage_backoff, shard_id)
  rescue
    ArgumentError -> :ok
  end
end
