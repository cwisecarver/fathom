defmodule Fathom.Shard.Storage.HeartbeatCache do
  @moduledoc """
  A tiny per-owner memo of heartbeat reads for the S3 steal path (review 2026-07-23 #13).

  On a node death the survivor steals MANY shards from the SAME dead owner, and each
  steal's `owner_live?` re-read the identical `heartbeat/<owner>` object — a 1,000-shard
  failover issued ~1,000 redundant GETs of one object, serialized per shard on the
  takeover critical path and contending the Finch pool with the pulls. This caches the
  raw heartbeat read per owner for a sub-second TTL, collapsing that to ~one GET per
  owner per second.

  Safety: the verdict a stale read can flip is bounded by the cache TTL, which must stay well
  inside `Storage.steal_margin_ms/0` (default 5000 ms) — the margin exists to absorb exactly this
  class of skew.

  That condition used to be stated HERE and enforced NOWHERE, against an operator-tunable margin
  (expert review 2026-08-01 #40). The caller now DERIVES the TTL as `min(1000, margin / 5)`
  (`Fathom.Shard.Storage.hb_cache_ttl_ms/0`), so it is structural rather than a documented hope —
  turn the margin down and the memo window shrinks with it. The TTL is passed in per call rather
  than owned here precisely so that derivation lives next to the margin it depends on.

  What the memo actually costs, stated plainly: a cached entry holds the heartbeat AND the store's
  clock sample together, so a hit REPLAYS a self-consistent verdict up to the TTL old. An owner
  that recovers inside that window can still be stolen from on a cached `:dead`. That is the
  bounded price of collapsing ~1,000 GETs into one, and it is why the bound moves with the margin.

  Errors are never cached (the fail-closed no-steal-on-blip behavior stays per-call). The table is
  a public ETS set owned here; readers fall back to a plain miss when the table isn't up
  (boot/tests), so the S3 backend never depends on this process.
  """
  use GenServer

  @table __MODULE__

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Cached value for `owner` if fetched within `ttl_ms`, else `:miss`."
  @spec get(String.t(), pos_integer()) :: {:hit, term()} | :miss
  def get(owner, ttl_ms) do
    case :ets.lookup(@table, owner) do
      [{^owner, value, at}] ->
        if System.monotonic_time(:millisecond) - at <= ttl_ms, do: {:hit, value}, else: :miss

      _ ->
        :miss
    end
  rescue
    ArgumentError -> :miss
  end

  # How long a loser waits for the winner's GET before fetching on its own, and how often it polls
  # the claim. The wait is the S3 GET's own bound (Req's receive timeout is far longer, so a wedged
  # winner must not wedge every loser behind it), kept short so a failover burst never queues
  # behind one slow read for longer than a couple of seconds.
  @single_flight_wait_ms 2_000
  @poll_ms 5

  @doc """
  SINGLE-FLIGHT fetch (expert review 2026-10-17 #29): `get/2`, and on a miss at most ONE caller per
  owner runs `fun` (the S3 heartbeat GET) while concurrent callers wait for its result instead of
  each issuing the identical GET -- the TTL memo alone only helps once the first read has landed, so
  a failover's simultaneous steals all missed together.

  `fun` returns the value to cache or `{:error, _}`, which is returned uncached and releases the
  claim (the fail-closed no-steal-on-blip behaviour stays per call; a waiter then tries for the
  claim itself, so errors serialise rather than herd). The claim is dropped in an `after`, and a
  waiter gives up waiting after `#{@single_flight_wait_ms}` ms and fetches unclaimed, so a dead or
  wedged winner cannot stall the takeover path. `stealable_at/4` and the TTL semantics are
  untouched: a waiter receives exactly the entry a later `get/2` would have returned.
  """
  @spec fetch(String.t(), non_neg_integer(), (-> term())) :: term()
  # TTL 0 (memo disabled by a zero margin): a waiter could never hit the winner's entry, so
  # single-flight would only serialise the GETs. Plain per-call read.
  def fetch(_owner, ttl_ms, fun) when ttl_ms <= 0, do: fun.()

  def fetch(owner, ttl_ms, fun) do
    deadline = System.monotonic_time(:millisecond) + @single_flight_wait_ms
    do_fetch(owner, ttl_ms, fun, deadline)
  end

  defp do_fetch(owner, ttl_ms, fun, deadline) do
    case get(owner, ttl_ms) do
      {:hit, value} ->
        value

      :miss ->
        claim = {:inflight, owner}

        cond do
          claim_table?() and claim(claim) ->
            try do
              value = fun.()
              unless match?({:error, _}, value), do: put(owner, value)
              value
            after
              :ets.delete(@table, claim)
            end

          claim_table?() and System.monotonic_time(:millisecond) < deadline ->
            Process.sleep(@poll_ms)
            do_fetch(owner, ttl_ms, fun, deadline)

          true ->
            # Table not up (boot/tests), or the winner outlasted our wait: fetch unclaimed.
            value = fun.()
            unless match?({:error, _}, value), do: put(owner, value)
            value
        end
    end
  end

  defp claim_table?, do: :ets.whereis(@table) != :undefined

  # Take the in-flight claim, evicting one a killed winner left behind (its `after` never ran): a
  # claim older than the wait window has already made every waiter give up, so it is dead weight.
  defp claim(key) do
    now = System.monotonic_time(:millisecond)

    :ets.insert_new(@table, {key, self(), now}) or
      case :ets.lookup(@table, key) do
        [{^key, _pid, at}] when now - at > @single_flight_wait_ms ->
          :ets.delete(@table, key)
          :ets.insert_new(@table, {key, self(), now})

        _ ->
          false
      end
  end

  @doc "Store `value` for `owner`, stamped now. Best-effort (no-op when the table isn't up)."
  @spec put(String.t(), term()) :: :ok
  def put(owner, value) do
    :ets.insert(@table, {owner, value, System.monotonic_time(:millisecond)})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc false
  def reset do
    :ets.delete_all_objects(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [:set, :public, :named_table, write_concurrency: true])
    # Boot owner of the steal-path quiescence table (expert review 2026-10-12 #17). It was created
    # lazily by whichever coordinator touched it first and died with that coordinator. This is the
    # right owner because it is the one always-on process on the S3 steal path: Fleet is started only
    # when replication is on, and Heartbeat is disabled in test.
    :ok = Fathom.Shard.Storage.init_quiescence_table()
    {:ok, %{}}
  end
end
