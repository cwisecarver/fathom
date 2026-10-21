defmodule Fathom.Shard.HandlePool do
  @moduledoc """
  A bounded, per-scope store of IDLE (checked-in) SQLite handles for ONE shard — the state behind
  per-stream connection pooling (the dsv41f.perf headline; see `docs/pooling-spike-plan.md`).

  **This module is PURE.** It holds opaque handle terms and decides which to reuse and which to
  evict; it never opens, closes, or resets a SQLite connection. The coordinator that owns the
  shard's fence does that I/O — which is why every function that REMOVES a handle returns it, so the
  caller can close it. Dropping a returned handle on the floor leaks a file descriptor.

  ## Invariants

  - **Scope isolation.** `:ro` and `:rw` handles live in separate buckets and a `take/2` for one
    scope is NEVER satisfied by the other's handle. Handing a `:ro` stream a writable connection is
    the same scope-leak class as the process-dict side-channel bug (`Fathom.ShardExecutor` audit
    #3), so it is structural here, not a runtime check.
  - **Reuse is MRU, eviction is LRU.** A checkout reuses the most-recently-returned handle (warmest
    `-shm` / page cache); when a bucket is over `max_per_scope` the oldest is evicted; a handle idle
    longer than `ttl_ms` is swept. All three hand the removed handle back to be closed.
  - **`drain/1` empties everything** — the coordinator calls it on flush-drop / fence / terminate so
    no pooled handle can outlive the shard's lease on this node, and at the end of the drain grace
    (or at any barrier inside it) once the shard has gone idle.
  """

  @type scope :: :ro | :rw
  @type handle :: term()
  # {handle, last_used_mono_ms, cache_state}; the list is newest-first, so head = MRU, tail = LRU.
  # `cache_state` is :fresh (page cache as the last stream left it), :shrinking (a side process is
  # running `PRAGMA shrink_memory` on it right now: NOT lendable) or :shrunk (cache released).
  @type cache_state :: :fresh | :shrinking | :shrunk
  @type entry :: {handle(), integer(), cache_state()}
  @type t :: %__MODULE__{
          idle: %{ro: [entry()], rw: [entry()]},
          max_per_scope: pos_integer(),
          ttl_ms: pos_integer(),
          drain_grace_ms: non_neg_integer(),
          shrink_idle_ms: non_neg_integer()
        }

  defstruct idle: %{ro: [], rw: []},
            max_per_scope: 1,
            ttl_ms: 30_000,
            drain_grace_ms: 250,
            shrink_idle_ms: 5_000

  @doc """
  A new empty pool. `:max_per_scope` caps idle handles held per scope (default 1); `:ttl_ms` is how
  long an idle handle may sit before `sweep/2` closes it (default 30_000); `:drain_grace_ms` is how
  long the coordinator keeps the pool after the shard's LAST stream checks in before it drains it
  (default 250; 0 drains on that checkin, the pre-2026-10-14 behaviour). The grace is the
  coordinator's to enforce (`Fathom.Shard`); this module only carries the setting.
  `:shrink_idle_ms` (default 5_000; 0 = off) is how long a handle sits idle before
  `begin_shrink/2` offers it for `PRAGMA shrink_memory` (expert review 2026-10-21 #15).
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      idle: %{ro: [], rw: []},
      max_per_scope: Keyword.get(opts, :max_per_scope, 1),
      ttl_ms: Keyword.get(opts, :ttl_ms, 30_000),
      drain_grace_ms: Keyword.get(opts, :drain_grace_ms, 250),
      shrink_idle_ms: Keyword.get(opts, :shrink_idle_ms, 5_000)
    }
  end

  @doc """
  Reuse an idle handle for `scope`, most-recently-returned first. `{:hit, handle, pool}` when one is
  available (it is removed from the pool — the caller now owns it), `{:miss, pool}` otherwise.
  """
  @spec take(t(), scope()) :: {:hit, handle(), t()} | {:miss, t()}
  def take(%__MODULE__{idle: idle} = pool, scope) when scope in [:ro, :rw] do
    # A :shrinking handle is skipped (a miss when it is the only one): its shrink is in flight on a
    # side process and must never overlap a stream's use of it. The next-warmest is lent instead.
    bucket = Map.fetch!(idle, scope)

    case Enum.find_index(bucket, fn {_h, _ts, st} -> st != :shrinking end) do
      nil ->
        {:miss, pool}

      i ->
        {{handle, _ts, _st}, rest} = List.pop_at(bucket, i)
        {:hit, handle, %{pool | idle: Map.put(idle, scope, rest)}}
    end
  end

  @doc """
  Return `handle` to the pool for `scope` at monotonic time `now_ms`. Keeps the newest
  `max_per_scope` handles and returns everything over the cap in `to_close` (a LIST, empty when
  nothing is evicted). Callers MUST close every returned handle.
  """
  @spec put(t(), scope(), handle(), integer()) :: {t(), [handle()]}
  def put(%__MODULE__{idle: idle, max_per_scope: max} = pool, scope, handle, now_ms)
      when scope in [:ro, :rw] do
    bucket = [{handle, now_ms, :fresh} | Map.fetch!(idle, scope)]

    # Keep the newest `max` (warmest -shm/cache); everything past the cap is evicted for the caller
    # to close. `put/4` prepends one and maintains `bucket <= max`, so `over` is normally 0 or 1
    # element — but return it as a LIST and map generally rather than matching a rigid `[{h, _}]`,
    # so a hypothetical invariant break (a lowered `max_per_scope` against a populated bucket)
    # evicts+closes ALL the extras instead of a MatchError that crashes the coordinator (expert
    # review 2026-09-18 #24).
    {kept, over} = Enum.split(bucket, max)
    {%{pool | idle: Map.put(idle, scope, kept)}, Enum.map(over, fn {h, _ts, _st} -> h end)}
  end

  @doc """
  Remove every handle idle longer than `ttl_ms` as of `now_ms`, across both scopes. Returns the
  swept pool and the expired handles to close (the density bound — a shard that stops taking traffic
  sheds its idle handles even before the coordinator drops it).
  """
  @spec sweep(t(), integer()) :: {t(), [handle()]}
  def sweep(%__MODULE__{idle: idle, ttl_ms: ttl} = pool, now_ms) do
    {ro_keep, ro_exp} = partition_expired(Map.fetch!(idle, :ro), now_ms, ttl)
    {rw_keep, rw_exp} = partition_expired(Map.fetch!(idle, :rw), now_ms, ttl)
    {%{pool | idle: %{ro: ro_keep, rw: rw_keep}}, ro_exp ++ rw_exp}
  end

  @doc """
  Mark every `:fresh` handle idle at least `shrink_idle_ms` as `:shrinking` and return them, so the
  coordinator can run `PRAGMA shrink_memory` on them OFF its mailbox (expert review 2026-10-21 #15).
  A `:shrinking` handle stays in the pool (so drain/terminate still close it) but `take/2` skips it,
  which is what keeps the shrink from overlapping a checkout. Returns `{pool, []}` when off (0).
  """
  @spec begin_shrink(t(), integer()) :: {t(), [handle()]}
  def begin_shrink(%__MODULE__{shrink_idle_ms: 0} = pool, _now_ms), do: {pool, []}

  def begin_shrink(%__MODULE__{idle: idle, shrink_idle_ms: after_ms} = pool, now_ms) do
    {ro, ro_h} = mark_shrinking(Map.fetch!(idle, :ro), now_ms, after_ms)
    {rw, rw_h} = mark_shrinking(Map.fetch!(idle, :rw), now_ms, after_ms)
    {%{pool | idle: %{ro: ro, rw: rw}}, ro_h ++ rw_h}
  end

  @doc """
  The shrink of `handles` finished: mark those still pooled and `:shrinking` as `:shrunk`, so they
  are not shrunk again and become lendable. Handles no longer pooled are ignored.
  """
  @spec end_shrink(t(), [handle()]) :: t()
  def end_shrink(%__MODULE__{idle: idle} = pool, handles) do
    fin = fn bucket ->
      Enum.map(bucket, fn
        {h, ts, :shrinking} = e -> if h in handles, do: {h, ts, :shrunk}, else: e
        e -> e
      end)
    end

    %{pool | idle: %{ro: fin.(Map.fetch!(idle, :ro)), rw: fin.(Map.fetch!(idle, :rw))}}
  end

  @doc "True when some pooled handle has not been shrunk yet (a shrink pass can still find work)."
  @spec shrink_pending?(t()) :: boolean()
  def shrink_pending?(%__MODULE__{shrink_idle_ms: 0}), do: false

  def shrink_pending?(%__MODULE__{idle: idle}),
    do: Enum.any?([:ro, :rw], fn s -> Enum.any?(idle[s], fn {_, _, st} -> st == :fresh end) end)

  @doc """
  Remove and return EVERY idle handle, leaving the pool empty. The coordinator calls this on
  flush-drop / fence / terminate — no pooled handle may outlive the shard's lease on this node.
  """
  @spec drain(t()) :: {[handle()], t()}
  def drain(%__MODULE__{idle: idle} = pool) do
    handles = for scope <- [:ro, :rw], {h, _ts, _st} <- Map.fetch!(idle, scope), do: h
    {handles, %{pool | idle: %{ro: [], rw: []}}}
  end

  @doc "Total idle handles held (both scopes)."
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{idle: idle}), do: length(idle.ro) + length(idle.rw)

  @doc "Idle handles held for one scope."
  @spec count(t(), scope()) :: non_neg_integer()
  def count(%__MODULE__{idle: idle}, scope) when scope in [:ro, :rw],
    do: length(Map.fetch!(idle, scope))

  # Split a newest-first bucket into {kept, expired-handles}. Age is now - last_used.
  # A :shrinking handle is kept past its ttl (closing it would race the shrink on a side process);
  # the next sweep takes it.
  defp partition_expired(bucket, now_ms, ttl) do
    {keep, expired} =
      Enum.split_with(bucket, fn {_h, ts, st} -> st == :shrinking or now_ms - ts < ttl end)

    {keep, Enum.map(expired, fn {h, _ts, _st} -> h end)}
  end

  defp mark_shrinking(bucket, now_ms, after_ms) do
    Enum.map_reduce(bucket, [], fn
      {h, ts, :fresh}, acc when now_ms - ts >= after_ms -> {{h, ts, :shrinking}, [h | acc]}
      e, acc -> {e, acc}
    end)
    |> then(fn {b, hs} -> {b, Enum.reverse(hs)} end)
  end
end
