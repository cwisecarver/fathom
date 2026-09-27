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
    no pooled handle can outlive the shard's lease on this node.
  """

  @type scope :: :ro | :rw
  @type handle :: term()
  # {handle, last_used_mono_ms}; the list is newest-first, so head = MRU, tail = LRU.
  @type entry :: {handle(), integer()}
  @type t :: %__MODULE__{
          idle: %{ro: [entry()], rw: [entry()]},
          max_per_scope: pos_integer(),
          ttl_ms: pos_integer()
        }

  defstruct idle: %{ro: [], rw: []}, max_per_scope: 1, ttl_ms: 30_000

  @doc """
  A new empty pool. `:max_per_scope` caps idle handles held per scope (default 1); `:ttl_ms` is how
  long an idle handle may sit before `sweep/2` closes it (default 30_000).
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      idle: %{ro: [], rw: []},
      max_per_scope: Keyword.get(opts, :max_per_scope, 1),
      ttl_ms: Keyword.get(opts, :ttl_ms, 30_000)
    }
  end

  @doc """
  Reuse an idle handle for `scope`, most-recently-returned first. `{:hit, handle, pool}` when one is
  available (it is removed from the pool — the caller now owns it), `{:miss, pool}` otherwise.
  """
  @spec take(t(), scope()) :: {:hit, handle(), t()} | {:miss, t()}
  def take(%__MODULE__{idle: idle} = pool, scope) when scope in [:ro, :rw] do
    case Map.fetch!(idle, scope) do
      [{handle, _ts} | rest] -> {:hit, handle, %{pool | idle: Map.put(idle, scope, rest)}}
      [] -> {:miss, pool}
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
    bucket = [{handle, now_ms} | Map.fetch!(idle, scope)]

    # Keep the newest `max` (warmest -shm/cache); everything past the cap is evicted for the caller
    # to close. `put/4` prepends one and maintains `bucket <= max`, so `over` is normally 0 or 1
    # element — but return it as a LIST and map generally rather than matching a rigid `[{h, _}]`,
    # so a hypothetical invariant break (a lowered `max_per_scope` against a populated bucket)
    # evicts+closes ALL the extras instead of a MatchError that crashes the coordinator (expert
    # review 2026-09-18 #24).
    {kept, over} = Enum.split(bucket, max)
    {%{pool | idle: Map.put(idle, scope, kept)}, Enum.map(over, fn {h, _ts} -> h end)}
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
  Remove and return EVERY idle handle, leaving the pool empty. The coordinator calls this on
  flush-drop / fence / terminate — no pooled handle may outlive the shard's lease on this node.
  """
  @spec drain(t()) :: {[handle()], t()}
  def drain(%__MODULE__{idle: idle} = pool) do
    handles = for scope <- [:ro, :rw], {h, _ts} <- Map.fetch!(idle, scope), do: h
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
  defp partition_expired(bucket, now_ms, ttl) do
    {keep, expired} = Enum.split_with(bucket, fn {_h, ts} -> now_ms - ts < ttl end)
    {keep, Enum.map(expired, fn {h, _ts} -> h end)}
  end
end
