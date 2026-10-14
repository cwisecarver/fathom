defmodule Fathom.Migrator.HeadCache do
  @moduledoc """
  A node-local, TTL-refreshed cache of the fleet **HEAD** (the highest released schema
  version) so the `lazy_migrate` checkout path never hits Postgres per checkout.

  `Fathom.Shards.checkout/1`'s lazy-migrate path asked `Fathom.Migrator.head/0` — a
  `SELECT max(version)` aggregate — on **every** checkout. That is exactly the
  per-checkout Postgres cost `Fathom.Directory.Recorder` exists to avoid, and HEAD is
  one fleet-global integer that changes only on a release. This caches it.

  ## Why TTL, not PubSub

  Fathom has **no BEAM cluster** (LB-keyspace-partition; S3 is the only cross-node
  coordination). A `Phoenix.PubSub` broadcast is node-local, so it can't invalidate
  other nodes' caches on a release. The one thing shared across nodes is Postgres, so
  each node simply **re-reads HEAD every `:migrator_head_ttl_ms`** (default 5s). A
  release is fleet-visible within the TTL — plenty, since migrations aren't
  latency-critical (the hourly reconcile sweep is the backstop).

  ## Why it's safe to be slightly stale

  HEAD rises as versions release, and a stale-LOW cache merely *delays* a lazy
  migration — it can never wrongly trigger one. HEAD can also **drop** when a release
  is yanked (`Fathom.Migrator.yank/1`, expert review #12): the yanking node refreshes
  its cache synchronously, and other nodes can read a stale-HIGH head for up to one
  TTL — but a yanked version's `statements/1` is `nil`, so a straggler lazy migration
  targeting it errors out instead of re-applying the bad schema. `get/0` reads `:persistent_term` (lock-free, no process
  hop); the background poll runs only while `:lazy_migrate` is enabled (nothing reads
  the cache otherwise — the `Fathom.ShardLoad` philosophy).
  """
  use GenServer

  require Logger

  alias Fathom.Migrator

  @pt_key {__MODULE__, :head}
  @default_ttl_ms 5_000
  @settled_table Module.concat(__MODULE__, Settled)
  @settled_max 200_000
  @default_settled_ttl_ms 30_000

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  The cached fleet HEAD (highest released schema version). Lock-free `:persistent_term`
  read — no Postgres, no GenServer hop. Returns `0` before the first refresh (which is
  correct: a HEAD of 0 means "no releases", so lazy-migrate does nothing).
  """
  @spec get() :: non_neg_integer()
  def get, do: :persistent_term.get(@pt_key, 0)

  @doc """
  Whether migrate-on-touch has already settled `shard_id` at fleet HEAD `head` on this node: it was
  read from the directory and found current, or its migration job was enqueued, within the last
  `:migrate_on_touch_recheck_ms` (default 30 s). `Fathom.Shards` skips the per-checkout
  `Directory.get/1` (and the `Oban.insert` for a laggard) while this is true (perf review
  2026-10-14 #9).

  INVALIDATION: the entry is keyed on `head`, so a release (HEAD rises) or a yank (HEAD drops)
  misses every entry at once and each shard is re-read on its next touch. The TTL covers what HEAD
  cannot see (a shard restored to an older version, a suspended shard resumed, a failed job whose
  retry is due). Never a way to hide a version bump: at worst a laggard is re-checked up to one TTL
  late after an out-of-band change. `false` when the table is not up.
  """
  @spec settled?(String.t(), non_neg_integer()) :: boolean()
  def settled?(shard_id, head) do
    case :ets.lookup(@settled_table, shard_id) do
      [{^shard_id, ^head, expires_at}] -> System.monotonic_time(:millisecond) < expires_at
      _ -> false
    end
  rescue
    ArgumentError -> false
  end

  @doc "Records `shard_id` as settled at `head` (see `settled?/2`). Cleared wholesale at the cap."
  @spec settle(String.t(), non_neg_integer()) :: :ok
  def settle(shard_id, head) do
    if :ets.info(@settled_table, :size) >= @settled_max,
      do: :ets.delete_all_objects(@settled_table)

    ttl = Application.get_env(:fathom, :migrate_on_touch_recheck_ms, @default_settled_ttl_ms)
    :ets.insert(@settled_table, {shard_id, head, System.monotonic_time(:millisecond) + ttl})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Forces a synchronous refresh from Postgres and returns the new HEAD (tests / ops)."
  @spec refresh() :: non_neg_integer()
  def refresh, do: GenServer.call(__MODULE__, :refresh)

  @impl true
  def init(opts) do
    :persistent_term.put(@pt_key, 0)

    # Owned here (a boot-time supervised process), not by a request process. See `settled?/2`.
    :ets.new(@settled_table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    ttl =
      Keyword.get(
        opts,
        :ttl_ms,
        Application.get_env(:fathom, :migrator_head_ttl_ms, @default_ttl_ms)
      )

    {:ok, %{ttl_ms: ttl}, {:continue, :refresh}}
  end

  @impl true
  def handle_continue(:refresh, state) do
    maybe_refresh()
    schedule(state.ttl_ms)
    {:noreply, state}
  end

  @impl true
  def handle_info(:refresh, state) do
    maybe_refresh()
    schedule(state.ttl_ms)
    {:noreply, state}
  end

  @impl true
  def handle_call(:refresh, _from, state) do
    {:reply, do_refresh(), state}
  end

  defp schedule(ttl), do: Process.send_after(self(), :refresh, ttl)

  # Background poll only when migrate-on-touch is enabled — nothing reads the cache otherwise.
  #
  # This gated on the LEGACY `:lazy_migrate` boolean, so setting the current `:migrate_on_touch`
  # (`:async` | `:inline`, expert review #40) enabled the cache's only consumer without enabling the
  # poll that fills it: `get/0` returned its initial 0 forever, `Fathom.Shards`' `head > 0` guard was
  # never true, and migrate-on-touch silently did NOTHING in either mode — the documented
  # convergence knob was a no-op. Found live 2026-07-30 with MIGRATE_ON_TOUCH=inline set and a
  # released HEAD of 2, while HeadCache.get/0 still read 0.
  #
  # Deliberately calls `Fathom.Shards.migrate_on_touch_mode/0` rather than re-deriving the mode here:
  # duplicating that resolution is exactly what let the two drift.
  defp maybe_refresh do
    if Fathom.Shards.migrate_on_touch_mode() != :off, do: do_refresh()
  end

  # Best-effort: a Postgres blip keeps the last cached value. HEAD is monotonic, so a
  # stale-low value only delays a migration; it never triggers a wrong one.
  defp do_refresh do
    head = Migrator.head()
    :persistent_term.put(@pt_key, head)
    head
  rescue
    e ->
      Logger.warning("Migrator.HeadCache refresh failed: #{inspect(e)}")
      get()
  catch
    :exit, reason ->
      Logger.warning("Migrator.HeadCache refresh exited: #{inspect(reason)}")
      get()
  end
end
