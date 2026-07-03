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

  HEAD is **monotonic**: it only rises as versions release (a revert flips shard
  pointers but leaves the `Release` row, so `max(version)` never drops). A stale cache
  is therefore only ever **too low**, which merely *delays* a lazy migration — it can
  never wrongly trigger one. `get/0` reads `:persistent_term` (lock-free, no process
  hop); the background poll runs only while `:lazy_migrate` is enabled (nothing reads
  the cache otherwise — the `Fathom.ShardLoad` philosophy).
  """
  use GenServer

  require Logger

  alias Fathom.Migrator

  @pt_key {__MODULE__, :head}
  @default_ttl_ms 5_000

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  The cached fleet HEAD (highest released schema version). Lock-free `:persistent_term`
  read — no Postgres, no GenServer hop. Returns `0` before the first refresh (which is
  correct: a HEAD of 0 means "no releases", so lazy-migrate does nothing).
  """
  @spec get() :: non_neg_integer()
  def get, do: :persistent_term.get(@pt_key, 0)

  @doc "Forces a synchronous refresh from Postgres and returns the new HEAD (tests / ops)."
  @spec refresh() :: non_neg_integer()
  def refresh, do: GenServer.call(__MODULE__, :refresh)

  @impl true
  def init(opts) do
    :persistent_term.put(@pt_key, 0)

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

  # Background poll only when lazy_migrate is on — nothing reads the cache otherwise.
  defp maybe_refresh do
    if Application.get_env(:fathom, :lazy_migrate, false), do: do_refresh()
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
