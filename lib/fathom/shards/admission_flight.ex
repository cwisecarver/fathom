defmodule Fathom.Shards.AdmissionFlight do
  @moduledoc """
  Per-shard single-flight for a COLD admission (expert review 2026-10-21 r7 #6).

  ## Why

  The Registry entry for a shard appears only when `Fathom.Shards` starts its coordinator, and
  `start/1`'s `{:already_started, pid}` dedup runs AFTER the whole admission: the novelty stat and
  directory read, a `NovelLimiter` token, an at-capacity eviction, a template fork. So K concurrent
  first requests for one cold shard (a woken tenant's app opening several connections, a failover
  moving a tenant with N clients) each ran all of it. Measured in-process at the cap (64 idle
  shards, 25 ms injected release/pull): K=16 checkouts of one cold id evicted 16 tenants to admit
  one shard, and K=32 got `:node_at_capacity` on some of its own requests. With a novel-rate limit
  set, one new tenant with more connections than the burst 429'd itself.

  ## What

  The first caller for a shard id (`insert_new`) becomes the LEADER and runs the admission. Every
  concurrent caller for the same id becomes a FOLLOWER: it waits (bounded) for the leader's result
  and takes it as its own, so a refusal is inherited for that attempt (the panel default) and an
  admission is shared. One shard id, one admission, one victim, one token.

  ## Mechanism, and why each piece is there

    * The leader row is `{id, leader_pid}` in a `:set`; a follower registers `{id, alias}` in a
      `:duplicate_bag`, then RE-READS the leader row. The leader deletes its row, then `take`s the
      followers and sends. A follower that saw the row after registering is therefore always in
      the leader's `take`; one that no longer sees it goes back and re-resolves.
    * The follower's wait is a monitor WITH AN ALIAS, and the leader sends to the alias. On a
      timeout the follower unaliases, so a result sent late is dropped by the runtime instead of
      sitting in a long-lived Hrana stream's mailbox forever (the leak class of expert review
      2026-08-01 #34).
    * A leader that dies mid-admission (a killed stream) leaves its row behind. Its followers see
      the DOWN, remove that exact row (`delete_object`, so a newer leader's row is untouched) and
      re-resolve; a later caller finds the dead leader the same way.
    * Cost to an uncontended cold open: one `insert_new`, one `delete`, one `take`. A running
      shard's checkout never reaches this module.
  """
  use GenServer

  @leaders __MODULE__
  @followers __MODULE__.Followers

  @doc false
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  Join the admission of `shard_id`.

    * `:leader` — the caller runs the admission and MUST call `finish/2` with its result (also on
      a raise; see `Fathom.Shards`).
    * `{:shared, result}` — another caller ran it; this is its result.
    * `:retry` — the leader finished or died before the result could be shared; re-resolve.
    * `:timeout` — the leader did not finish within `wait_ms`; the caller runs its own admission.
  """
  @spec join(String.t(), non_neg_integer()) :: :leader | {:shared, term()} | :retry | :timeout
  def join(shard_id, wait_ms) do
    if :ets.insert_new(@leaders, {shard_id, self()}) do
      :leader
    else
      case :ets.lookup(@leaders, shard_id) do
        [{^shard_id, leader}] -> follow(shard_id, leader, wait_ms)
        [] -> :retry
      end
    end
  rescue
    # The table is gone (the owner is restarting during shutdown): no single-flight, admit as before.
    ArgumentError -> :leader
  end

  defp follow(shard_id, leader, wait_ms) do
    ref = :erlang.monitor(:process, leader, [{:alias, :explicit_unalias}])
    :ets.insert(@followers, {shard_id, ref})

    case :ets.lookup(@leaders, shard_id) do
      [{^shard_id, ^leader}] ->
        await(shard_id, leader, ref, wait_ms)

      _ ->
        # Finished (or replaced) between our two reads: it may or may not have seen our row.
        abandon(shard_id, ref)
        :retry
    end
  end

  defp await(shard_id, leader, ref, wait_ms) do
    receive do
      {__MODULE__, ^ref, result} ->
        :erlang.demonitor(ref, [:flush])
        :erlang.unalias(ref)
        {:shared, result}

      {:DOWN, ^ref, :process, ^leader, _reason} ->
        :erlang.unalias(ref)
        :ets.delete_object(@leaders, {shard_id, leader})
        :ets.delete_object(@followers, {shard_id, ref})
        :retry
    after
      wait_ms ->
        abandon(shard_id, ref)
        :timeout
    end
  end

  # Stop listening: unalias first, so nothing sent from here on is delivered, then drop what
  # already arrived and our follower row.
  defp abandon(shard_id, ref) do
    :erlang.demonitor(ref, [:flush])
    :erlang.unalias(ref)

    receive do
      {__MODULE__, ^ref, _} -> :ok
    after
      0 -> :ok
    end

    :ets.delete_object(@followers, {shard_id, ref})
    :ok
  end

  @doc "The leader publishes its admission `result` to every follower and ends the flight."
  @spec finish(String.t(), term()) :: :ok
  def finish(shard_id, result) do
    # Row first, then the followers: see the moduledoc for why this order loses no one.
    :ets.delete_object(@leaders, {shard_id, self()})

    for {_, ref} <- :ets.take(@followers, shard_id), do: send(ref, {__MODULE__, ref, result})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @impl true
  def init(_opts) do
    :ets.new(@leaders, [
      :set,
      :public,
      :named_table,
      write_concurrency: true,
      read_concurrency: true
    ])

    :ets.new(@followers, [
      :duplicate_bag,
      :public,
      :named_table,
      write_concurrency: true
    ])

    {:ok, %{}}
  end
end
