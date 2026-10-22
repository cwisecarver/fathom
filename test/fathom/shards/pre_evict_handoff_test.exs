defmodule Fathom.Shards.PreEvictHandoffTest do
  @moduledoc """
  The pre-evictor under CONCURRENT admissions (expert review 2026-10-21 r7 #7).

  Symptom: round 6's background pre-evictor only helped near-serial admission. Measured in-process
  (FaultyStorage, 25 ms injected on the lock release and the pull, cap 256, K=16 concurrent cold
  admissions, prod-like clean detection): admitted-open p50 52-54 ms with the pre-evictor on, the
  same as with it off, and 81-90 of 160 admissions still evicted a victim inline. Three causes:
    * an admission at the cap skipped the pre-evictor's in-flight victims (claimed) and drained one
      of its own, paying a whole drop while slots were already on their way;
    * the pre-evictor drained a fixed 8 victims at a time;
    * an admission whose LRU probe met only victims someone else had claimed refused
      `:node_at_capacity`, though idle shards sat right behind them.
  After the fix the same probe measured p50 29-30 ms (uncapped 27-28 ms) and 3-16 inline evictions
  of 160, the 16 being the first wave on a node that starts full.

  Pinned invariants (each fails against the pre-fix code):
    * an admission at the cap ADOPTS an offered in-flight pre-eviction and waits for that victim
      instead of evicting another tenant (both liveness modes: the victim's drop releases a lease);
    * adoption is one-to-one, and a lost slot (the drain did not free it) is reported at once;
    * a claimed shard is not an eviction candidate and does not use up the probe limit, so an
      admission behind a claimed LRU front still finds an idle victim;
    * a pre-eviction pass drains more than 8 clean victims at once, and dirty ones at most 8.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shards
  alias Fathom.Shard.Heartbeat
  alias Fathom.Shards.Lru
  alias Fathom.Test.FaultyStorage

  @moduletag :capture_log
  @modes [:legacy, :heartbeat]

  @env [
    :shard_storage,
    :faulty_before,
    :max_open_shards,
    :pre_evict_headroom,
    :pre_evict_min_idle_ms,
    :shard_idle_ms,
    :evict_idle_at_capacity,
    :metrics_collector
  ]

  setup do
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})
    Lru.reset()
    test_pid = self()
    handler = "pre_evict_handoff_test_#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      [[:fathom, :shards, :evicted], [:fathom, :shards, :pre_evicted]],
      fn event, _, %{shard_id: id}, _ -> send(test_pid, {event, id}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler)

      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      Lru.reset()
    end)

    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :evict_idle_at_capacity, true)
    Application.put_env(:fathom, :pre_evict_headroom, 0)
    :ok
  end

  defp set_mode!(:legacy) do
    refute Heartbeat.running?()
    :ok
  end

  defp set_mode!(:heartbeat) do
    hb = start_supervised!({Heartbeat, ttl_ms: 30_000})
    _ = :sys.get_state(hb)
    :ok
  end

  defp assert_mode!(id, mode) do
    [{pid, _}] = Registry.lookup(Fathom.ShardRegistry, id)
    gen = :sys.get_state(pid).acquire_gen

    if mode == :heartbeat,
      do: assert(is_integer(gen), "heartbeat mode did not take"),
      else: assert(is_nil(gen), "legacy mode did not take")
  end

  defp uniq, do: "pehand_#{System.unique_integer([:positive])}"

  defp open_idle(id) do
    {:ok, pid, ref, _path} = Shards.checkout(id)
    Fathom.Shard.checkin(pid, ref)
    _ = :sys.get_state(pid)
    id
  end

  defp open?(id), do: Registry.lookup(Fathom.ShardRegistry, id) != []

  defp coordinator(id),
    do: id |> then(&Registry.lookup(Fathom.ShardRegistry, &1)) |> hd() |> elem(0)

  # Fill `n` idle shards with the cap out of the way, then put the cap at the population.
  defp fill_to_cap(n) do
    Application.put_env(:fathom, :max_open_shards, Shards.open_count() + 10 * n)
    ids = for _ <- 1..n, do: open_idle(uniq())
    Application.put_env(:fathom, :max_open_shards, Shards.open_count())
    ids
  end

  defp await_pre_evictor do
    case Process.whereis(Fathom.Shards.PreEvictor) do
      nil ->
        :ok

      pid ->
        ref = Process.monitor(pid)
        assert_receive {:DOWN, ^ref, :process, ^pid, _}, 30_000
        await_pre_evictor()
    end
  end

  defp wait_until(fun, tries \\ 400) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        flunk("condition never held")

      true ->
        receive do
        after
          5 -> wait_until(fun, tries - 1)
        end
    end
  end

  defp cleanup(ids) do
    Application.put_env(:fathom, :max_open_shards, :infinity)
    Enum.each(ids, &Shards.stop/1)

    for id <- ids,
        f <- Path.wildcard(Fathom.Shard.db_path(id) <> "*"),
        do: File.rm_rf(f)

    for id <- ids, do: Fathom.Shard.Storage.Local.purge_shard(id)
  end

  for mode <- @modes do
    @tag mode: mode
    test "#{mode}: an admission at the cap adopts an in-flight pre-eviction instead of evicting",
         %{mode: mode} do
      set_mode!(mode)
      [victim | _] = filled = fill_to_cap(4)
      assert_mode!(victim, mode)
      victim_pid = coordinator(victim)
      new = uniq()
      on_exit(fn -> cleanup([new | filled]) end)

      # A pre-eviction of `victim` is in flight: claimed and offered, as the pre-evictor does.
      test_pid = self()

      evictor =
        spawn(fn ->
          true = Lru.claim_eviction(victim)
          :ok = Lru.offer_slot(victim, victim_pid)
          send(test_pid, :offered)

          receive do
            :drop -> :ok
          end

          :ok = Shards.drain(victim, 0)
          :ok = Lru.withdraw_slot(victim, true)
          :ok = Lru.release_eviction(victim)
        end)

      assert_receive :offered
      admission = Task.async(fn -> Shards.checkout(new) end)

      # The admission adopted the offer and is waiting for the victim, not evicting anyone.
      # Pre-fix it never adopts: it walks the LRU, evicts the next idle shard and returns.
      wait_until(fn ->
        match?([{_, _, ref}] when is_reference(ref), :ets.lookup(Lru.Offered, victim))
      end)

      refute_received {[:fathom, :shards, :evicted], _}

      send(evictor, :drop)
      assert {:ok, pid, ref, _} = Task.await(admission, 10_000)
      Fathom.Shard.checkin(pid, ref)

      refute open?(victim)
      refute_received {[:fathom, :shards, :evicted], _}, "the admission evicted a second tenant"
      assert Enum.all?(tl(filled), &open?/1)
      assert_mode!(new, mode)
    end
  end

  describe "the slot offer" do
    test "two adopters, one offer: exactly one adopts" do
      {:ok, holder} = Task.start(fn -> Process.sleep(:infinity) end)
      id = uniq()
      :ok = Lru.offer_slot(id, holder)
      test_pid = self()

      for _ <- 1..2 do
        spawn(fn -> send(test_pid, {:adopt, Lru.adopt_slot()}) end)
      end

      results = for _ <- 1..2, do: receive(do: ({:adopt, r} -> r))
      assert Enum.count(results, &match?({:ok, ^id, ^holder, _}, &1)) == 1
      assert Enum.count(results, &(&1 == :none)) == 1
      :ok = Lru.withdraw_slot(id, true)
      Process.exit(holder, :kill)
    end

    test "a slot the drain did not free is reported to its adopter at once" do
      {:ok, holder} = Task.start(fn -> Process.sleep(:infinity) end)
      id = uniq()
      :ok = Lru.offer_slot(id, holder)
      assert {:ok, ^id, ^holder, ref} = Lru.adopt_slot()

      :ok = Lru.withdraw_slot(id, false)
      assert_receive {:slot_lost, ^ref}
      assert :ets.lookup(Lru.Offered, id) == []
      :erlang.demonitor(ref, [:flush])
      :erlang.unalias(ref)
      Process.exit(holder, :kill)
    end

    test "a freed slot sends nothing; its adopter learns from the victim's exit" do
      {:ok, holder} = Task.start(fn -> Process.sleep(:infinity) end)
      id = uniq()
      :ok = Lru.offer_slot(id, holder)
      assert {:ok, ^id, ^holder, ref} = Lru.adopt_slot()
      :ok = Lru.withdraw_slot(id, true)
      refute_received {:slot_lost, ^ref}
      Process.exit(holder, :kill)
      assert_receive {:DOWN, ^ref, :process, ^holder, :killed}
      :erlang.unalias(ref)
    end
  end

  test "an admission behind a claimed LRU front still finds an idle victim" do
    # 20 idle shards; the 16 coldest (the whole probe window) are being evicted by someone else.
    filled = fill_to_cap(20)
    new = uniq()
    on_exit(fn -> cleanup([new | filled]) end)
    test_pid = self()

    claimer =
      spawn(fn ->
        for id <- Lru.lru_order(16), do: true = Lru.claim_eviction(id)
        send(test_pid, :claimed)
        Process.sleep(:infinity)
      end)

    assert_receive :claimed
    on_exit(fn -> Process.exit(claimer, :kill) end)

    # Pre-fix the probe window held only claimed shards and the admission refused 503.
    assert {:ok, pid, ref, _} = Shards.checkout(new)
    Fathom.Shard.checkin(pid, ref)
    assert_received {[:fathom, :shards, :evicted], victim}
    assert victim in Enum.drop(filled, 16), "a claimed shard was evicted twice"
  end

  describe "pre-eviction parallelism" do
    # Peak concurrent victim drops, observed in the lock release each drop ends with.
    defp measure_peak_release(fun) do
      inflight = :atomics.new(2, [])

      Application.put_env(
        :fathom,
        :faulty_before,
        {:release,
         fn _ ->
           now = :atomics.add_get(inflight, 1, 1)
           peak = :atomics.get(inflight, 2)
           if now > peak, do: :atomics.compare_exchange(inflight, 2, peak, now)
           Process.sleep(50)
           :atomics.sub(inflight, 1, 1)
         end}
      )

      fun.()
      :atomics.get(inflight, 2)
    end

    defp run_pass(metrics?) do
      Application.put_env(:fathom, :metrics_collector, metrics?)
      Application.put_env(:fathom, :shard_storage, FaultyStorage)
      Application.put_env(:fathom, :pre_evict_min_idle_ms, 0)
      base = Shards.open_count()
      # cap 60 with headroom 24: one admission at the cap leaves 25 victims to drop in one pass.
      Application.put_env(:fathom, :max_open_shards, base + 1_000)
      ids = for _ <- 1..60, do: open_idle(uniq())
      Application.put_env(:fathom, :max_open_shards, base + 61)
      Application.put_env(:fathom, :pre_evict_headroom, 24)

      peak =
        measure_peak_release(fn ->
          ids = ids ++ [open_idle(uniq())]
          send(self(), {:ids, ids})
          await_pre_evictor()
        end)

      ids = receive(do: ({:ids, ids} -> ids))
      on_exit(fn -> cleanup(ids) end)
      peak
    end

    test "clean victims drop more than 8 at a time" do
      # With the metrics collector on (prod's default) an unwritten shard is known clean.
      peak = run_pass(true)
      # Pre-fix: a fixed concurrency of 8.
      assert peak > 8, "peak concurrent drops #{peak}"
    end

    test "victims not known to be clean stay at 8 at a time" do
      # With it off (the test default) nothing is known clean, so every victim counts as dirty.
      assert run_pass(false) <= 8
    end
  end
end
