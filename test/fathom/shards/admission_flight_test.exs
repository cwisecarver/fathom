defmodule Fathom.Shards.AdmissionFlightTest do
  @moduledoc """
  Per-shard single-flight for a cold admission (expert review 2026-10-21 r7 #6).

  Symptom: K concurrent first requests for ONE cold shard each ran the whole admission, because
  only `start/1` deduped (`{:already_started, pid}`), after the eviction, the limiter token and the
  novelty read. Measured in-process at the cap with 25 ms injected on the lock release and the
  pull: K=16 checkouts of one cold id evicted 16 tenants to admit one shard (post rv6 F10, whose
  claims made the victims distinct), and K=32 refused some of its own requests with
  `:node_at_capacity`.

  Pinned invariants (the first two fail against the pre-fix `Fathom.Shards`):
    * at the cap, K concurrent checkouts of one cold id evict exactly ONE tenant, all K succeed,
      and the population stays at the cap (both liveness modes; eviction drops a lease);
    * with a novel-shard rate limit, K concurrent first requests of one new tenant spend ONE token,
      so a burst smaller than K does not 429 the tenant's own requests;
    * the flight primitive: a dead leader's followers re-resolve and clear its row, and a follower
      that times out leaves no message behind in its mailbox.
  """
  use Fathom.DataCase, async: false

  alias Fathom.Shards
  alias Fathom.Shard.Heartbeat
  alias Fathom.Shards.{AdmissionFlight, Lru, NovelLimiter}
  alias Fathom.Test.FaultyStorage

  @moduletag :capture_log
  @modes [:legacy, :heartbeat]
  @k 16
  @fill 8

  @env [
    :shard_storage,
    :faulty_before,
    :storage_pull_delay_ms,
    :max_open_shards,
    :shard_idle_ms,
    :pre_evict_headroom,
    :evict_idle_at_capacity,
    :novel_shard_rate,
    :novel_shard_burst
  ]

  setup do
    prev = Map.new(@env, &{&1, Application.get_env(:fathom, &1)})
    Lru.reset()
    test_pid = self()
    handler = "admission_flight_test_#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shards, :evicted],
      fn _, _, %{shard_id: id}, _ -> send(test_pid, {:evicted, id}) end,
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

    Application.put_env(:fathom, :shard_storage, FaultyStorage)
    Application.put_env(:fathom, :shard_idle_ms, 600_000)
    Application.put_env(:fathom, :evict_idle_at_capacity, true)
    Application.put_env(:fathom, :pre_evict_headroom, 0)
    # The victim's lock release (an S3 DELETE) takes a while, so the leader's admission is still
    # in flight when the other K-1 requests arrive: the race the probe measured.
    Application.put_env(:fathom, :faulty_before, {:release, fn _ -> Process.sleep(25) end})
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

  defp uniq, do: "admfl_#{System.unique_integer([:positive])}"

  defp open_idle(id) do
    {:ok, pid, ref, _path} = Shards.checkout(id)
    Fathom.Shard.checkin(pid, ref)
    _ = :sys.get_state(pid)
    id
  end

  # Fill the node with idle shards, then put the cap exactly at the population.
  defp fill_to_cap do
    Application.put_env(:fathom, :max_open_shards, Shards.open_count() + 10 * @fill)
    filled = for _ <- 1..@fill, do: open_idle(uniq())
    cap = Shards.open_count()
    Application.put_env(:fathom, :max_open_shards, cap)
    {filled, cap}
  end

  defp burst(id, k) do
    1..k
    |> Task.async_stream(
      fn _ ->
        case Shards.checkout(id) do
          {:ok, pid, ref, _path} ->
            Fathom.Shard.checkin(pid, ref)
            :ok

          {:error, _} = error ->
            error
        end
      end,
      max_concurrency: k,
      timeout: 30_000
    )
    |> Enum.map(fn {:ok, r} -> r end)
  end

  defp drain_evicted(acc \\ []) do
    receive do
      {:evicted, id} -> drain_evicted([id | acc])
    after
      0 -> acc
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
    test "#{mode}: K concurrent checkouts of one cold shard at the cap evict one tenant",
         %{mode: mode} do
      set_mode!(mode)
      {filled, cap} = fill_to_cap()
      assert_mode!(hd(filled), mode)
      hot = uniq()
      on_exit(fn -> cleanup([hot | filled]) end)

      results = burst(hot, @k)

      assert Enum.all?(results, &(&1 == :ok)),
             "every request for the admitted shard must succeed: #{inspect(results)}"

      assert_mode!(hot, mode)
      evicted = drain_evicted()

      # Pre-fix: one eviction per concurrent request (16 here), and the node fell below the cap.
      assert length(evicted) == 1,
             "one admission must evict one tenant, evicted #{length(evicted)}: #{inspect(evicted)}"

      assert Shards.open_count() == cap
    end
  end

  test "a new tenant's concurrent first requests spend ONE novel-shard token" do
    # At the cap, so the leader's admission includes an eviction and the others arrive mid-flight.
    # Filled before the gate is on: the fill shards are novel too.
    {filled, _cap} = fill_to_cap()
    Application.put_env(:fathom, :novel_shard_rate, 1)
    Application.put_env(:fathom, :novel_shard_burst, 4)
    :ok = NovelLimiter.reset()
    novel = uniq()
    on_exit(fn -> cleanup([novel | filled]) end)

    results = burst(novel, @k)

    # Pre-fix: each concurrent request took its own token; with a burst of 4, 12 of the 16 were
    # refused `:novel_shard_rate_limited` (429) on the tenant's very first connection burst.
    assert Enum.all?(results, &(&1 == :ok)),
           "the tenant must not rate-limit itself: #{inspect(Enum.frequencies(results))}"
  end

  describe "the flight primitive" do
    test "a leader that dies mid-flight releases its followers, who clear its row" do
      id = uniq()
      parent = self()

      leader =
        spawn(fn ->
          send(parent, {:joined, AdmissionFlight.join(id, 1_000)})

          receive do
            :die -> exit(:killed_mid_admission)
          end
        end)

      assert_receive {:joined, :leader}
      follower = Task.async(fn -> AdmissionFlight.join(id, 5_000) end)
      # The follower is registered and waiting before the leader dies.
      wait_until(fn -> :ets.lookup(AdmissionFlight.Followers, id) != [] end)
      send(leader, :die)

      assert Task.await(follower) == :retry
      assert :ets.lookup(AdmissionFlight, id) == []
      assert :ets.lookup(AdmissionFlight.Followers, id) == []
      # The id is free for the next admission.
      assert AdmissionFlight.join(id, 10) == :leader
      AdmissionFlight.finish(id, :done)
    end

    test "followers receive the leader's result" do
      id = uniq()
      assert AdmissionFlight.join(id, 10) == :leader
      followers = for _ <- 1..4, do: Task.async(fn -> AdmissionFlight.join(id, 5_000) end)
      wait_until(fn -> length(:ets.lookup(AdmissionFlight.Followers, id)) == 4 end)

      AdmissionFlight.finish(id, {:error, :node_at_capacity})

      for f <- followers, do: assert(Task.await(f) == {:shared, {:error, :node_at_capacity}})
      assert :ets.lookup(AdmissionFlight, id) == []
    end

    test "a follower that times out keeps nothing in its mailbox when the result lands later" do
      id = uniq()
      assert AdmissionFlight.join(id, 10) == :leader

      follower =
        Task.async(fn ->
          r = AdmissionFlight.join(id, 50)

          receive do
            :check -> :ok
          end

          {r, Process.info(self(), :message_queue_len)}
        end)

      wait_until(fn -> :ets.lookup(AdmissionFlight.Followers, id) != [] end)
      # The address the leader would send to, captured while the follower is still waiting.
      [{^id, alias_ref}] = :ets.lookup(AdmissionFlight.Followers, id)
      wait_until(fn -> :ets.lookup(AdmissionFlight.Followers, id) == [] end)
      # A result that lands after the follower gave up (the leader `take`s just before the
      # follower's own row delete) is dropped by the runtime, not queued in a long-lived stream.
      send(alias_ref, {AdmissionFlight, alias_ref, {:ok, self()}})
      AdmissionFlight.finish(id, {:ok, self()})
      send(follower.pid, :check)

      assert {:timeout, {:message_queue_len, 0}} = Task.await(follower)
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
end
