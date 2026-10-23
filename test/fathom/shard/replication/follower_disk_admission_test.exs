defmodule Fathom.Shard.Replication.FollowerDiskAdmissionTest do
  @moduledoc """
  Seed admission's volume reading (expert review 2026-10-22 r8 #10, decision Q10 = A).

  A seed begin used to read the replica volume itself whenever its 1 s cache had expired: a `df -Pi`
  fork plus a call queued on the single `disksup` process, from the follower's per-connection
  worker, while holding the seeding shard's lock. In a seed storm every begin that arrived during a
  refresh ran its own pair (no single-flight), and every push for the shards hashed to that worker
  waited behind it.

  Now readers never fork: an expired reading is returned as-is and ONE background refresh replaces
  it; only with no reading at all does one caller read synchronously. These tests pin that, plus
  the reservation keying the move ahead of the lock depends on.
  """
  use ExUnit.Case, async: false

  alias Fathom.Shard.Replication.Follower
  alias Fathom.Shard.Replication.Protocol

  setup do
    root = Path.join(System.tmp_dir!(), "replcdisk_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    keys = [
      :replication_disk_free_floor_bytes,
      :replication_disk_free_floor_inodes,
      :replication_shard_lock_wait_ms
    ]

    prev = for k <- keys, do: {k, Application.get_env(:fathom, k)}

    on_exit(fn ->
      for {k, v} <- prev do
        if is_nil(v),
          do: Application.delete_env(:fathom, k),
          else: Application.put_env(:fathom, k, v)
      end

      File.rm_rf(root)
    end)

    # Floors any real volume clears, so only an injected reading can refuse.
    Application.put_env(:fathom, :replication_disk_free_floor_bytes, 1)
    Application.put_env(:fathom, :replication_disk_free_floor_inodes, 1)

    name = :"cdisk_f#{System.unique_integer([:positive])}"
    dir = Path.join(root, "replica")
    pid = start_supervised!({Follower, name: name, port: 0, dir: dir}, id: name)
    # Boot housekeeping (which starts the priming refresh) has run once this returns. One begin
    # also guarantees a reading on either side of the fix (the pre-fix follower read on demand).
    _ = :sys.get_state(pid)
    name |> Follower.begin_seed(%{}, begin("cd_prime")) |> Follower.discard_seeds()
    await_reading!(name, dir)

    %{name: name, dir: dir}
  end

  defp begin(id, size \\ 4096) do
    %Protocol.SeedBegin{
      shard_id: id,
      epoch: 1,
      wal_gen: 0,
      salt1: 0,
      wal_offset: 0,
      db_size: size,
      wal_size: 0
    }
  end

  defp reading(name, dir) do
    case :ets.lookup(Follower.locks(name), {:disk, dir}) do
      [{_, at, result}] -> {at, result}
      [] -> nil
    end
  end

  defp refreshing?(name, dir), do: :ets.lookup(Follower.locks(name), {:disk_refresh, dir}) != []

  # A reading is present and no refresh is in flight.
  defp await_reading!(name, dir, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      cond do
        reading(name, dir) != nil and not refreshing?(name, dir) -> true
        System.monotonic_time(:millisecond) > deadline -> false
        true -> Process.sleep(5) && nil
      end
    end)
    |> Enum.find(&is_boolean/1)
    |> assert("no volume reading landed for #{dir}")
  end

  # An EXPIRED reading saying the volume is full.
  defp inject_stale_full!(name, dir) do
    stale_at = System.monotonic_time(:millisecond) - 60_000
    :ets.insert(Follower.locks(name), {{:disk, dir}, stale_at, {:ok, 1, nil}})
  end

  # REGRESSION: before r8 #10 an expired reading was ignored and the begin read the volume itself,
  # synchronously, under the shard lock. Here the expired reading says "full": the begin refusing
  # proves it used the reading it had instead of forking; the background refresh then replaces it
  # and the next begin is admitted. Fails against the pre-fix follower.ex (the first begin is
  # admitted off its own fresh read).
  test "an expired reading is used as-is and refreshed in the background", %{name: name, dir: dir} do
    inject_stale_full!(name, dir)

    refute Map.has_key?(Follower.begin_seed(name, %{}, begin("cd_stale")), "cd_stale"),
           "the begin read the volume itself instead of using the cached (expired) reading"

    await_reading!(name, dir)
    assert {_at, {:ok, free, _}} = reading(name, dir)
    assert free > 1, "the background refresh never replaced the expired reading"

    seeds = Follower.begin_seed(name, %{}, begin("cd_stale"))
    assert %{failed: false} = seeds["cd_stale"]
    Follower.discard_seeds(seeds)
  end

  # The node poller's one `df` (`Measurements.disk/0`) feeds this same cache, so the gauge and seed
  # admission read the same number.
  test "the poller's reading is what admission uses", %{name: name, dir: dir} do
    :ok = Follower.note_disk_reading(dir, {:ok, 1, nil}, name)
    refute Map.has_key?(Follower.begin_seed(name, %{}, begin("cd_noted")), "cd_noted")
  end

  test "a burst of begins on an expired reading starts exactly one refresh", %{
    name: name,
    dir: dir
  } do
    {at0, real} = reading(name, dir)
    :ets.insert(Follower.locks(name), {{:disk, dir}, at0 - 60_000, real})

    test_pid = self()

    pids =
      for i <- 1..16 do
        spawn(fn ->
          receive do
            :go -> :ok
          end

          seeds = Follower.begin_seed(name, %{}, begin("cd_burst_#{i}"))
          send(test_pid, {:done, self(), Map.has_key?(seeds, "cd_burst_#{i}")})
          Follower.discard_seeds(seeds)
        end)
      end

    trace_reads!(pids)
    for p <- pids, do: send(p, :go)
    for p <- pids, do: assert_receive({:done, ^p, true}, 5_000)

    refute_received {:trace, _, :call, {Follower, :read_disk_free, _}},
                    "a begin read the volume itself"

    await_reading!(name, dir)
    {at1, _} = reading(name, dir)
    assert at1 > at0 - 60_000, "nothing refreshed the expired reading"
  end

  # Moving the headroom check ahead of the shard lock means two connections can hold a reservation
  # for the SAME shard at once. Keyed by shard alone (the old key), the second begin's row replaced
  # the first's and its release on a busy lock deleted it, under-counting a seed that is streaming.
  test "a same-shard begin refused on a busy lock leaves the streaming seed's reservation", %{
    name: name
  } do
    Application.put_env(:fathom, :replication_shard_lock_wait_ms, 20)
    seeds = Follower.begin_seed(name, %{}, begin("cd_same", 12_345))
    assert %{failed: false} = seeds["cd_same"]

    task =
      Task.async(fn -> Follower.begin_seed(name, %{}, begin("cd_same", 999))["cd_same"] end)

    assert %{failed: true} = Task.await(task)

    me = self()

    assert [{{:reserved, "cd_same", ^me}, ^me, 12_345}] =
             :ets.match_object(Follower.locks(name), {{:reserved, "cd_same", :_}, :_, :_}),
           "the busy begin's release removed (or its row replaced) the streaming seed's reservation"

    Follower.discard_seeds(seeds)
  end

  # Sixty-four concurrent begins on an expired reading (the seed-storm shape): none reads the
  # volume itself, and none waits on a subprocess.
  @tag :bench
  test "64 concurrent begins on an expired reading: no caller forks, p99 bounded", %{
    name: name,
    dir: dir
  } do
    {at0, real} = reading(name, dir)
    :ets.insert(Follower.locks(name), {{:disk, dir}, at0 - 60_000, real})
    test_pid = self()

    pids =
      for i <- 1..64 do
        spawn(fn ->
          receive do
            :go -> :ok
          end

          t0 = System.monotonic_time(:microsecond)
          seeds = Follower.begin_seed(name, %{}, begin("cd_bench_#{i}"))
          us = System.monotonic_time(:microsecond) - t0
          send(test_pid, {:done, self(), us, Map.has_key?(seeds, "cd_bench_#{i}")})
          Follower.discard_seeds(seeds)
        end)
      end

    trace_reads!(pids)
    for p <- pids, do: send(p, :go)

    times =
      for p <- pids do
        assert_receive {:done, ^p, us, true}, 10_000
        us
      end

    p99 = times |> Enum.sort() |> Enum.at(62)
    IO.puts("begin_seed x64 on an expired reading: p99 #{p99} us")

    refute_received {:trace, _, :call, {Follower, :read_disk_free, _}}
    assert p99 < 20_000, "a begin waited on something slow (pre-fix ~110 ms): p99 #{p99} us"
  end

  # Trace calls to the private volume read made BY these processes (not by anything they spawn: the
  # background refresh is a separate, untraced process, which is the point).
  defp trace_reads!(pids) do
    :erlang.trace_pattern({Follower, :read_disk_free, 1}, true, [:local])
    for p <- pids, do: :erlang.trace(p, true, [:call, {:tracer, self()}])

    on_exit(fn -> :erlang.trace_pattern({Follower, :read_disk_free, 1}, false, [:local]) end)
  end
end
