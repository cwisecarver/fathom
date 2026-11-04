defmodule Fathom.Tenants.TombstoneBurstBenchTest do
  @moduledoc """
  Watch-only measurement for expert review 2026-11-04 #6: a burst of tombstones used to cost one
  whole-data-dir listing per id per node (unbounded concurrency). Data dir of ~100k stub files, 200
  ids tombstoned through the real `Tombstones.put/1` -> `on_tombstoned` path; records wall time to
  every local erase, peak `:erlang.memory(:total)`, and p99 of point queries on an unrelated shard
  (vs a no-tombstone baseline loop). Prints numbers; asserts only that all 200 were erased and an
  unrelated stub survived. Run: `mix test --include bench <this file>`.
  """
  use ExUnit.Case, async: false
  @moduletag :bench
  @moduletag timeout: 600_000

  alias Fathom.ShardExecutor
  alias Fathom.Tenants.Tombstones
  alias Filo.Stmt

  @stubs 100_000
  @burst 200

  setup do
    prev = Application.get_env(:fathom, :shard_data_dir)
    dir = Path.join(System.tmp_dir!(), "fathom_tomb_burst_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    Application.put_env(:fathom, :shard_data_dir, dir)

    on_exit(fn ->
      if is_nil(prev),
        do: Application.delete_env(:fathom, :shard_data_dir),
        else: Application.put_env(:fathom, :shard_data_dir, prev)

      File.rm_rf(dir)
    end)

    %{dir: dir}
  end

  defp p99(xs) do
    s = Enum.sort(xs)
    Enum.at(s, min(length(s) - 1, trunc(length(s) * 0.99)))
  end

  defp query_loop(conn, parent, acc \\ []) do
    t = System.monotonic_time(:microsecond)
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "SELECT 1", args: []})
    acc = [System.monotonic_time(:microsecond) - t | acc]

    receive do
      {:stop, ^parent} -> acc
    after
      0 -> query_loop(conn, parent, acc)
    end
  end

  defp sample_loop(parent, mx) do
    Process.sleep(5)
    mx = max(mx, :erlang.memory(:total))

    receive do
      {:stop, ^parent} -> mx
    after
      0 -> sample_loop(parent, mx)
    end
  end

  test "burst of #{@burst} tombstones over #{@stubs} stub files", %{dir: dir} do
    for i <- 1..@stubs, do: :ok = Fathom.FS.write(Path.join(dir, "stub#{i}.db"), "")
    victims = for i <- 1..@burst, do: "victim#{i}"

    for v <- victims,
        f <- [".db", ".db-wal", ".db.etag"],
        do: :ok = Fathom.FS.write(Path.join(dir, v <> f), "x")

    unrelated = "unrelated_#{System.unique_integer([:positive])}"
    {:ok, conn} = ShardExecutor.open(unrelated)
    {:ok, _} = ShardExecutor.execute(conn, %Stmt{sql: "SELECT 1", args: []})

    me = self()

    base_task = Task.async(fn -> query_loop(conn, me) end)
    Process.sleep(2_000)
    send(base_task.pid, {:stop, me})
    base = Task.await(base_task, 10_000)

    :erlang.garbage_collect()

    :telemetry.attach(
      "burst-bench",
      [:fathom, :tenants, :local_erased],
      fn _, _, _, pid -> send(pid, :erased) end,
      me
    )

    sampler = Task.async(fn -> sample_loop(me, 0) end)

    mem0 = :erlang.memory(:total)
    q_task = Task.async(fn -> query_loop(conn, me) end)
    t0 = System.monotonic_time(:millisecond)
    for v <- victims, do: Tombstones.put(v)
    for _ <- 1..@burst, do: assert_receive(:erased, 300_000)
    wall = System.monotonic_time(:millisecond) - t0
    send(q_task.pid, {:stop, me})
    during = Task.await(q_task, 10_000)
    send(sampler.pid, {:stop, me})
    peak = Task.await(sampler, 10_000)
    :telemetry.detach("burst-bench")

    IO.puts("""

    BURST_BENCH stubs=#{@stubs} burst=#{@burst}
      wall_to_all_erased_ms=#{wall}
      peak_mem_delta_mb=#{Float.round((peak - mem0) / 1_048_576, 1)}
      baseline_q n=#{length(base)} p99_us=#{p99(base)}
      during_q   n=#{length(during)} p99_us=#{p99(during)} max_us=#{Enum.max(during)}
    """)

    for v <- victims, do: refute(File.exists?(Path.join(dir, v <> ".db")))
    assert File.exists?(Path.join(dir, "stub1.db"))
    ShardExecutor.close(conn)
  end
end
