defmodule Fathom.Shard.BusyFairnessBenchTest do
  # Expert review 2026-10-20-2027 #13 (the time suffix separates it from the earlier 2026-10-20
  # series): the busy-yield retry was unfair. Every waiter ran its own 1..100 ms ladder per
  # statement, so a waiter that had already backed off to 50-100 ms steps lost the lock to every
  # newcomer still on 1-2 ms steps (~LIFO). Measured with this probe: K=16 writers on one shard
  # p99 ~11 ms, K=32 ~38 ms. The fix is a per-shard FIFO wait queue (`Connection`'s busy queue):
  # the stream whose statement leaves the write lock free wakes the queue's head.
  #
  # K closed-loop writers on ONE shard, each on its own tenant handle, for a fixed window; reports
  # per-write latency percentiles and asserts an order-of-magnitude ceiling on p99. Two shapes:
  # bare autocommit INSERTs, and Django's `transaction.atomic` (BEGIN, INSERT, COMMIT), where the
  # lock is held across three statements and released only by the COMMIT.
  #
  # @tag :bench: excluded from the default suite (seconds per arm, host-load sensitive). Run with
  # `mix test --include bench test/fathom/shard/busy_fairness_bench_test.exs`.
  use ExUnit.Case, async: false

  alias Fathom.Shard.Connection

  @moduletag :bench
  @window_ms 2_000

  setup do
    unless Fathom.Shard.Extension.available?(),
      do: flunk("fathom_udf extension not built: the busy yield (and this probe) needs it")

    prev = Application.get_env(:fathom, :query_timeout_ms)
    Application.delete_env(:fathom, :query_timeout_ms)
    on_exit(fn -> if prev, do: Application.put_env(:fathom, :query_timeout_ms, prev) end)
    :ok
  end

  # Ceilings are loose (an order of magnitude above the FIFO result, well below the ladder's) so
  # host noise does not flake them; the printed numbers are the measurement.
  for {k, shape, ceiling_ms} <- [
        {16, :autocommit, 8},
        {32, :autocommit, 15},
        {16, :django, 15},
        {32, :django, 30},
        {16, :django_rtt, 150},
        {32, :django_rtt, 300}
      ] do
    test "K=#{k} #{shape} writers on one shard: p99 stays low" do
      # The [:fathom, :shard, :busy_retry] counter: episodes, re-steps, and timer overrun.
      stats = :counters.new(3, [])
      handler = "busy-fairness-#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach(
          handler,
          [:fathom, :shard, :busy_retry],
          fn _, m, _, c ->
            :counters.add(c, 1, 1)
            :counters.add(c, 2, m.attempts)
            :counters.add(c, 3, m.oversleep_us)
          end,
          stats
        )

      {p50, p99, max, max_at, n, failures} =
        try do
          run(unquote(k), unquote(shape))
        after
          :telemetry.detach(handler)
        end

      IO.puts(
        "busy_fairness K=#{unquote(k)} #{unquote(shape)}: writes=#{n} failures=#{failures} " <>
          "p50=#{fmt(p50)} p99=#{fmt(p99)} max=#{fmt(max)} ms (at #{max_at} ms) | busy_retry episodes=" <>
          "#{:counters.get(stats, 1)} attempts=#{:counters.get(stats, 2)} " <>
          "oversleep=#{fmt(:counters.get(stats, 3))} ms"
      )

      assert failures == 0
      assert p99 < unquote(ceiling_ms) * 1000, "p99 #{fmt(p99)} ms"
    end
  end

  defp fmt(us), do: :erlang.float_to_binary(us / 1000, decimals: 2)

  defp run(k, shape) do
    path =
      Path.join(
        System.tmp_dir!(),
        "busy_fair_#{k}_#{shape}_#{System.unique_integer([:positive])}.db"
      )

    on_exit(fn -> for s <- ["", "-wal", "-shm"], do: File.rm(path <> s) end)
    {:ok, setup_conn} = Connection.open(path, tenant?: true)
    :ok = Connection.exec(setup_conn, "CREATE TABLE t (a INTEGER)")
    test = self()

    pids =
      for i <- 1..k do
        spawn_link(fn ->
          {:ok, conn} = Connection.open(path, tenant?: true)
          send(test, {:ready, self()})

          receive do
            {:go, until} -> send(test, {:done, self(), loop(conn, shape, i, until, [], 0)})
          end

          Connection.close(conn)
        end)
      end

    for pid <- pids, do: assert_receive({:ready, ^pid}, 5_000)
    until = System.monotonic_time(:microsecond) + @window_ms * 1000
    for pid <- pids, do: send(pid, {:go, until})

    {lats, failures} =
      Enum.reduce(pids, {[], 0}, fn pid, {acc, f} ->
        assert_receive {:done, ^pid, {lats, fails}}, @window_ms + 10_000
        {lats ++ acc, f + fails}
      end)

    Connection.close(setup_conn)
    sorted = Enum.sort(lats)
    n = length(sorted)
    pct = fn p -> sorted |> Enum.at(min(n - 1, trunc(n * p))) |> elem(0) end
    {max, max_t0} = List.last(sorted)
    start = until - @window_ms * 1000
    {pct.(0.5), pct.(0.99), max, div(max_t0 - start, 1000), n, failures}
  end

  defp loop(conn, shape, i, until, lats, fails) do
    t0 = System.monotonic_time(:microsecond)

    if t0 >= until do
      {lats, fails}
    else
      ok? = match?({:ok, _}, write(conn, shape, i))
      if not ok?, do: Connection.rollback_if_open(conn)
      lat = System.monotonic_time(:microsecond) - t0
      loop(conn, shape, i, until, [{lat, t0} | lats], if(ok?, do: fails, else: fails + 1))
    end
  end

  # `dml?` as `ShardExecutor` sets it (its verb check): true for the INSERT, false for
  # BEGIN/COMMIT. `Connection.query/4` defaults it to true, which would gate BEGIN too.
  defp q(conn, sql, args \\ []) do
    Connection.query(conn, sql, args, deadline: true, dml?: String.starts_with?(sql, "INSERT"))
  end

  defp write(conn, :autocommit, i), do: q(conn, "INSERT INTO t VALUES (?1)", [i])

  # Django with a 1 ms client round trip between statements and between transactions: the lock
  # is held across two round trips, the shape a real app (network + ORM) puts on a hot shard.
  defp write(conn, :django_rtt, i) do
    rtt()

    with {:ok, _} <- q(conn, "BEGIN"),
         :ok <- rtt(),
         {:ok, _} = ok <- q(conn, "INSERT INTO t VALUES (?1)", [i]),
         :ok <- rtt(),
         {:ok, _} <- q(conn, "COMMIT") do
      ok
    end
  end

  defp write(conn, :django, i) do
    with {:ok, _} <- q(conn, "BEGIN"),
         {:ok, _} = ok <- q(conn, "INSERT INTO t VALUES (?1)", [i]),
         {:ok, _} <- q(conn, "COMMIT") do
      ok
    end
  end

  defp rtt do
    receive do
    after
      1 -> :ok
    end
  end
end
