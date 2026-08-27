defmodule Fathom.ShardsDrainAllTest do
  # Expert review #28: node-level graceful drain. Everything drained per-shard before; there was no
  # node-scope lever, so every deploy funnelled through the crash-adjacent supervisor-shutdown path
  # (burst flushes, hard-cut streams). drain_all/1 walks the open coordinators and runs ordered
  # voluntary drains within a bounded budget, and the HealthPlug "draining" state stops the LB
  # routing first. Not async — shards + the Registry + the health flag are global.
  use ExUnit.Case, async: false

  alias Fathom.{HealthPlug, ShardExecutor, Shards}

  setup do
    prev_idle = Application.get_env(:fathom, :shard_idle_ms)
    # High idle so a just-opened coordinator doesn't idle-stop before drain_all runs.
    Application.put_env(:fathom, :shard_idle_ms, 60_000)
    ids = for i <- 1..3, do: "drainall_#{System.unique_integer([:positive])}_#{i}"

    on_exit(fn ->
      HealthPlug.end_draining()
      restore(:shard_idle_ms, prev_idle)

      for id <- ids, do: Shards.drain(id, 2_000)

      for id <- ids,
          dir <- [local_dir(), remote_dir()],
          s <- [".db", ".db-wal", ".db-shm", ".db.etag", ".lock"],
          do: File.rm(Path.join(dir, id <> s))
    end)

    %{ids: ids}
  end

  defp restore(k, nil), do: Application.delete_env(:fathom, k)
  defp restore(k, v), do: Application.put_env(:fathom, k, v)

  defp open_idle!(id) do
    {:ok, conn} = ShardExecutor.open(id)
    :ok = ShardExecutor.close(conn)
    {:ok, pid} = Shards.ensure(id)
    pid
  end

  # Per-shard invariants, not exact tallies: drain_all is node-global, so a coordinator leaked by an
  # earlier test would inflate the totals. What must hold is that MY coordinators drained.
  test "drain_all voluntarily drains every idle open coordinator", %{ids: ids} do
    pids = Enum.map(ids, &open_idle!/1)

    result = Shards.drain_all(3_000)

    assert HealthPlug.draining?(), "drain_all deregisters the node from the LB first"

    assert result.drained >= 3,
           "at least this test's 3 idle coordinators drained (#{inspect(result)})"

    for pid <- pids,
        do: refute(Process.alive?(pid), "each coordinator flushed + released + stopped")
  end

  # Expert review 2026-08-26 #5. drain_all/1 passed each coordinator the WHOLE remaining budget
  # while its docstring claimed "each coordinator gets a slice". With the defaults the first 16
  # coordinators each got a 50 s window, so a handful of busy shards in the first wave burned the
  # entire 55 s budget and every shard behind them got `deadline - now <= 0` — returning
  # :timed_out WITHOUT BEING CONTACTED, and falling to the unbounded supervisor teardown that
  # prep_stop/1 exists to avoid.
  #
  # TWO THINGS THIS TEST NEEDS, both learned by writing it wrong first:
  #
  #   * CONCURRENCY 1. The default is 16, so three shards all run in ONE wave and nothing is behind
  #     anything — the first draft passed against the unfixed code for that reason. Starvation is a
  #     property of SEQUENTIAL waves.
  #   * EVERY shard busy. With a mix, whether starvation is observed depends on where the busy
  #     shard lands in `Registry.select` order, which is not controllable — a coin-flip test. If
  #     they are all busy, order stops mattering: pre-fix the first consumes the whole budget and
  #     the rest report :timed_out; post-fix each gets a slice and reports :busy.
  test "a busy shard does not consume the whole budget and strand the shards behind it", %{
    ids: ids
  } do
    prev_conc = Application.get_env(:fathom, :drain_all_concurrency)
    prev_grace = Application.get_env(:fathom, :drain_all_flush_grace_ms)
    Application.put_env(:fathom, :drain_all_concurrency, 1)

    # A SMALL grace, not zero. At the 5 s default a test-sized budget makes `remaining - grace`
    # negative for every shard, so they all abort instantly and nothing is consumed (draft two of
    # this test passed against the unfixed code for that reason). At exactly zero the drain window
    # equals the exit wait, so the coordinator's abort races the await's timeout and everything
    # reports :timed_out (draft three failed BOTH ways for that reason). It has to sit between.
    Application.put_env(:fathom, :drain_all_flush_grace_ms, 200)

    on_exit(fn ->
      restore(:drain_all_concurrency, prev_conc)
      restore(:drain_all_flush_grace_ms, prev_grace)
    end)

    conns =
      for id <- ids do
        {:ok, conn} = ShardExecutor.open(id)
        {:ok, _} = Shards.ensure(id)
        conn
      end

    on_exit(fn -> Enum.each(conns, &ShardExecutor.close/1) end)

    test_pid = self()
    handler = "drainslice-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:fathom, :shards, :drain_all, :slice],
      fn _e, m, _meta, _ -> send(test_pid, {:slice, m.window_ms}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    result = Shards.drain_all(3_600)

    windows =
      for _ <- ids do
        assert_receive {:slice, w}, 10_000
        w
      end

    # THE ASSERTION IS FAIRNESS, not non-zero, and getting there took four drafts. What the
    # unfixed code produces is windows like [3600, 200, 200]: the first shard is handed the entire
    # budget, drains for nearly all of it, and the shards behind it are left the crumbs. They are
    # not literally zero, so "min > 0" passes against the defect.
    #
    # The audit predicted the leftovers would surface as `:timed_out`. Traced, they do not: a
    # crumb-window shard returns `:busy` immediately rather than exhausting the budget, so the
    # %{drained:, busy:, timed_out:} tally is identical either way. That is why the window itself
    # is emitted and asserted — it is the decision this fix makes, and the only thing that differs.
    #
    # Half of a fair share is a deliberately loose bound: the slice is recomputed per wave from the
    # remaining budget, so a shard that drains early legitimately hands time to the ones behind it
    # and the windows are not identical.
    fair_share = div(3_600, length(ids))

    assert Enum.min(windows) >= div(fair_share, 2),
           "a coordinator was given only #{Enum.min(windows)}ms of drain window against a fair " <>
             "share of #{fair_share}ms — the shards ahead of it consumed the budget, so its " <>
             "streams are hard-cut rather than drained (windows: #{inspect(windows)}, " <>
             "result: #{inspect(result)})"

    assert Enum.max(windows) <= 3_600,
           "a coordinator was handed more than the whole budget (windows: #{inspect(windows)})"
  end

  test "a busy coordinator (a held connection) is reported busy, not hard-cut", %{ids: [id | _]} do
    {:ok, conn} = ShardExecutor.open(id)
    {:ok, pid} = Shards.ensure(id)

    # A short budget so the (held-stream) drain aborts quickly instead of waiting the full window.
    result = Shards.drain_all(500)

    assert result.busy >= 1,
           "the held-connection coordinator is busy, not drained (#{inspect(result)})"

    assert Process.alive?(pid),
           "a busy coordinator keeps serving; drain_all is voluntary, never a cut"

    :ok = ShardExecutor.close(conn)
  end

  test "health returns 503 while draining, 200 otherwise (#28 LB deregistration)" do
    HealthPlug.end_draining()
    assert probe() == {200, "ok"}

    HealthPlug.begin_draining()
    assert probe() == {503, "draining"}

    HealthPlug.end_draining()
    assert probe() == {200, "ok"}
  end

  defp probe do
    conn = HealthPlug.call(Plug.Test.conn(:get, "/health"), [])
    {conn.status, conn.resp_body}
  end

  defp local_dir, do: Fathom.Shard.data_dir()
  defp remote_dir, do: Fathom.Shard.Storage.Local.dir()
end
