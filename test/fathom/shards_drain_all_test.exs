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
